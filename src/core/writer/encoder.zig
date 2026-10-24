//! Parquet PLAIN encoder for column chunks.
//!
//! Supports the re-encode path: per-type physical encodings, optional
//! compression, dictionary/delta choices where profitable, and data pages
//! cut at a target (`default_page_bytes`, `default_page_rows`) with what each
//! page's index entry needs. The encoder owns the page bytes and the matching
//! ColumnMetaData so callers can assemble a valid footer.
//!
//! Design: the per-page primitive matches DuckDB / Polars: emit a
//! PageHeader thrift, then the encoded data bytes. ColumnMetaData
//! is built side-by-side with the bytes so the caller can drop it
//! into the file footer.

const std = @import("std");
test "a proven unique integer column skips only a declined dictionary attempt" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 10, 64, 65, 5000 }) |n| for ([_]bool{ false, true }) |nulls| {
        const values = try a.alloc(i64, n);
        const defs = try a.alloc(u32, n);
        for (values, defs, 0..) |*v, *d, i| {
            v.* = @as(i64, @intCast(i)) * -730201;
            d.* = @intFromBool(i % 13 != 0);
        }
        const elem: schema.SchemaElement = .{
            .name = "k",
            .type = .INT64,
            .repetition_type = if (nulls) .OPTIONAL else .REQUIRED,
            .type_length = null,
            .num_children = 0,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        const input: ColumnInput = .{
            .values = .{ .i64 = .{
                .values = values,
                .def_levels = if (nulls) defs else null,
                .max_def = @intFromBool(nulls),
                .has_nulls = nulls,
            } },
            .schema_elem = &elem,
            .path_in_schema = &.{"k"},
        };
        const expected = try encodeColumn(a, input);
        var skipped = input;
        skipped.distinct = true;
        const got = try encodeColumn(a, skipped);
        try std.testing.expectEqualSlices(u8, expected.bytes, got.bytes);
    };
}
const schema = @import("../schema.zig");
test "a proven unique short byte column skips only a declined dictionary attempt" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    for ([_]usize{ 10, 64, 65, 5000 }) |n| for ([_]bool{ false, true }) |nulls| {
        const values = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        for (values, defs, 0..) |*v, *d, i| {
            v.* = try std.fmt.allocPrint(a, "key-{d}", .{i});
            d.* = @intFromBool(i % 13 != 0);
        }
        const elem: schema.SchemaElement = .{
            .name = "k",
            .type = .BYTE_ARRAY,
            .repetition_type = if (nulls) .OPTIONAL else .REQUIRED,
            .type_length = null,
            .num_children = 0,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        const input: ColumnInput = .{
            .values = .{ .string = .{
                .values = values,
                .def_levels = if (nulls) defs else null,
                .max_def = @intFromBool(nulls),
                .has_nulls = nulls,
            } },
            .schema_elem = &elem,
            .path_in_schema = &.{"k"},
        };
        const expected = try encodeColumn(a, input);
        var skipped = input;
        skipped.distinct = true;
        const got = try encodeColumn(a, skipped);
        try std.testing.expectEqualSlices(u8, expected.bytes, got.bytes);
    };
}
const thrift = @import("../thrift.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const hybrid_rle = @import("../parquet/encoding/hybrid_rle.zig");
const snappy = @import("../parquet/snappy.zig");
const compression = @import("../parquet/compression.zig");
const delta_binary_packed = @import("../parquet/encoding/delta_binary_packed.zig");
const bloom = @import("../parquet/bloom.zig");
const agg_coded = @import("../expr/agg_coded.zig");
const prefix16 = @import("../expr/agg.zig").prefix16;
const decimal_mod = @import("../parquet/decimal.zig");

pub const Error = error{
    NullableNotSupported,
    UnsupportedType,
    TooLarge,
    CorruptInput, // from snappy (vendored google/snappy)
    OutputTooSmall, // from snappy
    DecimalValueExceedsPrecision,
} || std.mem.Allocator.Error || compression.Error;

pub const EncodedColumn = struct {
    /// Page headers and their (compressed) payloads, ready to concatenate: the dictionary page, if any, then the
    /// data pages in row order.
    bytes: []u8,
    /// ColumnMetaData for the file footer. data_page_offset is relative
    /// to the start of `bytes`; the caller adds the column's absolute
    /// position in the output file.
    meta: schema.ColumnMetaData,
    /// The chunk's Bloom filter (header and bitset) when `ColumnInput.bloom_fpp` asked for one.
    bloom: []const u8 = &.{},
    /// One per data page, in order: what the chunk's page index (ColumnIndex and OffsetIndex) is built from.
    pages: []const PageInfo = &.{},
};

/// A data page as its chunk's page index describes it.
pub const PageInfo = struct {
    /// Where the page header starts, relative to `EncodedColumn.bytes`.
    offset: u64,
    /// Header plus compressed payload.
    size: u32,
    first_row: u64,
    rows: u64,
    null_count: i64,
    nan_count: ?i64 = null,
    /// Bounds over the page's present values, in the chunk statistics' encoding; null when it has none (all NULL, or
    /// a float page of NaN only).
    min: ?[]const u8 = null,
    max: ?[]const u8 = null,
};

/// The data page target: a page is closed once its present values take this many PLAIN bytes or it holds
/// `default_page_rows` rows, whichever comes first. These are the defaults Arrow C++ (pyarrow) writes with and
/// parquet-mr's 1 MiB page and 20,000-row page cap; they keep a page small enough for the page index to skip at a
/// useful grain and large enough that per-page overhead stays in the noise.
pub const default_page_bytes: usize = 1 << 20;
pub const default_page_rows: usize = 20_000;

pub const ColumnInput = struct {
    /// Decoded values for this column. Caller pre-applies any filter
    /// SelectionVector before calling encodeColumn.
    values: filter_eval.Batch.Column,
    /// Schema element for this column (used for type, name, options).
    schema_elem: *const schema.SchemaElement,
    /// path_in_schema for the resulting ColumnMetaData. Typically a
    /// one-element list [name] for flat schemas.
    path_in_schema: []const []const u8,
    /// Output codec for this column's pages. SNAPPY is the default
    /// (small overhead, ~250 MB/s); ZSTD is a higher-ratio option
    /// (~30% smaller output, ~400 MB/s encode at level 3).
    /// UNCOMPRESSED is supported but discouraged outside diagnostics.
    codec: schema.CompressionCodec = .SNAPPY,
    /// How zstd pages are compressed, when `codec` is ZSTD.
    zstd: compression.ZstdPlan = .{},
    /// Build a split-block Bloom filter of the chunk at this false-positive probability.
    bloom_fpp: ?f64 = null,
    /// The caller proves present values unique, so a dictionary attempt would decline without changing the bytes.
    distinct: bool = false,
    /// The data page target (`default_page_bytes`, `default_page_rows`). A repeated column is one page.
    page_bytes: usize = default_page_bytes,
    page_rows: usize = default_page_rows,
};

/// Encode one column chunk: optional dictionary page, then data pages of at most the page target, and the chunk's
/// ColumnMetaData. The caller fills in the absolute file offset on data_page_offset after concatenating into the
/// output file.
///
/// For OPTIONAL leaves (max_def == 1), each data page carries a definition-level prefix using RLE/bit-packed-hybrid
/// before its values. Two cases:
///
/// 1. Input column has no `def_levels` (REQUIRED column or filter
///    survivors that are guaranteed non-null): emit all-1s def levels
///    (~3 bytes regardless of N), and PLAIN bytes carry every value.
/// 2. Input column has `def_levels` (projected-through nullable
///    column): emit the actual def levels via hybrid_rle.encode, and
///    PLAIN bytes carry only the non-null values (parquet PLAIN
///    encoding for OPTIONAL columns excludes null slots).
pub fn encodeColumn(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    const p = try plan(arena, in);
    const pages = try arena.alloc(BuiltPage, p.pageCount());
    for (pages, 0..) |*pg, i| pg.* = try p.page(arena, i);
    return p.finish(arena, pages);
}

/// How a column chunk is written, decided once over all its rows (`plan`): its encoding, statistics, dictionary page
/// and page boundaries. Its pages (`page`) are then independent of one another: they may be built in any order, on
/// any thread, each on an allocator of its own, and `finish` lays them out in row order. The bytes are the same
/// however the pages were built.
pub const Plan = struct {
    in: ColumnInput,
    kind: Kind,
    /// Per page, the row it ends at (exclusive); the last is the chunk's row count.
    ends: []const usize,
    stats: ?schema.Statistics,
    /// The compressed dictionary page with its header, and its uncompressed length with that header.
    dict_page: []const u8 = &.{},
    dict_page_uncompressed: usize = 0,
    values_encoding: schema.Encoding,
    num_values: i64,
    def_levels: ?[]const u32,
    max_def: u32,
    optional: bool,

    const Kind = union(enum) {
        /// Values encoded in each page (`values_encoding`).
        values,
        /// Indices into the dictionary page, one per present value; `present_before[i]` of them precede page i.
        dict: struct {
            entries: filter_eval.Batch.Column,
            indices: []const u32,
            bit_width: u8,
            present_before: []const usize,
        },
        /// A DECIMAL as big-endian bytes of the schema's width (`encodeDecimalFlba`).
        flba_decimal: usize,
        /// A repeated column: the whole chunk is one page, its levels as they are.
        nested,
    };

    pub fn pageCount(self: *const Plan) usize {
        return self.ends.len;
    }

    /// A page's definition-level stream, for an OPTIONAL leaf; empty for a REQUIRED one.
    fn pageDefPrefix(self: *const Plan, arena: std.mem.Allocator, def_levels: ?[]const u32, rows: i64) ![]const u8 {
        return if (self.optional) try defLevelPrefix(arena, def_levels, self.max_def, rows) else &.{};
    }

    fn pageStart(self: *const Plan, i: usize) usize {
        return if (i == 0) 0 else self.ends[i - 1];
    }

    /// Data page `i`, on `arena`. Reads only the plan and its inputs, so pages may be built on several threads.
    pub fn page(self: *const Plan, arena: std.mem.Allocator, i: usize) Error!BuiltPage {
        const lo = self.pageStart(i);
        const hi = self.ends[i];
        const sub = sliceRows(self.in.values, lo, hi);
        const def_levels = if (self.def_levels) |d| d[lo..hi] else null;
        const rows: i64 = @intCast(hi - lo);
        var page_stats: ?schema.Statistics = null;
        const payload: []const u8 = switch (self.kind) {
            .values, .nested => blk: {
                const fixed = self.in.schema_elem.type == .FIXED_LEN_BYTE_ARRAY;
                const encoding = self.values_encoding;
                const values_bytes = try encodeValuesAs(arena, sub, def_levels, self.max_def, encoding, fixed);
                const rep_prefix: []const u8 = if (self.kind == .nested)
                    try levelPrefix(
                        arena,
                        sourceRepLevels(sub) orelse return error.NullableNotSupported,
                        bitWidthFor(sourceMaxRep(sub)),
                    )
                else
                    &.{};
                const def_prefix: []const u8 = if (self.optional or self.kind == .nested)
                    try defLevelPrefix(arena, def_levels, self.max_def, rows)
                else
                    &.{};
                if (self.kind == .values)
                    page_stats = computeStats(arena, sub, def_levels, self.max_def, self.in.schema_elem);
                break :blk try std.mem.concat(arena, u8, &.{ rep_prefix, def_prefix, values_bytes });
            },
            .dict => |d| blk: {
                const idx = d.indices[d.present_before[i]..d.present_before[i + 1]];
                const def_prefix = try self.pageDefPrefix(arena, def_levels, rows);
                const idx_rle = try hybrid_rle.encode(arena, idx, d.bit_width);
                page_stats = try entryStats(arena, d.entries, idx, self.in.schema_elem);
                if (page_stats) |*st| {
                    st.null_count = rows - @as(i64, @intCast(idx.len));
                }
                break :blk try std.mem.concat(arena, u8, &.{ def_prefix, &.{d.bit_width}, idx_rle });
            },
            .flba_decimal => |width| blk: {
                const c = sub.i128;
                const values_bytes = try flbaDecimalBytes(arena, c.values, def_levels, self.max_def, width);
                const def_prefix = try self.pageDefPrefix(arena, def_levels, rows);
                page_stats = try flbaDecimalStats(arena, c.values, def_levels, self.max_def, width);
                break :blk try std.mem.concat(arena, u8, &.{ def_prefix, values_bytes });
            },
        };
        // A page header's sizes and count are i32: a page past them (one value of 2 GiB) cannot be written.
        if (hi - lo > pageLimit() or payload.len > pageLimit()) return error.TooLarge;
        const compressed = try compression.compressPage(arena, payload, self.in.codec, self.in.zstd);
        if (compressed.len > pageLimit()) return error.TooLarge;
        const header = try pageHeaderBytes(arena, .{
            .type = .DATA_PAGE,
            .uncompressed_page_size = @intCast(payload.len),
            .compressed_page_size = @intCast(compressed.len),
            .crc = null,
            .data_page_header = .{
                .num_values = @intCast(rows),
                .encoding = self.values_encoding,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = null,
            .data_page_header_v2 = null,
        });
        const bytes = try std.mem.concat(arena, u8, &.{ header, compressed });
        const st = page_stats orelse schema.Statistics{};
        return .{
            .bytes = bytes,
            .uncompressed = header.len + payload.len,
            .info = .{
                .offset = 0,
                .size = @intCast(bytes.len),
                .first_row = lo,
                .rows = hi - lo,
                .null_count = st.null_count orelse countNulls(def_levels, self.max_def, hi - lo),
                .nan_count = st.nan_count,
                .min = st.min_value,
                .max = st.max_value,
            },
        };
    }

    /// The chunk: the dictionary page, then `pages` (one per `page(i)`, in order), and its metadata, all on `arena`.
    pub fn finish(self: *const Plan, arena: std.mem.Allocator, pages: []const BuiltPage) Error!EncodedColumn {
        std.debug.assert(pages.len == self.ends.len);
        var total: usize = self.dict_page.len;
        var uncompressed: usize = self.dict_page_uncompressed;
        for (pages) |pg| {
            total += pg.bytes.len;
            uncompressed += pg.uncompressed;
        }
        const bytes = try arena.alloc(u8, total);
        @memcpy(bytes[0..self.dict_page.len], self.dict_page);
        const infos = try arena.alloc(PageInfo, pages.len);
        var pos: usize = self.dict_page.len;
        for (pages, infos) |pg, *info| {
            @memcpy(bytes[pos..][0..pg.bytes.len], pg.bytes);
            info.* = pg.info;
            info.offset = pos;
            if (info.min) |v| info.min = try arena.dupe(u8, v);
            if (info.max) |v| info.max = try arena.dupe(u8, v);
            pos += pg.bytes.len;
        }

        var encodings: schema.EncodingList = .empty;
        var path_list: schema.StringList = .empty;
        try path_list.appendSlice(arena, self.in.path_in_schema);
        const has_dict = self.dict_page.len > 0;
        if (has_dict) {
            try encodings.appendSlice(arena, &.{ .RLE, .PLAIN, .PLAIN_DICTIONARY });
        } else {
            try encodings.appendSlice(arena, &.{ .RLE, self.values_encoding });
        }
        var enc_stats: std.ArrayListUnmanaged(schema.PageEncodingStats) = .empty;
        // Every value is in the dictionary when there is one: readers may prune row groups on it.
        if (has_dict) try enc_stats.append(arena, .{
            .page_type = @backingInt(schema.PageType.DICTIONARY_PAGE),
            .encoding = @backingInt(schema.Encoding.PLAIN),
            .count = 1,
        });
        try enc_stats.append(arena, .{
            .page_type = @backingInt(schema.PageType.DATA_PAGE),
            .encoding = @backingInt(self.values_encoding),
            .count = @intCast(pages.len),
        });
        var out: EncodedColumn = .{
            .bytes = bytes,
            .meta = .{
                .type = self.in.schema_elem.type.?,
                .encodings = encodings,
                .path_in_schema = path_list,
                .codec = self.in.codec,
                .num_values = self.num_values,
                .total_uncompressed_size = @intCast(uncompressed),
                .total_compressed_size = @intCast(bytes.len),
                .data_page_offset = @intCast(self.dict_page.len),
                .dictionary_page_offset = if (has_dict) 0 else null,
                .index_page_offset = null,
                .statistics = try ownStats(arena, self.stats),
                .encoding_stats = enc_stats,
            },
            .pages = infos,
        };
        if (self.in.bloom_fpp) |fpp| out.bloom = try bloomFilter(arena, self.in.values, out.meta.type, fpp) orelse &.{};
        return out;
    }
};

/// The split-block Bloom filter (header and bitset) of a column's present values at false-positive probability
/// `fpp`, hashed as the PLAIN bytes `physical` stores them as. Sized as if every present value were distinct, then
/// folded down to what the values actually set: the size their distinct count calls for. Null for a lane that is not
/// `physical`'s stored form, and for BOOLEAN, which no reader filters.
pub fn bloomFilter(
    arena: std.mem.Allocator,
    col: filter_eval.Batch.Column,
    physical: schema.Type,
    fpp: f64,
) Error!?[]u8 {
    const stored: schema.Type = switch (col) {
        .i32 => .INT32,
        .i64 => .INT64,
        .f32 => .FLOAT,
        .f64 => .DOUBLE,
        .string => if (physical == .FIXED_LEN_BYTE_ARRAY) .FIXED_LEN_BYTE_ARRAY else .BYTE_ARRAY,
        .boolean, .i128 => return null,
    };
    if (stored != physical) return null;
    return switch (col) {
        .boolean, .i128 => unreachable,
        // A coded column may hold no values: its dictionary's entries, a superset of those present, fill the filter.
        inline else => |c| if (c.codes != null)
            try bloomOf(@TypeOf(c.dict[0]), arena, c.dict, null, 0, fpp)
        else
            try bloomOf(@TypeOf(c.values[0]), arena, c.values, c.def_levels, c.max_def, fpp),
    };
}

fn bloomOf(
    comptime T: type,
    arena: std.mem.Allocator,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    fpp: f64,
) Error![]u8 {
    var b = try bloom.Builder.init(arena, countPresent(values.len, def_levels, max_def), fpp);
    defer b.deinit(arena);
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        b.insert(switch (@typeInfo(T)) {
            .int => bloom.hashBytes(&std.mem.toBytes(std.mem.nativeToLittle(T, v))),
            .float => blk: {
                const Bits = @Int(.unsigned, @bitSizeOf(T));
                break :blk bloom.hashBytes(&std.mem.toBytes(std.mem.nativeToLittle(Bits, @bitCast(v))));
            },
            else => bloom.hashBytes(v),
        });
    }
    b.foldToFpp(fpp);
    return b.serialize(arena);
}

/// The most a page header's i32 sizes and counts hold. Tests lower it to reach the limits.
fn pageLimit() usize {
    return if (@import("builtin").is_test) test_page_limit else std.math.maxInt(i32);
}

var test_page_limit: usize = std.math.maxInt(i32);

/// `stats` with its bounds copied onto `arena`: a finished chunk holds nothing of the memory its plan and pages were
/// built on, which may be reused as soon as it is finished.
fn ownStats(arena: std.mem.Allocator, stats: ?schema.Statistics) Error!?schema.Statistics {
    var st = stats orelse return null;
    inline for (.{ "max", "min", "max_value", "min_value" }) |f| {
        if (@field(st, f)) |v| @field(st, f) = try arena.dupe(u8, v);
    }
    return st;
}

/// A data page built by `Plan.page`: header and compressed payload, its uncompressed length with the header, and its
/// page index entry (`offset` set by `Plan.finish`).
pub const BuiltPage = struct {
    bytes: []const u8,
    uncompressed: usize,
    info: PageInfo,
};

/// Decide how `in` is written (see `Plan`). A dictionary chunk has its dictionary page built here.
pub fn plan(arena: std.mem.Allocator, in: ColumnInput) Error!Plan {
    if (in.values == .i128) return planDecimal(arena, in);
    if (in.values.codes() != null) return planCoded(arena, in);
    return planValues(arena, in);
}

/// Rows `lo..hi` of a flat column: values, levels and codes cut alike. A repeated column is passed whole (it is one
/// page).
fn sliceRows(c: filter_eval.Batch.Column, lo: usize, hi: usize) filter_eval.Batch.Column {
    switch (c) {
        inline else => |x, tag| {
            if (x.max_rep > 0) return c;
            var out = x;
            if (x.values.len > 0) out.values = x.values[lo..hi];
            if (x.def_levels) |d| out.def_levels = d[lo..hi];
            if (x.codes) |k| out.codes = k[lo..hi];
            return @unionInit(filter_eval.Batch.Column, @tagName(tag), out);
        },
    }
}

/// Rows of `rows` whose level is `max_def` (all of them without levels).
pub fn countPresentRows(rows: usize, def_levels: ?[]const u32, max_def: u32) usize {
    return countPresent(rows, def_levels, max_def);
}

fn countNulls(def_levels: ?[]const u32, max_def: u32, rows: usize) i64 {
    return @intCast(rows - countPresent(rows, def_levels, max_def));
}

/// Where a flat chunk's data pages end (exclusive rows): a page is closed once its present values take `page_bytes` of
/// PLAIN bytes (a byte array 4 + its length, a fixed-width value its width, a BOOLEAN one bit) or it holds `page_rows`
/// rows. From the values alone, never the encoding chosen, so a chunk's pages are the same however it is encoded.
fn pageEnds(
    arena: std.mem.Allocator,
    c: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
    fixed_width: ?usize,
    page_bytes: usize,
    page_rows_in: usize,
) Error![]const usize {
    const n = switch (c) {
        inline else => |x| x.rowCount(),
    };
    const page_rows = @max(1, page_rows_in);
    const budget_bits: u64 = @as(u64, @max(1, page_bytes)) * 8;
    var ends: std.ArrayListUnmanaged(usize) = .empty;
    if (n == 0) {
        try ends.append(arena, 0);
        return ends.items;
    }
    // Bits per present value for fixed-width lanes; null for byte arrays, whose length counts.
    const width_bits: ?u64 = switch (c) {
        .boolean => 1,
        .i32, .f32 => 32,
        .i64, .f64 => 64,
        .i128 => @as(u64, fixed_width orelse 16) * 8,
        .string => if (fixed_width) |w| @as(u64, w) * 8 else null,
    };
    if (width_bits) |w| if (def_levels == null) {
        const per_page: usize = @intCast(@max(1, @min(@as(u64, page_rows), budget_bits / w)));
        var e: usize = per_page;
        while (e < n) : (e += per_page) try ends.append(arena, e);
        try ends.append(arena, n);
        return ends.items;
    };
    var bits: u64 = 0;
    var start: usize = 0;
    for (0..n) |r| {
        if (isPresent(def_levels, max_def, r)) bits += width_bits orelse blk: {
            const v = switch (c) {
                .string => |x| if (x.codes) |k| x.dict[k[r]] else x.values[r],
                else => unreachable,
            };
            break :blk (4 + @as(u64, v.len)) * 8;
        };
        if (r + 1 - start >= page_rows or bits >= budget_bits) {
            try ends.append(arena, r + 1);
            start = r + 1;
            bits = 0;
        }
    }
    if (start < n or ends.items.len == 0) try ends.append(arena, n);
    return ends.items;
}

/// The plan of a column written as values (no dictionary): DELTA_BINARY_PACKED for INT32/INT64, a delta string layout
/// for byte arrays (`stringsEncoding`), PLAIN otherwise; each page encodes its own rows.
fn planValues(arena: std.mem.Allocator, in: ColumnInput) Error!Plan {
    const elem = in.schema_elem;
    const phys = elem.type orelse return error.UnsupportedType;
    const num_values: i64 = @intCast(valueCount(in.values));
    const src_def_levels = sourceDefLevels(in.values);
    const src_max_def = sourceMaxDef(in.values);
    const src_max_rep = sourceMaxRep(in.values);
    const fixed = phys == .FIXED_LEN_BYTE_ARRAY;
    // FLBA's PLAIN form is the bare bytes, so a value of another width would shift every value after it.
    if (fixed and in.values == .string) {
        const width: usize = @intCast(elem.type_length orelse return error.UnsupportedType);
        const c = in.values.string;
        for (c.values, 0..) |v, i| {
            if (isPresent(c.def_levels, c.max_def, i) and v.len != width) return error.UnsupportedType;
        }
    }

    // Try dictionary encoding for BYTE_ARRAY first. Within the limits of
    // `tryEncodeDictBytes` (a quarter of the values distinct, 1 MiB of
    // dictionary) AND with at least 64 present values, we emit a
    // dictionary page, then data pages of RLE_DICTIONARY-encoded
    // indices. Otherwise we fall through to the paths below.
    if (in.values == .string and src_max_rep == 0 and !in.distinct) {
        if (try tryEncodeDictBytes(arena, in, num_values)) |p| return p;
    }
    // Dict-encode low-cardinality INT32/INT64 columns. The big win is
    // hourly-rounded timestamp columns (24 unique values per daily
    // shard) where dict crushes DELTA: 1.3 bytes/row vs 4.4.
    // Cardinality cap: num_present / 5.
    //
    // FLOAT/DOUBLE columns get the same treatment for the same reason:
    // rounded cost / percentage data is heavily repeated (1M cents in
    // [0.01, 100.00] have ≤10K unique values). An empirical bench
    // showed BYTE_STREAM_SPLIT is a loss on this shape; dict captures
    // the redundancy properly.
    if (src_max_rep == 0 and !in.distinct) {
        switch (in.values) {
            .i32 => if (try tryEncodeDictInt(i32, arena, in, num_values)) |p| return p,
            .i64 => if (try tryEncodeDictInt(i64, arena, in, num_values)) |p| return p,
            .f32 => if (try tryEncodeDictFloat(f32, arena, in, num_values)) |p| return p,
            .f64 => if (try tryEncodeDictFloat(f64, arena, in, num_values)) |p| return p,
            else => {},
        }
    }

    // i32/i64 take DELTA_BINARY_PACKED: it dominates PLAIN by 4-8× on sorted/timestamp columns and is roughly
    // equivalent on random data (per-block overhead is small and bit-packing adapts to actual delta range). Strings
    // the dictionary declined take DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY (`stringsEncoding`), decided over the
    // whole chunk so every page has the same one. Everything else is PLAIN.
    const values_encoding: schema.Encoding = if (src_max_rep == 0 and (in.values == .i32 or in.values == .i64))
        .DELTA_BINARY_PACKED
    else if (src_max_rep == 0 and in.values == .string)
        stringsEncoding(in.values.string.values, src_def_levels, src_max_def, fixed) orelse .PLAIN
    else
        .PLAIN;

    const nested = src_max_rep > 0;
    const ends: []const usize = if (nested) blk: {
        const one = try arena.alloc(usize, 1);
        one[0] = @intCast(num_values);
        break :blk one;
    } else blk: {
        const width: ?usize = if (fixed) @intCast(elem.type_length.?) else null;
        break :blk try pageEnds(arena, in.values, src_def_levels, src_max_def, width, in.page_bytes, in.page_rows);
    };

    // Statistics over the whole chunk. Pass `arena` so byte-array min/max strings can be dupe'd onto a stable arena
    // (the source values may live on a row-group-scoped arena that's freed before the file footer is written;
    // borrowing the slice into the footer would dangle).
    return .{
        .in = in,
        .kind = if (nested) .nested else .values,
        .ends = ends,
        .stats = computeStats(arena, in.values, src_def_levels, src_max_def, in.schema_elem),
        .values_encoding = values_encoding,
        .num_values = num_values,
        .def_levels = if (nested) null else src_def_levels,
        .max_def = src_max_def,
        .optional = elem.repetition_type == .OPTIONAL,
    };
}

/// `c`'s present values in `encoding` (as `planValues` chose it).
fn encodeValuesAs(
    arena: std.mem.Allocator,
    c: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
    encoding: schema.Encoding,
    fixed: bool,
) Error![]const u8 {
    return switch (encoding) {
        .DELTA_BINARY_PACKED => (try tryEncodeValuesDelta(arena, c, def_levels, max_def)).?,
        .DELTA_BYTE_ARRAY, .DELTA_LENGTH_BYTE_ARRAY => try encodeStringsAs(
            arena,
            c.string.values,
            def_levels,
            max_def,
            encoding,
        ),
        else => try encodeValuesPlain(arena, c, def_levels, max_def, fixed),
    };
}

/// Statistics of the values `indices` select from dictionary `entries` (present values only), as `computeStats`
/// gives them for those values in a column.
fn entryStats(
    arena: std.mem.Allocator,
    entries: filter_eval.Batch.Column,
    indices: []const u32,
    elem: *const schema.SchemaElement,
) Error!?schema.Statistics {
    if (indices.len == 0) return .{ .null_count = 0 };
    switch (entries) {
        inline else => |e, tag| {
            const T = @typeInfo(@TypeOf(e.values)).pointer.child;
            const vals = try arena.alloc(T, indices.len);
            for (vals, indices) |*v, k| v.* = e.values[k];
            const col = @unionInit(filter_eval.Batch.Column, @tagName(tag), .{ .values = vals });
            return computeStats(arena, col, null, 0, elem);
        },
    }
}

/// An exact DECIMAL column (unscaled i128) at the physical type its schema element declares: INT32/INT64 values are
/// narrowed (an out-of-range value is an error, never a wrap) and take the integer encoders; FLBA writes each present
/// value as `type_length` big-endian two's-complement bytes (parquet's decimal-FLBA wire format), PLAIN, matching what
/// `decimal.decodeColumnAsI128` reads back.
fn planDecimal(arena: std.mem.Allocator, in: ColumnInput) Error!Plan {
    const c = in.values.i128;
    switch (in.schema_elem.type orelse return error.UnsupportedType) {
        .FIXED_LEN_BYTE_ARRAY => return planDecimalFlba(arena, in),
        inline .INT32, .INT64 => |phys| {
            const T = if (phys == .INT32) i32 else i64;
            const vals = try arena.alloc(T, c.values.len);
            for (c.values, vals) |v, *o| o.* = std.math.cast(T, v) orelse return error.UnsupportedType;
            var narrowed = in;
            const col: filter_eval.ColumnT(T) = .{
                .values = vals,
                .def_levels = c.def_levels,
                .max_def = c.max_def,
                .has_nulls = c.has_nulls,
            };
            narrowed.values = if (T == i32) .{ .i32 = col } else .{ .i64 = col };
            return plan(arena, narrowed);
        },
        else => return error.UnsupportedType,
    }
}

/// Encode an FLBA-backed DECIMAL column from its unscaled values (`planDecimal`).
pub fn encodeDecimalFlba(
    arena: std.mem.Allocator,
    col: filter_eval.ColumnT(i128),
    schema_elem: *const schema.SchemaElement,
    path_in_schema: []const []const u8,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) Error!EncodedColumn {
    if (schema_elem.type != .FIXED_LEN_BYTE_ARRAY) return error.UnsupportedType;
    return encodeColumn(arena, .{
        .values = .{ .i128 = col },
        .schema_elem = schema_elem,
        .path_in_schema = path_in_schema,
        .codec = codec,
        .zstd = zstd,
    });
}

fn planDecimalFlba(arena: std.mem.Allocator, in: ColumnInput) Error!Plan {
    const schema_elem = in.schema_elem;
    const col = in.values.i128;
    const bw: usize = if (schema_elem.type_length) |tl| @intCast(tl) else return error.UnsupportedType;
    if (bw == 0 or bw > 16) return error.UnsupportedType;
    const def_levels = col.def_levels;
    const max_def = col.max_def;

    // Refuse when a value overflows declared precision to avoid emitting wrapped negative values or corrupt stats.
    const dec_kind = decimal_mod.kindFromSchema(schema_elem);
    const precision: ?i32 = if (dec_kind) |k| k.precision else schema_elem.precision;
    if (precision) |p| {
        if (p >= 1 and p <= 38) {
            const bound = decimal_mod.pow10(p);
            for (col.values, 0..) |v, i| {
                if (!isPresent(def_levels, max_def, i)) continue;
                if (v >= bound or v <= -bound) return error.DecimalValueExceedsPrecision;
            }
        }
    }
    return .{
        .in = in,
        .kind = .{ .flba_decimal = bw },
        .ends = try pageEnds(arena, in.values, def_levels, max_def, bw, in.page_bytes, in.page_rows),
        .stats = try flbaDecimalStats(arena, col.values, def_levels, max_def, bw),
        .values_encoding = .PLAIN,
        .num_values = @intCast(col.values.len),
        .def_levels = def_levels,
        .max_def = max_def,
        .optional = schema_elem.repetition_type == .OPTIONAL,
    };
}

/// PLAIN values of a decimal FLBA: `width` big-endian bytes per present value.
fn flbaDecimalBytes(
    arena: std.mem.Allocator,
    values: []const i128,
    def_levels: ?[]const u32,
    max_def: u32,
    width: usize,
) Error![]u8 {
    const out = try arena.alloc(u8, countPresent(values.len, def_levels, max_def) * width);
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        var be: [16]u8 = undefined;
        std.mem.writeInt(i128, &be, v, .big);
        @memcpy(out[pos..][0..width], be[16 - width ..]);
        pos += width;
    }
    return out;
}

/// Min/max over present values as big-endian `width` bytes, and the null count.
fn flbaDecimalStats(
    arena: std.mem.Allocator,
    values: []const i128,
    def_levels: ?[]const u32,
    max_def: u32,
    width: usize,
) Error!schema.Statistics {
    var stats = schema.Statistics{};
    var have = false;
    var lo: i128 = 0;
    var hi: i128 = 0;
    var nulls: i64 = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) {
            nulls += 1;
            continue;
        }
        if (!have) {
            lo = v;
            hi = v;
            have = true;
        } else {
            if (v < lo) lo = v;
            if (v > hi) hi = v;
        }
    }
    if (have) {
        const lob = try arena.alloc(u8, width);
        const hib = try arena.alloc(u8, width);
        var be: [16]u8 = undefined;
        std.mem.writeInt(i128, &be, lo, .big);
        @memcpy(lob, be[16 - width ..]);
        std.mem.writeInt(i128, &be, hi, .big);
        @memcpy(hib, be[16 - width ..]);
        stats.min_value = lob;
        stats.max_value = hib;
        stats.is_min_value_exact = true;
        stats.is_max_value_exact = true;
    }
    stats.null_count = nulls;
    return stats;
}

/// A column carried as codes into its source dictionary (`ColumnT.codes`). The dictionary is rebuilt from the codes
/// alone: each code met among present rows becomes the next entry, in first-occurrence order and under the same
/// limits as the hashing encoders (`stringDictMaxEntries` and `string_dict_max_bytes` for strings), so with a source
/// dictionary of distinct entries the chunk is the one
/// hashing the values would write, without hashing any. Past the cap (or with too few values) the values are
/// gathered and encoded as any other column.
fn planCoded(arena: std.mem.Allocator, in: ColumnInput) Error!Plan {
    switch (in.values) {
        inline else => |c, tag| {
            const T = @TypeOf(c.dict[0]);
            const codes = c.codes.?;
            const n = codes.len;
            const num_present = countPresent(n, c.def_levels, c.max_def);
            if (T != bool and c.max_rep == 0 and num_present >= 64) dict: {
                const is_string = T == []const u8;
                const len_prefix: usize = if (in.schema_elem.type == .FIXED_LEN_BYTE_ARRAY) 0 else 4;
                const max_cardinality = if (is_string) stringDictMaxEntries(num_present) else num_present / 5;
                var dict_bytes: usize = 0;
                const none = std.math.maxInt(u32);
                const remap = try arena.alloc(u32, c.dict.len);
                @memset(remap, none);
                const entries = try arena.alloc(T, @min(max_cardinality, c.dict.len));
                var n_entries: usize = 0;
                const indices = try arena.alloc(u32, num_present);
                var pos: usize = 0;
                for (codes, 0..) |k, i| {
                    if (!isPresent(c.def_levels, c.max_def, i)) continue;
                    if (k >= c.dict.len) return error.CorruptInput;
                    if (remap[k] == none) {
                        if (n_entries >= entries.len) break :dict;
                        if (is_string) {
                            dict_bytes += len_prefix + c.dict[k].len;
                            if (dict_bytes > string_dict_max_bytes) break :dict;
                        }
                        remap[k] = @intCast(n_entries);
                        entries[n_entries] = c.dict[k];
                        n_entries += 1;
                    }
                    indices[pos] = remap[k];
                    pos += 1;
                }
                const used = entries[0..n_entries];
                // Equal entries under two codes would make two dictionary entries where hashing the values makes one:
                // the bytes would then depend on whether the rows came coded.
                if (!try distinctEntries(T, arena, used)) break :dict;
                if (T == []const u8 and len_prefix == 0) {
                    const width: usize = @intCast(in.schema_elem.type_length orelse return error.UnsupportedType);
                    for (used) |v| if (v.len != width) return error.UnsupportedType;
                }
                const dict_plain = if (T == []const u8)
                    try encodePlainBytes(arena, used, null, 0, len_prefix == 0)
                else blk: {
                    const Bits = @Int(.unsigned, @bitSizeOf(T));
                    const out = try arena.alloc(u8, used.len * @sizeOf(T));
                    for (used, 0..) |v, i| {
                        std.mem.writeInt(Bits, out[i * @sizeOf(T) ..][0..@sizeOf(T)], @bitCast(v), .little);
                    }
                    break :blk out;
                };
                const dict_col = @unionInit(filter_eval.Batch.Column, @tagName(tag), .{ .values = used });
                if (try planDictionary(arena, in, @intCast(n), dict_plain, dict_col, indices)) |p| return p;
            }
            var vals = c;
            const gathered = try agg_coded.materialize(arena, in.values);
            vals.values = @field(gathered, @tagName(tag)).values;
            vals.codes = null;
            vals.dict = &.{};
            var plain = in;
            plain.values = @unionInit(filter_eval.Batch.Column, @tagName(tag), vals);
            return plan(arena, plain);
        },
    }
}

/// Whether `entries` holds no value twice (by bits, as the hashing encoders key them).
fn distinctEntries(comptime T: type, arena: std.mem.Allocator, entries: []const T) Error!bool {
    if (T == []const u8) {
        var seen = std.StringHashMap(void).init(arena);
        defer seen.deinit();
        try seen.ensureTotalCapacity(@intCast(entries.len));
        for (entries) |e| if (seen.getOrPutAssumeCapacity(e).found_existing) return false;
    } else {
        const K = @Int(.unsigned, @bitSizeOf(T));
        var seen: std.AutoHashMapUnmanaged(K, void) = .empty;
        defer seen.deinit(arena);
        try seen.ensureTotalCapacity(arena, @intCast(entries.len));
        for (entries) |e| if (seen.getOrPutAssumeCapacity(@bitCast(e)).found_existing) return false;
    }
    return true;
}

/// A string column is dictionary-encoded when its values repeat four times on average or more (at most a quarter of
/// them distinct) and the dictionary's PLAIN bytes stay within 1 MiB, the dictionary page limit Arrow writers default
/// to. Both limits only grow as values are interned, so either one passing ends the attempt with the same answer the
/// whole column would give. A dictionary beats every other string encoding where it applies: smaller, the fastest to
/// read in every reader we measured, and it lets readers prune row groups by `=` and `IN`.
const string_dict_max_bytes = 1 << 20;

fn stringDictMaxEntries(num_present: usize) usize {
    return num_present / 4;
}

/// Try to dictionary-encode a BYTE_ARRAY column (page layout in
/// `finishDictionaryColumn`) within the limits above. Returns null when
/// the column passes them (the caller then writes DELTA_BYTE_ARRAY or
/// PLAIN), or when there are too few values to bother.
fn tryEncodeDictBytes(arena: std.mem.Allocator, in: ColumnInput, num_values: i64) Error!?Plan {
    const string_col = in.values.string;
    const values = string_col.values;
    const def_levels = string_col.def_levels;
    const max_def = string_col.max_def;
    const fixed = in.schema_elem.type == .FIXED_LEN_BYTE_ARRAY;
    const len_prefix: usize = if (fixed) 0 else 4;

    const num_present = countPresent(values.len, def_levels, max_def);
    if (num_present < 64) return null;

    var lookup = std.StringHashMap(u32).init(arena);
    defer lookup.deinit();
    var dict_values: std.ArrayList([]const u8) = .empty;
    var indices = try arena.alloc(u32, num_present);
    const max_entries = stringDictMaxEntries(num_present);
    // Sized for the most entries the cap admits, so the table never rehashes on the way.
    try lookup.ensureTotalCapacity(@intCast(@min(max_entries + 1, num_present)));

    var dict_bytes: usize = 0;
    var idx_pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const gop = try lookup.getOrPut(v);
        if (!gop.found_existing) {
            dict_bytes += len_prefix + v.len;
            if (dict_values.items.len >= max_entries or dict_bytes > string_dict_max_bytes) return null;
            const new_idx: u32 = @intCast(dict_values.items.len);
            try dict_values.append(arena, v);
            gop.value_ptr.* = new_idx;
        }
        indices[idx_pos] = gop.value_ptr.*;
        idx_pos += 1;
    }

    if (dict_values.items.len == 0) return null;

    const dict_plain = try encodePlainBytes(arena, dict_values.items, null, 0, fixed);
    const dict_col: filter_eval.Batch.Column = .{ .string = .{ .values = dict_values.items } };
    return try planDictionary(arena, in, num_values, dict_plain, dict_col, indices);
}

/// An open-addressing set of fixed-width keys for the dictionary attempts: at most `limit` entries (the cardinality
/// cap), so a power-of-two table at least twice that never fills and never grows. A slot holds its entry's index + 1
/// (0 is empty); a multiply-shift hash replaces the general map's per-key hash call. The hash is fixed, so keys can be
/// chosen to collide: a lookup gives up after `max_probe` slots, and the caller then interns with `MapInterner`.
fn Interner(comptime K: type) type {
    return struct {
        keys: []K,
        slots: []u32,
        shift: u6,

        const Self = @This();

        fn init(arena: std.mem.Allocator, limit: usize) Error!Self {
            const bits: u6 = @intCast(std.math.log2_int_ceil(usize, @max(16, 2 * (limit + 1))));
            const n = @as(usize, 1) << bits;
            const slots = try arena.alloc(u32, n);
            @memset(slots, 0);
            return .{ .keys = try arena.alloc(K, n), .slots = slots, .shift = @intCast(64 - @as(u7, bits)) };
        }

        /// Slots a lookup compares before giving up; at half load a well-spread key set needs one or two.
        const max_probe = 16;

        /// The slot for `key`: its entry's index + 1, or 0 for the caller to fill (the key is then recorded); null
        /// when `max_probe` slots hold other keys.
        inline fn find(self: *Self, key: K) ?*u32 {
            const mask = self.slots.len - 1;
            var h: usize = @intCast((@as(u64, key) *% 0x9E3779B97F4A7C15) >> self.shift);
            for (0..max_probe) |_| {
                if (self.slots[h] == 0) {
                    self.keys[h] = key;
                    return &self.slots[h];
                }
                if (self.keys[h] == key) return &self.slots[h];
                h = (h + 1) & mask;
            }
            return null;
        }
    };
}

/// The general map, with the same interface as `Interner`, for keys that collide in its hash.
fn MapInterner(comptime K: type) type {
    return struct {
        map: std.AutoHashMapUnmanaged(K, u32) = .empty,

        const Self = @This();

        fn init(arena: std.mem.Allocator, limit: usize) Error!Self {
            var self: Self = .{};
            // One key past the cap is recorded before the caller sees it is one too many.
            try self.map.ensureTotalCapacity(arena, @intCast(limit + 1));
            return self;
        }

        inline fn find(self: *Self, key: K) ?*u32 {
            const gop = self.map.getOrPutAssumeCapacity(key);
            if (!gop.found_existing) gop.value_ptr.* = 0;
            return gop.value_ptr;
        }
    };
}

/// Interns the present entries of `values`, keyed by their bits, into `dict` (first occurrence first, at most `limit`)
/// and `indices` (one per present value). True when done, false when the column has more than `limit` distinct
/// values, null when `Lookup` gave up on a key.
fn internValues(
    comptime T: type,
    comptime Lookup: type,
    arena: std.mem.Allocator,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    limit: usize,
    dict: *std.ArrayList(@Int(.unsigned, @bitSizeOf(T))),
    indices: []u32,
) Error!?bool {
    var lookup = try Lookup.init(arena, limit);
    dict.clearRetainingCapacity();
    var idx_pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const key: @Int(.unsigned, @bitSizeOf(T)) = @bitCast(v);
        const slot = lookup.find(key) orelse return null;
        if (slot.* == 0) {
            if (dict.items.len >= limit) return false;
            dict.appendAssumeCapacity(key);
            slot.* = @intCast(dict.items.len);
        }
        indices[idx_pos] = slot.* - 1;
        idx_pos += 1;
    }
    return true;
}

/// `internValues`, with `Interner` and, should its fixed hash be defeated, the general map.
fn internColumn(
    comptime T: type,
    arena: std.mem.Allocator,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    limit: usize,
    dict: *std.ArrayList(@Int(.unsigned, @bitSizeOf(T))),
    indices: []u32,
) Error!bool {
    const K = @Int(.unsigned, @bitSizeOf(T));
    return (try internValues(T, Interner(K), arena, values, def_levels, max_def, limit, dict, indices)) orelse
        (try internValues(T, MapInterner(K), arena, values, def_levels, max_def, limit, dict, indices)).?;
}

/// Generic INT32/INT64 dictionary encoder. Same two-page layout as
/// tryEncodeDictBytes: PLAIN-encoded dictionary page, then a data
/// page of RLE/bit-packed indices. Returns null when the dictionary
/// would exceed `num_present / 5` or when there are fewer than 64
/// present values.
fn tryEncodeDictInt(
    comptime T: type,
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
) Error!?Plan {
    if (T != i32 and T != i64) @compileError("tryEncodeDictInt expects i32/i64");
    const col = switch (T) {
        i32 => in.values.i32,
        i64 => in.values.i64,
        else => unreachable,
    };
    const values = col.values;
    const def_levels = col.def_levels;
    const max_def = col.max_def;

    const num_present = countPresent(values.len, def_levels, max_def);
    if (num_present < 64) return null;

    const max_cardinality = num_present / 5;
    var dict_keys: std.ArrayList(@Int(.unsigned, @bitSizeOf(T))) = .empty;
    try dict_keys.ensureTotalCapacity(arena, max_cardinality);
    const indices = try arena.alloc(u32, num_present);
    if (!try internColumn(T, arena, values, def_levels, max_def, max_cardinality, &dict_keys, indices)) return null;
    if (dict_keys.items.len == 0) return null;
    const dict_values: []const T = @ptrCast(dict_keys.items);

    const value_size = @sizeOf(T);
    const dict_plain = try arena.alloc(u8, dict_values.len * value_size);
    for (dict_values, 0..) |v, i| {
        std.mem.writeInt(T, dict_plain[i * value_size ..][0..value_size], v, .little);
    }
    const dict_col: filter_eval.Batch.Column = @unionInit(
        filter_eval.Batch.Column,
        if (T == i32) "i32" else "i64",
        .{ .values = dict_values },
    );
    return try planDictionary(arena, in, num_values, dict_plain, dict_col, indices);
}

/// FLOAT/DOUBLE dictionary encoder. Mirrors `tryEncodeDictInt` —
/// PLAIN-encoded dict page + RLE-dict data page — but uses the bit-
/// pattern of the float (`@bitCast` to u32/u64) as the hash key so
/// the encoder is bit-stable. Two NaNs with different payloads stay
/// distinct in the dictionary (matches the SQL "distinct value"
/// model where NaN != NaN but each specific NaN bit pattern is its
/// own entry).
///
/// Cardinality cap = num_present / 5 (as the INT path). Returns null
/// when the dictionary would exceed the cap or when there are fewer
/// than 64 present values.
fn tryEncodeDictFloat(
    comptime T: type,
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
) Error!?Plan {
    if (T != f32 and T != f64) @compileError("tryEncodeDictFloat expects f32/f64");
    const KeyT = if (T == f32) u32 else u64;

    const col = switch (T) {
        f32 => in.values.f32,
        f64 => in.values.f64,
        else => unreachable,
    };
    const values = col.values;
    const def_levels = col.def_levels;
    const max_def = col.max_def;

    const num_present = countPresent(values.len, def_levels, max_def);
    if (num_present < 64) return null;

    const max_cardinality = num_present / 5;
    var dict_values: std.ArrayList(KeyT) = .empty;
    try dict_values.ensureTotalCapacity(arena, max_cardinality);
    const indices = try arena.alloc(u32, num_present);
    if (!try internColumn(T, arena, values, def_levels, max_def, max_cardinality, &dict_values, indices)) return null;
    if (dict_values.items.len == 0) return null;

    // The keys are the floats' bit patterns, so writing them little-endian is the PLAIN encoding.
    const value_size = @sizeOf(T);
    const dict_plain = try arena.alloc(u8, dict_values.items.len * value_size);
    for (dict_values.items, 0..) |k, i| {
        std.mem.writeInt(KeyT, dict_plain[i * value_size ..][0..value_size], k, .little);
    }
    const dict_floats: []const T = @ptrCast(dict_values.items);
    const dict_col: filter_eval.Batch.Column = @unionInit(
        filter_eval.Batch.Column,
        if (T == f32) "f32" else "f64",
        .{ .values = dict_floats },
    );
    return try planDictionary(arena, in, num_values, dict_plain, dict_col, indices);
}

/// The plan every dictionary encoder ends in once it has interned its values (the encoders differ only in how they
/// key and PLAIN-encode entries): the compressed `dict_plain` dictionary page holding `dict`'s entries, built here,
/// then data pages of `indices`, one per present value. Null when the dictionary page would not fit a page header's
/// i32 sizes: the caller writes the values instead. `dictionary_page_offset` is 0 and `data_page_offset` the
/// dictionary page's length, both relative to the chunk's bytes; callers add the chunk's absolute start to both.
fn planDictionary(
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
    dict_plain: []const u8,
    /// The entries in first-occurrence order, every one present at least once.
    dict: filter_eval.Batch.Column,
    indices: []const u32,
) Error!?Plan {
    const def_levels = sourceDefLevels(in.values);
    const max_def = sourceMaxDef(in.values);
    const dict_count = valueCount(dict);
    if (dict_count > pageLimit() or dict_plain.len > pageLimit()) return null;
    // Statistics over the distinct values, not every row: the dictionary holds exactly the present values, first
    // occurrence first, so the scan meets them in the order a row scan first would and keeps the same bounds.
    var stats = computeStats(arena, dict, null, 0, in.schema_elem);
    if (stats) |*st| {
        st.null_count = num_values - @as(i64, @intCast(indices.len));
        // Over the entries it counted NaN entries; the chunk's count is of rows.
        if (st.nan_count) |entries| st.nan_count = if (entries == 0) 0 else switch (dict) {
            inline .f32, .f64 => |d| nanRows(d.values, indices),
            else => unreachable,
        };
    }
    const dict_compressed = try compression.compressPage(arena, dict_plain, in.codec, in.zstd);
    if (dict_compressed.len > pageLimit()) return null;
    const dict_header = try pageHeaderBytes(arena, .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = @intCast(dict_plain.len),
        .compressed_page_size = @intCast(dict_compressed.len),
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{
            .num_values = @intCast(dict_count),
            .encoding = .PLAIN,
            .is_sorted = null,
        },
        .data_page_header_v2 = null,
    });
    const fixed_width: ?usize = if (in.schema_elem.type == .FIXED_LEN_BYTE_ARRAY)
        @intCast(in.schema_elem.type_length.?)
    else
        null;
    const ends = try pageEnds(arena, in.values, def_levels, max_def, fixed_width, in.page_bytes, in.page_rows);
    const present_before = try arena.alloc(usize, ends.len + 1);
    present_before[0] = 0;
    var lo: usize = 0;
    for (ends, 1..) |e, i| {
        const levels = if (def_levels) |d| d[lo..e] else null;
        present_before[i] = present_before[i - 1] + countPresent(e - lo, levels, max_def);
        lo = e;
    }
    std.debug.assert(present_before[ends.len] == indices.len);
    return .{
        .in = in,
        .kind = .{ .dict = .{
            .entries = dict,
            .indices = indices,
            .bit_width = dictIndexBitWidth(dict_count),
            .present_before = present_before,
        } },
        .ends = ends,
        .stats = stats,
        .dict_page = try std.mem.concat(arena, u8, &.{ dict_header, dict_compressed }),
        .dict_page_uncompressed = dict_header.len + dict_plain.len,
        .values_encoding = .PLAIN_DICTIONARY,
        .num_values = num_values,
        .def_levels = def_levels,
        .max_def = max_def,
        .optional = in.schema_elem.repetition_type == .OPTIONAL,
    };
}

/// Index width for a `dict_count`-entry dictionary, never below one bit. A 0-bit index page is legal RLE, but
/// strict readers (pyarrow) reject it ("Invalid number of indices: 0"), so a column filtered down to one distinct
/// value would be unreadable elsewhere while ZPQ's lenient reader hid it. parquet-mr uses the same minimum. Found by
/// the differential harness (ZPQ writes, pyarrow reads).
fn dictIndexBitWidth(dict_count: usize) u8 {
    return if (dict_count <= 1) 1 else bitWidthFor(@intCast(dict_count - 1));
}

/// A V1 data page's level stream: `<u32 LE byte length><RLE/bit-packed hybrid levels>`.
fn levelPrefix(arena: std.mem.Allocator, levels: []const u32, bit_width: u8) Error![]const u8 {
    const rle = try hybrid_rle.encode(arena, levels, bit_width);
    const prefix = try arena.alloc(u8, 4 + rle.len);
    std.mem.writeInt(u32, prefix[0..4], std.math.cast(u32, rle.len) orelse return error.TooLarge, .little);
    @memcpy(prefix[4..], rle);
    return prefix;
}

/// Def-level stream for a column that carries one. Without source levels every slot is present, so the stream is
/// all ones: a few bytes of RLE however many values there are.
fn defLevelPrefix(arena: std.mem.Allocator, def_levels: ?[]const u32, max_def: u32, num_values: i64) Error![]const u8 {
    const levels = def_levels orelse blk: {
        const ones = try arena.alloc(u32, @intCast(num_values));
        @memset(ones, 1);
        break :blk ones;
    };
    return levelPrefix(arena, levels, bitWidthFor(@max(max_def, 1)));
}

/// Serialized thrift for one page header; never compressed, so it goes straight in front of its payload.
fn pageHeaderBytes(arena: std.mem.Allocator, header: schema.PageHeader) Error![]const u8 {
    var w: thrift.Writer = .init(arena);
    try header.write(&w);
    return w.bytes();
}

/// Materialize the subset of `c` that's active in `sel` into newly-
/// allocated typed slices in `arena`. Used by the filter+encode path
/// to convert (decoded values, SelectionVector) into pre-filtered
/// values ready for encodeColumn.
///
/// Two cases:
/// - Filter touched this column: filter eval already dropped null
///   rows via SQL three-valued logic, so all surviving rows have
///   `def_levels[i] == max_def`. We could produce a dense output
///   (def_levels = null), but for uniformity we carry the filtered
///   def_levels through unchanged.
/// - Filter did NOT touch this column (projected through): nulls in
///   this column are still in the surviving set. We carry their
///   def_levels through so the encoder can emit a wire-correct
///   OPTIONAL page. Values at null slots stay at the decoder's
///   default (0 / false / "") — they're never written to PLAIN bytes.
pub fn applySelection(
    arena: std.mem.Allocator,
    c: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
) Error!filter_eval.Batch.Column {
    const surviving = sel.count();
    return switch (c) {
        .i32 => |col| .{ .i32 = try filterColumn(i32, arena, col, sel, surviving) },
        .i64 => |col| .{ .i64 = try filterColumn(i64, arena, col, sel, surviving) },
        .f32 => |col| .{ .f32 = try filterColumn(f32, arena, col, sel, surviving) },
        .f64 => |col| .{ .f64 = try filterColumn(f64, arena, col, sel, surviving) },
        .string => |col| .{ .string = try filterColumn([]const u8, arena, col, sel, surviving) },
        .boolean => |col| .{ .boolean = try filterColumn(bool, arena, col, sel, surviving) },
        .i128 => |col| .{ .i128 = try filterColumn(i128, arena, col, sel, surviving) },
    };
}

/// Apply a selection vector to an i128 column (the decimal output lane,
/// which lives outside `Batch.Column`). Reuses the generic compactor.
pub fn applySelectionI128(
    arena: std.mem.Allocator,
    col: filter_eval.ColumnT(i128),
    sel: *const filter_selection.SelectionVector,
) Error!filter_eval.ColumnT(i128) {
    return filterColumn(i128, arena, col, sel, sel.count());
}

fn filterColumn(
    comptime T: type,
    arena: std.mem.Allocator,
    col: filter_eval.ColumnT(T),
    sel: *const filter_selection.SelectionVector,
    surviving: usize,
) Error!filter_eval.ColumnT(T) {
    if (col.max_rep > 0) return filterColumnNested(T, arena, col, sel);

    // Flat / struct path: each value is one logical row, 1:1 with sel.
    // Hot loop: per-64-row dispatch on the selection bitmap word.
    // The naive `for (col.values) |v, i| if (sel.isActive(i))` shape
    // showed up as #1 leaf in CLI flamegraphs (5.28B cycles, ~70% of
    // encode CPU on a 50% selectivity workload). Three reasons:
    //   * 14 M `isActive` calls per RG (random branch each time)
    //   * Per-element store with no batching
    //   * Branch mispredicts dominate at mixed selectivities
    //
    // Per-word dispatch fixes all three. Empty word → skip 64 rows.
    // Full word → bulk @memcpy 64 elements. Mixed → ctz-walk set
    // bits without per-bit branch.
    const out_values = try arena.alloc(T, surviving);
    compactByMask(T, col.values, sel.mask, out_values);

    if (col.def_levels) |dl| {
        const out_dls = try arena.alloc(u32, surviving);
        compactByMask(u32, dl, sel.mask, out_dls);
        return .{ .values = out_values, .def_levels = out_dls, .max_def = col.max_def };
    }
    return .{ .values = out_values };
}

/// Compress `src` into `dst` using `mask` as the survival bitmap.
/// `dst.len` must equal `popcount(mask)` over the relevant prefix.
/// Walks `mask` 64 rows at a time — empty word skipped, full word
/// bulk-copied, mixed word iterated via @ctz.
pub fn compactByMask(
    comptime T: type,
    src: []const T,
    mask: []const u64,
    dst: []T,
) void {
    var i: usize = 0;
    var w: usize = 0;
    const ALL_ONES = ~@as(u64, 0);
    var word_i: usize = 0;
    while (i + 64 <= src.len) : ({
        i += 64;
        word_i += 1;
    }) {
        const word = mask[word_i];
        if (word == 0) continue;
        if (word == ALL_ONES) {
            @memcpy(dst[w..][0..64], src[i..][0..64]);
            w += 64;
            continue;
        }
        var bits = word;
        while (bits != 0) {
            const idx: u6 = @intCast(@ctz(bits));
            dst[w] = src[i + idx];
            w += 1;
            bits &= bits - 1;
        }
    }
    // Tail: process the last <64 rows, if any. The selection vector
    // already cleared bits past `len` at init time, so reading the
    // last word is safe.
    if (i < src.len) {
        const word = mask[word_i];
        var bits = word;
        while (bits != 0) {
            const idx: u6 = @intCast(@ctz(bits));
            const row = i + idx;
            if (row >= src.len) break;
            dst[w] = src[row];
            w += 1;
            bits &= bits - 1;
        }
    }
    std.debug.assert(w == dst.len);
}

/// Nested path: walk leaves by rep_levels (rep_levels[i] == 0 marks
/// the start of a new logical row). For each logical row, if it's
/// active in `sel`, emit ALL of its leaves; otherwise emit none.
/// This preserves list/map structure end-to-end.
fn filterColumnNested(
    comptime T: type,
    arena: std.mem.Allocator,
    col: filter_eval.ColumnT(T),
    sel: *const filter_selection.SelectionVector,
) Error!filter_eval.ColumnT(T) {
    const rep = col.rep_levels.?;
    const dl = col.def_levels.?;

    // First pass: count leaves we'll emit. Walk leaves, increment
    // logical-row index when rep == 0, sum leaves of active rows.
    var emit_count: usize = 0;
    {
        var row_idx: i64 = -1;
        var active = false;
        for (rep) |r| {
            if (r == 0) {
                row_idx += 1;
                active = sel.isActive(@intCast(row_idx));
            }
            if (active) emit_count += 1;
        }
    }

    const out_values = try arena.alloc(T, emit_count);
    const out_dls = try arena.alloc(u32, emit_count);
    const out_rls = try arena.alloc(u32, emit_count);

    var w: usize = 0;
    var row_idx: i64 = -1;
    var active = false;
    for (rep, 0..) |r, i| {
        if (r == 0) {
            row_idx += 1;
            active = sel.isActive(@intCast(row_idx));
        }
        if (active) {
            out_values[w] = col.values[i];
            out_dls[w] = dl[i];
            out_rls[w] = r;
            w += 1;
        }
    }

    return .{
        .values = out_values,
        .def_levels = out_dls,
        .max_def = col.max_def,
        .rep_levels = out_rls,
        .max_rep = col.max_rep,
    };
}

// ============================================================
// PLAIN encoders per physical type
// ============================================================

/// Get the column's def_levels slice (null when REQUIRED / no nulls).
fn sourceDefLevels(c: filter_eval.Batch.Column) ?[]const u32 {
    return switch (c) {
        .i32 => |x| x.def_levels,
        .i64 => |x| x.def_levels,
        .f32 => |x| x.def_levels,
        .f64 => |x| x.def_levels,
        .string => |x| x.def_levels,
        .boolean => |x| x.def_levels,
        .i128 => |x| x.def_levels,
    };
}

fn sourceMaxDef(c: filter_eval.Batch.Column) u32 {
    return switch (c) {
        .i32 => |x| x.max_def,
        .i64 => |x| x.max_def,
        .f32 => |x| x.max_def,
        .f64 => |x| x.max_def,
        .string => |x| x.max_def,
        .boolean => |x| x.max_def,
        .i128 => |x| x.max_def,
    };
}

fn sourceRepLevels(c: filter_eval.Batch.Column) ?[]const u32 {
    return switch (c) {
        .i32 => |x| x.rep_levels,
        .i64 => |x| x.rep_levels,
        .f32 => |x| x.rep_levels,
        .f64 => |x| x.rep_levels,
        .string => |x| x.rep_levels,
        .boolean => |x| x.rep_levels,
        .i128 => |x| x.rep_levels,
    };
}

fn sourceMaxRep(c: filter_eval.Batch.Column) u32 {
    return switch (c) {
        .i32 => |x| x.max_rep,
        .i64 => |x| x.max_rep,
        .f32 => |x| x.max_rep,
        .f64 => |x| x.max_rep,
        .string => |x| x.max_rep,
        .boolean => |x| x.max_rep,
        .i128 => |x| x.max_rep,
    };
}

/// Bits to encode 0..max inclusive (parquet uses ceil(log2(max+1))).
/// Mirrors the helper in `core/parquet/column.zig` — kept local here
/// so encoder doesn't have to import the column module.
fn bitWidthFor(max: u32) u8 {
    if (max == 0) return 0;
    return @intCast(32 - @clz(max));
}

/// True iff `def_levels[i] == max_def` (slot is non-null). Returns
/// true unconditionally when `def_levels == null` (column is dense).
inline fn isPresent(def_levels: ?[]const u32, max_def: u32, i: usize) bool {
    if (def_levels) |dl| return dl[i] == max_def;
    return true;
}

fn encodeValuesPlain(
    arena: std.mem.Allocator,
    vals: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
    fixed: bool,
) Error![]u8 {
    return switch (vals) {
        .i32 => |c| try encodePlainTyped(i32, arena, c.values, def_levels, max_def),
        .i64 => |c| try encodePlainTyped(i64, arena, c.values, def_levels, max_def),
        .f32 => |c| try encodePlainTyped(f32, arena, c.values, def_levels, max_def),
        .f64 => |c| try encodePlainTyped(f64, arena, c.values, def_levels, max_def),
        .string => |c| try encodePlainBytes(arena, c.values, def_levels, max_def, fixed),
        .boolean => |c| try encodePlainBool(arena, c.values, def_levels, max_def),
        // encodeColumn sends DECIMAL to encodeDecimalColumn before any PLAIN encoding.
        .i128 => unreachable,
    };
}

/// DELTA_BINARY_PACKED encoding for i32/i64 columns. Materializes the
/// present-only subset as a contiguous typed slice (for use in the
/// delta encoder which doesn't know about def_levels), then encodes
/// it. Returns null for non-INT32/INT64 columns — caller falls back
/// to PLAIN.
fn tryEncodeValuesDelta(
    arena: std.mem.Allocator,
    vals: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
) Error!?[]u8 {
    return switch (vals) {
        .i32 => |c| try buildAndDeltaEncode(i32, arena, c.values, def_levels, max_def),
        .i64 => |c| try buildAndDeltaEncode(i64, arena, c.values, def_levels, max_def),
        else => null,
    };
}

fn buildAndDeltaEncode(
    comptime T: type,
    arena: std.mem.Allocator,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
) Error![]u8 {
    const num_present = countPresent(values.len, def_levels, max_def);
    if (num_present == 0) {
        // Empty payload still needs a valid header. delta encoder
        // handles len=0 by writing header-only with 0 first_value.
        return try delta_binary_packed.encodeDefault(T, arena, &[_]T{});
    }

    if (def_levels == null) {
        // Dense path — values is already the present-only slice.
        return try delta_binary_packed.encodeDefault(T, arena, values);
    }

    // Sparse path — copy present values into a contiguous buffer.
    const dense = try arena.alloc(T, num_present);
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        dense[pos] = v;
        pos += 1;
    }
    return try delta_binary_packed.encodeDefault(T, arena, dense);
}

const StringValues = struct { bytes: []const u8, encoding: schema.Encoding };

/// The layout a BYTE_ARRAY column the dictionary declined is written in (present values only), one of two the decoder
/// reads:
/// - DELTA_BYTE_ARRAY, `DBP(prefix_lengths) ++ DBP(suffix_lengths) ++ concat(suffixes)` (prefix_length[i] the length
///   shared with the previous present value, suffix[i] the rest), where those prefixes cover at least a quarter of
///   the column's bytes: sorted keys, paths;
/// - otherwise DELTA_LENGTH_BYTE_ARRAY, `DBP(lengths) ++ concat(values)`.
/// Both keep the bytes contiguous, apart from their lengths: zstd compresses that faster and smaller than PLAIN's
/// interleaved lengths, and readers decode it faster (DELTA_LENGTH_BYTE_ARRAY fastest of all in Polars, pyarrow and
/// DuckDB; DELTA_BYTE_ARRAY several times slower in Polars, hence the prefix condition). Decided over the whole chunk,
/// each value against the previous present one. Null (PLAIN) for an empty column, and for a FIXED_LEN_BYTE_ARRAY column
/// (`fixed`) whose prefixes do not pay: DELTA_LENGTH_BYTE_ARRAY is defined for BYTE_ARRAY only, and strict readers
/// reject it on FLBA.
fn stringsEncoding(values: []const []const u8, def_levels: ?[]const u32, max_def: u32, fixed: bool) ?schema.Encoding {
    var prev: []const u8 = &.{};
    var any = false;
    var prefix_total: usize = 0;
    var total: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        prefix_total += commonPrefix(prev, v);
        total += v.len;
        prev = v;
        any = true;
    }
    if (!any) return null;
    if (prefix_total * 4 >= total) return .DELTA_BYTE_ARRAY;
    return if (fixed) null else .DELTA_LENGTH_BYTE_ARRAY;
}

/// The present `values` in `encoding` (DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY), the first one's prefix empty.
fn encodeStringsAs(
    arena: std.mem.Allocator,
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    encoding: schema.Encoding,
) Error![]const u8 {
    const num_present = countPresent(values.len, def_levels, max_def);
    if (encoding == .DELTA_LENGTH_BYTE_ARRAY) {
        const lengths = try arena.alloc(i32, num_present);
        var total: usize = 0;
        var k: usize = 0;
        for (values, 0..) |v, i| {
            if (!isPresent(def_levels, max_def, i)) continue;
            lengths[k] = @intCast(v.len);
            total += v.len;
            k += 1;
        }
        const len_enc = try delta_binary_packed.encodeDefault(i32, arena, lengths);
        const out = try arena.alloc(u8, len_enc.len + total);
        @memcpy(out[0..len_enc.len], len_enc);
        var o = len_enc.len;
        for (values, 0..) |v, i| {
            if (!isPresent(def_levels, max_def, i)) continue;
            @memcpy(out[o..][0..v.len], v);
            o += v.len;
        }
        return out;
    }
    const prefix_lengths = try arena.alloc(i32, num_present);
    const suffix_lengths = try arena.alloc(i32, num_present);
    var prev: []const u8 = &.{};
    var k: usize = 0;
    var suffix_total: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const lcp = commonPrefix(prev, v);
        prefix_lengths[k] = @intCast(lcp);
        suffix_lengths[k] = @intCast(v.len - lcp);
        suffix_total += v.len - lcp;
        prev = v;
        k += 1;
    }
    const prefix_enc = try delta_binary_packed.encodeDefault(i32, arena, prefix_lengths);
    const suffix_len_enc = try delta_binary_packed.encodeDefault(i32, arena, suffix_lengths);
    const out = try arena.alloc(u8, prefix_enc.len + suffix_len_enc.len + suffix_total);
    @memcpy(out[0..prefix_enc.len], prefix_enc);
    @memcpy(out[prefix_enc.len..][0..suffix_len_enc.len], suffix_len_enc);
    var o = prefix_enc.len + suffix_len_enc.len;
    k = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const lcp: usize = @intCast(prefix_lengths[k]);
        @memcpy(out[o..][0 .. v.len - lcp], v[lcp..]);
        o += v.len - lcp;
        k += 1;
    }
    return out;
}

/// A whole string column in the layout `stringsEncoding` picks, or null where it picks PLAIN.
fn encodeValuesStrings(
    arena: std.mem.Allocator,
    vals: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
    fixed: bool,
) Error!?StringValues {
    const col = switch (vals) {
        .string => |c| c,
        else => return null,
    };
    const encoding = stringsEncoding(col.values, def_levels, max_def, fixed) orelse return null;
    return .{ .bytes = try encodeStringsAs(arena, col.values, def_levels, max_def, encoding), .encoding = encoding };
}

/// Length of the common prefix of `a` and `b`: 16 bytes compared per step, then 8, then one overlapping 8-byte
/// window over the tail (its bytes before `n` are already known equal), so no byte loop unless both are under 8.
fn commonPrefix(a: []const u8, b: []const u8) usize {
    const cap = @min(a.len, b.len);
    var n: usize = 0;
    while (n + 16 <= cap) : (n += 16) {
        const x: @Vector(16, u8) = a[n..][0..16].*;
        const y: @Vector(16, u8) = b[n..][0..16].*;
        const ne: u16 = @bitCast(x != y);
        if (ne != 0) return n + @ctz(ne);
    }
    if (cap >= 8) {
        while (n + 8 <= cap) : (n += 8) {
            const x = std.mem.readInt(u64, a[n..][0..8], .little) ^ std.mem.readInt(u64, b[n..][0..8], .little);
            if (x != 0) return n + @ctz(x) / 8;
        }
        if (n == cap) return n;
        const t = cap - 8;
        const x = std.mem.readInt(u64, a[t..][0..8], .little) ^ std.mem.readInt(u64, b[t..][0..8], .little);
        return if (x != 0) t + @ctz(x) / 8 else cap;
    }
    while (n < cap and a[n] == b[n]) : (n += 1) {}
    return n;
}

/// Number of slots where def_levels[i] == max_def. When def_levels is
/// null, every slot counts.
fn countPresent(values_len: usize, def_levels: ?[]const u32, max_def: u32) usize {
    const dl = def_levels orelse return values_len;
    const L = 16;
    const want: @Vector(L, u32) = @splat(max_def);
    var n: usize = 0;
    var i: usize = 0;
    while (i + L <= dl.len) : (i += L) {
        const v: @Vector(L, u32) = dl[i..][0..L].*;
        n += @popCount(@as(u16, @bitCast(v == want)));
    }
    for (dl[i..]) |d| n += @intFromBool(d == max_def);
    return n;
}

/// PLAIN encoding for fixed-width primitive types (i32/i64/f32/f64).
/// When `def_levels` is non-null, only emits values at slots where
/// `def_levels[i] == max_def` (parquet PLAIN excludes null slots).
fn encodePlainTyped(
    comptime T: type,
    arena: std.mem.Allocator,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
) Error![]u8 {
    const item_size = @sizeOf(T);
    const num_present = countPresent(values.len, def_levels, max_def);
    const out = try arena.alloc(u8, num_present * item_size);
    // Dense: the values are already PLAIN's little-endian bytes.
    if (def_levels == null and comptime @import("builtin").cpu.arch.endian() == .little) {
        @memcpy(out, std.mem.sliceAsBytes(values));
        return out;
    }
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        switch (@typeInfo(T)) {
            .int => std.mem.writeInt(T, out[pos..][0..item_size], v, .little),
            .float => {
                const Bits = @Int(.unsigned, @bitSizeOf(T));
                const bits: Bits = @bitCast(v);
                std.mem.writeInt(Bits, out[pos..][0..item_size], bits, .little);
            },
            else => @compileError("encodePlainTyped: unsupported type"),
        }
        pos += item_size;
    }
    return out;
}

/// PLAIN encoding for BYTE_ARRAY: each present value is `<u32 LE length><bytes>`. For FIXED_LEN_BYTE_ARRAY (`fixed`)
/// the bytes alone: the width is the schema's `type_length`, and a length prefix would be read as value bytes.
fn encodePlainBytes(
    arena: std.mem.Allocator,
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    fixed: bool,
) Error![]u8 {
    const len_prefix: usize = if (fixed) 0 else 4;
    var total: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        total += len_prefix + v.len;
    }
    // Every length prefix below is at most `total`.
    if (total > std.math.maxInt(u32)) return error.TooLarge;

    const out = try arena.alloc(u8, total);
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        if (!fixed) std.mem.writeInt(u32, out[pos..][0..4], @intCast(v.len), .little);
        @memcpy(out[pos + len_prefix ..][0..v.len], v);
        pos += len_prefix + v.len;
    }
    return out;
}

/// PLAIN encoding for BOOLEAN: bit-packed, LSB first. Only present
/// values are packed; null slots are excluded entirely.
fn encodePlainBool(
    arena: std.mem.Allocator,
    values: []const bool,
    def_levels: ?[]const u32,
    max_def: u32,
) Error![]u8 {
    const num_present = countPresent(values.len, def_levels, max_def);
    const num_bytes = (num_present + 7) / 8;
    const out = try arena.alloc(u8, num_bytes);
    @memset(out, 0);
    if (def_levels == null) {
        // Dense: 8 values per output byte, LSB first.
        var b: usize = 0;
        while (b * 8 + 8 <= values.len) : (b += 1) {
            const v: @Vector(8, bool) = values[b * 8 ..][0..8].*;
            out[b] = @bitCast(v);
        }
        for (values[b * 8 ..], 0..) |v, k| out[b] |= @as(u8, @intFromBool(v)) << @intCast(k);
        return out;
    }
    var bit_idx: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        if (v) {
            const byte_idx = bit_idx / 8;
            const bit_in_byte: u3 = @intCast(bit_idx % 8);
            out[byte_idx] |= (@as(u8, 1) << bit_in_byte);
        }
        bit_idx += 1;
    }
    return out;
}

fn valueCount(c: filter_eval.Batch.Column) usize {
    return switch (c) {
        .i32 => |x| x.values.len,
        .i64 => |x| x.values.len,
        .f32 => |x| x.values.len,
        .f64 => |x| x.values.len,
        .string => |x| x.values.len,
        .boolean => |x| x.values.len,
        .i128 => |x| x.values.len,
    };
}

// ============================================================
// Statistics — min/max over surviving non-null values
// ============================================================

/// Rows whose dictionary entry is NaN.
fn nanRows(entries: anytype, indices: []const u32) i64 {
    var n: i64 = 0;
    for (indices) |k| n += @intFromBool(std.math.isNan(entries[k]));
    return n;
}

fn computeStats(
    arena: std.mem.Allocator,
    c: filter_eval.Batch.Column,
    def_levels: ?[]const u32,
    max_def: u32,
    elem: *const schema.SchemaElement,
) ?schema.Statistics {
    // An unsigned column's bounds are ordered unsigned. The deprecated min/max pair is signed-ordered by definition,
    // so it is left unset rather than written in the wrong order.
    if (schema.isUnsignedInt(elem.*)) {
        const lane_fits = switch (c) {
            .i32 => elem.type == .INT32,
            .i64 => elem.type == .INT64,
            else => false,
        };
        if (!lane_fits) return null;
        var stats = switch (c) {
            .i32 => |x| statsTyped(arena, u32, @ptrCast(x.values), def_levels, max_def),
            .i64 => |x| statsTyped(arena, u64, @ptrCast(x.values), def_levels, max_def),
            else => unreachable,
        } orelse return null;
        stats.min = null;
        stats.max = null;
        return stats;
    }
    return switch (c) {
        .i32 => |x| statsTyped(arena, i32, x.values, def_levels, max_def),
        .i64 => |x| statsTyped(arena, i64, x.values, def_levels, max_def),
        .f32 => |x| statsTypedFloat(arena, f32, x.values, def_levels, max_def),
        .f64 => |x| statsTypedFloat(arena, f64, x.values, def_levels, max_def),
        .string => |x| statsBytes(arena, x.values, def_levels, max_def, boundCut(elem.*)),
        .boolean => |x| statsBool(arena, x.values, def_levels, max_def),
        .i128 => unreachable,
    };
}

/// Min/max scan, dense or null-aware. The dense path is hoisted out
/// of the null-check so LLVM autovectorizes (no per-iteration branch
/// on def_levels presence). For nullable columns it falls back to
/// the scan-with-isPresent loop. Stats buffers come from `arena`,
/// not `std.heap.page_allocator` — those 4-byte allocs were mmap
/// syscalls (the page allocator's minimum), 1000+ per Lambda
/// invocation; the arena collapses them to bumps.
fn statsTyped(arena: std.mem.Allocator, comptime T: type, values: []const T, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var lo: T = undefined;
    var hi: T = undefined;
    var null_count: i64 = 0;
    if (def_levels) |dl| {
        var first_idx: ?usize = null;
        for (values, 0..) |_, i| {
            if (dl[i] >= max_def) {
                first_idx = i;
                break;
            }
            null_count += 1;
        }
        // All-null column: min/max undefined, but null_count alone is
        // useful (strict pruners short-circuit count(col) + IS NULL).
        const fi = first_idx orelse return .{ .null_count = null_count };
        lo = values[fi];
        hi = values[fi];
        for (values[fi + 1 ..], fi + 1..) |v, i| {
            if (dl[i] < max_def) {
                null_count += 1;
                continue;
            }
            if (v < lo) lo = v;
            if (v > hi) hi = v;
        }
    } else {
        // Dense path, null_count 0: explicit vector lanes, which the conditional-assignment loop did not become.
        const L = 16;
        const V = @Vector(L, T);
        var vlo: V = @splat(values[0]);
        var vhi: V = vlo;
        var i: usize = 0;
        while (i + L <= values.len) : (i += L) {
            const v: V = values[i..][0..L].*;
            vlo = @min(vlo, v);
            vhi = @max(vhi, v);
        }
        lo = @reduce(.Min, vlo);
        hi = @reduce(.Max, vhi);
        for (values[i..]) |v| {
            lo = @min(lo, v);
            hi = @max(hi, v);
        }
    }
    const item_size = @sizeOf(T);
    const min_buf = arena.alloc(u8, item_size) catch return null;
    const max_buf = arena.alloc(u8, item_size) catch return null;
    std.mem.writeInt(T, min_buf[0..item_size], lo, .little);
    std.mem.writeInt(T, max_buf[0..item_size], hi, .little);
    return .{
        .min = min_buf,
        .max = max_buf,
        .min_value = min_buf,
        .max_value = max_buf,
        .is_min_value_exact = true,
        .is_max_value_exact = true,
        .null_count = null_count,
    };
}

/// Float bounds follow the format's TYPE_DEFINED_ORDER writing rules: NaN never enters min or max (a chunk whose
/// present values are all NaN gets null_count and nan_count only), nan_count is always written, a zero min is written
/// as -0.0 and a zero max as +0.0, so a reader that trusts the bounds still keeps a chunk holding the other zero. A
/// reader that knows a chunk holds no NaN may prune `>`/`>=` on its max, where NaN would pass.
fn statsTypedFloat(arena: std.mem.Allocator, comptime T: type, values: []const T, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var lo: T = std.math.inf(T);
    var hi: T = -std.math.inf(T);
    var any = false;
    var null_count: i64 = 0;
    var nan_count: i64 = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) {
            null_count += 1;
            continue;
        }
        if (std.math.isNan(v)) {
            nan_count += 1;
            continue;
        }
        any = true;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    if (!any) return .{ .null_count = null_count, .nan_count = nan_count };
    if (lo == 0) lo = -0.0;
    if (hi == 0) hi = 0.0;
    const Bits = @Int(.unsigned, @bitSizeOf(T));
    const item_size = @sizeOf(T);
    const min_buf = arena.alloc(u8, item_size) catch return null;
    const max_buf = arena.alloc(u8, item_size) catch return null;
    const lo_bits: Bits = @bitCast(lo);
    const hi_bits: Bits = @bitCast(hi);
    std.mem.writeInt(Bits, min_buf[0..item_size], lo_bits, .little);
    std.mem.writeInt(Bits, max_buf[0..item_size], hi_bits, .little);
    return .{
        .min = min_buf,
        .max = max_buf,
        .min_value = min_buf,
        .max_value = max_buf,
        .is_min_value_exact = true,
        .is_max_value_exact = true,
        .null_count = null_count,
        .nan_count = nan_count,
    };
}

/// Written BYTE_ARRAY bounds are cut to this many bytes, the length arrow-rs cuts statistics to: kept whole, one 100 MB
/// value fills the footer twice over, past what readers parse (pyarrow refuses a thrift string over 100 MB).
const stats_bound_len = 64;

/// How a column's byte bounds are cut: FIXED_LEN_BYTE_ARRAY not at all (a bound of another width reads as corrupt),
/// UTF-8 text at a character boundary so the bound stays text, other bytes anywhere.
const BoundCut = enum { none, bytes, utf8 };

fn boundCut(elem: schema.SchemaElement) BoundCut {
    if (elem.type != .BYTE_ARRAY) return .none;
    if (elem.logical_type) |lt| switch (lt) {
        .STRING, .ENUM, .JSON => return .utf8,
        else => {},
    };
    if (elem.converted_type) |ct| switch (ct) {
        .UTF8, .ENUM, .JSON => return .utf8,
        else => {},
    };
    return .bytes;
}

/// `v` cut to at most `stats_bound_len` bytes: a prefix, so still no greater than `v`.
fn cutMin(v: []const u8, cut: BoundCut) []const u8 {
    if (cut == .none or v.len <= stats_bound_len) return v;
    var n: usize = stats_bound_len;
    if (cut == .utf8 and std.unicode.utf8ValidateSlice(v)) {
        while (n > 0 and (v[n] & 0xC0) == 0x80) n -= 1;
    }
    return v[0..n];
}

/// An upper bound for `v` of at most `stats_bound_len` bytes, on `arena`: `v` cut, then its last byte (UTF-8: last
/// character) that can grow by one without changing width grown, and what follows it dropped. Null when nothing
/// can grow (all 0xFF), and then no max is written. `v` itself when it needs no cut.
fn cutMax(arena: std.mem.Allocator, v: []const u8, cut: BoundCut) std.mem.Allocator.Error!?[]const u8 {
    if (cut == .none or v.len <= stats_bound_len) return v;
    if (cut == .utf8 and std.unicode.utf8ValidateSlice(v)) {
        var end: usize = stats_bound_len;
        while (end > 0 and (v[end] & 0xC0) == 0x80) end -= 1;
        while (end > 0) {
            var start = end - 1;
            while (start > 0 and (v[start] & 0xC0) == 0x80) start -= 1;
            const cp = std.unicode.utf8Decode(v[start..end]) catch unreachable;
            const next = cp + 1;
            const surrogate = next >= 0xD800 and next <= 0xDFFF;
            const width = std.unicode.utf8CodepointSequenceLength(next) catch 0;
            if (!surrogate and next <= 0x10FFFF and width == end - start) {
                const out = try arena.alloc(u8, end);
                @memcpy(out[0..start], v[0..start]);
                _ = std.unicode.utf8Encode(next, out[start..end]) catch unreachable;
                return out;
            }
            end = start;
        }
        return null;
    }
    var n: usize = stats_bound_len;
    while (n > 0 and v[n - 1] == 0xFF) n -= 1;
    if (n == 0) return null;
    const out = try arena.dupe(u8, v[0..n]);
    out[n - 1] += 1;
    return out;
}

fn statsBytes(
    arena: std.mem.Allocator,
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    cut: BoundCut,
) ?schema.Statistics {
    if (values.len == 0) return null;
    var first_idx: ?usize = null;
    var null_count: i64 = 0;
    for (values, 0..) |_, i| {
        if (isPresent(def_levels, max_def, i)) {
            first_idx = i;
            break;
        }
        null_count += 1;
    }
    // All-null column: see statsTyped above.
    const fi = first_idx orelse return .{ .null_count = null_count };
    var lo: []const u8 = values[fi];
    var hi: []const u8 = values[fi];
    // Compared by 16-byte big-endian prefix first (a lesser prefix is a lesser string); bytes only on a tie.
    var lo_key = prefix16(lo);
    var hi_key = lo_key;
    for (values[fi + 1 ..], fi + 1..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) {
            null_count += 1;
            continue;
        }
        // A first byte strictly between the bounds' first bytes makes v neither (most values, once bounds settle).
        if (v.len > 0 and lo.len > 0 and hi.len > 0 and v[0] > lo[0] and v[0] < hi[0]) continue;
        const k = prefix16(v);
        if (k < lo_key or (k == lo_key and std.mem.lessThan(u8, v, lo))) {
            lo = v;
            lo_key = k;
        }
        if (k > hi_key or (k == hi_key and std.mem.lessThan(u8, hi, v))) {
            hi = v;
            hi_key = k;
        }
    }
    // Caller's arena is the file-footer-lifetime arena; the values
    // slices typically borrow from a row-group-scoped arena. Dupe so
    // the footer write later sees stable bytes.
    const lo_cut = cutMin(lo, cut);
    const hi_cut = cutMax(arena, hi, cut) catch return null;
    const lo_owned = arena.dupe(u8, lo_cut) catch return null;
    const hi_owned: ?[]const u8 = if (hi_cut) |h| arena.dupe(u8, h) catch return null else null;
    // Bytewise unsigned is a byte array's type-defined order. The deprecated min/max are defined as signed, so they
    // are left unset rather than written in an order a legacy reader would misread. A cut bound says so, and readers
    // then take it as a bound only (zpq's own full-match proof needs an exact max).
    return .{
        .min_value = lo_owned,
        .max_value = hi_owned,
        .null_count = null_count,
        .is_min_value_exact = lo_cut.len == lo.len,
        // `cutMax` hands back `hi` itself when it needs no cut.
        .is_max_value_exact = if (hi_cut) |h| h.ptr == hi.ptr else null,
    };
}

fn statsBool(arena: std.mem.Allocator, values: []const bool, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var has_t = false;
    var has_f = false;
    var any_present = false;
    var null_count: i64 = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) {
            null_count += 1;
            continue;
        }
        any_present = true;
        if (v) has_t = true else has_f = true;
    }
    // All-null bool column: see statsTyped above.
    if (!any_present) return .{ .null_count = null_count };
    const min_buf = arena.alloc(u8, 1) catch return null;
    const max_buf = arena.alloc(u8, 1) catch return null;
    min_buf[0] = if (has_f) 0 else 1;
    max_buf[0] = if (has_t) 1 else 0;
    return .{
        .min = min_buf,
        .max = max_buf,
        .min_value = min_buf,
        .max_value = max_buf,
        .is_min_value_exact = true,
        .is_max_value_exact = true,
        .null_count = null_count,
    };
}

// ============================================================
// Tests — round-trip through metadata.open
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

fn expectMirroredStats(stats: schema.Statistics) !void {
    try testing.expectEqual(@as(?bool, true), stats.is_min_value_exact);
    try testing.expectEqual(@as(?bool, true), stats.is_max_value_exact);
    try testing.expect(stats.min != null);
    try testing.expect(stats.max != null);
    try testing.expect(stats.min_value != null);
    try testing.expect(stats.max_value != null);
    try testing.expectEqualSlices(u8, stats.min.?, stats.min_value.?);
    try testing.expectEqualSlices(u8, stats.max.?, stats.max_value.?);
}

test "statistics populate legacy bounds only where their signed order is the type order" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const ints = [_]i32{ 7, -3, 9, 0 };
    try expectMirroredStats(statsTyped(arena, i32, &ints, null, 0) orelse return error.MissingStats);

    const floats = [_]f64{ 3.5, -1.25, 9.0 };
    try expectMirroredStats(statsTypedFloat(arena, f64, &floats, null, 0) orelse return error.MissingStats);

    const bytes = [_][]const u8{ "bravo", "alpha", "charlie", "\xc3\xa9" };
    const byte_stats = statsBytes(arena, &bytes, null, 0, .bytes) orelse return error.MissingStats;
    try testing.expect(byte_stats.min == null and byte_stats.max == null);
    try testing.expectEqualStrings("alpha", byte_stats.min_value.?);
    try testing.expectEqualStrings("\xc3\xa9", byte_stats.max_value.?);

    const bools = [_]bool{ true, true, false };
    try expectMirroredStats(statsBool(arena, &bools, null, 0) orelse return error.MissingStats);
}

test "encodePlainTyped i32 round-trip via slice copy" {
    const arena = testing.allocator;
    const values = [_]i32{ -1, 0, 1, 100, std.math.maxInt(i32), std.math.minInt(i32) };
    const encoded = try encodePlainTyped(i32, arena, &values, null, 0);
    defer arena.free(encoded);

    try testing.expectEqual(@as(usize, values.len * @sizeOf(i32)), encoded.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        const got = std.mem.readInt(i32, encoded[i * 4 ..][0..4], .little);
        try testing.expectEqual(values[i], got);
    }
}

test "encodePlainTyped i32 with mixed nulls only emits present values" {
    const arena = testing.allocator;
    // Source: 5 rows, [v0, NULL, v2, NULL, v4]. PLAIN bytes contain
    // only v0, v2, v4. Reader recovers nulls from def_levels (not
    // exercised here — this test just checks the encoder side).
    const values = [_]i32{ 100, 0, 200, 0, 300 }; // null slots have default 0
    const def_levels = [_]u32{ 1, 0, 1, 0, 1 };
    const encoded = try encodePlainTyped(i32, arena, &values, &def_levels, 1);
    defer arena.free(encoded);

    // 3 values × 4 bytes = 12 bytes total.
    try testing.expectEqual(@as(usize, 3 * @sizeOf(i32)), encoded.len);
    try testing.expectEqual(@as(i32, 100), std.mem.readInt(i32, encoded[0..4], .little));
    try testing.expectEqual(@as(i32, 200), std.mem.readInt(i32, encoded[4..8], .little));
    try testing.expectEqual(@as(i32, 300), std.mem.readInt(i32, encoded[8..12], .little));
}

test "encodePlainBytes round-trip" {
    const arena = testing.allocator;
    const v = [_][]const u8{ "alpha", "", "BETA", "γ" };
    const encoded = try encodePlainBytes(arena, &v, null, 0, false);
    defer arena.free(encoded);

    var pos: usize = 0;
    for (v) |orig| {
        const len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
        try testing.expectEqual(@as(u32, @intCast(orig.len)), len);
        try testing.expectEqualStrings(orig, encoded[pos + 4 .. pos + 4 + len]);
        pos += 4 + len;
    }
}

test "both string layouts round-trip through their decoders (incl. nulls), chosen by shared prefixes" {
    const dba = @import("../parquet/encoding/delta_byte_array.zig");
    var arena_inst = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_inst.deinit();
    const a = arena_inst.allocator();
    const dls = [_]u32{ 1, 1, 0, 1, 1, 1 }; // index 2 is null (def < max_def)

    // Shared-prefix strings: DELTA_BYTE_ARRAY.
    const shared = [_][]const u8{ "apple", "apple_pie", "", "apricot", "banana", "bandana" };
    // Unrelated strings: DELTA_LENGTH_BYTE_ARRAY.
    const unrelated = [_][]const u8{ "kiwi", "fig", "", "lime", "", "date" };
    for ([_]struct { vals: []const []const u8, want: schema.Encoding }{
        .{ .vals = &shared, .want = .DELTA_BYTE_ARRAY },
        .{ .vals = &unrelated, .want = .DELTA_LENGTH_BYTE_ARRAY },
    }) |case| {
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = case.vals, .def_levels = &dls, .max_def = 1 } };
        const enc = (try encodeValuesStrings(a, col, &dls, 1, false)) orelse return error.ExpectedEncoding;
        try testing.expectEqual(case.want, enc.encoding);
        var out: [5][]const u8 = undefined; // 5 present values
        const n = if (enc.encoding == .DELTA_BYTE_ARRAY) blk: {
            var dec = try dba.DeltaByteArrayDecoder.init(enc.bytes, a, std.math.maxInt(usize), std.math.maxInt(usize));
            break :blk try dec.decode(&out);
        } else blk: {
            var dec = try dba.DeltaLengthByteArrayDecoder.init(enc.bytes, a, std.math.maxInt(usize));
            break :blk try dec.decode(&out);
        };
        try testing.expectEqual(@as(usize, 5), n);
        var k: usize = 0;
        for (case.vals, dls) |v, d| {
            if (d == 0) continue;
            try testing.expectEqualStrings(v, out[k]);
            k += 1;
        }
    }
}

test "encodePlainBool packs 8 per byte LSB-first" {
    const arena = testing.allocator;
    const v = [_]bool{ true, false, true, true, false, false, true, false, true };
    const encoded = try encodePlainBool(arena, &v, null, 0);
    defer arena.free(encoded);

    try testing.expectEqual(@as(usize, 2), encoded.len);
    try testing.expectEqual(@as(u8, 0b01001101), encoded[0]);
    try testing.expectEqual(@as(u8, 0b00000001), encoded[1]);
}

test "encodeColumn string with mixed nulls writes valid OPTIONAL page" {
    // Repro for the lambda segfault on string_nullable projection.
    // Decodes the actually-null string column, then re-encodes through
    // encodeColumn with the source's def_levels carried through.
    const column = @import("../parquet/column.zig");
    const meta_mod = @import("../parquet/metadata.zig");
    const file_bytes = meta_mod.readFileSlice("data/benchmark_100mb.parquet", testing.allocator) catch |err| {
        if (err == error.FileNotFound) return; // missing fixture
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const meta = try meta_mod.open(arena, file_bytes);
    const col_idx = meta_mod.findColumnIndex(&meta, "string_nullable") orelse return error.MissingColumn;
    const rg0 = &meta.row_groups.items[0];
    const col = rg0.columns.items[col_idx].meta_data.?;

    const chunk_start: usize = if (col.dictionary_page_offset) |dp| @intCast(dp) else @intCast(col.data_page_offset);
    const chunk_len: usize = @intCast(col.total_compressed_size);
    const chunk = file_bytes[chunk_start .. chunk_start + chunk_len];

    const path_arr: [1][]const u8 = .{"string_nullable"};
    const levels = meta.getColumnLevels(&path_arr);

    var reader = column.ColumnChunkReader([]const u8).init(chunk, col.codec, levels, arena);
    const num_rows: usize = @intCast(col.num_values);
    const values = try arena.alloc([]const u8, num_rows);
    const def_levels = try arena.alloc(u32, num_rows);

    var written: usize = 0;
    while (written < num_rows) {
        const n = try reader.decodeWithLevels(values[written..], def_levels[written..]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(num_rows, written);

    // Re-encode through encodeColumn.
    const elem = meta.schema.items[col_idx + 1];
    const enc = try encodeColumn(arena, .{
        .values = .{ .string = .{
            .values = values,
            .def_levels = def_levels,
            .max_def = 1,
        } },
        .schema_elem = &elem,
        .path_in_schema = &path_arr,
    });

    // Sanity: encoded bytes are non-empty and meta has the right row count.
    try testing.expect(enc.bytes.len > 0);
    try testing.expectEqual(@as(i64, @intCast(num_rows)), enc.meta.num_values);
    std.debug.print(
        "[encoder] string_nullable re-encoded: {d}B (input had {d} rows, ~{d} non-null)\n",
        .{ enc.bytes.len, num_rows, blk: {
            var p: usize = 0;
            for (def_levels) |d| if (d == 1) {
                p += 1;
            };
            break :blk p;
        } },
    );
}

// ============================================================
// Tests — all-null pages emit Statistics with null_count only.
// Previous behavior was to return null (no Statistics struct);
// emitting null_count alone preserves the IS NULL / count(col)
// short-circuit signal that a fully-null page still carries.
// ============================================================

test "statsTyped i32 all-null page emits null_count only" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const vals = [_]i32{ 0, 0, 0, 0 };
    const dls = [_]u32{ 0, 0, 0, 0 };
    const s = statsTyped(arena, i32, &vals, &dls, 1) orelse return error.MissingStats;
    try testing.expectEqual(@as(?i64, 4), s.null_count);
    try testing.expectEqual(@as(?[]const u8, null), s.min_value);
    try testing.expectEqual(@as(?[]const u8, null), s.max_value);
}

test "encodeDecimalFlba refuses values exceeding declared precision" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const elem = schema.SchemaElement{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 3,
        .repetition_type = .REQUIRED,
        .name = "d5",
        .num_children = 0,
        .converted_type = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 5 } },
        .scale = 2,
        .precision = 5,
        .field_id = null,
    };

    // 100000.00 is 10^7 unscaled, which requires 6 digits (bound is 10^5 = 100000)
    const bad_values = [_]i128{10_000_000};
    const col_bad: filter_eval.ColumnT(i128) = .{ .values = &bad_values };
    const err = encodeDecimalFlba(arena, col_bad, &elem, &.{}, .UNCOMPRESSED, .{});
    try testing.expectError(error.DecimalValueExceedsPrecision, err);

    // Negative overflow: -100000.00 (-10^7)
    const neg_bad_values = [_]i128{-10_000_000};
    const col_neg_bad: filter_eval.ColumnT(i128) = .{ .values = &neg_bad_values };
    const neg_err = encodeDecimalFlba(arena, col_neg_bad, &elem, &.{}, .UNCOMPRESSED, .{});
    try testing.expectError(error.DecimalValueExceedsPrecision, neg_err);

    // Within bounds: 999.99 (99999 unscaled)
    const ok_values = [_]i128{ 99_999, -99_999 };
    const col_ok: filter_eval.ColumnT(i128) = .{ .values = &ok_values };
    const ok_res = try encodeDecimalFlba(arena, col_ok, &elem, &.{}, .UNCOMPRESSED, .{});
    try testing.expect(ok_res.bytes.len > 0);
}

test "statsTypedFloat f64 all-null page emits null_count only" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const vals = [_]f64{ 0.0, 0.0, 0.0 };
    const dls = [_]u32{ 0, 0, 0 };
    const s = statsTypedFloat(arena, f64, &vals, &dls, 1) orelse return error.MissingStats;
    try testing.expectEqual(@as(?i64, 3), s.null_count);
    try testing.expectEqual(@as(?[]const u8, null), s.min_value);
    try testing.expectEqual(@as(?[]const u8, null), s.max_value);
}

test "statsBytes all-null page emits null_count only" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const vals = [_][]const u8{ "", "", "" };
    const dls = [_]u32{ 0, 0, 0 };
    const s = statsBytes(arena, &vals, &dls, 1, .bytes) orelse return error.MissingStats;
    try testing.expectEqual(@as(?i64, 3), s.null_count);
    try testing.expectEqual(@as(?[]const u8, null), s.min_value);
    try testing.expectEqual(@as(?[]const u8, null), s.max_value);
}

test "statsBool all-null page emits null_count only" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    const vals = [_]bool{ false, false, false };
    const dls = [_]u32{ 0, 0, 0 };
    const s = statsBool(arena, &vals, &dls, 1) orelse return error.MissingStats;
    try testing.expectEqual(@as(?i64, 3), s.null_count);
    try testing.expectEqual(@as(?[]const u8, null), s.min_value);
    try testing.expectEqual(@as(?[]const u8, null), s.max_value);
}

test "Statistics with only null_count round-trips through thrift" {
    // Confirms the all-null wire bytes are valid thrift — strict
    // readers (pyarrow, duckdb, parquet-mr) parse a Statistics with
    // null_count set and min/max unset without complaint.
    const stats: schema.Statistics = .{ .null_count = 42 };
    var w = thrift.Writer.init(testing.allocator);
    defer w.deinit();
    try stats.write(&w);

    var r = thrift.Reader.init(w.bytes());
    const round = try schema.Statistics.read(&r);
    try testing.expectEqual(@as(?i64, 42), round.null_count);
    try testing.expectEqual(@as(?[]const u8, null), round.min_value);
    try testing.expectEqual(@as(?[]const u8, null), round.max_value);
}

const decimal_mod_test = @import("../parquet/decimal.zig");

test "encodeDecimalFlba round-trips i128 through decodeColumnAsI128 (DECIMAL(38,8))" {
    var arena_s = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_s.deinit();
    const arena = arena_s.allocator();

    // DECIMAL(38,8) backed by FLBA(16): values f64 cannot represent.
    const elem = schema.SchemaElement{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 16,
        .repetition_type = .REQUIRED,
        .name = "cost",
        .num_children = 0,
        .converted_type = null,
        .logical_type = .{ .DECIMAL = .{ .scale = 8, .precision = 38 } },
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const vals = [_]i128{
        1_00000001,
        1234567890123456789012345678, // ~28 digits
        -5555555555555555555500000001,
        std.math.maxInt(i128) >> 4,
    };
    const col = filter_eval.ColumnT(i128){ .values = &vals };
    const enc = try encodeDecimalFlba(arena, col, &elem, &[_][]const u8{"cost"}, .UNCOMPRESSED, .{});
    try testing.expectEqual(schema.Type.FIXED_LEN_BYTE_ARRAY, enc.meta.type);

    const kind = decimal_mod_test.kindFromSchema(&elem).?;
    const levels = schema.Levels{ .max_def = 0, .max_rep = 0 };
    const back = try decimal_mod_test.decodeColumnAsI128(arena, enc.bytes, .UNCOMPRESSED, levels, vals.len, kind);
    for (vals, 0..) |v, i| try testing.expectEqual(v, back.values[i]);
}

test "a dictionary chunk's statistics, taken over its entries, equal those of a scan over every row" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const n = 400;
    const strs = try a.alloc([]const u8, n);
    const ints = try a.alloc(i64, n);
    const floats = try a.alloc(f64, n);
    const defs = try a.alloc(u32, n);
    const words = [_][]const u8{ "pear", "apple", "zebra", "", "mango" };
    for (0..n) |i| {
        defs[i] = @intFromBool(i % 7 != 3);
        strs[i] = words[(i * 3) % words.len];
        ints[i] = @as(i64, @intCast((i * 5) % 9)) - 4;
        floats[i] = if (i % 11 == 0) -0.0 else @as(f64, @floatFromInt((i * 7) % 13)) / 4.0;
    }
    const opt = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "s",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var opt_i = opt;
    opt_i.type = .INT64;
    var opt_f = opt;
    opt_f.type = .DOUBLE;
    const Case = struct { col: filter_eval.Batch.Column, elem: *const schema.SchemaElement };
    for ([_]Case{
        .{ .col = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1 } }, .elem = &opt },
        .{ .col = .{ .i64 = .{ .values = ints, .def_levels = defs, .max_def = 1 } }, .elem = &opt_i },
        .{ .col = .{ .f64 = .{ .values = floats, .def_levels = defs, .max_def = 1 } }, .elem = &opt_f },
    }) |c| {
        const enc = try encodeColumn(a, .{ .values = c.col, .schema_elem = c.elem, .path_in_schema = &.{"s"} });
        try testing.expect(enc.meta.dictionary_page_offset != null);
        const want = computeStats(a, c.col, defs, 1, c.elem).?;
        const got = enc.meta.statistics.?;
        try testing.expectEqual(want.null_count, got.null_count);
        try testing.expectEqualSlices(u8, want.min_value.?, got.min_value.?);
        try testing.expectEqualSlices(u8, want.max_value.?, got.max_value.?);
    }
}

test "a coded column encodes to the same bytes as its values, over and past the dictionary cap" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "s",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    // A source dictionary listed in another order than rows first use it, with an entry no row uses.
    const dict = [_][]const u8{ "unused", "kiwi", "fig", "lime", "date" };
    for ([_]usize{ 4, 400 }) |distinct_rows| {
        const n = 1000;
        const codes = try a.alloc(u32, n);
        const defs = try a.alloc(u32, n);
        const vals = try a.alloc([]const u8, n);
        for (0..n) |i| {
            defs[i] = @intFromBool(i % 9 != 5);
            codes[i] = if (defs[i] == 1) @intCast(1 + (i * 7) % 4) else 0;
            vals[i] = if (defs[i] == 1) dict[codes[i]] else "";
        }
        // Past the cap the column is one whose dictionary holds `distinct_rows` entries.
        const big_dict = try a.alloc([]const u8, distinct_rows);
        for (big_dict, 0..) |*d, k| d.* = try std.fmt.allocPrint(a, "v{d}", .{k});
        const big_codes = try a.alloc(u32, n);
        for (big_codes, 0..) |*c, i| c.* = @intCast(i % distinct_rows);
        const big_vals = try a.alloc([]const u8, n);
        for (big_vals, defs, big_codes) |*x, d, c| x.* = if (d == 1) big_dict[c] else "";
        const Pair = struct { coded: filter_eval.Batch.Column, plain: filter_eval.Batch.Column };
        for ([_]Pair{
            .{
                .coded = .{ .string = .{
                    .values = &.{},
                    .codes = codes,
                    .dict = &dict,
                    .def_levels = defs,
                    .max_def = 1,
                } },
                .plain = .{ .string = .{ .values = vals, .def_levels = defs, .max_def = 1 } },
            },
            .{
                .coded = .{ .string = .{
                    .values = &.{},
                    .codes = big_codes,
                    .dict = big_dict,
                    .def_levels = defs,
                    .max_def = 1,
                } },
                .plain = .{ .string = .{ .values = big_vals, .def_levels = defs, .max_def = 1 } },
            },
        }) |pair| {
            const path: []const []const u8 = &.{"s"};
            const want = try encodeColumn(a, .{ .values = pair.plain, .schema_elem = &elem, .path_in_schema = path });
            const got = try encodeColumn(a, .{ .values = pair.coded, .schema_elem = &elem, .path_in_schema = path });
            try testing.expectEqualSlices(u8, want.bytes, got.bytes);
        }
    }
}

test "string statistics equal a byte-order fold: shared prefixes, empty strings, zero bytes, nulls" {
    var prng = std.Random.DefaultPrng.init(0x57a7);
    const r = prng.random();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for (0..3000) |_| {
        _ = arena_state.reset(.retain_capacity);
        const n = 1 + r.uintLessThan(usize, 60);
        const vals = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        const nullable = r.boolean();
        for (vals, defs) |*v, *d| {
            const len = r.uintLessThan(usize, 24);
            const buf = try a.alloc(u8, len);
            const prefix = r.uintLessThan(usize, 20);
            for (buf, 0..) |*c, i| c.* = if (i < prefix) 'p' else switch (r.uintLessThan(u8, 4)) {
                0 => 0,
                1 => 0xff,
                else => r.int(u8),
            };
            v.* = buf;
            d.* = if (nullable and r.uintLessThan(u8, 4) == 0) 0 else 1;
        }
        const st = statsBytes(a, vals, if (nullable) defs else null, 1, .bytes) orelse return error.TestUnexpectedResult;
        var lo: ?[]const u8 = null;
        var hi: ?[]const u8 = null;
        for (vals, defs) |v, d| {
            if (nullable and d == 0) continue;
            if (lo == null or std.mem.lessThan(u8, v, lo.?)) lo = v;
            if (hi == null or std.mem.lessThan(u8, hi.?, v)) hi = v;
        }
        if (lo) |l| {
            try testing.expectEqualSlices(u8, l, st.min_value.?);
            try testing.expectEqualSlices(u8, hi.?, st.max_value.?);
        } else try testing.expect(st.min_value == null);
    }
}

test "commonPrefix agrees with a byte loop at every length and mismatch position" {
    var prng = std.Random.DefaultPrng.init(0xc0);
    const r = prng.random();
    var a: [64]u8 = undefined;
    var b: [64]u8 = undefined;
    for (0..20000) |_| {
        const la = r.uintAtMost(usize, 40);
        const lb = r.uintAtMost(usize, 40);
        r.bytes(a[0..la]);
        const common = r.uintAtMost(usize, @min(la, lb));
        @memcpy(b[0..common], a[0..common]);
        r.bytes(b[common..lb]);
        var want: usize = 0;
        while (want < @min(la, lb) and a[want] == b[want]) want += 1;
        try testing.expectEqual(want, commonPrefix(a[0..la], b[0..lb]));
    }
}

test "dense boolean PLAIN packs as the bit loop does" {
    var prng = std.Random.DefaultPrng.init(0xb0);
    const r = prng.random();
    for (0..500) |_| {
        const n = r.uintAtMost(usize, 100);
        const vals = try testing.allocator.alloc(bool, n);
        defer testing.allocator.free(vals);
        for (vals) |*v| v.* = r.boolean();
        const got = try encodePlainBool(testing.allocator, vals, null, 0);
        defer testing.allocator.free(got);
        for (vals, 0..) |v, i| try testing.expectEqual(v, (got[i / 8] >> @intCast(i % 8)) & 1 == 1);
        if (n % 8 != 0) try testing.expectEqual(@as(u8, 0), got[n / 8] >> @intCast(n % 8));
    }
}

test "dictionary keys chosen to collide in the interner's hash: bounded probes, the same column" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // i * inverse(multiplier) hashes every key to slot 0: one chain, quadratic without a bound.
    const inverse: u64 = 0xF1DE83E19937733D;
    var lookup = try Interner(u64).init(a, 1000);
    var gave_up = false;
    for (0..1000) |i| {
        const key = @as(u64, i) *% inverse;
        try testing.expectEqual(@as(u64, 0), (key *% 0x9E3779B97F4A7C15) >> lookup.shift);
        const slot = lookup.find(key) orelse {
            gave_up = true;
            break;
        };
        slot.* = @intCast(i + 1);
    }
    try testing.expect(gave_up);

    // A column of those keys repeated: interned by the general map, entries in first-occurrence order.
    const values = try a.alloc(i64, 20_000);
    for (values, 0..) |*v, i| v.* = @bitCast(@as(u64, i % 1000) *% inverse);
    var dict: std.ArrayList(u64) = .empty;
    try dict.ensureTotalCapacity(a, values.len / 10);
    const indices = try a.alloc(u32, values.len);
    try testing.expect(try internColumn(i64, a, values, null, 0, values.len / 10, &dict, indices));
    try testing.expectEqual(@as(usize, 1000), dict.items.len);
    for (dict.items, 0..) |k, i| try testing.expectEqual(@as(u64, i) *% inverse, k);
    for (indices, 0..) |x, i| try testing.expectEqual(@as(u32, @intCast(i % 1000)), x);
    // Too many distinct keys still declines, on either path.
    try testing.expect(!try internColumn(i64, a, values, null, 0, 999, &dict, indices));
}

test "string columns: dictionary within its limits, DELTA_BYTE_ARRAY where prefixes pay, else DELTA_LENGTH_BYTE_ARRAY" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const path: []const []const u8 = &.{"s"};
    const n = 4000;
    var prng = std.Random.DefaultPrng.init(0x5715);
    const r = prng.random();
    const Form = enum { dictionary, delta_byte_array, delta_length_byte_array, plain };
    const Case = struct { name: []const u8, want: Form };
    for ([_]Case{
        // 15% distinct: past the old one-in-ten cap, within a quarter.
        .{ .name = "repeating", .want = .dictionary },
        // Every value distinct and unrelated: DELTA_LENGTH_BYTE_ARRAY.
        .{ .name = "random", .want = .delta_length_byte_array },
        // Every value distinct, sorted with long shared prefixes: DELTA_BYTE_ARRAY.
        .{ .name = "sorted", .want = .delta_byte_array },
        // 100 distinct values of 16 KiB: repeat often, but the dictionary would pass 1 MiB.
        .{ .name = "large", .want = .delta_length_byte_array },
    }) |case| {
        const vals = try a.alloc([]const u8, n);
        const big = try a.alloc(u8, 100 * (16 << 10));
        r.bytes(big);
        for (vals, 0..) |*v, i| v.* = if (std.mem.eql(u8, case.name, "repeating"))
            try std.fmt.allocPrint(a, "k{d}", .{(i * 7919) % 600})
        else if (std.mem.eql(u8, case.name, "random")) blk: {
            const b = try a.alloc(u8, 16);
            r.bytes(b);
            break :blk b;
        } else if (std.mem.eql(u8, case.name, "sorted"))
            try std.fmt.allocPrint(a, "s3://bucket/some/long/prefix/part-{d:0>8}", .{i})
        else
            big[(i % 100) * (16 << 10) ..][0 .. 16 << 10];
        const enc = try encodeColumn(a, .{ .values = .{ .string = .{ .values = vals } }, .schema_elem = &elem, .path_in_schema = path });
        const got: Form = if (enc.meta.dictionary_page_offset != null)
            .dictionary
        else if (std.mem.indexOfScalar(schema.Encoding, enc.meta.encodings.items, .DELTA_BYTE_ARRAY) != null)
            .delta_byte_array
        else if (std.mem.indexOfScalar(schema.Encoding, enc.meta.encodings.items, .DELTA_LENGTH_BYTE_ARRAY) != null)
            .delta_length_byte_array
        else
            .plain;
        try testing.expectEqual(case.want, got);
    }
}

test "a coded string column past the dictionary's byte limit encodes to the same bytes as its values" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const path: []const []const u8 = &.{"s"};
    // 100 entries of 12 KiB: 1.2 MiB of dictionary, crossing the limit part-way through the first pass.
    const dict = try a.alloc([]const u8, 100);
    for (dict, 0..) |*d, k| {
        const b = try a.alloc(u8, 12 << 10);
        @memset(b, @intCast(k));
        d.* = b;
    }
    const n = 2000;
    const codes = try a.alloc(u32, n);
    const vals = try a.alloc([]const u8, n);
    for (codes, vals, 0..) |*c, *v, i| {
        c.* = @intCast((i * 37) % 100);
        v.* = dict[c.*];
    }
    const want = try encodeColumn(a, .{ .values = .{ .string = .{ .values = vals } }, .schema_elem = &elem, .path_in_schema = path });
    const got = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &.{}, .codes = codes, .dict = dict } },
        .schema_elem = &elem,
        .path_in_schema = path,
    });
    try testing.expectEqualSlices(u8, want.bytes, got.bytes);
    try testing.expect(want.meta.dictionary_page_offset == null);
}
test "the string prefix choice is inclusive at a quarter, and skips nulls" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const defs = [_]u32{ 1, 0, 1 };
    for ([_]struct { last: []const u8, want: schema.Encoding }{
        .{ .last = "aXYZ", .want = .DELTA_LENGTH_BYTE_ARRAY },
        .{ .last = "abXY", .want = .DELTA_BYTE_ARRAY },
        .{ .last = "abcX", .want = .DELTA_BYTE_ARRAY },
    }) |case| {
        const vals = [_][]const u8{ "abcd", "a-null-is-not-the-previous-value", case.last };
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = &vals, .def_levels = &defs, .max_def = 1 } };
        const got = (try encodeValuesStrings(a, col, &defs, 1, false)).?;
        try testing.expectEqual(case.want, got.encoding);
        const dba = @import("../parquet/encoding/delta_byte_array.zig");
        var out: [2][]const u8 = undefined;
        const n = if (got.encoding == .DELTA_BYTE_ARRAY) blk: {
            var decoder = try dba.DeltaByteArrayDecoder.init(got.bytes, a, 2, std.math.maxInt(usize));
            break :blk try decoder.decode(&out);
        } else blk: {
            var decoder = try dba.DeltaLengthByteArrayDecoder.init(got.bytes, a, 2);
            break :blk try decoder.decode(&out);
        };
        try testing.expectEqual(@as(usize, 2), n);
        try testing.expectEqualStrings("abcd", out[0]);
        try testing.expectEqualStrings(case.last, out[1]);
    }
}

test "the string dictionary limits admit exactly a quarter distinct and exactly 1 MiB" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const elem = schema.SchemaElement{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const path: []const []const u8 = &.{"s"};
    // Sixteen entries * (65532 bytes + four PLAIN length bytes) = exactly 1 MiB.
    for ([_]usize{ 65531, 65532, 65533 }) |last_len| {
        var dict: [16][]const u8 = undefined;
        for (&dict, 0..) |*d, i| {
            const bytes = try a.alloc(u8, if (i == 15) last_len else 65532);
            @memset(bytes, @intCast(i));
            d.* = bytes;
        }
        var vals: [64][]const u8 = undefined;
        var codes: [64]u32 = undefined;
        for (&vals, &codes, 0..) |*v, *c, i| {
            c.* = @intCast(i % 16);
            v.* = dict[c.*];
        }
        const hashed = try encodeColumn(a, .{
            .values = .{ .string = .{ .values = &vals } },
            .schema_elem = &elem,
            .path_in_schema = path,
        });
        const coded = try encodeColumn(a, .{
            .values = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } },
            .schema_elem = &elem,
            .path_in_schema = path,
        });
        try testing.expectEqual(last_len <= 65532, hashed.meta.dictionary_page_offset != null);
        try testing.expectEqualSlices(u8, hashed.bytes, coded.bytes);
    }
    var dict: [17][]const u8 = undefined;
    for (&dict, 0..) |*d, i| d.* = try a.dupe(u8, &.{@intCast(i)});
    for ([_]usize{ 16, 17 }) |k| {
        var vals: [64][]const u8 = undefined;
        var codes: [64]u32 = undefined;
        for (&vals, &codes, 0..) |*v, *c, i| {
            c.* = @intCast(i % k);
            v.* = dict[c.*];
        }
        const hashed = try encodeColumn(a, .{
            .values = .{ .string = .{ .values = &vals } },
            .schema_elem = &elem,
            .path_in_schema = path,
        });
        const coded = try encodeColumn(a, .{
            .values = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } },
            .schema_elem = &elem,
            .path_in_schema = path,
        });
        try testing.expectEqual(k == 16, hashed.meta.dictionary_page_offset != null);
        try testing.expectEqualSlices(u8, hashed.bytes, coded.bytes);
    }
}

const TestPage = struct { header: schema.PageHeader, payload: []const u8 };

/// The pages of an UNCOMPRESSED chunk, header and payload each.
fn testPages(a: std.mem.Allocator, bytes: []const u8) ![]const TestPage {
    var pages: std.ArrayList(TestPage) = .empty;
    var off: usize = 0;
    while (off < bytes.len) {
        var r = thrift.Reader.init(bytes[off..]);
        const header = try schema.PageHeader.read(&r);
        const len: usize = @intCast(header.compressed_page_size);
        try pages.append(a, .{ .header = header, .payload = bytes[off + r.pos ..][0..len] });
        off += r.pos + len;
    }
    return pages.items;
}

fn flbaElem(width: i32, repetition: schema.FieldRepetitionType) schema.SchemaElement {
    return .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = width,
        .repetition_type = repetition,
        .name = "u",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
}

test "a FIXED_LEN_BYTE_ARRAY column is written in FLBA's PLAIN form (no lengths), never as DELTA_LENGTH_BYTE_ARRAY" {
    const consumer = @import("../consumer.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const path: []const []const u8 = &.{"u"};
    const required = flbaElem(16, .REQUIRED);
    var prng = std.Random.DefaultPrng.init(7);
    const rand = prng.random();

    // Distinct values sharing no prefix: no dictionary, no DELTA_BYTE_ARRAY, so PLAIN: the values back to back.
    var unrelated: [200][]const u8 = undefined;
    for (&unrelated) |*v| {
        const b = try a.alloc(u8, 16);
        rand.bytes(b);
        v.* = b;
    }
    const plain = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &unrelated } },
        .schema_elem = &required,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
    });
    const plain_pages = try testPages(a, plain.bytes);
    try testing.expectEqual(1, plain_pages.len);
    try testing.expectEqual(schema.Encoding.PLAIN, plain_pages[0].header.data_page_header.?.encoding);
    try testing.expectEqualSlices(u8, try std.mem.concat(a, u8, &unrelated), plain_pages[0].payload);
    try testing.expect(std.mem.indexOfScalar(schema.Encoding, plain.meta.encodings.items, .DELTA_LENGTH_BYTE_ARRAY) ==
        null);
    const flat: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
    const plain_back = try consumer.decodeFlbaColumn(a, plain.bytes, .UNCOMPRESSED, flat, 200, 16);
    for (unrelated, plain_back.values) |want, got| try testing.expectEqualSlices(u8, want, got);

    // Nullable: the page ends with the present values alone, after the definition levels.
    var defs: [200]u32 = undefined;
    var present: std.ArrayList([]const u8) = .empty;
    for (&defs, unrelated) |*d, v| {
        d.* = @intFromBool(rand.boolean());
        if (d.* == 1) try present.append(a, v);
    }
    const optional = flbaElem(16, .OPTIONAL);
    const nullable = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &unrelated, .def_levels = &defs, .max_def = 1 } },
        .schema_elem = &optional,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
    });
    const nullable_pages = try testPages(a, nullable.bytes);
    try testing.expectEqual(schema.Encoding.PLAIN, nullable_pages[0].header.data_page_header.?.encoding);
    try testing.expect(std.mem.endsWith(u8, nullable_pages[0].payload, try std.mem.concat(a, u8, present.items)));

    // Three distinct values: the dictionary page holds the bare entries, 16 bytes each, in first-occurrence order; a
    // coded column (its source dictionary holding an entry no row uses) writes the same bytes.
    const entries = [_][]const u8{ unrelated[5], unrelated[9], unrelated[2], unrelated[7] };
    var repeated: [200][]const u8 = undefined;
    var codes: [200]u32 = undefined;
    for (&repeated, &codes, 0..) |*v, *c, i| {
        c.* = @intCast(i % 3);
        v.* = entries[c.*];
    }
    const dict = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &repeated } },
        .schema_elem = &required,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
    });
    const dict_pages = try testPages(a, dict.bytes);
    try testing.expectEqual(schema.PageType.DICTIONARY_PAGE, dict_pages[0].header.type);
    try testing.expectEqualSlices(u8, try std.mem.concat(a, u8, entries[0..3]), dict_pages[0].payload);
    const dict_back = try consumer.decodeFlbaColumn(a, dict.bytes, .UNCOMPRESSED, flat, 200, 16);
    for (repeated, dict_back.values) |want, got| try testing.expectEqualSlices(u8, want, got);
    const coded = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &entries } },
        .schema_elem = &required,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
    });
    try testing.expectEqualSlices(u8, dict.bytes, coded.bytes);

    // Shared prefixes keep DELTA_BYTE_ARRAY, which the format defines for FIXED_LEN_BYTE_ARRAY too.
    var prefixed: [200][]const u8 = undefined;
    for (&prefixed, 0..) |*v, i| {
        const b = try a.alloc(u8, 16);
        @memset(b[0..12], 0);
        std.mem.writeInt(u32, b[12..16], @intCast(i), .big);
        v.* = b;
    }
    const delta = try encodeColumn(a, .{
        .values = .{ .string = .{ .values = &prefixed } },
        .schema_elem = &required,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
    });
    const delta_page = (try testPages(a, delta.bytes))[0];
    try testing.expectEqual(schema.Encoding.DELTA_BYTE_ARRAY, delta_page.header.data_page_header.?.encoding);

    // A value of another width is refused rather than shifting every value after it, by both paths.
    var bad = unrelated;
    bad[100] = unrelated[100][0..15];
    try testing.expectError(error.UnsupportedType, encodeColumn(a, .{
        .values = .{ .string = .{ .values = &bad } },
        .schema_elem = &required,
        .path_in_schema = path,
    }));
    var bad_entries = entries;
    bad_entries[1] = entries[1][0..15];
    try testing.expectError(error.UnsupportedType, encodeColumn(a, .{
        .values = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &bad_entries } },
        .schema_elem = &required,
        .path_in_schema = path,
    }));
}

fn expectFloatBounds(comptime T: type, stats: ?schema.Statistics, lo: ?T, hi: ?T, nulls: i64, nans: i64) !void {
    const Bits = @Int(.unsigned, @bitSizeOf(T));
    const st = stats.?;
    try testing.expectEqual(nulls, st.null_count.?);
    // Always written, zero included: a missing count means NaN may be present.
    try testing.expectEqual(@as(?i64, nans), st.nan_count);
    if (lo == null) {
        try testing.expect(st.min == null and st.max == null and st.min_value == null and st.max_value == null);
        return;
    }
    // Compared as bits, so -0.0 and +0.0 differ.
    try testing.expectEqual(@as(Bits, @bitCast(lo.?)), std.mem.readInt(Bits, st.min_value.?[0..@sizeOf(T)], .little));
    try testing.expectEqual(@as(Bits, @bitCast(hi.?)), std.mem.readInt(Bits, st.max_value.?[0..@sizeOf(T)], .little));
    try expectMirroredStats(st);
}

test "float statistics leave NaN out of the bounds, count it, and write a zero min as -0.0 and a zero max as +0.0" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    inline for (.{ f32, f64 }) |T| {
        const nan = std.math.nan(T);
        const inf = std.math.inf(T);
        const Case = struct { vals: []const T, defs: ?[]const u32 = null, lo: ?T, hi: ?T, nulls: i64 = 0, nans: i64 = 0 };
        const cases = [_]Case{
            // A NaN first value no longer becomes both bounds.
            .{ .vals = &.{ nan, 1, -2, 3, nan, 0.5 }, .lo = -2, .hi = 3, .nans = 2 },
            .{ .vals = &.{ 1, nan, -2 }, .lo = -2, .hi = 1, .nans = 1 },
            // Every present value NaN: null_count and nan_count only, as for an all-null chunk.
            .{ .vals = &.{ nan, nan }, .lo = null, .hi = null, .nans = 2 },
            .{ .vals = &.{ 7, nan, 7, nan }, .defs = &.{ 0, 1, 0, 1 }, .lo = null, .hi = null, .nulls = 2, .nans = 2 },
            .{ .vals = &.{ 7, nan, 5, 0 }, .defs = &.{ 0, 1, 1, 0 }, .lo = 5, .hi = 5, .nulls = 2, .nans = 1 },
            // Zero bounds, whichever zero the chunk holds.
            .{ .vals = &.{ 0.0, 1, 2 }, .lo = -0.0, .hi = 2 },
            .{ .vals = &.{ -0.0, -1, -2 }, .lo = -2, .hi = 0.0 },
            .{ .vals = &.{ 0.0, -0.0 }, .lo = -0.0, .hi = 0.0 },
            .{ .vals = &.{ -0.0, nan, 0.0 }, .lo = -0.0, .hi = 0.0, .nans = 1 },
            // Infinities are ordinary bounds.
            .{ .vals = &.{ inf, nan }, .lo = inf, .hi = inf, .nans = 1 },
            .{ .vals = &.{ -inf, nan, inf }, .lo = -inf, .hi = inf, .nans = 1 },
        };
        for (cases) |c| {
            try expectFloatBounds(T, statsTypedFloat(a, T, c.vals, c.defs, 1), c.lo, c.hi, c.nulls, c.nans);
        }

        // The dictionary path takes its bounds over its entries: the same rules, NaN among them.
        const elem = schema.SchemaElement{
            .type = if (T == f32) .FLOAT else .DOUBLE,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "f",
            .num_children = 0,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        var vals: [200]T = undefined;
        for (&vals, 0..) |*v, i| v.* = ([_]T{ nan, 0.0, 2.5 })[i % 3];
        const enc = try encodeColumn(a, .{
            .values = @unionInit(filter_eval.Batch.Column, if (T == f32) "f32" else "f64", .{ .values = &vals }),
            .schema_elem = &elem,
            .path_in_schema = &.{"f"},
        });
        try testing.expect(enc.meta.dictionary_page_offset != null);
        // NaN counted per row, not per entry: one NaN entry, 67 rows of it.
        try expectFloatBounds(T, enc.meta.statistics, -0.0, 2.5, 0, 67);
        // A dictionary without NaN counts none.
        for (&vals, 0..) |*v, i| v.* = ([_]T{ 1.5, 0.0, 2.5 })[i % 3];
        const clean = try encodeColumn(a, .{
            .values = @unionInit(filter_eval.Batch.Column, if (T == f32) "f32" else "f64", .{ .values = &vals }),
            .schema_elem = &elem,
            .path_in_schema = &.{"f"},
        });
        try testing.expect(clean.meta.dictionary_page_offset != null);
        try expectFloatBounds(T, clean.meta.statistics, -0.0, 2.5, 0, 0);
    }
}

/// Each data page's rows and its present values, decoded back, as `PageInfo` describes them.
fn expectPagesDescribed(a: std.mem.Allocator, enc: EncodedColumn, rows: usize) !void {
    const pages = try testPages(a, enc.bytes);
    var data: usize = 0;
    var first: u64 = 0;
    for (pages, 0..) |pg, i| {
        if (pg.header.type == .DICTIONARY_PAGE) {
            try testing.expectEqual(@as(usize, 0), i);
            continue;
        }
        const info = enc.pages[data];
        try testing.expectEqual(first, info.first_row);
        try testing.expectEqual(@as(u64, @intCast(pg.header.data_page_header.?.num_values)), info.rows);
        const header_at = @intFromPtr(pg.payload.ptr) - @intFromPtr(enc.bytes.ptr) - (info.size - pg.payload.len);
        try testing.expectEqual(@as(usize, @intCast(info.offset)), header_at);
        first += info.rows;
        data += 1;
    }
    try testing.expectEqual(enc.pages.len, data);
    try testing.expectEqual(@as(u64, rows), first);
}

test "a chunk is cut into pages at the page target, decodes whole, and pages built in any order are the same" {
    const consumer = @import("../consumer.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0x9a6e);
    const r = prng.random();
    const n = 47_123;
    const defs = try a.alloc(u32, n);
    const ints = try a.alloc(i64, n);
    const low = try a.alloc(i64, n);
    const floats = try a.alloc(f64, n);
    const strs = try a.alloc([]const u8, n);
    for (0..n) |i| {
        defs[i] = @intFromBool(r.uintLessThan(u8, 10) != 0);
        ints[i] = r.int(i64);
        low[i] = r.intRangeAtMost(i64, 0, 50);
        floats[i] = if (i % 997 == 0) std.math.nan(f64) else r.float(f64) * 100;
        const len = r.uintLessThan(usize, 120);
        const s = try a.alloc(u8, len);
        r.bytes(s);
        strs[i] = s;
    }
    const Case = struct { values: filter_eval.Batch.Column, t: schema.Type, want: schema.Encoding };
    const cases = [_]Case{
        .{
            .values = .{ .i64 = .{ .values = ints, .def_levels = defs, .max_def = 1 } },
            .t = .INT64,
            .want = .DELTA_BINARY_PACKED,
        },
        .{
            .values = .{ .i64 = .{ .values = low, .def_levels = defs, .max_def = 1 } },
            .t = .INT64,
            .want = .PLAIN_DICTIONARY,
        },
        .{ .values = .{ .f64 = .{ .values = floats } }, .t = .DOUBLE, .want = .PLAIN },
        .{
            .values = .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1 } },
            .t = .BYTE_ARRAY,
            .want = .DELTA_LENGTH_BYTE_ARRAY,
        },
    };
    for (cases) |case| {
        var elem: schema.SchemaElement = .{
            .type = case.t,
            .type_length = null,
            .repetition_type = .OPTIONAL,
            .name = "c",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };
        const in: ColumnInput = .{
            .values = case.values,
            .schema_elem = &elem,
            .path_in_schema = &.{"c"},
            .codec = .ZSTD,
        };
        const enc = try encodeColumn(a, in);
        const data_stats = enc.meta.encoding_stats.?.getLast();
        try testing.expectEqual(case.want, @as(schema.Encoding, @fromBackingInt(@intCast(data_stats.encoding))));
        try testing.expect(enc.pages.len > 1);
        try expectPagesDescribed(a, enc, n);
        // Pages cut by rows (20,000) or, for the strings, by bytes (1 MiB at about 64 bytes a value).
        for (enc.pages[0 .. enc.pages.len - 1]) |p| {
            if (case.t == .BYTE_ARRAY)
                try testing.expect(p.rows < default_page_rows)
            else
                try testing.expectEqual(@as(u64, default_page_rows), p.rows);
        }
        // Built back to front, the pages are the same bytes.
        const p = try plan(a, in);
        const built = try a.alloc(BuiltPage, p.pageCount());
        var i = built.len;
        while (i > 0) {
            i -= 1;
            built[i] = try p.page(a, i);
        }
        const again = try p.finish(a, built);
        try testing.expectEqualSlices(u8, enc.bytes, again.bytes);
        // Every row reads back, and each page's bounds and counts are those of its rows.
        const levels: schema.Levels = .{ .max_def = 1, .max_rep = 0 };
        for (enc.pages) |pg| {
            var nulls: i64 = 0;
            var nans: i64 = 0;
            for (pg.first_row..pg.first_row + pg.rows) |row| {
                const present = switch (case.values) {
                    inline else => |c| c.def_levels == null or c.def_levels.?[row] == 1,
                };
                if (!present) nulls += 1 else if (case.t == .DOUBLE and std.math.isNan(floats[row])) nans += 1;
            }
            try testing.expectEqual(nulls, pg.null_count);
            if (case.t == .DOUBLE) try testing.expectEqual(nans, pg.nan_count.?);
        }
        switch (case.values) {
            .i64 => |c| {
                const back = try consumer.decodeColumnT(i64, a, enc.bytes, .ZSTD, levels, n);
                for (0..n) |row| if (c.def_levels.?[row] == 1) try testing.expectEqual(c.values[row], back.values[row]);
                for (enc.pages) |pg| {
                    var lo: i64 = std.math.maxInt(i64);
                    var hi: i64 = std.math.minInt(i64);
                    for (pg.first_row..pg.first_row + pg.rows) |row| if (c.def_levels.?[row] == 1) {
                        lo = @min(lo, c.values[row]);
                        hi = @max(hi, c.values[row]);
                    };
                    try testing.expectEqual(lo, std.mem.readInt(i64, pg.min.?[0..8], .little));
                    try testing.expectEqual(hi, std.mem.readInt(i64, pg.max.?[0..8], .little));
                }
            },
            .f64 => |c| {
                const back = try consumer.decodeColumnT(f64, a, enc.bytes, .ZSTD, levels, n);
                for (c.values, back.values) |w, g| {
                    try testing.expectEqual(@as(u64, @bitCast(w)), @as(u64, @bitCast(g)));
                }
            },
            .string => |c| {
                const back = try consumer.decodeColumnT([]const u8, a, enc.bytes, .ZSTD, levels, n);
                for (0..n) |row| if (c.def_levels.?[row] == 1) {
                    try testing.expectEqualSlices(u8, c.values[row], back.values[row]);
                };
            },
            else => unreachable,
        }
    }
}

test "page boundaries come from the values, not the encoding: a coded column has its values' pages" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const x = try a.alloc(u8, 300);
    @memset(x, 'x');
    const y = try a.alloc(u8, 900);
    @memset(y, 'y');
    const dict = [_][]const u8{ x, y, "z" };
    const n = 9_000;
    const codes = try a.alloc(u32, n);
    const vals = try a.alloc([]const u8, n);
    for (codes, vals, 0..) |*c, *v, i| {
        c.* = @intCast((i * 7) % 3);
        v.* = dict[c.*];
    }
    const elem: schema.SchemaElement = .{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const path: []const []const u8 = &.{"s"};
    const coded_col: filter_eval.Batch.Column = .{ .string = .{ .values = &.{}, .codes = codes, .dict = &dict } };
    const coded = try encodeColumn(a, .{ .values = coded_col, .schema_elem = &elem, .path_in_schema = path });
    const plain_col: filter_eval.Batch.Column = .{ .string = .{ .values = vals } };
    const plain = try encodeColumn(a, .{ .values = plain_col, .schema_elem = &elem, .path_in_schema = path });
    try testing.expectEqualSlices(u8, plain.bytes, coded.bytes);
    // About 1.2 KiB of PLAIN bytes per three rows: a 1 MiB page holds about 2,600 rows.
    try testing.expectEqual(@as(usize, 4), coded.pages.len);
    try expectPagesDescribed(a, coded, n);
}

test "a coded column whose dictionary repeats a value encodes to its values' bytes" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const dict = [_][]const u8{ "a", "b", "a", "c" };
    const fdict = [_]f64{ 1.5, -0.0, 1.5, 0.0 };
    var codes: [400]u32 = undefined;
    var vals: [400][]const u8 = undefined;
    var fvals: [400]f64 = undefined;
    for (&codes, &vals, &fvals, 0..) |*c, *v, *f, i| {
        c.* = @intCast((i * 5) % 4);
        v.* = dict[c.*];
        f.* = fdict[c.*];
    }
    var elem: schema.SchemaElement = .{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "s",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    const path: []const []const u8 = &.{"s"};
    const coded_col: filter_eval.Batch.Column = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } };
    const coded = try encodeColumn(a, .{ .values = coded_col, .schema_elem = &elem, .path_in_schema = path });
    const plain_col: filter_eval.Batch.Column = .{ .string = .{ .values = &vals } };
    const plain = try encodeColumn(a, .{ .values = plain_col, .schema_elem = &elem, .path_in_schema = path });
    try testing.expectEqualSlices(u8, plain.bytes, coded.bytes);
    elem.type = .DOUBLE;
    const fcoded_col: filter_eval.Batch.Column = .{ .f64 = .{ .values = &.{}, .codes = &codes, .dict = &fdict } };
    const fcoded = try encodeColumn(a, .{ .values = fcoded_col, .schema_elem = &elem, .path_in_schema = path });
    const fplain_col: filter_eval.Batch.Column = .{ .f64 = .{ .values = &fvals } };
    const fplain = try encodeColumn(a, .{ .values = fplain_col, .schema_elem = &elem, .path_in_schema = path });
    try testing.expectEqualSlices(u8, fplain.bytes, fcoded.bytes);
}

const split_path = [_][]const u8{"c"};

/// `values` under a page target small enough (100 rows, 300 bytes) to cut a test-sized chunk into many pages.
fn splitInput(
    values: filter_eval.Batch.Column,
    elem: *const schema.SchemaElement,
    codec: schema.CompressionCodec,
) ColumnInput {
    return .{
        .values = values,
        .schema_elem = elem,
        .path_in_schema = &split_path,
        .codec = codec,
        .page_rows = 100,
        .page_bytes = 300,
    };
}

/// Every data page of `enc` (after its dictionary page, if any) within `limit`, their value counts summed.
fn expectPagesWithin(a: std.mem.Allocator, enc: EncodedColumn, limit: usize) !usize {
    const start: usize = @intCast(enc.meta.data_page_offset);
    var values: usize = 0;
    const pages = try testPages(a, enc.bytes[start..]);
    for (pages) |p| {
        try testing.expectEqual(schema.PageType.DATA_PAGE, p.header.type);
        const n: usize = @intCast(p.header.data_page_header.?.num_values);
        try testing.expect(n <= limit);
        try testing.expect(@as(usize, @intCast(p.header.uncompressed_page_size)) <= limit);
        try testing.expect(@as(usize, @intCast(p.header.compressed_page_size)) <= limit);
        values += n;
    }
    try testing.expectEqual(enc.pages.len, pages.len);
    var counted: i32 = 0;
    for (enc.meta.encoding_stats.?.items) |st| {
        if (st.page_type == @backingInt(schema.PageType.DATA_PAGE)) counted += st.count;
    }
    try testing.expectEqual(@as(i32, @intCast(pages.len)), counted);
    try testing.expectEqual(@as(i64, @intCast(enc.bytes.len)), enc.meta.total_compressed_size);
    return values;
}

fn ReadBack(comptime T: type) type {
    return struct { values: []T, defs: []u32, reps: []u32 };
}

/// The `n` entries of `enc`'s chunk, read back by the column reader.
fn readBack(comptime T: type, a: std.mem.Allocator, enc: EncodedColumn, levels: schema.Levels, n: usize) !ReadBack(T) {
    const column = @import("../parquet/column.zig");
    var reader = column.ColumnChunkReader(T).init(enc.bytes, enc.meta.codec, levels, a);
    const out: ReadBack(T) = .{ .values = try a.alloc(T, n), .defs = try a.alloc(u32, n), .reps = try a.alloc(u32, n) };
    var got: usize = 0;
    while (got < n) {
        const k = if (levels.max_rep > 0)
            try reader.decodeWithRepLevels(out.values[got..], out.defs[got..], out.reps[got..])
        else if (levels.max_def > 0)
            try reader.decodeWithLevels(out.values[got..], out.defs[got..])
        else
            try reader.decode(out.values[got..]);
        if (k == 0) break;
        got += k;
    }
    try testing.expectEqual(n, got);
    return out;
}

test "a chunk under a small page target splits into pages that read back as the chunk" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const limit = 600; // what the 100-row / 300-byte target leaves a page at most, header and levels included
    const n = 1000;
    var required = flbaElem(0, .REQUIRED);
    var optional = flbaElem(0, .OPTIONAL);

    // INT64, REQUIRED: DELTA_BINARY_PACKED pages.
    {
        required.type = .INT64;
        const vals = try a.alloc(i64, n);
        for (vals, 0..) |*v, i| v.* = @as(i64, @intCast(i)) * 7919 - 100_000;
        const enc = try encodeColumn(a, splitInput(.{ .i64 = .{ .values = vals } }, &required, .UNCOMPRESSED));
        try testing.expect(enc.pages.len > 1);
        try testing.expectEqual(@as(usize, n), try expectPagesWithin(a, enc, limit));
        const back = try readBack(i64, a, enc, .{ .max_def = 0, .max_rep = 0 }, n);
        try testing.expectEqualSlices(i64, vals, back.values);
    }
    // INT64, OPTIONAL, few distinct values: one dictionary page, then index pages.
    {
        optional.type = .INT64;
        const vals = try a.alloc(i64, n);
        const defs = try a.alloc(u32, n);
        for (vals, defs, 0..) |*v, *d, i| {
            d.* = @intFromBool(i % 3 != 0);
            v.* = if (d.* == 1) @intCast(i % 20) else 0;
        }
        const col: filter_eval.Batch.Column = .{ .i64 = .{ .values = vals, .def_levels = defs, .max_def = 1 } };
        const enc = try encodeColumn(a, splitInput(col, &optional, .UNCOMPRESSED));
        try testing.expect(enc.meta.dictionary_page_offset != null and enc.pages.len > 1);
        try testing.expectEqual(@as(usize, n), try expectPagesWithin(a, enc, limit));
        const back = try readBack(i64, a, enc, .{ .max_def = 1, .max_rep = 0 }, n);
        try testing.expectEqualSlices(u32, defs, back.defs);
        for (vals, defs, back.values) |v, d, b| if (d == 1) try testing.expectEqual(v, b);
    }
    // Strings, OPTIONAL, all distinct: cut by bytes as well as rows, snappy-compressed.
    {
        optional.type = .BYTE_ARRAY;
        const vals = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        for (vals, defs, 0..) |*v, *d, i| {
            d.* = @intFromBool(i % 7 != 0);
            v.* = if (d.* == 1) try std.fmt.allocPrint(a, "value-{d}-{d}", .{ i, i * i }) else "";
        }
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = vals, .def_levels = defs, .max_def = 1 } };
        const enc = try encodeColumn(a, splitInput(col, &optional, .SNAPPY));
        try testing.expect(enc.pages.len > n / 100);
        const back = try readBack([]const u8, a, enc, .{ .max_def = 1, .max_rep = 0 }, n);
        try testing.expectEqualSlices(u32, defs, back.defs);
        for (vals, defs, back.values) |v, d, b| if (d == 1) try testing.expectEqualStrings(v, b);
    }
    // BOOLEAN, REQUIRED.
    {
        required.type = .BOOLEAN;
        const vals = try a.alloc(bool, n);
        for (vals, 0..) |*v, i| v.* = (i * 31) % 7 < 3;
        const enc = try encodeColumn(a, splitInput(.{ .boolean = .{ .values = vals } }, &required, .UNCOMPRESSED));
        try testing.expectEqual(@as(usize, n), try expectPagesWithin(a, enc, limit));
        const back = try readBack(bool, a, enc, .{ .max_def = 0, .max_rep = 0 }, n);
        try testing.expectEqualSlices(bool, vals, back.values);
    }
    // DECIMAL(38, 8) over FLBA(16): 16 bytes a value, so about 18 values a page.
    {
        var elem = flbaElem(16, .REQUIRED);
        elem.logical_type = .{ .DECIMAL = .{ .scale = 8, .precision = 38 } };
        const vals = try a.alloc(i128, n);
        for (vals, 0..) |*v, i| v.* = @as(i128, @intCast(i)) * 1_000_000_000_000_000_007 - 3;
        const enc = try encodeColumn(a, splitInput(.{ .i128 = .{ .values = vals } }, &elem, .UNCOMPRESSED));
        try testing.expect(enc.pages.len >= n * 16 / 300);
        try testing.expectEqual(@as(usize, n), try expectPagesWithin(a, enc, limit));
        const kind = decimal_mod_test.kindFromSchema(&elem).?;
        const flat: schema.Levels = .{ .max_def = 0, .max_rep = 0 };
        const back = try decimal_mod_test.decodeColumnAsI128(a, enc.bytes, .UNCOMPRESSED, flat, n, kind);
        try testing.expectEqualSlices(i128, vals, back.values);
    }
}

test "a page no page header holds is TooLarge, and a dictionary page none holds leaves the chunk to its values" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    test_page_limit = 1000;
    defer test_page_limit = std.math.maxInt(i32);
    // One value longer than a page header holds cannot be split.
    var s_elem = flbaElem(0, .REQUIRED);
    s_elem.type = .BYTE_ARRAY;
    const big = try a.alloc(u8, 1500);
    @memset(big, 'x');
    const one = [_][]const u8{big};
    try testing.expectError(error.TooLarge, encodeColumn(a, splitInput(.{ .string = .{ .values = &one } }, &s_elem, .UNCOMPRESSED)));
    // A repeated leaf is one page: past the limit, it is refused, never written wrapped.
    var r_elem = flbaElem(0, .OPTIONAL);
    r_elem.type = .INT64;
    const reps = try a.alloc(u32, 400);
    const defs = try a.alloc(u32, 400);
    const rvals = try a.alloc(i64, 400);
    for (reps, defs, rvals, 0..) |*r, *d, *v, i| {
        r.* = @intFromBool(i % 3 != 0);
        d.* = 2;
        v.* = @intCast(i * 1_000_003);
    }
    const nested: filter_eval.Batch.Column = .{ .i64 = .{
        .values = rvals,
        .def_levels = defs,
        .max_def = 2,
        .rep_levels = reps,
        .max_rep = 1,
    } };
    try testing.expectError(error.TooLarge, encodeColumn(a, splitInput(nested, &r_elem, .UNCOMPRESSED)));
    // 4000 values, 200 distinct: a 1600-byte dictionary, past the limit, so the values are written instead.
    var elem = flbaElem(0, .REQUIRED);
    elem.type = .INT64;
    const vals = try a.alloc(i64, 4000);
    for (vals, 0..) |*v, i| v.* = @intCast((i * 37) % 200);
    const in = splitInput(.{ .i64 = .{ .values = vals } }, &elem, .UNCOMPRESSED);
    test_page_limit = std.math.maxInt(i32);
    const whole = try encodeColumn(a, in);
    try testing.expect(whole.meta.dictionary_page_offset != null);
    test_page_limit = 1000;
    const split = try encodeColumn(a, in);
    try testing.expect(split.meta.dictionary_page_offset == null and split.pages.len > 1);
    try testing.expectEqual(@as(usize, 4000), try expectPagesWithin(a, split, 1000));
    const back = try readBack(i64, a, split, .{ .max_def = 0, .max_rep = 0 }, 4000);
    try testing.expectEqualSlices(i64, vals, back.values);
}

/// `s` `n` times over, at comptime.
fn repeat(comptime s: []const u8, comptime n: usize) *const [s.len * n]u8 {
    comptime {
        var out: [s.len * n]u8 = undefined;
        for (0..n) |k| @memcpy(out[k * s.len ..][0..s.len], s);
        const final = out;
        return &final;
    }
}

test "byte bounds past 64 bytes are cut: a prefix for the min, a grown prefix for the max, none past all 0xFF" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    const long = comptime repeat("a", 100);
    try testing.expectEqualStrings(comptime repeat("a", 64), cutMin(long, .bytes));
    try testing.expectEqualStrings(comptime repeat("a", 63) ++ "b", (try cutMax(a, long, .bytes)).?);
    // Short bounds, and FIXED_LEN_BYTE_ARRAY ones, are kept whole (the very slice).
    try testing.expectEqual(@as([*]const u8, "abc".ptr), (try cutMax(a, "abc", .bytes)).?.ptr);
    try testing.expectEqual(long.len, cutMin(long, .none).len);
    try testing.expectEqual(long.len, (try cutMax(a, long, .none)).?.len);

    // Trailing 0xFF bytes cannot grow: the byte before them does, and they go.
    const ff = comptime repeat("x", 60) ++ "\xfe" ++ repeat("\xff", 10);
    try testing.expectEqualStrings(comptime repeat("x", 60) ++ "\xff", (try cutMax(a, ff, .bytes)).?);
    try testing.expect(try cutMax(a, comptime repeat("\xff", 70), .bytes) == null);

    // UTF-8 is cut at a character boundary and grows by a character of the same width.
    const euro = comptime repeat("€", 30); // 3 bytes each: the boundary below 64 is 63
    try testing.expectEqualStrings(comptime repeat("€", 21), cutMin(euro, .utf8));
    try testing.expectEqualStrings(comptime repeat("€", 20) ++ "\u{20AD}", (try cutMax(a, euro, .utf8)).?);
    // U+FFFF grows to a 4-byte character: it is dropped and the one before it grows instead.
    const top = comptime repeat("a", 61) ++ "\u{FFFF}" ++ repeat("z", 10); // the cut at 64 ends on U+FFFF
    try testing.expectEqualStrings(comptime repeat("a", 60) ++ "b", (try cutMax(a, top, .utf8)).?);
    // Text that is not UTF-8 is cut as bytes.
    const bad = comptime repeat("\xc3", 70);
    try testing.expectEqualStrings(comptime repeat("\xc3", 64), cutMin(bad, .utf8));

    // Every cut bound lies on the right side of the value.
    for ([_][]const u8{ long, ff, euro, top, bad }) |v| for ([_]BoundCut{ .bytes, .utf8 }) |c| {
        try testing.expect(!std.mem.lessThan(u8, v, cutMin(v, c)));
        if (try cutMax(a, v, c)) |m| try testing.expect(!std.mem.lessThan(u8, m, v));
    };
}

test "string statistics say whether their bounds were cut; FIXED_LEN_BYTE_ARRAY bounds are never cut" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const big = comptime repeat("m", 200);
    var str = flbaElem(0, .REQUIRED);
    str.type = .BYTE_ARRAY;
    str.logical_type = .{ .STRING = .{} };

    const cut = computeStats(a, .{ .string = .{ .values = &.{ big, "zz" ++ big } } }, null, 0, &str).?;
    try testing.expectEqual(@as(usize, 64), cut.min_value.?.len);
    try testing.expect(cut.max_value.?.len <= 64);
    try testing.expectEqual(@as(?bool, false), cut.is_min_value_exact);
    try testing.expectEqual(@as(?bool, false), cut.is_max_value_exact);

    const whole = computeStats(a, .{ .string = .{ .values = &.{ "a", "b" } } }, null, 0, &str).?;
    try testing.expectEqual(@as(?bool, true), whole.is_min_value_exact);
    try testing.expectEqual(@as(?bool, true), whole.is_max_value_exact);

    const fixed = flbaElem(200, .REQUIRED);
    const flba = computeStats(a, .{ .string = .{ .values = &.{ big, big } } }, null, 0, &fixed).?;
    try testing.expectEqual(@as(usize, 200), flba.max_value.?.len);
    try testing.expectEqual(@as(?bool, true), flba.is_min_value_exact);
    try testing.expectEqual(@as(?bool, true), flba.is_max_value_exact);

    // The flags survive the footer.
    var w: thrift.Writer = .init(a);
    try cut.write(&w);
    var r = thrift.Reader.init(w.bytes());
    const back = try schema.Statistics.read(&r);
    try testing.expectEqual(@as(?bool, false), back.is_max_value_exact);
    try testing.expectEqual(@as(?bool, false), back.is_min_value_exact);
}

test "numeric dictionaries admit a fifth of present rows in hashed and coded columns" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    inline for (.{ i32, i64, f32, f64 }) |T| {
        const tag = @typeName(T);
        const physical: schema.Type = if (T == i32) .INT32 else if (T == i64) .INT64 else if (T == f32) .FLOAT else .DOUBLE;
        for ([_]usize{ 150, 200, 201 }) |cardinality| for ([_]bool{ false, true }) |nullable| {
            const n: usize = if (nullable) 1250 else 1000;
            const values = try a.alloc(T, n);
            const codes = try a.alloc(u32, n);
            const defs = try a.alloc(u32, n);
            const dict = try a.alloc(T, cardinality);
            for (dict, 0..) |*v, i| v.* = if (T == f32 or T == f64) @floatFromInt(i) else @intCast(i);
            for (values, codes, defs, 0..) |*v, *k, *d, i| {
                k.* = @intCast(i % cardinality);
                v.* = dict[k.*];
                d.* = @intFromBool(i < 1000);
            }
            const elem: schema.SchemaElement = .{
                .name = "v",
                .type = physical,
                .type_length = null,
                .repetition_type = if (nullable) .OPTIONAL else .REQUIRED,
                .num_children = 0,
                .scale = null,
                .precision = null,
                .field_id = null,
            };
            const Col = filter_eval.ColumnT(T);
            const common: Col = .{
                .values = values,
                .def_levels = if (nullable) defs else null,
                .max_def = @intFromBool(nullable),
            };
            var coded = common;
            coded.values = &.{};
            coded.codes = codes;
            coded.dict = dict;
            const plain = try encodeColumn(a, .{
                .values = @unionInit(filter_eval.Batch.Column, tag, common),
                .schema_elem = &elem,
                .path_in_schema = &.{"v"},
            });
            const got = try encodeColumn(a, .{
                .values = @unionInit(filter_eval.Batch.Column, tag, coded),
                .schema_elem = &elem,
                .path_in_schema = &.{"v"},
            });
            try testing.expectEqual(cardinality <= 200, plain.meta.dictionary_page_offset != null);
            try testing.expectEqualSlices(u8, plain.bytes, got.bytes);
        };
    }
}
