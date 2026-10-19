//! Parquet PLAIN encoder for column chunks.
//!
//! Supports the re-encode path: per-type physical encodings, optional
//! compression, dictionary/delta choices where profitable, and a single
//! data page per output column chunk. The encoder owns the page bytes and
//! the matching ColumnMetaData so callers can assemble a valid footer.
//!
//! Design: the per-page primitive matches DuckDB / Polars: emit a
//! PageHeader thrift, then the encoded data bytes. ColumnMetaData
//! is built side-by-side with the bytes so the caller can drop it
//! into the file footer.

const std = @import("std");
const schema = @import("../schema.zig");
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
    /// Page header thrift + encoded data bytes, ready to concatenate.
    bytes: []u8,
    /// ColumnMetaData for the file footer. data_page_offset is relative
    /// to the start of `bytes`; the caller adds the column's absolute
    /// position in the output file.
    meta: schema.ColumnMetaData,
    /// The chunk's Bloom filter (header and bitset) when `ColumnInput.bloom_fpp` asked for one.
    bloom: []const u8 = &.{},
};

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
};

/// Encode one column chunk: header + (optional def-level prefix) + data + ColumnMetaData.
/// The caller fills in the absolute file offset on data_page_offset
/// after concatenating into the output file.
///
/// For OPTIONAL leaves (max_def == 1), emit a definition-level prefix
/// using RLE/bit-packed-hybrid before the PLAIN values. Two cases:
///
/// 1. Input column has no `def_levels` (REQUIRED column or filter
///    survivors that are guaranteed non-null): emit all-1s def levels
///    (~3 bytes regardless of N), and PLAIN bytes carry every value.
/// 2. Input column has `def_levels` (projected-through nullable
///    column): emit the actual def levels via hybrid_rle.encode, and
///    PLAIN bytes carry only the non-null values (parquet PLAIN
///    encoding for OPTIONAL columns excludes null slots).
pub fn encodeColumn(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    if (in.values == .i128) return encodeDecimalColumn(arena, in);
    var enc = if (in.values.codes() != null) try encodeCodedColumn(arena, in) else try encodeValues(arena, in);
    if (in.bloom_fpp) |fpp| enc.bloom = try bloomFilter(arena, in.values, enc.meta.type, fpp) orelse &.{};
    return enc;
}

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

fn encodeValues(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    const elem = in.schema_elem;
    if (elem.type == null) return error.UnsupportedType;
    const phys = elem.type.?;

    const num_values: i64 = @intCast(valueCount(in.values));
    const is_optional = elem.repetition_type == .OPTIONAL;
    const src_def_levels = sourceDefLevels(in.values);
    const src_max_def = sourceMaxDef(in.values);
    const src_rep_levels = sourceRepLevels(in.values);
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
    // dictionary) AND with at least 64 present values, we emit a two-page
    // chunk (DICTIONARY_PAGE then DATA_PAGE with RLE_DICTIONARY-encoded
    // indices). Otherwise we fall through to the paths below.
    if (in.values == .string and src_max_rep == 0) {
        if (try tryEncodeDictBytes(arena, in, num_values)) |enc| return enc;
    }
    // Dict-encode low-cardinality INT32/INT64 columns. The big win is
    // hourly-rounded timestamp columns (24 unique values per daily
    // shard) where dict crushes DELTA: 1.3 bytes/row vs 4.4.
    // Cardinality cap: num_present / 10.
    //
    // FLOAT/DOUBLE columns get the same treatment for the same reason:
    // rounded cost / percentage data is heavily repeated (1M cents in
    // [0.01, 100.00] have ≤10K unique values). An empirical bench
    // showed BYTE_STREAM_SPLIT is a loss on this shape; dict captures
    // the redundancy properly.
    if (src_max_rep == 0) {
        switch (in.values) {
            .i32 => if (try tryEncodeDictInt(i32, arena, in, num_values)) |enc| return enc,
            .i64 => if (try tryEncodeDictInt(i64, arena, in, num_values)) |enc| return enc,
            .f32 => if (try tryEncodeDictFloat(f32, arena, in, num_values)) |enc| return enc,
            .f64 => if (try tryEncodeDictFloat(f64, arena, in, num_values)) |enc| return enc,
            else => {},
        }
    }

    // 1. Encode values. For i32/i64 columns we use DELTA_BINARY_PACKED
    //    — it dominates PLAIN by 4-8× on sorted/timestamp columns and
    //    is roughly equivalent on random data (per-block overhead is
    //    small and bit-packing adapts to actual delta range). For
    //    f32/f64/string/bool we use PLAIN.
    //
    //    The page-header `encoding` field is set accordingly below.
    const delta_bytes_opt = if (src_max_rep == 0)
        try tryEncodeValuesDelta(arena, in.values, src_def_levels, src_max_def)
    else
        null;
    // Strings the dictionary declined: DELTA_BYTE_ARRAY or DELTA_LENGTH_BYTE_ARRAY (see `encodeValuesStrings`).
    // Only for flat columns.
    const strings_opt = if (delta_bytes_opt == null and src_max_rep == 0)
        try encodeValuesStrings(arena, in.values, src_def_levels, src_max_def, fixed)
    else
        null;
    const values_bytes = if (delta_bytes_opt) |b|
        b
    else if (strings_opt) |sv|
        sv.bytes
    else
        try encodeValuesPlain(arena, in.values, src_def_levels, src_max_def, fixed);
    const values_encoding: schema.Encoding = if (delta_bytes_opt != null)
        .DELTA_BINARY_PACKED
    else if (strings_opt) |sv|
        sv.encoding
    else
        .PLAIN;

    // 2a. Rep-level prefix when the column has nesting (max_rep > 0).
    //     V1 layout puts rep before def.
    const rep_prefix: []const u8 = if (src_max_rep > 0)
        try levelPrefix(arena, src_rep_levels orelse return error.NullableNotSupported, bitWidthFor(src_max_rep))
    else
        &.{};

    // 2b. Def-level prefix when OPTIONAL (or when nested with max_def > 0).
    const need_def_prefix = is_optional or src_max_rep > 0;
    const def_prefix: []const u8 = if (need_def_prefix)
        try defLevelPrefix(arena, src_def_levels, src_max_def, num_values)
    else
        &.{};

    const data_total_len = rep_prefix.len + def_prefix.len + values_bytes.len;

    // 3. Compute stats. Pass `arena` so byte-array min/max strings can
    //    be dupe'd onto a stable arena (the source values may live on
    //    a row-group-scoped arena that's freed before the file footer
    //    is written; borrowing the slice into the footer would dangle).
    var stats = computeStats(arena, in.values, src_def_levels, src_max_def, in.schema_elem);
    _ = &stats;

    // 4a. Concatenate the V1 data-page payload: rep_prefix (if
    //     max_rep > 0), def_prefix (if optional/nested), values.
    //     This is the "uncompressed page" that compressed_page_size
    //     measures against in the spec.
    const payload = try arena.alloc(u8, data_total_len);
    {
        var pos: usize = 0;
        if (rep_prefix.len > 0) {
            @memcpy(payload[pos..][0..rep_prefix.len], rep_prefix);
            pos += rep_prefix.len;
        }
        if (def_prefix.len > 0) {
            @memcpy(payload[pos..][0..def_prefix.len], def_prefix);
            pos += def_prefix.len;
        }
        @memcpy(payload[pos..], values_bytes);
    }

    // 4b. Compress the payload. Page header thrift is NEVER
    //     compressed in Parquet — only the data portion. If the
    //     compressed output is somehow >= original (incompressible
    //     data) we still ship the compressed version; the format
    //     allows it. (zstd pages pick their own mode, raw blocks
    //     among them; see `compression.ZstdPlan`.)
    const compressed = try compression.compressPage(arena, payload, in.codec, in.zstd);

    // 5. Build PageHeader. uncompressed_page_size measures the
    //    payload-as-if-uncompressed; compressed_page_size measures
    //    the on-disk payload we actually write. Header bytes are
    //    counted in neither — only by total_*_size on ColumnMetaData.
    var page_hdr: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(data_total_len),
        .compressed_page_size = @intCast(compressed.len),
        .crc = null,
        .data_page_header = .{
            .num_values = @intCast(num_values),
            .encoding = values_encoding,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try page_hdr.write(&w);
    const header_bytes = w.bytes();

    // 6. Concatenate: page header thrift (uncompressed) || compressed payload.
    const total = try arena.alloc(u8, header_bytes.len + compressed.len);
    @memcpy(total[0..header_bytes.len], header_bytes);
    @memcpy(total[header_bytes.len..], compressed);

    // 7. Build ColumnMetaData. total_uncompressed_size and
    //    total_compressed_size include the page header (parquet spec).
    //    They differ by `data_total_len - compressed.len`.
    //
    //    The encodings list reports every encoding present in the
    //    chunk. We always have RLE (def/rep levels), then either
    //    PLAIN or DELTA_BINARY_PACKED for the values themselves.
    var encodings: schema.EncodingList = .empty;
    try encodings.append(arena, .RLE);
    try encodings.append(arena, values_encoding);

    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, in.path_in_schema);

    const meta: schema.ColumnMetaData = .{
        .type = phys,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = in.codec,
        .num_values = num_values,
        .total_uncompressed_size = @intCast(header_bytes.len + data_total_len),
        .total_compressed_size = @intCast(total.len),
        .data_page_offset = 0, // caller adjusts to absolute offset
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = stats,
        .encoding_stats = try pageStats(arena, &.{.{ .page_type = .DATA_PAGE, .encoding = values_encoding }}),
    };

    return .{ .bytes = total, .meta = meta };
}

/// `encoding_stats` for a chunk of one page per entry.
fn pageStats(
    arena: std.mem.Allocator,
    pages: []const struct { page_type: schema.PageType, encoding: schema.Encoding },
) Error!std.ArrayListUnmanaged(schema.PageEncodingStats) {
    var out: std.ArrayListUnmanaged(schema.PageEncodingStats) = .empty;
    for (pages) |p| try out.append(arena, .{
        .page_type = @backingInt(p.page_type),
        .encoding = @backingInt(p.encoding),
        .count = 1,
    });
    return out;
}

/// PLAIN-encode a FIXED_LEN_BYTE_ARRAY-backed DECIMAL column from its
/// unscaled i128 values (the lossless lane for precision > 18 — D2).
/// Each present value is written as `byte_width` big-endian two's-
/// complement bytes (parquet's decimal-FLBA wire format), matching what
/// `decimal.decodeColumnAsI128` reads back. The DECIMAL annotation lives
/// in the footer `schema_elem`, unchanged. Single data page, PLAIN.
/// An exact DECIMAL column (unscaled i128) at the physical type its schema element declares: INT32/INT64 values are
/// narrowed (an out-of-range value is an error, never a wrap) and take the integer encoders; FLBA writes big-endian.
fn encodeDecimalColumn(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    const c = in.values.i128;
    switch (in.schema_elem.type orelse return error.UnsupportedType) {
        .FIXED_LEN_BYTE_ARRAY => {
            return encodeDecimalFlba(arena, c, in.schema_elem, in.path_in_schema, in.codec, in.zstd);
        },
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
            return encodeColumn(arena, narrowed);
        },
        else => return error.UnsupportedType,
    }
}

pub fn encodeDecimalFlba(
    arena: std.mem.Allocator,
    col: filter_eval.ColumnT(i128),
    schema_elem: *const schema.SchemaElement,
    path_in_schema: []const []const u8,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) Error!EncodedColumn {
    if (schema_elem.type != .FIXED_LEN_BYTE_ARRAY) return error.UnsupportedType;
    const bw: usize = if (schema_elem.type_length) |tl| @intCast(tl) else return error.UnsupportedType;
    if (bw == 0 or bw > 16) return error.UnsupportedType;

    const num_values: i64 = @intCast(col.values.len);
    const is_optional = schema_elem.repetition_type == .OPTIONAL;
    const def_levels = col.def_levels;
    const max_def = col.max_def;
    const num_present = countPresent(col.values.len, def_levels, max_def);

    // Refuse when a value overflows declared precision to avoid emitting wrapped negative values or corrupt stats.
    const dec_kind = decimal_mod.kindFromSchema(schema_elem);
    const precision: ?i32 = if (dec_kind) |k| k.precision else schema_elem.precision;
    if (precision) |p| {
        if (p >= 1 and p <= 38) {
            const bound = decimal_mod.pow10(p);
            if (def_levels == null or max_def == 0) {
                for (col.values) |v| {
                    if (v >= bound or v <= -bound) return error.DecimalValueExceedsPrecision;
                }
            } else {
                for (col.values, 0..) |v, i| {
                    if (isPresent(def_levels, max_def, i)) {
                        if (v >= bound or v <= -bound) return error.DecimalValueExceedsPrecision;
                    }
                }
            }
        }
    }

    // PLAIN values: `bw` big-endian bytes per present value.
    const values_bytes = try arena.alloc(u8, num_present * bw);
    {
        var pos: usize = 0;
        for (col.values, 0..) |v, i| {
            if (!isPresent(def_levels, max_def, i)) continue;
            var be: [16]u8 = undefined;
            std.mem.writeInt(i128, &be, v, .big);
            @memcpy(values_bytes[pos .. pos + bw], be[16 - bw ..]);
            pos += bw;
        }
    }

    const def_prefix: []const u8 = if (is_optional) try defLevelPrefix(arena, def_levels, max_def, num_values) else &.{};

    const data_total_len = def_prefix.len + values_bytes.len;
    const payload = try arena.alloc(u8, data_total_len);
    @memcpy(payload[0..def_prefix.len], def_prefix);
    @memcpy(payload[def_prefix.len..], values_bytes);
    const compressed = try compression.compressPage(arena, payload, codec, zstd);

    // Stats: min/max over present values as big-endian `bw` bytes.
    var stats = schema.Statistics{};
    {
        var have = false;
        var lo: i128 = 0;
        var hi: i128 = 0;
        var nulls: i64 = 0;
        for (col.values, 0..) |v, i| {
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
            const lob = try arena.alloc(u8, bw);
            const hib = try arena.alloc(u8, bw);
            var be: [16]u8 = undefined;
            std.mem.writeInt(i128, &be, lo, .big);
            @memcpy(lob, be[16 - bw ..]);
            std.mem.writeInt(i128, &be, hi, .big);
            @memcpy(hib, be[16 - bw ..]);
            stats.min_value = lob;
            stats.max_value = hib;
        }
        stats.null_count = nulls;
    }

    var page_hdr: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(data_total_len),
        .compressed_page_size = @intCast(compressed.len),
        .crc = null,
        .data_page_header = .{
            .num_values = @intCast(num_values),
            .encoding = .PLAIN,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    };
    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try page_hdr.write(&w);
    const header_bytes = w.bytes();

    const total = try arena.alloc(u8, header_bytes.len + compressed.len);
    @memcpy(total[0..header_bytes.len], header_bytes);
    @memcpy(total[header_bytes.len..], compressed);

    var encodings: schema.EncodingList = .empty;
    try encodings.append(arena, .RLE);
    try encodings.append(arena, .PLAIN);
    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, path_in_schema);

    const meta: schema.ColumnMetaData = .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = codec,
        .num_values = num_values,
        .total_uncompressed_size = @intCast(header_bytes.len + data_total_len),
        .total_compressed_size = @intCast(total.len),
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = stats,
        .encoding_stats = try pageStats(arena, &.{.{ .page_type = .DATA_PAGE, .encoding = .PLAIN }}),
    };
    return .{ .bytes = total, .meta = meta };
}

/// A column carried as codes into its source dictionary (`ColumnT.codes`). The dictionary is rebuilt from the codes
/// alone: each code met among present rows becomes the next entry, in first-occurrence order and under the same
/// limits as the hashing encoders (`stringDictMaxEntries` and `string_dict_max_bytes` for strings), so with a source
/// dictionary of distinct entries the chunk is the one
/// hashing the values would write, without hashing any. Past the cap (or with too few values) the values are
/// gathered and encoded as any other column.
fn encodeCodedColumn(arena: std.mem.Allocator, in: ColumnInput) Error!EncodedColumn {
    switch (in.values) {
        inline else => |c, tag| {
            const T = @TypeOf(c.dict[0]);
            const codes = c.codes.?;
            const n = codes.len;
            const num_present = countPresent(n, c.def_levels, c.max_def);
            if (T != bool and c.max_rep == 0 and num_present >= 64) dict: {
                const is_string = T == []const u8;
                const len_prefix: usize = if (in.schema_elem.type == .FIXED_LEN_BYTE_ARRAY) 0 else 4;
                const max_cardinality = if (is_string) stringDictMaxEntries(num_present) else num_present / 10;
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
                return finishDictionaryColumn(arena, in, @intCast(n), dict_plain, dict_col, indices);
            }
            var vals = c;
            const gathered = try agg_coded.materialize(arena, in.values);
            vals.values = @field(gathered, @tagName(tag)).values;
            vals.codes = null;
            vals.dict = &.{};
            var plain = in;
            plain.values = @unionInit(filter_eval.Batch.Column, @tagName(tag), vals);
            return encodeColumn(arena, plain);
        },
    }
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
fn tryEncodeDictBytes(arena: std.mem.Allocator, in: ColumnInput, num_values: i64) Error!?EncodedColumn {
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
    return try finishDictionaryColumn(arena, in, num_values, dict_plain, dict_col, indices);
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
/// would exceed `num_present / 10` or when there are fewer than 64
/// present values.
fn tryEncodeDictInt(
    comptime T: type,
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
) Error!?EncodedColumn {
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

    const max_cardinality = num_present / 10;
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
    return try finishDictionaryColumn(arena, in, num_values, dict_plain, dict_col, indices);
}

/// FLOAT/DOUBLE dictionary encoder. Mirrors `tryEncodeDictInt` —
/// PLAIN-encoded dict page + RLE-dict data page — but uses the bit-
/// pattern of the float (`@bitCast` to u32/u64) as the hash key so
/// the encoder is bit-stable. Two NaNs with different payloads stay
/// distinct in the dictionary (matches the SQL "distinct value"
/// model where NaN != NaN but each specific NaN bit pattern is its
/// own entry).
///
/// Cardinality cap = num_present / 10 (as the INT path). Returns null
/// when the dictionary would exceed the cap or when there are fewer
/// than 64 present values.
fn tryEncodeDictFloat(
    comptime T: type,
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
) Error!?EncodedColumn {
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

    const max_cardinality = num_present / 10;
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
    return try finishDictionaryColumn(arena, in, num_values, dict_plain, dict_col, indices);
}

/// The two pages every dictionary encoder emits once it has interned its values (the encoders differ only in how
/// they key and PLAIN-encode entries): the compressed `dict_plain` dictionary page holding `dict_count` entries,
/// then a data page of `indices`, one per present value. `dictionary_page_offset` is 0 and `data_page_offset` the
/// dictionary page's length, both relative to the returned bytes; callers add the chunk's absolute start to both.
fn finishDictionaryColumn(
    arena: std.mem.Allocator,
    in: ColumnInput,
    num_values: i64,
    dict_plain: []const u8,
    /// The entries in first-occurrence order, every one present at least once.
    dict: filter_eval.Batch.Column,
    indices: []const u32,
) Error!EncodedColumn {
    const def_levels = sourceDefLevels(in.values);
    const max_def = sourceMaxDef(in.values);
    const dict_count = valueCount(dict);
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

    // Data page payload: def prefix (OPTIONAL only), the index bit width, then the RLE/bit-packed indices.
    const def_prefix: []const u8 = if (in.schema_elem.repetition_type == .OPTIONAL)
        try defLevelPrefix(arena, def_levels, max_def, num_values)
    else
        &.{};
    const idx_bit_width = dictIndexBitWidth(dict_count);
    const idx_rle = try hybrid_rle.encode(arena, indices, idx_bit_width);
    const data_payload = try std.mem.concat(arena, u8, &.{ def_prefix, &.{idx_bit_width}, idx_rle });
    const data_compressed = try compression.compressPage(arena, data_payload, in.codec, in.zstd);
    const data_header = try pageHeaderBytes(arena, .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(data_payload.len),
        .compressed_page_size = @intCast(data_compressed.len),
        .crc = null,
        .data_page_header = .{
            .num_values = @intCast(num_values),
            .encoding = .PLAIN_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
        .data_page_header_v2 = null,
    });

    const total = try std.mem.concat(arena, u8, &.{ dict_header, dict_compressed, data_header, data_compressed });

    var encodings: schema.EncodingList = .empty;
    try encodings.appendSlice(arena, &.{ .RLE, .PLAIN, .PLAIN_DICTIONARY });
    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, in.path_in_schema);

    return .{
        .bytes = total,
        .meta = .{
            .type = in.schema_elem.type.?,
            .encodings = encodings,
            .path_in_schema = path_list,
            .codec = in.codec,
            .num_values = num_values,
            .total_uncompressed_size = @intCast(dict_header.len + dict_plain.len + data_header.len + data_payload.len),
            .total_compressed_size = @intCast(total.len),
            .data_page_offset = @intCast(dict_header.len + dict_compressed.len),
            .dictionary_page_offset = 0,
            .index_page_offset = null,
            .statistics = stats,
            // Every value is in the dictionary: readers may prune row groups on it.
            .encoding_stats = try pageStats(arena, &.{
                .{ .page_type = .DICTIONARY_PAGE, .encoding = .PLAIN },
                .{ .page_type = .DATA_PAGE, .encoding = .PLAIN_DICTIONARY },
            }),
        },
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
    std.mem.writeInt(u32, prefix[0..4], @intCast(rle.len), .little);
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

const StringValues = struct { bytes: []u8, encoding: schema.Encoding };

/// The values of a BYTE_ARRAY column the dictionary declined (present values only), in one of two layouts the
/// decoder reads:
/// - DELTA_BYTE_ARRAY, `DBP(prefix_lengths) ++ DBP(suffix_lengths) ++ concat(suffixes)` (prefix_length[i] the length
///   shared with the previous present value, suffix[i] the rest), where those prefixes cover at least a quarter of
///   the column's bytes: sorted keys, paths;
/// - otherwise DELTA_LENGTH_BYTE_ARRAY, `DBP(lengths) ++ concat(values)`.
/// Both keep the bytes contiguous, apart from their lengths: zstd compresses that faster and smaller than PLAIN's
/// interleaved lengths, and readers decode it faster (DELTA_LENGTH_BYTE_ARRAY fastest of all in Polars, pyarrow and
/// DuckDB; DELTA_BYTE_ARRAY several times slower in Polars, hence the prefix condition). Returns null for non-string
/// columns and empty ones (the caller writes PLAIN), and for a FIXED_LEN_BYTE_ARRAY column (`fixed`) whose prefixes
/// do not pay: DELTA_LENGTH_BYTE_ARRAY is defined for BYTE_ARRAY only, and strict readers reject it on FLBA.
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
    const num_present = countPresent(col.values.len, def_levels, max_def);
    if (num_present == 0) return null;

    const prefix_lengths = try arena.alloc(i32, num_present);
    const suffix_lengths = try arena.alloc(i32, num_present);
    var prev: []const u8 = &.{};
    var k: usize = 0;
    var prefix_total: usize = 0;
    var suffix_total: usize = 0;
    for (col.values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const lcp = commonPrefix(prev, v);
        prefix_lengths[k] = @intCast(lcp);
        suffix_lengths[k] = @intCast(v.len - lcp);
        prefix_total += lcp;
        suffix_total += v.len - lcp;
        prev = v;
        k += 1;
    }
    const prefixes_pay = prefix_total * 4 >= prefix_total + suffix_total;
    if (!prefixes_pay and fixed) return null;
    if (!prefixes_pay) {
        // The lengths, then the values whole.
        for (prefix_lengths, suffix_lengths) |p, *l| l.* += p;
        const len_enc = try delta_binary_packed.encodeDefault(i32, arena, suffix_lengths);
        const out = try arena.alloc(u8, len_enc.len + prefix_total + suffix_total);
        @memcpy(out[0..len_enc.len], len_enc);
        var o = len_enc.len;
        for (col.values, 0..) |v, i| {
            if (!isPresent(def_levels, max_def, i)) continue;
            @memcpy(out[o..][0..v.len], v);
            o += v.len;
        }
        return .{ .bytes = out, .encoding = .DELTA_LENGTH_BYTE_ARRAY };
    }
    const prefix_enc = try delta_binary_packed.encodeDefault(i32, arena, prefix_lengths);
    const suffix_len_enc = try delta_binary_packed.encodeDefault(i32, arena, suffix_lengths);
    const out = try arena.alloc(u8, prefix_enc.len + suffix_len_enc.len + suffix_total);
    @memcpy(out[0..prefix_enc.len], prefix_enc);
    @memcpy(out[prefix_enc.len..][0..suffix_len_enc.len], suffix_len_enc);
    var o = prefix_enc.len + suffix_len_enc.len;
    k = 0;
    for (col.values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const lcp: usize = @intCast(prefix_lengths[k]);
        @memcpy(out[o..][0 .. v.len - lcp], v[lcp..]);
        o += v.len - lcp;
        k += 1;
    }
    return .{ .bytes = out, .encoding = .DELTA_BYTE_ARRAY };
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
        .string => |x| statsBytes(arena, x.values, def_levels, max_def),
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
        .null_count = null_count,
        .nan_count = nan_count,
    };
}

fn statsBytes(arena: std.mem.Allocator, values: []const []const u8, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
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
    const lo_owned = arena.dupe(u8, lo) catch return null;
    const hi_owned = arena.dupe(u8, hi) catch return null;
    // Bytewise unsigned is a byte array's type-defined order. The deprecated min/max are defined as signed, so they
    // are left unset rather than written in an order a legacy reader would misread.
    return .{
        .min_value = lo_owned,
        .max_value = hi_owned,
        .null_count = null_count,
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
        .null_count = null_count,
    };
}

// ============================================================
// Tests — round-trip through metadata.open
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

fn expectMirroredStats(stats: schema.Statistics) !void {
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
    const byte_stats = statsBytes(arena, &bytes, null, 0) orelse return error.MissingStats;
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
    const s = statsBytes(arena, &vals, &dls, 1) orelse return error.MissingStats;
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
        const st = statsBytes(a, vals, if (nullable) defs else null, 1) orelse return error.TestUnexpectedResult;
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
