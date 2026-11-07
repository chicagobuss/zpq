//! Rewriting single pages of a flat column chunk in place, for UPDATE: a page is decoded, the value set at some of
//! its rows, and encoded again in its own encoding and codec, header and statistics included. A page that changes
//! size keeps the offsets after it congruent modulo `fastpath.share_block` through a pad in its header (`finish`):
//! PageHeader field `pad_field_id`, binary, written in the compact protocol's short form just before the STOP byte.
//! Every reader the 0.6.0 reader survey ran skips it; the short form is the one fastparquet's decoder reads.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const encoder = @import("encoder.zig");
const compression = @import("../parquet/compression.zig");
const column_mod = @import("../parquet/column.zig");
const hybrid_rle = @import("../parquet/encoding/hybrid_rle.zig");
const filter_eval = @import("../filter/eval.zig");
const patch = @import("thrift_patch.zig");
const fastpath = @import("fastpath.zig");
const decimal_mod = @import("../parquet/decimal.zig");

/// The PageHeader field the pad is written as: above the 2.14.0 IDL's highest PageHeader field (8), and at most 15
/// above any per-type header field (5, 7, 8), so its tag is one byte.
pub const pad_field_id: i16 = 20;

pub const Error = error{
    /// A page this editor cannot rewrite exactly: its levels, encoding or page type.
    UnsupportedPage,
    /// The page header's last field is too far below `pad_field_id` for a one-byte tag.
    PadFieldUnreachable,
} || encoder.Error || column_mod.Error || error{ShortDecode} || std.mem.Allocator.Error;

/// The value an UPDATE sets, in the column's physical lane.
pub const Value = union(enum) {
    null,
    boolean: bool,
    i32: i32,
    i64: i64,
    f32: f32,
    f64: f64,
    bytes: []const u8,

    /// Its PLAIN bytes, as statistics and Bloom filters encode a value (a byte array without its length).
    pub fn plain(self: Value, buf: *[8]u8) []const u8 {
        return switch (self) {
            .null => &.{},
            .boolean => |b| blk: {
                buf[0] = @intFromBool(b);
                break :blk buf[0..1];
            },
            .bytes => |b| b,
            inline else => |v| blk: {
                const T = @TypeOf(v);
                const U = @Int(.unsigned, @bitSizeOf(T));
                std.mem.writeInt(U, buf[0..@sizeOf(T)], @bitCast(v), .little);
                break :blk buf[0..@sizeOf(T)];
            },
        };
    }
};

/// What the editor knows of the chunk a page belongs to.
pub const Chunk = struct {
    elem: *const schema.SchemaElement,
    levels: schema.Levels,
    codec: schema.CompressionCodec,
    type_length: usize = 0,
};

/// A page of the chunk as it lies in the file.
pub const SourcePage = struct {
    /// Where its header starts in the file.
    offset: u64,
    header_len: usize,
    /// Header plus stored payload.
    total_len: usize,
    header: schema.PageHeader,
    /// For a data page: its first row in the row group, and its rows.
    first_row: u64 = 0,
    rows: u64 = 0,
    /// The page header carries statistics (DataPageHeader field 5, DataPageHeaderV2 field 8).
    has_stats: bool = false,

    pub fn bytes(self: SourcePage, file: []const u8) []const u8 {
        return file[@intCast(self.offset)..][0..self.total_len];
    }

    pub fn payload(self: SourcePage, file: []const u8) []const u8 {
        return self.bytes(file)[self.header_len..];
    }

    pub fn encoding(self: SourcePage) ?schema.Encoding {
        if (self.header.data_page_header) |h| return h.encoding;
        if (self.header.data_page_header_v2) |h| return h.encoding;
        return null;
    }

    pub fn dictionaryEncoded(self: SourcePage) bool {
        const e = self.encoding() orelse return false;
        return e == .PLAIN_DICTIONARY or e == .RLE_DICTIONARY;
    }
};

/// The pages of the chunk at `file[start..][0..len]`, in order, each data page with its rows. A flat column's rows are
/// its values.
pub fn walkPages(arena: std.mem.Allocator, file: []const u8, start: u64, len: u64) ![]SourcePage {
    var out: std.ArrayList(SourcePage) = .empty;
    var pos: u64 = start;
    const end = start + len;
    if (end > file.len) return error.UnsupportedPage;
    var row: u64 = 0;
    while (pos < end) {
        var r = thrift.Reader.init(file[@intCast(pos)..@intCast(end)]);
        const h = schema.PageHeader.read(&r) catch return error.UnsupportedPage;
        if (h.compressed_page_size < 0 or h.uncompressed_page_size < 0) return error.UnsupportedPage;
        const total = r.pos + @as(usize, @intCast(h.compressed_page_size));
        if (pos + total > end) return error.UnsupportedPage;
        var p: SourcePage = .{ .offset = pos, .header_len = r.pos, .total_len = total, .header = h };
        switch (h.type) {
            .DICTIONARY_PAGE => if (out.items.len != 0) return error.UnsupportedPage,
            .DATA_PAGE, .DATA_PAGE_V2 => {
                const n: u64 = if (h.data_page_header) |d|
                    @intCast(@max(d.num_values, 0))
                else if (h.data_page_header_v2) |d| @intCast(@max(d.num_rows, 0)) else return error.UnsupportedPage;
                p.first_row = row;
                p.rows = n;
                row += n;
                p.has_stats = try headerHasStats(arena, file[@intCast(pos)..][0..r.pos], h.type);
            },
            else => return error.UnsupportedPage,
        }
        try out.append(arena, p);
        pos += total;
    }
    return out.items;
}

fn headerHasStats(arena: std.mem.Allocator, header: []const u8, t: schema.PageType) !bool {
    const fields = patch.fieldsOf(arena, header) catch return error.UnsupportedPage;
    const sub_id: i16, const stats_id: i16 = if (t == .DATA_PAGE) .{ 5, 5 } else .{ 8, 8 };
    const sub = patch.find(fields, sub_id) orelse return false;
    const inner = patch.fieldsOf(arena, sub.raw) catch return error.UnsupportedPage;
    return patch.find(inner, stats_id) != null;
}

/// A rewritten page before its pad: header fields (no STOP byte) and the stored payload.
pub const Built = struct {
    head: []const u8,
    /// The id of the header's last field, from which the pad's tag is a delta.
    last_id: i16,
    body: []const u8,
    /// The payload's uncompressed length (the header's uncompressed_page_size).
    uncompressed: usize,
};

/// The rewritten page as it will lie in the file: `built` with a pad sized so the page is `src_len` long when it fits
/// there, else `src_len` plus the fewest whole `fastpath.share_block`s that hold it. It never shrinks: a page that
/// keeps its length moves nothing after it.
pub fn finish(arena: std.mem.Allocator, built: Built, src_len: usize) Error![]u8 {
    const delta = pad_field_id - built.last_id;
    if (delta <= 0 or delta > 15) return error.PadFieldUnreachable;
    const block = fastpath.share_block;
    const bare = built.head.len + 1 + built.body.len;
    // The source's own length when the page fits in it, so nothing after it moves and every later offset, page
    // index entry and Bloom filter stays as it is; else the least length past it congruent modulo the block.
    var target = if (bare <= src_len) src_len else bare + (block + (src_len % block) - (bare % block)) % block;
    while (padPayload(target - bare) == null) target += block;
    const n = padPayload(target - bare).?;
    const out = try arena.alloc(u8, target);
    var w: usize = 0;
    @memcpy(out[w..][0..built.head.len], built.head);
    w += built.head.len;
    if (target != bare) {
        out[w] = @as(u8, @intCast(delta)) << 4 | @backingInt(thrift.Type.Binary);
        w += 1;
        w += writeVarint(out[w..], n);
        @memset(out[w..][0..n], 0);
        w += n;
    }
    out[w] = 0;
    w += 1;
    @memcpy(out[w..][0..built.body.len], built.body);
    w += built.body.len;
    std.debug.assert(w == target);
    return out;
}

/// The binary length whose pad field (one tag byte, the length's varint, the zeros) takes exactly `bytes`; 0 bytes is
/// no field, and lengths a varint boundary skips over (1, 130, ...) have none.
fn padPayload(bytes: usize) ?usize {
    if (bytes == 0) return 0;
    var vl: usize = 1;
    while (vl <= 5) : (vl += 1) {
        if (bytes < 1 + vl) return null;
        const n = bytes - 1 - vl;
        if (varintLen(n) == vl) return n;
    }
    return null;
}

fn varintLen(n: usize) usize {
    var len: usize = 1;
    var x = n >> 7;
    while (x != 0) : (x >>= 7) len += 1;
    return len;
}

fn writeVarint(out: []u8, n: usize) usize {
    var x = n;
    var i: usize = 0;
    while (true) {
        const b: u8 = @truncate(x & 0x7f);
        x >>= 7;
        if (x == 0) {
            out[i] = b;
            return i + 1;
        }
        out[i] = b | 0x80;
        i += 1;
    }
}

/// A chunk's dictionary while its pages are edited: decoded from the first dictionary-encoded page an edit reads,
/// extended by values it lacks.
pub const Dictionary = struct {
    page: ?SourcePage,
    /// The entries, in the column's lane; null until a page decode supplies them.
    entries: ?filter_eval.Batch.Column = null,
    /// Entries before any append.
    source_len: usize = 0,

    pub fn appended(self: *const Dictionary) bool {
        const e = self.entries orelse return false;
        return lenOf(e) != self.source_len;
    }

    /// The dictionary page holding the entries now, built only when an edit appended to them.
    pub fn build(self: *const Dictionary, arena: std.mem.Allocator, chunk: Chunk) Error!?Built {
        if (!self.appended()) return null;
        const src = self.page.?;
        const entries = self.entries.?;
        const fixed = chunk.elem.type == .FIXED_LEN_BYTE_ARRAY;
        const plain = try encoder.encodeValuesPlain(arena, entries, null, 0, fixed);
        const stored = try compression.compressPage(arena, plain, chunk.codec, .{});
        const dh = src.header.dictionary_page_header orelse return error.UnsupportedPage;
        var w = thrift.Writer.init(arena);
        w.writeStructBegin();
        try w.writeFieldI32(1, @backingInt(schema.PageType.DICTIONARY_PAGE));
        try w.writeFieldI32(2, try i32Size(plain.len));
        try w.writeFieldI32(3, try i32Size(stored.len));
        if (src.header.crc != null) try w.writeFieldI32(4, crc(stored));
        try w.writeFieldBegin(.Struct, 7);
        w.writeStructBegin();
        try w.writeFieldI32(1, try i32Size(lenOf(entries)));
        try w.writeFieldI32(2, @backingInt(dh.encoding));
        // An appended entry goes last, whatever order the entries were in.
        if (dh.is_sorted) |s| if (s) try w.writeFieldBool(3, false);
        try w.writeStructEnd();
        try w.writeStructEnd();
        return .{ .head = w.bytes()[0 .. w.bytes().len - 1], .last_id = 7, .body = stored, .uncompressed = plain.len };
    }
};

fn lenOf(c: filter_eval.Batch.Column) usize {
    return switch (c) {
        inline else => |x| x.values.len,
    };
}

fn i32Size(n: usize) error{TooLarge}!i32 {
    return std.math.cast(i32, n) orelse error.TooLarge;
}

fn crc(bytes: []const u8) i32 {
    return @bitCast(std.hash.Crc32.hash(bytes));
}

/// The bounds a statistic or index entry holds, PLAIN-encoded; the editor reports whether a replaced value was one.
pub const Bounds = struct { min: ?[]const u8 = null, max: ?[]const u8 = null };

/// A data page after an edit.
pub const Edited = struct {
    built: Built,
    /// Rows whose value changed.
    changed: usize,
    /// The page's statistics after the edit (its present values; null count over its rows).
    stats: schema.Statistics,
    rows: u64,
    nulls_before: u64,
    nulls_after: u64,
    /// Total bytes of its present BYTE_ARRAY values, before and after.
    unencoded_before: i64 = 0,
    unencoded_after: i64 = 0,
    /// Count per definition level (0..max_def), before and after.
    def_hist_before: []u64,
    def_hist_after: []u64,
    /// A replaced value equalled the given chunk bound.
    replaced_min: bool = false,
    replaced_max: bool = false,
};

/// Set `rows` (page-relative, ascending) of data page `page` to `value`. Null when no row's value changes.
pub fn editDataPage(
    arena: std.mem.Allocator,
    chunk: Chunk,
    dict: *Dictionary,
    file: []const u8,
    page: SourcePage,
    rows: []const u32,
    value: Value,
    chunk_bounds: Bounds,
) Error!?Edited {
    if (chunk.levels.max_rep != 0) return error.UnsupportedPage;
    return switch (chunk.elem.type orelse return error.UnsupportedPage) {
        .BOOLEAN => editTyped(bool, .boolean, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .INT32 => editTyped(i32, .i32, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .INT64 => editTyped(i64, .i64, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .FLOAT => editTyped(f32, .f32, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .DOUBLE => editTyped(f64, .f64, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => editTyped([]const u8, .string, arena, chunk, dict, file, page, rows, value, chunk_bounds),
        .INT96 => error.UnsupportedPage,
    };
}

fn valueAs(comptime T: type, v: Value) ?T {
    return switch (v) {
        .null => null,
        .boolean => |x| if (T == bool) x else unreachable,
        .i32 => |x| if (T == i32) x else unreachable,
        .i64 => |x| if (T == i64) x else unreachable,
        .f32 => |x| if (T == f32) x else unreachable,
        .f64 => |x| if (T == f64) x else unreachable,
        .bytes => |x| if (T == []const u8) x else unreachable,
    };
}

fn same(comptime T: type, a: T, b: T) bool {
    return switch (@typeInfo(T)) {
        .float => @as(@Int(.unsigned, @bitSizeOf(T)), @bitCast(a)) == @as(@Int(.unsigned, @bitSizeOf(T)), @bitCast(b)),
        .pointer => std.mem.eql(u8, a, b),
        else => a == b,
    };
}

fn plainOf(comptime T: type, v: T, buf: *[8]u8) []const u8 {
    return switch (T) {
        bool => (Value{ .boolean = v }).plain(buf),
        []const u8 => v,
        else => blk: {
            const U = @Int(.unsigned, @bitSizeOf(T));
            std.mem.writeInt(U, buf[0..@sizeOf(T)], @bitCast(v), .little);
            break :blk buf[0..@sizeOf(T)];
        },
    };
}

fn editTyped(
    comptime T: type,
    comptime tag: std.meta.Tag(filter_eval.Batch.Column),
    arena: std.mem.Allocator,
    chunk: Chunk,
    dict: *Dictionary,
    file: []const u8,
    page: SourcePage,
    rows: []const u32,
    value: Value,
    chunk_bounds: Bounds,
) Error!?Edited {
    const max_def: u32 = @intCast(chunk.levels.max_def);
    const n: usize = @intCast(page.rows);
    const coded = page.dictionaryEncoded();
    // The page decodes on its own after the chunk's dictionary page, if any.
    const src = if (dict.page) |dp|
        try std.mem.concat(arena, u8, &.{ dp.bytes(file), page.bytes(file) })
    else
        page.bytes(file);
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(src, chunk.codec, chunk.levels, arena, .{ .any_dictionary = true }, null);
    reader.type_length = chunk.type_length;

    var values: []T = &.{};
    var codes: []u32 = &.{};
    var defs: ?[]u32 = null;
    if (coded) {
        if (T == bool) return error.UnsupportedPage;
        const got = (try reader.readAllCodes(n)) orelse return error.UnsupportedPage;
        if (got != .codes) return error.UnsupportedPage;
        codes = got.codes.codes;
        defs = got.codes.def_levels;
        if (dict.entries == null) {
            const entries = try arena.dupe(T, got.codes.dict);
            dict.entries = @unionInit(filter_eval.Batch.Column, @tagName(tag), .{ .values = entries });
            dict.source_len = entries.len;
        }
    } else {
        const got = try reader.readAll(n);
        values = got.values;
        defs = got.def_levels;
    }
    const entries_ptr: ?*filter_eval.ColumnT(T) = if (coded) &@field(dict.entries.?, @tagName(tag)) else null;
    if (max_def > 0 and defs == null) {
        const d = try arena.alloc(u32, n);
        @memset(d, max_def);
        defs = d;
    }

    var e: Edited = .{
        .built = undefined,
        .changed = 0,
        .stats = .{},
        .rows = page.rows,
        .nulls_before = 0,
        .nulls_after = 0,
        .def_hist_before = try arena.alloc(u64, max_def + 1),
        .def_hist_after = try arena.alloc(u64, max_def + 1),
    };
    @memset(e.def_hist_before, 0);
    if (defs) |d| {
        for (d) |l| e.def_hist_before[@min(l, max_def)] += 1;
    } else e.def_hist_before[max_def] = n;
    var buf: [8]u8 = undefined;
    if (T == []const u8 and chunk.elem.type == .BYTE_ARRAY) {
        for (0..n) |i| if (present(defs, max_def, i)) {
            e.unencoded_before += @intCast(rowValue(T, values, codes, entries_ptr, i).len);
        };
    }

    const new: ?T = valueAs(T, value);
    var new_code: u32 = 0;
    if (coded) if (new) |v| {
        const ent = entries_ptr.?;
        new_code = for (ent.values, 0..) |x, k| {
            if (same(T, x, v)) break @intCast(k);
        } else blk: {
            const grown = try arena.alloc(T, ent.values.len + 1);
            @memcpy(grown[0..ent.values.len], ent.values);
            grown[ent.values.len] = v;
            ent.values = grown;
            break :blk @intCast(grown.len - 1);
        };
    };

    for (rows) |r32| {
        const r: usize = r32;
        if (r >= n) return error.UnsupportedPage;
        const was = present(defs, max_def, r);
        if (new) |v| {
            if (was and same(T, rowValue(T, values, codes, entries_ptr, r), v)) continue;
        } else if (!was) continue;
        if (was) {
            const old = plainOf(T, rowValue(T, values, codes, entries_ptr, r), &buf);
            if (chunk_bounds.min) |m| if (std.mem.eql(u8, m, old)) {
                e.replaced_min = true;
            };
            if (chunk_bounds.max) |m| if (std.mem.eql(u8, m, old)) {
                e.replaced_max = true;
            };
        }
        e.changed += 1;
        if (new) |v| {
            if (defs) |d| d[r] = max_def;
            if (coded) codes[r] = new_code else values[r] = v;
        } else {
            defs.?[r] = max_def - 1;
            if (coded) codes[r] = 0;
        }
    }
    if (e.changed == 0) return null;

    @memset(e.def_hist_after, 0);
    if (defs) |d| {
        for (d) |l| e.def_hist_after[@min(l, max_def)] += 1;
    } else e.def_hist_after[max_def] = n;
    e.nulls_before = n - e.def_hist_before[max_def];
    e.nulls_after = n - e.def_hist_after[max_def];
    if (T == []const u8 and chunk.elem.type == .BYTE_ARRAY) {
        for (0..n) |i| if (present(defs, max_def, i)) {
            e.unencoded_after += @intCast(rowValue(T, values, codes, entries_ptr, i).len);
        };
    }

    // The page's present values, materialized for its statistics (and for a coded page, nowhere else).
    const page_values: []const T = if (coded) blk: {
        const vs = try arena.alloc(T, n);
        for (vs, 0..) |*v, i| v.* = if (present(defs, max_def, i)) entries_ptr.?.values[codes[i]] else zero(T);
        break :blk vs;
    } else values;
    if (!coded) for (0..n) |i| if (!present(defs, max_def, i)) {
        values[i] = zero(T);
    };
    const col = @unionInit(filter_eval.Batch.Column, @tagName(tag), .{
        .values = page_values,
        .def_levels = defs,
        .max_def = max_def,
    });
    e.stats = encoder.computeStats(arena, col, defs, max_def, chunk.elem) orelse .{ .null_count = @intCast(e.nulls_after) };
    if (e.stats.null_count == null) e.stats.null_count = @intCast(e.nulls_after);
    // A DECIMAL in bytes orders as its signed unscaled integer, not as its bytes: the byte order would put 3.00 below
    // -2.00 and prune the rows an edit wrote.
    if (T == []const u8) if (decimal_mod.kindFromSchema(chunk.elem)) |kind| {
        e.stats = decimalStats(page_values, defs, max_def, kind, @intCast(e.nulls_after));
    };

    const encoded_values: []const u8 = if (coded) blk: {
        var present_codes: std.ArrayList(u32) = .empty;
        for (0..n) |i| if (present(defs, max_def, i)) try present_codes.append(arena, codes[i]);
        const width = encoder.dictIndexBitWidth(entries_ptr.?.values.len);
        const rle = try hybrid_rle.encode(arena, present_codes.items, width);
        break :blk try std.mem.concat(arena, u8, &.{ &.{width}, rle });
    } else try encodeAs(arena, col, defs, max_def, page.encoding().?, chunk.elem.type == .FIXED_LEN_BYTE_ARRAY);

    e.built = try buildDataPage(arena, chunk, page, defs, max_def, encoded_values, @intCast(e.nulls_after), e.stats);
    return e;
}

/// The bounds of a page of DECIMAL bytes in signed unscaled order, each the value's own bytes.
fn decimalStats(values: []const []const u8, defs: ?[]const u32, max_def: u32, kind: decimal_mod.Kind, nulls: i64) schema.Statistics {
    var st: schema.Statistics = .{ .null_count = nulls };
    var lo: ?i128 = null;
    var hi: ?i128 = null;
    for (values, 0..) |v, i| {
        if (!present(defs, max_def, i)) continue;
        // A value the decimal cannot be read as leaves the page without bounds rather than with incorrect ones.
        const x = decimal_mod.statToI128(v, kind) orelse return .{ .null_count = nulls };
        if (lo == null or x < lo.?) {
            lo = x;
            st.min_value = v;
        }
        if (hi == null or x > hi.?) {
            hi = x;
            st.max_value = v;
        }
    }
    if (lo != null) {
        st.is_min_value_exact = true;
        st.is_max_value_exact = true;
    }
    return st;
}

fn zero(comptime T: type) T {
    return if (T == []const u8) "" else std.mem.zeroes(T);
}

inline fn present(defs: ?[]const u32, max_def: u32, i: usize) bool {
    return if (defs) |d| d[i] == max_def else true;
}

fn rowValue(comptime T: type, values: []const T, codes: []const u32, entries: ?*const filter_eval.ColumnT(T), i: usize) T {
    return if (entries) |e| e.values[codes[i]] else values[i];
}

/// `col`'s present values in `encoding`, which a page of the source already used.
fn encodeAs(
    arena: std.mem.Allocator,
    col: filter_eval.Batch.Column,
    defs: ?[]const u32,
    max_def: u32,
    encoding: schema.Encoding,
    fixed: bool,
) Error![]const u8 {
    return switch (encoding) {
        .PLAIN => encoder.encodeValuesPlain(arena, col, defs, max_def, fixed),
        .DELTA_BINARY_PACKED => switch (col) {
            .i32, .i64 => encoder.encodeValuesAs(arena, col, defs, max_def, .DELTA_BINARY_PACKED, fixed),
            else => error.UnsupportedPage,
        },
        .DELTA_LENGTH_BYTE_ARRAY, .DELTA_BYTE_ARRAY => switch (col) {
            .string => encoder.encodeValuesAs(arena, col, defs, max_def, encoding, fixed),
            else => error.UnsupportedPage,
        },
        .BYTE_STREAM_SPLIT => byteStreamSplit(arena, col, defs, max_def),
        // A BOOLEAN page in RLE: the hybrid stream of its bits after a 4-byte length.
        .RLE => switch (col) {
            .boolean => |c| blk: {
                var bits: std.ArrayList(bool) = .empty;
                for (c.values, 0..) |b, i| if (present(defs, max_def, i)) try bits.append(arena, b);
                const rle = try bitPackedBools(arena, bits.items);
                const out = try arena.alloc(u8, 4 + rle.len);
                std.mem.writeInt(u32, out[0..4], @intCast(rle.len), .little);
                @memcpy(out[4..], rle);
                break :blk out;
            },
            else => error.UnsupportedPage,
        },
        else => error.UnsupportedPage,
    };
}

/// Booleans as one bit-packed run of the RLE/bit-packing hybrid (header `groups << 1 | 1`, then the bits, eight to a
/// byte, low bit first): what Arrow writes for mixed values, and the one form of the hybrid parquet-go 0.32 decodes
/// exactly for booleans, whose RLE runs it reads as whole bytes.
fn bitPackedBools(arena: std.mem.Allocator, bits: []const bool) ![]u8 {
    const groups = (bits.len + 7) / 8;
    var header: [10]u8 = undefined;
    const hl = writeVarint(&header, groups << 1 | 1);
    const out = try arena.alloc(u8, hl + groups);
    @memcpy(out[0..hl], header[0..hl]);
    const body = out[hl..];
    @memset(body, 0);
    for (bits, 0..) |b, i| {
        if (b) body[i / 8] |= @as(u8, 1) << @intCast(i % 8);
    }
    return out;
}

/// BYTE_STREAM_SPLIT: byte k of every present value, for each k in turn.
fn byteStreamSplit(arena: std.mem.Allocator, col: filter_eval.Batch.Column, defs: ?[]const u32, max_def: u32) Error![]const u8 {
    var plain: std.ArrayList(u8) = .empty;
    var width: usize = 0;
    switch (col) {
        .string => |c| for (c.values, 0..) |v, i| if (present(defs, max_def, i)) {
            if (width == 0) width = v.len;
            if (v.len != width) return error.UnsupportedPage;
            try plain.appendSlice(arena, v);
        },
        .boolean, .i128 => return error.UnsupportedPage,
        inline else => |c| {
            const T = @TypeOf(c.values[0]);
            width = @sizeOf(T);
            for (c.values, 0..) |v, i| if (present(defs, max_def, i)) {
                var b: [8]u8 = undefined;
                try plain.appendSlice(arena, plainOf(T, v, &b));
            };
        },
    }
    const out = try arena.alloc(u8, plain.items.len);
    if (width == 0) return out;
    const count = plain.items.len / width;
    for (0..count) |i| for (0..width) |k| {
        out[k * count + i] = plain.items[i * width + k];
    };
    return out;
}

/// A data page of the source's version, header and payload, from its rows' levels and encoded values.
fn buildDataPage(
    arena: std.mem.Allocator,
    chunk: Chunk,
    page: SourcePage,
    defs: ?[]const u32,
    max_def: u32,
    values: []const u8,
    nulls: i32,
    stats: schema.Statistics,
) Error!Built {
    const h = page.header;
    const rows: i64 = @intCast(page.rows);
    var w = thrift.Writer.init(arena);
    w.writeStructBegin();
    if (h.data_page_header) |dh| {
        if (max_def > 0 and dh.definition_level_encoding != .RLE) return error.UnsupportedPage;
        const levels = if (max_def > 0) try encoder.defLevelPrefix(arena, defs, max_def, rows) else &.{};
        const plain = try std.mem.concat(arena, u8, &.{ levels, values });
        const stored = try compression.compressPage(arena, plain, chunk.codec, .{});
        try w.writeFieldI32(1, @backingInt(schema.PageType.DATA_PAGE));
        try w.writeFieldI32(2, try i32Size(plain.len));
        try w.writeFieldI32(3, try i32Size(stored.len));
        if (h.crc != null) try w.writeFieldI32(4, crc(stored));
        try w.writeFieldBegin(.Struct, 5);
        w.writeStructBegin();
        try w.writeFieldI32(1, dh.num_values);
        try w.writeFieldI32(2, @backingInt(dh.encoding));
        try w.writeFieldI32(3, @backingInt(dh.definition_level_encoding));
        try w.writeFieldI32(4, @backingInt(dh.repetition_level_encoding));
        if (page.has_stats) {
            try w.writeFieldBegin(.Struct, 5);
            try stats.write(&w);
        }
        try w.writeStructEnd();
        try w.writeStructEnd();
        return .{ .head = w.bytes()[0 .. w.bytes().len - 1], .last_id = 5, .body = stored, .uncompressed = plain.len };
    }
    const dh = h.data_page_header_v2 orelse return error.UnsupportedPage;
    if (dh.repetition_levels_byte_length != 0) return error.UnsupportedPage;
    const levels: []const u8 = if (max_def > 0) blk: {
        const d = defs orelse return error.UnsupportedPage;
        break :blk try hybrid_rle.encode(arena, d, encoder.bitWidthFor(max_def));
    } else &.{};
    const stored_values = if (dh.is_compressed) try compression.compressPage(arena, values, chunk.codec, .{}) else values;
    const body = try std.mem.concat(arena, u8, &.{ levels, stored_values });
    try w.writeFieldI32(1, @backingInt(schema.PageType.DATA_PAGE_V2));
    try w.writeFieldI32(2, try i32Size(levels.len + values.len));
    try w.writeFieldI32(3, try i32Size(body.len));
    if (h.crc != null) try w.writeFieldI32(4, crc(body));
    try w.writeFieldBegin(.Struct, 8);
    w.writeStructBegin();
    try w.writeFieldI32(1, dh.num_values);
    try w.writeFieldI32(2, nulls);
    try w.writeFieldI32(3, dh.num_rows);
    try w.writeFieldI32(4, @backingInt(dh.encoding));
    try w.writeFieldI32(5, try i32Size(levels.len));
    try w.writeFieldI32(6, 0);
    if (!dh.is_compressed) try w.writeFieldBool(7, false);
    if (page.has_stats) {
        try w.writeFieldBegin(.Struct, 8);
        try stats.write(&w);
    }
    try w.writeStructEnd();
    try w.writeStructEnd();
    return .{ .head = w.bytes()[0 .. w.bytes().len - 1], .last_id = 8, .body = body, .uncompressed = levels.len + values.len };
}

test "page pad: every source length is met modulo the block, in short form, and a length no pad reaches moves a block" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const body: [300]u8 = @splat(0xAB);
    const head = [_]u8{ 0x55, 0x00 }; // field 5, an i32
    const block = fastpath.share_block;
    // bare = 2 + 1 + 300 = 303. Shortfalls of 0 (no pad), 2, 129 (n 127, the last 1-byte length), 130 (none:
    // 129 or 131 bytes), 131 (n 128, a 2-byte length), and 1 (none: the field is at least 2 bytes).
    for ([_]usize{ 303, 305, 432, 433, 434, 304, 303 + block }) |src_len| {
        const out = try finish(a, .{ .head = &head, .last_id = 5, .body = &body, .uncompressed = 0 }, src_len);
        try std.testing.expectEqual(src_len % block, out.len % block);
        try std.testing.expect(out.len >= src_len);
        if (src_len != 304 and src_len != 433) try std.testing.expectEqual(src_len, out.len);
        try std.testing.expect(out.len < 303 + 2 * block);
        try std.testing.expectEqualSlices(u8, &body, out[out.len - body.len ..]);
        // The header parses as a struct whose fields are the head's and, past it, the pad: tag (20 - 5) << 4 | 8.
        var r = thrift.Reader.init(out);
        const fields = try patch.readFields(a, &r);
        try std.testing.expectEqual(out.len - body.len, r.pos);
        if (out.len == 303) {
            try std.testing.expectEqual(@as(usize, 1), fields.len);
        } else {
            try std.testing.expectEqual(@as(usize, 2), fields.len);
            try std.testing.expectEqual(pad_field_id, fields[1].id);
            try std.testing.expectEqual(thrift.Type.Binary, fields[1].type);
            try std.testing.expectEqual(@as(u8, 0xF8), out[2]);
        }
    }
    try std.testing.expectEqual(@as(usize, 304 + block), (try finish(a, .{ .head = &head, .last_id = 5, .body = &body, .uncompressed = 0 }, 304)).len);
    try std.testing.expectError(error.PadFieldUnreachable, finish(a, .{ .head = &head, .last_id = 4, .body = &body, .uncompressed = 0 }, 400));
}

test "page edit: RLE booleans are one bit-packed run, and DECIMAL bytes order numerically" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // 35 values, a run of 32 trues then alternation: parquet-go 0.32 misread RLE runs of booleans (review UPD-5).
    var bits: [35]bool = undefined;
    for (&bits, 0..) |*b, i| b.* = i < 32 or i % 2 == 1;
    const enc = try bitPackedBools(a, &bits);
    try std.testing.expectEqual(@as(u8, (5 << 1) | 1), enc[0]);
    var dec = hybrid_rle.HybridRleDecoder.init(enc, 1);
    var got: [40]u32 = undefined;
    const n = try dec.decode(&got);
    try std.testing.expect(n >= bits.len);
    for (bits, got[0..bits.len]) |want, g| try std.testing.expectEqual(@as(u32, @intFromBool(want)), g);

    // DECIMAL(9,2) in 4 big-endian bytes: 3.00 sorts above -2.00 and -1.00 (review UPD-4).
    const kind: decimal_mod.Kind = .{ .scale = 2, .precision = 9, .physical = .FIXED_LEN_BYTE_ARRAY, .byte_width = 4 };
    const v300 = [_]u8{ 0, 0, 1, 0x2C };
    const vm200 = [_]u8{ 0xFF, 0xFF, 0xFF, 0x38 };
    const vm100 = [_]u8{ 0xFF, 0xFF, 0xFF, 0x9C };
    const values = [_][]const u8{ &v300, &vm200, &vm100 };
    const st = decimalStats(&values, null, 0, kind, 0);
    try std.testing.expectEqualSlices(u8, &vm200, st.min_value.?);
    try std.testing.expectEqualSlices(u8, &v300, st.max_value.?);
}
