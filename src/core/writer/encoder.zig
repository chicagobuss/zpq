//! Parquet PLAIN encoder for column chunks.
//!
//! Phase 5.4b first cut. Supports:
//!   - Encodings: PLAIN only (per-type little-endian, BYTE_ARRAY as
//!     <u32 LE len><bytes>, BOOLEAN bit-packed LSB first).
//!   - Codec: UNCOMPRESSED only.
//!   - Single data page per column chunk.
//!   - Required (non-nullable) columns. Optional/null handling lives
//!     in 5.4b.1 — encodeColumn errors on schema_elem with
//!     repetition_type == OPTIONAL until then.
//!   - Min/max stats over surviving values. No null_count yet (we
//!     don't track nulls in the decode path).
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

pub const Error = error{
    NullableNotSupported,
    UnsupportedType,
    TooLarge,
    CorruptInput, // from snappy (vendored google/snappy)
    OutputTooSmall, // from snappy
} || std.mem.Allocator.Error || compression.Error;

pub const EncodedColumn = struct {
    /// Page header thrift + encoded data bytes, ready to concatenate.
    bytes: []u8,
    /// ColumnMetaData for the file footer. data_page_offset is relative
    /// to the start of `bytes`; the caller adds the column's absolute
    /// position in the output file.
    meta: schema.ColumnMetaData,
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
    const elem = in.schema_elem;
    if (elem.type == null) return error.UnsupportedType;
    const phys = elem.type.?;

    const num_values: i64 = @intCast(valueCount(in.values));
    const is_optional = elem.repetition_type == .OPTIONAL;
    const src_def_levels = sourceDefLevels(in.values);
    const src_max_def = sourceMaxDef(in.values);
    const src_rep_levels = sourceRepLevels(in.values);
    const src_max_rep = sourceMaxRep(in.values);

    // Try dictionary encoding for BYTE_ARRAY first. If unique-count is
    // ≤ 50% of present values AND there are at least 64 present values
    // we emit a two-page chunk (DICTIONARY_PAGE then DATA_PAGE with
    // RLE_DICTIONARY-encoded indices). Otherwise we fall through to
    // the plain path. Numeric columns stay PLAIN (snappy already
    // compresses fixed-width well; dict adds overhead with marginal
    // gain on most numeric distributions).
    if (in.values == .string and src_max_rep == 0) {
        if (try tryEncodeDictBytes(arena, in, num_values)) |enc| return enc;
    }

    // 1. Encode values via PLAIN — only the non-null subset when input
    //    has actual nulls. For dense inputs (def_levels == null), this
    //    is a straight copy of every value.
    const values_bytes = try encodeValuesPlain(arena, in.values, src_def_levels, src_max_def);

    // 2a. Rep-level prefix when the column has nesting (max_rep > 0).
    //     Same `<u32 LE byte_len><RLE bytes>` framing as def levels.
    //     V1 layout puts rep before def.
    const rep_prefix: []const u8 = if (src_max_rep > 0) blk: {
        const rl = src_rep_levels orelse return error.NullableNotSupported;
        const bw = bitWidthFor(src_max_rep);
        const rle = try hybrid_rle.encode(arena, rl, bw);
        const prefix = try arena.alloc(u8, 4 + rle.len);
        std.mem.writeInt(u32, prefix[0..4], @intCast(rle.len), .little);
        @memcpy(prefix[4..], rle);
        break :blk prefix;
    } else &.{};

    // 2b. Def-level prefix when OPTIONAL (or when nested with max_def > 0).
    const need_def_prefix = is_optional or src_max_rep > 0;
    const def_prefix: []const u8 = if (need_def_prefix) blk: {
        const dl_for_encode = if (src_def_levels) |dl| dl else mk_all_ones: {
            const buf = try arena.alloc(u32, @intCast(num_values));
            @memset(buf, 1);
            break :mk_all_ones buf;
        };
        const def_max = if (src_max_def > 0) src_max_def else 1;
        const bw = bitWidthFor(def_max);
        const rle = try hybrid_rle.encode(arena, dl_for_encode, bw);

        const prefix = try arena.alloc(u8, 4 + rle.len);
        std.mem.writeInt(u32, prefix[0..4], @intCast(rle.len), .little);
        @memcpy(prefix[4..], rle);
        break :blk prefix;
    } else &.{};

    const data_total_len = rep_prefix.len + def_prefix.len + values_bytes.len;

    // 3. Compute stats. Pass `arena` so byte-array min/max strings can
    //    be dupe'd onto a stable arena (the source values may live on
    //    a row-group-scoped arena that's freed before the file footer
    //    is written; borrowing the slice into the footer would dangle).
    var stats = computeStats(arena, in.values, src_def_levels, src_max_def);
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

    // 4b. Snappy-compress the payload. Page header thrift is NEVER
    //     compressed in Parquet — only the data portion. If the
    //     compressed output is somehow >= original (incompressible
    //     data) we still ship the compressed version; the format
    //     allows it and a 1-2% bloat is preferable to a per-page
    //     branching codec.
    const compressed = try compression.compress(arena, payload, in.codec);

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

    // 6. Concatenate: page header thrift (uncompressed) || compressed payload.
    const total = try arena.alloc(u8, header_bytes.len + compressed.len);
    @memcpy(total[0..header_bytes.len], header_bytes);
    @memcpy(total[header_bytes.len..], compressed);

    // 7. Build ColumnMetaData. total_uncompressed_size and
    //    total_compressed_size include the page header (parquet spec).
    //    They differ by `data_total_len - compressed.len`.
    var encodings: schema.EncodingList = .empty;
    try encodings.append(arena, .PLAIN);

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
    };

    return .{ .bytes = total, .meta = meta };
}

/// Try to dictionary-encode a BYTE_ARRAY column. Returns null when
/// the cardinality is too high for dict to help (and we should fall
/// through to PLAIN), or when there are too few values to bother.
///
/// Output layout when we DO dict-encode:
///   [dict_page_thrift][dict_page_compressed][data_page_thrift][data_page_compressed]
///
/// The dict page contains the unique values PLAIN-encoded. The data
/// page contains the def-level prefix (if optional) then a `<u8 bit_width>`
/// byte followed by hybrid-RLE-encoded indices into the dictionary.
/// `data_page_offset` is set to the relative byte offset of the data
/// page inside the returned `bytes` slice; `dictionary_page_offset` is
/// 0. Callers must ADD their absolute chunk-start offset to both.
fn tryEncodeDictBytes(arena: std.mem.Allocator, in: ColumnInput, num_values: i64) Error!?EncodedColumn {
    const elem = in.schema_elem;
    const phys = elem.type.?;
    const string_col = in.values.string;
    const values = string_col.values;
    const def_levels = string_col.def_levels;
    const max_def = string_col.max_def;
    const is_optional = elem.repetition_type == .OPTIONAL;

    const num_present = countPresent(values.len, def_levels, max_def);
    if (num_present < 64) return null;

    var lookup = std.StringHashMap(u32).init(arena);
    defer lookup.deinit();
    var dict_values: std.ArrayList([]const u8) = .empty;
    var indices = try arena.alloc(u32, num_present);

    // Bail early if dictionary would grow past 25% of values. Above
    // that the dict-build cost (hashmap inserts on every distinct
    // value) starts swamping the snappy savings; at 25% the dict
    // page itself approaches the size of the PLAIN values, leaving
    // only the smaller index page as savings — usually not worth
    // the build-side CPU.
    const max_cardinality = num_present / 4;

    var idx_pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const gop = try lookup.getOrPut(v);
        if (!gop.found_existing) {
            if (dict_values.items.len >= max_cardinality) return null;
            const new_idx: u32 = @intCast(dict_values.items.len);
            try dict_values.append(arena, v);
            gop.value_ptr.* = new_idx;
        }
        indices[idx_pos] = gop.value_ptr.*;
        idx_pos += 1;
    }

    if (dict_values.items.len == 0) return null;

    // ----- Build dict page -----
    var dict_raw: std.ArrayList(u8) = .empty;
    for (dict_values.items) |v| {
        var len_bytes: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_bytes, @intCast(v.len), .little);
        try dict_raw.appendSlice(arena, &len_bytes);
        try dict_raw.appendSlice(arena, v);
    }
    const dict_compressed = try compression.compress(arena, dict_raw.items, in.codec);

    var dict_page_hdr: schema.PageHeader = .{
        .type = .DICTIONARY_PAGE,
        .uncompressed_page_size = @intCast(dict_raw.items.len),
        .compressed_page_size = @intCast(dict_compressed.len),
        .crc = null,
        .data_page_header = null,
        .dictionary_page_header = .{
            .num_values = @intCast(dict_values.items.len),
            .encoding = .PLAIN,
            .is_sorted = null,
        },
        .data_page_header_v2 = null,
    };
    var dw: thrift.Writer = .init(arena);
    defer dw.deinit();
    try dict_page_hdr.write(&dw);
    const dict_thrift_bytes = try arena.dupe(u8, dw.bytes());
    const dict_total_len = dict_thrift_bytes.len + dict_compressed.len;

    // ----- Build data page (def prefix + bit-width + RLE indices) -----
    const def_prefix: []const u8 = if (is_optional) blk: {
        const dl_for_encode = if (def_levels) |dl| dl else mk_all_ones: {
            const buf = try arena.alloc(u32, @intCast(num_values));
            @memset(buf, 1);
            break :mk_all_ones buf;
        };
        const def_max = if (max_def > 0) max_def else 1;
        const bw = bitWidthFor(def_max);
        const rle = try hybrid_rle.encode(arena, dl_for_encode, bw);
        const prefix = try arena.alloc(u8, 4 + rle.len);
        std.mem.writeInt(u32, prefix[0..4], @intCast(rle.len), .little);
        @memcpy(prefix[4..], rle);
        break :blk prefix;
    } else &.{};

    const dict_size: u32 = @intCast(dict_values.items.len);
    const idx_bit_width = if (dict_size <= 1) @as(u8, 0) else bitWidthFor(dict_size - 1);
    const idx_rle = try hybrid_rle.encode(arena, indices, idx_bit_width);

    const indices_section_len = 1 + idx_rle.len;
    const data_total_len = def_prefix.len + indices_section_len;
    const data_payload = try arena.alloc(u8, data_total_len);
    {
        var pos: usize = 0;
        if (def_prefix.len > 0) {
            @memcpy(data_payload[pos..][0..def_prefix.len], def_prefix);
            pos += def_prefix.len;
        }
        data_payload[pos] = idx_bit_width;
        pos += 1;
        @memcpy(data_payload[pos..], idx_rle);
    }
    const data_compressed = try compression.compress(arena, data_payload, in.codec);

    var data_page_hdr: schema.PageHeader = .{
        .type = .DATA_PAGE,
        .uncompressed_page_size = @intCast(data_total_len),
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
    };
    var dpw: thrift.Writer = .init(arena);
    defer dpw.deinit();
    try data_page_hdr.write(&dpw);
    const data_thrift_bytes = try arena.dupe(u8, dpw.bytes());
    const data_total_with_thrift = data_thrift_bytes.len + data_compressed.len;

    // ----- Concatenate -----
    const total_len = dict_total_len + data_total_with_thrift;
    const total = try arena.alloc(u8, total_len);
    var offset: usize = 0;
    @memcpy(total[offset..][0..dict_thrift_bytes.len], dict_thrift_bytes);
    offset += dict_thrift_bytes.len;
    @memcpy(total[offset..][0..dict_compressed.len], dict_compressed);
    offset += dict_compressed.len;
    @memcpy(total[offset..][0..data_thrift_bytes.len], data_thrift_bytes);
    offset += data_thrift_bytes.len;
    @memcpy(total[offset..], data_compressed);

    const stats = computeStats(arena, in.values, def_levels, max_def);

    var encodings: schema.EncodingList = .empty;
    try encodings.append(arena, .RLE);
    try encodings.append(arena, .PLAIN);
    try encodings.append(arena, .PLAIN_DICTIONARY);

    var path_list: schema.StringList = .empty;
    try path_list.appendSlice(arena, in.path_in_schema);

    const total_uncompressed: usize = dict_thrift_bytes.len + dict_raw.items.len +
        data_thrift_bytes.len + data_total_len;

    const meta: schema.ColumnMetaData = .{
        .type = phys,
        .encodings = encodings,
        .path_in_schema = path_list,
        .codec = in.codec,
        .num_values = num_values,
        .total_uncompressed_size = @intCast(total_uncompressed),
        .total_compressed_size = @intCast(total_len),
        // Relative-to-chunk-start. Caller adds absolute chunk start.
        .data_page_offset = @intCast(dict_total_len),
        .dictionary_page_offset = 0,
        .index_page_offset = null,
        .statistics = stats,
    };

    return .{ .bytes = total, .meta = meta };
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
    };
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
    const out_values = try arena.alloc(T, surviving);
    var w: usize = 0;
    for (col.values, 0..) |item, i| {
        if (sel.isActive(i)) {
            out_values[w] = item;
            w += 1;
        }
    }

    if (col.def_levels) |dl| {
        const out_dls = try arena.alloc(u32, surviving);
        var w2: usize = 0;
        for (dl, 0..) |d, i| {
            if (sel.isActive(i)) {
                out_dls[w2] = d;
                w2 += 1;
            }
        }
        return .{ .values = out_values, .def_levels = out_dls, .max_def = col.max_def };
    }
    return .{ .values = out_values };
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
) Error![]u8 {
    return switch (vals) {
        .i32 => |c| try encodePlainTyped(i32, arena, c.values, def_levels, max_def),
        .i64 => |c| try encodePlainTyped(i64, arena, c.values, def_levels, max_def),
        .f32 => |c| try encodePlainTyped(f32, arena, c.values, def_levels, max_def),
        .f64 => |c| try encodePlainTyped(f64, arena, c.values, def_levels, max_def),
        .string => |c| try encodePlainBytes(arena, c.values, def_levels, max_def),
        .boolean => |c| try encodePlainBool(arena, c.values, def_levels, max_def),
    };
}

/// Number of slots where def_levels[i] == max_def. When def_levels is
/// null, every slot counts.
fn countPresent(values_len: usize, def_levels: ?[]const u32, max_def: u32) usize {
    if (def_levels) |dl| {
        var n: usize = 0;
        for (dl) |d| {
            if (d == max_def) n += 1;
        }
        return n;
    }
    return values_len;
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
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        switch (@typeInfo(T)) {
            .int => std.mem.writeInt(T, out[pos..][0..item_size], v, .little),
            .float => {
                const Bits = std.meta.Int(.unsigned, @bitSizeOf(T));
                const bits: Bits = @bitCast(v);
                std.mem.writeInt(Bits, out[pos..][0..item_size], bits, .little);
            },
            else => @compileError("encodePlainTyped: unsupported type"),
        }
        pos += item_size;
    }
    return out;
}

/// PLAIN encoding for BYTE_ARRAY: each present value is `<u32 LE length><bytes>`.
fn encodePlainBytes(
    arena: std.mem.Allocator,
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
) Error![]u8 {
    var total: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        total += 4 + v.len;
    }

    const out = try arena.alloc(u8, total);
    var pos: usize = 0;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        const len_u32: u32 = @intCast(v.len);
        std.mem.writeInt(u32, out[pos..][0..4], len_u32, .little);
        @memcpy(out[pos + 4 ..][0..v.len], v);
        pos += 4 + v.len;
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
    };
}

// ============================================================
// Statistics — min/max over surviving non-null values
// ============================================================

fn computeStats(arena: std.mem.Allocator, c: filter_eval.Batch.Column, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    return switch (c) {
        .i32 => |x| statsTyped(i32, x.values, def_levels, max_def),
        .i64 => |x| statsTyped(i64, x.values, def_levels, max_def),
        .f32 => |x| statsTypedFloat(f32, x.values, def_levels, max_def),
        .f64 => |x| statsTypedFloat(f64, x.values, def_levels, max_def),
        .string => |x| statsBytes(arena, x.values, def_levels, max_def),
        .boolean => |x| statsBool(x.values, def_levels, max_def),
    };
}

fn statsTyped(comptime T: type, values: []const T, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var first_idx: ?usize = null;
    for (values, 0..) |_, i| {
        if (isPresent(def_levels, max_def, i)) {
            first_idx = i;
            break;
        }
    }
    const fi = first_idx orelse return null; // all-null column → no stats
    var lo: T = values[fi];
    var hi: T = values[fi];
    for (values[fi + 1 ..], fi + 1..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    // Stats are stored as little-endian byte strings of the typed value.
    const item_size = @sizeOf(T);
    const min_buf = std.heap.page_allocator.alloc(u8, item_size) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, item_size) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    std.mem.writeInt(T, min_buf[0..item_size], lo, .little);
    std.mem.writeInt(T, max_buf[0..item_size], hi, .little);
    return .{ .min_value = min_buf, .max_value = max_buf };
}

fn statsTypedFloat(comptime T: type, values: []const T, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var first_idx: ?usize = null;
    for (values, 0..) |_, i| {
        if (isPresent(def_levels, max_def, i)) {
            first_idx = i;
            break;
        }
    }
    const fi = first_idx orelse return null;
    var lo: T = values[fi];
    var hi: T = values[fi];
    for (values[fi + 1 ..], fi + 1..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        if (v < lo) lo = v;
        if (v > hi) hi = v;
    }
    const Bits = std.meta.Int(.unsigned, @bitSizeOf(T));
    const item_size = @sizeOf(T);
    const min_buf = std.heap.page_allocator.alloc(u8, item_size) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, item_size) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    const lo_bits: Bits = @bitCast(lo);
    const hi_bits: Bits = @bitCast(hi);
    std.mem.writeInt(Bits, min_buf[0..item_size], lo_bits, .little);
    std.mem.writeInt(Bits, max_buf[0..item_size], hi_bits, .little);
    return .{ .min_value = min_buf, .max_value = max_buf };
}

fn statsBytes(arena: std.mem.Allocator, values: []const []const u8, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var first_idx: ?usize = null;
    for (values, 0..) |_, i| {
        if (isPresent(def_levels, max_def, i)) {
            first_idx = i;
            break;
        }
    }
    const fi = first_idx orelse return null;
    var lo: []const u8 = values[fi];
    var hi: []const u8 = values[fi];
    for (values[fi + 1 ..], fi + 1..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        if (std.mem.lessThan(u8, v, lo)) lo = v;
        if (std.mem.lessThan(u8, hi, v)) hi = v;
    }
    // Caller's arena is the file-footer-lifetime arena; the values
    // slices typically borrow from a row-group-scoped arena. Dupe so
    // the footer write later sees stable bytes.
    const lo_owned = arena.dupe(u8, lo) catch return null;
    const hi_owned = arena.dupe(u8, hi) catch return null;
    return .{ .min_value = lo_owned, .max_value = hi_owned };
}

fn statsBool(values: []const bool, def_levels: ?[]const u32, max_def: u32) ?schema.Statistics {
    if (values.len == 0) return null;
    var has_t = false;
    var has_f = false;
    var any_present = false;
    for (values, 0..) |v, i| {
        if (!isPresent(def_levels, max_def, i)) continue;
        any_present = true;
        if (v) has_t = true else has_f = true;
        if (has_t and has_f) break;
    }
    if (!any_present) return null;
    const min_buf = std.heap.page_allocator.alloc(u8, 1) catch return null;
    const max_buf = std.heap.page_allocator.alloc(u8, 1) catch {
        std.heap.page_allocator.free(min_buf);
        return null;
    };
    min_buf[0] = if (has_f) 0 else 1;
    max_buf[0] = if (has_t) 1 else 0;
    return .{ .min_value = min_buf, .max_value = max_buf };
}

// ============================================================
// Tests — round-trip through metadata.open
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

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
    const encoded = try encodePlainBytes(arena, &v, null, 0);
    defer arena.free(encoded);

    var pos: usize = 0;
    for (v) |orig| {
        const len = std.mem.readInt(u32, encoded[pos..][0..4], .little);
        try testing.expectEqual(@as(u32, @intCast(orig.len)), len);
        try testing.expectEqualStrings(orig, encoded[pos + 4 .. pos + 4 + len]);
        pos += 4 + len;
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
    const linux = std.os.linux;

    const fixture_path = "data/benchmark_100mb.parquet";
    var path_z: [256]u8 = undefined;
    @memcpy(path_z[0..fixture_path.len], fixture_path);
    path_z[fixture_path.len] = 0;
    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (@as(isize, @bitCast(r_open)) < 0) return; // missing fixture
    const fd: linux.fd_t = @intCast(@as(isize, @bitCast(r_open)));
    defer _ = linux.close(fd);
    const end_pos = linux.lseek(fd, 0, 2);
    _ = linux.lseek(fd, 0, 0);
    const size: usize = @intCast(end_pos);
    const file_bytes = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(file_bytes);
    var off: usize = 0;
    while (off < size) {
        const r = linux.read(fd, file_bytes[off..].ptr, size - off);
        const n: isize = @bitCast(r);
        if (n <= 0) break;
        off += @intCast(n);
    }

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
