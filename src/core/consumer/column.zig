const std = @import("std");
const profile = @import("../../profile.zig");
const int96_mod = @import("../parquet/int96.zig");
const metadata = @import("../parquet/metadata.zig");
const SliceReader = @import("slice_reader.zig").SliceReader;
const sourceBytes = @import("types.zig").sourceBytes;
const chunkArgs = @import("types.zig").chunkArgs;
const decimal_mod = @import("../parquet/decimal.zig");
const invariant = @import("../invariant.zig");
const enterChunk = @import("types.zig").enterChunk;
const schema = @import("../schema.zig");
const filter_eval = @import("../filter/eval.zig");
const column_mod = @import("../parquet/column.zig");
const RGSrc = @import("types.zig").RGSrc;
const DecodeOptions = column_mod.DecodeOptions;
const DecodeScratch = column_mod.DecodeScratch;

/// A decoded column must describe exactly its row group's rows: a flat column one value per row, a repeated one
/// `num_rows` record starts (repetition level 0), the first entry among them. Filters, the selection vector and the
/// output aggregator all walk every column against `num_rows`, so a chunk whose counts or levels disagree is corrupt.
/// O(1) for flat columns; a pass over the levels for repeated ones.
pub fn checkRowShape(col: filter_eval.Batch.Column, num_rows: usize) error{ColumnRowCountMismatch}!void {
    switch (col) {
        inline else => |c| {
            if (c.rep_levels) |rl| {
                if (rl.len > 0 and rl[0] != 0) return error.ColumnRowCountMismatch;
                var starts: usize = 0;
                for (rl) |r| starts += @intFromBool(r == 0);
                if (starts != num_rows) return error.ColumnRowCountMismatch;
            } else if (c.rowCount() != num_rows) return error.ColumnRowCountMismatch;
            if (c.def_levels) |dl| if (dl.len != c.rowCount()) return error.ColumnRowCountMismatch;
        },
    }
}

/// One uncompressed page: header + body.
/// Slice the source page-index bytes for a chunk out of `src.bytes`,
/// accounting for `byte_origin` (the buffer may start partway into the
/// file). Null when the index isn't present in the fetched bytes.
pub fn originSlice(src: RGSrc, file_off: i64, len: i32) ?[]const u8 {
    if (file_off < 0 or len < 0) return null;
    const fo: u64 = @intCast(file_off);
    if (fo < src.byte_origin) return null;
    const buf_off: usize = @intCast(fo - src.byte_origin);
    const l: usize = @intCast(len);
    if (buf_off + l > src.bytes.len) return null;
    return src.bytes[buf_off .. buf_off + l];
}

/// A chunk's page index resolved against the filter: which pages to skip,
/// which the filter matches in full, and where each page lies.
pub const PruningInfo = struct {
    locations: []const schema.PageLocation,
    page_is_skipped: []const bool,
    page_is_always_match: []const bool,
    dictionary_page_offset: ?i64,
    chunk_file_offset: i64,
    col_index: schema.ColumnIndex,
    /// Decode only these rows, in order, into a column of their total length (`compactRows`).
    keep: ?[]const RowRange = null,
};

/// Rows `lo..hi` of a row group.
pub const RowRange = @import("../row_subgroups.zig").Range;

pub fn defaultVal(comptime T: type) T {
    return switch (T) {
        i32, i64 => 0,
        f32, f64 => 0.0,
        bool => false,
        []const u8 => "",
        else => unreachable,
    };
}

/// `decodeWithReaderPruned` for just the rows of `keep` (sorted, disjoint, one value per row): pages with no kept row
/// are skipped unread, the others decode into a page-sized scratch and their kept rows are copied out in order. The
/// result has one entry per kept row and never zero-fills the rows it leaves out.
/// Whether a data page of rows `start..end` is passed unread by the reader's row gate (`DecodeOptions.row_gate`).
fn gatedOut(comptime T: type, reader: *const column_mod.ColumnChunkReader(T), start: usize, end: usize) bool {
    const gate = reader.gate orelse return false;
    return !gate.anyInRange(@min(start, gate.len), @min(end, gate.len));
}

fn decodeKeptRows(
    comptime T: type,
    arena: std.mem.Allocator,
    reader: *column_mod.ColumnChunkReader(T),
    levels: schema.Levels,
    num_leaves: usize,
    prune: PruningInfo,
    keep: []const RowRange,
) !filter_eval.ColumnT(T) {
    if (levels.max_rep != 0) return error.ShortDecode;
    // The row gate is in compact positions here: decided below per page, never by the reader in row-group rows.
    const gate_kept = reader.gate;
    reader.gate = null;
    var total: usize = 0;
    for (keep) |r| total += r.hi - r.lo;
    reader.value_budget = num_leaves;
    const max_def = levels.max_def;
    const values = try arena.alloc(T, total);
    const def_levels: ?[]u32 = if (max_def > 0) try arena.alloc(u32, total) else null;
    const possible_dict_offset = prune.dictionary_page_offset orelse blk: {
        if (prune.locations.len > 0 and prune.locations[0].offset > prune.chunk_file_offset)
            break :blk prune.chunk_file_offset;
        break :blk null;
    };
    if (possible_dict_offset) |dict_off| _ = try reader.seekAndInstallDictionaryPage(dict_off, prune.chunk_file_offset);
    for (prune.locations) |loc| if (loc.first_row_index > num_leaves) return error.ShortDecode;
    var page_values: []T = &.{};
    var page_def: []u32 = &.{};
    var k: usize = 0; // first range that may meet the current page
    var out: usize = 0; // where range `k`'s rows go
    for (prune.locations, 0..) |loc, pi| {
        const start: usize = @intCast(loc.first_row_index);
        const end: usize = if (pi + 1 < prune.locations.len) @intCast(prune.locations[pi + 1].first_row_index) else num_leaves;
        while (k < keep.len and keep[k].hi <= start) : (k += 1) out += keep[k].hi - keep[k].lo;
        var meets = k < keep.len and keep[k].lo < end;
        if (meets) if (gate_kept) |gate| {
            // Gated by the filter's survivors among the kept rows, in compact positions.
            var any = false;
            var j = k;
            var o = out;
            while (j < keep.len and keep[j].lo < end) : (j += 1) {
                const lo = @max(keep[j].lo, start);
                const hi = @min(keep[j].hi, end);
                const at = o + (lo - keep[j].lo);
                any = any or gate.anyInRange(at, at + (hi - lo));
                if (keep[j].hi > end) break;
                o += keep[j].hi - keep[j].lo;
            }
            if (!any) {
                // The page survived page pruning and the filter then dropped every kept row. Fill those slots and
                // leave `k`/`out` so the next page still lines up. Required columns have no definition levels; a
                // missing slice panics, and writing 0 there would call present defaults null.
                var j2 = k;
                var o2 = out;
                while (j2 < keep.len and keep[j2].lo < end) : (j2 += 1) {
                    const lo = @max(keep[j2].lo, start);
                    const hi = @min(keep[j2].hi, end);
                    const at = o2 + (lo - keep[j2].lo);
                    @memset(values[at..][0 .. hi - lo], defaultVal(T));
                    if (def_levels) |dl| @memset(dl[at..][0 .. hi - lo], 0);
                    if (keep[j2].hi > end) break;
                    o2 += keep[j2].hi - keep[j2].lo;
                }
                meets = false;
            }
        };
        const null_page = pi < prune.col_index.null_pages.items.len and prune.col_index.null_pages.items[pi];
        const always = prune.page_is_always_match[pi] and (!null_page or def_levels != null);
        // A skipped page holds no kept row (pruning deselected its rows); one that did would decode below.
        if (!meets or always) {
            if (meets) {
                // An always-match page stands in for its rows as `decodeWithReaderPruned` fills them.
                const fill: T = if (null_page or T == []const u8 or T == bool) defaultVal(T) else blk: {
                    const encoded_mod = @import("../filter/encoded.zig");
                    break :blk encoded_mod.readFixedLE(T, prune.col_index.min_values.items[pi]) orelse defaultVal(T);
                };
                var j = k;
                var o = out;
                while (j < keep.len and keep[j].lo < end) : (j += 1) {
                    const lo = @max(keep[j].lo, start);
                    const hi = @min(keep[j].hi, end);
                    const at = o + (lo - keep[j].lo);
                    @memset(values[at..][0 .. hi - lo], fill);
                    if (def_levels) |dl| @memset(dl[at..][0 .. hi - lo], if (null_page) 0 else @intCast(max_def));
                    if (keep[j].hi > end) break;
                    o += keep[j].hi - keep[j].lo;
                }
            }
            if (pi + 1 < prune.locations.len) {
                try reader.pages.seekToPage(prune.locations[pi + 1].offset, prune.chunk_file_offset);
            } else reader.pages.pos = reader.pages.chunk.len;
            continue;
        }
        // The row gate counts from this page's first row, which a seek skips to.
        reader.gate_row = start;
        _ = try reader.seekAndInstallPage(loc.offset, prune.chunk_file_offset);
        const want = end - start;
        if (page_values.len < want) {
            page_values = try arena.alloc(T, want);
            if (max_def > 0) page_def = try arena.alloc(u32, want);
        }
        // Null slots keep the default the decoder leaves them at.
        @memset(page_values[0..want], defaultVal(T));
        const n = if (max_def > 0)
            try reader.decodeWithLevels(page_values[0..want], page_def[0..want])
        else
            try reader.decode(page_values[0..want]);
        if (n != want) return error.ShortDecode;
        var j = k;
        var o = out;
        while (j < keep.len and keep[j].lo < end) : (j += 1) {
            const lo = @max(keep[j].lo, start);
            const hi = @min(keep[j].hi, end);
            const at = o + (lo - keep[j].lo);
            @memcpy(values[at..][0 .. hi - lo], page_values[lo - start .. hi - start]);
            if (def_levels) |dl| @memcpy(dl[at..][0 .. hi - lo], page_def[lo - start .. hi - start]);
            if (keep[j].hi > end) break;
            o += keep[j].hi - keep[j].lo;
        }
    }
    return .{
        .values = values,
        .def_levels = def_levels,
        .max_def = @intCast(max_def),
        .rep_levels = null,
        .max_rep = 0,
        .has_nulls = reader.has_nulls,
    };
}

pub fn decodeWithReaderPruned(
    comptime T: type,
    arena: std.mem.Allocator,
    reader: *column_mod.ColumnChunkReader(T),
    levels: schema.Levels,
    num_leaves: usize,
    prune: PruningInfo,
) !filter_eval.ColumnT(T) {
    if (prune.keep) |keep| return decodeKeptRows(T, arena, reader, levels, num_leaves, prune, keep);
    reader.value_budget = num_leaves;
    const values = try arena.alloc(T, num_leaves);
    @memset(values, defaultVal(T));

    const max_def = levels.max_def;
    const max_rep = levels.max_rep;

    var def_levels: ?[]u32 = null;
    var rep_levels: ?[]u32 = null;

    if (max_rep > 0) {
        def_levels = try arena.alloc(u32, num_leaves);
        @memset(def_levels.?, 0);
        rep_levels = try arena.alloc(u32, num_leaves);
        @memset(rep_levels.?, 0);
    } else if (max_def > 0) {
        // Always materialised on the pruned path, even under
        // `--fast-levels`. The zero-fill is load-bearing here: a page
        // this scan skips keeps def level 0, which is what makes
        // downstream treat its untouched default values as null rather
        // than as real data. Dropping the array would turn skipped
        // pages into present zeros. Pages that ARE decoded still take
        // the cheap all-present path inside `decodePageSlice`.
        def_levels = try arena.alloc(u32, num_leaves);
        @memset(def_levels.?, 0);
    }

    // `dictionary_page_offset` is optional. When it is absent, writers may
    // place a dictionary at `data_page_offset` or put the first data page
    // there. The OffsetIndex distinguishes the observed shapes: a first data
    // page after the chunk start leaves a leading page worth probing, while an
    // equal offset proves there is nothing to install. Do not use the
    // deprecated `ColumnChunk.file_offset`, which is inconsistent across
    // writers. A file that hides a dictionary before an unfetched
    // `data_page_offset` still fails cleanly with DictionaryMissing.
    const possible_dict_offset = prune.dictionary_page_offset orelse blk: {
        if (prune.locations.len > 0 and
            prune.locations[0].offset > prune.chunk_file_offset)
        {
            break :blk prune.chunk_file_offset;
        }
        break :blk null;
    };
    if (possible_dict_offset) |dict_off| {
        _ = try reader.seekAndInstallDictionaryPage(dict_off, prune.chunk_file_offset);
    }

    // Page row starts slice `values`, which is sized from the chunk's value count, not the row group's row count
    // the index was checked against.
    for (prune.locations) |loc| if (loc.first_row_index > num_leaves) return error.ShortDecode;

    // 2. Loop over pages and decode or skip
    for (prune.locations, 0..) |loc, pi| {
        const start: usize = @intCast(loc.first_row_index);
        const end: usize = if (pi + 1 < prune.locations.len) @intCast(prune.locations[pi + 1].first_row_index) else num_leaves;

        const is_skipped = prune.page_is_skipped[pi] or gatedOut(T, reader, start, end);
        // An always-match page stands in for its rows with values that satisfy what the filter proved. On a page
        // the ColumnIndex flags all-null that is `IS NULL`, so its rows must read as absent (definition level 0),
        // not as present copies of the page minimum. A leaf without levels cannot express that: decode instead.
        const null_page = pi < prune.col_index.null_pages.items.len and prune.col_index.null_pages.items[pi];
        const is_always_match = prune.page_is_always_match[pi] and (!null_page or def_levels != null);

        if (is_skipped or is_always_match) {
            if (is_always_match and null_page) {
                @memset(values[start..end], defaultVal(T));
                @memset(def_levels.?[start..end], 0);
            } else if (is_always_match) {
                const min_bytes = prune.col_index.min_values.items[pi];
                const min_v = if (T == []const u8 or T == bool)
                    defaultVal(T)
                else blk: {
                    const encoded_mod = @import("../filter/encoded.zig");
                    break :blk encoded_mod.readFixedLE(T, min_bytes) orelse defaultVal(T);
                };
                @memset(values[start..end], min_v);

                if (def_levels) |dl| {
                    @memset(dl[start..end], @intCast(max_def));
                }
            }

            // Reposition the PageReader past this page
            if (pi + 1 < prune.locations.len) {
                try reader.pages.seekToPage(prune.locations[pi + 1].offset, prune.chunk_file_offset);
            } else {
                reader.pages.pos = reader.pages.chunk.len;
            }
            continue;
        }

        // The row gate counts from this page's first row, which a seek skips to.
        reader.gate_row = start;
        _ = try reader.seekAndInstallPage(loc.offset, prune.chunk_file_offset);

        const want = end - start;
        if (max_rep > 0) {
            const n = try reader.decodeWithRepLevels(values[start..end], def_levels.?[start..end], rep_levels.?[start..end]);
            if (n != want) return error.ShortDecode;
        } else if (max_def > 0) {
            const n = try reader.decodeWithLevels(values[start..end], def_levels.?[start..end]);
            if (n != want) return error.ShortDecode;
        } else {
            const n = try reader.decode(values[start..end]);
            if (n != want) return error.ShortDecode;
        }
    }

    return .{
        .values = values,
        .def_levels = def_levels,
        .max_def = @intCast(max_def),
        .rep_levels = rep_levels,
        .max_rep = @intCast(max_rep),
        .has_nulls = reader.has_nulls,
    };
}

/// Decode an entire column chunk into a `ColumnT(T)` view: values
/// plus def_levels (OPTIONAL) and optionally rep_levels (LIST/MAP).
/// `num_leaves` is the column chunk's `num_values` from metadata
/// (counts LEAVES, not logical rows — for nested cols this can be
/// larger than the RG's row count).
pub fn decodeColumnTPruned(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, arena, decode_options, scratch);
    if (prune) |p| {
        return decodeWithReaderPruned(T, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader(T, &reader, num_leaves);
    }
}

pub fn decodeFloat16ColumnAsF64Pruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumnPruned(arena, chunk, codec, levels, num_leaves, 2, prune, decode_options, scratch);
    const out = try arena.alloc(f64, cb.values.len);
    @memset(out, 0);
    for (cb.values, 0..) |bytes, i| {
        if (bytes.len >= 2) {
            const bits = std.mem.readInt(u16, bytes[0..2], .little);
            out[i] = @floatCast(@as(f16, @bitCast(bits)));
        }
    }
    return .{
        .values = out,
        .def_levels = cb.def_levels,
        .max_def = cb.max_def,
        .rep_levels = cb.rep_levels,
        .has_nulls = cb.has_nulls,
    };
}

pub fn decodeU32ColumnAsI64Pruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnTPruned(i32, arena, chunk, codec, levels, num_leaves, prune, decode_options, scratch);
    const out = try arena.alloc(i64, c32.values.len);
    @memset(out, 0);
    for (c32.values, 0..) |v, i| out[i] = @as(i64, @as(u32, @bitCast(v)));
    return .{
        .values = out,
        .def_levels = c32.def_levels,
        .max_def = c32.max_def,
        .rep_levels = c32.rep_levels,
        .has_nulls = c32.has_nulls,
    };
}

pub fn decodeFlbaColumnPruned(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
    prune: ?PruningInfo,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk,
        codec,
        levels,
        arena,
        decode_options,
        scratch,
    );
    reader.type_length = type_length;
    if (prune) |p| {
        return decodeWithReaderPruned([]const u8, arena, &reader, levels, num_leaves, p);
    } else {
        return decodeWithReader([]const u8, &reader, num_leaves);
    }
}

pub fn decodeColumnT(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    return decodeColumnTWithOptions(T, arena, chunk, codec, levels, num_leaves, .{});
}

pub fn decodeColumnTWithOptions(
    comptime T: type,
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(T) {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, arena, decode_options, null);
    return decodeWithReader(T, &reader, num_leaves);
}

/// Fixed byte width of a FIXED_LEN_BYTE_ARRAY column from its SchemaElement
/// (0 if absent). Used to drive `decodeFlbaColumn`.
pub fn flbaWidth(se: ?schema.SchemaElement) usize {
    if (se) |s| if (s.type_length) |t| return @intCast(t);
    return 0;
}

/// Decode a FLOAT16 column (FIXED_LEN_BYTE_ARRAY(2), IEEE half) into the f64
/// lane: each 2-byte little-endian half → `f16` → `f64`. Mirrors the decimal
/// path so float16 flows through numeric agg/filter/expr.
pub fn decodeFloat16ColumnAsF64(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(f64) {
    const cb = try decodeFlbaColumnWithOptions(arena, chunk, codec, levels, num_leaves, 2, decode_options);
    const out = try arena.alloc(f64, cb.values.len);
    for (cb.values, 0..) |bytes, i| {
        if (bytes.len >= 2) {
            const bits = std.mem.readInt(u16, bytes[0..2], .little);
            out[i] = @floatCast(@as(f16, @bitCast(bits)));
        } else {
            out[i] = 0;
        }
    }
    return .{
        .values = out,
        .def_levels = cb.def_levels,
        .max_def = cb.max_def,
        .rep_levels = cb.rep_levels,
        .has_nulls = cb.has_nulls,
    };
}

/// Decode an INT32 column whose values are unsigned, zero-extending each into
/// an `i64` lane (`u32` → `i64`). See `schema.isUnsignedIntTo32`.
fn decodeU32ColumnAsI64(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT(i64) {
    const c32 = try decodeColumnTWithOptions(i32, arena, chunk, codec, levels, num_leaves, decode_options);
    const out = try arena.alloc(i64, c32.values.len);
    for (c32.values, 0..) |v, i| out[i] = @as(i64, @as(u32, @bitCast(v)));
    return .{
        .values = out,
        .def_levels = c32.def_levels,
        .max_def = c32.max_def,
        .rep_levels = c32.rep_levels,
        .has_nulls = c32.has_nulls,
    };
}

/// FIXED_LEN_BYTE_ARRAY columns decode to raw fixed-width `[]const u8` slices
/// (no per-value length prefix). `type_length` is the column's fixed byte width
/// from its SchemaElement. Decimal-FLBA has its own f64 path (decimal.zig); this
/// is the raw-bytes path — UUID / Float16 / fixed binary.
pub fn decodeFlbaColumn(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
) !filter_eval.ColumnT([]const u8) {
    return decodeFlbaColumnWithOptions(arena, chunk, codec, levels, num_leaves, type_length, .{});
}

pub fn decodeFlbaColumnWithOptions(
    arena: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    num_leaves: usize,
    type_length: usize,
    decode_options: DecodeOptions,
) !filter_eval.ColumnT([]const u8) {
    var reader = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        chunk,
        codec,
        levels,
        arena,
        decode_options,
        null,
    );
    reader.type_length = type_length;
    return decodeWithReader([]const u8, &reader, num_leaves);
}

fn decodeWithReader(
    comptime T: type,
    reader: *column_mod.ColumnChunkReader(T),
    num_leaves: usize,
) !filter_eval.ColumnT(T) {
    const leaves = try reader.readAll(num_leaves);
    return .{
        .values = leaves.values,
        .def_levels = leaves.def_levels,
        .max_def = @intCast(reader.levels.max_def),
        .rep_levels = leaves.rep_levels,
        .max_rep = @intCast(reader.levels.max_rep),
        .has_nulls = leaves.has_nulls,
    };
}

test "checkRowShape ties flat and repeated columns to the row count" {
    const testing = std.testing;
    try checkRowShape(.{ .i32 = .{ .values = &.{ 1, 2, 3 } } }, 3);
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(.{ .i32 = .{ .values = &.{ 1, 2 } } }, 3));
    // Repeated: three records, the second holding two entries.
    const rep = filter_eval.Batch.Column{ .i32 = .{
        .values = &.{ 1, 2, 3, 4 },
        .def_levels = &.{ 1, 1, 1, 1 },
        .max_def = 1,
        .rep_levels = &.{ 0, 0, 1, 0 },
        .max_rep = 1,
    } };
    try checkRowShape(rep, 3);
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(rep, 4));
    const no_start = filter_eval.Batch.Column{ .i32 = .{ .values = &.{1}, .rep_levels = &.{1}, .max_rep = 1 } };
    try testing.expectError(error.ColumnRowCountMismatch, checkRowShape(no_start, 1));
}


pub fn valuesCol(comptime T: type, v: anytype, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{
        .values = v.values,
        .def_levels = v.def_levels,
        .max_def = @intCast(levels.max_def),
        .has_nulls = v.has_nulls,
    };
}


pub fn codedCol(comptime T: type, c: anytype, dict: []const T, levels: schema.Levels) filter_eval.ColumnT(T) {
    return .{
        .values = &.{},
        .codes = c.codes,
        .dict = dict,
        .def_levels = c.def_levels,
        .max_def = @intCast(levels.max_def),
        .has_nulls = c.has_nulls,
    };
}


/// Decode one DECIMAL column's chunk to unscaled i128 integers (the
/// lossless output lane). Re-derives the chunk window the same way the
/// batch-decode loop does. Caller guarantees the column is an
/// INT32/INT64-backed decimal.
/// Leaf `ci`'s DECIMAL kind, or null when it is no decimal.
pub fn decimalKind(rg: *const schema.RowGroup, meta: *const schema.FileMetaData, ci: usize) ?decimal_mod.Kind {
    const cm = rg.columns.items[ci].meta_data orelse return null;
    const se = meta.getColumnSchema(cm.path_in_schema.items) orelse return null;
    return decimal_mod.exactKind(&se);
}

pub fn decodeDecimalI128(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    kept_ci: usize,
    decode_options_in: DecodeOptions,
) !filter_eval.ColumnT(i128) {
    enterChunk(rg, kept_ci);
    const cm = rg.columns.items[kept_ci].meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    const chunk = src.bytes[buf_off .. buf_off + len];

    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);
    const se = meta.getColumnSchema(cm.path_in_schema.items) orelse return error.SchemaLookupFailed;
    const kind = decimal_mod.exactKind(&se) orelse return error.SchemaLookupFailed;
    return decimal_mod.decodeColumnAsI128WithOptions(
        ra,
        chunk,
        cm.codec,
        levels,
        n_leaves,
        kind,
        decode_options,
    );
}


pub fn codedOrValues(
    comptime T: type,
    comptime tag: std.meta.Tag(filter_eval.Batch.Column),
    ra: std.mem.Allocator,
    chunk: []const u8,
    cm: schema.ColumnMetaData,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !?filter_eval.Batch.Column {
    const r = (try decodeCodes(T, ra, chunk, cm.codec, levels, n_leaves, decode_options, scratch)) orelse return null;
    return switch (r) {
        .codes => |c| @unionInit(filter_eval.Batch.Column, @tagName(tag), codedCol(T, c, c.dict, levels)),
        .values => |v| @unionInit(filter_eval.Batch.Column, @tagName(tag), valuesCol(T, v, levels)),
    };
}

pub fn decodeCodes(
    comptime T: type,
    ra: std.mem.Allocator,
    chunk: []const u8,
    codec: schema.CompressionCodec,
    levels: schema.Levels,
    n_leaves: usize,
    decode_options: DecodeOptions,
    scratch: ?*DecodeScratch,
) !?column_mod.ColumnChunkReader(T).CodedOrValues {
    var reader = column_mod.ColumnChunkReader(T).initWithOptions(chunk, codec, levels, ra, decode_options, scratch);
    return reader.readAllCodes(n_leaves);
}


/// Decode column `ci` of a row group into the batch lane the write path keeps it in.
pub fn decodeWriteColumn(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
    decode_options_in: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    enterChunk(rg, ci);
    const num_rows: usize = @intCast(rg.num_rows);
    const chunk_span = profile.Span.begin(prof);
    const col = &rg.columns.items[ci];
    const cm = col.meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    // Programmer invariant: the chunk's absolute start is at or past
    // this byte window's origin, so the subtraction below can't
    // underflow. A malformed file is caught by the bounds error on
    // the next line; this guards our own RGSrc/byte_origin math.
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    const chunk = src.bytes[buf_off .. buf_off + len];

    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const n_leaves: usize = @intCast(cm.num_values);

    // DECIMAL columns, whatever their backing: the f64 view, with the exact unscaled values beside it that filters,
    // expressions and aggregates read (`decodeColumnExact`).
    const schema_elem = meta.getColumnSchema(cm.path_in_schema.items);
    const dec_kind: ?decimal_mod.Kind = if (schema_elem) |se| decimal_mod.exactKind(&se) else null;

    const decoded: filter_eval.Batch.Column = if (dec_kind) |k| .{
        .f64 = try decimal_mod.decodeColumnExact(ra, chunk, cm.codec, levels, n_leaves, k, decode_options, null),
    } else if (schema_elem != null and schema.isFloat16(schema_elem.?)) .{
        // FLOAT16 (FLBA(2), IEEE half) → f64 lane for numeric agg/filter.
        .f64 = try decodeFloat16ColumnAsF64(ra, chunk, cm.codec, levels, n_leaves, decode_options),
    } else switch (cm.type) {
        .INT32 => .{
            .i32 = try decodeColumnTWithOptions(i32, ra, chunk, cm.codec, levels, n_leaves, decode_options),
        },
        .INT64 => .{
            .i64 = try decodeColumnTWithOptions(i64, ra, chunk, cm.codec, levels, n_leaves, decode_options),
        },
        .FLOAT => .{
            .f32 = try decodeColumnTWithOptions(f32, ra, chunk, cm.codec, levels, n_leaves, decode_options),
        },
        .DOUBLE => .{
            .f64 = try decodeColumnTWithOptions(f64, ra, chunk, cm.codec, levels, n_leaves, decode_options),
        },
        .BYTE_ARRAY => .{
            .string = try decodeColumnTWithOptions(
                []const u8,
                ra,
                chunk,
                cm.codec,
                levels,
                n_leaves,
                decode_options,
            ),
        },
        .BOOLEAN => .{
            .boolean = try decodeColumnTWithOptions(bool, ra, chunk, cm.codec, levels, n_leaves, decode_options),
        },
        // INT96 (legacy Spark/Impala timestamp) → i64 epoch-nanoseconds.
        .INT96 => .{ .i64 = try int96_mod.decodeColumnAsI64Nanos(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            decode_options.page_bytes_limit,
        ) },
        // FIXED_LEN_BYTE_ARRAY (non-decimal — decimal handled above): raw
        // fixed-width bytes (UUID / Float16 / fixed binary).
        .FIXED_LEN_BYTE_ARRAY => .{ .string = try decodeFlbaColumnWithOptions(
            ra,
            chunk,
            cm.codec,
            levels,
            n_leaves,
            flbaWidth(schema_elem),
            decode_options,
        ) },
    };
    try checkRowShape(decoded, num_rows);
    if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(0, 0, ci, cm));
    return decoded;
}

/// The dictionary of flat BYTE_ARRAY chunk `ci`, on `a`, when every one of its data pages is dictionary-encoded, so
/// that each of its values is an entry; null otherwise. Reads no data page's body.
pub fn chunkDictionary(
    a: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
) !?[]const []const u8 {
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    if (cm.type != .BYTE_ARRAY) return null;
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    if (levels.max_rep > 0) return null;
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const len: usize = @intCast(cm.total_compressed_size);
    invariant.assert(start >= @as(usize, @intCast(src.byte_origin)));
    const buf_off = start - @as(usize, @intCast(src.byte_origin));
    if (buf_off + len > src.bytes.len) return error.MissingChunkBytes;
    var r = column_mod.ColumnChunkReader([]const u8).initWithOptions(
        src.bytes[buf_off..][0..len],
        cm.codec,
        levels,
        a,
        column_mod.chunkOptions(.{}, meta, cm),
        null,
    );
    return r.dictionaryOfEveryPage();
}

/// `decodeWriteColumn`, but a dictionary-encoded chunk of a type the write path keeps unconverted decodes to codes
/// (see `ColumnChunkReader.readAllCodes`); anything else decodes as values.
pub fn decodeWriteColumnMaybeCoded(
    ra: std.mem.Allocator,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    src: RGSrc,
    ci: usize,
    decode_options_in: DecodeOptions,
    prof: ?*profile.Thread,
) !filter_eval.Batch.Column {
    enterChunk(rg, ci);
    const cm = rg.columns.items[ci].meta_data orelse return error.ColumnMetaMissing;
    const decode_options = column_mod.chunkOptions(decode_options_in, meta, cm);
    const levels = meta.getColumnLevels(cm.path_in_schema.items);
    const se = meta.getColumnSchema(cm.path_in_schema.items);
    const converted = if (se) |e| decimal_mod.exactKind(&e) != null or schema.isFloat16(e) else false;
    if (!decode_options.encoded or converted or levels.max_rep > 0)
        return decodeWriteColumn(ra, rg, meta, src, ci, decode_options, prof);
    const start: usize = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
    const chunk = try sourceBytes(src, .{ .start = start, .len = @intCast(cm.total_compressed_size) });
    const n_leaves: usize = @intCast(cm.num_values);
    const chunk_span = profile.Span.begin(prof);
    const coded: ?filter_eval.Batch.Column = switch (cm.type) {
        .INT32 => try codedOrValues(i32, .i32, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .INT64 => try codedOrValues(i64, .i64, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .FLOAT => try codedOrValues(f32, .f32, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .DOUBLE => try codedOrValues(f64, .f64, ra, chunk, cm, levels, n_leaves, decode_options, null),
        .BYTE_ARRAY => try codedOrValues([]const u8, .string, ra, chunk, cm, levels, n_leaves, decode_options, null),
        else => null,
    };
    const col = coded orelse return decodeWriteColumn(ra, rg, meta, src, ci, decode_options, prof);
    try checkRowShape(col, @intCast(rg.num_rows));
    if (chunk_span.t != null) chunk_span.end(.column_chunk, chunkArgs(0, 0, ci, cm));
    return col;
}

test "chunkDictionary gives a string chunk's dictionary only when every data page is coded by it" {
    const testing = std.testing;
    for ([_][]const u8{ "ci/fixtures/parquet/topn_gather_a.parquet", "ci/fixtures/parquet/topn_gather_b.parquet" }) |path| {
        const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
            if (err == error.FileNotFound) return error.SkipZigTest;
            return err;
        };
        defer testing.allocator.free(bytes);
        var meta = try metadata.open(testing.allocator, bytes);
        defer meta.deinit(testing.allocator);
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const a = arena_state.allocator();
        const src: RGSrc = .{ .bytes = bytes, .byte_origin = 0 };
        for (meta.row_groups.items) |*rg| {
            // `s` outgrows its dictionary page and turns PLAIN; `c` stays coded; `id` is not a string.
            try testing.expect(try chunkDictionary(a, rg, &meta, src, 2) == null);
            try testing.expect(try chunkDictionary(a, rg, &meta, src, 0) == null);
            const dict = (try chunkDictionary(a, rg, &meta, src, 7)).?;
            // Eight values (those ending 0 or 5 fall on the rows holding '') and the empty string; NULL is no entry.
            try testing.expectEqual(@as(usize, 9), dict.len);
            var whole = (try SliceReader.init(a, testing.allocator, rg, &meta, src, 7, .{})).?;
            defer whole.deinit();
            const col = (try whole.next(a, @intCast(rg.num_rows))).string;
            for (col.values, 0..) |v, i| {
                if (col.def_levels.?[i] < col.max_def) continue;
                for (dict) |d| {
                    if (std.mem.eql(u8, d, v)) break;
                } else return error.TestValueNotInDictionary;
            }
        }
    }
}

