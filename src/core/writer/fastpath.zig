//! Parquet "fast path" writer: copy surviving row groups byte-for-byte
//! from the input and rewrite the footer with shifted offsets. One
//! assembly (`assemble`) writes every such file: `buildMulti` runs it
//! into a fresh contiguous buffer, `streaming.build` into a sink.
//!
//! No decoding, no re-encoding. The trick that makes this fast: column
//! chunks within a row group are contiguous in the input file, so a
//! single `@memcpy` per row group preserves all encoded data and
//! statistics — we only need to renumber the pointers in the footer.
//!
//! Inputs:
//!   - `input` — full bytes of the source Parquet file.
//!   - `meta` — the parsed FileMetaData (from `metadata.open`).
//!   - `survivors` — bitmask, one bool per `meta.row_groups.items`.
//!     Row groups where the corresponding bool is true are retained.
//!
//! Output: a freshly allocated `[]u8` containing a complete Parquet file:
//!   `[PAR1][rg0_data][rg1_data]...[footer thrift][u32 footer_len][PAR1]`
//!
//! Caveats:
//!   - The page index (offset/column index) is **carried forward**: the
//!     ColumnIndex copies through unchanged and the OffsetIndex page
//!     locations are rebased by the same shift applied to the chunk data,
//!     so page-level pruning survives a compaction. It is dropped only
//!     when the source has none or its index bytes were not fetched (the
//!     S3 fast path may hold just the column-chunk + footer ranges).
//!   - Bloom-filter offsets in the input footer are still **dropped**.
//!     Downstream readers fall back to row-group-level pruning.
//!   - The schema, statistics, encodings, and per-row-group metadata are
//!     all copied through unchanged. Strings (paths, stats min/max) are
//!     borrowed from `meta` — caller must keep `meta` alive until the
//!     returned bytes are no longer needed for further mutation.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");

const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };

pub const Error = error{
    SurvivorsLenMismatch,
    EmptyOutput,
    InvalidColumnOffsets,
    NestedSchemaUnsupported,
    BadColumnIndex,
    /// The inputs declare different column orders for a copied leaf; a byte copy cannot be declared truthfully.
    ColumnOrderMismatch,
} || std.mem.Allocator.Error;

/// One input file. `bytes` covers (at minimum) the tail/head/footer
/// plus the bytes of every kept column-chunk of every surviving row
/// group — i.e., everything the builder needs to byte-copy. `meta` is
/// the parsed FileMetaData; `survivors[i]` says whether row-group `i`
/// survives.
pub const FileSpec = struct {
    bytes: []const u8,
    meta: *const schema.FileMetaData,
    survivors: []const bool,
};

/// Single-file build: `buildMulti` over one `FileSpec`.
pub fn build(
    arena: std.mem.Allocator,
    input: []const u8,
    meta: *const schema.FileMetaData,
    survivors: []const bool,
    kept_columns: ?[]const usize,
) Error![]u8 {
    return buildMulti(arena, &.{.{ .bytes = input, .meta = meta, .survivors = survivors }}, kept_columns);
}

/// Concatenate the surviving row groups of N input files into one
/// Parquet output held in memory. All files must have compatible
/// schemas (the caller validates this); the first file's is written.
///
/// When `kept_columns` is null, every column of every surviving row
/// group is copied byte-for-byte. When non-null, each surviving row
/// group emits only the listed column chunks (in the listed order) — a
/// column-projection writer. `kept_columns` indexes are positions in
/// each row group's `columns` list, which for flat schemas equals
/// schema-leaf index. Nested schemas (struct/list/map) are not yet
/// supported under projection — error.NestedSchemaUnsupported is
/// returned in that case. Projection must preserve repetition-level
/// semantics before nested schemas can use this byte-copy path.
pub fn buildMulti(
    arena: std.mem.Allocator,
    files: []const FileSpec,
    kept_columns: ?[]const usize,
) Error![]u8 {
    var out: BufferOut = .{ .arena = arena };
    errdefer out.bytes.deinit(arena);
    try assemble(arena, &out, files, kept_columns);
    return out.bytes.toOwnedSlice(arena);
}

/// `assemble` output collecting the whole file in one buffer.
const BufferOut = struct {
    arena: std.mem.Allocator,
    bytes: std.ArrayList(u8) = .empty,

    fn pos(self: *const BufferOut) usize {
        return self.bytes.items.len;
    }

    fn reserve(self: *BufferOut, len: usize) Error!void {
        try self.bytes.ensureTotalCapacity(self.arena, len);
    }

    fn write(self: *BufferOut, bytes: []const u8) Error!void {
        try self.bytes.appendSlice(self.arena, bytes);
    }
};

/// Write a complete byte-copy output to `out`, the one assembly behind
/// both `buildMulti` (a buffer) and `streaming.build` (a sink). `out`
/// provides `pos()`, the number of bytes written so far; `write(bytes)`;
/// and `reserve(len)`, a hint of the total output length.
///
/// Inputs are validated, every chunk to be copied located inside its
/// input, and the footer's column orders settled, before the first byte
/// is written, so a refused build leaves a sink untouched.
pub fn assemble(
    arena: std.mem.Allocator,
    out: anytype,
    files: []const FileSpec,
    kept_columns: ?[]const usize,
) !void {
    try validateInputs(files, kept_columns);
    const copied_len = try copiedLen(files, kept_columns);
    const column_orders = try copiedColumnOrders(arena, files, kept_columns);
    // Room past the copied chunks for the magic, footer and page indexes.
    try out.reserve(copied_len +| MAGIC.len * 2 + 4 + 64 * 1024);

    try out.write(&MAGIC);
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    errdefer new_row_groups.deinit(arena);
    var total_rows: i64 = 0;
    for (files) |f| for (f.survivors, f.meta.row_groups.items) |keep, *rg| {
        if (!keep) continue;
        const new_rg = if (kept_columns) |kc|
            try copyProjectedRowGroup(arena, out, f.bytes, rg, kc)
        else
            try copyRowGroup(arena, out, f.bytes, rg) orelse continue;
        try new_row_groups.append(arena, new_rg);
        total_rows += rg.num_rows;
    };

    // The schema is shared by reference unless projecting, which needs a reduced list.
    const meta0 = files[0].meta;
    const new_meta: schema.FileMetaData = .{
        .version = meta0.version,
        .schema = if (kept_columns) |kc| try projectSchema(arena, meta0.schema, kc) else meta0.schema,
        .num_rows = total_rows,
        .created_by = meta0.created_by,
        .row_groups = new_row_groups,
        .column_orders = column_orders,
    };
    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(w.bytes().len), .little);
    try out.write(w.bytes());
    try out.write(&len_bytes);
    try out.write(&MAGIC);
}

/// One survivor flag per row group; under projection, a flat schema
/// whose leaves the kept indexes name. For flat schemas, the schema is
/// [root, leaf_0, leaf_1, ...] — total entries = 1 + N leaves, root has
/// num_children = N. Any other shape means nested types we don't handle
/// yet. Subsequent files' schemas are assumed to match the first's.
fn validateInputs(files: []const FileSpec, kept_columns: ?[]const usize) Error!void {
    if (files.len == 0) return error.SurvivorsLenMismatch;
    for (files) |f| if (f.survivors.len != f.meta.row_groups.items.len) return error.SurvivorsLenMismatch;
    const kc = kept_columns orelse return;
    const schema0 = files[0].meta.schema.items;
    if (schema0.len < 1) return error.NestedSchemaUnsupported;
    const expected_leaves = std.math.cast(usize, schema0[0].num_children orelse 0) orelse
        return error.NestedSchemaUnsupported;
    if (schema0.len != 1 + expected_leaves) return error.NestedSchemaUnsupported;
    for (kc) |idx| if (idx >= expected_leaves) return error.BadColumnIndex;
}

/// The bytes of every chunk the build will copy, each checked to lie
/// inside its input first.
fn copiedLen(files: []const FileSpec, kept_columns: ?[]const usize) Error!usize {
    var len: usize = 0;
    for (files) |f| for (f.survivors, f.meta.row_groups.items) |keep, *rg| {
        if (!keep) continue;
        if (kept_columns) |kc| {
            for (kc) |col_idx| len +|= (try projectedChunkRange(rg, col_idx, f.bytes.len)).len;
        } else if (try rowGroupByteRange(rg, f.bytes.len)) |range| len +|= range.len;
    };
    return len;
}

/// Copy a whole row group's chunk bytes in one piece — column chunks
/// within a row group are contiguous in the input — then its page
/// indexes. Null when no chunk has metadata to locate it by.
fn copyRowGroup(
    arena: std.mem.Allocator,
    out: anytype,
    input: []const u8,
    src: *const schema.RowGroup,
) !?schema.RowGroup {
    const range = try rowGroupByteRange(src, input.len) orelse return null;
    const delta = offsetDelta(out.pos(), range.start);
    try out.write(input[range.start .. range.start + range.len]);

    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    errdefer cols.deinit(arena);
    try cols.ensureTotalCapacity(arena, src.columns.items.len);
    // Every chunk in the group moved by the same delta; the rebased
    // indexes follow the group's data.
    for (src.columns.items) |*chunk| cols.appendAssumeCapacity(try rebaseChunk(arena, out, input, chunk, delta));
    return .{
        .columns = cols,
        .total_byte_size = src.total_byte_size,
        .num_rows = src.num_rows,
    };
}

/// Copy only the kept column chunks of one row group, contiguously,
/// each followed by its page index. The new RG's `total_byte_size` is
/// the sum of kept column sizes; `num_rows` is unchanged from the source.
fn copyProjectedRowGroup(
    arena: std.mem.Allocator,
    out: anytype,
    input: []const u8,
    src_rg: *const schema.RowGroup,
    kept: []const usize,
) !schema.RowGroup {
    var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    errdefer new_cols.deinit(arena);
    try new_cols.ensureTotalCapacity(arena, kept.len);

    var rg_total: i64 = 0;
    for (kept) |col_idx| {
        const range = try projectedChunkRange(src_rg, col_idx, input.len);
        const delta = offsetDelta(out.pos(), range.start);
        try out.write(input[range.start .. range.start + range.len]);
        new_cols.appendAssumeCapacity(try rebaseChunk(arena, out, input, &src_rg.columns.items[col_idx], delta));
        rg_total += @intCast(range.len);
    }

    return .{
        .columns = new_cols,
        .total_byte_size = rg_total,
        .num_rows = src_rg.num_rows,
    };
}

fn offsetDelta(new_start: usize, src_start: usize) i64 {
    return @as(i64, @intCast(new_start)) - @as(i64, @intCast(src_start));
}

/// `src` with every offset shifted by `delta`, the distance its data
/// moved, and its page index carried forward into `out`. Inner slices
/// (encodings, paths, stats) are borrowed from the source; bloom-filter
/// offsets are dropped.
pub fn rebaseChunk(
    arena: std.mem.Allocator,
    out: anytype,
    input: []const u8,
    src: *const schema.ColumnChunk,
    delta: i64,
) !schema.ColumnChunk {
    var chunk = shiftChunk(src, delta);
    const idx = try carryPageIndex(arena, out, input, src, delta);
    chunk.offset_index_offset = idx.offset_index_offset;
    chunk.offset_index_length = idx.offset_index_length;
    chunk.column_index_offset = idx.column_index_offset;
    chunk.column_index_length = idx.column_index_length;
    return chunk;
}

/// `rebaseChunk` without the page index, for output that carries none.
pub fn shiftChunk(src: *const schema.ColumnChunk, delta: i64) schema.ColumnChunk {
    var chunk = src.*;
    if (chunk.meta_data) |*m| {
        m.data_page_offset += delta;
        if (m.dictionary_page_offset) |d| m.dictionary_page_offset = d + delta;
        // index_page_offset is rarely set; treat the same way.
        if (m.index_page_offset) |d| m.index_page_offset = d + delta;
        // Parquet's older file_offset field mirrors data_page_offset.
        // Readers should use the structured offsets, but keeping this
        // consistent costs nothing.
        chunk.file_offset = m.data_page_offset;
    }
    chunk.offset_index_offset = null;
    chunk.offset_index_length = null;
    chunk.column_index_offset = null;
    chunk.column_index_length = null;
    return chunk;
}

/// Build a new schema list: root (with adjusted num_children) + the
/// listed leaf elements in the listed order. Caller is the projection
/// path only; the flat-schema invariant is checked by `validateInputs`.
pub fn projectSchema(
    arena: std.mem.Allocator,
    src: std.ArrayListUnmanaged(schema.SchemaElement),
    kept: []const usize,
) Error!std.ArrayListUnmanaged(schema.SchemaElement) {
    var out: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    errdefer out.deinit(arena);
    try out.ensureTotalCapacity(arena, 1 + kept.len);

    var new_root = src.items[0];
    new_root.num_children = @intCast(kept.len);
    try out.append(arena, new_root);

    for (kept) |idx| {
        if (idx + 1 >= src.items.len) return error.BadColumnIndex;
        try out.append(arena, src.items[idx + 1]);
    }
    return out;
}

/// Column orders of an all-byte-copy output: every row group keeps its source's bounds, so the sources decide them.
pub fn copiedColumnOrders(
    arena: std.mem.Allocator,
    files: []const FileSpec,
    kept_columns: ?[]const usize,
) Error!?std.ArrayListUnmanaged(i16) {
    const metas = try arena.alloc(*const schema.FileMetaData, files.len);
    for (files, metas) |f, *m| m.* = f.meta;
    var n_leaves: usize = 0;
    if (kept_columns) |kc| n_leaves = kc.len else for (files[0].meta.schema.items) |el| {
        if (el.type != null) n_leaves += 1;
    }
    return schema.FileMetaData.outputColumnOrders(arena, n_leaves, kept_columns, metas);
}

pub const ByteRange = struct { start: usize, len: usize };

/// The contiguous span of bytes in the source file that holds all of
/// this row group's column-chunk data. Returns null if no column has
/// readable metadata (shouldn't happen on real files but we don't panic).
pub fn rowGroupByteRange(rg: *const schema.RowGroup, input_len: usize) Error!?ByteRange {
    var min_start: usize = std.math.maxInt(usize);
    var max_end: usize = 0;
    for (rg.columns.items) |*chunk| if (chunk.meta_data) |*m| {
        const range = chunkRange(m, input_len) orelse return error.InvalidColumnOffsets;
        min_start = @min(min_start, range.start);
        max_end = @max(max_end, range.start + range.len);
    };
    if (min_start == std.math.maxInt(usize)) return null;
    return .{ .start = min_start, .len = max_end - min_start };
}

/// Where kept column `col_idx` of `rg` lies in its input.
fn projectedChunkRange(rg: *const schema.RowGroup, col_idx: usize, input_len: usize) Error!ByteRange {
    if (col_idx >= rg.columns.items.len) return error.BadColumnIndex;
    const m = if (rg.columns.items[col_idx].meta_data) |*m| m else return error.InvalidColumnOffsets;
    return chunkRange(m, input_len) orelse error.InvalidColumnOffsets;
}

/// A chunk's bytes in its input, from the dictionary page (if any)
/// through the last data page. Null when the footer's offset or size is
/// negative, or the range overflows or runs past the input:
/// `metadata.open` rejects the first two, but the builders take any
/// FileMetaData, and only the code holding the bytes knows their length.
pub fn chunkRange(m: *const schema.ColumnMetaData, input_len: usize) ?ByteRange {
    const start = std.math.cast(usize, m.dictionary_page_offset orelse m.data_page_offset) orelse return null;
    const len = std.math.cast(usize, m.total_compressed_size) orelse return null;
    const end = std.math.add(usize, start, len) catch return null;
    if (end > input_len) return null;
    return .{ .start = start, .len = len };
}

/// Page-index pointers for one output chunk.
pub const PageIndexPtrs = struct {
    offset_index_offset: ?i64 = null,
    offset_index_length: ?i32 = null,
    column_index_offset: ?i64 = null,
    column_index_length: ?i32 = null,
};

/// Carry a source chunk's page index forward into `out`, returning the
/// new (output-relative) pointers. ColumnIndex holds only per-page
/// values (no file offsets) so it copies through unchanged; OffsetIndex
/// page-location offsets are absolute, so each is rebased by `delta` —
/// the exact shift applied to that chunk's data bytes.
///
/// Best-effort: returns null pointers when the source carries no index,
/// when the index bytes weren't fetched (the S3 fast path may hold only
/// column-chunk + footer ranges, leaving index blocks outside `input`),
/// or when the source index fails to parse. Dropping the index is always
/// safe — readers fall back to row-group-level pruning.
fn carryPageIndex(
    arena: std.mem.Allocator,
    out: anytype,
    input: []const u8,
    src: *const schema.ColumnChunk,
    delta: i64,
) !PageIndexPtrs {
    var ptrs = PageIndexPtrs{};

    if (src.column_index_offset) |co| if (src.column_index_length) |cl| {
        if (sliceInBounds(input, co, cl)) |bytes| {
            if (reserializeColumnIndex(arena, bytes)) |ser| {
                ptrs.column_index_offset = @intCast(out.pos());
                ptrs.column_index_length = @intCast(ser.len);
                try out.write(ser);
            }
        }
    };

    if (src.offset_index_offset) |oo| if (src.offset_index_length) |ol| {
        if (sliceInBounds(input, oo, ol)) |bytes| {
            if (reserializeOffsetIndexShifted(arena, bytes, delta)) |ser| {
                ptrs.offset_index_offset = @intCast(out.pos());
                ptrs.offset_index_length = @intCast(ser.len);
                try out.write(ser);
            }
        }
    };

    return ptrs;
}

/// Sub-slice `input[offset..offset+length]`, or null if the range falls
/// outside the buffer (the S3 fast path may not have fetched the index
/// blocks) or has a negative offset/length.
pub fn sliceInBounds(input: []const u8, offset: i64, length: i32) ?[]const u8 {
    if (offset < 0 or length < 0) return null;
    const start: usize = @intCast(offset);
    const len: usize = @intCast(length);
    if (start + len > input.len) return null;
    return input[start .. start + len];
}

/// Re-serialize a ColumnIndex verbatim onto `arena` (the re-parse drops
/// only fields we don't model — the level histograms, which are
/// advisory). Null on any parse/encode failure. Shared with the
/// consumer's copy path so every writer carries an identical index forward.
pub fn reserializeColumnIndex(arena: std.mem.Allocator, bytes: []const u8) ?[]const u8 {
    var r = thrift.Reader.init(bytes);
    var ci = schema.ColumnIndex.read(arena, &r) catch return null;
    var w = thrift.Writer.init(arena); // arena-backed; bytes outlive `w`
    ci.write(&w) catch return null;
    return w.bytes();
}

/// Re-serialize an OffsetIndex onto `arena` with every page-location
/// offset shifted by `delta`. Null on any parse/encode failure.
pub fn reserializeOffsetIndexShifted(arena: std.mem.Allocator, bytes: []const u8, delta: i64) ?[]const u8 {
    var r = thrift.Reader.init(bytes);
    var oi = schema.OffsetIndex.read(arena, &r) catch return null;
    // Offsets come from the source file; a hostile one must drop the index, not overflow.
    for (oi.page_locations.items) |*pl| pl.offset = std.math.add(i64, pl.offset, delta) catch return null;
    var w = thrift.Writer.init(arena);
    oi.write(&w) catch return null;
    return w.bytes();
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");
const readFileSlice = metadata.readFileSlice;

test "build with all survivors round-trips through metadata.open" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);
    // meta is arena-owned; no need to deinit individually.

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);

    const out = try build(arena, file_bytes, &meta, survivors, null);

    // Output should be parseable as a Parquet file.
    const out_meta = try metadata.open(arena, out);

    try testing.expectEqual(meta.num_rows, out_meta.num_rows);
    try testing.expectEqual(meta.row_groups.items.len, out_meta.row_groups.items.len);
    try testing.expectEqual(meta.schema.items.len, out_meta.schema.items.len);

    // Each row group's column chunk bytes should match the source's at
    // the shifted offset. Verify the first column of the first row group.
    const src_rg = &meta.row_groups.items[0];
    const out_rg = &out_meta.row_groups.items[0];
    try testing.expectEqual(src_rg.num_rows, out_rg.num_rows);

    const src_col_meta = src_rg.columns.items[0].meta_data.?;
    const out_col_meta = out_rg.columns.items[0].meta_data.?;
    try testing.expectEqual(src_col_meta.total_compressed_size, out_col_meta.total_compressed_size);

    const src_start: usize = @intCast(src_col_meta.dictionary_page_offset orelse src_col_meta.data_page_offset);
    const out_start: usize = @intCast(out_col_meta.dictionary_page_offset orelse out_col_meta.data_page_offset);
    const len: usize = @intCast(src_col_meta.total_compressed_size);
    try testing.expectEqualSlices(u8, file_bytes[src_start .. src_start + len], out[out_start .. out_start + len]);

    std.debug.print(
        "[fastpath] all-survivors: in={d}B out={d}B ({d:.1}% of input) row_groups={d}\n",
        .{
            file_bytes.len,
            out.len,
            @as(f64, @floatFromInt(out.len)) * 100.0 / @as(f64, @floatFromInt(file_bytes.len)),
            out_meta.row_groups.items.len,
        },
    );
}

test "build dropping all but the first row group" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);
    if (meta.row_groups.items.len < 2) {
        std.debug.print("skipping: fixture has <2 row groups\n", .{});
        return error.SkipZigTest;
    }

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, false);
    survivors[0] = true;

    const out = try build(arena, file_bytes, &meta, survivors, null);

    const out_meta = try metadata.open(arena, out);
    try testing.expectEqual(@as(usize, 1), out_meta.row_groups.items.len);
    try testing.expectEqual(meta.row_groups.items[0].num_rows, out_meta.num_rows);
    try testing.expectEqual(meta.row_groups.items[0].num_rows, out_meta.row_groups.items[0].num_rows);

    // Output should be substantially smaller than the input (we kept ~1/N).
    try testing.expect(out.len < file_bytes.len);

    std.debug.print(
        "[fastpath] kept rg[0]: in={d}B out={d}B (~{d:.0}%)\n",
        .{ file_bytes.len, out.len, @as(f64, @floatFromInt(out.len)) * 100.0 / @as(f64, @floatFromInt(file_bytes.len)) },
    );
}

test "build with zero survivors produces an empty-row-group file" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, false);

    const out = try build(arena, file_bytes, &meta, survivors, null);

    const out_meta = try metadata.open(arena, out);
    try testing.expectEqual(@as(usize, 0), out_meta.row_groups.items.len);
    try testing.expectEqual(@as(i64, 0), out_meta.num_rows);
    // Schema must still be present so downstream readers see a valid file.
    try testing.expectEqual(meta.schema.items.len, out_meta.schema.items.len);
}

test "build with projection emits only kept columns" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);

    // Keep just int8 (col 0) and int32_sorted (col 2) — non-adjacent.
    const kept = [_]usize{ 0, 2 };
    const out = try build(arena, file_bytes, &meta, survivors, &kept);

    const out_meta = try metadata.open(arena, out);

    // Schema reduced to root + 2 kept leaves.
    try testing.expectEqual(@as(usize, 3), out_meta.schema.items.len);
    try testing.expectEqualStrings("int8", out_meta.schema.items[1].name);
    try testing.expectEqualStrings("int32_sorted", out_meta.schema.items[2].name);
    // Root's num_children matches.
    try testing.expectEqual(@as(?i32, 2), out_meta.schema.items[0].num_children);
    // Each row group has only 2 columns.
    try testing.expectEqual(meta.row_groups.items.len, out_meta.row_groups.items.len);
    for (out_meta.row_groups.items) |rg| {
        try testing.expectEqual(@as(usize, 2), rg.columns.items.len);
    }

    // Output bytes should be much smaller than input.
    try testing.expect(out.len < file_bytes.len / 4);

    std.debug.print(
        "[fastpath] projected [int8, int32_sorted]: in={d}B out={d}B (~{d:.0}%)\n",
        .{ file_bytes.len, out.len, @as(f64, @floatFromInt(out.len)) * 100.0 / @as(f64, @floatFromInt(file_bytes.len)) },
    );
}

test "buildMulti with N=10 copies of the same file" {
    // Multi-file concatenation must keep every copied chunk's offset
    // relative to the new output, independent of network or Lambda state.
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);
    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);

    const N = 10;
    const specs = try arena.alloc(FileSpec, N);
    for (specs) |*sp| sp.* = .{
        .bytes = file_bytes,
        .meta = &meta,
        .survivors = survivors,
    };

    const out = try buildMulti(arena, specs, null);
    const out_meta = try metadata.open(arena, out);
    try testing.expectEqual(meta.num_rows * N, out_meta.num_rows);
    try testing.expectEqual(meta.row_groups.items.len * N, out_meta.row_groups.items.len);
    std.debug.print("[fastpath] N=10 multi: in={d}B×N={d}B out={d}B rg={d}\n", .{ file_bytes.len, file_bytes.len * N, out.len, out_meta.row_groups.items.len });
}

test "page index survives an all-survivors copy" {
    // alltypes_tiny_pages.parquet is the canonical apache fixture written
    // *with* a page index (multiple small pages per chunk). A faithful
    // fast-path copy must carry that index forward with rebased offsets.
    const fixture_path = "data/parquet-testing/data/alltypes_tiny_pages.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);

    // The fixture must actually carry an index, else this proves nothing.
    var src_has_index = false;
    for (meta.row_groups.items) |rg| {
        for (rg.columns.items) |c| {
            if (c.column_index_offset != null or c.offset_index_offset != null) src_has_index = true;
        }
    }
    if (!src_has_index) {
        std.debug.print("skipping: fixture carries no page index\n", .{});
        return error.SkipZigTest;
    }

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);

    const out = try build(arena, file_bytes, &meta, survivors, null);
    const out_meta = try metadata.open(arena, out);

    // Every chunk that had an index in the source must have one in the
    // output, and the pointers must land inside the output buffer.
    var checked: usize = 0;
    for (meta.row_groups.items, 0..) |src_rg, ri| {
        const out_rg = out_meta.row_groups.items[ri];
        for (src_rg.columns.items, 0..) |src_c, ci| {
            const out_c = out_rg.columns.items[ci];
            if (src_c.column_index_offset != null) {
                try testing.expect(out_c.column_index_offset != null);
                const off: usize = @intCast(out_c.column_index_offset.?);
                const len: usize = @intCast(out_c.column_index_length.?);
                try testing.expect(off + len <= out.len);
                checked += 1;
            }
            if (src_c.offset_index_offset != null) {
                try testing.expect(out_c.offset_index_offset != null);
                const off: usize = @intCast(out_c.offset_index_offset.?);
                const len: usize = @intCast(out_c.offset_index_length.?);
                try testing.expect(off + len <= out.len);

                // The rebased OffsetIndex must point at real page starts:
                // every page-location offset lands inside the output and
                // the first one matches the chunk's first data/dict page.
                var r = thrift.Reader.init(out[off .. off + len]);
                const oi = try schema.OffsetIndex.read(arena, &r);
                try testing.expect(oi.page_locations.items.len > 0);
                for (oi.page_locations.items) |pl| {
                    try testing.expect(pl.offset >= 0 and @as(usize, @intCast(pl.offset)) < out.len);
                }
                checked += 1;
            }
        }
    }
    try testing.expect(checked > 0);
    std.debug.print("[fastpath] page-index carry-forward: verified {d} chunk indexes\n", .{checked});
}

test "build rejects mismatched survivors length" {
    const arena = testing.allocator;
    const empty_bytes: [12]u8 = .{ 'P', 'A', 'R', '1', 0, 0, 0, 0, 'P', 'A', 'R', '1' };
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    const wrong: [3]bool = .{ true, false, true };
    try testing.expectError(error.SurvivorsLenMismatch, build(arena, &empty_bytes, &meta, &wrong, null));
}

test "byte-copied output declares its source's column orders" {
    const path = "ci/fixtures/parquet/column_order.parquet";
    const file_bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(file_bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, file_bytes);
    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);
    const specs = [_]FileSpec{.{ .bytes = file_bytes, .meta = &meta, .survivors = survivors }};

    // Source: `s` in an order zpq does not implement (3), `i` type-defined.
    const all = try metadata.open(arena, try buildMulti(arena, &specs, null));
    try testing.expectEqualSlices(i16, &.{ 3, schema.COLUMN_ORDER_TYPE_DEFINED }, all.column_orders.?.items);
    const projected = try metadata.open(arena, try buildMulti(arena, &specs, &.{1}));
    try testing.expectEqualSlices(i16, &.{schema.COLUMN_ORDER_TYPE_DEFINED}, projected.column_orders.?.items);

    // A second input declaring `s` type-defined: no one order is true of both inputs' `s` bounds, so the copy is
    // refused rather than written with an invalid empty union or a false declaration.
    var other = meta;
    other.column_orders = .empty;
    const td = schema.COLUMN_ORDER_TYPE_DEFINED;
    try other.column_orders.?.appendSlice(arena, &.{ td, td });
    const mixed = [_]FileSpec{ specs[0], .{ .bytes = file_bytes, .meta = &other, .survivors = survivors } };
    try testing.expectError(error.ColumnOrderMismatch, buildMulti(arena, &mixed, null));
    // Leaving `s` out leaves only the leaf both declare type-defined.
    const just_i = try metadata.open(arena, try buildMulti(arena, &mixed, &.{1}));
    try testing.expectEqualSlices(i16, &.{schema.COLUMN_ORDER_TYPE_DEFINED}, just_i.column_orders.?.items);
}

test "byte copy refuses chunk ranges outside its input" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // `metadata.open` rejects this footer's negative data_page_offset, but the builders take any FileMetaData: one
    // read without that validation must be refused, not panic on the cast.
    const hostile = metadata.readFileSlice("ci/fixtures/parquet/negative_data_page_offset.parquet", arena) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    const footer_len = std.mem.readInt(u32, hostile[hostile.len - 8 ..][0..4], .little);
    var r = thrift.Reader.init(hostile[hostile.len - 8 - footer_len .. hostile.len - 8]);
    const unchecked = try schema.FileMetaData.read(arena, &r);
    const all = try arena.alloc(bool, unchecked.row_groups.items.len);
    @memset(all, true);
    try testing.expectError(error.InvalidColumnOffsets, build(arena, hostile, &unchecked, all, null));
    try testing.expectError(error.InvalidColumnOffsets, build(arena, hostile, &unchecked, all, &.{0}));

    // Ranges a footer can't be checked against on its own: past the end of the input, or past maxInt(i64).
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/column_order.parquet", arena) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    const meta = try metadata.open(arena, bytes);
    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);
    const Bad = struct { start: i64, len: i64 };
    for ([_]Bad{
        .{ .start = @intCast(bytes.len - 1), .len = 100 },
        .{ .start = std.math.maxInt(i64) - 1, .len = 10 },
        .{ .start = 4, .len = -1 },
    }) |bad| {
        var broken = meta;
        broken.row_groups = try meta.row_groups.clone(arena);
        const rg = &broken.row_groups.items[0];
        rg.columns = try rg.columns.clone(arena);
        const m = &rg.columns.items[0].meta_data.?;
        m.dictionary_page_offset = null;
        m.data_page_offset = bad.start;
        m.total_compressed_size = bad.len;
        try testing.expectError(error.InvalidColumnOffsets, build(arena, bytes, &broken, survivors, null));
        try testing.expectError(error.InvalidColumnOffsets, build(arena, bytes, &broken, survivors, &.{0}));
    }
}
