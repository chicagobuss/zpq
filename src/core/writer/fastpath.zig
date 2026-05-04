//! Parquet "fast path" writer: copy surviving row groups byte-for-byte
//! from the input, rewrite the footer with shifted offsets, emit the
//! result as a fresh contiguous buffer.
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
//!   - The page index (offset/column index) and bloom-filter offsets in
//!     the input footer are **dropped**. Downstream readers fall back to
//!     row-group-level pruning. Re-add when a workload demands them.
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
} || std.mem.Allocator.Error;

pub fn build(
    arena: std.mem.Allocator,
    input: []const u8,
    meta: *const schema.FileMetaData,
    survivors: []const bool,
) Error![]u8 {
    if (survivors.len != meta.row_groups.items.len) return error.SurvivorsLenMismatch;

    // Pre-size the output buffer. Footer size is unknown but typically
    // 1-2% of file size; reserve generously.
    var bytes_estimate: usize = MAGIC.len * 2 + 4; // leading magic + footer-len + trailing magic
    for (survivors, 0..) |keep, i| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[i];
        const range = rowGroupByteRange(rg) orelse continue;
        bytes_estimate += range.len;
    }
    bytes_estimate += 64 * 1024;

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(arena);
    try out.ensureTotalCapacity(arena, bytes_estimate);

    try out.appendSlice(arena, &MAGIC);

    // For each surviving row group: copy bytes, build a shifted clone
    // for the new footer.
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    errdefer new_row_groups.deinit(arena);

    var total_rows: i64 = 0;
    for (survivors, 0..) |keep, i| {
        if (!keep) continue;
        const rg = &meta.row_groups.items[i];
        const range = rowGroupByteRange(rg) orelse continue;
        if (range.start + range.len > input.len) return error.InvalidColumnOffsets;

        const new_start: usize = out.items.len;
        try out.appendSlice(arena, input[range.start .. range.start + range.len]);

        const delta: i64 = @as(i64, @intCast(new_start)) - @as(i64, @intCast(range.start));
        const cloned = try cloneRowGroupShifted(arena, rg, delta);
        try new_row_groups.append(arena, cloned);
        total_rows += rg.num_rows;
    }

    // Build new FileMetaData. Shallow share schema and created_by — they
    // point into `meta`'s footer buffer.
    const new_meta: schema.FileMetaData = .{
        .version = meta.version,
        .schema = meta.schema,
        .num_rows = total_rows,
        .created_by = meta.created_by,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);

    const footer_start: usize = out.items.len;
    try out.appendSlice(arena, w.bytes());
    const footer_len: u32 = @intCast(out.items.len - footer_start);

    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, footer_len, .little);
    try out.appendSlice(arena, &len_bytes);
    try out.appendSlice(arena, &MAGIC);

    return out.toOwnedSlice(arena);
}

const ByteRange = struct { start: usize, len: usize };

/// The contiguous span of bytes in the source file that holds all of
/// this row group's column-chunk data. Returns null if no column has
/// readable metadata (shouldn't happen on real files but we don't panic).
fn rowGroupByteRange(rg: *const schema.RowGroup) ?ByteRange {
    if (rg.columns.items.len == 0) return null;
    var min_start: u64 = std.math.maxInt(u64);
    var max_end: u64 = 0;
    for (rg.columns.items) |chunk| {
        const m = chunk.meta_data orelse continue;
        const start: u64 = if (m.dictionary_page_offset) |d|
            @intCast(d)
        else
            @intCast(m.data_page_offset);
        const end: u64 = start + @as(u64, @intCast(m.total_compressed_size));
        if (start < min_start) min_start = start;
        if (end > max_end) max_end = end;
    }
    if (min_start == std.math.maxInt(u64)) return null;
    return .{ .start = @intCast(min_start), .len = @intCast(max_end - min_start) };
}

/// Build a new RowGroup whose column chunks have all offsets shifted
/// by `delta`, with page-index / bloom-filter offsets dropped. Inner
/// slices (encodings, paths, stats) are borrowed from the source.
fn cloneRowGroupShifted(
    arena: std.mem.Allocator,
    src: *const schema.RowGroup,
    delta: i64,
) !schema.RowGroup {
    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    errdefer cols.deinit(arena);
    try cols.ensureTotalCapacity(arena, src.columns.items.len);

    for (src.columns.items) |chunk| {
        var new_chunk = chunk; // shallow copy
        // Drop page-index pointers — those bytes aren't carried forward.
        new_chunk.offset_index_offset = null;
        new_chunk.offset_index_length = null;
        new_chunk.column_index_offset = null;
        new_chunk.column_index_length = null;

        if (new_chunk.meta_data) |*m| {
            m.data_page_offset += delta;
            if (m.dictionary_page_offset) |d| m.dictionary_page_offset = d + delta;
            // index_page_offset is rarely set; treat the same way.
            if (m.index_page_offset) |d| m.index_page_offset = d + delta;
        }
        // The legacy file_offset field — set to data_page_offset when
        // present, else 0. Reader doesn't rely on this; we keep the
        // contract simple.
        if (new_chunk.meta_data) |m| {
            new_chunk.file_offset = m.data_page_offset;
        }

        try cols.append(arena, new_chunk);
    }

    return .{
        .columns = cols,
        .total_byte_size = src.total_byte_size,
        .num_rows = src.num_rows,
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

test "build with all survivors round-trips through metadata.open" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return;
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

    const out = try build(arena, file_bytes, &meta, survivors);

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
            return;
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
        return;
    }

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, false);
    survivors[0] = true;

    const out = try build(arena, file_bytes, &meta, survivors);

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
            return;
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

    const out = try build(arena, file_bytes, &meta, survivors);

    const out_meta = try metadata.open(arena, out);
    try testing.expectEqual(@as(usize, 0), out_meta.row_groups.items.len);
    try testing.expectEqual(@as(i64, 0), out_meta.num_rows);
    // Schema must still be present so downstream readers see a valid file.
    try testing.expectEqual(meta.schema.items.len, out_meta.schema.items.len);
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
    try testing.expectError(error.SurvivorsLenMismatch, build(arena, &empty_bytes, &meta, &wrong));
}

fn readFileSlice(path: []const u8, allocator: std.mem.Allocator) ![]u8 {
    const linux = std.os.linux;
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const fd: linux.fd_t = signedOrError(r_open) catch return error.FileNotFound;
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    if (errIs(end_pos)) return error.SeekFailed;
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);

    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        if (errIs(n)) return error.ReadFailed;
        const bytes: usize = @intCast(n);
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}

fn signedOrError(r: usize) error{SyscallFailed}!std.os.linux.fd_t {
    if (errIs(r)) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}
