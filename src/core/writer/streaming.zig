//! Streaming Parquet writer.
//!
//! Same row-group-by-row-group structure as `fastpath.buildMulti`,
//! but bytes go to a generic `Sink` instead of accumulating in a
//! single `[]u8`. Memory cost is O(footer + one in-flight chunk in
//! the sink), independent of total output size — the unlock that
//! lets a Lambda emit a multi-GB Parquet without ever materializing
//! it in 512 MB of working memory.
//!
//! Layering note: this module lives in `core/`, so it must NOT depend
//! on any I/O. The `Sink` interface is opaque — callers in `lambda/`
//! or `cli/` plug in `MultipartSink` (S3) or anything else by
//! providing a `*anyopaque` ctx and a write function pointer.

const std = @import("std");
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const fastpath = @import("fastpath.zig");

const MAGIC: [4]u8 = .{ 'P', 'A', 'R', '1' };

/// Type-erased byte sink. The writer treats each `write` call as an
/// opaque "you owe me these bytes at offset X" handoff; impls buffer
/// however they like. Errors propagate verbatim — the writer doesn't
/// try to recover (any sink-level retry happens inside the impl).
pub const Sink = struct {
    ctx: *anyopaque,
    write_fn: *const fn (*anyopaque, []const u8) anyerror!void,

    pub fn write(self: Sink, bytes: []const u8) anyerror!void {
        return self.write_fn(self.ctx, bytes);
    }
};

pub const Error = error{
    SurvivorsLenMismatch,
    InvalidColumnOffsets,
    NestedSchemaUnsupported,
    BadColumnIndex,
} || std.mem.Allocator.Error;

/// Streaming counterpart of `fastpath.buildMulti`. Same input shape,
/// same projection rules, same flat-schema invariants — bytes are
/// written to `sink` as soon as we know what they should be, instead
/// of being collected into a buffer and returned.
pub fn build(
    arena: std.mem.Allocator,
    sink: Sink,
    files: []const fastpath.FileSpec,
    kept_columns: ?[]const usize,
) !u64 {
    if (files.len == 0) return error.SurvivorsLenMismatch;

    const meta0 = files[0].meta;

    if (kept_columns) |kc| {
        if (meta0.schema.items.len < 1) return error.NestedSchemaUnsupported;
        const root = meta0.schema.items[0];
        const expected_leaves: usize = if (root.num_children) |nc| @intCast(nc) else 0;
        if (meta0.schema.items.len != 1 + expected_leaves) return error.NestedSchemaUnsupported;
        for (kc) |idx| if (idx >= expected_leaves) return error.BadColumnIndex;
    }

    // Track absolute byte offset into the output stream. The fastpath
    // version reads this from `out.items.len`; here we maintain it
    // explicitly because the sink is opaque about how far it's gotten.
    var offset: usize = 0;
    try sink.write(&MAGIC);
    offset += MAGIC.len;

    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    var total_rows: i64 = 0;

    for (files) |f| {
        if (f.survivors.len != f.meta.row_groups.items.len) return error.SurvivorsLenMismatch;
        for (f.survivors, 0..) |keep, i| {
            if (!keep) continue;
            const rg = &f.meta.row_groups.items[i];
            if (kept_columns) |kc| {
                const new_rg = try writeProjectedRowGroup(arena, sink, &offset, f.bytes, rg, kc);
                try new_row_groups.append(arena, new_rg);
            } else {
                const range = rowGroupByteRange(rg) orelse continue;
                if (range.start + range.len > f.bytes.len) return error.InvalidColumnOffsets;
                const new_start = offset;
                try sink.write(f.bytes[range.start .. range.start + range.len]);
                offset += range.len;
                const delta: i64 = @as(i64, @intCast(new_start)) - @as(i64, @intCast(range.start));
                const cloned = try cloneRowGroupShifted(arena, rg, delta);
                try new_row_groups.append(arena, cloned);
            }
            total_rows += rg.num_rows;
        }
    }

    var new_schema = meta0.schema;
    if (kept_columns) |kc| new_schema = try fastpath.projectSchema(arena, meta0.schema, kc);

    const new_meta: schema.FileMetaData = .{
        .version = meta0.version,
        .schema = new_schema,
        .num_rows = total_rows,
        .created_by = meta0.created_by,
        .row_groups = new_row_groups,
    };

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();

    try sink.write(footer_bytes);
    offset += footer_bytes.len;
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try sink.write(&len_bytes);
    offset += len_bytes.len;
    try sink.write(&MAGIC);
    offset += MAGIC.len;
    return @intCast(offset);
}

fn writeProjectedRowGroup(
    arena: std.mem.Allocator,
    sink: Sink,
    offset: *usize,
    input: []const u8,
    src_rg: *const schema.RowGroup,
    kept: []const usize,
) !schema.RowGroup {
    var new_cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try new_cols.ensureTotalCapacity(arena, kept.len);

    var rg_total: i64 = 0;
    for (kept) |col_idx| {
        if (col_idx >= src_rg.columns.items.len) return error.BadColumnIndex;
        const src_chunk = src_rg.columns.items[col_idx];
        const m = src_chunk.meta_data orelse return error.InvalidColumnOffsets;

        const src_start: usize = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
        const src_len: usize = @intCast(m.total_compressed_size);
        if (src_start + src_len > input.len) return error.InvalidColumnOffsets;

        const new_col_start = offset.*;
        try sink.write(input[src_start .. src_start + src_len]);
        offset.* += src_len;

        const delta: i64 = @as(i64, @intCast(new_col_start)) - @as(i64, @intCast(src_start));
        var new_chunk = src_chunk;
        new_chunk.offset_index_offset = null;
        new_chunk.offset_index_length = null;
        new_chunk.column_index_offset = null;
        new_chunk.column_index_length = null;
        if (new_chunk.meta_data) |*nm| {
            nm.data_page_offset += delta;
            if (nm.dictionary_page_offset) |d| nm.dictionary_page_offset = d + delta;
            if (nm.index_page_offset) |d| nm.index_page_offset = d + delta;
        }
        if (new_chunk.meta_data) |nm| new_chunk.file_offset = nm.data_page_offset;

        try new_cols.append(arena, new_chunk);
        rg_total += @intCast(src_len);
    }

    return .{
        .columns = new_cols,
        .total_byte_size = rg_total,
        .num_rows = src_rg.num_rows,
    };
}

const ByteRange = struct { start: usize, len: usize };

fn rowGroupByteRange(rg: *const schema.RowGroup) ?ByteRange {
    if (rg.columns.items.len == 0) return null;
    var min_start: u64 = std.math.maxInt(u64);
    var max_end: u64 = 0;
    for (rg.columns.items) |chunk| {
        const m = chunk.meta_data orelse continue;
        const start: u64 = if (m.dictionary_page_offset) |d| @intCast(d) else @intCast(m.data_page_offset);
        const end: u64 = start + @as(u64, @intCast(m.total_compressed_size));
        if (start < min_start) min_start = start;
        if (end > max_end) max_end = end;
    }
    if (min_start == std.math.maxInt(u64)) return null;
    return .{ .start = @intCast(min_start), .len = @intCast(max_end - min_start) };
}

fn cloneRowGroupShifted(
    arena: std.mem.Allocator,
    src: *const schema.RowGroup,
    delta: i64,
) !schema.RowGroup {
    var cols: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
    try cols.ensureTotalCapacity(arena, src.columns.items.len);
    for (src.columns.items) |chunk| {
        var new_chunk = chunk;
        new_chunk.offset_index_offset = null;
        new_chunk.offset_index_length = null;
        new_chunk.column_index_offset = null;
        new_chunk.column_index_length = null;
        if (new_chunk.meta_data) |*m| {
            m.data_page_offset += delta;
            if (m.dictionary_page_offset) |d| m.dictionary_page_offset = d + delta;
            if (m.index_page_offset) |d| m.index_page_offset = d + delta;
        }
        if (new_chunk.meta_data) |m| new_chunk.file_offset = m.data_page_offset;
        try cols.append(arena, new_chunk);
    }
    return .{
        .columns = cols,
        .total_byte_size = src.total_byte_size,
        .num_rows = src.num_rows,
    };
}

/// In-memory sink: collects everything into an ArrayList. Useful for
/// tests and as the local-CLI default. Production lambda paths plug
/// in `io.multipart_sink.MultipartSink` instead.
pub const MemorySink = struct {
    arena: std.mem.Allocator,
    buffer: std.ArrayList(u8) = .empty,

    pub fn sink(self: *MemorySink) Sink {
        return .{
            .ctx = @ptrCast(self),
            .write_fn = writeImpl,
        };
    }

    fn writeImpl(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *MemorySink = @ptrCast(@alignCast(ctx));
        try self.buffer.appendSlice(self.arena, bytes);
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const metadata = @import("../parquet/metadata.zig");

test "streaming.build is byte-identical to fastpath.buildMulti (no projection)" {
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
    @memset(survivors, true);

    const specs = [_]fastpath.FileSpec{
        .{ .bytes = file_bytes, .meta = &meta, .survivors = survivors },
    };

    const buf_out = try fastpath.buildMulti(arena, &specs, null);

    var msink: MemorySink = .{ .arena = arena };
    _ = try build(arena, msink.sink(), &specs, null);

    try testing.expectEqualSlices(u8, buf_out, msink.buffer.items);
    std.debug.print(
        "[streaming] all-survivors equivalence verified: {d}B\n",
        .{msink.buffer.items.len},
    );
}

test "streaming.build is byte-identical to fastpath.buildMulti (with projection)" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return;
        return err;
    };
    defer testing.allocator.free(file_bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var meta = try metadata.open(arena, file_bytes);
    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);

    const specs = [_]fastpath.FileSpec{
        .{ .bytes = file_bytes, .meta = &meta, .survivors = survivors },
    };

    // Same projection used by fastpath's own test: int8 + int32_sorted
    const kept = [_]usize{ 0, 2 };

    const buf_out = try fastpath.buildMulti(arena, &specs, &kept);

    var msink: MemorySink = .{ .arena = arena };
    _ = try build(arena, msink.sink(), &specs, &kept);

    try testing.expectEqualSlices(u8, buf_out, msink.buffer.items);
    std.debug.print(
        "[streaming] projected equivalence verified: {d}B\n",
        .{msink.buffer.items.len},
    );
}

test "streaming.build N=10 multi-file" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return;
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
    const specs = try arena.alloc(fastpath.FileSpec, N);
    for (specs) |*sp| sp.* = .{
        .bytes = file_bytes,
        .meta = &meta,
        .survivors = survivors,
    };

    const buf_out = try fastpath.buildMulti(arena, specs, null);

    var msink: MemorySink = .{ .arena = arena };
    _ = try build(arena, msink.sink(), specs, null);

    try testing.expectEqualSlices(u8, buf_out, msink.buffer.items);

    // Verify the streamed bytes parse back as a valid Parquet file.
    const out_meta = try metadata.open(arena, msink.buffer.items);
    try testing.expectEqual(meta.num_rows * N, out_meta.num_rows);
    std.debug.print(
        "[streaming] N=10 multi: {d}B, {d} row groups\n",
        .{ msink.buffer.items.len, out_meta.row_groups.items.len },
    );
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
