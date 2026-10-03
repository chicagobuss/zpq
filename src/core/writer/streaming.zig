//! Streaming Parquet writer.
//!
//! The byte-copy output of `fastpath.buildMulti`, sent to a generic
//! `Sink` instead of accumulating in a single `[]u8`: both run
//! `fastpath.assemble`. Memory cost is O(footer + one in-flight chunk
//! in the sink), independent of total output size — the unlock that
//! lets a Lambda emit a multi-GB Parquet without ever materializing
//! it in 512 MB of working memory.
//!
//! Layering note: this module lives in `core/`, so it must NOT depend
//! on any I/O. The `Sink` interface is opaque — callers in `lambda/`
//! or `cli/` plug in `MultipartSink` (S3) or anything else by
//! providing a `*anyopaque` ctx and a write function pointer.

const std = @import("std");
const fastpath = @import("fastpath.zig");

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

pub const Error = fastpath.Error;

/// Streaming counterpart of `fastpath.buildMulti`: same inputs, same
/// projection rules, same bytes, written to `sink` as they are produced.
/// Returns the number of bytes written.
pub fn build(
    arena: std.mem.Allocator,
    sink: Sink,
    files: []const fastpath.FileSpec,
    kept_columns: ?[]const usize,
) !u64 {
    var out: SinkOut = .{ .sink = sink };
    try fastpath.assemble(arena, &out, files, kept_columns);
    return out.offset;
}

/// `fastpath.assemble` output over a `Sink`, which is opaque about how
/// far it has got, so the output offset is tracked here.
const SinkOut = struct {
    sink: Sink,
    offset: usize = 0,

    pub fn pos(self: *const SinkOut) usize {
        return self.offset;
    }

    /// Nothing to pre-size: bytes leave as they are written.
    pub fn reserve(_: *SinkOut, _: usize) !void {}

    pub fn write(self: *SinkOut, bytes: []const u8) anyerror!void {
        try self.sink.write(bytes);
        self.offset += bytes.len;
    }
};

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
const schema = @import("../schema.zig");
const thrift = @import("../thrift.zig");
const metadata = @import("../parquet/metadata.zig");
const readFileSlice = metadata.readFileSlice;

test "streaming.build is byte-identical to fastpath.buildMulti (no projection)" {
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
        if (err == error.FileNotFound) return error.SkipZigTest;
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

test "streaming.build carries the page index forward" {
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
    var src_has_index = false;
    for (meta.row_groups.items) |rg| for (rg.columns.items) |c| {
        if (c.column_index_offset != null or c.offset_index_offset != null) src_has_index = true;
    };
    if (!src_has_index) {
        std.debug.print("skipping: fixture carries no page index\n", .{});
        return error.SkipZigTest;
    }

    const survivors = try arena.alloc(bool, meta.row_groups.items.len);
    @memset(survivors, true);
    const specs = [_]fastpath.FileSpec{
        .{ .bytes = file_bytes, .meta = &meta, .survivors = survivors },
    };

    var msink: MemorySink = .{ .arena = arena };
    _ = try build(arena, msink.sink(), &specs, null);
    const out = msink.buffer.items;
    const out_meta = try metadata.open(arena, out);

    var checked: usize = 0;
    for (meta.row_groups.items, 0..) |src_rg, ri| {
        const out_rg = out_meta.row_groups.items[ri];
        for (src_rg.columns.items, 0..) |src_c, ci| {
            const out_c = out_rg.columns.items[ci];
            if (src_c.offset_index_offset != null) {
                try testing.expect(out_c.offset_index_offset != null);
                const off: usize = @intCast(out_c.offset_index_offset.?);
                const len: usize = @intCast(out_c.offset_index_length.?);
                try testing.expect(off + len <= out.len);
                var r = thrift.Reader.init(out[off .. off + len]);
                const oi = try schema.OffsetIndex.read(arena, &r);
                for (oi.page_locations.items) |pl|
                    try testing.expect(pl.offset >= 0 and @as(usize, @intCast(pl.offset)) < out.len);
                checked += 1;
            }
        }
    }
    try testing.expect(checked > 0);
    std.debug.print("[streaming] page-index carry-forward: verified {d} chunk indexes\n", .{checked});
}

test "streaming.build N=10 multi-file" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(fixture_path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
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

test "streaming.build refuses before writing a byte" {
    const file_bytes = readFileSlice("ci/fixtures/parquet/column_order.parquet", testing.allocator) catch |err| {
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
    const good: fastpath.FileSpec = .{ .bytes = file_bytes, .meta = &meta, .survivors = survivors };

    // A sink is often a remote upload: a refusal found after the first input was sent would leave a partial object.
    var msink: MemorySink = .{ .arena = arena };
    const short: fastpath.FileSpec = .{ .bytes = file_bytes, .meta = &meta, .survivors = survivors[1..] };
    try testing.expectError(error.SurvivorsLenMismatch, build(arena, msink.sink(), &.{ good, short }, null));
    try testing.expectEqual(@as(usize, 0), msink.buffer.items.len);

    var other = meta;
    other.column_orders = .empty;
    try other.column_orders.?.appendSlice(arena, &.{ schema.COLUMN_ORDER_TYPE_DEFINED, schema.COLUMN_ORDER_TYPE_DEFINED });
    const mixed = [_]fastpath.FileSpec{ good, .{ .bytes = file_bytes, .meta = &other, .survivors = survivors } };
    try testing.expectError(error.ColumnOrderMismatch, build(arena, msink.sink(), &mixed, null));
    try testing.expectEqual(@as(usize, 0), msink.buffer.items.len);
}
