const std = @import("std");
const builtin = @import("builtin");
const test_skip = @import("../test_skip.zig");
const core_memory = @import("../core/memory.zig");
const linux = std.os.linux;
const posix = std.posix;
const ac = @import("local_reader.zig");
const Config = ac.Config;
const Faults = ac.Faults;
const Mode = ac.Mode;
const Policy = ac.Policy;
const Range = ac.Range;
const Residency = ac.Residency;
const testing = std.testing;
const uringAvailable = ac.uringAvailable;
const Inject = ac.Inject;
const Reader = ac.Reader;
const Reason = ac.Reason;
const pageLen = ac.pageLen;
const sampleRanges = ac.sampleRanges;
const weightShare = ac.weightShare;
const Choice = ac.Choice;
const File = ac.File;
const nowNs = ac.nowNs;
const residencyFrom = ac.residencyFrom;
const uring_retry_bound = ac.uring_retry_bound;

fn testFile(tmp: *testing.TmpDir, name: []const u8, data: []const u8, buf: []u8) ![]const u8 {
    var f = try tmp.dir.createFile(testing.io, name, .{});
    defer f.close(testing.io);
    try f.writeStreamingAll(testing.io, data);
    return std.fmt.bufPrint(buf, ".zig-cache/tmp/{s}/{s}", .{ tmp.sub_path, name });
}

test "every backend reads exactly the planned bytes, row group by row group, within any window" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x10ad);
    const r = prng.random();
    const size: usize = (4 << 20) + 1234; // an unaligned tail, which O_DIRECT reads short
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    r.bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);

    for ([_]Mode{ .pread, .uring }) |mode| for ([_]u64{ 1 << 30, 4096 }) |window| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 3,
            .block = 64 << 10,
            .window = window,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        // Eight row groups over consecutive eighths of the file, each wanting a few ranges of it; one pruned.
        var plans: [8]?[]Range = undefined;
        var store: [8][4]Range = undefined;
        for (&plans, 0..) |*p, g| {
            if (g == 5) {
                p.* = null;
                continue;
            }
            const lo: u64 = g * size / 8;
            const hi: u64 = (g + 1) * size / 8;
            for (&store[g]) |*rg| {
                const a = r.intRangeLessThan(u64, lo, hi);
                rg.* = .{ .start = a, .end = @min(hi, a + r.intRangeAtMost(u64, 1, 300_000)) };
            }
            p.* = &store[g];
        }
        try rd.planFile(f, &plans, 4096);
        try rd.start();
        const gate = f.gate();
        for (plans, 0..) |p, g| {
            try gate.wait(gate.ctx, g);
            if (p) |rs| for (rs) |rg| {
                const lo: usize = @intCast(rg.start);
                const hi: usize = @intCast(rg.end);
                try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            };
            gate.done(gate.ctx, g, 1);
        }
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "probed ranges are read first, once, and given back with their row groups" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x9e0b);
    const size: usize = (2 << 20) + 777;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    const page = pageLen();
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]u64{ 1 << 30, 4096 }) |window| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 3, .block = 32 << 10, .window = window });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        // Four row groups of a quarter each, one pruned; each probes the first 10 000 bytes of its quarter (a
        // dictionary page at the start of a chunk), and the last also a range no row group reads.
        const q = size / 4;
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&store, &plans, 0..) |*st, *p, g| {
            st[0] = .{ .start = g * q + 100, .end = (g + 1) * q };
            p.* = if (g == 2) null else st;
        }
        try rd.planFile(f, &plans, 0);
        for (0..4) |g| try rd.probe(f, g, g * q + 100, g * q + 10_100);
        try rd.probe(f, 3, size - 50, size + 4096);
        try rd.start();
        // The probes are read before any row group's data is asked for.
        try File.probeWait(f, 0);
        for (0..4) |g| {
            const lo = g * q + 100;
            try testing.expectEqualSlices(u8, data[lo .. lo + 10_000], f.bytes()[lo .. lo + 10_000]);
        }
        try testing.expectEqualSlices(u8, data[size - 50 ..], f.bytes()[size - 50 ..]);
        // Planning is done: the pruned row group's probe pages go back, all but any page another row group covers.
        rd.releasePruned();
        const inner = std.mem.alignForward(u64, 2 * q + 100, page) + page;
        if (inner + page <= 2 * q + 10_100) try testing.expectEqual(@as(u8, 0), f.bytes()[inner]);
        const gate = f.gate();
        for (plans, 0..) |p, g| {
            try gate.wait(gate.ctx, g);
            if (p) |rs| for (rs) |r| {
                try testing.expectEqualSlices(u8, data[r.start..r.end], f.bytes()[r.start..r.end]);
            };
            gate.done(gate.ctx, g, 1);
        }
        // Every probed page is read exactly once, by a probe; no row group's own reads take it again.
        const pages = try testing.allocator.alloc(u8, (size + page - 1) / page);
        defer testing.allocator.free(pages);
        @memset(pages, 0);
        for (rd.units.items) |u| {
            var off = u.off;
            while (off < u.off + u.len) : (off += page) pages[off / page] += 1;
        }
        for (f.probes.items) |pr| {
            var off = pr.r.start;
            while (off < pr.r.end) : (off += page) try testing.expectEqual(@as(u8, 1), pages[off / page]);
        }
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "an abandoned reader stops, gives its pages back, and still tears down" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 9);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        // A one-page window: most row groups wait on releases nobody makes when the reader is abandoned.
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 2, .block = 16 << 10, .window = 4096 });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [8][1]Range = undefined;
        var plans: [8]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (128 << 10), .end = (g + 1) * (128 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.probe(f, 0, 0, 100);
        try rd.start();
        try File.probeWait(f, 0);
        const gate = f.gate();
        try gate.wait(gate.ctx, 0);
        rd.abandon();
        try testing.expectEqual(@as(usize, 0), rd.threads.items.len);
        // The pages are given back: what is left reads as the anonymous mapping's zeros.
        try testing.expectEqual(@as(u8, 0), f.bytes()[0]);
    }
}

test "a failed probe fails planning's wait and every row group's" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        // Unit 0 is the first probe's: probes go before every row group's reads.
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 2, .block = 16 << 10, .fail_unit = 0 });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [2][1]Range = undefined;
        var plans: [2]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (512 << 10), .end = (g + 1) * (512 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.probe(f, 1, 512 << 10, (512 << 10) + 100);
        try rd.start();
        try testing.expectError(error.LocalReadFailed, File.probeWait(f, 1));
        const gate = f.gate();
        for (0..2) |g| try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, g));
    }
}

test "a failed read fails the row groups waiting on it, and teardown neither hangs nor leaks" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 2,
            .block = 16 << 10,
            .fail_unit = 5,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (256 << 10), .end = (g + 1) * (256 << 10) };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const gate = f.gate();
        // 16 units of 16 KiB per row group: unit 5 is row group 0's.
        try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, 0));
        // The rest either read before the failure or fail too; none waits forever.
        for (1..4) |g| gate.wait(gate.ctx, g) catch |err| try testing.expectEqual(error.LocalReadFailed, err);
    }
}

test "a row group's release keeps pages other row groups' ranges still cover" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x1d8);
    const size: usize = 2 << 20;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);

    // Arrow's layout: each row group's data, then every row group's column indexes, then every offset index. With a
    // gap this wide each row group's index ranges coalesce over all the others' (and its data over its neighbours').
    const n = 8;
    var store: [n][3]Range = undefined;
    var plans: [n]?[]Range = undefined;
    for (&store, &plans, 0..) |*st, *p, g| {
        st[0] = .{ .start = g * (128 << 10) + 100, .end = (g + 1) * (128 << 10) + 3000 };
        st[1] = .{ .start = (1 << 20) + g * 300, .end = (1 << 20) + g * 300 + 300 };
        st[2] = .{ .start = (1 << 20) + 9000 + g * 300, .end = (1 << 20) + 9000 + g * 300 + 300 };
        p.* = st;
    }
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]bool{ false, true }) |reverse| {
        if (mode == .uring and !uringAvailable()) continue;
        const window: u64 = if (reverse) 1 << 30 else 4096;
        const rd = try Reader.create(testing.allocator, .{
            .mode = mode,
            .threads = 3,
            .block = 16 << 10,
            .window = window,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        try rd.planFile(f, &plans, 1 << 20);
        try rd.start();
        const gate = f.gate();
        var order: [n]usize = undefined;
        for (&order, 0..) |*o, i| o.* = if (reverse) n - 1 - i else i;
        if (reverse) for (0..n) |g| try gate.wait(gate.ctx, g);
        for (order, 0..) |g, i| {
            if (!reverse) try gate.wait(gate.ctx, g);
            // Every row group read and not yet done still sees all its bytes.
            for (if (reverse) order[i..] else order[i .. i + 1]) |live| for (store[live]) |r| {
                const lo: usize = @intCast(r.start);
                const hi: usize = @intCast(r.end);
                try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            };
            gate.done(gate.ctx, g, 1);
        }
        for (f.seg_refs) |*r| try testing.expectEqual(@as(u32, 0), r.load(.monotonic));
        try testing.expectEqual(@as(u64, 0), rd.submitted.load(.monotonic) - rd.released.load(.monotonic));
    };
}

test "an allocation failing anywhere in open, plan or start leaks nothing and leaves no thread behind" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 256 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, p: []const u8, size: usize) !void {
            const rd = try Reader.create(gpa, .{ .mode = .pread, .threads = 3, .block = 16 << 10 });
            defer rd.destroy();
            const f = try rd.openFile(p, size);
            var store: [2][1]Range = .{ .{.{ .start = 0, .end = 100_000 }}, .{.{ .start = 90_000, .end = size }} };
            var plans: [2]?[]Range = .{ &store[0], &store[1] };
            try rd.planFile(f, &plans, 4096);
            try rd.start();
            const gate = f.gate();
            for (0..2) |g| {
                try gate.wait(gate.ctx, g);
                gate.done(gate.ctx, g, 1);
            }
        }
    }.run, .{ path, data.len });
}

test "threads whose ring fails to set up read with pread, and say so" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0xf1);
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]struct { fail: u64, uring: u32 }{ .{ .fail = 0b101, .uring = 1 }, .{ .fail = 0b111, .uring = 0 } }) |c| {
        const rd = try Reader.create(testing.allocator, .{
            .mode = .uring,
            .threads = 3,
            .block = 16 << 10,
            .fail_rings = c.fail,
        });
        defer rd.destroy();
        const f = try rd.openFile(path, data.len);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * (256 << 10) + 7, .end = (g + 1) * (256 << 10) - 7 };
            p.* = st;
        }
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const b = rd.backends();
        try testing.expectEqual(c.uring, b.uring_threads);
        try testing.expectEqual(3 - c.uring, b.pread_threads);
        try testing.expectEqual(@as(u32, 1), b.direct_files + b.buffered_files);
        try testing.expectEqual(f.direct, b.direct_files == 1);
        const gate = f.gate();
        for (store, 0..) |st, g| {
            try gate.wait(gate.ctx, g);
            const lo: usize = @intCast(st[0].start);
            const hi: usize = @intCast(st[0].end);
            try testing.expectEqualSlices(u8, data[lo..hi], f.bytes()[lo..hi]);
            gate.done(gate.ctx, g, 1);
        }
    }
}

test "a direct file whose rings never start is counted through the page cache" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "norings.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
        .fail_rings = 1,
    });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(f.read_fallback.load(.acquire));
    const b = rd.backends();
    try testing.expectEqual(@as(u32, 0), b.uring_threads);
    try testing.expectEqual(@as(u32, 1), b.pread_threads);
    try testing.expectEqual(@as(u32, 0), b.direct_files);
    try testing.expectEqual(@as(u32, 1), b.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

fn virtualBytesForTest() !u64 {
    const rc = linux.open("/proc/self/maps", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    var buf: [128 << 10]u8 = undefined;
    const nr = linux.read(fd, &buf, buf.len);
    if (linux.errno(nr) != .SUCCESS or nr == buf.len) return error.ReadFailed;
    var total: u64 = 0;
    var lines = std.mem.splitScalar(u8, buf[0..nr], '\n');
    while (lines.next()) |line| {
        const dash = std.mem.indexOfScalar(u8, line, '-') orelse continue;
        const space = std.mem.indexOfScalar(u8, line, ' ') orelse return error.ReadFailed;
        const lo = try std.fmt.parseInt(u64, line[0..dash], 16);
        const hi = try std.fmt.parseInt(u64, line[dash + 1 .. space], 16);
        total += hi - lo;
    }
    return total;
}

test "failed reader open returns its anonymous mapping too" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", "x", &path_buf);
    const rd = try Reader.create(testing.allocator, .{ .mode = .pread });
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 1 });
    rd.gpa = failing.allocator();
    defer rd.destroy();
    _ = try virtualBytesForTest();
    const before = try virtualBytesForTest();
    // File alloc succeeds; files.append fails after a 32 MiB mapping has been made.
    try testing.expectError(error.OutOfMemory, rd.openFile(path, 32 << 20));
    const after = try virtualBytesForTest();
    // Ignore a small allocator/stack VMA change; the failed open's mapping must be gone.
    try testing.expect(after <= before + (1 << 20));
}

test "failed row group gates do not release bytes never admitted" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 256 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{ .mode = .pread, .threads = 3, .window = 4096, .fail_unit = 0 });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    var store: [2][1]Range = .{ .{.{ .start = 0, .end = 128 << 10 }}, .{.{ .start = 128 << 10, .end = 256 << 10 }} };
    var plans: [2]?[]Range = .{ &store[0], &store[1] };
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    for (0..2) |g| try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, g));
    // End the I/O failure before examining the scan workers' required error callbacks.
    for (rd.threads.items) |t| t.join();
    rd.threads.clearRetainingCapacity();
    for (0..2) |g| gate.done(gate.ctx, g, 1);
    try testing.expect(rd.released.load(.acquire) <= rd.submitted.load(.acquire));
}

test "continuation reads wait until their row group is admitted" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 8 << 20);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        try continuationsWaitFor(mode, path, data.len);
    }
}

fn continuationsWaitFor(mode: Mode, path: []const u8, size: usize) !void {
    const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 3, .block = 64 << 10, .window = 4096 });
    defer rd.destroy();
    const f = try rd.openFile(path, size);
    var store: [2][1]Range = .{ .{.{ .start = 0, .end = 4 << 20 }}, .{.{ .start = 4 << 20, .end = 8 << 20 }} };
    var plans: [2]?[]Range = .{ &store[0], &store[1] };
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    // Keep the first row group's window charge: the second cannot yet be admitted.
    // Its continuation units must not read behind its waiting first unit.
    const deadline = nowNs() + 200 * std.time.ns_per_ms;
    while (nowNs() < deadline and rd.stats.bytes.load(.acquire) <= rd.submitted.load(.acquire)) {
        var ts: linux.timespec = .{ .sec = 0, .nsec = std.time.ns_per_ms };
        _ = linux.nanosleep(&ts, null);
    }
    const read = rd.stats.bytes.load(.acquire);
    const charged = rd.submitted.load(.acquire);
    try testing.expectEqual(@as(u64, 4 << 20), charged);
    try testing.expectEqual(charged, read);
    gate.done(gate.ctx, 0, 1);
    try gate.wait(gate.ctx, 1);
    gate.done(gate.ctx, 1, 1);
}

test "local read bytes exclude EOF padding without shrinking the window charge" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", "x", &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| {
        if (mode == .uring and !uringAvailable()) continue;
        const rd = try Reader.create(testing.allocator, .{ .mode = mode, .threads = 1 });
        defer rd.destroy();
        const f = try rd.openFile(path, 1);
        var ranges = [_]Range{.{ .start = 0, .end = 1 }};
        const plans = [_]?[]Range{&ranges};
        try rd.planFile(f, &plans, 0);
        try rd.start();
        const gate = f.gate();
        try gate.wait(gate.ctx, 0);
        try testing.expectEqual(@as(u64, 1), rd.stats.bytes.load(.acquire));
        try testing.expectEqual(pageLen(), rd.submitted.load(.acquire));
        gate.done(gate.ctx, 0, 1);
        try testing.expectEqual(pageLen(), rd.released.load(.acquire));
    }
}

fn wholeShare(gpa: std.mem.Allocator, bytes: []align(std.heap.page_size_min) const u8) f64 {
    if (bytes.len == 0) return 0;
    const ranges = [_]Range{.{ .start = 0, .end = bytes.len }};
    return sampleRanges(gpa, bytes, &ranges).share;
}

test "planned-range residency bounds its allocation and partial-page windows" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    const page = std.heap.pageSize();
    const mapping = try posix.mmap(null, 4096 * page + 1, .{ .READ = true, .WRITE = true }, .{
        .TYPE = .PRIVATE,
        .ANONYMOUS = true,
        .NORESERVE = true,
    }, -1, 0);
    defer posix.munmap(mapping);
    @memset(mapping, 1);
    var storage: [2048]u8 = undefined;
    var bounded = std.heap.FixedBufferAllocator.init(&storage);
    try testing.expectEqual(@as(f64, 1), wholeShare(bounded.allocator(), mapping));
    bounded.reset();
    try testing.expectEqual(@as(f64, 1), wholeShare(bounded.allocator(), mapping[0 .. 32 * page - 1]));
    bounded.reset();
    try testing.expectEqual(@as(f64, 0), wholeShare(bounded.allocator(), mapping[0..0]));
}

test "auto maps a warm file, buffers a partly warm one, and reads a cold one direct" {
    const auto: Policy = .{};
    const pinned: Policy = .{ .profile = .pread };
    try testing.expectEqual(Mode.pread, pinned.choose(0, 1).mode);
    try testing.expectEqual(Reason.explicit, pinned.choose(0, 1).reason);
    const asked: Policy = .{ .profile = .uring };
    if (uringAvailable()) {
        const direct = asked.choose(0, 0.1);
        try testing.expectEqual(Mode.uring, direct.mode);
        try testing.expect(direct.directRead());
    }
    const hot = auto.choose(Policy.warm_resident, 1);
    try testing.expectEqual(Mode.mmap, hot.mode);
    try testing.expectEqual(Reason.cached, hot.reason);
    try testing.expect(!hot.directRead());
    if (builtin.target.os.tag != .linux) return;
    if (uringAvailable()) {
        const cold_wide = auto.choose(0, 0.9);
        try testing.expectEqual(Mode.uring, cold_wide.mode);
        try testing.expectEqual(Reason.cold_direct, cold_wide.reason);
        try testing.expect(cold_wide.directRead());
        try testing.expectEqualStrings(
            "cold, O_DIRECT where the file allows it; --cache reads through the page cache",
            cold_wide.reason.phrase(),
        );
        const cold_narrow = auto.choose(Policy.cold_resident - 0.01, 0.1);
        try testing.expectEqual(Mode.uring, cold_narrow.mode);
        try testing.expect(cold_narrow.directRead());
        const warm_wide = auto.choose(0.5, 0.9);
        try testing.expectEqual(Mode.uring, warm_wide.mode);
        try testing.expectEqual(Reason.cold_buffered, warm_wide.reason);
        try testing.expect(!warm_wide.directRead());
    } else {
        try testing.expectEqual(Mode.pread, auto.choose(0, 0.9).mode);
        try testing.expectEqual(Reason.no_uring, auto.choose(0, 0.9).reason);
        try testing.expect(!auto.choose(0, 0.9).directRead());
    }
    const warm_narrow = auto.choose(Policy.cold_resident, 0.2);
    try testing.expectEqual(Mode.pread, warm_narrow.mode);
    try testing.expectEqual(Reason.cold_buffered, warm_narrow.reason);
    try testing.expect(!warm_narrow.directRead());
}

test "--cache reads through the page cache and never O_DIRECT" {
    const cache: Policy = .{ .cache = true };
    const wide = cache.choose(0, 0.9);
    const narrow = cache.choose(0, 0.1);
    try testing.expect(!wide.directRead());
    try testing.expect(!narrow.directRead());
    try testing.expectEqualStrings("through the page cache (--cache)", Reason.page_cache.phrase());
    if (builtin.target.os.tag == .linux) {
        try testing.expectEqual(Reason.page_cache, wide.reason);
        try testing.expectEqual(Reason.page_cache, narrow.reason);
        try testing.expectEqual(if (uringAvailable()) Mode.uring else Mode.pread, wide.mode);
        try testing.expectEqual(Mode.pread, narrow.mode);
    } else {
        // Off Linux `auto` always maps, and a mapping reads through the page cache.
        for ([_]Choice{ wide, narrow }) |choice| try testing.expectEqual(Choice{ .mode = .mmap, .reason = .not_linux }, choice);
    }
    const hot = cache.choose(Policy.warm_resident, 1);
    try testing.expectEqual(Mode.mmap, hot.mode);
    try testing.expectEqual(Reason.cached, hot.reason);
    const asked: Policy = .{ .profile = .uring, .cache = true };
    try testing.expect(!asked.choose(0, 1).directRead());
    try testing.expectEqual(Reason.page_cache, asked.choose(0, 1).reason);
}

test "a cold read that does not fit the reader is mapped" {
    const auto: Policy = .{};
    const unknown = auto.choose(0, 0.9);
    if (builtin.target.os.tag == .linux and uringAvailable()) {
        try testing.expect(unknown.directRead());
        try testing.expectEqual(Reason.cold_direct, unknown.reason);
    } else {
        try testing.expect(!unknown.directRead());
    }
    const sparse = residencyFrom(0, 32, 10_000);
    try testing.expectEqual(@as(u64, 10_000), sparse.pages);
    const from_sample = auto.choose(sparse.share, 0.9);
    try testing.expectEqual(unknown.directRead(), from_sample.directRead());
    const failed: Residency = .{ .pages = 10 };
    try testing.expectEqual(@as(f64, 0), failed.share);
    const mapped = unknown.withinReader(false);
    try testing.expectEqual(Mode.mmap, mapped.mode);
    try testing.expectEqual(Reason.over_budget, mapped.reason);
    try testing.expect(!mapped.directRead());
    const explicit: Policy = .{ .profile = .uring };
    if (uringAvailable()) {
        const asked = explicit.choose(0, 0.2);
        try testing.expect(asked.directRead());
        const over = asked.withinReader(false);
        try testing.expectEqual(Mode.mmap, over.mode);
        try testing.expectEqual(Reason.over_budget, over.reason);
    }
}

test "residency weights a file by its planned pages, and an unknown sample is not cached" {
    const page = 4096;
    const cold: Residency = .{ .share = 0, .pages = (8 << 30) / page };
    const hot: Residency = .{ .share = 1, .pages = (64 << 20) / page };
    try testing.expect(weightShare(&.{ cold, hot }) < 0.8);
    const unknown: Residency = .{ .share = 0, .pages = hot.pages };
    const hidden = weightShare(&.{ hot, unknown });
    try testing.expect(hidden < 0.8);
    try testing.expect(hidden > 0.4);
}

test "residency follows the planned ranges, not the rest of the file" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    const page = std.heap.pageSize();
    const pages: usize = 512;
    const size = page * pages;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "ranges.bin", data, &path_buf);
    try forgetFile(path, size);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const warm_bytes = page * 4;
    const scratch = try testing.allocator.alloc(u8, warm_bytes);
    defer testing.allocator.free(scratch);
    const got = linux.pread(fd, scratch.ptr, warm_bytes, 0);
    if (linux.errno(got) != .SUCCESS) return error.ReadFailed;
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    const warm_r = [_]Range{.{ .start = 0, .end = warm_bytes }};
    const warm = sampleRanges(testing.allocator, map, &warm_r);
    try testing.expectEqual(@as(u64, 4), warm.pages);
    try testing.expect(warm.share >= 0.8);
    const whole = wholeShare(testing.allocator, map);
    try testing.expect(whole < 0.8);
    const cached = (Policy{}).choose(warm.share, 0.2);
    try testing.expectEqual(Reason.cached, cached.reason);
    try testing.expectEqual(Mode.mmap, cached.mode);
}

fn readWhole(path: []const u8, data: []const u8, cfg: Config) !void {
    const rd = try Reader.create(testing.allocator, cfg);
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "pread, buffered io_uring and direct io_uring read the same bytes" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    var prng = std.Random.DefaultPrng.init(0x10);
    const data = try testing.allocator.alloc(u8, 1 << 20);
    defer testing.allocator.free(data);
    prng.random().bytes(data);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try readWhole(path, data, .{ .mode = .pread, .threads = 2, .block = 64 << 10 });
    if (!uringAvailable()) return;
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = false });
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = true });
}

test "io_uring retries a refused enter and resubmits an EAGAIN completion" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 3);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "retry.bin", data, &path_buf);
    const inject = [_]Inject{ .intr, .intr, .again, .busy };
    var faults: Faults = .{ .inject = &inject, .cqe_replays = 1 };
    try readFaulted(path, data, &faults);
    try testing.expectEqual(@as(usize, 4), faults.at);
    try testing.expectEqual(@as(u8, 0), faults.cqe_replays);
    try testing.expectEqual(@as(u32, 0), faults.preads);
}

fn readFaulted(path: []const u8, data: []const u8, faults: *Faults) !void {
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = false,
    });
    defer rd.destroy();
    rd.faults = faults;
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    const expect_fail = faults.fail_pread;
    try rd.start();
    const gate = f.gate();
    if (expect_fail) {
        try testing.expectError(error.LocalReadFailed, gate.wait(gate.ctx, 0));
        return;
    }
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "io_uring past the retry bound finishes the unit with pread" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 5);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "bound.bin", data, &path_buf);
    var again: [16]Inject = undefined;
    for (&again) |*step| step.* = .again;
    var refused: Faults = .{ .inject = &again };
    try readFaulted(path, data, &refused);
    try testing.expectEqual(@as(usize, uring_retry_bound), refused.at);
    try testing.expect(refused.preads > 0);
    var replays: Faults = .{ .cqe_replays = 100 };
    try readFaulted(path, data, &replays);
    try testing.expectEqual(@as(u8, 100 - uring_retry_bound), replays.cqe_replays);
    var refuse_pread: Faults = .{ .inject = &again, .fail_pread = true };
    try readFaulted(path, data, &refuse_pread);
    var replay_pread: Faults = .{ .cqe_replays = 100, .fail_pread = true };
    try readFaulted(path, data, &replay_pread);
    const wide_data = try testing.allocator.alloc(u8, 192 << 10);
    defer testing.allocator.free(wide_data);
    @memset(wide_data, 5);
    var wide_buf: [256]u8 = undefined;
    const wide = try testFile(&tmp, "partial.bin", wide_data, &wide_buf);
    const partial_inject = [_]Inject{
        .{ .submit = 1 }, .again, .again, .again, .again, .again, .again, .again, .again,
    };
    var partial: Faults = .{ .inject = &partial_inject };
    try readFaulted(wide, wide_data, &partial);
    try testing.expectEqual(@as(u32, 2), partial.preads);
}

test "a short repair pread failure reaps the rest of its completion batch" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "batch.bin", data, &path_buf);
    var faults: Faults = .{ .hold_batch = 2, .short_first = true, .fail_pread = true };
    try readFaulted(path, data, &faults);
    try testing.expect(faults.batch_n >= 2);
    try testing.expectEqual(@as(usize, 0), faults.inflight_at_deinit);
}

test "a submitted io_uring read is not published while its wait is interrupted" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 192 << 10);
    defer testing.allocator.free(data);
    @memset(data, 6);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "inflight.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = false,
    });
    defer rd.destroy();
    var faults: Faults = .{ .wait_intr = 4 };
    rd.faults = &faults;
    const f = try rd.openFile(path, data.len);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expect(!faults.early_ready);
    try testing.expectEqual(@as(u8, 0), faults.wait_intr);
    try testing.expectEqual(@as(u64, 0), rd.released.load(.acquire));
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "a file that cannot open O_DIRECT is still read through the page cache" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 5);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "nodirect.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .direct = true,
        .fail_direct_open = true,
    });
    defer rd.destroy();
    const f = try rd.borrowFile(fd, data.len);
    try testing.expect(!f.direct);
    try testing.expectEqual(fd, f.fd);
    try testing.expect(!f.owned);
    try testing.expectEqual(@as(i32, -1), f.buffered_fd);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

test "a finished direct read of EINVAL is pread, and the direct fd stays open" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 128 << 10);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "reject.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    var faults: Faults = .{ .hold_batch = 2, .reject_direct = 1 };
    rd.faults = &faults;
    const f = try rd.borrowFile(fd, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(f.read_fallback.load(.acquire));
    try testing.expect(faults.batch_n >= 2);
    const backs = rd.backends();
    try testing.expectEqual(@as(u32, 0), backs.direct_files);
    try testing.expectEqual(@as(u32, 1), backs.buffered_files);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    const live = linux.fcntl(f.fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(live) == .SUCCESS);
    gate.done(gate.ctx, 0, 1);
}

test "an unaligned direct tail is read through the buffered fd and stays direct" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, (64 << 10) + 100);
    defer testing.allocator.free(data);
    @memset(data, 8);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "tail.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(!f.read_fallback.load(.acquire));
    const backs = rd.backends();
    try testing.expectEqual(@as(u32, 1), backs.direct_files);
    try testing.expectEqual(@as(u32, 0), backs.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

test "a positive short completion repairs from an unaligned offset and stays direct" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 2);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "short.bin", data, &path_buf);
    const rd = try Reader.create(testing.allocator, .{
        .mode = .uring,
        .threads = 1,
        .block = 64 << 10,
        .direct = true,
    });
    defer rd.destroy();
    var faults: Faults = .{ .short_got = 100 };
    rd.faults = &faults;
    const f = try rd.openFile(path, data.len);
    if (!f.direct) return error.SkipZigTest;
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    try testing.expect(faults.repaired);
    try testing.expect(faults.repair_at % pageLen() != 0);
    try testing.expectEqual(@as(u64, 100), faults.repair_at);
    try testing.expect(!f.read_fallback.load(.acquire));
    const b = rd.backends();
    try testing.expectEqual(@as(u32, 1), b.direct_files);
    try testing.expectEqual(@as(u32, 0), b.buffered_files);
    gate.done(gate.ctx, 0, 1);
}

test "direct io opens its own fd and does not change a borrowed one" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const data = try testing.allocator.alloc(u8, 64 << 10);
    defer testing.allocator.free(data);
    @memset(data, 9);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const before = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expect(linux.errno(before) == .SUCCESS);
    const rd = try Reader.create(testing.allocator, .{ .mode = .uring, .threads = 1, .direct = true });
    defer rd.destroy();
    const f = try rd.borrowFile(fd, data.len);
    const after = linux.fcntl(fd, linux.F.GETFL, 0);
    try testing.expectEqual(before, after);
    if (f.direct) {
        try testing.expect(f.fd != fd);
        try testing.expect(f.owned);
        try testing.expectEqual(fd, f.buffered_fd);
        try testing.expect(!f.own_buffered);
    } else {
        try testing.expectEqual(fd, f.fd);
        try testing.expect(!f.owned);
        try testing.expectEqual(@as(i32, -1), f.buffered_fd);
    }
    var range = [_]Range{.{ .start = 0, .end = data.len }};
    var plans = [_]?[]Range{&range};
    try rd.planFile(f, &plans, 0);
    try rd.start();
    const gate = f.gate();
    try gate.wait(gate.ctx, 0);
    try testing.expectEqualSlices(u8, data, f.bytes()[0..data.len]);
    gate.done(gate.ctx, 0, 1);
}

fn fileResidency(path: []const u8, size: usize) !f64 {
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= path_z.len) return error.NameTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    return wholeShare(testing.allocator, map);
}

fn forgetFile(path: []const u8, size: usize) !void {
    var path_z: [std.fs.max_path_bytes:0]u8 = undefined;
    if (path.len >= path_z.len) return error.NameTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;
    const rc = linux.open(path_z[0..path.len :0], .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OpenFailed;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    _ = linux.fsync(fd);
    _ = linux.fadvise(fd, 0, 0, linux.POSIX_FADV.DONTNEED);
    const map = try posix.mmap(null, size, .{ .READ = true }, .{ .TYPE = .SHARED }, fd, 0);
    defer posix.munmap(map);
    _ = linux.madvise(map.ptr, map.len, linux.MADV.DONTNEED);
}

test "a buffered read of a cold file leaves it cached for the next choice" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "io_uring");
    if (!uringAvailable()) return error.SkipZigTest;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const page = std.heap.pageSize();
    const size = page * 128;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 4);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    try forgetFile(path, size);
    const cold = try fileResidency(path, size);
    try readWhole(path, data, .{ .mode = .uring, .threads = 2, .block = 64 << 10, .direct = false });
    const warm = try fileResidency(path, size);
    // This read was buffered on purpose. Auto on a cold file is O_DIRECT and would have left it cold.
    const auto: Policy = .{};
    const first = auto.choose(cold, 1);
    if (cold < Policy.cold_resident) {
        try testing.expectEqual(Reason.cold_direct, first.reason);
        try testing.expect(first.directRead());
    }
    try testing.expect(warm >= Policy.warm_resident);
    try testing.expect(warm > cold);
    const second = auto.choose(warm, 1);
    try testing.expectEqual(Mode.mmap, second.mode);
    try testing.expectEqual(Reason.cached, second.reason);
}

test "row groups draw their bytes from the query's ledger as they are admitted, and give them back as released" {
    if (comptime test_skip.lacks(.linux)) return test_skip.skipNeeding(.linux, "the local reader's backends");
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();
    const size: usize = 1 << 20;
    const data = try testing.allocator.alloc(u8, size);
    defer testing.allocator.free(data);
    @memset(data, 7);
    var path_buf: [256]u8 = undefined;
    const path = try testFile(&tmp, "data.bin", data, &path_buf);
    for ([_]Mode{ .pread, .uring }) |mode| for ([_]bool{ false, true }) |abandon| {
        if (mode == .uring and !uringAvailable()) continue;
        // Room for one row group of four at a time: each waits on the one before it, and none is refused.
        var ledger: core_memory.SharedBudget = .{ .limit = size / 4 + 4096 };
        _ = ledger.used.fetchAdd(4096, .monotonic);
        const cfg: Config = .{ .mode = mode, .threads = 2, .window = 1 << 30, .ledger = &ledger };
        const rd = try Reader.create(testing.allocator, cfg);
        defer rd.destroy();
        const f = try rd.openFile(path, size);
        var store: [4][1]Range = undefined;
        var plans: [4]?[]Range = undefined;
        for (&plans, &store, 0..) |*p, *st, g| {
            st[0] = .{ .start = g * size / 4, .end = (g + 1) * size / 4 };
            p.* = st;
        }
        try rd.planFile(f, &plans, 4096);
        try rd.start();
        const gate = f.gate();
        for (0..4) |g| {
            try gate.wait(gate.ctx, g);
            try testing.expect(ledger.used.load(.monotonic) <= ledger.limit + size / 4);
            const lo = g * size / 4;
            try testing.expectEqualSlices(u8, data[lo..][0 .. size / 4], f.bytes()[lo..][0 .. size / 4]);
            if (abandon and g == 1) break;
            gate.done(gate.ctx, g, 1);
        }
        if (abandon) rd.abandon();
        try testing.expectEqual(@as(usize, 4096), ledger.used.load(.monotonic));
    };
}
