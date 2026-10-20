//! An aggregate's S3 row groups fetched ahead of the scan workers within a byte window, in the order workers claim
//! them, each row group's pages handed back once its work items are done (`scan.Input.gate`). The fetched bytes held
//! at once are then bounded by the window, not by everything the query reads, so a scan larger than memory completes.
//!
//! Each row group's chunks sit in their own page-aligned segment of an anonymous buffer laid out by the caller, so a
//! finished row group's pages go back with MADV_DONTNEED without touching a neighbour's. Linux only, like
//! `local_reader.zig`; elsewhere the caller fetches everything up front.

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const s3 = @import("s3.zig");
const coalescer = @import("coalescer.zig");
const profile = @import("../profile.zig");
const scan = @import("../core/scan.zig");
const metadata = @import("../core/parquet/metadata.zig");

pub const Gate = scan.RowGroupGate;

/// Whether the window runs here. Elsewhere the caller never builds one, and the system calls below are not compiled
/// for the target (their std wrappers are Linux-only).
pub const supported = builtin.os.tag == .linux;

pub const page = std.heap.page_size_min;

/// A row group's `state`.
const State = struct {
    const waiting: u32 = 0;
    const ready: u32 = 1;
    const failed: u32 = 2;
    /// Every work item finished before the bytes landed (the scan skipped it): never admitted, or released on landing.
    const dropped: u32 = 3;
};

pub const Copied = struct {
    dst: []u8,
    src: []const u8,
};

pub const RowGroup = struct {
    /// Requests filling this row group's bytes; their targets lie in `seg`.
    jobs: []s3.FetchJob,
    /// Suffix bytes copied into `seg` when this row group is admitted, so they count toward the window instead of
    /// landing all at once while the fetch is planned.
    copied: []const Copied = &.{},
    /// Bytes the jobs fetch plus `copied`: the row group's window charge.
    bytes: u64,
    /// Coalesce gap for this row group's file, from the footer's first-byte latency. The spans were chosen before
    /// admission; this records the cap that plan used.
    gap: u64 = coalescer.s3_gap_default,
    /// Set when this row group's `bytes` have been added to the window. A group the scan dropped before admission
    /// stays false, so finishing the batch does not release a charge it never took.
    admitted: bool = false,
    /// The page-aligned span of the file buffer only this row group's bytes occupy.
    seg: []align(page) u8,
    window: *Window = undefined,
    pending: std.atomic.Value(u64) = .init(0),
    state: std.atomic.Value(u32) = .init(State.waiting),
    items_done: std.atomic.Value(u32) = .init(0),

    fn landed(ctx: *anyopaque, bytes: usize) void {
        const rg: *RowGroup = @ptrCast(@alignCast(ctx));
        if (rg.pending.fetchSub(bytes, .acq_rel) != bytes) return;
        const prev = rg.state.cmpxchgStrong(State.waiting, State.ready, .acq_rel, .acquire);
        if (prev) |p| {
            // Finished by the scan while in flight: nobody will release it but us.
            if (p == State.dropped) rg.window.release(rg);
            return;
        }
        wakeAll(&rg.state);
    }
};

/// One input file's row groups, null where nothing is fetched (pruned). A row group served from the footer suffix still
/// has an entry: those bytes are copied when it is admitted, so they count toward the window.
pub const File = struct {
    rgs: []?*RowGroup,

    pub fn gate(f: *File) Gate {
        return .{ .ctx = f, .wait = gateWait, .done = gateDone };
    }

    fn gateWait(ctx: *anyopaque, rg_index: usize) anyerror!void {
        const f: *File = @ptrCast(@alignCast(ctx));
        const rg = (if (rg_index < f.rgs.len) f.rgs[rg_index] else null) orelse return;
        var v = rg.state.load(.acquire);
        if (v == State.ready) return;
        const w = rg.window;
        const t0 = nowNs();
        while (v == State.waiting) {
            wait(&rg.state, v, null);
            v = rg.state.load(.acquire);
        }
        _ = w.stalls.fetchAdd(1, .monotonic);
        _ = w.stall_ns.fetchAdd(nowNs() - t0, .monotonic);
        if (v != State.ready) return w.err orelse error.RangeFetchFailed;
    }

    /// One of the row group's `items` work items is done; the last one hands its pages and window charge back.
    fn gateDone(ctx: *anyopaque, rg_index: usize, items: usize) void {
        const f: *File = @ptrCast(@alignCast(ctx));
        const rg = (if (rg_index < f.rgs.len) f.rgs[rg_index] else null) orelse return;
        if (rg.items_done.fetchAdd(1, .acq_rel) + 1 != items) return;
        const prev = rg.state.cmpxchgStrong(State.waiting, State.dropped, .acq_rel, .acquire);
        // Waiting: dropped instead, released on landing if it was in flight. Ready: release now. Failed: the scan is
        // failing; the buffer goes at teardown.
        if (prev != null and prev.? == State.ready) rg.window.release(rg);
    }
};

pub const Stats = struct {
    batches: u64 = 0,
    bytes: u64 = 0,
    /// Times the fetcher waited for the scan to give window back.
    window_waits: u64 = 0,
};

pub fn Fetcher(comptime Pool: type) type {
    return struct {
        const Self = @This();

        window: Window,
        pool: *Pool,
        creds: s3.Credentials,
        /// Concurrent requests per batch.
        workers: usize,
        profiler: ?*profile.Profiler,
        /// The fetch thread's executor, as the up-front fetch builds its own.
        executor: *const fn (std.mem.Allocator) std.Io.Threaded,
        /// This query's data wave, shared by every batch. Footer and Bloom fetches do not use it.
        wave: s3.DataWave = .{},
        thread: ?std.Thread = null,

        /// Start fetching `order` (every fetched row group, in the order workers claim them) on a thread of its own.
        /// `self` must stay put until `stop`.
        pub fn start(self: *Self) !void {
            for (self.window.order) |rg| {
                rg.window = &self.window;
                var pending: u64 = 0;
                for (rg.jobs) |j| pending += j.target.len;
                rg.pending.store(pending, .monotonic);
                for (rg.jobs) |*j| j.landed = .{ .ctx = rg, .f = RowGroup.landed };
            }
            self.thread = try std.Thread.spawn(.{}, run, .{self});
        }

        /// Stop admitting row groups, let the batch in flight finish, and join. Safe after a failure or a finished
        /// scan.
        pub fn stop(self: *Self) void {
            self.window.cancel.store(true, .release);
            self.window.wakeFetcher();
            if (self.thread) |t| t.join();
            self.thread = null;
        }

        fn run(self: *Self) void {
            self.runErr() catch |err| self.window.fail(err);
        }

        fn runErr(self: *Self) !void {
            const w = &self.window;
            var exec = self.executor(w.gpa);
            defer exec.deinit();
            var next: usize = 0;
            // Slow start, for a reader that may stop early (a print's `--limit`): the first batch is one row group, so
            // the first rows are ready soon and little is fetched for nothing; each batch may then take twice the last,
            // up to `batch`. A reader that takes every row group starts at `batch`: halving its first batches only
            // left the connections idle between batches.
            var cap: u64 = if (w.slow_start) 0 else w.batch;
            while (next < w.order.len) {
                if (w.cancel.load(.acquire)) return;
                const first = next;
                var bytes: u64 = 0;
                while (next < w.order.len) {
                    const rg = w.order[next];
                    if (rg.state.load(.acquire) == State.dropped) {
                        next += 1;
                        continue;
                    }
                    // A row group bigger than the window still goes, alone, when nothing else is held.
                    const out = w.submitted -| w.released.load(.acquire);
                    if (out != 0 and out + rg.bytes > w.limit) break;
                    if (bytes != 0 and bytes + rg.bytes > cap) break;
                    w.submitted += rg.bytes;
                    rg.admitted = true;
                    if (builtin.is_test) if (test_drop_after_admit) |drop| if (drop == rg) {
                        rg.state.store(State.dropped, .release);
                    };
                    bytes += rg.bytes;
                    next += 1;
                }
                if (bytes == 0) {
                    if (next == first) w.waitRelease();
                    continue;
                }
                cap = @min(w.batch, @max(cap, bytes) * 2);
                var arena_state = std.heap.ArenaAllocator.init(w.gpa);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                var jobs: std.ArrayList(s3.FetchJob) = .empty;
                for (w.order[first..next]) |rg| {
                    if (rg.state.load(.acquire) == State.dropped) {
                        // Only a group this batch charged. One the admission loop skipped was never added to
                        // `submitted`, and releasing it lets the next group in on a charge that is still held.
                        if (rg.admitted and rg.jobs.len == 0) w.release(rg);
                        continue;
                    }
                    for (rg.copied) |c| @memcpy(c.dst, c.src);
                    if (rg.jobs.len == 0) {
                        const prev = rg.state.cmpxchgStrong(State.waiting, State.ready, .acq_rel, .acquire);
                        if (prev) |p| {
                            if (p == State.dropped) w.release(rg);
                        } else wakeAll(&rg.state);
                        continue;
                    }
                    try jobs.appendSlice(arena, rg.jobs);
                }
                // The spans were chosen before admission and already sit in each row group's buffer. Merging here
                // would allocate a scratch the window never charged.
                const planned = try planDispatch(arena, jobs.items);
                w.stats.bytes += try s3.fetchJobs(
                    exec.io(),
                    self.pool,
                    w.gpa,
                    arena,
                    self.creds,
                    planned.fetches,
                    self.workers,
                    self.profiler,
                    &self.wave,
                );
                for (planned.held) |h| {
                    @memcpy(h.dst, h.src);
                    if (h.landed) |l| l.f(l.ctx, h.dst.len);
                }
                for (planned.fetches) |j| {
                    if (j.range.start == 0 and j.target.len >= 4 and !metadata.hasLeadingMagic(j.target)) {
                        return error.NotParquet;
                    }
                }
                w.stats.batches += 1;
            }
        }
    };
}

const Held = struct {
    dst: []u8,
    src: []const u8,
    landed: ?s3.Landed,
};

/// The requests for one admitted window, in fetch order. The bytes are already the planned spans; this does not
/// merge or allocate.
fn planDispatch(
    arena: std.mem.Allocator,
    jobs: []const s3.FetchJob,
) !struct { fetches: []s3.FetchJob, held: []Held } {
    if (jobs.len == 0) return .{ .fetches = &.{}, .held = &.{} };
    var fetches: std.ArrayList(s3.FetchJob) = .empty;
    try fetches.appendSlice(arena, jobs);
    if (fetches.items.len > 1) {
        var front: std.ArrayList(s3.FetchJob) = .empty;
        var back: std.ArrayList(s3.FetchJob) = .empty;
        for (fetches.items) |j| {
            if (j.urgent) try front.append(arena, j) else try back.append(arena, j);
        }
        var n: usize = 0;
        for (front.items) |j| {
            fetches.items[n] = j;
            n += 1;
        }
        for (back.items) |j| {
            fetches.items[n] = j;
            n += 1;
        }
    }
    return .{ .fetches = fetches.items, .held = &.{} };
}

/// The window's accounting, shared by the fetcher and the scan workers' gates.
pub const Window = struct {
    gpa: std.mem.Allocator,
    order: []*RowGroup,
    /// Fetched-and-unreleased bytes allowed at once.
    limit: u64,
    /// Bytes one fetch batch may take: smaller than the window, so the next batch can be admitted while the scan
    /// works through the last.
    batch: u64,
    /// Start with a one-row-group batch and double (`Fetcher.runErr`): for a reader that may stop before the end.
    slow_start: bool = true,
    /// Fetcher thread only.
    submitted: u64 = 0,
    released: std.atomic.Value(u64) = .init(0),
    rel_gen: std.atomic.Value(u32) = .init(0),
    cancel: std.atomic.Value(bool) = .init(false),
    /// Set before any row group is marked failed, read only after.
    err: ?anyerror = null,
    stats: Stats = .{},
    stalls: std.atomic.Value(u64) = .init(0),
    stall_ns: std.atomic.Value(u64) = .init(0),

    fn release(w: *Window, rg: *RowGroup) void {
        if (rg.seg.len > 0) dontNeed(rg.seg);
        _ = w.released.fetchAdd(rg.bytes, .release);
        w.wakeFetcher();
    }

    fn wakeFetcher(w: *Window) void {
        _ = w.rel_gen.fetchAdd(1, .release);
        wakeAll(&w.rel_gen);
    }

    fn waitRelease(w: *Window) void {
        w.stats.window_waits += 1;
        const g = w.rel_gen.load(.acquire);
        // Bounded, so a missed wake costs a millisecond rather than a hang.
        wait(&w.rel_gen, g, 1_000_000);
    }

    /// A fetch failed: every row group not yet in fails the workers waiting on it.
    fn fail(w: *Window, err: anyerror) void {
        w.err = err;
        w.cancel.store(true, .release);
        for (w.order) |rg| {
            const prev = rg.state.cmpxchgStrong(State.waiting, State.failed, .acq_rel, .acquire);
            if (prev == null) wakeAll(&rg.state);
        }
    }
};

/// The window for an aggregate given `max_memory`: a quarter of it, between 64 MiB and 1 GiB. The rest is for what
/// the scan decodes from the fetched bytes (a row group per worker) and its GROUP BY tables.
pub fn windowFor(max_memory: usize) u64 {
    return std.math.clamp(@as(u64, max_memory) / 4, 64 << 20, 1 << 30);
}

/// A page-aligned anonymous buffer: committed only where written, and given back page by page with MADV_DONTNEED.
pub fn mapBuffer(len: usize) ![]align(page) u8 {
    if (len == 0) return &.{};
    if (comptime !supported) return error.Unsupported;
    const n = std.mem.alignForward(usize, len, page);
    const rc = linux.mmap(null, n, .{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OutOfMemory;
    const ptr: [*]align(page) u8 = @ptrFromInt(rc);
    return ptr[0..n];
}

fn nowNs() u64 {
    if (comptime !supported) return 0;
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

pub fn unmapBuffer(buf: []align(page) u8) void {
    if (comptime !supported) return;
    if (buf.len > 0) _ = linux.munmap(buf.ptr, buf.len);
}

fn wait(word: *std.atomic.Value(u32), expect: u32, timeout_ns: ?u32) void {
    if (comptime !supported) unreachable;
    const ts: linux.timespec = .{ .sec = 0, .nsec = timeout_ns orelse 0 };
    _ = linux.futex_4arg(&word.raw, .{ .cmd = .WAIT, .private = true }, expect, if (timeout_ns != null) &ts else null);
}

fn wakeAll(word: *std.atomic.Value(u32)) void {
    if (comptime !supported) unreachable;
    _ = linux.futex_3arg(&word.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
}

/// The ownership test sets this to a row group that should be dropped the moment it is admitted.
var test_drop_after_admit: ?*RowGroup = null;

fn dontNeed(seg: []align(page) u8) void {
    if (comptime !supported) unreachable;
    _ = linux.madvise(seg.ptr, seg.len, linux.MADV.DONTNEED);
}

const copied_byte = [_]u8{0xab};

fn copiedGroup(buf: []align(page) u8, copy: []const Copied) RowGroup {
    return .{
        .jobs = &.{},
        .copied = copy,
        .bytes = page,
        .seg = buf,
    };
}

fn testExecutor(gpa: std.mem.Allocator) std.Io.Threaded {
    return std.Io.Threaded.init(gpa, .{ .concurrent_limit = .limited(1) });
}

fn startCopied(gpa: std.mem.Allocator, order: []*RowGroup) !Fetcher(s3.Pool(1)) {
    const pool = try gpa.create(s3.Pool(1));
    errdefer gpa.destroy(pool);
    try pool.init(gpa);
    return .{
        .window = .{ .gpa = gpa, .order = order, .limit = page, .batch = page },
        .pool = pool,
        .creds = .{ .access_key = "a", .secret_key = "b", .region = "us-east-1" },
        .workers = 1,
        .profiler = null,
        .executor = testExecutor,
    };
}

fn stopCopied(gpa: std.mem.Allocator, fetcher: *Fetcher(s3.Pool(1))) void {
    fetcher.stop();
    fetcher.pool.deinit();
    gpa.destroy(fetcher.pool);
}

test "a skipped copied-only group does not release a charge it never took" {
    if (builtin.os.tag != .linux) return error.SkipZigTest; // where the window runs (`supported`)
    const gpa = std.testing.allocator;
    var bufs: [3][]align(page) u8 = undefined;
    for (&bufs) |*b| b.* = try mapBuffer(page);
    defer for (bufs) |b| unmapBuffer(b);

    var copies = [3]Copied{
        .{ .dst = bufs[0][0..copied_byte.len], .src = &copied_byte },
        .{ .dst = bufs[1][0..copied_byte.len], .src = &copied_byte },
        .{ .dst = bufs[2][0..copied_byte.len], .src = &copied_byte },
    };
    var one = copiedGroup(bufs[0], copies[0..1]);
    var two = copiedGroup(bufs[1], copies[1..2]);
    var three = copiedGroup(bufs[2], copies[2..3]);
    two.state.store(State.dropped, .release);
    var order = [_]*RowGroup{ &one, &two, &three };
    var fetcher = try startCopied(gpa, &order);
    try fetcher.start();
    const until = nowNs() + 1_000_000_000;
    while (nowNs() < until and one.state.load(.acquire) != State.ready) std.atomic.spinLoopHint();
    stopCopied(gpa, &fetcher);

    try std.testing.expectEqual(State.ready, one.state.load(.acquire));
    try std.testing.expectEqual(State.dropped, two.state.load(.acquire));
    try std.testing.expectEqual(State.waiting, three.state.load(.acquire));
    try std.testing.expectEqual(@as(u64, page), fetcher.window.submitted);
    try std.testing.expectEqual(@as(u64, 0), fetcher.window.released.load(.acquire));
}

test "a copied-only group dropped after it is admitted releases its charge" {
    if (builtin.os.tag != .linux) return error.SkipZigTest; // where the window runs (`supported`)
    const gpa = std.testing.allocator;
    const buf = try mapBuffer(page);
    defer unmapBuffer(buf);
    var copy = [_]Copied{.{ .dst = buf[0..copied_byte.len], .src = &copied_byte }};
    var one = copiedGroup(buf, &copy);
    var order = [_]*RowGroup{&one};
    test_drop_after_admit = &one;
    defer test_drop_after_admit = null;
    var fetcher = try startCopied(gpa, &order);
    // On this thread: admission drops the group before the copy, then releases the charge it just took.
    fetcher.runErr() catch |err| return err;
    stopCopied(gpa, &fetcher);

    try std.testing.expectEqual(State.dropped, one.state.load(.acquire));
    try std.testing.expect(one.admitted);
    try std.testing.expectEqual(@as(u64, page), fetcher.window.submitted);
    try std.testing.expectEqual(@as(u64, page), fetcher.window.released.load(.acquire));
}

test "only a reader that may stop early starts its fetch at one row group" {
    if (comptime !supported) return error.SkipZigTest;
    const gpa = std.testing.allocator;
    for ([_]bool{ false, true }) |slow| {
        var bufs: [4][]align(page) u8 = undefined;
        for (&bufs) |*b| b.* = try mapBuffer(page);
        defer for (bufs) |b| unmapBuffer(b);
        var copies: [4]Copied = undefined;
        var groups: [4]RowGroup = undefined;
        var order: [4]*RowGroup = undefined;
        for (0..4) |i| {
            copies[i] = .{ .dst = bufs[i][0..copied_byte.len], .src = &copied_byte };
            groups[i] = copiedGroup(bufs[i], copies[i .. i + 1]);
            order[i] = &groups[i];
        }
        var fetcher = try startCopied(gpa, &order);
        fetcher.window.limit = 4 * page;
        fetcher.window.batch = 4 * page;
        fetcher.window.slow_start = slow;
        // On this thread, so the batches are counted when it returns.
        fetcher.runErr() catch |err| {
            stopCopied(gpa, &fetcher);
            return err;
        };
        stopCopied(gpa, &fetcher);
        for (&groups) |*g| try std.testing.expectEqual(State.ready, g.state.load(.acquire));
        // One row group, then two, then the last; or, taking every row group, all four in one batch.
        try std.testing.expectEqual(@as(u64, if (slow) 3 else 1), fetcher.window.stats.batches);
    }
}

test "dispatch fetches the admitted spans and does not allocate a second buffer" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var storage: [2][8]u8 = undefined;
    const jobs = [_]s3.FetchJob{
        .{ .bucket = "b", .key = "k", .range = .{ .start = 0, .end = 8 }, .target = &storage[0], .urgent = false },
        .{ .bucket = "b", .key = "k", .range = .{ .start = 100, .end = 108 }, .target = &storage[1], .urgent = true },
    };
    const planned = try planDispatch(arena, &jobs);
    try std.testing.expectEqual(@as(usize, 2), planned.fetches.len);
    try std.testing.expectEqual(@as(usize, 0), planned.held.len);
    try std.testing.expect(planned.fetches[0].urgent);
    var charged: u64 = 0;
    for (planned.fetches) |j| charged += j.target.len;
    try std.testing.expectEqual(@as(u64, 16), charged);
}
