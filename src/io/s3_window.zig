//! An aggregate's S3 row groups fetched ahead of the scan workers within a byte window, in the order workers claim
//! them, each row group's pages handed back once its work items are done (`scan.Input.gate`). The fetched bytes held
//! at once are then bounded by the window, not by everything the query reads, so a scan larger than memory completes.
//!
//! Each row group's chunks sit in their own page-aligned segment of an anonymous buffer laid out by the caller, so a
//! finished row group's pages go back with MADV_DONTNEED without touching a neighbour's. Linux only, like
//! `local_reader.zig`; elsewhere the caller fetches everything up front.

const std = @import("std");
const linux = std.os.linux;
const s3 = @import("s3.zig");
const profile = @import("../profile.zig");
const scan = @import("../core/scan.zig");

pub const Gate = scan.RowGroupGate;

pub const page = std.heap.page_size_min;

/// A row group's `state`.
const State = struct {
    const waiting: u32 = 0;
    const ready: u32 = 1;
    const failed: u32 = 2;
    /// Every work item finished before the bytes landed (the scan skipped it): never admitted, or released on landing.
    const dropped: u32 = 3;
};

pub const RowGroup = struct {
    /// Requests filling this row group's bytes; their targets lie in `seg`.
    jobs: []s3.FetchJob,
    /// Bytes the jobs fetch: the row group's window charge.
    bytes: u64,
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
        _ = linux.futex_3arg(&rg.state.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
    }
};

/// One input file's row groups, null where nothing is fetched (pruned, or served whole from the footer tail).
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
            _ = linux.futex_4arg(&rg.state.raw, .{ .cmd = .WAIT, .private = true }, v, null);
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
        thread: ?std.Thread = null,

        /// Start fetching `order` (every fetched row group, in the order workers claim them) on a thread of its own.
        /// `self` must stay put until `stop`.
        pub fn start(self: *Self) !void {
            for (self.window.order) |rg| {
                rg.window = &self.window;
                rg.pending.store(rg.bytes, .monotonic);
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
                    if (bytes != 0 and bytes + rg.bytes > w.batch) break;
                    w.submitted += rg.bytes;
                    bytes += rg.bytes;
                    next += 1;
                }
                if (bytes == 0) {
                    if (next == first) w.waitRelease();
                    continue;
                }
                var arena_state = std.heap.ArenaAllocator.init(w.gpa);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                var jobs: std.ArrayList(s3.FetchJob) = .empty;
                for (w.order[first..next]) |rg| {
                    if (rg.state.load(.acquire) == State.dropped) continue;
                    try jobs.appendSlice(arena, rg.jobs);
                }
                w.stats.bytes += try s3.fetchJobs(
                    exec.io(),
                    self.pool,
                    w.gpa,
                    arena,
                    self.creds,
                    jobs.items,
                    self.workers,
                    self.profiler,
                );
                w.stats.batches += 1;
            }
        }
    };
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
        if (rg.seg.len > 0) _ = linux.madvise(rg.seg.ptr, rg.seg.len, linux.MADV.DONTNEED);
        _ = w.released.fetchAdd(rg.bytes, .release);
        w.wakeFetcher();
    }

    fn wakeFetcher(w: *Window) void {
        _ = w.rel_gen.fetchAdd(1, .release);
        _ = linux.futex_3arg(&w.rel_gen.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
    }

    fn waitRelease(w: *Window) void {
        w.stats.window_waits += 1;
        const g = w.rel_gen.load(.acquire);
        // Bounded, so a missed wake costs a millisecond rather than a hang.
        const ts: linux.timespec = .{ .sec = 0, .nsec = 1_000_000 };
        _ = linux.futex_4arg(&w.rel_gen.raw, .{ .cmd = .WAIT, .private = true }, g, &ts);
    }

    /// A fetch failed: every row group not yet in fails the workers waiting on it.
    fn fail(w: *Window, err: anyerror) void {
        w.err = err;
        w.cancel.store(true, .release);
        for (w.order) |rg| {
            const prev = rg.state.cmpxchgStrong(State.waiting, State.failed, .acq_rel, .acquire);
            if (prev == null) _ = linux.futex_3arg(&rg.state.raw, .{ .cmd = .WAKE, .private = true }, std.math.maxInt(i32));
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
    const n = std.mem.alignForward(usize, len, page);
    const rc = linux.mmap(null, n, .{ .READ = true, .WRITE = true }, .{ .TYPE = .PRIVATE, .ANONYMOUS = true }, -1, 0);
    if (linux.errno(rc) != .SUCCESS) return error.OutOfMemory;
    const ptr: [*]align(page) u8 = @ptrFromInt(rc);
    return ptr[0..n];
}

fn nowNs() u64 {
    var ts: linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = linux.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

pub fn unmapBuffer(buf: []align(page) u8) void {
    if (buf.len > 0) _ = linux.munmap(buf.ptr, buf.len);
}
