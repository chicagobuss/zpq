//! Thread spawning with a test-only failure seam.
//!
//! Every parallel region must survive `Thread.spawn` failing mid-loop while the threads already running still hold
//! pointers into state the unwinding caller is about to free. That failure cannot be provoked cleanly — `RLIMIT_NPROC`
//! is process-wide and would break the test runner, memory pressure is not reproducible — so tests inject it here.
//! Outside tests this folds to a direct `Thread.spawn`.

const std = @import("std");
const builtin = @import("builtin");
const blocking = @import("blocking.zig");

/// Test-only. Atomic because `runTree` spawns from several threads at once; set and reset only between regions.
var fail_after: ?usize = null;
var success_count: std.atomic.Value(usize) = .init(0);
/// Lets a test assert its injection was not vacuous: a region that never spawned would otherwise pass while testing
/// nothing.
var injected_count: std.atomic.Value(usize) = .init(0);

/// Fail every spawn after `n` successes. Call `resetFailure` when done, or the next test in the same binary inherits
/// the injection.
pub fn injectFailureAfter(n: usize) void {
    std.debug.assert(builtin.is_test);
    fail_after = n;
    success_count.store(0, .monotonic);
    injected_count.store(0, .monotonic);
}

pub fn resetFailure() void {
    fail_after = null;
    success_count.store(0, .monotonic);
    injected_count.store(0, .monotonic);
}

pub fn failuresInjected() usize {
    return injected_count.load(.monotonic);
}

/// For the binaries' `std_options`. std reserves this alternate signal stack as static TLS, which the C library
/// zero-fills at every thread start (scan workers start per pass); only a safety-checked build's segfault handler runs
/// on it, so other builds go without.
pub const signal_stack_size: ?u64 = if (std.debug.default_enable_segfault_handler)
    (std.Options{}).signal_stack_size
else
    null;

pub fn spawn(
    config: std.Thread.SpawnConfig,
    comptime function: anytype,
    args: anytype,
) std.Thread.SpawnError!std.Thread {
    if (builtin.is_test) {
        if (fail_after) |limit| {
            if (success_count.fetchAdd(1, .monotonic) >= limit) {
                _ = injected_count.fetchAdd(1, .monotonic);
                // What real thread-table exhaustion (EAGAIN) surfaces, so callers are exercised on the error they
                // will actually see.
                return error.SystemResources;
            }
        }
    }
    return std.Thread.spawn(config, function, args);
}

/// Run `func(&ctxs[i])` for every context on a thread of its own. The threads come from a process-wide pool that
/// outlives each parallel region (`Pool`), so a query's passes and the queries of a long-lived process (libzpq, the
/// service) reuse warm threads instead of spawning and joining tens of them. The pool grows on demand: a region gets
/// one thread per context, never a context queued behind another, since contexts may wait on each other's progress.
///
/// A thread the pool cannot provide (a failed spawn) leaves its context unrun, so the work must be claimable by
/// whichever threads did start (a shared cursor) and an unrun context must read as idle. Returns false, having run
/// nothing, when not one thread started; the caller then does the work itself. When it returns true every started
/// context has finished. A stack larger than the pool's falls back to a tree of fresh threads with that stack.
pub fn runTree(comptime Ctx: type, ctxs: []Ctx, config: std.Thread.SpawnConfig, comptime func: fn (*Ctx) void) bool {
    if (ctxs.len == 0) return true;
    if (builtin.single_threaded or config.stack_size > Pool.stack_size) {
        const Node = TreeNode(Ctx, func);
        const root = spawn(config, Node.run, .{ ctxs, 0, config }) catch return false;
        root.join();
        return true;
    }
    const Group = struct {
        ctxs: []Ctx,
        next: std.atomic.Value(usize) = .init(0),
        pending: std.atomic.Value(u32) = .init(0),

        fn runOne(arg: *anyopaque) void {
            const g: *@This() = @ptrCast(@alignCast(arg));
            func(&g.ctxs[g.next.fetchAdd(1, .monotonic)]);
            done(g);
        }
        /// A posted job whose thread never started: it claims no context, which stays unrun.
        fn cancelOne(arg: *anyopaque) void {
            done(@ptrCast(@alignCast(arg)));
        }
        fn done(g: *@This()) void {
            // The last store the region's caller waits on; nothing of `g` is touched after it.
            if (g.pending.fetchSub(1, .acq_rel) == 1) blocking.io.futexWake(u32, &g.pending.raw, 1);
        }
    };
    var group: Group = .{ .ctxs = ctxs };
    // Counted before any job is posted: a fast context must not see the count reach zero while others are posted.
    _ = group.pending.fetchAdd(@intCast(ctxs.len), .monotonic);
    var warm: usize = 0;
    while (warm < ctxs.len) : (warm += 1) {
        const slot = Pool.takeIdle() orelse break;
        slot.post(Group.runOne, Group.cancelOne, &group);
    }
    // The rest start as a tree of fresh pooled threads, each starting its children before its own job, so a cold
    // pool (a short-lived process) starts its last thread after a few spawns, as a spawn tree would.
    var grown = false;
    var fresh: []*Pool.Slot = &.{};
    defer std.heap.page_allocator.free(fresh);
    if (warm < ctxs.len) {
        if (std.heap.page_allocator.alloc(*Pool.Slot, ctxs.len - warm)) |slots| {
            fresh = slots;
            grown = Pool.grow(fresh, Group.runOne, Group.cancelOne, &group);
        } else |_| for (warm..ctxs.len) |_| Group.cancelOne(&group);
    }
    // Not one thread: every job was cancelled, so nothing ran and nothing still refers to `group`.
    if (warm == 0 and !grown) return false;
    while (true) {
        const left = group.pending.load(.acquire);
        if (left == 0) break;
        blocking.io.futexWaitUncancelable(u32, &group.pending.raw, left);
    }
    return true;
}

/// Threads kept for `runTree`. Each waits on its own futex word for one posted job, runs it, and returns itself to
/// the idle list. Idle threads cost a sleeping stack each (reserved, touched only as deep as jobs went). A process
/// that forks discards the parent's pool in the child, where its threads no longer exist. An embedding context
/// (libzpq) holds the pool with `retainPool` and joins its threads at the last `releasePool`; other processes keep
/// it until exit.
pub const Pool = struct {
    /// Every pooled thread's stack: the largest any parallel region asks for (`std.Thread`'s default).
    pub const stack_size: usize = std.Thread.SpawnConfig.default_stack_size;

    const idle_state = 0;
    const posted_state = 1;
    const exit_state = 2;

    pub const Slot = struct {
        thread: std.Thread = undefined,
        state: std.atomic.Value(u32) = .init(idle_state),
        job: *const fn (*anyopaque) void = undefined,
        cancel: *const fn (*anyopaque) void = undefined,
        arg: *anyopaque = undefined,
        next_idle: ?*Slot = null,
        next_all: ?*Slot = null,

        fn post(s: *Slot, job: *const fn (*anyopaque) void, cancel: *const fn (*anyopaque) void, arg: *anyopaque) void {
            s.job = job;
            s.cancel = cancel;
            s.arg = arg;
            s.state.store(posted_state, .release);
            blocking.io.futexWake(u32, &s.state.raw, 1);
        }

        fn main(s: *Slot) void {
            while (true) {
                const state = s.state.load(.acquire);
                if (state == idle_state) {
                    blocking.io.futexWaitUncancelable(u32, &s.state.raw, idle_state);
                    continue;
                }
                if (state == exit_state) return;
                freshThreadState();
                s.job(s.arg);
                s.state.store(idle_state, .release);
                release(s);
            }
        }
    };

    var lock: std.Io.Mutex = .init;
    var idle: ?*Slot = null;
    var all: ?*Slot = null;
    var owner_pid: std.posix.pid_t = 0;
    var holders: usize = 0;

    /// A warm idle thread, or null when none is idle.
    fn takeIdle() ?*Slot {
        lock.lockUncancelable(blocking.io);
        defer lock.unlock(blocking.io);
        const pid = currentPid();
        if (pid != owner_pid) {
            // A forked child inherits the lists but none of the threads: start over (the slots leak, as they must).
            idle = null;
            all = null;
            owner_pid = pid;
        }
        const s = idle orelse return null;
        if (injectedFailure()) return null;
        idle = s.next_idle;
        s.next_idle = null;
        return s;
    }

    /// Post the job to `fresh.len` new threads started as a binary tree. A thread that cannot be created cancels its
    /// job and its subtree's. True when the root started (so at least one job runs). `fresh` is the caller's, and
    /// must outlive the region: the tree's threads read it until each has started its children.
    fn grow(fresh: []*Slot, job: *const fn (*anyopaque) void, cancel: *const fn (*anyopaque) void, arg: *anyopaque) bool {
        const n = fresh.len;
        var made: usize = 0;
        while (made < n) : (made += 1) {
            const s = std.heap.page_allocator.create(Slot) catch break;
            s.* = .{};
            s.post(job, cancel, arg);
            fresh[made] = s;
        }
        for (made..n) |_| cancel(arg);
        if (made == 0) return false;
        // Every spawn below finishes before its parent runs its job, so before the region's count reaches zero.
        return start(fresh[0..made], 0);
    }

    fn start(fresh: []*Slot, i: usize) bool {
        const s = fresh[i];
        if (injectedFailure()) {
            cancelSubtree(fresh, i);
            return false;
        }
        const t = std.Thread.spawn(.{ .stack_size = stack_size }, boot, .{ fresh, i }) catch {
            cancelSubtree(fresh, i);
            return false;
        };
        lock.lockUncancelable(blocking.io);
        defer lock.unlock(blocking.io);
        s.thread = t;
        s.next_all = all;
        all = s;
        return true;
    }

    fn boot(fresh: []*Slot, i: usize) void {
        for ([_]usize{ 2 * i + 1, 2 * i + 2 }) |c| if (c < fresh.len) {
            _ = start(fresh, c);
        };
        Slot.main(fresh[i]);
    }

    fn cancelSubtree(fresh: []*Slot, i: usize) void {
        if (i >= fresh.len) return;
        cancelSubtree(fresh, 2 * i + 1);
        cancelSubtree(fresh, 2 * i + 2);
        // This node's cancel may be the region's last count: nothing of `fresh` is read after it.
        const s = fresh[i];
        const cancel = s.cancel;
        const arg = s.arg;
        std.heap.page_allocator.destroy(s);
        cancel(arg);
    }

    /// Test-only: every thread a region obtains counts as a spawn, warm or fresh, so injected failures stay exact.
    fn injectedFailure() bool {
        if (!builtin.is_test) return false;
        const limit = fail_after orelse return false;
        if (success_count.fetchAdd(1, .monotonic) < limit) return false;
        _ = injected_count.fetchAdd(1, .monotonic);
        return true;
    }

    fn release(s: *Slot) void {
        lock.lockUncancelable(blocking.io);
        defer lock.unlock(blocking.io);
        s.next_idle = idle;
        idle = s;
    }

    fn currentPid() std.posix.pid_t {
        return if (builtin.os.tag == .linux) std.os.linux.getpid() else std.c.getpid();
    }

    /// Threads the pool holds, idle or busy.
    pub fn threadCount() usize {
        lock.lockUncancelable(blocking.io);
        defer lock.unlock(blocking.io);
        var n: usize = 0;
        var s = all;
        while (s) |slot| : (s = slot.next_all) n += 1;
        return n;
    }
};

/// Hold the pool for an embedding context. Pair with `releasePool`.
pub fn retainPool() void {
    Pool.lock.lockUncancelable(blocking.io);
    defer Pool.lock.unlock(blocking.io);
    Pool.holders += 1;
}

/// Drop a hold; the last one joins every pooled thread and returns true. No parallel region may be running.
pub fn releasePool() bool {
    Pool.lock.lockUncancelable(blocking.io);
    Pool.holders -= 1;
    if (Pool.holders > 0) {
        Pool.lock.unlock(blocking.io);
        return false;
    }
    if (Pool.owner_pid != Pool.currentPid()) {
        Pool.lock.unlock(blocking.io);
        return true;
    }
    var list = Pool.all;
    Pool.all = null;
    Pool.idle = null;
    Pool.lock.unlock(blocking.io);
    while (list) |s| {
        list = s.next_all;
        s.state.store(Pool.exit_state, .release);
        blocking.io.futexWake(u32, &s.state.raw, 1);
        s.thread.join();
        std.heap.page_allocator.destroy(s);
    }
    return true;
}

/// The thread-local state a freshly spawned thread starts with. A pooled thread resets it before every job, so no
/// job observes what an earlier region on the same thread left (a decode failure site, a borrowed charge).
fn freshThreadState() void {
    @import("consumer.zig").setDecodeFailure(null);
    @import("expr/agg.zig").distinct_pairs = null;
    @import("expr/agg.zig").winner_charge = null;
    @import("expr/eval.zig").forced_batch_rows = null;
}

fn TreeNode(comptime Ctx: type, comptime func: fn (*Ctx) void) type {
    return struct {
        fn run(ctxs: []Ctx, i: usize, config: std.Thread.SpawnConfig) void {
            var kids: [2]?std.Thread = .{ null, null };
            for (&kids, 1..) |*k, d| {
                const c = 2 * i + d;
                if (c < ctxs.len) k.* = spawn(config, run, .{ ctxs, c, config }) catch null;
            }
            func(&ctxs[i]);
            for (kids) |k| if (k) |t| t.join();
        }
    };
}

test "runTree runs every context once, and only a started subtree when spawns fail" {
    const Ctx = struct {
        runs: usize = 0,
        fn bump(c: *@This()) void {
            c.runs += 1;
        }
    };
    var ctxs: [13]Ctx = @splat(.{});
    try std.testing.expect(runTree(Ctx, &ctxs, .{}, Ctx.bump));
    for (ctxs) |c| try std.testing.expectEqual(@as(usize, 1), c.runs);

    // One spawn succeeds: the root, whose children both fail, so it runs alone.
    injectFailureAfter(1);
    defer resetFailure();
    var root_only: [13]Ctx = @splat(.{});
    try std.testing.expect(runTree(Ctx, &root_only, .{}, Ctx.bump));
    for (root_only, 0..) |c, i| try std.testing.expectEqual(@as(usize, @intFromBool(i == 0)), c.runs);
    try std.testing.expect(failuresInjected() > 0);

    // Three spawns succeed: the root, then two of the nodes racing to start below it. Which two depends on timing
    // (node 1 may start node 3 before the root starts node 2), so check what holds either way: three nodes ran, once
    // each, and each one's parent ran too.
    injectFailureAfter(3);
    var some: [13]Ctx = @splat(.{});
    try std.testing.expect(runTree(Ctx, &some, .{}, Ctx.bump));
    var ran: usize = 0;
    for (some, 0..) |c, i| {
        try std.testing.expect(c.runs <= 1);
        ran += c.runs;
        if (c.runs == 1 and i > 0) try std.testing.expectEqual(@as(usize, 1), some[(i - 1) / 2].runs);
    }
    try std.testing.expectEqual(@as(usize, 3), ran);
    try std.testing.expect(failuresInjected() > 0);

    injectFailureAfter(0);
    var none: [4]Ctx = @splat(.{});
    try std.testing.expect(!runTree(Ctx, &none, .{}, Ctx.bump));
    for (none) |c| try std.testing.expectEqual(@as(usize, 0), c.runs);
}

test "pooled regions reuse threads, nest, give every context its own thread and reset thread state" {
    const Ctx = struct {
        arrived: *std.atomic.Value(u32),
        total: u32,
        nested: bool = false,
        runs: usize = 0,
        saw_pairs: bool = false,
        fn run(c: *@This()) void {
            c.runs += 1;
            // Every context waits for all of them: queueing one behind another on a thread would never finish.
            _ = c.arrived.fetchAdd(1, .acq_rel);
            while (c.arrived.load(.acquire) < c.total) std.atomic.spinLoopHint();
            if (c.nested) {
                var inner_arrived: std.atomic.Value(u32) = .init(0);
                var inner: [3]@This() = @splat(.{ .arrived = &inner_arrived, .total = 3 });
                std.debug.assert(runTree(@This(), &inner, .{}, run));
                for (inner) |i| std.debug.assert(i.runs == 1);
            }
            c.saw_pairs = @import("expr/agg.zig").distinct_pairs != null;
            @import("expr/agg.zig").distinct_pairs = @ptrFromInt(64);
        }
    };
    var arrived: std.atomic.Value(u32) = .init(0);
    var first: [8]Ctx = @splat(.{ .arrived = &arrived, .total = 8 });
    first[3].nested = true;
    try std.testing.expect(runTree(Ctx, &first, .{}, Ctx.run));
    for (first) |c| try std.testing.expectEqual(@as(usize, 1), c.runs);
    const threads = Pool.threadCount();
    try std.testing.expect(threads >= 11);
    arrived.store(0, .monotonic);
    var again: [8]Ctx = @splat(.{ .arrived = &arrived, .total = 8 });
    try std.testing.expect(runTree(Ctx, &again, .{}, Ctx.run));
    // Warm threads served the second region, and none saw the state the first region's jobs left behind.
    try std.testing.expectEqual(threads, Pool.threadCount());
    for (again) |c| {
        try std.testing.expectEqual(@as(usize, 1), c.runs);
        try std.testing.expect(!c.saw_pairs);
    }
    // A stack past the pool's still runs, on a tree of fresh threads.
    arrived.store(0, .monotonic);
    var big: [2]Ctx = @splat(.{ .arrived = &arrived, .total = 2 });
    try std.testing.expect(runTree(Ctx, &big, .{ .stack_size = Pool.stack_size * 2 }, Ctx.run));
    for (big) |c| try std.testing.expectEqual(@as(usize, 1), c.runs);
}
