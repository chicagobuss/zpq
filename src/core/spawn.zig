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
            // The last store the region's caller waits on; nothing of `g` is touched after it.
            if (g.pending.fetchSub(1, .acq_rel) == 1) blocking.io.futexWake(u32, &g.pending.raw, 1);
        }
    };
    var group: Group = .{ .ctxs = ctxs };
    var started: u32 = 0;
    for (ctxs) |_| {
        const slot = Pool.acquire() orelse break;
        // Counted before it is posted: a fast context must not see the count reach zero while others are posted.
        _ = group.pending.fetchAdd(1, .monotonic);
        slot.post(Group.runOne, &group);
        started += 1;
    }
    if (started == 0) return false;
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
        arg: *anyopaque = undefined,
        next_idle: ?*Slot = null,
        next_all: ?*Slot = null,

        fn post(s: *Slot, job: *const fn (*anyopaque) void, arg: *anyopaque) void {
            s.job = job;
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

    fn acquire() ?*Slot {
        if (builtin.is_test) if (fail_after) |limit| {
            // Every thread a region obtains counts as a spawn, reused or not, so injected failures stay exact.
            if (success_count.fetchAdd(1, .monotonic) >= limit) {
                _ = injected_count.fetchAdd(1, .monotonic);
                return null;
            }
        };
        lock.lockUncancelable(blocking.io);
        defer lock.unlock(blocking.io);
        const pid = currentPid();
        if (pid != owner_pid) {
            // A forked child inherits the lists but none of the threads: start over (the slots leak, as they must).
            idle = null;
            all = null;
            owner_pid = pid;
        }
        if (idle) |s| {
            idle = s.next_idle;
            s.next_idle = null;
            return s;
        }
        const s = std.heap.page_allocator.create(Slot) catch return null;
        s.* = .{};
        s.thread = std.Thread.spawn(.{ .stack_size = stack_size }, Slot.main, .{s}) catch {
            std.heap.page_allocator.destroy(s);
            return null;
        };
        s.next_all = all;
        all = s;
        return s;
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
