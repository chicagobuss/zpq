//! Thread spawning with a test-only failure seam.
//!
//! Every parallel region must survive `Thread.spawn` failing mid-loop while the threads already running still hold
//! pointers into state the unwinding caller is about to free. That failure cannot be provoked cleanly — `RLIMIT_NPROC`
//! is process-wide and would break the test runner, memory pressure is not reproducible — so tests inject it here.
//! Outside tests this folds to a direct `Thread.spawn`.

const std = @import("std");
const builtin = @import("builtin");

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

/// Run `func(&ctxs[i])` for every context on a thread of its own, spawned as a binary tree: thread i starts threads
/// 2i+1 and 2i+2 before its own work and joins them after it. Spawning costs tens of microseconds, so a loop on one
/// thread starts the last of 16 workers more than a millisecond late; a tree starts it after about four spawns.
///
/// A failed spawn drops that thread and its subtree, so the work must be claimable by whichever threads did start
/// (a shared cursor) and an unrun context must read as idle. Returns false, having run nothing, when not even the
/// root started; the caller then does the work itself. When it returns true every started thread has been joined.
pub fn runTree(comptime Ctx: type, ctxs: []Ctx, config: std.Thread.SpawnConfig, comptime func: fn (*Ctx) void) bool {
    if (ctxs.len == 0) return true;
    const Node = TreeNode(Ctx, func);
    const root = spawn(config, Node.run, .{ ctxs, 0, config }) catch return false;
    root.join();
    return true;
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
