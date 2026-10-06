//! A slice handed out one element at a time, so a fixed number of worker loops can cover an arbitrarily long work
//! list. The task-per-item alternative makes `Io.Group.concurrent` grow the thread pool without bound.
//!
//! `startWorkers` starts those loops on an executor with a `concurrent_limit` without ever failing the batch.

const std = @import("std");
const Io = std.Io;
const clock = @import("../clock.zig");

/// Longest the caller waits for the executor to accept a worker before it starts handling items itself.
const MAX_SUBMIT_WAIT_NS: i64 = 5 * std.time.ns_per_ms;

/// Start up to `workers` loops on `group`, each passing items from `cursor` to `handle(context, item)` until it is
/// drained. An executor at its `concurrent_limit` delays a loop instead of failing the batch with
/// `error.ConcurrencyUnavailable`.
///
/// `std.Io.Threaded` counts a worker as busy until it re-locks the pool after its task returns, which is after the
/// task's group has woken its awaiter. A stage that awaits one group and at once fills the next up to the limit can
/// therefore find finished workers still counted. That lag lasts microseconds, so the caller first waits, up to
/// `MAX_SUBMIT_WAIT_NS` of real time. If the executor still has no room (every worker genuinely busy, or a thread the OS
/// will not create), the caller handles one item itself and then tries again, so the batch keeps its other loops and
/// gains the rest as soon as workers free up. Returns once every loop is started or the cursor is drained; the caller
/// still awaits `group`.
pub fn startWorkers(
    comptime T: type,
    group: *Io.Group,
    io: Io,
    cursor: *AtomicWorkCursor(T),
    workers: usize,
    context: anytype,
    comptime handle: fn (@TypeOf(context), *T) Io.Cancelable!void,
) Io.Cancelable!void {
    const Loop = struct {
        fn run(cur: *AtomicWorkCursor(T), ctx: @TypeOf(context)) Io.Cancelable!void {
            while (cur.next()) |item| try handle(ctx, item);
        }
    };
    var started: usize = 0;
    var waiting_since: ?i64 = null;
    var pause_ns: i64 = 10 * std.time.ns_per_us;
    while (started < workers and !cursor.drained()) {
        if (group.concurrent(io, Loop.run, .{ cursor, context })) |_| {
            started += 1;
            waiting_since = null;
            pause_ns = 10 * std.time.ns_per_us;
            continue;
        } else |err| switch (err) {
            error.ConcurrencyUnavailable => {},
        }
        const now = clock.monoNs();
        const since = waiting_since orelse now;
        waiting_since = since;
        if (now - since < MAX_SUBMIT_WAIT_NS) {
            try io.sleep(.fromNanoseconds(pause_ns), .awake);
            pause_ns = @min(pause_ns * 2, std.time.ns_per_ms);
            continue;
        }
        const item = cursor.next() orelse return;
        try handle(context, item);
    }
}

pub fn AtomicWorkCursor(comptime T: type) type {
    return struct {
        items: []T,
        cursor: std.atomic.Value(usize) = .init(0),

        /// `.monotonic` suffices: claims only need to be distinct, and each item writes its own slot, so there is no
        /// ordering to publish.
        pub fn next(self: *@This()) ?*T {
            const i = self.cursor.fetchAdd(1, .monotonic);
            if (i >= self.items.len) return null;
            return &self.items[i];
        }

        /// Every item has been handed out (though not necessarily finished).
        pub fn drained(self: *const @This()) bool {
            return self.cursor.load(.monotonic) >= self.items.len;
        }
    };
}

test "AtomicWorkCursor hands out every item exactly once" {
    var items = [_]u32{ 0, 1, 2, 3, 4, 5, 6 };
    var cur: AtomicWorkCursor(u32) = .{ .items = &items };

    var seen: [7]bool = @splat(false);
    var count: usize = 0;
    while (cur.next()) |p| {
        const i = p.*;
        try std.testing.expect(!seen[i]);
        seen[i] = true;
        count += 1;
    }
    try std.testing.expectEqual(@as(usize, 7), count);
    for (seen) |s| try std.testing.expect(s);

    // Drained stays drained: a worker loop that keeps asking gets null rather than wrapping or running off the end.
    try std.testing.expectEqual(@as(?*u32, null), cur.next());
    try std.testing.expectEqual(@as(?*u32, null), cur.next());
}

test "AtomicWorkCursor gives disjoint items to concurrent workers" {
    const N = 512;
    var items: [N]usize = undefined;
    for (&items, 0..) |*it, i| it.* = i;

    // A double claim would show up as a count of two in `hits`.
    var hits: [N]std.atomic.Value(u32) = @splat(.init(0));
    var cur: AtomicWorkCursor(usize) = .{ .items = &items };

    const Worker = struct {
        fn run(wc: *AtomicWorkCursor(usize), h: []std.atomic.Value(u32)) void {
            while (wc.next()) |p| _ = h[p.*].fetchAdd(1, .monotonic);
        }
    };

    var threads: [8]std.Thread = undefined;
    var spawned: usize = 0;
    for (&threads) |*t| {
        t.* = std.Thread.spawn(.{}, Worker.run, .{ &cur, hits[0..] }) catch break;
        spawned += 1;
    }
    for (threads[0..spawned]) |t| t.join();
    if (spawned == 0) return error.SkipZigTest;

    for (&hits) |*h| try std.testing.expectEqual(@as(u32, 1), h.load(.monotonic));
}

test "startWorkers fills stages that refill the limit as soon as the previous one is awaited" {
    // The read stage's shape: a one-worker metadata stage, then a full-width fetch stage, on one limited executor.
    // Submitting straight to `group.concurrent`, a few hundred of these rounds were enough to see
    // `error.ConcurrencyUnavailable`.
    const limit = 4;
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(limit) });
    defer threaded.deinit();
    const io = threaded.io();

    const Count = struct {
        fn one(n: *std.atomic.Value(usize), _: *u8) Io.Cancelable!void {
            _ = n.fetchAdd(1, .monotonic);
        }
    };
    var items: [limit]u8 = undefined;
    var ran: std.atomic.Value(usize) = .init(0);
    const rounds = 2000;
    for (0..rounds) |_| {
        inline for (.{ 1, limit }) |width| {
            var cur: AtomicWorkCursor(u8) = .{ .items = items[0..width] };
            var group: Io.Group = .init;
            defer group.cancel(io);
            try startWorkers(u8, &group, io, &cur, width, &ran, Count.one);
            try group.await(io);
        }
    }
    try std.testing.expectEqual(@as(usize, rounds * (1 + limit)), ran.load(.monotonic));
}

test "startWorkers completes a batch wider than the limit and keeps it parallel when a worker frees up" {
    // Two executor slots, one held for 10 ms by another group's task: the second loop cannot start at first. The
    // caller must handle items one at a time meanwhile, not drain the batch alone, so that the second loop starts on
    // a worker once the slot frees.
    var threaded = std.Io.Threaded.init(std.testing.allocator, .{ .concurrent_limit = .limited(2) });
    defer threaded.deinit();
    const io = threaded.io();

    const Ctx = struct {
        io: Io,
        fn item(c: *@This(), ran_on: *std.Thread.Id) Io.Cancelable!void {
            try c.io.sleep(.fromMilliseconds(2), .awake);
            ran_on.* = std.Thread.getCurrentId();
        }
        fn hold(t_io: Io) Io.Cancelable!void {
            try t_io.sleep(.fromMilliseconds(10), .awake);
        }
    };
    var ctx: Ctx = .{ .io = io };
    const caller = std.Thread.getCurrentId();
    var ran_on: [60]std.Thread.Id = @splat(caller);
    var cur: AtomicWorkCursor(std.Thread.Id) = .{ .items = &ran_on };

    var other: Io.Group = .init;
    defer other.cancel(io);
    try other.concurrent(io, Ctx.hold, .{io});

    var group: Io.Group = .init;
    defer group.cancel(io);
    // Four loops asked for, two slots: the batch completes all the same.
    try startWorkers(std.Thread.Id, &group, io, &cur, 4, &ctx, Ctx.item);
    try group.await(io);
    try other.await(io);

    // Every item ran, and on two distinct workers besides the caller: the second loop did start once the slot freed.
    var workers: [2]?std.Thread.Id = .{ null, null };
    for (ran_on) |id| {
        if (id == caller) continue;
        for (&workers) |*w| {
            if (w.* == null) w.* = id;
            if (w.* == id) break;
        } else return error.TestUnexpectedResult;
    }
    try std.testing.expect(workers[1] != null);
}
