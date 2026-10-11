//! A context's ceiling on the budgets its queries declare, shared by the queries that run on it at once. It admits
//! declared budgets; what a query then allocates is the engine's to hold to its budget (docs/libzpq.md, Memory).
//!
//! A query reserves its whole budget before it starts and returns it when it ends. Reservations are granted in
//! arrival order: a query waits while any query that arrived before it is still waiting, so a large query is never
//! overtaken forever by small ones. A query asking for more than the ceiling is refused at once. `close` wakes every
//! waiter with `error.Closed`.
//!
//! Evictable bytes (F5's footer and ETag cache) will live inside the same ceiling. The head waiter asks `reclaimer`,
//! when one is set, to free what it is short of before it blocks; nothing sets one yet.

const std = @import("std");
const c = std.c;

pub const Error = error{
    /// The query asked for more than the whole ceiling.
    OverCeiling,
    /// `wait_ms` passed before the reservation could be granted.
    WaitTimeout,
    /// `close` has begun.
    Closed,
};

/// Frees evictable bytes held inside the ceiling. Called with the budget's lock held, so it must not call back into
/// the budget; it returns how many bytes it released, which `reserved` stops counting at once.
pub const Reclaimer = struct {
    ctx: *anyopaque,
    reclaimFn: *const fn (ctx: *anyopaque, need: usize) usize,
};

pub const Budget = struct {
    lock: c.pthread_mutex_t = .{},
    cond: c.pthread_cond_t = .{},
    ceiling: usize,
    /// Bytes reserved by running queries, plus evictable bytes once a cache charges them.
    reserved: usize = 0,
    /// The highest `reserved` has been; for tests and stats.
    peak: usize = 0,
    closed: bool = false,
    /// Waiters in arrival order; only the first may be granted.
    queue: std.DoublyLinkedList = .{},
    reclaimer: ?Reclaimer = null,

    const Waiter = struct { node: std.DoublyLinkedList.Node = .{} };

    pub fn init(ceiling: usize) Budget {
        return .{ .ceiling = ceiling };
    }

    /// Reserve `bytes`, waiting at most `wait_ms` (null: as long as it takes) behind the queries that arrived first.
    pub fn acquire(self: *Budget, bytes: usize, wait_ms: ?u64) Error!void {
        if (bytes > self.ceiling) return error.OverCeiling;
        const deadline: ?u64 = if (wait_ms) |ms| monoMs() +| ms else null;
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        if (self.closed) return error.Closed;

        var me: Waiter = .{};
        self.queue.append(&me.node);
        defer {
            self.queue.remove(&me.node);
            // The next waiter may now be first, or may fit in what this one did not take.
            _ = c.pthread_cond_broadcast(&self.cond);
        }
        while (true) {
            if (self.closed) return error.Closed;
            if (self.queue.first == &me.node) {
                if (self.fits(bytes)) {
                    self.reserved += bytes;
                    self.peak = @max(self.peak, self.reserved);
                    return;
                }
            }
            if (deadline) |d| {
                const now = monoMs();
                if (now >= d) return error.WaitTimeout;
                waitSlice(&self.cond, &self.lock, @min(d - now, slice_ms));
            } else {
                waitSlice(&self.cond, &self.lock, slice_ms);
            }
        }
    }

    /// Whether `bytes` more fit, after asking the reclaimer for any shortfall. Called with the lock held.
    fn fits(self: *Budget, bytes: usize) bool {
        const free = self.ceiling - self.reserved;
        if (bytes <= free) return true;
        const r = self.reclaimer orelse return false;
        const released = @min(r.reclaimFn(r.ctx, bytes - free), self.reserved);
        self.reserved -= released;
        return bytes <= self.ceiling - self.reserved;
    }

    pub fn release(self: *Budget, bytes: usize) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        self.reserved -= bytes;
        _ = c.pthread_cond_broadcast(&self.cond);
    }

    /// Refuse new reservations and wake every waiter with `error.Closed`. Running queries keep theirs.
    pub fn close(self: *Budget) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        self.closed = true;
        _ = c.pthread_cond_broadcast(&self.cond);
    }

    /// Wait until no waiter is left in the queue (after `close`, they all leave with `error.Closed`).
    pub fn drainWaiters(self: *Budget) void {
        _ = c.pthread_mutex_lock(&self.lock);
        defer _ = c.pthread_mutex_unlock(&self.lock);
        while (self.queue.first != null) waitSlice(&self.cond, &self.lock, slice_ms);
    }
};

/// The longest one condition wait lasts. Waits take an absolute CLOCK_REALTIME time, which a clock step can move;
/// bounding each wait and re-reading the monotonic clock after it keeps a deadline honest whatever the wall clock does.
const slice_ms: u64 = 100;

fn waitSlice(cond: *c.pthread_cond_t, lock: *c.pthread_mutex_t, ms: u64) void {
    var now: c.timespec = undefined;
    _ = c.clock_gettime(.REALTIME, &now);
    const ns = @as(u64, @intCast(now.nsec)) + ms * std.time.ns_per_ms;
    const abs: c.timespec = .{
        .sec = now.sec + @as(@TypeOf(now.sec), @intCast(ns / std.time.ns_per_s)),
        .nsec = @intCast(ns % std.time.ns_per_s),
    };
    _ = c.pthread_cond_timedwait(cond, lock, &abs);
}

fn monoMs() u64 {
    var ts: c.timespec = undefined;
    _ = c.clock_gettime(.MONOTONIC, &ts);
    return @as(u64, @intCast(ts.sec)) * std.time.ms_per_s + @as(u64, @intCast(ts.nsec)) / std.time.ns_per_ms;
}

const testing = std.testing;

test "a reservation over the ceiling is refused at once" {
    var b: Budget = .init(100);
    try testing.expectError(error.OverCeiling, b.acquire(101, null));
    try b.acquire(100, 0);
    b.release(100);
    try testing.expectEqual(@as(usize, 0), b.reserved);
}

test "a waiter times out when nothing is released, and the queue it leaves lets the next one in" {
    var b: Budget = .init(100);
    try b.acquire(80, null);
    try testing.expectError(error.WaitTimeout, b.acquire(50, 30));
    try b.acquire(20, 0);
    b.release(20);
    b.release(80);
    try testing.expect(b.queue.first == null);
    try testing.expectEqual(@as(usize, 100), b.peak);
}

const Order = struct {
    budget: *Budget,
    bytes: usize,
    hold_ms: u64,
    log: *std.atomic.Value(u32),
    slot: *u32,
    err: ?Error = null,

    fn run(o: *Order) void {
        o.budget.acquire(o.bytes, null) catch |e| {
            o.err = e;
            return;
        };
        o.slot.* = o.log.fetchAdd(1, .monotonic);
        sleepMs(o.hold_ms);
        o.budget.release(o.bytes);
    }
};

fn sleepMs(ms: u64) void {
    const ts: c.timespec = .{ .sec = 0, .nsec = @intCast(ms * std.time.ns_per_ms) };
    _ = c.nanosleep(&ts, null);
}

test "reservations are granted in arrival order, and never exceed the ceiling" {
    var b: Budget = .init(100);
    // Hold the whole ceiling so every later thread queues, in a known order.
    try b.acquire(100, null);
    var grants = std.atomic.Value(u32).init(0);
    const sizes = [_]usize{ 60, 10, 70, 30, 100, 5 };
    var slots: [sizes.len]u32 = @splat(0);
    var orders: [sizes.len]Order = undefined;
    var threads: [sizes.len]std.Thread = undefined;
    for (sizes, 0..) |n, i| {
        orders[i] = .{ .budget = &b, .bytes = n, .hold_ms = 5, .log = &grants, .slot = &slots[i] };
        threads[i] = try std.Thread.spawn(.{}, Order.run, .{&orders[i]});
        // Arrival order: wait until this thread is queued before starting the next.
        while (true) {
            _ = c.pthread_mutex_lock(&b.lock);
            var len: usize = 0;
            var it = b.queue.first;
            while (it) |node| : (it = node.next) len += 1;
            _ = c.pthread_mutex_unlock(&b.lock);
            if (len == i + 1) break;
            sleepMs(1);
        }
    }
    b.release(100);
    for (threads) |t| t.join();
    for (orders) |o| try testing.expect(o.err == null);
    // Granted in the order they arrived, whatever their sizes.
    for (slots, 0..) |s, i| try testing.expectEqual(@as(u32, @intCast(i)), s);
    try testing.expect(b.peak <= b.ceiling);
    try testing.expectEqual(@as(usize, 0), b.reserved);
}

test "close wakes every waiter with Closed and refuses new reservations" {
    var b: Budget = .init(100);
    try b.acquire(100, null);
    var grants = std.atomic.Value(u32).init(0);
    var slots: [3]u32 = @splat(0);
    var orders: [3]Order = undefined;
    var threads: [3]std.Thread = undefined;
    for (&orders, &threads, 0..) |*o, *t, i| {
        o.* = .{ .budget = &b, .bytes = 10, .hold_ms = 0, .log = &grants, .slot = &slots[i] };
        t.* = try std.Thread.spawn(.{}, Order.run, .{o});
    }
    while (true) {
        _ = c.pthread_mutex_lock(&b.lock);
        const all = b.queue.first != null and b.queue.first.?.next != null and b.queue.first.?.next.?.next != null;
        _ = c.pthread_mutex_unlock(&b.lock);
        if (all) break;
        sleepMs(1);
    }
    b.close();
    b.drainWaiters();
    for (threads) |t| t.join();
    for (orders) |o| try testing.expectEqual(@as(?Error, error.Closed), o.err);
    try testing.expectError(error.Closed, b.acquire(1, null));
    b.release(100);
}

test "the head waiter reclaims evictable bytes before it blocks" {
    const Cache = struct {
        held: usize,
        fn reclaim(ctx: *anyopaque, need: usize) usize {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const n = @min(need, self.held);
            self.held -= n;
            return n;
        }
    };
    var cache: Cache = .{ .held = 40 };
    var b: Budget = .init(100);
    b.reserved = 40; // the cache's evictable bytes, charged inside the ceiling
    b.reclaimer = .{ .ctx = &cache, .reclaimFn = Cache.reclaim };
    try b.acquire(90, 0);
    try testing.expectEqual(@as(usize, 10), cache.held);
    try testing.expectEqual(@as(usize, 100), b.reserved);
}
