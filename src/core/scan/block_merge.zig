const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const blocking = @import("../blocking.zig");
const mergeOwnedAccumulator = @import("owned_merge.zig").mergeOwnedAccumulator;

/// Ordered GROUP BY whose results depend on the order partials meet in (`orderSensitive`): each finished block merges
/// into one table, strictly in block order, as soon as every block before it has, and its own table is freed. A worker
/// starts block `b` only once `b` is fewer than `ahead` blocks past the first unmerged one, so at most `ahead` block
/// tables are alive beside the merged one, about what one table per worker holds.
///
/// A worker ahead of the window sleeps on `moved` rather than spinning: with more workers than free CPUs (an
/// oversubscribed or virtualised host) a yielding waiter takes the CPU the merging worker needs to let it go.
pub const BlockMerge = struct {
    tables: []expr_agg.GroupTable,
    merged: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    gpa: std.mem.Allocator,
    done: []std.atomic.Value(bool),
    /// The first block not yet merged.
    next: std.atomic.Value(usize) = .init(0),
    ahead: usize,
    lock: std.Io.Mutex = .init,
    failed: std.atomic.Value(bool) = .init(false),
    failed_from: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    /// Bumped whenever `next` advances or a worker fails: the word `awaitTurn` sleeps on.
    moved: std.atomic.Value(u32) = .init(0),

    /// Wait until block `b` may start; false once another worker has failed.
    pub fn awaitTurn(self: *BlockMerge, b: usize) bool {
        while (true) {
            // Read `moved` before the condition: a merge or failure between the two changes it, and the wait returns.
            const seen = self.moved.load(.acquire);
            if (self.failed.load(.acquire) and b >= self.failed_from.load(.acquire)) return false;
            if (b < self.next.load(.acquire) + self.ahead) return true;
            blocking.io.futexWaitUncancelable(u32, &self.moved.raw, seen);
        }
    }

    /// Block `b` is complete: merge it, and any finished after it, once every earlier block has merged. Whoever holds
    /// the lock merges every block ready by then, so none is left behind.
    pub fn finish(self: *BlockMerge, b: usize) !void {
        self.done[b].store(true, .release);
        self.lock.lockUncancelable(blocking.io);
        defer self.lock.unlock(blocking.io);
        var n = self.next.load(.monotonic);
        while (n < self.tables.len and self.done[n].load(.acquire)) : (n += 1) {
            const src = &self.tables[n];
            const len = self.calls.len;
            for (0..src.groupCount()) |g| {
                const id = try self.merged.getOrInsert(src.keyAt(g), self.calls, src.first.items[g]);
                const dst = self.merged.accumulators.items[@as(usize, id) * len ..][0..len];
                for (dst, src.accumulators.items[g * len ..][0..len]) |*d, *sa| {
                    try expr_agg.mergePersistent(d, sa, self.gpa, mergeOwnedAccumulator);
                }
            }
            src.deinit();
            src.* = expr_agg.GroupTable.init(self.gpa, 0);
            self.next.store(n + 1, .release);
            self.wake();
        }
    }

    pub fn fail(self: *BlockMerge) void {
        self.failFrom(0);
    }

    pub fn failFrom(self: *BlockMerge, b: usize) void {
        _ = self.failed_from.fetchMin(b, .acq_rel);
        self.failed.store(true, .release);
        self.wake();
    }

    fn wake(self: *BlockMerge) void {
        _ = self.moved.fetchAdd(1, .release);
        blocking.io.futexWake(u32, &self.moved.raw, std.math.maxInt(u32));
    }
};

test "an ordered merge's waiting worker sleeps until the block before its window merges, and wakes on failure" {
    const gpa = testing.allocator;
    var tables: [3]expr_agg.GroupTable = .{ .init(gpa, 0), .init(gpa, 0), .init(gpa, 0) };
    defer for (&tables) |*t| t.deinit();
    var merged = expr_agg.GroupTable.init(gpa, 0);
    defer merged.deinit();
    var done: [3]std.atomic.Value(bool) = @splat(.init(false));
    var m: BlockMerge = .{ .tables = &tables, .merged = &merged, .calls = &.{}, .gpa = gpa, .done = &done, .ahead = 1 };
    const Waiter = struct {
        fn run(merge: *BlockMerge, b: usize, got: *std.atomic.Value(u8)) void {
            got.store(if (merge.awaitTurn(b)) 1 else 2, .release);
        }
    };
    try testing.expect(m.awaitTurn(0));
    var got: std.atomic.Value(u8) = .init(0);
    const t = try std.Thread.spawn(.{}, Waiter.run, .{ &m, 1, &got });
    // Block 1 may not start while block 0 is unmerged.
    for (0..20) |_| {
        try testing.io.sleep(.fromMilliseconds(1), .awake);
        try testing.expectEqual(@as(u8, 0), got.load(.acquire));
    }
    try m.finish(0);
    t.join();
    try testing.expectEqual(@as(u8, 1), got.load(.acquire));
    try testing.expectEqual(@as(usize, 1), m.next.load(.acquire));

    var failed: std.atomic.Value(u8) = .init(0);
    const t2 = try std.Thread.spawn(.{}, Waiter.run, .{ &m, 2, &failed });
    try testing.io.sleep(.fromMilliseconds(2), .awake);
    try testing.expectEqual(@as(u8, 0), failed.load(.acquire));
    m.fail();
    t2.join();
    try testing.expectEqual(@as(u8, 2), failed.load(.acquire));
}

test "a later failed ordered block cannot cancel an earlier block before it starts" {
    var tables: [3]expr_agg.GroupTable = undefined;
    var merged: expr_agg.GroupTable = undefined;
    var done: [3]std.atomic.Value(bool) = @splat(.init(false));
    var m: BlockMerge = .{
        .tables = &tables,
        .merged = &merged,
        .calls = &.{},
        .gpa = testing.allocator,
        .done = &done,
        .ahead = 3,
    };
    m.failFrom(2);
    try testing.expect(m.awaitTurn(0));
    try testing.expect(m.awaitTurn(1));
    try testing.expect(!m.awaitTurn(2));
    m.failFrom(1);
    m.failFrom(2);
    try testing.expect(!m.awaitTurn(1));
    try testing.expect(m.awaitTurn(0));
}

