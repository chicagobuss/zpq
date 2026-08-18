// ! A slice handed out one element at a time to however many workers ask. ! ! Lets a fixed number of worker loops cover
// an arbitrarily long work list, ! instead of the task-per-item submission that makes `Io.Group.concurrent` ! grow the
// thread pool without bound.

const std = @import("std");

pub fn AtomicWorkCursor(comptime T: type) type {
    return struct {
        items: []T,
        cursor: std.atomic.Value(usize) = .init(0),

        /// The next unclaimed item, or null once the list is drained. `.monotonic` suffices: claims only need to be
        /// distinct, and each item writes its own slot, so there is no ordering to publish.
        pub fn next(self: *@This()) ?*T {
            const i = self.cursor.fetchAdd(1, .monotonic);
            if (i >= self.items.len) return null;
            return &self.items[i];
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
    var hits = [_]std.atomic.Value(u32){.init(0)} ** N;
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
