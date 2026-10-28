const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");

/// Merge an accumulator while transferring ownership of any string winner
/// out of the source. `Accumulator.merge` intentionally consumes its
/// source's owned string; clear the source so its owner (a GroupTable, a
/// work item) cannot free it a second time.
pub fn mergeAccOwned(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, allocator: std.mem.Allocator) anyerror!void {
    return mergeOwnedAccumulator(dst, src, allocator);
}

pub fn detachOwned(acc: *expr_agg.Accumulator) void {
    switch (acc.*) {
        .min_bytes => acc.* = .{ .min_bytes = null },
        .max_bytes => acc.* = .{ .max_bytes = null },
        .distinct => acc.* = .{ .distinct = .{} },
        else => {},
    }
}

pub fn mergeOwnedAccumulator(
    dst: *expr_agg.Accumulator,
    src: *expr_agg.Accumulator,
    allocator: std.mem.Allocator,
) !void {
    if (src.* == .count and dst.* == .count) {
        dst.count += src.count;
        return;
    }
    if (src.* == .count and dst.* == .distinct) {
        const n = dst.distinct.count() + src.count;
        dst.distinct.deinit(allocator);
        dst.* = .{ .count = n };
        return;
    }
    if (src.* == .distinct and dst.* == .count) {
        dst.count += src.distinct.count();
        src.distinct.deinit(allocator);
        src.* = .{ .distinct = .{} };
        return;
    }
    if (src.* == .distinct and dst.* == .distinct) {
        // The larger spilled set is handed over whole and the smaller folded into it.
        return dst.distinct.takeFrom(&src.distinct, allocator);
    }
    const moved = src.*;
    try dst.merge(moved, allocator);
    switch (src.*) {
        .min_bytes => src.* = .{ .min_bytes = null },
        .max_bytes => src.* = .{ .max_bytes = null },
        .min_arena => src.min_arena.ref = null,
        .max_arena => src.max_arena.ref = null,
        else => {},
    }
}

pub fn freeOwnedAccumulatorStrings(
    allocator: std.mem.Allocator,
    accumulators: []expr_agg.Accumulator,
) void {
    for (accumulators) |*acc| switch (acc.*) {
        .min_bytes => |s| if (s) |owned| {
            allocator.free(owned);
            acc.min_bytes = null;
        },
        .max_bytes => |s| if (s) |owned| {
            allocator.free(owned);
            acc.max_bytes = null;
        },
        .distinct => |*d| d.deinit(allocator),
        else => {},
    };
}

test "mergeOwnedAccumulator: distinct sets merge whichever side is larger, own every value once, and leak nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn set(gpa: std.mem.Allocator, vals: []const u64) !*expr_agg.DistinctSet {
            const ds = try gpa.create(expr_agg.DistinctSet);
            ds.* = .{};
            errdefer {
                ds.deinit(gpa);
                gpa.destroy(ds);
            }
            for (vals) |v| try ds.insertU64(gpa, v);
            return ds;
        }
        fn run(gpa: std.mem.Allocator) !void {
            for ([_][2][]const u64{
                .{ &.{ 1, 2 }, &.{ 2, 3, 4, 5 } }, // theirs larger: handed over
                .{ &.{ 1, 2, 3, 4 }, &.{ 4, 5 } }, // ours larger
                .{ &.{}, &.{ 1, 2, 3, 4, 5 } },
            }) |c| {
                var dst: expr_agg.Accumulator = .{ .distinct = .{} };
                defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
                if (c[0].len > 0) dst = .{ .distinct = .fromSet(try set(gpa, c[0])) };
                var src: expr_agg.Accumulator = .{ .distinct = .fromSet(try set(gpa, c[1])) };
                defer freeOwnedAccumulatorStrings(gpa, (&src)[0..1]);
                try mergeOwnedAccumulator(&dst, &src, gpa);
                try testing.expect(src.distinct.heap() == null);
                try testing.expectEqual(@as(usize, 5), dst.distinct.count());
            }
            // Nothing seen on the source side: the destination is untouched.
            var dst: expr_agg.Accumulator = .{ .distinct = .fromSet(try set(gpa, &.{7})) };
            defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
            var none: expr_agg.Accumulator = .{ .distinct = .{} };
            try mergeOwnedAccumulator(&dst, &none, gpa);
            try testing.expectEqual(@as(usize, 1), dst.distinct.count());
        }
    }.run, .{});
}

