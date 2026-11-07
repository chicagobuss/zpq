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
        .blob => |*b| b.release(),
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
        .blob => |*b| b.release(),
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
        .blob => |*b| b.deinit(allocator),
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


test "mergeOwnedAccumulator: sketches fold or move whole, the source is left empty, and nothing leaks" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var a: expr_agg.Accumulator = .{ .blob = .{ .kind = .hll } };
            defer a.blob.deinit(gpa);
            var b: expr_agg.Accumulator = .{ .blob = .{ .kind = .hll } };
            defer b.blob.deinit(gpa);
            var c: expr_agg.Accumulator = .{ .blob = .{ .kind = .hll } };
            defer c.blob.deinit(gpa);
            (try a.blob.sketch(gpa, null))[3] = 5;
            const sb = try b.blob.sketch(gpa, null);
            sb[3] = 7;
            sb[4] = 2;
            try mergeOwnedAccumulator(&a, &b, gpa);
            try testing.expectEqual(@as(u8, 7), a.blob.bytes[3]);
            try testing.expectEqual(@as(u8, 2), a.blob.bytes[4]);
            try testing.expectEqual(@as(usize, 0), b.blob.bytes.len);
            // Into an empty one the bytes move.
            try mergeOwnedAccumulator(&c, &a, gpa);
            try testing.expectEqual(@as(u8, 7), c.blob.bytes[3]);
            try testing.expectEqual(@as(usize, 0), a.blob.bytes.len);
            // An empty source changes nothing.
            try mergeOwnedAccumulator(&c, &b, gpa);
            try testing.expectEqual(@as(u8, 7), c.blob.bytes[3]);
        }
    }.run, .{});
}

test "mergeOwnedAccumulator: quantile lists join whichever is larger, in any order, and leak nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn list(gpa: std.mem.Allocator, acc: *expr_agg.Accumulator, vals: []const f64) !void {
            for (vals) |v| try acc.blob.appendF64(gpa, null, v);
        }
        fn run(gpa: std.mem.Allocator) !void {
            var a: expr_agg.Accumulator = .{ .blob = .{ .kind = .quantile } };
            defer a.blob.deinit(gpa);
            var b: expr_agg.Accumulator = .{ .blob = .{ .kind = .quantile } };
            defer b.blob.deinit(gpa);
            var c: expr_agg.Accumulator = .{ .blob = .{ .kind = .quantile } };
            defer c.blob.deinit(gpa);
            try list(gpa, &a, &.{ 1, 2 });
            try list(gpa, &b, &.{ 3, 4, 5, 6 });
            // A failed merge leaves both sides as they were, still owned by their holders.
            mergeOwnedAccumulator(&a, &b, gpa) catch |err| {
                try testing.expectEqual(error.OutOfMemory, err);
                try testing.expectEqual(@as(usize, 16), a.blob.len);
                try testing.expectEqual(@as(usize, 32), b.blob.len);
                return err;
            };
            try testing.expectEqual(@as(usize, 48), a.blob.len);
            try testing.expectEqual(@as(usize, 0), b.blob.len);
            var sum: f64 = 0;
            for (a.blob.f64s()) |v| sum += v;
            try testing.expectEqual(@as(f64, 21), sum);
            try mergeOwnedAccumulator(&c, &a, gpa);
            try testing.expectEqual(@as(usize, 48), c.blob.len);
            try testing.expectEqual(@as(usize, 0), a.blob.len);
        }
    }.run, .{});
}
