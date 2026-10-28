//! Reusable contiguous owner groups and open-addressed pointers, shared by full and top-H finishes.
const std = @import("std");

pub fn Table(comptime Layout: type) type {
    return struct {
        groups: []Layout.Group = &.{},
        slots: []u32 = &.{},
        used: usize = 0,
        limit: usize = 0,
        last: ?usize = null,
        const Self = @This();

        /// The caller prepays this bound, including rounding. reset/growth free old buffers before allocating.
        pub fn bytes(max_groups: usize) usize {
            if (max_groups == 0) return 0;
            const cap = std.math.ceilPowerOfTwo(usize, max_groups *| 2) catch return std.math.maxInt(usize);
            return max_groups *| @sizeOf(Layout.Group) +| cap *| @sizeOf(u32);
        }

        /// Each worker retains scratch for its largest claimed partition. Those maxima name distinct partitions,
        /// so the largest W exact histograms bound all retained tables, including pointer-table rounding.
        pub fn concurrentBytes(ctx: anytype, parts: usize, workers: usize, comptime rows: anytype) usize {
            std.debug.assert(workers > 0 and workers <= 64);
            var largest: [64]usize = @splat(0);
            for (0..parts) |p| {
                var n = rows(ctx, p);
                for (largest[0..workers]) |*v| if (n > v.*) std.mem.swap(usize, &n, v);
            }
            var charge: usize = 0;
            for (largest[0..workers]) |n| charge +|= bytes(n);
            return charge;
        }

        pub fn deinit(t: *Self, a: std.mem.Allocator) void {
            a.free(t.groups);
            a.free(t.slots);
            t.* = .{};
        }

        /// No per-row allocation: reserve group storage once; fault only discovered groups and pointer slots.
        pub fn reset(t: *Self, a: std.mem.Allocator, max_groups: usize, initial_groups: usize) !void {
            if (max_groups > std.math.maxInt(u32) - 1) return error.ExceededMemoryBudget;
            t.used = 0;
            t.limit = max_groups;
            t.last = null;
            if (t.groups.len < max_groups) {
                a.free(t.groups);
                t.groups = &.{};
                t.groups = try a.alloc(Layout.Group, max_groups);
            }
            if (max_groups == 0) return;
            const cap = try std.math.ceilPowerOfTwo(usize, @max(1, @min(max_groups, initial_groups)) * 2);
            try t.ensureSlots(a, cap);
        }

        fn ensureSlots(t: *Self, a: std.mem.Allocator, cap: usize) !void {
            if (t.slots.len < cap) {
                a.free(t.slots);
                t.slots = &.{};
                t.slots = try a.alloc(u32, cap);
            }
            @memset(t.slots, 0);
            for (t.groups[0..t.used], 0..) |g, i| {
                var pos: usize = @as(usize, @truncate(Layout.hashGroup(g))) & (t.slots.len - 1);
                while (t.slots[pos] != 0) pos = (pos + 1) & (t.slots.len - 1);
                t.slots[pos] = @intCast(i + 1);
            }
        }

        inline fn position(t: *const Self, r: Layout.Record) usize {
            var pos: usize = @as(usize, @truncate(Layout.hashRecord(r))) & (t.slots.len - 1);
            while (t.slots[pos] != 0) : (pos = (pos + 1) & (t.slots.len - 1)) {
                if (Layout.same(r, t.groups[t.slots[pos] - 1])) break;
            }
            return pos;
        }

        pub inline fn append(t: *Self, a: std.mem.Allocator, r: Layout.Record) !void {
            if (t.limit == 0) return error.ExceededMemoryBudget;
            if (t.last) |i| if (Layout.same(r, t.groups[i])) {
                try t.groups[i].add(r);
                return;
            };
            if (t.used >= t.slots.len / 2) {
                if (t.slots.len == 0) return error.ExceededMemoryBudget;
                // A full group bound can only contain existing keys. Do not probe a completely full pointer table.
                if (t.used < t.limit) try t.ensureSlots(a, t.slots.len * 2);
            }
            const pos = t.position(r);
            const id: usize = if (t.slots[pos] != 0) t.slots[pos] - 1 else {
                if (t.used == t.limit) return error.ExceededMemoryBudget;
                t.groups[t.used] = try Layout.Group.init(r);
                t.slots[pos] = @intCast(t.used + 1);
                t.used += 1;
                t.last = t.used - 1;
                return;
            };
            try t.groups[id].add(r);
            t.last = id;
        }

        pub fn items(t: *Self) []Layout.Group {
            return t.groups[0..t.used];
        }
    };
}

fn foldTrial(a: std.mem.Allocator) !void {
    const L = @import("owner_lanes.zig").Layout(u128, &.{ .sum, .present_count }, u64);
    var t: Table(L) = .{};
    defer t.deinit(a);
    try t.reset(a, 257, 1);
    for (0..257 * 3) |i| {
        const id = i % 257;
        // The high half alone separates these keys, including zero and all-one low halves.
        try t.append(a, .{
            .key = @as(u128, id) << 64 | std.math.maxInt(u64),
            .ordinal = @intCast(257 * 3 - i),
            .weight = 1,
            .values = .{ if (id == 0) 0 else -7, 1 },
            .argument_nulls = if (id == 0) 1 else 0,
        });
    }
    try std.testing.expectEqual(@as(usize, 257), t.items().len);
    for (t.items(), 0..) |g, i| {
        try std.testing.expectEqual(@as(u64, 3), g.weight);
        try std.testing.expectEqual(@as(u64, @intCast(257 - i)), g.first);
        try std.testing.expectEqual(@as(?i128, 3), g.integer(1));
        try std.testing.expectEqual(if (i == 0) @as(?i128, null) else -21, g.integer(0));
    }
    try t.reset(a, 1, 1);
    try t.append(a, .{ .key = 0, .ordinal = 1 << 40, .weight = 1, .values = .{ 0, 0 } });
    try std.testing.expectEqual(@as(?i128, 0), t.items()[0].integer(0));
    try std.testing.expectError(error.ExceededMemoryBudget, t.append(a, .{
        .key = 1,
        .ordinal = 0,
        .weight = 1,
        .values = .{ 0, 0 },
    }));
    try t.reset(a, 0, 0);
    try std.testing.expectError(error.ExceededMemoryBudget, t.append(a, .{
        .key = 0,
        .ordinal = 0,
        .weight = 1,
        .values = .{ 0, 0 },
    }));
}

test "owner fold reuses exact u128 identities and wide lanes through every allocation failure" {
    try foldTrial(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, foldTrial, .{});
}

test "owner fold concurrent quote follows exact largest partitions including retained rounded slots" {
    const L = @import("owner_lanes.zig").Layout(u64, &.{}, u32);
    const F = Table(L);
    const hist = [_]usize{ 1, 1001, 17, 2, 2000, 0 };
    const Rows = struct {
        fn get(h: []const usize, p: usize) usize {
            return h[p];
        }
    };
    try std.testing.expectEqual(F.bytes(2000) + F.bytes(1001), F.concurrentBytes(&hist, hist.len, 2, Rows.get));
    try std.testing.expectEqual(F.bytes(2000), F.concurrentBytes(&hist, hist.len, 1, Rows.get));
    var all: usize = 0;
    for (hist) |n| all += F.bytes(n);
    try std.testing.expectEqual(all, F.concurrentBytes(&hist, hist.len, 16, Rows.get));
}
