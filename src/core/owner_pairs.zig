//! DISTINCT uses complete (group,value) identities and the ordinary owner weight as duplicate count.
const std = @import("std");
const hashBits = @import("owner_lanes.zig").hashBits;
pub const Partition = enum { group, value, pair };

pub fn Owner(comptime Bits: type, comptime Ordinal: type, comptime partition: Partition) type {
    const L = Layout(Bits, Ordinal);
    const Routing = struct {
        fn hash(r: L.Record) u64 {
            return switch (partition) {
                .group => hashBits(r.key.group),
                .value => L.valueHash(r.key.value, r.ordinal & L.null_arg != 0),
                .pair => L.hashRecord(r),
            };
        }
    };
    return @import("owner_packed.zig").Framework(L, Ordinal, Routing.hash);
}

pub fn Layout(comptime Bits: type, comptime Ordinal: type) type {
    comptime {
        std.debug.assert(Bits == u64 or Bits == u128);
        std.debug.assert(Ordinal == u32 or Ordinal == u64);
    }
    return struct {
        const Self = @This();
        const Key = extern struct { group: Bits align(8), value: u64 };
        const null_arg = @as(Ordinal, 1) << (@bitSizeOf(Ordinal) - 1);
        pub const Record = extern struct { key: Key align(8), ordinal: Ordinal, weight: Ordinal };

        /// The ordinal's spare bit is used only under the checked original-input bound, never as identity payload.
        pub fn admitsRows(rows: usize) bool {
            return rows < null_arg;
        }
        pub fn ordinal(r: Record) Ordinal {
            return r.ordinal & (null_arg - 1);
        }

        pub fn record(key: Bits, value: ?i64, first: Ordinal, weight: Ordinal) !Record {
            if (first >= null_arg or weight == 0) return error.BadPartial;
            return .{
                .key = .{ .group = key, .value = if (value) |v| @bitCast(v) else 0 },
                .ordinal = first | @as(Ordinal, if (value == null) null_arg else 0),
                .weight = weight,
            };
        }

        pub fn validate(r: Record) error{BadPartial}!void {
            if (r.weight == 0 or (r.ordinal & null_arg != 0 and r.key.value != 0)) return error.BadPartial;
        }

        fn valueHash(value: u64, is_null: bool) u64 {
            return if (is_null) 0x621985775bcbd930 else hashBits(value);
        }

        fn hash(key: Key, first: Ordinal) u64 {
            return hashBits(@as(u128, hashBits(key.group)) << 64 | valueHash(key.value, first & null_arg != 0));
        }

        pub fn hashRecord(r: Record) u64 {
            return hash(r.key, r.ordinal);
        }
        pub fn hashGroup(g: Group) u64 {
            return hash(g.key, g.first);
        }
        fn equal(a: Key, af: Ordinal, b: Key, bf: Ordinal) bool {
            return a.group == b.group and a.value == b.value and (af & null_arg) == (bf & null_arg);
        }
        pub fn same(r: Record, g: Group) bool {
            return equal(r.key, r.ordinal, g.key, g.first);
        }

        pub const Group = struct {
            key: Key align(8),
            first: Ordinal,
            weight: Ordinal,
            pub fn init(r: Record) error{BadPartial}!Group {
                try validate(r);
                return .{ .key = r.key, .first = r.ordinal, .weight = r.weight };
            }
            pub fn add(g: *Group, r: Record) error{BadPartial}!void {
                try validate(r);
                if (!same(r, g.*)) return error.BadPartial;
                const weight = std.math.add(Ordinal, g.weight, r.weight) catch return error.BadPartial;
                g.weight = weight;
                g.first = @min(g.first, r.ordinal);
            }
            pub fn ordinal(g: Group) Ordinal {
                return g.first & (null_arg - 1);
            }
            pub fn argumentNull(g: Group) bool {
                return g.first & null_arg != 0;
            }
        };

        pub const CachePolicy = struct {
            pub const Entry = Record;
            pub fn validate(r: Record) error{BadPartial}!void {
                return Self.validate(r);
            }
            pub fn active(r: Record) bool {
                return r.weight != 0;
            }
            pub fn same(a: Record, b: Record) bool {
                return equal(a.key, a.ordinal, b.key, b.ordinal);
            }
            pub fn hash(r: Record) u64 {
                return hashRecord(r);
            }
            pub fn clear(r: *Record) void {
                r.weight = 0;
            }
            pub fn coalesce(a: *Record, b: Record) error{BadPartial}!bool {
                try Self.validate(b);
                if (!equal(a.key, a.ordinal, b.key, b.ordinal)) return false;
                const weight = std.math.add(Ordinal, a.weight, b.weight) catch return false;
                a.weight = weight;
                a.ordinal = @min(a.ordinal, b.ordinal);
                return true;
            }
        };
    };
}

fn pairTrial(a: std.mem.Allocator, comptime partition: Partition, workers: usize) !void {
    const O = Owner(u128, u32, partition);
    const L = Layout(u128, u32);
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 256 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    {
        const s = (try O.State.init(a, &ledger, 4096, 4096, workers, 64)).?;
        defer s.deinit();
        var counts: [17]u32 = @splat(0);
        var firsts: [17]u32 = @splat(std.math.maxInt(u32));
        var present: [17][7]bool = @splat(@splat(false));
        for (0..4096) |i| {
            const g = i % 17;
            const v = (i / 17) % 7;
            const first: u32 = @intCast(4095 - i);
            const value: ?i64 = if (v == 0 or g == 0) null else if (v == 1) std.math.minInt(i64) else @intCast(v);
            try s.locals[(i / 101) % workers].append(try L.record(@as(u128, g) << 80, value, first, 1));
            counts[g] += 1;
            firsts[g] = @min(firsts[g], first);
            if (value != null) present[g][v] = true;
        }
        const Receiver = struct {
            counts: [17]u32 = @splat(0),
            distinct: [17]u32 = @splat(0),
            first: [17]u32 = @splat(std.math.maxInt(u32)),
            fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
                for (groups) |pair| {
                    const g: usize = @intCast(pair.key.group >> 80);
                    r.counts[g] += pair.weight;
                    r.distinct[g] += @intFromBool(!pair.argumentNull());
                    r.first[g] = @min(r.first[g], pair.ordinal());
                }
            }
        };
        const receivers = try a.alloc(Receiver, workers);
        defer a.free(receivers);
        @memset(receivers, .{});
        try s.finish(receivers, Receiver.receive);
        for (0..17) |g| {
            var count: u32 = 0;
            var distinct: u32 = 0;
            var first: u32 = std.math.maxInt(u32);
            for (receivers) |r| {
                count += r.counts[g];
                distinct += r.distinct[g];
                first = @min(first, r.first[g]);
            }
            var want: u32 = 0;
            for (present[g]) |p| want += @intFromBool(p);
            try std.testing.expectEqual(counts[g], count);
            try std.testing.expectEqual(want, distinct);
            try std.testing.expectEqual(firsts[g], first);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "owner pairs group value and pair routing preserve DISTINCT count duplicates NULL and original ties" {
    inline for (.{ Partition.group, Partition.value, Partition.pair }) |partition| {
        for ([_]usize{ 1, 3, 16 }) |workers| try pairTrial(std.testing.allocator, partition, workers);
        const Trial = struct {
            fn run(a: std.mem.Allocator) !void {
                try pairTrial(a, partition, 1);
            }
        };
        try std.testing.checkAllAllocationFailures(std.testing.allocator, Trial.run, .{});
    }
    try std.testing.expectEqual(@as(usize, 24), @sizeOf(Layout(u64, u32).Record));
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(Layout(u128, u32).Record));
}
