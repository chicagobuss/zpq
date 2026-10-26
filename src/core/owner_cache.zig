//! Borrowed, prepaid exact partials. Evictions use the owner's ordinary record emitter.
const std = @import("std");
const hw = @import("hw.zig");

pub fn capacity(entry_bytes: usize) usize {
    const l2 = hw.l2BytesPerWorkerOr(640 << 10);
    const known_l1 = hw.l1dBytesPerWorker();
    const l1 = if (known_l1 == 0) l2 / 16 else known_l1;
    const bytes = @min(l2 / 16, @max(l1 / 2, 16 << 10));
    const line = @max(hw.cacheLineBytes(), entry_bytes);
    if (bytes < line) return 0;
    return @as(usize, 1) << @intCast(std.math.log2_int(usize, (bytes / line * line) / entry_bytes));
}

pub fn trialRows(entries: usize, entry_bytes: usize) usize {
    return entries * @max(2, hw.cacheLineBytes() / entry_bytes *| 2);
}

/// Policy owns complete identity, emptiness and atomic checked coalescing; a hash is never an identity.
pub fn Local(comptime Policy: type) type {
    const Entry = Policy.Entry;
    return struct {
        entries: []Entry = &.{},
        last: ?usize = null,
        const Self = @This();

        /// Returns whether an existing partial absorbed the contribution, including a last-pair hit without hashing.
        pub inline fn push(c: *Self, r: Entry, ctx: anytype, comptime emit: anytype) !bool {
            std.debug.assert(c.entries.len != 0 and std.math.isPowerOfTwo(c.entries.len));
            try Policy.validate(r);
            if (c.last) |i| {
                const entry = &c.entries[i];
                if (Policy.active(entry.*) and Policy.same(entry.*, r))
                    return c.replaceOrMerge(entry, r, ctx, emit);
            }
            const i = @as(usize, @truncate(Policy.hash(r))) & (c.entries.len - 1);
            const entry = &c.entries[i];
            const hit = if (Policy.active(entry.*) and Policy.same(entry.*, r))
                try c.replaceOrMerge(entry, r, ctx, emit)
            else blk: {
                try flushEntry(entry, ctx, emit);
                entry.* = r;
                break :blk false;
            };
            c.last = i;
            return hit;
        }

        inline fn replaceOrMerge(c: *Self, entry: *Entry, r: Entry, ctx: anytype, comptime emit: anytype) !bool {
            _ = c;
            if (try Policy.coalesce(entry, r)) return true;
            try flushEntry(entry, ctx, emit);
            entry.* = r;
            return false;
        }

        pub fn flush(c: *Self, ctx: anytype, comptime emit: anytype) !void {
            for (c.entries) |*entry| try flushEntry(entry, ctx, emit);
            c.last = null;
        }

        pub inline fn flushEntry(entry: *Entry, ctx: anytype, comptime emit: anytype) !void {
            if (!Policy.active(entry.*)) return;
            try emit(ctx, entry.*);
            Policy.clear(entry);
        }
    };
}

test "owner cache last-pair hits skip hashing and failed evictions retain the pending partial" {
    const Layout = @import("owner_lanes.zig").Layout(u128, &.{ .min, .sum }, u32);
    const P = struct {
        pub const Entry = Layout.Record;
        fn validate(r: Entry) !void {
            try Layout.validate(r);
        }
        var hashes: usize = 0;
        fn active(r: Entry) bool {
            return r.weight != 0;
        }
        fn same(a: Entry, b: Entry) bool {
            return a.key == b.key;
        }
        fn hash(r: Entry) u64 {
            hashes += 1;
            return @truncate(r.key);
        }
        fn clear(r: *Entry) void {
            r.weight = 0;
        }
        fn coalesce(a: *Entry, b: Entry) !bool {
            return Layout.coalesce(a, b);
        }
    };
    const Sink = struct {
        groups: [2]Layout.Group = undefined,
        len: usize = 0,
        fail: bool = false,
        fn emit(s: *@This(), r: P.Entry) !void {
            if (s.fail) return error.ExceededMemoryBudget;
            if (s.len == 0 or s.groups[s.len - 1].key != r.key) {
                s.groups[s.len] = try Layout.Group.init(r);
                s.len += 1;
            } else try s.groups[s.len - 1].add(r);
        }
    };
    var entries: [1]P.Entry = undefined;
    entries[0].weight = 0;
    var cache: Local(P) = .{ .entries = &entries };
    var sink: Sink = .{};
    P.hashes = 0;
    const first: P.Entry = .{ .key = 7, .ordinal = 90, .weight = 1, .values = .{ 7, std.math.minInt(i64) } };
    try std.testing.expect(!try cache.push(first, &sink, Sink.emit));
    const next: P.Entry = .{ .key = 7, .ordinal = 4, .weight = 1, .values = .{ -7, std.math.minInt(i64) } };
    sink.fail = true;
    try std.testing.expectError(error.ExceededMemoryBudget, cache.push(next, &sink, Sink.emit));
    try std.testing.expectEqualDeep(first, entries[0]);
    sink.fail = false;
    try std.testing.expect(!try cache.push(next, &sink, Sink.emit));
    var third = next;
    third.ordinal = 2;
    third.values[1] = 0;
    try std.testing.expect(try cache.push(third, &sink, Sink.emit));
    try std.testing.expectEqual(@as(usize, 1), P.hashes);
    const collision: P.Entry = .{ .key = 8, .ordinal = 3, .weight = 1, .values = .{ 1, 0 } };
    sink.fail = true;
    try std.testing.expectError(error.ExceededMemoryBudget, cache.push(collision, &sink, Sink.emit));
    try std.testing.expectEqual(@as(u128, 7), entries[0].key);
    try std.testing.expectEqual(@as(u32, 2), entries[0].ordinal);
    sink.fail = false;
    try std.testing.expect(!try cache.push(collision, &sink, Sink.emit));
    try cache.flush(&sink, Sink.emit);
    try std.testing.expectEqual(@as(usize, 2), sink.len);
    try std.testing.expectEqual(-(@as(i128, 1) << 64), sink.groups[0].integer(1).?);
    try std.testing.expectEqual(@as(?i128, -7), sink.groups[0].integer(0));
    try std.testing.expectEqual(@as(u32, 3), sink.groups[0].weight);
    try std.testing.expectEqual(@as(u32, 2), sink.groups[0].first);
    try std.testing.expectEqual(@as(u32, 0), entries[0].weight);
    try std.testing.expectEqual(@as(?usize, null), cache.last);
}
