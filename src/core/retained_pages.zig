//! Fixed payload storage funded by the caller before decoding. Reader scratch never owns these bytes.
const std = @import("std");
const huge_pages = @import("huge_pages.zig");

pub const Chunk = struct {
    buffer: []u8,
    used: usize = 0,

    pub fn take(c: *Chunk, n: usize) error{PageLargerThanChunk}![]u8 {
        if (n > c.buffer.len - c.used) return error.PageLargerThanChunk;
        const out = c.buffer[c.used..][0..n];
        c.used += n;
        return out;
    }

    pub fn contains(c: *const Chunk, value: []const u8) bool {
        if (value.len == 0) return true;
        const base = @intFromPtr(c.buffer.ptr);
        const ptr = @intFromPtr(value.ptr);
        return ptr >= base and ptr - base <= c.used and value.len <= c.used - (ptr - base);
    }
};

pub const Store = struct {
    bytes: []u8 = &.{},
    chunks: []Chunk = &.{},

    /// Allocation bytes only. The caller includes this in its complete upfront ledger reservation.
    pub fn reservation(capacities: []const usize) usize {
        var n = capacities.len *| @sizeOf(Chunk);
        for (capacities) |cap| n +|= cap;
        return n;
    }

    pub fn init(a: std.mem.Allocator, capacities: []const usize) !Store {
        var s: Store = .{};
        errdefer s.deinit(a);
        var total: usize = 0;
        for (capacities) |cap| total = std.math.add(usize, total, cap) catch return error.OutOfMemory;
        // The complete capacity is prepaid. Huge-page advice changes fault/teardown cost, never admission.
        var advisor: huge_pages.HugePageAdvisor = .{ .child = a };
        s.bytes = try advisor.allocator().alloc(u8, total);
        s.chunks = try a.alloc(Chunk, capacities.len);
        var at: usize = 0;
        for (s.chunks, capacities) |*c, cap| {
            c.* = .{ .buffer = s.bytes[at..][0..cap] };
            at += cap;
        }
        return s;
    }

    pub fn deinit(s: *Store, a: std.mem.Allocator) void {
        a.free(s.bytes);
        a.free(s.chunks);
        s.* = .{};
    }
};

fn allocTrial(a: std.mem.Allocator) !void {
    var store = try Store.init(a, &.{ 32, 64 });
    defer store.deinit(a);
    const one = try store.chunks[0].take(32);
    @memset(one, 0x53);
    const two = try store.chunks[1].take(16);
    @memset(two, 0x71);
    try std.testing.expect(store.chunks[0].contains(one));
    try std.testing.expect(!store.chunks[1].contains(one));
    try std.testing.expect(!store.chunks[0].contains(two));
    try std.testing.expectError(error.PageLargerThanChunk, store.chunks[0].take(1));
    try std.testing.expectEqual(@as(usize, 32), store.chunks[0].used);
    try std.testing.expectEqual(@as(u8, 0x53), one[31]);
    try std.testing.expectEqual(@as(u8, 0x71), two[15]);
}
test "retained payload stores preserve owners and release partial allocations" {
    try allocTrial(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, allocTrial, .{});
}
