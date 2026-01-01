const std = @import("std");

/// A selection vector tracks which row indices passed a filter.
/// Used to avoid copying data - instead we track indices and selectively read.
pub const SelectionVector = struct {
    indices: std.ArrayListUnmanaged(usize),
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) SelectionVector {
        return .{
            .indices = .{},
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *SelectionVector) void {
        self.indices.deinit(self.allocator);
    }

    pub fn append(self: *SelectionVector, idx: usize) !void {
        try self.indices.append(self.allocator, idx);
    }

    pub fn count(self: SelectionVector) usize {
        return self.indices.items.len;
    }

    pub fn items(self: SelectionVector) []const usize {
        return self.indices.items;
    }

    /// Reserve capacity for expected number of matches
    pub fn ensureCapacity(self: *SelectionVector, capacity: usize) !void {
        try self.indices.ensureTotalCapacity(self.allocator, capacity);
    }

    /// Clear without deallocating
    pub fn clear(self: *SelectionVector) void {
        self.indices.clearRetainingCapacity();
    }

    /// Check if a row index is in the selection (O(n) - use sparingly)
    pub fn contains(self: SelectionVector, idx: usize) bool {
        for (self.indices.items) |i| {
            if (i == idx) return true;
        }
        return false;
    }
};

test "SelectionVector basic operations" {
    const allocator = std.testing.allocator;

    var sel = SelectionVector.init(allocator);
    defer sel.deinit();

    try sel.append(5);
    try sel.append(10);
    try sel.append(15);

    try std.testing.expectEqual(@as(usize, 3), sel.count());
    try std.testing.expectEqualSlices(usize, &[_]usize{ 5, 10, 15 }, sel.items());

    try std.testing.expect(sel.contains(10));
    try std.testing.expect(!sel.contains(7));
}

test "SelectionVector clear and reuse" {
    const allocator = std.testing.allocator;

    var sel = SelectionVector.init(allocator);
    defer sel.deinit();

    try sel.append(1);
    try sel.append(2);
    try std.testing.expectEqual(@as(usize, 2), sel.count());

    sel.clear();
    try std.testing.expectEqual(@as(usize, 0), sel.count());

    try sel.append(100);
    try std.testing.expectEqual(@as(usize, 1), sel.count());
    try std.testing.expectEqual(@as(usize, 100), sel.items()[0]);
}

test "SelectionVector with capacity" {
    const allocator = std.testing.allocator;

    var sel = SelectionVector.init(allocator);
    defer sel.deinit();

    try sel.ensureCapacity(1000);

    for (0..1000) |i| {
        try sel.append(i * 2); // Even numbers
    }

    try std.testing.expectEqual(@as(usize, 1000), sel.count());
    try std.testing.expectEqual(@as(usize, 0), sel.items()[0]);
    try std.testing.expectEqual(@as(usize, 1998), sel.items()[999]);
}
