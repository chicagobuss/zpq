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

    /// Bulk append a range of consecutive indices [start, start+count)
    pub fn appendRange(self: *SelectionVector, start: usize, len: u32) !void {
        if (len == 0) return;
        try self.indices.ensureUnusedCapacity(self.allocator, len);
        for (0..len) |i| {
            self.indices.appendAssumeCapacity(start + i);
        }
    }

    /// Bulk append from a bitmask. Each set bit at position i adds (base_idx + i) to selection.
    /// Uses popcount and ctz for efficient bit scanning.
    pub fn appendFromMask(self: *SelectionVector, mask: u8, base_idx: usize) !void {
        if (mask == 0) return;

        // Pre-calculate how many bits are set
        const num_bits = @popCount(mask);
        try self.indices.ensureUnusedCapacity(self.allocator, num_bits);

        // Extract set bit positions using ctz (count trailing zeros)
        var remaining = mask;
        while (remaining != 0) {
            const bit_pos = @ctz(remaining);
            self.indices.appendAssumeCapacity(base_idx + bit_pos);
            remaining &= remaining - 1; // Clear lowest set bit
        }
    }

    /// Bulk append from a 64-bit bitmask
    pub fn appendFromMask64(self: *SelectionVector, mask: u64, base_idx: usize) !void {
        if (mask == 0) return;

        const num_bits = @popCount(mask);
        try self.indices.ensureUnusedCapacity(self.allocator, num_bits);

        var remaining = mask;
        while (remaining != 0) {
            const bit_pos = @ctz(remaining);
            self.indices.appendAssumeCapacity(base_idx + bit_pos);
            remaining &= remaining - 1;
        }
    }

    /// Convert selection vector to a bitmap for O(1) lookup.
    /// Returns a bit array where bit i is set if row i is selected.
    /// Caller must free the returned slice.
    pub fn toBitmap(self: *const SelectionVector, allocator: std.mem.Allocator, total_rows: usize) ![]u8 {
        const num_bytes = (total_rows + 7) / 8;
        const bitmap = try allocator.alloc(u8, num_bytes);
        @memset(bitmap, 0);

        for (self.indices.items) |idx| {
            if (idx < total_rows) {
                const byte_idx = idx / 8;
                const bit_idx: u3 = @intCast(idx % 8);
                bitmap[byte_idx] |= @as(u8, 1) << bit_idx;
            }
        }

        return bitmap;
    }

    /// Check if a row is selected using a pre-computed bitmap (O(1))
    pub fn isSelectedInBitmap(bitmap: []const u8, row: usize) bool {
        const byte_idx = row / 8;
        if (byte_idx >= bitmap.len) return false;
        const bit_idx: u3 = @intCast(row % 8);
        return (bitmap[byte_idx] & (@as(u8, 1) << bit_idx)) != 0;
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
