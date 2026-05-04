//! SelectionVector — u64 bitmask, one bit per row.
//!
//! 1 = row is active (still passes all predicates seen so far).
//! 0 = row was filtered out by some predicate.
//!
//! Init: all-ones (every row active).
//! AND a predicate: clear bits for rows where the predicate is false.
//! OR composition: temp-copy, evaluate left, save, restore, evaluate
//! right, then bitwise-OR the saved-left into the right.
//!
//! Why bitmask over a row-index list:
//!   - O(rows/64) AND/OR via word-level bitops; SIMD-friendly later.
//!   - popcount per word → match count is constant per word.
//!   - Compact: 8 KB per million rows.
//!   - Branchless inner loop: compute `pass`, then either clear the
//!     bit or don't — no skip-list traversal.

const std = @import("std");

pub const SelectionVector = struct {
    allocator: std.mem.Allocator,
    len: usize,
    mask: []u64,

    pub fn init(allocator: std.mem.Allocator, len: usize) !SelectionVector {
        const num_words = (len + 63) / 64;
        const mask = try allocator.alloc(u64, num_words);
        // Initialize as all active.
        @memset(mask, ~@as(u64, 0));
        // Clear extra bits in the final word so popcount is correct.
        const extra = num_words * 64 - len;
        if (extra > 0) {
            mask[num_words - 1] &= ~@as(u64, 0) >> @intCast(extra);
        }
        return .{ .allocator = allocator, .len = len, .mask = mask };
    }

    pub fn deinit(self: *SelectionVector) void {
        self.allocator.free(self.mask);
        self.* = undefined;
    }

    pub inline fn set(self: *SelectionVector, index: usize, active: bool) void {
        const word = index / 64;
        const bit: u6 = @intCast(index % 64);
        if (active) {
            self.mask[word] |= (@as(u64, 1) << bit);
        } else {
            self.mask[word] &= ~(@as(u64, 1) << bit);
        }
    }

    pub inline fn isActive(self: SelectionVector, index: usize) bool {
        const word = index / 64;
        const bit: u6 = @intCast(index % 64);
        return ((self.mask[word] >> bit) & 1) == 1;
    }

    pub fn count(self: SelectionVector) usize {
        var n: usize = 0;
        for (self.mask) |w| n += @popCount(w);
        return n;
    }

    /// Replace this bitmask with `mask AND other`.
    pub fn intersect(self: *SelectionVector, other: *const SelectionVector) void {
        std.debug.assert(self.len == other.len);
        for (0..self.mask.len) |i| self.mask[i] &= other.mask[i];
    }

    /// Replace this bitmask with `mask OR other`.
    pub fn unionWith(self: *SelectionVector, other: *const SelectionVector) void {
        std.debug.assert(self.len == other.len);
        for (0..self.mask.len) |i| self.mask[i] |= other.mask[i];
    }

    /// Snapshot the current mask into a fresh copy. Used by OR
    /// evaluation to save / restore state across two child filters.
    pub fn cloneAlloc(self: SelectionVector, allocator: std.mem.Allocator) !SelectionVector {
        const words = try allocator.dupe(u64, self.mask);
        return .{ .allocator = allocator, .len = self.len, .mask = words };
    }

    /// Restore from a snapshot (in-place).
    pub fn copyFrom(self: *SelectionVector, other: *const SelectionVector) void {
        std.debug.assert(self.len == other.len);
        @memcpy(self.mask, other.mask);
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "SelectionVector init is all-ones" {
    var sv = try SelectionVector.init(testing.allocator, 100);
    defer sv.deinit();
    try testing.expectEqual(@as(usize, 100), sv.count());
    var i: usize = 0;
    while (i < 100) : (i += 1) try testing.expect(sv.isActive(i));
}

test "SelectionVector init handles non-multiple-of-64 length" {
    // 100 isn't a multiple of 64; verify no spurious bits past index 99.
    var sv = try SelectionVector.init(testing.allocator, 100);
    defer sv.deinit();
    try testing.expectEqual(@as(usize, 100), sv.count());
}

test "SelectionVector set + isActive" {
    var sv = try SelectionVector.init(testing.allocator, 64);
    defer sv.deinit();
    sv.set(5, false);
    sv.set(63, false);
    try testing.expectEqual(@as(usize, 62), sv.count());
    try testing.expect(!sv.isActive(5));
    try testing.expect(!sv.isActive(63));
    try testing.expect(sv.isActive(0));
    try testing.expect(sv.isActive(6));
}

test "SelectionVector intersect" {
    var a = try SelectionVector.init(testing.allocator, 8);
    defer a.deinit();
    var b = try SelectionVector.init(testing.allocator, 8);
    defer b.deinit();

    a.set(0, false);
    a.set(2, false); // a bits: 0 1 0 1 1 1 1 1  → bits 1,3,4,5,6,7 set
    b.set(2, false);
    b.set(7, false); // b bits: 1 1 0 1 1 1 1 0  → bits 0,1,3,4,5,6 set
    a.intersect(&b); // a&b:    0 1 0 1 1 1 1 0  → bits 1,3,4,5,6 set (5 active)

    try testing.expectEqual(@as(usize, 5), a.count());
    try testing.expect(!a.isActive(0));
    try testing.expect(a.isActive(1));
    try testing.expect(!a.isActive(2));
    try testing.expect(a.isActive(3));
    try testing.expect(!a.isActive(7));
}

test "SelectionVector unionWith" {
    var a = try SelectionVector.init(testing.allocator, 8);
    defer a.deinit();
    @memset(a.mask, 0); // start all-zero
    a.set(0, true);
    a.set(2, true);
    var b = try SelectionVector.init(testing.allocator, 8);
    defer b.deinit();
    @memset(b.mask, 0);
    b.set(1, true);
    b.set(2, true);

    a.unionWith(&b); // {0, 2} ∪ {1, 2} = {0, 1, 2}
    try testing.expectEqual(@as(usize, 3), a.count());
    try testing.expect(a.isActive(0));
    try testing.expect(a.isActive(1));
    try testing.expect(a.isActive(2));
    try testing.expect(!a.isActive(3));
}
