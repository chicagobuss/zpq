//! SelectionVector — u64 bitmask, one bit per row.
//!
//! 1 = row is active (still passes all predicates seen so far).
//! 0 = row was filtered out by some predicate.
//!
//! Init: all-ones (every row active).
//! AND a predicate: clear bits for rows where the predicate is false.
//! OR composition: snapshot the entry set, evaluate each operand from
//! it, and bitwise-OR their results together.
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
    /// False when `mask` belongs to a retained scratch: `deinit` must not free it.
    owned: bool = true,

    pub fn init(allocator: std.mem.Allocator, len: usize) !SelectionVector {
        const num_words = (len + 63) / 64;
        const mask = try allocator.alloc(u64, num_words);
        paintAll(mask, len);
        return .{ .allocator = allocator, .len = len, .mask = mask };
    }

    /// All rows active, in a mask the caller keeps. `mask` is `(len + 63) / 64` words.
    pub fn borrowed(mask: []u64, len: usize) SelectionVector {
        std.debug.assert(mask.len == (len + 63) / 64);
        paintAll(mask, len);
        return .{ .allocator = undefined, .len = len, .mask = mask, .owned = false };
    }

    fn paintAll(mask: []u64, len: usize) void {
        @memset(mask, ~@as(u64, 0));
        const extra = mask.len * 64 - len;
        if (extra > 0) mask[mask.len - 1] &= ~@as(u64, 0) >> @intCast(extra);
    }

    /// Borrow whole mask words. A partial final word is valid only at the original selection's end.
    /// Explicit checks also run in ReleaseFast: shifting a window silently changes which rows are selected.
    pub fn alignedWindow(self: SelectionVector, lo: usize, n: usize) error{UnalignedSelectionWindow}!SelectionVector {
        if (lo % 64 != 0 or lo > self.len or n > self.len - lo or
            (n % 64 != 0 and n != self.len - lo) or self.mask.len < (self.len + 63) / 64)
            return error.UnalignedSelectionWindow;
        return .{
            .allocator = self.allocator,
            .len = n,
            .mask = self.mask[lo / 64 ..][0 .. (n + 63) / 64],
            .owned = false,
        };
    }

    /// Visits active row indices, clipping padding and borrowed storage to the logical row count.
    pub fn iterator(self: SelectionVector) Iterator {
        return .{ .words = self.mask[0..@min(self.mask.len, self.len / 64 + @intFromBool(self.len % 64 != 0))], .len = self.len };
    }

    pub const Iterator = struct {
        words: []const u64,
        len: usize,
        word: usize = 0,
        base: usize = 0,
        bits: std.bit_set.IntegerBitSet(64) = .empty,

        pub fn next(self: *Iterator) ?usize {
            while (true) {
                if (self.bits.toggleFirstSet()) |bit| return self.base + bit;
                if (self.word == self.words.len) return null;
                self.base = self.word * 64;
                self.bits.mask = self.words[self.word] & bitsAt(0, @min(64, self.len - self.base));
                self.word += 1;
            }
        }
    };

    pub fn deinit(self: *SelectionVector) void {
        if (self.owned) self.allocator.free(self.mask);
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

    pub fn any(self: SelectionVector) bool {
        for (self.mask) |w| if (w != 0) return true;
        return false;
    }

    /// Deselect rows `lo..hi`, a word at a time.
    pub fn clearRange(self: *SelectionVector, lo: usize, hi: usize) void {
        var at = lo;
        while (at < hi) {
            const word = at / 64;
            const n = @min(hi - at, 64 - at % 64);
            self.mask[word] &= ~bitsAt(at % 64, n);
            at += n;
        }
    }

    /// Whether any row of `lo..hi` is selected, a word at a time.
    pub fn anyInRange(self: SelectionVector, lo: usize, hi: usize) bool {
        var at = lo;
        while (at < hi) {
            const word = at / 64;
            const n = @min(hi - at, 64 - at % 64);
            if (self.mask[word] & bitsAt(at % 64, n) != 0) return true;
            at += n;
        }
        return false;
    }

    /// `n` (1..64) set bits from bit `shift` up.
    inline fn bitsAt(shift: usize, n: usize) u64 {
        const ones: u64 = if (n == 64) ~@as(u64, 0) else (@as(u64, 1) << @intCast(n)) - 1;
        return ones << @intCast(shift);
    }

    /// Deselect every row.
    pub fn clearAll(self: *SelectionVector) void {
        @memset(self.mask, 0);
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

test "borrowed selection windows reject shifted or interior partial words" {
    var s = try SelectionVector.init(std.testing.allocator, 129);
    defer s.deinit();
    s.set(64, false);
    try std.testing.expect(!(try s.alignedWindow(64, 64)).isActive(0));
    try std.testing.expectEqual(@as(usize, 1), (try s.alignedWindow(128, 1)).count());
    try std.testing.expectError(error.UnalignedSelectionWindow, s.alignedWindow(1, 64));
    try std.testing.expectError(error.UnalignedSelectionWindow, s.alignedWindow(64, 1));
    try std.testing.expectError(error.UnalignedSelectionWindow, s.alignedWindow(128, 2));
}

test "SelectionVector range clears and tests match the per-row reference" {
    var prng = std.Random.DefaultPrng.init(7);
    const r = prng.random();
    for (0..200) |_| {
        const len = r.intRangeAtMost(usize, 1, 300);
        var fast = try SelectionVector.init(testing.allocator, len);
        defer fast.deinit();
        var slow = try SelectionVector.init(testing.allocator, len);
        defer slow.deinit();
        for (0..len) |i| if (r.boolean()) {
            fast.set(i, false);
            slow.set(i, false);
        };
        for (0..8) |_| {
            const lo = r.uintLessThan(usize, len + 1);
            const hi = r.intRangeAtMost(usize, lo, len);
            var want = false;
            for (lo..hi) |i| want = want or slow.isActive(i);
            try testing.expectEqual(want, fast.anyInRange(lo, hi));
            if (r.boolean()) {
                fast.clearRange(lo, hi);
                for (lo..hi) |i| slow.set(i, false);
                try testing.expectEqualSlices(u64, slow.mask, fast.mask);
            }
        }
    }
}

test "SelectionVector iterator clips dirty padding and extra words" {
    var mask = [_]u64{ 0, 1 | (@as(u64, 1) << 63), ~@as(u64, 0) };
    const sel: SelectionVector = .{ .allocator = testing.allocator, .len = 65, .mask = &mask };
    var it = sel.iterator();
    try testing.expectEqual(@as(?usize, 64), it.next());
    try testing.expectEqual(@as(?usize, null), it.next());
    var empty = sel;
    empty.len = 0;
    it = empty.iterator();
    try testing.expectEqual(@as(?usize, null), it.next());
}

test "a borrowed selection does not free its words" {
    const mask = try testing.allocator.alloc(u64, 2);
    var sel = SelectionVector.borrowed(mask, 100);
    try testing.expect(sel.isActive(0));
    try testing.expect(sel.isActive(99));
    sel.set(3, false);
    try testing.expect(!sel.isActive(3));
    sel.deinit();
    testing.allocator.free(mask);
}
