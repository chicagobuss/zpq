const std = @import("std");

/// SelectionVector tracks which rows are active/filtered in a Morsel.
/// Currently implements a simple bitmask (bitmap).
pub const SelectionVector = struct {
    const Self = @This();

    allocator: std.mem.Allocator,
    len: usize,
    /// Bitmap where 1 means row is active, 0 means filtered out.
    mask: []u64,

    pub fn init(allocator: std.mem.Allocator, len: usize) !Self {
        const num_words = (len + 63) / 64;
        const mask = try allocator.alloc(u64, num_words);
        // Initialize as all active
        @memset(mask, ~@as(u64, 0));
        return .{
            .allocator = allocator,
            .len = len,
            .mask = mask,
        };
    }

    pub fn deinit(self: *Self) void {
        self.allocator.free(self.mask);
    }

    pub inline fn set(self: *Self, index: usize, active: bool) void {
        const word_idx = index / 64;
        const bit_idx: u6 = @intCast(index % 64);
        if (active) {
            self.mask[word_idx] |= (@as(u64, 1) << bit_idx);
        } else {
            self.mask[word_idx] &= ~(@as(u64, 1) << bit_idx);
        }
    }

    pub inline fn isActive(self: SelectionVector, index: usize) bool {
        const word_idx = index / 64;
        const bit_idx: u6 = @intCast(index % 64);
        return (self.mask[word_idx] >> bit_idx) & 1 == 1;
    }

    pub fn count(self: SelectionVector) usize {
        var n: usize = 0;
        for (self.mask) |word| {
            n += @popCount(word);
        }
        // Mask off extra bits in the last word if len is not a multiple of 64
        const bits_in_last = self.len % 64;
        if (bits_in_last > 0) {
            const word = self.mask[self.mask.len - 1];
            const extra_mask = (@as(u64, 1) << @intCast(bits_in_last)) - 1;
            n -= @popCount(word & ~extra_mask);
        }
        return n;
    }

    /// Bitwise AND with another selection vector.
    pub fn intersect(self: *Self, other: SelectionVector) void {
        std.debug.assert(self.len == other.len);
        for (self.mask, 0..) |*word, i| {
            word.* &= other.mask[i];
        }
    }

    pub fn allSet(self: SelectionVector, limit: usize) bool {
        const full_words = limit / 64;
        for (self.mask[0..full_words]) |word| {
            if (word != ~@as(u64, 0)) return false;
        }
        const rem = limit % 64;
        if (rem > 0) {
            const word = self.mask[full_words];
            const extra_mask = (@as(u64, 1) << @intCast(rem)) - 1;
            if ((word & extra_mask) != extra_mask) return false;
        }
        return true;
    }

    pub fn allClear(self: SelectionVector, limit: usize) bool {
        const full_words = limit / 64;
        for (self.mask[0..full_words]) |word| {
            if (word != 0) return false;
        }
        const rem = limit % 64;
        if (rem > 0) {
            const word = self.mask[full_words];
            const extra_mask = (@as(u64, 1) << @intCast(rem)) - 1;
            if ((word & extra_mask) != 0) return false;
        }
        return true;
    }

    pub fn anySet(self: SelectionVector, limit: usize) bool {
        return !self.allClear(limit);
    }

    pub fn clearAll(self: *Self) void {
        @memset(self.mask, 0);
    }
};
