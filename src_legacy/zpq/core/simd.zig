const std = @import("std");

/// Expand values into a buffer based on a bitmask (definition levels == max_def_level).
/// values: the present values (compact)
/// mask: bitmask where 1 means value is present, 0 means null
/// out: destination buffer
/// returns: number of values consumed from 'values'
pub fn expandNulls(comptime T: type, values: []const T, mask: u64, out: []?T) usize {
    var val_idx: usize = 0;
    const limit = @min(64, out.len);
    for (0..limit) |i| {
        if ((mask >> @intCast(i)) & 1 == 1) {
            if (val_idx < values.len) {
                out[i] = values[val_idx];
                val_idx += 1;
            } else {
                out[i] = null;
            }
        } else {
            out[i] = null;
        }
    }
    return val_idx;
}

/// SIMD-accelerated null expansion for types that fit in a register (u64, f64, etc.)
/// This version uses a switch to make the shuffle mask comptime-known.
pub fn expandNullsBatch8(comptime T: type, values: []const T, mask: u8, out: []?T) usize {
    std.debug.assert(out.len >= 8);
    
    // We must have at least 8 values available for a safe vector load.
    // Also, only types that fit in a register are supported by @Vector.
    if (values.len < 8 or T == []const u8 or T == [12]u8) {
        return expandNullsBatch8Scalar(T, values, mask, out);
    }

    const v_values: @Vector(8, T) = values[0..8].*;

    const v_shuffled: @Vector(8, T) = switch (mask) {
        inline else => |m| @shuffle(T, v_values, undefined, expansion_shuffle_table[m]),
    };

    // Use inline loop for storing results into the nullable buffer
    inline for (0..8) |i| {
        out[i] = if ((mask >> i) & 1 == 1) v_shuffled[i] else null;
    }

    return @popCount(mask);
}

/// Scalar fallback for small buffers
fn expandNullsBatch8Scalar(comptime T: type, values: []const T, mask: u8, out: []?T) usize {
    var val_idx: usize = 0;
    inline for (0..8) |i| {
        const present = (mask >> i) & 1;
        if (present == 1) {
            out[i] = values[val_idx];
            val_idx += 1;
        } else {
            out[i] = null;
        }
    }
    return val_idx;
}

/// Convert a vector of 8 definition levels to a u8 bitmask where 1 means present.
pub fn defLevelsToMask8(def_levels: @Vector(8, u64), max_def_level: u16) u8 {
    const v_max: @Vector(8, u64) = @splat(max_def_level);
    const v_bool = def_levels == v_max;
    // In Zig, @bitCast of a bool vector to an integer of the same bit-size 
    // produces a bitmask.
    return @bitCast(v_bool);
}

/// Precomputed shuffle table for expansion.
/// Each entry maps a bitmask to the indices used to scatter compact values into expanded positions.
const expansion_shuffle_table = generateExpansionTable();

/// Selection Vector: bitmask of rows that passed a predicate.
pub const SelectionVector = struct {
    mask: [128]u8, // 1024 bits
    set_count: usize,

    pub fn init() SelectionVector {
        return SelectionVector{
            .mask = [_]u8{0} ** 128,
            .set_count = 0,
        };
    }

    pub fn setBitIndices(self: *SelectionVector, idx: usize) void {
        const byte_idx = idx / 8;
        const bit_idx = @as(u3, @intCast(idx % 8));
        if ((self.mask[byte_idx] >> bit_idx) & 1 == 0) {
            self.mask[byte_idx] |= (@as(u8, 1) << bit_idx);
            self.set_count += 1;
        }
    }

    pub fn count(self: *const SelectionVector) usize {
        return self.set_count;
    }

    pub fn setAll(self: *SelectionVector, limit: usize) void {
        const full_bytes = limit / 8;
        @memset(self.mask[0..full_bytes], 0xFF);
        self.set_count = full_bytes * 8;
        const rem = limit % 8;
        if (rem > 0) {
            self.mask[full_bytes] = (@as(u8, 1) << @intCast(rem)) - 1;
            self.set_count += rem;
        }
    }

    pub fn isSet(self: *const SelectionVector, idx: usize) bool {
        return (self.mask[idx / 8] >> @intCast(idx % 8)) & 1 == 1;
    }

    pub fn anySet(self: *const SelectionVector) bool {
        for (self.mask) |m| if (m != 0) return true;
        return false;
    }

    pub fn allSet(self: *const SelectionVector, limit: usize) bool {
        const full_bytes = limit / 8;
        for (self.mask[0..full_bytes]) |m| if (m != 0xFF) return false;
        const rem = limit % 8;
        if (rem > 0) {
            const m = self.mask[full_bytes];
            const mask: u8 = (@as(u16, 1) << @intCast(rem)) - 1;
            if ((m & mask) != mask) return false;
        }
        return true;
    }
};

/// SIMD comparison: Equal
pub fn eqBatch8(comptime T: type, values: @Vector(8, T), target: T) u8 {
    const v_target: @Vector(8, T) = @splat(target);
    return @bitCast(values == v_target);
}

/// SIMD comparison: Not Equal
pub fn neBatch8(comptime T: type, values: @Vector(8, T), target: T) u8 {
    const v_target: @Vector(8, T) = @splat(target);
    return @bitCast(values != v_target);
}

/// SIMD comparison: Less Than
pub fn ltBatch8(comptime T: type, values: @Vector(8, T), target: T) u8 {
    const v_target: @Vector(8, T) = @splat(target);
    return @bitCast(values < v_target);
}

/// SIMD comparison: Greater Than
pub fn gtBatch8(comptime T: type, values: @Vector(8, T), target: T) u8 {
    const v_target: @Vector(8, T) = @splat(target);
    return @bitCast(values > v_target);
}

fn generateExpansionTable() [256]@Vector(8, i32) {
    @setEvalBranchQuota(4000);
    var table: [256]@Vector(8, i32) = undefined;
    for (0..256) |mask| {
        var current: i32 = 0;
        var indices: [8]i32 = undefined;
        for (0..8) |i| {
            if ((mask >> @intCast(i)) & 1 == 1) {
                indices[i] = current;
                current += 1;
            } else {
                indices[i] = 0; // Don't care, will be masked to null
            }
        }
        table[mask] = indices;
    }
    return table;
}

/// Generic version for any type T
pub fn expandNullsGeneric(comptime T: type, values: []const T, mask: u64, out: []?T) usize {
    var val_idx: usize = 0;
    const limit = @min(64, out.len);
    for (0..limit) |i| {
        const present = (mask >> @intCast(i)) & 1;
        if (present == 1) {
            out[i] = values[val_idx];
            val_idx += 1;
        } else {
            out[i] = null;
        }
    }
    return val_idx;
}

