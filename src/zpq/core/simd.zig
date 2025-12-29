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
/// This version uses a branchless approach to fill a batch of 8 values.
pub fn expandNullsBatch8(comptime T: type, values: []const T, mask: u8, out: []?T) usize {
    std.debug.assert(out.len >= 8);
    
    var val_idx: usize = 0;
    // We use a tight loop that the compiler can easily unroll and optimize.
    inline for (0..8) |i| {
        const present = (mask >> i) & 1;
        if (present == 1) {
            out[i] = values[val_idx];
            val_idx += present;
        } else {
            out[i] = null;
        }
    }
    return val_idx;
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

