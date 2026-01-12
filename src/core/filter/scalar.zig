const std = @import("std");
const column_batch = @import("../column_batch.zig");
const selection = @import("../selection.zig");
const Operator = @import("operator.zig").Operator;

/// Evaluates a primitive filter on a batch using a scalar loop (fallback/reference implementation).
/// T must be a primitive type (i32, i64, f32, f64).
pub fn evalPrimitive(comptime T: type, col_idx: usize, op: Operator, value: T, batch: *column_batch.ColumnBatch, sel: *selection.SelectionVector) void {
    const col = &batch.columns.items[col_idx];
    
    const data = switch (T) {
        i32 => col.data.i32,
        i64 => col.data.i64,
        f32 => col.data.f32,
        f64 => col.data.f64,
        else => @panic("Unsupported type"),
    };
    
    // Scalar loop implementation first for correctness
    const count = batch.num_rows;
    const valid = col.validity;

    for (0..count) |i| {
        if (!sel.isActive(i)) continue; // Already filtered

        // Check null
        if (valid) |v_map| {
            if (!isSet(v_map, i)) {
                sel.set(i, false);
                continue;
            }
        }

        const val = data[i];
        const pass = switch (op) {
            .Eq => val == value,
            .NotEq => val != value,
            .Lt => val < value,
            .LtEq => val <= value,
            .Gt => val > value,
            .GtEq => val >= value,
        };

        if (!pass) {
            sel.set(i, false);
        }
    }
}

inline fn isSet(mask: []const u64, index: usize) bool {
    const word = mask[index / 64];
    return (word >> @intCast(index % 64)) & 1 == 1;
}
