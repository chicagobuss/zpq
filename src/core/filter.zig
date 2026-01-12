const std = @import("std");
const schema = @import("schema.zig");
const column_batch = @import("column_batch.zig");
const selection = @import("selection.zig");

// Re-export shared types
pub const Operator = @import("filter/operator.zig").Operator;

// Kernels
const scalar = @import("filter/scalar.zig");

pub const Filter = union(enum) {
    // Leaf predicates
    int32: struct { col_idx: usize, op: Operator, value: i32 },
    int64: struct { col_idx: usize, op: Operator, value: i64 },
    float: struct { col_idx: usize, op: Operator, value: f32 },
    double: struct { col_idx: usize, op: Operator, value: f64 },
    string: struct { col_idx: usize, op: Operator, value: []const u8 },
    boolean: struct { col_idx: usize, op: Operator, value: bool },

    // Composite filters
    and_filter: struct { left: *Filter, right: *Filter },
    or_filter: struct { left: *Filter, right: *Filter },

    pub fn evaluate(self: Filter, batch: *column_batch.ColumnBatch, sel: *selection.SelectionVector, req_cols: []const usize) void {
        switch (self) {
            .int32 => |f| scalar.evalPrimitive(i32, getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .int64 => |f| scalar.evalPrimitive(i64, getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .float => |f| scalar.evalPrimitive(f32, getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .double => |f| scalar.evalPrimitive(f64, getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .string => |f| scalar.evalString(getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .boolean => |f| scalar.evalBool(getLocalIdx(f.col_idx, req_cols), f.op, f.value, batch, sel),
            .and_filter => |f| {
                f.left.evaluate(batch, sel, req_cols);
                f.right.evaluate(batch, sel, req_cols);
            },
            .or_filter => |f| {
                // For OR, we need to save the current selection, evaluate left,
                // save that result, restore original, evaluate right, then merge
                const count = batch.num_rows;
                
                // Save original selection
                var original_sel: [1024]u64 = undefined;
                const words_needed = (count + 63) / 64;
                @memcpy(original_sel[0..words_needed], sel.mask[0..words_needed]);
                
                // Evaluate left
                f.left.evaluate(batch, sel, req_cols);
                
                // Save left result
                var left_result: [1024]u64 = undefined;
                @memcpy(left_result[0..words_needed], sel.mask[0..words_needed]);
                
                // Restore original for right evaluation
                @memcpy(sel.mask[0..words_needed], original_sel[0..words_needed]);
                
                // Evaluate right
                f.right.evaluate(batch, sel, req_cols);
                
                // OR: merge left | right
                for (0..words_needed) |w| {
                    sel.mask[w] = left_result[w] | sel.mask[w];
                }
            },
        }
    }

    fn getLocalIdx(global_idx: usize, req_cols: []const usize) usize {
        for (req_cols, 0..) |req, i| {
            if (req == global_idx) return i;
        }
        unreachable;
    }
};

test "filter primitive int32" {
    const allocator = std.testing.allocator;
    
    // 1. Create a dummy batch
    const count = 4;
    var batch = try column_batch.ColumnBatch.init(allocator, count);
    defer batch.deinit();
    
    // Add column (index 0)
    const col_def = schema.SchemaElement{
        .type = .INT32,
        .name = "test",
        .repetition_type = .REQUIRED,
        .type_length = null, .num_children = null, .scale = null, .precision = null, .field_id = null
    };
    
    const col = try batch.addColumn(col_def);
    col.data.i32[0] = 10;
    col.data.i32[1] = 20;
    col.data.i32[2] = 30;
    col.data.i32[3] = 40;
    batch.num_rows = count;

    // 2. Selection vector is batch.selection
    
    // 3. Filter: col[0] < 25
    const f = Filter{ .int32 = .{ .col_idx = 0, .op = .Lt, .value = 25 } };
    
    // Evaluate
    f.evaluate(&batch, &batch.selection, &[_]usize{0});
    
    // Verify
    try std.testing.expect(batch.selection.isActive(0)); // 10 < 25
    try std.testing.expect(batch.selection.isActive(1)); // 20 < 25
    try std.testing.expect(!batch.selection.isActive(2)); // 30 >= 25
    try std.testing.expect(!batch.selection.isActive(3)); // 40 >= 25
}
