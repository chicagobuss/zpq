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
    // TODO: String filters / byte array

    // Composite filters
    and_filter: struct { left: *Filter, right: *Filter },
    or_filter: struct { left: *Filter, right: *Filter },

    pub fn evaluate(self: Filter, batch: *column_batch.ColumnBatch, sel: *selection.SelectionVector) void {
        switch (self) {
            .int32 => |f| scalar.evalPrimitive(i32, f.col_idx, f.op, f.value, batch, sel),
            .int64 => |f| scalar.evalPrimitive(i64, f.col_idx, f.op, f.value, batch, sel),
            .float => |f| scalar.evalPrimitive(f32, f.col_idx, f.op, f.value, batch, sel),
            .double => |f| scalar.evalPrimitive(f64, f.col_idx, f.op, f.value, batch, sel),
            .and_filter => |f| {
                f.left.evaluate(batch, sel);
                f.right.evaluate(batch, sel);
            },
            .or_filter => |f| {
                _ = f;
                @panic("OR filter not yet implemented"); 
            },
        }
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
    f.evaluate(&batch, &batch.selection);
    
    // Verify
    try std.testing.expect(batch.selection.isActive(0)); // 10 < 25
    try std.testing.expect(batch.selection.isActive(1)); // 20 < 25
    try std.testing.expect(!batch.selection.isActive(2)); // 30 >= 25
    try std.testing.expect(!batch.selection.isActive(3)); // 40 >= 25
}
