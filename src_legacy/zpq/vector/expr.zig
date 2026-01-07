const std = @import("std");
const batch = @import("batch.zig");
const compute = @import("compute.zig");
const Vector = batch.Vector;
const RecordBatch = batch.RecordBatch;

pub const Expr = union(enum) {
    column: usize, // index
    literal_i32: i32,
    eq: struct { left: *const Expr, right: *const Expr },
};

pub fn evaluate(allocator: std.mem.Allocator, batch_in: RecordBatch, expr: Expr) !Vector {
    switch (expr) {
        .column => |idx| {
            // Return a shallow copy (borrow) of the column vector?
            // Or deep copy?
            // For expression trees, we usually want to consume intermediate results but keep source alive.
            // A "View" vector would be best.
            // Our Vector struct supports non-owned data (allocator=null).
            const col = batch_in.column(idx);
            return Vector{
                .type = col.type,
                .len = col.len,
                .data = col.data,
                .validity = col.validity,
                .capacity = 0,
                .allocator = null, // Borrowed
            };
        },
        .literal_i32 => |val| {
            // Expand literal to vector? Or handle scalar?
            // For now, let's expand to a flat vector for simplicity of kernels.
            // (Inefficient but correct).
            const len = batch_in.len;
            const bytes = try allocator.alloc(u8, len * 4);
            const ints = std.mem.bytesAsSlice(i32, bytes);
            @memset(ints, val);
            
            return Vector{
                .type = .i32,
                .len = len,
                .data = bytes,
                .capacity = bytes.len,
                .allocator = allocator,
            };
        },
        .eq => |e| {
            const l_vec = try evaluate(allocator, batch_in, e.left.*);
            // If l_vec is owned, we should defer deinit?
            // If expr tree is deep, we need to manage intermediates.
            // This naive recursion will leak intermediates unless we track them.
            // For prototype: manual management or verify we free.
            
            // To be safe, let's just leak intermediates in this function scope and rely on arena?
            // Using an ArenaAllocator for the expression evaluation is standard practice.
            // Let's assume `allocator` is an Arena.
            
            const r_vec = try evaluate(allocator, batch_in, e.right.*);
            
            return compute.eq_i32(allocator, l_vec, r_vec);
        },
    }
}

test "eval expr: col(0) == 42" {
    const allocator = std.testing.allocator;
    
    // Create batch
    const data = try allocator.alloc(u8, 4 * 4);

    const ints = std.mem.bytesAsSlice(i32, data);
    ints[0] = 10;
    ints[1] = 42;
    ints[2] = 42;
    ints[3] = 99;
    
    const vec = Vector{ .type = .i32, .len = 4, .data = data };
    const batch_obj = unsafe_batch(vec); // helper

    // Expr
    const col0 = Expr{ .column = 0 };
    const lit42 = Expr{ .literal_i32 = 42 };
    const expr = Expr{ .eq = .{ .left = &col0, .right = &lit42 } };
    
    // Eval in Arena
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const res = try evaluate(arena.allocator(), batch_obj, expr);
    
    try std.testing.expectEqual(batch.VectorType.bool, res.type);
    const vals = res.values(bool);
    try std.testing.expectEqual(false, vals[0]);
    try std.testing.expectEqual(true, vals[1]);
    try std.testing.expectEqual(true, vals[2]);
    try std.testing.expectEqual(false, vals[3]);
    
    allocator.free(data);
    allocator.free(batch_obj.columns);
}

fn unsafe_batch(vec: Vector) RecordBatch {
    // Quick hack for test
    var cols = std.heap.page_allocator.alloc(Vector, 1) catch unreachable;
    cols[0] = vec;
    return RecordBatch{
        .len = vec.len,
        .columns = cols,
        .allocator = std.heap.page_allocator,
    };
}
