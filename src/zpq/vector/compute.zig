const std = @import("std");
const batch = @import("batch.zig");
const Vector = batch.Vector;

/// Comparison kernels for i32
pub fn eq_i32(allocator: std.mem.Allocator, left: Vector, right: Vector) !Vector {
    // Assert types (in debug)
    std.debug.assert(left.type == .i32);
    std.debug.assert(right.type == .i32);
    std.debug.assert(left.len == right.len);

    const len = left.len;
    // Result is a BOOLEAN vector.
    // In our simplified Vector struct, BOOLEAN is just a vector of type .bool.
    // The data representation for .bool needs to be decided.
    // Parquet uses bit-packed. Arrow uses bit-packed.
    // For SIMD efficiency during processing, using 1 byte per bool is often faster (Vector(u8)).
    // Let's use 1 byte per bool for intermediate vectors.

    const out_data = try allocator.alloc(u8, len);
    errdefer allocator.free(out_data);

    const l_vals = left.values(i32);
    const r_vals = right.values(i32);

    // SIMD Loop
    const VecType = @Vector(8, i32);

    var i: usize = 0;

    while (i + 8 <= len) : (i += 8) {
        const v_l: VecType = l_vals[i..][0..8].*;
        const v_r: VecType = r_vals[i..][0..8].*;
        const v_res = v_l == v_r;

        // Convert bool vector to u8 array
        // Use @select to convert bool vector to integer vector, then extract
        const ones: @Vector(8, u8) = @splat(1);
        const zeros: @Vector(8, u8) = @splat(0);
        const res_vec = @select(u8, v_res, ones, zeros);
        const res_arr: [8]u8 = res_vec;
        for (0..8) |j| {
            out_data[i + j] = res_arr[j];
        }
    }

    // Tail
    while (i < len) : (i += 1) {
        out_data[i] = @intFromBool(l_vals[i] == r_vals[i]);
    }

    return Vector{
        .type = .bool, // Interpreted as byte-bool here
        .len = len,
        .data = out_data,
        .capacity = len,
        .allocator = allocator,
    };
}

pub fn eq_i32_scalar(allocator: std.mem.Allocator, left: Vector, right_scalar: i32) !Vector {
    std.debug.assert(left.type == .i32);

    const len = left.len;
    const out_data = try allocator.alloc(u8, len);
    errdefer allocator.free(out_data);

    const l_vals = left.values(i32);
    const v_scalar: @Vector(8, i32) = @splat(right_scalar);

    var i: usize = 0;
    while (i + 8 <= len) : (i += 8) {
        const v_l: @Vector(8, i32) = l_vals[i..][0..8].*;
        const v_res = v_l == v_scalar;

        const res_arr: [8]bool = @bitCast(v_res);
        for (0..8) |j| {
            out_data[i + j] = @intFromBool(res_arr[j]);
        }
    }

    while (i < len) : (i += 1) {
        out_data[i] = @intFromBool(l_vals[i] == right_scalar);
    }

    return Vector{
        .type = .bool,
        .len = len,
        .data = out_data,
        .capacity = len,
        .allocator = allocator,
    };
}

/// Filter kernel: Takes a data vector and a boolean selector, returns new dense vector.
/// This is "Selection" / "Compress".
pub fn filter(allocator: std.mem.Allocator, data: Vector, selection: Vector) !Vector {
    // Assert selection is byte-bool
    std.debug.assert(selection.type == .bool);
    std.debug.assert(data.len == selection.len);

    // 1. Count output size
    const sel_bytes = selection.values(u8); // since we used u8 for bool
    var count: usize = 0;
    for (sel_bytes) |b| count += b;

    // 2. Allocate output
    const width = switch (data.type) {
        .i32 => 4,
        else => @panic("Unsupported type for filter"),
    };

    const out_bytes = try allocator.alloc(u8, count * width);
    errdefer allocator.free(out_bytes);

    // 3. Scatter/Gather
    if (data.type == .i32) {
        const src = data.values(i32);
        const dst = std.mem.bytesAsSlice(i32, out_bytes);
        var dst_idx: usize = 0;

        for (src, 0..) |val, i| {
            if (sel_bytes[i] == 1) {
                dst[dst_idx] = val;
                dst_idx += 1;
            }
        }
    }

    return Vector{
        .type = data.type,
        .len = count,
        .data = out_bytes,
        .validity = null, // TODO handle nulls
        .capacity = out_bytes.len,
        .allocator = allocator,
    };
}
