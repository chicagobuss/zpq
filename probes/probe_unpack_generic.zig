const std = @import("std");

pub fn unpackBlock(comptime T: type, comptime bit_width: u8, in: []const u8, out: []T) void {
    const num_values = 8;
    const total_bits = bit_width * num_values;
    const total_bytes = (total_bits + 7) / 8;
    
    // For small bit widths, we can load everything into a single u64 or u128
    // and then use vector shifts.
    if (total_bits <= 64) {
        var bits: u64 = 0;
        // Manual unrolling for speed
        const limit = @min(in.len, total_bytes);
        for (0..limit) |i| {
            bits |= @as(u64, in[i]) << @intCast(i * 8);
        }
        
        const v_bits: @Vector(8, u64) = @splat(bits);
        comptime var shifts: [8]u6 = undefined;
        inline for (0..8) |i| {
            shifts[i] = @intCast(i * bit_width);
        }
        const v_shifts: @Vector(8, u6) = shifts;
        const mask: u64 = (@as(u64, 1) << bit_width) - 1;
        const v_res = (v_bits >> v_shifts) & @as(@Vector(8, u64), @splat(mask));
        
        // Store results
        inline for (0..8) |i| {
            out[i] = @intCast(v_res[i]);
        }
    } else {
        // Fallback or more complex implementation
    }
}

pub fn main() !void {
    const data = [_]u8{ 0x88, 0xC6, 0xFA };
    var out: [8]u32 = undefined;
    unpackBlock(u32, 3, &data, &out);
    std.debug.print("Unpacked: {any}\n", .{out});
}

