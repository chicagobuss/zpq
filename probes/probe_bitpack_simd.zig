const std = @import("std");

pub fn main() !void {
    const data = [_]u8{ 0x88, 0xC6, 0xFA };
    const bit_width = 3;
    
    // Scalar check
    var res: [8]u32 = undefined;
    var bits: u32 = 0;
    bits |= @as(u32, data[0]);
    bits |= @as(u32, data[1]) << 8;
    bits |= @as(u32, data[2]) << 16;
    
    for (0..8) |i| {
        res[i] = (bits >> @intCast(i * bit_width)) & 0x07;
    }
    std.debug.print("Scalar res: {any}\n", .{res});
    
    // SIMD attempt
    const v_bits: @Vector(8, u32) = @splat(bits);
    const v_shifts: @Vector(8, u5) = .{ 0, 3, 6, 9, 12, 15, 18, 21 };
    const v_res = (v_bits >> v_shifts) & @as(@Vector(8, u32), @splat(0x07));
    std.debug.print("SIMD res:   {any}\n", .{v_res});
}

