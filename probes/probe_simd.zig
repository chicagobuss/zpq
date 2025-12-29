const std = @import("std");

pub fn main() !void {
    const v1: @Vector(8, u32) = .{ 1, 2, 3, 4, 5, 6, 7, 8 };
    const v2: @Vector(8, u32) = .{ 10, 20, 30, 40, 50, 60, 70, 80 };
    
    const v3 = v1 + v2;
    std.debug.print("v3: {any}\n", .{v3});
    
    const v4 = v1 << @as(@Vector(8, u5), @splat(1));
    std.debug.print("v4 (v1 << 1): {any}\n", .{v4});
    
    const mask: @Vector(8, u32) = @splat(0x0F);
    const v5 = v1 & mask;
    std.debug.print("v5 (v1 & 0x0F): {any}\n", .{v5});

    // Check for @shuffle and other builtins
    const v6 = @shuffle(u32, v1, undefined, @as(@Vector(8, i32), .{ 7, 6, 5, 4, 3, 2, 1, 0 }));
    std.debug.print("v6 (reversed): {any}\n", .{v6});
}

