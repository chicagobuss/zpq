const std = @import("std");

pub fn main() !void {
    const v1: @Vector(8, u256) = @splat(std.math.maxInt(u256));
    const v_shifts: @Vector(8, u8) = .{ 0, 32, 64, 96, 128, 160, 192, 224 };
    const v_res = v1 >> v_shifts;
    std.debug.print("v_res[7]: {x}\n", .{v_res[7]});
}

