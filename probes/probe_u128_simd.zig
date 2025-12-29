const std = @import("std");

pub fn main() !void {
    const v1: @Vector(8, u128) = @splat(0xFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF);
    const v_shifts: @Vector(8, u7) = .{ 0, 10, 20, 30, 40, 50, 60, 70 };
    const v_res = v1 >> v_shifts;
    std.debug.print("v_res[7]: {x}\n", .{v_res[7]});
}

