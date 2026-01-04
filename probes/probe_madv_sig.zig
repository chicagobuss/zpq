const std = @import("std");

pub fn main() !void {
    const madvise_info = @typeInfo(@TypeOf(std.posix.madvise));
    std.debug.print("madvise type info: {}\n", .{madvise_info});
    
    if (madvise_info == .Fn) {
        const params = madvise_info.Fn.params;
        if (params.len >= 3) {
            std.debug.print("3rd param type: {}\n", .{params[2].type});
            const p3_info = @typeInfo(params[2].type.?);
            std.debug.print("3rd param info: {}\n", .{p3_info});
        }
    }
}
