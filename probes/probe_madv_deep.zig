const std = @import("std");

pub fn main() !void {
    const MADV = std.posix.MADV;
    const info = @typeInfo(MADV);
    
    std.debug.print("Type: {}\n", .{@TypeOf(MADV)});
    std.debug.print("Info: {}\n", .{info});
    
    if (info == .@"struct") {
        std.debug.print("Fields:\n", .{});
        inline for (info.@"struct".fields) |f| {
            std.debug.print("  {s}: {}\n", .{f.name, f.type});
        }
        std.debug.print("Decls:\n", .{});
        inline for (info.@"struct".decls) |d| {
            std.debug.print("  {s}\n", .{d.name});
        }
    }
}
