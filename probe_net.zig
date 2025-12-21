const std = @import("std");

pub fn main() !void {
    // Probe for getAddressList
    if (@hasDecl(std, "net")) {
        std.debug.print("std.net exists\n", .{});
        if (@hasDecl(std.net, "getAddressList")) {
            std.debug.print("std.net.getAddressList exists\n", .{});
        }
    } else {
        std.debug.print("std.net DOES NOT exist\n", .{});
    }

    if (@hasDecl(std, "Io")) {
        std.debug.print("std.Io exists\n", .{});
        if (@hasDecl(std.Io, "net")) {
            std.debug.print("std.Io.net exists\n", .{});
             if (@hasDecl(std.Io.net, "getAddressList")) {
                std.debug.print("std.Io.net.getAddressList exists\n", .{});
            }
        }
    }
}

