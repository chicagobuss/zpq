const std = @import("std");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    // Try to access std.net directly
    if (@hasDecl(std, "net")) {
        std.debug.print("std.net exists\n", .{});
        const list = try std.net.getAddressList(allocator, "google.com", 80);
        defer list.deinit();
        std.debug.print("Found: {any}\n", .{list.addrs[0]});
    } else {
        std.debug.print("std.net DOES NOT exist\n", .{});
        // Check what DOES exist
        // This iteration is not easily possible in runtime Zig without huge compilation overhead or comptime reflection that prints during compilation
    }
}

