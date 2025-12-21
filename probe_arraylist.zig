const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var list = std.ArrayList(u8).empty;
    defer list.deinit(allocator);
    
    try list.append(allocator, 'A');
    std.debug.print("ArrayList empty works.\n", .{});
    std.debug.print("List len: {d}\n", .{list.items.len});
}
