const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var list = try std.ArrayList(u8).initCapacity(allocator, 10);
    defer list.deinit(allocator);
    
    try list.append(allocator, 'D');
    std.debug.print("len: {d}, cap: {d}\n", .{list.items.len, list.capacity});
}

