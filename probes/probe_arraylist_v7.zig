const std = @import("std");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    const allocator = gpa.allocator();

    var list = std.ArrayList(u8).init(allocator);
    try list.append('a');
    list.deinit();

    var unmanaged = std.ArrayListUnmanaged(u8){};
    try unmanaged.append(allocator, 'b');
    unmanaged.deinit(allocator);
}
