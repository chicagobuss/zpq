const std = @import("std");

/// Demonstrates the 0.16.dev pattern for ArrayListUnmanaged.
/// Managed ArrayList.init(allocator) is now discouraged in core logic.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // The 0.16 pattern: empty init, pass allocator to all operations
    var list = std.ArrayListUnmanaged(u8){};
    defer list.deinit(allocator);

    try list.append(allocator, 'H');
    try list.appendSlice(allocator, "ello");

    std.debug.print("List: {s}\n", .{list.items});
}

