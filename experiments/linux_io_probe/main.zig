const std = @import("std");
const Io = std.Io;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var evented: Io.Evented = undefined;
    // Correct signature for IoUring backend: init(allocator)
    try evented.init(allocator); 
    defer evented.deinit();
    
    std.debug.print("std.Io.Evented (IoUring) initialized successfully on Linux!\n", .{});
}
