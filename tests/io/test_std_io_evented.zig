const std = @import("std");
const Io = std.Io;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var evented: Io.Evented = undefined;
    try evented.init(allocator, .{});
    defer evented.deinit();
    
    std.debug.print("Io.Evented initialized on {s}!\n", .{@tagName((@import("builtin").os.tag))});
    
    const io = evented.io();
    _ = io;
}
