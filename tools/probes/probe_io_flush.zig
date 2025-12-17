const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub fn main() !void {
    var w: net.Stream.Writer = undefined;
    try w.interface.flush();
}

