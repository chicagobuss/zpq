const std = @import("std");
const Io = std.Io;
const net = Io.net;

pub fn main() !void {
    const info = @typeInfo(net.Stream.Writer).@"struct";
    inline for (info.fields) |field| {
        std.debug.print("Field: {s} type: {any}\n", .{field.name, field.type});
    }
}

