const std = @import("std");

pub fn main() !void {
    const Client = std.crypto.tls.Client;
    const info = @typeInfo(Client).@"struct";
    
    inline for (info.fields) |field| {
        std.debug.print("Field: {s} type: {any}\n", .{field.name, field.type});
    }
}

