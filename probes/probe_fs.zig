const std = @import("std");

pub fn main() !void {
    @compileLog("--- Statx struct fields ---");
    inline for (@typeInfo(std.os.linux.Statx).@"struct".fields) |field| {
        @compileLog(field.name);
    }
}
