const std = @import("std");

pub fn main() !void {
    var x: std.posix.MADV = .WILLNEED;
    _ = x;
}
