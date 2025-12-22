const std = @import("std");

pub fn main() !void {
    const hostname = "google.com\x00";
    var hints: std.c.addrinfo = std.mem.zeroInit(std.c.addrinfo, .{
        .family = std.c.AF.UNSPEC,
        .socktype = std.c.SOCK.STREAM,
    });
    var res: ?*std.c.addrinfo = null;
    const rc = std.c.getaddrinfo(hostname.ptr, "443\x00", &hints, &res);
    if (rc != 0) {
        std.debug.print("getaddrinfo failed: {s}\n", .{std.mem.span(std.c.gai_strerror(rc))});
        return;
    }
    defer std.c.freeaddrinfo(res);

    var cur = res;
    while (cur) |info| : (cur = info.next) {
        std.debug.print("Family: {}, Socktype: {}\n", .{ info.family, info.socktype });
    }
}
