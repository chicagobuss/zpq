const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var client = zpq.io.http.Client.init(&loop, allocator);

    std.debug.print("Testing HTTP Client against S3 (52.216.48.72)...\n", .{});
    
    // Note: We use the IP because the simplistic Client doesn't do DNS yet.
    // The Host header inside Client will use the IP too unless we change it.
    // Wait, Client.fetch takes `host` and uses it for BOTH connection and Host header.
    // So if I pass IP, Host header is IP. S3 might reject that or 404.
    // But `test_s3_head.zig` used IP for connection but string for Host header.
    // I should improve Client to take separate IP and Hostname if needed.
    // For now, I'll update Client.fetch to take `ip` and `host`.
    
    try client.fetch("s3.amazonaws.com", "52.216.48.72", 443, "/");

    try loop.run(.until_done);
}

