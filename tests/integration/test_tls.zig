const std = @import("std");
const zpq = @import("zpq");
const TlsAdapter = zpq.s3.TlsAdapter;
const net = std.Io.net;

pub fn main() !void {
    // Skip if ZPQ_TEST_NETWORK not set (requires internet access)
    if (std.posix.getenv("ZPQ_TEST_NETWORK") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_NETWORK=1 to run (requires internet)\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    
    std.debug.print("\n--- Testing TlsAdapter against 1.1.1.1 ---\n", .{});
    
    // 1. Connect to 1.1.1.1:443
    const addr = try net.IpAddress.parse("1.1.1.1", 443);
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    errdefer std.posix.close(fd);
    
    switch (addr) {
        .ip4 => |ip4| {
            const sa = std.posix.sockaddr.in{
                .family = std.posix.AF.INET,
                .port = std.mem.nativeToBig(u16, ip4.port),
                .addr = @as(u32, @bitCast(ip4.bytes)), 
            };
            try std.posix.connect(fd, @ptrCast(&sa), @sizeOf(std.posix.sockaddr.in));
        },
        else => return error.UnsupportedAddressFamily,
    }
    std.debug.print("Connected to 1.1.1.1:443\n", .{});
    
    // 3. Init TLS
    var adapter = try TlsAdapter.init(allocator, fd, "one.one.one.one", null);
    defer adapter.deinit();
    
    std.debug.print("TLS Handshake completed!\n", .{});
    
    // 4. Send HTTP Request
    const req = "HEAD / HTTP/1.1\r\nHost: one.one.one.one\r\nConnection: close\r\n\r\n";
    _ = try adapter.write(req);
    std.debug.print("Sent request\n", .{});
    
    // 5. Read Response
    var buf: [4096]u8 = undefined;
    const n = try adapter.read(&buf);
    std.debug.print("Received {d} bytes\n", .{n});
    if (n > 0) {
        std.debug.print("Response:\n{s}\n", .{buf[0..n]});
    }
    
    std.debug.print("SUCCESS: TlsAdapter works!\n", .{});
}
