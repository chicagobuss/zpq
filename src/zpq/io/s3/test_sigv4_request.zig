const std = @import("std");
// Use the new S3 implementation
const AsyncRequest = @import("request.zig").AsyncRequest;
const Io = std.Io;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing AsyncRequest with SigV4 ---\n", .{});

    // 1. Connect
    const addr = try Io.net.IpAddress.parse(HOST, PORT);
    const fd = try std.posix.socket(std.posix.AF.INET, std.posix.SOCK.STREAM, 0);
    defer std.posix.close(fd);

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

    // 2. Init Request
    var req = AsyncRequest.init();
    defer req.deinit(allocator);

    var buf1: [100]u8 = undefined;
    try req.addSegment(allocator, &buf1, 100);

    const config = @import("types.zig").S3Config{
        .credentials = .{
            .access_key = "test_access",
            .secret_key = "test_secret",
        },
        .region = "us-east-1",
    };

    try req.prepare(allocator, HOST, PORT, "/bucket/key", 0, 100, null, config);

    // Verify Authorization header is present in write_buf
    const request_str = req.write_buf.items;
    std.debug.print("Request:\n{s}\n", .{request_str});

    if (std.mem.indexOf(u8, request_str, "Authorization: AWS4-HMAC-SHA256") == null) {
        std.debug.print("FAILURE: Authorization header missing!\n", .{});
        return error.AuthHeaderMissing;
    }
    
    if (std.mem.indexOf(u8, request_str, "X-Amz-Date:") == null) {
        std.debug.print("FAILURE: X-Amz-Date header missing!\n", .{});
        return error.DateHeaderMissing;
    }

    // 3. Drive State Machine (optional, just needed to verify request generation)
    // We don't strictly need to send it to verify signing generation, but let's try writing.
    // ...
    
    std.debug.print("SUCCESS: AsyncRequest generated SigV4 headers!\n", .{});
}
