const std = @import("std");
const Io = std.Io;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    _ = allocator;

    std.debug.print("\n--- Testing No-HEAD Open (Suffix Range) ---\n", .{});

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

    // Request last 10 bytes
    // Mock server file size is 10MB (10485760 bytes)
    var req_buf: [1024]u8 = undefined;
    const req = try std.fmt.bufPrint(&req_buf, 
        "GET /bucket/key HTTP/1.1\r\n" ++
        "Host: {s}:{d}\r\n" ++
        "Range: bytes=-10\r\n" ++
        "Connection: close\r\n" ++
        "\r\n",
        .{HOST, PORT}
    );
    _ = try std.posix.write(fd, req);

    // Read Response
    var buf: [4096]u8 = undefined;
    const n = try std.posix.read(fd, &buf);
    const response = buf[0..n];
    
    std.debug.print("Response:\n{s}\n", .{response});

    // Check Status 206
    if (std.mem.indexOf(u8, response, "206 Partial Content") == null) {
        std.debug.print("FAIL: Expected 206 status\n", .{});
        return error.TestFailed;
    }

    // Check Content-Range
    // Expected: Content-Range: bytes 10485750-10485759/10485760
    if (std.mem.indexOf(u8, response, "Content-Range: bytes 10485750-10485759/10485760") == null) {
        std.debug.print("FAIL: Invalid Content-Range header\n", .{});
        return error.TestFailed;
    }

    // Check Content-Length
    if (std.mem.indexOf(u8, response, "Content-Length: 10") == null) {
        std.debug.print("FAIL: Invalid Content-Length header\n", .{});
        return error.TestFailed;
    }

    std.debug.print("SUCCESS: Suffix Range request worked and returned total size!\n", .{});
}

