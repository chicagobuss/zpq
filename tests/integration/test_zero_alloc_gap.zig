const std = @import("std");
const Io = std.Io;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    std.debug.print("\n--- Testing Zero-Allocation Gap ---\n", .{});
    
    // Connect
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

    // Request bytes 0-20 (21 bytes total)
    // We want: [0..5] (5 bytes), skip [5..15] (10 bytes), [15..21] (6 bytes)
    var req_buf: [1024]u8 = undefined;
    const req = try std.fmt.bufPrint(&req_buf, 
        "GET /bucket/key HTTP/1.1\r\n" ++
        "Host: {s}:{d}\r\n" ++
        "Range: bytes=0-20\r\n" ++
        "Connection: close\r\n" ++
        "\r\n",
        .{HOST, PORT}
    );
    _ = try std.posix.write(fd, req);

    // 1. Consume Headers
    var header_buf: [1]u8 = undefined;
    var last_four: [4]u8 = .{0, 0, 0, 0};
    while (true) {
        const n = try std.posix.read(fd, &header_buf);
        if (n == 0) return error.UnexpectedEOF;
        
        // Shift and push
        last_four[0] = last_four[1];
        last_four[1] = last_four[2];
        last_four[2] = last_four[3];
        last_four[3] = header_buf[0];
        
        if (std.mem.eql(u8, &last_four, "\r\n\r\n")) break;
    }
    std.debug.print("Headers consumed.\n", .{});

    // 2. Read first 5 bytes
    var buf_A: [5]u8 = undefined;
    var read_A: usize = 0;
    while (read_A < 5) {
        const n = try std.posix.read(fd, buf_A[read_A..]);
        if (n == 0) return error.UnexpectedEOF;
        read_A += n;
    }
    std.debug.print("Read Buf A: {any}\n", .{buf_A});

    // 3. Skip 10 bytes (The "Zero-Allocation Gap")
    var skipped: usize = 0;
    var trash_buf: [1]u8 = undefined; // Tiny stack buffer
    while (skipped < 10) {
        const n = try std.posix.read(fd, &trash_buf);
        if (n == 0) return error.UnexpectedEOF;
        skipped += n;
    }
    std.debug.print("Skipped {d} bytes.\n", .{skipped});

    // 4. Read last 6 bytes
    var buf_B: [6]u8 = undefined;
    var read_B: usize = 0;
    while (read_B < 6) {
        const n = try std.posix.read(fd, buf_B[read_B..]);
        if (n == 0) return error.UnexpectedEOF;
        read_B += n;
    }
    std.debug.print("Read Buf B: {any}\n", .{buf_B});

    // Verification
    // Mock server data is offset % 256
    // Buf A: 0, 1, 2, 3, 4
    for (buf_A, 0..) |b, i| {
        if (b != i % 256) {
            std.debug.print("FAIL: Buf A mismatch at {d}: expected {d}, got {d}\n", .{i, i, b});
            return error.TestFailed;
        }
    }

    // Buf B: 15, 16, 17, 18, 19, 20
    for (buf_B, 0..) |b, i| {
        const expected = (15 + i) % 256;
        if (b != expected) {
            std.debug.print("FAIL: Buf B mismatch at {d}: expected {d}, got {d}\n", .{i, expected, b});
            return error.TestFailed;
        }
    }

    std.debug.print("SUCCESS: Gap skipping worked correctly!\n", .{});
}

