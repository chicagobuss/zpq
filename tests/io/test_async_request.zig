const std = @import("std");
const zpq = @import("zpq");
const AsyncRequest = zpq.s3.AsyncRequest;
const Io = std.Io;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing AsyncRequest State Machine (with Gaps) ---\n", .{});

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
    var req = AsyncRequest.init(allocator);
    defer req.deinit();

    // We want 0-100.
    // Let's read 0-10, skip 10-90, read 90-100.
    var buf1: [10]u8 = undefined;
    var buf2: [10]u8 = undefined;
    
    try req.addSegment(&buf1, 10);
    try req.addSegment(null, 80); // GAP
    try req.addSegment(&buf2, 10);

    try req.prepare(HOST, PORT, "/bucket/key", 0, 100, null);

    // 3. Drive State Machine
    while (req.state != .Finished and req.state != .Error) {
        switch (req.state) {
            .SendingRequest => {
                const done = try req.stepWrite(fd);
                if (done) std.debug.print("Write Complete -> ReadingHeaders\n", .{});
            },
            .ReadingHeaders => {
                const done = try req.stepReadHeaders(fd);
                if (done) std.debug.print("Headers Complete -> ReadingBody\n", .{});
            },
            .ReadingBody => {
                const done = try req.stepReadBody(fd);
                if (done) std.debug.print("Body Complete -> Finished\n", .{});
            },
            else => break,
        }
        
        // Busy wait simulation
        std.posix.nanosleep(0, 1 * std.time.ns_per_ms);
    }

    // 4. Verify Data
    std.debug.print("Finished! Read Total: {d}\n", .{req.body_read_total});
    
    // Verify Buf1 (0-9)
    for (buf1, 0..) |b, i| {
        if (b != i % 256) {
            std.debug.print("Buf1 Mismatch at {d}: Expected {d}, Got {d}\n", .{i, i % 256, b});
            return error.DataMismatch;
        }
    }

    // Verify Buf2 (90-99)
    for (buf2, 0..) |b, i| {
        const expected = (90 + i) % 256;
        if (b != expected) {
            std.debug.print("Buf2 Mismatch at {d}: Expected {d}, Got {d}\n", .{i, expected, b});
            return error.DataMismatch;
        }
    }

    std.debug.print("SUCCESS: AsyncRequest handled Gaps correctly!\n", .{});
}
