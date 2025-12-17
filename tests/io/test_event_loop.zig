const std = @import("std");
const zpq = @import("zpq");
const EventLoop = zpq.s3.EventLoop;
const AsyncRequest = zpq.s3.AsyncRequest;
const Io = std.Io;

const HOST = "127.0.0.1";
const PORT = 9000;

fn connect() !std.posix.fd_t {
    const addr = try Io.net.IpAddress.parse(HOST, PORT);
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
    
    // Set non-blocking
    const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
    var flags_o: std.posix.O = @bitCast(@as(u32, @truncate(flags)));
    flags_o.NONBLOCK = true;
    _ = try std.posix.fcntl(fd, std.posix.F.SETFL, @as(u32, @bitCast(flags_o)));
    
    return fd;
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing EventLoop + AsyncRequest Integration ---\n", .{});

    var loop = try EventLoop.init(allocator);
    defer loop.deinit();

    // 1. Create 3 requests
    var req1 = AsyncRequest.init(allocator);
    var req2 = AsyncRequest.init(allocator);
    var req3 = AsyncRequest.init(allocator);
    defer req1.deinit();
    defer req2.deinit();
    defer req3.deinit();

    var buf1: [100]u8 = undefined;
    var buf2: [100]u8 = undefined;
    var buf3: [100]u8 = undefined;

    try req1.addSegment(&buf1, 100);
    try req2.addSegment(&buf2, 100);
    try req3.addSegment(&buf3, 100);

    try req1.prepare(HOST, PORT, "/req1", 0, 100, null);
    try req2.prepare(HOST, PORT, "/req2", 100, 200, null);
    try req3.prepare(HOST, PORT, "/req3", 200, 300, null);

    const fd1 = try connect();
    const fd2 = try connect();
    const fd3 = try connect();
    defer std.posix.close(fd1);
    defer std.posix.close(fd2);
    defer std.posix.close(fd3);

    // 2. Register for WRITE (to send) and READ (to receive)
    try loop.registerWrite(fd1, &req1);
    try loop.registerRead(fd1, &req1);
    
    try loop.registerWrite(fd2, &req2);
    try loop.registerRead(fd2, &req2);
    
    try loop.registerWrite(fd3, &req3);
    try loop.registerRead(fd3, &req3);

    // 3. Drive Loop
    var done_count: usize = 0;
    var tick_count: usize = 0;
    const MAX_TICKS = 100; // Increased timeout for slow environments
    
    while (done_count < 3) {
        tick_count += 1;
        if (tick_count > MAX_TICKS) {
            std.debug.print("TIMEOUT after {d} ticks! States: req1={s} req2={s} req3={s}\n", 
                .{tick_count, @tagName(req1.state), @tagName(req2.state), @tagName(req3.state)});
            return error.TestTimeout;
        }
        
        std.debug.print("Tick #{d}: req1={s} req2={s} req3={s}\n", .{tick_count, @tagName(req1.state), @tagName(req2.state), @tagName(req3.state)});
        const n = try loop.tick();
        std.debug.print("  Processed {d} events\n", .{n});
        
        // Check status
        done_count = 0;
        if (req1.state == .Finished or req1.state == .Error) done_count += 1;
        if (req2.state == .Finished or req2.state == .Error) done_count += 1;
        if (req3.state == .Finished or req3.state == .Error) done_count += 1;
    }

    // 4. Verify
    if (req1.state == .Finished and req2.state == .Finished and req3.state == .Finished) {
        std.debug.print("SUCCESS: All 3 requests finished!\n", .{});
        
        // Verify data
        for (buf1, 0..) |b, i| if (b != i % 256) return error.DataMismatch1;
        for (buf2, 0..) |b, i| if (b != (100 + i) % 256) return error.DataMismatch2;
        for (buf3, 0..) |b, i| if (b != (200 + i) % 256) return error.DataMismatch3;
        
        std.debug.print("Data verified.\n", .{});
    } else {
        std.debug.print("FAIL: Requests did not finish correctly.\n", .{});
        std.debug.print("Req1: {}\n", .{req1.state});
        std.debug.print("Req2: {}\n", .{req2.state});
        std.debug.print("Req3: {}\n", .{req3.state});
        return error.TestFailed;
    }
}
