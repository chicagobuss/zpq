const std = @import("std");
const zpq = @import("zpq");
const EventLoop = zpq.s3.EventLoop;
const AsyncRequest = zpq.s3.AsyncRequest;
const Connection = zpq.s3.Connection;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing EventLoop + AsyncRequest Integration ---\n", .{});

    var loop = try EventLoop.init(allocator);
    defer loop.deinit();

    const addr = try zpq.s3.dns.xev.shim_net.Address.parseIp4(HOST, PORT);

    const BatchCtx = struct {
        pending: usize = 0,
        fn onDone(ctx: ?*anyopaque, r: *AsyncRequest) void {
            _ = r;
            const self_ptr: *@This() = @ptrCast(@alignCast(ctx));
            self_ptr.pending -= 1;
        }
    };
    var batch_ctx = BatchCtx{ .pending = 3 };

    // 1. Create 3 requests
    var req1 = AsyncRequest.init(allocator);
    var req2 = AsyncRequest.init(allocator);
    var req3 = AsyncRequest.init(allocator);
    defer req1.deinit();
    defer req2.deinit();
    defer req3.deinit();

    req1.done_ctx = &batch_ctx;
    req1.on_done = BatchCtx.onDone;
    req2.done_ctx = &batch_ctx;
    req2.on_done = BatchCtx.onDone;
    req3.done_ctx = &batch_ctx;
    req3.on_done = BatchCtx.onDone;

    var buf1: [100]u8 = undefined;
    var buf2: [100]u8 = undefined;
    var buf3: [100]u8 = undefined;

    try req1.addSegment(&buf1, 100);
    try req2.addSegment(&buf2, 100);
    try req3.addSegment(&buf3, 100);

    try req1.prepare(HOST, PORT, "/req1", 0, 100, false, null);
    try req2.prepare(HOST, PORT, "/req2", 100, 200, false, null);
    try req3.prepare(HOST, PORT, "/req3", 200, 300, false, null);

    // 2. Create 3 connections
    var conn1 = try Connection.init(loop.loop, allocator, HOST, false);
    var conn2 = try Connection.init(loop.loop, allocator, HOST, false);
    var conn3 = try Connection.init(loop.loop, allocator, HOST, false);
    defer {
        conn1.close(); conn1.deinit();
        conn2.close(); conn2.deinit();
        conn3.close(); conn3.deinit();
    }

    try conn1.connect(addr);
    try conn2.connect(addr);
    try conn3.connect(addr);

    // 3. Execute
    try req1.execute(conn1);
    try req2.execute(conn2);
    try req3.execute(conn3);

    // 4. Drive Loop
    while (batch_ctx.pending > 0) {
        _ = try loop.tick();
    }

    // 5. Verify
    if (req1.state == .Finished and req2.state == .Finished and req3.state == .Finished) {
        std.debug.print("SUCCESS: All 3 requests finished!\n", .{});
        
        // Verify data
        for (buf1, 0..) |b, i| if (b != i % 256) return error.DataMismatch1;
        for (buf2, 0..) |b, i| if (b != (100 + i) % 256) return error.DataMismatch2;
        for (buf3, 0..) |b, i| if (b != (200 + i) % 256) return error.DataMismatch3;
        
        std.debug.print("Data verified.\n", .{});
    } else {
        std.debug.print("FAIL: Requests did not finish correctly.\n", .{});
        return error.TestFailed;
    }
}
