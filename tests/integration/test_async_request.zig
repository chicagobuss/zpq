const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const AsyncRequest = zpq.s3.AsyncRequest;
const Connection = zpq.s3.Connection;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing AsyncRequest State Machine (with Gaps) ---\n", .{});

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // 1. Connect
    const addr = try xev.shim_net.Address.parseIp4(HOST, PORT);
    var conn = try Connection.init(&loop, allocator, HOST, false);
    defer conn.deinit();
    try conn.connect(addr);

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

    try req.prepare(HOST, PORT, "/bucket/key", 0, 100, false, null);

    const WaitCtx = struct {
        done: bool = false,
        fn onDone(ctx: ?*anyopaque, r: *AsyncRequest) void {
            _ = r;
            const self_ptr: *@This() = @ptrCast(@alignCast(ctx));
            self_ptr.done = true;
        }
    };
    var wait_ctx = WaitCtx{};
    req.done_ctx = &wait_ctx;
    req.on_done = WaitCtx.onDone;

    // 3. Execute
    try req.execute(conn);

    // 4. Drive Loop
    while (!wait_ctx.done) {
        try loop.run(.once);
    }

    // 5. Verify Data
    std.debug.print("Finished! Read Total: {d}\n", .{req.body_read_total});

    if (req.state == .Error) return error.RequestFailed;

    // Verify Buf1 (0-9)
    for (buf1, 0..) |b, i| {
        if (b != i % 256) {
            std.debug.print("Buf1 Mismatch at {d}: Expected {d}, Got {d}\n", .{ i, i % 256, b });
            return error.DataMismatch;
        }
    }

    // Verify Buf2 (90-99)
    for (buf2, 0..) |b, i| {
        const expected = (90 + i) % 256;
        if (b != expected) {
            std.debug.print("Buf2 Mismatch at {d}: Expected {d}, Got {d}\n", .{ i, expected, b });
            return error.DataMismatch;
        }
    }

    std.debug.print("SUCCESS: AsyncRequest handled Gaps correctly!\n", .{});
}
