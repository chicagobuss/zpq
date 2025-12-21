const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const Connection = zpq.s3.Connection;

const Context = struct {
    done: bool = false,
    bytes_sent: usize = 0,
    would_block_count: usize = 0,
    drain_count: usize = 0,
    error_occurred: ?anyerror = null,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // Start a dummy echo server that reads VERY slowly
    const port = 12345;
    const addr = try xev.shim_net.Address.parseIp4("127.0.0.1", port);
    var server = try xev.TCP.init(addr);
    try server.bind(addr);
    try server.listen(1);
    
    var ctx = Context{};
    var server_ctx = ServerContext{ .loop = &loop };
    
    var c_accept: xev.Completion = .{};
    server.accept(&loop, &c_accept, ServerContext, &server_ctx, onAccept);

    var conn = try Connection.init(&loop, allocator, "localhost", false);
    defer conn.deinit();
    conn.user_ctx = &ctx;
    conn.on_connect = onConnect;
    conn.on_drain = onDrain;
    conn.on_error = onError;

    try conn.connect(addr);

    // Run the loop. We expect to see WouldBlock errors when the 64KB high water mark is hit.
    while (!ctx.done or conn.write_in_flight) {
        try loop.run(.once);
    }

    if (ctx.error_occurred) |err| return err;

    std.debug.print("Backpressure Test Results:\n", .{});
    std.debug.print("- Bytes sent: {}\n", .{ctx.bytes_sent});
    std.debug.print("- WouldBlock hits: {}\n", .{ctx.would_block_count});
    std.debug.print("- Drain callbacks: {}\n", .{ctx.drain_count});

    if (ctx.would_block_count == 0) {
        std.debug.print("FAIL: Never hit High Water Mark!\n", .{});
        std.process.exit(1);
    }
    if (ctx.drain_count == 0) {
        std.debug.print("FAIL: Never received Drain callback!\n", .{});
        std.process.exit(1);
    }

    std.debug.print("test_backpressure passed!\n", .{});
}

const ServerContext = struct {
    loop: *xev.Loop,
    conn: ?xev.TCP = null,
    read_buf: [1024]u8 = undefined,
    c_read: xev.Completion = .{},
    c_timer: xev.Completion = .{},
    timer: ?xev.Timer = null,
};

fn onAccept(s_ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, r: xev.AcceptError!xev.TCP) xev.CallbackAction {
    _ = c;
    const ctx = s_ctx.?;
    if (r) |conn| {
        ctx.conn = conn;
        // Start reading VERY slowly (1KB every 10ms) to trigger backpressure
        slowRead(ctx, loop);
    } else |_| {}
    return .disarm;
}

fn slowRead(ctx: *ServerContext, loop: *xev.Loop) void {
    ctx.conn.?.read(loop, &ctx.c_read, .{ .slice = &ctx.read_buf }, ServerContext, ctx, onServerRead);
}

fn onServerRead(ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, s: xev.TCP, buf: xev.ReadBuffer, r: xev.ReadError!usize) xev.CallbackAction {
    _ = c; _ = s; _ = buf;
    const s_ctx = ctx.?;
    if (r) |n| {
        if (n == 0) return .disarm;
        // Wait 10ms before next read using xev.Timer
        s_ctx.timer = xev.Timer.init() catch return .disarm;
        s_ctx.timer.?.run(loop, &s_ctx.c_timer, 10, ServerContext, s_ctx, onTimer);
    } else |_| {}
    return .disarm;
}

fn onTimer(ctx: ?*ServerContext, loop: *xev.Loop, c: *xev.Completion, r: xev.Timer.RunError!void) xev.CallbackAction {
    _ = c;
    r catch return .disarm;
    const s_ctx = ctx.?;
    slowRead(s_ctx, loop);
    return .disarm;
}

fn onConnect(conn: *Connection, ctx_void: ?*anyopaque) void {
    const ctx: *Context = @ptrCast(@alignCast(ctx_void));
    sendData(conn, ctx);
}

fn sendData(conn: *Connection, ctx: *Context) void {
    // Send 16KB chunks until we hit WouldBlock
    var chunk: [16 * 1024]u8 = undefined;
    @memset(&chunk, 'A');

    while (ctx.bytes_sent < 1024 * 1024) {
        conn.write(&chunk) catch |err| {
            if (err == error.WouldBlock) {
                ctx.would_block_count += 1;
                return;
            }
            ctx.error_occurred = err;
            return;
        };
        ctx.bytes_sent += chunk.len;
    }
    ctx.done = true;
}

fn onDrain(conn: *Connection, ctx_void: ?*anyopaque) void {
    const ctx: *Context = @ptrCast(@alignCast(ctx_void));
    ctx.drain_count += 1;
    sendData(conn, ctx);
}

fn onError(conn: *Connection, ctx_void: ?*anyopaque, err: anyerror) void {
    _ = conn;
    const ctx: *Context = @ptrCast(@alignCast(ctx_void));
    if (err != error.EOF) {
        ctx.error_occurred = err;
    }
}
