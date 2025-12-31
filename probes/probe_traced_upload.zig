const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const tls = zpq.io.tls;
const Connection = tls.ConnectionGen(xev);
const SigV4 = zpq.s3.sigv4.SigV4;

/// Heavily instrumented probe to trace exactly what happens during a TLS upload.
/// Goal: See every async operation, every callback, every state change.
var trace_start: std.time.Instant = undefined;

fn trace(comptime fmt: []const u8, args: anytype) void {
    const now = std.time.Instant.now() catch unreachable;
    const elapsed_ns = now.since(trace_start);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    std.debug.print("[{d:8.2}ms] " ++ fmt ++ "\n", .{elapsed_ms} ++ args);
}

const UploadContext = struct {
    allocator: std.mem.Allocator,
    conn: *Connection,
    request_data: []const u8,
    response_buf: std.ArrayListUnmanaged(u8) = .{},
    done: bool = false,
    err: ?anyerror = null,
    bytes_sent: usize = 0,
    bytes_received: usize = 0,

    fn deinit(self: *UploadContext) void {
        self.response_buf.deinit(self.allocator);
    }
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));
    trace("CALLBACK on_connect: handshake complete, sending {d} bytes", .{ctx.request_data.len});
    trace("  conn state: pending_read={}, pending_write={}, idling={}, closed={}", .{
        ctx.conn.pending_read,
        ctx.conn.pending_write,
        ctx.conn.idling,
        ctx.conn.closed,
    });

    ctx.conn.write(ctx.request_data) catch |err| {
        trace("  ERROR write failed: {}", .{err});
        ctx.err = err;
        ctx.done = true;
        return;
    };
    trace("  write() returned successfully", .{});
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));
    ctx.bytes_received += data.len;
    trace("CALLBACK on_data: received {d} bytes (total: {d})", .{ data.len, ctx.bytes_received });

    ctx.response_buf.appendSlice(ctx.allocator, data) catch |err| {
        trace("  ERROR appendSlice failed: {}", .{err});
        ctx.err = err;
        ctx.done = true;
        return;
    };

    // Check for complete HTTP response
    if (std.mem.indexOf(u8, ctx.response_buf.items, "\r\n\r\n")) |header_end| {
        const headers = ctx.response_buf.items[0..header_end];
        var status: u16 = 0;
        if (headers.len > 12 and std.mem.startsWith(u8, headers, "HTTP/1.1 ")) {
            status = std.fmt.parseInt(u16, headers[9..12], 10) catch 0;
        }

        trace("  Response complete! status={d}, headers_len={d}", .{ status, header_end });
        trace("  Setting done=true, idling=true", .{});
        ctx.done = true;
        ctx.conn.idling = true;

        trace("  conn state after: pending_read={}, pending_write={}, idling={}", .{
            ctx.conn.pending_read,
            ctx.conn.pending_write,
            ctx.conn.idling,
        });
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));
    trace("CALLBACK on_error: {}", .{err});
    trace("  conn state: pending_read={}, pending_write={}, idling={}, closed={}", .{
        ctx.conn.pending_read,
        ctx.conn.pending_write,
        ctx.conn.idling,
        ctx.conn.closed,
    });
    trace("  response_buf_len={d}", .{ctx.response_buf.items.len});

    // EOF after response is ok
    if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.done) {
        trace("  EOF after done, this is expected", .{});
        return;
    }

    if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.response_buf.items.len > 0) {
        if (std.mem.indexOf(u8, ctx.response_buf.items, "\r\n\r\n") != null) {
            trace("  EOF with complete response, marking done", .{});
            ctx.done = true;
            ctx.conn.idling = true;
            return;
        }
    }

    ctx.err = err;
    ctx.done = true;
    ctx.conn.close();
}

fn buildPutRequest(
    allocator: std.mem.Allocator,
    host: []const u8,
    port: u16,
    path: []const u8,
    body: []const u8,
) ![]const u8 {
    // Sign request
    const signer = SigV4{
        .region = "us-east-1",
        .access_key = "rustfsadmin",
        .secret_key = "rustfsadmin",
        .session_token = null,
        .use_unsigned_payload = false,
    };

    var headers = std.ArrayListUnmanaged(std.http.Header){};
    defer {
        for (headers.items) |h| allocator.free(h.value);
        headers.deinit(allocator);
    }

    const url = try std.fmt.allocPrint(allocator, "https://{s}:{d}{s}", .{ host, port, path });
    defer allocator.free(url);

    const uri = try std.Uri.parse(url);
    try signer.sign(allocator, "PUT", uri, &headers, body);

    // Build HTTP request
    var req_buf = std.ArrayListUnmanaged(u8){};
    errdefer req_buf.deinit(allocator);

    try req_buf.appendSlice(allocator, "PUT ");
    try req_buf.appendSlice(allocator, path);
    try req_buf.appendSlice(allocator, " HTTP/1.1\r\n");

    for (headers.items) |h| {
        try req_buf.appendSlice(allocator, h.name);
        try req_buf.appendSlice(allocator, ": ");
        try req_buf.appendSlice(allocator, h.value);
        try req_buf.appendSlice(allocator, "\r\n");
    }

    var len_buf: [20]u8 = undefined;
    const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch unreachable;
    try req_buf.appendSlice(allocator, "Content-Length: ");
    try req_buf.appendSlice(allocator, len_str);
    try req_buf.appendSlice(allocator, "\r\n\r\n");
    try req_buf.appendSlice(allocator, body);

    return req_buf.toOwnedSlice(allocator);
}

pub fn main() !void {
    trace_start = try std.time.Instant.now();

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    trace("=== Traced Upload Probe ===", .{});

    // Setup
    trace("Creating loop...", .{});
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const host = "localhost";
    const port: u16 = 9999;

    trace("Resolving address...", .{});
    const addr = try xev.shim_net.Address.parseIp4("127.0.0.1", port);

    // Create test body - 16KB to require multiple TLS records
    const BODY_SIZE = 16 * 1024;
    trace("Creating {d}KB body...", .{BODY_SIZE / 1024});
    const body = try allocator.alloc(u8, BODY_SIZE);
    defer allocator.free(body);
    @memset(body, 'X');

    trace("Building signed request...", .{});
    const request = try buildPutRequest(allocator, host, port, "/test-bucket/traced_upload.bin", body);
    defer allocator.free(request);
    trace("Request size: {d} bytes (headers + {d} body)", .{ request.len, BODY_SIZE });

    // Create connection
    trace("Creating TLS connection...", .{});
    const conn = try allocator.create(Connection);
    defer allocator.destroy(conn);
    conn.* = try Connection.initWithOptions(&loop, allocator, host, .{});
    defer conn.deinit();

    // Create context
    var ctx = UploadContext{
        .allocator = allocator,
        .conn = conn,
        .request_data = request,
    };
    defer ctx.deinit();

    // Set up callbacks
    conn.user_ctx = &ctx;
    conn.on_connect = onConnect;
    conn.on_data = onData;
    conn.on_error = onError;

    trace("conn initial state: pending_read={}, pending_write={}, idling={}, handshake_complete={}", .{
        conn.pending_read,
        conn.pending_write,
        conn.idling,
        conn.handshake_complete,
    });

    // Connect
    trace("Calling connect()...", .{});
    try conn.connect(addr);
    trace("connect() returned, conn state: pending_read={}, pending_write={}", .{
        conn.pending_read,
        conn.pending_write,
    });

    // Run loop with periodic status
    trace("Starting loop.run(.until_done)...", .{});
    var iterations: usize = 0;
    const max_iterations = 1000;

    while (!ctx.done and ctx.err == null and iterations < max_iterations) {
        iterations += 1;
        if (iterations % 100 == 0) {
            trace("Loop iteration {d}: pending_read={}, pending_write={}, idling={}, done={}", .{
                iterations,
                conn.pending_read,
                conn.pending_write,
                conn.idling,
                ctx.done,
            });
        }

        // Run one iteration
        loop.run(.once) catch |err| {
            trace("loop.run error: {}", .{err});
            break;
        };
    }

    trace("Loop finished after {d} iterations", .{iterations});
    trace("Final state: done={}, err={?}", .{ ctx.done, ctx.err });
    trace("Bytes received: {d}", .{ctx.bytes_received});

    if (ctx.response_buf.items.len > 0) {
        const preview_len = @min(500, ctx.response_buf.items.len);
        trace("Response preview:\n{s}", .{ctx.response_buf.items[0..preview_len]});
    }

    if (ctx.done and ctx.err == null) {
        trace("SUCCESS!", .{});
    } else {
        trace("FAILED", .{});
    }
}
