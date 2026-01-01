const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const tls = zpq.io.tls;
const Connection = tls.ConnectionGen(xev);
const GlobalConnectionPool = zpq.s3.GlobalConnectionPool(xev);
const ConnectionKey = zpq.s3.global_pool.ConnectionKey;

const log = std.log.scoped(.probe_parallel);

/// Minimal probe to test parallel TLS writes using xev.
/// Goal: Understand how to do N parallel HTTP requests (uploads) with xev.
///
/// Key learnings from Orchestrator:
/// 1. Set up all connections with callbacks
/// 2. Kick them all off (connect)
/// 3. Run loop.run(.until_done) - handles all connections in parallel
/// 4. Check results after loop completes
const RequestContext = struct {
    allocator: std.mem.Allocator,
    id: usize,
    conn: *Connection,
    pool: *GlobalConnectionPool,
    key: ConnectionKey,
    request_data: []const u8,
    response_buf: std.ArrayListUnmanaged(u8) = .{},
    done: bool = false,
    err: ?anyerror = null,
    status_code: u16 = 0,
    start_time: std.time.Instant,

    fn deinit(self: *RequestContext) void {
        self.response_buf.deinit(self.allocator);
    }
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    log.info("[{d}] Connected, sending request ({d} bytes)", .{ ctx.id, ctx.request_data.len });

    ctx.conn.write(ctx.request_data) catch |err| {
        log.err("[{d}] Write failed: {}", .{ ctx.id, err });
        ctx.err = err;
        ctx.done = true;
        return;
    };
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
    log.debug("[{d}] Received {d} bytes", .{ ctx.id, data.len });

    ctx.response_buf.appendSlice(ctx.allocator, data) catch |err| {
        ctx.err = err;
        ctx.done = true;
        return;
    };

    // Simple check for HTTP response complete (look for end of headers + some body)
    // For a real implementation, use ResponseParser
    if (std.mem.indexOf(u8, ctx.response_buf.items, "\r\n\r\n")) |header_end| {
        // Check if we have Content-Length and received full body
        const headers = ctx.response_buf.items[0..header_end];

        // Extract status code
        if (headers.len > 12) {
            if (std.mem.startsWith(u8, headers, "HTTP/1.1 ")) {
                ctx.status_code = std.fmt.parseInt(u16, headers[9..12], 10) catch 0;
            }
        }

        // For simplicity, mark done after receiving headers + some data
        // A real impl would parse Content-Length
        if (ctx.response_buf.items.len > header_end + 100 or ctx.status_code >= 400) {
            const elapsed = (std.time.Instant.now() catch unreachable).since(ctx.start_time);
            const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
            log.info("[{d}] Complete: status={d}, {d:.1}ms", .{ ctx.id, ctx.status_code, elapsed_ms });
            ctx.done = true;

            // Return connection to pool
            ctx.conn.user_ctx = null;
            ctx.conn.idling = true;
            ctx.pool.release(ctx.key, ctx.conn);
        }
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));

    // EOF after response is ok
    if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.response_buf.items.len > 0) {
        const elapsed = (std.time.Instant.now() catch unreachable).since(ctx.start_time);
        const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
        log.info("[{d}] EOF (ok): {d} bytes received, {d:.1}ms", .{ ctx.id, ctx.response_buf.items.len, elapsed_ms });
        ctx.done = true;
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.pool.release(ctx.key, ctx.conn);
        return;
    }

    log.err("[{d}] Error: {}", .{ ctx.id, err });
    ctx.err = err;
    ctx.done = true;
    ctx.conn.close();
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== Parallel Write Probe ===\n", .{});
    std.debug.print("Testing N parallel HTTP requests to rustfs\n\n", .{});

    // Setup
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var pool = GlobalConnectionPool.init(allocator);
    defer pool.deinit();

    const host = "localhost";
    const port: u16 = 9999;
    const key = ConnectionKey{ .host = host, .port = port, .use_tls = true };

    // Resolve DNS once - use xev's address type directly
    const xev_addr = try xev.shim_net.Address.parseIp4("127.0.0.1", port);

    // Number of parallel requests
    const N = 3;
    std.debug.print("Starting {d} parallel requests...\n\n", .{N});

    // Create contexts for each request
    var contexts: [N]*RequestContext = undefined;
    const overall_start = try std.time.Instant.now();

    for (0..N) |i| {
        // Build a simple HTTP GET request (we'll use HEAD for speed)
        const request = try std.fmt.allocPrint(allocator,
            \\HEAD /test-bucket/test.txt HTTP/1.1
            \\Host: {s}:{d}
            \\Connection: keep-alive
            \\
            \\
        , .{ host, port });

        // Create connection
        const conn = try allocator.create(Connection);
        conn.* = try Connection.initWithOptions(&loop, allocator, host, .{});

        // Create context
        const ctx = try allocator.create(RequestContext);
        ctx.* = .{
            .allocator = allocator,
            .id = i,
            .conn = conn,
            .pool = &pool,
            .key = key,
            .request_data = request,
            .start_time = try std.time.Instant.now(),
        };
        contexts[i] = ctx;

        // Set up callbacks
        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;

        // Kick off connection (non-blocking)
        log.info("[{d}] Starting connection...", .{i});
        try conn.connect(xev_addr);
    }

    // Run the event loop until all connections complete
    std.debug.print("\nRunning event loop (.until_done)...\n", .{});
    try loop.run(.until_done);

    const overall_elapsed = (try std.time.Instant.now()).since(overall_start);
    const overall_ms = @as(f64, @floatFromInt(overall_elapsed)) / 1_000_000.0;

    // Check results
    std.debug.print("\n=== Results ===\n", .{});
    var success_count: usize = 0;
    var error_count: usize = 0;

    for (contexts) |ctx| {
        if (ctx.err) |err| {
            std.debug.print("[{d}] FAILED: {}\n", .{ ctx.id, err });
            error_count += 1;
        } else {
            std.debug.print("[{d}] OK: status={d}, response_size={d}\n", .{ ctx.id, ctx.status_code, ctx.response_buf.items.len });
            success_count += 1;
        }

        // Cleanup
        allocator.free(ctx.request_data);
        ctx.deinit();
        allocator.destroy(ctx);
    }

    std.debug.print("\nTotal: {d} success, {d} errors\n", .{ success_count, error_count });
    std.debug.print("Overall time: {d:.1}ms for {d} parallel requests\n", .{ overall_ms, N });

    if (success_count == N) {
        std.debug.print("\n✓ Parallel connections work!\n", .{});
    } else {
        std.debug.print("\n✗ Some requests failed\n", .{});
    }
}
