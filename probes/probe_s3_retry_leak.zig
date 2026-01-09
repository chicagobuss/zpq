const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const transport = zpq.io.transport;
const protocol = zpq.protocol;

/// This probe stresses S3 connection reuse and memory stability.
/// It performs multiple HEAD/GET requests in a loop using the same Context structure
/// but re-initializing connections to verify proper cleanup.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer {
        const status = gpa.deinit();
        if (status == .leak) std.debug.print("GPA detected leaks!\n", .{});
    }
    const allocator = gpa.allocator();

    // 1. Get credentials from environment
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch return error.NoAccessKey;
    defer allocator.free(access_key);

    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch return error.NoSecretKey;
    defer allocator.free(secret_key);

    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch try allocator.dupe(u8, "us-west-2");
    defer allocator.free(region);

    const bucket = std.process.getEnvVarOwned(allocator, "AWS_S3_BUCKET") catch
        std.process.getEnvVarOwned(allocator, "TEST_BUCKET") catch
        try allocator.dupe(u8, "zpq-test-bucket");
    defer allocator.free(bucket);

    // Use a known existing file. benchmark_100mb.parquet is standard.
    const key = std.process.getEnvVarOwned(allocator, "TEST_KEY") catch try allocator.dupe(u8, "zpq_test_data/benchmark/benchmark_100mb.parquet");
    defer allocator.free(key);

    std.log.info("Starting S3 Stress Probe: bucket={s}, key={s}, region={s}", .{ bucket, key, region });

    // 2. Initialize libxev
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    const resolver = transport.Resolver.init(allocator, &thread_pool);
    const s3 = protocol.s3.S3.init(bucket, region, access_key, secret_key, null);

    const Context = struct {
        const Conn = transport.ConnectionGen(xev);

        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        resolver: transport.Resolver,
        s3: protocol.s3.S3,
        key: []const u8,
        host: []const u8,
        conn: ?*Conn = null,
        parser: protocol.http.ResponseParser = .{ .is_head = true },

        requests_remaining: usize = 10,
        finished: bool = false,

        fn start(ctx: *@This()) !void {
            if (ctx.conn) |c| {
                if (!c.closed) {
                    std.log.info("Reuse connection...", .{});
                    c.callback_ctx = ctx;
                    c.on_data = onData;
                    c.on_error = onError;
                    c.on_handshake = onHandshake;

                    onHandshake(@ptrCast(ctx));
                    return;
                }
                c.deinit();
                ctx.conn = null;
            }

            try ctx.resolver.resolve(ctx.loop, ctx.host, 443, ctx, @ptrCast(&onResolved));
        }

        fn onResolved(ptr: ?*anyopaque, addr: ?transport.Address) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            const address = addr orelse {
                std.log.err("Resolution failed", .{});
                ctx.finished = true;
                return;
            };

            ctx.conn = Conn.init(ctx.allocator, ctx.loop, true, ctx.host) catch |e| {
                std.log.err("Connection init error: {}", .{e});
                ctx.finished = true;
                return;
            };
            ctx.conn.?.callback_ctx = ctx;
            ctx.conn.?.on_data = onData;
            ctx.conn.?.on_error = onError;
            ctx.conn.?.on_handshake = onHandshake;

            ctx.conn.?.connect(address) catch |e| {
                std.log.err("Connect error: {}", .{e});
                ctx.finished = true;
            };
        }

        fn onHandshakeCommon(ptr: ?*anyopaque) !void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            ctx.parser.reset();

            // Format HEAD request
            const signed_headers = try ctx.s3.formatHeadRequest(ctx.allocator, ctx.key, .{});
            // Cleanup headers manually to verify no leaks in calling code (the leak was inside formatHeadRequest, but this is good hygiene)
            defer {
                for (signed_headers) |h| {
                    ctx.allocator.free(h.name);
                    ctx.allocator.free(h.value);
                }
                ctx.allocator.free(signed_headers);
            }

            var req_buf = std.ArrayListUnmanaged(u8){};
            defer req_buf.deinit(ctx.allocator);

            const path = try std.fmt.allocPrint(ctx.allocator, "/{s}", .{ctx.key});
            defer ctx.allocator.free(path);

            try req_buf.appendSlice(ctx.allocator, "HEAD ");
            try req_buf.appendSlice(ctx.allocator, path);
            try req_buf.appendSlice(ctx.allocator, " HTTP/1.1\r\n");
            for (signed_headers) |h| {
                try req_buf.appendSlice(ctx.allocator, h.name);
                try req_buf.appendSlice(ctx.allocator, ": ");
                try req_buf.appendSlice(ctx.allocator, h.value);
                try req_buf.appendSlice(ctx.allocator, "\r\n");
            }
            // Keep-Alive to test reuse
            try req_buf.appendSlice(ctx.allocator, "Connection: keep-alive\r\n\r\n");

            try ctx.conn.?.write(req_buf.items);
        }

        fn onHandshake(ptr: ?*anyopaque) void {
            onHandshakeCommon(ptr) catch |e| {
                onError(ptr, e);
            };
        }

        fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            const BodyCtx = struct {
                fn onBody(_: *anyopaque, _: []const u8) anyerror!void {}
            };
            var bctx = BodyCtx{};
            try ctx.parser.feed(data, &bctx, BodyCtx.onBody);

            if (ctx.parser.state == .done) {
                // Stop further reads on this connection
                if (ctx.conn) |conn| conn.stopped = true;

                if (ctx.parser.status_code >= 400) {
                    std.log.err("Request failed: {d}", .{ctx.parser.status_code});
                    ctx.finished = true;
                    return;
                }

                ctx.requests_remaining -= 1;
                std.log.info("Request complete. Remaining: {d}", .{ctx.requests_remaining});

                if (ctx.requests_remaining == 0) {
                    ctx.finished = true;
                    return;
                }

                // Next request
                try ctx.start();
            }
        }

        fn onError(ptr: ?*anyopaque, err: anyerror) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            if (ctx.finished) return;
            std.log.err("Error encountered: {s}", .{@errorName(err)});

            // If EOF/Reset, try to destruct and restart (logic similar to s3.zig)
            if (err == error.EndOfStream or err == error.ConnectionReset or err == error.BrokenPipe) {
                if (ctx.conn) |c| {
                    c.deinit();
                    ctx.conn = null;
                }
                ctx.start() catch |e| {
                    std.log.err("Critical restart error: {s}", .{@errorName(e)});
                    ctx.finished = true;
                };
                return;
            }

            ctx.finished = true;
        }
    };

    const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
    defer allocator.free(host);

    var context = Context{
        .allocator = allocator,
        .loop = &loop,
        .resolver = resolver,
        .s3 = s3,
        .key = key,
        .host = host,
    };

    try context.start();

    while (!context.finished) {
        try loop.run(.once);
    }

    if (context.conn) |c| c.deinit();
    std.log.info("Probe finished successfully.", .{});
}
