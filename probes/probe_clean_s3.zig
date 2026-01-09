const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const transport = zpq.io.transport;
const protocol = zpq.protocol;

/// Integration probe for the new ZPQ S3 stack.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Get credentials from environment
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch |err| {
        std.debug.print("Error: AWS_ACCESS_KEY_ID not set: {}\n", .{err});
        return;
    };
    defer allocator.free(access_key);

    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch |err| {
        std.debug.print("Error: AWS_SECRET_ACCESS_KEY not set: {}\n", .{err});
        return;
    };
    defer allocator.free(secret_key);

    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch try allocator.dupe(u8, "us-west-2");
    defer allocator.free(region);

    const bucket = std.process.getEnvVarOwned(allocator, "AWS_S3_BUCKET") catch
        std.process.getEnvVarOwned(allocator, "TEST_BUCKET") catch
        try allocator.dupe(u8, "zpq-test-bucket");
    defer allocator.free(bucket);
    const key = std.process.getEnvVarOwned(allocator, "TEST_KEY") catch try allocator.dupe(u8, "zpq_test_data/benchmark/benchmark_100mb.parquet");
    defer allocator.free(key);

    std.log.info("Starting S3 Probe: bucket={s}, key={s}, region={s}", .{ bucket, key, region });

    // 2. Initialize libxev
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    const resolver = transport.Resolver.init(allocator, &thread_pool);
    const s3 = protocol.s3.S3.init(bucket, region, access_key, secret_key, null);

    const Context = struct {
        const Step = enum { put, head, done };
        const Conn = transport.ConnectionGen(xev);

        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        resolver: transport.Resolver,
        s3: protocol.s3.S3,
        key: []const u8,
        host: []const u8,
        conn: ?*Conn = null,
        parser: protocol.http.ResponseParser = .{ .is_head = true },
        step: Step = .put,
        finished: bool = false,

        fn start(ctx: *@This()) !void {
            try ctx.resolver.resolve(ctx.loop, ctx.host, 443, ctx, @ptrCast(&onResolved));
        }

        fn onResolved(ptr: ?*anyopaque, addr: ?transport.Address) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            const address = addr orelse {
                std.log.err("Resolution failed", .{});
                ctx.finished = true;
                return;
            };

            if (ctx.conn) |c| c.deinit();
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

        fn onHandshake(ptr: ?*anyopaque) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            ctx.performStep() catch |err| {
                std.log.err("Failed to perform step {s}: {}", .{ @tagName(ctx.step), err });
                ctx.finished = true;
            };
        }

        fn performStep(ctx: *@This()) !void {
            ctx.parser.reset();
            const path = try std.fmt.allocPrint(ctx.allocator, "/{s}", .{ctx.key});
            defer ctx.allocator.free(path);

            var req_buf = std.ArrayListUnmanaged(u8){};
            defer req_buf.deinit(ctx.allocator);

            switch (ctx.step) {
                .put => {
                    const test_data = "Hello from ZPQ Clean Slate!";
                    const signed_headers = try ctx.s3.formatPutRequest(ctx.allocator, ctx.key, test_data, .{});
                    defer {
                        for (signed_headers) |h| {
                            ctx.allocator.free(h.name);
                            ctx.allocator.free(h.value);
                        }
                        ctx.allocator.free(signed_headers);
                    }
                    try req_buf.appendSlice(ctx.allocator, "PUT ");
                    try req_buf.appendSlice(ctx.allocator, path);
                    try req_buf.appendSlice(ctx.allocator, " HTTP/1.1\r\n");
                    for (signed_headers) |h| {
                        try req_buf.appendSlice(ctx.allocator, h.name);
                        try req_buf.appendSlice(ctx.allocator, ": ");
                        try req_buf.appendSlice(ctx.allocator, h.value);
                        try req_buf.appendSlice(ctx.allocator, "\r\n");
                    }
                    var cl_buf: [32]u8 = undefined;
                    const cl_str = try std.fmt.bufPrint(&cl_buf, "Content-Length: {d}\r\n", .{test_data.len});
                    try req_buf.appendSlice(ctx.allocator, cl_str);
                    try req_buf.appendSlice(ctx.allocator, "Connection: close\r\n\r\n");
                    try req_buf.appendSlice(ctx.allocator, test_data);
                },
                .head => {
                    const signed_headers = try ctx.s3.formatHeadRequest(ctx.allocator, ctx.key, .{});
                    defer {
                        for (signed_headers) |h| {
                            ctx.allocator.free(h.name);
                            ctx.allocator.free(h.value);
                        }
                        ctx.allocator.free(signed_headers);
                    }
                    try req_buf.appendSlice(ctx.allocator, "HEAD ");
                    try req_buf.appendSlice(ctx.allocator, path);
                    try req_buf.appendSlice(ctx.allocator, " HTTP/1.1\r\n");
                    for (signed_headers) |h| {
                        try req_buf.appendSlice(ctx.allocator, h.name);
                        try req_buf.appendSlice(ctx.allocator, ": ");
                        try req_buf.appendSlice(ctx.allocator, h.value);
                        try req_buf.appendSlice(ctx.allocator, "\r\n");
                    }
                    try req_buf.appendSlice(ctx.allocator, "Connection: close\r\n\r\n");
                },
                .done => unreachable,
            }

            try ctx.conn.?.write(req_buf.items);
        }

        fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            const BodyCtx = struct {
                fn onBody(_: *anyopaque, _: []const u8) anyerror!void {}
            };
            var bctx = BodyCtx{};
            try ctx.parser.feed(data, &bctx, BodyCtx.onBody);

            if (ctx.parser.state == .done) {
                std.log.info("Response received: Status {d}", .{ctx.parser.status_code});
                if (ctx.parser.status_code >= 400) {
                    ctx.finished = true;
                    return;
                }
                switch (ctx.step) {
                    .put => ctx.step = .head,
                    .head => {
                        ctx.finished = true;
                        return;
                    },
                    .done => {},
                }
                try ctx.start();
            }
        }

        fn onError(ptr: ?*anyopaque, err: anyerror) void {
            const ctx: *@This() = @ptrCast(@alignCast(ptr));
            if (ctx.finished) return;
            if (err == error.TlsConnectionClosed and ctx.parser.state == .done) return;
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
        .key = "zpq_test_data/lifecycle_test.txt",
        .host = host,
    };

    try context.start();

    while (!context.finished) {
        try loop.run(.once);
    }

    if (context.conn) |c| c.deinit();
    std.log.info("Probe finished.", .{});
}
