/// Probe to diagnose the async S3 HEAD request hang.
/// This isolates the fetchSize() path that hangs on large files.
///
/// Usage: zig build probe-s3-head-hang
/// Requires: AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, AWS_REGION env vars
const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const dns = zpq.s3.dns;
const Connection = zpq.s3.Connection;
const AsyncRequest = zpq.s3.AsyncRequest;
const types = zpq.s3.types;

const Instant = std.time.Instant;

const HOST = "s3.us-west-2.amazonaws.com";
const PORT: u16 = 443;
const BUCKET = "skyway-diat-staging-data";

// Small file that works
const KEY_SMALL = "test_data/valid/sizes/small/10k_rows.parquet";
// Large file that hangs
const KEY_LARGE = "raw/cccis-duckbill/skyway/skyway-export/data/BILLING_PERIOD=2025-02/skyway-export-00160.snappy.parquet";

const TIMEOUT_NS: u64 = 5_000_000_000; // 5 seconds in nanoseconds

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Load credentials from environment
    const access_key = std.posix.getenv("AWS_ACCESS_KEY_ID") orelse {
        std.debug.print("ERROR: AWS_ACCESS_KEY_ID not set\n", .{});
        return error.MissingCredentials;
    };
    const secret_key = std.posix.getenv("AWS_SECRET_ACCESS_KEY") orelse {
        std.debug.print("ERROR: AWS_SECRET_ACCESS_KEY not set\n", .{});
        return error.MissingCredentials;
    };
    const session_token = std.posix.getenv("AWS_SESSION_TOKEN");
    const region = std.posix.getenv("AWS_REGION") orelse "us-west-2";

    const config = types.S3Config{
        .credentials = types.Credentials{
            .access_key = access_key,
            .secret_key = secret_key,
            .session_token = session_token,
        },
        .region = region,
        .endpoint = null,
    };

    std.debug.print("=== S3 HEAD Request Hang Probe ===\n", .{});
    std.debug.print("Host: {s}:{d}\n", .{ HOST, PORT });
    std.debug.print("Timeout: 5000ms\n\n", .{});

    // Test 1: Small file (should work)
    std.debug.print("--- Test 1: Small file ---\n", .{});
    std.debug.print("Key: {s}\n", .{KEY_SMALL});
    const result1 = testHeadRequest(allocator, KEY_SMALL, config);
    if (result1) |size| {
        std.debug.print("SUCCESS: Content-Length = {d} bytes\n\n", .{size});
    } else |err| {
        std.debug.print("FAILED: {}\n\n", .{err});
    }

    // Test 2: Large file (expected to hang/timeout)
    std.debug.print("--- Test 2: Large file ---\n", .{});
    std.debug.print("Key: {s}\n", .{KEY_LARGE});
    const result2 = testHeadRequest(allocator, KEY_LARGE, config);
    if (result2) |size| {
        std.debug.print("SUCCESS: Content-Length = {d} bytes\n\n", .{size});
    } else |err| {
        std.debug.print("FAILED: {}\n\n", .{err});
    }

    std.debug.print("=== Probe Complete ===\n", .{});
}

fn elapsedMs(start: Instant) u64 {
    const now = Instant.now() catch return 0;
    return now.since(start) / std.time.ns_per_ms;
}

fn testHeadRequest(allocator: std.mem.Allocator, key: []const u8, config: types.S3Config) !u64 {
    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        std.debug.print("[defer] Shutdown thread_pool...\n", .{});
        thread_pool.shutdown();
        std.debug.print("[defer] Deinit thread_pool...\n", .{});
        thread_pool.deinit();
        std.debug.print("[defer] thread_pool done\n", .{});
    }

    var loop = try xev.Loop.init(.{});
    defer {
        std.debug.print("[defer] Deinit loop...\n", .{});
        loop.deinit();
        std.debug.print("[defer] loop done\n", .{});
    }

    // Step 1: DNS Resolution
    std.debug.print("[1/4] Resolving DNS for {s}...\n", .{HOST});
    const start_dns = try Instant.now();

    var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
    defer tp_resolver.deinit();

    var sf_resolver = dns.SingleFlightResolver.init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    const resolver = sf_resolver.resolver();

    const DnsCtx = struct {
        results: []dns.Address = &.{},
        err: ?anyerror = null,
        done: bool = false,
        completion: dns.Resolver.Completion = .{},

        fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
            const ctx: *@This() = @ptrCast(@alignCast(ud));
            err catch |e| {
                ctx.err = e;
                ctx.done = true;
                return;
            };
            ctx.results = @constCast(results);
            ctx.done = true;
        }
    };

    var dns_ctx = DnsCtx{};
    resolver.resolve(&loop, HOST, PORT, &dns_ctx.completion, DnsCtx.callback, &dns_ctx);

    var tick_count: usize = 0;
    while (!dns_ctx.done) {
        _ = try loop.run(.once);
        tick_count += 1;
        const now = Instant.now() catch continue;
        if (now.since(start_dns) > TIMEOUT_NS) {
            std.debug.print("DNS TIMEOUT after {d}ms ({d} ticks)\n", .{ elapsedMs(start_dns), tick_count });
            return error.DnsTimeout;
        }
    }

    if (dns_ctx.err) |err| {
        std.debug.print("DNS ERROR: {}\n", .{err});
        return err;
    }

    std.debug.print("[1/4] DNS resolved in {d}ms ({d} addresses, {d} ticks)\n", .{ elapsedMs(start_dns), dns_ctx.results.len, tick_count });

    // Step 2: TCP + TLS Connection
    std.debug.print("[2/4] Connecting TCP + TLS...\n", .{});
    const start_conn = try Instant.now();

    const conn = try Connection.init(&loop, allocator, HOST, true);
    errdefer conn.deinit();

    const ConnCtx = struct {
        connected: bool = false,
        err: ?anyerror = null,

        fn onConnect(c: *Connection, ctx: ?*anyopaque) void {
            _ = c;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.connected = true;
        }

        fn onError(c: *Connection, ctx: ?*anyopaque, err: anyerror) void {
            _ = c;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.err = err;
        }
    };

    var conn_ctx = ConnCtx{};
    conn.user_ctx = &conn_ctx;
    conn.on_connect = ConnCtx.onConnect;
    conn.on_error = ConnCtx.onError;

    try conn.connect(dns_ctx.results[0]);

    tick_count = 0;
    while (!conn_ctx.connected and conn_ctx.err == null) {
        _ = try loop.run(.once);
        tick_count += 1;
        const now = Instant.now() catch continue;
        if (now.since(start_conn) > TIMEOUT_NS) {
            std.debug.print("CONNECT TIMEOUT after {d}ms ({d} ticks)\n", .{ elapsedMs(start_conn), tick_count });
            return error.ConnectTimeout;
        }
    }

    if (conn_ctx.err) |err| {
        std.debug.print("CONNECT ERROR: {}\n", .{err});
        return err;
    }

    std.debug.print("[2/4] Connected in {d}ms ({d} ticks)\n", .{ elapsedMs(start_conn), tick_count });

    // Step 3: Send HEAD request
    std.debug.print("[3/4] Sending HEAD request...\n", .{});
    const start_req = try Instant.now();

    const path = try std.fmt.allocPrint(allocator, "/{s}/{s}", .{ BUCKET, key });
    defer allocator.free(path);

    std.debug.print("  [DEBUG] Request path: {s}\n", .{path});

    var req = AsyncRequest.init(allocator);
    defer req.deinit();

    try req.prepareHead(HOST, PORT, path, true, config);

    const ReqCtx = struct {
        done: bool = false,
        fn onDone(ctx: ?*anyopaque, r: *AsyncRequest) void {
            _ = r;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.done = true;
        }
    };

    var req_ctx = ReqCtx{};
    req.done_ctx = &req_ctx;
    req.on_done = ReqCtx.onDone;

    try req.execute(conn);

    tick_count = 0;
    while (!req_ctx.done) {
        _ = try loop.run(.once);
        tick_count += 1;
        const now = Instant.now() catch continue;
        if (now.since(start_req) > TIMEOUT_NS) {
            std.debug.print("REQUEST TIMEOUT after {d}ms ({d} ticks)\n", .{ elapsedMs(start_req), tick_count });
            std.debug.print("  Request state: {}\n", .{req.state});
            std.debug.print("  Connection handshake_complete: {}\n", .{conn.handshake_complete});
            std.debug.print("  Write queue len: {d}\n", .{conn.write_queue.items.len});
            std.debug.print("  Write in flight: {}\n", .{conn.write_in_flight});
            std.debug.print("  Read in flight: {}\n", .{conn.read_in_flight});
            return error.RequestTimeout;
        }

        // Progress reporting every 100 ticks
        if (tick_count % 100 == 0) {
            std.debug.print("  ... {d} ticks, state={}, elapsed={d}ms\n", .{ tick_count, req.state, elapsedMs(start_req) });
        }
    }

    std.debug.print("[3/4] Request completed in {d}ms ({d} ticks)\n", .{ elapsedMs(start_req), tick_count });

    // Step 4: Check result
    std.debug.print("[4/4] Checking response...\n", .{});

    if (req.state == .Finished) {
        const content_len = req.content_length;
        std.debug.print("[4/4] Content-Length: {d}\n", .{content_len});
        std.debug.print("[cleanup] Cleaning up connection...\n", .{});
        conn.deinit();
        std.debug.print("[cleanup] Cleaning up DNS completion...\n", .{});
        dns_ctx.completion.deinit(allocator);
        std.debug.print("[cleanup] Done\n", .{});
        return content_len;
    } else {
        std.debug.print("[4/4] Request failed with state: {}\n", .{req.state});
        conn.deinit();
        dns_ctx.completion.deinit(allocator);
        return error.HeadRequestFailed;
    }
}
