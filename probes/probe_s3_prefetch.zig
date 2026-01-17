const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const transport = zpq.io.transport;
const protocol = zpq.protocol;

/// Probe to test parallel S3 prefetching with connection pool.
/// Goal: Issue N concurrent range requests and measure throughput.
///
/// V2: Reuse connections via HTTP keep-alive instead of reconnecting each time.
const NUM_CONNECTIONS = 4;
const RANGE_SIZE = 64 * 1024; // 64KB per range request

const RangeRequest = struct {
    offset: u64,
    len: u64,
    buffer: []u8,
    completed: bool = false,
    err: ?anyerror = null,
};

const ConnectionState = enum {
    idle, // Ready for new request (connection established)
    resolving, // DNS lookup in progress
    connecting, // TCP connect in progress
    handshaking, // TLS handshake in progress
    sending, // Sending HTTP request
    reading, // Reading HTTP response
};

/// Each pooled connection holds its own TCP/TLS state and current request.
/// With keep-alive, we reuse the connection across multiple requests.
const PooledConnection = struct {
    const Conn = transport.ConnectionGen(xev);

    pool: *ConnectionPool,
    index: usize,
    conn: ?*Conn = null,
    state: ConnectionState = .idle,
    current_request: ?*RangeRequest = null,
    parser: protocol.http.ResponseParser = .{},
    bytes_received: usize = 0,

    // Track if connection is established (can skip DNS/connect/handshake)
    connection_ready: bool = false,

    fn reset(self: *PooledConnection) void {
        self.state = .idle;
        self.current_request = null;
        self.parser = .{};
        self.bytes_received = 0;
        // Don't reset connection_ready - we want to reuse the connection
    }

    fn startRequest(self: *PooledConnection, request: *RangeRequest) void {
        self.current_request = request;
        self.parser = .{};
        self.bytes_received = 0;

        std.log.info("[Conn {d}] Starting request: offset={d} len={d} reuse={}", .{ self.index, request.offset, request.len, self.connection_ready });

        if (self.connection_ready and self.conn != null) {
            // Reuse existing connection - skip straight to sending
            self.state = .sending;
            // Reset the stopped flag so transport will schedule reads again
            if (self.conn) |c| {
                c.stopped = false;
            }
            self.sendRequest() catch |err| {
                self.completeWithError(err);
            };
        } else {
            // Need to establish new connection
            self.state = .resolving;
            self.pool.resolver.resolve(
                self.pool.loop,
                self.pool.host,
                443,
                self,
                onResolved,
            ) catch |err| {
                self.completeWithError(err);
            };
        }
    }

    fn onResolved(ptr: ?*anyopaque, addr: ?transport.Address) void {
        const self: *PooledConnection = @ptrCast(@alignCast(ptr));

        const address = addr orelse {
            self.completeWithError(error.ResolutionFailed);
            return;
        };

        std.log.info("[Conn {d}] DNS resolved, connecting...", .{self.index});
        self.state = .connecting;

        // Clean up old connection if any
        if (self.conn) |c| c.deinit();
        self.connection_ready = false;

        self.conn = Conn.init(self.pool.allocator, self.pool.loop, true, self.pool.host) catch |err| {
            self.completeWithError(err);
            return;
        };

        const conn = self.conn.?;
        conn.callback_ctx = self;
        conn.on_data = onData;
        conn.on_error = onError;
        conn.on_handshake = onHandshake;

        conn.connect(address) catch |err| {
            self.completeWithError(err);
        };
    }

    fn onHandshake(ptr: ?*anyopaque) void {
        const self: *PooledConnection = @ptrCast(@alignCast(ptr));
        std.log.info("[Conn {d}] TLS handshake complete", .{self.index});
        self.connection_ready = true;
        self.state = .sending;
        self.sendRequest() catch |err| {
            self.completeWithError(err);
        };
    }

    fn sendRequest(self: *PooledConnection) !void {
        const request = self.current_request orelse return error.NoRequest;
        const allocator = self.pool.allocator;

        var req_buf = std.ArrayListUnmanaged(u8){};
        defer req_buf.deinit(allocator);

        const path = try std.fmt.allocPrint(allocator, "/{s}", .{self.pool.key});
        defer allocator.free(path);

        const range_end = request.offset + request.len - 1;
        const signed_headers = try self.pool.s3.formatGetRequest(
            allocator,
            self.pool.key,
            .{ .bytes = .{ .start = request.offset, .end = range_end + 1 } },
            .{},
        );
        defer {
            for (signed_headers) |h| {
                allocator.free(h.name);
                allocator.free(h.value);
            }
            allocator.free(signed_headers);
        }

        try req_buf.appendSlice(allocator, "GET ");
        try req_buf.appendSlice(allocator, path);
        try req_buf.appendSlice(allocator, " HTTP/1.1\r\n");
        for (signed_headers) |h| {
            try req_buf.appendSlice(allocator, h.name);
            try req_buf.appendSlice(allocator, ": ");
            try req_buf.appendSlice(allocator, h.value);
            try req_buf.appendSlice(allocator, "\r\n");
        }
        // Use keep-alive to reuse the connection
        try req_buf.appendSlice(allocator, "Connection: keep-alive\r\n\r\n");

        self.state = .reading;
        if (self.conn) |conn| {
            try conn.write(req_buf.items);
            // After writing, we need to ensure reads are scheduled
            // The transport should auto-schedule reads after write completes
        }
    }

    fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
        const self: *PooledConnection = @ptrCast(@alignCast(ptr));
        const request = self.current_request orelse return;

        const BodyCtx = struct {
            pc: *PooledConnection,
            req: *RangeRequest,

            fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) anyerror!void {
                const bctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
                const pc = bctx.pc;
                const r = bctx.req;

                if (pc.bytes_received + chunk.len > r.buffer.len) {
                    return error.BufferOverflow;
                }
                @memcpy(r.buffer[pc.bytes_received .. pc.bytes_received + chunk.len], chunk);
                pc.bytes_received += chunk.len;
            }
        };
        var bctx = BodyCtx{ .pc = self, .req = request };

        try self.parser.feed(data, &bctx, BodyCtx.onBody);

        if (self.parser.state == .done) {
            std.log.info("[Conn {d}] Response complete: {d} bytes", .{ self.index, self.bytes_received });
            // Don't close - keep alive for reuse
            // But we do need to stop reading until the next request
            if (self.conn) |conn| {
                conn.stopped = true;
            }
            self.completeSuccess();
        }
    }

    fn onError(ptr: ?*anyopaque, err: anyerror) void {
        const self: *PooledConnection = @ptrCast(@alignCast(ptr));
        // Ignore errors if already done
        if (self.current_request) |req| {
            if (req.completed) return;
        }
        if (self.parser.state == .done) return;

        std.log.err("[Conn {d}] Error: {}", .{ self.index, err });
        // Connection error - mark as not ready for reuse
        self.connection_ready = false;
        self.completeWithError(err);
    }

    fn completeSuccess(self: *PooledConnection) void {
        if (self.current_request) |req| {
            req.completed = true;
            req.err = null;
        }
        self.pool.onRequestComplete(self);
    }

    fn completeWithError(self: *PooledConnection, err: anyerror) void {
        if (self.current_request) |req| {
            req.completed = true;
            req.err = err;
        }
        self.pool.onRequestComplete(self);
    }
};

const ConnectionPool = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    resolver: transport.Resolver,
    s3: protocol.s3.S3,
    host: []const u8,
    key: []const u8,

    connections: [NUM_CONNECTIONS]PooledConnection = undefined,

    // Request queue
    pending: std.ArrayListUnmanaged(*RangeRequest) = .{},
    in_flight: usize = 0,
    completed: usize = 0,
    total_requests: usize = 0,

    // Completion signal
    all_done: bool = false,

    fn init(allocator: std.mem.Allocator, loop: *xev.Loop, thread_pool: *xev.ThreadPool, s3_config: protocol.s3.S3, host: []const u8, key: []const u8) ConnectionPool {
        var pool = ConnectionPool{
            .allocator = allocator,
            .loop = loop,
            .resolver = transport.Resolver.init(allocator, thread_pool),
            .s3 = s3_config,
            .host = host,
            .key = key,
        };
        // Initialize connections with back-pointer to pool
        for (0..NUM_CONNECTIONS) |i| {
            pool.connections[i] = PooledConnection{
                .pool = &pool,
                .index = i,
            };
        }
        return pool;
    }

    fn deinit(self: *ConnectionPool) void {
        for (&self.connections) |*pc| {
            if (pc.conn) |c| c.deinit();
        }
        self.pending.deinit(self.allocator);
    }

    fn submit(self: *ConnectionPool, request: *RangeRequest) !void {
        try self.pending.append(self.allocator, request);
        self.total_requests += 1;
        self.tryStartNext();
    }

    fn tryStartNext(self: *ConnectionPool) void {
        // Find idle connection and pending request
        for (&self.connections) |*pc| {
            if (pc.state == .idle and self.pending.items.len > 0) {
                const request = self.pending.orderedRemove(0);
                self.in_flight += 1;
                pc.startRequest(request);
            }
        }
    }

    fn onRequestComplete(self: *ConnectionPool, pc: *PooledConnection) void {
        self.in_flight -= 1;
        self.completed += 1;
        pc.reset();

        std.log.info("Progress: {d}/{d} complete, {d} in-flight, {d} pending", .{
            self.completed,
            self.total_requests,
            self.in_flight,
            self.pending.items.len,
        });

        if (self.completed == self.total_requests and self.pending.items.len == 0) {
            self.all_done = true;
        } else {
            self.tryStartNext();
        }
    }
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Get credentials from environment
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch {
        std.debug.print("Error: AWS_ACCESS_KEY_ID not set\n", .{});
        return;
    };
    defer allocator.free(access_key);

    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch {
        std.debug.print("Error: AWS_SECRET_ACCESS_KEY not set\n", .{});
        return;
    };
    defer allocator.free(secret_key);

    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch try allocator.dupe(u8, "us-west-2");
    defer allocator.free(region);

    const bucket = std.process.getEnvVarOwned(allocator, "AWS_S3_BUCKET") catch try allocator.dupe(u8, "zpq-test-bucket");
    defer allocator.free(bucket);

    const key = "zpq_test_data/benchmark/benchmark_100mb.parquet";

    std.log.info("=== S3 Prefetch Probe (Keep-Alive) ===", .{});
    std.log.info("bucket={s}, key={s}", .{ bucket, key });
    std.log.info("Connection pool size: {d}", .{NUM_CONNECTIONS});
    std.log.info("Range size: {d} KB", .{RANGE_SIZE / 1024});

    // Initialize libxev
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    const s3_config = protocol.s3.S3.init(bucket, region, access_key, secret_key, null);
    const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
    defer allocator.free(host);

    var pool = ConnectionPool.init(allocator, &loop, &thread_pool, s3_config, host, key);
    defer pool.deinit();

    // IMPORTANT: Fix the self-references after init returns
    // Since pool is moved after init(), we need to update the back-pointers
    for (&pool.connections) |*pc| {
        pc.pool = &pool;
    }

    // Create test range requests (160 x 64KB = 10MB total)
    const num_ranges = 160;
    var requests: [num_ranges]RangeRequest = undefined;
    var buffers: [num_ranges][]u8 = undefined;

    for (0..num_ranges) |i| {
        buffers[i] = try allocator.alloc(u8, RANGE_SIZE);
        requests[i] = .{
            .offset = @intCast(i * RANGE_SIZE),
            .len = RANGE_SIZE,
            .buffer = buffers[i],
        };
        try pool.submit(&requests[i]);
    }
    defer for (&buffers) |buf| allocator.free(buf);

    std.log.info("Submitted {d} range requests ({d} KB total)", .{ num_ranges, num_ranges * RANGE_SIZE / 1024 });

    // Run event loop until all done
    const start = try std.time.Instant.now();
    while (!pool.all_done) {
        try loop.run(.once);
    }
    const end = try std.time.Instant.now();
    const elapsed_ns = end.since(start);

    // Count successes
    var successes: usize = 0;
    var total_bytes: usize = 0;
    for (&requests) |*req| {
        if (req.completed and req.err == null) {
            successes += 1;
            total_bytes += req.len;
        }
    }

    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const throughput_mbps = @as(f64, @floatFromInt(total_bytes)) / elapsed_ms / 1000.0;

    std.log.info("=== Results ===", .{});
    std.log.info("Completed: {d}/{d} requests", .{ successes, num_ranges });
    std.log.info("Total bytes: {d} KB", .{total_bytes / 1024});
    std.log.info("Elapsed: {d:.2} ms", .{elapsed_ms});
    std.log.info("Throughput: {d:.2} MB/s", .{throughput_mbps});
}
