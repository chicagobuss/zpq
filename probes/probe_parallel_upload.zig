const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

const tls = zpq.io.tls;
const Connection = tls.ConnectionGen(xev);
const GlobalConnectionPool = zpq.s3.GlobalConnectionPool(xev);
const ConnectionKey = zpq.s3.global_pool.ConnectionKey;
const SigV4 = zpq.s3.sigv4.SigV4;

const log = std.log.scoped(.probe_upload);

/// Probe to test parallel PUT uploads with body data.
/// This simulates what S3Writer's parallel multipart upload needs to do.
///
/// Key pattern from Orchestrator:
/// 1. Set up all connections with callbacks
/// 2. Kick them all off (connect) - non-blocking
/// 3. Run loop.run(.until_done) - processes all in parallel
/// 4. Check results after loop completes
/// Maximum chunk size for writes to avoid TLS buffer issues.
/// Testing shows uploads fail above ~130KB due to TLS not interleaving reads.
/// Set to 0 to disable chunking (send entire request in one write).
const WRITE_CHUNK_SIZE = 0; // Disabled - test single write

const UploadContext = struct {
    allocator: std.mem.Allocator,
    id: usize,
    conn: *Connection,
    pool: *GlobalConnectionPool,
    key: ConnectionKey,
    request_data: []const u8, // Owned - headers + body combined
    write_offset: usize = 0, // Track chunked write progress
    write_started: bool = false, // Guard against callbacks during handshake
    response_buf: std.ArrayListUnmanaged(u8) = .{},
    done: bool = false,
    err: ?anyerror = null,
    status_code: u16 = 0,
    etag: ?[]const u8 = null,
    start_time: std.time.Instant,
    body_size: usize,

    fn deinit(self: *UploadContext) void {
        self.response_buf.deinit(self.allocator);
        if (self.etag) |e| self.allocator.free(e);
    }

    /// Write the next chunk of data. Returns true if more data to write.
    /// Returns false if done, nothing to write, or write not possible yet.
    fn writeNextChunk(self: *UploadContext) bool {
        if (self.done) return false;
        if (self.write_offset >= self.request_data.len) return false;

        // KEY: Check if a write is already in progress
        // This handles the case where on_connect fires while handshake write is pending
        if (self.conn.pending_write) {
            log.debug("[{d}] Write pending, deferring chunk", .{self.id});
            return true; // Still have data to write, will retry on next callback
        }

        const remaining = self.request_data.len - self.write_offset;
        const chunk_size = @min(remaining, WRITE_CHUNK_SIZE);
        const chunk = self.request_data[self.write_offset .. self.write_offset + chunk_size];

        log.debug("[{d}] Writing chunk: offset={d}, size={d}, remaining={d}", .{
            self.id,
            self.write_offset,
            chunk_size,
            remaining - chunk_size,
        });

        self.conn.write(chunk) catch |err| {
            log.err("[{d}] Chunk write failed: {}", .{ self.id, err });
            self.err = err;
            self.done = true;
            return false;
        };

        self.write_offset += chunk_size;
        return self.write_offset < self.request_data.len;
    }
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));
    log.info("[{d}] Connected, sending {d} bytes ({d} body) in {d}KB chunks, pending_write={}", .{
        ctx.id,
        ctx.request_data.len,
        ctx.body_size,
        WRITE_CHUNK_SIZE / 1024,
        ctx.conn.pending_write,
    });

    // Mark that we're starting application data writes
    ctx.write_started = true;

    // Start chunked write - first chunk (may defer if pending_write=true)
    const has_more = ctx.writeNextChunk();
    log.info("[{d}] After first writeNextChunk: has_more={}, offset={}, pending_write={}", .{
        ctx.id,
        has_more,
        ctx.write_offset,
        ctx.conn.pending_write,
    });
}

fn onWriteComplete(ctx_void: ?*anyopaque, bytes_written: usize) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));

    // Ignore callbacks during TLS handshake (before onConnect is called)
    if (!ctx.write_started) {
        log.debug("[{d}] onWriteComplete (handshake): {d} bytes", .{ ctx.id, bytes_written });
        return;
    }

    log.info("[{d}] onWriteComplete: {d} bytes, offset={d}/{d}, pending_write={}", .{
        ctx.id,
        bytes_written,
        ctx.write_offset,
        ctx.request_data.len,
        ctx.conn.pending_write,
    });

    // Continue with next chunk if there's more data
    const has_more = ctx.writeNextChunk();
    log.debug("[{d}] After writeNextChunk: has_more={}", .{ ctx.id, has_more });
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));
    log.debug("[{d}] Received {d} bytes", .{ ctx.id, data.len });

    ctx.response_buf.appendSlice(ctx.allocator, data) catch |err| {
        ctx.err = err;
        ctx.done = true;
        return;
    };

    // Check for HTTP response complete
    if (std.mem.indexOf(u8, ctx.response_buf.items, "\r\n\r\n")) |header_end| {
        const headers = ctx.response_buf.items[0..header_end];

        // Extract status code
        if (headers.len > 12 and std.mem.startsWith(u8, headers, "HTTP/1.1 ")) {
            ctx.status_code = std.fmt.parseInt(u16, headers[9..12], 10) catch 0;
        }

        // Extract ETag
        var lines = std.mem.splitSequence(u8, headers, "\r\n");
        while (lines.next()) |line| {
            if (std.ascii.startsWithIgnoreCase(line, "etag:")) {
                const value = std.mem.trimStart(u8, line[5..], " ");
                ctx.etag = ctx.allocator.dupe(u8, value) catch null;
                break;
            }
        }

        // Mark done
        const elapsed = (std.time.Instant.now() catch unreachable).since(ctx.start_time);
        const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
        const throughput = (@as(f64, @floatFromInt(ctx.body_size)) / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0);

        log.info("[{d}] Complete: status={d}, etag={s}, {d:.1}ms, {d:.1} MB/s", .{
            ctx.id,
            ctx.status_code,
            ctx.etag orelse "none",
            elapsed_ms,
            throughput,
        });
        ctx.done = true;

        // Return connection to pool
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.pool.release(ctx.key, ctx.conn);
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *UploadContext = @ptrCast(@alignCast(ctx_void));

    // Log any partial response we received
    if (ctx.response_buf.items.len > 0) {
        log.info("[{d}] Partial response before error ({d} bytes): {s}", .{
            ctx.id,
            ctx.response_buf.items.len,
            ctx.response_buf.items[0..@min(500, ctx.response_buf.items.len)],
        });
    }

    // EOF after response is ok
    if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.response_buf.items.len > 0) {
        // Check if we got a complete response
        if (std.mem.indexOf(u8, ctx.response_buf.items, "\r\n\r\n") != null) {
            const elapsed = (std.time.Instant.now() catch unreachable).since(ctx.start_time);
            const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
            log.info("[{d}] EOF (ok): {d:.1}ms", .{ ctx.id, elapsed_ms });
            ctx.done = true;
            ctx.conn.user_ctx = null;
            ctx.conn.idling = true;
            ctx.pool.release(ctx.key, ctx.conn);
            return;
        }
    }

    log.err("[{d}] Error: {} (response_buf_len={d})", .{ ctx.id, err, ctx.response_buf.items.len });
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
    access_key: []const u8,
    secret_key: []const u8,
    region: []const u8,
) ![]const u8 {
    // Build headers list for signing
    var headers = std.ArrayListUnmanaged(std.http.Header){};
    defer {
        for (headers.items) |h| allocator.free(h.value);
        headers.deinit(allocator);
    }

    // Sign request
    const signer = SigV4{
        .region = region,
        .access_key = access_key,
        .secret_key = secret_key,
        .session_token = null,
        .use_unsigned_payload = false, // Sign the payload for rustfs
    };

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

    // Content-Length
    var len_buf: [20]u8 = undefined;
    const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch unreachable;
    try req_buf.appendSlice(allocator, "Content-Length: ");
    try req_buf.appendSlice(allocator, len_str);
    try req_buf.appendSlice(allocator, "\r\n\r\n");

    // Append body
    try req_buf.appendSlice(allocator, body);

    return req_buf.toOwnedSlice(allocator);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== Parallel Upload Probe ===\n", .{});
    std.debug.print("Testing N parallel PUT uploads to R2\n\n", .{});

    // Load R2 credentials from environment
    const access_key = std.posix.getenv("R2_ACCESS_KEY_ID") orelse {
        std.debug.print("ERROR: R2_ACCESS_KEY_ID not set\n", .{});
        return error.MissingCredentials;
    };
    const secret_key = std.posix.getenv("R2_SECRET_ACCESS_KEY") orelse {
        std.debug.print("ERROR: R2_SECRET_ACCESS_KEY not set\n", .{});
        return error.MissingCredentials;
    };
    const account_id = std.posix.getenv("R2_ACCOUNT_ID") orelse {
        std.debug.print("ERROR: R2_ACCOUNT_ID not set\n", .{});
        return error.MissingCredentials;
    };
    const bucket = std.posix.getenv("R2_BUCKET") orelse "zpq";
    const region = "auto"; // R2 uses "auto"

    // Build R2 host from account ID
    const host = try std.fmt.allocPrint(allocator, "{s}.r2.cloudflarestorage.com", .{account_id});
    defer allocator.free(host);

    std.debug.print("R2 endpoint: {s}\n", .{host});
    std.debug.print("Bucket: {s}\n\n", .{bucket});

    // Setup
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var pool = GlobalConnectionPool.init(allocator);
    defer pool.deinit();

    const port: u16 = 443;
    const key = ConnectionKey{ .host = host, .port = port, .use_tls = true };

    // DNS resolution for R2 using std.c.getaddrinfo
    const host_z = try allocator.dupeZ(u8, host);
    defer allocator.free(host_z);

    var hints: std.c.addrinfo = std.mem.zeroInit(std.c.addrinfo, .{
        .family = std.c.AF.UNSPEC,
        .socktype = std.c.SOCK.STREAM,
    });
    var res: ?*std.c.addrinfo = null;
    const rc = std.c.getaddrinfo(host_z.ptr, "443", &hints, &res);
    if (@intFromEnum(rc) != 0) {
        std.debug.print("DNS resolution failed: {s}\n", .{std.mem.span(std.c.gai_strerror(rc))});
        return error.DnsResolutionFailed;
    }
    defer std.c.freeaddrinfo(res.?);

    const xev_addr = xev.shim_net.Address.initPosix(res.?.addr.?);

    // Number of parallel uploads and body size
    const N = 1; // Single upload to isolate the issue
    const BODY_SIZE = 1 * 1024 * 1024; // 1MB - should need 16 chunks

    std.debug.print("Starting {d} parallel uploads ({d} KB each)...\n\n", .{ N, BODY_SIZE / 1024 });

    // Create test body data
    const body = try allocator.alloc(u8, BODY_SIZE);
    defer allocator.free(body);
    @memset(body, 'X');

    // Create contexts for each upload
    var contexts: [N]*UploadContext = undefined;
    const overall_start = try std.time.Instant.now();

    for (0..N) |i| {
        // Each upload goes to a different key
        const path = try std.fmt.allocPrint(allocator, "/{s}/testdata/probe_parallel_upload_{d}.bin", .{ bucket, i });
        defer allocator.free(path);

        const request = try buildPutRequest(
            allocator,
            host,
            port,
            path,
            body,
            access_key,
            secret_key,
            region,
        );

        // Create connection
        const conn = try allocator.create(Connection);
        conn.* = try Connection.initWithOptions(&loop, allocator, host, .{});

        // Create context
        const ctx = try allocator.create(UploadContext);
        ctx.* = .{
            .allocator = allocator,
            .id = i,
            .conn = conn,
            .pool = &pool,
            .key = key,
            .request_data = request,
            .start_time = try std.time.Instant.now(),
            .body_size = BODY_SIZE,
        };
        contexts[i] = ctx;

        // Set up callbacks
        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;
        conn.on_write_complete = onWriteComplete;

        // Kick off connection (non-blocking)
        log.info("[{d}] Starting connection...", .{i});
        try conn.connect(xev_addr);
    }

    // Run the event loop until all connections complete
    std.debug.print("\nRunning event loop (.until_done)...\n", .{});
    try loop.run(.until_done);

    const overall_elapsed = (try std.time.Instant.now()).since(overall_start);
    const overall_ms = @as(f64, @floatFromInt(overall_elapsed)) / 1_000_000.0;
    const total_bytes = N * BODY_SIZE;
    const overall_throughput = (@as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0)) / (overall_ms / 1000.0);

    // Check results
    std.debug.print("\n=== Results ===\n", .{});
    var success_count: usize = 0;
    var error_count: usize = 0;

    for (contexts) |ctx| {
        if (ctx.err) |err| {
            std.debug.print("[{d}] FAILED: {}\n", .{ ctx.id, err });
            error_count += 1;
        } else if (ctx.status_code == 200) {
            std.debug.print("[{d}] OK: etag={s}\n", .{ ctx.id, ctx.etag orelse "none" });
            success_count += 1;
        } else {
            std.debug.print("[{d}] HTTP {d}: {s}\n", .{
                ctx.id,
                ctx.status_code,
                ctx.response_buf.items[0..@min(200, ctx.response_buf.items.len)],
            });
            error_count += 1;
        }

        // Cleanup
        allocator.free(ctx.request_data);
        ctx.deinit();
        allocator.destroy(ctx);
    }

    std.debug.print("\nTotal: {d} success, {d} errors\n", .{ success_count, error_count });
    std.debug.print("Overall: {d:.1}ms for {d} MB = {d:.1} MB/s\n", .{
        overall_ms,
        total_bytes / (1024 * 1024),
        overall_throughput,
    });

    if (success_count == N) {
        std.debug.print("\n✓ Parallel uploads work!\n", .{});
    } else {
        std.debug.print("\n✗ Some uploads failed\n", .{});
    }
}
