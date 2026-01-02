const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const SigV4 = @import("sigv4.zig").SigV4;
const dns = @import("dns.zig");
const global_pool_mod = @import("global_pool.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;

const log = std.log.scoped(.s3_writer);

/// S3 Writer implementing std.io.Writer for streaming uploads.
///
/// Design:
/// - Buffers writes until reaching part_size (default 8MB, matching AWS CLI)
/// - Automatically uses multipart upload for large files (>8MB threshold)
/// - Uploads parts in parallel while buffering next chunk
/// - Falls back to single PUT for small files (< threshold)
///
/// Performance vs AWS CLI:
/// - AWS CLI: 10 threads, 8MB chunks, 80MB memory footprint, no network awareness
/// - S3Writer: Event-driven (xev), adaptive concurrency, UNSIGNED-PAYLOAD option
///
/// Usage:
///   var s3w = try S3Writer.init(allocator, "bucket", "key", "us-east-1");
///   defer s3w.deinit();
///   try s3w.setCredentials(access_key, secret_key, session_token);
///   try s3w.writeAll(data);
///   try s3w.finish(); // Completes multipart or does single PUT
///
pub fn S3WriterGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;
    const GlobalConnectionPool = global_pool_mod.GlobalConnectionPool(XevApi);
    const ThreadPoolResolver = dns.ThreadPoolResolverGen(XevApi);
    const Connection = tls.ConnectionGen(XevApi);
    const ConnectionKey = global_pool_mod.ConnectionKey;

    return struct {
        const Self = @This();

        // Type declarations (must come before fields that use them)
        const PartInfo = struct {
            part_number: u32,
            etag: []const u8,
        };

        const PartUploadContext = struct {
            allocator: std.mem.Allocator,
            part_number: u32,
            data_len: usize,
            start_time: std.time.Instant,
            parser: ResponseParser = .{},
            body_buf: std.ArrayListUnmanaged(u8) = .{},
            done: bool = false,
            err: ?anyerror = null,
            etag: ?[]const u8 = null,
            conn: *Connection = undefined,
            pool: *GlobalConnectionPool = undefined,
            key: ConnectionKey = undefined,
            writer_self: *Self = undefined,
            // Request data for sending after connect
            request_buf: []const u8 = &[_]u8{},
            body_data: []const u8 = &[_]u8{},
            headers_sent: bool = false,

            fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) void {
                const ctx: *PartUploadContext = @ptrCast(@alignCast(ctx_ptr));
                ctx.body_buf.appendSlice(ctx.allocator, chunk) catch |e| {
                    ctx.err = e;
                };
            }

            fn deinit(ctx: *PartUploadContext) void {
                ctx.body_buf.deinit(ctx.allocator);
            }
        };

        const InFlightPart = struct {
            part_number: u32,
            data: []const u8, // Owned slice
            ctx: ?*PartUploadContext = null,
            start_time: std.time.Instant,
        };

        // Configuration
        allocator: std.mem.Allocator,
        loop: *LoopType,
        thread_pool: *xev.ThreadPool,
        resolver: dns.ResolverGen(XevApi),
        tp_resolver: ?*ThreadPoolResolver = null,
        pool: *GlobalConnectionPool,

        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        host: []const u8,
        port: u16,
        use_tls: bool,

        // Auth
        access_key: ?[]const u8 = null,
        secret_key: ?[]const u8 = null,
        session_token: ?[]const u8 = null,

        // Multipart state
        upload_id: ?[]const u8 = null,
        parts: std.ArrayListUnmanaged(PartInfo) = .{},
        current_part_number: u32 = 1,

        // Buffering - AWS CLI defaults: 8MB threshold, 8MB chunk
        buffer: std.ArrayListUnmanaged(u8) = .{},
        part_size: usize = 8 * 1024 * 1024, // 8MB (AWS CLI default)
        multipart_threshold: usize = 8 * 1024 * 1024, // 8MB threshold

        // Parallelism - we can go higher than AWS CLI's 10
        max_concurrent_uploads: usize = 4,

        // In-flight uploads for parallel execution
        in_flight: std.ArrayListUnmanaged(InFlightPart) = .{},

        // Options
        use_unsigned_payload: bool = false, // Skip SHA-256 for speed (safe in VPC)
        use_path_style: bool = false, // Path-style URLs for MinIO/rustfs compatibility

        // Ownership flags
        owns_loop: bool = false,
        owns_pool: bool = false,

        // Cached DNS
        cached_addr: ?xev.shim_net.Address = null,

        pub fn init(
            allocator: std.mem.Allocator,
            bucket: []const u8,
            key: []const u8,
            region: []const u8,
        ) !*Self {
            // Detect best available backend at runtime for xev.Dynamic
            if (@hasDecl(XevApi, "Dynamic") or @hasDecl(XevApi, "detect")) {
                if (@hasDecl(XevApi, "detect")) {
                    try XevApi.detect();
                }
            }

            const loop = try allocator.create(LoopType);
            loop.* = try LoopType.init(.{});
            errdefer {
                loop.deinit();
                allocator.destroy(loop);
            }

            const thread_pool = try allocator.create(xev.ThreadPool);
            thread_pool.* = xev.ThreadPool.init(.{});
            errdefer {
                thread_pool.shutdown();
                thread_pool.deinit();
                allocator.destroy(thread_pool);
            }

            const local_pool = try allocator.create(GlobalConnectionPool);
            local_pool.* = GlobalConnectionPool.init(allocator);
            errdefer {
                local_pool.deinit();
                allocator.destroy(local_pool);
            }

            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
            errdefer allocator.free(host);

            var tp_resolver = try allocator.create(ThreadPoolResolver);
            tp_resolver.* = ThreadPoolResolver.init(thread_pool, allocator);

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .loop = loop,
                .thread_pool = thread_pool,
                .resolver = tp_resolver.resolver(),
                .tp_resolver = tp_resolver,
                .pool = local_pool,
                .bucket = try allocator.dupe(u8, bucket),
                .key = try allocator.dupe(u8, key),
                .region = try allocator.dupe(u8, region),
                .host = host,
                .port = 443,
                .use_tls = true,
                .owns_loop = true,
                .owns_pool = true,
            };
            return self;
        }

        /// Initialize with an existing event loop and thread pool (for Lambda/unibin).
        /// The caller retains ownership of loop and pool - they won't be freed on deinit.
        pub fn initWithLoop(
            allocator: std.mem.Allocator,
            loop: *LoopType,
            thread_pool: *xev.ThreadPool,
            bucket: []const u8,
            key: []const u8,
            region: []const u8,
        ) !*Self {
            const local_pool = try allocator.create(GlobalConnectionPool);
            local_pool.* = GlobalConnectionPool.init(allocator);
            errdefer {
                local_pool.deinit();
                allocator.destroy(local_pool);
            }

            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, region });
            errdefer allocator.free(host);

            var tp_resolver = try allocator.create(ThreadPoolResolver);
            tp_resolver.* = ThreadPoolResolver.init(thread_pool, allocator);

            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .loop = loop,
                .thread_pool = thread_pool,
                .resolver = tp_resolver.resolver(),
                .tp_resolver = tp_resolver,
                .pool = local_pool,
                .bucket = try allocator.dupe(u8, bucket),
                .key = try allocator.dupe(u8, key),
                .region = try allocator.dupe(u8, region),
                .host = host,
                .port = 443,
                .use_tls = true,
                .owns_loop = false, // Caller owns loop
                .owns_pool = true,
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            // Free in-flight uploads
            for (self.in_flight.items) |part| {
                self.allocator.free(part.data);
                if (part.ctx) |ctx| {
                    ctx.deinit();
                    self.allocator.destroy(ctx);
                }
            }
            self.in_flight.deinit(self.allocator);

            // Free completed parts
            for (self.parts.items) |part| {
                self.allocator.free(part.etag);
            }
            self.parts.deinit(self.allocator);

            // Free buffer
            self.buffer.deinit(self.allocator);

            // Free upload_id if present
            if (self.upload_id) |id| self.allocator.free(id);

            // Free strings
            self.allocator.free(self.bucket);
            self.allocator.free(self.key);
            self.allocator.free(self.region);
            self.allocator.free(self.host);

            // Free auth
            if (self.access_key) |k| self.allocator.free(k);
            if (self.secret_key) |k| self.allocator.free(k);
            if (self.session_token) |t| self.allocator.free(t);

            // Free resolver
            if (self.tp_resolver) |r| self.allocator.destroy(r);

            // Free owned resources
            if (self.owns_pool) {
                self.pool.deinit();
                self.allocator.destroy(self.pool);
            }
            if (self.owns_loop) {
                self.thread_pool.shutdown();
                self.thread_pool.deinit();
                self.loop.deinit();
                self.allocator.destroy(self.thread_pool);
                self.allocator.destroy(self.loop);
            }

            self.allocator.destroy(self);
        }

        pub fn setCredentials(self: *Self, access_key: []const u8, secret_key: []const u8, session_token: ?[]const u8) !void {
            if (self.access_key) |k| self.allocator.free(k);
            if (self.secret_key) |k| self.allocator.free(k);
            if (self.session_token) |t| self.allocator.free(t);

            self.access_key = try self.allocator.dupe(u8, access_key);
            self.secret_key = try self.allocator.dupe(u8, secret_key);
            self.session_token = if (session_token) |t| try self.allocator.dupe(u8, t) else null;
        }

        // =====================================================================
        // Write interface
        // =====================================================================

        pub const WriteError = error{WriteFailed};

        /// Write bytes to S3. Buffers internally until part_size is reached.
        pub fn write(self: *Self, bytes: []const u8) WriteError!usize {
            self.writeInternal(bytes) catch |err| {
                log.err("S3Writer.write failed: {}", .{err});
                return error.WriteFailed;
            };
            return bytes.len;
        }

        /// Write all bytes to S3.
        pub fn writeAll(self: *Self, bytes: []const u8) !void {
            try self.writeInternal(bytes);
        }

        fn writeInternal(self: *Self, bytes: []const u8) !void {
            try self.buffer.appendSlice(self.allocator, bytes);

            // If buffer exceeds part size, start uploading parts
            while (self.buffer.items.len >= self.part_size) {
                try self.flushPart();
            }
        }

        fn flushPart(self: *Self) !void {
            if (self.buffer.items.len == 0) return;

            // Initialize multipart upload if not started
            if (self.upload_id == null) {
                try self.createMultipartUpload();
            }

            // Extract part data from buffer
            const part_len = @min(self.buffer.items.len, self.part_size);
            const part_data = try self.allocator.dupe(u8, self.buffer.items[0..part_len]);
            errdefer self.allocator.free(part_data);

            // Shift remaining data to start of buffer
            const remaining = self.buffer.items.len - part_len;
            if (remaining > 0) {
                std.mem.copyForwards(u8, self.buffer.items[0..remaining], self.buffer.items[part_len..]);
            }
            self.buffer.shrinkRetainingCapacity(remaining);

            // Queue this part for parallel upload
            const part_number = self.current_part_number;
            self.current_part_number += 1;

            try self.in_flight.append(self.allocator, .{
                .part_number = part_number,
                .data = part_data,
                .start_time = std.time.Instant.now() catch unreachable,
            });

            // Start uploads up to max concurrency
            try self.startPendingUploads();

            // If we're at max concurrency, drain one completed upload before returning
            // This provides backpressure to avoid unbounded memory growth
            if (self.in_flight.items.len >= self.max_concurrent_uploads) {
                try self.drainCompletedUploads(false);
            }
        }

        /// Start uploads for queued parts that haven't been started yet
        fn startPendingUploads(self: *Self) !void {
            // Count how many are currently active (started but not done)
            var active: usize = 0;
            for (self.in_flight.items) |part| {
                if (part.ctx != null and !part.ctx.?.done) {
                    active += 1;
                }
            }

            // Start new uploads up to max concurrency
            for (self.in_flight.items) |*part| {
                if (active >= self.max_concurrent_uploads) break;
                if (part.ctx == null) {
                    try self.startPartUpload(part);
                    active += 1;
                }
            }
        }

        /// Start an individual part upload (non-blocking)
        fn startPartUpload(self: *Self, part: *InFlightPart) !void {
            // Resolve DNS if needed
            if (self.cached_addr == null) {
                self.cached_addr = try self.resolve();
            }
            const addr = self.cached_addr.?;

            // Get or create connection
            const key = ConnectionKey{
                .host = self.host,
                .port = self.port,
                .use_tls = self.use_tls,
            };

            const conn = if (self.pool.acquire(key)) |c| blk: {
                c.idling = false;
                c.pending_read = false;
                c.pending_write = false;
                break :blk c;
            } else blk: {
                const c = try self.allocator.create(Connection);
                c.* = try Connection.initWithOptions(self.loop, self.allocator, self.host, .{});
                break :blk c;
            };

            // Create upload context
            const ctx = try self.allocator.create(PartUploadContext);
            ctx.* = .{
                .allocator = self.allocator,
                .part_number = part.part_number,
                .data_len = part.data.len,
                .start_time = part.start_time,
                .conn = conn,
                .pool = self.pool,
                .key = key,
                .writer_self = self,
            };
            part.ctx = ctx;

            // Set up callbacks
            conn.user_ctx = ctx;
            conn.on_connect = onPartConnect;
            conn.on_data = onPartData;
            conn.on_error = onPartError;

            // Build and sign request
            const path = try self.buildPath("/{s}?partNumber={d}&uploadId={s}", .{
                self.key,
                part.part_number,
                self.upload_id.?,
            });
            defer self.allocator.free(path);

            var headers = std.ArrayListUnmanaged(std.http.Header){};
            defer {
                for (headers.items) |h| self.allocator.free(h.value);
                headers.deinit(self.allocator);
            }
            try self.signRequest("PUT", path, &headers, part.data);

            // Build HTTP request with body included (single write to avoid two-phase complexity)
            var req_buf = std.ArrayListUnmanaged(u8){};
            errdefer req_buf.deinit(self.allocator);

            try req_buf.appendSlice(self.allocator, "PUT ");
            try req_buf.appendSlice(self.allocator, path);
            try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");

            for (headers.items) |h| {
                try req_buf.appendSlice(self.allocator, h.name);
                try req_buf.appendSlice(self.allocator, ": ");
                try req_buf.appendSlice(self.allocator, h.value);
                try req_buf.appendSlice(self.allocator, "\r\n");
            }

            var len_buf: [20]u8 = undefined;
            const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{part.data.len}) catch unreachable;
            try req_buf.appendSlice(self.allocator, "Content-Length: ");
            try req_buf.appendSlice(self.allocator, len_str);
            try req_buf.appendSlice(self.allocator, "\r\n\r\n");

            // Append body directly to request buffer (single write)
            try req_buf.appendSlice(self.allocator, part.data);

            // Store complete request (headers + body)
            ctx.request_buf = try req_buf.toOwnedSlice(self.allocator);
            ctx.body_data = part.data; // Keep reference for data_len tracking

            // Connect (or use existing connection)
            if (!conn.handshake_complete) {
                try conn.connect(addr);
            } else {
                // Already connected, send immediately
                onPartConnect(ctx);
            }
        }

        fn onPartConnect(ctx_void: ?*anyopaque) void {
            const ctx: *PartUploadContext = @ptrCast(@alignCast(ctx_void));
            // Send complete request (headers + body) in single write
            ctx.conn.write(ctx.request_buf) catch |err| {
                ctx.err = err;
                ctx.done = true;
                return;
            };
        }

        fn onPartData(ctx_void: ?*anyopaque, data: []const u8) void {
            const ctx: *PartUploadContext = @ptrCast(@alignCast(ctx_void));

            ctx.parser.feed(data, ctx, PartUploadContext.onBody) catch |e| {
                ctx.err = e;
                ctx.done = true;
                return;
            };

            if (ctx.parser.state == .done) {
                // Extract ETag from headers
                const header_str = ctx.parser.header_buf[0..ctx.parser.header_len];
                var lines = std.mem.splitSequence(u8, header_str, "\r\n");
                _ = lines.first(); // Skip status line

                while (lines.next()) |line| {
                    if (line.len == 0) break;
                    if (std.mem.indexOf(u8, line, ": ")) |colon_pos| {
                        if (std.ascii.eqlIgnoreCase(line[0..colon_pos], "etag")) {
                            ctx.etag = ctx.allocator.dupe(u8, line[colon_pos + 2 ..]) catch null;
                            break;
                        }
                    }
                }

                ctx.done = true;

                // Return connection to pool
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);
            }
        }

        fn onPartError(ctx_void: ?*anyopaque, err: anyerror) void {
            const ctx: *PartUploadContext = @ptrCast(@alignCast(ctx_void));
            if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.parser.headersComplete()) {
                ctx.done = true;
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);
                return;
            }
            ctx.err = err;
            ctx.done = true;
            ctx.conn.close();
        }

        /// Check for completed uploads and collect results
        fn drainCompletedUploads(self: *Self, wait_all: bool) !void {
            while (true) {
                // Run event loop to progress in-flight uploads
                if (self.countPending() > 0) {
                    try self.loop.run(.once);
                }

                // Collect completed uploads
                var i: usize = 0;
                while (i < self.in_flight.items.len) {
                    const part = &self.in_flight.items[i];
                    if (part.ctx) |ctx| {
                        if (ctx.done) {
                            if (ctx.err) |err| {
                                // Clean up and propagate error
                                self.allocator.free(ctx.request_buf);
                                ctx.deinit();
                                self.allocator.destroy(ctx);
                                self.allocator.free(part.data);
                                _ = self.in_flight.swapRemove(i);
                                return err;
                            }

                            // Success - record the part
                            const elapsed = (std.time.Instant.now() catch unreachable).since(ctx.start_time);
                            const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
                            const throughput_mbps = (@as(f64, @floatFromInt(ctx.data_len)) / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0);
                            log.info("Part {d}: {d} bytes in {d:.1}ms ({d:.1} MB/s)", .{
                                ctx.part_number, ctx.data_len, elapsed_ms, throughput_mbps,
                            });

                            try self.parts.append(self.allocator, .{
                                .part_number = ctx.part_number,
                                .etag = ctx.etag orelse return error.NoETagInResponse,
                            });

                            // Clean up
                            self.allocator.free(ctx.request_buf);
                            ctx.deinit();
                            self.allocator.destroy(ctx);
                            self.allocator.free(part.data);
                            _ = self.in_flight.swapRemove(i);
                            continue; // Don't increment i
                        }
                    }
                    i += 1;
                }

                // Start any newly-available uploads
                try self.startPendingUploads();

                // Exit condition
                if (!wait_all or self.in_flight.items.len == 0) break;
            }
        }

        fn countPending(self: *Self) usize {
            var count: usize = 0;
            for (self.in_flight.items) |part| {
                if (part.ctx) |ctx| {
                    if (!ctx.done) count += 1;
                }
            }
            return count;
        }

        // =====================================================================
        // Multipart Upload Operations
        // =====================================================================

        /// Build path with optional bucket prefix for path-style URLs
        fn buildPath(self: *Self, comptime fmt: []const u8, args: anytype) ![]const u8 {
            if (self.use_path_style) {
                // Path-style: /{bucket}/{key}
                return std.fmt.allocPrint(self.allocator, "/{s}" ++ fmt, .{self.bucket} ++ args);
            } else {
                // Virtual-hosted style: /{key}
                return std.fmt.allocPrint(self.allocator, fmt, args);
            }
        }

        fn createMultipartUpload(self: *Self) !void {
            log.debug("Creating multipart upload for {s}/{s}", .{ self.bucket, self.key });

            const path = try self.buildPath("/{s}?uploads", .{self.key});
            defer self.allocator.free(path);

            const response = try self.doRequest("POST", path, "", null);
            defer self.allocator.free(response.body);
            defer self.allocator.free(response.headers);

            // Parse XML response to extract UploadId
            // <InitiateMultipartUploadResult>...<UploadId>XXX</UploadId>...</>
            if (std.mem.indexOf(u8, response.body, "<UploadId>")) |start| {
                const id_start = start + "<UploadId>".len;
                if (std.mem.indexOf(u8, response.body[id_start..], "</UploadId>")) |end| {
                    self.upload_id = try self.allocator.dupe(u8, response.body[id_start .. id_start + end]);
                    log.debug("Got upload_id: {s}", .{self.upload_id.?});
                    return;
                }
            }

            log.err("Failed to parse UploadId from response: {s}", .{response.body});
            return error.InvalidMultipartResponse;
        }

        fn uploadPart(self: *Self, part_number: u32, data: []const u8) ![]const u8 {
            const start = std.time.Instant.now() catch unreachable;
            log.debug("Uploading part {d} ({d} bytes)", .{ part_number, data.len });

            const path = try self.buildPath("/{s}?partNumber={d}&uploadId={s}", .{
                self.key,
                part_number,
                self.upload_id.?,
            });
            defer self.allocator.free(path);

            const response = try self.doRequest("PUT", path, data, null);
            defer self.allocator.free(response.body);

            // Extract ETag from response headers
            for (response.headers) |h| {
                if (std.ascii.eqlIgnoreCase(h.name, "etag")) {
                    const etag = try self.allocator.dupe(u8, h.value);
                    const elapsed = (std.time.Instant.now() catch unreachable).since(start);
                    const elapsed_ms = @as(f64, @floatFromInt(elapsed)) / 1_000_000.0;
                    const throughput_mbps = (@as(f64, @floatFromInt(data.len)) / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0);
                    log.info("Part {d}: {d} bytes in {d:.1}ms ({d:.1} MB/s), etag: {s}", .{ part_number, data.len, elapsed_ms, throughput_mbps, etag });
                    return etag;
                }
            }

            log.err("No ETag in response headers", .{});
            return error.NoETagInResponse;
        }

        fn completeMultipartUpload(self: *Self) !void {
            log.debug("Completing multipart upload with {d} parts", .{self.parts.items.len});

            // Build XML body
            var xml = std.ArrayListUnmanaged(u8){};
            defer xml.deinit(self.allocator);

            try xml.appendSlice(self.allocator, "<CompleteMultipartUpload>");

            // Sort parts by part number
            std.mem.sort(PartInfo, self.parts.items, {}, struct {
                fn lessThan(_: void, a: PartInfo, b: PartInfo) bool {
                    return a.part_number < b.part_number;
                }
            }.lessThan);

            for (self.parts.items) |part| {
                try xml.appendSlice(self.allocator, "<Part><PartNumber>");
                var num_buf: [10]u8 = undefined;
                const num_str = std.fmt.bufPrint(&num_buf, "{d}", .{part.part_number}) catch unreachable;
                try xml.appendSlice(self.allocator, num_str);
                try xml.appendSlice(self.allocator, "</PartNumber><ETag>");
                try xml.appendSlice(self.allocator, part.etag);
                try xml.appendSlice(self.allocator, "</ETag></Part>");
            }
            try xml.appendSlice(self.allocator, "</CompleteMultipartUpload>");

            const path = try self.buildPath("/{s}?uploadId={s}", .{ self.key, self.upload_id.? });
            defer self.allocator.free(path);

            var extra_headers = [_]std.http.Header{
                .{ .name = "Content-Type", .value = "application/xml" },
            };

            const response = try self.doRequest("POST", path, xml.items, &extra_headers);
            defer self.allocator.free(response.body);

            if (response.status >= 400) {
                log.err("CompleteMultipartUpload failed: {d} - {s}", .{ response.status, response.body });
                return error.CompleteMultipartFailed;
            }

            log.info("Multipart upload completed: {s}/{s}", .{ self.bucket, self.key });
        }

        fn singlePut(self: *Self, data: []const u8) !void {
            log.debug("Single PUT for {s}/{s} ({d} bytes)", .{ self.bucket, self.key, data.len });

            const path = try self.buildPath("/{s}", .{self.key});
            defer self.allocator.free(path);

            const response = try self.doRequest("PUT", path, data, null);
            defer self.allocator.free(response.body);
            defer self.allocator.free(response.headers);

            if (response.status >= 400) {
                log.err("PUT failed: {d} - {s}", .{ response.status, response.body });
                return error.PutFailed;
            }

            log.info("Single PUT completed: {s}/{s}", .{ self.bucket, self.key });
        }

        // =====================================================================
        // HTTP Request (callback-based xev pattern)
        // =====================================================================

        const RequestResult = struct {
            body: []const u8,
            headers: []ResponseHeader,
            status: u16,
        };

        const ResponseHeader = struct {
            name: []const u8,
            value: []const u8,
        };

        /// Context for tracking an in-flight HTTP request
        const RequestContext = struct {
            allocator: std.mem.Allocator,
            parser: ResponseParser = .{},
            body_buf: std.ArrayListUnmanaged(u8) = .{},
            done: bool = false,
            err: ?anyerror = null,
            conn: *Connection = undefined,
            pool: *GlobalConnectionPool = undefined,
            key: ConnectionKey = undefined,

            fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) void {
                const ctx: *RequestContext = @ptrCast(@alignCast(ctx_ptr));
                ctx.body_buf.appendSlice(ctx.allocator, chunk) catch |e| {
                    ctx.err = e;
                };
            }

            fn deinit(ctx: *RequestContext) void {
                ctx.body_buf.deinit(ctx.allocator);
            }
        };

        fn onConnect(ctx_void: ?*anyopaque) void {
            // Connection established - request already sent after handshake
            _ = ctx_void;
        }

        fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));

            ctx.parser.feed(data, ctx, RequestContext.onBody) catch |e| {
                ctx.err = e;
                ctx.done = true;
                return;
            };

            if (ctx.parser.state == .done) {
                ctx.done = true;
                // Return connection to pool
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);
            }
        }

        fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void));
            // EOF after headers complete is ok
            if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.parser.headersComplete()) {
                ctx.done = true;
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);
                return;
            }
            ctx.err = err;
            ctx.done = true;
            ctx.conn.close();
        }

        fn doRequest(
            self: *Self,
            method: []const u8,
            path: []const u8,
            body: []const u8,
            extra_headers: ?[]const std.http.Header,
        ) !RequestResult {
            // Resolve DNS if needed
            if (self.cached_addr == null) {
                self.cached_addr = try self.resolve();
            }
            const addr = self.cached_addr.?;

            // Get or create connection
            const key = ConnectionKey{
                .host = self.host,
                .port = self.port,
                .use_tls = self.use_tls,
            };

            const conn = if (self.pool.acquire(key)) |c| blk: {
                c.idling = false;
                c.pending_read = false;
                c.pending_write = false;
                break :blk c;
            } else blk: {
                const c = try self.allocator.create(Connection);
                c.* = try Connection.initWithOptions(self.loop, self.allocator, self.host, .{});
                break :blk c;
            };
            errdefer {
                conn.close();
                self.allocator.destroy(conn);
            }

            // Build request with signing
            var headers = std.ArrayListUnmanaged(std.http.Header){};
            defer {
                for (headers.items) |h| {
                    self.allocator.free(h.value);
                }
                headers.deinit(self.allocator);
            }

            // Add extra headers first
            if (extra_headers) |eh| {
                for (eh) |h| {
                    try headers.append(self.allocator, .{
                        .name = h.name,
                        .value = try self.allocator.dupe(u8, h.value),
                    });
                }
            }

            // Sign request (adds Host, X-Amz-Date, Authorization, etc.)
            try self.signRequest(method, path, &headers, body);

            // Build HTTP request bytes
            var req_buf = std.ArrayListUnmanaged(u8){};
            defer req_buf.deinit(self.allocator);

            try req_buf.appendSlice(self.allocator, method);
            try req_buf.appendSlice(self.allocator, " ");
            try req_buf.appendSlice(self.allocator, path);
            try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");

            for (headers.items) |h| {
                try req_buf.appendSlice(self.allocator, h.name);
                try req_buf.appendSlice(self.allocator, ": ");
                try req_buf.appendSlice(self.allocator, h.value);
                try req_buf.appendSlice(self.allocator, "\r\n");
            }

            // Add Content-Length (always required by S3, even for 0-byte bodies)
            try req_buf.appendSlice(self.allocator, "Content-Length: ");
            var len_buf: [20]u8 = undefined;
            const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch unreachable;
            try req_buf.appendSlice(self.allocator, len_str);
            try req_buf.appendSlice(self.allocator, "\r\n");

            try req_buf.appendSlice(self.allocator, "\r\n");

            // Set up request context
            var ctx = RequestContext{
                .allocator = self.allocator,
                .conn = conn,
                .pool = self.pool,
                .key = key,
            };
            defer ctx.deinit();

            conn.user_ctx = &ctx;
            conn.on_connect = onConnect;
            conn.on_data = onData;
            conn.on_error = onError;

            // Connect if needed, then send request
            if (!conn.handshake_complete) {
                try conn.connect(addr);
                // Run loop until handshake completes
                while (!conn.handshake_complete and !ctx.done and ctx.err == null) {
                    try self.loop.run(.once);
                }
                if (ctx.err) |err| return err;
            }

            // Wait for any pending writes to complete (e.g., from TLS handshake)
            while (conn.pending_write and !ctx.done and ctx.err == null) {
                try self.loop.run(.once);
            }
            if (ctx.err) |err| return err;

            // Send request headers
            log.debug("Sending {s} {s} ({d} body bytes)", .{ method, path, body.len });
            try conn.write(req_buf.items);

            // Send body if present
            if (body.len > 0) {
                // Wait for headers to be sent
                while (conn.pending_write and !ctx.done and ctx.err == null) {
                    try self.loop.run(.once);
                }
                if (ctx.err) |err| return err;
                try conn.write(body);
            }

            // Run loop until response complete
            while (!ctx.done and ctx.err == null) {
                try self.loop.run(.once);
            }

            if (ctx.err) |err| return err;

            // Check status
            if (ctx.parser.status_code >= 400) {
                log.err("S3 request failed: {d} - {s}", .{ ctx.parser.status_code, ctx.body_buf.items });
            }

            // Parse response headers
            var result_headers = std.ArrayListUnmanaged(ResponseHeader){};
            defer result_headers.deinit(self.allocator);

            const header_str = ctx.parser.header_buf[0..ctx.parser.header_len];
            var lines = std.mem.splitSequence(u8, header_str, "\r\n");
            _ = lines.first(); // Skip status line

            while (lines.next()) |line| {
                if (line.len == 0) break;
                if (std.mem.indexOf(u8, line, ": ")) |colon_pos| {
                    try result_headers.append(self.allocator, .{
                        .name = line[0..colon_pos],
                        .value = line[colon_pos + 2 ..],
                    });
                }
            }

            return .{
                .body = try self.allocator.dupe(u8, ctx.body_buf.items),
                .headers = try result_headers.toOwnedSlice(self.allocator),
                .status = ctx.parser.status_code,
            };
        }

        fn signRequest(
            self: *Self,
            method: []const u8,
            path: []const u8,
            headers: *std.ArrayListUnmanaged(std.http.Header),
            payload: []const u8,
        ) !void {
            if (self.access_key) |ak| {
                if (self.secret_key) |sk| {
                    const signer = SigV4{
                        .region = self.region,
                        .access_key = ak,
                        .secret_key = sk,
                        .session_token = self.session_token,
                        // UNSIGNED-PAYLOAD: Skip SHA-256 hash of body for uploads
                        // This is safe over HTTPS and gives us a big CPU win over AWS CLI
                        .use_unsigned_payload = self.use_unsigned_payload,
                    };

                    // Include port in URL if non-standard
                    const url = if (self.port == 443 or self.port == 80)
                        try std.fmt.allocPrint(self.allocator, "https://{s}{s}", .{ self.host, path })
                    else
                        try std.fmt.allocPrint(self.allocator, "https://{s}:{d}{s}", .{ self.host, self.port, path });
                    defer self.allocator.free(url);

                    log.debug("Signing: {s} {s}", .{ method, url });

                    const uri = try std.Uri.parse(url);
                    try signer.sign(self.allocator, method, uri, headers, payload);
                }
            } else {
                // No credentials - just add Host header
                try headers.append(self.allocator, .{
                    .name = "Host",
                    .value = try self.allocator.dupe(u8, self.host),
                });
            }
        }

        fn resolve(self: *Self) !xev.shim_net.Address {
            var comp = dns.ResolverGen(XevApi).Completion.init();
            defer comp.deinit(self.allocator);

            const DnsCtx = struct {
                addr: ?xev.shim_net.Address = null,
                err: ?anyerror = null,
                done: bool = false,

                fn callback(ud: ?*anyopaque, results: []const xev.shim_net.Address, err: anyerror!void) void {
                    const c: *@This() = @ptrCast(@alignCast(ud));
                    err catch |e| {
                        c.err = e;
                        c.done = true;
                        return;
                    };
                    if (results.len > 0) {
                        c.addr = results[0];
                    } else {
                        c.err = error.HostNotFound;
                    }
                    c.done = true;
                }
            };

            var dctx = DnsCtx{};
            var resolver_iface = self.resolver;
            resolver_iface.resolve(self.loop, self.host, self.port, &comp, DnsCtx.callback, &dctx);

            while (!dctx.done) {
                try self.loop.run(.once);
            }

            if (dctx.err) |err| return err;
            return dctx.addr orelse error.HostNotFound;
        }

        // =====================================================================
        // Public API
        // =====================================================================

        /// Finish the upload. Must be called after all writes.
        /// - If using multipart: completes the multipart upload
        /// - If small file: does a single PUT
        pub fn finish(self: *Self) !void {
            if (self.upload_id != null) {
                // Multipart mode: upload final part if buffer has data
                if (self.buffer.items.len > 0) {
                    try self.flushPart();
                }

                // Wait for all in-flight uploads to complete
                try self.drainCompletedUploads(true);

                try self.completeMultipartUpload();
            } else if (self.buffer.items.len > 0) {
                // Small file: single PUT
                try self.singlePut(self.buffer.items);
            }
        }
    };
}

pub const S3Writer = S3WriterGen(xev);

// Also provide Epoll-specific version for Lambda
pub const EpollS3Writer = S3WriterGen(xev.Epoll);
