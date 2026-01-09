const std = @import("std");
const io = @import("interface.zig");
const transport = @import("transport.zig");
const protocol_s3 = @import("../protocol/s3.zig");
const protocol_http = @import("../protocol/http.zig");
const sigv4 = @import("../protocol/sigv4.zig");
const xev_mod = @import("xev");

/// Number of parallel connections in the pool
const NUM_CONNECTIONS = 4;

pub fn AsyncS3SourceGen(comptime Xev: type) type {
    const Loop = *Xev.Loop;
    const Conn = transport.ConnectionGen(Xev);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        loop: Loop,
        s3: protocol_s3.S3,
        bucket: []const u8,
        key: []const u8,
        host: []const u8,

        resolver: transport.Resolver,

        /// Connection pool for parallel requests
        pool: ConnectionPool,

        /// Size of the object, fetched on init.
        content_length: u64,

        /// Resolved address (cached after first DNS lookup)
        resolved_addr: ?transport.Address = null,

        pub fn init(allocator: std.mem.Allocator, loop: Loop, thread_pool: *xev_mod.ThreadPool, s3_config: protocol_s3.S3, bucket: []const u8, key: []const u8) !Self {
            const host = try std.fmt.allocPrint(allocator, "{s}.s3.{s}.amazonaws.com", .{ bucket, s3_config.region });

            var self = Self{
                .allocator = allocator,
                .loop = loop,
                .s3 = s3_config,
                .bucket = bucket,
                .key = key,
                .host = host,
                .resolver = transport.Resolver.init(allocator, thread_pool),
                .pool = undefined,
                .content_length = 0,
            };

            // Initialize pool with back-pointer
            self.pool = ConnectionPool.init(&self);

            // Fetch size via HEAD
            try self.fetchSize();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.pool.deinit();
            self.allocator.free(self.host);
        }

        pub fn randomAccessSource(self: *Self) io.RandomAccessSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .readAt = readAt,
                    .size = size,
                    .close = close,
                },
            };
        }

        fn fetchSize(self: *Self) !void {
            var ctx = RequestContext{
                .source = self,
                .method = .HEAD,
                .allocator = self.allocator,
            };
            try ctx.run();
            if (ctx.status_code != 200) return error.S3HeadFailed;
            self.content_length = ctx.content_length;
        }

        fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            const range_end = offset + buf.len - 1;

            var ctx = RequestContext{
                .source = self,
                .method = .GET,
                .allocator = self.allocator,
                .range_start = offset,
                .range_end = range_end,
                .output_buf = buf,
            };
            try ctx.run();

            if (ctx.status_code != 200 and ctx.status_code != 206) return error.S3GetFailed;
            return ctx.bytes_read;
        }

        fn size(ptr: *anyopaque) u64 {
            const self: *Self = @ptrCast(@alignCast(ptr));
            return self.content_length;
        }

        fn close(ptr: *anyopaque) void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            self.deinit();
            self.allocator.destroy(self);
        }

        /// Connection pool manages N connections with keep-alive reuse
        const ConnectionPool = struct {
            source: *Self,
            connections: [NUM_CONNECTIONS]PooledConnection = undefined,

            fn init(source: *Self) ConnectionPool {
                var pool = ConnectionPool{ .source = source };
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
            }

            /// Get an idle connection, or the first one if none idle
            fn acquire(self: *ConnectionPool) *PooledConnection {
                // Find an idle connection that's ready
                for (&self.connections) |*pc| {
                    if (pc.state == .idle and pc.connection_ready) {
                        return pc;
                    }
                }
                // Find any idle connection
                for (&self.connections) |*pc| {
                    if (pc.state == .idle) {
                        return pc;
                    }
                }
                // All busy - use first one (will wait)
                return &self.connections[0];
            }

            fn fixPointers(self: *ConnectionPool) void {
                for (&self.connections) |*pc| {
                    pc.pool = self;
                }
            }
        };

        const ConnectionState = enum {
            idle,
            resolving,
            connecting,
            handshaking,
            sending,
            reading,
        };

        const PooledConnection = struct {
            pool: *ConnectionPool,
            index: usize,
            conn: ?*Conn = null,
            state: ConnectionState = .idle,
            connection_ready: bool = false,

            fn ensureConnected(self: *PooledConnection, ctx: *RequestContext) !void {
                if (self.connection_ready and self.conn != null) {
                    // Reuse existing connection
                    const c = self.conn.?;
                    c.stopped = false;
                    c.callback_ctx = ctx;
                    c.on_data = RequestContext.onData;
                    c.on_error = RequestContext.onError;
                    c.on_handshake = null; // Already connected
                    return;
                }

                // Need to establish new connection
                self.state = .resolving;
                const source = self.pool.source;

                // Use cached address if available
                if (source.resolved_addr) |addr| {
                    try self.connectTo(ctx, addr);
                } else {
                    // Need DNS resolution
                    const ResolveCtx = struct {
                        pc: *PooledConnection,
                        req_ctx: *RequestContext,
                    };
                    const resolve_ctx = try source.allocator.create(ResolveCtx);
                    resolve_ctx.* = .{ .pc = self, .req_ctx = ctx };

                    try source.resolver.resolve(
                        source.loop,
                        source.host,
                        443,
                        resolve_ctx,
                        struct {
                            fn cb(ptr: ?*anyopaque, addr: ?transport.Address) void {
                                const rctx: *ResolveCtx = @ptrCast(@alignCast(ptr));
                                defer rctx.pc.pool.source.allocator.destroy(rctx);

                                if (addr) |a| {
                                    // Cache the address
                                    rctx.pc.pool.source.resolved_addr = a;
                                    rctx.pc.connectTo(rctx.req_ctx, a) catch |err| {
                                        rctx.req_ctx.err = err;
                                        rctx.req_ctx.done = true;
                                    };
                                } else {
                                    rctx.req_ctx.err = error.ResolutionFailed;
                                    rctx.req_ctx.done = true;
                                }
                            }
                        }.cb,
                    );
                }
            }

            fn connectTo(self: *PooledConnection, ctx: *RequestContext, addr: transport.Address) !void {
                self.state = .connecting;
                const source = self.pool.source;

                // Clean up old connection if any
                if (self.conn) |c| c.deinit();
                self.connection_ready = false;

                self.conn = try Conn.init(source.allocator, source.loop, true, source.host);
                const conn = self.conn.?;
                conn.callback_ctx = ctx;
                conn.on_data = RequestContext.onData;
                conn.on_error = RequestContext.onError;
                conn.on_handshake = RequestContext.onHandshake;

                try conn.connect(addr);
                self.state = .handshaking;
            }

            fn markReady(self: *PooledConnection) void {
                self.connection_ready = true;
                self.state = .idle;
            }

            fn markFailed(self: *PooledConnection) void {
                self.connection_ready = false;
                self.state = .idle;
            }
        };

        const RequestContext = struct {
            source: *Self,
            method: enum { HEAD, GET },
            allocator: std.mem.Allocator,
            range_start: u64 = 0,
            range_end: u64 = 0,
            output_buf: []u8 = &[_]u8{},

            // Which pooled connection we're using
            pooled_conn: ?*PooledConnection = null,

            // Result state
            done: bool = false,
            status_code: u16 = 0,
            content_length: u64 = 0,
            bytes_read: usize = 0,
            err: ?anyerror = null,

            // Internal parser state
            parser: protocol_http.ResponseParser = .{},

            fn run(self: *RequestContext) !void {
                return self.runWithRetry(true);
            }

            fn runWithRetry(self: *RequestContext, allow_retry: bool) !void {
                self.parser = .{ .is_head = (self.method == .HEAD) };
                self.done = false;
                self.err = null;
                self.bytes_read = 0;

                // Fix pool pointers (in case source was moved)
                self.source.pool.source = self.source;
                self.source.pool.fixPointers();

                // Acquire a connection from the pool
                const pc = self.source.pool.acquire();
                self.pooled_conn = pc;
                const was_reused = pc.connection_ready;

                // Ensure we're connected (reuses if possible)
                try pc.ensureConnected(self);

                // If already connected, send immediately
                if (pc.connection_ready) {
                    try self.sendRequest();
                }
                // Otherwise, onHandshake will call sendRequest

                while (!self.done) {
                    try self.source.loop.run(.once);
                }

                // Handle connection errors - retry once if we were reusing
                if (self.err != null and was_reused and allow_retry) {
                    // Connection was stale, force reconnect and retry
                    pc.markFailed();
                    if (pc.conn) |c| {
                        c.deinit();
                        pc.conn = null;
                    }
                    return self.runWithRetry(false);
                }

                // Mark connection as ready for reuse (unless error)
                if (self.err == null) {
                    pc.markReady();
                } else {
                    pc.markFailed();
                }

                if (self.err) |e| return e;
            }

            fn onHandshake(ptr: ?*anyopaque) void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                if (self.pooled_conn) |pc| {
                    pc.connection_ready = true;
                    pc.state = .sending;
                }
                self.sendRequest() catch |e| {
                    self.err = e;
                    self.done = true;
                };
            }

            fn sendRequest(self: *RequestContext) !void {
                var req_buf = std.ArrayListUnmanaged(u8){};
                defer req_buf.deinit(self.allocator);

                const path = try std.fmt.allocPrint(self.allocator, "/{s}", .{self.source.key});
                defer self.allocator.free(path);

                const signed_headers = switch (self.method) {
                    .HEAD => try self.source.s3.formatHeadRequest(self.allocator, self.source.key, .{}),
                    .GET => try self.source.s3.formatGetRequest(self.allocator, self.source.key, .{ .start = self.range_start, .end = self.range_end + 1 }, .{}),
                };
                defer {
                    for (signed_headers) |h| {
                        self.allocator.free(h.name);
                        self.allocator.free(h.value);
                    }
                    self.allocator.free(signed_headers);
                }

                const method_str = switch (self.method) {
                    .HEAD => "HEAD",
                    .GET => "GET",
                };
                try req_buf.appendSlice(self.allocator, method_str);
                try req_buf.appendSlice(self.allocator, " ");
                try req_buf.appendSlice(self.allocator, path);
                try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");
                for (signed_headers) |h| {
                    try req_buf.appendSlice(self.allocator, h.name);
                    try req_buf.appendSlice(self.allocator, ": ");
                    try req_buf.appendSlice(self.allocator, h.value);
                    try req_buf.appendSlice(self.allocator, "\r\n");
                }
                try req_buf.appendSlice(self.allocator, "Connection: keep-alive\r\n\r\n");

                if (self.pooled_conn) |pc| {
                    pc.state = .reading;
                    if (pc.conn) |conn| {
                        try conn.write(req_buf.items);
                    }
                }
            }

            fn onData(ptr: ?*anyopaque, data: []const u8) anyerror!void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                const BodyCtx = struct {
                    ctx: *RequestContext,
                    fn onBody(ctx_ptr: *anyopaque, chunk: []const u8) anyerror!void {
                        const bctx: *@This() = @ptrCast(@alignCast(ctx_ptr));
                        const me = bctx.ctx;

                        if (me.method == .HEAD) return;

                        if (me.bytes_read + chunk.len > me.output_buf.len) {
                            return error.BufferOverflow;
                        }
                        @memcpy(me.output_buf[me.bytes_read .. me.bytes_read + chunk.len], chunk);
                        me.bytes_read += chunk.len;
                    }
                };
                var bctx = BodyCtx{ .ctx = self };

                try self.parser.feed(data, &bctx, BodyCtx.onBody);

                if (self.parser.state == .done) {
                    self.status_code = self.parser.status_code;
                    if (self.parser.content_length) |cl| {
                        self.content_length = @intCast(cl);
                    }
                    // Stop the connection from scheduling more reads
                    if (self.pooled_conn) |pc| {
                        if (pc.conn) |conn| {
                            conn.stopped = true;
                        }
                    }
                    self.done = true;
                }
            }

            fn onError(ptr: ?*anyopaque, err: anyerror) void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                // If we already have a complete response, ignore connection close errors
                if (self.done or self.parser.state == .done) return;
                self.err = err;
                self.done = true;
            }
        };
    };
}
