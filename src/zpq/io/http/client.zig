const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const global_pool_mod = @import("../pool.zig");
const ResponseParser = @import("response_parser.zig").ResponseParser;
const dns = @import("../dns.zig");
const tracer = @import("../../trace.zig");

const log = @import("../../../zpq.zig").log.http;

pub fn ClientGen(comptime XevApi: type) type {
    const LoopType = XevApi.Loop;
    const GlobalConnectionPool = global_pool_mod.GlobalConnectionPool(XevApi);
    const Connection = tls.ConnectionGen(XevApi);
    const ConnectionKey = global_pool_mod.ConnectionKey;
    const Resolver = dns.ResolverGen(XevApi);
    const ThreadPoolResolver = dns.ThreadPoolResolverGen(XevApi);

    return struct {
        const Self = @This();

        // Configuration
        allocator: std.mem.Allocator,
        loop: *LoopType,
        pool: *GlobalConnectionPool,
        resolver: Resolver,
        // Optional own resolver if we created it
        tp_resolver: ?*ThreadPoolResolver = null,

        // Target
        host: []const u8,
        port: u16,
        use_tls: bool,

        // Cached DNS
        cached_addr: ?xev.shim_net.Address = null,

        pub fn init(
            allocator: std.mem.Allocator,
            loop: *LoopType,
            pool: *GlobalConnectionPool,
            thread_pool: *xev.ThreadPool,
            host: []const u8,
            port: u16,
            use_tls: bool,
        ) !*Self {
            const self = try allocator.create(Self);

            // Create a thread pool resolver since we need async DNS
            // Note: In a real app we might share this resolver, but for now we create one per client if not provided
            const tp_resolver = try allocator.create(ThreadPoolResolver);
            tp_resolver.* = ThreadPoolResolver.init(thread_pool, allocator);

            self.* = .{
                .allocator = allocator,
                .loop = loop,
                .pool = pool,
                .resolver = tp_resolver.resolver(),
                .tp_resolver = tp_resolver,
                .host = try allocator.dupe(u8, host),
                .port = port,
                .use_tls = use_tls,
            };
            return self;
        }

        /// Init with existing resolver (e.g. from factory)
        pub fn initWithResolver(
            allocator: std.mem.Allocator,
            loop: *LoopType,
            pool: *GlobalConnectionPool,
            resolver: Resolver,
            host: []const u8,
            port: u16,
            use_tls: bool,
        ) !*Self {
            const self = try allocator.create(Self);
            self.* = .{
                .allocator = allocator,
                .loop = loop,
                .pool = pool,
                .resolver = resolver,
                .tp_resolver = null,
                .host = try allocator.dupe(u8, host),
                .port = port,
                .use_tls = use_tls,
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.host);
            if (self.tp_resolver) |r| {
                self.allocator.destroy(r);
            }
            self.allocator.destroy(self);
        }

        pub const RequestResult = struct {
            body: []const u8,
            headers: []ResponseHeader,
            status: u16,

            pub fn deinit(self: RequestResult, allocator: std.mem.Allocator) void {
                allocator.free(self.body);
                for (self.headers) |h| {
                    allocator.free(h.name);
                    allocator.free(h.value);
                }
                allocator.free(self.headers);
            }
        };

        pub const ResponseHeader = struct {
            name: []const u8,
            value: []const u8,
        };

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

        /// Async request context - heap allocated, persists until completion callback is invoked.
        /// Used when caller wants to drive the event loop themselves (avoiding nested loop.run()).
        pub const AsyncRequestContext = struct {
            allocator: std.mem.Allocator,
            client: *Self,
            parser: ResponseParser = .{},
            body_buf: std.ArrayListUnmanaged(u8) = .{},
            conn: *Connection = undefined,
            pool: *GlobalConnectionPool = undefined,
            key: ConnectionKey = undefined,

            // Request data (owned by this context)
            request_buf: []u8 = &[_]u8{},
            body_data: []const u8 = &[_]u8{},

            // State machine
            phase: Phase = .connecting,
            body_offset: usize = 0,
            done: bool = false,
            err: ?anyerror = null,

            // Completion callback
            on_complete: ?*const fn (*AsyncRequestContext, anyerror!RequestResult) void = null,
            user_data: ?*anyopaque = null,

            pub const Phase = enum {
                connecting,
                sending_headers,
                sending_body,
                waiting_response,
                done,
            };

            fn onBodyChunk(ctx_ptr: *anyopaque, chunk: []const u8) void {
                const self: *AsyncRequestContext = @ptrCast(@alignCast(ctx_ptr));
                self.body_buf.appendSlice(self.allocator, chunk) catch |e| {
                    self.err = e;
                };
            }

            pub fn deinit(self: *AsyncRequestContext) void {
                self.body_buf.deinit(self.allocator);
                if (self.request_buf.len > 0) {
                    self.allocator.free(self.request_buf);
                }
            }

            /// Check if request is complete (success or error)
            pub fn isComplete(self: *const AsyncRequestContext) bool {
                return self.done or self.err != null;
            }

            /// Get result if complete, null otherwise
            pub fn getResult(self: *AsyncRequestContext) ?anyerror!RequestResult {
                if (!self.isComplete()) return null;

                if (self.err) |e| return e;

                // Extract headers
                var result_headers = std.ArrayListUnmanaged(ResponseHeader){};
                errdefer result_headers.deinit(self.allocator);

                const header_str = self.parser.header_buf[0..self.parser.header_len];
                var lines = std.mem.splitSequence(u8, header_str, "\r\n");
                _ = lines.first(); // Skip status

                while (lines.next()) |line| {
                    if (line.len == 0) break;
                    if (std.mem.indexOf(u8, line, ": ")) |colon_pos| {
                        result_headers.append(self.allocator, .{
                            .name = self.allocator.dupe(u8, line[0..colon_pos]) catch |e| return e,
                            .value = self.allocator.dupe(u8, line[colon_pos + 2 ..]) catch |e| return e,
                        }) catch |e| return e;
                    }
                }

                return RequestResult{
                    .body = self.allocator.dupe(u8, self.body_buf.items) catch |e| return e,
                    .headers = result_headers.toOwnedSlice(self.allocator) catch |e| return e,
                    .status = self.parser.status_code,
                };
            }
        };

        fn onConnect(ctx_void: ?*anyopaque) void {
            _ = ctx_void;
        }

        // =====================================================================
        // Async request callbacks (for requestAsync() - no loop.run() inside)
        // =====================================================================

        fn onAsyncConnect(ctx_void: ?*anyopaque) void {
            const ctx: *AsyncRequestContext = @ptrCast(@alignCast(ctx_void orelse return));
            ctx.phase = .sending_headers;
            // Write headers (will trigger onAsyncWriteComplete when done)
            ctx.conn.write(ctx.request_buf) catch |e| {
                ctx.err = e;
                ctx.done = true;
            };
        }

        fn onAsyncWriteComplete(ctx_void: ?*anyopaque, _: usize) void {
            const ctx: *AsyncRequestContext = @ptrCast(@alignCast(ctx_void orelse return));

            switch (ctx.phase) {
                .sending_headers => {
                    if (ctx.body_data.len > 0) {
                        ctx.phase = .sending_body;
                        ctx.body_offset = 0;
                        writeNextAsyncChunk(ctx);
                    } else {
                        ctx.phase = .waiting_response;
                    }
                },
                .sending_body => {
                    const chunk_size = 8 * 1024;
                    ctx.body_offset += chunk_size;
                    if (ctx.body_offset >= ctx.body_data.len) {
                        ctx.phase = .waiting_response;
                    } else {
                        writeNextAsyncChunk(ctx);
                    }
                },
                else => {},
            }
        }

        fn writeNextAsyncChunk(ctx: *AsyncRequestContext) void {
            const chunk_size = 8 * 1024;
            const end = @min(ctx.body_offset + chunk_size, ctx.body_data.len);
            const chunk = ctx.body_data[ctx.body_offset..end];
            ctx.conn.write(chunk) catch |e| {
                ctx.err = e;
                ctx.done = true;
            };
        }

        fn onAsyncData(ctx_void: ?*anyopaque, data: []const u8) void {
            const ctx: *AsyncRequestContext = @ptrCast(@alignCast(ctx_void orelse return));

            ctx.parser.feed(data, ctx, AsyncRequestContext.onBodyChunk) catch |e| {
                ctx.err = e;
                ctx.done = true;
                return;
            };

            if (ctx.parser.state == .done) {
                ctx.done = true;
                ctx.phase = .done;
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);

                // Call completion callback if set
                if (ctx.on_complete) |cb| {
                    const result = ctx.getResult() orelse unreachable;
                    cb(ctx, result);
                }
            }
        }

        fn onAsyncError(ctx_void: ?*anyopaque, err: anyerror) void {
            const ctx: *AsyncRequestContext = @ptrCast(@alignCast(ctx_void orelse return));
            if ((err == error.EOF or err == error.TlsConnectionClosed) and ctx.parser.headersComplete()) {
                ctx.done = true;
                ctx.phase = .done;
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);

                if (ctx.on_complete) |cb| {
                    const result = ctx.getResult() orelse unreachable;
                    cb(ctx, result);
                }
                return;
            }
            ctx.err = err;
            ctx.done = true;
            ctx.conn.close();

            if (ctx.on_complete) |cb| {
                cb(ctx, err);
            }
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
                ctx.conn.user_ctx = null;
                ctx.conn.idling = true;
                ctx.pool.release(ctx.key, ctx.conn);
            }
        }

        fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
            const ctx: *RequestContext = @ptrCast(@alignCast(ctx_void orelse return));
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

        /// Perform an HTTP request.
        /// headers: Array of std.http.Header. Caller owns the memory.
        pub fn request(
            self: *Self,
            method: []const u8,
            path: []const u8,
            headers: []const std.http.Header,
            body: []const u8,
        ) !RequestResult {
            // Resolve if needed
            if (self.cached_addr == null) {
                self.cached_addr = try self.resolve();
            }
            const addr = self.cached_addr.?;

            const key = ConnectionKey{
                .host = self.host,
                .port = self.port,
                .use_tls = self.use_tls,
            };

            // Acquire connection
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

            // Build request bytes
            var req_buf = std.ArrayListUnmanaged(u8){};
            defer req_buf.deinit(self.allocator);

            try req_buf.appendSlice(self.allocator, method);
            try req_buf.appendSlice(self.allocator, " ");
            try req_buf.appendSlice(self.allocator, path);
            try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");

            for (headers) |h| {
                try req_buf.appendSlice(self.allocator, h.name);
                try req_buf.appendSlice(self.allocator, ": ");
                try req_buf.appendSlice(self.allocator, h.value);
                try req_buf.appendSlice(self.allocator, "\r\n");
            }

            // Content-Length
            try req_buf.appendSlice(self.allocator, "Content-Length: ");
            var len_buf: [20]u8 = undefined;
            const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch unreachable;
            try req_buf.appendSlice(self.allocator, len_str);
            try req_buf.appendSlice(self.allocator, "\r\n\r\n");

            // Setup context
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

            // Connect
            if (!conn.handshake_complete) {
                const tls_zone = tracer.zone("HTTP/tlsHandshake");
                defer tls_zone.end();
                try conn.connect(addr);
                while (!conn.handshake_complete and !ctx.done and ctx.err == null) {
                    try self.loop.run(.once);
                }
                if (ctx.err) |err| return err;
            }

            // Wait for pending writes
            while (conn.pending_write and !ctx.done and ctx.err == null) {
                try self.loop.run(.once);
            }
            if (ctx.err) |err| return err;

            // Send headers
            log.debug("Sending {s} {s} ({d} body bytes)", .{ method, path, body.len });
            try conn.write(req_buf.items);

            // Send body
            if (body.len > 0) {
                const upload_zone = tracer.zone("HTTP/uploadBody");
                defer upload_zone.end();
                while (conn.pending_write and !ctx.done and ctx.err == null) {
                    try self.loop.run(.once);
                }
                if (ctx.err) |err| return err;

                const chunk_size = 8 * 1024;
                var offset: usize = 0;
                while (offset < body.len) {
                    const end = @min(offset + chunk_size, body.len);
                    try conn.write(body[offset..end]);

                    while (conn.pending_write and !ctx.done and ctx.err == null) {
                        try self.loop.run(.once);
                    }
                    if (ctx.err) |err| return err;
                    offset = end;
                }
            }

            // Wait for response
            {
                const resp_zone = tracer.zone("HTTP/waitResponse");
                defer resp_zone.end();
                while (!ctx.done and ctx.err == null) {
                    try self.loop.run(.once);
                }
            }
            if (ctx.err) |err| return err;

            if (ctx.parser.status_code >= 400) {
                log.err("Request failed: {d}", .{ctx.parser.status_code});
            }

            // Extract headers
            var result_headers = std.ArrayListUnmanaged(ResponseHeader){};
            defer result_headers.deinit(self.allocator);

            const header_str = ctx.parser.header_buf[0..ctx.parser.header_len];
            var lines = std.mem.splitSequence(u8, header_str, "\r\n");
            _ = lines.first(); // Skip status

            while (lines.next()) |line| {
                if (line.len == 0) break;
                if (std.mem.indexOf(u8, line, ": ")) |colon_pos| {
                    try result_headers.append(self.allocator, .{
                        .name = try self.allocator.dupe(u8, line[0..colon_pos]),
                        .value = try self.allocator.dupe(u8, line[colon_pos + 2 ..]),
                    });
                }
            }

            return .{
                .body = try self.allocator.dupe(u8, ctx.body_buf.items),
                .headers = try result_headers.toOwnedSlice(self.allocator),
                .status = ctx.parser.status_code,
            };
        }

        /// Perform an HTTP request asynchronously.
        /// Returns a heap-allocated context that tracks the request state.
        /// The caller must drive the event loop (loop.run()) until ctx.isComplete() returns true.
        /// Then call ctx.getResult() to get the result, and ctx.deinit() + allocator.destroy(ctx).
        ///
        /// NOTE: The caller must ensure `body` remains valid until the request completes.
        pub fn requestAsync(
            self: *Self,
            method: []const u8,
            path: []const u8,
            headers: []const std.http.Header,
            body: []const u8,
            addr: xev.shim_net.Address,
        ) !*AsyncRequestContext {
            const key = ConnectionKey{
                .host = self.host,
                .port = self.port,
                .use_tls = self.use_tls,
            };

            // Acquire connection
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

            // Build request bytes (owned by context)
            var req_buf = std.ArrayListUnmanaged(u8){};
            errdefer req_buf.deinit(self.allocator);

            try req_buf.appendSlice(self.allocator, method);
            try req_buf.appendSlice(self.allocator, " ");
            try req_buf.appendSlice(self.allocator, path);
            try req_buf.appendSlice(self.allocator, " HTTP/1.1\r\n");

            for (headers) |h| {
                try req_buf.appendSlice(self.allocator, h.name);
                try req_buf.appendSlice(self.allocator, ": ");
                try req_buf.appendSlice(self.allocator, h.value);
                try req_buf.appendSlice(self.allocator, "\r\n");
            }

            // Content-Length
            try req_buf.appendSlice(self.allocator, "Content-Length: ");
            var len_buf: [20]u8 = undefined;
            const len_str = std.fmt.bufPrint(&len_buf, "{d}", .{body.len}) catch unreachable;
            try req_buf.appendSlice(self.allocator, len_str);
            try req_buf.appendSlice(self.allocator, "\r\n\r\n");

            // Create heap-allocated async context
            const ctx = try self.allocator.create(AsyncRequestContext);
            errdefer self.allocator.destroy(ctx);

            ctx.* = .{
                .allocator = self.allocator,
                .client = self,
                .conn = conn,
                .pool = self.pool,
                .key = key,
                .request_buf = try req_buf.toOwnedSlice(self.allocator),
                .body_data = body,
            };

            // Setup async callbacks
            conn.user_ctx = ctx;
            conn.on_connect = onAsyncConnect;
            conn.on_data = onAsyncData;
            conn.on_error = onAsyncError;
            conn.on_write_complete = onAsyncWriteComplete;

            // Initiate connection (non-blocking)
            if (!conn.handshake_complete) {
                try conn.connect(addr);
            } else {
                // Already connected, immediately send headers
                ctx.phase = .sending_headers;
                try conn.write(ctx.request_buf);
            }

            return ctx;
        }

        /// Helper to get cached address or resolve DNS.
        /// This still calls loop.run() internally for DNS resolution.
        /// For fully async flow, caller should resolve DNS separately.
        pub fn getOrResolveAddr(self: *Self) !xev.shim_net.Address {
            if (self.cached_addr == null) {
                self.cached_addr = try self.resolve();
            }
            return self.cached_addr.?;
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
    };
}

pub const Client = ClientGen(xev);
