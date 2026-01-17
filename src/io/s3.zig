const std = @import("std");
const io = @import("interface.zig");
const transport = @import("transport.zig");
const protocol_s3 = @import("../protocol/s3.zig");
const protocol_http = @import("../protocol/http.zig");
const sigv4 = @import("../protocol/sigv4.zig");
const xev_mod = @import("xev");

/// Number of parallel connections in the pool
const NUM_CONNECTIONS = 16;
const COALESCE_THRESHOLD = 64 * 1024; // 64KB

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

        /// Speculative tail cache (usually contains the Parquet footer)
        tail_buffer: []u8 = &[_]u8{},
        tail_offset: u64 = 0,

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
            
            // Ensure we clean up if fetch fails
            errdefer {
                self.pool.deinit();
                allocator.free(host);
                if (self.tail_buffer.len > 0) allocator.free(self.tail_buffer);
            }

            // Fetch tail (speculative footer + size) in one go
            try self.fetchTail();
            return self;
        }

        pub fn deinit(self: *Self) void {
            self.pool.deinit();
            self.allocator.free(self.host);
            if (self.tail_buffer.len > 0) self.allocator.free(self.tail_buffer);
        }

        pub fn randomAccessSource(self: *Self) io.RandomAccessSource {
            return .{
                .ptr = self,
                .vtable = &.{
                    .readAt = readAt,
                    .readRanges = readRanges,
                    .readRangesAsync = readRangesAsync,
                    .size = size,
                    .close = close,
                },
            };
        }

        /// Read multiple ranges in parallel using the connection pool
        fn readRanges(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            if (ranges.len != buffers.len) return error.InvalidArgs;
            if (ranges.len == 0) return;

            // Fix pool pointers
            self.pool.source = self;
            self.pool.fixPointers();

            // 1. Coalescing Pass
            // We want to merge ranges that are "close" to each other to reduce HTTP overhead.
            var sorted_indices = try self.allocator.alloc(usize, ranges.len);
            defer self.allocator.free(sorted_indices);
            for (sorted_indices, 0..) |*idx, i| idx.* = i;

            // Sort indices by range start
            const SortCtx = struct {
                r: []const io.Range,
                fn lessThan(ctx: @This(), lo: usize, hi: usize) bool {
                    return ctx.r[lo].start < ctx.r[hi].start;
                }
            };
            std.sort.pdq(usize, sorted_indices, SortCtx{ .r = ranges }, SortCtx.lessThan);

            var coalesced_ranges = std.ArrayListUnmanaged(RequestContext){};
            defer {
                for (coalesced_ranges.items) |*ctx| {
                    ctx.sub_ranges.deinit(self.allocator);
                }
                coalesced_ranges.deinit(self.allocator);
            }

            if (ranges.len > 0) {
                var i: usize = 0;
                while (i < sorted_indices.len) {
                    const first_idx = sorted_indices[i];
                    const start = ranges[first_idx].start;
                    var end = ranges[first_idx].end;
                    
                    var sub_ranges = std.ArrayListUnmanaged(SubRange){};
                    try sub_ranges.append(self.allocator, .{
                        .offset_in_coalesced = 0,
                        .len = ranges[first_idx].len(),
                        .dest = buffers[first_idx],
                    });

                    i += 1;
                    while (i < sorted_indices.len) {
                        const next_idx = sorted_indices[i];
                        const next_start = ranges[next_idx].start;
                        const next_end = ranges[next_idx].end;

                        // If gap is small enough, merge
                        if (next_start >= end and next_start - end <= COALESCE_THRESHOLD) {
                            try sub_ranges.append(self.allocator, .{
                                .offset_in_coalesced = next_start - start,
                                .len = ranges[next_idx].len(),
                                .dest = buffers[next_idx],
                            });
                            end = @max(end, next_end);
                            i += 1;
                        } else {
                            break;
                        }
                    }

                    try coalesced_ranges.append(self.allocator, RequestContext{
                        .source = self,
                        .method = .GET,
                        .allocator = self.allocator,
                        .range_start = start,
                        .range_end = end - 1,
                        .sub_ranges = sub_ranges,
                        .parser = .{},
                    });
                }
            }

            // 2. Dispatch all requests
            const contexts = coalesced_ranges.items;
            for (contexts) |*ctx| {
                try self.pool.dispatch(ctx);
            }

            // 3. Drive loop until all completed
            while (true) {
                try self.loop.run(.once);
                
                var done_count: usize = 0;
                for (contexts) |*ctx| {
                    if (ctx.done) done_count += 1;
                }
                if (done_count == contexts.len) break;
            }

            // Check for errors
            for (contexts) |ctx| {
                if (ctx.err) |e| return e;
                if (ctx.status_code != 200 and ctx.status_code != 206) {
                    std.debug.print("[S3Source] readRanges failed with status {d} for range {d}-{d}\n", .{ctx.status_code, ctx.range_start, ctx.range_end});
                    return error.S3GetFailed;
                }
            }
        }

        fn readRangesAsync(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8, cb: *const fn (ptr: ?*anyopaque, err: ?anyerror) void, ctx_ptr: ?*anyopaque) anyerror!void {
            const self: *Self = @ptrCast(@alignCast(ptr));
            // Fix pool pointers as this struct may have moved
            self.pool.source = self;
            self.pool.fixPointers();

            if (ranges.len != buffers.len) return error.InvalidArgs;
            if (ranges.len == 0) {
                cb(ctx_ptr, null);
                return;
            }

            // For now, we only support a single coalesced range for simplicity in async
            // (Wait, S3Source already has coalescing logic for readRanges, I should reuse it!)
            // Actually, for Phase 2 Conductor Model, we usually call readRangesAsync for ONE row group at a time.
            
            // IMPLEMENTATION:
            // 1. Create a master callback context that waits for ALL coalesced requests to finish.
            const MasterCtx = struct {
                allocator: std.mem.Allocator,
                cb: *const fn (ptr: ?*anyopaque, err: ?anyerror) void,
                ctx_ptr: ?*anyopaque,
                remaining: usize,
                err: ?anyerror = null,
                
                fn checkDone(mctx: *@This()) void {
                    mctx.remaining -= 1;
                    if (mctx.remaining == 0) {
                        mctx.cb(mctx.ctx_ptr, mctx.err);
                        mctx.allocator.destroy(mctx);
                    }
                }
            };
            
            // Reuse coalescing logic (simplified for heap-allocated contexts)
            // ... (I'll implement a more direct async version for now)
            
            // To be robust, let's just use RequestContext.start() for each range separately 
            // if they are many, but S3 likes coalescing.
            
            // FOR NOW: Treat it as a single request if length is 1, or error/coalesce if more.
            // If ranges.len > 1, we should coalesce.
            
            // Actually, I'll just implement the parallel version since I added the queue.
            const mctx = try self.allocator.create(MasterCtx);
            mctx.* = .{
                .allocator = self.allocator,
                .cb = cb,
                .ctx_ptr = ctx_ptr,
                .remaining = ranges.len,
            };

            for (ranges, 0..) |r, i| {
                const rctx = try self.allocator.create(RequestContext);
                rctx.* = .{
                    .source = self,
                    .method = .GET,
                    .allocator = self.allocator,
                    .range_start = r.start,
                    .range_end = r.end - 1,
                    .output_buf = buffers[i],
                    .callback = struct {
                        fn call(done_ctx: *RequestContext) void {
                            const master: *MasterCtx = @ptrCast(@alignCast(done_ctx.callback_ctx.?));
                            if (done_ctx.err) |e| master.err = e;
                            const allocator = done_ctx.allocator;
                            master.checkDone();
                            allocator.destroy(done_ctx);
                        }
                    }.call,
                };
                rctx.callback_ctx = mctx; // Need to add this field to RequestContext too!
                try rctx.start();
            }
        }

        fn fetchTail(self: *Self) !void {
            const SPECULATIVE_SIZE = 256 * 1024; // Align with ParquetFile.MAX_PREFETCH
            
            // We don't know the exact size yet, so we request the end of the file.
            // S3 Range: bytes=-131072 (last 128KB)
            var ctx = RequestContext{
                .source = self,
                .method = .GET,
                .allocator = self.allocator,
                .is_tail_fetch = true,
                .output_buf = try self.allocator.alloc(u8, SPECULATIVE_SIZE),
            };
            errdefer self.allocator.free(ctx.output_buf);

            try ctx.run();
            
            if (ctx.status_code != 200 and ctx.status_code != 206) return error.S3GetFailed;
            if (ctx.total_size) |ts| {
                self.content_length = ts;
            } else return error.MissingContentRange;

            // Store tail cache
            self.tail_buffer = ctx.output_buf[0..ctx.bytes_read];
            self.tail_offset = if (self.content_length > self.tail_buffer.len) 
                self.content_length - self.tail_buffer.len 
            else 
                0;

            std.debug.print("[S3Source] Collapsed Fetch: size={d} tail={d}kb\n", .{self.content_length, self.tail_buffer.len / 1024});
        }

        fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
            const self: *Self = @ptrCast(@alignCast(ptr));
            
            // 1. Check Tail Cache
            if (self.tail_buffer.len > 0 and offset >= self.tail_offset) {
                const relative_offset = offset - self.tail_offset;
                if (relative_offset + buf.len <= self.tail_buffer.len) {
                    @memcpy(buf, self.tail_buffer[relative_offset .. relative_offset + buf.len]);
                    return buf.len;
                }
            }

            // Fix pool pointers as this struct may have moved
            self.pool.source = self;
            self.pool.fixPointers();

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

            if (ctx.status_code != 200 and ctx.status_code != 206) {
                std.debug.print("[S3Source] {s} readAt failed with status {d} for range {d}-{d} (len {d})\n", .{@tagName(ctx.method), ctx.status_code, offset, range_end, buf.len});
                return error.S3GetFailed;
            }
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

        const ConnectionPool = struct {
            source: *Self,
            connections: [NUM_CONNECTIONS]PooledConnection = undefined,
            queue: std.ArrayListUnmanaged(*RequestContext) = .{},

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
                self.queue.deinit(self.source.allocator);
            }

            /// Dispatch a request context. Either uses an idle connection or queues it.
            fn dispatch(self: *ConnectionPool, ctx: *RequestContext) !void {
                // Find an idle connection
                for (&self.connections) |*pc| {
                    if (pc.state == .idle) {
                        try pc.startRequest(ctx);
                        return;
                    }
                }
                
                // All busy - queue it
                try self.queue.append(self.source.allocator, ctx);
            }

            fn onConnectionIdle(self: *ConnectionPool, pc: *PooledConnection) void {
                if (self.queue.items.len > 0) {
                    const ctx = self.queue.orderedRemove(0);
                    pc.startRequest(ctx) catch |err| {
                        ctx.err = err;
                        ctx.done = true;
                        if (ctx.callback) |cb| cb(ctx);
                        self.onConnectionIdle(pc); // Try next in queue
                    };
                }
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

            fn startRequest(self: *PooledConnection, ctx: *RequestContext) !void {
                self.state = .sending;
                ctx.pooled_conn = self;
                
                ctx.parser = .{ .is_head = (ctx.method == .HEAD) };
                ctx.done = false;
                ctx.err = null;
                ctx.bytes_read = 0;

                try self.ensureConnected(ctx);
                if (self.connection_ready) {
                    try ctx.sendRequest();
                }
            }

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
                self.pool.onConnectionIdle(self);
            }

            fn markFailed(self: *PooledConnection) void {
                self.connection_ready = false;
                self.state = .idle;
                self.pool.onConnectionIdle(self);
            }
        };

        const SubRange = struct {
            offset_in_coalesced: u64,
            len: u64,
            dest: []u8,
        };

        const RequestContext = struct {
            source: *Self,
            method: enum { HEAD, GET },
            allocator: std.mem.Allocator,
            range_start: u64 = 0,
            range_end: u64 = 0,
            is_tail_fetch: bool = false,
            
            /// Sub-ranges within the coalesced response that need copying to user buffers.
            sub_ranges: std.ArrayListUnmanaged(SubRange) = .{},
            output_buf: []u8 = &[_]u8{},

            // Which pooled connection we're using
            pooled_conn: ?*PooledConnection = null,

            // Result state
            done: bool = false,
            status_code: u16 = 0,
            content_length: u64 = 0,
            total_size: ?u64 = null,
            bytes_read: usize = 0,
            err: ?anyerror = null,
            
            callback: ?*const fn(ctx: *RequestContext) void = null,
            callback_ctx: ?*anyopaque = null,

            // Internal parser state
            parser: protocol_http.ResponseParser = .{},
            parser_pos: usize = 0,

            fn run(self: *RequestContext) !void {
                self.sub_ranges = .{};
                try self.source.pool.dispatch(self);
                while (!self.done) {
                    if (self.pooled_conn) |pc| {
                        if (pc.conn) |c| {
                            if (c.closed) {
                                return error.ConnectionClosedUnexpectedly;
                            }
                        }
                    }
                    try self.source.loop.run(.once);
                }
                if (self.err) |e| return e;
            }

            fn start(self: *RequestContext) !void {
                self.sub_ranges = .{};
                try self.source.pool.dispatch(self);
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
                    .GET => if (self.is_tail_fetch)
                        try self.source.s3.formatGetRequest(self.allocator, self.source.key, .{ .suffix = self.output_buf.len }, .{})
                    else
                        try self.source.s3.formatGetRequest(self.allocator, self.source.key, .{ .bytes = .{ .start = self.range_start, .end = self.range_end + 1 } }, .{}),
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

                        // Map chunk back to original user buffers
                        for (me.sub_ranges.items) |sr| {
                            const chunk_start = me.parser_pos;
                            const chunk_end = me.parser_pos + chunk.len;
                            const sr_end = sr.offset_in_coalesced + sr.len;

                            // Intersection of [chunk_start, chunk_end) and [sr.offset, sr_end)
                            const intersect_start = @max(chunk_start, sr.offset_in_coalesced);
                            const intersect_end = @min(chunk_end, sr_end);

                            if (intersect_start < intersect_end) {
                                const dest_offset = intersect_start - sr.offset_in_coalesced;
                                const src_offset = intersect_start - chunk_start;
                                const intersect_len = intersect_end - intersect_start;
                                @memcpy(sr.dest[dest_offset .. dest_offset + intersect_len], chunk[src_offset .. src_offset + intersect_len]);
                            }
                        }

                        // Also handle single output_buf for readAt/fetchSize
                        if (me.output_buf.len > 0) {
                            const clen = chunk.len;
                            const remaining = me.output_buf.len - me.bytes_read;
                            const to_copy = @min(clen, remaining);
                            if (to_copy > 0) {
                                @memcpy(me.output_buf[me.bytes_read .. me.bytes_read + to_copy], chunk[0..to_copy]);
                                me.bytes_read += to_copy;
                            }
                        }
                        
                        me.parser_pos += chunk.len;
                    }
                };
                var bctx = BodyCtx{ .ctx = self };

                try self.parser.feed(data, &bctx, BodyCtx.onBody);

                if (self.parser.state == .done) {
                    self.status_code = self.parser.status_code;
                    if (self.parser.content_length) |cl| {
                        self.content_length = @intCast(cl);
                    }
                    self.total_size = self.parser.total_size;
                    // Stop the connection from scheduling more reads
                    if (self.pooled_conn) |pc| {
                        if (pc.conn) |conn| {
                            conn.stopped = true;
                        }
                    }
                    self.done = true;
                    if (self.callback) |cb| cb(self);
                }
            }

            fn onError(ptr: ?*anyopaque, err: anyerror) void {
                const self: *RequestContext = @ptrCast(@alignCast(ptr));
                // If we already have a complete response, ignore connection close errors
                if (self.done or self.parser.state == .done) return;
                self.err = err;
                self.done = true;
                if (self.callback) |cb| cb(self);
            }
        };

    };
}
