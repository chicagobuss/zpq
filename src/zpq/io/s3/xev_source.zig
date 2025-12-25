const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const SigV4 = @import("sigv4.zig").SigV4;
const io = @import("../interface.zig");
const dns = @import("dns.zig");
const pool_mod = @import("xev_connection_pool.zig");
const XevConnectionPool = pool_mod.XevConnectionPool;
const ConnectionKey = pool_mod.ConnectionKey;

const log = @import("std").log.scoped(.s3_source);

/// A cross-platform S3 Source that uses libxev + boring_tls.
/// Implements io.RandomAccessSource for ParquetFile.
/// This source owns its own EventLoop and blocks on read calls (spinning the loop).
pub const XevS3Source = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: dns.ThreadPoolResolver,
    pool: XevConnectionPool,

    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    region: []const u8,
    use_tls: bool,
    port: u16,

    // Auth (optional)
    access_key: ?[]const u8 = null,
    secret_key: ?[]const u8 = null,
    session_token: ?[]const u8 = null,

    // Cached DNS result
    cached_addr: ?xev.shim_net.Address = null,
    
    // File size (fetched on init or on demand)
    file_size: u64 = 0,

    pub fn init(
        allocator: std.mem.Allocator,
        host: []const u8,
        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        use_tls: bool,
        port: u16,
    ) !*XevS3Source {
        const self = try allocator.create(XevS3Source);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.loop = try allocator.create(xev.Loop);
        errdefer allocator.destroy(self.loop);
        self.loop.* = try xev.Loop.init(.{});
        
        self.thread_pool = try allocator.create(xev.ThreadPool);
        errdefer {
            self.loop.deinit();
            allocator.destroy(self.loop);
            allocator.destroy(self.thread_pool);
        }
        self.thread_pool.* = xev.ThreadPool.init(.{});
        
        self.resolver = dns.ThreadPoolResolver.init(self.thread_pool, allocator);
        self.pool = XevConnectionPool.init(allocator);
        
        self.host = try allocator.dupe(u8, host);
        self.bucket = try allocator.dupe(u8, bucket);
        self.key = try allocator.dupe(u8, key);
        self.region = try allocator.dupe(u8, region);
        self.use_tls = use_tls;
        self.port = port;
        self.cached_addr = null;
        self.file_size = 0;
        self.access_key = null;
        self.secret_key = null;
        self.session_token = null;

        return self;
    }

    pub fn deinit(self: *XevS3Source) void {
        self.pool.deinit();
        self.resolver.deinit();
        self.thread_pool.shutdown(); // Ensure threads stop
        self.thread_pool.deinit();
        self.loop.deinit();
        
        self.allocator.destroy(self.thread_pool);
        self.allocator.destroy(self.loop);
        
        self.allocator.free(self.host);
        self.allocator.free(self.bucket);
        self.allocator.free(self.key);
        self.allocator.free(self.region);
        
        if (self.access_key) |k| self.allocator.free(k);
        if (self.secret_key) |k| self.allocator.free(k);
        if (self.session_token) |t| self.allocator.free(t);
    }

    pub fn setCredentials(self: *XevS3Source, access_key: []const u8, secret_key: []const u8, session_token: ?[]const u8) !void {
        if (self.access_key) |k| self.allocator.free(k);
        if (self.secret_key) |k| self.allocator.free(k);
        if (self.session_token) |t| self.allocator.free(t);

        self.access_key = try self.allocator.dupe(u8, access_key);
        self.secret_key = try self.allocator.dupe(u8, secret_key);
        if (session_token) |t| {
            self.session_token = try self.allocator.dupe(u8, t);
        } else {
            self.session_token = null;
        }
    }

    // --- IO Interface Implementation ---

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.readAt(offset, buf);
    }

    fn readRangesImpl(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8) anyerror!void {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        if (ranges.len == 0) return;
        
        const addr = try self.resolve();

        // 1. Merge ranges using scheduler
        var merged_list = try @import("scheduler.zig").mergeRanges(self.allocator, ranges);
        defer {
            for (merged_list.items) |*m| m.original_indices.deinit(self.allocator);
            merged_list.deinit(self.allocator);
        }

        const max_concurrency = 64; // Bumped for high-throughput S3
        var i: usize = 0;
        while (i < merged_list.items.len) {
            const batch_size = @min(max_concurrency, merged_list.items.len - i);
            const batch_merged = merged_list.items[i .. i + batch_size];

            const contexts = try self.allocator.alloc(*ReqContext, batch_size);
            defer self.allocator.free(contexts);
            
            var cleanup_idx: usize = 0;
            errdefer {
                for (0..cleanup_idx) |j| {
                    // Free the sub-range/buffer slices we allocated
                    self.allocator.free(contexts[j].sub_ranges);
                    self.allocator.free(contexts[j].dest_buffers);
                    self.allocator.destroy(contexts[j]);
                }
            }

            for (batch_merged, 0..) |merged, j| {
                const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
                const conn = if (self.pool.acquire(key)) |c| blk: {
                    c.idling = false;
                    break :blk c;
                } else blk: {
                    const c = try self.allocator.create(tls.Connection);
                    c.* = try tls.Connection.init(self.loop, self.allocator, self.host);
                    break :blk c;
                };

                // Prepare sub-ranges and buffers for this merged request
                const sub_ranges = try self.allocator.alloc(io.Range, merged.original_indices.items.len);
                const dest_buffers = try self.allocator.alloc([]u8, merged.original_indices.items.len);
                for (merged.original_indices.items, 0..) |orig_idx, k| {
                    sub_ranges[k] = ranges[orig_idx];
                    dest_buffers[k] = buffers[orig_idx];
                }

                const ctx = try self.allocator.create(ReqContext);
                contexts[j] = ctx;
                ctx.* = .{
                    .source = self,
                    .conn = conn,
                    .request_offset = merged.request_range.start,
                    .request_end = merged.request_range.end,
                    .sub_ranges = sub_ranges,
                    .dest_buffers = dest_buffers,
                    .allocator = self.allocator,
                    .parser = .{},
                };
                cleanup_idx += 1;

                conn.user_ctx = ctx;
                conn.on_connect = onConnect;
                conn.on_data = onData;
                conn.on_error = onError;

                if (!conn.handshake_complete) {
                    try conn.connect(addr);
                } else {
                    onConnect(ctx);
                }
            }

            try self.loop.run(.until_done);

            // Check for errors and cleanup contexts
            var first_err: ?anyerror = null;
            for (contexts) |ctx| {
                if (ctx.err) |err| {
                    if (err == error.EOF or err == error.TlsConnectionClosed) {
                        if (ctx.finished) {
                            self.allocator.free(ctx.sub_ranges);
                            self.allocator.free(ctx.dest_buffers);
                            self.allocator.destroy(ctx);
                            continue;
                        }
                    }
                    if (first_err == null) first_err = err;
                }
                self.allocator.free(ctx.sub_ranges);
                self.allocator.free(ctx.dest_buffers);
                self.allocator.destroy(ctx);
            }

            if (first_err) |e| return e;

            i += batch_size;
        }
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        if (self.file_size == 0) {
            self.fetchSize() catch |err| {
                log.debug("sizeImpl: fetchSize failed: {}", .{err});
                return 0;
            };
        }
        return self.file_size;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    pub fn source(self: *XevS3Source) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = readRangesImpl,
                .size = sizeImpl,
                .close = closeImpl,
            },
        };
    }

    // --- Core Logic ---

    pub fn fetchSize(self: *XevS3Source) !void {
        log.debug("fetchSize: starting HEAD request", .{});
        const addr = try self.resolve();
        
        const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
        const conn = if (self.pool.acquire(key)) |c| blk: {
            c.idling = false;
            break :blk c;
        } else blk: {
            const c = try self.allocator.create(tls.Connection);
            c.* = try tls.Connection.init(self.loop, self.allocator, self.host);
            break :blk c;
        };
        
        const ctx = try self.allocator.create(ReqContext);
        ctx.* = .{
            .source = self,
            .conn = conn,
            .request_offset = 0,
            .request_end = 0,
            .sub_ranges = &.{},
            .dest_buffers = &.{},
            .allocator = self.allocator,
            .parser = .{},
            .is_head = true,
        };
        defer self.allocator.destroy(ctx);

        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;

        if (!conn.handshake_complete) {
            try conn.connect(addr);
        } else {
            // Already connected, trigger onConnect manually
            onConnect(ctx);
        }
        
        try self.loop.run(.until_done);

        if (ctx.err) |err| {
            if (err == error.EOF or err == error.TlsConnectionClosed) {
                if (ctx.finished) return;
            }
            return err;
        }
        if (self.file_size == 0) return error.FailedToFetchSize;
    }

    pub fn readAt(self: *XevS3Source, offset: u64, buf: []u8) !usize {
        log.debug("readAt: start offset={d} len={d}", .{offset, buf.len});
        const addr = try self.resolve();

        const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
        const conn = if (self.pool.acquire(key)) |c| blk: {
            c.idling = false;
            break :blk c;
        } else blk: {
            const c = try self.allocator.create(tls.Connection);
            c.* = try tls.Connection.init(self.loop, self.allocator, self.host);
            break :blk c;
        };
        
        const ctx = try self.allocator.create(ReqContext);
        
        // Single range setup
        const range = try self.allocator.alloc(io.Range, 1);
        range[0] = .{ .start = offset, .end = offset + buf.len };
        const dest = try self.allocator.alloc([]u8, 1);
        dest[0] = buf;

        ctx.* = .{
            .source = self,
            .conn = conn,
            .request_offset = offset,
            .request_end = offset + buf.len,
            .sub_ranges = range,
            .dest_buffers = dest,
            .allocator = self.allocator,
            .parser = .{},
        };
        defer {
            self.allocator.free(range);
            self.allocator.free(dest);
            self.allocator.destroy(ctx);
        }

        conn.user_ctx = ctx;
        conn.on_connect = onConnect;
        conn.on_data = onData;
        conn.on_error = onError;

        if (!conn.handshake_complete) {
            log.debug("readAt: connecting...", .{});
            try conn.connect(addr);
        } else {
            log.debug("readAt: reusing connection...", .{});
            onConnect(ctx);
        }

        log.debug("readAt: running loop...", .{});
        try self.loop.run(.until_done);
        log.debug("readAt: loop finished total_body_read={d} finished={}", .{ctx.total_body_read, ctx.finished});

        if (ctx.err) |err| {
            if (err == error.EOF or err == error.TlsConnectionClosed) {
                if (ctx.finished) return ctx.total_body_read;
            }
            log.debug("readAt: error return {}", .{err});
            return err;
        }
        
        return ctx.total_body_read;
    }
    
    pub fn resolve(self: *XevS3Source) !xev.shim_net.Address {
        if (self.cached_addr) |a| return a;
        
        var comp = dns.Resolver.Completion.init();
        defer comp.deinit(self.allocator);
        
        const DnsCtx = struct {
            addr: ?xev.shim_net.Address = null,
            err: ?anyerror = null,
            done: bool = false,
            
            fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
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
        
        var resolver_iface = self.resolver.resolver();
        log.debug("resolve: starting resolution for {s}:{d}", .{self.host, self.port});
        resolver_iface.resolve(self.loop, self.host, self.port, &comp, DnsCtx.callback, &dctx);

        log.debug("resolve: running loop until done", .{});
        while (!dctx.done) {
            try self.loop.run(.once);
        }
        
        log.debug("resolve: done err={?}", .{dctx.err});
        if (dctx.err) |err| return err;
        if (dctx.addr) |a| {
            self.cached_addr = a;
            return a;
        }
        return error.HostNotFound;
    }

};

const ReqContext = struct {
    source: *XevS3Source,
    conn: *tls.Connection,
    
    // For coalesced reads:
    // request_offset is where the S3 GET starts (e.g. 0)
    request_offset: u64,
    request_end: u64, // The full end of the S3 request
    
    // sub_ranges are the parts we actually want (e.g. 0..100, 110..200)
    sub_ranges: []const io.Range,
    dest_buffers: []const []u8,
    
    allocator: std.mem.Allocator,
    parser: ResponseParser,
    
    // Progress tracking
    total_body_read: usize = 0,
    finished: bool = false,
    err: ?anyerror = null,
    http_status: u16 = 0,
    is_head: bool = false,
    
    pub fn deinit(self: *ReqContext) void {
        self.allocator.destroy(self);
    }
};

fn onConnect(ctx_void: ?*anyopaque) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    
    // Use an arena for building the request (headers, signature, etc.)
    var arena = std.heap.ArenaAllocator.init(ctx.allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    const path = std.fmt.allocPrint(aa, "/{s}/{s}", .{ctx.source.bucket, ctx.source.key}) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    const method = if (ctx.is_head) "HEAD" else "GET";
    
    // Build Headers
    var headers = std.ArrayListUnmanaged(std.http.Header){};
    headers.append(aa, .{ .name = "User-Agent", .value = "zpq-xev" }) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    headers.append(aa, .{ .name = "Connection", .value = "keep-alive" }) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    if (!ctx.is_head) {
        const end_inclusive = ctx.request_end - 1;
        const range_val = std.fmt.allocPrint(aa, "bytes={d}-{d}", .{ ctx.request_offset, end_inclusive }) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
        headers.append(aa, .{ .name = "Range", .value = range_val }) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
    }

    // Sign if credentials provided
    if (ctx.source.access_key) |ak| {
        if (ctx.source.secret_key) |sk| {
            const signer = SigV4{
                .region = ctx.source.region,
                .access_key = ak,
                .secret_key = sk,
                .session_token = ctx.source.session_token,
            };

            const scheme = if (ctx.source.use_tls) "https" else "http";
            const url = std.fmt.allocPrint(aa, "{s}://{s}{s}", .{ scheme, ctx.source.host, path }) catch unreachable;
            const uri = std.Uri.parse(url) catch unreachable;

            signer.sign(aa, method, uri, &headers, "") catch |err| {
                ctx.err = err;
                ctx.conn.close();
                return;
            };
        }
    } else {
        // Must add Host manually for anonymous requests (SigV4 adds it automatically)
        const host_header = if (ctx.source.port == 443 or ctx.source.port == 80)
            ctx.source.host
        else
            std.fmt.allocPrint(aa, "{s}:{d}", .{ ctx.source.host, ctx.source.port }) catch |err| {
                ctx.err = err;
                ctx.conn.close();
                return;
            };
            
        headers.append(aa, .{ .name = "Host", .value = host_header }) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
    }

    // Serialize Request
    const encoded_path = @import("sigv4.zig").encodeS3Path(aa, path) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    
    var head_list = std.ArrayListUnmanaged(u8){};
    defer head_list.deinit(aa);
    
    head_list.appendSlice(aa, method) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    head_list.appendSlice(aa, " ") catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    head_list.appendSlice(aa, encoded_path) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    head_list.appendSlice(aa, " HTTP/1.1\r\n") catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    for (headers.items) |h| {
        head_list.appendSlice(aa, h.name) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
        head_list.appendSlice(aa, ": ") catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
        head_list.appendSlice(aa, h.value) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
        head_list.appendSlice(aa, "\r\n") catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
    }
    head_list.appendSlice(aa, "\r\n") catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    log.debug("Sending Request:\n{s}", .{head_list.items});

    ctx.conn.write(head_list.items) catch |err| {
        ctx.err = err;
        ctx.conn.close();
    };
}

fn onData(ctx_void: ?*anyopaque, data: []const u8) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    log.debug("onData: received {d} bytes", .{data.len});
    
    ctx.parser.feed(data, ctx, onBody) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };

    if (ctx.is_head and ctx.parser.headersComplete()) {
        log.debug("HEAD response status: {d}", .{ctx.parser.status_code});
        if (ctx.parser.content_length) |len| {
            ctx.source.file_size = len;
            log.debug("HEAD response content-length: {d}", .{len});
        } else {
            log.debug("HEAD response missing content-length", .{});
        }
        ctx.finished = true;
        // Don't close, pool instead!
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.source.pool.release(.{ .host = ctx.source.host, .port = ctx.source.port, .use_tls = ctx.source.use_tls }, ctx.conn) catch {
            ctx.conn.close();
        };
    }
}

fn onBody(ctx_void: *anyopaque, chunk: []const u8) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    
    if (ctx.parser.status_code != 0) ctx.http_status = ctx.parser.status_code;
    
    const chunk_start_abs = ctx.request_offset + ctx.total_body_read;
    const chunk_end_abs = chunk_start_abs + chunk.len;

    // Dispatch bytes to all overlapping sub-ranges
    for (ctx.sub_ranges, 0..) |range, i| {
        // Find intersection of current chunk and this sub-range
        const intersect_start = @max(chunk_start_abs, range.start);
        const intersect_end = @min(chunk_end_abs, range.end);

        if (intersect_start < intersect_end) {
            const chunk_offset = intersect_start - chunk_start_abs;
            const dest_offset = intersect_start - range.start;
            const len = intersect_end - intersect_start;
            
            @memcpy(ctx.dest_buffers[i][dest_offset .. dest_offset + len], chunk[chunk_offset .. chunk_offset + len]);
        }
    }

    ctx.total_body_read += chunk.len;
    
    // Check for completion based on Content-Length or total body size
    const body_len = if (ctx.parser.content_length) |cl| cl else 0;
    const is_last_byte = if (body_len > 0) ctx.total_body_read >= body_len else false;

    if (is_last_byte) {
        log.debug("[s3] Coalesced request finished. total_read={d}", .{ctx.total_body_read});
        ctx.finished = true;
        // Release to pool
        ctx.conn.user_ctx = null;
        ctx.conn.idling = true;
        ctx.source.pool.release(.{ .host = ctx.source.host, .port = ctx.source.port, .use_tls = ctx.source.use_tls }, ctx.conn) catch {
            ctx.conn.close();
        };
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    ctx.err = err;
    ctx.conn.close();
}
