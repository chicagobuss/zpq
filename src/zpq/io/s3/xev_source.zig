const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const SigV4 = @import("sigv4.zig").SigV4;
const io = @import("../interface.zig");
const dns = @import("dns.zig");
const pool_mod = @import("connection_pool.zig");
const ConnectionPool = pool_mod.ConnectionPool;
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
    pool: ConnectionPool,

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
        self.pool = ConnectionPool.init(allocator);
        
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

        // Try to load credentials from environment
        if (std.posix.getenv("AWS_ACCESS_KEY_ID")) |ak| {
            if (std.posix.getenv("AWS_SECRET_ACCESS_KEY")) |sk| {
                self.access_key = try allocator.dupe(u8, ak);
                self.secret_key = try allocator.dupe(u8, sk);
                if (std.posix.getenv("AWS_SESSION_TOKEN")) |st| {
                    self.session_token = try allocator.dupe(u8, st);
                }
                log.debug("XevS3Source: loaded credentials from environment", .{});
            }
        }

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

        const max_concurrency = 16;
        var i: usize = 0;
        while (i < ranges.len) {
            const batch_size = @min(max_concurrency, ranges.len - i);
            const batch_ranges = ranges[i .. i + batch_size];
            const batch_buffers = buffers[i .. i + batch_size];

            const contexts = try self.allocator.alloc(*ReqContext, batch_size);
            defer self.allocator.free(contexts);
            
            const conns = try self.allocator.alloc(*tls.Connection, batch_size);
            defer self.allocator.free(conns);

            var cleanup_idx: usize = 0;
            errdefer {
                for (0..cleanup_idx) |j| {
                    self.allocator.destroy(contexts[j]);
                }
            }

            for (batch_ranges, 0..) |range, j| {
                const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
                const conn = if (self.pool.acquire(key)) |c| blk: {
                    c.idling = false;
                    break :blk c;
                } else blk: {
                    const c = try self.allocator.create(tls.Connection);
                    c.* = try tls.Connection.init(self.loop, self.allocator, self.host);
                    break :blk c;
                };
                conns[j] = conn;
                cleanup_idx += 1;

                const ctx = try self.allocator.create(ReqContext);
                contexts[j] = ctx;
                ctx.* = .{
                    .source = self,
                    .conn = conn,
                    .buf = batch_buffers[j][0 .. range.end - range.start],
                    .offset = range.start,
                    .allocator = self.allocator,
                    .parser = .{},
                };

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
                            self.allocator.destroy(ctx);
                            continue;
                        }
                    }
                    if (first_err == null) first_err = err;
                }
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
            .buf = &[_]u8{}, // No body expected for HEAD
            .offset = 0,
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
        ctx.* = .{
            .source = self,
            .conn = conn,
            .buf = buf,
            .offset = offset,
            .allocator = self.allocator,
            .parser = .{},
        };
        defer self.allocator.destroy(ctx);

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
        log.debug("readAt: loop finished bytes_read={d} finished={}", .{ctx.bytes_read, ctx.finished});

        if (ctx.err) |err| {
            if (err == error.EOF or err == error.TlsConnectionClosed) {
                if (ctx.finished) return ctx.bytes_read;
            }
            log.debug("readAt: error return {}", .{err});
            return err;
        }
        
        return ctx.bytes_read;
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
    buf: []u8,
    offset: u64,
    allocator: std.mem.Allocator,
    
    parser: ResponseParser,
    
    bytes_read: usize = 0,
    finished: bool = false,
    err: ?anyerror = null,
    http_status: u16 = 0,
    is_head: bool = false,
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
        const end_inclusive = ctx.offset + ctx.buf.len - 1;
        const range_val = std.fmt.allocPrint(aa, "bytes={d}-{d}", .{ ctx.offset, end_inclusive }) catch |err| {
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
        headers.append(aa, .{ .name = "Host", .value = ctx.source.host }) catch |err| {
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
    
    const remaining = ctx.buf.len - ctx.bytes_read;
    const take = @min(remaining, chunk.len);
    
    if (take > 0) {
        @memcpy(ctx.buf[ctx.bytes_read .. ctx.bytes_read + take], chunk[0..take]);
        ctx.bytes_read += take;
    }
    
    if (ctx.bytes_read == ctx.buf.len) {
        log.debug("[s3] Request finished. offset={d} len={d} self={*}", .{ctx.offset, ctx.buf.len, ctx.conn});
        ctx.finished = true;
        // Don't close, pool instead!
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
