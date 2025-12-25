const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const SigV4 = @import("sigv4.zig").SigV4;
const io = @import("../interface.zig");
const dns = @import("dns.zig");

const log = @import("std").log.scoped(.s3_source);

/// A cross-platform S3 Source that uses libxev + boring_tls.
/// Implements io.RandomAccessSource for ParquetFile.
/// This source owns its own EventLoop and blocks on read calls (spinning the loop).
pub const XevS3Source = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: dns.ThreadPoolResolver,

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
        
        // Use parallel implementation for all range counts >= 1
        log.debug("readRangesImpl: fetching {d} ranges in parallel", .{ranges.len});
        const addr = try self.resolve();

        const contexts = try self.allocator.alloc(*ReqContext, ranges.len);
        defer self.allocator.free(contexts);
        
        const conns = try self.allocator.alloc(*tls.Connection, ranges.len);
        defer self.allocator.free(conns);

        var cleanup_idx: usize = 0;
        errdefer {
            for (0..cleanup_idx) |i| {
                conns[i].deinit();
                self.allocator.destroy(conns[i]);
                self.allocator.destroy(contexts[i]);
            }
        }

        for (ranges, 0..) |range, i| {
            const conn = try self.allocator.create(tls.Connection);
            conns[i] = conn;
            conn.* = try tls.Connection.init(self.loop, self.allocator, self.host);
            cleanup_idx += 1;

            const ctx = try self.allocator.create(ReqContext);
            contexts[i] = ctx;
            ctx.* = .{
                .source = self,
                .conn = conn,
                .buf = buffers[i][0 .. range.end - range.start],
                .offset = range.start,
                .allocator = self.allocator,
                .parser = .{},
            };

            conn.user_ctx = ctx;
            conn.on_connect = onConnect;
            conn.on_data = onData;
            conn.on_error = onError;

            try conn.connect(addr);
        }

        // Run loop until ALL requests are finished or one has a fatal error
        try self.loop.run(.until_done);

        // Check for fatal errors
        var first_err: ?anyerror = null;
        for (contexts) |ctx| {
            if (ctx.err) |err| {
                if (err != error.EOF and err != error.TlsConnectionClosed) {
                    if (first_err == null) first_err = err;
                } else if (!ctx.finished) {
                    if (first_err == null) first_err = err;
                }
            }
        }

        // Cleanup
        for (0..ranges.len) |i| {
            conns[i].deinit();
            self.allocator.destroy(conns[i]);
            self.allocator.destroy(contexts[i]);
        }
        
        if (first_err) |err| return err;
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
        
        const conn = try self.allocator.create(tls.Connection);
        conn.* = try tls.Connection.init(self.loop, self.allocator, self.host);
        defer {
            conn.deinit();
            self.allocator.destroy(conn);
        }

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

        try conn.connect(addr);
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

        const conn = try self.allocator.create(tls.Connection);
        conn.* = try tls.Connection.init(self.loop, self.allocator, self.host);
        defer {
            conn.deinit(); 
            self.allocator.destroy(conn);
        }
        
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

        log.debug("readAt: connecting...", .{});
        try conn.connect(addr);

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
    
    const path = std.fmt.allocPrint(ctx.allocator, "/{s}/{s}", .{ctx.source.bucket, ctx.source.key}) catch |err| {
        ctx.err = err;
        ctx.conn.close();
        return;
    };
    defer ctx.allocator.free(path);

    var req: []u8 = undefined;
    if (ctx.is_head) {
        const req_fmt =
            "HEAD {s} HTTP/1.1\r\n" ++
            "Host: {s}\r\n" ++
            "User-Agent: zpq-xev\r\n" ++
            "Connection: close\r\n" ++
            "\r\n";
        req = std.fmt.allocPrint(ctx.allocator, req_fmt, .{ path, ctx.source.host }) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
    } else {
        const end_inclusive = ctx.offset + ctx.buf.len - 1;
        const req_fmt =
            "GET {s} HTTP/1.1\r\n" ++
            "Host: {s}\r\n" ++
            "User-Agent: zpq-xev\r\n" ++
            "Range: bytes={d}-{d}\r\n" ++
            "Connection: close\r\n" ++
            "\r\n";

        req = std.fmt.allocPrint(ctx.allocator, req_fmt, .{ path, ctx.source.host, ctx.offset, end_inclusive }) catch |err| {
            ctx.err = err;
            ctx.conn.close();
            return;
        };
    }
    defer ctx.allocator.free(req);

    log.debug("Sending Request: {s}", .{req});

    ctx.conn.write(req) catch |err| {
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
        if (ctx.parser.content_length) |len| {
            ctx.source.file_size = len;
        }
        ctx.finished = true;
        ctx.conn.close();
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
        std.debug.print("[s3] Request finished. offset={d} len={d} self={*}\n", .{ctx.offset, ctx.buf.len, ctx.conn});
        ctx.finished = true;
        ctx.conn.close();
    }
}

fn onError(ctx_void: ?*anyopaque, err: anyerror) void {
    const ctx: *ReqContext = @ptrCast(@alignCast(ctx_void));
    ctx.err = err;
    ctx.conn.close();
}
