const std = @import("std");
const io = @import("../interface.zig");
const EventLoop = @import("event_loop.zig").EventLoop;
const AsyncRequest = @import("request.zig").AsyncRequest;
const connection_pool_mod = @import("connection_pool.zig");
const ConnectionPool = connection_pool_mod.ConnectionPool;
const ConnectionKey = connection_pool_mod.ConnectionKey;
const connection_mod = @import("connection.zig");
const Connection = connection_mod.Connection;
const dns = @import("dns.zig");
pub const scheduler = @import("scheduler.zig");
const Range = io.Range;
const types = @import("types.zig");

pub const AsyncS3Source = struct {
    allocator: std.mem.Allocator,
    pool: *ConnectionPool,
    event_loop: EventLoop,
    resolver: dns.Resolver,

    host: []const u8,
    port: u16,
    path_prefix: []const u8,
    use_tls: bool,
    trusted_cert: ?[]const u8,
    config: ?types.S3Config,

    file_size: u64,
    resolved_ips: []dns.Address,
    next_ip_idx: usize,

    pub fn init(allocator: std.mem.Allocator, pool: *ConnectionPool, resolver: dns.Resolver, host: []const u8, port: u16, bucket: []const u8, key: []const u8, use_tls: bool, trusted_cert: ?[]const u8, config: ?types.S3Config) !AsyncS3Source {
        const path = try std.fmt.allocPrint(allocator, "/{s}/{s}", .{ bucket, key });
        errdefer allocator.free(path);

        const host_dupe = try allocator.dupe(u8, host);
        errdefer allocator.free(host_dupe);

        var cert_dupe: ?[]const u8 = null;
        if (trusted_cert) |cert| {
            cert_dupe = try allocator.dupe(u8, cert);
        }
        errdefer if (cert_dupe) |c| allocator.free(c);

        var config_dupe: ?types.S3Config = null;
        if (config) |c| {
            config_dupe = types.S3Config{
                .credentials = if (c.credentials) |creds| types.Credentials{
                    .access_key = try allocator.dupe(u8, creds.access_key),
                    .secret_key = try allocator.dupe(u8, creds.secret_key),
                    .session_token = if (creds.session_token) |t| try allocator.dupe(u8, t) else null,
                } else null,
                .region = try allocator.dupe(u8, c.region),
                .endpoint = if (c.endpoint) |ep| try allocator.dupe(u8, ep) else null,
            };
        }

        const loop = try EventLoop.init(allocator);

        var self = AsyncS3Source{
            .allocator = allocator,
            .pool = pool,
            .event_loop = loop,
            .resolver = resolver,
            .host = host_dupe,
            .port = port,
            .path_prefix = path,
            .use_tls = use_tls,
            .trusted_cert = cert_dupe,
            .config = config_dupe,
            .file_size = 0,
            .resolved_ips = &.{},
            .next_ip_idx = 0,
        };
        errdefer self.deinit();

        try self.resolveHost();
        try self.fetchSize();

        return self;
    }

    pub fn deinit(self: *AsyncS3Source) void {
        self.event_loop.deinit();
        self.allocator.free(self.path_prefix);
        self.allocator.free(self.host);
        if (self.trusted_cert) |c| self.allocator.free(c);
        if (self.config) |c| {
            if (c.credentials) |creds| {
                self.allocator.free(creds.access_key);
                self.allocator.free(creds.secret_key);
                if (creds.session_token) |t| self.allocator.free(t);
            }
            self.allocator.free(c.region);
            if (c.endpoint) |ep| self.allocator.free(ep);
        }
        if (self.resolved_ips.len > 0) self.allocator.free(self.resolved_ips);
    }

    fn resolveHost(self: *AsyncS3Source) !void {
        var dns_comp = dns.Resolver.Completion.init();
        defer dns_comp.deinit(self.allocator);

        const DnsCtx = struct {
            results: []dns.Address = &.{},
            err: ?anyerror = null,
            done: bool = false,
            allocator: std.mem.Allocator,

            fn callback(ud: ?*anyopaque, results: []const dns.Address, err: anyerror!void) void {
                const ctx: *@This() = @ptrCast(@alignCast(ud));
                err catch |e| {
                    ctx.err = e;
                    ctx.done = true;
                    return;
                };
                const copy = ctx.allocator.alloc(dns.Address, results.len) catch |e| {
                    ctx.err = e;
                    ctx.done = true;
                    return;
                };
                @memcpy(copy, results);
                ctx.results = copy;
                ctx.done = true;
            }
        };

        var dns_ctx = DnsCtx{ .allocator = self.allocator };
        self.resolver.resolve(self.event_loop.loop, self.host, self.port, &dns_comp, DnsCtx.callback, &dns_ctx);

        while (!dns_ctx.done) {
            _ = try self.event_loop.tick();
        }

        if (dns_ctx.err) |err| return err;
        if (dns_ctx.results.len == 0) return error.HostNotFound;

        if (self.resolved_ips.len > 0) self.allocator.free(self.resolved_ips);
        self.resolved_ips = dns_ctx.results;
        self.next_ip_idx = 0;
    }

    fn fetchSize(self: *AsyncS3Source) !void {
        var req = AsyncRequest.init(self.allocator);
        defer req.deinit();

        try req.prepareHead(self.host, self.port, self.path_prefix, self.use_tls, self.config);

        const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
        var conn = self.pool.acquire(key);
        if (conn == null) {
            conn = try self.connectNew();
        }
        const connection = conn.?;

        const WaitCtx = struct {
            done: bool = false,
            fn onDone(ctx: ?*anyopaque, r: *AsyncRequest) void {
                _ = r;
                const self_ptr: *@This() = @ptrCast(@alignCast(ctx));
                self_ptr.done = true;
            }
        };
        var wait_ctx = WaitCtx{};
        req.done_ctx = &wait_ctx;
        req.on_done = WaitCtx.onDone;

        try req.execute(connection);

        while (!wait_ctx.done) {
            _ = try self.event_loop.tick();
        }

        if (req.state == .Finished) {
            self.file_size = req.content_length;
            try self.pool.release(key, connection);
        } else {
            connection.close();
            // In a real pool, we'd deinit the connection if it's dead.
            connection.deinit();
            return error.HeadRequestFailed;
        }
    }

    pub fn readRanges(self: *AsyncS3Source, ranges: []const Range, buffers: []const []u8) !void {
        if (ranges.len != buffers.len) return error.InvalidArgs;
        if (ranges.len == 0) return;

        var merged_reqs = try scheduler.mergeRanges(self.allocator, ranges);
        defer {
            for (merged_reqs.items) |*m| m.original_indices.deinit(self.allocator);
            merged_reqs.deinit(self.allocator);
        }

        var requests = std.ArrayListUnmanaged(AsyncRequest){};
        defer {
            for (requests.items) |*r| r.deinit();
            requests.deinit(self.allocator);
        }

        const BatchCtx = struct {
            pending: usize,
            fn onDone(ctx: ?*anyopaque, r: *AsyncRequest) void {
                _ = r;
                const self_ptr: *@This() = @ptrCast(@alignCast(ctx));
                self_ptr.pending -= 1;
            }
        };
        var batch_ctx = BatchCtx{ .pending = 0 };

        for (merged_reqs.items) |merged| {
            var splitter = scheduler.RangeSplitter.init(merged.request_range, scheduler.CHUNK_SIZE);
            while (splitter.next()) |chunk_range| {
                var req = AsyncRequest.init(self.allocator);
                errdefer req.deinit();

                var current_offset = chunk_range.start;
                for (merged.original_indices.items) |orig_idx| {
                    const target_range = ranges[orig_idx];
                    const target_buffer = buffers[orig_idx];

                    if (target_range.start >= chunk_range.end or target_range.end <= chunk_range.start) continue;

                    const intersection_start = @max(target_range.start, chunk_range.start);
                    const intersection_end = @min(target_range.end, chunk_range.end);

                    if (intersection_start > current_offset) {
                        try req.addSegment(null, intersection_start - current_offset);
                    }

                    const buf_offset = intersection_start - target_range.start;
                    const buf_len = intersection_end - intersection_start;
                    try req.addSegment(target_buffer[buf_offset .. buf_offset + buf_len], buf_len);
                    current_offset = intersection_end;
                }

                if (chunk_range.end > current_offset) {
                    try req.addSegment(null, chunk_range.end - current_offset);
                }

                try req.prepare(self.host, self.port, self.path_prefix, chunk_range.start, chunk_range.end, self.use_tls, self.config);

                req.done_ctx = &batch_ctx;
                req.on_done = BatchCtx.onDone;
                batch_ctx.pending += 1;

                try requests.append(self.allocator, req);
            }
        }

        const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };

        for (requests.items) |*req| {
            var conn = self.pool.acquire(key);
            if (conn == null) conn = try self.connectNew();
            const connection = conn.?;
            try req.execute(connection);
        }

        while (batch_ctx.pending > 0) {
            _ = try self.event_loop.tick();
        }

        // Release connections
        for (requests.items) |*req| {
            if (req.connection) |conn| {
                if (req.state == .Finished) {
                    try self.pool.release(key, conn);
                } else {
                    conn.close();
                    conn.deinit();
                }
            }
        }
    }

    fn connectNew(self: *AsyncS3Source) !*Connection {
        const addr = self.resolved_ips[self.next_ip_idx];
        self.next_ip_idx = (self.next_ip_idx + 1) % self.resolved_ips.len;

        const conn = try Connection.init(self.event_loop.loop, self.allocator, self.host, self.use_tls);
        try conn.connect(addr);
        return conn;
    }

    pub fn readAt(self: *AsyncS3Source, offset: u64, buf: []u8) !usize {
        const range = Range{ .start = offset, .end = offset + buf.len };
        const buffers = [1][]u8{buf};
        try self.readRanges(&[1]Range{range}, &buffers);
        return buf.len;
    }

    pub fn size(self: *AsyncS3Source) u64 {
        return self.file_size;
    }

    pub fn close(self: *AsyncS3Source) void {
        _ = self;
        // The pool handles closing idle connections.
        // EventLoop is owned by self and closed in deinit.
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        return self.readAt(offset, buf);
    }

    fn readRangesImpl(ptr: *anyopaque, ranges: []const Range, buffers: []const []u8) anyerror!void {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        return self.readRanges(ranges, buffers);
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        return self.size();
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        self.close();
    }

    pub fn source(self: *AsyncS3Source) io.RandomAccessSource {
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
};
