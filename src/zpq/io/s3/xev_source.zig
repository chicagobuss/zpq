const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const ResponseParser = @import("../http/response_parser.zig").ResponseParser;
const SigV4 = @import("sigv4.zig").SigV4;
const io = @import("../interface.zig");
const dns = @import("dns.zig");
const orchestrator = @import("../cloud/orchestrator.zig");
const CloudProvider = orchestrator.CloudProvider;
const RequestContext = orchestrator.RequestContext;
const global_pool_mod = @import("global_pool.zig");
const GlobalConnectionPool = global_pool_mod.GlobalConnectionPool;
const ConnectionKey = global_pool_mod.ConnectionKey;

const log = @import("std").log.scoped(.s3_source);

/// A cross-platform S3 Source that uses libxev + boring_tls.
/// Implements io.RandomAccessSource for ParquetFile.
/// This source owns its own EventLoop and blocks on read calls (spinning the loop).
pub const XevS3Source = struct {
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: dns.ThreadPoolResolver,
    pool: *GlobalConnectionPool,

    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    region: []const u8,
    use_tls: bool,
    port: u16,
    verify_certificate: bool = false,

    // Throughput Options
    tcp_read_buf_size: usize = 4096,
    use_direct: bool = true,

    owns_loop: bool = false,
    owns_pool: bool = false, // If true, we created and own the pool (must deinit)

    // Auth (optional)
    access_key: ?[]const u8 = null,
    secret_key: ?[]const u8 = null,
    session_token: ?[]const u8 = null,

    // Cached DNS result
    cached_addr: ?xev.shim_net.Address = null,

    // File size (fetched on init or on demand)
    file_size: u64 = 0,

    pub const Options = struct {
        verify_certificate: bool = false,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        host: []const u8,
        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        use_tls: bool,
        port: u16,
    ) !*XevS3Source {
        return initWithOptions(allocator, host, bucket, key, region, use_tls, port, .{});
    }

    pub fn initWithOptions(
        allocator: std.mem.Allocator,
        host: []const u8,
        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        use_tls: bool,
        port: u16,
        options: Options,
    ) !*XevS3Source {
        const loop = try allocator.create(xev.Loop);
        loop.* = try xev.Loop.init(.{});
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

        // When we own the loop, we must also own a local pool.
        // The global pool can't be used because connections are tied to their event loop.
        // Reusing a connection from a different loop causes hangs.
        const local_pool = try allocator.create(GlobalConnectionPool);
        local_pool.* = GlobalConnectionPool.init(allocator);
        errdefer {
            local_pool.deinit();
            allocator.destroy(local_pool);
        }

        var self = try initWithLoop(allocator, loop, thread_pool, host, bucket, key, region, use_tls, port, 4096, true, local_pool, options);
        self.owns_loop = true;
        self.owns_pool = true;
        return self;
    }

    pub fn initWithLoop(
        allocator: std.mem.Allocator,
        loop: *xev.Loop,
        thread_pool: *xev.ThreadPool,
        host: []const u8,
        bucket: []const u8,
        key: []const u8,
        region: []const u8,
        use_tls: bool,
        port: u16,
        tcp_read_buf_size: usize,
        use_direct: bool,
        external_pool: ?*GlobalConnectionPool,
        options: Options,
    ) !*XevS3Source {
        const self = try allocator.create(XevS3Source);
        self.* = .{
            .allocator = allocator,
            .loop = loop,
            .thread_pool = thread_pool,
            .resolver = dns.ThreadPoolResolver.init(thread_pool, allocator),
            .pool = external_pool orelse global_pool_mod.getGlobalPool(allocator),
            .host = try allocator.dupe(u8, host),
            .bucket = try allocator.dupe(u8, bucket),
            .key = try allocator.dupe(u8, key),
            .region = try allocator.dupe(u8, region),
            .use_tls = use_tls,
            .port = port,
            .verify_certificate = options.verify_certificate,
            .tcp_read_buf_size = tcp_read_buf_size,
            .use_direct = use_direct,
            .owns_loop = false,
        };

        return self;
    }

    pub fn deinit(self: *XevS3Source) void {
        self.resolver.deinit();

        // Clean up pool if we own it (local pool for owns_loop case)
        if (self.owns_pool) {
            self.pool.deinit();
            self.allocator.destroy(self.pool);
        }
        // NOTE: If we don't own the pool, it's the global pool.
        // Connections stay alive for reuse by other XevS3Source instances.
        // The global pool is cleaned up at process exit via shutdownGlobalPool().

        if (self.owns_loop) {
            self.thread_pool.shutdown(); // Ensure threads stop
            self.thread_pool.deinit();
            self.loop.deinit();

            self.allocator.destroy(self.thread_pool);
            self.allocator.destroy(self.loop);
        }

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

    // --- CloudProvider Implementation ---

    fn getHost(ptr: *anyopaque) []const u8 {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.host;
    }
    fn getPort(ptr: *anyopaque) u16 {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.port;
    }
    fn getUseTls(ptr: *anyopaque) bool {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.use_tls;
    }
    fn getVerifyCertificate(ptr: *anyopaque) bool {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.verify_certificate;
    }
    fn getTcpReadBufSize(ptr: *anyopaque) usize {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.tcp_read_buf_size;
    }
    fn getUseDirect(ptr: *anyopaque) bool {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.use_direct;
    }
    fn getPool(ptr: *anyopaque) *GlobalConnectionPool {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.pool;
    }
    fn resolveProvider(ptr: *anyopaque) anyerror!xev.shim_net.Address {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        return self.resolve();
    }
    fn onHeadCompleteProvider(ptr: *anyopaque, status: u16, content_length: ?u64) u64 {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        _ = status;
        if (content_length) |cl| {
            self.file_size = cl;
            return cl;
        }
        return 0;
    }

    fn provider(self: *XevS3Source) CloudProvider {
        return .{
            .ptr = self,
            .vtable = &.{
                .onConnect = onConnectProvider,
                .onHeadComplete = onHeadCompleteProvider,
                .resolve = resolveProvider,
                .getHost = getHost,
                .getPort = getPort,
                .getUseTls = getUseTls,
                .getVerifyCertificate = getVerifyCertificate,
                .getTcpReadBufSize = getTcpReadBufSize,
                .getUseDirect = getUseDirect,
                .getPool = getPool,
            },
        };
    }

    // --- IO Interface Implementation ---

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        var orch = orchestrator.Orchestrator.init(self.allocator, self.loop, self.provider());
        const ranges = &[_]io.Range{.{ .start = offset, .end = offset + buf.len }};
        const buffers = &[_][]u8{buf};
        try orch.readRanges(ranges, buffers);
        return buf.len;
    }

    fn readRangesImpl(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8) anyerror!void {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        var orch = orchestrator.Orchestrator.init(self.allocator, self.loop, self.provider());
        return orch.readRanges(ranges, buffers);
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));
        if (self.file_size == 0) {
            var orch = orchestrator.Orchestrator.init(self.allocator, self.loop, self.provider());
            self.file_size = orch.fetchSize() catch |err| {
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

    // --- CloudProvider Implementation Helpers ---

    fn onConnectProvider(ptr: *anyopaque, ctx: *RequestContext) anyerror!void {
        const self: *XevS3Source = @ptrCast(@alignCast(ptr));

        // Use an arena for building the request (headers, signature, etc.)
        var arena = std.heap.ArenaAllocator.init(ctx.allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        const path = try std.fmt.allocPrint(aa, "/{s}/{s}", .{ self.bucket, self.key });
        const method = if (ctx.is_head) "HEAD" else "GET";

        // Build Headers - only include headers that should be signed
        var headers = std.ArrayListUnmanaged(std.http.Header){};

        if (!ctx.is_head) {
            const end_inclusive = ctx.request_end - 1;
            const range_val = try std.fmt.allocPrint(aa, "bytes={d}-{d}", .{ ctx.request_offset, end_inclusive });
            try headers.append(aa, .{ .name = "Range", .value = range_val });
        }

        // Sign if credentials provided
        if (self.access_key) |ak| {
            if (self.secret_key) |sk| {
                const signer = SigV4{
                    .region = self.region,
                    .access_key = ak,
                    .secret_key = sk,
                    .session_token = self.session_token,
                };

                const scheme = if (self.use_tls) "https" else "http";
                const url = try std.fmt.allocPrint(aa, "{s}://{s}{s}", .{ scheme, self.host, path });
                const uri = try std.Uri.parse(url);

                try signer.sign(aa, method, uri, &headers, "");
            }
        } else {
            // Must add Host manually for anonymous requests (SigV4 adds it automatically)
            const host_header = if (self.port == 443 or self.port == 80)
                self.host
            else
                try std.fmt.allocPrint(aa, "{s}:{d}", .{ self.host, self.port });

            try headers.append(aa, .{ .name = "Host", .value = host_header });
        }

        // Add non-signed headers after signing
        try headers.append(aa, .{ .name = "User-Agent", .value = "zpq-xev" });
        try headers.append(aa, .{ .name = "Connection", .value = "keep-alive" });

        // Serialize Request
        const encoded_path = try @import("sigv4.zig").encodeS3Path(aa, path);

        var head_list = std.ArrayListUnmanaged(u8){};
        defer head_list.deinit(aa);

        try head_list.appendSlice(aa, method);
        try head_list.appendSlice(aa, " ");
        try head_list.appendSlice(aa, encoded_path);
        try head_list.appendSlice(aa, " HTTP/1.1\r\n");

        for (headers.items) |h| {
            try head_list.appendSlice(aa, h.name);
            try head_list.appendSlice(aa, ": ");
            try head_list.appendSlice(aa, h.value);
            try head_list.appendSlice(aa, "\r\n");
        }
        try head_list.appendSlice(aa, "\r\n");

        log.debug("Sending Request:\n{s}", .{head_list.items});
        try ctx.conn.write(head_list.items);
    }

    // --- Core Logic ---

    pub fn fetchSize(self: *XevS3Source) !void {
        var orch = orchestrator.Orchestrator.init(self.allocator, self.loop, self.provider());
        _ = try orch.fetchSize();
    }

    pub fn readAt(self: *XevS3Source, offset: u64, buf: []u8) !usize {
        var orch = orchestrator.Orchestrator.init(self.allocator, self.loop, self.provider());
        const ranges = &[_]io.Range{.{ .start = offset, .end = offset + buf.len }};
        const buffers = &[_][]u8{buf};
        try orch.readRanges(ranges, buffers);
        return buf.len;
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
        log.debug("resolve: starting resolution for {s}:{d}", .{ self.host, self.port });
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
