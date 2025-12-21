const std = @import("std");
const io = @import("../interface.zig");
const EventLoop = @import("event_loop.zig").EventLoop;
const AsyncRequest = @import("request.zig").AsyncRequest;
const connection_pool_mod = @import("connection_pool.zig");
const ConnectionPool = connection_pool_mod.ConnectionPool;
const ConnectionKey = connection_pool_mod.ConnectionKey;
const Connection = connection_pool_mod.Connection;
const dns = @import("dns.zig");
pub const scheduler = @import("scheduler.zig");
const Range = io.Range;
const TlsAdapter = @import("tls_adapter.zig").TlsAdapter;
const types = @import("types.zig");

// Re-exports for tests
pub const connection_pool = connection_pool_mod;

pub const AsyncS3Source = struct {
    allocator: std.mem.Allocator,
    pool: *ConnectionPool, // Shared pool
    event_loop: EventLoop,
    resolver: dns.Resolver,

    host: []const u8,
    port: u16,
    path_prefix: []const u8, // "/bucket/key"
    use_tls: bool,
    trusted_cert: ?[]const u8,
    config: ?types.S3Config, // Unified Auth Config

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

        // Deep copy config if present
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

        // Note: EventLoop creation might fail
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

        // 1. Initial DNS Resolution
        try self.resolveHost();

        // 2. Fetch size via HEAD request (synchronously waiting on loop)
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
        std.debug.print("[AsyncS3Source] Resolving {s}:{d}...\n", .{ self.host, self.port });

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

                // Copy results
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
        self.resolver.resolve(&self.event_loop.loop, self.host, self.port, &dns_comp, DnsCtx.callback, &dns_ctx);

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
        var req = AsyncRequest.init();
        defer req.deinit(self.allocator);

        try req.prepareHead(self.allocator, self.host, self.port, self.path_prefix, null, self.config);

        // Execute single request
        var fd: std.posix.fd_t = undefined;
        var tls: ?*TlsAdapter = null;
        const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };

        if (self.pool.acquire(key)) |conn| {
            fd = conn.fd;
            tls = conn.tls;
        } else {
            const conn = try self.connectNew();
            fd = conn.fd;
            tls = conn.tls;
        }

        req.tls = tls;
        try self.event_loop.registerWrite(fd, &req);
        try self.event_loop.registerRead(fd, &req);

        while (req.state != .Finished and req.state != .Error) {
            const events = try self.event_loop.tick();
            if (events == 0) {
                std.debug.print("[AsyncS3Source] Timeout waiting for events (FETCH_SIZE).\n", .{});
            }
        }

        // Cleanup
        if (req.state == .Finished) {
            self.file_size = req.content_length;
            try self.pool.release(key, .{ .fd = fd, .tls = tls });
        } else {
            // Unregister before closing to clean up map
            self.event_loop.unregister(fd);

            if (tls) |t| t.deinit();
            std.posix.close(fd);
            return error.HeadRequestFailed;
        }
    }

    /// Read multiple ranges into provided buffers.
    /// ranges[i] corresponds to buffers[i].
    pub fn readRanges(self: *AsyncS3Source, ranges: []const Range, buffers: []const []u8) !void {
        if (ranges.len != buffers.len) return error.InvalidArgs;
        if (ranges.len == 0) return;

        // 1. Coalesce Ranges
        var merged_reqs = try scheduler.mergeRanges(self.allocator, ranges);
        defer {
            for (merged_reqs.items) |*m| m.original_indices.deinit(self.allocator);
            merged_reqs.deinit(self.allocator);
        }

        // 2. Prepare AsyncRequests
        var requests = std.ArrayListUnmanaged(AsyncRequest){};
        defer {
            for (requests.items) |*r| r.deinit(self.allocator);
            requests.deinit(self.allocator);
        }

        // Track FDs to release them back to pool later
        var fds = std.ArrayListUnmanaged(std.posix.fd_t){};
        defer fds.deinit(self.allocator);

        // Track TLS adapters (owned by Connection, passed to Pool or destroyed)
        var tls_adapters = std.ArrayListUnmanaged(?*TlsAdapter){};
        defer tls_adapters.deinit(self.allocator);

        for (merged_reqs.items) |merged| {
            // Split huge requests into 64MB chunks
            var splitter = scheduler.RangeSplitter.init(merged.request_range, scheduler.CHUNK_SIZE);

            // We need to map the merged range segments to these chunks.
            // First, build the full list of segments for the merged range.
            var all_segments = std.ArrayListUnmanaged(AsyncRequest.Segment){};
            defer all_segments.deinit(self.allocator);

            var current_offset = merged.request_range.start;
            for (merged.original_indices.items) |orig_idx| {
                const target_range = ranges[orig_idx];
                const target_buffer = buffers[orig_idx];

                // Gap?
                if (target_range.start > current_offset) {
                    const gap_len = target_range.start - current_offset;
                    try all_segments.append(self.allocator, .{ .buffer = null, .len = gap_len });
                }

                // Data
                try all_segments.append(self.allocator, .{ .buffer = target_buffer, .len = target_range.len() });
                current_offset = target_range.end;
            }
            // Trailing gap?
            if (current_offset < merged.request_range.end) {
                try all_segments.append(self.allocator, .{ .buffer = null, .len = merged.request_range.end - current_offset });
            }

            // Now distribute segments across chunks
            var seg_idx: usize = 0;
            var seg_offset: usize = 0; // Offset into current segment

            while (splitter.next()) |chunk_range| {
                var req = AsyncRequest.init();
                // We will add it to 'requests' list later

                var chunk_filled: u64 = 0;
                const chunk_len = chunk_range.len();

                while (chunk_filled < chunk_len and seg_idx < all_segments.items.len) {
                    const seg = all_segments.items[seg_idx];
                    const seg_remaining = seg.len - seg_offset;
                    const chunk_remaining = chunk_len - chunk_filled;
                    const to_take = @as(usize, @intCast(@min(seg_remaining, chunk_remaining)));

                    if (seg.buffer) |buf| {
                        // Slice the user buffer
                        try req.addSegment(self.allocator, buf[seg_offset .. seg_offset + to_take], to_take);
                    } else {
                        // Gap
                        try req.addSegment(self.allocator, null, to_take);
                    }

                    chunk_filled += to_take;
                    seg_offset += to_take;

                    if (seg_offset >= seg.len) {
                        seg_idx += 1;
                        seg_offset = 0;
                    }
                }

                // Note: We don't have TLS adapter yet, pass null for now, set later
                try req.prepare(self.allocator, self.host, self.port, self.path_prefix, chunk_range.start, chunk_range.end, null, self.config);
                try requests.append(self.allocator, req);
            }
        }

        // 3. Execute Requests
        // Acquire connections and register
        for (requests.items) |*req| {
            // Get connection
            const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
            var fd: std.posix.fd_t = undefined;
            var tls: ?*TlsAdapter = null;

            if (self.pool.acquire(key)) |conn| {
                fd = conn.fd;
                tls = conn.tls;
            } else {
                // Connect new
                const conn = try self.connectNew();
                fd = conn.fd;
                tls = conn.tls;
            }
            try fds.append(self.allocator, fd);
            try tls_adapters.append(self.allocator, tls);

            // Set TLS on request
            req.tls = tls;

            // Register
            try self.event_loop.registerWrite(fd, req);
            try self.event_loop.registerRead(fd, req);
        }

        // Drive Loop
        while (true) {
            var all_done = true;
            for (requests.items) |*req| {
                if (req.state != .Finished and req.state != .Error) {
                    all_done = false;
                    break;
                }
            }
            if (all_done) break;

            const events = try self.event_loop.tick();
            if (events == 0) {
                std.debug.print("[AsyncS3Source] Timeout waiting for events (READ_RANGES).\n", .{});
            }
        }

        // 4. Cleanup / Release Connections
        for (requests.items, 0..) |*req, i| {
            const fd = fds.items[i];
            const tls = tls_adapters.items[i];
            const key = ConnectionKey{ .host = self.host, .port = self.port, .use_tls = self.use_tls };
            const conn = Connection{ .fd = fd, .tls = tls };

            if (req.state == .Finished) {
                // Keep-Alive: Release to pool
                try self.pool.release(key, conn);
            } else {
                // Error: Close
                if (tls) |t| t.deinit();
                std.posix.close(fd);
            }
        }
    }

    fn connectNew(self: *AsyncS3Source) !Connection {
        if (self.resolved_ips.len == 0) return error.HostNotFound;

        const addr = self.resolved_ips[self.next_ip_idx];
        self.next_ip_idx = (self.next_ip_idx + 1) % self.resolved_ips.len;

        const fd = try std.posix.socket(switch (addr.any.family) {
            std.posix.AF.INET => std.posix.AF.INET,
            std.posix.AF.INET6 => std.posix.AF.INET6,
            else => return error.UnsupportedAddressFamily,
        }, std.posix.SOCK.STREAM, 0);
        errdefer std.posix.close(fd);

        std.debug.print("[AsyncS3Source] Connecting to {}...\n", .{addr.any});
        try std.posix.connect(fd, &addr.any, addr.getOsSockLen());

        var tls_adapter: ?*TlsAdapter = null;
        if (self.use_tls) {
            std.debug.print("[AsyncS3Source] Initiating TLS handshake...\n", .{});
            tls_adapter = try TlsAdapter.init(self.allocator, fd, self.host, self.trusted_cert);
        }

        // Set Non-Blocking
        const flags = try std.posix.fcntl(fd, std.posix.F.GETFL, 0);
        var flags_o: std.posix.O = @bitCast(@as(u32, @truncate(flags)));
        flags_o.NONBLOCK = true;
        _ = try std.posix.fcntl(fd, std.posix.F.SETFL, @as(u32, @bitCast(flags_o)));

        std.debug.print("[AsyncS3Source] Connected (FD {d})\n", .{fd});
        return Connection{ .fd = fd, .tls = tls_adapter };
    }

    // --- RandomAccessSource Implementation ---

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        const range = Range{ .start = offset, .end = offset + buf.len };
        const ranges = &[_]Range{range};
        const buffers = &[_][]u8{buf};

        try self.readRanges(ranges, buffers);
        // If readRanges succeeds, it means it filled the buffers (or error)
        // Check if we hit EOF logic inside readRanges?
        // readRanges splits and fills. If EOF, AsyncRequest might error or fill partial?
        // Current AsyncRequest logic errors on short reads unless handled.
        // For now assume full read.
        return buf.len;
    }

    fn readRangesImpl(ptr: *anyopaque, ranges: []const Range, buffers: []const []u8) !void {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        return self.readRanges(ranges, buffers);
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        return self.file_size;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *AsyncS3Source = @ptrCast(@alignCast(ptr));
        self.deinit();
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
