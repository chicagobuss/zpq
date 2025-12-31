//! S3 Client for PUT/GET operations
//!
//! Simple client for uploading objects to S3. Uses the same SigV4 signing
//! and TLS infrastructure as XevS3Source but focused on write operations.

const std = @import("std");
const xev = @import("xev");
const tls = @import("../tls/connection.zig");
const SigV4 = @import("sigv4.zig").SigV4;
const encodeS3Path = @import("sigv4.zig").encodeS3Path;
const dns = @import("dns.zig");
const log = @import("../../log.zig").s3;

/// S3 Client for object operations (PUT, etc.)
pub fn S3ClientGen(comptime XevApi: type) type {
    return struct {
        const Self = @This();
        const Connection = tls.ConnectionGen(XevApi);
        const ThreadPoolResolver = dns.ThreadPoolResolverGen(XevApi);

        allocator: std.mem.Allocator,
        loop: *XevApi.Loop,
        thread_pool: *xev.ThreadPool,

        host: []const u8,
        region: []const u8,
        port: u16,
        use_tls: bool,

        access_key: ?[]const u8 = null,
        secret_key: ?[]const u8 = null,
        session_token: ?[]const u8 = null,

        // Owned resources
        tp_resolver: ?*ThreadPoolResolver = null,

        pub fn init(
            allocator: std.mem.Allocator,
            loop: *XevApi.Loop,
            thread_pool: *xev.ThreadPool,
            host: []const u8,
            region: []const u8,
            use_tls: bool,
            port: u16,
        ) !*Self {
            const self = try allocator.create(Self);
            self.* = Self{
                .allocator = allocator,
                .loop = loop,
                .thread_pool = thread_pool,
                .host = host,
                .region = region,
                .port = port,
                .use_tls = use_tls,
            };
            return self;
        }

        pub fn deinit(self: *Self) void {
            if (self.tp_resolver) |r| {
                self.allocator.destroy(r);
            }
            self.allocator.destroy(self);
        }

        pub fn setCredentials(self: *Self, access_key: []const u8, secret_key: []const u8, session_token: ?[]const u8) void {
            self.access_key = access_key;
            self.secret_key = secret_key;
            self.session_token = session_token;
        }

        /// Upload an object to S3
        pub fn putObject(self: *Self, bucket: []const u8, key: []const u8, body: []const u8) !void {
            var arena = std.heap.ArenaAllocator.init(self.allocator);
            defer arena.deinit();
            const aa = arena.allocator();

            log.debug("putObject: bucket={s} key={s} body_len={d}", .{ bucket, key, body.len });

            // Resolve DNS
            const addr = try self.resolve();

            // Build request
            const path = try std.fmt.allocPrint(aa, "/{s}/{s}", .{ bucket, key });
            const encoded_path = try encodeS3Path(aa, path);

            var headers: std.ArrayListUnmanaged(std.http.Header) = .{};

            // Content-Length
            const content_length_str = try std.fmt.allocPrint(aa, "{d}", .{body.len});
            try headers.append(aa, .{ .name = "Content-Length", .value = content_length_str });
            try headers.append(aa, .{ .name = "Content-Type", .value = "application/octet-stream" });

            // Sign request (includes Host, X-Amz-Date, x-amz-content-sha256, Authorization)
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

                    try signer.sign(aa, "PUT", uri, &headers, body);
                }
            }

            try headers.append(aa, .{ .name = "Connection", .value = "close" });

            // Build HTTP request
            var request_buf: std.ArrayListUnmanaged(u8) = .{};
            try request_buf.appendSlice(aa, "PUT ");
            try request_buf.appendSlice(aa, encoded_path);
            try request_buf.appendSlice(aa, " HTTP/1.1\r\n");

            for (headers.items) |h| {
                try request_buf.appendSlice(aa, h.name);
                try request_buf.appendSlice(aa, ": ");
                try request_buf.appendSlice(aa, h.value);
                try request_buf.appendSlice(aa, "\r\n");
            }
            try request_buf.appendSlice(aa, "\r\n");

            log.debug("PUT request headers:\n{s}", .{request_buf.items});

            // Connect
            var conn = try Connection.init(self.allocator, self.loop, .{
                .host = self.host,
                .verify_certificate = true,
            });
            defer conn.deinit();

            try conn.connect(addr);

            // Send request headers
            try conn.write(request_buf.items);

            // Send body
            if (body.len > 0) {
                try conn.write(body);
            }

            // Read response
            var response_buf: [4096]u8 = undefined;
            const n = try conn.read(&response_buf);

            if (n == 0) {
                return error.EmptyResponse;
            }

            log.debug("PUT response ({d} bytes):\n{s}", .{ n, response_buf[0..n] });

            // Parse response status
            const response = response_buf[0..n];
            if (!std.mem.startsWith(u8, response, "HTTP/1.1 200") and
                !std.mem.startsWith(u8, response, "HTTP/1.1 204"))
            {
                // Extract status line for error
                const line_end = std.mem.indexOf(u8, response, "\r\n") orelse n;
                log.err("S3 PUT failed: {s}", .{response[0..line_end]});
                return error.S3PutFailed;
            }

            log.debug("putObject: success", .{});
        }

        fn resolve(self: *Self) !XevApi.shim_net.Address {
            // Create resolver if needed
            if (self.tp_resolver == null) {
                self.tp_resolver = try self.allocator.create(ThreadPoolResolver);
                self.tp_resolver.?.* = ThreadPoolResolver.init(self.thread_pool, self.allocator);
            }

            const resolver = self.tp_resolver.?.resolver();

            var result = struct {
                addr: ?XevApi.shim_net.Address = null,
                err: ?anyerror = null,
                done: bool = false,

                fn callback(ctx: *@This(), addr_result: anyerror!XevApi.shim_net.Address) void {
                    if (addr_result) |a| {
                        ctx.addr = a;
                    } else |e| {
                        ctx.err = e;
                    }
                    ctx.done = true;
                }
            }{};

            try resolver.resolve(self.loop, self.host, self.port, &result, @TypeOf(result).callback);

            while (!result.done) {
                try self.loop.run(.once);
            }

            if (result.err) |e| return e;
            return result.addr orelse error.ResolveFailed;
        }
    };
}

/// Default S3Client using auto-detected xev backend
pub const S3Client = S3ClientGen(xev);

/// Create an S3Client from environment variables (same pattern as factory.zig)
pub fn createFromEnv(
    allocator: std.mem.Allocator,
    loop: anytype,
    thread_pool: *xev.ThreadPool,
) !*S3ClientGen(@TypeOf(loop.*).Api) {
    const LoopType = @TypeOf(loop.*);
    const XevApi = LoopType.Api;

    // Load config from environment
    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => try allocator.dupe(u8, "us-east-1"),
        else => return err,
    };
    errdefer allocator.free(region);

    const endpoint = std.process.getEnvVarOwned(allocator, "S3_ENDPOINT") catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => return err,
    };

    // Determine host/port/tls
    var host: []const u8 = undefined;
    var port: u16 = 443;
    var use_tls = true;
    var allocated_host = false;

    if (endpoint) |ep| {
        defer allocator.free(ep);
        if (std.mem.startsWith(u8, ep, "https://") or std.mem.startsWith(u8, ep, "http://")) {
            const uri = try std.Uri.parse(ep);
            if (uri.host) |h| {
                host = try allocator.dupe(u8, h.percent_encoded);
                allocated_host = true;
            }
            port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) @as(u16, 443) else 80);
            use_tls = std.mem.eql(u8, uri.scheme, "https");
        } else {
            host = try allocator.dupe(u8, ep);
            allocated_host = true;
        }
    } else {
        host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
        allocated_host = true;
    }
    errdefer if (allocated_host) allocator.free(host);

    const client = try S3ClientGen(XevApi).init(
        allocator,
        loop,
        thread_pool,
        host,
        region,
        use_tls,
        port,
    );

    // Load credentials
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch null;
    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch null;
    const session_token = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;

    if (access_key != null and secret_key != null) {
        client.setCredentials(access_key.?, secret_key.?, session_token);
    }

    return client;
}
