const std = @import("std");
const zpq = @import("../../../zpq.zig");
const s3 = zpq.s3;
const dns = s3.dns;
const xev = @import("xev");

const log = zpq.log.s3;

pub const S3Context = struct {
    pool: s3.ConnectionPool,
    source: s3.AsyncS3Source,
    allocator: std.mem.Allocator,
    host_owned: ?[]const u8 = null,
    thread_pool: xev.ThreadPool,
    tp_resolver: dns.ThreadPoolResolver,
    sf_resolver: dns.SingleFlightResolver,
    spec_resolver: dns.SpeculativeResolver,

    pub fn deinit(self: *S3Context) void {
        log.debug("S3Context.deinit called", .{});
        // Correct order: Pool first (closes connections using loop), then Source (deinits loop), then stack.
        self.pool.deinit();
        self.source.deinit();
        if (self.host_owned) |h| self.allocator.free(h);
        self.spec_resolver.deinit();
        self.sf_resolver.deinit();
        self.tp_resolver.deinit();
        self.thread_pool.shutdown();
        self.thread_pool.deinit();
    }
};

pub fn cleanupS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const s: *s3.S3Source = @ptrCast(@alignCast(ctx));
    s.deinit();
    s.allocator.destroy(s);
}

pub fn cleanupAsyncS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const s: *S3Context = @ptrCast(@alignCast(ctx));
    s.deinit();
    s.allocator.destroy(s);
}

pub fn openFile(allocator: std.mem.Allocator, path: []const u8, force_async: bool) !zpq.file.ParquetFile {
    if (std.mem.startsWith(u8, path, "s3://")) {
        return openS3Internal(allocator, path, force_async);
    }
    return zpq.file.ParquetFile.open(allocator, path);
}

pub fn openS3Source(
    allocator: std.mem.Allocator,
    resolver: dns.Resolver,
    path: []const u8,
    force_async: bool,
) !zpq.file.ParquetFile {
    _ = resolver;
    return openFile(allocator, path, force_async);
}

fn openS3Internal(allocator: std.mem.Allocator, path: []const u8, force_async: bool) !zpq.file.ParquetFile {
    const s3_path = path[5..];
    const slash_idx = std.mem.indexOf(u8, s3_path, "/") orelse return error.InvalidS3Path;
    const bucket = s3_path[0..slash_idx];
    const key = s3_path[slash_idx + 1 ..];

    const endpoint_env = try getEnvOrNull(allocator, "S3_ENDPOINT");
    defer if (endpoint_env) |ep| allocator.free(ep);

    const access_key = try getEnvOrNull(allocator, "AWS_ACCESS_KEY_ID");
    defer if (access_key) |s| allocator.free(s);
    const secret_key = try getEnvOrNull(allocator, "AWS_SECRET_ACCESS_KEY");
    defer if (secret_key) |s| allocator.free(s);
    const session_token = try getEnvOrNull(allocator, "AWS_SESSION_TOKEN");
    defer if (session_token) |s| allocator.free(s);
    const region_env = try getEnvOrNull(allocator, "AWS_REGION");
    defer if (region_env) |s| allocator.free(s);

    const config = s3.S3Config{
        .credentials = if (access_key != null and secret_key != null) .{
            .access_key = access_key.?,
            .secret_key = secret_key.?,
            .session_token = session_token,
        } else null,
        .region = region_env orelse "us-east-1",
        .endpoint = endpoint_env,
    };

    if (!force_async and config.credentials != null) {
        const s3_src = try allocator.create(s3.S3Source);
        errdefer allocator.destroy(s3_src);
        s3_src.* = try s3.S3Source.init(allocator, bucket, key, config);
        return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, cleanupS3);
    }

    // Host discovery based on region to avoid 301
    var host: []const u8 = "s3.amazonaws.com";
    var host_allocated = false;
    if (region_env) |region| {
        if (!std.mem.eql(u8, region, "us-east-1")) {
            host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
            host_allocated = true;
        }
    }
    defer if (host_allocated) allocator.free(host);

    var port: u16 = 443;
    var use_tls: bool = true;

    if (endpoint_env) |ep| {
        const uri = try std.Uri.parse(ep);
        if (uri.host) |h| {
            switch (h) {
                .raw => |s| host = s,
                .percent_encoded => |s| host = s,
            }
        }
        port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
        use_tls = std.mem.eql(u8, uri.scheme, "https");
    }

    // Now create the context and initialize fully
    log.debug("factory: creating S3Context", .{});
    const ctx = try allocator.create(S3Context);
    errdefer allocator.destroy(ctx);

    log.debug("factory: ctx={*}", .{ctx});

    ctx.allocator = allocator;
    ctx.host_owned = try allocator.dupe(u8, host);
    errdefer allocator.free(ctx.host_owned.?);

    ctx.pool = s3.ConnectionPool.init(allocator);
    errdefer ctx.pool.deinit();

    ctx.thread_pool = xev.ThreadPool.init(.{});
    errdefer {
        ctx.thread_pool.shutdown();
        ctx.thread_pool.deinit();
    }

    ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
    errdefer ctx.tp_resolver.deinit();

    log.debug("factory: sf_resolver init, &ctx.sf_resolver={*}", .{&ctx.sf_resolver});
    ctx.sf_resolver = dns.SingleFlightResolver.init(allocator, ctx.tp_resolver.resolver());
    log.debug("factory: sf_resolver initialized, inflight count={d}", .{ctx.sf_resolver.inflight.count()});
    errdefer {
        log.debug("factory: errdefer sf_resolver.deinit, &ctx.sf_resolver={*}", .{&ctx.sf_resolver});
        ctx.sf_resolver.deinit();
    }

    ctx.spec_resolver = dns.SpeculativeResolver.init(allocator, ctx.sf_resolver.resolver());
    errdefer ctx.spec_resolver.deinit();

    log.debug("factory: calling AsyncS3Source.init", .{});
    ctx.source = try s3.AsyncS3Source.init(allocator, &ctx.pool, ctx.spec_resolver.resolver(), ctx.host_owned.?, port, bucket, key, use_tls, null, config);
    errdefer ctx.source.deinit();

    // Note: Don't manually call cleanupAsyncS3 on error - the errdefers above handle cleanup.
    // Only cleanupAsyncS3 should be called later when ParquetFile.deinit() runs on success path.
    return zpq.file.ParquetFile.initOwned(allocator, ctx.source.source(), ctx, cleanupAsyncS3);
}

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => |e| return e,
    };
}
