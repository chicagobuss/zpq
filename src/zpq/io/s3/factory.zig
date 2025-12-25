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

    // New high-performance Xev stack
    const s3_src = try s3.XevS3Source.init(
        allocator,
        host,
        bucket,
        key,
        config.region,
        use_tls,
        port,
    );
    // Note: ParquetFile.openS3 takes ownership of s3_src and will call its deinit.
    
    if (config.credentials) |creds| {
        s3_src.access_key = try allocator.dupe(u8, creds.access_key);
        s3_src.secret_key = try allocator.dupe(u8, creds.secret_key);
        if (creds.session_token) |st| {
            s3_src.session_token = try allocator.dupe(u8, st);
        }
    }

    return zpq.file.ParquetFile.openS3(allocator, s3_src);
}

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => |e| return e,
    };
}
