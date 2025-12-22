const std = @import("std");
const zpq = @import("../../../zpq.zig");
const s3 = zpq.s3;
const dns = s3.dns;

pub const S3Context = struct {
    pool: s3.ConnectionPool,
    source: s3.AsyncS3Source,
    allocator: std.mem.Allocator,
    host_owned: ?[]const u8 = null,

    pub fn deinit(self: *S3Context) void {
        self.source.deinit();
        self.pool.deinit();
        if (self.host_owned) |h| self.allocator.free(h);
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

pub fn openS3Source(
    allocator: std.mem.Allocator,
    resolver: dns.Resolver,
    path: []const u8,
    force_async: bool,
) !zpq.file.ParquetFile {
    if (!std.mem.startsWith(u8, path, "s3://")) return error.NotS3Path;

    const s3_path = path[5..];
    const slash_idx = std.mem.indexOf(u8, s3_path, "/") orelse return error.InvalidS3Path;
    const bucket = s3_path[0..slash_idx];
    const key = s3_path[slash_idx + 1 ..];

    if (bucket.len == 0 or key.len == 0) {
        return error.InvalidS3Path;
    }

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

    // Use synchronous S3Source if we have credentials and aren't forcing async
    if (!force_async and config.credentials != null) {
        const s3_src = try allocator.create(s3.S3Source);
        errdefer allocator.destroy(s3_src);
        s3_src.* = try s3.S3Source.init(allocator, bucket, key, config);

        return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, cleanupS3);
    }

    // Parse Endpoint for Async
    var host: []const u8 = "s3.amazonaws.com"; // Default
    var port: u16 = 443;
    var use_tls: bool = true;

    if (endpoint_env) |ep| {
        const uri = try std.Uri.parse(ep);
        if (uri.host) |h| {
            switch (h) {
                .raw => |s| host = s,
                .percent_encoded => |s| host = s,
            }
        } else return error.InvalidEndpoint;

        port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
        use_tls = std.mem.eql(u8, uri.scheme, "https");
    } else {
        // TODO: Region support for host resolution
        if (region_env) |region| {
            if (!std.mem.eql(u8, region, "us-east-1")) {
                // For regions other than us-east-1, we should ideally construct s3.{region}.amazonaws.com
                // However, doing so requires allocation which we must track.
                // For now, sticking to default or S3_ENDPOINT.
            }
        }
    }

    const ctx = try allocator.create(S3Context);
    errdefer allocator.destroy(ctx);

    ctx.allocator = allocator;
    ctx.pool = s3.ConnectionPool.init(allocator);
    errdefer ctx.pool.deinit();

    const host_copy = try allocator.dupe(u8, host);
    errdefer allocator.free(host_copy);
    ctx.host_owned = host_copy;

    const ca_cert_env = try getEnvOrNull(allocator, "S3_CA_CERT");
    defer if (ca_cert_env) |c| allocator.free(c);

    var trusted_cert: ?[]const u8 = null;
    if (ca_cert_env) |path_val| {
        const max_size = 1024 * 1024;
        const content = try std.fs.cwd().readFileAlloc(path_val, allocator, @enumFromInt(max_size));
        trusted_cert = content;
    }
    defer if (trusted_cert) |c| allocator.free(c);

    ctx.source = try s3.AsyncS3Source.init(
        allocator,
        &ctx.pool,
        resolver,
        host_copy,
        port,
        bucket,
        key,
        use_tls,
        trusted_cert,
        if (config.credentials) |_| config else null,
    );

    return zpq.file.ParquetFile.initOwned(allocator, ctx.source.source(), ctx, cleanupAsyncS3);
}

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => |e| return e,
    };
}
