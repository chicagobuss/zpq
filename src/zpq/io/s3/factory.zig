const std = @import("std");
const zpq = @import("../../../zpq.zig");
const s3 = zpq.s3;
const xev = @import("xev");

const log = zpq.log.s3;

/// Options for opening a Parquet file from S3 or Local storage.
/// Uses xev.Dynamic for runtime backend selection (io_uring -> epoll fallback).
pub const OpenOptions = struct {
    force_async: bool = false,
    verify_tls: bool = false,
    loop: ?*xev.Dynamic.Loop = null,
    thread_pool: ?*xev.ThreadPool = null,
    resolver: ?s3.dns.ResolverGen(xev.Dynamic) = null,
};

/// Cleanup helper for S3 sources owned by ParquetFile.
pub fn cleanupS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const src: *s3.S3Source = @ptrCast(@alignCast(ctx));
    src.deinit();
    src.allocator.destroy(src);
}

/// Main entry point for opening a Parquet file.
/// Automatically detects s3:// vs local paths.
pub fn openFile(allocator: std.mem.Allocator, path: []const u8, force_async: bool) !zpq.file.ParquetFile {
    return openFileWithOptions(allocator, path, .{ .force_async = force_async });
}

pub fn openFileWithOptions(
    allocator: std.mem.Allocator,
    path: []const u8,
    options: OpenOptions,
) !zpq.file.ParquetFile {
    var pf = if (std.mem.startsWith(u8, path, "s3://")) blk: {
        if (options.loop) |loop| {
            break :blk try openS3WithLoop(allocator, loop, options.thread_pool.?, path, options);
        }
        break :blk try openS3Internal(allocator, path, options);
    } else try zpq.file.ParquetFile.openMmap(allocator, path);

    errdefer pf.deinit();
    try pf.readFooter();
    return pf;
}

/// Low-level S3 opener that accepts an existing xev Loop.
/// Useful for Lambda or other environments where the loop is managed externally.
pub fn openS3WithLoop(
    allocator: std.mem.Allocator,
    loop: anytype,
    thread_pool: *xev.ThreadPool,
    path: []const u8,
    options: OpenOptions,
) !zpq.file.ParquetFile {
    const LoopType = @TypeOf(loop.*);
    const builtin = @import("builtin");

    // Discover the specific Xev API type based on the Loop type
    const XevApi = switch (LoopType) {
        xev.Dynamic.Loop => xev.Dynamic,
        xev.Epoll.Loop => xev.Epoll,
        xev.IO_Uring.Loop => xev.IO_Uring,
        else => blk: {
            if (builtin.os.tag == .macos or builtin.os.tag == .freebsd or builtin.os.tag == .openbsd or builtin.os.tag == .dragonfly) {
                if (LoopType == xev.Kqueue.Loop) break :blk xev.Kqueue;
            }
            break :blk xev;
        },
    };

    const XevS3Source = @import("xev_source.zig").XevS3SourceGen(XevApi);

    const s3_info = try parseS3Path(path);
    const config = try loadS3ConfigFromEnv(allocator);
    defer config.deinit(allocator);

    const discovery = try discoverS3Endpoint(allocator, config.endpoint, config.region);
    defer discovery.deinit(allocator);

    const s3_src = try XevS3Source.initWithLoop(
        allocator,
        loop,
        thread_pool,
        discovery.host,
        s3_info.bucket,
        s3_info.key,
        config.region,
        discovery.use_tls,
        discovery.port,
        4096, // TODO: Configurable buffer size
        true, // use_direct
        null, // Use global pool
        .{
            .verify_certificate = options.verify_tls,
            .resolver = if (options.resolver) |r| .{
                .ptr = r.ptr,
                .vtable = @ptrCast(@alignCast(r.vtable)),
            } else null,
        },
    );

    if (config.credentials) |creds| {
        try s3_src.setCredentials(creds.access_key, creds.secret_key, creds.session_token);
    }

    const Cleanup = struct {
        fn func(ctx: *anyopaque, alloc: std.mem.Allocator) void {
            _ = alloc;
            const src: *XevS3Source = @ptrCast(@alignCast(ctx));
            src.deinit();
            src.allocator.destroy(src);
        }
    }.func;

    return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, Cleanup);
}

/// Internal S3 opener that creates its own IO resources if needed.
pub fn openS3Internal(allocator: std.mem.Allocator, path: []const u8, options: OpenOptions) !zpq.file.ParquetFile {
    const s3_info = try parseS3Path(path);
    const config = try loadS3ConfigFromEnv(allocator);
    defer config.deinit(allocator);

    // Fallback to legacy sync stack ONLY if requested and not forced to async.
    // In production, we prefer the Xev stack for its superior performance and pooling.
    if (!options.force_async and config.credentials != null and !(std.process.hasEnvVar(allocator, "ZPQ_FORCE_XEV") catch false)) {
        const s3_src = try allocator.create(s3.S3Source);
        errdefer allocator.destroy(s3_src);
        s3_src.* = try s3.S3Source.init(allocator, s3_info.bucket, s3_info.key, config.toLegacy());
        return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, cleanupS3);
    }

    const discovery = try discoverS3Endpoint(allocator, config.endpoint, config.region);
    defer discovery.deinit(allocator);

    // New high-performance Xev stack (creates its own loop)
    // Use xev.Dynamic for runtime backend selection (io_uring -> epoll fallback)
    const XevS3Source = @import("xev_source.zig").XevS3SourceGen(xev.Dynamic);
    const s3_src = try XevS3Source.initWithOptions(
        allocator,
        discovery.host,
        s3_info.bucket,
        s3_info.key,
        config.region,
        discovery.use_tls,
        discovery.port,
        .{
            .verify_certificate = options.verify_tls,
            .resolver = options.resolver,
        },
    );

    if (config.credentials) |creds| {
        try s3_src.setCredentials(creds.access_key, creds.secret_key, creds.session_token);
    }

    const Cleanup = struct {
        fn func(ctx: *anyopaque, alloc: std.mem.Allocator) void {
            _ = alloc;
            const src: *XevS3Source = @ptrCast(@alignCast(ctx));
            src.deinit();
            src.allocator.destroy(src);
        }
    }.func;

    return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, Cleanup);
}

// --- Internal Helpers ---

const S3PathInfo = struct {
    bucket: []const u8,
    key: []const u8,
};

fn parseS3Path(path: []const u8) !S3PathInfo {
    if (!std.mem.startsWith(u8, path, "s3://")) return error.InvalidS3Path;
    const s3_path = path[5..];
    const slash_idx = std.mem.indexOf(u8, s3_path, "/") orelse return error.InvalidS3Path;
    return S3PathInfo{
        .bucket = s3_path[0..slash_idx],
        .key = s3_path[slash_idx + 1 ..],
    };
}

pub const ManagedS3Config = struct {
    credentials: ?s3.Credentials = null,
    region: []const u8,
    endpoint: ?[]const u8 = null,

    pub fn deinit(self: *const ManagedS3Config, allocator: std.mem.Allocator) void {
        if (self.credentials) |creds| {
            allocator.free(creds.access_key);
            allocator.free(creds.secret_key);
            if (creds.session_token) |t| allocator.free(t);
        }
        allocator.free(self.region);
        if (self.endpoint) |ep| allocator.free(ep);
    }

    pub fn toLegacy(self: ManagedS3Config) s3.S3Config {
        return .{
            .credentials = self.credentials,
            .region = self.region,
            .endpoint = self.endpoint,
        };
    }
};

pub fn loadS3ConfigFromEnv(allocator: std.mem.Allocator) !ManagedS3Config {
    const access_key = try getEnvOrNull(allocator, "AWS_ACCESS_KEY_ID");
    errdefer if (access_key) |s| allocator.free(s);
    const secret_key = try getEnvOrNull(allocator, "AWS_SECRET_ACCESS_KEY");
    errdefer if (secret_key) |s| allocator.free(s);
    const session_token = try getEnvOrNull(allocator, "AWS_SESSION_TOKEN");
    errdefer if (session_token) |s| allocator.free(s);
    const region_env = try getEnvOrNull(allocator, "AWS_REGION");
    errdefer if (region_env) |s| allocator.free(s);

    return ManagedS3Config{
        .credentials = if (access_key != null and secret_key != null) .{
            .access_key = access_key.?,
            .secret_key = secret_key.?,
            .session_token = session_token,
        } else blk: {
            if (access_key) |s| allocator.free(s);
            if (secret_key) |s| allocator.free(s);
            if (session_token) |s| allocator.free(s);
            break :blk null;
        },
        .region = region_env orelse try allocator.dupe(u8, "us-east-1"),
        .endpoint = try getEnvOrNull(allocator, "S3_ENDPOINT"),
    };
}

pub const EndpointDiscovery = struct {
    host: []const u8,
    port: u16,
    use_tls: bool,
    allocated_host: bool = false,

    pub fn deinit(self: EndpointDiscovery, allocator: std.mem.Allocator) void {
        if (self.allocated_host) allocator.free(self.host);
    }
};

pub fn discoverS3Endpoint(allocator: std.mem.Allocator, endpoint_env: ?[]const u8, region: []const u8) !EndpointDiscovery {
    var disc = EndpointDiscovery{
        .host = "s3.amazonaws.com",
        .port = 443,
        .use_tls = true,
    };

    if (endpoint_env) |ep| {
        const has_scheme = std.mem.startsWith(u8, ep, "https://") or std.mem.startsWith(u8, ep, "http://");
        if (has_scheme) {
            const uri = try std.Uri.parse(ep);
            if (uri.host) |h| {
                const host_slice = switch (h) {
                    .raw => |s| s,
                    .percent_encoded => |s| s,
                };
                disc.host = try allocator.dupe(u8, host_slice);
                disc.allocated_host = true;
            }
            disc.port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
            disc.use_tls = std.mem.eql(u8, uri.scheme, "https");
        } else {
            disc.host = try allocator.dupe(u8, ep);
            disc.allocated_host = true;
        }
    } else if (!std.mem.eql(u8, region, "us-east-1")) {
        disc.host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
        disc.allocated_host = true;
    }

    return disc;
}

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    const val = std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => return null,
        else => |e| return e,
    };
    // Treat empty string as null (e.g., Lambda RIE sets AWS_SESSION_TOKEN="")
    if (val.len == 0) {
        allocator.free(val);
        return null;
    }
    return val;
}
