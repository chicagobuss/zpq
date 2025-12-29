const std = @import("std");
const zpq = @import("../../../zpq.zig");
const s3 = zpq.s3;
const xev = @import("xev");

const log = zpq.log.s3;

pub fn cleanupS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const src: *s3.S3Source = @ptrCast(@alignCast(ctx));
    src.deinit();
    src.allocator.destroy(src);
}

    pub const OpenOptions = struct {
        force_async: bool = false,
        verify_tls: bool = false,
    };

    pub fn openFile(allocator: std.mem.Allocator, path: []const u8, force_async: bool) !zpq.file.ParquetFile {
        return openFileWithOptions(allocator, path, .{ .force_async = force_async });
    }

    pub fn openFileWithOptions(
        allocator: std.mem.Allocator,
        path: []const u8,
        options: struct {
            force_async: bool = false,
            loop: ?*xev.Loop = null,
            thread_pool: ?*xev.ThreadPool = null,
            resolver: ?zpq.s3.dns.ResolverGen(xev) = null,
            verify_tls: bool = false,
        },
    ) !zpq.file.ParquetFile {
    if (std.mem.startsWith(u8, path, "s3://")) {
        if (options.loop) |loop| {
            return openS3WithLoop(allocator, loop, options.thread_pool.?, path, .{
                .force_async = options.force_async,
                .verify_tls = options.verify_tls,
            });
        }
        return openS3Internal(allocator, path, .{
            .force_async = options.force_async,
            .verify_tls = options.verify_tls,
        });
    }
    return zpq.file.ParquetFile.open(allocator, path);
}

pub fn openS3WithLoop(allocator: std.mem.Allocator, loop: anytype, thread_pool: anytype, path: []const u8, options: OpenOptions) !zpq.file.ParquetFile {
    const LoopType = @TypeOf(loop.*);
    const builtin = @import("builtin");
    const XevApi = switch (LoopType) {
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

    const region = region_env orelse "us-east-1";

    // Host discovery based on region to avoid 301
    var host: []const u8 = "s3.amazonaws.com";
    var host_allocated = false;

    var port: u16 = 443;
    var use_tls: bool = true;

    if (endpoint_env) |ep| {
        const has_scheme = std.mem.startsWith(u8, ep, "https://") or std.mem.startsWith(u8, ep, "http://");
        if (has_scheme) {
            const uri = try std.Uri.parse(ep);
            if (uri.host) |h| {
                const host_slice = switch (h) {
                    .raw => |s| s,
                    .percent_encoded => |s| s,
                };
                host = try allocator.dupe(u8, host_slice);
                host_allocated = true;
            }
            port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
            use_tls = std.mem.eql(u8, uri.scheme, "https");
        } else {
            host = try allocator.dupe(u8, ep);
            host_allocated = true;
        }
    } else if (region_env) |reg| {
        if (!std.mem.eql(u8, reg, "us-east-1")) {
            host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{reg});
            host_allocated = true;
        }
    }
    defer if (host_allocated) allocator.free(host);

    const s3_src = try XevS3Source.initWithLoop(
        allocator,
        loop,
        thread_pool,
        host,
        bucket,
        key,
        region,
        use_tls,
        port,
        4096,
        true,
        null, // Use global pool
        .{ .verify_certificate = options.verify_tls },
    );

    if (access_key != null and secret_key != null) {
        try s3_src.setCredentials(access_key.?, secret_key.?, session_token);
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

pub fn openS3Internal(allocator: std.mem.Allocator, path: []const u8, options: OpenOptions) !zpq.file.ParquetFile {
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

    if (!options.force_async and config.credentials != null) {
        const s3_src = try allocator.create(s3.S3Source);
        errdefer allocator.destroy(s3_src);
        s3_src.* = try s3.S3Source.init(allocator, bucket, key, config);
        return zpq.file.ParquetFile.initOwned(allocator, s3_src.source(), s3_src, cleanupS3);
    }

    // Host discovery based on region to avoid 301
    var host: []const u8 = "s3.amazonaws.com";
    var host_allocated = false;

    var port: u16 = 443;
    var use_tls: bool = true;

    if (endpoint_env) |ep| {
        // Support both "https://host.com" and bare "host.com" formats
        const has_scheme = std.mem.startsWith(u8, ep, "https://") or std.mem.startsWith(u8, ep, "http://");
        if (has_scheme) {
            const uri = try std.Uri.parse(ep);
            if (uri.host) |h| {
                const host_slice = switch (h) {
                    .raw => |s| s,
                    .percent_encoded => |s| s,
                };
                host = try allocator.dupe(u8, host_slice);
                host_allocated = true;
            }
            port = uri.port orelse (if (std.mem.eql(u8, uri.scheme, "https")) 443 else 80);
            use_tls = std.mem.eql(u8, uri.scheme, "https");
        } else {
            // Bare hostname - assume HTTPS
            host = try allocator.dupe(u8, ep);
            host_allocated = true;
        }
    } else if (region_env) |region| {
        if (!std.mem.eql(u8, region, "us-east-1")) {
            host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
            host_allocated = true;
        }
    }
    defer if (host_allocated) allocator.free(host);

    // New high-performance Xev stack (using default xev.Loop)
    const XevS3Source = @import("xev_source.zig").XevS3SourceGen(xev);
    const s3_src = try XevS3Source.initWithOptions(
        allocator,
        host,
        bucket,
        key,
        config.region,
        use_tls,
        port,
        .{ .verify_certificate = options.verify_tls },
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

fn getEnvOrNull(allocator: std.mem.Allocator, key: []const u8) !?[]const u8 {
    return std.process.getEnvVarOwned(allocator, key) catch |err| switch (err) {
        error.EnvironmentVariableNotFound => null,
        else => |e| return e,
    };
}
