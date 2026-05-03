const std = @import("std");
const interface = @import("interface.zig");
const sink_mod = @import("sink.zig");
const local = @import("local.zig");
const s3 = @import("s3.zig");
const xev = @import("xev");
const protocol_s3 = @import("../protocol/s3.zig");

pub fn openSource(allocator: std.mem.Allocator, path: []const u8, options: anytype) !interface.RandomAccessSource {
    if (std.mem.startsWith(u8, path, "s3://")) {
        const bucket_end = std.mem.indexOfScalarPos(u8, path, 5, '/') orelse return error.InvalidS3Path;
        const bucket = path[5..bucket_end];
        const key = path[bucket_end + 1 ..];

        const loop_ptr = if (@typeInfo(@TypeOf(options.loop)) == .optional)
            (options.loop orelse return error.LoopRequiredForS3)
        else
            options.loop;

        if (!@hasField(@TypeOf(options), "thread_pool")) return error.ThreadPoolRequiredForS3;
        const pool_ptr = if (@typeInfo(@TypeOf(options.thread_pool)) == .optional)
            (options.thread_pool orelse return error.ThreadPoolRequiredForS3)
        else
            options.thread_pool;

        // Fetch credentials from environment
        const access_key = std.posix.getenv("AWS_ACCESS_KEY_ID") orelse return error.MissingAWSAccessKey;
        const secret_key = std.posix.getenv("AWS_SECRET_ACCESS_KEY") orelse return error.MissingAWSSecretKey;
        const region = std.posix.getenv("AWS_REGION") orelse "us-east-1";
        const session_token = std.posix.getenv("AWS_SESSION_TOKEN");

        const s3_config = protocol_s3.S3.init(bucket, region, access_key, secret_key, session_token);

        const LoopType = @TypeOf(loop_ptr.*);
        const Xev = if (LoopType == xev.Dynamic.Loop) xev.Dynamic
                   else if (@hasDecl(xev, "Epoll") and LoopType == xev.Epoll.Loop) xev.Epoll
                   else xev;
        const S3Source = s3.AsyncS3SourceGen(Xev);
        var s3_source = try allocator.create(S3Source);
        errdefer allocator.destroy(s3_source);

        s3_source.* = try S3Source.init(allocator, @ptrCast(loop_ptr), pool_ptr, s3_config, bucket, key);
        return s3_source.randomAccessSource();
    } else {
        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);

        const fd = try std.posix.open(path_z, std.posix.O{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
        
        var local_source = try allocator.create(local.AsyncFileSource);
        errdefer {
            std.posix.close(fd);
            allocator.destroy(local_source);
        }
        
        local_source.* = try local.AsyncFileSource.init(allocator, fd);
        
        return local_source.randomAccessSource();
    }
}

pub fn openSink(allocator: std.mem.Allocator, path: []const u8, options: anytype) !sink_mod.Sink {
    if (std.mem.startsWith(u8, path, "s3://")) {
        const bucket_end = std.mem.indexOfScalarPos(u8, path, 5, '/') orelse return error.InvalidS3Path;
        const bucket = path[5..bucket_end];
        const key = path[bucket_end + 1 ..];

        if (!@hasField(@TypeOf(options), "loop")) return error.LoopRequiredForS3;
        const loop_ptr = if (@typeInfo(@TypeOf(options.loop)) == .optional)
            (options.loop orelse return error.LoopRequiredForS3)
        else
            options.loop;

        if (!@hasField(@TypeOf(options), "thread_pool")) return error.ThreadPoolRequiredForS3;
        const pool_ptr = if (@typeInfo(@TypeOf(options.thread_pool)) == .optional)
            (options.thread_pool orelse return error.ThreadPoolRequiredForS3)
        else
            options.thread_pool;

        const access_key = std.posix.getenv("AWS_ACCESS_KEY_ID") orelse return error.MissingAWSAccessKey;
        const secret_key = std.posix.getenv("AWS_SECRET_ACCESS_KEY") orelse return error.MissingAWSSecretKey;
        const region = std.posix.getenv("AWS_REGION") orelse "us-east-1";
        const session_token = std.posix.getenv("AWS_SESSION_TOKEN");

        const s3_config = protocol_s3.S3.init(bucket, region, access_key, secret_key, session_token);

        const LoopType = @TypeOf(loop_ptr.*);
        const Xev = if (LoopType == xev.Dynamic.Loop) xev.Dynamic
                   else if (@hasDecl(xev, "Epoll") and LoopType == xev.Epoll.Loop) xev.Epoll
                   else xev;
        const S3Sink = @import("s3_sink.zig").AsyncS3SinkGen(Xev);
        const sink_ptr = try S3Sink.init(allocator, @ptrCast(loop_ptr), pool_ptr, s3_config, bucket, key);
        return sink_ptr.sink();
    } else {
        const path_z = try allocator.dupeZ(u8, path);
        defer allocator.free(path_z);

        const fd = try std.posix.open(path_z, std.posix.O{ .ACCMODE = .WRONLY, .CREAT = true, .TRUNC = true, .CLOEXEC = true }, 0o644);
        const local_sink = @import("local_sink.zig");
        
        var sink_ptr = try allocator.create(local_sink.AsyncFileSink);
        errdefer {
            allocator.destroy(sink_ptr);
            std.posix.close(fd);
        }

        sink_ptr.* = local_sink.AsyncFileSink.init(fd);
        return sink_ptr.sink();
    }
}
