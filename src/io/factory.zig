const std = @import("std");
const interface = @import("interface.zig");
const local = @import("local.zig");
const s3 = @import("s3.zig");
const xev = @import("xev");
const protocol_s3 = @import("../protocol/s3.zig");

pub const FactoryOptions = struct {
    loop: ?*xev.Dynamic.Loop = null,
    thread_pool: ?*xev.ThreadPool = null,
};

pub fn openSource(allocator: std.mem.Allocator, path: []const u8, options: FactoryOptions) !interface.RandomAccessSource {
    if (std.mem.startsWith(u8, path, "s3://")) {
        const bucket_end = std.mem.indexOfScalarPos(u8, path, 5, '/') orelse return error.InvalidS3Path;
        const bucket = path[5..bucket_end];
        const key = path[bucket_end + 1 ..];

        if (options.loop == null) return error.LoopRequiredForS3;
        if (options.thread_pool == null) return error.ThreadPoolRequiredForS3;

        // Fetch credentials from environment
        const access_key = std.posix.getenv("AWS_ACCESS_KEY_ID") orelse return error.MissingAWSAccessKey;
        const secret_key = std.posix.getenv("AWS_SECRET_ACCESS_KEY") orelse return error.MissingAWSSecretKey;
        const region = std.posix.getenv("AWS_REGION") orelse "us-east-1";
        const session_token = std.posix.getenv("AWS_SESSION_TOKEN");

        const s3_config = protocol_s3.S3.init(bucket, region, access_key, secret_key, session_token);

        const Xev = if (@hasDecl(@TypeOf(options.loop.?.*), "connect")) xev.Dynamic else xev;
        const S3Source = s3.AsyncS3SourceGen(Xev);
        var s3_source = try allocator.create(S3Source);
        errdefer allocator.destroy(s3_source);

        s3_source.* = try S3Source.init(allocator, @ptrCast(options.loop.?), options.thread_pool.?, s3_config, bucket, key);
        return s3_source.randomAccessSource();
    } else {
        var file = try std.fs.cwd().openFile(path, .{});
        var local_source = try allocator.create(local.AsyncFileSource);
        errdefer {
            allocator.destroy(local_source);
            file.close();
        }

        local_source.* = try local.AsyncFileSource.init(allocator, file);
        return local_source.randomAccessSource();
    }
}
