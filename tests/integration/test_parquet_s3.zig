const std = @import("std");
const zpq = @import("zpq");
const ParquetFile = zpq.file.ParquetFile;

pub const std_options = std.Options{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .s3_source, .level = .debug },
        .{ .scope = .tls, .level = .debug },
    },
};

pub fn main() !void {
    // Ignore SIGPIPE
    if (@import("builtin").os.tag != .windows) {
        std.posix.sigaction(std.posix.SIG.PIPE, &.{
            .handler = .{ .handler = std.posix.SIG.IGN },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        }, null);
    }

    if (std.posix.getenv("ZPQ_TEST_MINIO") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_MINIO=1 to run\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = "127.0.0.1";
    const bucket = "zpq-ci";
    const key = "simple.parquet";
    const region = "us-east-1";
    const port = 9000;

    std.debug.print("Opening Parquet from MinIO S3...\n", .{});
    var file = try ParquetFile.openS3(allocator, host, bucket, key, region, true, port);
    defer file.deinit();

    std.debug.print("Reading Footer...\n", .{});
    try file.readFooter();

    if (file.metadata) |meta| {
        std.debug.print("Success! Version: {d}, Row Groups: {d}\n", .{meta.version, meta.row_groups.items.len});
        
        for (meta.schema.items, 0..) |s, i| {
            std.debug.print("Column {d}: {s}\n", .{i, s.name});
        }
    } else {
        return error.NoMetadata;
    }

    std.debug.print("End-to-End Parquet S3 Test OK\n", .{});
}

