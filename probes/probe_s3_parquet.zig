const std = @import("std");
const zpq = @import("zpq");
const io = zpq.io.s3;
const core = zpq.core.file;
const protocol = zpq.protocol;
const xev = @import("xev");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // 1. Env
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch return error.MissingAWSAccessKey;
    defer allocator.free(access_key);
    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch return error.MissingAWSSecretKey;
    defer allocator.free(secret_key);
    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch try allocator.dupe(u8, "us-west-2");
    defer allocator.free(region);
    const bucket = std.process.getEnvVarOwned(allocator, "AWS_S3_BUCKET") catch return error.MissingBucket;
    defer allocator.free(bucket);

    // Use the 100MB benchmark file we know exists
    const key = "zpq_test_data/benchmark/benchmark_100mb.parquet";

    std.log.info("S3 Parquet Probe: {s}/{s}", .{ bucket, key });

    // 2. Loop & ThreadPool
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer thread_pool.deinit();

    // 3. Init S3 Source
    const s3_proto = protocol.s3.S3.init(bucket, region, access_key, secret_key, null);

    var source = try io.AsyncS3Source.init(allocator, &loop, &thread_pool, s3_proto, bucket, key);
    defer source.deinit();

    std.log.info("HEAD successful. Content-Length: {d}", .{source.content_length});

    // 4. Parquet File
    var parquet_file = core.ParquetFile.init(allocator, source.randomAccessSource());
    defer parquet_file.deinit();

    // 5. Read Footer
    std.log.info("Reading Footer...", .{});
    try parquet_file.readFooter(); // This will trigger readAt -> S3 GET Range

    std.log.info("Parsed Footer successfully!", .{});
    std.log.info("Num Row Groups: {d}", .{parquet_file.numRowGroups()});
    std.log.info("Num Rows: {d}", .{parquet_file.numRows()});

    const schema_len = parquet_file.metadata.schema.items.len;
    std.log.info("Schema elements: {d}", .{schema_len});

    // Print first few columns
    for (parquet_file.metadata.schema.items, 0..) |elem, i| {
        if (i > 5) break;
        std.log.info("  [{d}] {s}", .{ i, elem.name });
    }
}
