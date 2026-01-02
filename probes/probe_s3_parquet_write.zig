const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

const S3Writer = zpq.s3.S3WriterGen(xev);
const ParquetFile = zpq.core.ParquetFile;
const LocalSource = zpq.io.interface.local.LocalSource;

/// Baseline test: Read a local parquet file, write it to S3 via S3Writer.
/// This verifies S3Writer works for real parquet data before integrating into pipeline.
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // rustfs local credentials
    const access_key = "rustfsadmin";
    const secret_key = "rustfsadmin";
    const bucket = "test-bucket";
    const region = "us-east-1";
    const output_key = "probe_parquet_output.parquet";

    // Use a test parquet file
    const input_path = "testdata/generated_1000.parquet";

    std.debug.print("=== S3 Parquet Write Probe ===\n", .{});
    std.debug.print("Input: {s}\n", .{input_path});
    std.debug.print("Output: s3://{s}/{s}\n\n", .{ bucket, output_key });

    // Read the local parquet file
    std.debug.print("1. Reading local parquet file...\n", .{});
    const file_data = std.fs.cwd().readFileAlloc(input_path, 100 * 1024 * 1024, allocator) catch |err| {
        std.debug.print("   ERROR: Could not read {s}: {}\n", .{ input_path, err });
        std.debug.print("   Try: zig build testdata\n", .{});
        return err;
    };
    defer allocator.free(file_data);
    std.debug.print("   Read {d} bytes\n", .{file_data.len});

    // Create S3Writer
    std.debug.print("\n2. Creating S3Writer...\n", .{});
    var s3w = try S3Writer.init(allocator, bucket, output_key, region);
    defer s3w.deinit();

    // Point to local rustfs
    allocator.free(s3w.host);
    s3w.host = try allocator.dupe(u8, "localhost");
    s3w.port = 9999;
    s3w.use_tls = true;
    s3w.use_path_style = true;

    try s3w.setCredentials(access_key, secret_key, null);
    std.debug.print("   Host: {s}:{d}\n", .{ s3w.host, s3w.port });

    // Write parquet data to S3
    std.debug.print("\n3. Writing to S3...\n", .{});
    const start = try std.time.Instant.now();

    try s3w.writeAll(file_data);
    try s3w.finish();

    const end = try std.time.Instant.now();
    const elapsed_ns = end.since(start);
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
    const throughput = (@as(f64, @floatFromInt(file_data.len)) / (1024.0 * 1024.0)) / (elapsed_ms / 1000.0);

    std.debug.print("   Wrote {d} bytes in {d:.1}ms ({d:.1} MB/s)\n", .{
        file_data.len,
        elapsed_ms,
        throughput,
    });

    std.debug.print("\n=== SUCCESS ===\n", .{});
    std.debug.print("Verify with: AWS_ACCESS_KEY_ID=rustfsadmin AWS_SECRET_ACCESS_KEY=rustfsadmin aws --endpoint-url https://localhost:9999 --no-verify-ssl s3 ls s3://{s}/{s}\n", .{ bucket, output_key });
}
