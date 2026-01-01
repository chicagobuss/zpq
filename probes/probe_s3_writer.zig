const std = @import("std");
const zpq = @import("zpq");

const S3WriterGen = zpq.s3.S3WriterGen;
const xev = @import("xev");

// Use default xev for macOS (kqueue)
const S3Writer = S3WriterGen(xev);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // rustfs local credentials
    const access_key = "rustfsadmin";
    const secret_key = "rustfsadmin";
    const bucket = "test-bucket";
    const region = "us-east-1";
    const key = "probe_s3_writer_output.txt";

    std.debug.print("=== S3Writer Probe (rustfs local) ===\n", .{});
    std.debug.print("Bucket: {s}\n", .{bucket});
    std.debug.print("Key: {s}\n", .{key});

    // Create writer
    std.debug.print("\n1. Creating S3Writer...\n", .{});
    var s3w = try S3Writer.init(allocator, bucket, key, region);
    defer s3w.deinit();

    // Point to local rustfs instead of AWS
    allocator.free(s3w.host);
    s3w.host = try allocator.dupe(u8, "localhost");
    s3w.port = 9999;
    s3w.use_tls = true; // rustfs has TLS enabled
    s3w.use_path_style = true; // rustfs/MinIO needs path-style URLs

    // Disable UNSIGNED-PAYLOAD for rustfs compatibility (it may not support it)
    s3w.use_unsigned_payload = false;
    std.debug.print("   UNSIGNED-PAYLOAD: disabled (rustfs compat)\n", .{});
    std.debug.print("   Path-style: enabled (for rustfs)\n", .{});

    // Set credentials
    try s3w.setCredentials(access_key, secret_key, null);
    std.debug.print("   Credentials: set\n", .{});

    // Test: Large file to trigger multipart
    // S3 requires minimum 5MB per part (except last part)
    s3w.part_size = 5 * 1024 * 1024; // 5MB parts (S3 minimum)
    s3w.multipart_threshold = 5 * 1024 * 1024;
    s3w.max_concurrent_uploads = 4; // Parallel uploads!

    std.debug.print("\n2. Writing 12MB test data (should use multipart with 5MB parts)...\n", .{});

    const upload_start = try std.time.Instant.now();

    // Write 12MB of data in chunks (will create 3 parts: 5MB, 5MB, 2MB)
    const chunk = "0123456789ABCDEF" ** 64; // 1KB
    const total_size = 12 * 1024 * 1024; // 12MB
    var written: usize = 0;
    while (written < total_size) {
        try s3w.writeAll(chunk);
        written += chunk.len;
    }
    std.debug.print("   Wrote: {d} bytes ({d} MB)\n", .{ written, written / (1024 * 1024) });

    // Finish the upload
    std.debug.print("\n3. Finishing upload...\n", .{});
    try s3w.finish();

    const upload_end = try std.time.Instant.now();
    const total_elapsed = upload_end.since(upload_start);
    const total_ms = @as(f64, @floatFromInt(total_elapsed)) / 1_000_000.0;
    const total_throughput = (@as(f64, @floatFromInt(written)) / (1024.0 * 1024.0)) / (total_ms / 1000.0);

    std.debug.print("\n=== SUCCESS ===\n", .{});
    std.debug.print("File uploaded to s3://{s}/{s}\n", .{ bucket, key });
    std.debug.print("\n=== TIMING (PARALLEL via xev) ===\n", .{});
    std.debug.print("Total: {d:.1}ms for {d} MB = {d:.1} MB/s\n", .{ total_ms, written / (1024 * 1024), total_throughput });
    std.debug.print("Parts: {d} completed\n", .{s3w.parts.items.len});
}
