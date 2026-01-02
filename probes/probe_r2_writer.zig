const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

const S3WriterGen = zpq.s3.S3WriterGen;
const S3Writer = S3WriterGen(xev);

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // R2 credentials from environment
    const access_key = std.posix.getenv("R2_ACCESS_KEY_ID") orelse {
        std.debug.print("ERROR: R2_ACCESS_KEY_ID not set\n", .{});
        return error.MissingCredentials;
    };
    const secret_key = std.posix.getenv("R2_SECRET_ACCESS_KEY") orelse {
        std.debug.print("ERROR: R2_SECRET_ACCESS_KEY not set\n", .{});
        return error.MissingCredentials;
    };
    const account_id = std.posix.getenv("R2_ACCOUNT_ID") orelse {
        std.debug.print("ERROR: R2_ACCOUNT_ID not set\n", .{});
        return error.MissingCredentials;
    };
    const bucket = std.posix.getenv("R2_BUCKET") orelse "zpq";

    const region = "auto"; // R2 uses "auto"
    const key = "probe_r2_writer_test.bin";

    std.debug.print("=== S3Writer Probe (R2) ===\n", .{});
    std.debug.print("Account: {s}\n", .{account_id});
    std.debug.print("Bucket: {s}\n", .{bucket});
    std.debug.print("Key: {s}\n", .{key});

    // Create writer
    std.debug.print("\n1. Creating S3Writer...\n", .{});
    var s3w = try S3Writer.init(allocator, bucket, key, region);
    defer s3w.deinit();

    // Point to R2 endpoint
    allocator.free(s3w.host);
    const host = try std.fmt.allocPrint(allocator, "{s}.r2.cloudflarestorage.com", .{account_id});
    s3w.host = host;
    s3w.port = 443;
    s3w.use_tls = true;
    s3w.use_path_style = false; // R2 uses virtual-hosted style

    std.debug.print("   Host: {s}\n", .{s3w.host});

    // Set credentials (no session token for R2)
    try s3w.setCredentials(access_key, secret_key, null);
    std.debug.print("   Credentials: set\n", .{});

    // Test with smaller size - 128KB (this worked in parallel probe)
    const test_size = 128 * 1024; // 128KB
    s3w.part_size = 5 * 1024 * 1024; // 5MB parts (won't trigger multipart)
    s3w.multipart_threshold = 5 * 1024 * 1024;

    std.debug.print("\n2. Writing {d}KB test data (single PUT, no multipart)...\n", .{test_size / 1024});

    const upload_start = try std.time.Instant.now();

    // Write test data
    const chunk = "X" ** 1024; // 1KB
    var written: usize = 0;
    while (written < test_size) {
        try s3w.writeAll(chunk);
        written += chunk.len;
    }
    std.debug.print("   Wrote: {d} bytes ({d} KB)\n", .{ written, written / 1024 });

    // Finish the upload
    std.debug.print("\n3. Finishing upload...\n", .{});
    try s3w.finish();

    const upload_end = try std.time.Instant.now();
    const total_elapsed = upload_end.since(upload_start);
    const total_ms = @as(f64, @floatFromInt(total_elapsed)) / 1_000_000.0;
    const total_throughput = (@as(f64, @floatFromInt(written)) / (1024.0 * 1024.0)) / (total_ms / 1000.0);

    std.debug.print("\n=== SUCCESS ===\n", .{});
    std.debug.print("File uploaded to s3://{s}/{s}\n", .{ bucket, key });
    std.debug.print("Total: {d:.1}ms for {d} KB = {d:.1} MB/s\n", .{ total_ms, written / 1024, total_throughput });
}
