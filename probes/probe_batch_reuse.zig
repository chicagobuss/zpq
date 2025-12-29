const std = @import("std");
const zpq = @import("zpq");

/// Probe: Batch-to-Batch Connection Reuse
///
/// Tests if connections from one parallel batch are reused in the next batch.
/// This simulates reading multiple row groups from the same parquet file.
///
/// Hypothesis: If pool works across batches, batch 2+ should be faster than batch 1.

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch {
        std.debug.print("Usage: S3_HOST=... S3_BUCKET=... S3_KEY=... probe_batch_reuse\n", .{});
        return;
    };
    defer allocator.free(host);

    const bucket = std.process.getEnvVarOwned(allocator, "S3_BUCKET") catch return;
    defer allocator.free(bucket);

    const key = std.process.getEnvVarOwned(allocator, "S3_KEY") catch return;
    defer allocator.free(key);

    const region = std.process.getEnvVarOwned(allocator, "S3_REGION") catch |err| blk: {
        if (err == error.EnvironmentVariableNotFound) break :blk try allocator.dupe(u8, "us-west-2");
        return err;
    };
    defer allocator.free(region);

    std.debug.print("\n=== BATCH-TO-BATCH REUSE PROBE ===\n", .{});
    std.debug.print("Testing if parallel batch 2 reuses connections from batch 1\n\n", .{});

    // Single source for all batches
    var source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, region, true, 443);
    defer {
        source.deinit();
        allocator.destroy(source);
    }

    // Set credentials
    if (std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch null) |ak| {
        defer allocator.free(ak);
        if (std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch null) |sk| {
            defer allocator.free(sk);
            const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;
            defer if (st) |t| allocator.free(t);
            try source.setCredentials(ak, sk, st);
        }
    }

    try source.fetchSize();
    const file_size = source.file_size;
    std.debug.print("File size: {d} bytes\n\n", .{file_size});

    // Simulate 8 parallel range requests per batch (like reading column chunks)
    // Use SPREAD OUT ranges to prevent coalescing - simulate scattered column chunks
    const num_ranges = 8;
    const range_size = 64 * 1024; // 64KB per range
    const gap = file_size / num_ranges; // Spread across file

    var ranges: [num_ranges]zpq.io.interface.Range = undefined;
    var buffers: [num_ranges][]u8 = undefined;
    var buffer_storage: [num_ranges][range_size]u8 = undefined;

    std.debug.print("Using {d} ranges of {d}KB each, spread across file\n", .{ num_ranges, range_size / 1024 });

    for (0..num_ranges) |i| {
        const start = i * gap;
        const end = @min(start + range_size, file_size);
        ranges[i] = .{ .start = start, .end = end };
        buffers[i] = buffer_storage[i][0..(end - start)];
    }

    var batch_times: [5]u64 = undefined;

    for (0..5) |batch| {
        const pool_before = source.pool.idle_connections.items.len;

        var timer = try std.time.Timer.start();
        try source.source().readRanges(&ranges, &buffers);
        batch_times[batch] = timer.read();

        const pool_after = source.pool.idle_connections.items.len;

        std.debug.print("Batch {d}: {d:.1}ms (pool: {d} -> {d})\n", .{
            batch + 1,
            @as(f64, @floatFromInt(batch_times[batch])) / 1e6,
            pool_before,
            pool_after,
        });
    }

    std.debug.print("\n=== ANALYSIS ===\n", .{});
    const batch1 = @as(f64, @floatFromInt(batch_times[0])) / 1e6;
    var warm_sum: u64 = 0;
    for (batch_times[1..]) |t| warm_sum += t;
    const warm_avg = @as(f64, @floatFromInt(warm_sum)) / 4.0 / 1e6;

    std.debug.print("Batch 1 (cold): {d:.1}ms\n", .{batch1});
    std.debug.print("Batch 2-5 avg:  {d:.1}ms\n", .{warm_avg});
    std.debug.print("Speedup:        {d:.2}x\n", .{batch1 / warm_avg});

    if (batch1 / warm_avg > 1.5) {
        std.debug.print("\nVERDICT: Batch reuse IS working\n", .{});
    } else {
        std.debug.print("\nVERDICT: Batch reuse NOT working (connections not surviving)\n", .{});
    }
}
