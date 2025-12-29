const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

/// Probe: Connection Reuse Verification
///
/// Tests whether the connection pool actually reuses connections within
/// a single XevS3Source lifetime. This is the "DuckDB question" - are we
/// getting keep-alive benefits or reconnecting every request?
///
/// What we measure:
/// 1. Multiple sequential reads on same source - do pool logs show reuse?
/// 2. Time difference between first request (cold) vs subsequent (warm)
/// 3. Pool state after each operation

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch {
        std.debug.print("Error: S3_HOST required\n", .{});
        return;
    };
    defer allocator.free(host);

    const bucket = std.process.getEnvVarOwned(allocator, "S3_BUCKET") catch {
        std.debug.print("Error: S3_BUCKET required\n", .{});
        return;
    };
    defer allocator.free(bucket);

    const key = std.process.getEnvVarOwned(allocator, "S3_KEY") catch {
        std.debug.print("Error: S3_KEY required\n", .{});
        return;
    };
    defer allocator.free(key);

    const region = std.process.getEnvVarOwned(allocator, "S3_REGION") catch |err| blk: {
        if (err == error.EnvironmentVariableNotFound) break :blk try allocator.dupe(u8, "us-west-2");
        return err;
    };
    defer allocator.free(region);

    std.debug.print("\n=== CONNECTION REUSE PROBE ===\n", .{});
    std.debug.print("Host: {s}\n", .{host});
    std.debug.print("Bucket: {s}\n", .{bucket});
    std.debug.print("Key: {s}\n", .{key});
    std.debug.print("Region: {s}\n\n", .{region});

    // Create a single XevS3Source - this owns one pool
    var source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, region, true, 443);
    defer {
        source.deinit();
        allocator.destroy(source);
    }

    // Set credentials if available
    if (std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch null) |ak| {
        defer allocator.free(ak);
        if (std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch null) |sk| {
            defer allocator.free(sk);
            const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;
            defer if (st) |t| allocator.free(t);
            try source.setCredentials(ak, sk, st);
        }
    }

    var timings: [6]u64 = undefined;
    var buf: [4096]u8 = undefined;

    // Request 1: HEAD (fetchSize) - must establish new connection
    std.debug.print("--- Request 1: HEAD (cold start) ---\n", .{});
    var timer = try std.time.Timer.start();
    try source.fetchSize();
    timings[0] = timer.read();
    std.debug.print("  File size: {d} bytes\n", .{source.file_size});
    std.debug.print("  Time: {d:.2}ms\n", .{@as(f64, @floatFromInt(timings[0])) / 1e6});
    std.debug.print("  Pool idle: {d}\n\n", .{source.pool.idle_connections.items.len});

    // Request 2: GET range - should reuse connection from pool
    std.debug.print("--- Request 2: GET 0-4096 (should reuse) ---\n", .{});
    timer.reset();
    _ = try source.readAt(0, &buf);
    timings[1] = timer.read();
    std.debug.print("  Time: {d:.2}ms\n", .{@as(f64, @floatFromInt(timings[1])) / 1e6});
    std.debug.print("  Pool idle: {d}\n\n", .{source.pool.idle_connections.items.len});

    // Request 3: Another GET range - should still reuse
    std.debug.print("--- Request 3: GET 4096-8192 (should reuse) ---\n", .{});
    timer.reset();
    _ = try source.readAt(4096, &buf);
    timings[2] = timer.read();
    std.debug.print("  Time: {d:.2}ms\n", .{@as(f64, @floatFromInt(timings[2])) / 1e6});
    std.debug.print("  Pool idle: {d}\n\n", .{source.pool.idle_connections.items.len});

    // Request 4-6: More requests to confirm pattern
    std.debug.print("--- Requests 4-6: Rapid fire ---\n", .{});
    for (3..6) |i| {
        timer.reset();
        _ = try source.readAt(@as(u64, i) * 4096, &buf);
        timings[i] = timer.read();
        std.debug.print("  Request {d}: {d:.2}ms, pool idle: {d}\n", .{
            i + 1,
            @as(f64, @floatFromInt(timings[i])) / 1e6,
            source.pool.idle_connections.items.len,
        });
    }

    // Analysis
    std.debug.print("\n=== ANALYSIS ===\n", .{});
    const cold = @as(f64, @floatFromInt(timings[0])) / 1e6;
    var warm_sum: u64 = 0;
    for (timings[1..]) |t| warm_sum += t;
    const warm_avg = @as(f64, @floatFromInt(warm_sum)) / 5.0 / 1e6;

    std.debug.print("Cold (first request): {d:.2}ms\n", .{cold});
    std.debug.print("Warm (avg of 5):      {d:.2}ms\n", .{warm_avg});
    std.debug.print("Speedup ratio:        {d:.1}x\n", .{cold / warm_avg});

    if (cold / warm_avg > 2.0) {
        std.debug.print("\nVERDICT: Connection reuse IS working (warm requests much faster)\n", .{});
    } else {
        std.debug.print("\nVERDICT: Connection reuse may NOT be working (similar times)\n", .{});
    }

    std.debug.print("\n=== DONE ===\n", .{});
}
