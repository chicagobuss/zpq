const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const tls = zpq.io.tls;

/// Probe: Connection Cost Analysis
///
/// Measures the actual cost breakdown of S3 requests to answer:
/// 1. How much does TLS handshake cost?
/// 2. How much does TTFB (time to first byte) cost?
/// 3. How much does transfer cost?
/// 4. At what request size does reuse stop mattering?
///
/// This informs whether connection pooling is worth the complexity.

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch {
        std.debug.print("Usage: S3_HOST=... S3_BUCKET=... S3_KEY=... probe_conn_cost\n", .{});
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

    std.debug.print("\n╔══════════════════════════════════════════╗\n", .{});
    std.debug.print("║     CONNECTION COST ANALYSIS PROBE       ║\n", .{});
    std.debug.print("╚══════════════════════════════════════════╝\n\n", .{});

    // Test 1: Measure cold vs warm for small request (4KB)
    std.debug.print("=== Test 1: Small Request (4KB) - Handshake Dominated ===\n", .{});
    try measureColdWarm(allocator, host, bucket, key, 4096);

    // Test 2: Measure cold vs warm for medium request (64KB)
    std.debug.print("\n=== Test 2: Medium Request (64KB) ===\n", .{});
    try measureColdWarm(allocator, host, bucket, key, 64 * 1024);

    // Test 3: Measure cold vs warm for large request (1MB)
    std.debug.print("\n=== Test 3: Large Request (1MB) - Transfer Dominated ===\n", .{});
    try measureColdWarm(allocator, host, bucket, key, 1024 * 1024);

    // Test 4: Parallel connection creation cost
    std.debug.print("\n=== Test 4: Parallel Connection Creation (simulates readRanges) ===\n", .{});
    try measureParallelCreation(allocator, host);
}

fn measureColdWarm(
    allocator: std.mem.Allocator,
    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    size: usize,
) !void {
    var source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, "auto", true, 443);
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

    const buf = try allocator.alloc(u8, size);
    defer allocator.free(buf);

    var cold_times: [3]u64 = undefined;
    var warm_times: [5]u64 = undefined;

    // Cold: Create fresh source each time (simulates benchmark iteration)
    for (0..3) |i| {
        var fresh_source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, "auto", true, 443);

        if (std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch null) |ak| {
            defer allocator.free(ak);
            if (std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch null) |sk| {
                defer allocator.free(sk);
                const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch null;
                defer if (st) |t| allocator.free(t);
                try fresh_source.setCredentials(ak, sk, st);
            }
        }

        var timer = try std.time.Timer.start();
        _ = try fresh_source.readAt(0, buf);
        cold_times[i] = timer.read();

        fresh_source.deinit();
        allocator.destroy(fresh_source);
    }

    // Warm: Reuse same source (pool should help)
    for (0..5) |i| {
        var timer = try std.time.Timer.start();
        _ = try source.readAt(0, buf);
        warm_times[i] = timer.read();
    }

    // Stats
    var cold_sum: u64 = 0;
    for (cold_times) |t| cold_sum += t;
    const cold_avg = @as(f64, @floatFromInt(cold_sum)) / 3.0 / 1e6;

    var warm_sum: u64 = 0;
    for (warm_times) |t| warm_sum += t;
    const warm_avg = @as(f64, @floatFromInt(warm_sum)) / 5.0 / 1e6;

    const handshake_cost = cold_avg - warm_avg;
    const handshake_pct = (handshake_cost / cold_avg) * 100.0;

    std.debug.print("  Cold (new conn):  {d:.1}ms avg\n", .{cold_avg});
    std.debug.print("  Warm (reused):    {d:.1}ms avg\n", .{warm_avg});
    std.debug.print("  Handshake cost:   {d:.1}ms ({d:.0}% of cold)\n", .{ handshake_cost, handshake_pct });
    std.debug.print("  Reuse speedup:    {d:.1}x\n", .{cold_avg / warm_avg});
}

fn measureParallelCreation(allocator: std.mem.Allocator, host: []const u8) !void {
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    const counts = [_]usize{ 1, 4, 16, 32, 64 };

    for (counts) |count| {
        var timer = try std.time.Timer.start();

        var conns = try allocator.alloc(*tls.Connection, count);
        defer allocator.free(conns);

        // Create connections (but don't actually connect - just measure alloc + init)
        for (0..count) |i| {
            conns[i] = try allocator.create(tls.Connection);
            conns[i].* = try tls.Connection.init(&loop, allocator, host);
        }

        const create_time = timer.read();

        // Cleanup
        for (0..count) |i| {
            conns[i].deinit();
            allocator.destroy(conns[i]);
        }

        std.debug.print("  {d:2} connections: {d:.2}ms create ({d:.2}ms each)\n", .{
            count,
            @as(f64, @floatFromInt(create_time)) / 1e6,
            @as(f64, @floatFromInt(create_time)) / @as(f64, @floatFromInt(count)) / 1e6,
        });
    }
}
