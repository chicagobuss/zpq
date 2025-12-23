//! Probe: Test multiple concurrent connections to S3
//! This isolates whether the hang is in connection setup or request dispatch.

const std = @import("std");
const zpq = @import("zpq");
const s3 = zpq.s3;
const dns = s3.dns;
const xev = s3.dns.xev;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("[PROBE] Starting multi-connection probe...\n", .{});

    // Get credentials from env
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch {
        std.debug.print("[PROBE] Missing AWS_ACCESS_KEY_ID\n", .{});
        return;
    };
    defer allocator.free(access_key);

    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch {
        std.debug.print("[PROBE] Missing AWS_SECRET_ACCESS_KEY\n", .{});
        return;
    };
    defer allocator.free(secret_key);

    const region = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch |_| try allocator.dupe(u8, "us-west-2");
    defer allocator.free(region);

    std.debug.print("[PROBE] Credentials loaded, region={s}\n", .{region});

    // Setup DNS
    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    var tp_resolver = dns.ThreadPoolResolver.init(&thread_pool, allocator);
    defer tp_resolver.deinit();

    std.debug.print("[PROBE] DNS resolver initialized\n", .{});

    // Create pool and event loop
    var pool = s3.ConnectionPool.init(allocator);
    defer pool.deinit();

    const host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
    defer allocator.free(host);

    std.debug.print("[PROBE] Host: {s}\n", .{host});

    // Initialize AsyncS3Source
    const config = s3.S3Config{
        .credentials = .{
            .access_key = access_key,
            .secret_key = secret_key,
            .session_token = null,
        },
        .region = region,
        .endpoint = null,
    };

    std.debug.print("[PROBE] Creating AsyncS3Source...\n", .{});

    var source = s3.AsyncS3Source.init(
        allocator,
        &pool,
        tp_resolver.resolver(),
        host,
        443,
        "skyway-diat-staging-data",
        "test_data/valid/sizes/small/10k_rows.parquet",
        true,
        null,
        config,
    ) catch |err| {
        std.debug.print("[PROBE] AsyncS3Source.init failed: {}\n", .{err});
        return;
    };
    defer source.deinit();

    std.debug.print("[PROBE] AsyncS3Source initialized, file_size={d}\n", .{source.file_size});

    // Now try to read multiple ranges (simulating what scan does)
    const num_ranges = 3;
    var ranges: [num_ranges]zpq.io.Range = undefined;
    var buffers: [num_ranges][]u8 = undefined;

    for (0..num_ranges) |i| {
        const start = i * 1000;
        const end = start + 1000;
        ranges[i] = .{ .start = start, .end = end };
        buffers[i] = try allocator.alloc(u8, 1000);
    }
    defer for (&buffers) |buf| allocator.free(buf);

    std.debug.print("[PROBE] Reading {d} ranges...\n", .{num_ranges});

    source.readRanges(&ranges, &buffers) catch |err| {
        std.debug.print("[PROBE] readRanges failed: {}\n", .{err});
        return;
    };

    std.debug.print("[PROBE] SUCCESS! Read {d} ranges\n", .{num_ranges});
}
