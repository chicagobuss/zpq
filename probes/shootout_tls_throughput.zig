const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const tls = zpq.io.tls;
const s3 = zpq.s3;

const log = std.log.scoped(.shootout);

const HOST_DEFAULT = "s3.us-west-2.amazonaws.com";
const PORT_DEFAULT = 443;
const BUCKET_DEFAULT = "skyway-diat-staging-data";
const KEY_DEFAULT = "test_data/valid/sizes/small/10k_rows.parquet";

const CHUNK_SIZE = 16 * 1024;   // 16KB per request (file is ~166KB)

pub const std_options: std.Options = .{
    .log_level = .debug,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, HOST_DEFAULT) else return err;
    defer allocator.free(host);
    const bucket = std.process.getEnvVarOwned(allocator, "S3_BUCKET") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, BUCKET_DEFAULT) else return err;
    defer allocator.free(bucket);
    const key = std.process.getEnvVarOwned(allocator, "S3_KEY") catch |err| if (err == error.EnvironmentVariableNotFound) try allocator.dupe(u8, KEY_DEFAULT) else return err;
    defer allocator.free(key);

    const port_str = std.process.getEnvVarOwned(allocator, "S3_PORT") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (port_str) |s| allocator.free(s);
    const port: u16 = if (port_str) |s| try std.fmt.parseInt(u16, s, 10) else PORT_DEFAULT;

    std.debug.print("\n=== TLS THROUGHPUT SHOOTOUT ===\n", .{});
    std.debug.print("Target: {s}/{s} (HTTPS port {d})\n", .{ bucket, key, port });
    std.debug.print("Host: {s}\n", .{host});
    std.debug.print("Chunk Size: {d} KB\n", .{ CHUNK_SIZE / 1024 });
    std.debug.print("================================\n\n", .{});

    // Strategy 1: The Buffered Baseline (Current implementation with internal copies)
    try runTest(allocator, &loop, &thread_pool, "Buffered Baseline", false, 4096, false, true, host, bucket, key, port);

    // Strategy 2: Direct Zero-Copy (Direct SSL_read into target buffer)
    try runTest(allocator, &loop, &thread_pool, "Direct Zero-Copy", true, 4096, false, true, host, bucket, key, port);

    // Strategy 3: The Monster Pipe (Large TCP Reads + Direct Decryption)
    try runTest(allocator, &loop, &thread_pool, "The Monster Pipe", true, 1024 * 1024, false, true, host, bucket, key, port);

    // Strategy 4: The Gatling Gun (Parallel 1MB Fetches + Monster Pipe)
    try runTest(allocator, &loop, &thread_pool, "The Gatling Gun", true, 1024 * 1024, true, true, host, bucket, key, port);

    // Strategy 5: The Naked Gun (No TLS, Parallel 1MB Fetches)
    // try runTest(allocator, &loop, &thread_pool, "The Naked Gun", false, 1024 * 1024, true, false, host, bucket, key, port);
}

fn runTest(
    allocator: std.mem.Allocator,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    name: []const u8,
    use_direct: bool,
    tcp_read_size: usize,
    use_parallel: bool,
    use_tls: bool,
    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    port: u16,
) !void {
    std.debug.print("Running: {s} (TCP Read={d}KB, Parallel={}, TLS={})...\n", .{ name, tcp_read_size / 1024, use_parallel, use_tls });

    var source = try s3.XevS3Source.initWithLoop(
        allocator,
        loop,
        thread_pool,
        host,
        bucket,
        key,
        "us-west-2",
        use_tls,
        port,
        tcp_read_size,
        use_direct,
    );
    defer {
        source.deinit();
        allocator.destroy(source);
    }

    // CONFIGURE STRATEGY
    source.use_direct = use_direct;
    source.tcp_read_buf_size = tcp_read_size;

    // Configure Credentials
    // Configure Credentials
    const env_ak = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    if (env_ak) |ak| {
        defer allocator.free(ak);
        const env_sk = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
        if (env_sk) |sk| {
            defer allocator.free(sk);
            const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
            defer if (st) |token| allocator.free(token);
            try source.setCredentials(ak, sk, st);
        }
    } else {
         // Fallback for local testing if env vars missing
        try source.setCredentials("rustfsadmin", "rustfsadmin", null);
    }

    // Fetch size first to populate source
    try source.fetchSize();
    const file_size = source.file_size;
    if (file_size == 0) return error.NoFileSize;

    var total_time_ms: f64 = 0;
    const iterations: usize = 5;

    // Read the entire file each iteration (simulating what PyArrow/DuckDB do)
    const buf = try allocator.alloc(u8, file_size);
    defer allocator.free(buf);

    if (use_parallel) {
        // Split file into chunks for parallel fetch
        const num_chunks = (file_size + CHUNK_SIZE - 1) / CHUNK_SIZE;
        var ranges = try allocator.alloc(zpq.io.interface.Range, num_chunks);
        defer allocator.free(ranges);
        var dests = try allocator.alloc([]u8, num_chunks);
        defer allocator.free(dests);

        for (0..num_chunks) |i| {
            const start_off = i * CHUNK_SIZE;
            const end_off = @min((i + 1) * CHUNK_SIZE, file_size);
            ranges[i] = .{ .start = start_off, .end = end_off };
            dests[i] = buf[start_off..end_off];
        }

        for (0..iterations) |iter| {
            // Clear buffer to verify data is actually fetched
            @memset(buf, 0);

            const start_inst = std.time.Instant.now() catch unreachable;
            try source.source().readRanges(ranges, dests);
            const end_inst = std.time.Instant.now() catch unreachable;
            const elapsed_ns = end_inst.since(start_inst);
            const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
            total_time_ms += elapsed_ms;

            // Verify data was actually read (check first bytes aren't zero)
            var non_zero: usize = 0;
            for (buf[0..@min(100, buf.len)]) |b| {
                if (b != 0) non_zero += 1;
            }
            std.debug.print("  iter {d}: {d:.1}ms (non-zero: {d}/100)\n", .{ iter + 1, elapsed_ms, non_zero });
        }
    } else {
        for (0..iterations) |iter| {
            const start_inst = std.time.Instant.now() catch unreachable;
            _ = try source.readAt(0, buf);
            const end_inst = std.time.Instant.now() catch unreachable;
            const elapsed_ns = end_inst.since(start_inst);
            const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;
            total_time_ms += elapsed_ms;
            std.debug.print("  iter {d}: {d:.1}ms\n", .{ iter + 1, elapsed_ms });
        }
    }

    const total_mb = @as(f64, @floatFromInt(file_size * iterations)) / 1024.0 / 1024.0;
    const avg_time = total_time_ms / @as(f64, @floatFromInt(iterations));
    const throughput = total_mb / (total_time_ms / 1000.0);

    std.debug.print("  Avg Time: {d:.2}ms\n", .{avg_time});
    std.debug.print("  Throughput: {d:.2} MB/s\n\n", .{throughput});
}
