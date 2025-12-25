const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");
const tls = zpq.io.tls;
const s3 = zpq.s3;

const log = std.log.scoped(.shootout);

const HOST = "localhost";
const PORT = 9999;
const BUCKET = "zpq-ci";
const KEY = "bench.parquet";

const CHUNK_SIZE = 1 * 1024 * 1024;   // 1MB per request for throughput measurement

pub const std_options: std.Options = .{
    .log_level = .info,
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

    std.debug.print("\n=== TLS THROUGHPUT SHOOTOUT ===\n", .{});
    std.debug.print("Target: {s}/{s} (HTTPS)\n", .{ BUCKET, KEY });
    std.debug.print("Chunk Size: {d} KB\n", .{ CHUNK_SIZE / 1024 });
    std.debug.print("================================\n\n", .{});

    // Strategy 1: The Buffered Baseline (Current implementation with internal copies)
    try runTest(allocator, &loop, &thread_pool, "Buffered Baseline", false, 4096, false, true);

    // Strategy 2: Direct Zero-Copy (Direct SSL_read into target buffer)
    try runTest(allocator, &loop, &thread_pool, "Direct Zero-Copy", true, 4096, false, true);

    // Strategy 3: The Monster Pipe (Large TCP Reads + Direct Decryption)
    try runTest(allocator, &loop, &thread_pool, "The Monster Pipe", true, 1024 * 1024, false, true);

    // Strategy 4: The Gatling Gun (Parallel 1MB Fetches + Monster Pipe)
    try runTest(allocator, &loop, &thread_pool, "The Gatling Gun", true, 1024 * 1024, true, true);

    // Strategy 5: The Naked Gun (No TLS, Parallel 1MB Fetches)
    // try runTest(allocator, &loop, &thread_pool, "The Naked Gun", false, 1024 * 1024, true, false);
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
) !void {
    std.debug.print("Running: {s} (TCP Read={d}KB, Parallel={}, TLS={})...\n", .{ name, tcp_read_size / 1024, use_parallel, use_tls });

    var source = try s3.XevS3Source.initWithLoop(
        allocator,
        loop,
        thread_pool,
        HOST,
        BUCKET,
        KEY,
        "us-east-1",
        use_tls,
        PORT,
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
    try source.setCredentials("rustfsadmin", "rustfsadmin", null);

    // Fetch size first to populate source
    try source.fetchSize();

    var total_time_ms: f64 = 0;
    const iterations = 10;

    if (use_parallel) {
        const start_inst = std.time.Instant.now() catch unreachable;
        
        var ranges = try allocator.alloc(zpq.io.interface.Range, iterations);
        defer allocator.free(ranges);
        var dests = try allocator.alloc([]u8, iterations);
        defer allocator.free(dests);
        
        for (0..iterations) |i| {
            ranges[i] = .{ .start = i * CHUNK_SIZE, .end = (i + 1) * CHUNK_SIZE };
            dests[i] = try allocator.alloc(u8, CHUNK_SIZE);
        }
        defer for (dests) |d| allocator.free(d);
        
        try source.source().readRanges(ranges, dests);
        
        const end_inst = std.time.Instant.now() catch unreachable;
        const total_ns = end_inst.since(start_inst);
        total_time_ms = @as(f64, @floatFromInt(total_ns)) / 1_000_000.0;
    } else {
        const buf = try allocator.alloc(u8, CHUNK_SIZE);
        defer allocator.free(buf);

        for (0..iterations) |i| {
            const start_inst = std.time.Instant.now() catch unreachable;
            _ = try source.readAt(i * CHUNK_SIZE, buf);
            const end_inst = std.time.Instant.now() catch unreachable;
            const total_ns = end_inst.since(start_inst);
            total_time_ms += @as(f64, @floatFromInt(total_ns)) / 1_000_000.0;
        }
    }

    const total_mb = @as(f64, @floatFromInt(CHUNK_SIZE * iterations)) / 1024.0 / 1024.0;
    const avg_time = if (use_parallel) total_time_ms else total_time_ms / @as(f64, @floatFromInt(iterations));
    const throughput = total_mb / (total_time_ms / 1000.0);

    std.debug.print("  Avg Time: {d:.2}ms\n", .{avg_time});
    std.debug.print("  Throughput: {d:.2} MB/s\n\n", .{throughput});
}
