const std = @import("std");
const zpq = @import("zpq");
const xev = zpq.s3.dns.xev;

/// Suppress debug output for clean benchmark results.
/// Only warnings and errors are shown.
pub const std_options: std.Options = .{
    .log_level = .warn,
};

const factory = zpq.s3.factory;
const s3 = zpq.s3;
const dns = s3.dns;

// Custom cleanup for manual AsyncS3Source with "Basic" DNS
const ManualContext = struct {
    pool: s3.ConnectionPool,
    source: s3.AsyncS3Source,
    allocator: std.mem.Allocator,
    host_owned: ?[]const u8 = null,
    thread_pool: xev.ThreadPool,
    tp_resolver: dns.ThreadPoolResolver,

    pub fn deinit(self: *ManualContext) void {
        // 1. Close all connections in the pool
        // We'll just leak them for now to avoid the libxev/io_uring use-after-free
        // during rapid benchmark shutdown. In a real app, the pool would live
        // for the duration of the process.

        // self.source.deinit(); // This closes the loop

        if (self.host_owned) |h| self.allocator.free(h);
        self.tp_resolver.deinit();
        self.thread_pool.shutdown();
        self.thread_pool.deinit();

        // We skip pool.deinit() and source.deinit() to avoid the crash
        // The GPA will report leaks, but we can ignore those for the E2E proof.
    }
};

fn cleanupManualAsyncS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    _ = allocator;
    const s: *ManualContext = @ptrCast(@alignCast(ctx));
    s.deinit();
    s.allocator.destroy(s);
}

fn openAsyncBasic(allocator: std.mem.Allocator, path: []const u8) !zpq.file.ParquetFile {
    if (!std.mem.startsWith(u8, path, "s3://")) return error.NotS3Path;

    const s3_path = path[5..];
    const slash_idx = std.mem.indexOf(u8, s3_path, "/") orelse return error.InvalidS3Path;
    const bucket = s3_path[0..slash_idx];
    const key = s3_path[slash_idx + 1 ..];

    const ctx = try allocator.create(ManualContext);
    errdefer allocator.destroy(ctx);

    ctx.allocator = allocator;
    ctx.pool = s3.ConnectionPool.init(allocator);
    ctx.thread_pool = xev.ThreadPool.init(.{});
    ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);

    const resolver = ctx.tp_resolver.resolver();

    const region_env = std.process.getEnvVarOwned(allocator, "AWS_REGION") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (region_env) |s| allocator.free(s);

    var host: []const u8 = "s3.amazonaws.com";
    var host_allocated = false;
    if (region_env) |region| {
        if (!std.mem.eql(u8, region, "us-east-1")) {
            host = try std.fmt.allocPrint(allocator, "s3.{s}.amazonaws.com", .{region});
            host_allocated = true;
        }
    }
    defer if (host_allocated) allocator.free(host);

    ctx.host_owned = try allocator.dupe(u8, host);
    errdefer allocator.free(ctx.host_owned.?);

    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (access_key) |s| allocator.free(s);
    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (secret_key) |s| allocator.free(s);

    const config = s3.S3Config{
        .credentials = if (access_key != null and secret_key != null) .{
            .access_key = try allocator.dupe(u8, access_key.?),
            .secret_key = try allocator.dupe(u8, secret_key.?),
        } else null,
        .region = try allocator.dupe(u8, region_env orelse "us-east-1"),
    };

    ctx.source = try s3.AsyncS3Source.init(allocator, &ctx.pool, resolver, ctx.host_owned.?, 443, bucket, key, true, null, config);

    return zpq.file.ParquetFile.initOwned(allocator, ctx.source.source(), ctx, cleanupManualAsyncS3);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <path> [iterations] [--sync] [--dns=basic] [--async]\n", .{args[0]});
        return;
    }

    const target_path = args[1];
    var iterations: usize = 1;
    var mode: enum { Sync, AsyncFancy, AsyncBasic } = .AsyncFancy;

    for (args[2..]) |arg| {
        if (std.mem.eql(u8, arg, "--sync")) {
            mode = .Sync;
        } else if (std.mem.eql(u8, arg, "--dns=basic")) {
            mode = .AsyncBasic;
        } else if (std.mem.eql(u8, arg, "--async")) {
            mode = .AsyncFancy;
        } else {
            iterations = std.fmt.parseInt(usize, arg, 10) catch 1;
        }
    }

    std.debug.print("Benchmarking ZPQ E2E\n", .{});
    std.debug.print("Target: {s}\n", .{target_path});
    std.debug.print("Mode: {s}\n", .{@tagName(mode)});
    std.debug.print("Iterations: {d}\n", .{iterations});

    var total_duration_ns: u64 = 0;
    var min_duration_ns: u64 = std.math.maxInt(u64);
    var max_duration_ns: u64 = 0;

    for (0..iterations) |i| {
        std.debug.print("\nRun {d}/{d}...\n", .{ i + 1, iterations });

        const start = std.time.Instant.now() catch unreachable;

        var file = if (std.mem.startsWith(u8, target_path, "s3://"))
            switch (mode) {
                .Sync => try factory.openFile(allocator, target_path, false),
                .AsyncFancy => try factory.openFile(allocator, target_path, true),
                .AsyncBasic => try openAsyncBasic(allocator, target_path),
            }
        else
            try zpq.file.ParquetFile.open(allocator, target_path);

        defer file.deinit();

        try file.readFooter();

        var values_count: usize = 0;
        // Use a per-iteration arena for the actual data pages
        var iter_arena = std.heap.ArenaAllocator.init(allocator);
        defer iter_arena.deinit();
        const aa = iter_arena.allocator();

        for (file.metadata.?.row_groups.items, 0..) |_, rg_idx| {
            var rg_reader = try file.rowGroup(rg_idx);
            defer rg_reader.deinit();

            // Prefetch ALL columns in one batched readRanges call
            try rg_reader.prefetch(null); // null = all columns

            // Now read from memory (no network calls)
            for (0..rg_reader.meta.columns.items.len) |col_idx| {
                var reader = try rg_reader.columnReader(col_idx);

                while (try reader.next(aa)) |page_val| {
                    var page = page_val;
                    if (page.header.data_page_header) |dph| {
                        values_count += @intCast(dph.num_values);
                    }
                }
            }
        }

        const end = std.time.Instant.now() catch unreachable;
        const duration = end.since(start);

        std.debug.print("  Scanned {d} values in {d:.2}ms\n", .{ values_count, @as(f64, @floatFromInt(duration)) / 1_000_000.0 });

        total_duration_ns += duration;
        if (duration < min_duration_ns) min_duration_ns = duration;
        if (duration > max_duration_ns) max_duration_ns = duration;

        if (iterations > 1) std.posix.nanosleep(0, 100 * std.time.ns_per_ms);
    }

    const avg_ns = total_duration_ns / iterations;
    std.debug.print("\n--- Results ({d} runs) ---\n", .{iterations});
    std.debug.print("Min: {d:.2}ms\n", .{@as(f64, @floatFromInt(min_duration_ns)) / 1_000_000.0});
    std.debug.print("Max: {d:.2}ms\n", .{@as(f64, @floatFromInt(max_duration_ns)) / 1_000_000.0});
    std.debug.print("Avg: {d:.2}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
}
