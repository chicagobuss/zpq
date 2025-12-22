const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");

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
        self.source.deinit();
        self.pool.deinit();
        if (self.host_owned) |h| self.allocator.free(h);
        self.tp_resolver.deinit();
        self.thread_pool.deinit();
        self.thread_pool.shutdown();
    }
};

fn cleanupManualAsyncS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
    const s: *ManualContext = @ptrCast(@alignCast(ctx));
    s.deinit();
    allocator.destroy(s);
}

fn openAsyncBasic(allocator: std.mem.Allocator, path: []const u8) !zpq.file.ParquetFile {
    const s3_path = path[5..];
    const slash_idx = std.mem.indexOf(u8, s3_path, "/") orelse return error.InvalidS3Path;
    const bucket = s3_path[0..slash_idx];
    const key = s3_path[slash_idx + 1 ..];

    const ctx = try allocator.create(ManualContext);
    errdefer allocator.destroy(ctx);

    ctx.* = .{
        .allocator = allocator,
        .pool = s3.ConnectionPool.init(allocator),
        .thread_pool = xev.ThreadPool.init(.{}),
        .source = undefined,
        .tp_resolver = undefined,
    };

    ctx.tp_resolver = dns.ThreadPoolResolver.init(&ctx.thread_pool, allocator);
    const resolver = ctx.tp_resolver.resolver();

    const host = "s3.amazonaws.com";
    const host_copy = try allocator.dupe(u8, host);
    errdefer allocator.free(host_copy);
    ctx.host_owned = host_copy;

    // Credentials from env
    const access_key = std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (access_key) |s| allocator.free(s);
    const secret_key = std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch |err| if (err == error.EnvironmentVariableNotFound) null else return err;
    defer if (secret_key) |s| allocator.free(s);

    const config = if (access_key != null and secret_key != null) s3.S3Config{
        .credentials = .{
            .access_key = access_key.?,
            .secret_key = secret_key.?,
        },
    } else s3.S3Config{ .credentials = null };

    ctx.source = try s3.AsyncS3Source.init(allocator, &ctx.pool, resolver, host_copy, 443, bucket, key, true, null, config);

    return zpq.file.ParquetFile.initOwned(allocator, ctx.source.source(), ctx, cleanupManualAsyncS3);
}

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} s3://bucket/key [iterations] [--sync] [--dns=basic]\n", .{args[0]});
        return;
    }

    const s3_path = args[1];
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
    std.debug.print("Target: {s}\n", .{s3_path});
    std.debug.print("Mode: {s}\n", .{@tagName(mode)});
    std.debug.print("Iterations: {d}\n", .{iterations});

    var total_duration_ns: u64 = 0;
    var min_duration_ns: u64 = std.math.maxInt(u64);
    var max_duration_ns: u64 = 0;

    for (0..iterations) |i| {
        std.debug.print("\nRun {d}/{d}...\n", .{ i + 1, iterations });

        const start = std.time.Instant.now() catch unreachable;

        var file = switch (mode) {
            .Sync => try factory.openFile(allocator, s3_path, false),
            .AsyncFancy => try factory.openFile(allocator, s3_path, true),
            .AsyncBasic => try openAsyncBasic(allocator, s3_path),
        };
        defer file.deinit();

        try file.readFooter();
        const row_group = file.metadata.?.row_groups.items[0];
        const col_chunk = row_group.columns.items[0];

        var reader = try zpq.column.ColumnReader.init(file.source, col_chunk);

        var values_count: usize = 0;
        while (try reader.next(allocator)) |_| {
            values_count += 1;
        }

        const end = std.time.Instant.now() catch unreachable;
        const duration = end.since(start);

        std.debug.print("  Scanned {d} values in {d:.2}ms\n", .{ values_count, @as(f64, @floatFromInt(duration)) / 1_000_000.0 });

        total_duration_ns += duration;
        if (duration < min_duration_ns) min_duration_ns = duration;
        if (duration > max_duration_ns) max_duration_ns = duration;

        std.posix.nanosleep(0, 500 * std.time.ns_per_ms);
    }

    const avg_ns = total_duration_ns / iterations;
    std.debug.print("\n--- Results ({d} runs) ---\n", .{iterations});
    std.debug.print("Min: {d:.2}ms\n", .{@as(f64, @floatFromInt(min_duration_ns)) / 1_000_000.0});
    std.debug.print("Max: {d:.2}ms\n", .{@as(f64, @floatFromInt(max_duration_ns)) / 1_000_000.0});
    std.debug.print("Avg: {d:.2}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
}
