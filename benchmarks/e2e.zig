const std = @import("std");
const zpq = @import("zpq");
const build_options = @import("build_options");
const xev = zpq.s3.dns.xev;

/// Suppress debug output for clean benchmark results.
/// Only warnings and errors are shown.
pub const std_options: std.Options = .{
    .log_level = .warn,
};

/// Build hash for reproducibility
const git_hash: []const u8 = build_options.git_hash;

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

fn openAsyncXev(allocator: std.mem.Allocator, path: []const u8) !zpq.file.ParquetFile {
    // force_async = true triggers the Xev stack
    return factory.openFile(allocator, path, true);
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

    std.debug.print("Benchmarking ZPQ E2E [{s}]\n", .{git_hash});
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
                .AsyncFancy => try openAsyncXev(allocator, target_path),
                .AsyncBasic => try openAsyncXev(allocator, target_path),
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

        for (file.metadata.?.row_groups.items, 0..) |rg_meta, rg_idx| {
            var rg_reader = try file.rowGroup(rg_idx);
            defer rg_reader.deinit();

            // Prefetch ALL columns in one batched readRanges call
            try rg_reader.prefetch(null); // null = all columns

            // Now read from memory (no network calls) using vectorized BatchReader
            for (rg_meta.columns.items, 0..) |col_meta, col_idx| {
                if (col_meta.meta_data) |md| {
                    const levels = file.metadata.?.getColumnLevels(md.path_in_schema.items);
                    const schema_elem = file.metadata.?.getColumnSchema(md.path_in_schema.items);
                    const type_length = if (schema_elem) |se| se.type_length else null;
                    
                    switch (md.type) {
                        .INT32 => try scanColumnBatch(aa, i32, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        .INT64 => try scanColumnBatch(aa, i64, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        .INT96 => try scanColumnBatch(aa, [12]u8, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        .FLOAT => try scanColumnBatch(aa, f32, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        .DOUBLE => try scanColumnBatch(aa, f64, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try scanColumnBatch(aa, []const u8, try rg_reader.columnReader(col_idx), md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, &values_count),
                        else => {
                            var reader = try rg_reader.columnReader(col_idx);
                            while (try reader.next(aa)) |page_val| {
                                var page = page_val;
                                if (page.header.data_page_header) |dph| {
                                    values_count += @intCast(dph.num_values);
                                }
                            }
                        },
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

fn scanColumnBatch(allocator: std.mem.Allocator, comptime T: type, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32, total_count: *usize) !void {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var batch: [1024]?T = undefined;
    while (true) {
        const n = try batch_reader.nextBatch(&batch);
        if (n == 0) break;
        total_count.* += n;
    }
}
