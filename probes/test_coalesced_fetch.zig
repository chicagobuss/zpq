///! Probe: Test coalesced vs sequential range fetches over S3
///!
///! Purpose: Measure if batching multiple small range requests into one
///! readRanges call reduces total latency vs sequential fetches.
///!
///! Date: 2024-12-29
const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <s3://bucket/path.parquet>\n", .{args[0]});
        return;
    }

    const path = args[1];
    std.debug.print("Testing coalesced fetch on: {s}\n\n", .{path});

    // Set up async I/O (same as main.zig)
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{});
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }

    var tp_resolver = zpq.s3.dns.ThreadPoolResolverGen(xev).init(&thread_pool, allocator);
    var sf_resolver = zpq.s3.dns.SingleFlightResolverGen(xev).init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    var spec_resolver = zpq.s3.dns.SpeculativeResolverGen(xev).init(allocator, sf_resolver.resolver());
    const resolver = spec_resolver.resolver();

    // Open file and read metadata
    var pf = try zpq.s3.factory.openFileWithOptions(allocator, path, .{
        .loop = &loop,
        .thread_pool = &thread_pool,
        .resolver = resolver,
        .verify_tls = false,
    });
    defer pf.deinit();

    try pf.readFooter();
    const meta = pf.metadata orelse return error.NoMetadata;

    std.debug.print("File has {d} row groups\n", .{meta.row_groups.items.len});

    // Find first BYTE_ARRAY column (filter column candidate)
    var filter_col_idx: ?usize = null;
    for (meta.row_groups.items[0].columns.items, 0..) |col, i| {
        if (col.meta_data) |md| {
            if (md.type == .BYTE_ARRAY) {
                filter_col_idx = i;
                const path_items = md.path_in_schema.items;
                std.debug.print("Using column {d} ({s}) as filter column\n\n", .{ i, path_items[path_items.len - 1] });
                break;
            }
        }
    }

    const col_idx = filter_col_idx orelse return error.NoByteArrayColumn;

    // Collect ranges for this column across all row groups
    const Range = zpq.io.interface.Range;
    var ranges = std.ArrayListUnmanaged(Range){};
    defer ranges.deinit(allocator);

    var total_bytes: u64 = 0;
    for (meta.row_groups.items) |rg| {
        const chunk = rg.columns.items[col_idx];
        const md = chunk.meta_data orelse continue;

        var start: u64 = @intCast(md.data_page_offset);
        if (md.dictionary_page_offset) |dpo| {
            if (dpo < start) start = @intCast(dpo);
        }
        const len: u64 = @intCast(md.total_compressed_size);

        try ranges.append(allocator, .{ .start = start, .end = start + len });
        total_bytes += len;
    }

    std.debug.print("Ranges to fetch: {d}, total bytes: {d}\n", .{ ranges.items.len, total_bytes });

    // Print ranges
    for (ranges.items, 0..) |r, i| {
        std.debug.print("  RG[{d}]: {d}-{d} ({d} bytes)\n", .{ i, r.start, r.end, r.end - r.start });
    }
    std.debug.print("\n", .{});

    // Allocate buffers
    var buffers = try allocator.alloc([]u8, ranges.items.len);
    defer {
        for (buffers) |buf| allocator.free(buf);
        allocator.free(buffers);
    }

    for (ranges.items, 0..) |r, i| {
        buffers[i] = try allocator.alloc(u8, r.end - r.start);
    }

    // Test 1: Sequential fetches
    std.debug.print("=== Test 1: Sequential fetches ===\n", .{});
    var seq_timer = try std.time.Timer.start();

    for (ranges.items, 0..) |r, i| {
        const single_range = &[_]Range{r};
        const single_buf = &[_][]u8{buffers[i]};
        try pf.source.readRanges(single_range, single_buf);
    }

    const seq_ns = seq_timer.read();
    std.debug.print("Sequential: {d:.1}ms\n\n", .{@as(f64, @floatFromInt(seq_ns)) / 1_000_000.0});

    // Clear buffers for fair test
    for (buffers) |buf| @memset(buf, 0);

    // Test 2: Batched fetch (single readRanges call)
    std.debug.print("=== Test 2: Batched fetch (single readRanges) ===\n", .{});
    var batch_timer = try std.time.Timer.start();

    try pf.source.readRanges(ranges.items, buffers);

    const batch_ns = batch_timer.read();
    std.debug.print("Batched: {d:.1}ms\n\n", .{@as(f64, @floatFromInt(batch_ns)) / 1_000_000.0});

    // Results
    const speedup = @as(f64, @floatFromInt(seq_ns)) / @as(f64, @floatFromInt(batch_ns));
    std.debug.print("=== Results ===\n", .{});
    std.debug.print("Sequential: {d:.1}ms\n", .{@as(f64, @floatFromInt(seq_ns)) / 1_000_000.0});
    std.debug.print("Batched:    {d:.1}ms\n", .{@as(f64, @floatFromInt(batch_ns)) / 1_000_000.0});
    std.debug.print("Speedup:    {d:.2}x\n", .{speedup});
}
