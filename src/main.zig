const std = @import("std");
const zpq = @import("zpq");
const xev = @import("xev");
const builtin = @import("builtin");

/// Control log level based on build mode.
/// Debug builds show all logs, release builds show only warnings and errors.
pub const std_options: std.Options = .{
    .log_level = if (builtin.mode == .Debug) .debug else .warn,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        printUsage(args[0]);
        return;
    }

    const command = args[1];

    const is_async = for (args) |arg| {
        if (std.mem.eql(u8, arg, "--async")) break true;
    } else false;

    const verify_tls = for (args) |arg| {
        if (std.mem.eql(u8, arg, "--tls-verify")) break true;
    } else false;

    var filter: ?[]const u8 = null;
    var count_only = false;
    var unified_filter = false;
    var trace_output: ?[]const u8 = null;
    for (args, 0..) |arg, i| {
        if (std.mem.eql(u8, arg, "--filter")) {
            if (i + 1 < args.len) {
                filter = args[i + 1];
            }
        }
        if (std.mem.eql(u8, arg, "--count")) {
            count_only = true;
        }
        if (std.mem.eql(u8, arg, "--unified-filter")) {
            unified_filter = true;
        }
        if (std.mem.eql(u8, arg, "--trace")) {
            // Optional: --trace output.json or just --trace for stdout
            if (i + 1 < args.len and args[i + 1][0] != '-') {
                trace_output = args[i + 1];
            } else {
                trace_output = "-"; // stdout marker
            }
        }
    }

    // Initialize core xev infrastructure for all commands
    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    var thread_pool = xev.ThreadPool.init(.{ .max_threads = 4 });
    defer {
        thread_pool.shutdown();
        thread_pool.deinit();
    }
    // Note: Global connection pool uses reference counting (Polars-style).
    // No explicit shutdown needed - pool auto-cleans when last ParquetFile closes.

    var tp_resolver = zpq.s3.dns.ThreadPoolResolverGen(xev).init(&thread_pool, allocator);
    var sf_resolver = zpq.s3.dns.SingleFlightResolverGen(xev).init(allocator, tp_resolver.resolver());
    defer sf_resolver.deinit();
    var spec_resolver = zpq.s3.dns.SpeculativeResolverGen(xev).init(allocator, sf_resolver.resolver());

    const resolver = spec_resolver.resolver();

    if (std.mem.eql(u8, command, "schema")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} schema <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdSchema(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "meta")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} meta <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdMeta(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "cat")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} cat <parquet_file> [limit]\n", .{args[0]});
            return;
        }
        const limit = if (args.len > 3) try std.fmt.parseInt(usize, args[3], 10) else 10;
        try cmdCat(allocator, args[2], limit, is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "scan")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} scan <parquet_file> [--filter col=val] [--count] [--unified-filter] [--trace [file]]\n", .{args[0]});
            return;
        }
        try cmdScan(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls, filter, count_only, unified_filter, trace_output);
    } else if (std.mem.eql(u8, command, "pages")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} pages <parquet_file>\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "inspect")) {
        // Legacy support
        if (args.len < 3) {
            std.debug.print("Usage: {s} inspect <parquet_file> [--async]\n", .{args[0]});
            return;
        }
        try cmdPages(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "debug-s3")) {
        if (args.len < 3) {
            std.debug.print("Usage: {s} debug-s3 <parquet_file> [--async]\n", .{args[0]});
            return;
        }
        try cmdDebugS3(allocator, args[2], is_async, &loop, &thread_pool, resolver, verify_tls);
    } else if (std.mem.eql(u8, command, "write-test")) {
        const output_path = if (args.len > 2) args[2] else "/tmp/zpq_test.parquet";
        try cmdWriteTest(allocator, output_path);
    } else {
        printUsage(args[0]);
    }
}

const Resolver = zpq.s3.dns.ResolverGen(xev);

fn cmdDebugS3(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    std.debug.print("Opening file: {s}\n", .{path});
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();

    // We assume it's S3Source.
    var buf: [1024]u8 = undefined;

    std.debug.print("Read 1 (offset 0, 1024 bytes)...\n", .{});
    var timer = try std.time.Timer.start();
    _ = try pf.source.readAt(0, &buf);
    var elapsed = timer.read();
    std.debug.print("Read 1 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});

    std.debug.print("Read 2 (offset 1024, 1024 bytes)...\n", .{});
    timer.reset();
    _ = try pf.source.readAt(1024, &buf);
    elapsed = timer.read();
    std.debug.print("Read 2 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});

    std.debug.print("Read 3 (offset 2048, 1024 bytes)...\n", .{});
    timer.reset();
    _ = try pf.source.readAt(2048, &buf);
    elapsed = timer.read();
    std.debug.print("Read 3 took: {d:.4}s\n", .{@as(f64, @floatFromInt(elapsed)) / 1_000_000_000.0});
}

fn printUsage(exe_name: []const u8) void {
    std.debug.print(
        \\Usage: {s} <command> [options]
        \\
        \\Commands:
        \\  schema <file>       Show the schema tree
        \\  meta   <file>       Show file and row group metadata
        \\  cat    <file>       Dump row data (JSON-like)
        \\  scan   <file>       Benchmark scan speed (no output)
        \\  pages  <file>       Inspect page headers and encodings (deep dive)
        \\  write-test [file]   Write a test parquet file (default: /tmp/zpq_test.parquet)
        \\
        \\Options:
        \\  --tls-verify        Enable TLS certificate verification for S3/HTTPS
        \\  --async             Force async I/O path
        \\  --filter col=val    Filter rows where column equals value
        \\  --count             Only count matching rows (skip Phase 2)
        \\  --unified-filter    Use unified byte-level filter matching (experimental)
        \\  --trace [file]      Output performance trace (JSON to file or stdout if no file)
        \\
        \\Environment Variables:
        \\  S3_ENDPOINT         Custom S3 endpoint (e.g. http://localhost:9000 for MinIO)
        \\
    , .{exe_name});
}

fn openFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    force_async: bool,
    loop: *xev.Loop,
    thread_pool: *xev.ThreadPool,
    resolver: Resolver,
    verify_tls: bool,
) !zpq.file.ParquetFile {
    return zpq.s3.factory.openFileWithOptions(
        allocator,
        path,
        .{
            .force_async = force_async,
            .loop = loop,
            .thread_pool = thread_pool,
            .resolver = resolver,
            .verify_tls = verify_tls,
        },
    );
}

fn cmdScan(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool, filter: ?[]const u8, count_only: bool, unified_filter: bool, trace_output: ?[]const u8) !void {
    var timer = try std.time.Timer.start();

    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    var total_values: u64 = 0;
    var row_groups_skipped: usize = 0;
    var rows_selected: u64 = 0;

    // Parse filter (simple col=val)
    var filter_col_name: ?[]const u8 = null;
    var filter_val: ?[]const u8 = null;
    if (filter) |f| {
        if (std.mem.indexOfScalar(u8, f, '=')) |idx| {
            filter_col_name = f[0..idx];
            filter_val = f[idx + 1 ..];
        }
    }

    // Initialize tracer if requested
    var tracer: ?zpq.trace.Tracer = if (trace_output != null) zpq.trace.Tracer.init(.{
        .timestamp_ms = zpq.trace.nowMs(),
        .file_path = path,
        .filter_column = filter_col_name,
        .filter_value = filter_val,
        .total_rows = if (pf.metadata) |m| @intCast(m.num_rows) else 0,
        .num_row_groups = if (pf.metadata) |m| @intCast(m.row_groups.items.len) else 0,
        .num_columns = if (pf.metadata) |m| @intCast(m.schema.items.len) else 0,
    }) else null;

    // Log unified filter mode
    if (unified_filter) {
        std.debug.print("Using unified filter path (EncodedFilter.matchesBytes)\n", .{});
    }

    if (pf.metadata) |meta| {
        // ========== BATCHED FILTER COLUMN FETCH ==========
        // Optimization: Fetch filter columns for ALL row groups in one parallel request.
        // This gives ~2.24x speedup over sequential per-RG fetches (verified in test_coalesced_fetch.zig).
        //
        // Steps:
        // 1. Identify which row groups to scan (skip via stats)
        // 2. Collect filter column ranges for all non-skipped row groups
        // 3. Fetch all filter columns in one batched readRanges call
        // 4. Process each row group using pre-fetched data

        var filter_col_idx: ?usize = null;
        var filter_col_type: ?zpq.core.schema.Type = null;
        var rg_skip_mask = try allocator.alloc(bool, meta.row_groups.items.len);
        defer allocator.free(rg_skip_mask);
        @memset(rg_skip_mask, false);

        // Find filter column index and type
        if (filter_col_name) |name| {
            if (meta.row_groups.items.len > 0) {
                for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                    if (col.meta_data) |md| {
                        const path_parts = md.path_in_schema.items;
                        if (std.mem.eql(u8, path_parts[path_parts.len - 1], name)) {
                            filter_col_idx = idx;
                            filter_col_type = md.type;
                            break;
                        }
                    }
                }
            }
        }

        // Create EncodedFilter for unified type handling (lives through entire scan)
        var encoded_filter: ?zpq.core.filter.EncodedFilter = null;
        defer if (encoded_filter) |*ef| ef.deinit();

        if (filter_col_type) |col_type| {
            encoded_filter = zpq.core.filter.EncodedFilter.parse(allocator, filter_val.?, col_type) catch |err| {
                std.debug.print("Failed to parse filter value '{s}' for type {}: {}\n", .{ filter_val.?, col_type, err });
                return error.InvalidFilterValue;
            };

            // Mark row groups to skip via metadata pruning
            for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
                if (pf.shouldSkipRowGroup(rg_idx, filter_col_name.?, &encoded_filter.?)) {
                    rg_skip_mask[rg_idx] = true;
                    row_groups_skipped += 1;
                    // Record skipped row group in tracer
                    if (tracer) |*t| {
                        t.recordRowGroup(false);
                        t.metrics.rows_skipped += @intCast(rg_meta.num_rows);
                    }
                }
            }
        }

        // Batched fetch of filter columns (if we have a filter)
        var filter_col_buffers: ?[][]u8 = null;
        var filter_col_offsets: ?[]u64 = null; // Base offset for each buffer
        var batched_fetch_ns: u64 = 0;
        defer {
            if (filter_col_buffers) |bufs| {
                for (bufs) |buf| allocator.free(buf);
                allocator.free(bufs);
            }
            if (filter_col_offsets) |offs| allocator.free(offs);
        }

        if (filter_col_idx) |f_idx| {
            // Count non-skipped row groups
            var active_count: usize = 0;
            for (rg_skip_mask) |skip| {
                if (!skip) active_count += 1;
            }

            if (active_count > 0) {
                // Collect ranges for filter column across all active row groups
                var ranges = try allocator.alloc(zpq.io.interface.Range, active_count);
                defer allocator.free(ranges);
                filter_col_buffers = try allocator.alloc([]u8, active_count);
                filter_col_offsets = try allocator.alloc(u64, active_count);

                var buf_idx: usize = 0;
                for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
                    if (rg_skip_mask[rg_idx]) continue;

                    const chunk = rg_meta.columns.items[f_idx];
                    const md = chunk.meta_data orelse continue;

                    var start: u64 = @intCast(md.data_page_offset);
                    if (md.dictionary_page_offset) |dpo| {
                        if (dpo < start) start = @intCast(dpo);
                    }
                    const len: u64 = @intCast(md.total_compressed_size);

                    ranges[buf_idx] = .{ .start = start, .end = start + len };
                    filter_col_buffers.?[buf_idx] = try allocator.alloc(u8, @intCast(len));
                    filter_col_offsets.?[buf_idx] = start;
                    buf_idx += 1;
                }

                // Single batched fetch for ALL filter columns
                var t0 = try std.time.Timer.start();
                try pf.source.readRanges(ranges, filter_col_buffers.?);
                batched_fetch_ns = t0.read();

                std.debug.print("  Batched filter fetch: {d} ranges in {d:.1}ms\n", .{
                    active_count,
                    @as(f64, @floatFromInt(batched_fetch_ns)) / 1_000_000.0,
                });
            }
        }

        // Process each row group
        var filter_buf_idx: usize = 0;
        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            // Skip row groups pruned by metadata
            if (rg_skip_mask[rg_idx]) continue;

            var rg = try pf.rowGroup(rg_idx);
            defer rg.deinit();

            if (filter_col_idx) |f_idx| {
                // ========== TWO-PHASE COLUMN FETCHING ==========
                // Phase 1 data was pre-fetched in the batched call above.
                // Now we just need to decode and build selection vectors.
                //
                // Timing instrumentation
                var phase1_decode_ns: u64 = 0;
                var phase2_fetch_ns: u64 = 0;
                var phase2_decode_ns: u64 = 0;

                // Inject the pre-fetched buffer as a memory source
                const prefetched_buf = filter_col_buffers.?[filter_buf_idx];
                const prefetched_offset = filter_col_offsets.?[filter_buf_idx];
                filter_buf_idx += 1;

                // Create a memory source from the pre-fetched buffer
                var mem_source = zpq.io.interface.local.MemorySource.initWithOffset(prefetched_buf, prefetched_offset);

                // Get filter column metadata and create reader using pre-fetched memory
                const filter_col = rg_meta.columns.items[f_idx];
                const filter_md = filter_col.meta_data.?;
                const filter_levels = meta.getColumnLevels(filter_md.path_in_schema.items);
                const filter_schema = meta.getColumnSchema(filter_md.path_in_schema.items);
                const filter_type_len = if (filter_schema) |se| se.type_length else null;
                // Use pre-fetched memory source instead of remote source
                const filter_col_reader = try zpq.column.ColumnReader.init(mem_source.source(), filter_col);

                // Try to get page-level statistics for smarter skipping
                var column_index: ?zpq.core.page_index.ColumnIndex = null;
                defer if (column_index) |*ci| ci.deinit(allocator);
                column_index = try rg.getColumnIndex(f_idx);
                var pages_skipped: usize = 0;

                // Storage for batch selections (used in Phase 2)
                var batch_selections = std.ArrayListUnmanaged(zpq.core.simd.SelectionVector){};
                defer batch_selections.deinit(allocator);
                var batch_sizes = std.ArrayListUnmanaged(usize){};
                defer batch_sizes.deinit(allocator);
                var rg_has_matches = false;

                // Phase 1: Decode filter column (already fetched in batch) and build selection vectors
                var t0 = try std.time.Timer.start();
                if (filter_md.type == .BYTE_ARRAY or filter_md.type == .FIXED_LEN_BYTE_ARRAY) {
                    var filter_reader = zpq.core.batch_reader.BatchReader([]const u8).init(
                        allocator,
                        filter_col_reader,
                        filter_md.type,
                        @intCast(filter_levels.max_def),
                        @intCast(filter_levels.max_rep),
                        filter_type_len,
                    );
                    defer filter_reader.deinit();

                    var row_idx: usize = 0;

                    // Try to use fast dictionary index path
                    // First, we need to load the first page to get the dictionary
                    var target_dict_idx: ?u64 = null;
                    var use_dict_fast_path = false;

                    while (row_idx < @as(usize, @intCast(rg_meta.num_rows))) {
                        // Only check skip logic at page boundaries
                        if (filter_reader.isAtPageBoundary()) {
                            const next_page = filter_reader.getPageIndex();
                            // Check ColumnIndex using EncodedFilter (handles all types)
                            const should_skip = if (column_index) |*ci|
                                if (encoded_filter) |*ef| !ef.mightContainInPage(ci, next_page) else false
                            else
                                false;
                            if (should_skip) {
                                // Skip this entire page
                                pages_skipped += 1;
                                if (try filter_reader.skipNextPage()) |skipped_rows| {
                                    var remaining = skipped_rows;
                                    while (remaining > 0) {
                                        const batch_size = @min(1024, remaining);
                                        const sel = zpq.core.simd.SelectionVector.init();
                                        try batch_selections.append(allocator, sel);
                                        try batch_sizes.append(allocator, batch_size);
                                        total_values += batch_size;
                                        remaining -= batch_size;
                                    }
                                    row_idx += skipped_rows;
                                    continue;
                                }
                            }
                        }

                        const batch_size = @min(1024, @as(usize, @intCast(rg_meta.num_rows)) - row_idx);

                        // Check if we can use dictionary fast path (after first page load)
                        if (target_dict_idx == null and filter_reader.hasDictionary()) {
                            target_dict_idx = filter_reader.findInDictionary(filter_val.?);
                            use_dict_fast_path = target_dict_idx != null;
                        }

                        var sel = zpq.core.simd.SelectionVector.init();
                        var n_read: usize = 0;

                        if (use_dict_fast_path) {
                            // Fast path: compare dictionary indices (integers)
                            n_read = try filter_reader.scanDictIndicesIntoBatch(target_dict_idx.?, &sel, batch_size);
                        } else {
                            // Slow path: decode strings and compare using EncodedFilter
                            var buf: [1024]?[]const u8 = undefined;
                            n_read = try filter_reader.nextBatch(buf[0..batch_size]);
                            for (buf[0..n_read], 0..) |val, i| {
                                if (val) |v| {
                                    if (encoded_filter.?.matchesBytes(v)) sel.setBitIndices(i);
                                }
                            }
                        }

                        if (n_read == 0) break;

                        rows_selected += sel.count();
                        total_values += n_read;
                        if (sel.count() > 0) rg_has_matches = true;
                        try batch_selections.append(allocator, sel);
                        try batch_sizes.append(allocator, n_read);
                        row_idx += n_read;
                    }
                    phase1_decode_ns = t0.read();
                } else if (filter_md.type == .INT64 or filter_md.type == .INT32 or
                    filter_md.type == .FLOAT or filter_md.type == .DOUBLE or
                    filter_md.type == .BOOLEAN or filter_md.type == .INT96)
                {
                    // Fixed-width numeric types - use unified or legacy path
                    if (unified_filter) {
                        // === UNIFIED PATH: byte-level comparison via EncodedFilter ===
                        const ci_ptr: ?*const zpq.core.page_index.ColumnIndex = if (column_index) |*ci| ci else null;

                        switch (filter_md.type) {
                            .INT64 => {
                                var filter_reader = zpq.core.batch_reader.BatchReader(i64).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    i64,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            .INT32 => {
                                var filter_reader = zpq.core.batch_reader.BatchReader(i32).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    i32,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            .FLOAT => {
                                var filter_reader = zpq.core.batch_reader.BatchReader(f32).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    f32,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            .DOUBLE => {
                                var filter_reader = zpq.core.batch_reader.BatchReader(f64).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    f64,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            .BOOLEAN => {
                                var filter_reader = zpq.core.batch_reader.BatchReader(bool).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    bool,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            .INT96 => {
                                var filter_reader = zpq.core.batch_reader.BatchReader([12]u8).init(
                                    allocator,
                                    filter_col_reader,
                                    filter_md.type,
                                    @intCast(filter_levels.max_def),
                                    @intCast(filter_levels.max_rep),
                                    filter_type_len,
                                );
                                defer filter_reader.deinit();

                                const result = try scanFixedWidthFilterColumn(
                                    [12]u8,
                                    allocator,
                                    &filter_reader,
                                    &encoded_filter.?,
                                    ci_ptr,
                                    @intCast(rg_meta.num_rows),
                                    &batch_selections,
                                    &batch_sizes,
                                );
                                total_values += result.total_values;
                                rows_selected += result.rows_selected;
                                pages_skipped = result.pages_skipped;
                                rg_has_matches = result.has_matches;
                            },
                            else => unreachable,
                        }
                        phase1_decode_ns = t0.read();
                    } else {
                        // === LEGACY PATH: type-specific comparison (INT64 only) ===
                        if (filter_md.type != .INT64) {
                            std.debug.print("Legacy path only supports INT64. Use --unified-filter for {any}\n", .{filter_md.type});
                            return error.UnsupportedFilterType;
                        }

                        const filter_i64 = std.fmt.parseInt(i64, filter_val.?, 10) catch {
                            std.debug.print("Invalid INT64 filter value: {s}\n", .{filter_val.?});
                            return error.InvalidFilterValue;
                        };

                        var filter_reader = zpq.core.batch_reader.BatchReader(i64).init(
                            allocator,
                            filter_col_reader,
                            filter_md.type,
                            @intCast(filter_levels.max_def),
                            @intCast(filter_levels.max_rep),
                            filter_type_len,
                        );
                        defer filter_reader.deinit();

                        var row_idx: usize = 0;

                        while (row_idx < @as(usize, @intCast(rg_meta.num_rows))) {
                            if (filter_reader.isAtPageBoundary()) {
                                const next_page = filter_reader.getPageIndex();
                                const should_skip = if (column_index) |*ci|
                                    if (encoded_filter) |*ef| !ef.mightContainInPage(ci, next_page) else false
                                else
                                    false;
                                if (should_skip) {
                                    pages_skipped += 1;
                                    if (try filter_reader.skipNextPage()) |skipped_rows| {
                                        var remaining = skipped_rows;
                                        while (remaining > 0) {
                                            const batch_size = @min(1024, remaining);
                                            const sel = zpq.core.simd.SelectionVector.init();
                                            try batch_selections.append(allocator, sel);
                                            try batch_sizes.append(allocator, batch_size);
                                            total_values += batch_size;
                                            remaining -= batch_size;
                                        }
                                        row_idx += skipped_rows;
                                        continue;
                                    }
                                }
                            }

                            const batch_size = @min(1024, @as(usize, @intCast(rg_meta.num_rows)) - row_idx);

                            var sel = zpq.core.simd.SelectionVector.init();
                            var buf: [1024]?i64 = undefined;
                            const n_read = try filter_reader.nextBatch(buf[0..batch_size]);

                            for (buf[0..n_read], 0..) |val, i| {
                                if (val) |v| {
                                    if (v == filter_i64) sel.setBitIndices(i);
                                }
                            }

                            if (n_read == 0) break;

                            rows_selected += sel.count();
                            total_values += n_read;
                            if (sel.count() > 0) rg_has_matches = true;
                            try batch_selections.append(allocator, sel);
                            try batch_sizes.append(allocator, n_read);
                            row_idx += n_read;
                        }
                        phase1_decode_ns = t0.read();
                    }
                } else {
                    std.debug.print("Unsupported filter column type: {any}\n", .{filter_md.type});
                    return error.UnsupportedFilterType;
                }

                // Phase 2: Only fetch remaining columns if we have matches (skip if --count)
                if (rg_has_matches and !count_only) {
                    t0.reset();
                    try rg.prefetchExcluding(&[_]usize{f_idx});
                    phase2_fetch_ns = t0.read();

                    t0.reset();

                    // Process each non-filter column with stored selections
                    for (rg_meta.columns.items, 0..) |col, col_idx| {
                        if (col_idx == f_idx) continue;

                        const md = col.meta_data orelse continue;
                        const levels = meta.getColumnLevels(md.path_in_schema.items);
                        const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                        const type_length = if (schema_elem) |se| se.type_length else null;
                        const col_reader = try rg.columnReader(col_idx);

                        switch (md.type) {
                            .INT32 => try processColumnWithSelection(i32, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            .INT64 => try processColumnWithSelection(i64, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            .FLOAT => try processColumnWithSelection(f32, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            .DOUBLE => try processColumnWithSelection(f64, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try processColumnWithSelection([]const u8, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            .INT96 => try processColumnWithSelection([12]u8, allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, batch_selections.items, batch_sizes.items),
                            else => {},
                        }
                    }
                    phase2_decode_ns = t0.read();
                }

                // Print timing breakdown for this row group
                // Note: P1-fetch is batched across all row groups (shown once above)
                const total_pages = if (column_index) |ci| ci.numPages() else 0;
                std.debug.print("  RG[{d}]: P1-decode={d:.1}ms P2-fetch={d:.1}ms P2-decode={d:.1}ms matches={} pages_skipped={d}/{d}\n", .{
                    rg_idx,
                    @as(f64, @floatFromInt(phase1_decode_ns)) / 1_000_000.0,
                    @as(f64, @floatFromInt(phase2_fetch_ns)) / 1_000_000.0,
                    @as(f64, @floatFromInt(phase2_decode_ns)) / 1_000_000.0,
                    rg_has_matches,
                    pages_skipped,
                    total_pages,
                });

                // Record tracer metrics for this row group
                if (tracer) |*t| {
                    t.recordFilterDecode(phase1_decode_ns, batch_selections.items.len * 1024); // Approximate rows
                    t.recordMaterialize(phase2_decode_ns, rows_selected);
                    t.metrics.pages_decoded += total_pages - pages_skipped;
                    t.recordRowGroup(true);
                }
                // If no matches in this row group, we skip fetching all other columns entirely!
            } else {
                // FULL SCAN PATH (no filter or column not found)
                for (rg_meta.columns.items, 0..) |col, col_idx| {
                    if (col.meta_data) |md| {
                        const levels = meta.getColumnLevels(md.path_in_schema.items);
                        const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                        const type_length = if (schema_elem) |se| se.type_length else null;
                        const reader = try rg.columnReader(col_idx);

                        const n = switch (md.type) {
                            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try scanColumnBatch(allocator, []const u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            .INT32 => try scanColumnBatch(allocator, i32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            .INT64 => try scanColumnBatch(allocator, i64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            .INT96 => try scanColumnBatch(allocator, [12]u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            .FLOAT => try scanColumnBatch(allocator, f32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            .DOUBLE => try scanColumnBatch(allocator, f64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length),
                            else => blk: {
                                var reader_inner = try rg.columnReader(col_idx);
                                var count: u64 = 0;
                                while (try reader_inner.next(allocator)) |page| {
                                    var p = page;
                                    defer p.deinit(allocator);
                                    if (p.header.data_page_header) |dph| count += @intCast(dph.num_values);
                                }
                                break :blk count;
                            },
                        };
                        total_values += n;
                    }
                }
            }
        }
    }

    const elapsed_ns = timer.read();
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000_000.0;
    const mvals_per_s = if (elapsed_s > 0) @as(f64, @floatFromInt(total_values)) / elapsed_s / 1_000_000.0 else 0.0;

    std.debug.print("Scanned {d} values in {d:.2}ms ({d:.2} MVal/s)\n", .{ total_values, @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0, mvals_per_s });
    if (filter_col_name) |_| {
        std.debug.print("Rows selected: {d}\n", .{rows_selected});
    }
    if (row_groups_skipped > 0) {
        std.debug.print("Row groups skipped via stats: {d}\n", .{row_groups_skipped});
    }

    // Output tracer results if enabled
    if (tracer) |*t| {
        // Populate final metrics
        t.metrics.rows_scanned = total_values;
        t.metrics.rows_selected = rows_selected;
        t.metrics.row_groups_skipped = row_groups_skipped;

        if (trace_output) |output| {
            if (std.mem.eql(u8, output, "-")) {
                // Write to stdout
                const json = t.toJson(allocator) catch |err| {
                    std.debug.print("Failed to generate trace JSON: {}\n", .{err});
                    return;
                };
                defer allocator.free(json);
                std.debug.print("\n--- Trace Output ---\n{s}\n", .{json});
            } else {
                // Write to file
                t.writeToFile(output) catch |err| {
                    std.debug.print("Failed to write trace to {s}: {}\n", .{ output, err });
                };
                std.debug.print("Trace written to: {s}\n", .{output});
            }
        }
    }
}

fn scanColumnBatch(allocator: std.mem.Allocator, comptime T: type, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32) !u64 {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var total: u64 = 0;
    var buffer: [1024]?T = undefined;

    while (true) {
        const n = try batch_reader.nextBatch(&buffer);
        if (n == 0) break;
        total += n;
    }
    return total;
}

/// Unified filter scanning for fixed-width types (INT32, INT64, FLOAT, DOUBLE).
/// Uses EncodedFilter.matchesBytes() for byte-level comparison - single code path for all types.
///
/// Key insight: For equality predicates, we can compare raw encoded bytes instead of
/// decoding to native types. This enables a single code path AND opens the door for
/// SIMD-accelerated comparison (compare 32 bytes at a time across multiple values).
const ScanFilterResult = struct {
    total_values: u64,
    rows_selected: u64,
    pages_skipped: usize,
    has_matches: bool,
};

fn scanFixedWidthFilterColumn(
    comptime T: type,
    allocator: std.mem.Allocator,
    filter_reader_ptr: *zpq.core.batch_reader.BatchReader(T),
    encoded_filter: *const zpq.core.filter.EncodedFilter,
    column_index: ?*const zpq.core.page_index.ColumnIndex,
    num_rows: usize,
    batch_selections: *std.ArrayListUnmanaged(zpq.core.simd.SelectionVector),
    batch_sizes: *std.ArrayListUnmanaged(usize),
) !ScanFilterResult {
    var result = ScanFilterResult{
        .total_values = 0,
        .rows_selected = 0,
        .pages_skipped = 0,
        .has_matches = false,
    };
    var row_idx: usize = 0;

    while (row_idx < num_rows) {
        // Page-level skip using EncodedFilter
        if (filter_reader_ptr.isAtPageBoundary()) {
            const next_page = filter_reader_ptr.getPageIndex();
            const should_skip = if (column_index) |ci|
                !encoded_filter.mightContainInPage(ci, next_page)
            else
                false;
            if (should_skip) {
                result.pages_skipped += 1;
                if (try filter_reader_ptr.skipNextPage()) |skipped_rows| {
                    var remaining = skipped_rows;
                    while (remaining > 0) {
                        const batch_size = @min(1024, remaining);
                        const sel = zpq.core.simd.SelectionVector.init();
                        try batch_selections.append(allocator, sel);
                        try batch_sizes.append(allocator, batch_size);
                        result.total_values += batch_size;
                        remaining -= batch_size;
                    }
                    row_idx += skipped_rows;
                    continue;
                }
            }
        }

        const batch_size = @min(1024, num_rows - row_idx);

        var sel = zpq.core.simd.SelectionVector.init();
        var buf: [1024]?T = undefined;
        const n_read = try filter_reader_ptr.nextBatch(buf[0..batch_size]);

        // Unified byte-level comparison using EncodedFilter
        for (buf[0..n_read], 0..) |val, i| {
            if (val) |v| {
                // Convert decoded value to bytes and compare
                // For fixed-width types, this is just a pointer cast (zero cost)
                const value_bytes = std.mem.asBytes(&v);
                if (encoded_filter.matchesBytes(value_bytes)) {
                    sel.setBitIndices(i);
                }
            }
        }

        if (n_read == 0) break;

        result.rows_selected += sel.count();
        result.total_values += n_read;
        if (sel.count() > 0) result.has_matches = true;
        try batch_selections.append(allocator, sel);
        try batch_sizes.append(allocator, n_read);
        row_idx += n_read;
    }

    return result;
}

/// Process a column using pre-computed selection vectors from Phase 1.
/// This is used in two-phase column fetching: we already know which rows match
/// from scanning the filter column, so we can skip/select accordingly.
fn processColumnWithSelection(
    comptime T: type,
    allocator: std.mem.Allocator,
    reader: zpq.column.ColumnReader,
    col_type: zpq.schema.Type,
    max_def: u16,
    max_rep: u16,
    type_length: ?i32,
    batch_selections: []const zpq.core.simd.SelectionVector,
    batch_sizes: []const usize,
) !void {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var buffer: [1024]?T = undefined;

    for (batch_selections, batch_sizes) |sel, batch_size| {
        if (sel.count() > 0) {
            // Materialize only selected rows
            var sel_copy = sel; // nextBatchSelected needs mutable pointer
            _ = try batch_reader.nextBatchSelected(buffer[0..sel.count()], &sel_copy, batch_size);
        } else {
            // Skip the entire batch
            try batch_reader.skip(batch_size);
        }
    }
}

fn cmdSchema(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("Schema for {s}:\n", .{path});
        for (meta.schema.items, 0..) |elem, i| {
            const indent = if (elem.num_children == null) "  " else "";
            const rt = elem.repetition_type orelse .REQUIRED;
            std.debug.print("{s}[{d}] {s} ({any}/{d})", .{ indent, i, elem.name, rt, @intFromEnum(rt) });
            if (elem.type) |t| {
                std.debug.print(" type={any}", .{t});
            }
            if (elem.type_length) |tl| {
                std.debug.print(" len={d}", .{tl});
            }
            if (elem.field_id) |fid| {
                std.debug.print(" id={d}", .{fid});
            }
            std.debug.print("\n", .{});
        }
    }
}

fn cmdMeta(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("File: {s}\n", .{path});
        std.debug.print("Version: {d}\n", .{meta.version});
        std.debug.print("Rows: {d}\n", .{meta.num_rows});
        std.debug.print("Created By: {s}\n", .{meta.created_by orelse "unknown"});
        std.debug.print("Row Groups: {d}\n", .{meta.row_groups.items.len});

        for (meta.row_groups.items, 0..) |rg, i| {
            std.debug.print("\nRow Group {d}:\n", .{i});
            std.debug.print("  Rows: {d}\n", .{rg.num_rows});
            std.debug.print("  Total Bytes: {d}\n", .{rg.total_byte_size});

            std.debug.print("  Columns:\n", .{});
            for (rg.columns.items, 0..) |col, j| {
                if (col.meta_data) |md| {
                    const ratio = if (md.total_compressed_size > 0)
                        @as(f64, @floatFromInt(md.total_uncompressed_size)) / @as(f64, @floatFromInt(md.total_compressed_size))
                    else
                        0.0;

                    std.debug.print("    [{d}] {any} ({any}) ratio={d:.2}x\n", .{ j, md.type, md.codec, ratio });
                    std.debug.print("          Values: {d}, Enc: ", .{md.num_values});
                    for (md.encodings.items) |enc| {
                        std.debug.print("{any} ", .{enc});
                    }
                    std.debug.print("\n", .{});
                    if (md.statistics) |stats| {
                        std.debug.print("          Stats: ", .{});
                        if (stats.min_value) |min| std.debug.print("min={s} ", .{min});
                        if (stats.max_value) |max| std.debug.print("max={s} ", .{max});
                        if (stats.null_count) |nc| std.debug.print("nulls={d} ", .{nc});
                        std.debug.print("\n", .{});
                    }
                }
            }
        }
    }
}

fn cmdCat(allocator: std.mem.Allocator, path: []const u8, limit: usize, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();
    try pf.readFooter();

    // Simple Columnar Dump
    if (pf.metadata) |meta| {
        for (meta.row_groups.items, 0..) |rg, rg_idx| {
            var rg_reader = try pf.rowGroup(rg_idx);
            defer rg_reader.deinit();

            for (rg.columns.items, 0..) |col, col_idx| {
                if (col.meta_data) |md| {
                    std.debug.print("Column {d} (", .{col_idx});
                    for (md.path_in_schema.items, 0..) |part, k| {
                        if (k > 0) std.debug.print(".", .{});
                        std.debug.print("{s}", .{part});
                    }
                    std.debug.print("):\n", .{});

                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                    const type_length = if (schema_elem) |se| se.type_length else null;
                    const reader = try rg_reader.columnReader(col_idx);

                    // Type dispatch for BatchReader
                    switch (md.type) {
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try dumpColumnBatch(allocator, []const u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT32 => try dumpColumnBatch(allocator, i32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT64 => try dumpColumnBatch(allocator, i64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .INT96 => try dumpColumnBatch(allocator, [12]u8, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .FLOAT => try dumpColumnBatch(allocator, f32, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        .DOUBLE => try dumpColumnBatch(allocator, f64, reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_length, limit),
                        else => std.debug.print("        (Type {any} not yet supported by BatchReader)\n", .{md.type}),
                    }
                }
            }
        }
    }
}

fn formatInt96(val: [12]u8) !void {
    const nanos = std.mem.readInt(u64, val[0..8], .little);
    const days = std.mem.readInt(u32, val[8..12], .little);

    // Julian Day 2440588 is 1970-01-01
    const julian_epoch = 2440588;
    const unix_seconds = (@as(i64, days) - julian_epoch) * 86400 + @as(i64, @intCast(nanos / 1_000_000_000));

    std.debug.print("{d} (JD={d}, NS={d})", .{ unix_seconds, days, nanos });
}

fn dumpColumnBatch(allocator: std.mem.Allocator, comptime T: type, reader: zpq.column.ColumnReader, col_type: zpq.schema.Type, max_def: u16, max_rep: u16, type_length: ?i32, limit: usize) !void {
    var batch_reader = zpq.core.batch_reader.BatchReader(T).init(allocator, reader, col_type, max_def, max_rep, type_length);
    defer batch_reader.deinit();

    var values_printed: usize = 0;
    var buffer: [1024]?T = undefined;

    while (values_printed < limit) {
        const batch_size = @min(buffer.len, limit - values_printed);
        const n = try batch_reader.nextBatch(buffer[0..batch_size]);
        if (n == 0) break;

        for (buffer[0..n]) |maybe_val| {
            if (maybe_val) |val| {
                if (T == []const u8) {
                    std.debug.print("  {s}\n", .{val});
                } else if (T == [12]u8) {
                    std.debug.print("  ", .{});
                    try formatInt96(val);
                    std.debug.print("\n", .{});
                } else {
                    std.debug.print("  {any}\n", .{val});
                }
            } else {
                std.debug.print("  null\n", .{});
            }
            values_printed += 1;
        }
    }
}

// The detailed deep-dive inspection (formerly 'inspect')
fn cmdPages(allocator: std.mem.Allocator, path: []const u8, is_async: bool, loop: *xev.Loop, thread_pool: *xev.ThreadPool, resolver: Resolver, verify_tls: bool) !void {
    var pf = try openFile(allocator, path, is_async, loop, thread_pool, resolver, verify_tls);
    defer pf.deinit();

    try pf.readFooter();

    if (pf.metadata) |meta| {
        // We skip printing metadata summary here as that is for 'meta' command

        for (meta.row_groups.items, 0..) |rg, i| {
            std.debug.print("Row Group {d}:\n", .{i});

            for (rg.columns.items, 0..) |col, j| {
                std.debug.print("    Column {d}:\n", .{j});

                // Store dictionary values for this column
                var dict_strings = std.ArrayListUnmanaged([]const u8){};
                defer dict_strings.deinit(allocator);
                var dict_int32 = std.ArrayListUnmanaged(i32){};
                defer dict_int32.deinit(allocator);
                var dict_int64 = std.ArrayListUnmanaged(i64){};
                defer dict_int64.deinit(allocator);
                var dict_double = std.ArrayListUnmanaged(f64){};
                defer dict_double.deinit(allocator);
                var dict_float = std.ArrayListUnmanaged(f32){};
                defer dict_float.deinit(allocator);

                if (col.meta_data) |md| {
                    std.debug.print("      Type: {any}, Codec: {any}\n", .{ md.type, md.codec });

                    const levels = meta.getColumnLevels(md.path_in_schema.items);
                    std.debug.print("      Levels: MaxDef={d}, MaxRep={d}\n", .{ levels.max_def, levels.max_rep });

                    var reader = try zpq.column.ColumnReader.init(pf.source, col);
                    var page_idx: usize = 0;
                    while (try reader.next(allocator)) |page| {
                        var p = page;
                        defer p.deinit(allocator);
                        std.debug.print("      Page {d}: {any} Size={d} (Comp={d})\n", .{ page_idx, p.header.type, p.header.uncompressed_page_size, p.header.compressed_page_size });

                        if (p.header.type == .DICTIONARY_PAGE) {
                            var decoder = zpq.decoder.Decoder.init(p.data);
                            std.debug.print("        Dictionary Values ({d} bytes):\n", .{p.data.len});
                            if (md.type == .BYTE_ARRAY) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readByteArray();
                                    // Store copy of string because p.data will be freed
                                    const val_copy = try allocator.dupe(u8, val);
                                    try dict_strings.append(allocator, val_copy);
                                    // std.debug.print("          [{d}] {s}\n", .{dict_strings.items.len - 1, val});
                                }
                            } else if (md.type == .INT32) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readInt32();
                                    try dict_int32.append(allocator, val);
                                }
                            } else if (md.type == .INT64) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readInt64();
                                    try dict_int64.append(allocator, val);
                                }
                            } else if (md.type == .DOUBLE) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readDouble();
                                    try dict_double.append(allocator, val);
                                }
                            } else if (md.type == .FLOAT) {
                                while (decoder.hasMore()) {
                                    const val = try decoder.readFloat();
                                    try dict_float.append(allocator, val);
                                }
                            }
                        } else if (p.header.type == .DATA_PAGE) {
                            if (p.header.data_page_header) |dph| {
                                std.debug.print("        Encoding: {any}, Values: {d}\n", .{ dph.encoding, dph.num_values });
                                if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                                    var data_slice = p.data;

                                    // Skip Repetition Levels
                                    if (levels.max_rep > 0) {
                                        if (data_slice.len < 4) {
                                            std.debug.print("        Error: Not enough data for Repetition Levels length\n", .{});
                                            continue;
                                        }
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        // std.debug.print("        Skipping Repetition Levels: {d} bytes\n", .{len});
                                        if (data_slice.len < 4 + len) {
                                            std.debug.print("        Error: Not enough data for Repetition Levels\n", .{});
                                            continue;
                                        }
                                        data_slice = data_slice[4 + len ..];
                                    }

                                    // Decode Definition Levels
                                    var def_levels = std.ArrayListUnmanaged(i32){};
                                    defer def_levels.deinit(allocator);

                                    if (levels.max_def > 0) {
                                        if (data_slice.len < 4) {
                                            std.debug.print("        Error: Not enough data for Definition Levels length\n", .{});
                                            continue;
                                        }
                                        const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                        // std.debug.print("        Definition Levels: {d} bytes\n", .{len});
                                        if (data_slice.len < 4 + len) {
                                            std.debug.print("        Error: Not enough data for Definition Levels\n", .{});
                                            continue;
                                        }

                                        const def_level_data = data_slice[4 .. 4 + len];
                                        data_slice = data_slice[4 + len ..];

                                        const max_val = @as(u32, @intCast(levels.max_def)) + 1;
                                        const next_pow2 = try std.math.ceilPowerOfTwo(u32, max_val);
                                        const bit_width = std.math.log2_int(u32, next_pow2);

                                        var rle_dec = zpq.rle.RleDecoder.init(def_level_data, @intCast(bit_width));

                                        var count: usize = 0;
                                        while (count < dph.num_values) : (count += 1) {
                                            const res = rle_dec.next();
                                            if (res) |maybe_val| {
                                                if (maybe_val) |val| {
                                                    try def_levels.append(allocator, @intCast(val));
                                                } else {
                                                    break;
                                                }
                                            } else |err| {
                                                std.debug.print("Error decoding def level: {any}\n", .{err});
                                                break;
                                            }
                                        }
                                    }

                                    if (data_slice.len > 0) {
                                        const bit_width = data_slice[0];
                                        std.debug.print("        Indices BitWidth: {d}\n", .{bit_width});
                                        var rle_dec = zpq.rle.RleDecoder.init(data_slice[1..], bit_width);

                                        var print_count: usize = 0;

                                        std.debug.print("        Data Sample:\n", .{});

                                        // If max_def > 0, we iterate def_levels
                                        if (levels.max_def > 0) {
                                            for (def_levels.items) |dl| {
                                                if (dl == levels.max_def) {
                                                    // Value present, read index
                                                    if (try rle_dec.next()) |idx| {
                                                        if (print_count < 10) {
                                                            if (md.type == .BYTE_ARRAY) {
                                                                if (idx < dict_strings.items.len) {
                                                                    std.debug.print("          - {s}\n", .{dict_strings.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .INT32) {
                                                                if (idx < dict_int32.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_int32.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .INT64) {
                                                                if (idx < dict_int64.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_int64.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .DOUBLE) {
                                                                if (idx < dict_double.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_double.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else if (md.type == .FLOAT) {
                                                                if (idx < dict_float.items.len) {
                                                                    std.debug.print("          - {d}\n", .{dict_float.items[idx]});
                                                                } else {
                                                                    std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                                }
                                                            } else {
                                                                std.debug.print("          - <idx {d}>\n", .{idx});
                                                            }
                                                            print_count += 1;
                                                        } else if (print_count == 10) {
                                                            std.debug.print("          ... \n", .{});
                                                            print_count += 1;
                                                        }
                                                    }
                                                } else {
                                                    // NULL
                                                    if (print_count < 10) {
                                                        std.debug.print("          - NULL\n", .{});
                                                        print_count += 1;
                                                    }
                                                }
                                            }
                                        } else {
                                            // No definition levels, all values present
                                            var k: i32 = 0;
                                            while (k < dph.num_values) : (k += 1) {
                                                if (try rle_dec.next()) |idx| {
                                                    if (print_count < 10) {
                                                        if (md.type == .BYTE_ARRAY) {
                                                            if (idx < dict_strings.items.len) {
                                                                std.debug.print("          - {s}\n", .{dict_strings.items[idx]});
                                                            } else {
                                                                std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                            }
                                                        } else if (md.type == .INT64) {
                                                            if (idx < dict_int64.items.len) {
                                                                std.debug.print("          - {d}\n", .{dict_int64.items[idx]});
                                                            } else {
                                                                std.debug.print("          - <idx {d} out of bounds>\n", .{idx});
                                                            }
                                                        } else {
                                                            std.debug.print("          - <idx {d}>\n", .{idx});
                                                        }
                                                        print_count += 1;
                                                    } else if (print_count == 10) {
                                                        std.debug.print("          ... \n", .{});
                                                        print_count += 1;
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                        page_idx += 1;
                    }
                }

                // Cleanup dictionary strings
                for (dict_strings.items) |s| {
                    allocator.free(s);
                }
            }
        }
    }
}

fn cmdWriteTest(allocator: std.mem.Allocator, output_path: []const u8) !void {
    const writer = zpq.core.writer;

    std.debug.print("=== ZPQ Write Test ===\n\n", .{});
    std.debug.print("Writing to: {s}\n\n", .{output_path});

    // Create writer
    var pw = try writer.ParquetWriter.init(allocator, output_path);
    defer pw.deinit();

    // Define schema
    try pw.setColumns(&[_]writer.ColumnDef{
        .{ .name = "id", .type = .INT32 },
        .{ .name = "value", .type = .DOUBLE },
        .{ .name = "name", .type = .BYTE_ARRAY },
        .{ .name = "active", .type = .BOOLEAN },
    });

    // Write row group with test data
    var rg = try pw.beginRowGroup();

    const ids = [_]i32{ 1, 2, 3, 4, 5 };
    const values = [_]f64{ 1.1, 2.2, 3.3, 4.4, 5.5 };
    const names = [_][]const u8{ "alice", "bob", "charlie", "diana", "eve" };
    const active = [_]bool{ true, false, true, true, false };

    try rg.writeInt32Column(&ids);
    try rg.writeDoubleColumn(&values);
    try rg.writeByteArrayColumn(&names);
    try rg.writeBooleanColumn(&active);

    try pw.finishRowGroup(rg, 5);
    try pw.finish();

    std.debug.print("Written 5 rows with 4 columns:\n", .{});
    std.debug.print("  - id: INT32 [1, 2, 3, 4, 5]\n", .{});
    std.debug.print("  - value: DOUBLE [1.1, 2.2, 3.3, 4.4, 5.5]\n", .{});
    std.debug.print("  - name: BYTE_ARRAY [alice, bob, charlie, diana, eve]\n", .{});
    std.debug.print("  - active: BOOLEAN [true, false, true, true, false]\n", .{});
    std.debug.print("\n=== SUCCESS ===\n", .{});
    std.debug.print("\nVerify with:\n", .{});
    std.debug.print("  zpq schema {s}\n", .{output_path});
    std.debug.print("  zpq cat {s}\n", .{output_path});
    std.debug.print("  duckdb -c \"SELECT * FROM '{s}'\"\n", .{output_path});
}
