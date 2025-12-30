/// Benchmark probe: Measure time breakdown for filtered scans.
///
/// Purpose: Quantify where time goes in lazy materialization path:
///   - Filter column decode
///   - Skip operations
///   - Materialization of selected rows
///   - Loop/dispatch overhead
///
/// Usage:
///   cd probes
///   zig build run-bench_skip_breakdown -Doptimize=ReleaseFast -- ../data/large.parquet status active
///
/// Output: JSON trace file to bench/traces/
///
/// Created: Dec 2024
const std = @import("std");
const zpq = @import("zpq");

const Tracer = zpq.trace.Tracer;
const BatchReader = zpq.core.batch_reader.BatchReader;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <parquet_file> [filter_col] [filter_val]\n", .{args[0]});
        std.debug.print("\nExample:\n", .{});
        std.debug.print("  {s} data/large.parquet status active\n", .{args[0]});
        return;
    }

    const path = args[1];
    const filter_col_name: ?[]const u8 = if (args.len > 2) args[2] else null;
    const filter_val: ?[]const u8 = if (args.len > 3) args[3] else null;

    // Open file
    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata orelse return error.NoMetadata;

    // Get file size
    const file_size: u64 = blk: {
        const stat = try std.fs.cwd().statFile(path);
        break :blk stat.size;
    };

    // Initialize tracer
    var tracer = Tracer.init(.{
        .timestamp_ms = zpq.trace.nowMs(),
        .file_path = path,
        .file_size_bytes = file_size,
        .num_columns = @intCast(meta.schema.items.len),
        .num_row_groups = @intCast(meta.row_groups.items.len),
        .total_rows = @intCast(meta.num_rows),
        .filter_column = filter_col_name,
        .filter_value = filter_val,
    });

    std.debug.print("Benchmarking: {s}\n", .{path});
    std.debug.print("  Rows: {d}, Row Groups: {d}, Columns: {d}\n", .{
        meta.num_rows,
        meta.row_groups.items.len,
        meta.schema.items.len,
    });
    if (filter_col_name) |col| {
        std.debug.print("  Filter: {s}={s}\n", .{ col, filter_val orelse "?" });
    }
    std.debug.print("\n", .{});

    // Overall timer
    var overall_timer = try std.time.Timer.start();

    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        // Row group skip check (metadata pruning)
        if (filter_col_name != null and filter_val != null) {
            if (pf.shouldSkipRowGroup(rg_idx, filter_col_name.?, filter_val.?)) {
                tracer.recordRowGroup(false);
                continue;
            }
        }
        tracer.recordRowGroup(true);

        var rg = try pf.rowGroup(rg_idx);
        defer rg.deinit();

        // Find filter column index
        var filter_col_idx: ?usize = null;
        if (filter_col_name) |name| {
            for (rg_meta.columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], name)) {
                        filter_col_idx = idx;
                        break;
                    }
                }
            }
        }

        if (filter_col_idx == null and filter_col_name != null) {
            std.debug.print("Warning: Filter column '{s}' not found\n", .{filter_col_name.?});
        }

        // Create readers for all columns
        const ReaderEntry = struct {
            reader: *BatchReader([]const u8),
            col_idx: usize,
        };
        var readers = std.ArrayListUnmanaged(ReaderEntry){};
        defer {
            for (readers.items) |entry| {
                entry.reader.deinit();
                allocator.destroy(entry.reader);
            }
            readers.deinit(allocator);
        }

        for (rg_meta.columns.items, 0..) |col, col_idx| {
            const md = col.meta_data orelse continue;

            // For simplicity, only handle BYTE_ARRAY columns in this benchmark
            if (md.type != .BYTE_ARRAY) continue;

            const levels = meta.getColumnLevels(md.path_in_schema.items);
            const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
            const type_length = if (schema_elem) |se| se.type_length else null;
            const col_reader = try rg.columnReader(col_idx);

            const r = try allocator.create(BatchReader([]const u8));
            r.* = BatchReader([]const u8).init(
                allocator,
                col_reader,
                md.type,
                @intCast(levels.max_def),
                @intCast(levels.max_rep),
                type_length,
            );
            try readers.append(allocator, .{ .reader = r, .col_idx = col_idx });
        }

        if (readers.items.len == 0) {
            std.debug.print("Warning: No BYTE_ARRAY columns found in row group {d}\n", .{rg_idx});
            continue;
        }

        // Find filter reader
        var filter_reader_idx: ?usize = null;
        if (filter_col_idx) |f_idx| {
            for (readers.items, 0..) |entry, i| {
                if (entry.col_idx == f_idx) {
                    filter_reader_idx = i;
                    break;
                }
            }
        }

        // Interleaved batch loop
        var row_idx: usize = 0;
        const num_rows: usize = @intCast(rg_meta.num_rows);

        while (row_idx < num_rows) {
            const batch_size = @min(1024, num_rows - row_idx);

            // --- FILTER DECODE ---
            var sel = zpq.core.simd.SelectionVector.init();
            var filter_timer = try std.time.Timer.start();
            var batch_rows: usize = batch_size;

            if (filter_reader_idx) |f_idx| {
                var filter_buf: [1024]?[]const u8 = undefined;
                const n = try readers.items[f_idx].reader.nextBatch(filter_buf[0..batch_size]);
                batch_rows = n;

                // Build selection vector
                for (filter_buf[0..n], 0..) |val, i| {
                    if (val) |v| {
                        if (filter_val) |fv| {
                            if (std.mem.eql(u8, v, fv)) {
                                sel.setBitIndices(i);
                            }
                        } else {
                            // No filter value = select all non-null
                            sel.setBitIndices(i);
                        }
                    }
                }

                tracer.recordFilterDecode(filter_timer.read(), n);
            } else {
                // No filter - select all
                for (0..batch_size) |i| {
                    sel.setBitIndices(i);
                }
                tracer.recordFilterDecode(filter_timer.read(), batch_size);
            }

            // Record row-level stats (once per batch, not per column)
            const selected_in_batch = sel.count();
            tracer.metrics.rows_selected += selected_in_batch;
            tracer.metrics.rows_skipped += batch_rows - selected_in_batch;

            // --- SKIP / MATERIALIZE OTHER COLUMNS ---
            for (readers.items, 0..) |entry, idx| {
                if (filter_reader_idx != null and idx == filter_reader_idx.?) continue;

                if (sel.count() > 0) {
                    // Materialize selected rows
                    var mat_timer = try std.time.Timer.start();
                    var buf: [1024]?[]const u8 = undefined;
                    _ = try entry.reader.nextBatchSelected(buf[0..sel.count()], &sel, batch_rows);
                    tracer.metrics.materialize_ns += mat_timer.read();
                    tracer.metrics.batches_processed += 1;
                } else {
                    // Skip entire batch
                    var skip_timer = try std.time.Timer.start();
                    try entry.reader.skip(batch_rows);
                    tracer.metrics.skip_ns += skip_timer.read();
                    tracer.metrics.batches_skipped += 1;
                }
            }

            row_idx += batch_size;
        }
    }

    const total_elapsed = overall_timer.read();

    // Calculate overhead (total - accounted time)
    const accounted = tracer.metrics.filter_decode_ns + tracer.metrics.skip_ns + tracer.metrics.materialize_ns;
    if (total_elapsed > accounted) {
        tracer.recordOverhead(total_elapsed - accounted);
    }

    // Print summary
    std.debug.print("\n=== Results ===\n", .{});
    std.debug.print("Total time:       {d:.2} ms\n", .{@as(f64, @floatFromInt(total_elapsed)) / 1_000_000.0});
    std.debug.print("Filter decode:    {d:.2} ms ({d:.1}%)\n", .{
        @as(f64, @floatFromInt(tracer.metrics.filter_decode_ns)) / 1_000_000.0,
        @as(f64, @floatFromInt(tracer.metrics.filter_decode_ns)) / @as(f64, @floatFromInt(total_elapsed)) * 100,
    });
    std.debug.print("Skip:             {d:.2} ms ({d:.1}%)\n", .{
        @as(f64, @floatFromInt(tracer.metrics.skip_ns)) / 1_000_000.0,
        @as(f64, @floatFromInt(tracer.metrics.skip_ns)) / @as(f64, @floatFromInt(total_elapsed)) * 100,
    });
    std.debug.print("Materialize:      {d:.2} ms ({d:.1}%)\n", .{
        @as(f64, @floatFromInt(tracer.metrics.materialize_ns)) / 1_000_000.0,
        @as(f64, @floatFromInt(tracer.metrics.materialize_ns)) / @as(f64, @floatFromInt(total_elapsed)) * 100,
    });
    std.debug.print("Overhead:         {d:.2} ms ({d:.1}%)\n", .{
        @as(f64, @floatFromInt(tracer.metrics.loop_overhead_ns)) / 1_000_000.0,
        @as(f64, @floatFromInt(tracer.metrics.loop_overhead_ns)) / @as(f64, @floatFromInt(total_elapsed)) * 100,
    });
    std.debug.print("\n", .{});
    std.debug.print("Rows scanned:     {d}\n", .{tracer.metrics.rows_scanned});
    std.debug.print("Rows selected:    {d} ({d:.2}%)\n", .{
        tracer.metrics.rows_selected,
        tracer.metrics.selectivity() * 100,
    });
    std.debug.print("Rows skipped:     {d}\n", .{tracer.metrics.rows_skipped});
    std.debug.print("Row groups:       {d} scanned, {d} skipped\n", .{
        tracer.metrics.row_groups_scanned,
        tracer.metrics.row_groups_skipped,
    });
    std.debug.print("Throughput:       {d:.2} MVal/s\n", .{tracer.metrics.throughputMValPerSec()});

    // Write JSON trace - handle running from probes/ directory
    const trace_path = "../bench/traces/latest.json";
    const alt_path = "latest_trace.json";

    tracer.writeToFile(trace_path) catch {
        // Try current directory as fallback
        tracer.writeToFile(alt_path) catch |err2| {
            std.debug.print("\nWarning: Could not write trace: {}\n", .{err2});
            return;
        };
        std.debug.print("\nTrace written to: {s}\n", .{alt_path});
        return;
    };
    std.debug.print("\nTrace written to: {s}\n", .{trace_path});
}
