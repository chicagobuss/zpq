const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "data/deranged.parquet";
    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata.?;
    std.debug.print("Testing Lazy Materialization on {s} ({d} rows)\n", .{ path, meta.num_rows });

    // Columns of interest:
    // 0: sparse_int (INT64, 99% nulls)
    // 1: bloat_string (BYTE_ARRAY, large strings)
    
    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        std.debug.print("\nRow Group {d} ({d} rows):\n", .{ rg_idx, rg_meta.num_rows });
        
        var rg_reader = try pf.rowGroup(rg_idx);
        defer rg_reader.deinit();

        const filter_col_idx = 0; // sparse_int
        const target_col_idx = 1; // bloat_string
        
        const filter_md = rg_meta.columns.items[filter_col_idx].meta_data.?;
        const target_md = rg_meta.columns.items[target_col_idx].meta_data.?;
        
        const filter_levels = meta.getColumnLevels(filter_md.path_in_schema.items);
        const target_levels = meta.getColumnLevels(target_md.path_in_schema.items);
        
        const filter_schema = meta.getColumnSchema(filter_md.path_in_schema.items).?;
        const target_schema = meta.getColumnSchema(target_md.path_in_schema.items).?;

        const filter_col_reader = try rg_reader.columnReader(filter_col_idx);
        const target_col_reader = try rg_reader.columnReader(target_col_idx);
        
        var filter_reader = zpq.core.batch_reader.BatchReader(i64).init(allocator, filter_col_reader, filter_md.type, @intCast(filter_levels.max_def), @intCast(filter_levels.max_rep), filter_schema.type_length);
        defer filter_reader.deinit();
        
        var target_reader = zpq.core.batch_reader.BatchReader([]const u8).init(allocator, target_col_reader, target_md.type, @intCast(target_levels.max_def), @intCast(target_levels.max_rep), target_schema.type_length);
        defer target_reader.deinit();

        var row_idx: usize = 0;
        var selected_count: usize = 0;
        var total_skipped: usize = 0;

        var filter_buf: [1024]?i64 = undefined;
        var target_buf: [1024]?[]const u8 = undefined;

        var timer = try std.time.Timer.start();

        while (row_idx < @as(usize, @intCast(rg_meta.num_rows))) {
            const batch_size = @min(1024, @as(usize, @intCast(rg_meta.num_rows)) - row_idx);
            
            // 1. Read Filter Column
            const n = try filter_reader.nextBatch(filter_buf[0..batch_size]);
            if (n == 0) break;

            // 2. Build Selection Vector (filter: sparse_int is NOT NULL)
            var sel = zpq.core.simd.SelectionVector.init();
            for (filter_buf[0..n], 0..) |val, i| {
                if (val != null) {
                    sel.setBitIndices(i);
                }
            }

            // 3. Lazy Materialization of Target Column
            if (sel.count() > 0) {
                const materialized = try target_reader.nextBatchSelected(target_buf[0..sel.count()], &sel, n);
                selected_count += materialized;
                
                // Sample output
                if (selected_count < 5) {
                    for (0..materialized) |i| {
                        std.debug.print("  Selected row {d}: {s}...\n", .{ row_idx + i, target_buf[i].?[0..@min(20, target_buf[i].?.len)] });
                    }
                }
            } else {
                try target_reader.skip(n);
                total_skipped += n;
            }

            row_idx += n;
        }

        const elapsed = timer.read();
        std.debug.print("Processed {d} rows in {d:.2}ms\n", .{ row_idx, @as(f64, @floatFromInt(elapsed)) / 1_000_000.0 });
        std.debug.print("Selected: {d}, Skipped: {d}\n", .{ selected_count, total_skipped });
    }
}

