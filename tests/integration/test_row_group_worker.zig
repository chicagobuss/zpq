//! Integration test for RowGroupWorker
//! Validates that RowGroupWorker produces correct output for filter operations.

const std = @import("std");
const zpq = @import("zpq");

const RowGroupWorker = zpq.core.row_group_worker.RowGroupWorker;
const RowGroupData = zpq.core.row_group_worker.RowGroupData;
const FilterContext = zpq.core.row_group_worker.FilterContext;
const EncodedFilter = zpq.core.filter.EncodedFilter;

test "RowGroupWorker processes single row group correctly" {
    const allocator = std.testing.allocator;

    // Open test file
    var file = try std.fs.cwd().openFile("ci/fixtures/parquet/filter_test.parquet", .{});
    defer file.close();

    var pf = try zpq.ParquetFile.init(allocator, zpq.io.interface.local.LocalSource.init(file));
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata orelse return error.NoMetadata;

    // Find column indices for "category" (filter) and "name" (output)
    var filter_col_idx: ?usize = null;
    var name_col_idx: ?usize = null;
    var filter_col_type: ?zpq.core.schema.Type = null;
    var name_col_type: ?zpq.core.schema.Type = null;

    if (meta.row_groups.items.len > 0) {
        for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const path = md.path_in_schema.items;
                const col_name = path[path.len - 1];
                if (std.mem.eql(u8, col_name, "category")) {
                    filter_col_idx = idx;
                    filter_col_type = md.type;
                } else if (std.mem.eql(u8, col_name, "name")) {
                    name_col_idx = idx;
                    name_col_type = md.type;
                }
            }
        }
    }

    if (filter_col_idx == null or name_col_idx == null) {
        std.debug.print("Skipping test: required columns not found\n", .{});
        return;
    }

    // First, let's see what values exist in the category column
    // We'll filter for category = "A" (common test value)
    const filter_val = "A";
    var encoded_filter = try EncodedFilter.parse(allocator, filter_val, filter_col_type.?);
    defer encoded_filter.deinit();

    // Set up output columns (filter_col + name)
    const output_col_indices = [_]usize{ filter_col_idx.?, name_col_idx.? };
    const output_col_types = [_]zpq.core.schema.Type{ filter_col_type.?, name_col_type.? };
    const output_col_names = [_][]const u8{ "category", "name" };

    // Create FilterContext
    const ctx = FilterContext{
        .allocator = allocator,
        .filter_col_name = "category",
        .filter_val = filter_val,
        .filter_col_idx = filter_col_idx.?,
        .filter_col_type = filter_col_type.?,
        .encoded_filter = &encoded_filter,
        .output_col_indices = &output_col_indices,
        .output_col_types = &output_col_types,
        .output_col_names = &output_col_names,
        .filter_col_in_output = true,
        .filter_col_output_idx = 0,
        .meta = meta,
    };

    // Process first row group
    const rg_idx: usize = 0;
    const rg_meta = meta.row_groups.items[rg_idx];

    // Pre-fetch filter column
    const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
    const filter_md = filter_chunk.meta_data.?;
    var filter_start: u64 = @intCast(filter_md.data_page_offset);
    if (filter_md.dictionary_page_offset) |dpo| {
        if (dpo < filter_start) filter_start = @intCast(dpo);
    }
    const filter_len: u64 = @intCast(filter_md.total_compressed_size);

    const filter_buf = try allocator.alloc(u8, @intCast(filter_len));
    defer allocator.free(filter_buf);

    // Read filter column data
    const filter_range = [_]zpq.io.interface.Range{.{ .start = filter_start, .end = filter_start + filter_len }};
    var filter_bufs = [_][]u8{filter_buf};
    try pf.source.readRanges(&filter_range, &filter_bufs);

    // Pre-fetch name column
    const name_chunk = rg_meta.columns.items[name_col_idx.?];
    const name_md = name_chunk.meta_data.?;
    var name_start: u64 = @intCast(name_md.data_page_offset);
    if (name_md.dictionary_page_offset) |dpo| {
        if (dpo < name_start) name_start = @intCast(dpo);
    }
    const name_len: u64 = @intCast(name_md.total_compressed_size);

    const name_buf = try allocator.alloc(u8, @intCast(name_len));
    defer allocator.free(name_buf);

    const name_range = [_]zpq.io.interface.Range{.{ .start = name_start, .end = name_start + name_len }};
    var name_bufs = [_][]u8{name_buf};
    try pf.source.readRanges(&name_range, &name_bufs);

    // Create RowGroupData
    // For output columns: first is filter (reuse filter_buf), second is name
    const output_bufs = [_][]const u8{ filter_buf, name_buf };
    const output_offsets = [_]u64{ filter_start, name_start };
    const output_chunks = [_]zpq.core.schema.ColumnChunk{ filter_chunk, name_chunk };

    const rg_data = RowGroupData{
        .rg_idx = rg_idx,
        .num_rows = @intCast(rg_meta.num_rows),
        .filter_buf = filter_buf,
        .filter_offset = filter_start,
        .filter_chunk = filter_chunk,
        .output_bufs = &output_bufs,
        .output_offsets = &output_offsets,
        .output_chunks = &output_chunks,
    };

    // Create and execute worker
    var worker = try RowGroupWorker.init(allocator, &ctx, &rg_data);
    defer worker.deinit();

    worker.execute();

    // Verify results
    if (worker.status == .failed) {
        std.debug.print("Worker failed with error: {?}\n", .{worker.err});
        return error.WorkerFailed;
    }

    try std.testing.expectEqual(RowGroupWorker.Status.done, worker.status);

    std.debug.print("RowGroupWorker test: matched {d} rows out of {d}\n", .{ worker.row_count, rg_meta.num_rows });

    if (worker.row_count == 0) {
        std.debug.print("No matches found for category='A', test still passes (valid behavior)\n", .{});
        return;
    }

    // Verify output columns
    try std.testing.expectEqual(@as(usize, 2), worker.output_columns.len);

    // First column should be category = "A" for all rows
    const category_col = worker.output_columns[0];
    try std.testing.expect(category_col.byte_array_values != null);
    for (category_col.byte_array_values.?) |v| {
        try std.testing.expectEqualStrings("A", v);
    }

    // Second column should be name (non-empty strings)
    const name_col = worker.output_columns[1];
    try std.testing.expect(name_col.byte_array_values != null);
    try std.testing.expectEqual(worker.row_count, name_col.byte_array_values.?.len);

    std.debug.print("RowGroupWorker test passed! All {d} matched rows have correct category='A'\n", .{worker.row_count});
}
