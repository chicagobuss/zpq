const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: test_page_index <parquet_file>\n", .{});
        return;
    }

    var pf = try zpq.file.ParquetFile.open(allocator, args[1]);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("File: {s}\n", .{args[1]});
        std.debug.print("Row Groups: {d}\n\n", .{meta.row_groups.items.len});

        var total_with_index: usize = 0;
        var total_columns: usize = 0;

        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            std.debug.print("Row Group {d}:\n", .{rg_idx});

            var rg = try pf.rowGroup(rg_idx);
            defer rg.deinit();

            for (rg_meta.columns.items, 0..) |col, col_idx| {
                if (col.meta_data) |md| {
                    total_columns += 1;
                    const path = md.path_in_schema.items;
                    const name = path[path.len - 1];

                    const has_col_idx = col.column_index_offset != null;
                    const has_off_idx = col.offset_index_offset != null;

                    if (has_col_idx) total_with_index += 1;

                    if (has_col_idx or has_off_idx) {
                        std.debug.print("  [{d}] {s}: ColumnIndex={} OffsetIndex={}\n", .{
                            col_idx, name, has_col_idx, has_off_idx,
                        });

                        if (has_col_idx) {
                            if (try rg.getColumnIndex(col_idx)) |idx| {
                                var col_index = idx;
                                defer col_index.deinit(allocator);
                                std.debug.print("       Pages: {d}, BoundaryOrder: {any}\n", .{
                                    col_index.numPages(), col_index.boundary_order,
                                });

                                // Show first page min/max for string columns
                                if (col_index.numPages() > 0 and md.type == .BYTE_ARRAY) {
                                    const min = col_index.min_values[0];
                                    const max = col_index.max_values[0];
                                    std.debug.print("       Page 0: min=\"{s}\" max=\"{s}\"\n", .{ min, max });
                                }
                            }
                        }
                    }
                }
            }
        }

        std.debug.print("\nSummary: {d}/{d} columns have ColumnIndex ({d:.1}%)\n", .{
            total_with_index,
            total_columns,
            if (total_columns > 0) @as(f64, @floatFromInt(total_with_index)) / @as(f64, @floatFromInt(total_columns)) * 100.0 else 0.0,
        });
    }
}
