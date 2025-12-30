//! Probe: Test ColumnIndex parsing and page skipping
//!
//! Purpose: Verify that column_index_offset is being read from parquet files
//! and that page-level statistics are accessible.
//!
//! Date: 2024-12-30
const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const path = if (args.len > 1) args[1] else "/tmp/multi_page_test.parquet";

    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    if (pf.metadata) |meta| {
        std.debug.print("File: {s}\n", .{path});
        std.debug.print("Row groups: {d}\n\n", .{meta.row_groups.items.len});
        
        for (meta.row_groups.items, 0..) |rg, rg_idx| {
            std.debug.print("RG[{d}]: {d} rows\n", .{rg_idx, rg.num_rows});
            
            for (rg.columns.items, 0..) |col, col_idx| {
                const name = if (col.meta_data) |md| blk: {
                    const parts = md.path_in_schema.items;
                    break :blk parts[parts.len - 1];
                } else "?";
                
                std.debug.print("  Col[{d}] {s}: ci_offset={?}, ci_len={?}\n", .{
                    col_idx,
                    name,
                    col.column_index_offset,
                    col.column_index_length,
                });
                
                // Try to read the ColumnIndex if present
                if (col.column_index_offset != null and col.column_index_length != null) {
                    var rg_reader = try pf.rowGroup(rg_idx);
                    defer rg_reader.deinit();
                    
                    if (try rg_reader.getColumnIndex(col_idx)) |ci| {
                        var column_index = ci;
                        defer column_index.deinit(allocator);
                        std.debug.print("    -> ColumnIndex: {d} pages\n", .{column_index.numPages()});
                        for (0..@min(5, column_index.numPages())) |i| {
                            std.debug.print("       Page[{d}]: null={}, min='{s}', max='{s}'\n", .{
                                i,
                                column_index.null_pages[i],
                                column_index.min_values[i],
                                column_index.max_values[i],
                            });
                        }
                        if (column_index.numPages() > 5) {
                            std.debug.print("       ... ({d} more pages)\n", .{column_index.numPages() - 5});
                        }
                    }
                }
            }
            std.debug.print("\n", .{});
        }
    }
}
