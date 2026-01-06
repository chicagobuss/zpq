const std = @import("std");
const file_mod = @import("../src/zpq/core/file.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const path = if (args.len > 1) args[1] else "/tmp/test_with_indexes.parquet";

    std.debug.print("Checking: {s}\n", .{path});

    var pf = try file_mod.ParquetFile.openMmap(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata.?;
    std.debug.print("Row groups: {d}\n", .{meta.row_groups.items.len});

    for (meta.row_groups.items, 0..) |rg, rg_idx| {
        std.debug.print("\nRow group {d} ({d} rows):\n", .{ rg_idx, rg.num_rows });
        for (rg.columns.items[0..@min(3, rg.columns.items.len)], 0..) |col, col_idx| {
            const md = col.meta_data.?;
            const name = md.path_in_schema.items[md.path_in_schema.items.len - 1];
            std.debug.print("  Column {d} ({s}):\n", .{ col_idx, name });
            std.debug.print("    column_index_offset: {?}\n", .{col.column_index_offset});
            std.debug.print("    column_index_length: {?}\n", .{col.column_index_length});
            std.debug.print("    offset_index_offset: {?}\n", .{col.offset_index_offset});
            std.debug.print("    offset_index_length: {?}\n", .{col.offset_index_length});

            // Try to read the actual ColumnIndex if present
            if (col.column_index_offset != null) {
                var rg_reader = try file_mod.RowGroupReader.init(&pf, rg, allocator);
                defer rg_reader.deinit();
                if (try rg_reader.getColumnIndex(col_idx)) |ci| {
                    var ci_mut = ci;
                    defer ci_mut.deinit(allocator);
                    std.debug.print("    ColumnIndex: {d} pages\n", .{ci.numPages()});
                }
            }
        }
    }
}
