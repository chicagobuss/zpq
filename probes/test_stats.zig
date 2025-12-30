const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const path = "../data/parquet-testing/data/data_index_bloom_encoding_stats.parquet";
    var pf = try zpq.file.ParquetFile.open(allocator, path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata.?;
    for (meta.row_groups.items, 0..) |rg, i| {
        std.debug.print("Row Group {d}:\n", .{i});
        for (rg.columns.items, 0..) |col, j| {
            if (col.meta_data) |md| {
                std.debug.print("  Column {d} statistics:\n", .{j});
                if (md.statistics) |stats| {
                    if (stats.min_value) |min| std.debug.print("    Min: {s}\n", .{min});
                    if (stats.max_value) |max| std.debug.print("    Max: {s}\n", .{max});
                    if (stats.null_count) |nc| std.debug.print("    Null count: {d}\n", .{nc});
                } else {
                    std.debug.print("    (No statistics found)\n", .{});
                }
            }
        }
    }
}

