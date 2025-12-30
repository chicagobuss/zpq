const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const file_path = if (args.len > 1) args[1] else "data/parquet-testing/data/alltypes_tiny_pages.parquet";
    const col_name = if (args.len > 2) args[2] else "date_string_col";
    const filter_val = if (args.len > 3) args[3] else "01/13/09";

    var pf = try zpq.file.ParquetFile.open(allocator, file_path);
    defer pf.deinit();
    try pf.readFooter();

    const meta = pf.metadata.?;

    // Find column index
    var col_idx: ?usize = null;
    for (meta.row_groups.items[0].columns.items, 0..) |col, i| {
        if (col.meta_data) |md| {
            const path = md.path_in_schema.items;
            if (std.mem.eql(u8, path[path.len - 1], col_name)) {
                col_idx = i;
                break;
            }
        }
    }

    if (col_idx == null) {
        std.debug.print("Column '{s}' not found\n", .{col_name});
        return;
    }

    std.debug.print("File: {s}\nColumn: {s} (idx={d})\nFilter: \"{s}\"\n\n", .{ file_path, col_name, col_idx.?, filter_val });

    var rg = try pf.rowGroup(0);
    defer rg.deinit();

    if (try rg.getColumnIndex(col_idx.?)) |col_index| {
        var ci = col_index;
        defer ci.deinit(allocator);

        std.debug.print("ColumnIndex pages: {d}\n", .{ci.numPages()});
        std.debug.print("null_pages.len: {d}\n", .{ci.null_pages.len});
        std.debug.print("min_values.len: {d}\n", .{ci.min_values.len});
        std.debug.print("max_values.len: {d}\n", .{ci.max_values.len});

        if (ci.min_values.len > 0) {
            std.debug.print("Sample page min/max values:\n", .{});
            for (0..@min(5, ci.numPages())) |i| {
                std.debug.print("  Page {d}: min=\"{s}\" max=\"{s}\" null={}\n", .{
                    i, ci.min_values[i], ci.max_values[i], ci.null_pages[i],
                });
            }
        }
        std.debug.print("\n", .{});

        var skip_count: usize = 0;
        var match_count: usize = 0;

        for (0..ci.numPages()) |i| {
            const might_contain = ci.mightContainString(i, filter_val);
            if (!might_contain) {
                skip_count += 1;
            } else {
                match_count += 1;
                if (match_count <= 10) {
                    std.debug.print("Page {d}: min=\"{s}\" max=\"{s}\" -> MIGHT contain\n", .{
                        i, ci.min_values[i], ci.max_values[i],
                    });
                }
            }
        }

        if (match_count > 10) {
            std.debug.print("... and {d} more pages might contain\n", .{match_count - 10});
        }

        std.debug.print("\nSummary: skip={d}, might_match={d}\n", .{ skip_count, match_count });
    } else {
        std.debug.print("No ColumnIndex available\n", .{});
    }
}
