const std = @import("std");
const zpq = @import("zpq");
const build_options = @import("build_options");

pub const std_options: std.Options = .{
    .log_level = .warn,
};

/// Build hash for reproducibility
const git_hash: []const u8 = build_options.git_hash;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <path> [num_columns] [iterations]\n", .{args[0]});
        std.debug.print("  num_columns: number of columns to read (default: 3, 0 = all)\n", .{});
        return;
    }

    const target_path = args[1];
    const num_cols: usize = if (args.len > 2) std.fmt.parseInt(usize, args[2], 10) catch 3 else 3;
    const iterations: usize = if (args.len > 3) std.fmt.parseInt(usize, args[3], 10) catch 5 else 5;

    std.debug.print("Column Projection Benchmark [{s}]\n", .{git_hash});
    std.debug.print("Target: {s}\n", .{target_path});
    if (num_cols == 0) {
        std.debug.print("Columns: all\n", .{});
    } else {
        std.debug.print("Columns: {d}\n", .{num_cols});
    }
    std.debug.print("Iterations: {d}\n", .{iterations});

    var total_duration_ns: u64 = 0;
    var min_duration_ns: u64 = std.math.maxInt(u64);
    var max_duration_ns: u64 = 0;

    for (0..iterations) |i| {
        const start = std.time.Instant.now() catch unreachable;

        var file = try zpq.file.ParquetFile.open(allocator, target_path);
        defer file.deinit();

        try file.readFooter();

        var values_count: usize = 0;
        var iter_arena = std.heap.ArenaAllocator.init(allocator);
        defer iter_arena.deinit();
        const aa = iter_arena.allocator();

        // Build column indices for projection
        const total_cols = file.metadata.?.row_groups.items[0].columns.items.len;
        const cols_to_read = if (num_cols == 0) total_cols else @min(num_cols, total_cols);

        var col_indices = try allocator.alloc(usize, cols_to_read);
        defer allocator.free(col_indices);
        for (0..cols_to_read) |idx| {
            col_indices[idx] = idx;
        }

        for (file.metadata.?.row_groups.items, 0..) |_, rg_idx| {
            var rg_reader = try file.rowGroup(rg_idx);
            defer rg_reader.deinit();

            // Prefetch only requested columns
            if (num_cols == 0) {
                try rg_reader.prefetch(null); // all columns
            } else {
                try rg_reader.prefetch(col_indices);
            }

            // Read only the requested columns
            for (col_indices) |col_idx| {
                var reader = try rg_reader.columnReader(col_idx);

                while (try reader.next(aa)) |page_val| {
                    var page = page_val;
                    if (page.header.data_page_header) |dph| {
                        values_count += @intCast(dph.num_values);
                    }
                }
            }
        }

        const end = std.time.Instant.now() catch unreachable;
        const duration = end.since(start);

        if (i == 0) {
            std.debug.print("  Total columns in file: {d}\n", .{total_cols});
            std.debug.print("  Reading {d} columns\n", .{cols_to_read});
        }

        total_duration_ns += duration;
        min_duration_ns = @min(min_duration_ns, duration);
        max_duration_ns = @max(max_duration_ns, duration);
    }

    const avg_ns = total_duration_ns / iterations;
    std.debug.print("\n--- Results ({d} runs) ---\n", .{iterations});
    std.debug.print("Min: {d:.2}ms\n", .{@as(f64, @floatFromInt(min_duration_ns)) / 1_000_000.0});
    std.debug.print("Max: {d:.2}ms\n", .{@as(f64, @floatFromInt(max_duration_ns)) / 1_000_000.0});
    std.debug.print("Avg: {d:.2}ms\n", .{@as(f64, @floatFromInt(avg_ns)) / 1_000_000.0});
}
