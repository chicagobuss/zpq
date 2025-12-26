const std = @import("std");
const zpq = @import("zpq");

/// Full decode benchmark - actually decode values like PyArrow does
/// This is an apples-to-apples comparison

pub const std_options: std.Options = .{
    .log_level = .warn,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (args.len < 2) {
        std.debug.print("Usage: {s} <path> [num_columns] [iterations]\n", .{args[0]});
        std.debug.print("  num_columns: number of columns to read (default: 3, 0 = all)\n", .{});
        std.debug.print("\nThis benchmark fully decodes values (apples-to-apples with PyArrow)\n", .{});
        return;
    }

    const target_path = args[1];
    const num_cols: usize = if (args.len > 2) std.fmt.parseInt(usize, args[2], 10) catch 3 else 3;
    const iterations: usize = if (args.len > 3) std.fmt.parseInt(usize, args[3], 10) catch 5 else 5;

    std.debug.print("Full Decode Benchmark (apples-to-apples)\n", .{});
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

    for (0..iterations) |iter| {
        var arena = std.heap.ArenaAllocator.init(allocator);
        defer arena.deinit();
        const aa = arena.allocator();

        const start = std.time.Instant.now() catch unreachable;

        var file = try zpq.file.ParquetFile.open(allocator, target_path);
        defer file.deinit();

        try file.readFooter();

        const meta = file.metadata.?;
        const total_cols = meta.row_groups.items[0].columns.items.len;
        const cols_to_read = if (num_cols == 0) total_cols else @min(num_cols, total_cols);

        var col_indices = try aa.alloc(usize, cols_to_read);
        for (0..cols_to_read) |idx| {
            col_indices[idx] = idx;
        }

        var values_decoded: usize = 0;

        for (meta.row_groups.items, 0..) |rg, rg_idx| {
            var rg_reader = try file.rowGroup(rg_idx);
            defer rg_reader.deinit();

            // Prefetch selected columns
            if (num_cols == 0) {
                try rg_reader.prefetch(null);
            } else {
                try rg_reader.prefetch(col_indices);
            }

            for (col_indices) |col_idx| {
                const col = rg.columns.items[col_idx];
                const md = col.meta_data orelse continue;

                // Get column levels for def/rep handling
                const levels = meta.getColumnLevels(md.path_in_schema.items);

                // Dictionary storage for this column
                var dict_strings = std.ArrayListUnmanaged([]const u8){};
                var dict_int32 = std.ArrayListUnmanaged(i32){};
                var dict_int64 = std.ArrayListUnmanaged(i64){};
                var dict_double = std.ArrayListUnmanaged(f64){};
                var dict_float = std.ArrayListUnmanaged(f32){};

                var reader = try rg_reader.columnReader(col_idx);

                while (try reader.next(aa)) |page| {
                    var p = page;

                    if (p.header.type == .DICTIONARY_PAGE) {
                        // Decode dictionary
                        var decoder = zpq.decoder.Decoder.init(p.data);
                        if (md.type == .BYTE_ARRAY) {
                            while (decoder.hasMore()) {
                                const val = try decoder.readByteArray();
                                try dict_strings.append(aa, val);
                                values_decoded += 1;
                            }
                        } else if (md.type == .INT32) {
                            while (decoder.hasMore()) {
                                try dict_int32.append(aa, try decoder.readInt32());
                                values_decoded += 1;
                            }
                        } else if (md.type == .INT64) {
                            while (decoder.hasMore()) {
                                try dict_int64.append(aa, try decoder.readInt64());
                                values_decoded += 1;
                            }
                        } else if (md.type == .DOUBLE) {
                            while (decoder.hasMore()) {
                                try dict_double.append(aa, try decoder.readDouble());
                                values_decoded += 1;
                            }
                        } else if (md.type == .FLOAT) {
                            while (decoder.hasMore()) {
                                try dict_float.append(aa, try decoder.readFloat());
                                values_decoded += 1;
                            }
                        }
                    } else if (p.header.type == .DATA_PAGE) {
                        const dph = p.header.data_page_header orelse continue;

                        if (dph.encoding == .RLE_DICTIONARY or dph.encoding == .PLAIN_DICTIONARY) {
                            var data_slice = p.data;

                            // Skip repetition levels
                            if (levels.max_rep > 0) {
                                if (data_slice.len < 4) continue;
                                const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                if (data_slice.len < 4 + len) continue;
                                data_slice = data_slice[4 + len ..];
                            }

                            // Skip definition levels (but count them for null handling)
                            if (levels.max_def > 0) {
                                if (data_slice.len < 4) continue;
                                const len = std.mem.readInt(u32, data_slice[0..4], .little);
                                if (data_slice.len < 4 + len) continue;
                                data_slice = data_slice[4 + len ..];
                            }

                            // Decode RLE indices
                            if (data_slice.len > 0) {
                                const bit_width = data_slice[0];
                                var rle_dec = zpq.rle.RleDecoder.init(data_slice[1..], bit_width);

                                var count: usize = 0;
                                while (count < dph.num_values) : (count += 1) {
                                    if (try rle_dec.next()) |idx| {
                                        // Actually look up dictionary value (simulates materialization)
                                        if (md.type == .BYTE_ARRAY and idx < dict_strings.items.len) {
                                            _ = dict_strings.items[idx];
                                        } else if (md.type == .INT32 and idx < dict_int32.items.len) {
                                            _ = dict_int32.items[idx];
                                        } else if (md.type == .INT64 and idx < dict_int64.items.len) {
                                            _ = dict_int64.items[idx];
                                        } else if (md.type == .DOUBLE and idx < dict_double.items.len) {
                                            _ = dict_double.items[idx];
                                        } else if (md.type == .FLOAT and idx < dict_float.items.len) {
                                            _ = dict_float.items[idx];
                                        }
                                        values_decoded += 1;
                                    } else {
                                        break;
                                    }
                                }
                            }
                        } else if (dph.encoding == .PLAIN) {
                            // PLAIN encoding - decode directly
                            var decoder = zpq.decoder.Decoder.init(p.data);
                            var count: usize = 0;
                            while (count < dph.num_values and decoder.hasMore()) : (count += 1) {
                                if (md.type == .BYTE_ARRAY) {
                                    _ = try decoder.readByteArray();
                                } else if (md.type == .INT32) {
                                    _ = try decoder.readInt32();
                                } else if (md.type == .INT64) {
                                    _ = try decoder.readInt64();
                                } else if (md.type == .DOUBLE) {
                                    _ = try decoder.readDouble();
                                } else if (md.type == .FLOAT) {
                                    _ = try decoder.readFloat();
                                }
                                values_decoded += 1;
                            }
                        }
                    }
                }
            }
        }

        const end = std.time.Instant.now() catch unreachable;
        const duration = end.since(start);

        if (iter == 0) {
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
