const std = @import("std");
const zpq = @import("zpq");
const ParquetFile = zpq.file.ParquetFile;

/// S3/R2 Column Projection Benchmark
/// Tests ZPQ's network-first design with column projection over HTTPS

pub const std_options = std.Options{
    .log_level = .warn,
};

const BenchResult = struct {
    open_ns: u64,
    footer_ns: u64,
    prefetch_ns: u64,
    decode_ns: u64,
    total_ns: u64,
    values_decoded: usize,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse environment
    const host = std.process.getEnvVarOwned(allocator, "S3_HOST") catch |err| {
        if (err == error.EnvironmentVariableNotFound) {
            std.debug.print("Error: S3_HOST environment variable required\n", .{});
            std.debug.print("Example: S3_HOST=<account>.r2.cloudflarestorage.com\n", .{});
            return;
        }
        return err;
    };
    defer allocator.free(host);

    const bucket = std.process.getEnvVarOwned(allocator, "S3_BUCKET") catch |err| {
        if (err == error.EnvironmentVariableNotFound) {
            std.debug.print("Error: S3_BUCKET environment variable required\n", .{});
            return;
        }
        return err;
    };
    defer allocator.free(bucket);

    const key = std.process.getEnvVarOwned(allocator, "S3_KEY") catch |err| {
        if (err == error.EnvironmentVariableNotFound) {
            std.debug.print("Error: S3_KEY environment variable required\n", .{});
            return;
        }
        return err;
    };
    defer allocator.free(key);

    const region = std.process.getEnvVarOwned(allocator, "S3_REGION") catch |err| blk: {
        if (err == error.EnvironmentVariableNotFound) break :blk try allocator.dupe(u8, "auto") else return err;
    };
    defer allocator.free(region);

    const num_cols: usize = blk: {
        const cols_str = std.process.getEnvVarOwned(allocator, "NUM_COLS") catch |err| {
            if (err == error.EnvironmentVariableNotFound) break :blk 3;
            return err;
        };
        defer allocator.free(cols_str);
        break :blk std.fmt.parseInt(usize, cols_str, 10) catch 3;
    };

    const iterations: usize = blk: {
        const iter_str = std.process.getEnvVarOwned(allocator, "ITERATIONS") catch |err| {
            if (err == error.EnvironmentVariableNotFound) break :blk 3;
            return err;
        };
        defer allocator.free(iter_str);
        break :blk std.fmt.parseInt(usize, iter_str, 10) catch 3;
    };

    std.debug.print("S3/R2 Column Projection Benchmark\n", .{});
    std.debug.print("==================================\n", .{});
    std.debug.print("Host: {s}\n", .{host});
    std.debug.print("Bucket: {s}\n", .{bucket});
    std.debug.print("Key: {s}\n", .{key});
    std.debug.print("Columns: {d}\n", .{num_cols});
    std.debug.print("Iterations: {d}\n\n", .{iterations});

    var results = std.ArrayListUnmanaged(BenchResult){};
    defer results.deinit(allocator);

    for (0..iterations) |iter| {
        const result = try runBenchmark(allocator, host, bucket, key, region, num_cols);
        try results.append(allocator, result);

        std.debug.print("Run {d}: total={d:.2}ms (open={d:.2}ms, footer={d:.2}ms, prefetch={d:.2}ms, decode={d:.2}ms) values={d}\n", .{
            iter + 1,
            @as(f64, @floatFromInt(result.total_ns)) / 1e6,
            @as(f64, @floatFromInt(result.open_ns)) / 1e6,
            @as(f64, @floatFromInt(result.footer_ns)) / 1e6,
            @as(f64, @floatFromInt(result.prefetch_ns)) / 1e6,
            @as(f64, @floatFromInt(result.decode_ns)) / 1e6,
            result.values_decoded,
        });
    }

    // Calculate statistics
    var min_total: u64 = std.math.maxInt(u64);
    var max_total: u64 = 0;
    var sum_total: u64 = 0;
    var sum_open: u64 = 0;
    var sum_footer: u64 = 0;
    var sum_prefetch: u64 = 0;
    var sum_decode: u64 = 0;

    for (results.items) |r| {
        min_total = @min(min_total, r.total_ns);
        max_total = @max(max_total, r.total_ns);
        sum_total += r.total_ns;
        sum_open += r.open_ns;
        sum_footer += r.footer_ns;
        sum_prefetch += r.prefetch_ns;
        sum_decode += r.decode_ns;
    }

    const n = results.items.len;
    std.debug.print("\n--- Summary ({d} runs) ---\n", .{n});
    std.debug.print("Total:    min={d:.2}ms  max={d:.2}ms  avg={d:.2}ms\n", .{
        @as(f64, @floatFromInt(min_total)) / 1e6,
        @as(f64, @floatFromInt(max_total)) / 1e6,
        @as(f64, @floatFromInt(sum_total / n)) / 1e6,
    });
    std.debug.print("Breakdown (avg):\n", .{});
    std.debug.print("  Open (HEAD):     {d:7.2}ms\n", .{@as(f64, @floatFromInt(sum_open / n)) / 1e6});
    std.debug.print("  Footer (GET):    {d:7.2}ms\n", .{@as(f64, @floatFromInt(sum_footer / n)) / 1e6});
    std.debug.print("  Prefetch (GETs): {d:7.2}ms\n", .{@as(f64, @floatFromInt(sum_prefetch / n)) / 1e6});
    std.debug.print("  Decode:          {d:7.2}ms\n", .{@as(f64, @floatFromInt(sum_decode / n)) / 1e6});
}

fn runBenchmark(
    allocator: std.mem.Allocator,
    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    region: []const u8,
    num_cols: usize,
) !BenchResult {
    var timer = try std.time.Timer.start();

    // Create XevS3Source
    const s3_source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, region, true, 443);

    // Set credentials if available (before passing ownership)
    // Try S3_ACCESS_KEY first, fall back to AWS_ACCESS_KEY_ID
    const ak_result = std.process.getEnvVarOwned(allocator, "S3_ACCESS_KEY") catch |err| blk: {
        if (err == error.EnvironmentVariableNotFound) {
            break :blk std.process.getEnvVarOwned(allocator, "AWS_ACCESS_KEY_ID") catch null;
        }
        return err;
    };
    if (ak_result) |ak| {
        defer allocator.free(ak);
        const sk_result = std.process.getEnvVarOwned(allocator, "S3_SECRET_KEY") catch |err| blk: {
            if (err == error.EnvironmentVariableNotFound) {
                break :blk std.process.getEnvVarOwned(allocator, "AWS_SECRET_ACCESS_KEY") catch null;
            }
            return err;
        };
        if (sk_result) |sk| {
            defer allocator.free(sk);
            const st = std.process.getEnvVarOwned(allocator, "AWS_SESSION_TOKEN") catch |err| blk: {
                if (err == error.EnvironmentVariableNotFound) break :blk null else return err;
            };
            defer if (st) |token| allocator.free(token);

            s3_source.setCredentials(ak, sk, st) catch |err| {
                s3_source.deinit();
                allocator.destroy(s3_source);
                return err;
            };
        }
    }

    // Open S3 file (HEAD request for size) - takes ownership of s3_source
    var file = ParquetFile.openS3(allocator, s3_source) catch |err| {
        s3_source.deinit();
        allocator.destroy(s3_source);
        return err;
    };
    defer file.deinit();

    const open_ns = timer.read();

    // Read footer (GET last 8 bytes + metadata)
    try file.readFooter();
    const footer_ns = timer.read() - open_ns;

    const meta = file.metadata.?;
    const total_cols = meta.row_groups.items[0].columns.items.len;
    const cols_to_read = if (num_cols == 0) total_cols else @min(num_cols, total_cols);

    // Build column indices
    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();
    const aa = arena.allocator();

    var col_indices = try aa.alloc(usize, cols_to_read);
    for (0..cols_to_read) |idx| {
        col_indices[idx] = idx;
    }

    // Prefetch selected columns (parallel range GETs)
    var prefetch_ns: u64 = 0;
    var decode_ns: u64 = 0;
    var values_decoded: usize = 0;

    for (meta.row_groups.items, 0..) |rg, rg_idx| {
        var rg_reader = try file.rowGroup(rg_idx);
        defer rg_reader.deinit();

        const prefetch_start = timer.read();
        try rg_reader.prefetch(col_indices);
        prefetch_ns += timer.read() - prefetch_start;

        // Decode values
        const decode_start = timer.read();
        for (col_indices) |col_idx| {
            const col = rg.columns.items[col_idx];
            const md = col.meta_data orelse continue;

            // Dictionary storage
            var dict_strings = std.ArrayListUnmanaged([]const u8){};
            var dict_int32 = std.ArrayListUnmanaged(i32){};
            var dict_int64 = std.ArrayListUnmanaged(i64){};
            var dict_double = std.ArrayListUnmanaged(f64){};
            var dict_float = std.ArrayListUnmanaged(f32){};

            const levels = meta.getColumnLevels(md.path_in_schema.items);

            var reader = try rg_reader.columnReader(col_idx);
            while (try reader.next(aa)) |page| {
                var p = page;

                if (p.header.type == .DICTIONARY_PAGE) {
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

                        // Skip rep levels
                        if (levels.max_rep > 0) {
                            if (data_slice.len < 4) continue;
                            const len = std.mem.readInt(u32, data_slice[0..4], .little);
                            if (data_slice.len < 4 + len) continue;
                            data_slice = data_slice[4 + len ..];
                        }

                        // Skip def levels
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
                                    // Dictionary lookup
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
        decode_ns += timer.read() - decode_start;
    }

    const total_ns = timer.read();

    return BenchResult{
        .open_ns = open_ns,
        .footer_ns = footer_ns,
        .prefetch_ns = prefetch_ns,
        .decode_ns = decode_ns,
        .total_ns = total_ns,
        .values_decoded = values_decoded,
    };
}
