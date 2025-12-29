const std = @import("std");
const zpq = @import("zpq");
const ParquetFile = zpq.file.ParquetFile;
const ParquetWriter = zpq.core.writer.ParquetWriter;
const rle = zpq.rle;
const schema = zpq.schema;

/// Integration test for parquet column passthrough optimization.
///
/// This test benchmarks the PASSTHROUGH case: when ALL rows in a row group
/// match the filter, we can copy raw column bytes without decode/reencode.
///
/// For partial selection (like 20% of rows), we'd need full decode/filter/encode
/// which is not yet implemented. That's a TODO for the encoder.
///
/// Test file: ci/fixtures/parquet/filter_test.parquet
/// - 10 columns, 10000 rows
/// - column 'category' has ~20% 'target' values
const TestConfig = struct {
    input_path: []const u8 = "ci/fixtures/parquet/filter_test.parquet",
    output_path: []const u8 = "/tmp/zpq_passthrough.parquet",

    // For filter column decoding
    filter_column: usize = 1,
    target_dict_index: u64 = 1,

    iterations: usize = 10,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const config = TestConfig{};

    std.debug.print("=== Parquet Passthrough Benchmark ===\n", .{});
    std.debug.print("(Simulates 100%% row selection → zero-copy column passthrough)\n\n", .{});

    // Warmup
    _ = try runPassthrough(allocator, config);

    // Benchmark
    var total_ns: u64 = 0;
    var stats: Stats = undefined;
    for (0..config.iterations) |_| {
        var timer = try std.time.Timer.start();
        stats = try runPassthrough(allocator, config);
        total_ns += timer.read();
    }

    const avg_ms = @as(f64, @floatFromInt(total_ns / config.iterations)) / 1_000_000.0;

    std.debug.print("zpq passthrough:\n", .{});
    std.debug.print("  Rows: {d}\n", .{stats.rows_read});
    std.debug.print("  Filter matches: {d} ({d:.1}%%)\n", .{
        stats.filter_matches,
        @as(f64, @floatFromInt(stats.filter_matches)) / @as(f64, @floatFromInt(stats.rows_read)) * 100,
    });
    std.debug.print("  Bytes written: {d}\n", .{stats.bytes_written});
    std.debug.print("  Avg time: {d:.3}ms ({d} iterations)\n\n", .{ avg_ms, config.iterations });

    // Verify output
    try verifyOutput(allocator, config.output_path);

    std.debug.print("NOTE: This is passthrough (all rows). For filtered writes,\n", .{});
    std.debug.print("      we need to implement page encoding (TODO).\n", .{});
}

const Stats = struct {
    rows_read: usize,
    filter_matches: usize,
    bytes_written: usize,
};

/// Passthrough: read columns, decode filter to count matches, write all columns unchanged
fn runPassthrough(allocator: std.mem.Allocator, config: TestConfig) !Stats {
    var file = try ParquetFile.open(allocator, config.input_path);
    defer file.deinit();

    try file.readFooter();
    const metadata = file.metadata orelse return error.NoMetadata;
    const num_rows: usize = @intCast(metadata.num_rows);
    const rg = metadata.row_groups.items[0];

    // Read all column data in one pass
    var column_data = try allocator.alloc([]u8, rg.columns.items.len);
    defer {
        for (column_data) |data| allocator.free(data);
        allocator.free(column_data);
    }

    for (rg.columns.items, 0..) |col_chunk, col_idx| {
        const col_meta = col_chunk.meta_data orelse {
            column_data[col_idx] = &[_]u8{};
            continue;
        };
        const col_start: u64 = @intCast(col_chunk.file_offset);
        const col_size: usize = @intCast(col_meta.total_compressed_size);

        column_data[col_idx] = try allocator.alloc(u8, col_size);
        _ = try file.source.readAt(col_start, column_data[col_idx]);
    }

    // Decode filter column to count matches (simulates filter evaluation)
    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(config.filter_column);

    var dict_page = (try col_reader.next(allocator)).?;
    defer dict_page.deinit(allocator);

    var page = (try col_reader.next(allocator)).?;
    defer page.deinit(allocator);

    const def_len = std.mem.readInt(u32, page.data[0..4], .little);
    const offset = 4 + def_len + 1;

    var dec = rle.RleDecoder.init(page.data[offset..], 1);

    var filter_matches: usize = 0;
    var i: usize = 0;
    while (i < num_rows) : (i += 1) {
        const v = try dec.next() orelse break;
        if (v == config.target_dict_index) filter_matches += 1;
    }

    // Write all columns (passthrough - simulates 100% selection)
    var writer = try ParquetWriter.init(allocator, config.output_path);
    defer writer.deinit();

    try writer.setSchema(metadata.schema.items);
    const rg_writer = try writer.beginRowGroup();

    var bytes_written: usize = 0;
    for (rg.columns.items, 0..) |col_chunk, col_idx| {
        const col_meta = col_chunk.meta_data orelse continue;
        try writer.writePassthroughColumn(rg_writer, col_meta, column_data[col_idx]);
        bytes_written += column_data[col_idx].len;
    }

    try writer.finishRowGroup(rg_writer, @intCast(num_rows));
    try writer.finish();

    return Stats{
        .rows_read = num_rows,
        .filter_matches = filter_matches,
        .bytes_written = bytes_written,
    };
}

fn verifyOutput(allocator: std.mem.Allocator, path: []const u8) !void {
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();

    try file.readFooter();
    const metadata = file.metadata orelse return error.NoMetadata;

    std.debug.print("Output verified: {d} rows, {d} columns\n\n", .{
        metadata.num_rows,
        metadata.row_groups.items[0].columns.items.len,
    });
}

test "passthrough integration" {
    const allocator = std.testing.allocator;

    var file = ParquetFile.open(allocator, "ci/fixtures/parquet/filter_test.parquet") catch |err| {
        std.debug.print("Skipping test - fixture not found: {}\n", .{err});
        return;
    };
    defer file.deinit();

    try file.readFooter();
    const metadata = file.metadata orelse return error.NoMetadata;

    try std.testing.expectEqual(@as(usize, 1), metadata.row_groups.items.len);
    try std.testing.expectEqual(@as(i64, 10000), metadata.num_rows);
}
