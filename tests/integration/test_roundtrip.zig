const std = @import("std");
const zpq = @import("zpq");

const schema = zpq.core.schema;
const encoder = zpq.core.encoder;
const page_writer = zpq.core.page_writer;
const thrift = zpq.core.thrift;
const ParquetFile = zpq.file.ParquetFile;

/// Integration test: Write a parquet file and read it back
pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("=== ZPQ Round-Trip Integration Test ===\n\n", .{});

    // Test 1: Simple INT32 column
    std.debug.print("Test 1: INT32 column (PLAIN encoding)\n", .{});
    try testInt32Roundtrip(allocator, "/tmp/zpq_test_int32.parquet");
    std.debug.print("  ✓ INT32 roundtrip passed\n\n", .{});

    // Test 2: Multiple column types
    std.debug.print("Test 2: Multiple column types\n", .{});
    try testMultiColumnRoundtrip(allocator, "/tmp/zpq_test_multi.parquet");
    std.debug.print("  ✓ Multi-column roundtrip passed\n\n", .{});

    // Test 3: Dictionary-encoded strings
    std.debug.print("Test 3: Dictionary-encoded BYTE_ARRAY\n", .{});
    try testDictStringRoundtrip(allocator, "/tmp/zpq_test_dict.parquet");
    std.debug.print("  ✓ Dictionary string roundtrip passed\n\n", .{});

    // Test 4: GZIP compressed INT32 column
    std.debug.print("Test 4: GZIP compressed INT32 column\n", .{});
    try testGzipInt32Roundtrip(allocator, "/tmp/zpq_test_gzip.parquet");
    std.debug.print("  ✓ GZIP compressed roundtrip passed\n\n", .{});

    // Test 5: SNAPPY compressed INT32 column
    std.debug.print("Test 5: SNAPPY compressed INT32 column\n", .{});
    try testSnappyInt32Roundtrip(allocator, "/tmp/zpq_test_snappy.parquet");
    std.debug.print("  ✓ SNAPPY compressed roundtrip passed\n\n", .{});

    // Test 6: ZSTD compressed INT32 column (only if ZSTD compression enabled)
    const zstd = zpq.core.zstd;
    if (comptime zstd.compression_enabled) {
        std.debug.print("Test 6: ZSTD compressed INT32 column\n", .{});
        try testZstdInt32Roundtrip(allocator, "/tmp/zpq_test_zstd.parquet");
        std.debug.print("  ✓ ZSTD compressed roundtrip passed\n\n", .{});
    } else {
        std.debug.print("Test 6: ZSTD compressed INT32 column (SKIPPED - not enabled)\n\n", .{});
    }

    std.debug.print("=== All Round-Trip Tests Passed ===\n", .{});
}

fn testInt32Roundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write
    const values = [_]i32{ 1, 2, 3, 4, 5, 100, -50, 999 };
    try writeInt32File(allocator, path, &values);

    // Read back and verify
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 8), meta.num_rows);
    try std.testing.expectEqual(@as(usize, 1), meta.row_groups.items.len);

    // Read column data using rowGroup reader
    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(0);

    // Read pages and decode values
    var decoded = std.ArrayListUnmanaged(i32){};
    defer decoded.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    while (try col_reader.next(arena.allocator())) |page| {
        if (page.header.type == .DATA_PAGE) {
            var decoder_inst = zpq.decoder.Decoder.init(page.data);
            while (decoder_inst.hasMore()) {
                const val = try decoder_inst.readInt32();
                try decoded.append(allocator, val);
            }
        }
    }

    try std.testing.expectEqual(@as(usize, 8), decoded.items.len);
    for (decoded.items, 0..) |val, i| {
        try std.testing.expectEqual(values[i], val);
    }
}

fn testMultiColumnRoundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write file with INT32, INT64, and DOUBLE columns
    const int32_vals = [_]i32{ 10, 20, 30 };
    const int64_vals = [_]i64{ 1000, 2000, 3000 };
    const double_vals = [_]f64{ 1.5, 2.5, 3.5 };

    try writeMultiColumnFile(allocator, path, &int32_vals, &int64_vals, &double_vals);

    // Read back
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 3), meta.num_rows);

    const rg = meta.row_groups.items[0];
    try std.testing.expectEqual(@as(usize, 3), rg.columns.items.len);

    // Verify column types
    try std.testing.expectEqual(schema.Type.INT32, rg.columns.items[0].meta_data.?.type);
    try std.testing.expectEqual(schema.Type.INT64, rg.columns.items[1].meta_data.?.type);
    try std.testing.expectEqual(schema.Type.DOUBLE, rg.columns.items[2].meta_data.?.type);
}

fn testDictStringRoundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write dictionary-encoded strings
    const strings = [_][]const u8{
        "apple",
        "banana",
        "apple", // duplicate
        "cherry",
        "banana", // duplicate
        "apple", // duplicate
    };

    try writeDictStringFile(allocator, path, &strings);

    // Read back
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 6), meta.num_rows);

    const rg = meta.row_groups.items[0];
    const col = rg.columns.items[0];

    // Check it used dictionary encoding
    var has_dict = false;
    for (col.meta_data.?.encodings.items) |enc| {
        if (enc == .RLE_DICTIONARY or enc == .PLAIN_DICTIONARY) {
            has_dict = true;
            break;
        }
    }
    try std.testing.expect(has_dict);
}

// ============================================================================
// Helper functions to write test files
// ============================================================================

fn writeInt32File(allocator: std.mem.Allocator, path: []const u8, values: []const i32) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    // Write PAR1 magic
    try file.writeAll("PAR1");
    var offset: u64 = 4;

    // Create column writer
    var col_writer = page_writer.ColumnWriter.init(allocator, .INT32, .UNCOMPRESSED);
    defer col_writer.deinit();

    try col_writer.writeInt32Plain(values);

    // Write column to file
    const chunk = try col_writer.writeToFile(file, &[_][]const u8{"value"}, offset);
    offset = @intCast(try file.getPos());

    // Build metadata
    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }

    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk);

    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col_writer.total_compressed_size,
        .num_rows = @intCast(values.len),
    });

    // Build schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, schema.SchemaElement{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = 1,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try schema_elements.append(allocator, schema.SchemaElement{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "value",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });

    // Write footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(values.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}

fn testGzipInt32Roundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write with GZIP compression
    const values = [_]i32{ 1, 2, 3, 4, 5, 100, -50, 999, 12345, -99999 };
    try writeGzipInt32File(allocator, path, &values);

    // Read back and verify
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 10), meta.num_rows);

    // Verify compression codec in metadata
    const rg = meta.row_groups.items[0];
    const col_meta = rg.columns.items[0].meta_data orelse return error.NoColumnMetadata;
    try std.testing.expectEqual(schema.CompressionCodec.GZIP, col_meta.codec);

    // Read and decode - ParquetFile should handle decompression
    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(0);

    var decoded = std.ArrayListUnmanaged(i32){};
    defer decoded.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    while (try col_reader.next(arena.allocator())) |page| {
        if (page.header.type == .DATA_PAGE) {
            var decoder_inst = zpq.decoder.Decoder.init(page.data);
            while (decoder_inst.hasMore()) {
                const val = try decoder_inst.readInt32();
                try decoded.append(allocator, val);
            }
        }
    }

    try std.testing.expectEqual(@as(usize, 10), decoded.items.len);
    for (decoded.items, 0..) |val, i| {
        try std.testing.expectEqual(values[i], val);
    }
}

fn writeGzipInt32File(allocator: std.mem.Allocator, path: []const u8, values: []const i32) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    try file.writeAll("PAR1");
    var offset: u64 = 4;

    // Create column writer with GZIP compression
    var col_writer = page_writer.ColumnWriter.init(allocator, .INT32, .GZIP);
    defer col_writer.deinit();

    try col_writer.writeInt32Plain(values);

    const chunk = try col_writer.writeToFile(file, &[_][]const u8{"value"}, offset);
    offset = @intCast(try file.getPos());

    // Build metadata
    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }

    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk);

    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col_writer.total_compressed_size,
        .num_rows = @intCast(values.len),
    });

    // Build schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 1, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .INT32, .type_length = null, .repetition_type = .REQUIRED, .name = "value", .num_children = null, .scale = null, .precision = null, .field_id = null });

    // Write footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(values.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}

fn testSnappyInt32Roundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write with SNAPPY compression
    const values = [_]i32{ 1, 2, 3, 4, 5, 100, -50, 999, 12345, -99999 };
    try writeSnappyInt32File(allocator, path, &values);

    // Read back and verify
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 10), meta.num_rows);

    // Verify compression codec in metadata
    const rg = meta.row_groups.items[0];
    const col_meta = rg.columns.items[0].meta_data orelse return error.NoColumnMetadata;
    try std.testing.expectEqual(schema.CompressionCodec.SNAPPY, col_meta.codec);

    // Read and decode - ParquetFile should handle decompression
    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(0);

    var decoded = std.ArrayListUnmanaged(i32){};
    defer decoded.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    while (try col_reader.next(arena.allocator())) |page| {
        if (page.header.type == .DATA_PAGE) {
            var decoder_inst = zpq.decoder.Decoder.init(page.data);
            while (decoder_inst.hasMore()) {
                const val = try decoder_inst.readInt32();
                try decoded.append(allocator, val);
            }
        }
    }

    try std.testing.expectEqual(@as(usize, 10), decoded.items.len);
    for (decoded.items, 0..) |val, i| {
        try std.testing.expectEqual(values[i], val);
    }
}

fn writeSnappyInt32File(allocator: std.mem.Allocator, path: []const u8, values: []const i32) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    try file.writeAll("PAR1");
    var offset: u64 = 4;

    // Create column writer with SNAPPY compression
    var col_writer = page_writer.ColumnWriter.init(allocator, .INT32, .SNAPPY);
    defer col_writer.deinit();

    try col_writer.writeInt32Plain(values);

    const chunk = try col_writer.writeToFile(file, &[_][]const u8{"value"}, offset);
    offset = @intCast(try file.getPos());

    // Build metadata
    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }

    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk);

    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col_writer.total_compressed_size,
        .num_rows = @intCast(values.len),
    });

    // Build schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 1, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .INT32, .type_length = null, .repetition_type = .REQUIRED, .name = "value", .num_children = null, .scale = null, .precision = null, .field_id = null });

    // Write footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(values.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}

fn testZstdInt32Roundtrip(allocator: std.mem.Allocator, path: []const u8) !void {
    // Write with ZSTD compression
    const values = [_]i32{ 1, 2, 3, 4, 5, 100, -50, 999, 12345, -99999 };
    try writeZstdInt32File(allocator, path, &values);

    // Read back and verify
    var file = try ParquetFile.open(allocator, path);
    defer file.deinit();
    try file.readFooter();

    const meta = file.metadata orelse return error.NoMetadata;
    try std.testing.expectEqual(@as(i64, 10), meta.num_rows);

    // Verify compression codec in metadata
    const rg = meta.row_groups.items[0];
    const col_meta = rg.columns.items[0].meta_data orelse return error.NoColumnMetadata;
    try std.testing.expectEqual(schema.CompressionCodec.ZSTD, col_meta.codec);

    // Read and decode - ParquetFile should handle decompression
    var rg_reader = try file.rowGroup(0);
    defer rg_reader.deinit();

    var col_reader = try rg_reader.columnReader(0);

    var decoded = std.ArrayListUnmanaged(i32){};
    defer decoded.deinit(allocator);

    var arena = std.heap.ArenaAllocator.init(allocator);
    defer arena.deinit();

    while (try col_reader.next(arena.allocator())) |page| {
        if (page.header.type == .DATA_PAGE) {
            var decoder_inst = zpq.decoder.Decoder.init(page.data);
            while (decoder_inst.hasMore()) {
                const val = try decoder_inst.readInt32();
                try decoded.append(allocator, val);
            }
        }
    }

    try std.testing.expectEqual(@as(usize, 10), decoded.items.len);
    for (decoded.items, 0..) |val, i| {
        try std.testing.expectEqual(values[i], val);
    }
}

fn writeZstdInt32File(allocator: std.mem.Allocator, path: []const u8, values: []const i32) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    try file.writeAll("PAR1");
    var offset: u64 = 4;

    // Create column writer with ZSTD compression
    var col_writer = page_writer.ColumnWriter.init(allocator, .INT32, .ZSTD);
    defer col_writer.deinit();

    try col_writer.writeInt32Plain(values);

    const chunk = try col_writer.writeToFile(file, &[_][]const u8{"value"}, offset);
    offset = @intCast(try file.getPos());

    // Build metadata
    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }

    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk);

    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col_writer.total_compressed_size,
        .num_rows = @intCast(values.len),
    });

    // Build schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 1, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .INT32, .type_length = null, .repetition_type = .REQUIRED, .name = "value", .num_children = null, .scale = null, .precision = null, .field_id = null });

    // Write footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(values.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}

fn writeMultiColumnFile(
    allocator: std.mem.Allocator,
    path: []const u8,
    int32_vals: []const i32,
    int64_vals: []const i64,
    double_vals: []const f64,
) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    try file.writeAll("PAR1");
    var offset: u64 = 4;

    // Write INT32 column
    var col1 = page_writer.ColumnWriter.init(allocator, .INT32, .UNCOMPRESSED);
    defer col1.deinit();
    try col1.writeInt32Plain(int32_vals);
    const chunk1 = try col1.writeToFile(file, &[_][]const u8{"int_col"}, offset);
    offset = @intCast(try file.getPos());

    // Write INT64 column
    var col2 = page_writer.ColumnWriter.init(allocator, .INT64, .UNCOMPRESSED);
    defer col2.deinit();
    try col2.writeInt64Plain(int64_vals);
    const chunk2 = try col2.writeToFile(file, &[_][]const u8{"long_col"}, offset);
    offset = @intCast(try file.getPos());

    // Write DOUBLE column
    var col3 = page_writer.ColumnWriter.init(allocator, .DOUBLE, .UNCOMPRESSED);
    defer col3.deinit();
    try col3.writeDoublePlain(double_vals);
    const chunk3 = try col3.writeToFile(file, &[_][]const u8{"double_col"}, offset);
    _ = @as(u64, @intCast(try file.getPos()));

    // Build row group
    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk1);
    try columns.append(allocator, chunk2);
    try columns.append(allocator, chunk3);

    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }
    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col1.total_compressed_size + col2.total_compressed_size + col3.total_compressed_size,
        .num_rows = @intCast(int32_vals.len),
    });

    // Schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 3, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .INT32, .type_length = null, .repetition_type = .REQUIRED, .name = "int_col", .num_children = null, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .INT64, .type_length = null, .repetition_type = .REQUIRED, .name = "long_col", .num_children = null, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .DOUBLE, .type_length = null, .repetition_type = .REQUIRED, .name = "double_col", .num_children = null, .scale = null, .precision = null, .field_id = null });

    // Footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(int32_vals.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}

fn writeDictStringFile(allocator: std.mem.Allocator, path: []const u8, strings: []const []const u8) !void {
    const file = try std.fs.cwd().createFile(path, .{});
    defer file.close();

    try file.writeAll("PAR1");
    const offset: u64 = 4;

    // Write BYTE_ARRAY column with dictionary encoding
    var col = page_writer.ColumnWriter.init(allocator, .BYTE_ARRAY, .UNCOMPRESSED);
    defer col.deinit();

    try col.writeByteArrayDict(strings);
    try col.flushDict();

    const chunk = try col.writeToFile(file, &[_][]const u8{"name"}, offset);
    _ = @as(u64, @intCast(try file.getPos()));

    // Row group
    var columns = std.ArrayListUnmanaged(schema.ColumnChunk){};
    try columns.append(allocator, chunk);

    var row_groups = std.ArrayListUnmanaged(schema.RowGroup){};
    defer {
        for (row_groups.items) |*rg| rg.deinit(allocator);
        row_groups.deinit(allocator);
    }
    try row_groups.append(allocator, schema.RowGroup{
        .columns = columns,
        .total_byte_size = col.total_compressed_size,
        .num_rows = @intCast(strings.len),
    });

    // Schema
    var schema_elements = std.ArrayListUnmanaged(schema.SchemaElement){};
    defer schema_elements.deinit(allocator);

    try schema_elements.append(allocator, .{ .type = null, .type_length = null, .repetition_type = null, .name = "schema", .num_children = 1, .scale = null, .precision = null, .field_id = null });
    try schema_elements.append(allocator, .{ .type = .BYTE_ARRAY, .type_length = null, .repetition_type = .REQUIRED, .name = "name", .num_children = null, .scale = null, .precision = null, .field_id = null });

    // Footer
    const metadata = schema.FileMetaData{
        .version = 2,
        .schema = schema_elements,
        .num_rows = @intCast(strings.len),
        .created_by = "zpq",
        .row_groups = row_groups,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try metadata.write(&writer);

    const footer_bytes = writer.bytes();
    try file.writeAll(footer_bytes);
    try file.writeAll(&std.mem.toBytes(@as(u32, @intCast(footer_bytes.len))));
    try file.writeAll("PAR1");
}
