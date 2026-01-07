//! Parquet file writer
//!
//! Supports two modes:
//! 1. Fresh data: encode values → write pages → build footer
//! 2. Passthrough: copy raw column bytes (zero-copy transforms)

const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const page_writer = @import("page_writer.zig");
const encoder = @import("encoder.zig");

/// Column definition for schema building
pub const ColumnDef = struct {
    name: []const u8,
    type: schema.Type,
    repetition: schema.FieldRepetitionType = .REQUIRED,
};

/// Parquet file writer
pub const ParquetWriter = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    current_offset: u64,

    // Metadata being built
    row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    schema_elements: std.ArrayListUnmanaged(schema.SchemaElement),
    column_defs: std.ArrayListUnmanaged(ColumnDef),
    num_rows: i64,

    // Settings
    compression: schema.CompressionCodec,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !ParquetWriter {
        return initWithOptions(allocator, path, .{});
    }

    pub const Options = struct {
        compression: schema.CompressionCodec = .UNCOMPRESSED,
    };

    pub fn initWithOptions(allocator: std.mem.Allocator, path: []const u8, options: Options) !ParquetWriter {
        const file = try std.fs.cwd().createFile(path, .{});
        errdefer file.close();

        // Write PAR1 magic
        try file.writeAll("PAR1");

        return ParquetWriter{
            .allocator = allocator,
            .file = file,
            .current_offset = 4, // After magic
            .row_groups = .{},
            .schema_elements = .{},
            .column_defs = .{},
            .num_rows = 0,
            .compression = options.compression,
        };
    }

    pub fn deinit(self: *ParquetWriter) void {
        self.file.close();
        // Free column chunk metadata arrays
        for (self.row_groups.items) |*rg| {
            for (rg.columns.items) |*col| {
                if (col.meta_data) |*meta| {
                    meta.encodings.deinit(self.allocator);
                    meta.path_in_schema.deinit(self.allocator);
                }
            }
            rg.columns.deinit(self.allocator);
        }
        self.row_groups.deinit(self.allocator);
        self.schema_elements.deinit(self.allocator);
        self.column_defs.deinit(self.allocator);
    }

    // ========================================================================
    // Schema Definition API
    // ========================================================================

    /// Define columns (simpler API - builds schema elements automatically)
    pub fn setColumns(self: *ParquetWriter, columns: []const ColumnDef) !void {
        self.column_defs.clearRetainingCapacity();
        self.schema_elements.clearRetainingCapacity();

        // Root schema element
        try self.schema_elements.append(self.allocator, .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = @intCast(columns.len),
            .scale = null,
            .precision = null,
            .field_id = null,
        });

        // Column schema elements
        for (columns) |col| {
            try self.column_defs.append(self.allocator, col);
            try self.schema_elements.append(self.allocator, .{
                .type = col.type,
                .type_length = null,
                .repetition_type = col.repetition,
                .name = col.name,
                .num_children = null,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
        }
    }

    /// Set raw schema elements (for advanced use / passthrough)
    pub fn setSchema(self: *ParquetWriter, elements: []const schema.SchemaElement) !void {
        self.schema_elements.clearRetainingCapacity();
        try self.schema_elements.appendSlice(self.allocator, elements);
    }

    // ========================================================================
    // Row Group Writing API
    // ========================================================================

    /// Begin a new row group
    pub fn beginRowGroup(self: *ParquetWriter) !*RowGroupWriter {
        const rg_writer = try self.allocator.create(RowGroupWriter);
        rg_writer.* = RowGroupWriter{
            .parent = self,
            .columns = .{},
            .total_byte_size = 0,
            .num_rows = 0,
            .col_index = 0,
        };
        return rg_writer;
    }

    /// Finish the current row group
    pub fn finishRowGroup(self: *ParquetWriter, rg_writer: *RowGroupWriter, num_rows: i64) !void {
        rg_writer.num_rows = num_rows;
        self.num_rows += num_rows;

        try self.row_groups.append(self.allocator, schema.RowGroup{
            .columns = rg_writer.columns,
            .total_byte_size = rg_writer.total_byte_size,
            .num_rows = rg_writer.num_rows,
        });

        // Don't deinit columns - ownership transferred to row_groups
        self.allocator.destroy(rg_writer);
    }

    /// Finish writing the file (write footer and magic)
    pub fn finish(self: *ParquetWriter) !void {
        // Build FileMetaData
        const metadata = schema.FileMetaData{
            .version = 2,
            .schema = self.schema_elements,
            .num_rows = self.num_rows,
            .created_by = "zpq",
            .row_groups = self.row_groups,
        };

        // Serialize footer
        var writer = thrift.Writer.init(self.allocator);
        defer writer.deinit();
        try metadata.write(&writer);

        const footer_bytes = writer.bytes();
        const footer_len: u32 = @intCast(footer_bytes.len);

        // Write footer
        try self.file.writeAll(footer_bytes);

        // Write footer length (4 bytes, little-endian)
        try self.file.writeAll(&std.mem.toBytes(footer_len));

        // Write PAR1 magic
        try self.file.writeAll("PAR1");
    }
};

/// Row group writer - accumulates columns for a single row group
pub const RowGroupWriter = struct {
    parent: *ParquetWriter,
    columns: std.ArrayListUnmanaged(schema.ColumnChunk),
    total_byte_size: i64,
    num_rows: i64,
    col_index: usize,

    // ========================================================================
    // Fresh Data Writing (encode values)
    // ========================================================================

    /// Write INT32 column with PLAIN encoding
    pub fn writeInt32Column(self: *RowGroupWriter, values: []const i32) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .INT32, self.parent.compression);
        defer cw.deinit();

        try cw.writeInt32Plain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write INT64 column with PLAIN encoding
    pub fn writeInt64Column(self: *RowGroupWriter, values: []const i64) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .INT64, self.parent.compression);
        defer cw.deinit();

        try cw.writeInt64Plain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write FLOAT column with PLAIN encoding
    pub fn writeFloatColumn(self: *RowGroupWriter, values: []const f32) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .FLOAT, self.parent.compression);
        defer cw.deinit();

        try cw.writeFloatPlain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write DOUBLE column with PLAIN encoding
    pub fn writeDoubleColumn(self: *RowGroupWriter, values: []const f64) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .DOUBLE, self.parent.compression);
        defer cw.deinit();

        try cw.writeDoublePlain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write BOOLEAN column
    pub fn writeBooleanColumn(self: *RowGroupWriter, values: []const bool) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .BOOLEAN, self.parent.compression);
        defer cw.deinit();

        try cw.writeBooleanPlain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write BYTE_ARRAY (string) column with PLAIN encoding
    pub fn writeByteArrayColumn(self: *RowGroupWriter, values: []const []const u8) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .BYTE_ARRAY, self.parent.compression);
        defer cw.deinit();

        try cw.writeByteArrayPlain(values);
        try self.writeColumnWriter(&cw);
    }

    /// Write BYTE_ARRAY column with dictionary encoding
    pub fn writeByteArrayDictColumn(self: *RowGroupWriter, values: []const []const u8) !void {
        var cw = page_writer.ColumnWriter.init(self.parent.allocator, .BYTE_ARRAY, self.parent.compression);
        defer cw.deinit();

        try cw.writeByteArrayDict(values);
        try cw.flushDict();
        try self.writeColumnWriter(&cw);
    }

    /// Internal: write a ColumnWriter's pages to the file
    fn writeColumnWriter(self: *RowGroupWriter, cw: *page_writer.ColumnWriter) !void {
        const col_name = if (self.col_index < self.parent.column_defs.items.len)
            self.parent.column_defs.items[self.col_index].name
        else
            "column";

        const path_in_schema = [_][]const u8{col_name};
        const chunk = try cw.writeToFile(self.parent.file, &path_in_schema, self.parent.current_offset);

        // Update offset
        self.parent.current_offset += @intCast(chunk.meta_data.?.total_compressed_size);

        // Track in row group
        try self.columns.append(self.parent.allocator, chunk);
        self.total_byte_size += chunk.meta_data.?.total_compressed_size;
        self.col_index += 1;
    }

    // ========================================================================
    // Passthrough Writing (zero-copy)
    // ========================================================================

    /// Write a column chunk with passthrough data (zero-copy from source)
    pub fn writePassthroughColumn(
        self: *RowGroupWriter,
        original_meta: schema.ColumnMetaData,
        raw_data: []const u8,
    ) !void {
        const data_offset = self.parent.current_offset;

        // Write raw column data directly
        try self.parent.file.writeAll(raw_data);
        self.parent.current_offset += raw_data.len;

        // Create column chunk metadata pointing to new location
        var chunk = schema.ColumnChunk{
            .file_path = null,
            .file_offset = @intCast(data_offset),
            .meta_data = schema.ColumnMetaData{
                .type = original_meta.type,
                .encodings = .{},
                .path_in_schema = .{},
                .codec = original_meta.codec,
                .num_values = original_meta.num_values,
                .total_uncompressed_size = original_meta.total_uncompressed_size,
                .total_compressed_size = original_meta.total_compressed_size,
                .data_page_offset = @intCast(data_offset),
                .index_page_offset = null,
                .dictionary_page_offset = if (original_meta.dictionary_page_offset != null)
                    @as(i64, @intCast(data_offset))
                else
                    null,
            },
        };

        // Copy encodings
        for (original_meta.encodings.items) |enc| {
            try chunk.meta_data.?.encodings.append(self.parent.allocator, enc);
        }

        // Copy path_in_schema
        for (original_meta.path_in_schema.items) |path| {
            try chunk.meta_data.?.path_in_schema.append(self.parent.allocator, path);
        }

        try self.columns.append(self.parent.allocator, chunk);
        self.total_byte_size += @intCast(raw_data.len);
        self.col_index += 1;
    }
};

// ============================================================================
// Tests
// ============================================================================

test "ParquetWriter - write INT32 column" {
    const allocator = std.testing.allocator;

    var writer = try ParquetWriter.init(allocator, "/tmp/test_int32.parquet");
    defer writer.deinit();

    // Define schema
    try writer.setColumns(&[_]ColumnDef{
        .{ .name = "id", .type = .INT32 },
    });

    // Write row group
    var rg = try writer.beginRowGroup();
    try rg.writeInt32Column(&[_]i32{ 1, 2, 3, 4, 5 });
    try writer.finishRowGroup(rg, 5);

    // Finish file
    try writer.finish();

    // Verify file exists and has correct magic bytes
    const file = try std.fs.cwd().openFile("/tmp/test_int32.parquet", .{});
    defer file.close();

    var magic: [4]u8 = undefined;
    _ = try file.readAll(&magic);
    try std.testing.expectEqualStrings("PAR1", &magic);

    // Check footer magic
    try file.seekFromEnd(-4);
    _ = try file.readAll(&magic);
    try std.testing.expectEqualStrings("PAR1", &magic);
}

test "ParquetWriter - multiple columns" {
    const allocator = std.testing.allocator;

    var writer = try ParquetWriter.init(allocator, "/tmp/test_multi.parquet");
    defer writer.deinit();

    // Define schema
    try writer.setColumns(&[_]ColumnDef{
        .{ .name = "id", .type = .INT32 },
        .{ .name = "value", .type = .DOUBLE },
        .{ .name = "name", .type = .BYTE_ARRAY },
    });

    // Write row group
    var rg = try writer.beginRowGroup();
    try rg.writeInt32Column(&[_]i32{ 1, 2, 3 });
    try rg.writeDoubleColumn(&[_]f64{ 1.5, 2.5, 3.5 });
    try rg.writeByteArrayColumn(&[_][]const u8{ "alice", "bob", "charlie" });
    try writer.finishRowGroup(rg, 3);

    try writer.finish();
}

test "ParquetWriter - dictionary encoded strings" {
    const allocator = std.testing.allocator;

    var writer = try ParquetWriter.init(allocator, "/tmp/test_dict.parquet");
    defer writer.deinit();

    try writer.setColumns(&[_]ColumnDef{
        .{ .name = "category", .type = .BYTE_ARRAY },
    });

    var rg = try writer.beginRowGroup();
    // Repeated values benefit from dictionary encoding
    try rg.writeByteArrayDictColumn(&[_][]const u8{
        "apple", "banana", "apple", "cherry", "banana", "apple",
    });
    try writer.finishRowGroup(rg, 6);

    try writer.finish();
}
