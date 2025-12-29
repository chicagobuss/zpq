const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");

/// Parquet file writer with support for passthrough (zero-copy) column data.
pub const ParquetWriter = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    current_offset: u64,

    // Metadata being built
    row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    schema_elements: std.ArrayListUnmanaged(schema.SchemaElement),
    num_rows: i64,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) !ParquetWriter {
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
            .num_rows = 0,
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
    }

    /// Set the schema (must be called before writing row groups)
    pub fn setSchema(self: *ParquetWriter, elements: []const schema.SchemaElement) !void {
        self.schema_elements.clearRetainingCapacity();
        try self.schema_elements.appendSlice(self.allocator, elements);
    }

    /// Begin a new row group
    pub fn beginRowGroup(self: *ParquetWriter) !*RowGroupWriter {
        const rg_writer = try self.allocator.create(RowGroupWriter);
        rg_writer.* = RowGroupWriter{
            .parent = self,
            .columns = .{},
            .total_byte_size = 0,
            .num_rows = 0,
        };
        return rg_writer;
    }

    /// Write a column chunk with passthrough data (zero-copy from source)
    pub fn writePassthroughColumn(
        self: *ParquetWriter,
        rg_writer: *RowGroupWriter,
        original_meta: schema.ColumnMetaData,
        raw_data: []const u8,
    ) !void {
        const data_offset = self.current_offset;

        // Write raw column data directly
        try self.file.writeAll(raw_data);
        self.current_offset += raw_data.len;

        // Create column chunk metadata pointing to new location
        var chunk = schema.ColumnChunk{
            .file_path = null,
            .file_offset = @intCast(data_offset),
            .meta_data = schema.ColumnMetaData{
                .type = original_meta.type,
                .encodings = .{}, // Will copy below
                .path_in_schema = .{}, // Will copy below
                .codec = original_meta.codec,
                .num_values = original_meta.num_values,
                .total_uncompressed_size = original_meta.total_uncompressed_size,
                .total_compressed_size = original_meta.total_compressed_size,
                .data_page_offset = @intCast(data_offset),
                .index_page_offset = null,
                .dictionary_page_offset = if (original_meta.dictionary_page_offset != null) @as(i64, @intCast(data_offset)) else null,
            },
        };

        // Copy encodings
        for (original_meta.encodings.items) |enc| {
            try chunk.meta_data.?.encodings.append(self.allocator, enc);
        }

        // Copy path_in_schema
        for (original_meta.path_in_schema.items) |path| {
            try chunk.meta_data.?.path_in_schema.append(self.allocator, path);
        }

        try rg_writer.columns.append(self.allocator, chunk);
        rg_writer.total_byte_size += @intCast(raw_data.len);
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

pub const RowGroupWriter = struct {
    parent: *ParquetWriter,
    columns: std.ArrayListUnmanaged(schema.ColumnChunk),
    total_byte_size: i64,
    num_rows: i64,
};

test "parquet writer basic" {
    const allocator = std.testing.allocator;

    // Create a simple parquet file
    var writer = try ParquetWriter.init(allocator, "/tmp/test_writer.parquet");
    defer writer.deinit();

    // Set schema: root + one column
    try writer.setSchema(&[_]schema.SchemaElement{
        .{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = 1,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
        .{
            .type = .INT32,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = "value",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        },
    });

    try writer.finish();
}
