const std = @import("std");
const schema = @import("schema.zig");
const encoder = @import("encoder.zig");
const thrift = @import("thrift.zig");

/// Page writer for encoding and writing Parquet data pages
pub const PageWriter = struct {
    allocator: std.mem.Allocator,

    // Page buffer
    page_buffer: std.ArrayListUnmanaged(u8),

    // Encoders
    plain_encoder: encoder.PlainEncoder,
    rle_encoder: ?encoder.RleEncoder,

    // Page settings
    compression: schema.CompressionCodec,
    target_page_size: usize,

    // Current page stats
    num_values: i32,
    num_nulls: i32,

    pub fn init(allocator: std.mem.Allocator, compression: schema.CompressionCodec) PageWriter {
        return .{
            .allocator = allocator,
            .page_buffer = .{},
            .plain_encoder = encoder.PlainEncoder.init(allocator),
            .rle_encoder = null,
            .compression = compression,
            .target_page_size = 1024 * 1024, // 1MB default
            .num_values = 0,
            .num_nulls = 0,
        };
    }

    pub fn deinit(self: *PageWriter) void {
        self.page_buffer.deinit(self.allocator);
        self.plain_encoder.deinit();
        if (self.rle_encoder) |*rle| {
            rle.deinit();
        }
    }

    pub fn reset(self: *PageWriter) void {
        self.page_buffer.clearRetainingCapacity();
        self.plain_encoder.reset();
        if (self.rle_encoder) |*rle| {
            rle.reset();
        }
        self.num_values = 0;
        self.num_nulls = 0;
    }

    /// Write definition levels (for nullable columns)
    pub fn writeDefinitionLevels(self: *PageWriter, max_def: u8, levels: []const u8) !void {
        if (max_def == 0) return; // No def levels needed for required columns

        // Initialize RLE encoder if needed
        if (self.rle_encoder == null) {
            const bit_width = bitWidth(max_def);
            self.rle_encoder = encoder.RleEncoder.init(self.allocator, bit_width);
        }

        var rle = &self.rle_encoder.?;
        for (levels) |level| {
            try rle.write(level);
        }
        try rle.finish();

        // Write length-prefixed RLE data
        const rle_data = rle.getData();
        const len: u32 = @intCast(rle_data.len);
        try self.page_buffer.appendSlice(self.allocator, std.mem.asBytes(&len));
        try self.page_buffer.appendSlice(self.allocator, rle_data);

        rle.reset();
    }

    /// Write repetition levels (for nested columns)
    pub fn writeRepetitionLevels(self: *PageWriter, max_rep: u8, levels: []const u8) !void {
        if (max_rep == 0) return; // No rep levels needed for flat columns

        // Same as definition levels
        try self.writeDefinitionLevels(max_rep, levels);
    }

    // ========================================================================
    // PLAIN encoding methods
    // ========================================================================

    pub fn writePlainInt32s(self: *PageWriter, values: []const i32) !void {
        try self.plain_encoder.writeInt32s(values);
        self.num_values += @intCast(values.len);
    }

    pub fn writePlainInt64s(self: *PageWriter, values: []const i64) !void {
        try self.plain_encoder.writeInt64s(values);
        self.num_values += @intCast(values.len);
    }

    pub fn writePlainFloats(self: *PageWriter, values: []const f32) !void {
        try self.plain_encoder.writeFloats(values);
        self.num_values += @intCast(values.len);
    }

    pub fn writePlainDoubles(self: *PageWriter, values: []const f64) !void {
        try self.plain_encoder.writeDoubles(values);
        self.num_values += @intCast(values.len);
    }

    pub fn writePlainByteArrays(self: *PageWriter, values: []const []const u8) !void {
        try self.plain_encoder.writeByteArrays(values);
        self.num_values += @intCast(values.len);
    }

    pub fn writePlainBooleans(self: *PageWriter, values: []const bool) !void {
        try self.plain_encoder.writeBooleans(values);
        self.num_values += @intCast(values.len);
    }

    // ========================================================================
    // Dictionary encoding methods
    // ========================================================================

    /// Write RLE-encoded dictionary indices
    pub fn writeDictIndices(self: *PageWriter, indices: []const u32, bit_width: u8) !void {
        // Write bit width as first byte
        try self.page_buffer.append(self.allocator, bit_width);

        // RLE encode the indices
        if (self.rle_encoder == null or self.rle_encoder.?.bit_width != bit_width) {
            if (self.rle_encoder) |*rle| {
                rle.deinit();
            }
            self.rle_encoder = encoder.RleEncoder.init(self.allocator, bit_width);
        }

        var rle = &self.rle_encoder.?;
        rle.reset();
        try rle.writeMany(indices);
        try rle.finish();

        try self.page_buffer.appendSlice(self.allocator, rle.getData());
        self.num_values += @intCast(indices.len);
    }

    // ========================================================================
    // Page finalization
    // ========================================================================

    /// Finalize data page and return compressed bytes with header
    pub fn finalizeDataPage(self: *PageWriter, page_encoding: schema.Encoding) !DataPageResult {
        // Combine definition levels (already in page_buffer) with encoded data
        const encoded_data = self.plain_encoder.getData();
        try self.page_buffer.appendSlice(self.allocator, encoded_data);

        const uncompressed_data = self.page_buffer.items;
        const uncompressed_size: i32 = @intCast(uncompressed_data.len);

        // Compress if needed
        // For now, only UNCOMPRESSED is supported
        if (self.compression != .UNCOMPRESSED) {
            return error.UnsupportedCompression;
        }

        const compressed_size = uncompressed_size;

        // Build page header
        const header = schema.PageHeader{
            .type = .DATA_PAGE,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = null,
            .data_page_header = schema.DataPageHeader{
                .num_values = self.num_values,
                .encoding = page_encoding,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = null,
        };

        // Copy data to owned buffer (so caller can safely reset the encoder)
        const owned_data = try self.allocator.dupe(u8, uncompressed_data);

        return DataPageResult{
            .header = header,
            .data = owned_data,
            .owns_data = true,
            .allocator = self.allocator,
        };
    }

    /// Finalize dictionary page
    pub fn finalizeDictPage(self: *PageWriter, num_values: i32) !DataPageResult {
        const encoded_data = self.plain_encoder.getData();
        const uncompressed_size: i32 = @intCast(encoded_data.len);

        // For now, only UNCOMPRESSED is supported
        if (self.compression != .UNCOMPRESSED) {
            return error.UnsupportedCompression;
        }

        const compressed_size = uncompressed_size;

        const header = schema.PageHeader{
            .type = .DICTIONARY_PAGE,
            .uncompressed_page_size = uncompressed_size,
            .compressed_page_size = compressed_size,
            .crc = null,
            .data_page_header = null,
            .dictionary_page_header = schema.DictionaryPageHeader{
                .num_values = num_values,
                .encoding = .PLAIN,
                .is_sorted = false,
            },
        };

        // Copy data to owned buffer (so caller can safely reset the encoder)
        const owned_data = try self.allocator.dupe(u8, encoded_data);

        return DataPageResult{
            .header = header,
            .data = owned_data,
            .owns_data = true,
            .allocator = self.allocator,
        };
    }
};

pub const DataPageResult = struct {
    header: schema.PageHeader,
    data: []u8,
    owns_data: bool,
    allocator: std.mem.Allocator,

    pub fn deinit(self: *DataPageResult) void {
        if (self.owns_data) {
            self.allocator.free(self.data);
        }
    }
};

/// Calculate bit width needed to represent a value
fn bitWidth(max_value: u8) u8 {
    if (max_value == 0) return 0;
    return @intCast(std.math.log2_int(u8, max_value) + 1);
}

// ============================================================================
// Column Writer - high-level column encoding
// ============================================================================

pub const ColumnWriter = struct {
    allocator: std.mem.Allocator,
    page_writer: PageWriter,
    column_type: schema.Type,
    compression: schema.CompressionCodec,

    // Accumulated pages
    pages: std.ArrayListUnmanaged(PageData),

    // Stats
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    num_values: i64,

    // Dictionary encoder (optional)
    string_dict: ?encoder.StringDictEncoder,
    int32_dict: ?encoder.DictEncoder(i32),
    int64_dict: ?encoder.DictEncoder(i64),

    pub const PageData = struct {
        header: schema.PageHeader,
        data: []u8,
        owns_data: bool,
    };

    pub fn init(
        allocator: std.mem.Allocator,
        column_type: schema.Type,
        compression: schema.CompressionCodec,
    ) ColumnWriter {
        return .{
            .allocator = allocator,
            .page_writer = PageWriter.init(allocator, compression),
            .column_type = column_type,
            .compression = compression,
            .pages = .{},
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .num_values = 0,
            .string_dict = null,
            .int32_dict = null,
            .int64_dict = null,
        };
    }

    pub fn deinit(self: *ColumnWriter) void {
        self.page_writer.deinit();
        for (self.pages.items) |*page| {
            if (page.owns_data) {
                self.allocator.free(page.data);
            }
        }
        self.pages.deinit(self.allocator);
        if (self.string_dict) |*d| d.deinit();
        if (self.int32_dict) |*d| d.deinit();
        if (self.int64_dict) |*d| d.deinit();
    }

    /// Enable dictionary encoding for strings
    pub fn enableStringDict(self: *ColumnWriter) void {
        if (self.string_dict == null) {
            self.string_dict = encoder.StringDictEncoder.init(self.allocator);
        }
    }

    /// Write INT32 values with PLAIN encoding
    pub fn writeInt32Plain(self: *ColumnWriter, values: []const i32) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainInt32s(values);
        try self.flushPage(.PLAIN);
    }

    /// Write INT64 values with PLAIN encoding
    pub fn writeInt64Plain(self: *ColumnWriter, values: []const i64) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainInt64s(values);
        try self.flushPage(.PLAIN);
    }

    /// Write FLOAT values with PLAIN encoding
    pub fn writeFloatPlain(self: *ColumnWriter, values: []const f32) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainFloats(values);
        try self.flushPage(.PLAIN);
    }

    /// Write DOUBLE values with PLAIN encoding
    pub fn writeDoublePlain(self: *ColumnWriter, values: []const f64) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainDoubles(values);
        try self.flushPage(.PLAIN);
    }

    /// Write BYTE_ARRAY values with PLAIN encoding
    pub fn writeByteArrayPlain(self: *ColumnWriter, values: []const []const u8) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainByteArrays(values);
        try self.flushPage(.PLAIN);
    }

    /// Write BOOLEAN values with PLAIN encoding
    pub fn writeBooleanPlain(self: *ColumnWriter, values: []const bool) !void {
        self.page_writer.reset();
        try self.page_writer.writePlainBooleans(values);
        try self.flushPage(.PLAIN);
    }

    /// Write BYTE_ARRAY values with dictionary encoding
    pub fn writeByteArrayDict(self: *ColumnWriter, values: []const []const u8) !void {
        self.enableStringDict();
        var dict = &self.string_dict.?;

        // Add all values to dictionary
        try dict.putMany(values);
    }

    /// Flush dictionary-encoded column (call after all values added)
    pub fn flushDict(self: *ColumnWriter) !void {
        if (self.string_dict) |*dict| {
            // Write dictionary page first
            self.page_writer.reset();
            try dict.writeDictPage(&self.page_writer.plain_encoder);
            var dict_result = try self.page_writer.finalizeDictPage(@intCast(dict.numEntries()));
            try self.pages.append(self.allocator, .{
                .header = dict_result.header,
                .data = dict_result.data,
                .owns_data = dict_result.owns_data,
            });
            self.total_uncompressed_size += dict_result.header.uncompressed_page_size;
            self.total_compressed_size += dict_result.header.compressed_page_size;

            // Write data page with RLE indices
            self.page_writer.reset();
            const indices = dict.getIndices();
            const bit_width = dict.getBitWidth();
            try self.page_writer.writeDictIndices(indices, bit_width);
            self.num_values += @intCast(indices.len);

            var data_result = try self.page_writer.finalizeDataPage(.RLE_DICTIONARY);
            try self.pages.append(self.allocator, .{
                .header = data_result.header,
                .data = data_result.data,
                .owns_data = data_result.owns_data,
            });
            self.total_uncompressed_size += data_result.header.uncompressed_page_size;
            self.total_compressed_size += data_result.header.compressed_page_size;
        }
    }

    fn flushPage(self: *ColumnWriter, page_encoding: schema.Encoding) !void {
        var result = try self.page_writer.finalizeDataPage(page_encoding);
        self.num_values += self.page_writer.num_values;
        self.total_uncompressed_size += result.header.uncompressed_page_size;
        self.total_compressed_size += result.header.compressed_page_size;

        try self.pages.append(self.allocator, .{
            .header = result.header,
            .data = result.data,
            .owns_data = result.owns_data,
        });
    }

    /// Get all pages for writing to file
    pub fn getPages(self: *const ColumnWriter) []const PageData {
        return self.pages.items;
    }

    /// Write all pages to a file and return metadata
    pub fn writeToFile(
        self: *ColumnWriter,
        file: std.fs.File,
        path_in_schema: []const []const u8,
        start_offset: u64,
    ) !schema.ColumnChunk {
        var thrift_writer = thrift.Writer.init(self.allocator);
        defer thrift_writer.deinit();

        var data_page_offset: ?i64 = null;
        var dict_page_offset: ?i64 = null;
        var current_offset = start_offset;

        // Track actual total sizes (including page headers)
        var actual_compressed_size: i64 = 0;
        var actual_uncompressed_size: i64 = 0;

        // Determine encoding
        var encodings = std.ArrayListUnmanaged(schema.Encoding){};
        errdefer encodings.deinit(self.allocator);

        for (self.pages.items) |page| {
            // Write page header
            thrift_writer.reset();
            try page.header.write(&thrift_writer);
            const header_bytes = thrift_writer.bytes();
            try file.writeAll(header_bytes);

            // Track offsets
            if (page.header.type == .DICTIONARY_PAGE) {
                dict_page_offset = @intCast(current_offset);
                try encodings.append(self.allocator, .PLAIN);
            } else if (page.header.type == .DATA_PAGE) {
                if (data_page_offset == null) {
                    data_page_offset = @intCast(current_offset);
                }
                if (page.header.data_page_header) |dph| {
                    try encodings.append(self.allocator, dph.encoding);
                }
            }
            current_offset += header_bytes.len;

            // Track sizes: header + data
            actual_compressed_size += @intCast(header_bytes.len);
            actual_compressed_size += page.header.compressed_page_size;
            actual_uncompressed_size += @intCast(header_bytes.len);
            actual_uncompressed_size += page.header.uncompressed_page_size;

            // Write page data
            try file.writeAll(page.data);
            current_offset += page.data.len;
        }

        // Build path_in_schema
        var path = std.ArrayListUnmanaged([]const u8){};
        errdefer path.deinit(self.allocator);
        for (path_in_schema) |p| {
            try path.append(self.allocator, p);
        }

        return schema.ColumnChunk{
            .file_path = null,
            .file_offset = @intCast(start_offset),
            .meta_data = schema.ColumnMetaData{
                .type = self.column_type,
                .encodings = encodings,
                .path_in_schema = path,
                .codec = self.compression,
                .num_values = self.num_values,
                .total_uncompressed_size = actual_uncompressed_size,
                .total_compressed_size = actual_compressed_size,
                .data_page_offset = data_page_offset orelse @intCast(start_offset),
                .index_page_offset = null,
                .dictionary_page_offset = dict_page_offset,
            },
        };
    }
};

// ============================================================================
// Tests
// ============================================================================

test "PageWriter - plain int32" {
    var pw = PageWriter.init(std.testing.allocator, .UNCOMPRESSED);
    defer pw.deinit();

    const values = [_]i32{ 1, 2, 3, 4, 5 };
    try pw.writePlainInt32s(&values);

    var result = try pw.finalizeDataPage(.PLAIN);
    defer result.deinit();

    try std.testing.expectEqual(@as(i32, 20), result.header.uncompressed_page_size);
    try std.testing.expectEqual(@as(i32, 5), result.header.data_page_header.?.num_values);
}

test "ColumnWriter - plain int32" {
    var cw = ColumnWriter.init(std.testing.allocator, .INT32, .UNCOMPRESSED);
    defer cw.deinit();

    const values = [_]i32{ 10, 20, 30, 40, 50 };
    try cw.writeInt32Plain(&values);

    try std.testing.expectEqual(@as(usize, 1), cw.pages.items.len);
    try std.testing.expectEqual(@as(i64, 5), cw.num_values);
}

test "ColumnWriter - dict strings" {
    var cw = ColumnWriter.init(std.testing.allocator, .BYTE_ARRAY, .UNCOMPRESSED);
    defer cw.deinit();

    const values = [_][]const u8{ "apple", "banana", "apple", "cherry", "banana" };
    try cw.writeByteArrayDict(&values);
    try cw.flushDict();

    // Should have 2 pages: dictionary + data
    try std.testing.expectEqual(@as(usize, 2), cw.pages.items.len);
    try std.testing.expectEqual(schema.PageHeader.PageType.DICTIONARY_PAGE, cw.pages.items[0].header.type);
    try std.testing.expectEqual(schema.PageHeader.PageType.DATA_PAGE, cw.pages.items[1].header.type);
}
