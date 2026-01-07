const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const snappy = @import("snappy.zig");
const sink_mod = @import("../io/sink.zig");

pub const WriterError = error{
    IoError,
    OutOfMemory,
    NotImplemented,
    EncodingError,
} || sink_mod.Sink.VTable.WriteError || snappy.CompressError;

const ColumnWriter = struct {
    allocator: std.mem.Allocator,
    type: schema.Type,
    values_buffer: std.ArrayListUnmanaged(u8),
    count: usize,
    path: []const []const u8,

    pub fn init(allocator: std.mem.Allocator, col_type: schema.Type, path: []const []const u8) ColumnWriter {
        return .{
            .allocator = allocator,
            .type = col_type,
            .values_buffer = .{},
            .count = 0,
            .path = path,
        };
    }

    pub fn deinit(self: *ColumnWriter) void {
        self.values_buffer.deinit(self.allocator);
    }

    pub fn append(self: *ColumnWriter, value: anytype) !void {
        const T = @TypeOf(value);
        switch (T) {
            i32 => {
                if (self.type != .INT32) return error.EncodingError;
                var buf: [4]u8 = undefined;
                std.mem.writeInt(i32, &buf, value, .little);
                try self.values_buffer.appendSlice(self.allocator, &buf);
            },
            i64 => {
                if (self.type != .INT64) return error.EncodingError;
                var buf: [8]u8 = undefined;
                std.mem.writeInt(i64, &buf, value, .little);
                try self.values_buffer.appendSlice(self.allocator, &buf);
            },
            []const u8 => {
                if (self.type != .BYTE_ARRAY) return error.EncodingError;
                const s: []const u8 = value;
                var len_buf: [4]u8 = undefined;
                std.mem.writeInt(u32, &len_buf, @intCast(s.len), .little);
                try self.values_buffer.appendSlice(self.allocator, &len_buf);
                try self.values_buffer.appendSlice(self.allocator, s);
            },
            else => return error.NotImplemented,
        }
        self.count += 1;
    }

    pub fn flushPage(self: *ColumnWriter, writer: *ParquetWriter) !PageResult {
        if (self.count == 0) return PageResult{};

        const uncompressed_data = self.values_buffer.items;
        const compressed_data = try snappy.compressAlloc(self.allocator, uncompressed_data);
        defer self.allocator.free(compressed_data);

        // Serialize PageHeader using thrift.Writer
        var ph_writer = thrift.Writer.init(self.allocator);
        defer ph_writer.deinit();

        const ph = schema.PageHeader{
            .type = .DATA_PAGE,
            .uncompressed_page_size = @intCast(uncompressed_data.len),
            .compressed_page_size = @intCast(compressed_data.len),
            .crc = null,
            .data_page_header = schema.DataPageHeader{
                .num_values = @intCast(self.count),
                .encoding = .PLAIN,
                .definition_level_encoding = .RLE,
                .repetition_level_encoding = .RLE,
            },
            .dictionary_page_header = null,
        };
        try ph.write(&ph_writer);

        const page_header_bytes = ph_writer.bytes();

        try writer.writeSink(page_header_bytes);
        try writer.writeSink(compressed_data);

        const res = PageResult{
            .total_compressed = @intCast(page_header_bytes.len + compressed_data.len),
            .total_uncompressed = @intCast(page_header_bytes.len + uncompressed_data.len),
            .data_uncompressed = @intCast(uncompressed_data.len),
            .data_compressed = @intCast(compressed_data.len),
            .num_values = @intCast(self.count),
        };

        self.values_buffer.clearRetainingCapacity();
        self.count = 0;
        return res;
    }
};

const PageResult = struct {
    total_compressed: i64 = 0,
    total_uncompressed: i64 = 0,
    data_uncompressed: i64 = 0,
    data_compressed: i64 = 0,
    num_values: i64 = 0,
};

pub const ParquetWriter = struct {
    allocator: std.mem.Allocator,
    sink: sink_mod.Sink,
    schema_elements: std.ArrayListUnmanaged(schema.SchemaElement),
    columns: []ColumnWriter,
    row_groups: std.ArrayListUnmanaged(schema.RowGroup),
    total_rows: i64 = 0,
    offset: i64 = 0,
    path_strings: std.ArrayListUnmanaged([]const u8),

    pub fn init(allocator: std.mem.Allocator, sink: sink_mod.Sink, schema_elems: []const schema.SchemaElement) !*ParquetWriter {
        const self = try allocator.create(ParquetWriter);
        var schema_list = std.ArrayListUnmanaged(schema.SchemaElement){};
        try schema_list.appendSlice(allocator, schema_elems);

        const cols = try allocator.alloc(ColumnWriter, schema_elems.len - 1);
        var path_strings = std.ArrayListUnmanaged([]const u8){};

        for (cols, 0..) |*c, i| {
            const elem = schema_elems[i + 1];
            const name_copy = try allocator.dupe(u8, elem.name);
            try path_strings.append(allocator, name_copy);

            const path_slice = try allocator.alloc([]const u8, 1);
            path_slice[0] = name_copy;

            c.* = ColumnWriter.init(allocator, elem.type.?, path_slice);
        }

        self.* = .{
            .allocator = allocator,
            .sink = sink,
            .schema_elements = schema_list,
            .columns = cols,
            .row_groups = .{},
            .path_strings = path_strings,
        };

        try self.writeSink("PAR1");
        return self;
    }

    pub fn deinit(self: *ParquetWriter) void {
        self.schema_elements.deinit(self.allocator);
        for (self.columns) |*c| {
            self.allocator.free(c.path);
            c.deinit();
        }
        self.allocator.free(self.columns);
        for (self.path_strings.items) |s| self.allocator.free(s);
        self.path_strings.deinit(self.allocator);
        for (self.row_groups.items) |*rg| rg.deinit(self.allocator);
        self.row_groups.deinit(self.allocator);
        self.allocator.destroy(self);
    }

    pub fn appendRow(self: *ParquetWriter, values: anytype) !void {
        inline for (values, 0..) |v, i| {
            if (i < self.columns.len) {
                try self.columns[i].append(v);
            }
        }
        self.total_rows += 1;
    }

    pub fn flushRowGroup(self: *ParquetWriter) !void {
        if (self.columns[0].count == 0) return;

        const num_rows = self.columns[0].count;
        var rg_cols = std.ArrayListUnmanaged(schema.ColumnChunk){};
        var total_rg_size: i64 = 0;

        for (self.columns) |*col| {
            const start_offset = self.offset;
            const res = try col.flushPage(self);

            var meta = schema.ColumnMetaData{
                .type = col.type,
                .encodings = .{},
                .path_in_schema = .{},
                .codec = .SNAPPY,
                .num_values = res.num_values,
                .total_uncompressed_size = res.data_uncompressed,
                .total_compressed_size = res.data_compressed,
                .data_page_offset = start_offset,
                .index_page_offset = null,
                .dictionary_page_offset = null,
                .statistics = null,
            };

            try meta.encodings.append(self.allocator, .PLAIN);
            for (col.path) |p| try meta.path_in_schema.append(self.allocator, p);

            const chunk = schema.ColumnChunk{
                .file_path = null,
                .file_offset = start_offset,
                .meta_data = meta,
            };

            try rg_cols.append(self.allocator, chunk);
            total_rg_size += res.total_compressed;
        }

        const rg = schema.RowGroup{
            .columns = rg_cols,
            .total_byte_size = total_rg_size,
            .num_rows = @intCast(num_rows),
        };
        try self.row_groups.append(self.allocator, rg);
    }

    pub fn close(self: *ParquetWriter) !void {
        try self.flushRowGroup();

        var file_meta = schema.FileMetaData{
            .version = 1,
            .schema = self.schema_elements,
            .num_rows = self.total_rows,
            .created_by = "zpq-0.1",
            .row_groups = self.row_groups,
        };

        var fw = thrift.Writer.init(self.allocator);
        defer fw.deinit();

        try file_meta.write(&fw);
        const footer_bytes = fw.bytes();
        try self.writeSink(footer_bytes);

        const footer_len: u32 = @intCast(footer_bytes.len);
        var len_buf: [4]u8 = undefined;
        std.mem.writeInt(u32, &len_buf, footer_len, .little);
        try self.writeSink(&len_buf);
        try self.writeSink("PAR1");

        try self.sink.close();
    }

    fn writeSink(self: *ParquetWriter, data: []const u8) !void {
        _ = try self.sink.write(data);
        self.offset += @intCast(data.len);
    }
};
