const std = @import("std");
const thrift = @import("thrift.zig");

pub const Type = enum(i32) {
    BOOLEAN = 0,
    INT32 = 1,
    INT64 = 2,
    INT96 = 3,
    FLOAT = 4,
    DOUBLE = 5,
    BYTE_ARRAY = 6,
    FIXED_LEN_BYTE_ARRAY = 7,
};

pub const Encoding = enum(i32) {
    PLAIN = 0,
    PLAIN_DICTIONARY = 2,
    RLE = 3,
    BIT_PACKED = 4,
    DELTA_BINARY_PACKED = 5,
    DELTA_LENGTH_BYTE_ARRAY = 6,
    DELTA_BYTE_ARRAY = 7,
    RLE_DICTIONARY = 8,
    BYTE_STREAM_SPLIT = 9,
};

pub const CompressionCodec = enum(i32) {
    UNCOMPRESSED = 0,
    SNAPPY = 1,
    GZIP = 2,
    LZO = 3,
    BROTLI = 4,
    LZ4 = 5,
    ZSTD = 6,
    LZ4_RAW = 7,
};

pub const PageType = enum(i32) {
    DATA_PAGE = 0,
    INDEX_PAGE = 1,
    DICTIONARY_PAGE = 2,
    DATA_PAGE_V2 = 3,
};

// Aliases for ArrayLists
const EncodingList = std.ArrayListUnmanaged(Encoding);
const StringList = std.ArrayListUnmanaged([]const u8);

pub const FieldRepetitionType = enum(i32) {
    REQUIRED = 0,
    OPTIONAL = 1,
    REPEATED = 2,
};

pub const SchemaElement = struct {
    type: ?Type,
    type_length: ?i32,
    repetition_type: ?FieldRepetitionType,
    name: []const u8,
    num_children: ?i32,
    scale: ?i32,
    precision: ?i32,
    field_id: ?i32,

    pub fn read(reader: *thrift.Reader) !SchemaElement {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var elem = SchemaElement{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "",
            .num_children = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => elem.type = @as(Type, @enumFromInt(try reader.readZigZag(i32))),
                2 => elem.type_length = try reader.readZigZag(i32),
                3 => elem.repetition_type = @as(FieldRepetitionType, @enumFromInt(try reader.readZigZag(i32))),
                4 => elem.name = try reader.readString(),
                5 => elem.num_children = try reader.readZigZag(i32),
                6 => try reader.skip(field.type), // converted_type
                7 => elem.scale = try reader.readZigZag(i32),
                8 => elem.precision = try reader.readZigZag(i32),
                9 => elem.field_id = try reader.readZigZag(i32),
                else => try reader.skip(field.type),
            }
        }
        return elem;
    }

    pub fn write(self: *const SchemaElement, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        if (self.type) |t| try writer.writeFieldI32(1, @intFromEnum(t));
        if (self.type_length) |v| try writer.writeFieldI32(2, v);
        if (self.repetition_type) |rt| try writer.writeFieldI32(3, @intFromEnum(rt));
        try writer.writeFieldString(4, self.name);
        if (self.num_children) |v| try writer.writeFieldI32(5, v);
        if (self.scale) |v| try writer.writeFieldI32(7, v);
        if (self.precision) |v| try writer.writeFieldI32(8, v);
        if (self.field_id) |v| try writer.writeFieldI32(9, v);
        try writer.writeStructEnd();
    }
};

pub const DataPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    definition_level_encoding: Encoding,
    repetition_level_encoding: Encoding,
    // statistics skipped for now

    pub fn read(reader: *thrift.Reader) !DataPageHeader {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var header = DataPageHeader{
            .num_values = 0,
            .encoding = .PLAIN,
            .definition_level_encoding = .PLAIN,
            .repetition_level_encoding = .PLAIN,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => header.num_values = try reader.readZigZag(i32),
                2 => header.encoding = @as(Encoding, @enumFromInt(try reader.readZigZag(i32))),
                3 => header.definition_level_encoding = @as(Encoding, @enumFromInt(try reader.readZigZag(i32))),
                4 => header.repetition_level_encoding = @as(Encoding, @enumFromInt(try reader.readZigZag(i32))),
                else => try reader.skip(field.type),
            }
        }
        return header;
    }

    pub fn write(self: *const DataPageHeader, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, self.num_values);
        try writer.writeFieldI32(2, @intFromEnum(self.encoding));
        try writer.writeFieldI32(3, @intFromEnum(self.definition_level_encoding));
        try writer.writeFieldI32(4, @intFromEnum(self.repetition_level_encoding));
        try writer.writeStructEnd();
    }
};

pub const DictionaryPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    is_sorted: ?bool,

    pub fn read(reader: *thrift.Reader) !DictionaryPageHeader {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var header = DictionaryPageHeader{
            .num_values = 0,
            .encoding = .PLAIN,
            .is_sorted = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => header.num_values = try reader.readZigZag(i32),
                2 => header.encoding = @as(Encoding, @enumFromInt(try reader.readZigZag(i32))),
                3 => header.is_sorted = (field.type == .True),
                else => try reader.skip(field.type),
            }
        }
        return header;
    }

    pub fn write(self: *const DictionaryPageHeader, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, self.num_values);
        try writer.writeFieldI32(2, @intFromEnum(self.encoding));
        if (self.is_sorted) |v| try writer.writeFieldBool(3, v);
        try writer.writeStructEnd();
    }
};

pub const PageHeader = struct {
    type: PageType,
    uncompressed_page_size: i32,
    compressed_page_size: i32,
    crc: ?i32,
    data_page_header: ?DataPageHeader,
    dictionary_page_header: ?DictionaryPageHeader,
    // v2 skipped for now

    pub fn read(reader: *thrift.Reader) !PageHeader {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var header = PageHeader{
            .type = .DATA_PAGE,
            .uncompressed_page_size = 0,
            .compressed_page_size = 0,
            .crc = null,
            .data_page_header = null,
            .dictionary_page_header = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => header.type = @as(PageType, @enumFromInt(try reader.readZigZag(i32))),
                2 => header.uncompressed_page_size = try reader.readZigZag(i32),
                3 => header.compressed_page_size = try reader.readZigZag(i32),
                4 => header.crc = try reader.readZigZag(i32),
                5 => header.data_page_header = try DataPageHeader.read(reader),
                7 => header.dictionary_page_header = try DictionaryPageHeader.read(reader),
                else => try reader.skip(field.type),
            }
        }
        return header;
    }

    pub fn write(self: *const PageHeader, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, @intFromEnum(self.type));
        try writer.writeFieldI32(2, self.uncompressed_page_size);
        try writer.writeFieldI32(3, self.compressed_page_size);
        if (self.crc) |v| try writer.writeFieldI32(4, v);
        if (self.data_page_header) |*dph| {
            try writer.writeFieldBegin(.Struct, 5);
            try dph.write(writer);
        }
        if (self.dictionary_page_header) |*dph| {
            try writer.writeFieldBegin(.Struct, 7);
            try dph.write(writer);
        }
        try writer.writeStructEnd();
    }
};

pub const ColumnMetaData = struct {
    type: Type,
    encodings: EncodingList,
    path_in_schema: StringList,
    codec: CompressionCodec,
    num_values: i64,
    total_uncompressed_size: i64,
    total_compressed_size: i64,
    data_page_offset: i64,
    index_page_offset: ?i64,
    dictionary_page_offset: ?i64,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnMetaData {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var meta = ColumnMetaData{
            .type = .INT32,
            .encodings = .{},
            .path_in_schema = .{},
            .codec = .UNCOMPRESSED,
            .num_values = 0,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
        };
        errdefer meta.encodings.deinit(allocator);
        errdefer meta.path_in_schema.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => meta.type = @as(Type, @enumFromInt(try reader.readZigZag(i32))),
                2 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.encodings.append(allocator, @as(Encoding, @enumFromInt(try reader.readZigZag(i32))));
                    }
                },
                3 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.path_in_schema.append(allocator, try reader.readString());
                    }
                },
                4 => meta.codec = @as(CompressionCodec, @enumFromInt(try reader.readZigZag(i32))),
                5 => meta.num_values = try reader.readZigZag(i64),
                6 => meta.total_uncompressed_size = try reader.readZigZag(i64),
                7 => meta.total_compressed_size = try reader.readZigZag(i64),
                9 => meta.data_page_offset = try reader.readZigZag(i64),
                10 => meta.index_page_offset = try reader.readZigZag(i64),
                11 => meta.dictionary_page_offset = try reader.readZigZag(i64),
                else => try reader.skip(field.type),
            }
        }
        return meta;
    }

    pub fn deinit(self: *ColumnMetaData, allocator: std.mem.Allocator) void {
        self.encodings.deinit(allocator);
        // Note: strings in path_in_schema are slices into the footer buffer, not owned.
        self.path_in_schema.deinit(allocator);
    }

    pub fn write(self: *const ColumnMetaData, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, @intFromEnum(self.type));
        // Field 2: encodings list
        try writer.writeFieldListBegin(2, .I32, self.encodings.items.len);
        for (self.encodings.items) |enc| {
            try writer.writeZigZag(@as(i32, @intFromEnum(enc)));
        }
        // Field 3: path_in_schema list
        try writer.writeFieldListBegin(3, .Binary, self.path_in_schema.items.len);
        for (self.path_in_schema.items) |path| {
            try writer.writeString(path);
        }
        try writer.writeFieldI32(4, @intFromEnum(self.codec));
        try writer.writeFieldI64(5, self.num_values);
        try writer.writeFieldI64(6, self.total_uncompressed_size);
        try writer.writeFieldI64(7, self.total_compressed_size);
        try writer.writeFieldI64(9, self.data_page_offset);
        if (self.index_page_offset) |v| try writer.writeFieldI64(10, v);
        if (self.dictionary_page_offset) |v| try writer.writeFieldI64(11, v);
        try writer.writeStructEnd();
    }
};

pub const ColumnChunk = struct {
    file_path: ?[]const u8,
    file_offset: i64,
    meta_data: ?ColumnMetaData,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnChunk {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var chunk = ColumnChunk{
            .file_path = null,
            .file_offset = 0,
            .meta_data = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => chunk.file_path = try reader.readString(),
                2 => chunk.file_offset = try reader.readZigZag(i64),
                3 => chunk.meta_data = try ColumnMetaData.read(allocator, reader),
                else => try reader.skip(field.type),
            }
        }
        return chunk;
    }

    pub fn deinit(self: *ColumnChunk, allocator: std.mem.Allocator) void {
        // file_path is a slice into footer buffer.
        if (self.meta_data) |*m| m.deinit(allocator);
    }

    pub fn write(self: *const ColumnChunk, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        if (self.file_path) |fp| try writer.writeFieldString(1, fp);
        try writer.writeFieldI64(2, self.file_offset);
        if (self.meta_data) |*md| {
            try writer.writeFieldBegin(.Struct, 3);
            try md.write(writer);
        }
        try writer.writeStructEnd();
    }
};

pub const RowGroup = struct {
    columns: std.ArrayListUnmanaged(ColumnChunk),
    total_byte_size: i64,
    num_rows: i64,
    // sorting_columns skipped

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !RowGroup {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var rg = RowGroup{
            .columns = .{},
            .total_byte_size = 0,
            .num_rows = 0,
        };
        errdefer rg.columns.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        const col = try ColumnChunk.read(allocator, reader);
                        try rg.columns.append(allocator, col);
                    }
                },
                2 => rg.total_byte_size = try reader.readZigZag(i64),
                3 => rg.num_rows = try reader.readZigZag(i64),
                else => try reader.skip(field.type),
            }
        }
        return rg;
    }

    pub fn deinit(self: *RowGroup, allocator: std.mem.Allocator) void {
        for (self.columns.items) |*chunk| {
            chunk.deinit(allocator);
        }
        self.columns.deinit(allocator);
    }

    pub fn write(self: *const RowGroup, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        // Field 1: columns list
        try writer.writeFieldListBegin(1, .Struct, self.columns.items.len);
        for (self.columns.items) |*col| {
            try col.write(writer);
        }
        try writer.writeFieldI64(2, self.total_byte_size);
        try writer.writeFieldI64(3, self.num_rows);
        try writer.writeStructEnd();
    }
};

pub const FileMetaData = struct {
    version: i32,
    schema: std.ArrayListUnmanaged(SchemaElement),
    num_rows: i64,
    created_by: ?[]const u8,
    row_groups: std.ArrayListUnmanaged(RowGroup),

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !FileMetaData {
        const saved_id = reader.last_field_id;
        defer reader.last_field_id = saved_id;

        var meta = FileMetaData{
            .version = 0,
            .schema = .{},
            .num_rows = 0,
            .created_by = null,
            .row_groups = .{},
        };
        errdefer meta.schema.deinit(allocator);
        errdefer meta.row_groups.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => meta.version = try reader.readZigZag(i32),
                2 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.schema.append(allocator, try SchemaElement.read(reader));
                    }
                },
                3 => meta.num_rows = try reader.readZigZag(i64),
                4 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.row_groups.append(allocator, try RowGroup.read(allocator, reader));
                    }
                },
                6 => meta.created_by = try reader.readString(),
                else => try reader.skip(field.type),
            }
        }
        return meta;
    }

    pub fn deinit(self: *FileMetaData, allocator: std.mem.Allocator) void {
        self.schema.deinit(allocator);
        for (self.row_groups.items) |*rg| {
            rg.deinit(allocator);
        }
        self.row_groups.deinit(allocator);
        // Note: created_by is a slice into the footer buffer.
    }

    pub fn write(self: *const FileMetaData, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, self.version);
        // Field 2: schema list
        try writer.writeFieldListBegin(2, .Struct, self.schema.items.len);
        for (self.schema.items) |*elem| {
            try elem.write(writer);
        }
        try writer.writeFieldI64(3, self.num_rows);
        // Field 4: row_groups list
        try writer.writeFieldListBegin(4, .Struct, self.row_groups.items.len);
        for (self.row_groups.items) |*rg| {
            try rg.write(writer);
        }
        if (self.created_by) |cb| try writer.writeFieldString(6, cb);
        try writer.writeStructEnd();
    }

    pub fn getColumnLevels(self: *const FileMetaData, path: []const []const u8) Levels {
        var iter = SchemaIterator{ .items = self.schema.items, .pos = 0 };
        return iter.find(path) catch .{ .max_def = 0, .max_rep = 0 };
    }
};

const SchemaIterator = struct {
    items: []const SchemaElement,
    pos: usize,

    fn find(self: *SchemaIterator, path: []const []const u8) !Levels {
        if (self.pos >= self.items.len) return error.NotFound;

        // Consume root
        const root = self.items[self.pos];
        self.pos += 1;

        const num_children = root.num_children orelse 0;
        var i: i32 = 0;
        while (i < num_children) : (i += 1) {
            if (try self.visit(path, 0, 0, 0)) |res| return res;
        }
        return error.NotFound;
    }

    fn visit(self: *SchemaIterator, target_path: []const []const u8, depth: usize, current_def: i32, current_rep: i32) !?Levels {
        if (self.pos >= self.items.len) return null;
        const elem = self.items[self.pos];
        self.pos += 1;

        var def = current_def;
        var rep = current_rep;

        if (elem.repetition_type) |rt| {
            if (rt == .OPTIONAL) {
                def += 1;
            } else if (rt == .REPEATED) {
                def += 1;
                rep += 1;
            }
        }

        if (std.mem.eql(u8, elem.name, target_path[depth])) {
            if (depth == target_path.len - 1) {
                return Levels{ .max_def = def, .max_rep = rep };
            } else {
                const num_children = elem.num_children orelse 0;
                var i: i32 = 0;
                while (i < num_children) : (i += 1) {
                    if (try self.visit(target_path, depth + 1, def, rep)) |res| return res;
                }
            }
        } else {
            // Not the node we're looking for, skip its children
            const num_children = elem.num_children orelse 0;
            var i: i32 = 0;
            while (i < num_children) : (i += 1) {
                _ = try self.skip();
            }
        }
        return null;
    }

    fn skip(self: *SchemaIterator) !void {
        if (self.pos >= self.items.len) return;
        const elem = self.items[self.pos];
        self.pos += 1;
        const num_children = elem.num_children orelse 0;
        var i: i32 = 0;
        while (i < num_children) : (i += 1) {
            try self.skip();
        }
    }
};

pub const Levels = struct {
    max_def: i32,
    max_rep: i32,
};

test "schema element roundtrip" {
    const allocator = std.testing.allocator;

    const elem = SchemaElement{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "test_column",
        .num_children = null,
        .scale = 2,
        .precision = 10,
        .field_id = 42,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try elem.write(&writer);

    var reader = thrift.Reader.init(writer.bytes());
    const decoded = try SchemaElement.read(&reader);

    try std.testing.expectEqual(elem.type, decoded.type);
    try std.testing.expectEqual(elem.repetition_type, decoded.repetition_type);
    try std.testing.expectEqualStrings(elem.name, decoded.name);
    try std.testing.expectEqual(elem.scale, decoded.scale);
    try std.testing.expectEqual(elem.precision, decoded.precision);
    try std.testing.expectEqual(elem.field_id, decoded.field_id);
}

test "page header roundtrip" {
    const allocator = std.testing.allocator;

    const header = PageHeader{
        .type = .DATA_PAGE,
        .uncompressed_page_size = 4096,
        .compressed_page_size = 2048,
        .crc = 0x12345678,
        .data_page_header = DataPageHeader{
            .num_values = 1000,
            .encoding = .RLE_DICTIONARY,
            .definition_level_encoding = .RLE,
            .repetition_level_encoding = .RLE,
        },
        .dictionary_page_header = null,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try header.write(&writer);

    var reader = thrift.Reader.init(writer.bytes());
    const decoded = try PageHeader.read(&reader);

    try std.testing.expectEqual(header.type, decoded.type);
    try std.testing.expectEqual(header.uncompressed_page_size, decoded.uncompressed_page_size);
    try std.testing.expectEqual(header.compressed_page_size, decoded.compressed_page_size);
    try std.testing.expectEqual(header.crc, decoded.crc);
    try std.testing.expect(decoded.data_page_header != null);
    try std.testing.expectEqual(header.data_page_header.?.num_values, decoded.data_page_header.?.num_values);
    try std.testing.expectEqual(header.data_page_header.?.encoding, decoded.data_page_header.?.encoding);
}
