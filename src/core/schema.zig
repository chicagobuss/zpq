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

pub const ConvertedType = enum(i32) {
    UTF8 = 0,
    MAP = 1,
    MAP_KEY_VALUE = 2,
    LIST = 3,
    ENUM = 4,
    DECIMAL = 5,
    DATE = 6,
    TIME_MILLIS = 7,
    TIME_MICROS = 8,
    TIMESTAMP_MILLIS = 9,
    TIMESTAMP_MICROS = 10,
    UINT_8 = 11,
    UINT_16 = 12,
    UINT_32 = 13,
    UINT_64 = 14,
    INT_8 = 15,
    INT_16 = 16,
    INT_32 = 17,
    INT_64 = 18,
    JSON = 19,
    BSON = 20,
    INTERVAL = 21,
};

pub const TimeUnit = union(enum) {
    MILLIS: struct {},
    MICROS: struct {},
    NANOS: struct {},

    pub fn read(reader: *thrift.Reader) !TimeUnit {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var unit: TimeUnit = .{ .MILLIS = .{} };
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => {
                    try reader.skip(.Struct);
                    unit = .{ .MILLIS = .{} };
                },
                2 => {
                    try reader.skip(.Struct);
                    unit = .{ .MICROS = .{} };
                },
                3 => {
                    try reader.skip(.Struct);
                    unit = .{ .NANOS = .{} };
                },
                else => try reader.skip(field.type),
            }
        }
        return unit;
    }

    pub fn write(self: TimeUnit, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        switch (self) {
            .MILLIS => {
                try writer.writeFieldBegin(.Struct, 1);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .MICROS => {
                try writer.writeFieldBegin(.Struct, 2);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .NANOS => {
                try writer.writeFieldBegin(.Struct, 3);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
        }
        try writer.writeStructEnd();
    }
};

pub const IntType = struct { bitWidth: i8, isSigned: bool };
pub const DecimalType = struct { scale: i32, precision: i32 };
pub const TimestampType = struct { isAdjustedToUTC: bool, unit: TimeUnit };
pub const TimeType = struct { isAdjustedToUTC: bool, unit: TimeUnit };

pub const LogicalType = union(enum) {
    STRING: struct {},
    MAP: struct {},
    LIST: struct {},
    ENUM: struct {},
    DECIMAL: DecimalType,
    DATE: struct {},
    TIME: TimeType,
    TIMESTAMP: TimestampType,
    INTEGER: IntType,
    UNKNOWN: struct {},
    JSON: struct {},
    BSON: struct {},
    UUID: struct {},
    FLOAT16: struct {},

    pub fn read(reader: *thrift.Reader) !LogicalType {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var lt: LogicalType = .{ .UNKNOWN = .{} };
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => {
                    try reader.skip(.Struct);
                    lt = .{ .STRING = .{} };
                },
                2 => {
                    try reader.skip(.Struct);
                    lt = .{ .MAP = .{} };
                },
                3 => {
                    try reader.skip(.Struct);
                    lt = .{ .LIST = .{} };
                },
                4 => {
                    try reader.skip(.Struct);
                    lt = .{ .ENUM = .{} };
                },
                5 => lt = .{ .DECIMAL = try readDecimal(reader) },
                6 => {
                    try reader.skip(.Struct);
                    lt = .{ .DATE = .{} };
                },
                7 => lt = .{ .TIME = try readTime(reader) },
                8 => lt = .{ .TIMESTAMP = try readTimestamp(reader) },
                10 => lt = .{ .INTEGER = try readInteger(reader) },
                11 => {
                    try reader.skip(.Struct);
                    lt = .{ .UNKNOWN = .{} };
                },
                12 => {
                    try reader.skip(.Struct);
                    lt = .{ .JSON = .{} };
                },
                13 => {
                    try reader.skip(.Struct);
                    lt = .{ .BSON = .{} };
                },
                14 => {
                    try reader.skip(.Struct);
                    lt = .{ .UUID = .{} };
                },
                15 => {
                    try reader.skip(.Struct);
                    lt = .{ .FLOAT16 = .{} };
                },
                else => try reader.skip(field.type),
            }
        }
        return lt;
    }

    fn readDecimal(reader: *thrift.Reader) !DecimalType {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;
        var res: DecimalType = .{ .scale = 0, .precision = 0 };
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => res.scale = try reader.readZigZag(i32),
                2 => res.precision = try reader.readZigZag(i32),
                else => try reader.skip(field.type),
            }
        }
        return res;
    }

    fn readTime(reader: *thrift.Reader) !TimeType {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;
        var res: TimeType = .{ .isAdjustedToUTC = false, .unit = undefined };
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => res.isAdjustedToUTC = (field.type == .True),
                2 => res.unit = try TimeUnit.read(reader),
                else => try reader.skip(field.type),
            }
        }
        return res;
    }

    fn readTimestamp(reader: *thrift.Reader) !TimestampType {
        const res = try readTime(reader);
        return TimestampType{
            .isAdjustedToUTC = res.isAdjustedToUTC,
            .unit = res.unit,
        };
    }

    fn readInteger(reader: *thrift.Reader) !IntType {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;
        var res: IntType = .{ .bitWidth = 0, .isSigned = false };
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => res.bitWidth = if (field.type == .Byte)
                    @as(i8, @bitCast(try reader.readByte()))
                else
                    @as(i8, @intCast(try reader.readZigZag(i16))),
                2 => res.isSigned = (field.type == .True),
                else => try reader.skip(field.type),
            }
        }
        return res;
    }

    pub fn write(self: LogicalType, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        switch (self) {
            .STRING => {
                try writer.writeFieldBegin(.Struct, 1);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .MAP => {
                try writer.writeFieldBegin(.Struct, 2);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .LIST => {
                try writer.writeFieldBegin(.Struct, 3);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .ENUM => {
                try writer.writeFieldBegin(.Struct, 4);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .DECIMAL => |d| {
                try writer.writeFieldBegin(.Struct, 5);
                writer.writeStructBegin();
                try writer.writeFieldI32(1, d.scale);
                try writer.writeFieldI32(2, d.precision);
                try writer.writeStructEnd();
            },
            .DATE => {
                try writer.writeFieldBegin(.Struct, 6);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .TIME => |t| {
                try writer.writeFieldBegin(.Struct, 7);
                writer.writeStructBegin();
                try writer.writeFieldBool(1, t.isAdjustedToUTC);
                try writer.writeFieldBegin(.Struct, 2);
                try t.unit.write(writer);
                try writer.writeStructEnd();
            },
            .TIMESTAMP => |t| {
                try writer.writeFieldBegin(.Struct, 8);
                writer.writeStructBegin();
                try writer.writeFieldBool(1, t.isAdjustedToUTC);
                try writer.writeFieldBegin(.Struct, 2);
                try t.unit.write(writer);
                try writer.writeStructEnd();
            },
            .INTEGER => |i| {
                try writer.writeFieldBegin(.Struct, 10);
                writer.writeStructBegin();
                try writer.writeFieldI8(1, i.bitWidth);
                try writer.writeFieldBool(2, i.isSigned);
                try writer.writeStructEnd();
            },
            .UNKNOWN => {
                try writer.writeFieldBegin(.Struct, 11);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .JSON => {
                try writer.writeFieldBegin(.Struct, 12);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .BSON => {
                try writer.writeFieldBegin(.Struct, 13);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .UUID => {
                try writer.writeFieldBegin(.Struct, 14);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
            .FLOAT16 => {
                try writer.writeFieldBegin(.Struct, 15);
                writer.writeStructBegin();
                try writer.writeStructEnd();
            },
        }
        try writer.writeStructEnd();
    }
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
pub const EncodingList = std.ArrayListUnmanaged(Encoding);
pub const StringList = std.ArrayListUnmanaged([]const u8);

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
    converted_type: ?ConvertedType = null,
    logical_type: ?LogicalType = null,
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
            .converted_type = null,
            .logical_type = null,
            .scale = null,
            .precision = null,
            .field_id = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => elem.type = (std.enums.fromInt(Type, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                2 => elem.type_length = try reader.readZigZag(i32),
                3 => elem.repetition_type = (std.enums.fromInt(FieldRepetitionType, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                4 => elem.name = try reader.readString(),
                5 => elem.num_children = try reader.readZigZag(i32),
                6 => elem.converted_type = (std.enums.fromInt(ConvertedType, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                7 => elem.scale = try reader.readZigZag(i32),
                8 => elem.precision = try reader.readZigZag(i32),
                9 => elem.field_id = try reader.readZigZag(i32),
                10 => elem.logical_type = try LogicalType.read(reader),
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
        if (self.converted_type) |ct| try writer.writeFieldI32(6, @intFromEnum(ct));
        if (self.scale) |v| try writer.writeFieldI32(7, v);
        if (self.precision) |v| try writer.writeFieldI32(8, v);
        if (self.field_id) |v| try writer.writeFieldI32(9, v);
        if (self.logical_type) |*lt| {
            try writer.writeFieldBegin(.Struct, 10);
            try lt.write(writer);
        }
        try writer.writeStructEnd();
    }
};

/// True for an INT32-backed column annotated UNSIGNED at ≤32 bits
/// (`UINT_8/16/32` converted type, or `INTEGER{bitWidth<=32, isSigned=false}`).
/// Such a column's top bit is a value bit, not a sign bit, so decoding it — or
/// reading its `Statistics` min/max — as a signed `i32` turns `0xFFFFFFFF` into
/// `-1` and corrupts sum/min/max. Callers route these through the zero-extended
/// i64 lane instead. uint64 is intentionally excluded: a u64 (or its sum)
/// overflows i64, so it needs the wider-accumulator work tracked separately.
pub fn isUnsignedIntTo32(se: SchemaElement) bool {
    if (se.logical_type) |lt| switch (lt) {
        .INTEGER => |it| return !it.isSigned and it.bitWidth <= 32,
        else => {},
    };
    if (se.converted_type) |ct| switch (ct) {
        .UINT_8, .UINT_16, .UINT_32 => return true,
        else => {},
    };
    return false;
}

/// True for an unsigned 64-bit INT64 column (`UINT_64` converted type, or
/// `INTEGER{bitWidth=64, isSigned=false}`). Unlike ≤32-bit unsigned ints, a u64
/// can't zero-extend into the i64 lane, so it's carried as raw i64 bits and the
/// aggregate fold reinterprets them as u64 (widening to i128 for sum/min/max).
pub fn isUnsignedInt64(se: SchemaElement) bool {
    if (se.logical_type) |lt| switch (lt) {
        .INTEGER => |it| return !it.isSigned and it.bitWidth == 64,
        else => {},
    };
    if (se.converted_type) |ct| switch (ct) {
        .UINT_64 => return true,
        else => {},
    };
    return false;
}

/// Any unsigned integer column. Used to force the decode path (the stats
/// short-circuit reads min/max bytes in signed order).
pub fn isUnsignedInt(se: SchemaElement) bool {
    return isUnsignedIntTo32(se) or isUnsignedInt64(se);
}

/// FLOAT16 (IEEE half) column — physically FIXED_LEN_BYTE_ARRAY(2) annotated
/// with the FLOAT16 logical type. Decoded to f64 for numeric agg/filter/expr
/// (the same f64 lane decimals use), rather than left as raw bytes.
pub fn isFloat16(se: SchemaElement) bool {
    if (se.logical_type) |lt| return lt == .FLOAT16;
    return false;
}

pub const DataPageHeader = struct {
    num_values: i32,
    encoding: Encoding,
    definition_level_encoding: Encoding,
    repetition_level_encoding: Encoding,
    // Page-header statistics are not consumed by the current scan path.

    pub fn read(reader: *thrift.Reader) !DataPageHeader {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
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
                2 => header.encoding = (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                3 => header.definition_level_encoding = (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                4 => header.repetition_level_encoding = (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
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
        reader.last_field_id = 0;
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
                2 => header.encoding = (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
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

/// Modern data page header (page_type == DATA_PAGE_V2). Differs from
/// V1 in three load-bearing ways:
///   1. Levels are stored UNCOMPRESSED in the page body, with their
///      lengths reported in this header (not as `<u32 LE>` prefixes
///      inside the compressed payload).
///   2. Only the values portion is optionally compressed (per
///      `is_compressed`); levels are never compressed.
///   3. The header distinguishes leaves (`num_values`) from logical
///      rows (`num_rows`), and reports `num_nulls` directly.
pub const DataPageHeaderV2 = struct {
    num_values: i32,
    num_nulls: i32,
    num_rows: i32,
    encoding: Encoding,
    definition_levels_byte_length: i32,
    repetition_levels_byte_length: i32,
    is_compressed: bool, // default true per spec
    // Page-header statistics are not consumed by the current scan path.

    pub fn read(reader: *thrift.Reader) !DataPageHeaderV2 {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var header = DataPageHeaderV2{
            .num_values = 0,
            .num_nulls = 0,
            .num_rows = 0,
            .encoding = .PLAIN,
            .definition_levels_byte_length = 0,
            .repetition_levels_byte_length = 0,
            .is_compressed = true, // spec default
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => header.num_values = try reader.readZigZag(i32),
                2 => header.num_nulls = try reader.readZigZag(i32),
                3 => header.num_rows = try reader.readZigZag(i32),
                4 => header.encoding = (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                5 => header.definition_levels_byte_length = try reader.readZigZag(i32),
                6 => header.repetition_levels_byte_length = try reader.readZigZag(i32),
                7 => header.is_compressed = (field.type == .True),
                else => try reader.skip(field.type),
            }
        }
        return header;
    }

    pub fn write(self: *const DataPageHeaderV2, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI32(1, self.num_values);
        try writer.writeFieldI32(2, self.num_nulls);
        try writer.writeFieldI32(3, self.num_rows);
        try writer.writeFieldI32(4, @intFromEnum(self.encoding));
        try writer.writeFieldI32(5, self.definition_levels_byte_length);
        try writer.writeFieldI32(6, self.repetition_levels_byte_length);
        // is_compressed defaults to true; only emit when false.
        if (!self.is_compressed) try writer.writeFieldBool(7, false);
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
    data_page_header_v2: ?DataPageHeaderV2,

    pub fn read(reader: *thrift.Reader) !PageHeader {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var header = PageHeader{
            .type = .DATA_PAGE,
            .uncompressed_page_size = 0,
            .compressed_page_size = 0,
            .crc = null,
            .data_page_header = null,
            .dictionary_page_header = null,
            .data_page_header_v2 = null,
        };

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => header.type = (std.enums.fromInt(PageType, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                2 => header.uncompressed_page_size = try reader.readZigZag(i32),
                3 => header.compressed_page_size = try reader.readZigZag(i32),
                4 => header.crc = try reader.readZigZag(i32),
                5 => header.data_page_header = try DataPageHeader.read(reader),
                7 => header.dictionary_page_header = try DictionaryPageHeader.read(reader),
                8 => header.data_page_header_v2 = try DataPageHeaderV2.read(reader),
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
        if (self.data_page_header_v2) |*dph| {
            try writer.writeFieldBegin(.Struct, 8);
            try dph.write(writer);
        }
        try writer.writeStructEnd();
    }
};

pub const Statistics = struct {
    max: ?[]const u8 = null,
    min: ?[]const u8 = null,
    null_count: ?i64 = null,
    distinct_count: ?i64 = null,
    max_value: ?[]const u8 = null,
    min_value: ?[]const u8 = null,

    pub fn read(reader: *thrift.Reader) !Statistics {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var stats = Statistics{};
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => stats.max = try reader.readString(),
                2 => stats.min = try reader.readString(),
                3 => stats.null_count = try reader.readZigZag(i64),
                4 => stats.distinct_count = try reader.readZigZag(i64),
                5 => stats.max_value = try reader.readString(),
                6 => stats.min_value = try reader.readString(),
                else => try reader.skip(field.type),
            }
        }
        return stats;
    }

    pub fn write(self: *const Statistics, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        if (self.max) |v| try writer.writeFieldString(1, v);
        if (self.min) |v| try writer.writeFieldString(2, v);
        if (self.null_count) |v| try writer.writeFieldI64(3, v);
        if (self.distinct_count) |v| try writer.writeFieldI64(4, v);
        if (self.max_value) |v| try writer.writeFieldString(5, v);
        if (self.min_value) |v| try writer.writeFieldString(6, v);
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
    statistics: ?Statistics = null,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnMetaData {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var meta = ColumnMetaData{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .empty,
            .codec = .UNCOMPRESSED,
            .num_values = 0,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        };
        errdefer meta.encodings.deinit(allocator);
        errdefer meta.path_in_schema.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;

            switch (field.id) {
                1 => meta.type = (std.enums.fromInt(Type, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                2 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    // Clamped by `remaining()`: `size` is attacker-controlled, so an oversized list header must
                    // not preallocate.
                    try meta.encodings.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.encodings.append(allocator, (std.enums.fromInt(Encoding, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue));
                    }
                },
                3 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try meta.path_in_schema.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try meta.path_in_schema.append(allocator, try reader.readString());
                    }
                },
                4 => meta.codec = (std.enums.fromInt(CompressionCodec, try reader.readZigZag(i32)) orelse return error.InvalidEnumValue),
                5 => meta.num_values = try reader.readZigZag(i64),
                6 => meta.total_uncompressed_size = try reader.readZigZag(i64),
                7 => meta.total_compressed_size = try reader.readZigZag(i64),
                8 => try reader.skip(field.type), // key_value_metadata is not needed by the engine
                9 => meta.data_page_offset = try reader.readZigZag(i64),
                10 => meta.index_page_offset = try reader.readZigZag(i64),
                11 => {
                    // A dictionary page can never legitimately start at offset 0
                    // (or any offset < 4): the file opens with the 4-byte "PAR1"
                    // magic, so the earliest a page can begin is offset 4. Some
                    // writers (see dict-page-offset-zero.parquet) emit a spurious
                    // dictionary_page_offset == 0 on chunks that have no dictionary
                    // at all. Treat that as absent so every downstream chunk-start
                    // computation falls back to data_page_offset instead of reading
                    // the magic bytes as a page header.
                    const dpo = try reader.readZigZag(i64);
                    meta.dictionary_page_offset = if (dpo >= 4) dpo else null;
                },
                12 => meta.statistics = try Statistics.read(reader),
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
        if (self.statistics) |*s| {
            try writer.writeFieldBegin(.Struct, 12);
            try s.write(writer);
        }
        try writer.writeStructEnd();
    }
};

pub const ColumnChunk = struct {
    file_path: ?[]const u8,
    file_offset: i64,
    meta_data: ?ColumnMetaData,
    // Page index locations (for page-level filtering)
    offset_index_offset: ?i64 = null,
    offset_index_length: ?i32 = null,
    column_index_offset: ?i64 = null,
    column_index_length: ?i32 = null,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnChunk {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
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
                4 => chunk.offset_index_offset = try reader.readZigZag(i64),
                5 => chunk.offset_index_length = try reader.readZigZag(i32),
                6 => chunk.column_index_offset = try reader.readZigZag(i64),
                7 => chunk.column_index_length = try reader.readZigZag(i32),
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
        // Fields 4-7: page-index pointers. Emitted only when set — the
        // writer that lays the index blocks down in the output stream
        // fills these with the new absolute offsets.
        if (self.offset_index_offset) |v| try writer.writeFieldI64(4, v);
        if (self.offset_index_length) |v| try writer.writeFieldI32(5, v);
        if (self.column_index_offset) |v| try writer.writeFieldI64(6, v);
        if (self.column_index_length) |v| try writer.writeFieldI32(7, v);
        try writer.writeStructEnd();
    }
};

/// Parquet page-index structures (`OffsetIndex` / `ColumnIndex` /
/// `PageLocation`). These live in the file body between the row-group
/// data and the footer; `ColumnChunk` fields 4-7 point at them. Adding
/// read+write here lets the writer carry a source file's page index
/// forward (fast path) or synthesize one (encode path) instead of
/// dropping it — page-level pruning survives a compaction.
pub const BoundaryOrder = enum(i32) {
    UNORDERED = 0,
    ASCENDING = 1,
    DESCENDING = 2,
};

/// One page's location within a column chunk. `offset` is an absolute
/// file offset; it must be rebased when the chunk's bytes move.
pub const PageLocation = struct {
    offset: i64 = 0,
    compressed_page_size: i32 = 0,
    first_row_index: i64 = 0,

    pub fn read(reader: *thrift.Reader) !PageLocation {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var p = PageLocation{};
        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => p.offset = try reader.readZigZag(i64),
                2 => p.compressed_page_size = try reader.readZigZag(i32),
                3 => p.first_row_index = try reader.readZigZag(i64),
                else => try reader.skip(field.type),
            }
        }
        return p;
    }

    pub fn write(self: *const PageLocation, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldI64(1, self.offset);
        try writer.writeFieldI32(2, self.compressed_page_size);
        try writer.writeFieldI64(3, self.first_row_index);
        try writer.writeStructEnd();
    }
};

pub const OffsetIndex = struct {
    page_locations: std.ArrayListUnmanaged(PageLocation) = .empty,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !OffsetIndex {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var oi = OffsetIndex{};
        errdefer oi.page_locations.deinit(allocator);

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try oi.page_locations.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try oi.page_locations.append(allocator, try PageLocation.read(reader));
                    }
                },
                else => try reader.skip(field.type),
            }
        }
        return oi;
    }

    pub fn write(self: *const OffsetIndex, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        try writer.writeFieldListBegin(1, .Struct, self.page_locations.items.len);
        for (self.page_locations.items) |*pl| try pl.write(writer);
        try writer.writeStructEnd();
    }
};

pub const ColumnIndex = struct {
    /// One bool per page: whether the page holds only nulls.
    null_pages: std.ArrayListUnmanaged(bool) = .empty,
    /// Per-page min/max bytes (empty slice for a null page). Length
    /// matches `null_pages`.
    min_values: std.ArrayListUnmanaged([]const u8) = .empty,
    max_values: std.ArrayListUnmanaged([]const u8) = .empty,
    boundary_order: BoundaryOrder = .UNORDERED,
    /// Optional per-page null counts. Histograms (fields 6-7) skipped.
    null_counts: ?std.ArrayListUnmanaged(i64) = null,

    pub fn read(allocator: std.mem.Allocator, reader: *thrift.Reader) !ColumnIndex {
        const saved_id = reader.last_field_id;
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var ci = ColumnIndex{};
        errdefer {
            ci.null_pages.deinit(allocator);
            ci.min_values.deinit(allocator);
            ci.max_values.deinit(allocator);
            if (ci.null_counts) |*nc| nc.deinit(allocator);
        }

        reader.readStructBegin();
        while (true) {
            const field = try reader.readFieldBegin();
            if (field.type == .Stop) break;
            switch (field.id) {
                1 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try ci.null_pages.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    // Compact protocol: a bool list element is a single
                    // byte (1 = true, 2 = false).
                    while (i < size) : (i += 1) {
                        try ci.null_pages.append(allocator, (try reader.readByte()) == 1);
                    }
                },
                2 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try ci.min_values.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try ci.min_values.append(allocator, try reader.readString());
                    }
                },
                3 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    try ci.max_values.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try ci.max_values.append(allocator, try reader.readString());
                    }
                },
                4 => ci.boundary_order = (std.enums.fromInt(BoundaryOrder, try reader.readZigZag(i32)) orelse .UNORDERED),
                5 => {
                    const header = try reader.readByte();
                    var size = @as(usize, header >> 4);
                    if (size == 0xF) size = try reader.readVarInt(usize);
                    var nc: std.ArrayListUnmanaged(i64) = .empty;
                    try nc.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
                    var i: usize = 0;
                    while (i < size) : (i += 1) {
                        try nc.append(allocator, try reader.readZigZag(i64));
                    }
                    ci.null_counts = nc;
                },
                else => try reader.skip(field.type),
            }
        }
        return ci;
    }

    pub fn write(self: *const ColumnIndex, writer: *thrift.Writer) !void {
        writer.writeStructBegin();
        // Field 1: null_pages (list<bool>) — 1 = true, 2 = false.
        try writer.writeFieldListBegin(1, .True, self.null_pages.items.len);
        for (self.null_pages.items) |b| try writer.writeByte(if (b) 1 else 2);
        // Fields 2-3: min/max value bytes.
        try writer.writeFieldListBegin(2, .Binary, self.min_values.items.len);
        for (self.min_values.items) |v| try writer.writeString(v);
        try writer.writeFieldListBegin(3, .Binary, self.max_values.items.len);
        for (self.max_values.items) |v| try writer.writeString(v);
        try writer.writeFieldI32(4, @intFromEnum(self.boundary_order));
        if (self.null_counts) |nc| {
            try writer.writeFieldListBegin(5, .I64, nc.items.len);
            for (nc.items) |v| try writer.writeZigZag(v);
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
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var rg = RowGroup{
            .columns = .empty,
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
                    try rg.columns.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
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
        reader.last_field_id = 0;
        defer reader.last_field_id = saved_id;

        var meta = FileMetaData{
            .version = 0,
            .schema = .empty,
            .num_rows = 0,
            .created_by = null,
            .row_groups = .empty,
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
                    try meta.schema.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
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
                    try meta.row_groups.ensureTotalCapacity(allocator, @min(size, reader.remaining()));
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

    pub fn getColumnSchema(self: *const FileMetaData, path: []const []const u8) ?SchemaElement {
        var iter = SchemaIterator{ .items = self.schema.items, .pos = 0 };
        return iter.findSchema(path) catch null;
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

    fn findSchema(self: *SchemaIterator, path: []const []const u8) !SchemaElement {
        if (self.pos >= self.items.len) return error.NotFound;

        // Consume root
        const root = self.items[self.pos];
        self.pos += 1;

        const num_children = root.num_children orelse 0;
        var i: i32 = 0;
        while (i < num_children) : (i += 1) {
            if (try self.visitSchema(path, 0)) |res| return res;
        }
        return error.NotFound;
    }

    fn visitSchema(self: *SchemaIterator, target_path: []const []const u8, depth: usize) !?SchemaElement {
        if (self.pos >= self.items.len) return null;
        const elem = self.items[self.pos];
        self.pos += 1;

        if (std.mem.eql(u8, elem.name, target_path[depth])) {
            if (depth == target_path.len - 1) {
                return elem;
            } else {
                const num_children = elem.num_children orelse 0;
                var i: i32 = 0;
                while (i < num_children) : (i += 1) {
                    if (try self.visitSchema(target_path, depth + 1)) |res| return res;
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

test "isUnsignedIntTo32 — converted + logical, signed/unsigned, width boundary" {
    const base = SchemaElement{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var e = base;

    // Converted types
    e.converted_type = .UINT_8;
    try std.testing.expect(isUnsignedIntTo32(e));
    e.converted_type = .UINT_32;
    try std.testing.expect(isUnsignedIntTo32(e));
    e.converted_type = .UINT_64; // u64 excluded — needs wider accumulator
    try std.testing.expect(!isUnsignedIntTo32(e));
    e.converted_type = .INT_32; // signed
    try std.testing.expect(!isUnsignedIntTo32(e));
    e.converted_type = null;

    // Logical INTEGER type
    e.logical_type = .{ .INTEGER = .{ .bitWidth = 32, .isSigned = false } };
    try std.testing.expect(isUnsignedIntTo32(e));
    e.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } }; // u64 excluded
    try std.testing.expect(!isUnsignedIntTo32(e));
    e.logical_type = .{ .INTEGER = .{ .bitWidth = 32, .isSigned = true } }; // signed
    try std.testing.expect(!isUnsignedIntTo32(e));

    // No annotation → signed by default
    try std.testing.expect(!isUnsignedIntTo32(base));
}

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
        .data_page_header_v2 = null,
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

test "OffsetIndex roundtrip" {
    const allocator = std.testing.allocator;

    var oi = OffsetIndex{};
    defer oi.page_locations.deinit(allocator);
    try oi.page_locations.append(allocator, .{ .offset = 4, .compressed_page_size = 120, .first_row_index = 0 });
    try oi.page_locations.append(allocator, .{ .offset = 124, .compressed_page_size = 96, .first_row_index = 1000 });

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try oi.write(&writer);

    var reader = thrift.Reader.init(writer.bytes());
    var decoded = try OffsetIndex.read(allocator, &reader);
    defer decoded.page_locations.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 2), decoded.page_locations.items.len);
    try std.testing.expectEqual(@as(i64, 124), decoded.page_locations.items[1].offset);
    try std.testing.expectEqual(@as(i32, 96), decoded.page_locations.items[1].compressed_page_size);
    try std.testing.expectEqual(@as(i64, 1000), decoded.page_locations.items[1].first_row_index);
}

test "ColumnIndex roundtrip" {
    const allocator = std.testing.allocator;

    var ci = ColumnIndex{ .boundary_order = .ASCENDING };
    defer {
        ci.null_pages.deinit(allocator);
        ci.min_values.deinit(allocator);
        ci.max_values.deinit(allocator);
        if (ci.null_counts) |*nc| nc.deinit(allocator);
    }
    try ci.null_pages.appendSlice(allocator, &[_]bool{ false, true });
    try ci.min_values.appendSlice(allocator, &[_][]const u8{ "\x01\x00\x00\x00", "" });
    try ci.max_values.appendSlice(allocator, &[_][]const u8{ "\xff\x00\x00\x00", "" });
    var nc: std.ArrayListUnmanaged(i64) = .empty;
    try nc.appendSlice(allocator, &[_]i64{ 0, 1000 });
    ci.null_counts = nc;

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try ci.write(&writer);

    var reader = thrift.Reader.init(writer.bytes());
    var decoded = try ColumnIndex.read(allocator, &reader);
    defer {
        decoded.null_pages.deinit(allocator);
        decoded.min_values.deinit(allocator);
        decoded.max_values.deinit(allocator);
        if (decoded.null_counts) |*d| d.deinit(allocator);
    }

    try std.testing.expectEqual(@as(usize, 2), decoded.null_pages.items.len);
    try std.testing.expectEqual(false, decoded.null_pages.items[0]);
    try std.testing.expectEqual(true, decoded.null_pages.items[1]);
    try std.testing.expectEqual(BoundaryOrder.ASCENDING, decoded.boundary_order);
    try std.testing.expectEqualSlices(u8, "\x01\x00\x00\x00", decoded.min_values.items[0]);
    try std.testing.expect(decoded.null_counts != null);
    try std.testing.expectEqual(@as(i64, 1000), decoded.null_counts.?.items[1]);
}

test "ColumnChunk write emits page-index pointers" {
    const allocator = std.testing.allocator;

    const chunk = ColumnChunk{
        .file_path = null,
        .file_offset = 4,
        .meta_data = null,
        .offset_index_offset = 5000,
        .offset_index_length = 40,
        .column_index_offset = 5040,
        .column_index_length = 64,
    };

    var writer = thrift.Writer.init(allocator);
    defer writer.deinit();
    try chunk.write(&writer);

    var reader = thrift.Reader.init(writer.bytes());
    var decoded = try ColumnChunk.read(allocator, &reader);
    defer decoded.deinit(allocator);

    try std.testing.expectEqual(@as(?i64, 5000), decoded.offset_index_offset);
    try std.testing.expectEqual(@as(?i32, 40), decoded.offset_index_length);
    try std.testing.expectEqual(@as(?i64, 5040), decoded.column_index_offset);
    try std.testing.expectEqual(@as(?i32, 64), decoded.column_index_length);
}
