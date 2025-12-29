const std = @import("std");
const minish = @import("minish");
const gen = minish.gen;
const combinators = minish.combinators;
const zpq = @import("zpq");
const thrift = zpq.core.thrift;
const schema = zpq.core.schema;

// --------------------------------------------------------------------------
// Thrift Compact Protocol Writer
// --------------------------------------------------------------------------
pub const Writer = struct {
    buffer: *std.ArrayListUnmanaged(u8),
    allocator: std.mem.Allocator,
    last_field_id: i16 = 0,

    pub fn init(allocator: std.mem.Allocator, buffer: *std.ArrayListUnmanaged(u8)) Writer {
        return .{ .buffer = buffer, .allocator = allocator };
    }

    pub fn writeByte(self: *Writer, byte: u8) !void {
        try self.buffer.append(self.allocator, byte);
    }

    pub fn writeVarInt(self: *Writer, val: u64) !void {
        var v = val;
        while (v >= 0x80) {
            try self.writeByte(@as(u8, @intCast(v & 0x7f)) | 0x80);
            v >>= 7;
        }
        try self.writeByte(@as(u8, @intCast(v)));
    }

    pub fn writeZigZag(self: *Writer, val: anytype) !void {
        const T = @TypeOf(val);
        const UT = std.meta.Int(.unsigned, @bitSizeOf(T));
        const v = @as(T, val);
        const zv = if (v < 0)
            (@as(UT, @bitCast(~v)) << 1) | 1
        else
            @as(UT, @bitCast(v)) << 1;
        try self.writeVarInt(zv);
    }

    pub fn writeFieldBegin(self: *Writer, t: thrift.Type, id: i16) !void {
        const delta = id - self.last_field_id;
        if (delta > 0 and delta <= 15) {
            try self.writeByte(@as(u8, @intCast(delta << 4)) | @intFromEnum(t));
        } else {
            try self.writeByte(@intFromEnum(t));
            try self.writeZigZag(id);
        }
        self.last_field_id = id;
    }

    pub fn writeFieldStop(self: *Writer) !void {
        try self.writeByte(0);
    }

    pub fn writeStructBegin(self: *Writer) i16 {
        const old = self.last_field_id;
        self.last_field_id = 0;
        return old;
    }

    pub fn writeStructEnd(self: *Writer, old_id: i16) void {
        self.last_field_id = old_id;
    }

    pub fn writeString(self: *Writer, str: []const u8) !void {
        try self.writeVarInt(str.len);
        try self.buffer.appendSlice(self.allocator, str);
    }

    pub fn writeListBegin(self: *Writer, t: thrift.Type, size: usize) !void {
        if (size <= 14) {
            try self.writeByte(@as(u8, @intCast(size << 4)) | @intFromEnum(t));
        } else {
            try self.writeByte(0xf0 | @intFromEnum(t));
            try self.writeVarInt(size);
        }
    }
};

// --------------------------------------------------------------------------
// Custom Generators for Schema Types
// --------------------------------------------------------------------------

fn genSchemaElement(tc: *minish.TestCase) !schema.SchemaElement {
    return schema.SchemaElement{
        .type = if (try tc.choice(1) == 1) try gen.enumValue(schema.Type).generateFn(tc) else null,
        .type_length = if (try tc.choice(1) == 1) try gen.intRange(i32, 0, 100).generateFn(tc) else null,
        .repetition_type = if (try tc.choice(1) == 1) try gen.enumValue(schema.FieldRepetitionType).generateFn(tc) else null,
        .name = try gen.string(.{ .min_len = 1, .max_len = 20, .charset = .alphanumeric }).generateFn(tc),
        .num_children = if (try tc.choice(1) == 1) try gen.intRange(i32, 0, 10).generateFn(tc) else null,
        .scale = if (try tc.choice(1) == 1) try gen.intRange(i32, 0, 10).generateFn(tc) else null,
        .precision = if (try tc.choice(1) == 1) try gen.intRange(i32, 0, 10).generateFn(tc) else null,
        .field_id = if (try tc.choice(1) == 1) try gen.intRange(i32, 0, 1000).generateFn(tc) else null,
    };
}

fn freeSchemaElement(allocator: std.mem.Allocator, elem: schema.SchemaElement) void {
    allocator.free(elem.name);
}

fn genColumnMetaData(tc: *minish.TestCase) !schema.ColumnMetaData {
    var meta = schema.ColumnMetaData{
        .type = try gen.enumValue(schema.Type).generateFn(tc),
        .encodings = .{},
        .path_in_schema = .{},
        .codec = try gen.enumValue(schema.CompressionCodec).generateFn(tc),
        .num_values = try gen.int(i64).generateFn(tc),
        .total_uncompressed_size = try gen.int(i64).generateFn(tc),
        .total_compressed_size = try gen.int(i64).generateFn(tc),
        .data_page_offset = try gen.int(i64).generateFn(tc),
        .index_page_offset = if (try tc.choice(1) == 1) try gen.int(i64).generateFn(tc) else null,
        .dictionary_page_offset = if (try tc.choice(1) == 1) try gen.int(i64).generateFn(tc) else null,
    };

    const num_enc = try tc.choice(5);
    for (0..num_enc) |_| {
        try meta.encodings.append(tc.allocator, try gen.enumValue(schema.Encoding).generateFn(tc));
    }

    const num_path = 1 + try tc.choice(3);
    for (0..num_path) |_| {
        try meta.path_in_schema.append(tc.allocator, try gen.string(.{ .min_len = 1, .max_len = 10 }).generateFn(tc));
    }

    return meta;
}

fn freeColumnMetaData(allocator: std.mem.Allocator, meta: schema.ColumnMetaData) void {
    var m = meta;
    for (m.path_in_schema.items) |p| allocator.free(p);
    m.encodings.deinit(allocator);
    m.path_in_schema.deinit(allocator);
}

fn genColumnChunk(tc: *minish.TestCase) !schema.ColumnChunk {
    return schema.ColumnChunk{
        .file_path = if (try tc.choice(1) == 1) try gen.string(.{ .min_len = 1, .max_len = 20 }).generateFn(tc) else null,
        .file_offset = try gen.int(i64).generateFn(tc),
        .meta_data = if (try tc.choice(1) == 1) try genColumnMetaData(tc) else null,
    };
}

fn freeColumnChunk(allocator: std.mem.Allocator, chunk: schema.ColumnChunk) void {
    if (chunk.file_path) |fp| allocator.free(fp);
    if (chunk.meta_data) |md| freeColumnMetaData(allocator, md);
}

fn genRowGroup(tc: *minish.TestCase) !schema.RowGroup {
    var rg = schema.RowGroup{
        .columns = .{},
        .total_byte_size = try gen.int(i64).generateFn(tc),
        .num_rows = try gen.int(i64).generateFn(tc),
    };

    const num_cols = 1 + try tc.choice(5);
    for (0..num_cols) |_| {
        try rg.columns.append(tc.allocator, try genColumnChunk(tc));
    }
    return rg;
}

fn freeRowGroup(allocator: std.mem.Allocator, rg: schema.RowGroup) void {
    var r = rg;
    for (r.columns.items) |c| freeColumnChunk(allocator, c);
    r.columns.deinit(allocator);
}

fn genFileMetaData(tc: *minish.TestCase) !schema.FileMetaData {
    var meta = schema.FileMetaData{
        .version = try gen.int(i32).generateFn(tc),
        .schema = .{},
        .num_rows = try gen.int(i64).generateFn(tc),
        .created_by = if (try tc.choice(1) == 1) try gen.string(.{ .min_len = 1, .max_len = 20 }).generateFn(tc) else null,
        .row_groups = .{},
    };

    const num_schema = 1 + try tc.choice(10);
    for (0..num_schema) |_| {
        try meta.schema.append(tc.allocator, try genSchemaElement(tc));
    }

    const num_rg = try tc.choice(3);
    for (0..num_rg) |_| {
        try meta.row_groups.append(tc.allocator, try genRowGroup(tc));
    }

    return meta;
}

fn freeFileMetaData(allocator: std.mem.Allocator, meta: schema.FileMetaData) void {
    var m = meta;
    for (m.schema.items) |s| freeSchemaElement(allocator, s);
    for (m.row_groups.items) |*rg| freeRowGroup(allocator, rg.*);
    if (m.created_by) |cb| allocator.free(cb);
    m.schema.deinit(allocator);
    m.row_groups.deinit(allocator);
}

// --------------------------------------------------------------------------
// Round-Trip Property
// --------------------------------------------------------------------------

fn encodeSchemaElement(w: *Writer, elem: schema.SchemaElement) !void {
    const old_id = w.writeStructBegin();
    defer w.writeStructEnd(old_id);

    if (elem.type) |t| {
        try w.writeFieldBegin(.I32, 1);
        try w.writeZigZag(@intFromEnum(t));
    }
    if (elem.type_length) |tl| {
        try w.writeFieldBegin(.I32, 2);
        try w.writeZigZag(tl);
    }
    if (elem.repetition_type) |rt| {
        try w.writeFieldBegin(.I32, 3);
        try w.writeZigZag(@intFromEnum(rt));
    }
    try w.writeFieldBegin(.Binary, 4);
    try w.writeString(elem.name);

    if (elem.num_children) |nc| {
        try w.writeFieldBegin(.I32, 5);
        try w.writeZigZag(nc);
    }
    if (elem.scale) |s| {
        try w.writeFieldBegin(.I32, 7);
        try w.writeZigZag(s);
    }
    if (elem.precision) |p| {
        try w.writeFieldBegin(.I32, 8);
        try w.writeZigZag(p);
    }
    if (elem.field_id) |fid| {
        try w.writeFieldBegin(.I32, 9);
        try w.writeZigZag(fid);
    }
    try w.writeFieldStop();
}

fn encodeColumnMetaData(w: *Writer, meta: schema.ColumnMetaData) !void {
    const old_id = w.writeStructBegin();
    defer w.writeStructEnd(old_id);

    try w.writeFieldBegin(.I32, 1);
    try w.writeZigZag(@intFromEnum(meta.type));

    try w.writeFieldBegin(.List, 2);
    try w.writeListBegin(.I32, meta.encodings.items.len);
    for (meta.encodings.items) |e| {
        try w.writeZigZag(@intFromEnum(e));
    }

    try w.writeFieldBegin(.List, 3);
    try w.writeListBegin(.Binary, meta.path_in_schema.items.len);
    for (meta.path_in_schema.items) |p| {
        try w.writeString(p);
    }

    try w.writeFieldBegin(.I32, 4);
    try w.writeZigZag(@intFromEnum(meta.codec));

    try w.writeFieldBegin(.I64, 5);
    try w.writeZigZag(meta.num_values);

    try w.writeFieldBegin(.I64, 6);
    try w.writeZigZag(meta.total_uncompressed_size);

    try w.writeFieldBegin(.I64, 7);
    try w.writeZigZag(meta.total_compressed_size);

    try w.writeFieldBegin(.I64, 9);
    try w.writeZigZag(meta.data_page_offset);

    if (meta.index_page_offset) |ipo| {
        try w.writeFieldBegin(.I64, 10);
        try w.writeZigZag(ipo);
    }
    if (meta.dictionary_page_offset) |dpo| {
        try w.writeFieldBegin(.I64, 11);
        try w.writeZigZag(dpo);
    }
    try w.writeFieldStop();
}

fn encodeColumnChunk(w: *Writer, chunk: schema.ColumnChunk) !void {
    const old_id = w.writeStructBegin();
    defer w.writeStructEnd(old_id);

    if (chunk.file_path) |fp| {
        try w.writeFieldBegin(.Binary, 1);
        try w.writeString(fp);
    }
    try w.writeFieldBegin(.I64, 2);
    try w.writeZigZag(chunk.file_offset);

    if (chunk.meta_data) |md| {
        try w.writeFieldBegin(.Struct, 3);
        try encodeColumnMetaData(w, md);
    }
    try w.writeFieldStop();
}

fn encodeRowGroup(w: *Writer, rg: schema.RowGroup) !void {
    const old_id = w.writeStructBegin();
    defer w.writeStructEnd(old_id);

    try w.writeFieldBegin(.List, 1);
    try w.writeListBegin(.Struct, rg.columns.items.len);
    for (rg.columns.items) |c| {
        try encodeColumnChunk(w, c);
    }

    try w.writeFieldBegin(.I64, 2);
    try w.writeZigZag(rg.total_byte_size);

    try w.writeFieldBegin(.I64, 3);
    try w.writeZigZag(rg.num_rows);

    try w.writeFieldStop();
}

fn encodeFileMetaData(w: *Writer, meta: schema.FileMetaData) !void {
    w.last_field_id = 0;

    try w.writeFieldBegin(.I32, 1);
    try w.writeZigZag(meta.version);

    try w.writeFieldBegin(.List, 2);
    try w.writeListBegin(.Struct, meta.schema.items.len);
    for (meta.schema.items) |s| {
        try encodeSchemaElement(w, s);
    }

    try w.writeFieldBegin(.I64, 3);
    try w.writeZigZag(meta.num_rows);

    try w.writeFieldBegin(.List, 4);
    try w.writeListBegin(.Struct, meta.row_groups.items.len);
    for (meta.row_groups.items) |rg| {
        try encodeRowGroup(w, rg);
    }

    if (meta.created_by) |cb| {
        try w.writeFieldBegin(.Binary, 6);
        try w.writeString(cb);
    }
    try w.writeFieldStop();
}

// --------------------------------------------------------------------------
// Deep Equality Comparison
// --------------------------------------------------------------------------

fn expectEqualMetaData(a: schema.FileMetaData, b: schema.FileMetaData) !void {
    try std.testing.expectEqual(a.version, b.version);
    try std.testing.expectEqual(a.num_rows, b.num_rows);
    if (a.created_by) |acb| {
        try std.testing.expectEqualStrings(acb, b.created_by.?);
    } else {
        try std.testing.expect(b.created_by == null);
    }

    try std.testing.expectEqual(a.schema.items.len, b.schema.items.len);
    for (a.schema.items, b.schema.items) |as, bs| {
        try std.testing.expectEqual(as.type, bs.type);
        try std.testing.expectEqual(as.type_length, bs.type_length);
        try std.testing.expectEqual(as.repetition_type, bs.repetition_type);
        try std.testing.expectEqualStrings(as.name, bs.name);
        try std.testing.expectEqual(as.num_children, bs.num_children);
        try std.testing.expectEqual(as.scale, bs.scale);
        try std.testing.expectEqual(as.precision, bs.precision);
        try std.testing.expectEqual(as.field_id, bs.field_id);
    }

    try std.testing.expectEqual(a.row_groups.items.len, b.row_groups.items.len);
    for (a.row_groups.items, b.row_groups.items) |arg, brg| {
        try std.testing.expectEqual(arg.total_byte_size, brg.total_byte_size);
        try std.testing.expectEqual(arg.num_rows, brg.num_rows);
        try std.testing.expectEqual(arg.columns.items.len, brg.columns.items.len);
        for (arg.columns.items, brg.columns.items) |acol, bcol| {
            if (acol.file_path) |afp| {
                try std.testing.expectEqualStrings(afp, bcol.file_path.?);
            }
            try std.testing.expectEqual(acol.file_offset, bcol.file_offset);
            if (acol.meta_data) |amd| {
                const bmd = bcol.meta_data.?;
                try std.testing.expectEqual(amd.type, bmd.type);
                try std.testing.expectEqual(amd.codec, bmd.codec);
                try std.testing.expectEqual(amd.num_values, bmd.num_values);
                try std.testing.expectEqual(amd.total_uncompressed_size, bmd.total_uncompressed_size);
                try std.testing.expectEqual(amd.total_compressed_size, bmd.total_compressed_size);
                try std.testing.expectEqual(amd.data_page_offset, bmd.data_page_offset);
                try std.testing.expectEqual(amd.index_page_offset, bmd.index_page_offset);
                try std.testing.expectEqual(amd.dictionary_page_offset, bmd.dictionary_page_offset);

                try std.testing.expectEqual(amd.encodings.items.len, bmd.encodings.items.len);
                try std.testing.expectEqualSlices(schema.Encoding, amd.encodings.items, bmd.encodings.items);

                try std.testing.expectEqual(amd.path_in_schema.items.len, bmd.path_in_schema.items.len);
                for (amd.path_in_schema.items, bmd.path_in_schema.items) |ap, bp| {
                    try std.testing.expectEqualStrings(ap, bp);
                }
            }
        }
    }
}

const FuzzContext = struct {
    allocator: std.mem.Allocator,

    pub fn run(self: @This(), meta: schema.FileMetaData) !void {
        var buf = std.ArrayListUnmanaged(u8){};
        defer buf.deinit(self.allocator);

        var w = Writer.init(self.allocator, &buf);
        try encodeFileMetaData(&w, meta);

        var reader = thrift.Reader.init(buf.items);
        var decoded = try schema.FileMetaData.read(self.allocator, &reader);
        defer decoded.deinit(self.allocator);

        try expectEqualMetaData(meta, decoded);
    }
};

test "thrift round-trip fuzz" {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const meta_gen = gen.Generator(schema.FileMetaData){
        .generateFn = genFileMetaData,
        .shrinkFn = null, // Shrinking nested structs is complex, skipping for now
        .freeFn = freeFileMetaData,
    };

    const ctx = FuzzContext{ .allocator = allocator };
    try minish.check(allocator, meta_gen, ctx, .{ .num_runs = 100, .verbose = true });
}
