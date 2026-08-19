const std = @import("std");

pub const Type = enum(u8) {
    Stop = 0,
    True = 1,
    False = 2,
    Byte = 3,
    I16 = 4,
    I32 = 5,
    I64 = 6,
    Double = 7,
    Binary = 8,
    List = 9,
    Set = 10,
    Map = 11,
    Struct = 12,
};

pub const Reader = struct {
    data: []const u8,
    pos: usize,
    last_field_id: i16,

    pub fn init(data: []const u8) Reader {
        return Reader{
            .data = data,
            .pos = 0,
            .last_field_id = 0,
        };
    }

    pub fn readVarInt(self: *Reader, comptime T: type) !T {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            if (self.pos >= self.data.len) return error.EndOfStream;
            const byte = self.data[self.pos];
            self.pos += 1;

            result |= @as(u64, byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) break;
            // A u64 varint is at most 10 groups of 7 bits; a further
            // continuation byte would overflow `shift` (u6). Reject
            // overlong encodings instead of trapping. Found by the
            // decode fuzzer, 2026-06-12.
            if (shift >= 63) return error.VarIntTooLong;
            shift += 7;
        }

        // Check for overflow if T is smaller than u64?
        // For signed T, we just bitcast
        // But readVarInt is usually used for Unsigned or just generic "int"
        // If T is signed, we usually just cast.

        switch (@typeInfo(T)) {
            .int => |info| {
                if (info.signedness == .signed) {
                    return @as(T, @bitCast(@as(std.meta.Int(.unsigned, @bitSizeOf(T)), @truncate(result))));
                } else {
                    return @as(T, @truncate(result));
                }
            },
            else => return @as(T, @truncate(result)),
        }
    }

    pub fn readZigZag(self: *Reader, comptime T: type) !T {
        // Read as unsigned equivalent
        const UT = std.meta.Int(.unsigned, @bitSizeOf(T));
        const n = try self.readVarInt(UT);

        // (n >> 1) ^ -(n & 1)
        const shifted = n >> 1;
        if ((n & 1) == 0) {
            return @as(T, @bitCast(shifted));
        } else {
            // In two's complement, ^ -1 is bitwise NOT, which is what we want for negative
            // Wait, zig zag: 0=-1 -> 1, -1 -> 1? No.
            // 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3
            // decode: (n >> 1) ^ -(n & 1)
            // if n is odd (negative), n&1 is 1. -(1) is ... all ones.
            // x ^ all_ones is ~x.
            // Correct.
            return @as(T, @bitCast(shifted)) ^ -1;
        }
    }

    pub fn readByte(self: *Reader) !u8 {
        if (self.pos >= self.data.len) return error.EndOfStream;
        const b = self.data[self.pos];
        self.pos += 1;
        return b;
    }

    /// Bytes remaining from the cursor. The cursor never passes `data.len`,
    /// so this never underflows.
    pub fn remaining(self: *const Reader) usize {
        return self.data.len - self.pos;
    }

    /// Advance the cursor by `n`, erroring if that would run past the end.
    /// Compares against `remaining()` so `pos + n` can't overflow on a
    /// hostile length.
    fn advance(self: *Reader, n: usize) !void {
        if (n > self.remaining()) return error.EndOfStream;
        self.pos += n;
    }

    pub fn readFieldBegin(self: *Reader) !struct { type: Type, id: i16 } {
        const byte = try self.readByte();
        if (byte == 0) return .{ .type = .Stop, .id = 0 };

        const delta = (byte >> 4) & 0x0f;
        const type_id = byte & 0x0f;

        var field_id: i16 = 0;
        if (delta == 0) {
            field_id = try self.readZigZag(i16);
        } else {
            // Checked add: a hostile delta chain must not overflow i16.
            field_id = std.math.add(i16, self.last_field_id, @as(i16, delta)) catch return error.InvalidFieldId;
        }

        self.last_field_id = field_id;
        // type_id is a 4-bit nibble (0..15) but only 0..12 are valid wire
        // types; an out-of-range tag from a malformed/hostile stream must
        // become a clean error, never an invalid-enum @enumFromInt (UB
        // under ReleaseFast). Found by the decode fuzzer, 2026-06-12.
        const t = std.enums.fromInt(Type, type_id) orelse return error.InvalidThriftType;

        return .{ .type = t, .id = field_id };
    }

    pub fn readStructBegin(self: *Reader) void {
        self.last_field_id = 0;
    }

    pub fn readString(self: *Reader) ![]const u8 {
        const len = try self.readVarInt(usize);
        // `self.pos + len` could overflow on a hostile length; compare
        // against the remaining byte count instead.
        if (len > self.remaining()) return error.EndOfStream;
        const str = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return str;
    }

    /// Cap on nested-container recursion. Parquet footers nest only a
    /// handful deep; this bounds a hostile, deeply-nested stream from
    /// blowing the stack.
    const max_skip_depth: u16 = 100;

    pub fn skip(self: *Reader, t: Type) !void {
        return self.skipDepth(t, 0);
    }

    fn skipDepth(self: *Reader, t: Type, depth: u16) !void {
        if (depth > max_skip_depth) return error.NestingTooDeep;
        switch (t) {
            .Stop => return,
            .True, .False => return,
            .Byte => _ = try self.readByte(),
            .I16, .I32, .I64 => _ = try self.readZigZag(i64),
            .Double => try self.advance(8),
            .Binary => {
                const len = try self.readVarInt(usize);
                try self.advance(len);
            },
            .List, .Set => {
                const header = try self.readByte();
                var size = @as(usize, header >> 4);
                const elem_type = std.enums.fromInt(Type, header & 0x0f) orelse return error.InvalidThriftType;
                if (size == 0xF) {
                    size = try self.readVarInt(usize);
                }
                // A container can't hold more elements than there are bytes
                // left to encode them; reject absurd counts so a hostile
                // size can't spin on zero-width element types.
                if (size > self.remaining()) return error.InvalidLength;
                var i: usize = 0;
                while (i < size) : (i += 1) {
                    try self.skipDepth(elem_type, depth + 1);
                }
            },
            .Map => {
                const count = try self.readVarInt(usize);
                if (count == 0) return;
                const types = try self.readByte();
                const key_type = std.enums.fromInt(Type, types >> 4) orelse return error.InvalidThriftType;
                const val_type = std.enums.fromInt(Type, types & 0x0f) orelse return error.InvalidThriftType;
                if (count > self.remaining()) return error.InvalidLength;
                var i: usize = 0;
                while (i < count) : (i += 1) {
                    try self.skipDepth(key_type, depth + 1);
                    try self.skipDepth(val_type, depth + 1);
                }
            },
            .Struct => {
                const saved_id = self.last_field_id;
                self.readStructBegin();
                while (true) {
                    const field = try self.readFieldBegin();
                    if (field.type == .Stop) break;
                    try self.skipDepth(field.type, depth + 1);
                }
                self.last_field_id = saved_id;
            },
        }
    }
};

/// Thrift Compact Protocol Writer
pub const Writer = struct {
    buffer: std.ArrayList(u8),
    allocator: std.mem.Allocator,
    last_field_id: i16,
    /// When a struct is nested as a field within another struct, the
    /// inner struct must use its own delta-encoding namespace for field
    /// IDs (starting at 0), but the *outer* struct's `last_field_id`
    /// must be preserved across the nested write so the next outer
    /// field encodes correctly. We push on writeStructBegin and pop on
    /// writeStructEnd. Stack depth 8 is plenty for Parquet metadata.
    saved_stack: [8]i16 = undefined,
    saved_stack_pos: u8 = 0,

    pub fn init(allocator: std.mem.Allocator) Writer {
        return Writer{
            .buffer = .empty,
            .allocator = allocator,
            .last_field_id = 0,
        };
    }

    pub fn deinit(self: *Writer) void {
        self.buffer.deinit(self.allocator);
    }

    pub fn toOwnedSlice(self: *Writer) ![]u8 {
        return self.buffer.toOwnedSlice(self.allocator);
    }

    pub fn bytes(self: *const Writer) []const u8 {
        return self.buffer.items;
    }

    pub fn reset(self: *Writer) void {
        self.buffer.clearRetainingCapacity();
        self.last_field_id = 0;
        self.saved_stack_pos = 0;
    }

    pub fn writeByte(self: *Writer, b: u8) !void {
        try self.buffer.append(self.allocator, b);
    }

    pub fn writeBytes(self: *Writer, data: []const u8) !void {
        try self.buffer.appendSlice(self.allocator, data);
    }

    pub fn writeVarInt(self: *Writer, value: anytype) !void {
        var v: u64 = switch (@typeInfo(@TypeOf(value))) {
            .int => |info| blk: {
                if (info.signedness == .signed) {
                    break :blk @bitCast(@as(std.meta.Int(.unsigned, info.bits), @bitCast(value)));
                } else {
                    break :blk @intCast(value);
                }
            },
            .@"enum" => @intCast(@intFromEnum(value)),
            else => @compileError("writeVarInt requires int or enum"),
        };

        while (v >= 0x80) {
            try self.buffer.append(self.allocator, @as(u8, @truncate(v)) | 0x80);
            v >>= 7;
        }
        try self.buffer.append(self.allocator, @as(u8, @truncate(v)));
    }

    pub fn writeZigZag(self: *Writer, value: anytype) !void {
        const T = @TypeOf(value);
        const info = @typeInfo(T).int;
        const UT = std.meta.Int(.unsigned, info.bits);

        // ZigZag encoding: (n << 1) ^ (n >> (bits - 1))
        const v: UT = @bitCast(value);
        const encoded = (v << 1) ^ @as(UT, @bitCast(@as(T, @bitCast(v)) >> (info.bits - 1)));
        try self.writeVarInt(encoded);
    }

    pub fn writeString(self: *Writer, s: []const u8) !void {
        try self.writeVarInt(@as(u32, @intCast(s.len)));
        try self.buffer.appendSlice(self.allocator, s);
    }

    pub fn writeDouble(self: *Writer, value: f64) !void {
        const bits: u64 = @bitCast(value);
        try self.buffer.appendSlice(self.allocator, &std.mem.toBytes(bits));
    }

    pub fn writeStructBegin(self: *Writer) void {
        if (self.saved_stack_pos < self.saved_stack.len) {
            self.saved_stack[self.saved_stack_pos] = self.last_field_id;
            self.saved_stack_pos += 1;
        }
        self.last_field_id = 0;
    }

    pub fn writeStructEnd(self: *Writer) !void {
        try self.writeByte(0); // Stop field
        if (self.saved_stack_pos > 0) {
            self.saved_stack_pos -= 1;
            self.last_field_id = self.saved_stack[self.saved_stack_pos];
        }
    }

    pub fn writeFieldBegin(self: *Writer, field_type: Type, field_id: i16) !void {
        const delta = field_id - self.last_field_id;

        if (delta > 0 and delta <= 15) {
            // Short form: delta in high nibble, type in low nibble
            try self.writeByte(@as(u8, @intCast(delta)) << 4 | @intFromEnum(field_type));
        } else {
            // Long form: 0 in high nibble, type in low nibble, then zigzag field id
            try self.writeByte(@intFromEnum(field_type));
            try self.writeZigZag(field_id);
        }
        self.last_field_id = field_id;
    }

    pub fn writeFieldI8(self: *Writer, field_id: i16, value: i8) !void {
        // Compact thrift i8/byte values are written as a single signed
        // byte — NOT zigzag, NOT varint. Strict readers (pyarrow,
        // parquet-mr, DuckDB) reject IntType.bitWidth and similar i8
        // fields when written with the I32 type marker.
        try self.writeFieldBegin(.Byte, field_id);
        try self.writeByte(@as(u8, @bitCast(value)));
    }

    pub fn writeFieldI32(self: *Writer, field_id: i16, value: i32) !void {
        try self.writeFieldBegin(.I32, field_id);
        try self.writeZigZag(value);
    }

    pub fn writeFieldI64(self: *Writer, field_id: i16, value: i64) !void {
        try self.writeFieldBegin(.I64, field_id);
        try self.writeZigZag(value);
    }

    pub fn writeFieldString(self: *Writer, field_id: i16, value: []const u8) !void {
        try self.writeFieldBegin(.Binary, field_id);
        try self.writeString(value);
    }

    pub fn writeFieldBool(self: *Writer, field_id: i16, value: bool) !void {
        try self.writeFieldBegin(if (value) .True else .False, field_id);
    }

    pub fn writeFieldDouble(self: *Writer, field_id: i16, value: f64) !void {
        try self.writeFieldBegin(.Double, field_id);
        try self.writeDouble(value);
    }

    pub fn writeListBegin(self: *Writer, elem_type: Type, size: usize) !void {
        if (size < 15) {
            try self.writeByte(@as(u8, @intCast(size)) << 4 | @intFromEnum(elem_type));
        } else {
            try self.writeByte(0xF0 | @intFromEnum(elem_type));
            try self.writeVarInt(@as(u32, @intCast(size)));
        }
    }

    pub fn writeFieldListBegin(self: *Writer, field_id: i16, elem_type: Type, size: usize) !void {
        try self.writeFieldBegin(.List, field_id);
        try self.writeListBegin(elem_type, size);
    }
};

test "thrift varint decoding" {
    const data = [_]u8{ 0x85, 0x02 }; // 261 in varint
    var reader = Reader.init(&data);
    const val = try reader.readVarInt(u32);
    try std.testing.expectEqual(@as(u32, 261), val);
}

test "thrift writer varint encoding" {
    var writer = Writer.init(std.testing.allocator);
    defer writer.deinit();

    try writer.writeVarInt(@as(u32, 261));
    try std.testing.expectEqualSlices(u8, &[_]u8{ 0x85, 0x02 }, writer.bytes());
}

test "thrift writer zigzag encoding" {
    var writer = Writer.init(std.testing.allocator);
    defer writer.deinit();

    // ZigZag: 0 -> 0, -1 -> 1, 1 -> 2, -2 -> 3
    try writer.writeZigZag(@as(i32, 0));
    try std.testing.expectEqualSlices(u8, &[_]u8{0}, writer.bytes());

    writer.reset();
    try writer.writeZigZag(@as(i32, -1));
    try std.testing.expectEqualSlices(u8, &[_]u8{1}, writer.bytes());

    writer.reset();
    try writer.writeZigZag(@as(i32, 1));
    try std.testing.expectEqualSlices(u8, &[_]u8{2}, writer.bytes());

    writer.reset();
    try writer.writeZigZag(@as(i32, -2));
    try std.testing.expectEqualSlices(u8, &[_]u8{3}, writer.bytes());
}

test "thrift writer roundtrip" {
    var writer = Writer.init(std.testing.allocator);
    defer writer.deinit();

    // Write a simple struct with fields
    writer.writeStructBegin();
    try writer.writeFieldI32(1, 42);
    try writer.writeFieldString(2, "hello");
    try writer.writeFieldI64(3, -12345);
    try writer.writeStructEnd();

    // Read it back
    var reader = Reader.init(writer.bytes());
    reader.readStructBegin();

    const f1 = try reader.readFieldBegin();
    try std.testing.expectEqual(@as(i16, 1), f1.id);
    try std.testing.expectEqual(Type.I32, f1.type);
    const v1 = try reader.readZigZag(i32);
    try std.testing.expectEqual(@as(i32, 42), v1);

    const f2 = try reader.readFieldBegin();
    try std.testing.expectEqual(@as(i16, 2), f2.id);
    try std.testing.expectEqual(Type.Binary, f2.type);
    const v2 = try reader.readString();
    try std.testing.expectEqualStrings("hello", v2);

    const f3 = try reader.readFieldBegin();
    try std.testing.expectEqual(@as(i16, 3), f3.id);
    try std.testing.expectEqual(Type.I64, f3.type);
    const v3 = try reader.readZigZag(i64);
    try std.testing.expectEqual(@as(i64, -12345), v3);

    const f4 = try reader.readFieldBegin();
    try std.testing.expectEqual(Type.Stop, f4.type);
}
