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

    pub fn readFieldBegin(self: *Reader) !struct { type: Type, id: i16 } {
        const byte = try self.readByte();
        if (byte == 0) return .{ .type = .Stop, .id = 0 };

        const delta = (byte >> 4) & 0x0f;
        const type_id = byte & 0x0f;
        
        var field_id: i16 = 0;
        if (delta == 0) {
            field_id = try self.readZigZag(i16);
        } else {
            field_id = self.last_field_id + @as(i16, delta);
        }

        self.last_field_id = field_id;
        const t = @as(Type, @enumFromInt(type_id));
        
        return .{ .type = t, .id = field_id };
    }

    pub fn readStructBegin(self: *Reader) void {
        self.last_field_id = 0;
    }

    pub fn readString(self: *Reader) ![]const u8 {
        const len = try self.readVarInt(usize);
        if (self.pos + len > self.data.len) return error.EndOfStream;
        const str = self.data[self.pos .. self.pos + len];
        self.pos += len;
        return str;
    }

    pub fn skip(self: *Reader, t: Type) !void {
        switch (t) {
            .Stop => return,
            .True, .False => return,
            .Byte => _ = try self.readByte(),
            .I16, .I32, .I64 => _ = try self.readZigZag(i64),
            .Double => self.pos += 8,
            .Binary => {
                const len = try self.readVarInt(usize);
                self.pos += len;
            },
            .List, .Set => {
                const header = try self.readByte();
                var size = @as(usize, header >> 4);
                const elem_type = @as(Type, @enumFromInt(header & 0x0f));
                if (size == 0xF) {
                    size = try self.readVarInt(usize);
                }
                var i: usize = 0;
                while (i < size) : (i += 1) {
                    try self.skip(elem_type);
                }
            },
            .Map => {
                const count = try self.readVarInt(usize);
                if (count == 0) return;
                const types = try self.readByte();
                const key_type = @as(Type, @enumFromInt(types >> 4));
                const val_type = @as(Type, @enumFromInt(types & 0x0f));
                var i: usize = 0;
                while (i < count) : (i += 1) {
                    try self.skip(key_type);
                    try self.skip(val_type);
                }
            },
            .Struct => {
                const saved_id = self.last_field_id;
                self.readStructBegin();
                while (true) {
                    const field = try self.readFieldBegin();
                    if (field.type == .Stop) break;
                    try self.skip(field.type);
                }
                self.last_field_id = saved_id;
            }
        }
    }
};
