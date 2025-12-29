const std = @import("std");

pub const Decoder = struct {
    data: []const u8,
    pos: usize,

    pub fn init(data: []const u8) Decoder {
        return Decoder{ .data = data, .pos = 0 };
    }

    pub fn readInt32(self: *Decoder) !i32 {
        if (self.pos + 4 > self.data.len) return error.EndOfStream;
        const val = std.mem.readInt(i32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return val;
    }

    pub fn readInt64(self: *Decoder) !i64 {
        if (self.pos + 8 > self.data.len) return error.EndOfStream;
        const val = std.mem.readInt(i64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return val;
    }

    pub fn readFloat(self: *Decoder) !f32 {
        if (self.pos + 4 > self.data.len) return error.EndOfStream;
        const val = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        return @bitCast(val);
    }

    pub fn readDouble(self: *Decoder) !f64 {
        if (self.pos + 8 > self.data.len) return error.EndOfStream;
        const val = std.mem.readInt(u64, self.data[self.pos..][0..8], .little);
        self.pos += 8;
        return @bitCast(val);
    }

    pub fn readByteArray(self: *Decoder) ![]const u8 {
        if (self.pos + 4 > self.data.len) return error.EndOfStream;
        const len = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        
        if (self.pos + len > self.data.len) return error.EndOfStream;
        const str = self.data[self.pos..][0..len];
        self.pos += len;
        return str;
    }
    
    pub fn hasMore(self: *Decoder) bool {
        return self.pos < self.data.len;
    }
};

