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

    pub fn readInt96(self: *Decoder) ![12]u8 {
        if (self.pos + 12 > self.data.len) return error.EndOfStream;
        const val = self.data[self.pos..][0..12].*;
        self.pos += 12;
        return val;
    }

    pub fn readFixedLenByteArray(self: *Decoder, len: usize) ![]const u8 {
        if (self.pos + len > self.data.len) return error.EndOfStream;
        const str = self.data[self.pos..][0..len];
        self.pos += len;
        return str;
    }

    pub fn readBatch(self: *Decoder, buffer: anytype) !usize {
        const T = @TypeOf(buffer[0]);

        // Booleans are bit-packed in Parquet
        if (T == bool) {
            return self.readBoolBatch(buffer);
        }

        const size = switch (T) {
            i32, f32 => 4,
            i64, f64 => 8,
            [12]u8 => 12,
            else => return error.UnsupportedTypeForBatchRead,
        };

        const num_to_read = @min(buffer.len, (self.data.len - self.pos) / size);
        if (num_to_read == 0) return 0;

        // Use @ptrCast to copy data in bulk if possible, or just a loop
        if (T == [12]u8) {
            for (0..num_to_read) |i| {
                buffer[i] = self.data[self.pos + i * 12 ..][0..12].*;
            }
        } else {
            const byte_len = num_to_read * size;
            const src = self.data[self.pos .. self.pos + byte_len];
            const dst = std.mem.sliceAsBytes(buffer[0..num_to_read]);
            @memcpy(dst, src);
        }

        self.pos += num_to_read * size;
        return num_to_read;
    }

    /// Read bit-packed booleans (1 bit per value, LSB first within each byte)
    fn readBoolBatch(self: *Decoder, buffer: []bool) usize {
        var count: usize = 0;
        while (count < buffer.len and self.pos < self.data.len) {
            const byte = self.data[self.pos];
            // Each byte contains up to 8 booleans, LSB first
            for (0..8) |bit| {
                if (count >= buffer.len) break;
                buffer[count] = ((byte >> @intCast(bit)) & 1) == 1;
                count += 1;
            }
            self.pos += 1;
        }
        return count;
    }

    pub fn skipByteArray(self: *Decoder) !void {
        if (self.pos + 4 > self.data.len) return error.EndOfStream;
        const len = std.mem.readInt(u32, self.data[self.pos..][0..4], .little);
        self.pos += 4;
        if (self.pos + len > self.data.len) return error.EndOfStream;
        self.pos += len;
    }

    pub fn skipFixedLenByteArray(self: *Decoder, len: usize) !void {
        if (self.pos + len > self.data.len) return error.EndOfStream;
        self.pos += len;
    }

    pub fn hasMore(self: *Decoder) bool {
        return self.pos < self.data.len;
    }

    pub fn skip(self: *Decoder, len: usize) !void {
        if (self.pos + len > self.data.len) return error.EndOfStream;
        self.pos += len;
    }
};
