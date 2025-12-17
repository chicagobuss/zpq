const std = @import("std");

pub const RleDecoder = struct {
    data: []const u8,
    bit_width: u8,
    pos: usize,
    
    current_count: u32 = 0,
    current_value: u64 = 0,
    
    bit_packed_groups_remaining: u32 = 0,
    buffered_values: [8]u64 = undefined,
    buffered_count: u8 = 0,
    buffered_index: u8 = 0,

    pub fn init(data: []const u8, bit_width: u8) RleDecoder {
        return RleDecoder{
            .data = data,
            .bit_width = bit_width,
            .pos = 0,
        };
    }

    pub fn next(self: *RleDecoder) !?u64 {
        if (self.buffered_index < self.buffered_count) {
            const val = self.buffered_values[self.buffered_index];
            self.buffered_index += 1;
            return val;
        }

        if (self.current_count > 0) {
            self.current_count -= 1;
            return self.current_value;
        }
        
        if (self.bit_packed_groups_remaining > 0) {
            try self.readBitPackedGroup();
            self.bit_packed_groups_remaining -= 1;
            return self.next();
        }

        if (self.pos >= self.data.len) return null;
        const header = try self.readVarInt();
        
        const is_bit_packed = (header & 1) == 1;
        const count = header >> 1;
        
        if (count == 0) return null; 
        
        if (is_bit_packed) {
            self.bit_packed_groups_remaining = count;
            try self.readBitPackedGroup();
            self.bit_packed_groups_remaining -= 1;
            return self.next();
        } else {
            // RLE
            const byte_width = (self.bit_width + 7) / 8;
            var val: u64 = 0;
            var i: usize = 0;
            while (i < byte_width) : (i += 1) {
                if (self.pos >= self.data.len) return error.EndOfStream;
                val |= @as(u64, self.data[self.pos]) << @intCast(i * 8);
                self.pos += 1;
            }
            self.current_value = val;
            self.current_count = count;
            
            self.current_count -= 1;
            return self.current_value;
        }
    }
    
    fn readBitPackedGroup(self: *RleDecoder) !void {
        const bytes_needed = self.bit_width; // 8 values * bit_width / 8 = bit_width bytes
        if (self.pos + bytes_needed > self.data.len) return error.EndOfStream;
        
        const group_data = self.data[self.pos .. self.pos + bytes_needed];
        self.pos += bytes_needed;
        
        try unpack8Values(&self.buffered_values, group_data, self.bit_width);
        
        self.buffered_count = 8;
        self.buffered_index = 0;
    }
    
    fn readVarInt(self: *RleDecoder) !u32 {
        var result: u32 = 0;
        var shift: u5 = 0;
        while (true) {
            if (self.pos >= self.data.len) return error.EndOfStream;
            const byte = self.data[self.pos];
            self.pos += 1;
            result |= @as(u32, byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) break;
            shift += 7;
            if (shift > 31) return error.CorruptData; // Prevent overflow
        }
        return result;
    }
};

fn unpack8Values(dest: *[8]u64, src: []const u8, bit_width: u8) !void {
    if (bit_width == 0) {
        @memset(dest, 0);
        return;
    }
    
    var byte_idx: usize = 0;
    var bit_idx: u3 = 0; 
    
    for (dest, 0..) |_, i| {
        var val: u64 = 0;
        var bits_read: u8 = 0;
        
        while (bits_read < bit_width) {
            if (byte_idx >= src.len) return error.EndOfStream;
            
            const bit = (src[byte_idx] >> bit_idx) & 1;
            val |= @as(u64, bit) << @intCast(bits_read);
            
            bits_read += 1;
            bit_idx +%= 1;
            if (bit_idx == 0) {
                byte_idx += 1;
            }
        }
        dest[i] = val;
    }
}

test "RLE Decoder - RLE Run" {
    const data = [_]u8{ 0x10, 0x05 };
    var dec = RleDecoder.init(&data, 3);
    
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        const val = try dec.next();
        try std.testing.expectEqual(@as(?u64, 5), val);
    }
    try std.testing.expectEqual(@as(?u64, null), try dec.next());
}

test "RLE Decoder - BitPacked Run" {
    // Header: (1 group << 1) | 1 = 3 (0x03)
    // Data: 0x88 0xC6 0xFA (from rle_proto test)
    const data = [_]u8{ 0x03, 0x88, 0xC6, 0xFA };
    var dec = RleDecoder.init(&data, 3);
    
    try std.testing.expectEqual(@as(?u64, 0), try dec.next());
    try std.testing.expectEqual(@as(?u64, 1), try dec.next());
    try std.testing.expectEqual(@as(?u64, 2), try dec.next());
    try std.testing.expectEqual(@as(?u64, 3), try dec.next());
    try std.testing.expectEqual(@as(?u64, 4), try dec.next());
    try std.testing.expectEqual(@as(?u64, 5), try dec.next());
    try std.testing.expectEqual(@as(?u64, 6), try dec.next());
    try std.testing.expectEqual(@as(?u64, 7), try dec.next());
    
    try std.testing.expectEqual(@as(?u64, null), try dec.next());
}

