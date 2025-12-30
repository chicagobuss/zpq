const std = @import("std");

/// RLE/Bit-Packed Hybrid Decoder
/// Based on DuckDB's implementation - tracks individual literal count and bit position
pub const RleDecoder = struct {
    data: []const u8,
    bit_width: u8,
    pos: usize,

    // RLE state
    repeat_count: u32 = 0,
    current_value: u64 = 0,

    // Bit-packed (literal) state
    literal_count: u32 = 0,
    bitpack_pos: u3 = 0, // Sub-byte bit position (0-7)

    // Cached values
    byte_width: u8,
    mask: u64,

    pub fn init(data: []const u8, bit_width: u8) RleDecoder {
        const byte_width = (bit_width + 7) / 8;
        const mask: u64 = if (bit_width >= 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(bit_width)) - 1;
        return RleDecoder{
            .data = data,
            .bit_width = bit_width,
            .pos = 0,
            .byte_width = byte_width,
            .mask = mask,
        };
    }

    pub fn next(self: *RleDecoder) !?u64 {
        // First check if we have a repeat value pending
        if (self.repeat_count > 0) {
            self.repeat_count -= 1;
            return self.current_value;
        }

        // Check if we have literal values pending
        if (self.literal_count > 0) {
            return try self.readBitPackedValue();
        }

        // Need to read next header
        if (!self.nextCounts()) {
            return null;
        }

        // Now we should have either repeat_count or literal_count set
        if (self.repeat_count > 0) {
            self.repeat_count -= 1;
            return self.current_value;
        }

        if (self.literal_count > 0) {
            return try self.readBitPackedValue();
        }

        return null;
    }

    /// Read multiple values into a buffer. Returns number of values actually read.
    pub fn nextBatch(self: *RleDecoder, buffer: []u64) !usize {
        var out_pos: usize = 0;
        while (out_pos < buffer.len) {
            // Check if we have repeat values
            if (self.repeat_count > 0) {
                const count = @min(buffer.len - out_pos, self.repeat_count);
                @memset(buffer[out_pos .. out_pos + count], self.current_value);
                self.repeat_count -= @intCast(count);
                out_pos += count;
                continue;
            }

            // Check if we have literal values
            if (self.literal_count > 0) {
                // Try to use vectorized unpacking for groups of 8
                if (self.bitpack_pos == 0 and self.literal_count >= 8 and (buffer.len - out_pos) >= 8) {
                    const count = @min(self.literal_count, (buffer.len - out_pos)) / 8 * 8;
                    for (0..count / 8) |_| {
                        self.readBitPackedBatch8(buffer[out_pos .. out_pos + 8]);
                        out_pos += 8;
                        self.literal_count -= 8;
                    }
                    continue;
                }

                // Fallback to scalar
                buffer[out_pos] = try self.readBitPackedValue();
                out_pos += 1;
                continue;
            }

            // Need new counts
            if (!self.nextCounts()) break;
        }
        return out_pos;
    }

    /// Optimized unpacking for 8 values. Requires bitpack_pos == 0.
    fn readBitPackedBatch8(self: *RleDecoder, out: []u64) void {
        std.debug.assert(self.bitpack_pos == 0);
        std.debug.assert(out.len >= 8);

        const bw = self.bit_width;
        if (bw == 0) {
            @memset(out[0..8], 0);
            return;
        }

        // Use inline switch for specialized bit-unpacking kernels
        switch (bw) {
            inline 1...32 => |width| {
                const total_bits: u16 = @as(u16, width) * 8;
                const total_bytes = (total_bits + 7) / 8;
                
                // Choose smallest container that fits all 8 values
                const Container = comptime switch (width) {
                    1...8 => u64,
                    9...16 => u128,
                    17...32 => u256,
                    else => unreachable,
                };

                var bits: Container = 0;
                const remaining = self.data.len - self.pos;
                if (remaining >= total_bytes) {
                    if (remaining >= @sizeOf(Container)) {
                        // Fast path: direct unaligned load
                        bits = std.mem.readInt(Container, self.data[self.pos..][0..@sizeOf(Container)], .little);
                    } else {
                        // Middle path: enough for group but not full Container load
                        var buf: [@sizeOf(Container)]u8 = @splat(0);
                        @memcpy(buf[0..total_bytes], self.data[self.pos .. self.pos + total_bytes]);
                        bits = std.mem.readInt(Container, &buf, .little);
                    }
                    self.pos += total_bytes;
                } else {
                    // Slow path: bounded load (not even enough for full group)
                    const limit = remaining;
                    var buf: [@sizeOf(Container)]u8 = @splat(0);
                    @memcpy(buf[0..limit], self.data[self.pos .. self.pos + limit]);
                    bits = std.mem.readInt(Container, &buf, .little);
                    self.pos += limit;
                }

                const v_bits: @Vector(8, Container) = @splat(bits);
                comptime var shifts: [8]std.math.Log2Int(Container) = undefined;
                inline for (0..8) |i| {
                    shifts[i] = @intCast(i * width);
                }
                const v_shifts: @Vector(8, std.math.Log2Int(Container)) = shifts;
                const mask: Container = (@as(Container, 1) << width) - 1;
                const v_res = (v_bits >> v_shifts) & @as(@Vector(8, Container), @splat(mask));
                
                inline for (0..8) |i| {
                    out[i] = @intCast(v_res[i]);
                }
            },
            else => {
                // Fallback to scalar for very large widths
                for (0..8) |i| {
                    out[i] = self.readBitPackedValue() catch unreachable;
                }
            },
        }
    }

    /// Read next RLE/literal header and set up state
    fn nextCounts(self: *RleDecoder) bool {
        // If we're mid-byte in bit-packed mode, advance to next byte
        if (self.bitpack_pos != 0) {
            self.pos += 1;
            self.bitpack_pos = 0;
        }

        if (self.pos >= self.data.len) return false;

        // Read varint header
        const header = self.readVarInt() orelse return false;

        // LSB indicates if it's literal (bit-packed) or RLE
        const is_literal = (header & 1) == 1;
        const count = header >> 1;

        if (count == 0) return false;

        if (is_literal) {
            // Literal run: count is number of groups of 8 values
            self.literal_count = count * 8;
        } else {
            // RLE run: count is number of repeated values
            self.repeat_count = count;

            // Read the repeated value (byte_width bytes, little-endian)
            if (self.pos + self.byte_width > self.data.len) return false;

            var val: u64 = 0;
            for (0..self.byte_width) |i| {
                val |= @as(u64, self.data[self.pos]) << @intCast(i * 8);
                self.pos += 1;
            }
            self.current_value = val;
        }

        return true;
    }

    /// Read a single bit-packed value
    fn readBitPackedValue(self: *RleDecoder) !u64 {
        if (self.literal_count == 0) return error.NoLiteralValues;

        // Handle bit_width = 0 case (all zeros)
        if (self.bit_width == 0) {
            self.literal_count -= 1;
            return 0;
        }

        var val: u64 = 0;
        var bits_read: u8 = 0;

        while (bits_read < self.bit_width) {
            if (self.pos >= self.data.len) return error.EndOfStream;

            // How many bits can we read from current byte?
            const bits_in_byte: u8 = 8 - @as(u8, self.bitpack_pos);
            const bits_needed = self.bit_width - bits_read;
            const bits_to_read: u8 = @min(bits_in_byte, bits_needed);

            if (bits_to_read == 0) return error.InvalidBitWidth;

            // Extract bits from current byte
            const byte_val = self.data[self.pos];
            const shifted = byte_val >> self.bitpack_pos;
            const extracted_mask: u8 = @intCast((@as(u16, 1) << @intCast(bits_to_read)) - 1);
            const extracted = shifted & extracted_mask;

            val |= @as(u64, extracted) << @intCast(bits_read);
            bits_read += bits_to_read;

            // Advance bit position
            const new_pos = @as(u8, self.bitpack_pos) + bits_to_read;
            if (new_pos >= 8) {
                self.pos += 1;
                self.bitpack_pos = 0;
            } else {
                self.bitpack_pos = @intCast(new_pos);
            }
        }

        self.literal_count -= 1;
        return val & self.mask;
    }

    fn readVarInt(self: *RleDecoder) ?u32 {
        var result: u32 = 0;
        var shift: u5 = 0;
        while (true) {
            if (self.pos >= self.data.len) return null;
            const byte = self.data[self.pos];
            self.pos += 1;
            result |= @as(u32, byte & 0x7f) << shift;
            if ((byte & 0x80) == 0) break;
            shift +|= 7;
            if (shift > 28) return null; // Prevent overflow
        }
        return result;
    }

    /// Skip n values
    pub fn skip(self: *RleDecoder, count: u32) !void {
        var remaining = count;

        while (remaining > 0) {
            if (self.repeat_count > 0) {
                const skip_amt = @min(remaining, self.repeat_count);
                self.repeat_count -= skip_amt;
                remaining -= skip_amt;
            } else if (self.literal_count > 0) {
                const skip_amt = @min(remaining, self.literal_count);
                // Need to advance bit position
                const total_bits = @as(u32, self.bit_width) * skip_amt;
                const bit_offset = @as(u32, self.bitpack_pos) + total_bits;
                self.pos += bit_offset / 8;
                self.bitpack_pos = @intCast(bit_offset % 8);
                self.literal_count -= skip_amt;
                remaining -= skip_amt;
            } else {
                if (!self.nextCounts()) return error.EndOfStream;
            }
        }
    }
};

test "RLE Decoder - RLE Run" {
    // Header: (8 << 1) | 0 = 16 (0x10) - RLE run of 8 values
    // Value: 0x05 (1 byte for bit_width=3)
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
    // Header: (1 << 1) | 1 = 3 (0x03) - 1 group of 8 literal values
    // Data: 0x88 0xC6 0xFA (from rle_proto test: values 0,1,2,3,4,5,6,7 at 3 bits each)
    // 0x88 = 10001000 -> bits 0-2: 000 (0), bits 3-5: 001 (1), bits 6-7: 10 (partial 2)
    // 0xC6 = 11000110 -> bit 0: 0 (completes 2), bits 1-3: 011 (3), bits 4-6: 100 (4), bit 7: 1 (partial 5)
    // 0xFA = 11111010 -> bits 0-1: 01 (completes 5), bits 2-4: 110 (6), bits 5-7: 111 (7)
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

test "RLE Decoder - Mixed Run" {
    // RLE of 4x value 3, then literal group of 8
    // RLE header: (4 << 1) | 0 = 8 (0x08)
    // RLE value: 3 (0x03) at 3 bits = 1 byte
    // Literal header: (1 << 1) | 1 = 3 (0x03)
    // Literal data: 0x88 0xC6 0xFA (values 0-7)
    const data = [_]u8{ 0x08, 0x03, 0x03, 0x88, 0xC6, 0xFA };
    var dec = RleDecoder.init(&data, 3);

    // First 4 should be 3 (RLE)
    try std.testing.expectEqual(@as(?u64, 3), try dec.next());
    try std.testing.expectEqual(@as(?u64, 3), try dec.next());
    try std.testing.expectEqual(@as(?u64, 3), try dec.next());
    try std.testing.expectEqual(@as(?u64, 3), try dec.next());

    // Next 8 should be 0-7 (literal)
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

test "RLE Decoder - Bit Width 1" {
    // This is common for definition levels
    // Header: (2 << 1) | 1 = 5 (0x05) - 2 groups of 8 = 16 literal values
    // With bit_width=1: 16 values = 16 bits = 2 bytes
    // 0xFF = 11111111 (8 ones)
    // 0x00 = 00000000 (8 zeros)
    const data = [_]u8{ 0x05, 0xFF, 0x00 };
    var dec = RleDecoder.init(&data, 1);

    // First 8 should be 1
    var i: usize = 0;
    while (i < 8) : (i += 1) {
        try std.testing.expectEqual(@as(?u64, 1), try dec.next());
    }
    // Next 8 should be 0
    while (i < 16) : (i += 1) {
        try std.testing.expectEqual(@as(?u64, 0), try dec.next());
    }

    try std.testing.expectEqual(@as(?u64, null), try dec.next());
}
