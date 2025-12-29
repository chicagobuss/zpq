const std = @import("std");

pub const Error = error{
    CorruptInput,
    OutputTooSmall,
};

/// Decodes the length of the uncompressed data from the preamble.
/// Returns the length and the number of bytes read from src.
pub fn decodedLen(src: []const u8) !struct { u64, usize } {
    var len: u64 = 0;
    var shift: u6 = 0;
    var count: usize = 0;

    for (src) |b| {
        count += 1;
        len |= @as(u64, b & 0x7f) << shift;
        if (b & 0x80 == 0) return .{ len, count };
        shift += 7;
        if (shift >= 64) return error.CorruptInput;
    }
    return error.CorruptInput;
}

/// Decompress raw snappy data.
/// src: Compressed data.
/// dest: Pre-allocated buffer for uncompressed data (must be large enough).
/// Returns: Number of bytes written to dest.
pub fn uncompress(src: []const u8, dest: []u8) !usize {
    const dlen_info = try decodedLen(src);
    const expected_len = dlen_info[0];
    var s_idx = dlen_info[1]; // Source index
    var d_idx: usize = 0; // Dest index

    if (dest.len < expected_len) return error.OutputTooSmall;

    while (s_idx < src.len) {
        if (d_idx >= expected_len) break;

        const tag_byte = src[s_idx];
        // std.debug.print("idx={d} tag={x}\n", .{s_idx, tag_byte});
        s_idx += 1;
        const tag = tag_byte & 0x03;

        if (tag == 0) {
            // Literal
            var len = @as(usize, tag_byte >> 2) + 1;
            if (len > 60) {
                // Extended length
                const len_bytes = len - 60; // 1 to 4 bytes
                if (s_idx + len_bytes > src.len) return error.CorruptInput;
                
                len = 0;
                var i: usize = 0;
                while (i < len_bytes) : (i += 1) {
                    len |= @as(usize, src[s_idx]) << @as(u6, @intCast(i * 8));
                    s_idx += 1;
                }
                len += 1;
            }

            if (s_idx + len > src.len) return error.CorruptInput;
            if (d_idx + len > dest.len) return error.OutputTooSmall;

            @memcpy(dest[d_idx .. d_idx + len], src[s_idx .. s_idx + len]);
            d_idx += len;
            s_idx += len;

        } else {
            // Copy (Back-reference)
            var offset: usize = 0;
            var len: usize = 0;

            switch (tag) {
                1 => {
                    // Copy with 1-byte offset
                    // Format: len (3 bits) . offset (high 3 bits) | offset (low 8 bits from next byte)
                    // The length is stored in the upper 3 bits of the tag byte (bits 2-4) + 4
                    len = ((tag_byte >> 2) & 0x07) + 4;
                    const high_offset = (tag_byte >> 5) & 0x07;
                    if (s_idx >= src.len) return error.CorruptInput;
                    offset = (@as(usize, high_offset) << 8) | src[s_idx];
                    s_idx += 1;
                },
                2 => {
                    // Copy with 2-byte offset
                    len = ((tag_byte >> 2) & 0x3F) + 1;
                    if (s_idx + 2 > src.len) return error.CorruptInput;
                    offset = std.mem.readInt(u16, src[s_idx..][0..2], .little);
                    s_idx += 2;
                },
                3 => {
                    // Copy with 4-byte offset
                    len = ((tag_byte >> 2) & 0x3F) + 1;
                    if (s_idx + 4 > src.len) return error.CorruptInput;
                    offset = std.mem.readInt(u32, src[s_idx..][0..4], .little);
                    s_idx += 4;
                },
                else => unreachable,
            }

            if (offset == 0 or offset > d_idx) return error.CorruptInput;
            
            // Standard memcpy cannot handle overlapping ranges where src < dest
            // But here we are copying from *previous* output.
            // We must copy byte by byte or careful block copy because strict overlap might happen
            // e.g. "aaaaa" encoded as "a" then "copy offset 1 length 4"
            // Zig's std.mem.copyForwards handles overlap correctly?
            // Actually, for LZ77 with overlap (offset < len), we are repeating a pattern.
            // std.mem.copyForwards works for overlapping buffers but specifically:
            // "The source and destination may overlap."
            
            // However, we can't slice dest[d_idx - offset ..] directly if we are writing to it
            // simultaneously if the compiler thinks they alias in a bad way.
            // But logically:
            var i: usize = 0;
            while (i < len) : (i += 1) {
                if (d_idx >= dest.len) return error.OutputTooSmall;
                dest[d_idx] = dest[d_idx - offset];
                d_idx += 1;
            }
        }
    }

    return d_idx;
}

test "snappy basic" {
    // "Wiki" encoded in snappy
    // 0x04 (len 4)
    // 0x0c (Literal len 4-1=3 stored in top 6 bits -> 0x0C, tag 00) "Wiki"
    const input = [_]u8{ 4, 0x0c, 'W', 'i', 'k', 'i' };
    var buf: [100]u8 = undefined;
    const len = try uncompress(&input, &buf);
    try std.testing.expectEqual(@as(usize, 4), len);
    try std.testing.expectEqualStrings("Wiki", buf[0..len]);
}

    test "snappy repeat" {
        // "aaaaa" compressed: 5, 0x00, 'a', 0x01, 0x01
        // Preamble: 5 (uncompressed len)
        // 0x00: Literal tag 00, len 1 (0 >> 2 + 1 = 1). Value 'a'.
        // 0x01: Copy tag 01, len 4 (0x01 >> 2 & 7 + 4 = 4). Offset 1 (0x01).
        const input = [_]u8{ 0x05, 0x00, 'a', 0x01, 0x01 };
        var buf: [100]u8 = undefined;
        const len = try uncompress(&input, &buf);
        try std.testing.expectEqual(@as(usize, 5), len);
        try std.testing.expectEqualStrings("aaaaa", buf[0..len]);
    }

