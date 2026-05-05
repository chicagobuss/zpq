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

// ============================================================================
// Compression
// ============================================================================

pub const CompressError = error{
    OutputTooSmall,
};

/// Maximum output size for snappy compression.
/// Snappy can expand data slightly in worst case.
pub fn maxCompressedLen(src_len: usize) usize {
    // Snappy format: varint length + worst case 1 byte overhead per 6 bytes
    return 5 + src_len + src_len / 6 + 1;
}

/// Encode a varint (used for uncompressed length preamble).
/// Returns number of bytes written.
fn encodeVarint(dest: []u8, value: u64) usize {
    var v = value;
    var i: usize = 0;
    while (v >= 0x80) {
        dest[i] = @intCast((v & 0x7f) | 0x80);
        v >>= 7;
        i += 1;
    }
    dest[i] = @intCast(v);
    return i + 1;
}

/// Emit a literal to the output buffer.
/// Returns number of bytes written.
fn emitLiteral(dest: []u8, literal: []const u8) CompressError!usize {
    const n = literal.len;
    if (n == 0) return 0;

    var d_idx: usize = 0;

    if (n <= 60) {
        // Short literal: length - 1 in top 6 bits, tag 00 in bottom 2 bits
        if (d_idx >= dest.len) return error.OutputTooSmall;
        dest[d_idx] = @intCast(((n - 1) << 2) | 0);
        d_idx += 1;
    } else if (n <= 256) {
        // 1-byte extended length
        if (d_idx + 2 > dest.len) return error.OutputTooSmall;
        dest[d_idx] = (60 << 2) | 0; // tag = 60, indicates 1 extra byte
        dest[d_idx + 1] = @intCast(n - 1);
        d_idx += 2;
    } else if (n <= 65536) {
        // 2-byte extended length
        if (d_idx + 3 > dest.len) return error.OutputTooSmall;
        dest[d_idx] = (61 << 2) | 0; // tag = 61, indicates 2 extra bytes
        std.mem.writeInt(u16, dest[d_idx + 1 ..][0..2], @intCast(n - 1), .little);
        d_idx += 3;
    } else if (n <= 16777216) {
        // 3-byte extended length
        if (d_idx + 4 > dest.len) return error.OutputTooSmall;
        dest[d_idx] = (62 << 2) | 0; // tag = 62, indicates 3 extra bytes
        const len_minus_1: u32 = @intCast(n - 1);
        dest[d_idx + 1] = @intCast(len_minus_1 & 0xff);
        dest[d_idx + 2] = @intCast((len_minus_1 >> 8) & 0xff);
        dest[d_idx + 3] = @intCast((len_minus_1 >> 16) & 0xff);
        d_idx += 4;
    } else {
        // 4-byte extended length
        if (d_idx + 5 > dest.len) return error.OutputTooSmall;
        dest[d_idx] = (63 << 2) | 0; // tag = 63, indicates 4 extra bytes
        std.mem.writeInt(u32, dest[d_idx + 1 ..][0..4], @intCast(n - 1), .little);
        d_idx += 5;
    }

    // Copy literal bytes
    if (d_idx + n > dest.len) return error.OutputTooSmall;
    @memcpy(dest[d_idx .. d_idx + n], literal);
    d_idx += n;

    return d_idx;
}

/// Emit a copy (back-reference) to the output buffer.
/// Returns number of bytes written.
fn emitCopy(dest: []u8, offset: usize, length: usize) CompressError!usize {
    var d_idx: usize = 0;
    var len = length;

    // Snappy copy commands have max length limits, may need multiple copies
    while (len > 0) {
        if (offset < 2048 and len >= 4 and len <= 11) {
            // Copy with 1-byte offset (tag 01)
            // Format: (len-4) in bits 2-4, high 3 bits of offset in bits 5-7, tag 01
            // Next byte: low 8 bits of offset
            if (d_idx + 2 > dest.len) return error.OutputTooSmall;
            // Same narrowing-on-shift footgun as below — keep operands in usize.
            const tag_byte: u8 = @intCast((@as(usize, len - 4) << 2) | (@as(usize, offset >> 8) << 5) | 1);
            dest[d_idx] = tag_byte;
            dest[d_idx + 1] = @intCast(offset & 0xff);
            d_idx += 2;
            return d_idx;
        } else if (offset < 65536) {
            // Copy with 2-byte offset (tag 02). copy_len in [1..64] →
            // (copy_len - 1) << 2 in [0..252], OR'd with the tag bits
            // (2) gives a byte in [2..254]. The earlier inline form
            // `@intCast(((copy_len-1)<<2)|2)` was triggering the result-
            // type to narrow the SHIFT to u8, where for copy_len > 32
            // the value 144+ would lose the 0x80 bit and produce a
            // wrong tag byte (e.g. 0x12 instead of 0x92 for copy_len=37).
            // Forcing the operands into usize first keeps full precision.
            const copy_len = @min(len, 64);
            if (d_idx + 3 > dest.len) return error.OutputTooSmall;
            const tag_byte: u8 = @intCast((@as(usize, copy_len - 1) << 2) | 2);
            dest[d_idx] = tag_byte;
            std.mem.writeInt(u16, dest[d_idx + 1 ..][0..2], @intCast(offset), .little);
            d_idx += 3;
            len -= copy_len;
        } else {
            // Copy with 4-byte offset (tag 03)
            const copy_len = @min(len, 64);
            if (d_idx + 5 > dest.len) return error.OutputTooSmall;
            const tag_byte: u8 = @intCast((@as(usize, copy_len - 1) << 2) | 3);
            dest[d_idx] = tag_byte;
            std.mem.writeInt(u32, dest[d_idx + 1 ..][0..4], @intCast(offset), .little);
            d_idx += 5;
            len -= copy_len;
        }
    }

    return d_idx;
}

/// Hash function for 4-byte sequences (used for match finding).
fn hash4(data: *const [4]u8) u32 {
    const v = std.mem.readInt(u32, data, .little);
    // Knuth multiplicative hash
    return @truncate((v *% 0x1e35a7bd) >> 17);
}

/// Compress data using Snappy format.
/// dest must be at least maxCompressedLen(src.len) bytes.
/// Returns the number of bytes written.
pub fn compress(src: []const u8, dest: []u8) CompressError!usize {
    if (src.len == 0) {
        if (dest.len < 1) return error.OutputTooSmall;
        dest[0] = 0; // varint 0
        return 1;
    }

    var d_idx: usize = 0;

    // Write uncompressed length as varint
    d_idx += encodeVarint(dest[d_idx..], src.len);

    // For very short inputs, just emit as literal
    if (src.len < 4) {
        d_idx += try emitLiteral(dest[d_idx..], src);
        return d_idx;
    }

    // Hash table for match finding (maps hash -> position)
    // Using 14 bits = 16384 entries, same as reference implementation
    const table_bits = 14;
    const table_size = 1 << table_bits;
    const table_mask = table_size - 1;
    var table: [table_size]u32 = [_]u32{0} ** table_size;

    var s_idx: usize = 0; // Current position in source
    var literal_start: usize = 0; // Start of current literal run

    while (s_idx + 4 <= src.len) {
        // Hash current 4 bytes
        const h = hash4(src[s_idx..][0..4]) & table_mask;
        const candidate = table[h];
        table[h] = @intCast(s_idx);

        // Check for match
        if (candidate > 0 and s_idx - candidate < 65536 and
            std.mem.eql(u8, src[candidate..][0..4], src[s_idx..][0..4]))
        {
            // Found a match! First emit any pending literal
            if (s_idx > literal_start) {
                d_idx += try emitLiteral(dest[d_idx..], src[literal_start..s_idx]);
            }

            // Extend match forward
            var match_len: usize = 4;
            while (s_idx + match_len < src.len and
                candidate + match_len < s_idx and
                src[s_idx + match_len] == src[candidate + match_len])
            {
                match_len += 1;
            }

            // Emit copy
            const offset = s_idx - candidate;
            d_idx += try emitCopy(dest[d_idx..], offset, match_len);

            s_idx += match_len;
            literal_start = s_idx;
        } else {
            s_idx += 1;
        }
    }

    // Emit remaining literal
    if (literal_start < src.len) {
        d_idx += try emitLiteral(dest[d_idx..], src[literal_start..]);
    }

    return d_idx;
}

/// Compress with allocation - returns owned slice
pub fn compressAlloc(allocator: std.mem.Allocator, src: []const u8) ![]u8 {
    const max_len = maxCompressedLen(src.len);
    const buf = try allocator.alloc(u8, max_len);
    errdefer allocator.free(buf);

    const compressed_len = try compress(src, buf);

    // Shrink to actual size
    if (compressed_len < max_len) {
        return allocator.realloc(buf, compressed_len) catch buf[0..compressed_len];
    }
    return buf[0..compressed_len];
}

test "snappy compress empty" {
    var buf: [10]u8 = undefined;
    const len = try compress("", &buf);
    try std.testing.expectEqual(@as(usize, 1), len);
    try std.testing.expectEqual(@as(u8, 0), buf[0]);
}

test "snappy compress short" {
    var buf: [100]u8 = undefined;
    const len = try compress("abc", &buf);

    // Decompress and verify
    var out: [100]u8 = undefined;
    const dec_len = try uncompress(buf[0..len], &out);
    try std.testing.expectEqualStrings("abc", out[0..dec_len]);
}

test "snappy compress roundtrip" {
    const input = "Hello, World! This is a test of Snappy compression. " ++
        "Snappy is fast! Snappy is fast! Snappy is fast!";

    var compressed: [200]u8 = undefined;
    const comp_len = try compress(input, &compressed);

    // Should compress since there's repetition
    try std.testing.expect(comp_len < input.len);

    // Decompress and verify
    var decompressed: [200]u8 = undefined;
    const dec_len = try uncompress(compressed[0..comp_len], &decompressed);
    try std.testing.expectEqualStrings(input, decompressed[0..dec_len]);
}

test "snappy compress repetitive" {
    // Repetitive data - verify roundtrip works
    const input = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    var compressed: [100]u8 = undefined;
    const comp_len = try compress(input, &compressed);

    // Should produce valid output (at minimum: varint length + some data)
    try std.testing.expect(comp_len > 0);
    try std.testing.expect(comp_len <= input.len + 10); // overhead is bounded

    // Decompress and verify roundtrip
    var decompressed: [100]u8 = undefined;
    const dec_len = try uncompress(compressed[0..comp_len], &decompressed);
    try std.testing.expectEqualStrings(input, decompressed[0..dec_len]);
}

test "snappy compress sparse-bool-like pattern roundtrip" {
    // Mirrors the bool_sparse output: mostly-zero bit-packed bytes.
    // This is what fails when read by pyarrow against our /tmp/snappy_out.
    const allocator = std.testing.allocator;

    // Build a 3266-byte payload that looks like our def-prefix + values:
    // first 4 bytes = u32 LE length of the RLE-encoded def levels, then
    // some RLE bytes, then bit-packed values mostly zero.
    var input: [3266]u8 = undefined;
    var i: usize = 0;
    while (i < input.len) : (i += 1) input[i] = 0;
    // Sprinkle a few non-zero bytes to mimic the actual data.
    input[0] = 0x10;
    input[1] = 0x40;
    input[100] = 0x05;
    input[500] = 0x09;
    input[1000] = 0x21;
    input[2000] = 0x36;
    input[3000] = 0x4e;
    input[3265] = 0x07;

    const compressed = try compressAlloc(allocator, &input);
    defer allocator.free(compressed);

    var out = try allocator.alloc(u8, input.len);
    defer allocator.free(out);
    const dec_len = try uncompress(compressed, out);
    try std.testing.expectEqual(input.len, dec_len);
    try std.testing.expectEqualSlices(u8, &input, out[0..dec_len]);
}

test "snappy compress regression: size=294 k%256 pattern" {
    // First failing case found by sweep. Deterministic.
    const allocator = std.testing.allocator;
    const size = 294;
    const input = try allocator.alloc(u8, size);
    defer allocator.free(input);
    for (input, 0..) |*b, k| b.* = @intCast(k % 256);

    const compressed = try compressAlloc(allocator, input);
    defer allocator.free(compressed);

    const out = try allocator.alloc(u8, size);
    defer allocator.free(out);
    const dec_len = try uncompress(compressed, out);
    if (dec_len != size) {
        std.debug.print("\n[bug] size={d} dec_len={d} compressed_len={d}\n", .{ size, dec_len, compressed.len });
        std.debug.print("[bug] LAST 30 compressed bytes (offsets {d}..{d}): ", .{ compressed.len - 30, compressed.len });
        for (compressed[compressed.len - 30 ..]) |b| std.debug.print("{x:0>2} ", .{b});
        std.debug.print("\n", .{});
    }
    try std.testing.expectEqual(size, dec_len);
}

test "snappy compress: copy-byte tag-narrowing regression sweep" {
    // Sweep many sizes that will trigger long backward-references (the
    // case where the bug bit). Each one's decompression must round-trip.
    const allocator = std.testing.allocator;
    var size: usize = 200;
    while (size <= 600) : (size += 1) {
        const input = try allocator.alloc(u8, size);
        defer allocator.free(input);
        for (input, 0..) |*b, k| b.* = @intCast(k % 256);

        const compressed = try compressAlloc(allocator, input);
        defer allocator.free(compressed);

        const out = try allocator.alloc(u8, size);
        defer allocator.free(out);
        const dec_len = try uncompress(compressed, out);
        try std.testing.expectEqual(size, dec_len);
        try std.testing.expectEqualSlices(u8, input, out[0..dec_len]);
    }
}
