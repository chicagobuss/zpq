//! PLAIN encoding decoder.
//!
//! The simplest Parquet encoding: values are stored back-to-back in
//! their natural binary representation, little-endian for fixed-width
//! types. BYTE_ARRAY uses a length-prefix variant. BOOLEAN uses
//! bit-packing. FIXED_LEN_BYTE_ARRAY is raw fixed-size chunks.
//!
//! Per the design contract (`docs/parquet_decode_design.md`):
//!
//!   pub fn Decoder(comptime T: type) type {
//!     return struct {
//!       pub fn init(encoded: []const u8) Self;
//!       pub fn decode(self: *Self, dest: []T) Error!usize;
//!       pub fn remaining(self: *const Self) i64;
//!     };
//!   }
//!
//! BYTE_ARRAY decodes into `[]const u8` slices that point into the
//! page bytes — no copying. Caller's lifetime contract: the returned
//! slice values are valid only as long as the encoded page payload is.

const std = @import("std");

pub const Error = error{
    UnexpectedEndOfPage,
    NegativeLength,
};

/// PLAIN decoder for fixed-width little-endian types: i32, i64, f32, f64.
/// Generic over T which must be one of those. Other types use type-
/// specific decoders below.
pub fn Decoder(comptime T: type) type {
    comptime {
        switch (T) {
            i32, i64, f32, f64 => {},
            else => @compileError("Plain.Decoder: unsupported type " ++ @typeName(T)),
        }
    }
    const elem_size = @sizeOf(T);

    return struct {
        const Self = @This();

        bytes: []const u8,
        pos: usize,

        pub fn init(encoded: []const u8) Self {
            return .{ .bytes = encoded, .pos = 0 };
        }

        pub fn decode(self: *Self, dest: []T) Error!usize {
            const available_values = (self.bytes.len - self.pos) / elem_size;
            const n = @min(dest.len, available_values);

            var i: usize = 0;
            while (i < n) : (i += 1) {
                const offset = self.pos + i * elem_size;
                const slice = self.bytes[offset..][0..elem_size];
                if (T == f32) {
                    const u = std.mem.readInt(u32, slice, .little);
                    dest[i] = @bitCast(u);
                } else if (T == f64) {
                    const u = std.mem.readInt(u64, slice, .little);
                    dest[i] = @bitCast(u);
                } else {
                    dest[i] = std.mem.readInt(T, slice, .little);
                }
            }
            self.pos += n * elem_size;
            return n;
        }

        pub fn remaining(self: *const Self) i64 {
            return @intCast((self.bytes.len - self.pos) / elem_size);
        }
    };
}

/// PLAIN decoder for BOOLEAN. Values are bit-packed LSB-first, 8 booleans
/// per byte. The Parquet spec is explicit: bit 0 of the first byte is
/// the first value.
pub const BooleanDecoder = struct {
    bytes: []const u8,
    bit_pos: usize, // total bits consumed
    bit_total: usize, // total bits available (= bytes.len * 8)

    pub fn init(encoded: []const u8, num_values: usize) BooleanDecoder {
        // Cap by both byte length and an explicit num_values, since the
        // last byte may have unused bits. The page's data_page_header
        // num_values is the source of truth; we use it to bound output.
        const max_bits = encoded.len * 8;
        return .{
            .bytes = encoded,
            .bit_pos = 0,
            .bit_total = @min(max_bits, num_values),
        };
    }

    pub fn decode(self: *BooleanDecoder, dest: []bool) Error!usize {
        const remaining_bits = self.bit_total - self.bit_pos;
        const n = @min(dest.len, remaining_bits);

        var i: usize = 0;
        while (i < n) : (i += 1) {
            const bit_idx = self.bit_pos + i;
            const byte = self.bytes[bit_idx >> 3];
            const bit = @as(u3, @intCast(bit_idx & 7));
            dest[i] = ((byte >> bit) & 1) != 0;
        }
        self.bit_pos += n;
        return n;
    }

    pub fn remaining(self: *const BooleanDecoder) i64 {
        return @intCast(self.bit_total - self.bit_pos);
    }
};

/// PLAIN decoder for BYTE_ARRAY. On-disk format per value:
///
///   [length: u32 LE][bytes...]
///
/// We yield `[]const u8` slices that point into the encoded payload —
/// zero-copy. Caller must keep the page payload alive for the lifetime
/// of the slices.
pub const ByteArrayDecoder = struct {
    bytes: []const u8,
    pos: usize,

    pub fn init(encoded: []const u8) ByteArrayDecoder {
        return .{ .bytes = encoded, .pos = 0 };
    }

    pub fn decode(self: *ByteArrayDecoder, dest: [][]const u8) Error!usize {
        var i: usize = 0;
        while (i < dest.len) : (i += 1) {
            if (self.pos >= self.bytes.len) break;
            if (self.pos + 4 > self.bytes.len) return error.UnexpectedEndOfPage;

            const len = std.mem.readInt(u32, self.bytes[self.pos..][0..4], .little);
            self.pos += 4;

            const ulen: usize = len;
            if (self.pos + ulen > self.bytes.len) return error.UnexpectedEndOfPage;
            dest[i] = self.bytes[self.pos..][0..ulen];
            self.pos += ulen;
        }
        return i;
    }

    pub fn remaining(self: *const ByteArrayDecoder) i64 {
        // Cheap to compute? Not without walking. -1 = unknown.
        if (self.pos >= self.bytes.len) return 0;
        return -1;
    }
};

/// PLAIN decoder for FIXED_LEN_BYTE_ARRAY. Values are raw fixed-size
/// chunks back-to-back. The fixed length comes from the column's
/// SchemaElement.type_length, supplied at init.
pub const FixedLenByteArrayDecoder = struct {
    bytes: []const u8,
    pos: usize,
    type_length: usize,

    pub fn init(encoded: []const u8, type_length: usize) FixedLenByteArrayDecoder {
        return .{ .bytes = encoded, .pos = 0, .type_length = type_length };
    }

    pub fn decode(self: *FixedLenByteArrayDecoder, dest: [][]const u8) Error!usize {
        const available = (self.bytes.len - self.pos) / self.type_length;
        const n = @min(dest.len, available);
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const offset = self.pos + i * self.type_length;
            dest[i] = self.bytes[offset..][0..self.type_length];
        }
        self.pos += n * self.type_length;
        return n;
    }

    pub fn remaining(self: *const FixedLenByteArrayDecoder) i64 {
        return @intCast((self.bytes.len - self.pos) / self.type_length);
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Decoder(i32) decodes raw little-endian ints" {
    const bytes = [_]u8{
        0x01, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00,
        0xff, 0xff, 0xff, 0xff, // -1
    };
    var dec = Decoder(i32).init(&bytes);
    var out: [4]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqual(@as(i32, 1), out[0]);
    try testing.expectEqual(@as(i32, 2), out[1]);
    try testing.expectEqual(@as(i32, -1), out[2]);
    try testing.expectEqual(@as(i64, 0), dec.remaining());
}

test "Decoder(i64) decodes raw little-endian ints" {
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(i64, bytes[0..8], 1234567890123, .little);
    std.mem.writeInt(i64, bytes[8..16], -42, .little);
    var dec = Decoder(i64).init(&bytes);
    var out: [2]i64 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(i64, 1234567890123), out[0]);
    try testing.expectEqual(@as(i64, -42), out[1]);
}

test "Decoder(f32) decodes via bitcast" {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], @bitCast(@as(f32, 3.14)), .little);
    std.mem.writeInt(u32, bytes[4..8], @bitCast(@as(f32, -1.5)), .little);
    var dec = Decoder(f32).init(&bytes);
    var out: [2]f32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectApproxEqAbs(@as(f32, 3.14), out[0], 1e-6);
    try testing.expectApproxEqAbs(@as(f32, -1.5), out[1], 1e-6);
}

test "Decoder(f64) decodes via bitcast" {
    var bytes: [8]u8 = undefined;
    std.mem.writeInt(u64, bytes[0..8], @bitCast(@as(f64, 2.718281828)), .little);
    var dec = Decoder(f64).init(&bytes);
    var out: [1]f64 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectApproxEqAbs(@as(f64, 2.718281828), out[0], 1e-9);
}

test "Decoder partial decode" {
    const bytes = [_]u8{
        0x01, 0x00, 0x00, 0x00,
        0x02, 0x00, 0x00, 0x00,
        0x03, 0x00, 0x00, 0x00,
    };
    var dec = Decoder(i32).init(&bytes);
    var out: [2]i32 = undefined;
    var n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(i32, 1), out[0]);
    try testing.expectEqual(@as(i32, 2), out[1]);

    n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 1), n);
    try testing.expectEqual(@as(i32, 3), out[0]);

    n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 0), n);
}

test "BooleanDecoder bit-packs LSB-first" {
    // Bit pattern: 0b11001010 = bits 0,1,2,3,4,5,6,7 = 0,1,0,1,0,0,1,1
    const bytes = [_]u8{0b11001010};
    var dec = BooleanDecoder.init(&bytes, 8);
    var out: [8]bool = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 8), n);
    try testing.expectEqualSlices(bool, &.{ false, true, false, true, false, false, true, true }, &out);
}

test "BooleanDecoder respects num_values bound" {
    const bytes = [_]u8{ 0xff, 0xff };
    var dec = BooleanDecoder.init(&bytes, 5);
    var out: [16]bool = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 5), n);
}

test "ByteArrayDecoder yields zero-copy slices" {
    // Layout: [len=3]"abc"[len=5]"hello"[len=0]""
    var bytes: [16]u8 = undefined;
    std.mem.writeInt(u32, bytes[0..4], 3, .little);
    @memcpy(bytes[4..7], "abc");
    std.mem.writeInt(u32, bytes[7..11], 5, .little);
    @memcpy(bytes[11..16], "hello");
    var dec = ByteArrayDecoder.init(&bytes);
    var out: [3][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqualStrings("abc", out[0]);
    try testing.expectEqualStrings("hello", out[1]);
    // The slices point into bytes — verify by checking pointer equality.
    try testing.expectEqual(@intFromPtr(&bytes[4]), @intFromPtr(out[0].ptr));
}

test "FixedLenByteArrayDecoder yields fixed-size slices" {
    const bytes = [_]u8{
        'A', 'B', 'C',
        'D', 'E', 'F',
        'X', 'Y', 'Z',
    };
    var dec = FixedLenByteArrayDecoder.init(&bytes, 3);
    var out: [3][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n);
    try testing.expectEqualStrings("ABC", out[0]);
    try testing.expectEqualStrings("DEF", out[1]);
    try testing.expectEqualStrings("XYZ", out[2]);
}
