//! RLE_DICTIONARY (a.k.a. PLAIN_DICTIONARY) encoding.
//!
//! The most common encoding in real-world Parquet files: a separate
//! dictionary page holds the unique values (PLAIN-encoded), and the
//! data pages hold dictionary indices (HYBRID_RLE-encoded with one
//! leading byte that gives the bit_width).
//!
//! Composition:
//!
//!   data_page_bytes := <bit_width: u8> <hybrid_rle stream of indices>
//!
//! The decoder fans this out: indices come through HybridRleDecoder,
//! lookups go into a caller-provided `dictionary: []const T`. We're
//! generic over T; the caller decoded the dictionary page once and
//! passes that buffer in.
//!
//! Lifetime: for non-primitive T (BYTE_ARRAY = []const u8), the
//! dictionary's value slices point into the dictionary page's
//! decompressed bytes. Caller keeps that buffer alive for the whole
//! column-chunk read.

const std = @import("std");
const hybrid_rle = @import("hybrid_rle.zig");

pub const Error = error{
    EmptyPage,
    BitWidthTooLarge,
} || hybrid_rle.Error;

pub fn Decoder(comptime T: type) type {
    return struct {
        const Self = @This();

        indices: hybrid_rle.HybridRleDecoder,
        dictionary: []const T,

        pub fn init(data_page_bytes: []const u8, dictionary: []const T) Error!Self {
            if (data_page_bytes.len == 0) return error.EmptyPage;
            const bit_width = data_page_bytes[0];
            if (bit_width > 32) return error.BitWidthTooLarge;
            return .{
                .indices = hybrid_rle.HybridRleDecoder.init(data_page_bytes[1..], bit_width),
                .dictionary = dictionary,
            };
        }

        pub fn decode(self: *Self, dest: []T) Error!usize {
            // Stage indices into a small buffer to keep the inner
            // index→value lookup loop tight (and to leave room to add
            // SIMD gather later). 256 is enough to keep call overhead
            // below decode cost without burning much stack.
            var idx_buf: [256]u32 = undefined;
            var written: usize = 0;
            while (written < dest.len) {
                const want = @min(dest.len - written, idx_buf.len);
                const n = try self.indices.decode(idx_buf[0..want]);
                if (n == 0) break;
                var i: usize = 0;
                while (i < n) : (i += 1) {
                    // Bounds check is a Zig safe-mode panic on bad data;
                    // ReleaseFast trusts the file. That's the right
                    // tradeoff: dev catches corruption, prod runs fast.
                    dest[written + i] = self.dictionary[idx_buf[i]];
                }
                written += n;
            }
            return written;
        }
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Decoder(i32) round-trips dictionary lookup" {
    // Dictionary page: 4 values [10, 20, 30, 40].
    const dict = [_]i32{ 10, 20, 30, 40 };

    // Data page: bit_width=2 (need 2 bits to address 4 entries),
    // followed by a HybridRle stream encoding indices 0,1,2,3,3,2,1,0.
    // Use a bit-packed run of 1 group=8 values.
    //   header: groups=1, kind=1 → (1<<1)|1 = 3 = 0x03
    //   values: 0,1,2,3,3,2,1,0 in 2 bits each, LSB-first.
    //   bits 0..1 = 00 (idx 0)
    //   bits 2..3 = 01 (idx 1)
    //   bits 4..5 = 10 (idx 2)
    //   bits 6..7 = 11 (idx 3) → byte 0 = 11_10_01_00 = 0xE4
    //   bits 8..9 = 11 (idx 3)
    //   bits 10..11 = 10 (idx 2)
    //   bits 12..13 = 01 (idx 1)
    //   bits 14..15 = 00 (idx 0) → byte 1 = 00_01_10_11 = 0x1B
    const bytes = [_]u8{ 2, 0x03, 0xe4, 0x1b };
    var dec = try Decoder(i32).init(&bytes, &dict);
    var out: [8]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 8), n);
    const expected = [_]i32{ 10, 20, 30, 40, 40, 30, 20, 10 };
    try testing.expectEqualSlices(i32, &expected, &out);
}

test "Decoder(BYTE_ARRAY) yields zero-copy slices through the dictionary" {
    // Dictionary entries point into a backing buffer.
    const dict = [_][]const u8{ "alpha", "beta", "gamma" };
    // RLE run, bit_width=2 (3 entries fit in 2 bits), count=4, value=1
    //   header: (4<<1)|0 = 8 = 0x08
    //   value:  0x01
    const bytes = [_]u8{ 2, 0x08, 0x01 };
    var dec = try Decoder([]const u8).init(&bytes, &dict);
    var out: [4][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 4), n);
    for (out) |v| try testing.expectEqualStrings("beta", v);
    // Zero-copy: same pointer as the dict entry.
    try testing.expectEqual(@intFromPtr(dict[1].ptr), @intFromPtr(out[0].ptr));
}

test "empty data page yields error" {
    const dec_or = Decoder(i32).init(&[_]u8{}, &[_]i32{});
    try testing.expectError(error.EmptyPage, dec_or);
}

test "Decoder respects partial fill" {
    const dict = [_]i32{ 100, 200 };
    // RLE run of 8 values=1, bit_width=1 → (8<<1)|0 = 16 = 0x10, body = 0x01
    const bytes = [_]u8{ 1, 0x10, 0x01 };
    var dec = try Decoder(i32).init(&bytes, &dict);
    var out: [3]i32 = undefined;

    var n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n);
    for (out) |v| try testing.expectEqual(@as(i32, 200), v);

    n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n);
    for (out) |v| try testing.expectEqual(@as(i32, 200), v);

    n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(i32, 200), out[0]);
    try testing.expectEqual(@as(i32, 200), out[1]);
}
