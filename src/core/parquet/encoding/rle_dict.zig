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
    /// An index points past the end of the dictionary.
    IndexOutOfRange,
} || hybrid_rle.Error;

pub fn Decoder(comptime T: type) type {
    return struct {
        const Self = @This();

        indices: hybrid_rle.HybridRleDecoder,
        dictionary: []const T,
        /// Every index the bit width can express is inside the dictionary, so no index needs checking.
        covers_all: bool,

        pub fn init(data_page_bytes: []const u8, dictionary: []const T) Error!Self {
            if (data_page_bytes.len == 0) return error.EmptyPage;
            const bit_width = data_page_bytes[0];
            if (bit_width > 32) return error.BitWidthTooLarge;
            return .{
                .indices = hybrid_rle.HybridRleDecoder.init(data_page_bytes[1..], bit_width),
                .dictionary = dictionary,
                .covers_all = dictionary.len >= (@as(u64, 1) << @intCast(bit_width)),
            };
        }

        pub fn decode(self: *Self, dest: []T) Error!usize {
            // Indices come from the file, and one past the dictionary is an out-of-bounds read in ReleaseFast.
            // Checking costs nothing when the bit width can't express such an index (`covers_all`), once per run
            // for RLE runs, and a branch-free clamp plus an OR-ed flag in the gather for bit-packed ones.
            //
            // Bit-packed indices are staged into a small buffer to keep the gather tight and amortize the
            // HybridRle call overhead; 256 keeps call overhead below decode cost without burning much stack.
            var idx_buf: [256]u32 = undefined;
            var written: usize = 0;
            while (written < dest.len) {
                if (try self.indices.takeRleRun(dest.len - written)) |run| {
                    if (run.value >= self.dictionary.len) return error.IndexOutOfRange;
                    @memset(dest[written..][0..run.count], self.dictionary[run.value]);
                    written += run.count;
                    continue;
                }
                const want = @min(dest.len - written, idx_buf.len);
                const n = try self.indices.decode(idx_buf[0..want]);
                if (n == 0) break;
                if (self.covers_all) {
                    _ = gather(dest[written..][0..n], self.dictionary, idx_buf[0..n], false);
                } else {
                    if (self.dictionary.len == 0) return error.IndexOutOfRange;
                    if (gather(dest[written..][0..n], self.dictionary, idx_buf[0..n], true)) return error.IndexOutOfRange;
                }
                written += n;
            }
            return written;
        }

        /// `dest[i] = dict[idx[i]]`. With `clamp`, an index past the dictionary reads its last entry instead and
        /// the return value reports that one did, so the loop stays branch-free; the caller rejects the batch.
        ///
        /// Unrolled by 8: for i64/f64 LLVM coalesces the eight scalar stores into two 256-bit writes, for i32/f32
        /// into one. The numeric decode path is write-bandwidth-bound and coalesced stores close most of the gap.
        inline fn gather(dest: []T, dict: []const T, idx: []const u32, comptime clamp: bool) bool {
            const last: u32 = if (clamp) @intCast(@min(dict.len - 1, std.math.maxInt(u32))) else 0;
            var oob: u32 = 0;
            var i: usize = 0;
            const N: usize = 8;
            while (i + N <= idx.len) : (i += N) {
                inline for (0..N) |k| {
                    const x = idx[i + k];
                    if (clamp) {
                        oob |= @intFromBool(x > last);
                        dest[i + k] = dict[@min(x, last)];
                    } else dest[i + k] = dict[x];
                }
            }
            while (i < idx.len) : (i += 1) {
                const x = idx[i];
                if (clamp) {
                    oob |= @intFromBool(x > last);
                    dest[i] = dict[@min(x, last)];
                } else dest[i] = dict[x];
            }
            return oob != 0;
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

test "Decoder rejects an index past the dictionary instead of reading beyond it" {
    const dict = [_]i32{ 10, 20, 30 };
    // bit_width=2, RLE run of 4 copies of index 3: one past the end.
    var dec = try Decoder(i32).init(&[_]u8{ 2, 0x08, 0x03 }, &dict);
    var out: [4]i32 = undefined;
    try testing.expectError(error.IndexOutOfRange, dec.decode(&out));

    // bit_width=0 means every index is 0, which an empty dictionary can't serve.
    var empty = try Decoder(i32).init(&[_]u8{0}, &[_]i32{});
    try testing.expectError(error.IndexOutOfRange, empty.decode(&out));
}

test "Decoder checks bit-packed indices against a dictionary the bit width overshoots" {
    const dict = [_]i32{ 10, 20, 30 }; // bit width 2 can say 3
    // One bit-packed group of 8 at bw 2: indices 0,1,2,0,1,2,0,1 → bytes 0x24 0x49 (LSB first)
    var ok = try Decoder(i32).init(&[_]u8{ 2, 0x03, 0b00_10_01_00, 0b01_00_10_01 }, &dict);
    var out: [8]i32 = undefined;
    try testing.expectEqual(@as(usize, 8), try ok.decode(&out));
    try testing.expectEqualSlices(i32, &.{ 10, 20, 30, 10, 20, 30, 10, 20 }, &out);
    // Same group with index 3 in the last slot.
    var bad = try Decoder(i32).init(&[_]u8{ 2, 0x03, 0b00_10_01_00, 0b11_00_10_01 }, &dict);
    try testing.expectError(error.IndexOutOfRange, bad.decode(&out));
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
