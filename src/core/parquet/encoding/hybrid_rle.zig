//! RLE / Bit-Packing Hybrid encoding.
//!
//! Single most-touched code path in the decoder: dictionary indices
//! and (eventually) definition/repetition levels both go through here.
//!
//! Stream format (Parquet spec):
//!
//!   stream := <run>*
//!   run    := <header (uleb128)> <body>
//!
//! The lowest bit of the decoded header determines the run kind:
//!   - bit 0 = 0 (RLE run):       header = (count << 1) | 0
//!     body = the value, encoded little-endian in
//!     ceil(bit_width / 8) bytes.
//!   - bit 0 = 1 (bit-packed run): header = (groups << 1) | 1
//!     body = `groups` groups of 8 values, each group `bit_width`
//!     bytes. Bits are packed LSB-first within each byte and
//!     little-endian across bytes within a group.
//!
//! Edge cases worth knowing:
//!   - bit_width = 0 means every value is implicitly zero. The stream
//!     may carry RLE runs with empty body in that case (we don't
//!     actually need to read the value byte).
//!   - The stream knows nothing about how many values the caller
//!     wants — caller drives the loop and stops when their dest is
//!     full or when remaining() (combined with run state) is exhausted.
//!
//! Output is always u32: indices and levels both fit comfortably.
//! Wider widths would need a different decoder.

const std = @import("std");

pub const Error = error{
    InvalidBitWidth,
    UnexpectedEndOfStream,
};

pub const HybridRleDecoder = struct {
    bytes: []const u8,
    pos: usize,
    bit_width: u8,
    /// (1 << bit_width) - 1, or 0 when bit_width == 0.
    mask: u32,

    // Current run state. `remaining_in_run` is in *values* (not bytes,
    // not bit groups). When 0, we read the next run header on the next
    // decode() call.
    remaining_in_run: usize,
    in_rle: bool,
    rle_value: u32,

    // Bit accumulator for the bit-packed path. We harvest bytes from
    // `bytes` into `bit_buffer` LSB-first and pull `bit_width` bits at
    // a time off the low end.
    bit_buffer: u64,
    bits_in_buffer: u8,

    pub fn init(bytes: []const u8, bit_width: u8) HybridRleDecoder {
        // Per Parquet, bit_width may be 0..32. Wider widths would
        // overflow our u32 output, and we don't need them for our
        // current use cases.
        std.debug.assert(bit_width <= 32);
        return .{
            .bytes = bytes,
            .pos = 0,
            .bit_width = bit_width,
            .mask = if (bit_width == 0) 0 else (@as(u32, 1) << @intCast(bit_width)) - 1,
            .remaining_in_run = 0,
            .in_rle = false,
            .rle_value = 0,
            .bit_buffer = 0,
            .bits_in_buffer = 0,
        };
    }

    /// Decode up to dest.len values into dest. Returns the number
    /// written. Zero means the stream is exhausted (no more runs).
    pub fn decode(self: *HybridRleDecoder, dest: []u32) Error!usize {
        // bit_width=0 short-circuit: output is all zeros up to dest.len,
        // but we have no way to know how many values the stream "has."
        // The caller bounds this externally (via num_values from the
        // page header). Without that bound we'd loop forever, so we
        // require the caller to pass dest.len = bound.
        if (self.bit_width == 0) {
            @memset(dest, 0);
            return dest.len;
        }

        var written: usize = 0;
        while (written < dest.len) {
            if (self.remaining_in_run == 0) {
                if (self.pos >= self.bytes.len) break;
                try self.readNextRun();
                if (self.remaining_in_run == 0) break;
            }
            const want = dest.len - written;
            const take = @min(want, self.remaining_in_run);

            if (self.in_rle) {
                @memset(dest[written .. written + take], self.rle_value);
            } else {
                try self.decodeBitPacked(dest[written .. written + take]);
            }

            written += take;
            self.remaining_in_run -= take;
        }
        return written;
    }

    fn readNextRun(self: *HybridRleDecoder) Error!void {
        const header = try self.readVarint();
        self.in_rle = (header & 1) == 0;
        const count_field = header >> 1;
        if (self.in_rle) {
            self.remaining_in_run = @intCast(count_field);
            self.rle_value = try self.readRleValue();
        } else {
            self.remaining_in_run = @as(usize, @intCast(count_field)) * 8;
            // Reset bit accumulator at the start of a bit-packed run.
            self.bit_buffer = 0;
            self.bits_in_buffer = 0;
        }
    }

    /// Read a ULEB128-encoded varint. Used for the run header.
    fn readVarint(self: *HybridRleDecoder) Error!u64 {
        var result: u64 = 0;
        var shift: u6 = 0;
        while (true) {
            if (self.pos >= self.bytes.len) return error.UnexpectedEndOfStream;
            const b = self.bytes[self.pos];
            self.pos += 1;
            result |= @as(u64, b & 0x7f) << shift;
            if ((b & 0x80) == 0) return result;
            shift += 7;
            if (shift >= 64) return error.UnexpectedEndOfStream;
        }
    }

    fn readRleValue(self: *HybridRleDecoder) Error!u32 {
        const value_bytes = (self.bit_width + 7) / 8;
        if (self.pos + value_bytes > self.bytes.len) return error.UnexpectedEndOfStream;
        var v: u32 = 0;
        var i: u8 = 0;
        while (i < value_bytes) : (i += 1) {
            v |= @as(u32, self.bytes[self.pos + i]) << @intCast(i * 8);
        }
        self.pos += value_bytes;
        return v & self.mask;
    }

    fn decodeBitPacked(self: *HybridRleDecoder, dest: []u32) Error!void {
        for (dest) |*slot| {
            // Refill the accumulator until we have at least bit_width bits.
            while (self.bits_in_buffer < self.bit_width) {
                if (self.pos >= self.bytes.len) return error.UnexpectedEndOfStream;
                self.bit_buffer |= @as(u64, self.bytes[self.pos]) << @intCast(self.bits_in_buffer);
                self.bits_in_buffer += 8;
                self.pos += 1;
            }
            const v: u32 = @intCast(self.bit_buffer & self.mask);
            self.bit_buffer >>= @intCast(self.bit_width);
            self.bits_in_buffer -= self.bit_width;
            slot.* = v;
        }
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "bit_width=0 outputs all zeros" {
    var dec = HybridRleDecoder.init(&[_]u8{}, 0);
    var out: [10]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 10), n);
    for (out) |v| try testing.expectEqual(@as(u32, 0), v);
}

test "single RLE run of length 5, bit_width=4" {
    // header: count=5, kind=0 (RLE) → header = (5 << 1) | 0 = 10 = 0x0a
    // value: bit_width=4, ceil(4/8)=1 byte → 7
    const bytes = [_]u8{ 0x0a, 0x07 };
    var dec = HybridRleDecoder.init(&bytes, 4);
    var out: [8]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 5), n);
    for (out[0..5]) |v| try testing.expectEqual(@as(u32, 7), v);
}

test "RLE value is masked to bit_width" {
    // bit_width=3, mask=0b111. If we feed 0xFF, we should get 0x07.
    // header: count=2, kind=0 → 0x04
    const bytes = [_]u8{ 0x04, 0xff };
    var dec = HybridRleDecoder.init(&bytes, 3);
    var out: [4]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n);
    try testing.expectEqual(@as(u32, 7), out[0]);
    try testing.expectEqual(@as(u32, 7), out[1]);
}

test "single bit-packed run, 1 group of 8, bit_width=3" {
    // header: groups=1, kind=1 → (1 << 1) | 1 = 3 = 0x03
    // body: 1 group × 3 bytes = 3 bytes encoding 8 values of 3 bits each.
    // Values 0..7 packed LSB-first:
    //   v0=0 (3 bits), v1=1 (3 bits), v2=2 (3 bits) = 0b010_001_000 = 0x88
    //   v3=3, v4=4, v5=5 = 0b101_100_011 = ...
    // It's easier to compute by packing manually:
    //   bits 0..2  = 0b000  (v0=0)
    //   bits 3..5  = 0b001  (v1=1)
    //   bits 6..8  = 0b010  (v2=2)
    //   bits 9..11 = 0b011  (v3=3)
    //   bits 12..14= 0b100  (v4=4)
    //   bits 15..17= 0b101  (v5=5)
    //   bits 18..20= 0b110  (v6=6)
    //   bits 21..23= 0b111  (v7=7)
    // Concatenated LSB-first: 111_110_101_100_011_010_001_000
    // That's a 24-bit number: 0xFAC688 reading as MSB-first; we need bytes
    // little-endian:
    //   byte 0 = bits 0..7   = 0b01_010_001_000 → take low 8: 0b01001000 = 0x88
    // Wait, easier: build the u24 then split.
    var packed_val: u64 = 0;
    var bit_pos: u6 = 0;
    var i: u32 = 0;
    while (i < 8) : (i += 1) {
        packed_val |= @as(u64, i) << @intCast(bit_pos);
        bit_pos += 3;
    }
    // packed_val now has 24 bits worth of payload.
    var bytes: [4]u8 = undefined;
    bytes[0] = 0x03; // header: 1 group, bit-packed
    bytes[1] = @truncate(packed_val);
    bytes[2] = @truncate(packed_val >> 8);
    bytes[3] = @truncate(packed_val >> 16);

    var dec = HybridRleDecoder.init(bytes[0..4], 3);
    var out: [8]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 8), n);
    for (out, 0..) |v, idx| {
        try testing.expectEqual(@as(u32, @intCast(idx)), v);
    }
}

test "mixed RLE then bit-packed run" {
    // Run 1: RLE, count=4, bit_width=2, value=3 → header=0x08, body=0x03
    // Run 2: bit-packed, 1 group=8 values bw=2: each value 2 bits.
    //   Pack v=0..7 mod 4 → 0,1,2,3,0,1,2,3 in 2 bits each.
    //   bits LSB→MSB: 00 01 10 11 00 01 10 11
    //   byte 0: bits 0..7 = 11_10_01_00 = 0xE4
    //   byte 1: bits 8..15= 11_10_01_00 = 0xE4
    // Header: groups=1 → (1<<1)|1 = 3 = 0x03
    const bytes = [_]u8{ 0x08, 0x03, 0x03, 0xe4, 0xe4 };
    var dec = HybridRleDecoder.init(&bytes, 2);
    var out: [12]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 12), n);
    // First 4 are RLE 3
    for (out[0..4]) |v| try testing.expectEqual(@as(u32, 3), v);
    // Next 8 are 0,1,2,3,0,1,2,3
    const expected = [_]u32{ 0, 1, 2, 3, 0, 1, 2, 3 };
    try testing.expectEqualSlices(u32, &expected, out[4..12]);
}

test "decode in two calls preserves run state" {
    // RLE run of 6 values=9, bit_width=4
    // header = (6 << 1) | 0 = 12 = 0x0c
    // value byte = 9
    const bytes = [_]u8{ 0x0c, 0x09 };
    var dec = HybridRleDecoder.init(&bytes, 4);
    var out: [3]u32 = undefined;

    const n1 = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n1);
    for (out) |v| try testing.expectEqual(@as(u32, 9), v);

    const n2 = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n2);
    for (out) |v| try testing.expectEqual(@as(u32, 9), v);

    const n3 = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 0), n3);
}

test "wide bit_width=12 RLE value spans two bytes" {
    // bit_width=12 → ceil(12/8)=2 bytes for RLE value
    // value = 0xABC = 2748, low 12 bits.
    // RLE bytes LE: 0xBC, 0x0A (since 0x0ABC LE = BC 0A)
    // header: count=3, RLE → (3<<1)|0 = 6 = 0x06
    const bytes = [_]u8{ 0x06, 0xbc, 0x0a };
    var dec = HybridRleDecoder.init(&bytes, 12);
    var out: [3]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 3), n);
    for (out) |v| try testing.expectEqual(@as(u32, 0xABC), v);
}

test "exhausted stream returns 0" {
    var dec = HybridRleDecoder.init(&[_]u8{}, 4);
    var out: [4]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 0), n);
}
