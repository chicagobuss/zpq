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
// Encoder
// ============================================================

/// Encode `values` using the RLE/bit-packed-hybrid scheme.
///
/// Strategy: scan once to find runs. Emit RLE for runs ≥ 8 (the
/// breakeven where RLE is shorter than bit-packing) and bit-packed
/// otherwise. Output is what `HybridRleDecoder.decode(.., bit_width)`
/// roundtrips.
///
/// Common case for OPTIONAL primitives: every value is 1 (all-present
/// definition levels). One RLE run of count=N, value=1 → ~3 bytes
/// regardless of N. Worth keeping simple.
pub fn encode(arena: std.mem.Allocator, values: []const u32, bit_width: u8) std.mem.Allocator.Error![]u8 {
    std.debug.assert(bit_width <= 32);

    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(arena);

    if (bit_width == 0) {
        // bit_width=0 means every value is implicitly zero; the stream
        // need not encode anything. Decoders short-circuit on this.
        return out.toOwnedSlice(arena);
    }
    if (values.len == 0) return out.toOwnedSlice(arena);

    // Walk values, emit one run at a time. The wire format requires
    // bit-packed runs to be whole groups of 8 — partial groups can't
    // be padded (the decoder can't tell padding from real values). So
    // the rule is: RLE for ≥8 same-value, else bit-pack in multiples
    // of 8, else emit any leftover trailing values as 1-element RLE
    // runs.
    var i: usize = 0;
    while (i < values.len) {
        const run_len = sameValueRun(values, i);

        if (run_len >= 8) {
            try writeRleRun(arena, &out, values[i], run_len, bit_width);
            i += run_len;
            continue;
        }

        // Find how many values we can bit-pack starting at i: walk
        // until we hit an RLE-eligible (≥8 same) stretch or end of
        // input. Then round down to a multiple of 8.
        var j = i;
        while (j < values.len) {
            if (sameValueRun(values, j) >= 8) break;
            j += 1;
        }
        const pack_total = j - i;
        const pack_groups = pack_total / 8;
        const pack_count = pack_groups * 8;

        if (pack_count > 0) {
            try writeBitPackedRun(arena, &out, values[i .. i + pack_count], bit_width);
            i += pack_count;
        } else {
            // No full group available and no RLE-eligible run starts
            // here. Emit values[i] as a single-element RLE run and
            // advance one. Costs ULEB128(2)=1 byte + ceil(bw/8) bytes.
            try writeRleRun(arena, &out, values[i], 1, bit_width);
            i += 1;
        }
    }

    return out.toOwnedSlice(arena);
}

/// How many consecutive `values[start..]` equal `values[start]`.
fn sameValueRun(values: []const u32, start: usize) usize {
    var k = start + 1;
    while (k < values.len and values[k] == values[start]) : (k += 1) {}
    return k - start;
}

fn writeUleb128(arena: std.mem.Allocator, out: *std.ArrayList(u8), value: u64) std.mem.Allocator.Error!void {
    var v = value;
    while (true) {
        var b: u8 = @truncate(v & 0x7f);
        v >>= 7;
        if (v != 0) b |= 0x80;
        try out.append(arena, b);
        if (v == 0) break;
    }
}

fn writeRleRun(
    arena: std.mem.Allocator,
    out: *std.ArrayList(u8),
    value: u32,
    count: usize,
    bit_width: u8,
) std.mem.Allocator.Error!void {
    // header: (count << 1) | 0
    try writeUleb128(arena, out, @as(u64, @intCast(count)) << 1);
    // value: ceil(bit_width/8) bytes, little-endian
    const value_bytes = (bit_width + 7) / 8;
    var i: u8 = 0;
    while (i < value_bytes) : (i += 1) {
        try out.append(arena, @as(u8, @truncate(value >> @intCast(i * 8))));
    }
}

fn writeBitPackedRun(
    arena: std.mem.Allocator,
    out: *std.ArrayList(u8),
    values: []const u32,
    bit_width: u8,
) std.mem.Allocator.Error!void {
    // Round up to whole groups of 8 with zero padding for trailing values.
    const groups = (values.len + 7) / 8;
    // header: (groups << 1) | 1
    try writeUleb128(arena, out, (@as(u64, @intCast(groups)) << 1) | 1);

    // Bit-pack: LSB-first within each byte, little-endian across bytes
    // within a group. We just stream `bit_width` bits per value into a
    // 64-bit accumulator, draining bytes as they fill.
    var bit_buf: u64 = 0;
    var bits_in_buf: u8 = 0;
    const mask: u32 = if (bit_width == 32) std.math.maxInt(u32) else (@as(u32, 1) << @intCast(bit_width)) - 1;
    for (0..groups * 8) |idx| {
        const v: u32 = if (idx < values.len) values[idx] & mask else 0;
        bit_buf |= @as(u64, v) << @intCast(bits_in_buf);
        bits_in_buf += bit_width;
        while (bits_in_buf >= 8) {
            try out.append(arena, @as(u8, @truncate(bit_buf & 0xff)));
            bit_buf >>= 8;
            bits_in_buf -= 8;
        }
    }
    // Flush any trailing bits (final byte's high bits are zero).
    if (bits_in_buf > 0) {
        try out.append(arena, @as(u8, @truncate(bit_buf & 0xff)));
    }
}

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

test "encode all-1s def levels for 1000 rows roundtrips" {
    const arena = testing.allocator;
    const values = try arena.alloc(u32, 1000);
    defer arena.free(values);
    @memset(values, 1);

    const encoded = try encode(arena, values, 1);
    defer arena.free(encoded);

    // RLE single-run encoding: ULEB128(2000) is 2 bytes (2000 = 0x7d0
    // → 0xd0 0x0f), plus 1 value byte = 3 bytes total.
    try testing.expectEqual(@as(usize, 3), encoded.len);

    var dec = HybridRleDecoder.init(encoded, 1);
    var out: [1000]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 1000), n);
    for (out) |v| try testing.expectEqual(@as(u32, 1), v);
}

test "encode mixed values roundtrips" {
    const arena = testing.allocator;
    // 100 ones, then 0,1,0,1,0,1,0,1, then 50 zeros — exercises
    // RLE → bit-packed → RLE transitions.
    var values: std.ArrayList(u32) = .empty;
    defer values.deinit(arena);
    for (0..100) |_| try values.append(arena, 1);
    for (0..8) |i| try values.append(arena, @as(u32, @intCast(i & 1)));
    for (0..50) |_| try values.append(arena, 0);

    const encoded = try encode(arena, values.items, 1);
    defer arena.free(encoded);

    var dec = HybridRleDecoder.init(encoded, 1);
    const out = try arena.alloc(u32, values.items.len);
    defer arena.free(out);
    const n = try dec.decode(out);
    try testing.expectEqual(values.items.len, n);
    try testing.expectEqualSlices(u32, values.items, out);
}

test "encode bit_width=4 mixed values roundtrips" {
    const arena = testing.allocator;
    const values = [_]u32{ 0, 5, 12, 7, 7, 7, 7, 7, 7, 7, 7, 7, 3, 14, 1, 0 };
    const encoded = try encode(arena, &values, 4);
    defer arena.free(encoded);

    var dec = HybridRleDecoder.init(encoded, 4);
    var out: [16]u32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 16), n);
    try testing.expectEqualSlices(u32, &values, &out);
}

test "encode empty input produces empty output" {
    const arena = testing.allocator;
    const encoded = try encode(arena, &.{}, 1);
    defer arena.free(encoded);
    try testing.expectEqual(@as(usize, 0), encoded.len);
}

test "encode bit_width=0 produces empty output" {
    const arena = testing.allocator;
    const values = [_]u32{ 0, 0, 0, 0 };
    const encoded = try encode(arena, &values, 0);
    defer arena.free(encoded);
    try testing.expectEqual(@as(usize, 0), encoded.len);
}
