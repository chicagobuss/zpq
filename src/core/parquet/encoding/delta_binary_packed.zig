//! DELTA_BINARY_PACKED encoding.
//!
//! Default integer encoding emitted by modern Spark (3.x+) and PyArrow
//! when the column distribution favors deltas. The wire format:
//!
//!   stream := <header> <block>*
//!   header :=
//!     <varint block_size_in_values>          # multiple of 128
//!     <varint number_of_mini_blocks>         # divides block_size
//!     <varint total_value_count>
//!     <zigzag-varint first_value>
//!   block :=
//!     <zigzag-varint min_delta>
//!     <num_mini_blocks bytes: per-mini-block bit widths>
//!     <bit-packed mini-blocks>
//!
//! Per-block min_delta is subtracted from each delta before bit-packing,
//! so all bit-packed values are non-negative. Mini-blocks are separately
//! bit-aligned — the bit accumulator is reset at each mini-block
//! boundary.
//!
//! The first value lives in the header and is emitted directly. Each
//! subsequent value is `prev + delta`, where `delta` is unpacked from
//! the current mini-block and offset by the block's min_delta.
//!
//! Phase 2 surface: i32 and i64.

const std = @import("std");

pub const Error = error{
    UnexpectedEndOfStream,
    InvalidHeader,
    TooManyMiniBlocks,
    VarintOverflow,
};

const MAX_MINI_BLOCKS: u32 = 16;

pub fn Decoder(comptime T: type) type {
    comptime {
        switch (T) {
            i32, i64 => {},
            else => @compileError("DELTA_BINARY_PACKED only supports i32 / i64"),
        }
    }

    return struct {
        const Self = @This();

        bytes: []const u8,
        pos: usize,

        // Header
        block_size: u32,
        num_mini_blocks: u32,
        mini_block_size: u32,
        total_value_count: u64,

        // Running state
        prev_value: T,
        values_emitted: u64,
        first_emitted: bool,

        // Current block
        block_min_delta: T,
        mini_block_bit_widths: [MAX_MINI_BLOCKS]u8,
        current_mini_block: u32,
        mini_block_pos: u32,
        /// Byte offset where the current block's bit-packed data
        /// starts (right after the bit_widths header bytes). Used by
        /// finishBlock to skip past padded values when
        /// total_value_count is exhausted before the block is fully
        /// consumed — chained streams (DELTA_BYTE_ARRAY) rely on
        /// `pos` pointing at the byte right after the last padded
        /// value of the last block.
        block_data_start: usize,

        // Bit accumulator for the current mini-block. Must be u128, not u64:
        // unpacking a `bw`-bit value can transiently need up to `bw-1 + 8` bits
        // staged (we refill a byte at a time until `bits_in_buffer >= bw`). For
        // bw 59..64 that exceeds 64, so a u64 buffer silently dropped the high
        // bits of the last byte loaded — and bw==64 also did an illegal
        // shift-by-64 in the mask/consume. u128 has the headroom for bw≤64.
        bit_buffer: u128,
        bits_in_buffer: u8,

        pub fn init(encoded: []const u8) Error!Self {
            var s = Self{
                .bytes = encoded,
                .pos = 0,
                .block_size = 0,
                .num_mini_blocks = 0,
                .mini_block_size = 0,
                .total_value_count = 0,
                .prev_value = 0,
                .values_emitted = 0,
                .first_emitted = false,
                .block_min_delta = 0,
                .mini_block_bit_widths = std.mem.zeroes([MAX_MINI_BLOCKS]u8),
                .current_mini_block = MAX_MINI_BLOCKS, // forces a block read on first decode
                .mini_block_pos = 0,
                .block_data_start = 0,
                .bit_buffer = 0,
                .bits_in_buffer = 0,
            };

            s.block_size = try s.readUVarint();
            s.num_mini_blocks = try s.readUVarint();
            s.total_value_count = try s.readUVarintLong();
            s.prev_value = try s.readZigzagT();

            if (s.num_mini_blocks == 0) return error.InvalidHeader;
            if (s.num_mini_blocks > MAX_MINI_BLOCKS) return error.TooManyMiniBlocks;
            if (s.block_size == 0 or s.block_size % s.num_mini_blocks != 0) return error.InvalidHeader;
            s.mini_block_size = s.block_size / s.num_mini_blocks;

            // current_mini_block starts at num_mini_blocks so the first
            // decode() call reads block 0 lazily. (Avoids reading a
            // block before any consumer asks for values.)
            s.current_mini_block = s.num_mini_blocks;

            return s;
        }

        fn unpack32(comptime bw: u8, dest: []T, src: []const u8, min_delta: T, prev_val: *T) void {
            const mask = comptime if (bw == 64) std.math.maxInt(u64) else (@as(u64, 1) << bw) - 1;
            var prev = prev_val.*;
            inline for (0..32) |k| {
                const start_bit: usize = k * bw;
                const byte_off = start_bit / 8;
                const bit_off: u6 = @intCast(start_bit % 8);
                const raw = if (bw <= 57) blk: {
                    const word = std.mem.readInt(u64, src[byte_off..][0..8], .little);
                    break :blk (word >> bit_off) & mask;
                } else blk: {
                    const word = std.mem.readInt(u128, src[byte_off..][0..16], .little);
                    break :blk @as(u64, @truncate((word >> bit_off) & mask));
                };
                const delta = @as(T, @bitCast(@as(asUnsigned(T), @truncate(raw)))) +% min_delta;
                prev +%= delta;
                dest[k] = prev;
            }
            prev_val.* = prev;
        }

        fn unpack32Dispatch(bw: u8, dest: []T, src: []const u8, min_delta: T, prev_val: *T) void {
            switch (bw) {
                inline 1...64 => |b| unpack32(b, dest, src, min_delta, prev_val),
                else => unreachable,
            }
        }

        pub fn decode(self: *Self, dest: []T) Error!usize {
            var written: usize = 0;

            // First value lives in the header.
            if (!self.first_emitted) {
                if (self.total_value_count == 0) return 0;
                if (dest.len == 0) return 0;
                dest[0] = self.prev_value;
                written = 1;
                self.values_emitted = 1;
                self.first_emitted = true;
            }

            while (written < dest.len and self.values_emitted < self.total_value_count) {
                // Time to read a new block?
                if (self.current_mini_block >= self.num_mini_blocks) {
                    try self.readNextBlock();
                }

                const bw = self.mini_block_bit_widths[self.current_mini_block];
                const want = dest.len - written;
                const max_in_minilock = self.mini_block_size - self.mini_block_pos;
                const max_total = self.total_value_count - self.values_emitted;
                const take = @min(@min(want, max_in_minilock), max_total);

                if (self.mini_block_size == 32 and take == 32 and self.mini_block_pos == 0) {
                    if (bw == 0) {
                        var prev = self.prev_value;
                        for (0..32) |k| {
                            prev +%= self.block_min_delta;
                            dest[written + k] = prev;
                        }
                        self.prev_value = prev;

                        written += 32;
                        self.values_emitted += 32;
                        self.current_mini_block += 1;
                        self.mini_block_pos = 0;
                        self.bit_buffer = 0;
                        self.bits_in_buffer = 0;
                        continue;
                    } else {
                        const needed_bytes = 4 * @as(usize, bw);
                        const safety_margin: usize = if (bw <= 57) 8 else 16;
                        if (self.pos + needed_bytes + safety_margin <= self.bytes.len) {
                            unpack32Dispatch(
                                bw,
                                dest[written..][0..32],
                                self.bytes[self.pos..],
                                self.block_min_delta,
                                &self.prev_value,
                            );
                            self.pos += needed_bytes;

                            written += 32;
                            self.values_emitted += 32;
                            self.current_mini_block += 1;
                            self.mini_block_pos = 0;
                            self.bit_buffer = 0;
                            self.bits_in_buffer = 0;
                            continue;
                        }
                    }
                }

                var i: usize = 0;
                while (i < take) : (i += 1) {
                    while (self.bits_in_buffer < bw) {
                        if (self.pos >= self.bytes.len) return error.UnexpectedEndOfStream;
                        self.bit_buffer |= @as(u128, self.bytes[self.pos]) << @intCast(self.bits_in_buffer);
                        self.bits_in_buffer += 8;
                        self.pos += 1;
                    }
                    const mask: u128 = if (bw == 0) 0 else (@as(u128, 1) << @intCast(bw)) - 1;
                    const raw = self.bit_buffer & mask;
                    self.bit_buffer >>= @intCast(bw);
                    self.bits_in_buffer -= bw;

                    const delta: T = @as(T, @bitCast(@as(asUnsigned(T), @truncate(raw)))) +% self.block_min_delta;
                    self.prev_value +%= delta;
                    dest[written + i] = self.prev_value;
                }

                written += take;
                self.values_emitted += take;
                self.mini_block_pos += @intCast(take);

                if (self.mini_block_pos >= self.mini_block_size) {
                    // Advance to the next mini-block. Per spec, the
                    // bit accumulator is reset at mini-block boundaries
                    // — bytes from the previous mini-block don't carry.
                    self.current_mini_block += 1;
                    self.mini_block_pos = 0;
                    self.bit_buffer = 0;
                    self.bits_in_buffer = 0;
                }
            }

            // If we just finished emitting all values mid-block, advance
            // past the padded portion so `pos` is at the byte after the
            // last block — chained streams (DELTA_BYTE_ARRAY) need this.
            if (self.values_emitted >= self.total_value_count and
                self.current_mini_block < self.num_mini_blocks)
            {
                self.finishBlock();
            }

            return written;
        }

        fn readNextBlock(self: *Self) Error!void {
            self.block_min_delta = try self.readZigzagT();
            if (self.pos + self.num_mini_blocks > self.bytes.len) return error.UnexpectedEndOfStream;
            var i: u32 = 0;
            while (i < self.num_mini_blocks) : (i += 1) {
                self.mini_block_bit_widths[i] = self.bytes[self.pos];
                self.pos += 1;
            }
            self.block_data_start = self.pos;
            self.current_mini_block = 0;
            self.mini_block_pos = 0;
            self.bit_buffer = 0;
            self.bits_in_buffer = 0;
        }

        /// Advance `pos` to the end of the current block's bit-packed
        /// data, accounting for any mini-blocks the caller never
        /// consumed because total_value_count was reached early.
        /// Called automatically by decode() when emission completes.
        fn finishBlock(self: *Self) void {
            // Block data length = sum(mini_block_size * bit_widths[i] / 8)
            // over only the miniblocks the writer actually emitted. A block
            // stores data for just the `ceil(values_in_block / mini_block_size)`
            // miniblocks needed to hold its values; trailing miniblocks get a
            // bit-width byte in the header but NO data bytes — even when that
            // bit width is non-zero (parquet-mr pads the last block with the
            // last delta value, so a trailing miniblock can show bw>0 yet carry
            // nothing). Summing all `num_mini_blocks` over-counts those phantom
            // miniblocks and lands `pos` past the true end — fatal for chained
            // streams (DELTA_BYTE_ARRAY's prefix→suffix). The count of emitted
            // miniblocks is exactly how far we'd walked: every fully-consumed
            // miniblock plus the partial one we stopped inside.
            //
            // mini_block_size is divisible by 8 (block_size is a multiple of
            // 128, num_mini_blocks divides it), so `mini_block_size * bw / 8`
            // is exact. Each emitted miniblock is full-width (the writer pads
            // values within it), so the partial one still counts full bytes.
            const emitted_mini_blocks: u32 =
                self.current_mini_block + @as(u32, if (self.mini_block_pos > 0) 1 else 0);
            var total_bytes: usize = 0;
            var i: u32 = 0;
            while (i < emitted_mini_blocks) : (i += 1) {
                total_bytes += @as(usize, self.mini_block_size) *
                    self.mini_block_bit_widths[i] / 8;
            }
            self.pos = self.block_data_start + total_bytes;
            self.current_mini_block = self.num_mini_blocks;
            self.bit_buffer = 0;
            self.bits_in_buffer = 0;
        }

        fn readUVarint(self: *Self) Error!u32 {
            const v = try self.readUVarintLong();
            if (v > std.math.maxInt(u32)) return error.VarintOverflow;
            return @intCast(v);
        }

        fn readUVarintLong(self: *Self) Error!u64 {
            var result: u64 = 0;
            var shift: u6 = 0;
            while (true) {
                if (self.pos >= self.bytes.len) return error.UnexpectedEndOfStream;
                const b = self.bytes[self.pos];
                self.pos += 1;
                result |= @as(u64, b & 0x7f) << shift;
                if ((b & 0x80) == 0) return result;
                shift += 7;
                if (shift >= 64) return error.VarintOverflow;
            }
        }

        fn readZigzagT(self: *Self) Error!T {
            const u = try self.readUVarintLong();
            // Zigzag decode: (u >> 1) ^ -(u & 1)
            const sign: u64 = 0 -% (u & 1);
            const decoded_u = (u >> 1) ^ sign;
            return @bitCast(@as(asUnsigned(T), @truncate(decoded_u)));
        }
    };
}

fn asUnsigned(comptime T: type) type {
    return switch (T) {
        i32 => u32,
        i64 => u64,
        else => @compileError("asUnsigned: unsupported"),
    };
}

// ============================================================
// Tests — encode synthetic streams, decode, verify
// ============================================================

const testing = std.testing;

/// Hand-rolled encoder matching the spec; lets tests be self-contained.
/// Not optimized; intent is clarity, not speed.
/// Default DELTA_BINARY_PACKED block parameters. Multiple of 128 is
/// required by spec; 128/4 (= 32-value miniblocks) is what most
/// implementations emit and what every reader is well-tested against.
pub const DEFAULT_BLOCK_SIZE: u32 = 128;
pub const DEFAULT_MINI_BLOCKS: u32 = 4;

/// Encode `values` as DELTA_BINARY_PACKED. Returns a newly-allocated
/// slice owned by `allocator`. T must be i32 or i64.
///
/// The encoder always writes the same wire format regardless of input
/// distribution. For sorted/timestamp columns this dominates PLAIN by
/// 4-8×; for random columns it's roughly equivalent (per-block
/// overhead is small and bit-packing adapts to the actual delta range).
pub fn encode(
    comptime T: type,
    allocator: std.mem.Allocator,
    values: []const T,
    block_size: u32,
    mini_blocks: u32,
) ![]u8 {
    comptime {
        switch (T) {
            i32, i64 => {},
            else => @compileError("DELTA_BINARY_PACKED only supports i32 / i64"),
        }
    }
    std.debug.assert(block_size > 0 and block_size % mini_blocks == 0);
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);

    try writeUVarint(allocator, &buf, block_size);
    try writeUVarint(allocator, &buf, mini_blocks);
    try writeUVarint(allocator, &buf, values.len);
    if (values.len == 0) {
        try writeZigzag(T, allocator, &buf, 0);
        return buf.toOwnedSlice(allocator);
    }
    try writeZigzag(T, allocator, &buf, values[0]);

    const mini_block_size = block_size / mini_blocks;

    var i: usize = 1;
    while (i < values.len) {
        const block_end = @min(i + block_size, values.len);
        const n = block_end - i;

        var deltas: [4096]T = undefined;
        var j: usize = 0;
        while (j < n) : (j += 1) {
            deltas[j] = values[i + j] -% values[i + j - 1];
        }
        var min_d: T = if (n == 0) 0 else deltas[0];
        var k: usize = 1;
        while (k < n) : (k += 1) {
            if (deltas[k] < min_d) min_d = deltas[k];
        }
        // Pad the rest of the block with min_d so the bit-packed delta = 0.
        while (j < block_size) : (j += 1) deltas[j] = min_d;

        try writeZigzag(T, allocator, &buf, min_d);

        var widths: [MAX_MINI_BLOCKS]u8 = std.mem.zeroes([MAX_MINI_BLOCKS]u8);
        var mb: u32 = 0;
        while (mb < mini_blocks) : (mb += 1) {
            const start = mb * mini_block_size;
            const stop = start + mini_block_size;
            var max_diff: u64 = 0;
            var v: usize = start;
            while (v < stop) : (v += 1) {
                // (delta - min_d) is guaranteed non-negative by min_d's
                // definition; widen via @bitCast for the unsigned compare.
                const d_signed: T = deltas[v] -% min_d;
                const u: u64 = switch (T) {
                    i32 => @as(u64, @as(u32, @bitCast(d_signed))),
                    i64 => @as(u64, @bitCast(d_signed)),
                    else => unreachable,
                };
                if (u > max_diff) max_diff = u;
            }
            widths[mb] = if (max_diff == 0) 0 else @intCast(64 - @clz(max_diff));
        }
        try buf.appendSlice(allocator, widths[0..mini_blocks]);

        mb = 0;
        while (mb < mini_blocks) : (mb += 1) {
            const start = mb * mini_block_size;
            const stop = start + mini_block_size;
            const bw = widths[mb];
            if (bw == 0) continue;

            // u128, not u64: staging `bits` (0..7) leftover bits plus a `bw`-bit
            // value needs up to 7+64=71 bits before the byte drain. A u64 buffer
            // silently dropped the high bits for bw 59..64 — the encode mirror of
            // the decode bug.
            var bit_buffer: u128 = 0;
            var bits: u8 = 0;
            var v: usize = start;
            while (v < stop) : (v += 1) {
                const d_signed: T = deltas[v] -% min_d;
                const u: u64 = switch (T) {
                    i32 => @as(u64, @as(u32, @bitCast(d_signed))),
                    i64 => @as(u64, @bitCast(d_signed)),
                    else => unreachable,
                };
                bit_buffer |= @as(u128, u) << @intCast(bits);
                bits += bw;
                while (bits >= 8) {
                    try buf.append(allocator, @truncate(bit_buffer));
                    bit_buffer >>= 8;
                    bits -= 8;
                }
            }
            if (bits > 0) {
                try buf.append(allocator, @truncate(bit_buffer));
            }
        }

        i = block_end;
    }

    return buf.toOwnedSlice(allocator);
}

/// Convenience wrapper: encode with the default block parameters.
pub fn encodeDefault(
    comptime T: type,
    allocator: std.mem.Allocator,
    values: []const T,
) ![]u8 {
    return encode(T, allocator, values, DEFAULT_BLOCK_SIZE, DEFAULT_MINI_BLOCKS);
}

fn writeUVarint(allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: u64) !void {
    var v = value;
    while (true) {
        if (v < 0x80) {
            try buf.append(allocator, @intCast(v));
            return;
        }
        try buf.append(allocator, @as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
}

fn writeZigzag(comptime T: type, allocator: std.mem.Allocator, buf: *std.ArrayList(u8), value: T) !void {
    const w: u64 = switch (T) {
        i32 => blk: {
            const z: i32 = (value << 1) ^ (value >> 31);
            break :blk @as(u64, @as(u32, @bitCast(z)));
        },
        i64 => blk: {
            const z: i64 = (value << 1) ^ (value >> 63);
            break :blk @as(u64, @bitCast(z));
        },
        else => @compileError("unsupported"),
    };
    try writeUVarint(allocator, buf, w);
}

test "single block, simple ascending sequence i32" {
    const values = [_]i32{ 10, 12, 14, 16, 18, 20, 22, 24 };
    const enc = try encode(i32, testing.allocator, &values, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i32).init(enc);
    var out: [16]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    try testing.expectEqualSlices(i32, &values, out[0..n]);
}

test "constant sequence (all-zero deltas)" {
    const values = [_]i32{42} ** 32;
    const enc = try encode(i32, testing.allocator, &values, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i32).init(enc);
    var out: [64]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    try testing.expectEqualSlices(i32, &values, out[0..n]);
}

test "descending sequence (negative deltas) i64" {
    var values: [16]i64 = undefined;
    var i: usize = 0;
    while (i < values.len) : (i += 1) values[i] = 1000 - @as(i64, @intCast(i)) * 7;
    const enc = try encode(i64, testing.allocator, &values, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i64).init(enc);
    var out: [16]i64 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    try testing.expectEqualSlices(i64, &values, out[0..n]);
}

test "high-bit-width i64 deltas (bw 59..64) round-trip" {
    // Alternating ±huge deltas force mini-block bit widths near 64 — the case
    // where a u64 bit accumulator dropped the top bits on both encode (u<<bits
    // overflows) and decode (refill past bit 63). Both buffers are u128 now.
    // Cross-checked against the apache delta_binary_packed.parquet corpus file,
    // whose bitwidth59..64 columns now match pyarrow/DuckDB exactly.
    var values: [128]i64 = undefined;
    var seed: u64 = 0x9E3779B97F4A7C15;
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        // xorshift for spread-out values that need wide deltas
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        values[i] = @bitCast(seed);
    }
    const enc = try encode(i64, testing.allocator, &values, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i64).init(enc);
    var out: [128]i64 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    try testing.expectEqualSlices(i64, &values, out[0..n]);
}

test "spans two blocks" {
    var values: [200]i32 = undefined;
    var i: usize = 0;
    while (i < values.len) : (i += 1) values[i] = @intCast(i * i);
    const enc = try encode(i32, testing.allocator, &values, 128, 4); // 128/block, 200 values → 2 blocks
    defer testing.allocator.free(enc);

    var dec = try Decoder(i32).init(enc);
    var out: [256]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    try testing.expectEqualSlices(i32, &values, out[0..n]);
}

test "empty stream returns 0" {
    const enc = try encode(i32, testing.allocator, &[_]i32{}, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i32).init(enc);
    var out: [4]i32 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 0), n);
}

test "partial decode preserves state across calls" {
    var values: [50]i32 = undefined;
    var i: usize = 0;
    while (i < values.len) : (i += 1) values[i] = @intCast(i * 3);
    const enc = try encode(i32, testing.allocator, &values, 128, 4);
    defer testing.allocator.free(enc);

    var dec = try Decoder(i32).init(enc);
    var out: [10]i32 = undefined;

    var got: usize = 0;
    while (got < values.len) {
        const n = try dec.decode(&out);
        if (n == 0) break;
        try testing.expectEqualSlices(i32, values[got .. got + n], out[0..n]);
        got += n;
    }
    try testing.expectEqual(values.len, got);
}
