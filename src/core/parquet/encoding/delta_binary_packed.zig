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
//! so all bit-packed values are non-negative. Each mini-block holds a
//! whole number of 32-value groups, so every mini-block and every group
//! starts on a byte boundary.
//!
//! The first value lives in the header and is emitted directly. Each
//! subsequent value is `prev + delta`, where `delta` is unpacked from
//! the current mini-block and offset by the block's min_delta.
//!
//! Phase 2 surface: i32 and i64.

const std = @import("std");
const readLe = @import("../../bytes.zig").readLe;

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

        /// Values per bit-packed group. The spec makes every miniblock a whole number of these, and 32
        /// values of `bw` bits are exactly 4*bw bytes, so every group starts byte-aligned and can be
        /// unpacked with fixed shifts.
        const GROUP: u32 = 32;
        const max_bw: u8 = @bitSizeOf(asUnsigned(T));
        /// Stack copy for a group too close to the end of `bytes` to read wide from: the group itself
        /// plus the 16-byte window `paddedGroup` reads at its last value.
        const PAD_BYTES: usize = 4 * @as(usize, max_bw) + 16;

        bytes: []const u8,
        pos: usize,

        // Header
        block_size: u32,
        num_mini_blocks: u32,
        mini_block_size: u32,
        total_value_count: u64,

        // Running state
        prev_value: T,
        /// Values taken off the stream, including any still parked in `ahead`.
        values_emitted: u64,
        first_emitted: bool,

        // Current block
        block_min_delta: T,
        mini_block_bit_widths: [MAX_MINI_BLOCKS]u8,
        current_mini_block: u32,
        /// Values consumed from the current mini-block; always a multiple of GROUP.
        mini_block_pos: u32,
        /// Byte offset where the current block's bit-packed data
        /// starts (right after the bit_widths header bytes). Used by
        /// finishBlock to skip past padded values when
        /// total_value_count is exhausted before the block is fully
        /// consumed — chained streams (DELTA_BYTE_ARRAY) rely on
        /// `pos` pointing at the byte right after the last padded
        /// value of the last block.
        block_data_start: usize,

        /// One group decoded ahead for callers whose `dest` ends mid-group; drained before the stream
        /// is touched again, so the group loop only ever sees whole groups.
        ahead: [GROUP]T,
        ahead_pos: u8,
        ahead_len: u8,

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
                .ahead = undefined,
                .ahead_pos = 0,
                .ahead_len = 0,
            };

            s.block_size = try s.readUVarint();
            s.num_mini_blocks = try s.readUVarint();
            s.total_value_count = try s.readUVarintLong();
            s.prev_value = try s.readZigzagT();

            if (s.num_mini_blocks == 0) return error.InvalidHeader;
            if (s.num_mini_blocks > MAX_MINI_BLOCKS) return error.TooManyMiniBlocks;
            if (s.block_size == 0 or s.block_size % s.num_mini_blocks != 0) return error.InvalidHeader;
            s.mini_block_size = s.block_size / s.num_mini_blocks;
            // Spec requirement, and what lets decode work in byte-aligned 32-value groups. Arrow
            // rejects the same headers.
            if (s.mini_block_size % GROUP != 0) return error.InvalidHeader;

            // current_mini_block starts at num_mini_blocks so the first
            // decode() call reads block 0 lazily. (Avoids reading a
            // block before any consumer asks for values.)
            s.current_mini_block = s.num_mini_blocks;

            return s;
        }

        /// Bytes past a group's start that `unpack32(bw)` may read: the last value's byte offset
        /// plus one 8-byte (or, above 57 bits, 16-byte) load.
        fn readSpan(comptime bw: u8) usize {
            if (bw == 0) return 0;
            return (31 * @as(usize, bw)) / 8 + @as(usize, if (bw <= 57) 8 else 16);
        }

        /// Unpack one 32-value group and run the prefix sum through it. `src` must hold
        /// `readSpan(bw)` bytes.
        inline fn unpack32(comptime bw: u8, dest: *[GROUP]T, src: []const u8, min_delta: T, prev_in: T) T {
            var prev = prev_in;
            if (bw == 0) {
                inline for (0..GROUP) |k| {
                    prev +%= min_delta;
                    dest[k] = prev;
                }
                return prev;
            }
            const mask = comptime if (bw == 64) std.math.maxInt(u64) else (@as(u64, 1) << bw) - 1;
            inline for (0..GROUP) |k| {
                const start_bit: usize = k * bw;
                const byte_off = start_bit / 8;
                const bit_off: u6 = @intCast(start_bit % 8);
                const raw = if (bw <= 57) blk: {
                    const word = readLe(u64, src[byte_off..][0..8]);
                    break :blk (word >> bit_off) & mask;
                } else blk: {
                    const word = readLe(u128, src[byte_off..][0..16]);
                    break :blk @as(u64, @truncate((word >> bit_off) & mask));
                };
                const delta = @as(T, @bitCast(@as(asUnsigned(T), @truncate(raw)))) +% min_delta;
                prev +%= delta;
                dest[k] = prev;
            }
            return prev;
        }

        /// Decode up to `groups` whole groups straight from `bytes` into `dest`, stopping at the
        /// first group whose wide reads would run past the end of `bytes`. Returns groups decoded.
        fn fastGroups(self: *Self, comptime bw: u8, dest: []T, groups: usize) usize {
            const group_bytes: usize = 4 * @as(usize, bw);
            const span = comptime readSpan(bw);
            var pos = self.pos;
            const n: usize = if (bw == 0)
                groups
            else if (self.bytes.len >= pos + span)
                @min(groups, (self.bytes.len - pos - span) / group_bytes + 1)
            else
                0;
            // Locals, not fields: through `self` the compiler must assume `dest` stores alias them.
            const md = self.block_min_delta;
            var prev = self.prev_value;
            var g: usize = 0;
            while (g < n) : (g += 1) {
                prev = unpack32(bw, dest[g * GROUP ..][0..GROUP], self.bytes[pos..], md, prev);
                pos += group_bytes;
            }
            self.prev_value = prev;
            self.pos = pos;
            return n;
        }

        /// One copy of the unrolled group loop per bit width, shared by both call sites in decode.
        noinline fn fastGroupsDispatch(self: *Self, bw: u8, dest: []T, groups: usize) usize {
            return switch (bw) {
                inline 0...max_bw => |b| self.fastGroups(b, dest, groups),
                else => unreachable,
            };
        }

        /// Decode one group whose bytes end too close to the end of `bytes` for `fastGroups`, via a
        /// zero-padded copy. Only `count` values need to be backed by real bytes: the writer pads the
        /// final miniblock, but a stream cut short after its last real value still decodes. Runs at
        /// most a couple of times per page, so `bw` stays a runtime value rather than adding another
        /// unrolled copy per width to the binary.
        fn paddedGroup(self: *Self, bw: u8, dest: *[GROUP]T, count: usize) Error!void {
            const group_bytes: usize = 4 * @as(usize, bw);
            const avail = self.bytes.len - self.pos;
            if (avail < (count * bw + 7) / 8) return error.UnexpectedEndOfStream;
            const n = @min(avail, group_bytes);
            var buf: [PAD_BYTES]u8 = @splat(0);
            @memcpy(buf[0..n], self.bytes[self.pos..][0..n]);

            const mask: u64 = if (bw == 64) std.math.maxInt(u64) else (@as(u64, 1) << @intCast(bw)) - 1;
            const md = self.block_min_delta;
            var prev = self.prev_value;
            for (dest, 0..) |*d, k| {
                const bit = k * bw;
                const word = readLe(u128, buf[bit / 8 ..][0..16]);
                const raw: u64 = @as(u64, @truncate(word >> @intCast(bit % 8))) & mask;
                prev +%= @as(T, @bitCast(@as(asUnsigned(T), @truncate(raw)))) +% md;
                d.* = prev;
            }
            self.prev_value = prev;
            self.pos += n;
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

            while (written < dest.len) {
                if (self.ahead_pos < self.ahead_len) {
                    const n = @min(self.ahead_len - self.ahead_pos, dest.len - written);
                    @memcpy(dest[written..][0..n], self.ahead[self.ahead_pos..][0..n]);
                    self.ahead_pos += @intCast(n);
                    written += n;
                    continue;
                }
                if (self.values_emitted >= self.total_value_count) break;

                if (self.current_mini_block >= self.num_mini_blocks) {
                    try self.readNextBlock();
                }

                const bw = self.mini_block_bit_widths[self.current_mini_block];
                const left: u64 = self.total_value_count - self.values_emitted;
                const groups_in_mini_block: usize = (self.mini_block_size - self.mini_block_pos) / GROUP;
                const whole: usize = @min(groups_in_mini_block, @min(dest.len - written, left) / GROUP);

                var done: usize = 0;
                if (whole > 0) {
                    done = self.fastGroupsDispatch(bw, dest[written..][0 .. whole * GROUP], whole);
                    if (done == 0) {
                        try self.paddedGroup(bw, dest[written..][0..GROUP], GROUP);
                        done = 1;
                    }
                    written += done * GROUP;
                    self.values_emitted += done * GROUP;
                } else {
                    // `dest` or the stream ends inside this group: decode it whole into `ahead`.
                    const count: usize = @intCast(@min(GROUP, left));
                    if (count < GROUP or self.fastGroupsDispatch(bw, &self.ahead, 1) == 0) {
                        try self.paddedGroup(bw, &self.ahead, count);
                    }
                    done = 1;
                    self.ahead_pos = 0;
                    self.ahead_len = @intCast(count);
                    self.values_emitted += count;
                }

                // Per spec, mini-blocks are byte-aligned — bytes from the previous one don't carry.
                self.mini_block_pos += @intCast(done * GROUP);
                if (self.mini_block_pos >= self.mini_block_size) {
                    self.current_mini_block += 1;
                    self.mini_block_pos = 0;
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
                const bw = self.bytes[self.pos];
                if (bw > max_bw) return error.InvalidHeader;
                self.mini_block_bit_widths[i] = bw;
                self.pos += 1;
            }
            self.block_data_start = self.pos;
            self.current_mini_block = 0;
            self.mini_block_pos = 0;
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
            // mini_block_size is a multiple of 32 (checked in init), so `mini_block_size * bw / 8`
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
            // Clamped: paddedGroup accepts a final miniblock cut short after its last real value.
            self.pos = @min(self.block_data_start + total_bytes, self.bytes.len);
            self.current_mini_block = self.num_mini_blocks;
        }

        fn readUVarint(self: *Self) Error!u32 {
            const v = try self.readUVarintLong();
            if (v > std.math.maxInt(u32)) return error.VarintOverflow;
            return @intCast(v);
        }

        fn readUVarintLong(self: *Self) Error!u64 {
            var result: u64 = 0;
            var shift: u7 = 0;
            while (true) {
                if (shift >= 64) return error.VarintOverflow;
                if (self.pos >= self.bytes.len) return error.UnexpectedEndOfStream;
                const b = self.bytes[self.pos];
                self.pos += 1;
                result |= @as(u64, b & 0x7f) << @intCast(shift);
                if ((b & 0x80) == 0) return result;
                shift += 7;
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
    const values: [32]i32 = @splat(42);
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

/// Fill `out` with a sequence whose deltas need roughly `bits` bits (0 = arithmetic progression).
fn fillPattern(comptime T: type, out: []T, bits: u7, seed0: u64) void {
    var seed: u64 = seed0 | 1;
    var v: T = @truncate(@as(i64, @bitCast(seed)));
    for (out) |*o| {
        seed ^= seed << 13;
        seed ^= seed >> 7;
        seed ^= seed << 17;
        const step: T = if (bits == 0)
            -3
        else if (bits >= @bitSizeOf(T))
            @bitCast(@as(@Int(.unsigned, @bitSizeOf(T)), @truncate(seed)))
        else
            @intCast(@as(u64, seed) & ((@as(u64, 1) << @intCast(bits)) - 1));
        v +%= step;
        o.* = v;
    }
}

/// Decode `enc` handing the decoder `take`-sized slices (cycling through `takes`) and compare.
fn expectDecodes(comptime T: type, enc: []const u8, want: []const T, takes: []const usize) !void {
    var dec = try Decoder(T).init(enc);
    const out = try testing.allocator.alloc(T, want.len + 64);
    defer testing.allocator.free(out);
    var got: usize = 0;
    var t: usize = 0;
    while (true) : (t += 1) {
        const take = @min(takes[t % takes.len], out.len - got);
        const n = try dec.decode(out[got..][0..take]);
        if (n == 0 and take > 0) break;
        got += n;
    }
    try testing.expectEqual(want.len, got);
    try testing.expectEqualSlices(T, want, out[0..got]);
    // The decoder leaves `pos` at the stream end, which chained streams depend on.
    try testing.expectEqual(enc.len, dec.pos);
}

test "miniblock sizes 32/64/128/256 x odd counts x bit widths x take patterns" {
    const layouts = [_][2]u32{ .{ 128, 4 }, .{ 128, 2 }, .{ 256, 2 }, .{ 256, 1 }, .{ 1024, 4 }, .{ 2048, 16 } };
    const counts = [_]usize{ 1, 2, 31, 32, 33, 65, 129, 257, 1000, 4097 };
    const takes = [_][]const usize{
        &.{100000}, &.{1},        &.{ 3, 7 }, &.{31},
        &.{32},     &.{ 33, 95 }, &.{1000},   &.{ 1, 255, 64, 17 },
    };
    inline for (.{ i32, i64 }) |T| {
        const bit_choices = [_]u7{ 0, 1, 5, 13, 31, 32, 40, 57, 58, 63, 64 };
        for (bit_choices) |bits| {
            if (bits > @bitSizeOf(T)) continue;
            for (layouts) |lay| for (counts) |count| {
                const values = try testing.allocator.alloc(T, count);
                defer testing.allocator.free(values);
                fillPattern(T, values, bits, count *% 0x9E37 +% bits);
                const enc = try encode(T, testing.allocator, values, lay[0], lay[1]);
                defer testing.allocator.free(enc);
                for (takes) |tk| try expectDecodes(T, enc, values, tk);
            };
        }
    }
}

test "bw=0 miniblocks across every miniblock size" {
    inline for (.{ i32, i64 }) |T| {
        for ([_][2]u32{ .{ 128, 4 }, .{ 128, 2 }, .{ 256, 2 }, .{ 256, 1 } }) |lay| {
            var values: [777]T = undefined;
            for (&values, 0..) |*v, i| v.* = 5 - 3 * @as(T, @intCast(i));
            const enc = try encode(T, testing.allocator, &values, lay[0], lay[1]);
            defer testing.allocator.free(enc);
            // Header (4 varints) then per block: min_delta + width bytes, no data bytes.
            try testing.expect(enc.len < 64);
            try expectDecodes(T, enc, &values, &.{ 7, 100000 });
        }
    }
}

test "bw=64 i64 at miniblock size 256" {
    var values: [600]i64 = undefined;
    fillPattern(i64, &values, 64, 42);
    const enc = try encode(i64, testing.allocator, &values, 256, 1);
    defer testing.allocator.free(enc);
    var probe = try Decoder(i64).init(enc);
    var two: [2]i64 = undefined;
    _ = try probe.decode(&two);
    try testing.expectEqual(@as(u8, 64), probe.mini_block_bit_widths[0]);
    try expectDecodes(i64, enc, &values, &.{100000});
    try expectDecodes(i64, enc, &values, &.{ 5, 31, 33 });
}

test "i32 deltas wrap around" {
    // Full-range i32 values: deltas overflow i32 and must wrap mod 2^32, exactly as the writer did.
    var values: [513]i32 = undefined;
    for (&values, 0..) |*v, i| {
        const k: i32 = @intCast(i);
        v.* = if (i % 2 == 0) std.math.maxInt(i32) - k else std.math.minInt(i32) + k;
    }
    for ([_][2]u32{ .{ 128, 4 }, .{ 128, 2 }, .{ 256, 1 } }) |lay| {
        const enc = try encode(i32, testing.allocator, &values, lay[0], lay[1]);
        defer testing.allocator.free(enc);
        try expectDecodes(i32, enc, &values, &.{100000});
        try expectDecodes(i32, enc, &values, &.{ 1, 30, 70 });
    }
}

test "stream cut short after the last value's bytes still decodes; cut further errors" {
    // 40 values = first + 39 deltas: one 256-value miniblock of which only 39 are real.
    var values: [40]i64 = undefined;
    fillPattern(i64, &values, 13, 7);
    const enc = try encode(i64, testing.allocator, &values, 256, 1);
    defer testing.allocator.free(enc);
    const full_mb = 256 * 13 / 8;
    const needed = (39 * 13 + 7) / 8;
    const trimmed = enc[0 .. enc.len - full_mb + needed];

    var dec = try Decoder(i64).init(trimmed);
    var out: [64]i64 = undefined;
    try testing.expectEqual(@as(usize, 40), try dec.decode(&out));
    try testing.expectEqualSlices(i64, &values, out[0..40]);
    try testing.expectEqual(trimmed.len, dec.pos);

    var short = try Decoder(i64).init(trimmed[0 .. trimmed.len - 1]);
    try testing.expectError(error.UnexpectedEndOfStream, short.decode(&out));
}

test "pos lands at end of stream when followed by more bytes" {
    var values: [300]i32 = undefined;
    fillPattern(i32, &values, 9, 3);
    const enc = try encode(i32, testing.allocator, &values, 256, 2);
    defer testing.allocator.free(enc);
    const tail: [40]u8 = @splat(0xAB);
    const chained = try std.mem.concat(testing.allocator, u8, &.{ enc, &tail });
    defer testing.allocator.free(chained);
    var dec = try Decoder(i32).init(chained);
    var out: [300]i32 = undefined;
    try testing.expectEqual(@as(usize, 300), try dec.decode(&out));
    try testing.expectEqualSlices(i32, &values, &out);
    try testing.expectEqual(enc.len, dec.pos);
}

test "miniblock size not a multiple of 32 is rejected" {
    var values: [10]i32 = undefined;
    fillPattern(i32, &values, 4, 1);
    const enc = try encode(i32, testing.allocator, &values, 128, 8); // 16-value miniblocks
    defer testing.allocator.free(enc);
    try testing.expectError(error.InvalidHeader, Decoder(i32).init(enc));
}

test "overlong header varint is rejected, not overflowed" {
    const enc = [_]u8{ 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x80, 0x01 };
    try testing.expectError(error.VarintOverflow, Decoder(i64).init(&enc));
}
