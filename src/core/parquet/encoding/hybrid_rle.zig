//! RLE / Bit-Packing Hybrid encoding.
//!
//! Single most-touched code path in the decoder: dictionary indices
//! and definition/repetition levels both go through here.
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
    //
    // We do NOT carry bit-accumulator state across calls: the bit-
    // packed decoder is stateless per call (reads bytes by direct
    // byte-offset arithmetic). Parquet bit-packed runs are always
    // multiples of 8 values, and `decode` only ever asks for whole
    // multiples of 8 from a single run — so each call resumes at a
    // byte-aligned position. Cross-call drift is the failure mode to
    // avoid: a persistent bit accumulator can silently carry one bad
    // offset through a long dictionary-index run. The stateless version
    // eliminates that class of error by construction. See
    // decodeBitPackedFixed for the per-call algorithm.
    remaining_in_run: usize,
    in_rle: bool,
    rle_value: u32,

    // Parquet bit-packed runs come in groups of 8 values. When a
    // caller asks for fewer than 8 values from a bit-packed group
    // (e.g. RLE→bit-packed transition with only 5 caller slots left
    // in the current batch), we have to decode the WHOLE group from
    // the byte stream — the group is byte-aligned but its 8 values
    // can't be split mid-byte by external offset arithmetic. The
    // 1..7 over-decoded values stash here until the next decode()
    // call drains them.
    //
    // Without this carry buffer, the alternative would be partial-
    // byte cross-call state (a `bit_buffer` accumulator), which is
    // harder to reason about and easier to drift.
    bp_carry: [7]u32,
    bp_carry_count: u8,

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
            .bp_carry = undefined,
            .bp_carry_count = 0,
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

        // Drain any bit-packed carry from a previous call first.
        // bp_carry holds 1..7 values from a partial group whose
        // tail couldn't fit in the previous call's dest.
        if (self.bp_carry_count > 0) {
            const drain = @min(@as(usize, self.bp_carry_count), dest.len);
            @memcpy(dest[0..drain], self.bp_carry[0..drain]);
            if (drain < self.bp_carry_count) {
                // Shift remaining carry to the front.
                const remaining = self.bp_carry_count - drain;
                std.mem.copyForwards(u32, self.bp_carry[0..remaining], self.bp_carry[drain..self.bp_carry_count]);
            }
            self.bp_carry_count -= @intCast(drain);
            written += drain;
        }

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
                // Bit-packed: decode must happen in whole 8-value groups
                // (each group is byte-aligned and exactly bw bytes). If
                // `take` isn't a multiple of 8, split into the aligned
                // prefix and a partial-group tail. The tail decodes a
                // whole next group and stashes the unused 1..7 into
                // bp_carry for the next decode() call.
                const aligned = (take / 8) * 8;
                if (aligned > 0) {
                    try self.decodeBitPacked(dest[written .. written + aligned]);
                }
                const tail = take - aligned;
                if (tail > 0) {
                    // Decode one full group (8 values) into a temp,
                    // copy `tail` to dest, stash the rest in carry.
                    var grp: [8]u32 = undefined;
                    try self.decodeBitPacked(&grp);
                    @memcpy(dest[written + aligned .. written + take], grp[0..tail]);
                    const leftover = 8 - tail;
                    @memcpy(self.bp_carry[0..leftover], grp[tail..8]);
                    self.bp_carry_count = @intCast(leftover);
                    // We consumed an extra `leftover` values from the
                    // run; reflect that in remaining_in_run. (Take
                    // bookkeeping below counts only `take`.)
                    self.remaining_in_run -= leftover;
                }
            }

            written += take;
            self.remaining_in_run -= take;
        }
        return written;
    }

    /// Peek the stream's opening run without expanding a single value.
    ///
    /// Returns null when the stream is empty/truncated or opens with a
    /// bit-packed run. Otherwise reports the RLE run's value and length
    /// in values, read straight out of the run header — O(1) regardless
    /// of how many values the run covers.
    ///
    /// This is the primitive behind the all-present definition-level
    /// check: a level stream that opens with one long enough RLE run at
    /// max_def proves the page has no nulls, so the page's levels never
    /// have to be materialised at all.
    pub fn peekFirstRun(bytes: []const u8, bit_width: u8) ?struct { value: u32, count: usize } {
        // bit_width 0 means every level is implicitly 0 with no run
        // structure to read; report it as an unbounded run of zeros so
        // callers compare against their own max level as usual.
        if (bit_width == 0) return .{ .value = 0, .count = std.math.maxInt(usize) };
        var d = HybridRleDecoder.init(bytes, bit_width);
        d.readNextRun() catch return null;
        if (!d.in_rle) return null;
        return .{ .value = d.rle_value, .count = d.remaining_in_run };
    }

    fn readNextRun(self: *HybridRleDecoder) Error!void {
        const header = try self.readVarint();
        self.in_rle = (header & 1) == 0;
        const count_field = header >> 1;
        if (self.in_rle) {
            self.remaining_in_run = @intCast(count_field);
            self.rle_value = try self.readRleValue();
        } else {
            // Bit-packed run: count_field is the number of 8-value
            // groups. Stateless decode reads bytes directly from
            // `self.bytes` by computed offsets — no accumulator to
            // reset, no carry state.
            self.remaining_in_run = @as(usize, @intCast(count_field)) * 8;
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
        // Comptime-specialize on bit_width. Lifts runtime shifts and
        // masks into compile-time constants — every shift is an
        // direct, every mask is a constant. The 32-value inner
        // batch is `inline for`-unrolled so LLVM can vectorize.
        //
        // Code-size cost: 32 specializations × ~40 ops body ≈ ~40 KB
        // total bloat. Acceptable for ZPQ's hot path; trade-off matches
        // arrow-rs's `unpack32` and DuckDB FastPFor's bitpacking — the
        // industry-standard fast-bit-unpacking shape.
        return switch (self.bit_width) {
            inline 1...32 => |bw| self.decodeBitPackedFixed(bw, dest),
            // bit_width=0 short-circuits in `decode()` before reaching
            // here; widths >32 are rejected at init().
            else => unreachable,
        };
    }

    /// Stateless bit-packed decode for `dest.len` values at comptime
    /// `bw`. Reads bytes directly at computed offsets — no cross-call
    /// bit-accumulator. After return, `self.pos` advances by exactly
    /// `(dest.len * bw + 7) / 8` bytes.
    ///
    /// Why stateless: every value's bit position is computed afresh
    /// from `i * bw`, so a long bit-packed sequence cannot accumulate
    /// cross-call bit drift. This mirrors the byte-offset approach used
    /// by arrow-rs and DuckDB's FastPFor.
    ///
    /// Contract assumed by caller:
    /// - `dest.len` ≤ `self.remaining_in_run`
    /// - `self.pos` is byte-aligned at the start of a bit-packed group
    ///   (parquet guarantees this: bit-packed runs are byte-aligned,
    ///   groups of 8 values are byte-aligned)
    fn decodeBitPackedFixed(
        self: *HybridRleDecoder,
        comptime bw: u8,
        dest: []u32,
    ) Error!void {
        if (dest.len == 0) return;
        const bytes_needed = (dest.len * @as(usize, bw) + 7) / 8;
        if (self.pos + bytes_needed > self.bytes.len) {
            return error.UnexpectedEndOfStream;
        }

        // Per-call mask. comptime so it folds into constants.
        const mask: u32 = comptime if (bw == 32) std.math.maxInt(u32) else (@as(u32, 1) << bw) - 1;

        // Fast path: when we have ≥ 8 trailing bytes after the last
        // byte we need, we can do unchecked little-endian u64 reads
        // per value. The "extra" bits read past the value's own bits
        // get masked away. This is the common case — page payloads
        // typically have trailing data (next run, end-of-page).
        if (self.pos + bytes_needed + 8 <= self.bytes.len) {
            unpackFastFixed(bw, mask, self.bytes, self.pos, dest);
        } else {
            // Slow path: copy bytes into a stack-pad with 8 trailing
            // zeros so the u64 reads at the end of the dest sequence
            // are safe. Bound: caller never asks for >256 values per
            // call (rle_dict.Decoder's idx_buf cap), so worst-case
            // bytes_needed = 256 * 32 / 8 = 1024.
            var pad_buf: [1024 + 8]u8 = undefined;
            std.debug.assert(bytes_needed <= 1024);
            @memcpy(pad_buf[0..bytes_needed], self.bytes[self.pos .. self.pos + bytes_needed]);
            @memset(pad_buf[bytes_needed .. bytes_needed + 8], 0);
            unpackFastFixed(bw, mask, &pad_buf, 0, dest);
        }
        self.pos += bytes_needed;
    }
};

/// RLE-encoded BOOLEAN values (encoding == RLE). Boolean data is the
/// hybrid RLE/bit-packed stream at a fixed bit-width of 1; a `1` bit is
/// `true`. The 4-byte little-endian length prefix that precedes the
/// stream in a data page is stripped by the caller — `bytes` here is the
/// raw RLE stream only. Thin streaming adapter over `HybridRleDecoder`
/// that emits `bool` rather than `u32`.
pub const BooleanRleDecoder = struct {
    inner: HybridRleDecoder,

    pub fn init(bytes: []const u8) BooleanRleDecoder {
        return .{ .inner = HybridRleDecoder.init(bytes, 1) };
    }

    pub fn decode(self: *BooleanRleDecoder, dest: []bool) Error!usize {
        var written: usize = 0;
        var buf: [256]u32 = undefined;
        while (written < dest.len) {
            const want = @min(buf.len, dest.len - written);
            const got = try self.inner.decode(buf[0..want]);
            if (got == 0) break;
            for (0..got) |i| dest[written + i] = (buf[i] != 0);
            written += got;
        }
        return written;
    }
};

/// Stateless 32-value-at-a-time unpacker. Loops over 32-value batches
/// then handles the tail per-value. Caller guarantees 8 trailing bytes
/// of safety in `src` after `(src_off + dest.len * bw + 7) / 8`. This
/// is the hot kernel; everything that can be comptime-lifted is.
///
/// Output is `u32`, so values up to 32 bits fit. Caller passes the
/// pre-computed `mask`.
inline fn unpackFastFixed(
    comptime bw: u8,
    mask: u32,
    src: []const u8,
    src_off: usize,
    dest: []u32,
) void {
    // 32-value batches first. `inline for (0..32)` is the autovec
    // trigger LLVM needs to issue wide loads + bit ops in parallel.
    // Within a batch, `start_bit_in_batch = i * bw`, byte_off relative
    // to the batch start = `start_bit_in_batch / 8`, bit_off within
    // that byte = `start_bit_in_batch % 8`. We read a u64 LE at the
    // byte_off, shift right by bit_off, mask. 32*bw is always a
    // multiple of 8, so consecutive 32-batches stay byte-aligned.
    var i: usize = 0;
    while (i + 32 <= dest.len) : (i += 32) {
        const batch_start_byte = src_off + (i * @as(usize, bw)) / 8;
        inline for (0..32) |k| {
            const start_bit: usize = k * bw;
            const byte_off = start_bit / 8;
            const bit_off: u6 = @intCast(start_bit % 8);
            const word = std.mem.readInt(u64, src[batch_start_byte + byte_off ..][0..8], .little);
            dest[i + k] = @as(u32, @intCast((word >> bit_off) & mask));
        }
    }
    // Tail: 0..31 values. Same stateless arithmetic.
    while (i < dest.len) : (i += 1) {
        const start_bit = src_off * 8 + i * @as(usize, bw);
        const byte_off = start_bit / 8;
        const bit_off: u6 = @intCast(start_bit % 8);
        const word = std.mem.readInt(u64, src[byte_off..][0..8], .little);
        dest[i] = @as(u32, @intCast((word >> bit_off) & mask));
    }
}

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

test "BooleanRleDecoder decodes a bit-width-1 RLE run" {
    // RLE run header: count<<1 | 0 = (5<<1)=10 (varint 0x0A), value byte 0x01.
    // Then a bit-packed group: header (1<<1)|1 = 3 (0x03), one byte 0b00000010
    // → 8 values LSB-first: 0,1,0,0,0,0,0,0.
    const bytes = [_]u8{ 0x0A, 0x01, 0x03, 0x02 };
    var d = BooleanRleDecoder.init(&bytes);
    var out: [13]bool = undefined;
    const n = try d.decode(&out);
    try std.testing.expectEqual(@as(usize, 13), n);
    try std.testing.expectEqualSlices(bool, &.{
        true, true, true, true, true, // RLE run of 5 ones
        false, true, false, false, false, false, false, false, // bit-packed 8
    }, &out);
}

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

test "large single bit-packed run at bw=15, decoded in 256-chunks" {
    // Reproduces the shape from a real-world wide-table production
    // parquet:
    //   - bit_width = 15 (dict has 16384..32768 entries)
    //   - one large single bit-packed run (no RLE breaks)
    //   - decoded in 256-value chunks the way rle_dict.Decoder calls
    //     HybridRleDecoder
    //
    // Real-world case discovered 2026-05-12: a DOUBLE column dict-
    // encoded with bit_width=15 and 122,880 values per page produced
    // systematically-wrong values, ~7% off the correct sum. Per-RG
    // round-trips through smaller pages were correct; only the large
    // single-page run shape broke. This test pins the shape so
    // regressions can't slip back in.
    const arena = testing.allocator;
    const N: usize = 122880;
    const bw: u8 = 15;

    // Pseudo-random indices in [0, 2^15 - 1]. Random distribution
    // ensures no RLE-eligible runs ≥ 8, so the encoder emits a single
    // bit-packed run, the shape most sensitive to cross-call drift.
    const values = try arena.alloc(u32, N);
    defer arena.free(values);
    var rng: u64 = 0xfeedfacecafebeef;
    for (values) |*v| {
        rng = rng *% 6364136223846793005 +% 1442695040888963407;
        v.* = @intCast((rng >> 32) & 0x7FFF); // 15-bit mask
    }

    const encoded = try encode(arena, values, bw);
    defer arena.free(encoded);

    var dec = HybridRleDecoder.init(encoded, bw);
    const out = try arena.alloc(u32, N);
    defer arena.free(out);

    // Decode in 256-value chunks — exactly how rle_dict.Decoder
    // calls HybridRleDecoder. State persistence across calls is
    // the load-bearing invariant.
    var written: usize = 0;
    while (written < N) {
        const want = @min(@as(usize, 256), N - written);
        const n = try dec.decode(out[written .. written + want]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(N, written);

    var mismatches: usize = 0;
    var first_bad: usize = N;
    for (values, out, 0..) |expected, actual, i| {
        if (expected != actual) {
            if (first_bad == N) first_bad = i;
            mismatches += 1;
        }
    }
    if (mismatches > 0) {
        std.debug.print(
            "\nbw=15 large run: {d} / {d} mismatches; first at idx={d} expected={d} got={d}\n",
            .{ mismatches, N, first_bad, values[first_bad], out[first_bad] },
        );
    }
    try testing.expectEqual(@as(usize, 0), mismatches);
}

test "bw=15 mixed RLE+bit-packed runs decoded in 256-chunks" {
    // Large dictionary-index streams often alternate repeated default
    // values with varied stretches. That produces mixed RLE and
    // bit-packed runs, which stresses both carry handling and cross-call
    // alignment.
    const arena = testing.allocator;
    const N: usize = 122880;
    const bw: u8 = 15;

    // Build the value sequence: blocks of [16 random values] then
    // [256 repeated zeros], repeating. The zeros block is RLE-eligible
    // (≥8 same), the random block is bit-packed (multiple of 8).
    const values = try arena.alloc(u32, N);
    defer arena.free(values);
    var rng: u64 = 0xfeedfacecafebeef;
    var i: usize = 0;
    while (i < N) {
        // Bit-packed block of 16 random 15-bit indices.
        const bp_end = @min(i + 16, N);
        while (i < bp_end) : (i += 1) {
            rng = rng *% 6364136223846793005 +% 1442695040888963407;
            values[i] = @intCast((rng >> 32) & 0x7FFF);
        }
        // RLE block of 256 zeros.
        const rle_end = @min(i + 256, N);
        while (i < rle_end) : (i += 1) values[i] = 0;
    }

    const encoded = try encode(arena, values, bw);
    defer arena.free(encoded);

    var dec = HybridRleDecoder.init(encoded, bw);
    const out = try arena.alloc(u32, N);
    defer arena.free(out);

    // Decode in 256-value chunks (matches rle_dict.Decoder's pattern).
    var written: usize = 0;
    while (written < N) {
        const want = @min(@as(usize, 256), N - written);
        const n = try dec.decode(out[written .. written + want]);
        if (n == 0) break;
        written += n;
    }
    try testing.expectEqual(N, written);

    var mismatches: usize = 0;
    var first_bad: usize = N;
    for (values, out, 0..) |expected, actual, k| {
        if (expected != actual) {
            if (first_bad == N) first_bad = k;
            mismatches += 1;
        }
    }
    if (mismatches > 0) {
        std.debug.print(
            "\nbw=15 mixed: {d} / {d} mismatches; first at idx={d} expected={d} got={d}\n",
            .{ mismatches, N, first_bad, values[first_bad], out[first_bad] },
        );
        // Show a few surrounding rows to see the drift pattern.
        const lo = if (first_bad > 4) first_bad - 4 else 0;
        const hi = @min(first_bad + 8, N);
        var k = lo;
        while (k < hi) : (k += 1) {
            const mark: u8 = if (values[k] != out[k]) '*' else ' ';
            std.debug.print("  {c} idx={d:>5} expected={d:>5} got={d:>5}\n", .{ mark, k, values[k], out[k] });
        }
    }
    try testing.expectEqual(@as(usize, 0), mismatches);
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
