//! DELTA_LENGTH_BYTE_ARRAY + DELTA_BYTE_ARRAY encodings.
//!
//! Both build on DELTA_BINARY_PACKED for the integer streams.
//!
//! DELTA_LENGTH_BYTE_ARRAY:
//!   <DELTA_BINARY_PACKED i32 lengths>  <concatenated bytes>
//! Each output value is `length[i]` bytes from the bytes section.
//! Slices yielded to the caller point into the bytes section directly
//! (zero-copy).
//!
//! DELTA_BYTE_ARRAY:
//!   <DELTA_BINARY_PACKED i32 prefix_lengths>
//!   <DELTA_LENGTH_BYTE_ARRAY suffixes>
//! Each output value is `prev[0..prefix_lengths[i]] ++ suffixes[i]`,
//! where `prev` is the previously-emitted value. Used by Spark for
//! highly-similar strings (sorted columns, JSON paths, etc.).
//!
//! DELTA_BYTE_ARRAY values must be materialized into a per-value
//! arena slice because they're conceptually constructed from a
//! prefix of the previous decoded value plus a suffix from the
//! suffix stream — there's no contiguous source slice.
//!
//! Both decoders eagerly decode the integer streams into an arena
//! up front. The integer streams are typically small (one i32 per
//! value, often heavily repeated/zero-bit-width), so this pays off
//! in simpler decode logic.

const std = @import("std");
const dbp = @import("delta_binary_packed.zig");

pub const Error = error{
    UnexpectedEndOfStream,
    NegativeLength,
    OutOfMemory,
    InvalidStream,
} || dbp.Error;

// ============================================================
// DELTA_LENGTH_BYTE_ARRAY
// ============================================================

pub const DeltaLengthByteArrayDecoder = struct {
    /// Decoded length per value. arena-owned.
    lengths: []const i32,
    cursor: usize,
    /// Bytes section, sliced from the end of the lengths stream.
    bytes: []const u8,
    bytes_pos: usize,

    pub fn init(encoded: []const u8, arena: std.mem.Allocator) Error!DeltaLengthByteArrayDecoder {
        var len_dec = try dbp.Decoder(i32).init(encoded);
        const total: usize = @intCast(len_dec.total_value_count);
        const lengths = try arena.alloc(i32, total);
        errdefer arena.free(lengths);

        var written: usize = 0;
        while (written < total) {
            const n = try len_dec.decode(lengths[written..]);
            if (n == 0) break;
            written += n;
        }
        if (written != total) return error.InvalidStream;

        return .{
            .lengths = lengths,
            .cursor = 0,
            .bytes = encoded[len_dec.pos..],
            .bytes_pos = 0,
        };
    }

    pub fn decode(self: *DeltaLengthByteArrayDecoder, dest: [][]const u8) Error!usize {
        var i: usize = 0;
        while (i < dest.len and self.cursor < self.lengths.len) : (i += 1) {
            const len = self.lengths[self.cursor];
            if (len < 0) return error.NegativeLength;
            const ulen: usize = @intCast(len);
            if (self.bytes_pos + ulen > self.bytes.len) return error.UnexpectedEndOfStream;
            dest[i] = self.bytes[self.bytes_pos..][0..ulen];
            self.bytes_pos += ulen;
            self.cursor += 1;
        }
        return i;
    }
};

// ============================================================
// DELTA_BYTE_ARRAY
// ============================================================

pub const DeltaByteArrayDecoder = struct {
    arena: std.mem.Allocator,
    /// prefix_lengths[i] = how many leading bytes to copy from the
    /// previous decoded value. arena-owned.
    prefix_lengths: []const i32,
    /// Suffix decoder yields the per-value tail.
    suffixes: DeltaLengthByteArrayDecoder,
    cursor: usize,
    /// Last fully-emitted value, kept so the next call can reference
    /// its prefix. arena-owned, replaced per value.
    last_value: []u8,

    pub fn init(encoded: []const u8, arena: std.mem.Allocator) Error!DeltaByteArrayDecoder {
        // First DELTA_BINARY_PACKED stream: prefix lengths.
        var prefix_dec = try dbp.Decoder(i32).init(encoded);
        const total: usize = @intCast(prefix_dec.total_value_count);
        const prefixes = try arena.alloc(i32, total);
        errdefer arena.free(prefixes);

        var written: usize = 0;
        while (written < total) {
            const n = try prefix_dec.decode(prefixes[written..]);
            if (n == 0) break;
            written += n;
        }
        if (written != total) return error.InvalidStream;

        // The remaining bytes are a DELTA_LENGTH_BYTE_ARRAY stream.
        const suffix_bytes = encoded[prefix_dec.pos..];
        const suffixes = try DeltaLengthByteArrayDecoder.init(suffix_bytes, arena);
        if (suffixes.lengths.len != total) return error.InvalidStream;

        return .{
            .arena = arena,
            .prefix_lengths = prefixes,
            .suffixes = suffixes,
            .cursor = 0,
            .last_value = &[_]u8{},
        };
    }

    pub fn decode(self: *DeltaByteArrayDecoder, dest: [][]const u8) Error!usize {
        var i: usize = 0;
        var suffix_buf: [1]([]const u8) = undefined;
        while (i < dest.len and self.cursor < self.prefix_lengths.len) : (i += 1) {
            const prefix_len_raw = self.prefix_lengths[self.cursor];
            if (prefix_len_raw < 0) return error.NegativeLength;
            const prefix_len: usize = @intCast(prefix_len_raw);
            if (prefix_len > self.last_value.len) return error.InvalidStream;

            // Pull one suffix.
            const got = try self.suffixes.decode(suffix_buf[0..1]);
            if (got != 1) return error.InvalidStream;
            const suffix = suffix_buf[0];

            const total_len = prefix_len + suffix.len;
            const buf = try self.arena.alloc(u8, total_len);
            @memcpy(buf[0..prefix_len], self.last_value[0..prefix_len]);
            @memcpy(buf[prefix_len..], suffix);

            dest[i] = buf;
            self.last_value = buf;
            self.cursor += 1;
        }
        return i;
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

// We need a DELTA_BINARY_PACKED encoder for tests. Reuse the helper
// from the dbp test file via direct call — but that's `fn encode(...)`
// in another file's test scope, not exported. Re-export-via-pub-fn
// would pollute the API. Easier: write a tiny encoder inline.
//
// Generates a DELTA_BINARY_PACKED stream from i32 values. Block size
// 128, 4 mini-blocks.
fn encodeDbp(values: []const i32) ![]u8 {
    return encodeDbpBytes(values);
}

// We just embed the encoder body here (mirrors the one in
// delta_binary_packed.zig tests).
fn encodeDbpBytes(values: []const i32) ![]u8 {
    const block_size: u32 = 128;
    const mini_blocks: u32 = 4;
    const mini_block_size = block_size / mini_blocks;

    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(testing.allocator);

    try writeUVarint(&buf, block_size);
    try writeUVarint(&buf, mini_blocks);
    try writeUVarint(&buf, values.len);
    if (values.len == 0) {
        try writeZigzagI32(&buf, 0);
        return buf.toOwnedSlice(testing.allocator);
    }
    try writeZigzagI32(&buf, values[0]);

    var i: usize = 1;
    while (i < values.len) {
        const block_end = @min(i + block_size, values.len);
        const n = block_end - i;

        var deltas: [128]i32 = undefined;
        var j: usize = 0;
        while (j < n) : (j += 1) {
            deltas[j] = values[i + j] - values[i + j - 1];
        }
        var min_d: i32 = if (n == 0) 0 else deltas[0];
        var k: usize = 1;
        while (k < n) : (k += 1) {
            if (deltas[k] < min_d) min_d = deltas[k];
        }
        while (j < block_size) : (j += 1) deltas[j] = min_d;

        try writeZigzagI32(&buf, min_d);

        var widths: [4]u8 = .{ 0, 0, 0, 0 };
        var mb: u32 = 0;
        while (mb < mini_blocks) : (mb += 1) {
            const start = mb * mini_block_size;
            const stop = start + mini_block_size;
            var max_diff: u64 = 0;
            var v: usize = start;
            while (v < stop) : (v += 1) {
                const u: u64 = @intCast(deltas[v] - min_d);
                if (u > max_diff) max_diff = u;
            }
            widths[mb] = if (max_diff == 0) 0 else @intCast(64 - @clz(max_diff));
        }
        try buf.appendSlice(testing.allocator, &widths);

        mb = 0;
        while (mb < mini_blocks) : (mb += 1) {
            const start = mb * mini_block_size;
            const stop = start + mini_block_size;
            const bw = widths[mb];
            if (bw == 0) continue;
            var bit_buffer: u64 = 0;
            var bits: u8 = 0;
            var v: usize = start;
            while (v < stop) : (v += 1) {
                const u: u64 = @intCast(deltas[v] - min_d);
                bit_buffer |= u << @intCast(bits);
                bits += bw;
                while (bits >= 8) {
                    try buf.append(testing.allocator, @truncate(bit_buffer));
                    bit_buffer >>= 8;
                    bits -= 8;
                }
            }
            if (bits > 0) try buf.append(testing.allocator, @truncate(bit_buffer));
        }

        i = block_end;
    }

    return buf.toOwnedSlice(testing.allocator);
}

fn writeUVarint(buf: *std.ArrayList(u8), value: u64) !void {
    var v = value;
    while (true) {
        if (v < 0x80) {
            try buf.append(testing.allocator, @intCast(v));
            return;
        }
        try buf.append(testing.allocator, @as(u8, @intCast(v & 0x7f)) | 0x80);
        v >>= 7;
    }
}

fn writeZigzagI32(buf: *std.ArrayList(u8), value: i32) !void {
    const z: i32 = (value << 1) ^ (value >> 31);
    const u: u32 = @bitCast(z);
    try writeUVarint(buf, u);
}

test "DELTA_LENGTH_BYTE_ARRAY round-trip" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const values = [_][]const u8{ "hello", "world", "foo", "x" };
    const lengths = [_]i32{ 5, 5, 3, 1 };

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(testing.allocator);
    const lens_enc = try encodeDbp(&lengths);
    defer testing.allocator.free(lens_enc);
    try stream.appendSlice(testing.allocator, lens_enc);
    for (values) |v| try stream.appendSlice(testing.allocator, v);

    var dec = try DeltaLengthByteArrayDecoder.init(stream.items, a);
    var out: [4][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    for (values, 0..) |v, i| try testing.expectEqualStrings(v, out[i]);
}

test "DELTA_BYTE_ARRAY round-trip with shared prefixes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const values = [_][]const u8{
        "common-prefix-A",
        "common-prefix-B",
        "common-prefix-C",
        "common-prefix-different",
    };
    // For each value, prefix_len = how many leading bytes match the previous.
    const prefix_lengths = [_]i32{ 0, 14, 14, 14 };
    const suffixes = [_][]const u8{ "common-prefix-A", "B", "C", "different" };
    const suffix_lengths = [_]i32{ 15, 1, 1, 9 };

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(testing.allocator);

    const pref_enc = try encodeDbp(&prefix_lengths);
    defer testing.allocator.free(pref_enc);
    try stream.appendSlice(testing.allocator, pref_enc);

    const suf_lens_enc = try encodeDbp(&suffix_lengths);
    defer testing.allocator.free(suf_lens_enc);
    try stream.appendSlice(testing.allocator, suf_lens_enc);
    for (suffixes) |s| try stream.appendSlice(testing.allocator, s);

    var dec = try DeltaByteArrayDecoder.init(stream.items, a);
    var out: [4][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, values.len), n);
    for (values, 0..) |v, i| try testing.expectEqualStrings(v, out[i]);
}

test "DELTA_LENGTH_BYTE_ARRAY empty stream" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const lens_enc = try encodeDbp(&[_]i32{});
    defer testing.allocator.free(lens_enc);

    var dec = try DeltaLengthByteArrayDecoder.init(lens_enc, a);
    var out: [4][]const u8 = undefined;
    const n = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 0), n);
}

test "DELTA_BYTE_ARRAY partial decode preserves last_value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    _ = [_][]const u8{ "abc", "abd", "abe", "abf" }; // expected
    const prefix_lengths = [_]i32{ 0, 2, 2, 2 };
    const suffixes = [_][]const u8{ "abc", "d", "e", "f" };
    const suffix_lengths = [_]i32{ 3, 1, 1, 1 };

    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(testing.allocator);

    const pref_enc = try encodeDbp(&prefix_lengths);
    defer testing.allocator.free(pref_enc);
    try stream.appendSlice(testing.allocator, pref_enc);

    const suf_lens_enc = try encodeDbp(&suffix_lengths);
    defer testing.allocator.free(suf_lens_enc);
    try stream.appendSlice(testing.allocator, suf_lens_enc);
    for (suffixes) |s| try stream.appendSlice(testing.allocator, s);

    var dec = try DeltaByteArrayDecoder.init(stream.items, a);
    var out: [2][]const u8 = undefined;

    const n1 = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n1);
    try testing.expectEqualStrings("abc", out[0]);
    try testing.expectEqualStrings("abd", out[1]);

    const n2 = try dec.decode(&out);
    try testing.expectEqual(@as(usize, 2), n2);
    try testing.expectEqualStrings("abe", out[0]);
    try testing.expectEqualStrings("abf", out[1]);
}
