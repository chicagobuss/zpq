//! LZ4 block-format decoder (LZ4_RAW codec in Parquet).
//!
//! Parquet's LZ4_RAW (codec 7) is the LZ4 block format directly —
//! no frame headers, no checksums. The legacy Parquet LZ4 codec
//! (codec 5) used Hadoop framing and is intentionally skipped: no
//! modern writer emits it.
//!
//! Block format reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
//!
//! A block is a sequence of "sequences". Each sequence:
//!   1. Token byte: high nibble = literal length (0..14, with 15
//!      meaning "extended"), low nibble = match length (0..14,
//!      same extension rule, biased by 4).
//!   2. Literal length extension bytes if high nibble == 15.
//!   3. `literal_length` raw bytes copied to output.
//!   4. (Last sequence ends here; no match.) Otherwise:
//!   5. Offset: u16 LE, in [1, history_size]. Match starts
//!      `offset` bytes back from the current output position.
//!   6. Match length extension bytes if low nibble == 15.
//!   7. Copy `match_length` bytes from `output[d_idx - offset]`
//!      onward to `output[d_idx]`. The copy *may* overlap (RLE-style).

const std = @import("std");

pub const Error = error{
    CorruptInput,
    OutputTooSmall,
};

pub fn uncompress(src: []const u8, dest: []u8) Error!usize {
    var s_idx: usize = 0;
    var d_idx: usize = 0;

    while (s_idx < src.len) {
        // Token
        const token = src[s_idx];
        s_idx += 1;
        var literal_len: usize = token >> 4;
        var match_len: usize = token & 0xf;

        // Literal-length extension
        if (literal_len == 15) {
            while (s_idx < src.len) {
                const b = src[s_idx];
                s_idx += 1;
                literal_len += b;
                if (b != 255) break;
            }
        }

        // Copy literals
        if (s_idx + literal_len > src.len) return error.CorruptInput;
        if (d_idx + literal_len > dest.len) return error.OutputTooSmall;
        @memcpy(dest[d_idx..][0..literal_len], src[s_idx..][0..literal_len]);
        s_idx += literal_len;
        d_idx += literal_len;

        // Last sequence: stream terminates after the final block of literals.
        if (s_idx == src.len) break;

        // Offset
        if (s_idx + 2 > src.len) return error.CorruptInput;
        const offset = std.mem.readInt(u16, src[s_idx..][0..2], .little);
        s_idx += 2;
        if (offset == 0 or offset > d_idx) return error.CorruptInput;

        // Match-length extension
        if (match_len == 15) {
            while (s_idx < src.len) {
                const b = src[s_idx];
                s_idx += 1;
                match_len += b;
                if (b != 255) break;
            }
        }
        match_len += 4; // minmatch

        // Copy match. Overlap is permitted (RLE) so we go byte-by-byte
        // when offset < match_len. Otherwise @memcpy is fine.
        if (d_idx + match_len > dest.len) return error.OutputTooSmall;
        const start = d_idx - offset;
        if (offset >= match_len) {
            @memcpy(dest[d_idx..][0..match_len], dest[start..][0..match_len]);
        } else {
            // Byte-by-byte: each iteration's dest may be the source of a
            // later iteration.
            var i: usize = 0;
            while (i < match_len) : (i += 1) {
                dest[d_idx + i] = dest[start + i];
            }
        }
        d_idx += match_len;
    }

    return d_idx;
}

// ============================================================
// Compression (greedy LZ4 block encoder)
// ============================================================
//
// Standard fast LZ4: a 4-byte rolling hash maps positions; at each spot we
// probe the table for a ≥4-byte match within 64 KB, extend it greedily, emit
// the pending literals + match, and continue. End-of-block rules from the
// reference encoder are honoured so *any* LZ4 decoder (pyarrow/cramjam/arrow)
// reads our output: the last 5 bytes are always literals (LASTLITERALS) and no
// match is searched within the last 12 bytes (MFLIMIT).

const MINMATCH: usize = 4;
const MFLIMIT: usize = 12;
const LASTLITERALS: usize = 5;
const HASH_LOG = 16;
pub const HASH_SIZE: usize = 1 << HASH_LOG;

fn hash4(seq: u32) u32 {
    return (seq *% 2654435761) >> (32 - HASH_LOG);
}

fn readU32(buf: []const u8, i: usize) u32 {
    return std.mem.readInt(u32, buf[i..][0..4], .little);
}

/// Maximum compressed size for `n` input bytes (reference LZ4_compressBound).
pub fn compressBound(n: usize) usize {
    return n + (n / 255) + 16;
}

/// Compress `src` into `dest` (≥ `compressBound(src.len)`). `table` is a
/// caller-provided scratch hash table of `HASH_SIZE` entries (kept off the
/// stack — 256 KB). Returns the number of bytes written.
pub fn compress(src: []const u8, dest: []u8, table: []u32) Error!usize {
    @memset(table, 0); // 0 = empty; we store position+1
    var d_idx: usize = 0;
    var anchor: usize = 0; // start of the not-yet-emitted literal run
    var s_idx: usize = 0;

    if (src.len >= MFLIMIT + 1) {
        const mf_limit = src.len - MFLIMIT; // no match search at/after here
        const match_limit = src.len - LASTLITERALS; // matches can't cover these
        s_idx = 1;
        while (s_idx < mf_limit) {
            const seq = readU32(src, s_idx);
            const h = hash4(seq);
            const cand = table[h];
            table[h] = @intCast(s_idx + 1);
            const matched = cand != 0 and blk: {
                const m = cand - 1;
                break :blk (s_idx - m) <= 65535 and readU32(src, m) == seq;
            };
            if (!matched) {
                s_idx += 1;
                continue;
            }
            const m = cand - 1;
            const offset = s_idx - m;
            // Extend the match forward (stop short of the last 5 literals).
            var mlen: usize = MINMATCH;
            while (s_idx + mlen < match_limit and src[m + mlen] == src[s_idx + mlen]) mlen += 1;
            try emitSequence(dest, &d_idx, src, anchor, s_idx, offset, mlen);
            s_idx += mlen;
            anchor = s_idx;
        }
    }
    try emitLastLiterals(dest, &d_idx, src, anchor);
    return d_idx;
}

fn emitLen(dest: []u8, d_idx: *usize, extra: usize) void {
    var rem = extra;
    while (rem >= 255) : (rem -= 255) {
        dest[d_idx.*] = 255;
        d_idx.* += 1;
    }
    dest[d_idx.*] = @intCast(rem);
    d_idx.* += 1;
}

fn emitSequence(dest: []u8, d_idx: *usize, src: []const u8, anchor: usize, s_idx: usize, offset: usize, mlen: usize) Error!void {
    const litlen = s_idx - anchor;
    const ml_field = mlen - MINMATCH;
    const tok_lit: u8 = if (litlen >= 15) 15 else @intCast(litlen);
    const tok_ml: u8 = if (ml_field >= 15) 15 else @intCast(ml_field);
    if (d_idx.* + 1 > dest.len) return error.OutputTooSmall;
    dest[d_idx.*] = (tok_lit << 4) | tok_ml;
    d_idx.* += 1;
    if (litlen >= 15) emitLen(dest, d_idx, litlen - 15);
    @memcpy(dest[d_idx.*..][0..litlen], src[anchor..][0..litlen]);
    d_idx.* += litlen;
    std.mem.writeInt(u16, dest[d_idx.*..][0..2], @intCast(offset), .little);
    d_idx.* += 2;
    if (ml_field >= 15) emitLen(dest, d_idx, ml_field - 15);
}

fn emitLastLiterals(dest: []u8, d_idx: *usize, src: []const u8, anchor: usize) Error!void {
    const litlen = src.len - anchor;
    const tok_lit: u8 = if (litlen >= 15) 15 else @intCast(litlen);
    if (d_idx.* + 1 > dest.len) return error.OutputTooSmall;
    dest[d_idx.*] = tok_lit << 4;
    d_idx.* += 1;
    if (litlen >= 15) emitLen(dest, d_idx, litlen - 15);
    @memcpy(dest[d_idx.*..][0..litlen], src[anchor..][0..litlen]);
    d_idx.* += litlen;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "compress→uncompress round-trip (literals, matches, RLE, large)" {
    const table = try testing.allocator.alloc(u32, HASH_SIZE);
    defer testing.allocator.free(table);

    const cases = [_][]const u8{
        "",
        "a",
        "hello world",
        "ababababababababababab", // short-offset overlapping matches
        "the quick brown fox jumps over the quick brown fox jumps over the lazy dog",
    };
    for (cases) |input| {
        const dst = try testing.allocator.alloc(u8, compressBound(input.len));
        defer testing.allocator.free(dst);
        const clen = try compress(input, dst, table);
        const out = try testing.allocator.alloc(u8, input.len);
        defer testing.allocator.free(out);
        const n = try uncompress(dst[0..clen], out);
        try testing.expectEqual(input.len, n);
        try testing.expectEqualSlices(u8, input, out[0..n]);
    }

    // Highly compressible: 4 KB of one byte must shrink and round-trip.
    const big = try testing.allocator.alloc(u8, 4096);
    defer testing.allocator.free(big);
    @memset(big, 'Z');
    const cbig = try testing.allocator.alloc(u8, compressBound(big.len));
    defer testing.allocator.free(cbig);
    const cn = try compress(big, cbig, table);
    try testing.expect(cn < big.len); // actually compressed
    const obig = try testing.allocator.alloc(u8, big.len);
    defer testing.allocator.free(obig);
    const n2 = try uncompress(cbig[0..cn], obig);
    try testing.expectEqualSlices(u8, big, obig[0..n2]);
}

test "uncompress empty input" {
    var out: [4]u8 = undefined;
    const n = try uncompress(&[_]u8{}, &out);
    try testing.expectEqual(@as(usize, 0), n);
}

test "uncompress all-literals (one short sequence)" {
    // Token: literal_len=5, match_len=0 → 0x50.
    // Literals: "hello".
    // No offset/match because we run out of src.
    const src = [_]u8{ 0x50, 'h', 'e', 'l', 'l', 'o' };
    var out: [16]u8 = undefined;
    const n = try uncompress(&src, &out);
    try testing.expectEqual(@as(usize, 5), n);
    try testing.expectEqualStrings("hello", out[0..n]);
}

test "uncompress with match (overlapping copy)" {
    // Encode "ababab": first sequence emits literal "ab", then a match
    // of length 4 with offset 2 (copies the previous "ab" twice with
    // overlap).
    // Token: lit=2, match=0 → 0x20. Wait, match nibble must encode
    // len-4. We want match_len=4, so low nibble = 0.
    // After literals, we read offset (2 bytes LE), then match.
    // Sequence layout:
    //   token=0x20  literals="ab"  offset=0x0002  (no extra match bytes)
    // Then end-of-stream — but LZ4 requires the last sequence to be
    // pure literals. So we add a final token=0x00 (no literals, no
    // match) — actually that's still requires offset/match. Hmm.
    //
    // Cleanest: encode "ababab\0" with a final all-literals sequence.
    // Sequence 1: token=0x20  "ab"  offset=2  → emits "ababab" (lit 2 + match 4)
    // Sequence 2: token=0x10  "\0"  → final literal
    // Wait, the final sequence is detected by exhausting src AFTER
    // emitting literals. So the encoder: token byte + literal extras +
    // literal bytes (and src ends here).
    //
    // Let's just do: lit "ab" + match offset=2 len=4 (=4 from token) +
    // final lit "X".
    const src = [_]u8{
        0x20,            // token: lit_len=2, match_len_field=0 (=4 final)
        'a', 'b',        // literals
        0x02, 0x00,      // offset=2 LE
        0x10,            // token: lit_len=1, match_len_field=0
        'X',             // final literal
    };
    var out: [16]u8 = undefined;
    const n = try uncompress(&src, &out);
    try testing.expectEqual(@as(usize, 7), n);
    try testing.expectEqualStrings("ababab" ++ "X", out[0..n]);
}

test "uncompress extended literal length" {
    // Token: lit=15, match=0. Then extension: 5. So lit_len=20.
    // Then 20 'A's, then no match (end of stream).
    var src: [22]u8 = undefined;
    src[0] = 0xf0;
    src[1] = 5;
    @memset(src[2..22], 'A');
    var out: [32]u8 = undefined;
    const n = try uncompress(&src, &out);
    try testing.expectEqual(@as(usize, 20), n);
    var i: usize = 0;
    while (i < 20) : (i += 1) try testing.expectEqual(@as(u8, 'A'), out[i]);
}

test "uncompress canned LZ4 block" {
    // Generated externally:
    //   $ printf 'aaaaaaaaaaaaaaaaaaaa' | lz4 -B4 --no-frame-crc | xxd
    // Just verify our decoder handles a well-formed LZ4 block.
    // Hand-built:
    //   Token: lit=1, match=15 → 0x1f. 1 literal 'a'.
    //   Offset = 1 (back-reference to that 'a').
    //   Match extension: 0 → match_len = 15 + 4 = 19. Wait no,
    //   match_len = 15+0+4 = 19. We get 1 'a' literal + 19 byte
    //   match → 20 chars total.
    //   But last sequence rule says match_len > 0 needs offset, then
    //   no more sequences after. Stream ends after match; that's fine
    //   per LZ4 unless length < 5 from end (a tail-of-block rule that
    //   doesn't strictly apply to LZ4_RAW — Parquet uses raw blocks).
    const src = [_]u8{ 0x1f, 'a', 0x01, 0x00, 0x00 }; // 0x00 => match_len=19
    var out: [32]u8 = undefined;
    const n = try uncompress(&src, &out);
    try testing.expectEqual(@as(usize, 20), n);
    for (out[0..20]) |b| try testing.expectEqual(@as(u8, 'a'), b);
}
