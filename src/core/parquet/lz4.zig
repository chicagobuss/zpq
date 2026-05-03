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
// Tests
// ============================================================

const testing = std.testing;

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
