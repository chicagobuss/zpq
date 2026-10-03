//! LZ4 for Parquet: the block format (LZ4_RAW, codec 7) plus the two
//! containers the deprecated LZ4 codec (codec 5) shows up in.
//!
//! Codec 5 is ambiguous in the wild (PARQUET-1241): parquet-mr wrote Hadoop's
//! BlockCompressorStream framing, older parquet-cpp a bare block, older
//! arrow-rs an LZ4 frame. `uncompressHadoop` and `uncompressFrame` parse the
//! containers; compression.zig owns the order they are tried in.
//!
//! Block format reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Block_format.md
//! Frame format reference: https://github.com/lz4/lz4/blob/dev/doc/lz4_Frame_format.md
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
    return uncompressAt(src, dest, 0);
}

/// Decode one block into `dest[d_start..]`, with `dest[0..d_start]` as the match history (an LZ4 frame's linked
/// blocks). Returns the end index in `dest`, not the byte count.
pub fn uncompressAt(src: []const u8, dest: []u8, d_start: usize) Error!usize {
    std.debug.assert(d_start <= dest.len);
    var s_idx: usize = 0;
    var d_idx: usize = d_start;

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
// Legacy codec 5 containers
// ============================================================

/// Hadoop BlockCompressorStream framing as parquet-mr writes it: back-to-back chunks of `[u32 BE uncompressed
/// len]` followed by `[u32 BE compressed len][LZ4 block]` pieces until that length is produced. Usually one piece;
/// a single write() larger than Hadoop's buffer is split into several, each compressed with no history from the
/// last (BlockDecompressorStream reads the same way). Every chunk has at least one piece, which is Arrow C++'s
/// `TryDecompressHadoop` (ARROW-9177) shape. The framing carries no magic, so it is only accepted when every length
/// checks out and the chunks fill `dest` exactly; anything else is `NotHadoop` and the caller tries the next
/// container.
pub fn uncompressHadoop(src: []const u8, dest: []u8) error{NotHadoop}!void {
    var s_idx: usize = 0;
    var d_idx: usize = 0;
    while (src.len - s_idx >= 8) {
        const ulen = std.mem.readInt(u32, src[s_idx..][0..4], .big);
        s_idx += 4;
        if (ulen > dest.len - d_idx) return error.NotHadoop;
        const chunk_end = d_idx + ulen;
        // Each pass consumes at least the 4-byte length, so even zero-length pieces cannot spin.
        while (true) {
            if (src.len - s_idx < 4) return error.NotHadoop;
            const clen = std.mem.readInt(u32, src[s_idx..][0..4], .big);
            s_idx += 4;
            if (clen > src.len - s_idx) return error.NotHadoop;
            // Bounded by `chunk_end`, so a piece that overshoots the chunk's length is OutputTooSmall.
            const n = uncompress(src[s_idx..][0..clen], dest[d_idx..chunk_end]) catch return error.NotHadoop;
            s_idx += clen;
            d_idx += n;
            if (d_idx == chunk_end) break;
        }
    }
    if (s_idx != src.len or d_idx != dest.len) return error.NotHadoop;
}

const frame_magic: u32 = 0x184D2204;
const skippable_magic: u32 = 0x184D2A50; // low nibble is free: 0x184D2A50..5F
const skippable_mask: u32 = 0xFFFFFFF0;

/// True when `src` opens with an LZ4 frame or skippable-frame magic number.
pub fn hasFrameMagic(src: []const u8) bool {
    if (src.len < 4) return false;
    const m = std.mem.readInt(u32, src[0..4], .little);
    return m == frame_magic or m & skippable_mask == skippable_magic;
}

/// Decode one or more concatenated LZ4 frames (skippable frames are skipped)
/// into `dest`. Returns the bytes written. Header, block and content checksums
/// are verified when present; a declared content size must match. Frames that
/// need an external dictionary are rejected — none can be supplied here.
pub fn uncompressFrame(src: []const u8, dest: []u8) Error!usize {
    var s_idx: usize = 0;
    var d_idx: usize = 0;
    if (src.len == 0) return error.CorruptInput;
    while (s_idx < src.len) {
        if (src.len - s_idx < 4) return error.CorruptInput;
        const magic = std.mem.readInt(u32, src[s_idx..][0..4], .little);
        s_idx += 4;
        if (magic & skippable_mask == skippable_magic) {
            if (src.len - s_idx < 4) return error.CorruptInput;
            const skip = std.mem.readInt(u32, src[s_idx..][0..4], .little);
            s_idx += 4;
            if (skip > src.len - s_idx) return error.CorruptInput;
            s_idx += skip;
            continue;
        }
        if (magic != frame_magic) return error.CorruptInput;
        d_idx = try decodeFrameBody(src, &s_idx, dest, d_idx);
    }
    return d_idx;
}

/// Decode the frame whose descriptor starts at `s_idx.*` into `dest[d_start..]`. Returns the new end of output.
fn decodeFrameBody(src: []const u8, s_idx: *usize, dest: []u8, d_start: usize) Error!usize {
    var s = s_idx.*;
    if (src.len - s < 3) return error.CorruptInput;
    const flg = src[s];
    const bd = src[s + 1];
    // Version must be 01; reserved bits must be zero.
    if (flg >> 6 != 1 or flg & 0x02 != 0 or bd & 0x8F != 0) return error.CorruptInput;
    const block_size_id = (bd >> 4) & 0x7;
    if (block_size_id < 4) return error.CorruptInput;
    const block_max: usize = @as(usize, 1) << @intCast(8 + 2 * @as(u32, block_size_id)); // 64 KiB .. 4 MiB
    const independent = flg & 0x20 != 0;
    const has_block_checksum = flg & 0x10 != 0;
    const has_content_size = flg & 0x08 != 0;
    const has_content_checksum = flg & 0x04 != 0;
    if (flg & 0x01 != 0) return error.CorruptInput; // DictID: no dictionary to resolve it against.

    const desc_len: usize = 2 + @as(usize, if (has_content_size) 8 else 0);
    if (src.len - s < desc_len + 1) return error.CorruptInput;
    const content_size: ?u64 = if (has_content_size) std.mem.readInt(u64, src[s + 2 ..][0..8], .little) else null;
    const hc: u8 = @truncate(std.hash.XxHash32.hash(0, src[s..][0..desc_len]) >> 8);
    if (hc != src[s + desc_len]) return error.CorruptInput;
    s += desc_len + 1;

    var d = d_start;
    while (true) {
        if (src.len - s < 4) return error.CorruptInput;
        const word = std.mem.readInt(u32, src[s..][0..4], .little);
        s += 4;
        if (word == 0) break; // EndMark
        const stored = word & 0x8000_0000 != 0;
        const bsize: usize = word & 0x7FFF_FFFF;
        if (bsize > block_max or bsize > src.len - s) return error.CorruptInput;
        const block = src[s..][0..bsize];
        s += bsize;
        if (has_block_checksum) {
            if (src.len - s < 4) return error.CorruptInput;
            if (std.hash.XxHash32.hash(0, block) != std.mem.readInt(u32, src[s..][0..4], .little))
                return error.CorruptInput;
            s += 4;
        }
        const room = @min(dest.len - d, block_max);
        if (stored) {
            if (bsize > room) return error.OutputTooSmall;
            @memcpy(dest[d..][0..bsize], block);
            d += bsize;
        } else if (independent) {
            d += try uncompress(block, dest[d..][0..room]);
        } else {
            // Linked blocks may reach back into earlier blocks of this frame, never into a previous frame.
            d = d_start + try uncompressAt(block, dest[d_start .. d + room], d - d_start);
        }
    }
    if (has_content_checksum) {
        if (src.len - s < 4) return error.CorruptInput;
        if (std.hash.XxHash32.hash(0, dest[d_start..d]) != std.mem.readInt(u32, src[s..][0..4], .little))
            return error.CorruptInput;
        s += 4;
    }
    if (content_size) |cs| if (cs != d - d_start) return error.CorruptInput;
    s_idx.* = s;
    return d;
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

// ----- Legacy codec 5 containers -----

/// Append one Hadoop chunk (`[ulen BE][clen BE][block]`) holding `data`, compressed with our block encoder.
fn appendHadoopChunk(list: *std.ArrayList(u8), data: []const u8) !void {
    const table = try testing.allocator.alloc(u32, HASH_SIZE);
    defer testing.allocator.free(table);
    const block = try testing.allocator.alloc(u8, compressBound(data.len));
    defer testing.allocator.free(block);
    const clen = try compress(data, block, table);
    var prefix: [8]u8 = undefined;
    std.mem.writeInt(u32, prefix[0..4], @intCast(data.len), .big);
    std.mem.writeInt(u32, prefix[4..8], @intCast(clen), .big);
    try list.appendSlice(testing.allocator, &prefix);
    try list.appendSlice(testing.allocator, block[0..clen]);
}

const hadoop_parts = [_][]const u8{
    "the quick brown fox jumps over the quick brown fox jumps over the lazy dog",
    "ababababababababababababababababab",
    "z",
};

fn hadoopMultiChunk() !std.ArrayList(u8) {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(testing.allocator);
    for (hadoop_parts) |p| try appendHadoopChunk(&list, p);
    return list;
}

test "hadoop: multi-chunk page decodes in order" {
    var src = try hadoopMultiChunk();
    defer src.deinit(testing.allocator);
    const want = hadoop_parts[0] ++ hadoop_parts[1] ++ hadoop_parts[2];
    var out: [want.len]u8 = undefined;
    try uncompressHadoop(src.items, &out);
    try testing.expectEqualStrings(want, &out);
}

test "hadoop: zero-length chunks are valid framing" {
    var src: std.ArrayList(u8) = .empty;
    defer src.deinit(testing.allocator);
    try appendHadoopChunk(&src, "");
    try appendHadoopChunk(&src, "hello hello hello hello");
    // A bare [0][0] prefix: zero bytes in, zero bytes out.
    try src.appendSlice(testing.allocator, &@as([8]u8, @splat(0)));
    var out: [23]u8 = undefined;
    try uncompressHadoop(src.items, &out);
    try testing.expectEqualStrings("hello hello hello hello", &out);

    // Empty input fills only an empty page; prefixes alone never fill a non-empty one.
    try uncompressHadoop("", &[_]u8{});
    var one: [1]u8 = undefined;
    try testing.expectError(error.NotHadoop, uncompressHadoop(&@as([16]u8, @splat(0)), &one));
}

test "hadoop: truncated input is rejected at every cut" {
    var src = try hadoopMultiChunk();
    defer src.deinit(testing.allocator);
    const total = hadoop_parts[0].len + hadoop_parts[1].len + hadoop_parts[2].len;
    var out: [total]u8 = undefined;
    var cut: usize = 0;
    while (cut < src.items.len) : (cut += 1) {
        try testing.expectError(error.NotHadoop, uncompressHadoop(src.items[0..cut], &out));
    }
    // Trailing bytes too short to be a prefix are not Hadoop framing either.
    try src.appendSlice(testing.allocator, &.{ 0, 0, 0 });
    try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, &out));
}

test "hadoop: lying lengths are rejected" {
    const total = hadoop_parts[0].len + hadoop_parts[1].len + hadoop_parts[2].len;
    var out: [total]u8 = undefined;
    const Lie = struct { field: usize, value: u32 };
    const lies = [_]Lie{
        .{ .field = 0, .value = hadoop_parts[0].len + 1 }, // claims more than the block decodes to
        .{ .field = 0, .value = hadoop_parts[0].len - 1 }, // claims less
        .{ .field = 0, .value = 0xFFFF_FFFF }, // more than the page holds
        .{ .field = 4, .value = 0xFFFF_FFFF }, // block runs past the input
        .{ .field = 4, .value = 0 }, // block swallowed into the next prefix
        .{ .field = 4, .value = 3 }, // block cut short
    };
    for (lies) |lie| {
        var src = try hadoopMultiChunk();
        defer src.deinit(testing.allocator);
        std.mem.writeInt(u32, src.items[lie.field..][0..4], lie.value, .big);
        try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, &out));
    }

    // Every chunk honest, but they sum to less (or more) than the page header's size.
    var src = try hadoopMultiChunk();
    defer src.deinit(testing.allocator);
    var big: [total + 1]u8 = undefined;
    try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, &big));
    try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, out[0 .. total - 1]));
}

/// One Hadoop chunk the way BlockCompressorStream writes a single write() larger than its buffer: the total
/// uncompressed length once, then a `[clen BE][block]` piece per `pieces` entry, each compressed on its own.
fn appendHadoopMultiPiece(list: *std.ArrayList(u8), ulen: u32, pieces: []const []const u8) !void {
    const table = try testing.allocator.alloc(u32, HASH_SIZE);
    defer testing.allocator.free(table);
    var prefix: [4]u8 = undefined;
    std.mem.writeInt(u32, &prefix, ulen, .big);
    try list.appendSlice(testing.allocator, &prefix);
    for (pieces) |p| {
        const block = try testing.allocator.alloc(u8, compressBound(p.len));
        defer testing.allocator.free(block);
        const clen = try compress(p, block, table);
        std.mem.writeInt(u32, &prefix, @intCast(clen), .big);
        try list.appendSlice(testing.allocator, &prefix);
        try list.appendSlice(testing.allocator, block[0..clen]);
    }
}

test "hadoop: one length header over several blocks" {
    const want = hadoop_parts[0] ++ hadoop_parts[1] ++ hadoop_parts[2];
    var out: [want.len]u8 = undefined;
    {
        // [ulen][clen1][b1][clen2][b2][clen3][b3], then an ordinary single-block chunk after it.
        var src: std.ArrayList(u8) = .empty;
        defer src.deinit(testing.allocator);
        try appendHadoopMultiPiece(&src, hadoop_parts[0].len + hadoop_parts[1].len, hadoop_parts[0..2]);
        try appendHadoopChunk(&src, hadoop_parts[2]);
        try uncompressHadoop(src.items, &out);
        try testing.expectEqualStrings(want, &out);
    }
    {
        // A zero-length piece makes no progress but is still well-formed.
        var src: std.ArrayList(u8) = .empty;
        defer src.deinit(testing.allocator);
        try appendHadoopMultiPiece(&src, want.len, &.{ hadoop_parts[0], "", hadoop_parts[1], hadoop_parts[2] });
        @memset(&out, 0);
        try uncompressHadoop(src.items, &out);
        try testing.expectEqualStrings(want, &out);
    }
    {
        // A piece that decodes past the header's length.
        var src: std.ArrayList(u8) = .empty;
        defer src.deinit(testing.allocator);
        try appendHadoopMultiPiece(&src, want.len - 1, &hadoop_parts);
        try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, out[0 .. want.len - 1]));
        try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, &out));
    }
    {
        // Pieces that stop short of the header's length, and every truncation of a valid chunk.
        var src: std.ArrayList(u8) = .empty;
        defer src.deinit(testing.allocator);
        try appendHadoopMultiPiece(&src, want.len + 1, &hadoop_parts);
        var big: [want.len + 1]u8 = undefined;
        try testing.expectError(error.NotHadoop, uncompressHadoop(src.items, &big));

        src.clearRetainingCapacity();
        try appendHadoopMultiPiece(&src, want.len, &hadoop_parts);
        try uncompressHadoop(src.items, &out);
        var cut: usize = 0;
        while (cut < src.items.len) : (cut += 1) {
            try testing.expectError(error.NotHadoop, uncompressHadoop(src.items[0..cut], &out));
        }
    }
}

/// `hello hello hello hello parquet lz4 frame` from python-lz4 4.4.5 (liblz4 1.10): content size, block and
/// content checksums.
pub const frame_all_checks = [_]u8{
    0x04, 0x22, 0x4d, 0x18, 0x7c, 0x40, 0x29, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x47, 0x1c,
    0x00, 0x00, 0x00, 0x6e, 0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x06, 0x00, 0xf0, 0x02, 0x70, 0x61,
    0x72, 0x71, 0x75, 0x65, 0x74, 0x20, 0x6c, 0x7a, 0x34, 0x20, 0x66, 0x72, 0x61, 0x6d, 0x65, 0x92,
    0xdb, 0xb8, 0xa6, 0x00, 0x00, 0x00, 0x00, 0x02, 0xe1, 0x0e, 0xc6,
};
/// The same text, no optional fields.
pub const frame_plain = [_]u8{
    0x04, 0x22, 0x4d, 0x18, 0x60, 0x40, 0x82, 0x1c, 0x00, 0x00, 0x00, 0x6e, 0x68, 0x65, 0x6c, 0x6c,
    0x6f, 0x20, 0x06, 0x00, 0xf0, 0x02, 0x70, 0x61, 0x72, 0x71, 0x75, 0x65, 0x74, 0x20, 0x6c, 0x7a,
    0x34, 0x20, 0x66, 0x72, 0x61, 0x6d, 0x65, 0x00, 0x00, 0x00, 0x00,
};
const frame_text = "hello hello hello hello parquet lz4 frame";
/// Two linked blocks; the second is one match reaching back into the first. Hand-built, and checked to decode
/// to `frame_linked_text` with liblz4's LZ4F_decompress.
pub const frame_linked = [_]u8{
    0x04, 0x22, 0x4d, 0x18, 0x40, 0x40, 0xc0, 0x12, 0x00, 0x00, 0x00, 0xf0, 0x01, 0x61, 0x62, 0x63,
    0x64, 0x65, 0x66, 0x67, 0x68, 0x69, 0x6a, 0x6b, 0x6c, 0x6d, 0x6e, 0x6f, 0x70, 0x09, 0x00, 0x00,
    0x00, 0x0c, 0x10, 0x00, 0x50, 0x56, 0x57, 0x58, 0x59, 0x5a, 0x00, 0x00, 0x00, 0x00,
};
const frame_linked_text = "abcdefghijklmnopabcdefghijklmnopVWXYZ";

test "frame: canned liblz4 frames decode" {
    var out: [frame_text.len]u8 = undefined;
    try testing.expectEqual(frame_text.len, try uncompressFrame(&frame_all_checks, &out));
    try testing.expectEqualStrings(frame_text, &out);
    @memset(&out, 0);
    try testing.expectEqual(frame_text.len, try uncompressFrame(&frame_plain, &out));
    try testing.expectEqualStrings(frame_text, &out);
    try testing.expect(hasFrameMagic(&frame_plain));
}

test "frame: linked blocks reach back into the previous block" {
    var out: [frame_linked_text.len]u8 = undefined;
    try testing.expectEqual(out.len, try uncompressFrame(&frame_linked, &out));
    try testing.expectEqualStrings(frame_linked_text, &out);

    // The same blocks declared independent: the back-reference has no history and must fail, as in liblz4.
    var indep = frame_linked;
    indep[4] = 0x60;
    indep[6] = @truncate(std.hash.XxHash32.hash(0, indep[4..6]) >> 8);
    try testing.expectError(error.CorruptInput, uncompressFrame(&indep, &out));
}

test "frame: skippable and concatenated frames" {
    const skippable = [_]u8{ 0x53, 0x2a, 0x4d, 0x18, 0x04, 0x00, 0x00, 0x00, 's', 'k', 'i', 'p' };
    const src = skippable ++ frame_plain ++ frame_all_checks;
    try testing.expect(hasFrameMagic(&src));
    var out: [2 * frame_text.len]u8 = undefined;
    try testing.expectEqual(out.len, try uncompressFrame(&src, &out));
    try testing.expectEqualStrings(frame_text ++ frame_text, &out);
    // A skippable frame that claims more bytes than remain.
    var lying = skippable;
    lying[4] = 0x40;
    try testing.expectError(error.CorruptInput, uncompressFrame(&lying, &out));
}

test "frame: corrupt headers, checksums and sizes are rejected" {
    var out: [frame_text.len]u8 = undefined;
    const Flip = struct { at: usize, xor: u8 };
    const flips = [_]Flip{
        .{ .at = 4, .xor = 0x80 }, // version bits
        .{ .at = 4, .xor = 0x01 }, // DictID: nothing to resolve it against
        .{ .at = 5, .xor = 0x40 }, // block size id 0
        .{ .at = 6, .xor = 0x01 }, // header checksum
        .{ .at = 7, .xor = 0x01 }, // content size no longer matches
        .{ .at = 20, .xor = 0x01 }, // a literal: block checksum catches it
        .{ .at = 47, .xor = 0x01 }, // block checksum itself
        .{ .at = 55, .xor = 0x01 }, // content checksum
    };
    for (flips) |f| {
        var bad = frame_all_checks;
        bad[f.at] ^= f.xor;
        // Header-field flips also break the header checksum; re-seal so the field check itself is exercised.
        if (f.at == 4 or f.at == 5 or f.at == 7) {
            const desc_len: usize = if (bad[4] & 0x08 != 0) 10 else 2;
            bad[4 + desc_len] = @truncate(std.hash.XxHash32.hash(0, bad[4..][0..desc_len]) >> 8);
        }
        if (uncompressFrame(&bad, &out)) |_| {
            std.debug.print("flip at {d} accepted\n", .{f.at});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
    var cut: usize = 0;
    while (cut < frame_all_checks.len) : (cut += 1) {
        if (uncompressFrame(frame_all_checks[0..cut], &out)) |_| return error.TestUnexpectedResult else |_| {}
    }
    var small: [frame_text.len - 1]u8 = undefined;
    try testing.expectError(error.OutputTooSmall, uncompressFrame(&frame_plain, &small));
}
