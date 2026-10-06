//! Codec dispatch for Parquet pages.
//!
//! UNCOMPRESSED, SNAPPY and the LZ4s are handled in-tree; ZSTD uses libzstd
//! and GZIP the Zig stdlib. The deprecated LZ4 codec (5) is read, never written:
//! the spec replaced it with LZ4_RAW, which is what `--codec lz4` writes.

const std = @import("std");
const schema = @import("../schema.zig");
const snappy = @import("snappy.zig");
const lz4 = @import("lz4.zig");

const c_zstd = @import("zstd_c");

pub const Error = error{
    UnsupportedCodec,
    DecompressionFailed,
    CompressionFailed,
    SizeMismatch,
} || std.mem.Allocator.Error;

/// Default zstd compression level. 3 is the upstream default — fast
/// encode (~400 MB/s on modern x86_64) with compression ratio close
/// to gzip-6. Higher levels (up to 22) compress better but are
/// slower; for our streaming-output use case the network is rarely
/// the bottleneck so we prefer the fast knob.
pub const ZSTD_DEFAULT_LEVEL: c_int = 3;

/// Compress a buffer using `codec`. Returns a newly-allocated slice
/// owned by `arena`. UNCOMPRESSED is a no-op identity copy onto the
/// arena (so the caller can free with the same allocator regardless
/// of codec).
pub fn compress(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
) Error![]u8 {
    return switch (codec) {
        .UNCOMPRESSED => try arena.dupe(u8, src),
        .SNAPPY => snappy.compressAlloc(arena, src) catch return error.CompressionFailed,
        .ZSTD => compressZstd(arena, src),
        .GZIP => compressFlate(arena, src, .gzip),
        .LZ4_RAW => compressLz4Raw(arena, src),
        else => error.UnsupportedCodec,
    };
}

fn compressLz4Raw(arena: std.mem.Allocator, src: []const u8) Error![]u8 {
    const table = try arena.alloc(u32, lz4.HASH_SIZE);
    defer arena.free(table);
    const dest = try arena.alloc(u8, lz4.compressBound(src.len));
    errdefer arena.free(dest);
    const n = lz4.compress(src, dest, table) catch return error.CompressionFailed;
    return dest[0..n];
}

/// Compress `src` to a single gzip member via stdlib deflate. Mirrors the
/// decode path's `decompressFlate`; output lands in an arena-backed growing
/// writer (deflate can briefly exceed the input on incompressible data, so a
/// fixed worst-case buffer is fiddly — the allocating writer just grows).
fn compressFlate(
    arena: std.mem.Allocator,
    src: []const u8,
    container: std.compress.flate.Container,
) Error![]u8 {
    // Allocating output: Compress.init asserts the output buffer is > 8, so
    // seed a small capacity that the writer grows as needed.
    var out = std.Io.Writer.Allocating.initCapacity(arena, @max(64, src.len / 2)) catch
        return error.CompressionFailed;
    // The compressor's history window (asserted >= max_window_len = 64 KB).
    const window = try arena.alloc(u8, std.compress.flate.max_window_len);
    defer arena.free(window);

    var c = std.compress.flate.Compress.init(&out.writer, window, container, .default) catch
        return error.CompressionFailed;
    c.writer.writeAll(src) catch return error.CompressionFailed;
    c.finish() catch return error.CompressionFailed;
    return out.writer.buffered();
}

fn compressZstd(arena: std.mem.Allocator, src: []const u8) Error![]u8 {
    const max_len = c_zstd.ZSTD_compressBound(src.len);
    if (c_zstd.ZSTD_isError(max_len) != 0) return error.CompressionFailed;
    const buf = try arena.alloc(u8, max_len);
    errdefer arena.free(buf);
    const n = c_zstd.ZSTD_compress(
        buf.ptr,
        buf.len,
        src.ptr,
        src.len,
        ZSTD_DEFAULT_LEVEL,
    );
    if (c_zstd.ZSTD_isError(n) != 0) return error.CompressionFailed;
    return buf[0..n];
}

/// Decompress a single Parquet page's payload.
///
/// `src` is the on-disk compressed bytes (its length matches
/// `compressed_page_size` in the page header).
/// `uncompressed_size` is what the page header says the output should be.
/// `arena` is the per-row-group allocator the decompressed bytes live in
/// for codecs that produce new bytes; UNCOMPRESSED returns `src` directly
/// without allocating.
///
/// Caller's lifetime contract: returned bytes are valid for at least
/// the lifetime of `arena` AND `src` (whichever is shorter). In practice
/// both have the row-group lifetime, so it doesn't matter at the call site.
pub fn decompress(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
    uncompressed_size: usize,
) Error![]const u8 {
    if (uncompressed_size == 0) return src[0..0];
    if (codec == .UNCOMPRESSED) {
        // Sanity check: header should agree with the slice length.
        if (src.len != uncompressed_size) return error.SizeMismatch;
        return src;
    }
    const out = try arena.alloc(u8, uncompressed_size);
    errdefer arena.free(out);
    try decompressInto(arena, src, codec, out);
    return out;
}

/// `decompress` into a caller-owned buffer of exactly the uncompressed size, so callers can reuse one buffer
/// across pages. UNCOMPRESSED copies. `arena` is only touched for codec working state (the flate window).
pub fn decompressInto(
    arena: std.mem.Allocator,
    src: []const u8,
    codec: schema.CompressionCodec,
    out: []u8,
) Error!void {
    // An empty page (0 values) decompresses to nothing regardless of codec.
    // Several codecs' decoders choke on a zero-length input (snappy reads a
    // varint length header first); short-circuit before dispatch. See
    // datapage_v2_empty_datapage.snappy.parquet.
    if (out.len == 0) return;

    switch (codec) {
        .UNCOMPRESSED => {
            if (src.len != out.len) return error.SizeMismatch;
            @memcpy(out, src);
        },
        .SNAPPY => {
            const n = snappy.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != out.len) return error.SizeMismatch;
        },
        .GZIP => try decompressFlate(arena, src, out, .gzip),
        .ZSTD => try decompressZstd(src, out),
        .LZ4_RAW => {
            const n = lz4.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != out.len) return error.SizeMismatch;
        },
        .LZ4 => try decompressLz4Legacy(src, out),
        else => return error.UnsupportedCodec,
    }
}

/// Codec 5 carries no marker for which of its historical containers a page uses, so try them in Arrow C++'s
/// order (ARROW-9177): Hadoop framing (parquet-mr) only if every length validates, else a bare block (older
/// parquet-cpp); then, as arrow-rs also does, an LZ4 frame (older arrow-rs) when its magic announces one. A bare
/// block cannot parse a standard frame (its first token matches at offset 0x4D22 into empty output), so trying
/// the block first costs nothing. Every attempt writes `out` from the start, so the one that succeeds overwrites a failed one's bytes.
fn decompressLz4Legacy(src: []const u8, out: []u8) Error!void {
    if (lz4.uncompressHadoop(src, out)) |_| return else |_| {}
    if (lz4.uncompress(src, out)) |n| {
        if (n == out.len) return;
    } else |_| {}
    if (lz4.hasFrameMagic(src)) {
        const n = lz4.uncompressFrame(src, out) catch return error.DecompressionFailed;
        if (n != out.len) return error.SizeMismatch;
        return;
    }
    return error.DecompressionFailed;
}

fn decompressFlate(
    arena: std.mem.Allocator,
    src: []const u8,
    out: []u8,
    container: std.compress.flate.Container,
) Error!void {
    const uncompressed_size = out.len;
    var input = std.Io.Reader.fixed(src);
    // The flate decoder asserts buffer.len >= max_window_len (64 KB).
    // Arena-allocated; lives until the row-group arena resets.
    const window = try arena.alloc(u8, std.compress.flate.max_window_len);
    defer arena.free(window);

    // gzip/zlib permit concatenated members: several independently-framed
    // streams back-to-back that together produce `uncompressed_size` bytes
    // (see concatenated_gzip_members.parquet). std's Decompress stops at the
    // first member's footer (state `.end` → EndOfStream), so we loop, re-
    // initialising a fresh decoder per member. This is safe because the
    // decoder advances `input.seek` precisely (it peeks then tosses exactly
    // the bytes it consumes), leaving `seek` at the next member's first byte.
    // Single-member streams take one iteration and exit.
    var written: usize = 0;
    while (written < uncompressed_size and input.seek < input.end) {
        var d = std.compress.flate.Decompress.init(&input, container, window);
        const n = d.reader.readSliceShort(out[written..]) catch return error.DecompressionFailed;
        if (n == 0) break;
        written += n;
    }
    if (written != uncompressed_size) return error.DecompressionFailed;
}

fn decompressZstd(src: []const u8, out: []u8) Error!void {
    // Use libzstd's `ZSTD_decompress` directly. Zig's stdlib zstd
    // decoder is correct but ~10× slower than libzstd on the
    // dict-encoded numeric column-chunks Parquet writers produce —
    // 92% of CLI aggregate time was in `std.compress.zstd` per perf
    // profile (2026-05-06). libzstd is already linked for the write
    // path; same library, different entry point.
    const n = c_zstd.ZSTD_decompress(
        @ptrCast(out.ptr),
        out.len,
        @ptrCast(src.ptr),
        src.len,
    );
    if (c_zstd.ZSTD_isError(n) != 0) return error.DecompressionFailed;
    if (n != out.len) return error.SizeMismatch;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "UNCOMPRESSED passthrough" {
    const src = "hello world";
    const out = try decompress(testing.allocator, src, .UNCOMPRESSED, src.len);
    try testing.expectEqualStrings(src, out);
}

test "UNCOMPRESSED size mismatch" {
    const src = "hello";
    try testing.expectError(error.SizeMismatch, decompress(testing.allocator, src, .UNCOMPRESSED, 99));
}

test "SNAPPY round-trip" {
    const input = "Hello, World! Hello, World! Hello, World!";
    var compressed: [128]u8 = undefined;
    const comp_len = try snappy.compress(input, &compressed);

    const out = try decompress(testing.allocator, compressed[0..comp_len], .SNAPPY, input.len);
    defer testing.allocator.free(out);
    try testing.expectEqualStrings(input, out);
}

test "unsupported codec" {
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .BROTLI, 1));
    try testing.expectError(error.UnsupportedCodec, compress(testing.allocator, "x", .LZ4)); // read-only codec
}

test "GZIP decompresses canned bytes" {
    // gzip of "hello" — generated by `python3 -c "import gzip,sys;
    // sys.stdout.buffer.write(gzip.compress(b'hello'))"`.
    const compressed = [_]u8{
        0x1f, 0x8b, 0x08, 0x00, 0xb6, 0xdf, 0xf7, 0x69, 0x02, 0xff,
        0xcb, 0x48, 0xcd, 0xc9, 0xc9, 0x07, 0x00, 0x86, 0xa6, 0x10,
        0x36, 0x05, 0x00, 0x00, 0x00,
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const out = try decompress(arena.allocator(), &compressed, .GZIP, 5);
    try testing.expectEqualStrings("hello", out);
}

test "ZSTD decompresses canned bytes" {
    // zstd of "hello world" — generated by `printf 'hello world' | zstd`.
    const compressed = [_]u8{
        0x28, 0xb5, 0x2f, 0xfd, 0x04, 0x58, 0x59, 0x00, 0x00,
        0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x20, 0x77, 0x6f, 0x72, 0x6c, 0x64,
        0x68, 0x69, 0x1e, 0xb2,
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const out = try decompress(arena.allocator(), &compressed, .ZSTD, 11);
    try testing.expectEqualStrings("hello world", out);
}

test "LZ4 (codec 5): Hadoop framing, bare block and frame all decode" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const text = "hello hello hello hello parquet lz4 frame";

    const block = try compress(a, text, .LZ4_RAW);
    const hadoop = try a.alloc(u8, 8 + block.len);
    std.mem.writeInt(u32, hadoop[0..4], text.len, .big);
    std.mem.writeInt(u32, hadoop[4..8], @intCast(block.len), .big);
    @memcpy(hadoop[8..], block);
    const frame = lz4.frame_plain; // python-lz4 `lz4.frame.compress(text, store_size=False)`
    for ([_][]const u8{ hadoop, block, &frame }) |src| {
        try testing.expectEqualStrings(text, try decompress(a, src, .LZ4, text.len));
    }

    // A page header whose size disagrees with the payload is an error, whichever container it was.
    for ([_][]const u8{ hadoop, block }) |src| {
        try testing.expectError(error.DecompressionFailed, decompress(a, src, .LZ4, text.len + 1));
    }
    try testing.expectError(error.SizeMismatch, decompress(a, &frame, .LZ4, text.len + 1));
    try testing.expectError(error.DecompressionFailed, decompress(a, "\x00\x00\x00\x00garbage!", .LZ4, 4));
}

test "LZ4 (codec 5): a Hadoop chunk split over several blocks decodes" {
    // BlockCompressorStream given one write() larger than its 256 KiB buffer writes the total length once, then
    // a `[clen][block]` piece per buffer-sized slice (MAX_INPUT_SIZE = 262144 - 262144/255 - 16).
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const raw = try a.alloc(u8, 600_000);
    var prng = std.Random.DefaultPrng.init(0x1a4);
    for (raw, 0..) |*b, i| b.* = if (i % 7 == 0) prng.random().int(u8) else @truncate(i / 64);

    var page: std.ArrayList(u8) = .empty;
    var word: [4]u8 = undefined;
    std.mem.writeInt(u32, &word, @intCast(raw.len), .big);
    try page.appendSlice(a, &word);
    const max_input = 262144 - 262144 / 255 - 16;
    var off: usize = 0;
    var pieces: usize = 0;
    while (off < raw.len) : (pieces += 1) {
        const n = @min(max_input, raw.len - off);
        const block = try compress(a, raw[off..][0..n], .LZ4_RAW);
        std.mem.writeInt(u32, &word, @intCast(block.len), .big);
        try page.appendSlice(a, &word);
        try page.appendSlice(a, block);
        off += n;
    }
    try testing.expectEqual(@as(usize, 3), pieces);
    try testing.expectEqualSlices(u8, raw, try decompress(a, page.items, .LZ4, raw.len));
}
