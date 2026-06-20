//! Codec dispatch for Parquet pages.
//!
//! UNCOMPRESSED and SNAPPY are handled in-tree; ZSTD and GZIP use the
//! Zig stdlib; LZ4_RAW uses the local Parquet block decoder. Legacy
//! Hadoop LZ4 framing is intentionally rejected.

const std = @import("std");
const schema = @import("../schema.zig");
const snappy = @import("snappy.zig");
const lz4 = @import("lz4.zig");

const c_zstd = @cImport({
    @cInclude("zstd.h");
});

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
    // An empty page (0 values) decompresses to nothing regardless of codec.
    // Several codecs' decoders choke on a zero-length input (snappy reads a
    // varint length header first); short-circuit before dispatch. See
    // datapage_v2_empty_datapage.snappy.parquet.
    if (uncompressed_size == 0) return src[0..0];

    switch (codec) {
        .UNCOMPRESSED => {
            // Sanity check: header should agree with the slice length.
            if (src.len != uncompressed_size) return error.SizeMismatch;
            return src;
        },
        .SNAPPY => {
            const out = try arena.alloc(u8, uncompressed_size);
            errdefer arena.free(out);
            const n = snappy.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != uncompressed_size) return error.SizeMismatch;
            return out[0..n];
        },
        .GZIP => return decompressFlate(arena, src, uncompressed_size, .gzip),
        .ZSTD => return decompressZstd(arena, src, uncompressed_size),
        .LZ4_RAW => {
            const out = try arena.alloc(u8, uncompressed_size);
            errdefer arena.free(out);
            const n = lz4.uncompress(src, out) catch return error.DecompressionFailed;
            if (n != uncompressed_size) return error.SizeMismatch;
            return out[0..n];
        },
        else => return error.UnsupportedCodec,
    }
}

fn decompressFlate(
    arena: std.mem.Allocator,
    src: []const u8,
    uncompressed_size: usize,
    container: std.compress.flate.Container,
) Error![]const u8 {
    const out = try arena.alloc(u8, uncompressed_size);
    errdefer arena.free(out);

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
    return out;
}

fn decompressZstd(
    arena: std.mem.Allocator,
    src: []const u8,
    uncompressed_size: usize,
) Error![]const u8 {
    const out = try arena.alloc(u8, uncompressed_size);
    errdefer arena.free(out);

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
    if (n != uncompressed_size) return error.SizeMismatch;
    return out;
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
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .LZ4, 1)); // legacy LZ4 (Hadoop framing) intentionally skipped
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
