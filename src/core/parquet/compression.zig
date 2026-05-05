//! Codec dispatch for Parquet pages.
//!
//! Phase 1: UNCOMPRESSED + SNAPPY (in-tree decoder, src/core/parquet/snappy.zig).
//! Phase 2: ZSTD + GZIP via stdlib (std.compress.zstd / std.compress.flate),
//! wrapped with std.Io.Reader.fixed(...) over the input slice.
//! LZ4_RAW lands separately (in-tree decoder, src/core/parquet/lz4.zig).

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
        else => error.UnsupportedCodec,
    };
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

    var d = std.compress.flate.Decompress.init(&input, container, window);
    d.reader.readSliceAll(out) catch return error.DecompressionFailed;
    return out;
}

fn decompressZstd(
    arena: std.mem.Allocator,
    src: []const u8,
    uncompressed_size: usize,
) Error![]const u8 {
    const out = try arena.alloc(u8, uncompressed_size);
    errdefer arena.free(out);

    var input = std.Io.Reader.fixed(src);
    // The zstd decoder asserts buffer.len >= window_len + block_size_max.
    const opts: std.compress.zstd.Decompress.Options = .{};
    const window = try arena.alloc(u8, opts.window_len + std.compress.zstd.block_size_max);
    defer arena.free(window);

    var d = std.compress.zstd.Decompress.init(&input, window, opts);
    d.reader.readSliceAll(out) catch return error.DecompressionFailed;
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
