//! Codec dispatch for Parquet pages.
//!
//! Phase 1: UNCOMPRESSED + SNAPPY (in-tree decoder, src/core/parquet/snappy.zig).
//! Phase 2: ZSTD, GZIP, LZ4_RAW. Stdlib has zstd + flate decoders we'll
//! wrap, but they require an Io.Reader vtable — wiring is non-trivial,
//! and our benchmark fixture is pure SNAPPY, so we defer until a real
//! file demands them.

const std = @import("std");
const schema = @import("../schema.zig");
const snappy = @import("snappy.zig");

pub const Error = error{
    UnsupportedCodec,
    DecompressionFailed,
    SizeMismatch,
} || std.mem.Allocator.Error;

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
        else => return error.UnsupportedCodec,
    }
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
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .ZSTD, 1));
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .GZIP, 1));
    try testing.expectError(error.UnsupportedCodec, decompress(testing.allocator, "x", .LZ4_RAW, 1));
}
