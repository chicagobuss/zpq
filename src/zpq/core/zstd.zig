//! ZSTD compression/decompression
//!
//! Decompression: Always available using std.compress.zstd (pure Zig)
//! Compression: Only available when built with -Dzstd-compression=true (links libzstd)

const std = @import("std");
const zpq_options = @import("zpq_options");

/// Whether ZSTD compression is available in this build
pub const compression_enabled = zpq_options.enable_zstd_compression;

// ============================================================================
// Decompression (always available - pure Zig)
// ============================================================================

pub const DecompressError = error{
    ReadFailed,
    WriteFailed,
    OutputTooSmall,
};

/// Decompress ZSTD data into a pre-allocated buffer.
/// src: Compressed data
/// dest: Pre-allocated buffer for uncompressed data (must be sized to uncompressed size)
/// Returns: Number of bytes written to dest
pub fn decompress(src: []const u8, dest: []u8) DecompressError!usize {
    // Create input reader from compressed source
    var input: std.Io.Reader = .fixed(src);

    // Create output writer to destination buffer
    var output: std.Io.Writer = .fixed(dest);

    // Initialize decompressor with empty internal buffer
    var decompressor = std.compress.zstd.Decompress.init(&input, &.{}, .{});

    // Stream all decompressed data to output
    const bytes_written = decompressor.reader.streamRemaining(&output) catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.WriteFailed => return error.WriteFailed,
        else => |e| return e,
    };

    return bytes_written;
}

// ============================================================================
// Compression (optional - requires libzstd)
// ============================================================================

pub const CompressError = error{
    OutOfMemory,
    CompressionFailed,
};

/// Compress data using ZSTD.
/// Only available when built with -Dzstd-compression=true
/// Returns owned slice that caller must free.
pub fn compressAlloc(allocator: std.mem.Allocator, src: []const u8) CompressError![]u8 {
    if (comptime !compression_enabled) {
        @compileError("ZSTD compression not enabled. Build with -Dzstd-compression=true");
    }

    // Get the maximum compressed size bound
    const max_size = c.ZSTD_compressBound(src.len);
    if (c.ZSTD_isError(max_size) != 0) {
        return error.CompressionFailed;
    }

    // Allocate output buffer
    const dest = allocator.alloc(u8, max_size) catch return error.OutOfMemory;
    errdefer allocator.free(dest);

    // Compress
    const compressed_size = c.ZSTD_compress(
        dest.ptr,
        dest.len,
        src.ptr,
        src.len,
        3, // Default compression level
    );

    if (c.ZSTD_isError(compressed_size) != 0) {
        allocator.free(dest);
        return error.CompressionFailed;
    }

    // Shrink to actual size
    if (compressed_size < max_size) {
        return allocator.realloc(dest, compressed_size) catch dest[0..compressed_size];
    }
    return dest[0..compressed_size];
}

/// Check if ZSTD compression is available at runtime
pub fn isCompressionAvailable() bool {
    return compression_enabled;
}

// C bindings for libzstd (only used when compression is enabled)
const c = if (compression_enabled) struct {
    extern fn ZSTD_compressBound(srcSize: usize) usize;
    extern fn ZSTD_compress(dst: [*]u8, dstCapacity: usize, src: [*]const u8, srcSize: usize, compressionLevel: c_int) usize;
    extern fn ZSTD_isError(code: usize) c_uint;
} else struct {};
