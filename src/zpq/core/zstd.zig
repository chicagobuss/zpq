//! ZSTD decompression using std.compress.zstd

const std = @import("std");

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

    // Initialize decompressor with empty internal buffer (uses direct_vtable)
    // This avoids the window_len + block_size_max buffer requirement
    var decompressor = std.compress.zstd.Decompress.init(&input, &.{}, .{});

    // Stream all decompressed data to output
    const bytes_written = decompressor.reader.streamRemaining(&output) catch |err| switch (err) {
        error.ReadFailed => return error.ReadFailed,
        error.WriteFailed => return error.WriteFailed,
    };

    return bytes_written;
}
