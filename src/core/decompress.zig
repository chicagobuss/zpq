const std = @import("std");
const schema = @import("schema.zig");
const zpq_options = @import("zpq_options");

const snappy = @import("snappy.zig");

pub const DecompressError = error{
    ReadFailed,
    WriteFailed,
    OutputTooSmall,
    UnsupportedCodec,
    DecompressionFailed,
} || snappy.Error;

/// Decompress data from src to dest based on the provided codec.
/// dest must be sized exactly to the uncompressed size.
pub fn decompress(codec: schema.CompressionCodec, src: []const u8, dest: []u8) DecompressError!usize {
    return switch (codec) {
        .UNCOMPRESSED => {
            if (dest.len < src.len) return error.OutputTooSmall;
            @memcpy(dest[0..src.len], src);
            return src.len;
        },
        .ZSTD => decompressZstd(src, dest),
        .SNAPPY => decompressSnappy(src, dest),
        else => error.UnsupportedCodec,
    };
}

fn decompressZstd(src: []const u8, dest: []u8) DecompressError!usize {
    // Create input reader from compressed source using new std.Io API
    var input: std.Io.Reader = .fixed(src);

    // Create output writer to destination buffer
    var output: std.Io.Writer = .fixed(dest);

    // Initialize decompressor with empty internal buffer
    var decompressor = std.compress.zstd.Decompress.init(&input, &.{}, .{});

    // Stream all decompressed data to output
    const bytes_written = decompressor.reader.streamRemaining(&output) catch |err| {
        std.debug.print("Zstd decompression error: {s}\n", .{@errorName(err)});
        return error.DecompressionFailed;
    };

    return bytes_written;
}

fn decompressSnappy(src: []const u8, dest: []u8) DecompressError!usize {
    return snappy.uncompress(src, dest);
}
