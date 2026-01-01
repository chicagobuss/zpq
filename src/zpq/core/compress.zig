//! Compression utilities for Parquet write path
//!
//! Supports GZIP compression using std.compress.flate.
//! ZSTD decompression is handled by zstd.zig.

const std = @import("std");
const flate = std.compress.flate;

pub const CompressError = error{
    CompressionFailed,
    OutOfMemory,
};

/// Compress data using GZIP format
/// Returns owned slice that caller must free
pub fn compressGzip(allocator: std.mem.Allocator, src: []const u8) CompressError![]u8 {
    // Use Allocating writer for compressed output - must have initial capacity > 8
    // for the Compress vtable to work properly
    const initial_capacity = @max(src.len + 128, 256);
    var output: std.Io.Writer.Allocating = std.Io.Writer.Allocating.initCapacity(allocator, initial_capacity) catch return error.OutOfMemory;
    errdefer output.deinit();

    // Allocate history buffer (required by flate) - needs to be 2x max_window_len
    const history_buffer = allocator.alloc(u8, flate.max_window_len * 2) catch return error.OutOfMemory;
    defer allocator.free(history_buffer);

    // Initialize compressor with GZIP container
    // Compress returns a struct with a .writer field that we write data TO
    var compressor = flate.Compress.init(
        &output.writer,
        history_buffer,
        .gzip,
        .default,
    ) catch return error.CompressionFailed;

    // Write input data to the compressor's writer
    compressor.writer.writeAll(src) catch return error.CompressionFailed;

    // Flush to finalize compression
    compressor.writer.flush() catch return error.CompressionFailed;

    // Get the compressed bytes from output
    return output.toOwnedSlice() catch return error.OutOfMemory;
}

/// Compress data using raw DEFLATE (no header/footer)
pub fn compressDeflate(allocator: std.mem.Allocator, src: []const u8) CompressError![]u8 {
    var output: std.Io.Writer.Allocating = .init(allocator);
    errdefer output.deinit();

    const history_buffer = allocator.alloc(u8, flate.max_window_len * 2) catch return error.OutOfMemory;
    defer allocator.free(history_buffer);

    // Use raw deflate (no gzip header/footer)
    var compressor = flate.Compress.init(
        &output.writer,
        history_buffer,
        .raw,
        .default,
    ) catch return error.CompressionFailed;

    compressor.writer.writeAll(src) catch return error.CompressionFailed;
    compressor.writer.flush() catch return error.CompressionFailed;

    return output.toOwnedSlice() catch return error.OutOfMemory;
}

test "gzip roundtrip" {
    const allocator = std.testing.allocator;

    const input = "Hello, World! This is a test of GZIP compression. " ++
        "Adding more text to make it more compressible. " ++
        "The quick brown fox jumps over the lazy dog. " ++
        "Pack my box with five dozen liquor jugs.";

    const compressed = try compressGzip(allocator, input);
    defer allocator.free(compressed);

    // Compressed should exist
    try std.testing.expect(compressed.len > 0);

    // Decompress and verify
    var input_reader: std.Io.Reader = .fixed(compressed);
    var output_writer: std.Io.Writer.Allocating = .init(allocator);
    defer output_writer.deinit();

    var decompressor = flate.Decompress.init(&input_reader, .gzip, &.{});
    _ = decompressor.reader.streamRemaining(&output_writer.writer) catch |err| {
        std.debug.print("Decompression error: {}\n", .{err});
        return err;
    };

    const result = output_writer.toOwnedSlice() catch return error.OutOfMemory;
    defer allocator.free(result);

    try std.testing.expectEqualStrings(input, result);
}
