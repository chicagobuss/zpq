//! Zig FFI bindings for vendor/snappy (google/snappy 1.2.1).

const c = @cImport({
    @cInclude("snappy-c.h");
});

pub const Status = enum(c_int) {
    ok = 0,
    invalid_input = 1,
    buffer_too_small = 2,
};

pub const Error = error{
    InvalidInput,
    OutputTooSmall,
};

/// Maximum compressed size for an input of `src_len` bytes.
pub fn maxCompressedLength(src_len: usize) usize {
    return c.snappy_max_compressed_length(src_len);
}

/// Compress `src` into `dest`. Returns the number of bytes written.
pub fn compress(src: []const u8, dest: []u8) Error!usize {
    var compressed_len: usize = dest.len;
    const status = c.snappy_compress(src.ptr, src.len, dest.ptr, &compressed_len);
    return switch (status) {
        @intFromEnum(Status.ok) => compressed_len,
        @intFromEnum(Status.buffer_too_small) => error.OutputTooSmall,
        else => error.InvalidInput,
    };
}

/// Decompress `src` into `dest`. Returns the number of bytes written.
pub fn uncompress(src: []const u8, dest: []u8) Error!usize {
    var uncompressed_len: usize = dest.len;
    const status = c.snappy_uncompress(src.ptr, src.len, dest.ptr, &uncompressed_len);
    return switch (status) {
        @intFromEnum(Status.ok) => uncompressed_len,
        @intFromEnum(Status.buffer_too_small) => error.OutputTooSmall,
        else => error.InvalidInput,
    };
}

/// Read the uncompressed length from a snappy block's header.
pub fn uncompressedLength(src: []const u8) Error!usize {
    var len: usize = 0;
    const status = c.snappy_uncompressed_length(src.ptr, src.len, &len);
    return switch (status) {
        @intFromEnum(Status.ok) => len,
        else => error.InvalidInput,
    };
}
