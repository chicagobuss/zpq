const std = @import("std");

/// The generic IO interface expected by the pure Parquet core.
/// We use comptime duck-typing (traits) to avoid virtual dispatch overhead,
/// allowing the compiler to fully inline S3/File reads.
///
/// Any Reader type `T` must implement:
///   pub fn readAt(self: *T, offset: u64, dest: []u8) anyerror!usize
///   pub fn size(self: *const T) u64
pub fn assertIsIOStrategy(comptime T: type) void {
    if (!@hasDecl(T, "readAt")) {
        @compileError("IOStrategy requires a 'readAt' function");
    }
    if (!@hasDecl(T, "size")) {
        @compileError("IOStrategy requires a 'size' function");
    }
}

/// A simple in-memory reader for testing the core logic without any real IO
pub const MemoryReader = struct {
    data: []const u8,

    pub fn init(data: []const u8) MemoryReader {
        return .{ .data = data };
    }

    pub fn readAt(self: *MemoryReader, offset: u64, dest: []u8) anyerror!usize {
        if (offset >= self.data.len) return 0;
        const available = self.data.len - offset;
        const to_copy = @min(available, dest.len);
        @memcpy(dest[0..to_copy], self.data[offset .. offset + to_copy]);
        return to_copy;
    }

    pub fn size(self: *const MemoryReader) u64 {
        return self.data.len;
    }
};

comptime {
    assertIsIOStrategy(MemoryReader);
}
