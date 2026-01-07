const std = @import("std");

/// Range of bytes [start, end).
pub const Range = struct {
    start: u64,
    end: u64, // Exclusive

    pub fn len(self: Range) u64 {
        return self.end - self.start;
    }
};

/// Abstract interface for a readable source that supports random access.
/// This allows ZPQ to operate on local files, memory buffers, or remote object stores (S3).
///
/// Design goals:
/// 1. Stateless "read at offset" (better for object stores than stateful seek/read).
/// 2. Thread-safe (implementations must handle their own locking if needed).
/// 3. Zero-copy friendly (caller provides buffer).
pub const RandomAccessSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read exactly `len` bytes from `offset` into `buf`.
        /// Returns the number of bytes read.
        /// Should return error.EndOfStream if fewer bytes are available than requested.
        readAt: *const fn (ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize,

        /// Read multiple ranges into provided buffers.
        /// ranges[i] corresponds to buffers[i].
        /// Default implementation (if null) loops readAt.
        readRanges: ?*const fn (ptr: *anyopaque, ranges: []const Range, buffers: []const []u8) anyerror!void = null,

        /// Returns the total size of the source in bytes.
        size: *const fn (ptr: *anyopaque) u64,

        /// Closes the source and releases resources.
        /// Note: Some sources (like memory buffers) might be no-ops.
        close: *const fn (ptr: *anyopaque) void,

        /// Zero-copy slice access (optional).
        /// Returns a slice into the source's internal buffer if supported.
        /// Returns null if zero-copy is not supported (e.g., for file/network sources).
        /// This enables avoiding allocations for in-memory sources like MemorySource.
        getSlice: ?*const fn (ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 = null,
    };

    /// Read bytes from the specified offset into the buffer.
    /// Attempt to fill the entire buffer.
    pub fn readAt(self: RandomAccessSource, offset: u64, buf: []u8) !usize {
        return self.vtable.readAt(self.ptr, offset, buf);
    }

    /// Read multiple ranges in parallel (if supported).
    pub fn readRanges(self: RandomAccessSource, ranges: []const Range, buffers: []const []u8) !void {
        if (self.vtable.readRanges) |func| {
            return func(self.ptr, ranges, buffers);
        } else {
            // Default fallback: Sequential readAt
            if (ranges.len != buffers.len) return error.InvalidArgs;
            for (ranges, 0..) |range, i| {
                const buf = buffers[i];
                if (buf.len != range.len()) return error.InvalidArgs;
                const n = try self.readAt(range.start, buf);
                if (n != buf.len) return error.UnexpectedEndOfFile;
            }
        }
    }

    /// Get total size of the source.
    pub fn size(self: RandomAccessSource) u64 {
        return self.vtable.size(self.ptr);
    }

    /// Close the source.
    pub fn close(self: RandomAccessSource) void {
        self.vtable.close(self.ptr);
    }

    /// Get a zero-copy slice if supported by the underlying source.
    /// Returns null if zero-copy is not available.
    pub fn getSlice(self: RandomAccessSource, offset: u64, len: u64) ?[]const u8 {
        if (self.vtable.getSlice) |func| {
            return func(self.ptr, offset, len);
        }
        return null;
    }
};

pub const local = struct {
    pub const FileSource = @import("local/file_source.zig").LocalFileSource;
    pub const MemorySource = @import("local/memory_source.zig").MemorySource;
    pub const MmapSource = @import("local/mmap_source.zig").MmapSource;
};

test {
    _ = @import("local/file_source.zig");
    _ = @import("local/memory_source.zig");
    _ = @import("local/mmap_source.zig");
}
