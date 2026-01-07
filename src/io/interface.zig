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
/// This follows the completion-based (async-agnostic) spirit of Zig 0.16.x.
///
/// Implementations can be synchronous (like local files/mmap) or asynchronous
/// (like S3 via libxev), but the interface remains consistent.
pub const RandomAccessSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read exactly `buf.len` bytes from `offset` into `buf`.
        /// In a fully completion-based future, this might take a callback.
        /// For now, we provide a synchronous-looking interface that can be
        /// backed by libxev's event loop.
        readAt: *const fn (ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize,

        /// Read multiple ranges into provided buffers.
        /// This is the primary hook for scatter-gather I/O (io_uring).
        readRanges: ?*const fn (ptr: *anyopaque, ranges: []const Range, buffers: []const []u8) anyerror!void = null,

        /// Returns the total size of the source in bytes.
        size: *const fn (ptr: *anyopaque) u64,

        /// Closes the source and releases resources.
        close: *const fn (ptr: *anyopaque) void,

        /// Zero-copy slice access (optional).
        /// Returns a slice into the source's internal memory (e.g. mmap or ring buffer).
        /// This enables the "Double-Buffer" copy elimination.
        getSlice: ?*const fn (ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 = null,
    };

    pub fn readAt(self: RandomAccessSource, offset: u64, buf: []u8) !usize {
        return self.vtable.readAt(self.ptr, offset, buf);
    }

    pub fn readRanges(self: RandomAccessSource, ranges: []const Range, buffers: []const []u8) !void {
        if (self.vtable.readRanges) |func| {
            return func(self.ptr, ranges, buffers);
        }
        // Fallback: Sequential readAt
        if (ranges.len != buffers.len) return error.InvalidArgs;
        for (ranges, 0..) |range, i| {
            const buf = buffers[i];
            if (buf.len != range.len()) return error.InvalidArgs;
            const n = try self.readAt(range.start, buf);
            if (n != buf.len) return error.UnexpectedEndOfFile;
        }
    }

    pub fn size(self: RandomAccessSource) u64 {
        return self.vtable.size(self.ptr);
    }

    pub fn close(self: RandomAccessSource) void {
        self.vtable.close(self.ptr);
    }

    pub fn getSlice(self: RandomAccessSource, offset: u64, len: u64) ?[]const u8 {
        if (self.vtable.getSlice) |func| {
            return func(self.ptr, offset, len);
        }
        return null;
    }
};

/// BufferLender is the "Push-Model" interface for the Parquet engine.
/// Instead of the engine pulling bytes, the transport "lends" buffers to the engine.
pub const BufferLender = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Provide a chunk of data to the consumer.
        /// The consumer must process it or copy it before returning,
        /// as the buffer may be reused.
        onChunk: *const fn (ptr: *anyopaque, chunk: []const u8) anyerror!void,
    };

    pub fn onChunk(self: BufferLender, chunk: []const u8) !void {
        return self.vtable.onChunk(self.ptr, chunk);
    }
};
