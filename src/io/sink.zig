const std = @import("std");

/// Abstract interface for writing output data.
/// Supports sequential writes and closing/flushing.
pub const Sink = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        write: *const fn (ctx: *anyopaque, data: []const u8) anyerror!usize,
        close: *const fn (ctx: *anyopaque) anyerror!void,
    };

    /// Write data to the sink.
    /// Returns the number of bytes accepted.
    /// Note: Implementation may buffer writes.
    pub fn write(self: Sink, data: []const u8) !usize {
        return self.vtable.write(self.ptr, data);
    }

    /// Close the sink, flushing any buffered data.
    /// For S3, this triggers the completion of the multipart upload.
    pub fn close(self: Sink) !void {
        return self.vtable.close(self.ptr);
    }
};
