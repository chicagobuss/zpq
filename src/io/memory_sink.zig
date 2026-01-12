const std = @import("std");
const sink_interface = @import("sink.zig");

pub const MemorySink = struct {
    allocator: std.mem.Allocator,
    data: std.ArrayListUnmanaged(u8),

    pub fn init(allocator: std.mem.Allocator) MemorySink {
        return .{
            .allocator = allocator,
            .data = .{},
        };
    }

    pub fn deinit(self: *MemorySink) void {
        self.data.deinit(self.allocator);
    }

    pub fn sink(self: *MemorySink) sink_interface.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .write = write,
                .close = close,
            },
        };
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        const self: *MemorySink = @ptrCast(@alignCast(ptr));
        try self.data.appendSlice(self.allocator, data);
        return data.len;
    }

    fn close(_: *anyopaque) anyerror!void {
        // No-op for memory sink
    }
};
