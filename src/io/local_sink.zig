const std = @import("std");
const sink_mod = @import("sink.zig");

pub const AsyncFileSink = struct {
    file: std.fs.File,

    pub fn init(file: std.fs.File) AsyncFileSink {
        return .{ .file = file };
    }

    pub fn sink(self: *AsyncFileSink) sink_mod.Sink {
        return .{
            .ptr = self,
            .vtable = &.{
                .write = write,
                .close = close,
            },
        };
    }

    fn write(ptr: *anyopaque, data: []const u8) anyerror!usize {
        const self: *AsyncFileSink = @ptrCast(@alignCast(ptr));
        try self.file.writeAll(data);
        return data.len;
    }

    fn close(ptr: *anyopaque) anyerror!void {
        const self: *AsyncFileSink = @ptrCast(@alignCast(ptr));
        self.file.close();
    }
};
