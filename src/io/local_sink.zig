const std = @import("std");
const sink_mod = @import("sink.zig");

pub const AsyncFileSink = struct {
    fd: std.posix.fd_t,

    pub fn init(fd: std.posix.fd_t) AsyncFileSink {
        return .{ .fd = fd };
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
        var index: usize = 0;
        while (index < data.len) {
            const rc = std.os.linux.write(self.fd, data.ptr + index, data.len - index);
             const n = switch (std.posix.errno(rc)) {
                .SUCCESS => rc,
                .INTR => continue,
                else => |err| return std.posix.unexpectedErrno(err),
            };
            if (n == 0) return error.DiskQuota;
            index += n;
        }
        return data.len;
    }

    fn close(ptr: *anyopaque) anyerror!void {
        const self: *AsyncFileSink = @ptrCast(@alignCast(ptr));
        std.posix.close(self.fd);
    }
};
