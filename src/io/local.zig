const std = @import("std");
const io = @import("interface.zig");

pub const AsyncFileSource = struct {
    allocator: std.mem.Allocator,
    fd: std.posix.fd_t,
    size_bytes: u64,

    pub fn init(allocator: std.mem.Allocator, fd: std.posix.fd_t) !AsyncFileSource {
        var st: std.os.linux.Statx = undefined;
        // statx(fd, path, flags, mask, buffer)
        const ret = std.os.linux.statx(fd, "", std.os.linux.AT.EMPTY_PATH, std.os.linux.STATX{ .SIZE = true }, &st);
        switch (std.posix.errno(ret)) {
            .SUCCESS => {},
            else => |err| return std.posix.unexpectedErrno(err),
        }
        
        return .{
            .allocator = allocator,
            .fd = fd,
            .size_bytes = st.size,
        };
    }
    
    pub fn deinit(self: *AsyncFileSource) void {
        std.posix.close(self.fd);
    }

    pub fn randomAccessSource(self: *AsyncFileSource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAt,
                .readRanges = readRanges,
                .readRangesAsync = readRangesAsync,
                .size = size,
                .close = close,
            },
        };
    }

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        const ret = std.os.linux.pread(self.fd, buf.ptr, buf.len, @intCast(offset));
        const n = switch (std.posix.errno(ret)) {
            .SUCCESS => ret,
            else => |err| return std.posix.unexpectedErrno(err),
        };
        return n;
    }

    fn readRanges(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8) anyerror!void {
        for (ranges, 0..) |r, i| {
            _ = try readAt(ptr, r.start, buffers[i]);
        }
    }

    fn readRangesAsync(ptr: *anyopaque, ranges: []const io.Range, buffers: []const []u8, cb: *const fn (ptr: ?*anyopaque, err: ?anyerror) void, ctx: ?*anyopaque) anyerror!void {
        if (readRanges(ptr, ranges, buffers)) |_| {
            cb(ctx, null);
        } else |err| {
            cb(ctx, err);
        }
    }

    fn size(ptr: *anyopaque) u64 {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        return self.size_bytes;
    }

    fn close(ptr: *anyopaque) void {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        const allocator = self.allocator;
        std.posix.close(self.fd);
        allocator.destroy(self);
    }
};
