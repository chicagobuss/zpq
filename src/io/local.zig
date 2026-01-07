const std = @import("std");
const io = @import("interface.zig");

pub const AsyncFileSource = struct {
    allocator: std.mem.Allocator,
    file: std.fs.File,
    size_bytes: u64,

    pub fn init(allocator: std.mem.Allocator, file: std.fs.File) !AsyncFileSource {
        const stat = try file.stat();
        return .{
            .allocator = allocator,
            .file = file,
            .size_bytes = stat.size,
        };
    }

    pub fn deinit(self: *AsyncFileSource) void {
        self.file.close();
    }

    pub fn randomAccessSource(self: *AsyncFileSource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAt,
                .size = size,
                .close = close,
            },
        };
    }

    fn readAt(ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        const n = try self.file.pread(buf, offset);
        return n;
    }

    fn size(ptr: *anyopaque) u64 {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        return self.size_bytes;
    }

    fn close(ptr: *anyopaque) void {
        const self: *AsyncFileSource = @ptrCast(@alignCast(ptr));
        const allocator = self.allocator;
        self.file.close();
        allocator.destroy(self);
    }
};
