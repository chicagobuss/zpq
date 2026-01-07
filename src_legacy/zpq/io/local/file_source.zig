const std = @import("std");
const io = @import("../interface.zig");

/// Implementation of RandomAccessSource for a local std.fs.File.
/// Uses `pread` (positioned read) to be stateless/thread-safe where supported.
pub const LocalFileSource = struct {
    file: std.fs.File,
    file_size: u64,

    pub fn init(path: []const u8) !LocalFileSource {
        const file = try std.fs.cwd().openFile(path, .{});
        const stat = try file.stat();
        return LocalFileSource{
            .file = file,
            .file_size = stat.size,
        };
    }

    pub fn deinit(self: *LocalFileSource) void {
        self.file.close();
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *LocalFileSource = @ptrCast(@alignCast(ptr));
        return self.file.pread(buf, offset);
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *LocalFileSource = @ptrCast(@alignCast(ptr));
        return self.file_size;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *LocalFileSource = @ptrCast(@alignCast(ptr));
        self.file.close();
    }

    pub fn source(self: *LocalFileSource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = null, // Use default fallback
                .size = sizeImpl,
                .close = closeImpl,
            },
        };
    }
};

test "LocalFileSource readAt" {
    const testing = std.testing;

    // Create a temporary file in current directory
    const data = "Hello, Random Access World!";
    const cwd = std.fs.cwd();
    try cwd.writeFile(.{ .sub_path = "test_local_source.tmp", .data = data });
    defer cwd.deleteFile("test_local_source.tmp") catch {};

    var local_source = try LocalFileSource.init("test_local_source.tmp");
    defer local_source.deinit();

    const source = local_source.source();

    // Test size
    try testing.expectEqual(@as(u64, data.len), source.size());

    // Test readAt (full)
    var buf: [30]u8 = undefined;
    const n = try source.readAt(0, buf[0..data.len]);
    try testing.expectEqual(data.len, n);
    try testing.expectEqualStrings(data, buf[0..n]);

    // Test readAt (partial/offset)
    const n2 = try source.readAt(7, buf[0..6]); // "Random"
    try testing.expectEqual(@as(usize, 6), n2);
    try testing.expectEqualStrings("Random", buf[0..n2]);

    // Test readAt (eof)
    const n3 = try source.readAt(100, buf[0..5]);
    try testing.expectEqual(@as(usize, 0), n3);

    // Test readRanges (fallback)
    var r1_buf: [5]u8 = undefined;
    var r2_buf: [6]u8 = undefined;
    const ranges = &[_]io.Range{
        .{ .start = 0, .end = 5 }, // "Hello"
        .{ .start = 7, .end = 13 }, // "Random"
    };
    const buffers = &[_][]u8{ &r1_buf, &r2_buf };

    try source.readRanges(ranges, buffers);
    try testing.expectEqualStrings("Hello", &r1_buf);
    try testing.expectEqualStrings("Random", &r2_buf);
}

