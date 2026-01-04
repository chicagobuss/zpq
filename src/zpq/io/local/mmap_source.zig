const std = @import("std");
const io = @import("../interface.zig");

/// Implementation of RandomAccessSource using memory-mapped I/O (mmap).
/// This provides zero-copy access to local files and leverages the OS page cache.
pub const MmapSource = struct {
    ptr: []align(std.heap.page_size_min) const u8,
    file_size: u64,

    pub fn init(path: []const u8) !MmapSource {
        const file = try std.fs.cwd().openFile(path, .{});
        defer file.close();

        const stat = try file.stat();
        const size = stat.size;

        if (size == 0) {
            return MmapSource{
                .ptr = &[_]u8{},
                .file_size = 0,
            };
        }

        const ptr = try std.posix.mmap(
            null,
            size,
            std.posix.PROT.READ,
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );

        return MmapSource{
            .ptr = ptr,
            .file_size = size,
        };
    }

    pub fn deinit(self: *MmapSource) void {
        if (self.ptr.len > 0) {
            std.posix.munmap(self.ptr);
        }
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *MmapSource = @ptrCast(@alignCast(ptr));
        if (offset >= self.file_size) return 0;
        
        const available = self.file_size - offset;
        const to_read = @min(available, buf.len);
        @memcpy(buf[0..to_read], self.ptr[offset .. offset + to_read]);
        return to_read;
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *MmapSource = @ptrCast(@alignCast(ptr));
        return self.file_size;
    }

    fn closeImpl(ptr: *anyopaque) void {
        const self: *MmapSource = @ptrCast(@alignCast(ptr));
        self.deinit();
    }

    fn getSliceImpl(ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 {
        const self: *MmapSource = @ptrCast(@alignCast(ptr));
        if (offset + len > self.file_size) return null;
        return self.ptr[offset .. offset + len];
    }

    pub fn source(self: *MmapSource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = null, // Default fallback
                .size = sizeImpl,
                .close = closeImpl,
                .getSlice = getSliceImpl,
            },
        };
    }

    /// Hint to the OS about the expected access pattern for a range.
    pub fn advise(self: *const MmapSource, offset: u64, len: u64, advice: std.posix.MADV) !void {
        if (offset + len > self.file_size) return;
        const slice = self.ptr[offset .. offset + len];
        try std.posix.madvise(@constCast(slice.ptr), slice.len, advice);
    }
};

test "MmapSource basic read" {
    const testing = std.testing;

    const data = "Mmap Source Test Data - Zero Copy Fun!";
    const path = "test_mmap_source.tmp";
    try std.fs.cwd().writeFile(.{ .sub_path = path, .data = data });
    defer std.fs.cwd().deleteFile(path) catch {};

    var mmap_source = try MmapSource.init(path);
    defer mmap_source.deinit();

    const src = mmap_source.source();
    
    // Test size
    try testing.expectEqual(@as(u64, data.len), src.size());

    // Test readAt
    var buf: [64]u8 = undefined;
    const n = try src.readAt(0, buf[0..data.len]);
    try testing.expectEqual(data.len, n);
    try testing.expectEqualStrings(data, buf[0..n]);

    // Test getSlice (Zero Copy)
    const slice = src.getSlice(5, 6).?;
    try testing.expectEqualStrings("Source", slice);
    
    // Test slice identity - it should be pointing into the same memory
    try testing.expect(&mmap_source.ptr[5] == &slice[0]);
}
