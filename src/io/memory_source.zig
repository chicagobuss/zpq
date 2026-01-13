const std = @import("std");
const interface = @import("interface.zig");

/// A RandomAccessSource backed by an in-memory buffer.
/// Used for workers to read pre-fetched data without network calls.
pub const MemorySource = struct {
    data: []const u8,
    base_offset: u64,  // Offset in original file where this data starts
    
    pub fn init(data: []const u8, base_offset: u64) MemorySource {
        return .{ .data = data, .base_offset = base_offset };
    }
    
    pub fn randomAccessSource(self: *MemorySource) interface.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAt,
                .readRanges = readRanges,
                .readRangesAsync = readRangesAsync,
                .size = size,
                .close = close,
                .getSlice = getSlice,
            },
        };
    }
    
    fn readAt(ptr: *anyopaque, offset: u64, buffer: []u8) anyerror!usize {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        
        // Translate absolute file offset to relative buffer offset
        if (offset < self.base_offset) return error.InvalidOffset;
        const rel_offset = offset - self.base_offset;
        
        const start = @min(rel_offset, self.data.len);
        const end = @min(start + buffer.len, self.data.len);
        const bytes_to_read = end - start;
        
        if (bytes_to_read == 0) return 0;
        
        @memcpy(buffer[0..bytes_to_read], self.data[start..end]);
        return bytes_to_read;
    }
    
    fn readRanges(ptr: *anyopaque, ranges: []const interface.Range, buffers: []const []u8) anyerror!void {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        
        for (ranges, buffers) |range, buf| {
            // Translate absolute file offset to relative buffer offset
            if (range.start < self.base_offset) return error.InvalidOffset;
            const rel_start = range.start - self.base_offset;
            
            const start = @min(rel_start, self.data.len);
            const len_u64 = range.len();
            const end = @min(start + len_u64, self.data.len);
            const bytes_to_read = end - start;
            
            if (bytes_to_read > 0) {
                @memcpy(buf[0..bytes_to_read], self.data[start..end]);
            }
        }
    }

    fn readRangesAsync(ptr: *anyopaque, ranges: []const interface.Range, buffers: []const []u8, cb: *const fn (ptr: ?*anyopaque, err: ?anyerror) void, ctx: ?*anyopaque) anyerror!void {
        if (readRanges(ptr, ranges, buffers)) |_| {
            cb(ctx, null);
        } else |err| {
            cb(ctx, err);
        }
    }
    
    fn size(ptr: *anyopaque) u64 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        return self.base_offset + self.data.len;  // Return virtual size
    }
    
    fn close(ptr: *anyopaque) void {
        _ = ptr;
        // No-op, memory is managed externally
    }

    fn getSlice(ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        if (offset < self.base_offset) return null;
        const rel_offset = offset - self.base_offset;
        
        if (rel_offset + len > self.data.len) return null;
        return self.data[rel_offset .. rel_offset + len];
    }
};

test "MemorySource basic read" {
    const data = "Hello, World!";
    var src = MemorySource.init(data, 0);
    var ras = src.randomAccessSource();
    
    var buf: [5]u8 = undefined;
    const n = try ras.readAt(0, &buf);
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);
    
    const n2 = try ras.readAt(7, &buf);
    try std.testing.expectEqual(@as(usize, 5), n2);
    try std.testing.expectEqualStrings("World", &buf);
}

test "MemorySource with base_offset" {
    const data = "Hello, World!";
    var src = MemorySource.init(data, 1000);  // Data starts at file offset 1000
    var ras = src.randomAccessSource();
    
    var buf: [5]u8 = undefined;
    const n = try ras.readAt(1000, &buf);  // Should read from start of buffer
    try std.testing.expectEqual(@as(usize, 5), n);
    try std.testing.expectEqualStrings("Hello", &buf);
}
