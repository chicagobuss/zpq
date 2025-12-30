const std = @import("std");
const io = @import("../interface.zig");

/// Implementation of RandomAccessSource for an in-memory buffer.
/// Useful for testing and for reading pre-fetched column chunks.
pub const MemorySource = struct {
    data: []const u8,
    base_offset: u64 = 0,

    pub fn init(data: []const u8) MemorySource {
        return MemorySource{ .data = data, .base_offset = 0 };
    }

    pub fn initWithOffset(data: []const u8, base_offset: u64) MemorySource {
        return MemorySource{ .data = data, .base_offset = base_offset };
    }

    fn readAtImpl(ptr: *anyopaque, offset: u64, buf: []u8) !usize {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        if (offset < self.base_offset) return 0;
        const relative = offset - self.base_offset;

        if (relative >= self.data.len) return 0;

        const end = @min(relative + buf.len, self.data.len);
        const available = end - relative;
        @memcpy(buf[0..available], self.data[relative .. relative + available]);
        return available;
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        return self.base_offset + self.data.len;
    }

    fn closeImpl(ptr: *anyopaque) void {
        _ = ptr;
    }

    fn getSliceImpl(ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        if (offset < self.base_offset) return null;
        const relative = offset - self.base_offset;

        if (relative >= self.data.len) return null;
        if (relative + len > self.data.len) return null;

        return self.data[relative..][0..len];
    }

    pub fn source(self: *MemorySource) io.RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = null,
                .size = sizeImpl,
                .close = closeImpl,
                .getSlice = getSliceImpl,
            },
        };
    }
};

test "MemorySource" {
    const testing = std.testing;
    const data = "Hello, Memory World!";
    var mem_source = MemorySource.init(data);
    const source = mem_source.source();

    try testing.expectEqual(@as(u64, data.len), source.size());

    var buf: [10]u8 = undefined;
    const n = try source.readAt(7, buf[0..6]); // "Memory"
    try testing.expectEqual(@as(usize, 6), n);
    try testing.expectEqualStrings("Memory", buf[0..n]);
}

test "MemorySource Offset" {
    const testing = std.testing;
    const data = "Offset World";
    var mem_source = MemorySource.initWithOffset(data, 100);
    const source = mem_source.source();

    // Size should include offset
    try testing.expectEqual(@as(u64, 100 + data.len), source.size());

    var buf: [10]u8 = undefined;
    // Read at 100 should be data[0]
    const n = try source.readAt(100, buf[0..6]);
    try testing.expectEqual(@as(usize, 6), n);
    try testing.expectEqualStrings("Offset", buf[0..n]);

    // Read before offset should be empty
    const n2 = try source.readAt(50, buf[0..5]);
    try testing.expectEqual(@as(usize, 0), n2);
}

test "MemorySource getSlice" {
    const testing = std.testing;
    const data = "Hello, Memory World!";
    var mem_source = MemorySource.init(data);
    const source = mem_source.source();

    // Zero-copy slice at offset 7, length 6 = "Memory"
    const slice = source.getSlice(7, 6);
    try testing.expect(slice != null);
    try testing.expectEqualStrings("Memory", slice.?);

    // Verify it's actually a slice into the original data (zero-copy)
    try testing.expectEqual(@intFromPtr(data.ptr + 7), @intFromPtr(slice.?.ptr));

    // Out of bounds should return null
    try testing.expect(source.getSlice(100, 5) == null);
    try testing.expect(source.getSlice(15, 10) == null); // Would exceed end
}

test "MemorySource getSlice with offset" {
    const testing = std.testing;
    const data = "Offset World";
    var mem_source = MemorySource.initWithOffset(data, 100);
    const source = mem_source.source();

    // Slice at absolute offset 100 = "Offset"
    const slice = source.getSlice(100, 6);
    try testing.expect(slice != null);
    try testing.expectEqualStrings("Offset", slice.?);

    // Before base offset should return null
    try testing.expect(source.getSlice(50, 5) == null);
}
