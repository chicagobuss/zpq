const std = @import("std");
const interface = @import("interface.zig");
const DataManager = @import("../core/data_manager.zig");

/// A RandomAccessSource backed by multiple in-memory chunks.
/// Used for workers to read pre-fetched data (which may be sparse).
pub const MemorySource = struct {
    chunks: []const DataManager.RowGroupData.Chunk,
    
    pub fn init(chunks: []const DataManager.RowGroupData.Chunk) MemorySource {
        return .{ .chunks = chunks };
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
        const end = offset + buffer.len;

        // Naive linear scan for relevant chunks (optimization: could sort/search, but usually few chunks)
        // Note: A single read might span multiple chunks IF they are contiguous. 
        // But for sparse coalescing, they are usually disjoint.
        // Let's assume reads don't span gaps. If they do, we err or partial read.
        
        var bytes_read: usize = 0;
        
        for (self.chunks) |chunk| {
            const chunk_end = chunk.base_offset + chunk.data.len;
            
            // Check intersection [offset, end) with [chunk_start, chunk_end)
            if (offset < chunk_end and end > chunk.base_offset) {
                // Determine intersection range in absolute coords
                const intersect_start = @max(offset, chunk.base_offset);
                const intersect_end = @min(end, chunk_end);
                
                // Copy
                const src_start = intersect_start - chunk.base_offset;
                const src_end = intersect_end - chunk.base_offset;
                const dst_start = intersect_start - offset;
                const dst_end = intersect_end - offset;
                
                @memcpy(buffer[dst_start..dst_end], chunk.data[src_start..src_end]);
                bytes_read += (dst_end - dst_start);
            }
        }
        
        return bytes_read;
    }
    
    fn readRanges(ptr: *anyopaque, ranges: []const interface.Range, buffers: []const []u8) anyerror!void {
        // self cast not strictly needed since we pass ptr to readAt
        // const self: *MemorySource = @ptrCast(@alignCast(ptr));
        
        for (ranges, buffers) |range, buf| {
            _ = try readAt(ptr, range.start, buf);
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
        _ = ptr;
        return std.math.maxInt(u64); // Virtual size
    }
    
    fn close(ptr: *anyopaque) void {
        _ = ptr;
        // No-op, memory is managed externally
    }

    fn getSlice(ptr: *anyopaque, offset: u64, len: u64) ?[]const u8 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        const end = offset + len;
        
        for (self.chunks) |chunk| {
            const chunk_end = chunk.base_offset + chunk.data.len;
            if (offset >= chunk.base_offset and end <= chunk_end) {
                const start_idx = offset - chunk.base_offset;
                return chunk.data[start_idx..][0..len];
            }
        }
        return null;
    }
};

test "MemorySource sparse chunks" {
    const allocator = std.testing.allocator;
    const Chunk = DataManager.RowGroupData.Chunk;
    
    const buf1 = try allocator.dupe(u8, "Hello");
    const buf2 = try allocator.dupe(u8, "World");
    defer allocator.free(buf1);
    defer allocator.free(buf2);
    
    var chunks = [_]Chunk{
        .{ .data = buf1, .base_offset = 0 },
        .{ .data = buf2, .base_offset = 100 },
    };
    
    var src = MemorySource.init(&chunks);
    var ras = src.randomAccessSource();
    
    var buf: [5]u8 = undefined;
    
    // Read Chunk 1
    _ = try ras.readAt(0, &buf);
    try std.testing.expectEqualStrings("Hello", &buf);
    
    // Read Chunk 2
    _ = try ras.readAt(100, &buf);
    try std.testing.expectEqualStrings("World", &buf);
    
    // Read gap (should be empty/unchanged if initialized)
    // Note: readAt doesn't zero buffer.
    @memset(&buf, 0);
    const n = try ras.readAt(50, &buf);
    try std.testing.expectEqual(@as(usize, 0), n);
}
