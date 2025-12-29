const std = @import("std");

/// Range of bytes [start, end).
pub const Range = struct {
    start: u64,
    end: u64, // Exclusive

    pub fn len(self: Range) u64 {
        return self.end - self.start;
    }
};

/// Abstract interface for a readable source that supports random access.
/// This allows ZPQ to operate on local files, memory buffers, or remote object stores (S3).
///
/// Design goals:
/// 1. Stateless "read at offset" (better for object stores than stateful seek/read).
/// 2. Thread-safe (implementations must handle their own locking if needed).
/// 3. Zero-copy friendly (caller provides buffer).
pub const RandomAccessSource = struct {
    ptr: *anyopaque,
    vtable: *const VTable,

    pub const VTable = struct {
        /// Read exactly `len` bytes from `offset` into `buf`.
        /// Returns the number of bytes read. 
        /// Should return error.EndOfStream if fewer bytes are available than requested.
        readAt: *const fn (ptr: *anyopaque, offset: u64, buf: []u8) anyerror!usize,
        
        /// Read multiple ranges into provided buffers.
        /// ranges[i] corresponds to buffers[i].
        /// Default implementation (if null) loops readAt.
        readRanges: ?*const fn (ptr: *anyopaque, ranges: []const Range, buffers: []const []u8) anyerror!void = null,

        /// Returns the total size of the source in bytes.
        size: *const fn (ptr: *anyopaque) u64,
        
        /// Closes the source and releases resources.
        /// Note: Some sources (like memory buffers) might be no-ops.
        close: *const fn (ptr: *anyopaque) void,
    };

    /// Read bytes from the specified offset into the buffer.
    /// Attempt to fill the entire buffer.
    pub fn readAt(self: RandomAccessSource, offset: u64, buf: []u8) !usize {
        return self.vtable.readAt(self.ptr, offset, buf);
    }

    /// Read multiple ranges in parallel (if supported).
    pub fn readRanges(self: RandomAccessSource, ranges: []const Range, buffers: []const []u8) !void {
        if (self.vtable.readRanges) |func| {
            return func(self.ptr, ranges, buffers);
        } else {
            // Default fallback: Sequential readAt
            if (ranges.len != buffers.len) return error.InvalidArgs;
            for (ranges, 0..) |range, i| {
                const buf = buffers[i];
                if (buf.len != range.len()) return error.InvalidArgs;
                const n = try self.readAt(range.start, buf);
                if (n != buf.len) return error.UnexpectedEndOfFile;
            }
        }
    }

    /// Get total size of the source.
    pub fn size(self: RandomAccessSource) u64 {
        return self.vtable.size(self.ptr);
    }

    /// Close the source.
    pub fn close(self: RandomAccessSource) void {
        self.vtable.close(self.ptr);
    }
};

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

    pub fn source(self: *LocalFileSource) RandomAccessSource {
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
        @memcpy(buf[0..available], self.data[relative..relative+available]);
        return available;
    }

    fn sizeImpl(ptr: *anyopaque) u64 {
        const self: *MemorySource = @ptrCast(@alignCast(ptr));
        return self.base_offset + self.data.len;
    }

    fn closeImpl(ptr: *anyopaque) void {
        _ = ptr;
    }

    pub fn source(self: *MemorySource) RandomAccessSource {
        return .{
            .ptr = self,
            .vtable = &.{
                .readAt = readAtImpl,
                .readRanges = null, 
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
    const ranges = &[_]Range{
        .{ .start = 0, .end = 5 }, // "Hello"
        .{ .start = 7, .end = 13 }, // "Random"
    };
    const buffers = &[_][]u8{ &r1_buf, &r2_buf };
    
    try source.readRanges(ranges, buffers);
    try testing.expectEqualStrings("Hello", &r1_buf);
    try testing.expectEqualStrings("Random", &r2_buf);
}

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
