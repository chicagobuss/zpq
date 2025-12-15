const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");

pub const ParquetFile = struct {
    file: std.fs.File,
    footer_len: u32,
    file_size: u64,
    metadata: ?schema.FileMetaData = null,
    footer_buffer: []u8 = &[_]u8{},
    allocator: std.mem.Allocator,

    pub fn open(allocator: std.mem.Allocator, path: []const u8) !ParquetFile {
        const file = try std.fs.cwd().openFile(path, .{});
        const stat = try file.stat();
        const file_size = stat.size;

        if (file_size < 8) {
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .file = file,
            .footer_len = 0,
            .file_size = file_size,
            .allocator = allocator,
        };
    }

    pub fn close(self: *ParquetFile) void {
        self.file.close();
    }

    pub fn readFooter(self: *ParquetFile) !void {
        // 1. Seek to end - 4 bytes to read magic "PAR1"
        try self.file.seekFromEnd(-4);
        var magic_buf: [4]u8 = undefined;
        _ = try self.file.read(&magic_buf);

        if (!std.mem.eql(u8, &magic_buf, "PAR1")) {
            return error.InvalidMagicBytes;
        }

        // 2. Seek to end - 8 bytes to read footer length (4 bytes)
        try self.file.seekFromEnd(-8);
        var len_buf: [4]u8 = undefined;
        _ = try self.file.read(&len_buf);

        // Parquet is Little Endian for this length
        self.footer_len = std.mem.readInt(u32, &len_buf, .little);
        
        // 3. Read Thrift Metadata
        const footer_start = self.file_size - 8 - self.footer_len;
        try self.file.seekTo(footer_start);
        
        // Allocate buffer for footer and keep it
        self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
        
        const bytes_read = try self.file.read(self.footer_buffer);
        if (bytes_read != self.footer_buffer.len) return error.UnexpectedEndOfFile;
        
        var reader = thrift.Reader.init(self.footer_buffer);
        self.metadata = try schema.FileMetaData.read(self.allocator, &reader);
        
        // Strings in metadata point to self.footer_buffer, which is safe as long as ParquetFile is alive
    }
    
    pub fn deinit(self: *ParquetFile) void {
        if (self.metadata) |*m| {
            m.deinit(self.allocator);
        }
        if (self.footer_buffer.len > 0) {
            self.allocator.free(self.footer_buffer);
        }
        self.close();
    }
};
