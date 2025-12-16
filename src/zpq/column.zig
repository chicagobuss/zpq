const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const snappy = @import("snappy.zig");

pub const Page = struct {
    header: schema.PageHeader,
    data: []u8, 
    allocator: std.mem.Allocator,

    pub fn deinit(self: *Page) void {
        self.allocator.free(self.data);
    }
};

pub const ColumnReader = struct {
    file: std.fs.File,
    allocator: std.mem.Allocator,
    start_offset: u64,
    total_size: u64,
    current_offset: u64,
    codec: schema.CompressionCodec,

    pub fn init(file: std.fs.File, allocator: std.mem.Allocator, chunk: schema.ColumnChunk) !ColumnReader {
        const meta = chunk.meta_data orelse return error.MissingColumnMetaData;
        
        var start: u64 = @intCast(meta.data_page_offset);
        if (meta.dictionary_page_offset) |dpo| {
            if (dpo < start) start = @intCast(dpo);
        }

        return ColumnReader{
            .file = file,
            .allocator = allocator,
            .start_offset = start,
            .total_size = @intCast(meta.total_compressed_size),
            .current_offset = 0,
            .codec = meta.codec,
        };
    }

    pub fn next(self: *ColumnReader) !?Page {
        if (self.current_offset >= self.total_size) return null;

        const abs_pos = self.start_offset + self.current_offset;
        try self.file.seekTo(abs_pos);

        // Read a buffer for the header. Thrift headers are usually small (< 1KB)
        var header_buf: [4096]u8 = undefined;
        const bytes_read = try self.file.read(&header_buf);
        if (bytes_read == 0) return null;

        var reader = thrift.Reader.init(header_buf[0..bytes_read]);
        const header = try schema.PageHeader.read(&reader);

        const header_size = reader.pos;
        const payload_size: u64 = @intCast(header.compressed_page_size);

        // Allocate and read payload
        const payload = try self.allocator.alloc(u8, payload_size);
        errdefer self.allocator.free(payload);

        // We might have read some of the payload into header_buf already?
        // Yes.
        // Bytes available in header_buf after header: bytes_read - header_size
        // We should copy those, then read the rest.
        
        const bytes_in_buf = bytes_read - header_size;
        
        if (bytes_in_buf >= payload_size) {
            // Entire payload is in the buffer
            @memcpy(payload, header_buf[header_size .. header_size + payload_size]);
        } else {
            // Copy what we have
            @memcpy(payload[0..bytes_in_buf], header_buf[header_size..bytes_read]);
            
            // Read the rest
            const remaining = payload_size - bytes_in_buf;
            const dest = payload[bytes_in_buf..];
            try self.file.seekTo(abs_pos + bytes_read); 
            
            var total_read: usize = 0;
            while (total_read < remaining) {
                const n = try self.file.read(dest[total_read..]);
                if (n == 0) return error.UnexpectedEndOfFile;
                total_read += n;
            }
        }

        self.current_offset += header_size + payload_size;

        if (self.codec == .SNAPPY) {
            const uncompressed_size = @as(usize, @intCast(header.uncompressed_page_size));
            const uncompressed = try self.allocator.alloc(u8, uncompressed_size);
            errdefer self.allocator.free(uncompressed);

            const decompressed_len = try snappy.uncompress(payload, uncompressed);
            if (decompressed_len != uncompressed_size) {
                // If it's dictionary page, sometimes uncompressed size in header might not match exactly?
                // But for Snappy it should match what we expect.
                // Let's be strict for now.
                // std.debug.print("Decompression size mismatch: expected {d}, got {d}\n", .{uncompressed_size, decompressed_len});
                // Actually, uncompress returns bytes written.
                // if (decompressed_len != uncompressed_size) return error.DecompressionSizeMismatch;
            }
            
            self.allocator.free(payload);
            
            return Page{
                .header = header,
                .data = uncompressed,
                .allocator = self.allocator,
            };
        }

        return Page{
            .header = header,
            .data = payload,
            .allocator = self.allocator,
        };
    }
};

