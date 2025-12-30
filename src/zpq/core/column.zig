const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const snappy = @import("snappy.zig");
const zstd = @import("zstd.zig");
const io = @import("../io/interface.zig");
const log = @import("../log.zig").core;

pub const Page = struct {
    header: schema.PageHeader,
    data: []const u8,
    /// If true, data is a borrowed slice (zero-copy) and should NOT be freed.
    /// If false, data is owned and must be freed via allocator.
    borrowed: bool = false,

    pub fn deinit(self: *Page, allocator: std.mem.Allocator) void {
        if (!self.borrowed) {
            allocator.free(@constCast(self.data));
        }
    }
};

pub const ColumnReader = struct {
    source: io.RandomAccessSource,
    start_offset: u64,
    total_size: u64,
    current_offset: u64,
    codec: schema.CompressionCodec,
    /// Reusable buffer for decompression (avoids per-page allocation for compressed data)
    decompression_buffer: ?[]u8 = null,
    decompression_buffer_allocator: ?std.mem.Allocator = null,

    pub fn init(source: io.RandomAccessSource, chunk: schema.ColumnChunk) !ColumnReader {
        const meta = chunk.meta_data orelse return error.MissingColumnMetaData;

        var start: u64 = @intCast(meta.data_page_offset);
        if (meta.dictionary_page_offset) |dpo| {
            if (dpo < start) start = @intCast(dpo);
        }

        return ColumnReader{
            .source = source,
            .start_offset = start,
            .total_size = @intCast(meta.total_compressed_size),
            .current_offset = 0,
            .codec = meta.codec,
        };
    }

    pub fn deinit(self: *ColumnReader) void {
        if (self.decompression_buffer) |buf| {
            if (self.decompression_buffer_allocator) |alloc| {
                alloc.free(buf);
            }
        }
        self.decompression_buffer = null;
        self.decompression_buffer_allocator = null;
    }

    /// Ensure decompression buffer is at least `size` bytes.
    /// Reuses existing buffer if large enough, otherwise reallocates.
    fn ensureDecompressionBuffer(self: *ColumnReader, allocator: std.mem.Allocator, size: usize) ![]u8 {
        if (self.decompression_buffer) |buf| {
            if (buf.len >= size) {
                return buf[0..size];
            }
            // Need larger buffer, free old one
            if (self.decompression_buffer_allocator) |alloc| {
                alloc.free(buf);
            }
        }
        // Allocate new buffer (with some headroom to avoid frequent reallocations)
        const alloc_size = @max(size, size + size / 4); // 25% headroom
        const new_buf = try allocator.alloc(u8, alloc_size);
        self.decompression_buffer = new_buf;
        self.decompression_buffer_allocator = allocator;
        return new_buf[0..size];
    }

    pub fn next(self: *ColumnReader, allocator: std.mem.Allocator) !?Page {
        if (self.current_offset >= self.total_size) return null;

        const abs_pos = self.start_offset + self.current_offset;

        // Read a buffer for the header. Thrift headers are usually small (< 1KB)
        var header_buf: [4096]u8 = undefined;
        // Limit read to remaining column size
        const bytes_to_read = @min(header_buf.len, self.total_size - self.current_offset);

        const bytes_read = try self.source.readAt(abs_pos, header_buf[0..bytes_to_read]);
        if (bytes_read == 0) return null;

        var reader = thrift.Reader.init(header_buf[0..bytes_read]);
        const header = try schema.PageHeader.read(&reader);

        const header_size = reader.pos;
        const payload_size: u64 = @intCast(header.compressed_page_size);
        const payload_offset = abs_pos + header_size;

        self.current_offset += header_size + payload_size;

        // For uncompressed data, try zero-copy first
        if (self.codec == .UNCOMPRESSED) {
            // Try zero-copy slice from source
            if (self.source.getSlice(payload_offset, payload_size)) |slice| {
                return Page{
                    .header = header,
                    .data = slice,
                    .borrowed = true,
                };
            }
        }

        // Fallback: allocate and read payload
        const payload = try allocator.alloc(u8, payload_size);
        errdefer allocator.free(payload);

        // Bytes available in header_buf after header: bytes_read - header_size
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
            const read_offset = abs_pos + bytes_read;

            var total_read: usize = 0;
            while (total_read < remaining) {
                const n = try self.source.readAt(read_offset + total_read, dest[total_read..]);
                if (n == 0) return error.UnexpectedEndOfFile;
                total_read += n;
            }
        }

        if (self.codec == .SNAPPY) {
            const uncompressed_size = @as(usize, @intCast(header.uncompressed_page_size));

            // Use reusable decompression buffer instead of allocating each time
            const uncompressed = try self.ensureDecompressionBuffer(allocator, uncompressed_size);

            const decompressed_len = try snappy.uncompress(payload, uncompressed);
            if (decompressed_len != uncompressed_size) {
                // std.debug.print("Decompression size mismatch: expected {d}, got {d}\n", .{uncompressed_size, decompressed_len});
            }

            // Free the compressed payload (we allocated it above)
            allocator.free(payload);

            // The uncompressed data is in the reusable buffer - we need to copy it out
            // because the buffer will be reused for the next page
            const result = try allocator.alloc(u8, uncompressed_size);
            @memcpy(result, uncompressed);

            return Page{
                .header = header,
                .data = result,
                .borrowed = false,
            };
        }

        if (self.codec == .ZSTD) {
            const uncompressed_size = @as(usize, @intCast(header.uncompressed_page_size));

            const result = try allocator.alloc(u8, uncompressed_size);
            errdefer allocator.free(result);

            const decompressed_len = try zstd.decompress(payload, result);
            _ = decompressed_len;

            // Free the compressed payload
            allocator.free(payload);

            return Page{
                .header = header,
                .data = result,
                .borrowed = false,
            };
        }

        return Page{
            .header = header,
            .data = payload,
            .borrowed = false,
        };
    }
};
