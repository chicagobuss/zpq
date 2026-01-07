const std = @import("std");
const thrift = @import("thrift.zig");
const schema = @import("schema.zig");
const io = @import("../io/interface.zig");

pub const ParquetFile = struct {
    allocator: std.mem.Allocator,
    source: io.RandomAccessSource,

    /// The parsed file metadata containing schema and row group info.
    /// This is allocated in metadata_arena.
    metadata: schema.FileMetaData = undefined,

    /// Arena used for all metadata-related allocations.
    metadata_arena: std.heap.ArenaAllocator,

    /// The raw footer buffer (Thrift encoded FileMetaData).
    footer_buffer: []u8 = &[_]u8{},
    footer_buffer_owned: bool = false,

    file_size: u64 = 0,
    footer_len: u32 = 0,

    pub fn init(allocator: std.mem.Allocator, source: io.RandomAccessSource) ParquetFile {
        return .{
            .allocator = allocator,
            .source = source,
            .metadata_arena = std.heap.ArenaAllocator.init(allocator),
        };
    }

    pub fn deinit(self: *ParquetFile) void {
        if (self.footer_buffer_owned) {
            self.allocator.free(self.footer_buffer);
        }
        self.metadata_arena.deinit();
    }

    /// Reads and parses the Parquet footer.
    /// Implements adaptive prefetching to minimize I/O roundtrips.
    pub fn readFooter(self: *ParquetFile) !void {
        self.file_size = self.source.size();
        if (self.file_size < 8) return error.InvalidFile;

        // Adaptive prefetch: try to read the tail of the file in one go.
        // For typical files, 16KB-256KB is enough to cover the footer.
        const MIN_PREFETCH = 16 * 1024;
        const MAX_PREFETCH = 256 * 1024;
        const adaptive_size = self.file_size / 256;
        const clamped_size = @max(MIN_PREFETCH, @min(adaptive_size, MAX_PREFETCH));
        const fetch_len = @min(self.file_size, clamped_size);
        const fetch_start = self.file_size - fetch_len;

        const prefetch_buf = try self.allocator.alloc(u8, fetch_len);
        defer self.allocator.free(prefetch_buf);

        const n = try self.source.readAt(fetch_start, prefetch_buf);
        if (n != fetch_len) return error.UnexpectedEndOfFile;

        // Parquet files end with 4 bytes of magic "PAR1"
        if (!std.mem.eql(u8, prefetch_buf[fetch_len - 4 ..], "PAR1")) {
            return error.InvalidMagicBytes;
        }

        // Footer length is the 4 bytes preceding the magic
        self.footer_len = std.mem.readInt(u32, prefetch_buf[fetch_len - 8 .. fetch_len - 4][0..4], .little);

        if (self.footer_len > self.file_size - 8) {
            return error.InvalidFooterLength;
        }

        const footer_start = self.file_size - 8 - self.footer_len;

        // Optimization: If the footer was already in our prefetch buffer, just copy it.
        if (self.footer_len + 8 <= fetch_len) {
            const start_in_buf = fetch_len - 8 - self.footer_len;
            const footer_slice = prefetch_buf[start_in_buf .. fetch_len - 8];

            self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
            @memcpy(self.footer_buffer, footer_slice);
            self.footer_buffer_owned = true;
        } else {
            // Otherwise, issue a targeted read for the exact footer size.
            // Try zero-copy first if the source supports it.
            if (self.source.getSlice(footer_start, self.footer_len)) |slice| {
                self.footer_buffer = @constCast(slice);
                self.footer_buffer_owned = false;
            } else {
                self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
                const bytes_read = try self.source.readAt(footer_start, self.footer_buffer);
                if (bytes_read != self.footer_len) return error.UnexpectedEndOfFile;
                self.footer_buffer_owned = true;
            }
        }

        // Parse the Thrift FileMetaData using the dedicated metadata arena.
        var reader = thrift.Reader.init(self.footer_buffer);
        self.metadata = try schema.FileMetaData.read(self.metadata_arena.allocator(), &reader);
    }

    pub fn numRowGroups(self: *const ParquetFile) usize {
        return self.metadata.row_groups.items.len;
    }

    pub fn numRows(self: *const ParquetFile) i64 {
        return self.metadata.num_rows;
    }
};

test "ParquetFile footer reading" {
    _ = std.testing;
    _ = @import("../io/local.zig");

    // This test assumes a valid parquet file exists or is mocked.
    // For now, we'll verify the logic with a small mock footer if possible,
    // but usually, we'd use a real test artifact.
}
