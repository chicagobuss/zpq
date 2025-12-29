const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const io = @import("../io/interface.zig");
const ColumnReader = @import("column.zig").ColumnReader;

pub const RowGroupReader = struct {
    file: *ParquetFile,
    meta: schema.RowGroup,
    allocator: std.mem.Allocator,

    // Sparse array of memory sources (indexed by column index)
    // If null, the column is not pre-fetched.
    memory_sources: []?io.MemorySource,

    // Buffers backing the memory sources.
    // We own these and must free them.
    buffers: std.ArrayListUnmanaged([]u8),

    pub fn init(file: *ParquetFile, meta: schema.RowGroup, allocator: std.mem.Allocator) !RowGroupReader {
        const sources = try allocator.alloc(?io.MemorySource, meta.columns.items.len);
        @memset(sources, null);

        return RowGroupReader{
            .file = file,
            .meta = meta,
            .allocator = allocator,
            .memory_sources = sources,
            .buffers = .{},
        };
    }

    pub fn deinit(self: *RowGroupReader) void {
        for (self.buffers.items) |buf| {
            self.allocator.free(buf);
        }
        self.buffers.deinit(self.allocator);
        self.allocator.free(self.memory_sources);
    }

    /// Pre-fetch specific columns (or all if indices is null)
    pub fn prefetch(self: *RowGroupReader, indices: ?[]const usize) !void {
        // If indices is null, we need to generate all column indices
        var all_indices: ?[]usize = null;
        defer if (all_indices) |a| self.allocator.free(a);

        const targets: []const usize = if (indices) |i| i else blk: {
            all_indices = try self.allocator.alloc(usize, self.meta.columns.items.len);
            for (0..self.meta.columns.items.len) |i| all_indices.?[i] = i;
            break :blk all_indices.?;
        };

        var ranges = try std.ArrayList(io.Range).initCapacity(self.allocator, targets.len);
        defer ranges.deinit(self.allocator);

        var buffers = try std.ArrayList([]u8).initCapacity(self.allocator, targets.len);
        defer buffers.deinit(self.allocator); // We only free the list, not the contents (which move to self.buffers)

        // Identify ranges and allocate buffers
        for (targets) |idx| {
            if (idx >= self.meta.columns.items.len) return error.InvalidColumnIndex;
            if (self.memory_sources[idx] != null) continue; // Already fetched

            const chunk = self.meta.columns.items[idx];
            const meta = chunk.meta_data orelse return error.MissingColumnMetaData;

            // Calculate offset and length
            var start: u64 = @intCast(meta.data_page_offset);
            if (meta.dictionary_page_offset) |dpo| {
                if (dpo < start) start = @intCast(dpo);
            }
            const len: u64 = @intCast(meta.total_compressed_size);

            try ranges.append(self.allocator, .{ .start = start, .end = start + len });

            const buf = try self.allocator.alloc(u8, @intCast(len));
            try buffers.append(self.allocator, buf);
        }

        if (ranges.items.len == 0) return;

        // Perform Parallel Read
        // We use errdefer to free buffers if read fails
        errdefer {
            for (buffers.items) |b| self.allocator.free(b);
        }

        // Convert ArrayList to slice for readRanges
        const buf_slice = try self.allocator.alloc([]u8, buffers.items.len);
        defer self.allocator.free(buf_slice);
        @memcpy(buf_slice, buffers.items);

        try self.file.source.readRanges(ranges.items, buf_slice);

        // Store results
        var buf_idx: usize = 0;
        for (targets) |col_idx| {
            if (self.memory_sources[col_idx] != null) continue; // Skip if we skipped above

            const buf = buffers.items[buf_idx];
            buf_idx += 1;

            const chunk = self.meta.columns.items[col_idx];
            const meta = chunk.meta_data.?;
            var start: u64 = @intCast(meta.data_page_offset);
            if (meta.dictionary_page_offset) |dpo| {
                if (dpo < start) start = @intCast(dpo);
            }

            self.memory_sources[col_idx] = io.MemorySource.initWithOffset(buf, start);
            try self.buffers.append(self.allocator, buf);
        }
    }

    pub fn columnReader(self: *RowGroupReader, index: usize) !ColumnReader {
        if (index >= self.meta.columns.items.len) return error.InvalidColumnIndex;

        const chunk = self.meta.columns.items[index];

        // Use memory source if available
        if (self.memory_sources[index] != null) {
            return ColumnReader.init(self.memory_sources[index].?.source(), chunk);
        }

        return ColumnReader.init(self.file.source, chunk);
    }
};

pub const ParquetFile = struct {
    source: io.RandomAccessSource,

    // Ownership management
    cleanup_context: ?*anyopaque = null,
    cleanup_fn: ?*const fn (ctx: *anyopaque, allocator: std.mem.Allocator) void = null,

    footer_len: u32,
    file_size: u64,
    metadata: ?schema.FileMetaData = null,
    footer_buffer: []u8 = &[_]u8{},
    allocator: std.mem.Allocator,

    fn cleanupLocal(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const s: *io.LocalFileSource = @ptrCast(@alignCast(ctx));
        s.deinit();
        allocator.destroy(s);
    }

    fn cleanupXevS3(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const zpq = @import("../../zpq.zig");
        const s: *zpq.s3.XevS3Source = @ptrCast(@alignCast(ctx));
        s.deinit();
        allocator.destroy(s);
    }

    /// Open a local file path. ParquetFile owns the file source.
    pub fn open(allocator: std.mem.Allocator, path: []const u8) !ParquetFile {
        const local_source = try allocator.create(io.LocalFileSource);
        errdefer allocator.destroy(local_source);

        local_source.* = try io.LocalFileSource.init(path);
        errdefer local_source.deinit();

        const source = local_source.source();
        const size = source.size();

        if (size < 8) {
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .source = source,
            .cleanup_context = local_source,
            .cleanup_fn = cleanupLocal,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
        };
    }

    /// Open an S3 object using the new xev-based stack. ParquetFile takes ownership of the provided XevS3Source.
    pub fn openS3(allocator: std.mem.Allocator, s3_source: *@import("../io/s3/xev_source.zig").XevS3Source) !ParquetFile {
        const source = s3_source.source();
        const size = source.size();

        if (size < 8) {
            s3_source.deinit();
            allocator.destroy(s3_source);
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .source = source,
            .cleanup_context = s3_source,
            .cleanup_fn = cleanupXevS3,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
        };
    }

    /// Initialize with an existing source. Caller retains ownership unless cleanup_fn is provided.
    pub fn init(allocator: std.mem.Allocator, source: io.RandomAccessSource) !ParquetFile {
        const size = source.size();
        if (size < 8) {
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .source = source,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
        };
    }

    /// Initialize taking ownership of a custom source context
    pub fn initOwned(allocator: std.mem.Allocator, source: io.RandomAccessSource, context: *anyopaque, cleanup: *const fn (*anyopaque, std.mem.Allocator) void) !ParquetFile {
        const size = source.size();
        if (size < 8) {
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .source = source,
            .cleanup_context = context,
            .cleanup_fn = cleanup,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *ParquetFile) void {
        if (self.metadata) |*m| {
            m.deinit(self.allocator);
            self.metadata = null;
        }
        if (self.footer_buffer.len > 0) {
            self.allocator.free(self.footer_buffer);
            self.footer_buffer = &[_]u8{};
        }

        if (self.cleanup_fn) |clean| {
            if (self.cleanup_context) |ctx| {
                clean(ctx, self.allocator);
                self.cleanup_context = null;
                self.cleanup_fn = null;
            }
        }
    }

    pub fn close(self: *ParquetFile) void {
        self.deinit();
    }

    pub fn readFooter(self: *ParquetFile) !void {
        // Optimization: Adaptive footer prefetch (DuckDB-style)
        // Use file_size / 256 clamped to [16KB, 256KB]
        // - Small files (1MB): prefetch ~4KB, clamped to 16KB
        // - Medium files (10MB): prefetch ~40KB
        // - Large files (100MB+): prefetch 256KB (max)
        const MIN_PREFETCH = 16 * 1024; // 16KB minimum
        const MAX_PREFETCH = 256 * 1024; // 256KB maximum
        const adaptive_size = self.file_size / 256;
        const clamped_size = @max(MIN_PREFETCH, @min(adaptive_size, MAX_PREFETCH));
        const fetch_len = @min(self.file_size, clamped_size);
        const fetch_start = self.file_size - fetch_len;

        var prefetch_buf = try self.allocator.alloc(u8, fetch_len);
        defer self.allocator.free(prefetch_buf);

        const n = try self.source.readAt(fetch_start, prefetch_buf);
        if (n != fetch_len) return error.UnexpectedEndOfFile;

        // Check magic (last 4 bytes)
        if (!std.mem.eql(u8, prefetch_buf[fetch_len - 4 ..], "PAR1")) {
            return error.InvalidMagicBytes;
        }

        // Read footer length (4 bytes before magic)
        self.footer_len = std.mem.readInt(u32, prefetch_buf[fetch_len - 8 .. fetch_len - 4][0..4], .little);

        // Ensure footer length is reasonable
        if (self.footer_len > self.file_size - 8) {
            return error.InvalidFooterLength;
        }

        if (self.footer_len + 8 <= fetch_len) {
            const start_in_buf = fetch_len - 8 - self.footer_len;
            const footer_slice = prefetch_buf[start_in_buf .. fetch_len - 8];

            if (self.footer_buffer.len > 0) self.allocator.free(self.footer_buffer);
            self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
            @memcpy(self.footer_buffer, footer_slice);
        } else {
            const footer_start = self.file_size - 8 - self.footer_len;
            if (self.footer_buffer.len > 0) self.allocator.free(self.footer_buffer);
            self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
            const bytes_read = try self.source.readAt(footer_start, self.footer_buffer);
            if (bytes_read != self.footer_len) return error.UnexpectedEndOfFile;
        }

        var reader = thrift.Reader.init(self.footer_buffer);
        self.metadata = try schema.FileMetaData.read(self.allocator, &reader);
    }

    pub fn rowGroup(self: *ParquetFile, index: usize) !RowGroupReader {
        if (self.metadata) |*meta| {
            if (index >= meta.row_groups.items.len) return error.InvalidRowGroupIndex;
            return RowGroupReader.init(self, meta.row_groups.items[index], self.allocator);
        }
        return error.MetadataNotLoaded;
    }
};
