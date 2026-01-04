const std = @import("std");
const schema = @import("schema.zig");
const thrift = @import("thrift.zig");
const io = @import("../io/interface.zig");
const ColumnReader = @import("column.zig").ColumnReader;
const page_index = @import("page_index.zig");
const filter = @import("filter.zig");

pub const RowGroupReader = struct {
    file: *ParquetFile,
    meta: schema.RowGroup,
    allocator: std.mem.Allocator,

    // Sparse array of memory sources (indexed by column index)
    // If null, the column is not pre-fetched.
    memory_sources: []?io.local.MemorySource,

    // Buffers backing the memory sources.
    // We own these and must free them.
    buffers: std.ArrayListUnmanaged([]u8),

    // Column name to index mapping (built lazily on first use)
    column_name_map: ?std.StringHashMapUnmanaged(usize) = null,

    // Projection indices if projection was specified
    projection_indices: ?[]const usize = null,

    pub fn init(file: *ParquetFile, meta: schema.RowGroup, allocator: std.mem.Allocator) !RowGroupReader {
        const sources = try allocator.alloc(?io.local.MemorySource, meta.columns.items.len);
        @memset(sources, null);

        return RowGroupReader{
            .file = file,
            .meta = meta,
            .allocator = allocator,
            .memory_sources = sources,
            .buffers = .{},
            .column_name_map = null,
            .projection_indices = null,
        };
    }

    /// Initialize with projection - only specified columns will be prefetched
    pub fn initWithProjection(file: *ParquetFile, meta: schema.RowGroup, allocator: std.mem.Allocator, column_names: []const []const u8) !RowGroupReader {
        var self = try init(file, meta, allocator);
        errdefer self.deinit();

        // Build name map and resolve projection indices
        try self.buildColumnNameMap();

        var indices = try allocator.alloc(usize, column_names.len);
        var valid_count: usize = 0;

        for (column_names) |name| {
            if (self.column_name_map.?.get(name)) |idx| {
                indices[valid_count] = idx;
                valid_count += 1;
            }
            // Silently skip columns not found (they might not exist in this row group)
        }

        if (valid_count > 0) {
            self.projection_indices = indices[0..valid_count];
            try self.prefetch(self.projection_indices);
        } else {
            allocator.free(indices);
        }

        return self;
    }

    pub fn deinit(self: *RowGroupReader) void {
        for (self.buffers.items) |buf| {
            self.allocator.free(buf);
        }
        self.buffers.deinit(self.allocator);
        self.allocator.free(self.memory_sources);

        // Clean up projection indices if we allocated them
        if (self.projection_indices) |indices| {
            // We need to get the original allocation size
            // The indices were allocated with column_names.len capacity
            self.allocator.free(@constCast(indices.ptr)[0..indices.len]);
        }

        // Clean up column name map
        if (self.column_name_map) |*map| {
            map.deinit(self.allocator);
        }
    }

    /// Build the column name to index mapping (lazy initialization)
    fn buildColumnNameMap(self: *RowGroupReader) !void {
        if (self.column_name_map != null) return;

        var map = std.StringHashMapUnmanaged(usize){};
        errdefer map.deinit(self.allocator);

        for (self.meta.columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const path_parts = md.path_in_schema.items;
                if (path_parts.len > 0) {
                    // Use leaf name as key (last component of path)
                    const leaf_name = path_parts[path_parts.len - 1];
                    try map.put(self.allocator, leaf_name, idx);
                }
            }
        }

        self.column_name_map = map;
    }

    /// Get column index by name
    pub fn getColumnIndexByName(self: *RowGroupReader, name: []const u8) !?usize {
        try self.buildColumnNameMap();
        return self.column_name_map.?.get(name);
    }

    /// Get a ColumnReader by column name
    pub fn columnReaderByName(self: *RowGroupReader, name: []const u8) !ColumnReader {
        const idx = try self.getColumnIndexByName(name) orelse return error.ColumnNotFound;
        return self.columnReader(idx);
    }

    /// Check if a column is in the projection (if projection was specified)
    pub fn isProjected(self: *const RowGroupReader, col_idx: usize) bool {
        if (self.projection_indices) |indices| {
            for (indices) |idx| {
                if (idx == col_idx) return true;
            }
            return false;
        }
        return true; // No projection = all columns are "projected"
    }

    /// Get column metadata by name
    pub fn getColumnMetadata(self: *RowGroupReader, name: []const u8) !?schema.ColumnMetaData {
        const idx = try self.getColumnIndexByName(name) orelse return null;
        if (idx >= self.meta.columns.items.len) return null;
        return self.meta.columns.items[idx].meta_data;
    }

    /// Check if a column has been prefetched
    pub fn isPrefetched(self: *const RowGroupReader, col_idx: usize) bool {
        if (col_idx >= self.memory_sources.len) return false;
        return self.memory_sources[col_idx] != null;
    }

    /// Pre-fetch only the specified columns (convenience wrapper)
    pub fn prefetchColumns(self: *RowGroupReader, indices: []const usize) !void {
        return self.prefetch(indices);
    }

    /// Pre-fetch all columns EXCEPT the specified ones
    pub fn prefetchExcluding(self: *RowGroupReader, exclude: []const usize) !void {
        // Build list of columns to fetch (all except excluded)
        var to_fetch = try self.allocator.alloc(usize, self.meta.columns.items.len);
        defer self.allocator.free(to_fetch);

        var count: usize = 0;
        for (0..self.meta.columns.items.len) |col_idx| {
            var excluded = false;
            for (exclude) |ex| {
                if (ex == col_idx) {
                    excluded = true;
                    break;
                }
            }
            if (!excluded) {
                to_fetch[count] = col_idx;
                count += 1;
            }
        }

        if (count > 0) {
            try self.prefetch(to_fetch[0..count]);
        }
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

        // Identify ranges and allocate buffers (or get zero-copy slices)
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

            // Try zero-copy first
            if (self.file.source.getSlice(start, len)) |slice| {
                self.memory_sources[idx] = io.local.MemorySource.initWithOffset(@constCast(slice), start);
                continue;
            }

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

            self.memory_sources[col_idx] = io.local.MemorySource.initWithOffset(buf, start);
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

    /// Fetch and parse the ColumnIndex for a column (page-level min/max stats).
    /// Returns null if the column doesn't have a ColumnIndex.
    pub fn getColumnIndex(self: *RowGroupReader, col_idx: usize) !?page_index.ColumnIndex {
        if (col_idx >= self.meta.columns.items.len) return error.InvalidColumnIndex;

        const chunk = self.meta.columns.items[col_idx];
        const offset = chunk.column_index_offset orelse return null;
        const length = chunk.column_index_length orelse return null;

        // Fetch the ColumnIndex data
        var buf = try self.allocator.alloc(u8, @intCast(length));
        errdefer self.allocator.free(buf);

        const n = try self.file.source.readAt(@intCast(offset), buf);
        if (n != buf.len) {
            self.allocator.free(buf);
            return error.UnexpectedEndOfFile;
        }

        // Parse it
        var reader = thrift.Reader.init(buf);
        const idx = try page_index.ColumnIndex.read(self.allocator, &reader);

        // Free the buffer (ColumnIndex has its own copies)
        self.allocator.free(buf);

        return idx;
    }

    /// Fetch and parse the OffsetIndex for a column (page locations).
    /// Returns null if the column doesn't have an OffsetIndex.
    pub fn getOffsetIndex(self: *RowGroupReader, col_idx: usize) !?page_index.OffsetIndex {
        if (col_idx >= self.meta.columns.items.len) return error.InvalidColumnIndex;

        const chunk = self.meta.columns.items[col_idx];
        const offset = chunk.offset_index_offset orelse return null;
        const length = chunk.offset_index_length orelse return null;

        // Fetch the OffsetIndex data
        var buf = try self.allocator.alloc(u8, @intCast(length));
        errdefer self.allocator.free(buf);

        const n = try self.file.source.readAt(@intCast(offset), buf);
        if (n != buf.len) {
            self.allocator.free(buf);
            return error.UnexpectedEndOfFile;
        }

        // Parse it
        var reader = thrift.Reader.init(buf);
        const idx = try page_index.OffsetIndex.read(self.allocator, &reader);

        // Free the buffer (OffsetIndex has its own copies)
        self.allocator.free(buf);

        return idx;
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
    footer_buffer_owned: bool = false,
    allocator: std.mem.Allocator,

    // Arena for metadata allocations (much faster than GPA for many small allocs)
    metadata_arena: std.heap.ArenaAllocator,

    fn cleanupLocal(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const s: *io.local.FileSource = @ptrCast(@alignCast(ctx));
        s.deinit();
        allocator.destroy(s);
    }

    fn cleanupMmap(ctx: *anyopaque, allocator: std.mem.Allocator) void {
        const s: *io.local.MmapSource = @ptrCast(@alignCast(ctx));
        s.deinit();
        allocator.destroy(s);
    }

    /// Open a local file path using mmap for zero-copy.
    pub fn openMmap(allocator: std.mem.Allocator, path: []const u8) !ParquetFile {
        const local_source = try allocator.create(io.local.MmapSource);
        errdefer allocator.destroy(local_source);

        local_source.* = try io.local.MmapSource.init(path);
        errdefer local_source.deinit();

        const source = local_source.source();
        const size = source.size();

        if (size < 8) {
            return error.InvalidParquetFile;
        }

        return ParquetFile{
            .source = source,
            .cleanup_context = local_source,
            .cleanup_fn = cleanupMmap,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
            .metadata_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    /// Open a local file path. ParquetFile owns the file source.
    pub fn open(allocator: std.mem.Allocator, path: []const u8) !ParquetFile {
        const local_source = try allocator.create(io.local.FileSource);
        errdefer allocator.destroy(local_source);

        local_source.* = try io.local.FileSource.init(path);
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
            .metadata_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    /// Open an S3 object using the new xev-based stack. ParquetFile takes ownership of the provided source.
    pub fn openS3(allocator: std.mem.Allocator, s3_source: anytype) !ParquetFile {
        const source = s3_source.source();
        const size = source.size();

        if (size < 8) {
            s3_source.deinit();
            allocator.destroy(s3_source);
            return error.InvalidParquetFile;
        }

        const SourceType = @TypeOf(s3_source.*);
        const Cleanup = struct {
            fn func(ctx: *anyopaque, alloc: std.mem.Allocator) void {
                const s: *SourceType = @ptrCast(@alignCast(ctx));
                s.deinit();
                alloc.destroy(s);
            }
        }.func;

        return ParquetFile{
            .source = source,
            .cleanup_context = s3_source,
            .cleanup_fn = Cleanup,
            .footer_len = 0,
            .file_size = size,
            .allocator = allocator,
            .metadata_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
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
            .metadata_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
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
            .metadata_arena = std.heap.ArenaAllocator.init(std.heap.page_allocator),
        };
    }

    pub fn deinit(self: *ParquetFile) void {
        // Arena handles all metadata allocations - no need to call m.deinit()
        self.metadata_arena.deinit();
        self.metadata = null;

        if (self.footer_buffer_owned and self.footer_buffer.len > 0) {
            self.allocator.free(self.footer_buffer);
        }
        self.footer_buffer = &[_]u8{};
        self.footer_buffer_owned = false;

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

    /// Explicitly provide an access hint to the OS (only if source is MmapSource).
    pub fn advise(self: *const ParquetFile, offset: u64, len: u64, advice: std.posix.MADV) void {
        // Try to downcast to MmapSource if possible
        // This is a bit hacky since we use anyopaque, but we can check the cleanup_fn
        if (self.cleanup_fn == cleanupMmap) {
            const mmap_src: *io.local.MmapSource = @ptrCast(@alignCast(self.cleanup_context.?));
            mmap_src.advise(offset, len, advice) catch {};
        }
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
            self.footer_buffer_owned = true;
        } else {
            const footer_start = self.file_size - 8 - self.footer_len;
            if (self.footer_buffer.len > 0) self.allocator.free(self.footer_buffer);

            // Try zero-copy for footer
            if (self.source.getSlice(footer_start, self.footer_len)) |slice| {
                self.footer_buffer = @constCast(slice);
                // We mark it as empty so deinit doesn't try to free it
                // Actually, we need to distinguish between owned and shared footer_buffer.
                // Let's add a flag or use an empty slice for ownership.
                self.footer_buffer_owned = false;
            } else {
                self.footer_buffer = try self.allocator.alloc(u8, self.footer_len);
                const bytes_read = try self.source.readAt(footer_start, self.footer_buffer);
                if (bytes_read != self.footer_len) return error.UnexpectedEndOfFile;
                self.footer_buffer_owned = true;
            }
        }

        var reader = thrift.Reader.init(self.footer_buffer);

        // Use arena allocator for metadata - much faster for many small allocations
        self.metadata = try schema.FileMetaData.read(self.metadata_arena.allocator(), &reader);
    }

    pub fn rowGroup(self: *ParquetFile, index: usize) !RowGroupReader {
        if (self.metadata) |*meta| {
            if (index >= meta.row_groups.items.len) return error.InvalidRowGroupIndex;
            return RowGroupReader.init(self, meta.row_groups.items[index], self.allocator);
        }
        return error.MetadataNotLoaded;
    }

    /// Create a RowGroupReader with column projection.
    /// Only the specified columns will be prefetched (in a single coalesced read).
    /// This is more efficient than reading all columns when you only need a subset.
    pub fn rowGroupWithProjection(self: *ParquetFile, index: usize, column_names: []const []const u8) !RowGroupReader {
        if (self.metadata) |*meta| {
            if (index >= meta.row_groups.items.len) return error.InvalidRowGroupIndex;
            return RowGroupReader.initWithProjection(self, meta.row_groups.items[index], self.allocator, column_names);
        }
        return error.MetadataNotLoaded;
    }

    /// Get column index by name from file metadata
    pub fn getColumnIndexByName(self: *const ParquetFile, name: []const u8) ?usize {
        const meta = self.metadata orelse return null;
        if (meta.row_groups.items.len == 0) return null;

        const rg0 = meta.row_groups.items[0];
        for (rg0.columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const path_parts = md.path_in_schema.items;
                if (path_parts.len > 0) {
                    const leaf_name = path_parts[path_parts.len - 1];
                    if (std.mem.eql(u8, leaf_name, name)) {
                        return idx;
                    }
                }
            }
        }
        return null;
    }

    /// Get column type by name from file metadata
    pub fn getColumnType(self: *const ParquetFile, name: []const u8) ?schema.Type {
        const meta = self.metadata orelse return null;
        if (meta.row_groups.items.len == 0) return null;

        const rg0 = meta.row_groups.items[0];
        for (rg0.columns.items) |col| {
            if (col.meta_data) |md| {
                const path_parts = md.path_in_schema.items;
                if (path_parts.len > 0) {
                    const leaf_name = path_parts[path_parts.len - 1];
                    if (std.mem.eql(u8, leaf_name, name)) {
                        return md.type;
                    }
                }
            }
        }
        return null;
    }

    /// Check if a row group should be skipped based on a simple equality filter on a column.
    /// Uses EncodedFilter for unified type handling.
    pub fn shouldSkipRowGroup(self: *const ParquetFile, rg_idx: usize, column_name: []const u8, encoded_filter: *const filter.EncodedFilter) bool {
        const meta = self.metadata orelse return false;
        if (rg_idx >= meta.row_groups.items.len) return false;
        const rg = meta.row_groups.items[rg_idx];

        for (rg.columns.items) |col| {
            if (col.meta_data) |md| {
                // Check if this column matches the name (simple leaf-name check for now)
                // In production, we'd want full path matching.
                const path = md.path_in_schema.items;
                const leaf_name = path[path.len - 1];
                if (!std.mem.eql(u8, leaf_name, column_name)) continue;

                // Found the column, check statistics using EncodedFilter
                if (md.statistics) |*stats| {
                    return !encoded_filter.mightContainInRowGroup(stats);
                }
                break;
            }
        }
        return false;
    }
};
