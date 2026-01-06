//! Surgical engine shared types
//!
//! Data structures used across the surgical read engine modules.

const std = @import("std");
const schema = @import("../schema.zig");
const page_index = @import("../page_index.zig");

/// Identifies a column within a row group
pub const ColumnRef = struct {
    rg_idx: usize,
    col_idx: usize,
    col_name: []const u8,
    col_type: schema.Type,
};

/// Byte range to fetch [offset, offset + length)
pub const ByteRange = struct {
    offset: u64,
    length: u32,

    pub fn end(self: ByteRange) u64 {
        return self.offset + self.length;
    }
};

/// Which pages to fetch for a column, with byte ranges
pub const PagePlan = struct {
    column: ColumnRef,

    /// Bitmap of pages to fetch (bit N = fetch page N)
    page_mask: std.DynamicBitSet,

    /// Byte ranges for each page (from OffsetIndex)
    /// Indexed by page number, null if page not in mask
    page_ranges: []?ByteRange,

    /// First row index of each page (for row-to-page mapping)
    page_first_rows: []u64,

    /// Dictionary byte range (if column uses dictionary encoding)
    dict_range: ?ByteRange,

    /// Total bytes to fetch for this column
    total_bytes: u64,

    pub fn deinit(self: *PagePlan, allocator: std.mem.Allocator) void {
        self.page_mask.deinit();
        allocator.free(self.page_ranges);
        allocator.free(self.page_first_rows);
    }

    /// Count pages that will be fetched
    pub fn fetchedPageCount(self: *const PagePlan) usize {
        return self.page_mask.count();
    }

    /// Get total page count
    pub fn totalPageCount(self: *const PagePlan) usize {
        return self.page_ranges.len;
    }
};

/// Complete fetch plan for a query
pub const FetchPlan = struct {
    /// Row groups that passed statistics pruning
    active_row_groups: []usize,

    /// Filter column page plan (one per active row group)
    filter_plans: []PagePlan,

    /// Output column page plans (one per column per active row group)
    /// Outer: row group index (into active_row_groups), Inner: output column
    output_plans: [][]PagePlan,

    /// Whether filter column is also in output (avoid double decode)
    filter_in_output: bool,
    filter_output_idx: ?usize,

    /// Total bytes to fetch (for progress/logging)
    total_bytes: u64,

    /// Estimated rows to scan (sum of page row counts)
    estimated_rows: u64,

    pub fn deinit(self: *FetchPlan, allocator: std.mem.Allocator) void {
        for (self.filter_plans) |*p| p.deinit(allocator);
        allocator.free(self.filter_plans);

        for (self.output_plans) |plans| {
            for (plans) |*p| p.deinit(allocator);
            allocator.free(plans);
        }
        allocator.free(self.output_plans);
        allocator.free(self.active_row_groups);
    }

    /// Get summary statistics for logging
    pub fn summary(self: *const FetchPlan) PlanSummary {
        var total_pages: usize = 0;
        var fetched_pages: usize = 0;

        for (self.filter_plans) |*plan| {
            total_pages += plan.totalPageCount();
            fetched_pages += plan.fetchedPageCount();
        }

        for (self.output_plans) |rg_plans| {
            for (rg_plans) |*plan| {
                total_pages += plan.totalPageCount();
                fetched_pages += plan.fetchedPageCount();
            }
        }

        return .{
            .active_row_groups = self.active_row_groups.len,
            .total_bytes = self.total_bytes,
            .estimated_rows = self.estimated_rows,
            .total_pages = total_pages,
            .fetched_pages = fetched_pages,
        };
    }
};

pub const PlanSummary = struct {
    active_row_groups: usize,
    total_bytes: u64,
    estimated_rows: u64,
    total_pages: usize,
    fetched_pages: usize,

    pub fn skipRatio(self: PlanSummary) f64 {
        if (self.total_pages == 0) return 0;
        return 1.0 - @as(f64, @floatFromInt(self.fetched_pages)) / @as(f64, @floatFromInt(self.total_pages));
    }
};

/// Fetched page data for a column
pub const FetchedPages = struct {
    column: ColumnRef,

    /// Sparse array: page_idx → page bytes (null if not fetched)
    pages: []?[]const u8,

    /// Dictionary data (shared across all pages)
    dictionary: ?[]const u8,

    /// Owns the underlying buffers
    buffers: std.ArrayListUnmanaged([]u8),

    pub fn init(allocator: std.mem.Allocator, column: ColumnRef, num_pages: usize) !FetchedPages {
        const pages = try allocator.alloc(?[]const u8, num_pages);
        @memset(pages, null);

        return .{
            .column = column,
            .pages = pages,
            .dictionary = null,
            .buffers = .{},
        };
    }

    pub fn deinit(self: *FetchedPages, allocator: std.mem.Allocator) void {
        for (self.buffers.items) |buf| allocator.free(buf);
        self.buffers.deinit(allocator);
        allocator.free(self.pages);
    }

    /// Set page data (takes ownership of buffer)
    pub fn setPage(self: *FetchedPages, allocator: std.mem.Allocator, page_idx: usize, data: []u8) !void {
        self.pages[page_idx] = data;
        try self.buffers.append(allocator, data);
    }

    /// Set dictionary data (takes ownership of buffer)
    pub fn setDictionary(self: *FetchedPages, allocator: std.mem.Allocator, data: []u8) !void {
        self.dictionary = data;
        try self.buffers.append(allocator, data);
    }

    /// Assemble sparse pages into a contiguous buffer for RowGroupWorker.
    /// Returns AssembledColumn with buffer laid out as: [dictionary][page0][page1]...
    /// The synthetic_chunk has offsets adjusted to point into this buffer.
    pub fn assemble(self: *const FetchedPages, allocator: std.mem.Allocator, original_chunk: schema.ColumnChunk) !AssembledColumn {
        // Calculate total size needed
        var total_size: usize = 0;
        if (self.dictionary) |dict| {
            total_size += dict.len;
        }
        for (self.pages) |maybe_page| {
            if (maybe_page) |page| {
                total_size += page.len;
            }
        }

        // Allocate contiguous buffer
        const buffer = try allocator.alloc(u8, total_size);
        errdefer allocator.free(buffer);

        // Copy dictionary first (if present)
        var offset: usize = 0;
        const data_offset: u64 = if (self.dictionary) |dict| blk: {
            @memcpy(buffer[offset..][0..dict.len], dict);
            offset += dict.len;
            break :blk dict.len;
        } else 0;

        // Copy data pages in order
        for (self.pages) |maybe_page| {
            if (maybe_page) |page| {
                @memcpy(buffer[offset..][0..page.len], page);
                offset += page.len;
            }
        }

        // Create synthetic chunk with adjusted offsets
        var synthetic_chunk = original_chunk;
        if (synthetic_chunk.meta_data) |*md| {
            // Point data_page_offset to where data pages start in our buffer
            md.data_page_offset = @intCast(data_offset);
            // Point dictionary_page_offset to start of buffer (0) if we have a dictionary
            if (self.dictionary != null) {
                md.dictionary_page_offset = 0;
            } else {
                md.dictionary_page_offset = null;
            }
        }

        return AssembledColumn{
            .buffer = buffer,
            .data_offset = data_offset,
            .chunk = original_chunk,
            .synthetic_chunk = synthetic_chunk,
        };
    }
};

/// Result of fetch phase
pub const FetchResult = struct {
    /// Filter column pages (one per active row group)
    filter_pages: []FetchedPages,

    /// Output column pages [rg_idx][col_idx]
    output_pages: [][]FetchedPages,

    pub fn deinit(self: *FetchResult, allocator: std.mem.Allocator) void {
        for (self.filter_pages) |*fp| fp.deinit(allocator);
        allocator.free(self.filter_pages);

        for (self.output_pages) |rg_pages| {
            for (rg_pages) |*op| op.deinit(allocator);
            allocator.free(rg_pages);
        }
        allocator.free(self.output_pages);
    }
};

/// Assembled column buffer ready for RowGroupWorker
/// Contains dictionary (if any) followed by contiguous data pages
pub const AssembledColumn = struct {
    /// Contiguous buffer: [dictionary][page0][page1]...
    buffer: []u8,

    /// Offset within buffer where dictionary ends and data pages begin
    /// If no dictionary, this is 0
    data_offset: u64,

    /// Original column chunk metadata (for type info, encoding, etc.)
    chunk: schema.ColumnChunk,

    /// Synthetic metadata with offsets adjusted to our buffer
    /// data_page_offset points to data_offset in our buffer
    /// dictionary_page_offset points to 0 if we have a dictionary
    synthetic_chunk: schema.ColumnChunk,

    pub fn deinit(self: *AssembledColumn, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
    }
};

/// Assembled data for one row group, ready for RowGroupWorker
pub const AssembledRowGroup = struct {
    rg_idx: usize,
    num_rows: usize,

    /// Assembled filter column
    filter: AssembledColumn,

    /// Assembled output columns
    outputs: []AssembledColumn,

    pub fn deinit(self: *AssembledRowGroup, allocator: std.mem.Allocator) void {
        self.filter.deinit(allocator);
        for (self.outputs) |*out| out.deinit(allocator);
        allocator.free(self.outputs);
    }

    /// Convert to RowGroupData format for use with RowGroupWorker.
    /// The caller must keep this AssembledRowGroup alive while using the RowGroupData,
    /// as it borrows the buffer slices.
    pub fn toRowGroupData(self: *const AssembledRowGroup, allocator: std.mem.Allocator) !RowGroupData {
        // Build output arrays
        const col_count = self.outputs.len;
        const output_bufs = try allocator.alloc([]const u8, col_count);
        errdefer allocator.free(output_bufs);

        const output_offsets = try allocator.alloc(u64, col_count);
        errdefer allocator.free(output_offsets);

        const output_chunks = try allocator.alloc(schema.ColumnChunk, col_count);
        errdefer allocator.free(output_chunks);

        for (self.outputs, 0..) |*out, i| {
            output_bufs[i] = out.buffer;
            output_offsets[i] = 0; // Buffer starts at offset 0
            output_chunks[i] = out.synthetic_chunk;
        }

        // Build filter arrays (surgical path currently only uses one filter)
        const filter_bufs = try allocator.alloc([]const u8, 1);
        errdefer allocator.free(filter_bufs);
        filter_bufs[0] = self.filter.buffer;

        const filter_offsets = try allocator.alloc(u64, 1);
        errdefer allocator.free(filter_offsets);
        filter_offsets[0] = 0;

        const filter_chunks = try allocator.alloc(schema.ColumnChunk, 1);
        errdefer allocator.free(filter_chunks);
        filter_chunks[0] = self.filter.synthetic_chunk;

        return RowGroupData{
            .num_rows = @intCast(self.num_rows),
            .rg_idx = self.rg_idx,
            .filter_bufs = filter_bufs,
            .filter_offsets = filter_offsets,
            .filter_chunks = filter_chunks,
            .output_bufs = output_bufs,
            .output_offsets = output_offsets,
            .output_chunks = output_chunks,
        };
    }
};

/// Result of surgical engine execution - assembled data ready for RowGroupWorker
pub const SurgicalResult = struct {
    /// Assembled row groups (one per active row group)
    row_groups: []AssembledRowGroup,

    /// Statistics
    bytes_fetched: u64,
    bytes_full_scan: u64,
    pages_skipped: usize,
    pages_total: usize,

    pub fn deinit(self: *SurgicalResult, allocator: std.mem.Allocator) void {
        for (self.row_groups) |*rg| rg.deinit(allocator);
        allocator.free(self.row_groups);
    }
};

// Re-export RowGroupData for convenience
const row_group_worker = @import("../row_group_worker.zig");
pub const RowGroupData = row_group_worker.RowGroupData;

test "ByteRange end calculation" {
    const range = ByteRange{ .offset = 100, .length = 50 };
    try std.testing.expectEqual(@as(u64, 150), range.end());
}

test "PagePlan page counting" {
    const allocator = std.testing.allocator;

    var mask = try std.DynamicBitSet.initEmpty(allocator, 10);
    mask.set(2);
    mask.set(5);
    mask.set(7);

    const page_ranges = try allocator.alloc(?ByteRange, 10);
    @memset(page_ranges, null);

    const page_first_rows = try allocator.alloc(u64, 10);
    for (page_first_rows, 0..) |*r, i| r.* = i * 1000;

    var plan = PagePlan{
        .column = .{ .rg_idx = 0, .col_idx = 0, .col_name = "test", .col_type = .INT64 },
        .page_mask = mask,
        .page_ranges = page_ranges,
        .page_first_rows = page_first_rows,
        .dict_range = null,
        .total_bytes = 0,
    };
    defer plan.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), plan.fetchedPageCount());
    try std.testing.expectEqual(@as(usize, 10), plan.totalPageCount());
}
