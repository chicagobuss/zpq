//! Page Fetcher - executes byte-range reads from FetchPlan
//!
//! This is the "FETCH" phase of the 3-phase surgical execution model.
//! Takes a FetchPlan and reads the minimal byte ranges from the source.
//!
//! Key design: Collects all ranges across all columns, issues a single
//! batch read (I/O layer handles coalescing), then distributes buffers.

const std = @import("std");
const types = @import("types.zig");
const interface = @import("../../io/interface.zig");

const FetchPlan = types.FetchPlan;
const PagePlan = types.PagePlan;
const ByteRange = types.ByteRange;
const FetchedPages = types.FetchedPages;
const FetchResult = types.FetchResult;
const Range = interface.Range;
const RandomAccessSource = interface.RandomAccessSource;

pub const PageFetcher = struct {
    allocator: std.mem.Allocator,
    source: RandomAccessSource,

    pub fn init(allocator: std.mem.Allocator, source: RandomAccessSource) PageFetcher {
        return .{ .allocator = allocator, .source = source };
    }

    /// Fetch all pages specified in the plan.
    /// Returns FetchedPages for filter column and each output column.
    pub fn fetch(self: *PageFetcher, plan: *const FetchPlan) !FetchResult {
        if (plan.active_row_groups.len == 0) {
            return FetchResult{
                .filter_pages = &[_]FetchedPages{},
                .output_pages = &[_][]FetchedPages{},
            };
        }

        // Collect all byte ranges across all columns
        var all_ranges = std.ArrayListUnmanaged(Range){};
        defer all_ranges.deinit(self.allocator);

        var range_mapping = std.ArrayListUnmanaged(RangeMapping){};
        defer range_mapping.deinit(self.allocator);

        // Add filter column ranges
        for (plan.filter_plans, 0..) |*filter_plan, rg_idx| {
            try self.collectRanges(filter_plan, rg_idx, .filter, 0, &all_ranges, &range_mapping);
        }

        // Add output column ranges (skip if same as filter)
        for (plan.output_plans, 0..) |rg_output_plans, rg_idx| {
            for (rg_output_plans, 0..) |*output_plan, col_idx| {
                if (plan.filter_in_output and col_idx == plan.filter_output_idx.?) {
                    continue; // Will reuse filter column data
                }
                try self.collectRanges(output_plan, rg_idx, .output, col_idx, &all_ranges, &range_mapping);
            }
        }

        if (all_ranges.items.len == 0) {
            // No ranges to fetch - create empty result
            return self.createEmptyResult(plan);
        }

        // Allocate buffers for each range
        var buffers = try self.allocator.alloc([]u8, all_ranges.items.len);
        errdefer {
            for (buffers) |buf| self.allocator.free(buf);
            self.allocator.free(buffers);
        }

        for (all_ranges.items, 0..) |range, i| {
            buffers[i] = try self.allocator.alloc(u8, @intCast(range.end - range.start));
        }

        // Execute batch read (I/O layer handles coalescing for S3)
        try self.source.readRanges(all_ranges.items, buffers);

        // Distribute buffers to FetchedPages structures
        return self.distributeBuffers(plan, buffers, range_mapping.items);
    }

    const ColumnType = enum { filter, output };

    const RangeMapping = struct {
        rg_plan_idx: usize, // Index into active_row_groups / filter_plans / output_plans
        col_type: ColumnType,
        col_idx: usize, // For output columns
        page_idx: ?usize, // null for dictionary
    };

    fn collectRanges(
        self: *PageFetcher,
        plan: *const PagePlan,
        rg_plan_idx: usize,
        col_type: ColumnType,
        col_idx: usize,
        ranges: *std.ArrayListUnmanaged(Range),
        mapping: *std.ArrayListUnmanaged(RangeMapping),
    ) !void {
        // Add dictionary range first if present
        if (plan.dict_range) |dr| {
            try ranges.append(self.allocator, .{ .start = dr.offset, .end = dr.end() });
            try mapping.append(self.allocator, .{
                .rg_plan_idx = rg_plan_idx,
                .col_type = col_type,
                .col_idx = col_idx,
                .page_idx = null,
            });
        }

        // Add page ranges
        var iter = plan.page_mask.iterator(.{});
        while (iter.next()) |page_idx| {
            if (plan.page_ranges[page_idx]) |pr| {
                try ranges.append(self.allocator, .{ .start = pr.offset, .end = pr.end() });
                try mapping.append(self.allocator, .{
                    .rg_plan_idx = rg_plan_idx,
                    .col_type = col_type,
                    .col_idx = col_idx,
                    .page_idx = page_idx,
                });
            }
        }
    }

    fn createEmptyResult(self: *PageFetcher, plan: *const FetchPlan) !FetchResult {
        // Allocate empty FetchedPages arrays
        var filter_pages = try self.allocator.alloc(FetchedPages, plan.filter_plans.len);
        errdefer self.allocator.free(filter_pages);

        for (plan.filter_plans, 0..) |*fp, i| {
            filter_pages[i] = try FetchedPages.init(self.allocator, fp.column, fp.page_ranges.len);
        }

        var output_pages = try self.allocator.alloc([]FetchedPages, plan.output_plans.len);
        errdefer self.allocator.free(output_pages);

        for (plan.output_plans, 0..) |rg_plans, rg_idx| {
            output_pages[rg_idx] = try self.allocator.alloc(FetchedPages, rg_plans.len);
            for (rg_plans, 0..) |*op, col_idx| {
                output_pages[rg_idx][col_idx] = try FetchedPages.init(self.allocator, op.column, op.page_ranges.len);
            }
        }

        return FetchResult{
            .filter_pages = filter_pages,
            .output_pages = output_pages,
        };
    }

    fn distributeBuffers(
        self: *PageFetcher,
        plan: *const FetchPlan,
        buffers: [][]u8,
        mappings: []const RangeMapping,
    ) !FetchResult {
        // Create FetchedPages structures
        var filter_pages = try self.allocator.alloc(FetchedPages, plan.filter_plans.len);
        errdefer {
            for (filter_pages) |*fp| fp.deinit(self.allocator);
            self.allocator.free(filter_pages);
        }

        for (plan.filter_plans, 0..) |*fp, i| {
            filter_pages[i] = try FetchedPages.init(self.allocator, fp.column, fp.page_ranges.len);
        }

        var output_pages = try self.allocator.alloc([]FetchedPages, plan.output_plans.len);
        errdefer {
            for (output_pages) |rg_pages| {
                for (rg_pages) |*op| op.deinit(self.allocator);
                self.allocator.free(rg_pages);
            }
            self.allocator.free(output_pages);
        }

        for (plan.output_plans, 0..) |rg_plans, rg_idx| {
            output_pages[rg_idx] = try self.allocator.alloc(FetchedPages, rg_plans.len);
            for (rg_plans, 0..) |*op, col_idx| {
                output_pages[rg_idx][col_idx] = try FetchedPages.init(self.allocator, op.column, op.page_ranges.len);
            }
        }

        // Distribute buffers according to mapping
        for (mappings, 0..) |m, buf_idx| {
            const buf = buffers[buf_idx];

            switch (m.col_type) {
                .filter => {
                    var fp = &filter_pages[m.rg_plan_idx];
                    if (m.page_idx) |page_idx| {
                        try fp.setPage(self.allocator, page_idx, buf);
                    } else {
                        try fp.setDictionary(self.allocator, buf);
                    }
                },
                .output => {
                    var op = &output_pages[m.rg_plan_idx][m.col_idx];
                    if (m.page_idx) |page_idx| {
                        try op.setPage(self.allocator, page_idx, buf);
                    } else {
                        try op.setDictionary(self.allocator, buf);
                    }
                },
            }
        }

        // Handle filter-in-output: copy references from filter to output
        if (plan.filter_in_output) {
            const out_col_idx = plan.filter_output_idx.?;
            for (0..plan.active_row_groups.len) |rg_idx| {
                // The output slot already has an empty FetchedPages
                // Replace it with a reference to filter pages
                output_pages[rg_idx][out_col_idx].deinit(self.allocator);
                output_pages[rg_idx][out_col_idx] = try self.cloneFetchedPages(&filter_pages[rg_idx]);
            }
        }

        // Free the buffers array (ownership transferred to FetchedPages)
        self.allocator.free(buffers);

        return FetchResult{
            .filter_pages = filter_pages,
            .output_pages = output_pages,
        };
    }

    /// Clone FetchedPages (shallow - references same buffers)
    /// Used for filter-in-output case where we want to share data
    fn cloneFetchedPages(self: *PageFetcher, src: *const FetchedPages) !FetchedPages {
        const pages = try self.allocator.alloc(?[]const u8, src.pages.len);
        @memcpy(pages, src.pages);

        return FetchedPages{
            .column = src.column,
            .pages = pages,
            .dictionary = src.dictionary,
            .buffers = .{}, // Don't own buffers - they're owned by original
        };
    }
};
