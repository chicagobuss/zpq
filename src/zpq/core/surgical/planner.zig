//! Query Planner - determines minimal pages to fetch
//!
//! Uses ColumnIndex (page min/max) and OffsetIndex (page byte ranges)
//! to build a surgical fetch plan. This is the "PLAN" phase of the
//! 3-phase surgical execution model.
//!
//! Key insight: We can determine exactly which pages might contain
//! matching rows BEFORE fetching any data, using page-level statistics.

const std = @import("std");
const types = @import("types.zig");
const file_mod = @import("../file.zig");
const filter_mod = @import("../filter.zig");
const filters_mod = @import("../filters/mod.zig");
const page_index = @import("../page_index.zig");
const schema = @import("../schema.zig");

const FetchPlan = types.FetchPlan;
const PagePlan = types.PagePlan;
const ByteRange = types.ByteRange;
const ColumnRef = types.ColumnRef;
const ParquetFile = file_mod.ParquetFile;
const RowGroupReader = file_mod.RowGroupReader;
const EncodedFilter = filter_mod.EncodedFilter;
const Filter = filters_mod.Filter;
const ColumnIndex = page_index.ColumnIndex;
const OffsetIndex = page_index.OffsetIndex;

pub const Planner = struct {
    allocator: std.mem.Allocator,
    file: *ParquetFile,

    pub fn init(allocator: std.mem.Allocator, file: *ParquetFile) Planner {
        return .{ .allocator = allocator, .file = file };
    }

    /// Build a fetch plan for the given filter and projection.
    /// Returns minimal byte ranges needed to execute the query.
    pub fn buildPlan(
        self: *Planner,
        filter_col: []const u8,
        filter: *const Filter,
        output_cols: []const []const u8,
    ) !FetchPlan {
        const meta = self.file.metadata orelse return error.NoMetadata;

        // Step 1: Row group pruning using existing stats
        var active_rgs = std.ArrayListUnmanaged(usize){};
        defer active_rgs.deinit(self.allocator);

        for (meta.row_groups.items, 0..) |_, rg_idx| {
            if (!self.file.shouldSkipRowGroup(rg_idx, filter_col, filter)) {
                try active_rgs.append(self.allocator, rg_idx);
            }
        }

        if (active_rgs.items.len == 0) {
            // No matching row groups - return empty plan
            return FetchPlan{
                .active_row_groups = &[_]usize{},
                .filter_plans = &[_]PagePlan{},
                .output_plans = &[_][]PagePlan{},
                .filter_in_output = false,
                .filter_output_idx = null,
                .total_bytes = 0,
                .estimated_rows = 0,
            };
        }

        // Check if filter column is in output (avoid double fetch/decode)
        var filter_in_output = false;
        var filter_output_idx: ?usize = null;
        for (output_cols, 0..) |col, i| {
            if (std.mem.eql(u8, col, filter_col)) {
                filter_in_output = true;
                filter_output_idx = i;
                break;
            }
        }

        // Step 2: For each active row group, build page plans
        var filter_plans = try self.allocator.alloc(PagePlan, active_rgs.items.len);
        errdefer self.allocator.free(filter_plans);

        var output_plans = try self.allocator.alloc([]PagePlan, active_rgs.items.len);
        errdefer self.allocator.free(output_plans);

        var total_bytes: u64 = 0;
        var estimated_rows: u64 = 0;
        var initialized_filter_plans: usize = 0;
        var initialized_output_plans: usize = 0;

        errdefer {
            // Clean up on error
            for (0..initialized_filter_plans) |i| filter_plans[i].deinit(self.allocator);
            for (0..initialized_output_plans) |i| {
                for (output_plans[i]) |*p| p.deinit(self.allocator);
                self.allocator.free(output_plans[i]);
            }
        }

        for (active_rgs.items, 0..) |rg_idx, plan_idx| {
            var rg_reader = try RowGroupReader.init(self.file, meta.row_groups.items[rg_idx], self.allocator);
            defer rg_reader.deinit();

            // Build filter column plan using ColumnIndex
            filter_plans[plan_idx] = try self.buildFilterColumnPlan(&rg_reader, rg_idx, filter_col, filter);
            initialized_filter_plans += 1;

            total_bytes += filter_plans[plan_idx].total_bytes;
            estimated_rows += self.countPlanRows(&filter_plans[plan_idx], &rg_reader);

            // Build output column plans
            output_plans[plan_idx] = try self.allocator.alloc(PagePlan, output_cols.len);
            initialized_output_plans += 1;

            var initialized_cols: usize = 0;
            errdefer {
                for (0..initialized_cols) |i| output_plans[plan_idx][i].deinit(self.allocator);
            }

            for (output_cols, 0..) |col_name, col_idx| {
                if (filter_in_output and col_idx == filter_output_idx.?) {
                    // Share the filter plan for this column (don't double-count bytes)
                    output_plans[plan_idx][col_idx] = try self.clonePagePlan(&filter_plans[plan_idx]);
                } else {
                    // Build plan based on filter's matching row ranges
                    output_plans[plan_idx][col_idx] = try self.buildOutputColumnPlan(
                        &rg_reader,
                        rg_idx,
                        col_name,
                        &filter_plans[plan_idx],
                    );
                    total_bytes += output_plans[plan_idx][col_idx].total_bytes;
                }
                initialized_cols += 1;
            }
        }

        return FetchPlan{
            .active_row_groups = try active_rgs.toOwnedSlice(self.allocator),
            .filter_plans = filter_plans,
            .output_plans = output_plans,
            .filter_in_output = filter_in_output,
            .filter_output_idx = filter_output_idx,
            .total_bytes = total_bytes,
            .estimated_rows = estimated_rows,
        };
    }

    /// Build page plan for filter column using ColumnIndex for page-level pruning
    fn buildFilterColumnPlan(
        self: *Planner,
        rg_reader: *RowGroupReader,
        rg_idx: usize,
        col_name: []const u8,
        filter: *const Filter,
    ) !PagePlan {
        const col_idx = try rg_reader.getColumnIndexByName(col_name) orelse return error.ColumnNotFound;
        const col_meta = rg_reader.meta.columns.items[col_idx].meta_data orelse return error.MissingColumnMetaData;

        // Try to get ColumnIndex for page-level pruning
        var column_index_opt: ?ColumnIndex = try rg_reader.getColumnIndex(col_idx);
        defer if (column_index_opt) |*ci| ci.deinit(self.allocator);

        // Get OffsetIndex for byte ranges
        var offset_index_opt: ?OffsetIndex = try rg_reader.getOffsetIndex(col_idx);
        defer if (offset_index_opt) |*oi| oi.deinit(self.allocator);

        if (column_index_opt == null or offset_index_opt == null) {
            // Fallback: fetch entire column (graceful degradation)
            return self.buildFullColumnPlan(rg_reader, rg_idx, col_idx, col_name, col_meta);
        }

        const ci = column_index_opt.?;
        const oi = offset_index_opt.?;
        const num_pages = ci.numPages();

        // Build page mask using ColumnIndex min/max
        var page_mask = try std.DynamicBitSet.initEmpty(self.allocator, num_pages);
        errdefer page_mask.deinit();

        var page_ranges = try self.allocator.alloc(?ByteRange, num_pages);
        errdefer self.allocator.free(page_ranges);

        var page_first_rows = try self.allocator.alloc(u64, num_pages);
        errdefer self.allocator.free(page_first_rows);

        var total_bytes: u64 = 0;

        for (0..num_pages) |page_idx| {
            page_first_rows[page_idx] = @intCast(oi.page_locations[page_idx].first_row_index);

            if (filter.mightMatchPage(&ci, page_idx)) {
                // This page might contain matching values
                page_mask.set(page_idx);
                const loc = oi.page_locations[page_idx];
                page_ranges[page_idx] = .{
                    .offset = @intCast(loc.offset),
                    .length = @intCast(loc.compressed_page_size),
                };
                total_bytes += @intCast(loc.compressed_page_size);
            } else {
                page_ranges[page_idx] = null;
            }
        }

        // Add dictionary if present (needed for decoding)
        var dict_range: ?ByteRange = null;
        if (col_meta.dictionary_page_offset) |dpo| {
            const dict_size: u64 = @intCast(col_meta.data_page_offset - dpo);
            dict_range = .{
                .offset = @intCast(dpo),
                .length = @intCast(dict_size),
            };
            total_bytes += dict_size;
        }

        return PagePlan{
            .column = .{
                .rg_idx = rg_idx,
                .col_idx = col_idx,
                .col_name = col_name,
                .col_type = col_meta.type,
            },
            .page_mask = page_mask,
            .page_ranges = page_ranges,
            .page_first_rows = page_first_rows,
            .dict_range = dict_range,
            .total_bytes = total_bytes,
        };
    }

    /// Build output column plan based on which rows the filter might match.
    /// Uses filter's page_first_rows to determine which output pages contain those rows.
    fn buildOutputColumnPlan(
        self: *Planner,
        rg_reader: *RowGroupReader,
        rg_idx: usize,
        col_name: []const u8,
        filter_plan: *const PagePlan,
    ) !PagePlan {
        const col_idx = try rg_reader.getColumnIndexByName(col_name) orelse return error.ColumnNotFound;
        const col_meta = rg_reader.meta.columns.items[col_idx].meta_data orelse return error.MissingColumnMetaData;

        // Get OffsetIndex for this column
        var offset_index_opt: ?OffsetIndex = try rg_reader.getOffsetIndex(col_idx);
        defer if (offset_index_opt) |*oi| oi.deinit(self.allocator);

        if (offset_index_opt == null) {
            return self.buildFullColumnPlan(rg_reader, rg_idx, col_idx, col_name, col_meta);
        }

        const oi = offset_index_opt.?;
        const num_pages = oi.page_locations.len;

        // Determine row range from filter plan
        const filter_row_range = self.getRowRangeFromPlan(filter_plan, rg_reader);

        // Find which pages of this column overlap with filter's row range
        var page_mask = try std.DynamicBitSet.initEmpty(self.allocator, num_pages);
        errdefer page_mask.deinit();

        var page_ranges = try self.allocator.alloc(?ByteRange, num_pages);
        errdefer self.allocator.free(page_ranges);

        var page_first_rows = try self.allocator.alloc(u64, num_pages);
        errdefer self.allocator.free(page_first_rows);

        var total_bytes: u64 = 0;

        for (0..num_pages) |page_idx| {
            const page_start: u64 = @intCast(oi.page_locations[page_idx].first_row_index);
            const page_end: u64 = if (page_idx + 1 < num_pages)
                @intCast(oi.page_locations[page_idx + 1].first_row_index)
            else
                @intCast(rg_reader.meta.num_rows);

            page_first_rows[page_idx] = page_start;

            // Check if this page overlaps with filter's row range
            if (page_end > filter_row_range.start and page_start < filter_row_range.end) {
                page_mask.set(page_idx);
                const loc = oi.page_locations[page_idx];
                page_ranges[page_idx] = .{
                    .offset = @intCast(loc.offset),
                    .length = @intCast(loc.compressed_page_size),
                };
                total_bytes += @intCast(loc.compressed_page_size);
            } else {
                page_ranges[page_idx] = null;
            }
        }

        // Add dictionary if present
        var dict_range: ?ByteRange = null;
        if (col_meta.dictionary_page_offset) |dpo| {
            const dict_size: u64 = @intCast(col_meta.data_page_offset - dpo);
            dict_range = .{
                .offset = @intCast(dpo),
                .length = @intCast(dict_size),
            };
            total_bytes += dict_size;
        }

        return PagePlan{
            .column = .{
                .rg_idx = rg_idx,
                .col_idx = col_idx,
                .col_name = col_name,
                .col_type = col_meta.type,
            },
            .page_mask = page_mask,
            .page_ranges = page_ranges,
            .page_first_rows = page_first_rows,
            .dict_range = dict_range,
            .total_bytes = total_bytes,
        };
    }

    /// Fallback: plan to fetch entire column (no page indexes available)
    fn buildFullColumnPlan(
        self: *Planner,
        rg_reader: *RowGroupReader,
        rg_idx: usize,
        col_idx: usize,
        col_name: []const u8,
        col_meta: schema.ColumnMetaData,
    ) !PagePlan {
        _ = rg_reader;

        // Single "page" representing entire column
        var page_mask = try std.DynamicBitSet.initEmpty(self.allocator, 1);
        errdefer page_mask.deinit();
        page_mask.set(0);

        var page_ranges = try self.allocator.alloc(?ByteRange, 1);
        errdefer self.allocator.free(page_ranges);

        var start: u64 = @intCast(col_meta.data_page_offset);
        if (col_meta.dictionary_page_offset) |dpo| {
            if (dpo < start) start = @intCast(dpo);
        }
        page_ranges[0] = .{
            .offset = start,
            .length = @intCast(col_meta.total_compressed_size),
        };

        var page_first_rows = try self.allocator.alloc(u64, 1);
        errdefer self.allocator.free(page_first_rows);
        page_first_rows[0] = 0;

        return PagePlan{
            .column = .{
                .rg_idx = rg_idx,
                .col_idx = col_idx,
                .col_name = col_name,
                .col_type = col_meta.type,
            },
            .page_mask = page_mask,
            .page_ranges = page_ranges,
            .page_first_rows = page_first_rows,
            .dict_range = null, // Included in the single range
            .total_bytes = @intCast(col_meta.total_compressed_size),
        };
    }

    /// Clone a PagePlan (for when filter column is also in output)
    fn clonePagePlan(self: *Planner, plan: *const PagePlan) !PagePlan {
        var page_mask = try plan.page_mask.clone(self.allocator);
        errdefer page_mask.deinit();

        const page_ranges = try self.allocator.dupe(?ByteRange, plan.page_ranges);
        errdefer self.allocator.free(page_ranges);

        const page_first_rows = try self.allocator.dupe(u64, plan.page_first_rows);
        errdefer self.allocator.free(page_first_rows);

        return PagePlan{
            .column = plan.column,
            .page_mask = page_mask,
            .page_ranges = page_ranges,
            .page_first_rows = page_first_rows,
            .dict_range = plan.dict_range,
            .total_bytes = 0, // Don't double-count bytes
        };
    }

    fn getRowRangeFromPlan(self: *Planner, plan: *const PagePlan, rg_reader: *RowGroupReader) struct { start: u64, end: u64 } {
        _ = self;
        var min_row: u64 = std.math.maxInt(u64);
        var max_row: u64 = 0;

        var iter = plan.page_mask.iterator(.{});
        while (iter.next()) |page_idx| {
            const start = plan.page_first_rows[page_idx];
            min_row = @min(min_row, start);

            // End row is next page's start, or row group end
            const end_row = if (page_idx + 1 < plan.page_first_rows.len)
                plan.page_first_rows[page_idx + 1]
            else
                @as(u64, @intCast(rg_reader.meta.num_rows));
            max_row = @max(max_row, end_row);
        }

        if (min_row == std.math.maxInt(u64)) {
            // No pages selected
            return .{ .start = 0, .end = 0 };
        }

        return .{ .start = min_row, .end = max_row };
    }

    fn countPlanRows(self: *Planner, plan: *const PagePlan, rg_reader: *RowGroupReader) u64 {
        _ = self;
        var total: u64 = 0;
        var iter = plan.page_mask.iterator(.{});
        while (iter.next()) |page_idx| {
            const start = plan.page_first_rows[page_idx];
            const end_row = if (page_idx + 1 < plan.page_first_rows.len)
                plan.page_first_rows[page_idx + 1]
            else
                @as(u64, @intCast(rg_reader.meta.num_rows));
            total += end_row - start;
        }
        return total;
    }
};
