//! Surgical Engine - orchestrates plan/fetch/process phases
//!
//! Main entry point for surgical query execution. Coordinates:
//! 1. PLAN: Build minimal fetch plan using page-level metadata
//! 2. FETCH: Read only the required byte ranges
//! 3. ASSEMBLE: Create contiguous buffers for RowGroupWorker
//!
//! The engine is source-agnostic - works with local files, mmap, or S3.
//! Returns AssembledRowGroup structures that can be directly fed to
//! the existing RowGroupWorker for decode/filter/output.

const std = @import("std");
const types = @import("types.zig");
const planner_mod = @import("planner.zig");
const fetcher_mod = @import("fetcher.zig");
const file_mod = @import("../file.zig");
const filter_mod = @import("../filter.zig");
const filters_mod = @import("../filters/mod.zig");
const schema = @import("../schema.zig");

const FetchPlan = types.FetchPlan;
const FetchResult = types.FetchResult;
const SurgicalResult = types.SurgicalResult;
const AssembledRowGroup = types.AssembledRowGroup;
const AssembledColumn = types.AssembledColumn;
const Planner = planner_mod.Planner;
const PageFetcher = fetcher_mod.PageFetcher;
const ParquetFile = file_mod.ParquetFile;
const EncodedFilter = filter_mod.EncodedFilter;
const Filter = filters_mod.Filter;

pub const SurgicalEngine = struct {
    allocator: std.mem.Allocator,
    file: *ParquetFile,

    // Query parameters
    filter_col: []const u8,
    filter_value: []const u8,
    filter_value2: ?[]const u8, // For BETWEEN: second value
    filter_op: filters_mod.FilterOp,
    output_cols: []const []const u8,

    pub fn init(
        allocator: std.mem.Allocator,
        file: *ParquetFile,
        filter_col: []const u8,
        filter_value: []const u8,
        filter_value2: ?[]const u8,
        filter_op: filters_mod.FilterOp,
        output_cols: []const []const u8,
    ) SurgicalEngine {
        return .{
            .allocator = allocator,
            .file = file,
            .filter_col = filter_col,
            .filter_value = filter_value,
            .filter_value2 = filter_value2,
            .filter_op = filter_op,
            .output_cols = output_cols,
        };
    }

    pub const Result = struct {
        /// Total rows in processed row groups (before filter)
        input_rows: u64,
        /// Rows that matched the filter
        output_rows: u64,
        /// Bytes actually fetched from source
        bytes_fetched: u64,
        /// Bytes that would be fetched without surgical optimization
        bytes_full_scan: u64,
        /// Execution time in milliseconds
        elapsed_ms: f64,
        /// Number of pages skipped via ColumnIndex
        pages_skipped: usize,
        /// Total pages in processed row groups
        pages_total: usize,
    };

    /// Execute the surgical query.
    pub fn execute(self: *SurgicalEngine) !Result {
        const start = try std.time.Instant.now();

        // Ensure metadata is loaded
        if (self.file.metadata == null) {
            try self.file.readFooter();
        }

        const meta = self.file.metadata orelse return error.NoMetadata;

        // Get filter column type for encoding
        const filter_col_type = self.file.getColumnType(self.filter_col) orelse return error.ColumnNotFound;

        // Create unified filter for comparison
        var filter = if (self.filter_op == .between)
            try Filter.fromBetween(self.allocator, self.filter_value, self.filter_value2 orelse return error.BetweenRequiresTwoValues, filter_col_type)
        else
            try Filter.fromPredicate(self.allocator, self.filter_op, self.filter_value, filter_col_type);
        defer filter.deinit();

        // === PHASE 1: PLAN ===
        var planner = Planner.init(self.allocator, self.file);
        var plan = try planner.buildPlan(self.filter_col, &filter, self.output_cols);
        defer plan.deinit(self.allocator);

        const summary = plan.summary();

        // Debug output
        std.debug.print("[SURGICAL] Plan: {d} active row groups, {d} bytes to fetch, {d}/{d} pages\n", .{
            plan.active_row_groups.len,
            plan.total_bytes,
            summary.fetched_pages,
            summary.total_pages,
        });

        if (plan.active_row_groups.len == 0) {
            const end = try std.time.Instant.now();
            const elapsed_ns = end.since(start);
            return Result{
                .input_rows = 0,
                .output_rows = 0,
                .bytes_fetched = 0,
                .bytes_full_scan = self.calculateFullScanBytes(&meta),
                .elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0,
                .pages_skipped = 0,
                .pages_total = 0,
            };
        }

        // === PHASE 2: FETCH ===
        var fetcher = PageFetcher.init(self.allocator, self.file.source);
        var fetch_result = try fetcher.fetch(&plan);
        defer fetch_result.deinit(self.allocator);

        // === PHASE 3: Count input rows from active row groups ===
        // Note: Full decoding will be integrated with existing decoder infrastructure.
        // For now, we measure the I/O savings from surgical planning.
        var total_input_rows: u64 = 0;
        for (plan.active_row_groups) |rg_idx| {
            total_input_rows += @intCast(meta.row_groups.items[rg_idx].num_rows);
        }

        // TODO: Actual filtering requires integrating with existing decoder.
        // For benchmarking, we report input_rows to show what would be processed.
        const total_output_rows: u64 = 0; // Placeholder until decoder integration

        const end = try std.time.Instant.now();
        const elapsed_ns = end.since(start);

        return Result{
            .input_rows = total_input_rows,
            .output_rows = total_output_rows,
            .bytes_fetched = plan.total_bytes,
            .bytes_full_scan = self.calculateFullScanBytes(&meta),
            .elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0,
            .pages_skipped = summary.total_pages - summary.fetched_pages,
            .pages_total = summary.total_pages,
        };
    }

    fn calculateFullScanBytes(self: *SurgicalEngine, meta: *const schema.FileMetaData) u64 {
        _ = self;
        var total: u64 = 0;
        for (meta.row_groups.items) |rg| {
            total += @intCast(rg.total_byte_size);
        }
        return total;
    }

    /// Execute and return just the count (no output writing)
    pub fn count(self: *SurgicalEngine) !u64 {
        const result = try self.execute();
        return result.output_rows;
    }

    /// Execute surgical query and return assembled buffers ready for RowGroupWorker.
    /// This is the main integration point - returns data in the same format that
    /// the regular pipeline provides to RowGroupWorker.
    pub fn executeAndAssemble(self: *SurgicalEngine) !SurgicalResult {
        // Ensure metadata is loaded
        if (self.file.metadata == null) {
            try self.file.readFooter();
        }

        const meta = self.file.metadata orelse return error.NoMetadata;

        // Get filter column index
        const filter_col_idx = self.file.getColumnIndexByName(self.filter_col) orelse return error.ColumnNotFound;
        const filter_col_type = self.file.getColumnType(self.filter_col) orelse return error.ColumnNotFound;

        // Create unified filter for comparison
        var filter = if (self.filter_op == .between)
            try Filter.fromBetween(self.allocator, self.filter_value, self.filter_value2 orelse return error.BetweenRequiresTwoValues, filter_col_type)
        else
            try Filter.fromPredicate(self.allocator, self.filter_op, self.filter_value, filter_col_type);
        defer filter.deinit();

        // === PHASE 1: PLAN ===
        var planner = Planner.init(self.allocator, self.file);
        var plan = try planner.buildPlan(self.filter_col, &filter, self.output_cols);
        defer plan.deinit(self.allocator);

        const summary = plan.summary();

        std.debug.print("[SURGICAL] Plan: {d} active row groups, {d} bytes to fetch, {d}/{d} pages\n", .{
            plan.active_row_groups.len,
            plan.total_bytes,
            summary.fetched_pages,
            summary.total_pages,
        });

        if (plan.active_row_groups.len == 0) {
            return SurgicalResult{
                .row_groups = &[_]AssembledRowGroup{},
                .bytes_fetched = 0,
                .bytes_full_scan = self.calculateFullScanBytes(&meta),
                .pages_skipped = 0,
                .pages_total = 0,
            };
        }

        // === PHASE 2: FETCH ===
        var fetcher = PageFetcher.init(self.allocator, self.file.source);
        var fetch_result = try fetcher.fetch(&plan);
        defer fetch_result.deinit(self.allocator);

        // === PHASE 3: ASSEMBLE ===
        var assembled_rgs = try self.allocator.alloc(AssembledRowGroup, plan.active_row_groups.len);
        errdefer self.allocator.free(assembled_rgs);

        var initialized_rgs: usize = 0;
        errdefer {
            for (0..initialized_rgs) |i| assembled_rgs[i].deinit(self.allocator);
        }

        for (plan.active_row_groups, 0..) |rg_idx, plan_rg_idx| {
            const rg_meta = meta.row_groups.items[rg_idx];

            // Assemble filter column
            const filter_chunk = rg_meta.columns.items[filter_col_idx];
            var filter_assembled = try fetch_result.filter_pages[plan_rg_idx].assemble(
                self.allocator,
                filter_chunk,
            );
            errdefer filter_assembled.deinit(self.allocator);

            // Assemble output columns
            var output_assembled = try self.allocator.alloc(AssembledColumn, self.output_cols.len);
            errdefer self.allocator.free(output_assembled);

            var initialized_cols: usize = 0;
            errdefer {
                for (0..initialized_cols) |i| output_assembled[i].deinit(self.allocator);
            }

            for (self.output_cols, 0..) |col_name, col_idx| {
                const out_col_idx = self.file.getColumnIndexByName(col_name) orelse return error.ColumnNotFound;
                const out_chunk = rg_meta.columns.items[out_col_idx];

                output_assembled[col_idx] = try fetch_result.output_pages[plan_rg_idx][col_idx].assemble(
                    self.allocator,
                    out_chunk,
                );
                initialized_cols += 1;
            }

            assembled_rgs[plan_rg_idx] = AssembledRowGroup{
                .rg_idx = rg_idx,
                .num_rows = @intCast(rg_meta.num_rows),
                .filter = filter_assembled,
                .outputs = output_assembled,
            };
            initialized_rgs += 1;
        }

        return SurgicalResult{
            .row_groups = assembled_rgs,
            .bytes_fetched = plan.total_bytes,
            .bytes_full_scan = self.calculateFullScanBytes(&meta),
            .pages_skipped = summary.total_pages - summary.fetched_pages,
            .pages_total = summary.total_pages,
        };
    }
};

/// Convenience function for quick queries
pub fn surgicalQuery(
    allocator: std.mem.Allocator,
    file: *ParquetFile,
    filter_col: []const u8,
    filter_value: []const u8,
    output_cols: []const []const u8,
) !SurgicalEngine.Result {
    var engine = SurgicalEngine.init(
        allocator,
        file,
        filter_col,
        filter_value,
        output_cols,
    );
    return engine.execute();
}
