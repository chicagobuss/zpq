const std = @import("std");
const xev = @import("xev");

const schema_mod = @import("schema.zig");
const file_mod = @import("file.zig");
const filter_mod = @import("filter.zig");
const writer_mod = @import("writer.zig");
const slot_writer_mod = @import("slot_writer.zig");
const row_group_worker_mod = @import("row_group_worker.zig");
const interface = @import("../io/interface.zig");
const factory = @import("../io/s3/factory.zig");
const s3_writer_mod = @import("../io/s3/writer.zig");

const ParquetFile = file_mod.ParquetFile;
const EncodedFilter = filter_mod.EncodedFilter;
const SlotWriter = slot_writer_mod.SlotWriter;
const RowGroupWorker = row_group_worker_mod.RowGroupWorker;
const RowGroupData = row_group_worker_mod.RowGroupData;
const FilterContext = row_group_worker_mod.FilterContext;
const SlotWriteCompletion = row_group_worker_mod.SlotWriteCompletion;
const SlotWriteCompletionGen = row_group_worker_mod.SlotWriteCompletionGen;
const WorkerCompletion = row_group_worker_mod.WorkerCompletion;
const Range = interface.Range;
const MemorySource = interface.local.MemorySource;
const SchemaElement = schema_mod.SchemaElement;
const Type = schema_mod.Type;
const ColumnChunk = schema_mod.ColumnChunk;

/// Execution mode for pipeline operations.
pub const ExecutionMode = enum {
    /// Sequential processing - one row group at a time
    sequential,
    /// Parallel workers with sequential output
    parallel,
    /// Slot-based parallel writes via pwrite (default)
    slot_parallel,
};

/// Filter predicate with column, operator, and value.
pub const Predicate = struct {
    column: []const u8,
    op: Operator,
    value: []const u8,

    pub const Operator = enum {
        eq, // =
        gt, // >
        lt, // <
        gte, // >=
        lte, // <=
    };

    /// Parse a predicate string like "col=val" or "col>10"
    pub fn parse(expr: []const u8) !Predicate {
        // Try two-char operators first
        if (std.mem.indexOf(u8, expr, ">=")) |idx| {
            return .{ .column = expr[0..idx], .op = .gte, .value = expr[idx + 2 ..] };
        }
        if (std.mem.indexOf(u8, expr, "<=")) |idx| {
            return .{ .column = expr[0..idx], .op = .lte, .value = expr[idx + 2 ..] };
        }
        // Single-char operators
        if (std.mem.indexOfScalar(u8, expr, '=')) |idx| {
            return .{ .column = expr[0..idx], .op = .eq, .value = expr[idx + 1 ..] };
        }
        if (std.mem.indexOfScalar(u8, expr, '>')) |idx| {
            return .{ .column = expr[0..idx], .op = .gt, .value = expr[idx + 1 ..] };
        }
        if (std.mem.indexOfScalar(u8, expr, '<')) |idx| {
            return .{ .column = expr[0..idx], .op = .lt, .value = expr[idx + 1 ..] };
        }
        return error.InvalidPredicateFormat;
    }
};

/// Result of pipeline execution.
pub const ExecutionResult = struct {
    input_rows: u64,
    output_rows: u64,
    elapsed_ms: f64,
};

/// Composable pipeline for Parquet operations.
///
/// Usage:
///   var pipeline = Pipeline.init(allocator);
///   defer pipeline.deinit();
///   pipeline.setInput("input.parquet");
///   try pipeline.setFilter("category=A");
///   try pipeline.setProjection("id,name,value");
///   pipeline.setOutput("output.parquet");
///   pipeline.setRuntime(&loop, &thread_pool);
///   const result = try pipeline.execute(.slot_parallel);
///
pub const Pipeline = struct {
    const Self = @This();

    allocator: std.mem.Allocator,

    // Input
    input_path: ?[]const u8 = null,
    input_file: ?*ParquetFile = null,

    // Operations
    predicate: ?Predicate = null,
    projection: ?[]const []const u8 = null,

    // Output
    output_path: ?[]const u8 = null,

    // Runtime context (required for parallel execution)
    // Uses xev.Dynamic for runtime backend selection (io_uring -> epoll fallback)
    loop: ?*xev.Dynamic.Loop = null,
    thread_pool: ?*xev.ThreadPool = null,

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{ .allocator = allocator };
    }

    pub fn deinit(self: *Self) void {
        if (self.input_file) |pf| {
            pf.deinit();
            self.allocator.destroy(pf);
        }
        if (self.projection) |cols| {
            self.allocator.free(cols);
        }
    }

    /// Set input path (local file or s3://)
    pub fn setInput(self: *Self, path: []const u8) void {
        self.input_path = path;
    }

    /// Set output path (local file or s3://)
    pub fn setOutput(self: *Self, path: []const u8) void {
        self.output_path = path;
    }

    /// Set filter predicate from expression string
    pub fn setFilter(self: *Self, expr: []const u8) !void {
        self.predicate = try Predicate.parse(expr);
    }

    /// Set column projection from comma-separated string
    pub fn setProjection(self: *Self, cols_str: []const u8) !void {
        var col_list = std.ArrayListUnmanaged([]const u8){};
        defer col_list.deinit(self.allocator);

        var iter = std.mem.splitScalar(u8, cols_str, ',');
        while (iter.next()) |col| {
            const trimmed = std.mem.trim(u8, col, " ");
            if (trimmed.len > 0) {
                try col_list.append(self.allocator, trimmed);
            }
        }

        self.projection = try col_list.toOwnedSlice(self.allocator);
    }

    /// Set runtime context for parallel operations.
    /// Uses xev.Dynamic for runtime backend selection (io_uring -> epoll fallback).
    pub fn setRuntime(self: *Self, loop: *xev.Dynamic.Loop, pool: *xev.ThreadPool) void {
        self.loop = loop;
        self.thread_pool = pool;
    }

    /// Open the input file and read metadata
    pub fn open(self: *Self) !void {
        const path = self.input_path orelse return error.NoInputPath;

        // Use factory to handle both local and s3:// paths
        const pf = try self.allocator.create(ParquetFile);
        errdefer self.allocator.destroy(pf);

        pf.* = try factory.openFileWithOptions(self.allocator, path, .{
            .force_async = false,
            .loop = self.loop,
            .thread_pool = self.thread_pool,
        });
        try pf.readFooter();

        self.input_file = pf;
    }

    /// Open with a generic loop type (for non-default xev backends like Epoll)
    pub fn openWithLoop(self: *Self, loop: anytype, pool: *xev.ThreadPool) !void {
        const path = self.input_path orelse return error.NoInputPath;

        const pf = try self.allocator.create(ParquetFile);
        errdefer self.allocator.destroy(pf);

        pf.* = try factory.openS3WithLoop(self.allocator, loop, pool, path, .{
            .force_async = false,
        });
        try pf.readFooter();

        self.input_file = pf;
    }

    /// Execute the pipeline with the given mode.
    /// Uses xev.Dynamic for runtime backend selection (io_uring -> epoll fallback).
    pub fn execute(self: *Self, mode: ExecutionMode) !ExecutionResult {
        if (self.input_file == null) {
            try self.open();
        }

        return switch (mode) {
            .sequential => self.executeSequential(),
            .parallel => self.executeParallel(),
            .slot_parallel => self.executeSlotParallel(),
        };
    }

    /// Execute with a generic xev backend (for Lambda/Epoll support)
    /// This is the unibin-compatible entry point that works with any xev backend.
    pub fn executeWithLoop(self: *Self, comptime XevApi: type, loop: *XevApi.Loop, pool: *xev.ThreadPool, mode: ExecutionMode) !ExecutionResult {
        if (self.input_file == null) {
            try self.openWithLoop(loop, pool);
        }

        return switch (mode) {
            .sequential => self.executeSequential(),
            .parallel => self.executeParallel(),
            .slot_parallel => self.executeSlotParallelWithLoop(XevApi, loop, pool),
        };
    }

    /// Print schema to stdout
    pub fn printSchema(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("Schema ({d} columns):\n", .{meta.schema.items.len - 1});
        for (meta.schema.items[1..], 0..) |elem, i| {
            const type_str = @tagName(elem.type orelse .BOOLEAN);
            std.debug.print("  {d}: {s} ({s})\n", .{ i, elem.name, type_str });
        }
    }

    /// Print file metadata to stdout
    pub fn printMeta(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("File: {s}\n", .{self.input_path.?});
        std.debug.print("  Rows: {d}\n", .{meta.num_rows});
        std.debug.print("  Row Groups: {d}\n", .{meta.row_groups.items.len});
        std.debug.print("  Columns: {d}\n", .{meta.schema.items.len - 1});
        if (meta.created_by) |cb| {
            std.debug.print("  Created by: {s}\n", .{cb});
        }
    }

    /// Schema field info for programmatic access
    pub const SchemaFieldInfo = struct {
        index: usize,
        name: []const u8,
        type_name: []const u8,
    };

    /// File metadata for programmatic access
    pub const FileMetaInfo = struct {
        path: []const u8,
        row_count: u64,
        row_group_count: usize,
        column_count: usize,
        created_by: ?[]const u8,
    };

    /// Get schema as struct array (for Lambda/HTTP responses)
    /// Caller owns the returned slice and all strings within it.
    pub fn getSchema(self: *Self, allocator: std.mem.Allocator) ![]SchemaFieldInfo {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        const col_count = meta.schema.items.len - 1;
        var fields = try allocator.alloc(SchemaFieldInfo, col_count);
        errdefer allocator.free(fields);

        for (meta.schema.items[1..], 0..) |elem, i| {
            fields[i] = .{
                .index = i,
                .name = try allocator.dupe(u8, elem.name),
                .type_name = @tagName(elem.type orelse .BOOLEAN),
            };
        }

        return fields;
    }

    /// Get file metadata as struct (for Lambda/HTTP responses)
    /// Caller owns the returned strings (path, created_by).
    pub fn getMeta(self: *Self, allocator: std.mem.Allocator) !FileMetaInfo {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        return .{
            .path = try allocator.dupe(u8, self.input_path.?),
            .row_count = @intCast(meta.num_rows),
            .row_group_count = meta.row_groups.items.len,
            .column_count = meta.schema.items.len - 1,
            .created_by = if (meta.created_by) |cb| try allocator.dupe(u8, cb) else null,
        };
    }

    /// Print summary (row count with filter applied if set)
    pub fn printSummary(self: *Self) !void {
        if (self.input_file == null) {
            try self.open();
        }
        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        std.debug.print("File: {s}\n", .{self.input_path.?});
        std.debug.print("  Total rows: {d}\n", .{meta.num_rows});
        std.debug.print("  Row groups: {d}\n", .{meta.row_groups.items.len});

        if (self.predicate) |pred| {
            std.debug.print("  Filter: {s} {s} {s}\n", .{
                pred.column,
                switch (pred.op) {
                    .eq => "=",
                    .gt => ">",
                    .lt => "<",
                    .gte => ">=",
                    .lte => "<=",
                },
                pred.value,
            });
        }

        if (self.projection) |cols| {
            std.debug.print("  Select: ", .{});
            for (cols, 0..) |col, i| {
                if (i > 0) std.debug.print(", ", .{});
                std.debug.print("{s}", .{col});
            }
            std.debug.print("\n", .{});
        }
    }

    // ========================================================================
    // Execution implementations
    // ========================================================================

    fn executeSequential(self: *Self) !ExecutionResult {
        _ = self;
        // Sequential mode not implemented - use slot_parallel (default)
        return error.NotImplemented;
    }

    fn executeParallel(self: *Self) !ExecutionResult {
        _ = self;
        // Parallel mode not implemented - use slot_parallel (default)
        return error.NotImplemented;
    }

    /// Slot-parallel execution: parallel filter + encode + pwrite per row group.
    fn executeSlotParallel(self: *Self) !ExecutionResult {
        var timer = try std.time.Timer.start();

        // Validate requirements
        const output_path = self.output_path orelse return error.NoOutputPath;
        const pred = self.predicate orelse return error.NoFilter;
        const loop = self.loop orelse return error.NoRuntime;
        const thread_pool = self.thread_pool orelse return error.NoRuntime;

        // Only equality filter supported for now
        if (pred.op != .eq) return error.UnsupportedFilterOperator;

        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        // Find filter column index and type
        var filter_col_idx: ?usize = null;
        var filter_col_type: ?Type = null;
        if (meta.row_groups.items.len > 0) {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], pred.column)) {
                        filter_col_idx = idx;
                        filter_col_type = md.type;
                        break;
                    }
                }
            }
        }

        if (filter_col_idx == null) return error.ColumnNotFound;

        // Build output column list
        var output_col_indices = std.ArrayListUnmanaged(usize){};
        defer output_col_indices.deinit(self.allocator);
        var output_col_types = std.ArrayListUnmanaged(Type){};
        defer output_col_types.deinit(self.allocator);
        var output_col_names = std.ArrayListUnmanaged([]const u8){};
        defer output_col_names.deinit(self.allocator);

        var filter_col_in_output = false;
        var filter_col_output_idx: ?usize = null;

        if (self.projection) |proj_cols| {
            // Select specified columns
            for (proj_cols) |col_name| {
                for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                    if (col.meta_data) |md| {
                        const path_parts = md.path_in_schema.items;
                        if (std.mem.eql(u8, path_parts[path_parts.len - 1], col_name)) {
                            try output_col_indices.append(self.allocator, idx);
                            try output_col_types.append(self.allocator, md.type);
                            try output_col_names.append(self.allocator, col_name);
                            if (idx == filter_col_idx.?) {
                                filter_col_in_output = true;
                                filter_col_output_idx = output_col_indices.items.len - 1;
                            }
                            break;
                        }
                    }
                }
            }
        } else {
            // Select all columns
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    try output_col_indices.append(self.allocator, idx);
                    try output_col_types.append(self.allocator, md.type);
                    try output_col_names.append(self.allocator, path_parts[path_parts.len - 1]);
                    if (idx == filter_col_idx.?) {
                        filter_col_in_output = true;
                        filter_col_output_idx = output_col_indices.items.len - 1;
                    }
                }
            }
        }

        // Create encoded filter
        var encoded_filter = try EncodedFilter.parse(self.allocator, pred.value, filter_col_type.?);
        defer encoded_filter.deinit();

        // =====================================================================
        // PHASE 1: Pre-fetch ALL columns for ALL row groups
        // =====================================================================
        const rg_count = meta.row_groups.items.len;
        const col_count = output_col_indices.items.len;

        // If filter column is not in output, we need to fetch it separately
        const need_separate_filter_fetch = !filter_col_in_output;

        var all_rg_data = try self.allocator.alloc(RowGroupData, rg_count);
        defer self.allocator.free(all_rg_data);

        var all_output_bufs = try self.allocator.alloc([][]u8, rg_count);
        defer {
            for (all_output_bufs) |bufs| {
                for (bufs) |buf| self.allocator.free(buf);
                self.allocator.free(bufs);
            }
            self.allocator.free(all_output_bufs);
        }

        var all_output_offsets = try self.allocator.alloc([]u64, rg_count);
        defer {
            for (all_output_offsets) |offs| self.allocator.free(offs);
            self.allocator.free(all_output_offsets);
        }

        var all_output_chunks = try self.allocator.alloc([]ColumnChunk, rg_count);
        defer {
            for (all_output_chunks) |chunks| self.allocator.free(chunks);
            self.allocator.free(all_output_chunks);
        }

        // Separate filter column buffers (only used when filter col not in output)
        var all_filter_bufs: ?[][]u8 = null;
        var all_filter_offsets: ?[]u64 = null;
        var all_filter_chunks: ?[]ColumnChunk = null;
        defer {
            if (all_filter_bufs) |bufs| {
                for (bufs) |buf| self.allocator.free(buf);
                self.allocator.free(bufs);
            }
            if (all_filter_offsets) |offs| self.allocator.free(offs);
            if (all_filter_chunks) |chunks| self.allocator.free(chunks);
        }

        if (need_separate_filter_fetch) {
            all_filter_bufs = try self.allocator.alloc([]u8, rg_count);
            all_filter_offsets = try self.allocator.alloc(u64, rg_count);
            all_filter_chunks = try self.allocator.alloc(ColumnChunk, rg_count);
        }

        // Calculate max row group size for slot allocation
        var max_input_rg_size: u64 = 0;
        var total_ranges: usize = 0;
        for (meta.row_groups.items) |rg_meta| {
            total_ranges += col_count;
            if (need_separate_filter_fetch) total_ranges += 1; // Extra range for filter column
            const rg_size: u64 = @intCast(rg_meta.total_byte_size);
            if (rg_size > max_input_rg_size) {
                max_input_rg_size = rg_size;
            }
        }

        var ranges = try self.allocator.alloc(Range, total_ranges);
        defer self.allocator.free(ranges);
        var buffers = try self.allocator.alloc([]u8, total_ranges);
        defer self.allocator.free(buffers);

        var range_idx: usize = 0;
        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            all_output_bufs[rg_idx] = try self.allocator.alloc([]u8, col_count);
            all_output_offsets[rg_idx] = try self.allocator.alloc(u64, col_count);
            all_output_chunks[rg_idx] = try self.allocator.alloc(ColumnChunk, col_count);

            // Fetch output columns
            for (output_col_indices.items, 0..) |col_idx, out_col_idx| {
                const chunk = rg_meta.columns.items[col_idx];
                const md = chunk.meta_data.?;

                var start: u64 = @intCast(md.data_page_offset);
                if (md.dictionary_page_offset) |dpo| {
                    if (dpo < start) start = @intCast(dpo);
                }
                const len: u64 = @intCast(md.total_compressed_size);

                ranges[range_idx] = .{ .start = start, .end = start + len };
                buffers[range_idx] = try self.allocator.alloc(u8, @intCast(len));

                all_output_bufs[rg_idx][out_col_idx] = buffers[range_idx];
                all_output_offsets[rg_idx][out_col_idx] = start;
                all_output_chunks[rg_idx][out_col_idx] = chunk;

                range_idx += 1;
            }

            // Fetch filter column separately if not in output
            if (need_separate_filter_fetch) {
                const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
                const filter_md = filter_chunk.meta_data.?;

                var filter_start: u64 = @intCast(filter_md.data_page_offset);
                if (filter_md.dictionary_page_offset) |dpo| {
                    if (dpo < filter_start) filter_start = @intCast(dpo);
                }
                const filter_len: u64 = @intCast(filter_md.total_compressed_size);

                ranges[range_idx] = .{ .start = filter_start, .end = filter_start + filter_len };
                buffers[range_idx] = try self.allocator.alloc(u8, @intCast(filter_len));

                all_filter_bufs.?[rg_idx] = buffers[range_idx];
                all_filter_offsets.?[rg_idx] = filter_start;
                all_filter_chunks.?[rg_idx] = filter_chunk;

                range_idx += 1;
            }
        }

        try pf.source.readRanges(ranges, buffers);

        // =====================================================================
        // PHASE 2: Create RowGroupData and FilterContext
        // =====================================================================
        // Find filter column index in output buffers (only if filter col is in output)
        var filter_col_buf_idx: usize = 0;
        if (filter_col_in_output) {
            for (output_col_indices.items, 0..) |col_idx, i| {
                if (col_idx == filter_col_idx.?) {
                    filter_col_buf_idx = i;
                    break;
                }
            }
        }

        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            // Get filter column data - either from output buffers or separate filter buffers
            const filter_buf = if (filter_col_in_output)
                all_output_bufs[rg_idx][filter_col_buf_idx]
            else
                all_filter_bufs.?[rg_idx];

            const filter_offset = if (filter_col_in_output) blk: {
                const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
                const filter_md = filter_chunk.meta_data.?;
                var filter_start: u64 = @intCast(filter_md.data_page_offset);
                if (filter_md.dictionary_page_offset) |dpo| {
                    if (dpo < filter_start) filter_start = @intCast(dpo);
                }
                break :blk filter_start;
            } else all_filter_offsets.?[rg_idx];

            const filter_chunk = if (filter_col_in_output)
                rg_meta.columns.items[filter_col_idx.?]
            else
                all_filter_chunks.?[rg_idx];

            const const_bufs: []const []const u8 = @ptrCast(all_output_bufs[rg_idx]);

            all_rg_data[rg_idx] = RowGroupData{
                .rg_idx = rg_idx,
                .num_rows = @intCast(rg_meta.num_rows),
                .filter_buf = filter_buf,
                .filter_offset = filter_offset,
                .filter_chunk = filter_chunk,
                .output_bufs = const_bufs,
                .output_offsets = all_output_offsets[rg_idx],
                .output_chunks = all_output_chunks[rg_idx],
            };
        }

        const filter_ctx = FilterContext{
            .allocator = self.allocator,
            .filter_col_name = pred.column,
            .filter_val = pred.value,
            .filter_col_idx = filter_col_idx.?,
            .filter_col_type = filter_col_type.?,
            .encoded_filter = &encoded_filter,
            .output_col_indices = output_col_indices.items,
            .output_col_types = output_col_types.items,
            .output_col_names = output_col_names.items,
            .filter_col_in_output = filter_col_in_output,
            .filter_col_output_idx = filter_col_output_idx,
            .meta = &meta,
        };

        // =====================================================================
        // PHASE 3: Initialize SlotWriter with pre-computed offsets
        // =====================================================================
        var output_schema = std.ArrayListUnmanaged(SchemaElement){};
        defer output_schema.deinit(self.allocator);

        // Root element
        try output_schema.append(self.allocator, SchemaElement{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = @intCast(output_col_names.items.len),
            .scale = null,
            .precision = null,
            .field_id = null,
        });

        // Column elements
        for (output_col_names.items, output_col_types.items) |name, col_type| {
            try output_schema.append(self.allocator, SchemaElement{
                .type = col_type,
                .type_length = null,
                .repetition_type = .REQUIRED,
                .name = name,
                .num_children = null,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
        }

        // Detect S3 output and use temp file if needed
        const is_s3_output = std.mem.startsWith(u8, output_path, "s3://");
        var temp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const effective_output_path = if (is_s3_output) blk: {
            // Generate temp file path for S3 staging using current time
            const now = std.time.Instant.now() catch return error.ClockUnavailable;
            const ts_value = if (@hasField(@TypeOf(now.timestamp), "sec"))
                @as(i128, now.timestamp.sec) * 1_000_000_000 + now.timestamp.nsec
            else if (@hasField(@TypeOf(now.timestamp), "tv_sec"))
                @as(i128, now.timestamp.tv_sec) * 1_000_000_000 + now.timestamp.tv_nsec
            else
                @as(i128, now.timestamp);
            const temp_path = std.fmt.bufPrint(&temp_path_buf, "/tmp/zpq_s3_stage_{d}.parquet", .{
                ts_value,
            }) catch return error.TempPathTooLong;
            break :blk temp_path;
        } else output_path;

        var sw = try SlotWriter.init(
            self.allocator,
            effective_output_path,
            rg_count,
            max_input_rg_size,
            output_schema.items,
        );
        defer sw.deinit();

        // =====================================================================
        // PHASE 4: Execute workers in PARALLEL with slot writes
        // =====================================================================
        var total_input_rows: u64 = 0;

        var workers = try self.allocator.alloc(*RowGroupWorker, rg_count);
        defer self.allocator.free(workers);

        var completions = try self.allocator.alloc(SlotWriteCompletion, rg_count);
        defer {
            for (completions) |*c| c.deinit(self.allocator);
            self.allocator.free(completions);
        }

        var pending = std.atomic.Value(usize).init(rg_count);

        for (all_rg_data, 0..) |*rg_data, i| {
            total_input_rows += rg_data.num_rows;
            workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
            completions[i] = try SlotWriteCompletion.init(
                workers[i],
                &sw,
                i,
                .SNAPPY,
                &pending,
            );
        }

        for (completions) |*c| {
            c.scheduleOn(loop, thread_pool);
        }

        while (pending.load(.acquire) > 0) {
            loop.run(.once) catch |err| {
                std.debug.print("Loop error: {}\n", .{err});
                break;
            };
        }

        // =====================================================================
        // PHASE 5: Collect metadata and finish (write footer)
        // =====================================================================
        var total_output_rows: u64 = 0;

        var rg_metas = try self.allocator.alloc(slot_writer_mod.RowGroupMeta, rg_count);
        defer self.allocator.free(rg_metas);

        for (completions, workers, 0..) |*c, worker, i| {
            defer worker.deinit();

            if (c.task_error) |err| {
                return err;
            }

            total_output_rows += worker.row_count;
            rg_metas[i] = c.getRowGroupMeta();
        }

        try sw.finish(rg_metas);

        // Upload to S3 if needed
        if (is_s3_output) {
            try self.uploadToS3WithLoop(xev.Dynamic, loop, thread_pool, effective_output_path, output_path);
            // Clean up temp file
            std.fs.cwd().deleteFile(effective_output_path) catch |err| {
                std.debug.print("Warning: could not delete temp file {s}: {}\n", .{ effective_output_path, err });
            };
        }

        const elapsed_ns = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

        return ExecutionResult{
            .input_rows = total_input_rows,
            .output_rows = total_output_rows,
            .elapsed_ms = elapsed_ms,
        };
    }

    /// Generic slot-parallel execution with any xev backend (for unibin Lambda support).
    /// This mirrors executeSlotParallel but uses SlotWriteCompletionGen for the specified XevApi.
    fn executeSlotParallelWithLoop(self: *Self, comptime XevApi: type, loop: *XevApi.Loop, thread_pool: *xev.ThreadPool) !ExecutionResult {
        const Completion = SlotWriteCompletionGen(XevApi);

        var timer = try std.time.Timer.start();

        // Validate requirements
        const output_path = self.output_path orelse return error.NoOutputPath;
        const pred = self.predicate orelse return error.NoFilter;

        if (pred.op != .eq) return error.UnsupportedFilterOperator;

        const pf = self.input_file.?;
        const meta = pf.metadata orelse return error.NoMetadata;

        // Find filter column index and type
        var filter_col_idx: ?usize = null;
        var filter_col_type: ?Type = null;
        if (meta.row_groups.items.len > 0) {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], pred.column)) {
                        filter_col_idx = idx;
                        filter_col_type = md.type;
                        break;
                    }
                }
            }
        }

        if (filter_col_idx == null) return error.ColumnNotFound;

        // Build output column list (same as executeSlotParallel)
        var output_col_indices = std.ArrayListUnmanaged(usize){};
        defer output_col_indices.deinit(self.allocator);
        var output_col_types = std.ArrayListUnmanaged(Type){};
        defer output_col_types.deinit(self.allocator);
        var output_col_names = std.ArrayListUnmanaged([]const u8){};
        defer output_col_names.deinit(self.allocator);

        var filter_col_in_output = false;
        var filter_col_output_idx: ?usize = null;

        if (self.projection) |proj_cols| {
            for (proj_cols) |col_name| {
                for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                    if (col.meta_data) |md| {
                        const path_parts = md.path_in_schema.items;
                        if (std.mem.eql(u8, path_parts[path_parts.len - 1], col_name)) {
                            try output_col_indices.append(self.allocator, idx);
                            try output_col_types.append(self.allocator, md.type);
                            try output_col_names.append(self.allocator, col_name);
                            if (idx == filter_col_idx.?) {
                                filter_col_in_output = true;
                                filter_col_output_idx = output_col_indices.items.len - 1;
                            }
                            break;
                        }
                    }
                }
            }
        } else {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    try output_col_indices.append(self.allocator, idx);
                    try output_col_types.append(self.allocator, md.type);
                    try output_col_names.append(self.allocator, path_parts[path_parts.len - 1]);
                    if (idx == filter_col_idx.?) {
                        filter_col_in_output = true;
                        filter_col_output_idx = output_col_indices.items.len - 1;
                    }
                }
            }
        }

        var encoded_filter = try EncodedFilter.parse(self.allocator, pred.value, filter_col_type.?);
        defer encoded_filter.deinit();

        const rg_count = meta.row_groups.items.len;
        const col_count = output_col_indices.items.len;
        const need_separate_filter_fetch = !filter_col_in_output;

        // Allocate row group data structures
        var all_rg_data = try self.allocator.alloc(RowGroupData, rg_count);
        defer self.allocator.free(all_rg_data);

        var all_output_bufs = try self.allocator.alloc([][]u8, rg_count);
        defer {
            for (all_output_bufs) |bufs| {
                for (bufs) |buf| self.allocator.free(buf);
                self.allocator.free(bufs);
            }
            self.allocator.free(all_output_bufs);
        }

        var all_output_offsets = try self.allocator.alloc([]u64, rg_count);
        defer {
            for (all_output_offsets) |offs| self.allocator.free(offs);
            self.allocator.free(all_output_offsets);
        }

        var all_output_chunks = try self.allocator.alloc([]ColumnChunk, rg_count);
        defer {
            for (all_output_chunks) |chunks| self.allocator.free(chunks);
            self.allocator.free(all_output_chunks);
        }

        var all_filter_bufs: ?[][]u8 = null;
        var all_filter_offsets: ?[]u64 = null;
        var all_filter_chunks: ?[]ColumnChunk = null;
        defer {
            if (all_filter_bufs) |bufs| {
                for (bufs) |buf| self.allocator.free(buf);
                self.allocator.free(bufs);
            }
            if (all_filter_offsets) |offs| self.allocator.free(offs);
            if (all_filter_chunks) |chunks| self.allocator.free(chunks);
        }

        if (need_separate_filter_fetch) {
            all_filter_bufs = try self.allocator.alloc([]u8, rg_count);
            all_filter_offsets = try self.allocator.alloc(u64, rg_count);
            all_filter_chunks = try self.allocator.alloc(ColumnChunk, rg_count);
        }

        var max_input_rg_size: u64 = 0;
        var total_ranges: usize = 0;
        for (meta.row_groups.items) |rg_meta| {
            total_ranges += col_count;
            if (need_separate_filter_fetch) total_ranges += 1;
            const rg_size: u64 = @intCast(rg_meta.total_byte_size);
            if (rg_size > max_input_rg_size) max_input_rg_size = rg_size;
        }

        var ranges = try self.allocator.alloc(Range, total_ranges);
        defer self.allocator.free(ranges);
        var buffers = try self.allocator.alloc([]u8, total_ranges);
        defer self.allocator.free(buffers);

        var range_idx: usize = 0;
        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            all_output_bufs[rg_idx] = try self.allocator.alloc([]u8, col_count);
            all_output_offsets[rg_idx] = try self.allocator.alloc(u64, col_count);
            all_output_chunks[rg_idx] = try self.allocator.alloc(ColumnChunk, col_count);

            for (output_col_indices.items, 0..) |col_idx, out_col_idx| {
                const chunk = rg_meta.columns.items[col_idx];
                const md = chunk.meta_data.?;
                var start: u64 = @intCast(md.data_page_offset);
                if (md.dictionary_page_offset) |dpo| {
                    if (dpo < start) start = @intCast(dpo);
                }
                const len: u64 = @intCast(md.total_compressed_size);

                ranges[range_idx] = .{ .start = start, .end = start + len };
                buffers[range_idx] = try self.allocator.alloc(u8, @intCast(len));
                all_output_bufs[rg_idx][out_col_idx] = buffers[range_idx];
                all_output_offsets[rg_idx][out_col_idx] = start;
                all_output_chunks[rg_idx][out_col_idx] = chunk;
                range_idx += 1;
            }

            if (need_separate_filter_fetch) {
                const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
                const filter_md = filter_chunk.meta_data.?;
                var filter_start: u64 = @intCast(filter_md.data_page_offset);
                if (filter_md.dictionary_page_offset) |dpo| {
                    if (dpo < filter_start) filter_start = @intCast(dpo);
                }
                const filter_len: u64 = @intCast(filter_md.total_compressed_size);

                ranges[range_idx] = .{ .start = filter_start, .end = filter_start + filter_len };
                buffers[range_idx] = try self.allocator.alloc(u8, @intCast(filter_len));
                all_filter_bufs.?[rg_idx] = buffers[range_idx];
                all_filter_offsets.?[rg_idx] = filter_start;
                all_filter_chunks.?[rg_idx] = filter_chunk;
                range_idx += 1;
            }
        }

        try pf.source.readRanges(ranges, buffers);

        // Build RowGroupData
        var filter_col_buf_idx: usize = 0;
        if (filter_col_in_output) {
            for (output_col_indices.items, 0..) |col_idx, i| {
                if (col_idx == filter_col_idx.?) {
                    filter_col_buf_idx = i;
                    break;
                }
            }
        }

        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            const filter_buf = if (filter_col_in_output)
                all_output_bufs[rg_idx][filter_col_buf_idx]
            else
                all_filter_bufs.?[rg_idx];

            const filter_offset = if (filter_col_in_output) blk: {
                const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
                const filter_md = filter_chunk.meta_data.?;
                var filter_start: u64 = @intCast(filter_md.data_page_offset);
                if (filter_md.dictionary_page_offset) |dpo| {
                    if (dpo < filter_start) filter_start = @intCast(dpo);
                }
                break :blk filter_start;
            } else all_filter_offsets.?[rg_idx];

            const filter_chunk = if (filter_col_in_output)
                rg_meta.columns.items[filter_col_idx.?]
            else
                all_filter_chunks.?[rg_idx];

            const const_bufs: []const []const u8 = @ptrCast(all_output_bufs[rg_idx]);
            all_rg_data[rg_idx] = RowGroupData{
                .rg_idx = rg_idx,
                .num_rows = @intCast(rg_meta.num_rows),
                .filter_buf = filter_buf,
                .filter_offset = filter_offset,
                .filter_chunk = filter_chunk,
                .output_bufs = const_bufs,
                .output_offsets = all_output_offsets[rg_idx],
                .output_chunks = all_output_chunks[rg_idx],
            };
        }

        const filter_ctx = FilterContext{
            .allocator = self.allocator,
            .filter_col_name = pred.column,
            .filter_val = pred.value,
            .filter_col_idx = filter_col_idx.?,
            .filter_col_type = filter_col_type.?,
            .encoded_filter = &encoded_filter,
            .output_col_indices = output_col_indices.items,
            .output_col_types = output_col_types.items,
            .output_col_names = output_col_names.items,
            .filter_col_in_output = filter_col_in_output,
            .filter_col_output_idx = filter_col_output_idx,
            .meta = &meta,
        };

        // Setup SlotWriter
        var output_schema = std.ArrayListUnmanaged(SchemaElement){};
        defer output_schema.deinit(self.allocator);

        try output_schema.append(self.allocator, SchemaElement{
            .type = null,
            .type_length = null,
            .repetition_type = null,
            .name = "schema",
            .num_children = @intCast(output_col_names.items.len),
            .scale = null,
            .precision = null,
            .field_id = null,
        });

        for (output_col_names.items, output_col_types.items) |name, col_type| {
            try output_schema.append(self.allocator, SchemaElement{
                .type = col_type,
                .type_length = null,
                .repetition_type = .REQUIRED,
                .name = name,
                .num_children = null,
                .scale = null,
                .precision = null,
                .field_id = null,
            });
        }

        const is_s3_output = std.mem.startsWith(u8, output_path, "s3://");
        var temp_path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const effective_output_path = if (is_s3_output) blk: {
            const now = std.time.Instant.now() catch return error.ClockUnavailable;
            const ts_value = if (@hasField(@TypeOf(now.timestamp), "sec"))
                @as(i128, now.timestamp.sec) * 1_000_000_000 + now.timestamp.nsec
            else if (@hasField(@TypeOf(now.timestamp), "tv_sec"))
                @as(i128, now.timestamp.tv_sec) * 1_000_000_000 + now.timestamp.tv_nsec
            else
                @as(i128, now.timestamp);
            const temp_path = std.fmt.bufPrint(&temp_path_buf, "/tmp/zpq_s3_stage_{d}.parquet", .{ts_value}) catch return error.TempPathTooLong;
            break :blk temp_path;
        } else output_path;

        var sw = try SlotWriter.init(self.allocator, effective_output_path, rg_count, max_input_rg_size, output_schema.items);
        defer sw.deinit();

        // Execute workers with generic completion type
        var total_input_rows: u64 = 0;

        var workers = try self.allocator.alloc(*RowGroupWorker, rg_count);
        defer self.allocator.free(workers);

        var completions = try self.allocator.alloc(Completion, rg_count);
        defer {
            for (completions) |*c| c.deinit(self.allocator);
            self.allocator.free(completions);
        }

        var pending = std.atomic.Value(usize).init(rg_count);

        for (all_rg_data, 0..) |*rg_data, i| {
            total_input_rows += rg_data.num_rows;
            workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
            completions[i] = try Completion.init(workers[i], &sw, i, .SNAPPY, &pending);
        }

        for (completions) |*c| {
            c.scheduleOn(loop, thread_pool);
        }

        while (pending.load(.acquire) > 0) {
            loop.run(.once) catch |err| {
                std.debug.print("Loop error: {}\n", .{err});
                break;
            };
        }

        // Collect results
        var total_output_rows: u64 = 0;
        var rg_metas = try self.allocator.alloc(slot_writer_mod.RowGroupMeta, rg_count);
        defer self.allocator.free(rg_metas);

        for (completions, workers, 0..) |*c, worker, i| {
            defer worker.deinit();
            if (c.task_error) |err| return err;
            total_output_rows += worker.row_count;
            rg_metas[i] = c.getRowGroupMeta();
        }

        try sw.finish(rg_metas);

        if (is_s3_output) {
            try self.uploadToS3WithLoop(XevApi, loop, thread_pool, effective_output_path, output_path);
            std.fs.cwd().deleteFile(effective_output_path) catch {};
        }

        const elapsed_ns = timer.read();
        const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

        return ExecutionResult{
            .input_rows = total_input_rows,
            .output_rows = total_output_rows,
            .elapsed_ms = elapsed_ms,
        };
    }

    /// Upload to S3 with generic xev backend
    fn uploadToS3WithLoop(self: *Self, comptime XevApi: type, loop: *XevApi.Loop, pool: *xev.ThreadPool, local_path: []const u8, s3_path: []const u8) !void {
        const S3Writer = s3_writer_mod.S3WriterGen(XevApi);

        if (!std.mem.startsWith(u8, s3_path, "s3://")) return error.InvalidS3Path;
        const path_without_prefix = s3_path[5..];
        const slash_idx = std.mem.indexOf(u8, path_without_prefix, "/") orelse return error.InvalidS3Path;
        const bucket = path_without_prefix[0..slash_idx];
        const key = path_without_prefix[slash_idx + 1 ..];

        const config = try factory.loadS3ConfigFromEnv(self.allocator);
        defer config.deinit(self.allocator);

        const discovery = try factory.discoverS3Endpoint(self.allocator, config.endpoint, config.region);
        defer discovery.deinit(self.allocator);

        var s3w = try S3Writer.initWithLoop(self.allocator, loop, pool, bucket, key, config.region);
        defer s3w.deinit();

        // Only override host/port/tls for custom endpoints (MinIO, R2, LocalStack)
        if (config.endpoint != null) {
            self.allocator.free(s3w.host);
            s3w.host = try self.allocator.dupe(u8, discovery.host);
            s3w.port = discovery.port;
            s3w.use_tls = discovery.use_tls;
            s3w.use_path_style = true;
        }

        if (config.credentials) |creds| {
            try s3w.setCredentials(creds.access_key, creds.secret_key, creds.session_token);
        }

        const file = try std.fs.cwd().openFile(local_path, .{});
        defer file.close();

        const file_size = try file.getEndPos();
        const chunk_size: usize = 8 * 1024 * 1024;
        var buf = try self.allocator.alloc(u8, chunk_size);
        defer self.allocator.free(buf);

        var uploaded: u64 = 0;
        while (uploaded < file_size) {
            const bytes_read = try file.read(buf);
            if (bytes_read == 0) break;
            try s3w.writeAll(buf[0..bytes_read]);
            uploaded += bytes_read;
        }

        try s3w.finish();
    }

    /// Upload a local file to S3.
    fn uploadToS3(self: *Self, local_path: []const u8, s3_path: []const u8) !void {
        const S3Writer = s3_writer_mod.S3WriterGen(xev.Dynamic);

        // Parse S3 path
        if (!std.mem.startsWith(u8, s3_path, "s3://")) return error.InvalidS3Path;
        const path_without_prefix = s3_path[5..];
        const slash_idx = std.mem.indexOf(u8, path_without_prefix, "/") orelse return error.InvalidS3Path;
        const bucket = path_without_prefix[0..slash_idx];
        const key = path_without_prefix[slash_idx + 1 ..];

        // Load S3 config from environment
        const config = try factory.loadS3ConfigFromEnv(self.allocator);
        defer config.deinit(self.allocator);

        // Discover endpoint (handles S3_ENDPOINT for rustfs/MinIO/R2)
        const discovery = try factory.discoverS3Endpoint(self.allocator, config.endpoint, config.region);
        defer discovery.deinit(self.allocator);

        // Create S3Writer
        var s3w = try S3Writer.init(self.allocator, bucket, key, config.region);
        defer s3w.deinit();

        // Configure endpoint
        self.allocator.free(s3w.host);
        s3w.host = try self.allocator.dupe(u8, discovery.host);
        s3w.port = discovery.port;
        s3w.use_tls = discovery.use_tls;

        // Use path-style for custom endpoints (rustfs, MinIO, R2)
        if (config.endpoint != null) {
            s3w.use_path_style = true;
        }

        // Set credentials
        if (config.credentials) |creds| {
            try s3w.setCredentials(creds.access_key, creds.secret_key, creds.session_token);
        }

        // Read local file and upload
        const file = try std.fs.cwd().openFile(local_path, .{});
        defer file.close();

        const file_size = try file.getEndPos();
        const chunk_size: usize = 8 * 1024 * 1024; // 8MB chunks
        var buf = try self.allocator.alloc(u8, chunk_size);
        defer self.allocator.free(buf);

        var uploaded: u64 = 0;
        while (uploaded < file_size) {
            const bytes_read = try file.read(buf);
            if (bytes_read == 0) break;
            try s3w.writeAll(buf[0..bytes_read]);
            uploaded += bytes_read;
        }

        try s3w.finish();
    }
};
