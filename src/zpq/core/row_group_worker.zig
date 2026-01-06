const std = @import("std");
const xev = @import("xev");
const tracer = @import("../trace.zig");

/// Simple trace helper - prints timing when ZPQ_TRACE=1
const Trace = struct {
    start: std.time.Instant,
    last: std.time.Instant,
    enabled: bool,

    fn init() Trace {
        const enabled = if (std.posix.getenv("ZPQ_TRACE")) |v| std.mem.eql(u8, v, "1") else false;
        const now = std.time.Instant.now() catch unreachable;
        return .{ .start = now, .last = now, .enabled = enabled };
    }

    fn mark(self: *Trace, comptime label: []const u8) void {
        if (!self.enabled) return;
        const now = std.time.Instant.now() catch return;
        const since_last = now.since(self.last);
        std.debug.print("[WORKER] {s}: +{d:.2}ms\n", .{
            label,
            @as(f64, @floatFromInt(since_last)) / 1_000_000.0,
        });
        self.last = now;
    }
};

const SelectionVector = @import("selection.zig").SelectionVector;
const FilterColumnCache = @import("filter_cache.zig").FilterColumnCache;
const EncodedFilter = @import("filter.zig").EncodedFilter;
const filters_mod = @import("filters/mod.zig");
const Filter = filters_mod.Filter;
const BatchReader = @import("batch_reader.zig").BatchReader;
const simd = @import("simd.zig");
const page_writer = @import("page_writer.zig");
const slot_writer = @import("slot_writer.zig");
const thrift = @import("thrift.zig");

const schema = @import("schema.zig");
const column_mod = @import("column.zig");
const ColumnReader = column_mod.ColumnReader;
const RleDecoder = @import("rle.zig").RleDecoder;
const interface = @import("../io/interface.zig");
const MemorySource = interface.local.MemorySource;

// ============================================================================
// Parallel Execution Support (Phase 2.3)
// ============================================================================

/// Default WorkerCompletion using xev.Dynamic for runtime backend selection.
/// This enables io_uring -> epoll fallback for unibin Lambda support.
pub const WorkerCompletion = WorkerCompletionGen(xev.Dynamic);

/// Generic completion tracker parameterized by xev API type.
/// Supports io_uring, epoll, kqueue backends via compile-time selection.
pub fn WorkerCompletionGen(comptime XevApi: type) type {
    return struct {
        const Self = @This();

        /// The worker being executed
        worker: *RowGroupWorker,

        /// Thread pool task (embedded for zero-allocation scheduling)
        task: xev.ThreadPool.Task,

        /// Async signal to notify main loop when worker completes
        async_signal: XevApi.Async,

        /// xev completion for async wait
        xev_completion: XevApi.Completion,

        /// Pointer to shared pending counter (atomic)
        pending: *std.atomic.Value(usize),

        /// Initialize a WorkerCompletion for a given worker
        pub fn init(worker: *RowGroupWorker, pending: *std.atomic.Value(usize)) !Self {
            return Self{
                .worker = worker,
                .task = .{ .callback = taskCallback },
                .async_signal = try XevApi.Async.init(),
                .xev_completion = .{},
                .pending = pending,
            };
        }

        /// Clean up resources
        pub fn deinit(self: *Self) void {
            self.async_signal.deinit();
        }

        /// Arm the async wait on the loop and schedule the task on the thread pool
        pub fn scheduleOn(self: *Self, loop: *XevApi.Loop, pool: *xev.ThreadPool) void {
            // First, arm the async wait so we get notified when worker completes
            self.async_signal.wait(loop, &self.xev_completion, Self, self, asyncCallback);

            // Then schedule the task on the thread pool
            pool.schedule(xev.ThreadPool.Batch.from(&self.task));
        }

        /// Thread pool task callback - runs on worker thread
        fn taskCallback(task: *xev.ThreadPool.Task) void {
            // Recover the WorkerCompletion from the task pointer
            const self: *Self = @fieldParentPtr("task", task);

            // Execute the worker (pure CPU work, no I/O)
            self.worker.execute();

            // Signal the main loop that we're done
            self.async_signal.notify() catch {};
        }

        /// Async callback - runs on main loop when worker signals completion
        fn asyncCallback(
            ud: ?*Self,
            _: *XevApi.Loop,
            _: *XevApi.Completion,
            _: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            const self = ud.?;

            // Decrement the pending counter (atomic)
            _ = self.pending.fetchSub(1, .release);

            // Disarm - we only need one notification per worker
            return .disarm;
        }
    };
}

/// Pre-fetched column data for a single row group.
/// All I/O happens BEFORE the worker starts - worker does only CPU work.
pub const RowGroupData = struct {
    rg_idx: usize,
    num_rows: usize,

    // Filter column data (one per predicate)
    filter_bufs: []const []const u8,
    filter_offsets: []const u64,
    filter_chunks: []const schema.ColumnChunk,

    // Output column data (in same order as output_col_indices)
    // For the filter column, we reuse filter_buf
    output_bufs: []const []const u8,
    output_offsets: []const u64,
    output_chunks: []const schema.ColumnChunk,
};

/// Shared immutable context for all row group workers.
/// Contains filter spec, output column spec, and file metadata.
/// NO source/I/O references - this is pure configuration.
pub const FilterContext = struct {
    allocator: std.mem.Allocator,

    // Filter specification (multiple predicates)
    filter_col_names: []const []const u8,
    filter_vals: []const []const u8,
    filter_col_indices: []const usize,
    filter_col_types: []const schema.Type,
    filters: []const Filter,

    // Output column specification
    output_col_indices: []const usize,
    output_col_types: []const schema.Type,
    output_col_names: []const []const u8,
    filter_cols_in_output: []const bool,
    filter_col_output_indices: []const ?usize,

    // File metadata (read-only)
    meta: *const schema.FileMetaData,

    // Legacy accessor for backwards compatibility
    pub fn getEncodedFilter(self: *const FilterContext) ?*const EncodedFilter {
        return self.filter.asEncodedFilter();
    }
};

/// Dictionary passthrough data - avoids decoding strings entirely
pub const DictPassthroughData = struct {
    /// Raw dictionary page bytes (may be compressed)
    dict_page_data: []const u8,
    /// Dictionary page header for writing
    dict_header: schema.PageHeader,
    /// Selected indices (gathered from RLE stream)
    indices: []u32,
    /// Bit width for RLE encoding
    bit_width: u8,
    /// Whether dict_page_data is borrowed (from input) or owned
    dict_borrowed: bool,

    pub fn deinit(self: *DictPassthroughData, allocator: std.mem.Allocator) void {
        if (!self.dict_borrowed) {
            allocator.free(self.dict_page_data);
        }
        allocator.free(self.indices);
    }
};

/// Output data for a single column, ready for writing
pub const OutputColumnData = struct {
    col_type: schema.Type,

    // Only one of these is populated based on col_type
    int32_values: ?[]i32 = null,
    int64_values: ?[]i64 = null,
    float_values: ?[]f32 = null,
    double_values: ?[]f64 = null,
    bool_values: ?[]bool = null,
    byte_array_values: ?[][]const u8 = null,

    // Dictionary passthrough (for BYTE_ARRAY columns with dict encoding)
    dict_passthrough: ?DictPassthroughData = null,

    pub fn deinit(self: *OutputColumnData, allocator: std.mem.Allocator) void {
        if (self.int32_values) |v| allocator.free(v);
        if (self.int64_values) |v| allocator.free(v);
        if (self.float_values) |v| allocator.free(v);
        if (self.double_values) |v| allocator.free(v);
        if (self.bool_values) |v| allocator.free(v);
        if (self.byte_array_values) |values| {
            for (values) |v| allocator.free(v);
            allocator.free(values);
        }
        if (self.dict_passthrough) |*dp| dp.deinit(allocator);
        self.* = .{ .col_type = self.col_type };
    }
};

/// Worker that processes a single row group.
/// Designed for parallel execution - does ONLY CPU work, NO I/O.
/// All data must be pre-fetched before execute() is called.
pub const RowGroupWorker = struct {
    const Self = @This();

    // === Configuration (set at init, immutable during execution) ===
    allocator: std.mem.Allocator,
    ctx: *const FilterContext,
    data: *const RowGroupData,

    // === Results (written during execution) ===
    selection: SelectionVector,
    filter_cache: ?FilterColumnCache,
    used_dict_fast_path: bool, // Track if dictionary fast path was used during scan
    output_columns: []OutputColumnData,
    row_count: usize,

    // === Status ===
    status: Status,
    err: ?anyerror,

    pub const Status = enum {
        pending,
        scanning,
        decoding,
        materializing,
        done,
        failed,
    };

    pub fn init(allocator: std.mem.Allocator, ctx: *const FilterContext, data: *const RowGroupData) !*Self {
        const self = try allocator.create(Self);
        self.* = .{
            .allocator = allocator,
            .ctx = ctx,
            .data = data,
            .selection = SelectionVector.init(allocator),
            .filter_cache = null,
            .used_dict_fast_path = false,
            .output_columns = &.{},
            .row_count = 0,
            .status = .pending,
            .err = null,
        };
        return self;
    }

    pub fn deinit(self: *Self) void {
        self.selection.deinit();
        if (self.filter_cache) |*fc| fc.deinit();
        for (self.output_columns) |*col| col.deinit(self.allocator);
        if (self.output_columns.len > 0) {
            self.allocator.free(self.output_columns);
        }
        self.allocator.destroy(self);
    }

    /// Execute the worker (can be called directly for sequential execution
    /// or from a thread pool callback for parallel execution).
    /// This is pure CPU work - no I/O.
    pub fn execute(self: *Self) void {
        const zone = tracer.zone("RowGroupWorker/execute");
        defer zone.end();
        var trace = Trace.init();
        self.status = .scanning;

        // Phase 1: Scan filter column to build selection vector
        self.scanFilterColumn() catch |e| {
            self.status = .failed;
            self.err = e;
            return;
        };
        trace.mark("scan_filter");

        // Early exit if no matches
        if (self.selection.count() == 0) {
            self.status = .done;
            self.row_count = 0;
            return;
        }

        // Phase 2: Decode other columns for selected rows
        self.status = .decoding;
        self.decodeOutputColumns() catch |e| {
            self.status = .failed;
            self.err = e;
            return;
        };
        trace.mark("decode_cols");

        // Phase 3: Materialize filter column values (if needed)
        self.status = .materializing;
        self.materializeFilterColumn() catch |e| {
            self.status = .failed;
            self.err = e;
            return;
        };
        trace.mark("materialize");

        self.status = .done;
        self.row_count = self.selection.count();
    }

    /// Parse the filter value bytes as type T for dictionary lookup.
    fn parseFilterValueFor(self: *const Self, filter_idx: usize, comptime T: type) ?T {
        const val = self.ctx.filter_vals[filter_idx];
        if (T == i32) {
            if (val.len != 4) {
                // Try parsing as string if it's not raw bytes
                const i = std.fmt.parseInt(i32, val, 10) catch return null;
                return i;
            }
            return std.mem.readInt(i32, val[0..4], .little);
        } else if (T == i64) {
            if (val.len != 8) {
                const i = std.fmt.parseInt(i64, val, 10) catch return null;
                return i;
            }
            return std.mem.readInt(i64, val[0..8], .little);
        } else if (T == f32) {
            if (val.len != 4) {
                const f = std.fmt.parseFloat(f32, val) catch return null;
                return f;
            }
            return @bitCast(std.mem.readInt(u32, val[0..4], .little));
        } else if (T == f64) {
            if (val.len != 8) {
                const f = std.fmt.parseFloat(f64, val) catch return null;
                return f;
            }
            return @bitCast(std.mem.readInt(u64, val[0..8], .little));
        } else if (T == bool) {
            if (std.mem.eql(u8, val, "true") or std.mem.eql(u8, val, "1")) return true;
            if (std.mem.eql(u8, val, "false") or std.mem.eql(u8, val, "0")) return false;
            return null;
        } else {
            return null;
        }
    }

    fn scanFilterColumn(self: *Self) !void {
        const num_rows = self.data.num_rows;
        const num_filters = self.ctx.filters.len;

        for (0..num_filters) |i| {
            try self.scanSingleFilter(i, num_rows);
            // Early exit if no matches (AND conjunction)
            if (self.selection.count() == 0) return;
        }
    }

    fn scanSingleFilter(self: *Self, filter_idx: usize, num_rows: usize) !void {
        const zone = tracer.zone("scanSingleFilter");
        defer zone.end();
        if (num_rows > 0) {
            var buf: [64]u8 = undefined;
            const msg = try std.fmt.bufPrint(&buf, "flt={d}", .{filter_idx});
            zone.addText(msg);
        }
        // Create memory source from pre-fetched buffer for THIS filter
        var mem_source = MemorySource.initWithOffset(self.data.filter_bufs[filter_idx], self.data.filter_offsets[filter_idx]);

        // Get filter column metadata
        const filter_chunk = self.data.filter_chunks[filter_idx];
        const filter_md = filter_chunk.meta_data.?;
        const filter_levels = self.ctx.meta.getColumnLevels(filter_md.path_in_schema.items);
        const filter_schema_elem = self.ctx.meta.getColumnSchema(filter_md.path_in_schema.items);
        const filter_type_len = if (filter_schema_elem) |se| se.type_length else null;
        const filter_type = self.ctx.filter_col_types[filter_idx];

        // Create column reader
        const filter_col_reader = try ColumnReader.init(mem_source.source(), filter_chunk);

        // If this is NOT the first filter, we filter the EXISTING selection
        // If this is NOT the first filter, we filter the EXISTING selection
        if (filter_idx > 0) {
            var intersected_selection = SelectionVector.init(self.allocator);
            errdefer intersected_selection.deinit();

            // Re-allocate based on current selection size
            try intersected_selection.ensureCapacity(self.selection.count());

            // Scan using skipToRow for efficiency
            switch (filter_type) {
                .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                    try self.scanByteArrayColumnSelected(filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection);
                },
                .INT32 => try self.scanTypedColumnSelected(i32, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection),
                .INT64 => try self.scanTypedColumnSelected(i64, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection),
                .FLOAT => try self.scanTypedColumnSelected(f32, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection),
                .DOUBLE => try self.scanTypedColumnSelected(f64, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection),
                .BOOLEAN => try self.scanTypedColumnSelected(bool, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows, &intersected_selection),
                else => return error.UnsupportedFilterType,
            }

            // Swap selections
            self.selection.deinit();
            self.selection = intersected_selection;
        } else {
            // First filter: standard scan (populates selection)
            // Pre-allocate for expected matches (~25% selectivity)
            try self.selection.ensureCapacity(num_rows / 4);

            switch (filter_type) {
                .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                    try self.scanByteArrayColumn(filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows);
                },
                .INT32 => try self.scanTypedColumn(i32, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
                .INT64 => try self.scanTypedColumn(i64, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
                .FLOAT => try self.scanTypedColumn(f32, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
                .DOUBLE => try self.scanTypedColumn(f64, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
                .BOOLEAN => try self.scanTypedColumn(bool, filter_idx, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
                else => return error.UnsupportedFilterType,
            }
        }
    }

    fn scanByteArrayColumn(
        self: *Self,
        filter_idx: usize,
        col_reader: ColumnReader,
        md: schema.ColumnMetaData,
        levels: schema.Levels,
        type_len: ?i32,
        num_rows: usize,
    ) !void {
        var reader = BatchReader([]const u8).init(
            self.allocator,
            col_reader,
            md.type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const filter = &self.ctx.filters[filter_idx];
        const filter_val = self.ctx.filter_vals[filter_idx];

        var row_idx: usize = 0;
        var buf: [1024]?[]const u8 = undefined;

        // Try dictionary fast path
        var target_dict_idx: ?u64 = null;
        var use_dict_fast_path = false;
        var dict_checked = false;

        while (row_idx < num_rows) {
            const batch_size = @min(1024, num_rows - row_idx);

            // Check for dictionary fast path (only once after dictionary is loaded)
            if (!dict_checked and reader.hasDictionary()) {
                dict_checked = true;
                target_dict_idx = reader.findInDictionary(filter_val);
                use_dict_fast_path = target_dict_idx != null;

                // Dictionary pre-filter: for equality filters, if value not in dictionary,
                // there can be zero matches - early exit
                if (!use_dict_fast_path and filter.isEquality()) {
                    // Value not in dictionary and this is an equality filter - no matches possible
                    return;
                }
            }

            if (use_dict_fast_path) {
                // Fast path: RLE-aware scan directly to selection vector
                const n_read = try reader.scanDictMatchesToSelection(target_dict_idx.?, row_idx, batch_size, &self.selection);
                if (n_read == 0) break;

                row_idx += n_read;
                self.used_dict_fast_path = true;
            } else {
                // Slow path: decode and compare
                const n_read = try reader.nextBatch(buf[0..batch_size]);
                if (n_read == 0) break;

                for (buf[0..n_read], 0..) |maybe_val, i| {
                    const matches = if (maybe_val) |v|
                        filter.matchesBytes(v)
                    else
                        filter.matchesNull();

                    if (matches) {
                        try self.selection.append(row_idx + i);
                        if (self.filter_cache) |*fc| {
                            if (maybe_val) |v| {
                                try fc.appendByteArray(v);
                            }
                            // Note: For IS NULL matches, we don't cache the value
                        }
                    }
                }
                row_idx += n_read;
            }
        }
    }

    fn scanTypedColumn(
        self: *Self,
        comptime T: type,
        filter_idx: usize,
        col_reader: ColumnReader,
        md: schema.ColumnMetaData,
        levels: schema.Levels,
        type_len: ?i32,
        num_rows: usize,
    ) !void {
        const zone = tracer.zone("scanTypedColumn");
        defer zone.end();

        var reader = BatchReader(T).init(
            self.allocator,
            col_reader,
            md.type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const filter = &self.ctx.filters[filter_idx];

        var row_idx: usize = 0;
        var buf: [1024]?T = undefined;
        var dict_checked = false;

        while (row_idx < num_rows) {
            const batch_size = @min(1024, num_rows - row_idx);
            const n_read = try reader.nextBatch(buf[0..batch_size]);
            if (n_read == 0) break;

            // Dictionary pre-filter: for equality filters on dict-encoded columns,
            // if value not in dictionary, there can be zero matches - early exit
            if (!dict_checked and reader.hasDictionary()) {
                dict_checked = true;
                if (filter.isEquality()) {
                    // Parse filter value as this type
                    const filter_val = self.parseFilterValueFor(filter_idx, T) orelse {
                        // Could not parse filter value for this type - skip optimization
                        continue;
                    };
                    if (reader.findInDictionary(filter_val) == null) {
                        // Value not in dictionary - no matches possible
                        return;
                    }
                }
            }

            for (buf[0..n_read], 0..) |maybe_val, i| {
                const matches = if (maybe_val) |v| blk: {
                    // Non-null value - check against filter
                    break :blk if (T == i32)
                        filter.matchesInt32(v)
                    else if (T == i64)
                        filter.matchesInt64(v)
                    else if (T == f32)
                        filter.matchesFloat(v)
                    else if (T == f64)
                        filter.matchesDouble(v)
                    else if (T == bool)
                        filter.matchesBool(v)
                    else
                        @compileError("Unsupported type for filter matching");
                } else blk: {
                    // Null value - check if this is a null filter
                    break :blk filter.matchesNull();
                };

                if (matches) {
                    try self.selection.append(row_idx + i);
                    if (self.filter_cache) |*fc| {
                        if (maybe_val) |v| {
                            if (T == i32) try fc.appendInt32(v) else if (T == i64) try fc.appendInt64(v) else if (T == f32) try fc.appendFloat(v) else if (T == f64) try fc.appendDouble(v) else if (T == bool) try fc.appendBool(v);
                        }
                        // Note: For IS NULL matches, we don't cache the value (it's null)
                    }
                }
            }
            row_idx += n_read;
        }

        if (self.selection.count() > 0) {
            var out_buf: [64]u8 = undefined;
            const msg = try std.fmt.bufPrint(&out_buf, "in={d} out={d}", .{ num_rows, self.selection.count() });
            zone.addText(msg);
        }
    }

    fn decodeOutputColumns(self: *Self) !void {
        const output_count = self.ctx.output_col_indices.len;

        // Allocate output column array
        self.output_columns = try self.allocator.alloc(OutputColumnData, output_count);
        @memset(self.output_columns, .{ .col_type = .INT32 });

        // Process each output column
        for (self.ctx.output_col_indices, self.ctx.output_col_types, 0..) |col_idx, col_type, out_idx| {
            self.output_columns[out_idx].col_type = col_type;

            // Check if this is a filter column that we want to skip (because it's cached)
            // For now, let's just decode all output columns normally to avoid complexity
            // with multiple filter caches.
            _ = col_idx;

            // Get the pre-fetched data for this column
            const buf = self.data.output_bufs[out_idx];
            const offset = self.data.output_offsets[out_idx];
            const chunk = self.data.output_chunks[out_idx];

            const md = chunk.meta_data.?;
            const levels = self.ctx.meta.getColumnLevels(md.path_in_schema.items);
            const schema_elem = self.ctx.meta.getColumnSchema(md.path_in_schema.items);
            const type_len = if (schema_elem) |se| se.type_length else null;

            var mem_source = MemorySource.initWithOffset(buf, offset);
            const col_reader = try ColumnReader.init(mem_source.source(), chunk);

            // Check if column uses dictionary encoding (from metadata)
            const is_dict_encoded = blk: {
                for (md.encodings.items) |enc| {
                    if (enc == .RLE_DICTIONARY or enc == .PLAIN_DICTIONARY) break :blk true;
                }
                break :blk false;
            };

            try self.readSelectedColumn(col_reader, col_type, levels, type_len, out_idx, is_dict_encoded);
        }
    }

    fn scanTypedColumnSelected(
        self: *Self,
        comptime T: type,
        filter_idx: usize,
        col_reader: ColumnReader,
        md: schema.ColumnMetaData,
        levels: schema.Levels,
        type_len: ?i32,
        num_rows: usize,
        new_selection: *SelectionVector,
    ) !void {
        const zone = tracer.zone("scanTypedColumnSelected");
        defer zone.end();
        var reader = BatchReader(T).init(
            self.allocator,
            col_reader,
            md.type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const filter = &self.ctx.filters[filter_idx];
        var row_idx: usize = 0;

        for (self.selection.items()) |target_row| {
            // Efficient skip using page-skipping
            row_idx = try reader.skipToRow(target_row, row_idx);

            const maybe_val = try reader.nextValue();
            row_idx += 1;

            const matches = if (maybe_val) |v| blk: {
                break :blk if (T == i32)
                    filter.matchesInt32(v)
                else if (T == i64)
                    filter.matchesInt64(v)
                else if (T == f32)
                    filter.matchesFloat(v)
                else if (T == f64)
                    filter.matchesDouble(v)
                else if (T == bool)
                    filter.matchesBool(v)
                else
                    @compileError("Unsupported type for filter matching");
            } else blk: {
                // Null value - check if this is a null filter
                break :blk filter.matchesNull();
            };

            if (matches) {
                try new_selection.append(target_row);
            }
        }

        if (new_selection.count() > 0) {
            var buf: [64]u8 = undefined;
            const msg = try std.fmt.bufPrint(&buf, "in={d} out={d}", .{ num_rows, new_selection.count() });
            zone.addText(msg);
        }
    }

    fn scanByteArrayColumnSelected(
        self: *Self,
        filter_idx: usize,
        col_reader: ColumnReader,
        md: schema.ColumnMetaData,
        levels: schema.Levels,
        type_len: ?i32,
        num_rows: usize,
        new_selection: *SelectionVector,
    ) !void {
        _ = num_rows;
        var reader = BatchReader([]const u8).init(
            self.allocator,
            col_reader,
            md.type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const filter = &self.ctx.filters[filter_idx];
        var row_idx: usize = 0;

        for (self.selection.items()) |target_row| {
            // Efficient skip using page-skipping
            row_idx = try reader.skipToRow(target_row, row_idx);

            const maybe_val = try reader.nextValue();
            row_idx += 1;

            const matches = if (maybe_val) |v|
                filter.matchesBytes(v)
            else
                filter.matchesNull();

            if (matches) {
                try new_selection.append(target_row);
            }
        }
    }

    fn readSelectedColumn(
        self: *Self,
        col_reader: ColumnReader,
        col_type: schema.Type,
        levels: schema.Levels,
        type_len: ?i32,
        out_idx: usize,
        is_dict_encoded: bool,
    ) !void {
        // Only try dictionary passthrough for dictionary-encoded BYTE_ARRAY types
        // This avoids reading pages just to check encoding (which corrupts the reader)
        if (is_dict_encoded and (col_type == .BYTE_ARRAY or col_type == .FIXED_LEN_BYTE_ARRAY)) {
            if (try self.tryReadSelectedWithDictPassthrough(col_reader, col_type, levels, out_idx)) {
                return; // Successfully used passthrough
            }
        }

        // Skip-based decode for selected rows
        switch (col_type) {
            .INT32 => {
                const values = try self.readSelectedTyped(i32, col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].int32_values = values;
            },
            .INT64 => {
                const values = try self.readSelectedTyped(i64, col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].int64_values = values;
            },
            .FLOAT => {
                const values = try self.readSelectedTyped(f32, col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].float_values = values;
            },
            .DOUBLE => {
                const values = try self.readSelectedTyped(f64, col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].double_values = values;
            },
            .BOOLEAN => {
                const values = try self.readSelectedTyped(bool, col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].bool_values = values;
            },
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                const values = try self.readSelectedByteArray(col_reader, col_type, levels, type_len);
                self.output_columns[out_idx].byte_array_values = values;
            },
            else => {},
        }
    }

    fn readSelectedTyped(
        self: *Self,
        comptime T: type,
        col_reader: ColumnReader,
        col_type: schema.Type,
        levels: schema.Levels,
        type_len: ?i32,
    ) ![]T {
        var reader = BatchReader(T).init(
            self.allocator,
            col_reader,
            col_type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const indices = self.selection.items();
        if (indices.len == 0) {
            return try self.allocator.alloc(T, 0);
        }

        // Page-level skip optimization:
        // If we need to skip more than a page worth of rows, skip entire pages
        // without decompressing them. This is the key to matching Polars' speed.
        var values = try self.allocator.alloc(T, indices.len);
        errdefer self.allocator.free(values);

        var current_row: usize = 0;
        for (indices, 0..) |target_row, out_idx| {
            // Skip to the target row using page-level skipping
            if (target_row > current_row) {
                current_row = try reader.skipToRow(target_row, current_row);
            }

            // Read the value at target position
            var buf: [1]?T = undefined;
            const n = try reader.nextBatch(&buf);
            if (n == 0) {
                values[out_idx] = std.mem.zeroes(T);
            } else {
                values[out_idx] = buf[0] orelse std.mem.zeroes(T);
            }
            current_row += 1;
        }

        return values;
    }

    fn readSelectedByteArray(
        self: *Self,
        col_reader: ColumnReader,
        col_type: schema.Type,
        levels: schema.Levels,
        type_len: ?i32,
    ) ![][]const u8 {
        var reader = BatchReader([]const u8).init(
            self.allocator,
            col_reader,
            col_type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        const indices = self.selection.items();
        if (indices.len == 0) {
            return try self.allocator.alloc([]const u8, 0);
        }

        // Page-level skip optimization (same as readSelectedTyped)
        var values = try self.allocator.alloc([]const u8, indices.len);
        errdefer {
            for (values) |v| self.allocator.free(v);
            self.allocator.free(values);
        }

        var current_row: usize = 0;
        for (indices, 0..) |target_row, out_idx| {
            // Skip to the target row using page-level skipping
            if (target_row > current_row) {
                current_row = try reader.skipToRow(target_row, current_row);
            }

            // Read the value at target position
            var buf: [1]?[]const u8 = undefined;
            const n = try reader.nextBatch(&buf);
            if (n == 0 or buf[0] == null) {
                values[out_idx] = try self.allocator.dupe(u8, "");
            } else {
                values[out_idx] = try self.allocator.dupe(u8, buf[0].?);
            }
            current_row += 1;
        }

        return values;
    }

    /// Try to read a column using dictionary passthrough.
    /// This avoids decoding values entirely - we keep the raw dictionary page
    /// and just gather the RLE indices for selected rows.
    /// Returns true if passthrough was used, false if caller should fall back to full decode.
    fn tryReadSelectedWithDictPassthrough(
        self: *Self,
        col_reader: ColumnReader,
        col_type: schema.Type,
        levels: schema.Levels,
        out_idx: usize,
    ) !bool {
        // Don't support nested/repeated columns yet
        if (levels.max_rep > 0) {
            return false;
        }

        var reader = col_reader;
        defer reader.deinit(); // Clean up decompression buffer

        const selection_indices = self.selection.items();
        const max_def: u16 = @intCast(levels.max_def);

        // Sentinel value for nulls (use max u32)
        const null_sentinel: u64 = std.math.maxInt(u32);

        // Step 1: Read pages and collect ALL indices + dictionary
        var dict_page_data: ?[]const u8 = null;
        var dict_header: ?schema.PageHeader = null;
        var rle_bit_width: u8 = 0;

        // Collect indices for selected rows only (skip-based)
        var all_indices = std.ArrayListUnmanaged(u64){};
        defer all_indices.deinit(self.allocator);
        try all_indices.ensureTotalCapacity(self.allocator, selection_indices.len);

        var current_row: usize = 0;

        while (try reader.next(self.allocator)) |page| {
            var mutable_page = page;
            defer mutable_page.deinit(self.allocator);

            if (mutable_page.header.type == .DICTIONARY_PAGE) {
                // Capture dictionary page - we need to keep it for output
                dict_header = mutable_page.header;
                dict_page_data = try self.allocator.dupe(u8, mutable_page.data);
                continue;
            }

            if (mutable_page.header.type == .DATA_PAGE) {
                const dph = mutable_page.header.data_page_header orelse continue;

                // Only support RLE_DICTIONARY encoding
                if (dph.encoding != .RLE_DICTIONARY and dph.encoding != .PLAIN_DICTIONARY) {
                    // Not dictionary encoded - abort passthrough
                    if (dict_page_data) |d| self.allocator.free(d);
                    return false;
                }

                var data_slice = mutable_page.data;

                // Parse definition levels if present
                var def_decoder: ?RleDecoder = null;
                if (max_def > 0) {
                    if (data_slice.len < 4) continue;
                    const def_len = std.mem.readInt(u32, data_slice[0..4], .little);
                    if (data_slice.len < 4 + def_len) continue;
                    const def_data = data_slice[4 .. 4 + def_len];
                    data_slice = data_slice[4 + def_len ..];

                    const def_bit_width = std.math.log2_int(u32, std.math.ceilPowerOfTwo(u32, @as(u32, max_def) + 1) catch 1);
                    def_decoder = RleDecoder.init(def_data, @intCast(def_bit_width));
                }

                // Parse RLE data: first byte is bit width
                if (data_slice.len < 1) continue;
                rle_bit_width = data_slice[0];
                const rle_data = data_slice[1..];

                var rle = RleDecoder.init(rle_data, rle_bit_width);
                const num_values: usize = @intCast(dph.num_values);
                const page_start_row = current_row;
                const page_end_row = current_row + num_values;

                // Find which selected rows fall in this page
                // Selection is sorted, so we can binary search
                const first_sel_idx = blk: {
                    var low: usize = 0;
                    var high: usize = selection_indices.len;
                    while (low < high) {
                        const mid = low + (high - low) / 2;
                        if (selection_indices[mid] < page_start_row) {
                            low = mid + 1;
                        } else {
                            high = mid;
                        }
                    }
                    break :blk low;
                };

                // Skip pages with no selected rows
                if (first_sel_idx >= selection_indices.len or selection_indices[first_sel_idx] >= page_end_row) {
                    current_row = page_end_row;
                    continue;
                }

                if (def_decoder != null) {
                    // Nullable columns: use efficient batch skip for def levels
                    var def_dec = def_decoder.?;
                    var rle_pos: usize = 0;
                    var sel_idx = first_sel_idx;

                    while (sel_idx < selection_indices.len) {
                        const target_row = selection_indices[sel_idx];
                        if (target_row >= page_end_row) break;

                        const target_pos = target_row - page_start_row;

                        // Skip def levels to target position using batch skip
                        if (target_pos > rle_pos) {
                            const to_skip = target_pos - rle_pos;
                            // skipAndCountMatching returns number of values that matched max_def
                            const values_to_skip = try def_dec.skipAndCountMatching(@intCast(to_skip), max_def);
                            // Skip the corresponding RLE values
                            if (values_to_skip > 0) {
                                try rle.skip(@intCast(values_to_skip));
                            }
                            rle_pos = target_pos;
                        }

                        // Read the value at target position
                        const def_level = (try def_dec.next()) orelse break;
                        if (def_level == max_def) {
                            const idx = (try rle.next()) orelse break;
                            all_indices.appendAssumeCapacity(idx);
                        } else {
                            all_indices.appendAssumeCapacity(null_sentinel);
                        }
                        rle_pos += 1;
                        sel_idx += 1;
                    }
                    current_row = page_end_row;
                } else {
                    // Non-nullable: use skip-based gathering
                    // Only decode indices we actually need
                    var rle_pos: usize = 0; // Position in RLE stream (relative to page)
                    var sel_idx = first_sel_idx;

                    while (sel_idx < selection_indices.len) {
                        const target_row = selection_indices[sel_idx];
                        if (target_row >= page_end_row) break;

                        const target_pos = target_row - page_start_row;

                        // Skip to target position
                        if (target_pos > rle_pos) {
                            try rle.skip(@intCast(target_pos - rle_pos));
                            rle_pos = target_pos;
                        }

                        // Read the index we need
                        const idx = (try rle.next()) orelse break;
                        all_indices.appendAssumeCapacity(idx);
                        rle_pos += 1;
                        sel_idx += 1;
                    }
                    current_row = page_end_row;
                }
            }
        }

        // Must have dictionary for passthrough
        if (dict_page_data == null or dict_header == null) {
            return false;
        }

        // For skip-based path, all_indices already contains only selected indices in order
        // Just convert to u32
        var selected_indices = try self.allocator.alloc(u32, all_indices.items.len);
        errdefer self.allocator.free(selected_indices);

        for (all_indices.items, 0..) |idx, i| {
            selected_indices[i] = if (idx == null_sentinel) 0 else @intCast(idx);
        }

        // Store passthrough data
        self.output_columns[out_idx].dict_passthrough = DictPassthroughData{
            .dict_page_data = dict_page_data.?,
            .dict_header = dict_header.?,
            .indices = selected_indices,
            .bit_width = rle_bit_width,
            .dict_borrowed = false,
        };
        self.output_columns[out_idx].col_type = col_type;

        return true;
    }

    fn materializeFilterColumn(self: *Self) !void {
        // Find if any filter column is also in the output
        var cached_filter_idx: ?usize = null;
        for (self.ctx.filter_cols_in_output, 0..) |in_output, i| {
            if (in_output) {
                // If there's a cache, it's for the FIRST filter column
                if (i == 0 and self.filter_cache != null) {
                    cached_filter_idx = i;
                    break;
                }
            }
        }

        const f_idx = cached_filter_idx orelse return;
        const out_idx = self.ctx.filter_col_output_indices[f_idx].?;
        const f_type = self.ctx.filter_col_types[f_idx];
        const filter = &self.ctx.filters[f_idx];
        const filter_val = self.ctx.filter_vals[f_idx];

        self.output_columns[out_idx].col_type = f_type;

        if (filter.isNullCheck() and filter.matchesNull()) {
            return;
        }

        // If dictionary fast path was used at all, regenerate ALL values from filter_val.
        // This handles the mixed case where first page is PLAIN but rest are dictionary.
        if (self.used_dict_fast_path) {
            // For BYTE_ARRAY with dict fast path, skip materialization entirely.
            // encodeToBuffer will use writeConstantByteArray which doesn't need the values.
            if (f_type == .BYTE_ARRAY or f_type == .FIXED_LEN_BYTE_ARRAY) {
                // Don't materialize - encodeToBuffer handles this with constant encoding
                return;
            }

            // Generate values from filter_val (all matching rows have same value)
            const count = self.selection.count();
            switch (f_type) {
                .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => unreachable, // Handled above
                .INT32 => {
                    const parsed = try std.fmt.parseInt(i32, filter_val, 10);
                    const values = try self.allocator.alloc(i32, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].int32_values = values;
                },
                .INT64 => {
                    const parsed = try std.fmt.parseInt(i64, filter_val, 10);
                    const values = try self.allocator.alloc(i64, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].int64_values = values;
                },
                .FLOAT => {
                    const parsed = try std.fmt.parseFloat(f32, filter_val);
                    const values = try self.allocator.alloc(f32, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].float_values = values;
                },
                .DOUBLE => {
                    const parsed = try std.fmt.parseFloat(f64, filter_val);
                    const values = try self.allocator.alloc(f64, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].double_values = values;
                },
                .BOOLEAN => {
                    const parsed = std.mem.eql(u8, filter_val, "true");
                    const values = try self.allocator.alloc(bool, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].bool_values = values;
                },
                else => {},
            }
        } else {
            // Use cached values from filter scan
            // NOTE: For byte arrays, we transfer ownership from filter_cache to output_columns
            switch (f_type) {
                .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                    const cache_values = self.filter_cache.?.getByteArrayValues();
                    const values = try self.allocator.alloc([]const u8, cache_values.len);
                    @memcpy(values, cache_values);
                    self.output_columns[out_idx].byte_array_values = values;
                    // Transfer ownership: clear cache's list without freeing the strings
                    // The strings are now owned by output_columns
                    self.filter_cache.?.byte_array_values.clearRetainingCapacity();
                },
                .INT32 => {
                    const cache_values = self.filter_cache.?.getInt32Values();
                    const values = try self.allocator.dupe(i32, cache_values);
                    self.output_columns[out_idx].int32_values = values;
                },
                .INT64 => {
                    const cache_values = self.filter_cache.?.getInt64Values();
                    const values = try self.allocator.dupe(i64, cache_values);
                    self.output_columns[out_idx].int64_values = values;
                },
                .FLOAT => {
                    const cache_values = self.filter_cache.?.getFloatValues();
                    const values = try self.allocator.dupe(f32, cache_values);
                    self.output_columns[out_idx].float_values = values;
                },
                .DOUBLE => {
                    const cache_values = self.filter_cache.?.getDoubleValues();
                    const values = try self.allocator.dupe(f64, cache_values);
                    self.output_columns[out_idx].double_values = values;
                },
                .BOOLEAN => {
                    const cache_values = self.filter_cache.?.getBoolValues();
                    const values = try self.allocator.dupe(bool, cache_values);
                    self.output_columns[out_idx].bool_values = values;
                },
                else => {},
            }
        }
    }

    /// Encode the worker's output columns into a buffer suitable for slot writing.
    /// The buffer contains serialized page headers + compressed page data for all columns.
    /// Returns metadata about each column for footer construction.
    pub fn encodeToBuffer(
        self: *Self,
        buffer: *std.ArrayListUnmanaged(u8),
        compression: schema.CompressionCodec,
    ) ![]slot_writer.ColumnMeta {
        if (self.status != .done or self.row_count == 0) {
            return &[_]slot_writer.ColumnMeta{};
        }

        var column_metas = try self.allocator.alloc(slot_writer.ColumnMeta, self.output_columns.len);
        errdefer self.allocator.free(column_metas);

        for (self.output_columns, self.ctx.output_col_types, self.ctx.output_col_names, 0..) |col_data, col_type, col_name, i| {
            var cw = page_writer.ColumnWriter.init(self.allocator, col_type, compression);
            defer cw.deinit();

            // Check if this is a filter column with dict fast path
            // For multi-filter, we only apply this to the first filter column for now
            const is_first_filter_col = self.ctx.filter_cols_in_output.len > 0 and
                self.ctx.filter_cols_in_output[0] and
                self.ctx.filter_col_output_indices[0] != null and
                self.ctx.filter_col_output_indices[0].? == i;

            const use_constant_encoding = is_first_filter_col and self.used_dict_fast_path;

            // Check if this is a null filter (IS NULL)
            const is_null_filter_col = is_first_filter_col and self.ctx.filters[0].isNullCheck();

            var used_dict_encoding = false;

            if (is_null_filter_col and self.ctx.filters[0].matchesNull()) {
                // IS NULL filter: write all-null column
                try cw.writeAllNulls(self.row_count);
            } else if (use_constant_encoding and (col_type == .BYTE_ARRAY or col_type == .FIXED_LEN_BYTE_ARRAY)) {
                // Optimized path: single-value dictionary + RLE indices
                try cw.writeConstantByteArray(self.ctx.filter_vals[0], self.row_count);
                used_dict_encoding = true;
            } else if (col_data.dict_passthrough) |dp| {
                // Dictionary passthrough: write original dict page + new RLE indices
                try cw.writeDictPassthrough(dp.dict_header, dp.dict_page_data, dp.indices, dp.bit_width);
                used_dict_encoding = true;
            } else {
                // Standard encoding path
                switch (col_type) {
                    .INT32 => {
                        if (col_data.int32_values) |values| {
                            try cw.writeInt32Plain(values);
                        }
                    },
                    .INT64 => {
                        if (col_data.int64_values) |values| {
                            try cw.writeInt64Plain(values);
                        }
                    },
                    .FLOAT => {
                        if (col_data.float_values) |values| {
                            try cw.writeFloatPlain(values);
                        }
                    },
                    .DOUBLE => {
                        if (col_data.double_values) |values| {
                            try cw.writeDoublePlain(values);
                        }
                    },
                    .BOOLEAN => {
                        if (col_data.bool_values) |values| {
                            try cw.writeBooleanPlain(values);
                        }
                    },
                    .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                        if (col_data.byte_array_values) |values| {
                            try cw.writeByteArrayPlain(values);
                        }
                    },
                    else => {},
                }
            }

            // Record buffer position before writing this column
            const col_start = buffer.items.len;

            // Write pages to buffer
            try cw.writePagesToBufferUnmanaged(buffer, self.allocator);

            const col_size = buffer.items.len - col_start;

            // Build path_in_schema
            var path = std.ArrayListUnmanaged([]const u8){};
            try path.append(self.allocator, col_name);

            // Build encodings list
            var encodings = std.ArrayListUnmanaged(schema.Encoding){};
            if (used_dict_encoding) {
                try encodings.append(self.allocator, .PLAIN); // For dictionary page
                try encodings.append(self.allocator, .RLE_DICTIONARY);
            } else {
                try encodings.append(self.allocator, .PLAIN);
            }

            column_metas[i] = slot_writer.ColumnMeta{
                .type = col_type,
                .encodings = encodings,
                .path_in_schema = path,
                .codec = compression,
                .num_values = @intCast(self.row_count),
                .uncompressed_size = cw.total_uncompressed_size,
                .compressed_size = @intCast(col_size),
                .has_dictionary = used_dict_encoding,
            };
        }

        return column_metas;
    }

    /// Get metadata for this row group (for footer construction).
    /// Call after execute() completes successfully.
    pub fn getRowGroupMeta(self: *const Self, column_metas: []const slot_writer.ColumnMeta, actual_size: u64) slot_writer.RowGroupMeta {
        return slot_writer.RowGroupMeta{
            .num_rows = @intCast(self.row_count),
            .columns = column_metas,
            .actual_size = actual_size,
        };
    }
};

// ============================================================================
// Slot-based Parallel Execution (Phase 2)
// ============================================================================

/// Default SlotWriteCompletion using xev.Dynamic for runtime backend selection.
/// This enables io_uring -> epoll fallback for unibin Lambda support.
pub const SlotWriteCompletion = SlotWriteCompletionGen(xev.Dynamic);

/// Generic slot write completion parameterized by xev API type.
/// Supports io_uring, epoll, kqueue backends via compile-time selection.
pub fn SlotWriteCompletionGen(comptime XevApi: type) type {
    return struct {
        const Self = @This();

        /// The worker being executed
        worker: *RowGroupWorker,

        /// Slot writer for output
        slot_writer_ptr: *slot_writer.SlotWriter,

        /// Which slot this worker writes to
        slot_index: usize,

        /// Compression codec for output
        compression: schema.CompressionCodec,

        /// Thread pool task
        task: xev.ThreadPool.Task,

        /// Async signal to notify main loop
        async_signal: XevApi.Async,

        /// xev completion for async wait
        xev_completion: XevApi.Completion,

        /// Pointer to shared pending counter
        pending: *std.atomic.Value(usize),

        /// Output buffer (filled during task execution)
        output_buffer: std.ArrayListUnmanaged(u8),

        /// Column metadata (filled during task execution)
        column_metas: ?[]slot_writer.ColumnMeta,

        /// Error from task execution (if any)
        task_error: ?anyerror,

        pub fn init(
            worker: *RowGroupWorker,
            sw: *slot_writer.SlotWriter,
            slot_idx: usize,
            compression: schema.CompressionCodec,
            pending: *std.atomic.Value(usize),
        ) !Self {
            return Self{
                .worker = worker,
                .slot_writer_ptr = sw,
                .slot_index = slot_idx,
                .compression = compression,
                .task = .{ .callback = taskCallback },
                .async_signal = try XevApi.Async.init(),
                .xev_completion = .{},
                .pending = pending,
                .output_buffer = .{},
                .column_metas = null,
                .task_error = null,
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            self.async_signal.deinit();
            self.output_buffer.deinit(allocator);
            if (self.column_metas) |metas| {
                for (metas) |*m| {
                    m.encodings.deinit(allocator);
                    m.path_in_schema.deinit(allocator);
                }
                allocator.free(metas);
            }
        }

        /// Schedule this completion on the thread pool
        pub fn scheduleOn(self: *Self, loop: *XevApi.Loop, pool: *xev.ThreadPool) void {
            // Arm async wait first
            self.async_signal.wait(loop, &self.xev_completion, Self, self, asyncCallback);

            // Schedule task on thread pool
            pool.schedule(xev.ThreadPool.Batch.from(&self.task));
        }

        /// Thread pool callback - runs filter, encode, and slot write
        fn taskCallback(task: *xev.ThreadPool.Task) void {
            const self: *Self = @fieldParentPtr("task", task);
            const allocator = self.worker.allocator;

            var trace = Trace.init();

            // Step 1: Execute filter (CPU work)
            self.worker.execute();
            trace.mark("execute");

            if (self.worker.status == .failed) {
                self.task_error = self.worker.err;
                self.async_signal.notify() catch {};
                return;
            }

            // Step 2: Encode to buffer (if we have output)
            if (self.worker.row_count > 0) {
                self.column_metas = self.worker.encodeToBuffer(&self.output_buffer, self.compression) catch |err| {
                    self.task_error = err;
                    self.async_signal.notify() catch {};
                    return;
                };
                trace.mark("encode");

                // Step 3: Write to slot via pwrite (thread-safe)
                self.slot_writer_ptr.writeSlot(self.slot_index, self.output_buffer.items) catch |err| {
                    self.task_error = err;
                    self.async_signal.notify() catch {};
                    return;
                };
                trace.mark("write_slot");
            } else {
                // Empty result - write nothing but still need metadata
                self.column_metas = allocator.alloc(slot_writer.ColumnMeta, 0) catch null;
            }

            // Signal completion
            self.async_signal.notify() catch {};
        }

        /// Async callback - runs on main loop when task completes
        fn asyncCallback(
            ud: ?*Self,
            _: *XevApi.Loop,
            _: *XevApi.Completion,
            _: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            const self = ud.?;
            _ = self.pending.fetchSub(1, .release);
            return .disarm;
        }

        /// Get the row group metadata after completion
        pub fn getRowGroupMeta(self: *const Self) slot_writer.RowGroupMeta {
            return self.worker.getRowGroupMeta(
                self.column_metas orelse &[_]slot_writer.ColumnMeta{},
                self.output_buffer.items.len,
            );
        }
    };
}

// Basic compile-time verification tests
test "RowGroupWorker struct has expected fields" {
    // Verify the struct compiles and has the right shape
    const worker_size = @sizeOf(RowGroupWorker);
    try std.testing.expect(worker_size > 0);

    const ctx_size = @sizeOf(FilterContext);
    try std.testing.expect(ctx_size > 0);

    const data_size = @sizeOf(RowGroupData);
    try std.testing.expect(data_size > 0);
}

test "OutputColumnData can be initialized and deinitialized" {
    const allocator = std.testing.allocator;

    var col = OutputColumnData{ .col_type = .INT32 };

    // Allocate some data
    col.int32_values = try allocator.alloc(i32, 10);
    @memset(col.int32_values.?, 42);

    // Verify data
    try std.testing.expectEqual(@as(i32, 42), col.int32_values.?[0]);
    try std.testing.expectEqual(@as(usize, 10), col.int32_values.?.len);

    // Cleanup
    col.deinit(allocator);
    try std.testing.expect(col.int32_values == null);
}

test "OutputColumnData byte array cleanup" {
    const allocator = std.testing.allocator;

    var col = OutputColumnData{ .col_type = .BYTE_ARRAY };

    // Allocate byte array values
    col.byte_array_values = try allocator.alloc([]const u8, 3);
    col.byte_array_values.?[0] = try allocator.dupe(u8, "hello");
    col.byte_array_values.?[1] = try allocator.dupe(u8, "world");
    col.byte_array_values.?[2] = try allocator.dupe(u8, "test");

    try std.testing.expectEqual(@as(usize, 3), col.byte_array_values.?.len);
    try std.testing.expectEqualStrings("hello", col.byte_array_values.?[0]);

    // Cleanup should free all nested allocations
    col.deinit(allocator);
    try std.testing.expect(col.byte_array_values == null);
}
