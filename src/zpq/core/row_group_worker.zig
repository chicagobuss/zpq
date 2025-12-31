const std = @import("std");
const xev = @import("xev");

const SelectionVector = @import("selection.zig").SelectionVector;
const FilterColumnCache = @import("filter_cache.zig").FilterColumnCache;
const EncodedFilter = @import("filter.zig").EncodedFilter;
const BatchReader = @import("batch_reader.zig").BatchReader;
const simd = @import("simd.zig");
const page_writer = @import("page_writer.zig");
const slot_writer = @import("slot_writer.zig");
const thrift = @import("thrift.zig");

const schema = @import("schema.zig");
const column_mod = @import("column.zig");
const ColumnReader = column_mod.ColumnReader;
const interface = @import("../io/interface.zig");
const MemorySource = interface.local.MemorySource;

// ============================================================================
// Parallel Execution Support (Phase 2.3)
// ============================================================================

/// Completion tracker for parallel worker execution via xev ThreadPool.
/// Each WorkerCompletion manages one RowGroupWorker's lifecycle through the thread pool.
pub const WorkerCompletion = struct {
    const Self = @This();

    /// The worker being executed
    worker: *RowGroupWorker,

    /// Thread pool task (embedded for zero-allocation scheduling)
    task: xev.ThreadPool.Task,

    /// Async signal to notify main loop when worker completes
    async_signal: xev.Async,

    /// xev completion for async wait
    xev_completion: xev.Completion,

    /// Pointer to shared pending counter (atomic)
    pending: *std.atomic.Value(usize),

    /// Initialize a WorkerCompletion for a given worker
    pub fn init(worker: *RowGroupWorker, pending: *std.atomic.Value(usize)) !Self {
        return Self{
            .worker = worker,
            .task = .{ .callback = taskCallback },
            .async_signal = try xev.Async.init(),
            .xev_completion = .{},
            .pending = pending,
        };
    }

    /// Clean up resources
    pub fn deinit(self: *Self) void {
        self.async_signal.deinit();
    }

    /// Arm the async wait on the loop and schedule the task on the thread pool
    pub fn scheduleOn(self: *Self, loop: *xev.Loop, pool: *xev.ThreadPool) void {
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
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.Async.WaitError!void,
    ) xev.CallbackAction {
        const self = ud.?;

        // Decrement the pending counter (atomic)
        _ = self.pending.fetchSub(1, .release);

        // Disarm - we only need one notification per worker
        return .disarm;
    }
};

/// Pre-fetched column data for a single row group.
/// All I/O happens BEFORE the worker starts - worker does only CPU work.
pub const RowGroupData = struct {
    rg_idx: usize,
    num_rows: usize,

    // Filter column data
    filter_buf: []const u8,
    filter_offset: u64,
    filter_chunk: schema.ColumnChunk,

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

    // Filter specification
    filter_col_name: []const u8,
    filter_val: []const u8,
    filter_col_idx: usize,
    filter_col_type: schema.Type,
    encoded_filter: *const EncodedFilter,

    // Output column specification
    output_col_indices: []const usize,
    output_col_types: []const schema.Type,
    output_col_names: []const []const u8,
    filter_col_in_output: bool,
    filter_col_output_idx: ?usize,

    // File metadata (read-only)
    meta: *const schema.FileMetaData,
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
        self.status = .scanning;

        // Phase 1: Scan filter column to build selection vector
        self.scanFilterColumn() catch |e| {
            self.status = .failed;
            self.err = e;
            return;
        };

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

        // Phase 3: Materialize filter column values (if needed)
        self.status = .materializing;
        self.materializeFilterColumn() catch |e| {
            self.status = .failed;
            self.err = e;
            return;
        };

        self.status = .done;
        self.row_count = self.selection.count();
    }

    fn scanFilterColumn(self: *Self) !void {
        const num_rows = self.data.num_rows;

        // Create memory source from pre-fetched buffer
        var mem_source = MemorySource.initWithOffset(self.data.filter_buf, self.data.filter_offset);

        // Get filter column metadata
        const filter_md = self.data.filter_chunk.meta_data.?;
        const filter_levels = self.ctx.meta.getColumnLevels(filter_md.path_in_schema.items);
        const filter_schema_elem = self.ctx.meta.getColumnSchema(filter_md.path_in_schema.items);
        const filter_type_len = if (filter_schema_elem) |se| se.type_length else null;

        // Create column reader
        const filter_col_reader = try ColumnReader.init(mem_source.source(), self.data.filter_chunk);

        // Pre-allocate for expected matches (~20% selectivity)
        try self.selection.ensureCapacity(num_rows / 4);

        // Initialize filter cache if filter column is in output
        if (self.ctx.filter_col_in_output) {
            self.filter_cache = FilterColumnCache.init(self.allocator, self.ctx.filter_col_type);
            try self.filter_cache.?.ensureCapacity(num_rows / 4);
        }

        // Scan based on type
        switch (self.ctx.filter_col_type) {
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                try self.scanByteArrayColumn(filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows);
            },
            .INT32 => try self.scanTypedColumn(i32, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
            .INT64 => try self.scanTypedColumn(i64, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
            .FLOAT => try self.scanTypedColumn(f32, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
            .DOUBLE => try self.scanTypedColumn(f64, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
            .BOOLEAN => try self.scanTypedColumn(bool, filter_col_reader, filter_md, filter_levels, filter_type_len, num_rows),
            else => return error.UnsupportedFilterType,
        }
    }

    fn scanByteArrayColumn(
        self: *Self,
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

        var row_idx: usize = 0;
        var buf: [1024]?[]const u8 = undefined;

        // Try dictionary fast path
        var target_dict_idx: ?u64 = null;
        var use_dict_fast_path = false;

        while (row_idx < num_rows) {
            const batch_size = @min(1024, num_rows - row_idx);

            // Check for dictionary fast path
            if (target_dict_idx == null and reader.hasDictionary()) {
                target_dict_idx = reader.findInDictionary(self.ctx.filter_val);
                use_dict_fast_path = target_dict_idx != null;
            }

            if (use_dict_fast_path) {
                // Fast path: compare dictionary indices
                var sel_batch = simd.SelectionVector.init();
                const n_read = try reader.scanDictIndicesIntoBatch(target_dict_idx.?, &sel_batch, batch_size);
                if (n_read == 0) break;

                for (0..n_read) |i| {
                    if (sel_batch.isSet(i)) {
                        try self.selection.append(row_idx + i);
                    }
                }
                // Don't cache values for dict path - we'll use filter_val directly
                row_idx += n_read;
                self.used_dict_fast_path = true;
            } else {
                // Slow path: decode and compare
                const n_read = try reader.nextBatch(buf[0..batch_size]);
                if (n_read == 0) break;

                for (buf[0..n_read], 0..) |maybe_val, i| {
                    if (maybe_val) |v| {
                        if (self.ctx.encoded_filter.matchesBytes(v)) {
                            try self.selection.append(row_idx + i);
                            if (self.filter_cache) |*fc| {
                                try fc.appendByteArray(v);
                            }
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
        col_reader: ColumnReader,
        md: schema.ColumnMetaData,
        levels: schema.Levels,
        type_len: ?i32,
        num_rows: usize,
    ) !void {
        var reader = BatchReader(T).init(
            self.allocator,
            col_reader,
            md.type,
            @intCast(levels.max_def),
            @intCast(levels.max_rep),
            type_len,
        );
        defer reader.deinit();

        var row_idx: usize = 0;
        var buf: [1024]?T = undefined;

        while (row_idx < num_rows) {
            const batch_size = @min(1024, num_rows - row_idx);
            const n_read = try reader.nextBatch(buf[0..batch_size]);
            if (n_read == 0) break;

            for (buf[0..n_read], 0..) |maybe_val, i| {
                if (maybe_val) |v| {
                    const value_bytes = std.mem.asBytes(&v);
                    if (self.ctx.encoded_filter.matchesBytes(value_bytes)) {
                        try self.selection.append(row_idx + i);
                        if (self.filter_cache) |*fc| {
                            if (T == i32) try fc.appendInt32(v) else if (T == i64) try fc.appendInt64(v) else if (T == f32) try fc.appendFloat(v) else if (T == f64) try fc.appendDouble(v) else if (T == bool) try fc.appendBool(v);
                        }
                    }
                }
            }
            row_idx += n_read;
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

            // Skip filter column - it will be materialized separately
            if (col_idx == self.ctx.filter_col_idx) {
                continue;
            }

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

            try self.readSelectedColumn(col_reader, col_type, levels, type_len, out_idx);
        }
    }

    fn readSelectedColumn(
        self: *Self,
        col_reader: ColumnReader,
        col_type: schema.Type,
        levels: schema.Levels,
        type_len: ?i32,
        out_idx: usize,
    ) !void {
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
        var values = try self.allocator.alloc(T, indices.len);
        errdefer self.allocator.free(values);

        var current_row: usize = 0;
        var out_idx: usize = 0;
        var batch_buf: [1024]?T = undefined;

        for (indices) |target_row| {
            // Skip to target row
            if (target_row > current_row) {
                try reader.skip(target_row - current_row);
                current_row = target_row;
            }

            // Read one value
            const n = try reader.nextBatch(batch_buf[0..1]);
            if (n == 1) {
                values[out_idx] = batch_buf[0] orelse std.mem.zeroes(T);
                out_idx += 1;
            }
            current_row += 1;
        }

        return values[0..out_idx];
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
        var values = try self.allocator.alloc([]const u8, indices.len);
        errdefer {
            for (values) |v| self.allocator.free(v);
            self.allocator.free(values);
        }

        var current_row: usize = 0;
        var out_idx: usize = 0;
        var batch_buf: [1]?[]const u8 = undefined;

        for (indices) |target_row| {
            if (target_row > current_row) {
                try reader.skip(target_row - current_row);
                current_row = target_row;
            }

            const n = try reader.nextBatch(&batch_buf);
            if (n == 1) {
                if (batch_buf[0]) |v| {
                    values[out_idx] = try self.allocator.dupe(u8, v);
                } else {
                    values[out_idx] = try self.allocator.dupe(u8, "");
                }
                out_idx += 1;
            }
            current_row += 1;
        }

        return values[0..out_idx];
    }

    fn materializeFilterColumn(self: *Self) !void {
        // Handle filter column if it's in output
        if (!self.ctx.filter_col_in_output) return;

        const out_idx = self.ctx.filter_col_output_idx.?;
        self.output_columns[out_idx].col_type = self.ctx.filter_col_type;

        // If dictionary fast path was used at all, regenerate ALL values from filter_val.
        // This handles the mixed case where first page is PLAIN but rest are dictionary.
        if (self.used_dict_fast_path) {
            // Generate values from filter_val (all matching rows have same value)
            const count = self.selection.count();
            switch (self.ctx.filter_col_type) {
                .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                    const values = try self.allocator.alloc([]const u8, count);
                    // Dupe filter_val for each entry since deinit will free them individually
                    for (values) |*v| {
                        v.* = try self.allocator.dupe(u8, self.ctx.filter_val);
                    }
                    self.output_columns[out_idx].byte_array_values = values;
                },
                .INT32 => {
                    const parsed = try std.fmt.parseInt(i32, self.ctx.filter_val, 10);
                    const values = try self.allocator.alloc(i32, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].int32_values = values;
                },
                .INT64 => {
                    const parsed = try std.fmt.parseInt(i64, self.ctx.filter_val, 10);
                    const values = try self.allocator.alloc(i64, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].int64_values = values;
                },
                .FLOAT => {
                    const parsed = try std.fmt.parseFloat(f32, self.ctx.filter_val);
                    const values = try self.allocator.alloc(f32, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].float_values = values;
                },
                .DOUBLE => {
                    const parsed = try std.fmt.parseFloat(f64, self.ctx.filter_val);
                    const values = try self.allocator.alloc(f64, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].double_values = values;
                },
                .BOOLEAN => {
                    const parsed = std.mem.eql(u8, self.ctx.filter_val, "true");
                    const values = try self.allocator.alloc(bool, count);
                    @memset(values, parsed);
                    self.output_columns[out_idx].bool_values = values;
                },
                else => {},
            }
        } else {
            // Use cached values from filter scan
            // NOTE: For byte arrays, we transfer ownership from filter_cache to output_columns
            switch (self.ctx.filter_col_type) {
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

            // Encode values based on type
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
            try encodings.append(self.allocator, .PLAIN);

            column_metas[i] = slot_writer.ColumnMeta{
                .type = col_type,
                .encodings = encodings,
                .path_in_schema = path,
                .codec = compression,
                .num_values = @intCast(self.row_count),
                .uncompressed_size = cw.total_uncompressed_size,
                .compressed_size = @intCast(col_size),
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

/// Completion tracker for slot-based parallel worker execution.
/// Extends WorkerCompletion to encode and write output to a slot.
pub const SlotWriteCompletion = struct {
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
    async_signal: xev.Async,

    /// xev completion for async wait
    xev_completion: xev.Completion,

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
            .async_signal = try xev.Async.init(),
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
    pub fn scheduleOn(self: *Self, loop: *xev.Loop, pool: *xev.ThreadPool) void {
        // Arm async wait first
        self.async_signal.wait(loop, &self.xev_completion, Self, self, asyncCallback);

        // Schedule task on thread pool
        pool.schedule(xev.ThreadPool.Batch.from(&self.task));
    }

    /// Thread pool callback - runs filter, encode, and slot write
    fn taskCallback(task: *xev.ThreadPool.Task) void {
        const self: *Self = @fieldParentPtr("task", task);
        const allocator = self.worker.allocator;

        // Step 1: Execute filter (CPU work)
        self.worker.execute();

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

            // Step 3: Write to slot via pwrite (thread-safe)
            self.slot_writer_ptr.writeSlot(self.slot_index, self.output_buffer.items) catch |err| {
                self.task_error = err;
                self.async_signal.notify() catch {};
                return;
            };
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
        _: *xev.Loop,
        _: *xev.Completion,
        _: xev.Async.WaitError!void,
    ) xev.CallbackAction {
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
