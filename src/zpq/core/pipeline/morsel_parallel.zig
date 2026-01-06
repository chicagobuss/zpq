const std = @import("std");
const xev = @import("xev");
const pipeline_mod = @import("../pipeline.zig");
const pipeline_types = @import("types.zig");
const schema_mod = @import("../schema.zig");
const filters_mod = @import("../filters/mod.zig");
const row_group_worker_mod = @import("../row_group_worker.zig");
const interface = @import("../../io/interface.zig");
const factory = @import("../../io/s3/factory.zig");
const morsel_mod = @import("../morsel.zig");

const Pipeline = pipeline_mod.Pipeline;
const ExecutionResult = pipeline_types.ExecutionResult;
const Filter = filters_mod.Filter;
const RowGroupWorker = row_group_worker_mod.RowGroupWorker;
const RowGroupData = row_group_worker_mod.RowGroupData;
const FilterContext = row_group_worker_mod.FilterContext;
const Range = interface.Range;
const SchemaElement = schema_mod.SchemaElement;
const Type = schema_mod.Type;
const ColumnChunk = schema_mod.ColumnChunk;
const Trace = pipeline_types.Trace;

pub fn execute(self: *Pipeline) !ExecutionResult {
    const loop = self.loop orelse return error.NoRuntime;
    const pool = self.thread_pool orelse return error.NoRuntime;
    // Surgical check is handled in pipeline.zig or passed down
    return executeWithLoop(self, xev.Dynamic, loop, pool);
}

pub fn executeWithLoop(self: *Pipeline, comptime XevApi: type, loop: *XevApi.Loop, thread_pool: *xev.ThreadPool) !ExecutionResult {
    const MorselCoordinator = morsel_mod.MorselCoordinatorGen(XevApi);

    var trace = Trace.init();
    var timer = try std.time.Timer.start();

    // Validate requirements
    const output_path = self.output_path orelse return error.NoOutputPath;
    // Morsel mode only supports S3 output
    if (!std.mem.startsWith(u8, output_path, "s3://")) {
        return error.MorselModeRequiresS3Output;
    }

    const pf = self.input_file.?;
    const meta = pf.metadata orelse return error.NoMetadata;

    // Parse S3 path
    const path_without_prefix = output_path[5..];
    const slash_idx = std.mem.indexOf(u8, path_without_prefix, "/") orelse return error.InvalidS3Path;
    const bucket = path_without_prefix[0..slash_idx];
    const key = path_without_prefix[slash_idx + 1 ..];

    // Load S3 config
    const s3_config = try factory.loadS3ConfigFromEnv(self.allocator);
    defer s3_config.deinit(self.allocator);

    // Find filter column indices and types for all predicates
    var filter_col_indices = try self.allocator.alloc(usize, self.predicates.len);
    defer self.allocator.free(filter_col_indices);
    var filter_col_types = try self.allocator.alloc(Type, self.predicates.len);
    defer self.allocator.free(filter_col_types);

    for (self.predicates, 0..) |p, p_idx| {
        var found = false;
        if (meta.row_groups.items.len > 0) {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], p.column)) {
                        filter_col_indices[p_idx] = idx;
                        filter_col_types[p_idx] = md.type;
                        found = true;
                        break;
                    }
                }
            }
        }
        if (!found) return error.ColumnNotFound;
    }

    // Build output column list
    var output_col_indices = std.ArrayListUnmanaged(usize){};
    defer output_col_indices.deinit(self.allocator);
    var output_col_types = std.ArrayListUnmanaged(Type){};
    defer output_col_types.deinit(self.allocator);
    var output_col_names = std.ArrayListUnmanaged([]const u8){};
    defer output_col_names.deinit(self.allocator);

    // Track which filter columns are in the output
    var filter_cols_in_output = try self.allocator.alloc(bool, self.predicates.len);
    defer self.allocator.free(filter_cols_in_output);
    @memset(filter_cols_in_output, false);
    var filter_col_output_indices = try self.allocator.alloc(?usize, self.predicates.len);
    defer self.allocator.free(filter_col_output_indices);
    @memset(filter_col_output_indices, null);

    if (self.projection) |proj_cols| {
        for (proj_cols) |col_name| {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], col_name)) {
                        try output_col_indices.append(self.allocator, idx);
                        try output_col_types.append(self.allocator, md.type);
                        try output_col_names.append(self.allocator, col_name);

                        // Check if this is one of our filter columns
                        for (filter_col_indices, 0..) |f_idx, p_idx| {
                            if (idx == f_idx) {
                                filter_cols_in_output[p_idx] = true;
                                filter_col_output_indices[p_idx] = output_col_indices.items.len - 1;
                            }
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

                // Check if this is one of our filter columns
                for (filter_col_indices, 0..) |f_idx, p_idx| {
                    if (idx == f_idx) {
                        filter_cols_in_output[p_idx] = true;
                        filter_col_output_indices[p_idx] = output_col_indices.items.len - 1;
                    }
                }
            }
        }
    }

    // Build output schema for coordinator
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

    // Create all filters
    var filters = try self.allocator.alloc(Filter, self.predicates.len);
    defer {
        for (filters) |*f| f.deinit();
        self.allocator.free(filters);
    }
    for (self.predicates, 0..) |p, p_idx| {
        filters[p_idx] = if (p.op == .between)
            try Filter.fromBetween(self.allocator, p.value, p.value2.?, filter_col_types[p_idx])
        else
            try Filter.fromPredicate(self.allocator, p.op.toFilterOp(), p.value, filter_col_types[p_idx]);
    }

    // PHASE 0: Row group pruning using column statistics
    const total_rg_count = meta.row_groups.items.len;
    var active_rg_indices = std.ArrayListUnmanaged(usize){};
    defer active_rg_indices.deinit(self.allocator);

    for (0..total_rg_count) |rg_idx| {
        var skip = false;
        for (self.predicates, 0..) |p, p_idx| {
            if (pf.shouldSkipRowGroup(rg_idx, p.column, &filters[p_idx])) {
                skip = true;
                break;
            }
        }
        if (!skip) {
            try active_rg_indices.append(self.allocator, rg_idx);
        }
    }

    const rg_count = active_rg_indices.items.len;
    const col_count = output_col_indices.items.len;

    // Identify unique extra filter columns to fetch (those not in output)
    var extra_filter_col_indices = std.ArrayListUnmanaged(usize){};
    defer extra_filter_col_indices.deinit(self.allocator);
    for (filter_col_indices, 0..) |f_idx, p_idx| {
        if (!filter_cols_in_output[p_idx]) {
            var duplicate = false;
            for (extra_filter_col_indices.items) |existing_idx| {
                if (existing_idx == f_idx) {
                    duplicate = true;
                    break;
                }
            }
            if (!duplicate) {
                try extra_filter_col_indices.append(self.allocator, f_idx);
            }
        }
    }
    const num_extra_filters = extra_filter_col_indices.items.len;

    // Initialize MorselCoordinator
    var coordinator = MorselCoordinator.init(
        self.allocator,
        loop,
        thread_pool,
        .{
            .bucket = bucket,
            .key = key,
            .region = s3_config.region,
            .endpoint = s3_config.endpoint,
            .max_in_flight = 8,
            .access_key = if (s3_config.credentials) |c| c.access_key else null,
            .secret_key = if (s3_config.credentials) |c| c.secret_key else null,
            .session_token = if (s3_config.credentials) |c| c.session_token else null,
        },
    );
    defer coordinator.deinit();

    // Start multipart upload
    try coordinator.start(output_schema.items, rg_count);
    errdefer coordinator.abort();

    // Pre-fetch all row group data
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

    var all_extra_filter_bufs = try self.allocator.alloc([][]u8, rg_count);
    defer {
        for (all_extra_filter_bufs) |bufs| {
            for (bufs) |buf| self.allocator.free(buf);
            self.allocator.free(bufs);
        }
        self.allocator.free(all_extra_filter_bufs);
    }

    var all_extra_filter_offsets = try self.allocator.alloc([]u64, rg_count);
    defer {
        for (all_extra_filter_offsets) |offs| self.allocator.free(offs);
        self.allocator.free(all_extra_filter_offsets);
    }

    var all_extra_filter_chunks = try self.allocator.alloc([]ColumnChunk, rg_count);
    defer {
        for (all_extra_filter_chunks) |chunks| self.allocator.free(chunks);
        self.allocator.free(all_extra_filter_chunks);
    }

    var max_input_rg_size: u64 = 0;
    var total_ranges: usize = 0;
    for (active_rg_indices.items) |orig_rg_idx| {
        const rg_meta = meta.row_groups.items[orig_rg_idx];
        total_ranges += col_count;
        total_ranges += num_extra_filters;
        const rg_size: u64 = @intCast(rg_meta.total_byte_size);
        if (rg_size > max_input_rg_size) max_input_rg_size = rg_size;
    }

    var ranges = try self.allocator.alloc(Range, total_ranges);
    defer self.allocator.free(ranges);
    var buffers = try self.allocator.alloc([]u8, total_ranges);
    defer self.allocator.free(buffers);

    var range_idx: usize = 0;
    for (active_rg_indices.items, 0..) |orig_rg_idx, local_rg_idx| {
        const rg_meta = meta.row_groups.items[orig_rg_idx];

        all_output_bufs[local_rg_idx] = try self.allocator.alloc([]u8, col_count);
        all_output_offsets[local_rg_idx] = try self.allocator.alloc(u64, col_count);
        all_output_chunks[local_rg_idx] = try self.allocator.alloc(ColumnChunk, col_count);

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
            all_output_bufs[local_rg_idx][out_col_idx] = buffers[range_idx];
            all_output_offsets[local_rg_idx][out_col_idx] = start;
            all_output_chunks[local_rg_idx][out_col_idx] = chunk;
            range_idx += 1;
        }

        all_extra_filter_bufs[local_rg_idx] = try self.allocator.alloc([]u8, num_extra_filters);
        all_extra_filter_offsets[local_rg_idx] = try self.allocator.alloc(u64, num_extra_filters);
        all_extra_filter_chunks[local_rg_idx] = try self.allocator.alloc(ColumnChunk, num_extra_filters);

        for (extra_filter_col_indices.items, 0..) |col_idx, extra_idx| {
            const chunk = rg_meta.columns.items[col_idx];
            const md = chunk.meta_data.?;
            var start: u64 = @intCast(md.data_page_offset);
            if (md.dictionary_page_offset) |dpo| {
                if (dpo < start) start = @intCast(dpo);
            }
            const len: u64 = @intCast(md.total_compressed_size);

            ranges[range_idx] = .{ .start = start, .end = start + len };
            buffers[range_idx] = try self.allocator.alloc(u8, @intCast(len));
            all_extra_filter_bufs[local_rg_idx][extra_idx] = buffers[range_idx];
            all_extra_filter_offsets[local_rg_idx][extra_idx] = start;
            all_extra_filter_chunks[local_rg_idx][extra_idx] = chunk;
            range_idx += 1;
        }
    }

    try pf.source.readRanges(ranges, buffers);

    for (active_rg_indices.items, 0..) |orig_rg_idx, local_rg_idx| {
        const rg_meta = meta.row_groups.items[orig_rg_idx];
        var rg_filter_bufs = try self.allocator.alloc([]const u8, self.predicates.len);
        var rg_filter_offsets = try self.allocator.alloc(u64, self.predicates.len);
        var rg_filter_chunks = try self.allocator.alloc(schema_mod.ColumnChunk, self.predicates.len);

        for (filter_col_indices, 0..) |f_col_idx, p_idx| {
            if (filter_cols_in_output[p_idx]) {
                const out_idx = filter_col_output_indices[p_idx].?;
                rg_filter_bufs[p_idx] = all_output_bufs[local_rg_idx][out_idx];
                rg_filter_offsets[p_idx] = all_output_offsets[local_rg_idx][out_idx];
                rg_filter_chunks[p_idx] = all_output_chunks[local_rg_idx][out_idx];
            } else {
                // Find in extra filters
                var found = false;
                for (extra_filter_col_indices.items, 0..) |extra_col_idx, extra_idx| {
                    if (extra_col_idx == f_col_idx) {
                        rg_filter_bufs[p_idx] = all_extra_filter_bufs[local_rg_idx][extra_idx];
                        rg_filter_offsets[p_idx] = all_extra_filter_offsets[local_rg_idx][extra_idx];
                        rg_filter_chunks[p_idx] = all_extra_filter_chunks[local_rg_idx][extra_idx];
                        found = true;
                        break;
                    }
                }
                if (!found) unreachable;
            }
        }

        const const_bufs: []const []const u8 = @ptrCast(all_output_bufs[local_rg_idx]);
        all_rg_data[local_rg_idx] = RowGroupData{
            .rg_idx = orig_rg_idx,
            .num_rows = @intCast(rg_meta.num_rows),
            .filter_bufs = rg_filter_bufs,
            .filter_offsets = rg_filter_offsets,
            .filter_chunks = rg_filter_chunks,
            .output_bufs = const_bufs,
            .output_offsets = all_output_offsets[local_rg_idx],
            .output_chunks = all_output_chunks[local_rg_idx],
        };
    }

    var filter_col_names = try self.allocator.alloc([]const u8, self.predicates.len);
    var filter_vals = try self.allocator.alloc([]const u8, self.predicates.len);
    for (self.predicates, 0..) |p, i| {
        filter_col_names[i] = p.column;
        filter_vals[i] = p.value;
    }

    const filter_ctx = FilterContext{
        .allocator = self.allocator,
        .filter_cols_in_output = try self.allocator.dupe(bool, filter_cols_in_output),
        .filter_col_output_indices = try self.allocator.dupe(?usize, filter_col_output_indices),
        .filters = try self.allocator.dupe(Filter, filters),
        .filter_col_names = filter_col_names,
        .filter_vals = filter_vals,
        .filter_col_indices = try self.allocator.dupe(usize, filter_col_indices),
        .filter_col_types = try self.allocator.dupe(schema_mod.Type, filter_col_types),
        .output_col_indices = output_col_indices.items,
        .output_col_types = output_col_types.items,
        .output_col_names = output_col_names.items,
        .meta = &meta,
    };

    // Execute workers via coordinator
    var total_input_rows: u64 = 0;
    for (all_rg_data) |rg_data| total_input_rows += rg_data.num_rows;

    trace.mark("setup_workers");

    const Completion = MorselCompletionGen(XevApi);
    var workers = try self.allocator.alloc(*RowGroupWorker, rg_count);
    defer self.allocator.free(workers);

    var completions = try self.allocator.alloc(Completion, rg_count);
    defer {
        for (completions) |*c| c.deinit(self.allocator);
        self.allocator.free(completions);
    }

    var pending = std.atomic.Value(usize).init(rg_count);

    for (all_rg_data, 0..) |*rg_data, i| {
        workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
        completions[i] = try Completion.init(
            workers[i],
            &coordinator,
            self.allocator,
            self.output_compression,
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

    // Check for errors and calculate total rows
    var total_output_rows: u64 = 0;
    for (completions, workers) |*c, worker| {
        defer worker.deinit(); // Always cleanup workers
        if (c.task_error) |err| return err;
        total_output_rows += worker.row_count;
    }

    trace.mark("workers_done");

    // Finish upload
    try coordinator.finishSubmissions();
    const success = try coordinator.waitForCompletion(null);
    if (!success) return error.MorselUploadFailed;

    trace.mark("finished");

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    return ExecutionResult{
        .input_rows = total_input_rows,
        .output_rows = total_output_rows,
        .elapsed_ms = elapsed_ms,
    };
}

fn MorselCompletionGen(comptime XevApi: type) type {
    const MorselCoordinator = morsel_mod.MorselCoordinatorGen(XevApi);
    return struct {
        const Self = @This();

        worker: *RowGroupWorker,
        coordinator: *MorselCoordinator,
        allocator: std.mem.Allocator,
        compression: schema_mod.CompressionCodec,
        pending: *std.atomic.Value(usize),

        task: xev.ThreadPool.Task,
        async_signal: XevApi.Async,
        xev_completion: XevApi.Completion,
        task_error: ?anyerror = null,

        pub fn init(
            worker: *RowGroupWorker,
            coordinator: *MorselCoordinator,
            allocator: std.mem.Allocator,
            compression: schema_mod.CompressionCodec,
            pending: *std.atomic.Value(usize),
        ) !Self {
            return Self{
                .worker = worker,
                .coordinator = coordinator,
                .allocator = allocator,
                .compression = compression,
                .pending = pending,
                .task = .{ .callback = taskCallback },
                .async_signal = try XevApi.Async.init(),
                .xev_completion = .{},
            };
        }

        pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
            _ = allocator;
            self.async_signal.deinit();
        }

        pub fn scheduleOn(self: *Self, loop: *XevApi.Loop, pool: *xev.ThreadPool) void {
            self.async_signal.wait(loop, &self.xev_completion, Self, self, asyncCallback);
            pool.schedule(xev.ThreadPool.Batch.from(&self.task));
        }

        fn taskCallback(task: *xev.ThreadPool.Task) void {
            const self: *Self = @fieldParentPtr("task", task);
            self.worker.execute();
            self.async_signal.notify() catch {};
        }

        fn asyncCallback(
            ud: ?*Self,
            _: *XevApi.Loop,
            _: *XevApi.Completion,
            _: XevApi.Async.WaitError!void,
        ) XevApi.CallbackAction {
            const self = ud.?;
            defer _ = self.pending.fetchSub(1, .release);

            if (self.worker.err) |err| {
                self.task_error = err;
                return .disarm;
            }

            // Encode logic reused from surgical/morsel flow
            var encode_buffer = std.ArrayListUnmanaged(u8){};
            // submitMorsel copies data, so we can free this after call
            defer encode_buffer.deinit(self.allocator);

            const col_metas_slot = self.worker.encodeToBuffer(&encode_buffer, self.compression) catch |err| {
                self.task_error = err;
                return .disarm;
            };
            defer {
                for (col_metas_slot) |*cm| {
                    cm.path_in_schema.deinit(self.allocator);
                    cm.encodings.deinit(self.allocator);
                }
                self.allocator.free(col_metas_slot);
            }

            // Convert to Morsel ColumnChunkMeta
            const col_count = col_metas_slot.len;
            var col_metas = self.allocator.alloc(morsel_mod.ColumnChunkMeta, col_count) catch |err| {
                self.task_error = err;
                return .disarm;
            };
            // submitMorsel dupes columns, so we free this array after call
            defer self.allocator.free(col_metas);

            var relative_offset: u64 = 0;
            for (col_metas_slot, 0..) |slot_meta, col_idx| {
                col_metas[col_idx] = .{
                    .column_index = col_idx,
                    .relative_offset = relative_offset,
                    .compressed_size = @intCast(slot_meta.compressed_size),
                    .uncompressed_size = @intCast(slot_meta.uncompressed_size),
                    .num_values = @intCast(slot_meta.num_values),
                };
                relative_offset += @intCast(slot_meta.compressed_size);
            }

            const rg_meta_morsel = morsel_mod.RowGroupMeta{
                .row_group_index = self.worker.data.rg_idx,
                .num_rows = self.worker.row_count,
                .total_byte_size = encode_buffer.items.len,
                .columns = col_metas,
            };

            _ = self.coordinator.submitMorsel(encode_buffer.items, rg_meta_morsel) catch |err| {
                self.task_error = err;
                return .disarm;
            };

            return .disarm;
        }
    };
}
