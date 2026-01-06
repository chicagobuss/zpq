const std = @import("std");
const xev = @import("xev");
const pipeline_mod = @import("../pipeline.zig");
const pipeline_types = @import("types.zig");
const schema_mod = @import("../schema.zig");
const filters_mod = @import("../filters/mod.zig");
const row_group_worker_mod = @import("../row_group_worker.zig");
const interface = @import("../../io/interface.zig");
const slot_writer_mod = @import("../slot_writer.zig");
const morsel_mod = @import("../morsel.zig");
const factory = @import("../../io/s3/factory.zig");
const surgical_engine = @import("../surgical/engine.zig");

const Pipeline = pipeline_mod.Pipeline;
const ExecutionResult = pipeline_types.ExecutionResult;
const Filter = filters_mod.Filter;
const RowGroupWorker = row_group_worker_mod.RowGroupWorker;
const RowGroupData = row_group_worker_mod.RowGroupData;
const FilterContext = row_group_worker_mod.FilterContext;
const SlotWriter = slot_writer_mod.SlotWriter;
const SlotWriteCompletionGen = row_group_worker_mod.SlotWriteCompletionGen;
const Range = interface.Range;
const SchemaElement = schema_mod.SchemaElement;
const Type = schema_mod.Type;
const ColumnChunk = schema_mod.ColumnChunk;
const Trace = pipeline_types.Trace;

pub fn executeSlotParallel(self: *Pipeline) !ExecutionResult {
    const loop = self.loop orelse return error.NoRuntime;
    const pool = self.thread_pool orelse return error.NoRuntime;
    return executeSlotParallelWithLoop(self, xev.Dynamic, loop, pool);
}

pub fn executeSlotParallelWithLoop(self: *Pipeline, comptime XevApi: type, loop: *XevApi.Loop, thread_pool: *xev.ThreadPool) !ExecutionResult {
    const Completion = SlotWriteCompletionGen(XevApi);
    const SurgicalEngine = surgical_engine.SurgicalEngine;

    var timer = try std.time.Timer.start();

    // Validate requirements
    const output_path = self.output_path orelse return error.NoOutputPath;
    if (self.predicates.len == 0) return error.NoFilter;
    const pred = &self.predicates[0];

    const pf = self.input_file.?;
    const meta = pf.metadata orelse return error.NoMetadata;

    // Find filter column index and type (Fallback to first predicate)
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

    // Build output schema
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

    // Create filter
    var filter = if (pred.op == .between)
        try Filter.fromBetween(self.allocator, pred.value, pred.value2.?, filter_col_type.?)
    else
        try Filter.fromPredicate(self.allocator, pred.op.toFilterOp(), pred.value, filter_col_type.?);
    defer filter.deinit();

    const col_count = output_col_indices.items.len;

    // === SURGICAL FETCH ===
    var engine = SurgicalEngine.init(
        self.allocator,
        pf,
        pred.column,
        pred.value,
        pred.value2,
        pred.op.toFilterOp(),
        output_col_names.items,
    );

    var surgical_result = try engine.executeAndAssemble();
    defer surgical_result.deinit(self.allocator);

    const active_rg_count = surgical_result.row_groups.len;

    std.debug.print("[SURGICAL-SLOT] Fetched {d} bytes (vs {d} full scan), {d}/{d} pages, {d} active row groups\n", .{
        surgical_result.bytes_fetched,
        surgical_result.bytes_full_scan,
        surgical_result.pages_total - surgical_result.pages_skipped,
        surgical_result.pages_total,
        active_rg_count,
    });

    if (active_rg_count == 0) {
        // No matching row groups - create empty output file
        var slot_writer = try SlotWriter.init(self.allocator, output_path, 0, 0, output_schema.items);
        defer slot_writer.deinit();
        const empty_rg_meta: []const slot_writer_mod.RowGroupMeta = &.{};
        try slot_writer.finish(empty_rg_meta);

        return ExecutionResult{
            .input_rows = 0,
            .output_rows = 0,
            .elapsed_ms = @as(f64, @floatFromInt(timer.read())) / 1_000_000.0,
        };
    }

    // Convert AssembledRowGroups to RowGroupData
    var rg_data_arrays = try self.allocator.alloc(struct {
        output_bufs: []const []const u8,
        output_offsets: []const u64,
        output_chunks: []const ColumnChunk,
    }, active_rg_count);
    defer {
        for (rg_data_arrays) |arr| {
            self.allocator.free(arr.output_bufs);
            self.allocator.free(@constCast(arr.output_offsets));
            self.allocator.free(@constCast(arr.output_chunks));
        }
        self.allocator.free(rg_data_arrays);
    }

    var all_rg_data = try self.allocator.alloc(RowGroupData, active_rg_count);
    defer self.allocator.free(all_rg_data);

    for (surgical_result.row_groups, 0..) |*assembled_rg, i| {
        all_rg_data[i] = try assembled_rg.toRowGroupData(self.allocator);
        rg_data_arrays[i] = .{
            .output_bufs = all_rg_data[i].output_bufs,
            .output_offsets = all_rg_data[i].output_offsets,
            .output_chunks = all_rg_data[i].output_chunks,
        };
    }

    const filter_ctx = FilterContext{
        .allocator = self.allocator,
        .filter_cols_in_output = try self.allocator.dupe(bool, &.{filter_col_in_output}),
        .filter_col_output_indices = try self.allocator.dupe(?usize, &.{filter_col_output_idx}),
        .filters = try self.allocator.dupe(Filter, &.{filter}),
        .filter_col_names = try self.allocator.dupe([]const u8, &.{pred.column}),
        .filter_vals = try self.allocator.dupe([]const u8, &.{pred.value}),
        .filter_col_indices = try self.allocator.dupe(usize, &.{filter_col_idx.?}),
        .filter_col_types = try self.allocator.dupe(schema_mod.Type, &.{filter_col_type.?}),
        .output_col_indices = output_col_indices.items,
        .output_col_types = output_col_types.items,
        .output_col_names = output_col_names.items,
        .meta = &meta,
    };

    // Estimate max output size per row group for slot allocation
    var max_input_rg_rows: usize = 0;
    for (surgical_result.row_groups) |*rg| {
        if (rg.num_rows > max_input_rg_rows) max_input_rg_rows = rg.num_rows;
    }
    const max_output_rg_size = max_input_rg_rows * col_count * 32; // Conservative estimate

    // Initialize SlotWriter
    var sw = try SlotWriter.init(self.allocator, output_path, active_rg_count, max_output_rg_size, output_schema.items);
    defer sw.deinit();

    // Setup workers and completions (same pattern as non-surgical)
    var total_input_rows: u64 = 0;

    var workers = try self.allocator.alloc(*RowGroupWorker, active_rg_count);
    defer self.allocator.free(workers);

    var completions = try self.allocator.alloc(Completion, active_rg_count);
    defer {
        for (completions) |*c| c.deinit(self.allocator);
        self.allocator.free(completions);
    }

    var pending = std.atomic.Value(usize).init(active_rg_count);

    for (all_rg_data, 0..) |*rg_data, i| {
        total_input_rows += rg_data.num_rows;
        workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
        completions[i] = try Completion.init(
            workers[i],
            &sw,
            i,
            self.output_compression,
            &pending,
        );
    }

    // Schedule on event loop
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
    var rg_metas = try self.allocator.alloc(slot_writer_mod.RowGroupMeta, active_rg_count);
    defer self.allocator.free(rg_metas);

    for (completions, workers, 0..) |*c, worker, i| {
        defer worker.deinit();
        if (c.task_error) |err| return err;
        total_output_rows += worker.row_count;
        rg_metas[i] = c.getRowGroupMeta();
    }

    try sw.finish(rg_metas);

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    return ExecutionResult{
        .input_rows = total_input_rows,
        .output_rows = total_output_rows,
        .elapsed_ms = elapsed_ms,
    };
}

pub fn executeMorselParallelSurgicalWithLoop(self: *Pipeline, comptime XevApi: type, loop: *XevApi.Loop, thread_pool: *xev.ThreadPool) !ExecutionResult {
    const MorselCoordinator = morsel_mod.MorselCoordinatorGen(XevApi);
    const SurgicalEngine = surgical_engine.SurgicalEngine;

    var timer = try std.time.Timer.start();

    // Validate requirements
    const output_path = self.output_path orelse return error.NoOutputPath;
    if (self.predicates.len == 0) return error.NoFilter;
    const pred = &self.predicates[0];

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

    // Find filter column
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

    // Create filter
    var filter = if (pred.op == .between)
        try Filter.fromBetween(self.allocator, pred.value, pred.value2.?, filter_col_type.?)
    else
        try Filter.fromPredicate(self.allocator, pred.op.toFilterOp(), pred.value, filter_col_type.?);
    defer filter.deinit();

    const col_count = output_col_indices.items.len;

    // === SURGICAL FETCH ===
    var engine = SurgicalEngine.init(
        self.allocator,
        pf,
        pred.column,
        pred.value,
        pred.value2,
        pred.op.toFilterOp(),
        output_col_names.items,
    );

    var surgical_result = try engine.executeAndAssemble();
    defer surgical_result.deinit(self.allocator);

    const active_rg_count = surgical_result.row_groups.len;

    std.debug.print("[SURGICAL-MORSEL] Fetched {d} bytes (vs {d} full scan), {d}/{d} pages, {d} active row groups\n", .{
        surgical_result.bytes_fetched,
        surgical_result.bytes_full_scan,
        surgical_result.pages_total - surgical_result.pages_skipped,
        surgical_result.pages_total,
        active_rg_count,
    });

    if (active_rg_count == 0) {
        // No matching row groups - return empty result
        return ExecutionResult{
            .input_rows = 0,
            .output_rows = 0,
            .elapsed_ms = @as(f64, @floatFromInt(timer.read())) / 1_000_000.0,
        };
    }

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

    try coordinator.start(output_schema.items, active_rg_count);
    errdefer coordinator.abort();

    // Convert AssembledRowGroups to RowGroupData
    var rg_data_arrays = try self.allocator.alloc(struct {
        output_bufs: []const []const u8,
        output_offsets: []const u64,
        output_chunks: []const ColumnChunk,
    }, active_rg_count);
    defer {
        for (rg_data_arrays) |arr| {
            self.allocator.free(arr.output_bufs);
            self.allocator.free(@constCast(arr.output_offsets));
            self.allocator.free(@constCast(arr.output_chunks));
        }
        self.allocator.free(rg_data_arrays);
    }

    var all_rg_data = try self.allocator.alloc(RowGroupData, active_rg_count);
    defer self.allocator.free(all_rg_data);

    for (surgical_result.row_groups, 0..) |*assembled_rg, i| {
        all_rg_data[i] = try assembled_rg.toRowGroupData(self.allocator);
        rg_data_arrays[i] = .{
            .output_bufs = all_rg_data[i].output_bufs,
            .output_offsets = all_rg_data[i].output_offsets,
            .output_chunks = all_rg_data[i].output_chunks,
        };
    }

    const filter_ctx = FilterContext{
        .allocator = self.allocator,
        .filter_cols_in_output = try self.allocator.dupe(bool, &.{filter_col_in_output}),
        .filter_col_output_indices = try self.allocator.dupe(?usize, &.{filter_col_output_idx}),
        .filters = try self.allocator.dupe(Filter, &.{filter}),
        .filter_col_names = try self.allocator.dupe([]const u8, &.{pred.column}),
        .filter_vals = try self.allocator.dupe([]const u8, &.{pred.value}),
        .filter_col_indices = try self.allocator.dupe(usize, &.{filter_col_idx.?}),
        .filter_col_types = try self.allocator.dupe(schema_mod.Type, &.{filter_col_type.?}),
        .output_col_indices = output_col_indices.items,
        .output_col_types = output_col_types.items,
        .output_col_names = output_col_names.items,
        .meta = &meta,
    };

    // Process each row group and submit to coordinator
    var total_input_rows: u64 = 0;
    var total_output_rows: u64 = 0;

    for (all_rg_data, 0..) |*rg_data, rg_idx| {
        total_input_rows += rg_data.num_rows;

        var worker = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
        defer worker.deinit();

        worker.execute();
        if (worker.err) |err| return err;
        if (worker.row_count == 0) continue;

        total_output_rows += worker.row_count;

        var encode_buffer = std.ArrayListUnmanaged(u8){};
        defer encode_buffer.deinit(self.allocator);

        const col_metas_slot = try worker.encodeToBuffer(&encode_buffer, self.output_compression);
        defer {
            for (col_metas_slot) |*cm| {
                cm.path_in_schema.deinit(self.allocator);
                cm.encodings.deinit(self.allocator);
            }
            self.allocator.free(col_metas_slot);
        }

        var col_metas = try self.allocator.alloc(morsel_mod.ColumnChunkMeta, col_count);
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
            .row_group_index = rg_idx,
            .num_rows = worker.row_count,
            .total_byte_size = encode_buffer.items.len,
            .columns = col_metas,
        };

        _ = try coordinator.submitMorsel(encode_buffer.items, rg_meta_morsel);
    }

    try coordinator.finishSubmissions();
    const success = try coordinator.waitForCompletion(null);
    if (!success) return error.MorselUploadFailed;

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    return ExecutionResult{
        .input_rows = total_input_rows,
        .output_rows = total_output_rows,
        .elapsed_ms = elapsed_ms,
    };
}
