const std = @import("std");
const xev = @import("xev");
const pipeline_mod = @import("../pipeline.zig");
const pipeline_types = @import("types.zig");
const schema_mod = @import("../schema.zig");
const filters_mod = @import("../filters/mod.zig");
const row_group_worker_mod = @import("../row_group_worker.zig");
const interface = @import("../../io/interface.zig");
const slot_writer_mod = @import("../slot_writer.zig");

const Pipeline = pipeline_mod.Pipeline;
const ExecutionResult = pipeline_types.ExecutionResult;
const Filter = filters_mod.Filter;
const RowGroupWorker = row_group_worker_mod.RowGroupWorker;
const RowGroupData = row_group_worker_mod.RowGroupData;
const FilterContext = row_group_worker_mod.FilterContext;
const SlotWriteCompletion = row_group_worker_mod.SlotWriteCompletion;
const SlotWriter = slot_writer_mod.SlotWriter;
const Range = interface.Range;
const SchemaElement = schema_mod.SchemaElement;
const Type = schema_mod.Type;
const ColumnChunk = schema_mod.ColumnChunk;
const Trace = pipeline_types.Trace;

pub fn execute(self: *Pipeline) !ExecutionResult {
    const loop = self.loop orelse return error.NoRuntime;
    const pool = self.thread_pool orelse return error.NoRuntime;

    var trace = Trace.init();
    var timer = try std.time.Timer.start();

    // Validate requirements
    const output_path = self.output_path orelse return error.NoOutputPath;
    const pf = self.input_file.?;
    const meta = pf.metadata orelse return error.NoMetadata;

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
        // Select specified columns
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
        // Select all columns
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
    const col_count = output_col_indices.items.len;

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

    if (trace.enabled) {
        std.debug.print("[TRACE] row_group_pruning: {d}/{d} row groups after stats filter\n", .{ rg_count, total_rg_count });
    }

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
    for (all_extra_filter_bufs) |*bufs| bufs.* = &.{};

    var all_extra_filter_offsets = try self.allocator.alloc([]u64, rg_count);
    defer {
        for (all_extra_filter_offsets) |offs| self.allocator.free(offs);
        self.allocator.free(all_extra_filter_offsets);
    }
    for (all_extra_filter_offsets) |*offs| offs.* = &.{};

    var all_extra_filter_chunks = try self.allocator.alloc([]ColumnChunk, rg_count);
    defer {
        for (all_extra_filter_chunks) |chunks| self.allocator.free(chunks);
        self.allocator.free(all_extra_filter_chunks);
    }
    for (all_extra_filter_chunks) |*chunks| chunks.* = &.{};

    var max_input_rg_size: u64 = 0;
    var total_ranges: usize = 0;
    for (active_rg_indices.items) |orig_rg_idx| {
        const rg_meta = meta.row_groups.items[orig_rg_idx];
        total_ranges += col_count;
        total_ranges += num_extra_filters;
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

        if (num_extra_filters > 0) {
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
    }

    trace.mark("setup_ranges");
    try pf.source.readRanges(ranges, buffers);
    trace.mark("read_data");

    for (active_rg_indices.items, 0..) |orig_rg_idx, local_rg_idx| {
        const rg_meta = meta.row_groups.items[orig_rg_idx];

        var rg_filter_bufs = try self.allocator.alloc([]const u8, self.predicates.len);
        var rg_filter_offsets = try self.allocator.alloc(u64, self.predicates.len);
        var rg_filter_chunks = try self.allocator.alloc(schema_mod.ColumnChunk, self.predicates.len);

        for (self.predicates, 0..) |_, p_idx| {
            const f_idx = filter_col_indices[p_idx];
            if (filter_cols_in_output[p_idx]) {
                const out_idx = filter_col_output_indices[p_idx].?;
                rg_filter_bufs[p_idx] = all_output_bufs[local_rg_idx][out_idx];
                rg_filter_offsets[p_idx] = all_output_offsets[local_rg_idx][out_idx];
                rg_filter_chunks[p_idx] = all_output_chunks[local_rg_idx][out_idx];
            } else {
                var extra_idx: ?usize = null;
                for (extra_filter_col_indices.items, 0..) |e_idx, i| {
                    if (e_idx == f_idx) {
                        extra_idx = i;
                        break;
                    }
                }
                rg_filter_bufs[p_idx] = all_extra_filter_bufs[local_rg_idx][extra_idx.?];
                rg_filter_offsets[p_idx] = all_extra_filter_offsets[local_rg_idx][extra_idx.?];
                rg_filter_chunks[p_idx] = all_extra_filter_chunks[local_rg_idx][extra_idx.?];
            }
        }

        all_rg_data[local_rg_idx] = RowGroupData{
            .rg_idx = orig_rg_idx,
            .num_rows = @intCast(rg_meta.num_rows),
            .filter_bufs = rg_filter_bufs,
            .filter_offsets = rg_filter_offsets,
            .filter_chunks = rg_filter_chunks,
            .output_bufs = @ptrCast(all_output_bufs[local_rg_idx]),
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
        .filter_col_names = filter_col_names,
        .filter_vals = filter_vals,
        .filter_col_indices = try self.allocator.dupe(usize, filter_col_indices),
        .filter_col_types = try self.allocator.dupe(schema_mod.Type, filter_col_types),
        .filters = try self.allocator.dupe(Filter, filters),
        .output_col_indices = output_col_indices.items,
        .output_col_types = output_col_types.items,
        .output_col_names = output_col_names.items,
        .filter_cols_in_output = try self.allocator.dupe(bool, filter_cols_in_output),
        .filter_col_output_indices = try self.allocator.dupe(?usize, filter_col_output_indices),
        .meta = &meta,
    };

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

    var sw = try SlotWriter.init(
        self.allocator,
        output_path,
        rg_count,
        max_input_rg_size,
        output_schema.items,
    );
    defer sw.deinit();

    var total_input_rows: u64 = 0;
    var workers = try self.allocator.alloc(*RowGroupWorker, rg_count);
    defer self.allocator.free(workers);

    var completions = try self.allocator.alloc(SlotWriteCompletion, rg_count);
    defer {
        for (completions) |*c| c.deinit(self.allocator);
        self.allocator.free(completions);
    }

    var pending = std.atomic.Value(usize).init(rg_count);

    trace.mark("setup_workers");

    for (all_rg_data, 0..) |*rg_data, i| {
        total_input_rows += rg_data.num_rows;
        workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
        completions[i] = try SlotWriteCompletion.init(
            workers[i],
            &sw,
            i,
            self.output_compression,
            &pending,
        );
    }

    for (completions) |*c| {
        c.scheduleOn(loop, pool);
    }

    trace.mark("scheduled");

    while (pending.load(.acquire) > 0) {
        loop.run(.once) catch |err| {
            std.debug.print("Loop error: {}\n", .{err});
            break;
        };
    }

    trace.mark("workers_done");

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

    trace.mark("collected");
    try sw.finish(rg_metas);
    trace.mark("finished");

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    return ExecutionResult{
        .input_rows = total_input_rows,
        .output_rows = total_output_rows,
        .elapsed_ms = elapsed_ms,
    };
}

pub fn executeWithLoop(self: *Pipeline, comptime XevApi: type, loop: *XevApi.Loop, thread_pool: *xev.ThreadPool) !ExecutionResult {
    const Completion = row_group_worker_mod.SlotWriteCompletionGen(XevApi);

    var trace = Trace.init();
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

    // Create filter
    var filter = if (pred.op == .between)
        try Filter.fromBetween(self.allocator, pred.value, pred.value2.?, filter_col_type.?)
    else
        try Filter.fromPredicate(self.allocator, pred.op.toFilterOp(), pred.value, filter_col_type.?);
    defer filter.deinit();

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
        all_filter_chunks = try self.allocator.alloc(schema_mod.ColumnChunk, rg_count);
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
        all_output_chunks[rg_idx] = try self.allocator.alloc(schema_mod.ColumnChunk, col_count);

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

        var filter_bufs_new = try self.allocator.alloc([]const u8, 1);
        filter_bufs_new[0] = filter_buf;

        var filter_offsets_new = try self.allocator.alloc(u64, 1);
        filter_offsets_new[0] = filter_offset;

        var filter_chunks_new = try self.allocator.alloc(schema_mod.ColumnChunk, 1);
        filter_chunks_new[0] = filter_chunk;

        all_rg_data[rg_idx] = RowGroupData{
            .rg_idx = rg_idx,
            .num_rows = @intCast(rg_meta.num_rows),
            .filter_bufs = filter_bufs_new,
            .filter_offsets = filter_offsets_new,
            .filter_chunks = filter_chunks_new,
            .output_bufs = const_bufs,
            .output_offsets = all_output_offsets[rg_idx],
            .output_chunks = all_output_chunks[rg_idx],
        };
    }

    var filter_cols_in_output_new = try self.allocator.alloc(bool, 1);
    filter_cols_in_output_new[0] = filter_col_in_output;

    var filter_col_output_indices_new = try self.allocator.alloc(?usize, 1);
    filter_col_output_indices_new[0] = filter_col_output_idx;

    var filters_new = try self.allocator.alloc(Filter, 1);
    filters_new[0] = filter;

    var filter_col_names_new = try self.allocator.alloc([]const u8, 1);
    filter_col_names_new[0] = pred.column;

    var filter_vals_new = try self.allocator.alloc([]const u8, 1);
    filter_vals_new[0] = pred.value;

    var filter_col_indices_new = try self.allocator.alloc(usize, 1);
    filter_col_indices_new[0] = filter_col_idx.?;

    var filter_col_types_new = try self.allocator.alloc(schema_mod.Type, 1);
    filter_col_types_new[0] = filter_col_type.?;

    const filter_ctx = FilterContext{
        .allocator = self.allocator,
        .filter_cols_in_output = filter_cols_in_output_new,
        .filter_col_output_indices = filter_col_output_indices_new,
        .filters = filters_new,
        .filter_col_names = filter_col_names_new,
        .filter_vals = filter_vals_new,
        .filter_col_indices = filter_col_indices_new,
        .filter_col_types = filter_col_types_new,
        .output_col_indices = output_col_indices.items,
        .output_col_types = output_col_types.items,
        .output_col_names = output_col_names.items,
        .meta = &meta,
    };

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

    var sw = try SlotWriter.init(self.allocator, output_path, rg_count, max_input_rg_size, output_schema.items);
    defer sw.deinit();

    var total_input_rows: u64 = 0;
    var workers = try self.allocator.alloc(*RowGroupWorker, rg_count);
    defer self.allocator.free(workers);

    var completions = try self.allocator.alloc(Completion, rg_count);
    defer {
        for (completions) |*c| c.deinit(self.allocator);
        self.allocator.free(completions);
    }

    var pending = std.atomic.Value(usize).init(rg_count);

    trace.mark("setup_workers");

    for (all_rg_data, 0..) |*rg_data, i| {
        total_input_rows += rg_data.num_rows;
        workers[i] = try RowGroupWorker.init(self.allocator, &filter_ctx, rg_data);
        completions[i] = try Completion.init(workers[i], &sw, i, self.output_compression, &pending);
    }

    for (completions) |*c| {
        c.scheduleOn(loop, thread_pool);
    }

    trace.mark("scheduled");

    while (pending.load(.acquire) > 0) {
        loop.run(.once) catch |err| {
            std.debug.print("Loop error: {}\n", .{err});
            break;
        };
    }

    trace.mark("workers_done");

    var total_output_rows: u64 = 0;
    var rg_metas = try self.allocator.alloc(slot_writer_mod.RowGroupMeta, rg_count);
    defer self.allocator.free(rg_metas);

    for (completions, workers, 0..) |*c, worker, i| {
        defer worker.deinit();
        if (c.task_error) |err| return err;
        total_output_rows += worker.row_count;
        rg_metas[i] = c.getRowGroupMeta();
    }

    trace.mark("collected");
    try sw.finish(rg_metas);
    trace.mark("finished");

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    return ExecutionResult{
        .input_rows = total_input_rows,
        .output_rows = total_output_rows,
        .elapsed_ms = elapsed_ms,
    };
}
