const std = @import("std");
const zpq = @import("zpq");
const common = @import("common.zig");

/// Filter command options
pub const Options = struct {
    input_path: []const u8,
    output_path: ?[]const u8,
    filter: ?[]const u8,
    select_columns: ?[]const u8,
};

/// Run the filter command
pub fn run(ctx: *const common.Context, opts: Options) !void {
    var timer = try std.time.Timer.start();
    var phase_t = try std.time.Timer.start();

    // Validate required arguments
    const output_path = opts.output_path orelse {
        std.debug.print("Error: --output (-o) is required for filter command\n", .{});
        return error.MissingOutputPath;
    };
    const filter_str = opts.filter orelse {
        std.debug.print("Error: --filter is required for filter command\n", .{});
        return error.MissingFilter;
    };

    // Parse filter (col=val)
    const eq_idx = std.mem.indexOfScalar(u8, filter_str, '=') orelse {
        std.debug.print("Error: Invalid filter format. Expected col=val, got '{s}'\n", .{filter_str});
        return error.InvalidFilterFormat;
    };
    const filter_col_name = filter_str[0..eq_idx];
    const filter_val = filter_str[eq_idx + 1 ..];

    // Parse select columns (comma-separated)
    var select_col_list = std.ArrayListUnmanaged([]const u8){};
    defer select_col_list.deinit(ctx.allocator);

    if (opts.select_columns) |cols| {
        var iter = std.mem.splitScalar(u8, cols, ',');
        while (iter.next()) |col| {
            const trimmed = std.mem.trim(u8, col, " ");
            if (trimmed.len > 0) {
                try select_col_list.append(ctx.allocator, trimmed);
            }
        }
    }

    std.debug.print("ZPQ Filter: {s} -> {s}\n", .{ opts.input_path, output_path });
    std.debug.print("  Filter: {s} = '{s}'\n", .{ filter_col_name, filter_val });

    // Open input file
    phase_t.reset();
    var pf = try ctx.openFile(opts.input_path);
    defer pf.deinit();
    try pf.readFooter();
    const open_ms = @as(f64, @floatFromInt(phase_t.read())) / 1_000_000.0;
    std.debug.print("  Open+footer: {d:.1}ms\n", .{open_ms});

    const meta = pf.metadata orelse return error.NoMetadata;
    phase_t.reset();

    // Find filter column index and type
    var filter_col_idx: ?usize = null;
    var filter_col_type: ?zpq.core.schema.Type = null;
    if (meta.row_groups.items.len > 0) {
        for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const path_parts = md.path_in_schema.items;
                if (std.mem.eql(u8, path_parts[path_parts.len - 1], filter_col_name)) {
                    filter_col_idx = idx;
                    filter_col_type = md.type;
                    break;
                }
            }
        }
    }

    if (filter_col_idx == null) {
        std.debug.print("Error: Filter column '{s}' not found\n", .{filter_col_name});
        return error.ColumnNotFound;
    }

    // Build list of output columns and track if filter is in output
    var output_col_indices = std.ArrayListUnmanaged(usize){};
    defer output_col_indices.deinit(ctx.allocator);
    var output_col_names = std.ArrayListUnmanaged([]const u8){};
    defer output_col_names.deinit(ctx.allocator);
    var output_col_types = std.ArrayListUnmanaged(zpq.core.schema.Type){};
    defer output_col_types.deinit(ctx.allocator);

    var filter_col_in_output: bool = false;
    var filter_col_output_idx: ?usize = null;

    if (select_col_list.items.len > 0) {
        for (select_col_list.items, 0..) |col_name, out_idx| {
            for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
                if (col.meta_data) |md| {
                    const path_parts = md.path_in_schema.items;
                    if (std.mem.eql(u8, path_parts[path_parts.len - 1], col_name)) {
                        try output_col_indices.append(ctx.allocator, idx);
                        try output_col_names.append(ctx.allocator, col_name);
                        try output_col_types.append(ctx.allocator, md.type);
                        if (idx == filter_col_idx.?) {
                            filter_col_in_output = true;
                            filter_col_output_idx = out_idx;
                        }
                        break;
                    }
                }
            }
        }
    } else {
        // All columns
        for (meta.row_groups.items[0].columns.items, 0..) |col, idx| {
            if (col.meta_data) |md| {
                const path_parts = md.path_in_schema.items;
                try output_col_indices.append(ctx.allocator, idx);
                try output_col_names.append(ctx.allocator, path_parts[path_parts.len - 1]);
                try output_col_types.append(ctx.allocator, md.type);
                if (idx == filter_col_idx.?) {
                    filter_col_in_output = true;
                    filter_col_output_idx = output_col_indices.items.len - 1;
                }
            }
        }
    }

    const meta_analysis_ms = @as(f64, @floatFromInt(phase_t.read())) / 1_000_000.0;
    std.debug.print("  Meta analysis: {d:.1}ms\n", .{meta_analysis_ms});
    std.debug.print("  Output columns: {d} (filter in output: {})\n", .{ output_col_indices.items.len, filter_col_in_output });

    // Create EncodedFilter
    var encoded_filter = zpq.core.filter.EncodedFilter.parse(ctx.allocator, filter_val, filter_col_type.?) catch |err| {
        std.debug.print("Error: Failed to parse filter value '{s}': {}\n", .{ filter_val, err });
        return error.InvalidFilterValue;
    };
    defer encoded_filter.deinit();

    // =========================================================================
    // PHASE 1: Build row group skip mask using statistics
    // =========================================================================
    var rg_skip_mask = try ctx.allocator.alloc(bool, meta.row_groups.items.len);
    defer ctx.allocator.free(rg_skip_mask);
    @memset(rg_skip_mask, false);

    var active_rg_count: usize = 0;
    var row_groups_skipped: usize = 0;
    for (meta.row_groups.items, 0..) |_, rg_idx| {
        if (pf.shouldSkipRowGroup(rg_idx, filter_col_name, &encoded_filter)) {
            rg_skip_mask[rg_idx] = true;
            row_groups_skipped += 1;
        } else {
            active_rg_count += 1;
        }
    }

    std.debug.print("  Row groups: {d} active, {d} skipped via stats\n", .{ active_rg_count, row_groups_skipped });

    // =========================================================================
    // PHASE 2: Batch fetch ALL filter columns in one I/O call
    // =========================================================================
    var filter_buffers: ?[][]u8 = null;
    var filter_offsets: ?[]u64 = null;
    defer {
        if (filter_buffers) |bufs| {
            for (bufs) |buf| ctx.allocator.free(buf);
            ctx.allocator.free(bufs);
        }
        if (filter_offsets) |offs| ctx.allocator.free(offs);
    }

    if (active_rg_count > 0) {
        var ranges = try ctx.allocator.alloc(zpq.io.interface.Range, active_rg_count);
        defer ctx.allocator.free(ranges);
        filter_buffers = try ctx.allocator.alloc([]u8, active_rg_count);
        filter_offsets = try ctx.allocator.alloc(u64, active_rg_count);

        var buf_idx: usize = 0;
        for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
            if (rg_skip_mask[rg_idx]) continue;

            const chunk = rg_meta.columns.items[filter_col_idx.?];
            const md = chunk.meta_data orelse continue;

            var start: u64 = @intCast(md.data_page_offset);
            if (md.dictionary_page_offset) |dpo| {
                if (dpo < start) start = @intCast(dpo);
            }
            const len: u64 = @intCast(md.total_compressed_size);

            ranges[buf_idx] = .{ .start = start, .end = start + len };
            filter_buffers.?[buf_idx] = try ctx.allocator.alloc(u8, @intCast(len));
            filter_offsets.?[buf_idx] = start;
            buf_idx += 1;
        }

        var t0 = try std.time.Timer.start();
        try pf.source.readRanges(ranges, filter_buffers.?);
        const fetch_ms = @as(f64, @floatFromInt(t0.read())) / 1_000_000.0;
        std.debug.print("  Batched filter fetch: {d} ranges in {d:.1}ms\n", .{ active_rg_count, fetch_ms });
    }

    // =========================================================================
    // PHASE 3: Initialize output writer
    // =========================================================================
    phase_t.reset();
    const writer_mod = zpq.core.writer;
    var pw = try writer_mod.ParquetWriter.initWithOptions(ctx.allocator, output_path, .{
        .compression = .SNAPPY,
    });
    defer pw.deinit();

    var col_defs = std.ArrayListUnmanaged(writer_mod.ColumnDef){};
    defer col_defs.deinit(ctx.allocator);
    for (output_col_names.items, output_col_types.items) |name, col_type| {
        try col_defs.append(ctx.allocator, .{ .name = name, .type = col_type });
    }
    try pw.setColumns(col_defs.items);
    const writer_init_ms = @as(f64, @floatFromInt(phase_t.read())) / 1_000_000.0;
    std.debug.print("  Writer init: {d:.1}ms\n", .{writer_init_ms});

    // =========================================================================
    // PHASE 4: Process each row group
    // =========================================================================
    phase_t.reset();
    var total_input_rows: u64 = 0;
    var total_output_rows: u64 = 0;
    var filter_buf_idx: usize = 0;

    for (meta.row_groups.items, 0..) |rg_meta, rg_idx| {
        total_input_rows += @intCast(rg_meta.num_rows);

        if (rg_skip_mask[rg_idx]) continue;

        const num_rows: usize = @intCast(rg_meta.num_rows);

        // ----- PHASE 4a: Create MemorySource from pre-fetched buffer -----
        const prefetched_buf = filter_buffers.?[filter_buf_idx];
        const prefetched_offset = filter_offsets.?[filter_buf_idx];
        filter_buf_idx += 1;

        var mem_source = zpq.io.interface.local.MemorySource.initWithOffset(prefetched_buf, prefetched_offset);

        // Get filter column metadata
        const filter_chunk = rg_meta.columns.items[filter_col_idx.?];
        const filter_md = filter_chunk.meta_data.?;
        const filter_levels = meta.getColumnLevels(filter_md.path_in_schema.items);
        const filter_schema_elem = meta.getColumnSchema(filter_md.path_in_schema.items);
        const filter_type_len = if (filter_schema_elem) |se| se.type_length else null;

        // Create column reader from memory source
        const filter_col_reader = try zpq.column.ColumnReader.init(mem_source.source(), filter_chunk);

        // ----- PHASE 4b: Scan filter column, build selection vector -----
        var phase_timer = try std.time.Timer.start();

        var selection = zpq.core.selection.SelectionVector.init(ctx.allocator);
        defer selection.deinit();
        try selection.ensureCapacity(num_rows / 4);

        var filter_cache: ?zpq.core.filter_cache.FilterColumnCache = null;
        defer if (filter_cache) |*fc| fc.deinit();
        if (filter_col_in_output) {
            filter_cache = zpq.core.filter_cache.FilterColumnCache.init(ctx.allocator, filter_col_type.?);
            try filter_cache.?.ensureCapacity(num_rows / 4);
        }

        // Scan filter column based on type
        switch (filter_col_type.?) {
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                var reader = zpq.core.batch_reader.BatchReader([]const u8).init(
                    ctx.allocator,
                    filter_col_reader,
                    filter_md.type,
                    @intCast(filter_levels.max_def),
                    @intCast(filter_levels.max_rep),
                    filter_type_len,
                );
                defer reader.deinit();

                var row_idx: usize = 0;
                var buf: [1024]?[]const u8 = undefined;

                // Dictionary fast path: compare indices instead of strings
                var target_dict_idx: ?u64 = null;
                var use_dict_fast_path = false;
                var first_batch = true;

                while (row_idx < num_rows) {
                    const batch_size = @min(1024, num_rows - row_idx);

                    if (target_dict_idx == null and reader.hasDictionary()) {
                        target_dict_idx = reader.findInDictionary(filter_val);
                        use_dict_fast_path = target_dict_idx != null;
                        if (first_batch) {
                            std.debug.print("    Dict fast path: {} (target_idx={?})\n", .{ use_dict_fast_path, target_dict_idx });
                            first_batch = false;
                        }
                    }

                    if (use_dict_fast_path) {
                        var sel_batch = zpq.core.simd.SelectionVector.init();
                        const n_read = try reader.scanDictIndicesIntoBatch(target_dict_idx.?, &sel_batch, batch_size);
                        if (n_read == 0) break;

                        const match_count = sel_batch.count();
                        if (match_count > 0) {
                            for (0..n_read) |i| {
                                if (sel_batch.isSet(i)) {
                                    try selection.append(row_idx + i);
                                }
                            }
                        }
                        row_idx += n_read;
                    } else {
                        const n_read = try reader.nextBatch(buf[0..batch_size]);
                        if (n_read == 0) break;

                        for (buf[0..n_read], 0..) |maybe_val, i| {
                            if (maybe_val) |v| {
                                if (encoded_filter.matchesBytes(v)) {
                                    try selection.append(row_idx + i);
                                    if (filter_cache) |*fc| {
                                        try fc.appendByteArray(v);
                                    }
                                }
                            }
                        }
                        row_idx += n_read;
                    }
                }
            },
            .INT32 => try scanAndCacheFilterColumn(i32, ctx.allocator, filter_col_reader, filter_md, filter_levels, filter_type_len, &encoded_filter, num_rows, &selection, if (filter_cache) |*fc| fc else null),
            .INT64 => try scanAndCacheFilterColumn(i64, ctx.allocator, filter_col_reader, filter_md, filter_levels, filter_type_len, &encoded_filter, num_rows, &selection, if (filter_cache) |*fc| fc else null),
            .FLOAT => try scanAndCacheFilterColumn(f32, ctx.allocator, filter_col_reader, filter_md, filter_levels, filter_type_len, &encoded_filter, num_rows, &selection, if (filter_cache) |*fc| fc else null),
            .DOUBLE => try scanAndCacheFilterColumn(f64, ctx.allocator, filter_col_reader, filter_md, filter_levels, filter_type_len, &encoded_filter, num_rows, &selection, if (filter_cache) |*fc| fc else null),
            .BOOLEAN => try scanAndCacheFilterColumn(bool, ctx.allocator, filter_col_reader, filter_md, filter_levels, filter_type_len, &encoded_filter, num_rows, &selection, if (filter_cache) |*fc| fc else null),
            else => {
                std.debug.print("Error: Unsupported filter column type {}\n", .{filter_col_type.?});
                return error.UnsupportedFilterType;
            },
        }

        const scan_time_ms = @as(f64, @floatFromInt(phase_timer.read())) / 1_000_000.0;

        if (selection.count() == 0) continue;

        total_output_rows += selection.count();

        // ----- PHASE 4c: Batch fetch other output columns -----
        phase_timer.reset();
        var other_col_count: usize = 0;
        for (output_col_indices.items) |idx| {
            if (idx != filter_col_idx.?) other_col_count += 1;
        }

        var other_buffers: ?[][]u8 = null;
        var other_offsets: ?[]u64 = null;
        defer {
            if (other_buffers) |bufs| {
                for (bufs) |buf| ctx.allocator.free(buf);
                ctx.allocator.free(bufs);
            }
            if (other_offsets) |offs| ctx.allocator.free(offs);
        }

        if (other_col_count > 0) {
            var other_ranges = try ctx.allocator.alloc(zpq.io.interface.Range, other_col_count);
            defer ctx.allocator.free(other_ranges);
            other_buffers = try ctx.allocator.alloc([]u8, other_col_count);
            other_offsets = try ctx.allocator.alloc(u64, other_col_count);

            var other_idx: usize = 0;
            for (output_col_indices.items) |col_idx| {
                if (col_idx == filter_col_idx.?) continue;

                const chunk = rg_meta.columns.items[col_idx];
                const md = chunk.meta_data.?;

                var start: u64 = @intCast(md.data_page_offset);
                if (md.dictionary_page_offset) |dpo| {
                    if (dpo < start) start = @intCast(dpo);
                }
                const len: u64 = @intCast(md.total_compressed_size);

                other_ranges[other_idx] = .{ .start = start, .end = start + len };
                other_buffers.?[other_idx] = try ctx.allocator.alloc(u8, @intCast(len));
                other_offsets.?[other_idx] = start;
                other_idx += 1;
            }

            try pf.source.readRanges(other_ranges, other_buffers.?);
        }
        const fetch_time_ms = @as(f64, @floatFromInt(phase_timer.read())) / 1_000_000.0;

        // ----- PHASE 4d: Materialize output columns -----
        phase_timer.reset();
        var out_rg = try pw.beginRowGroup();

        var other_buf_idx: usize = 0;
        const filter_used_dict_path = (filter_col_type.? == .BYTE_ARRAY or filter_col_type.? == .FIXED_LEN_BYTE_ARRAY) and
            (filter_cache == null or filter_cache.?.count() == 0);

        for (output_col_indices.items, output_col_types.items, 0..) |col_idx, col_type, out_col_idx| {
            if (col_idx == filter_col_idx.? and filter_col_in_output) {
                if (filter_used_dict_path) {
                    const values = try ctx.allocator.alloc([]const u8, selection.count());
                    defer ctx.allocator.free(values);
                    for (values) |*v| {
                        v.* = filter_val;
                    }
                    try out_rg.writeByteArrayColumn(values);
                } else if (filter_cache != null) {
                    switch (col_type) {
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => try out_rg.writeByteArrayColumn(filter_cache.?.getByteArrayValues()),
                        .INT32 => try out_rg.writeInt32Column(filter_cache.?.getInt32Values()),
                        .INT64 => try out_rg.writeInt64Column(filter_cache.?.getInt64Values()),
                        .FLOAT => try out_rg.writeFloatColumn(filter_cache.?.getFloatValues()),
                        .DOUBLE => try out_rg.writeDoubleColumn(filter_cache.?.getDoubleValues()),
                        .BOOLEAN => try out_rg.writeBooleanColumn(filter_cache.?.getBoolValues()),
                        else => {},
                    }
                }
            } else {
                const col_buf = other_buffers.?[other_buf_idx];
                const col_offset = other_offsets.?[other_buf_idx];
                other_buf_idx += 1;

                var col_mem_source = zpq.io.interface.local.MemorySource.initWithOffset(col_buf, col_offset);
                const chunk = rg_meta.columns.items[col_idx];
                const col_reader = try zpq.column.ColumnReader.init(col_mem_source.source(), chunk);

                const md = chunk.meta_data.?;
                const levels = meta.getColumnLevels(md.path_in_schema.items);
                const schema_elem = meta.getColumnSchema(md.path_in_schema.items);
                const type_len = if (schema_elem) |se| se.type_length else null;

                switch (col_type) {
                    .INT32 => {
                        var values = std.ArrayListUnmanaged(i32){};
                        defer values.deinit(ctx.allocator);
                        try readSelectedRowsTyped(i32, ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeInt32Column(values.items);
                    },
                    .INT64 => {
                        var values = std.ArrayListUnmanaged(i64){};
                        defer values.deinit(ctx.allocator);
                        try readSelectedRowsTyped(i64, ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeInt64Column(values.items);
                    },
                    .FLOAT => {
                        var values = std.ArrayListUnmanaged(f32){};
                        defer values.deinit(ctx.allocator);
                        try readSelectedRowsTyped(f32, ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeFloatColumn(values.items);
                    },
                    .DOUBLE => {
                        var values = std.ArrayListUnmanaged(f64){};
                        defer values.deinit(ctx.allocator);
                        try readSelectedRowsTyped(f64, ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeDoubleColumn(values.items);
                    },
                    .BOOLEAN => {
                        var values = std.ArrayListUnmanaged(bool){};
                        defer values.deinit(ctx.allocator);
                        try readSelectedRowsTyped(bool, ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeBooleanColumn(values.items);
                    },
                    .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
                        var values = std.ArrayListUnmanaged([]const u8){};
                        defer {
                            for (values.items) |v| ctx.allocator.free(v);
                            values.deinit(ctx.allocator);
                        }
                        try readSelectedByteArrayRows(ctx.allocator, col_reader, md.type, @intCast(levels.max_def), @intCast(levels.max_rep), type_len, &selection, &values);
                        try out_rg.writeByteArrayColumn(values.items);
                    },
                    else => {
                        std.debug.print("Warning: Unsupported column type {}, skipping\n", .{col_type});
                    },
                }
            }
            _ = out_col_idx;
        }

        try pw.finishRowGroup(out_rg, @intCast(selection.count()));
        const write_time_ms = @as(f64, @floatFromInt(phase_timer.read())) / 1_000_000.0;

        std.debug.print("  RG[{d}]: {d}/{d} rows matched (scan={d:.1}ms, fetch={d:.1}ms, write={d:.1}ms)\n", .{ rg_idx, selection.count(), rg_meta.num_rows, scan_time_ms, fetch_time_ms, write_time_ms });
    }

    const loop_total_ms = @as(f64, @floatFromInt(phase_t.read())) / 1_000_000.0;
    std.debug.print("  RG loop total: {d:.1}ms\n", .{loop_total_ms});

    phase_t.reset();
    try pw.finish();
    const finish_ms = @as(f64, @floatFromInt(phase_t.read())) / 1_000_000.0;
    std.debug.print("  Writer finish: {d:.1}ms\n", .{finish_ms});

    const elapsed_ns = timer.read();
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / 1_000_000.0;

    std.debug.print("\nFilter complete:\n", .{});
    std.debug.print("  Input rows:  {d}\n", .{total_input_rows});
    std.debug.print("  Output rows: {d}\n", .{total_output_rows});
    std.debug.print("  Row groups skipped: {d}\n", .{row_groups_skipped});
    std.debug.print("  Time: {d:.1}ms\n", .{elapsed_ms});
    std.debug.print("  Output: {s}\n", .{output_path});
}

/// Scan filter column and cache values if needed (for numeric types)
fn scanAndCacheFilterColumn(
    comptime T: type,
    allocator: std.mem.Allocator,
    col_reader: zpq.column.ColumnReader,
    md: zpq.core.schema.ColumnMetaData,
    levels: zpq.core.schema.Levels,
    type_len: ?i32,
    encoded_filter: *const zpq.core.filter.EncodedFilter,
    num_rows: usize,
    selection: *zpq.core.selection.SelectionVector,
    filter_cache: ?*zpq.core.filter_cache.FilterColumnCache,
) !void {
    var reader = zpq.core.batch_reader.BatchReader(T).init(
        allocator,
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
                if (encoded_filter.matchesBytes(value_bytes)) {
                    try selection.append(row_idx + i);
                    if (filter_cache) |fc| {
                        if (T == i32) try fc.appendInt32(v) else if (T == i64) try fc.appendInt64(v) else if (T == f32) try fc.appendFloat(v) else if (T == f64) try fc.appendDouble(v) else if (T == bool) try fc.appendBool(v);
                    }
                }
            }
        }
        row_idx += n_read;
    }
}

/// Read selected rows using skip() for efficiency
fn readSelectedRowsTyped(
    comptime T: type,
    allocator: std.mem.Allocator,
    col_reader: zpq.column.ColumnReader,
    col_type: zpq.core.schema.Type,
    max_def: u16,
    max_rep: u16,
    type_len: ?i32,
    selection: *const zpq.core.selection.SelectionVector,
    out_values: *std.ArrayListUnmanaged(T),
) !void {
    var reader = zpq.core.batch_reader.BatchReader(T).init(allocator, col_reader, col_type, max_def, max_rep, type_len);
    defer reader.deinit();

    const indices = selection.items();
    if (indices.len == 0) return;

    try out_values.ensureTotalCapacity(allocator, indices.len);

    var current_row: usize = 0;
    var idx: usize = 0;
    var batch_buf: [1024]?T = undefined;

    while (idx < indices.len) {
        const target_row = indices[idx];

        if (target_row > current_row) {
            try reader.skip(target_row - current_row);
            current_row = target_row;
        }

        var run_len: usize = 1;
        while (idx + run_len < indices.len and indices[idx + run_len] == target_row + run_len) {
            run_len += 1;
            if (run_len >= batch_buf.len) break;
        }

        const to_read = @min(run_len, batch_buf.len);
        const n = try reader.nextBatch(batch_buf[0..to_read]);

        for (batch_buf[0..n]) |maybe_val| {
            try out_values.append(allocator, maybe_val orelse std.mem.zeroes(T));
        }

        current_row += n;
        idx += n;
    }
}

/// Read selected byte array rows using skip()
fn readSelectedByteArrayRows(
    allocator: std.mem.Allocator,
    col_reader: zpq.column.ColumnReader,
    col_type: zpq.core.schema.Type,
    max_def: u16,
    max_rep: u16,
    type_len: ?i32,
    selection: *const zpq.core.selection.SelectionVector,
    out_values: *std.ArrayListUnmanaged([]const u8),
) !void {
    var reader = zpq.core.batch_reader.BatchReader([]const u8).init(allocator, col_reader, col_type, max_def, max_rep, type_len);
    defer reader.deinit();

    const indices = selection.items();
    if (indices.len == 0) return;

    try out_values.ensureTotalCapacity(allocator, indices.len);

    var current_row: usize = 0;
    var idx: usize = 0;
    var batch_buf: [1024]?[]const u8 = undefined;

    while (idx < indices.len) {
        const target_row = indices[idx];

        if (target_row > current_row) {
            try reader.skip(target_row - current_row);
            current_row = target_row;
        }

        var run_len: usize = 1;
        while (idx + run_len < indices.len and indices[idx + run_len] == target_row + run_len) {
            run_len += 1;
            if (run_len >= batch_buf.len) break;
        }

        const to_read = @min(run_len, batch_buf.len);
        const n = try reader.nextBatch(batch_buf[0..to_read]);

        for (batch_buf[0..n]) |maybe_val| {
            if (maybe_val) |v| {
                try out_values.append(allocator, try allocator.dupe(u8, v));
            } else {
                try out_values.append(allocator, try allocator.dupe(u8, ""));
            }
        }

        current_row += n;
        idx += n;
    }
}
