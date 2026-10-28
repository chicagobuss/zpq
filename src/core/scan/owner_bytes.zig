const std = @import("std");
const testing = std.testing;
const thrift_mod = @import("../thrift.zig");
const nowMonoNs = @import("../../clock.zig").monoNs;
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const ndv = @import("../ndv.zig");
const owner_groups = @import("../owner_groups.zig");
const msd_groups = @import("../msd_groups.zig");
const column_mod = @import("../parquet/column.zig");
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_result = @import("result.zig");
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const scan_binding = @import("binding.zig");
const leafSchemaElem = scan_binding.leafSchemaElem;
const record_workers = @import("record_workers.zig");
const runRecordWorkers = record_workers.runRecordWorkers;
const owner_support = @import("owner_support.zig");
const outputNamesBytes = owner_support.outputNamesBytes;
const scan_owner_distinct = @import("owner_distinct.zig");
const shortSampleMayFit = scan_owner_distinct.shortSampleMayFit;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const byte_stream = @import("byte_stream.zig");
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;

/// A missing footer bound warrants a bounded look at the first page. It can only decline: later keys still face
/// the exact width guard. A corrupt sample also declines, so the ordinary scan preserves decode error precedence.
pub fn byteSampleMayFit(
    gpa: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    key_col: usize,
) !bool {
    return shortSampleMayFit(gpa, args, metas, work, key_col, 16);
}

pub fn msdProbeHeaders(args: MultiAggArgs, metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr, explain: bool) !?u64 {
    for (args.inputs) |input| if (input.gate != null and
        (!input.readerBacked() or input.dict_wait == null or args.dict_probe == null or args.start_reads == null)) return null;
    var bytes: u64 = 0;
    for (work) |item| for (keys) |key| {
        const cm = metas[item.file].row_groups.items[item.rg].columns.items[key.col_ref.col_idx].meta_data orelse return null;
        const off = cm.dictionary_page_offset orelse cm.data_page_offset;
        if (off < 0 or cm.total_compressed_size < 0) return null;
        if (args.inputs[item.file].readerBacked()) bytes +|= @as(u64, @min(@as(u64, @intCast(cm.total_compressed_size)), 4096)) + 8192;
    };
    if (bytes > args.probe_cap orelse ndv.probeCap(args.max_memory)) return null;
    if (!explain) if (args.dict_probe) |probe| for (work) |item| for (keys) |key| {
        const cm = metas[item.file].row_groups.items[item.rg].columns.items[key.col_ref.col_idx].meta_data.?;
        const start: u64 = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const len: u64 = @min(@as(u64, @intCast(cm.total_compressed_size)), 4096);
        try probe.add(probe.ctx, item.file, item.rg, start, start + len);
    };
    return bytes;
}

pub fn msdWorkspace(args: MultiAggArgs, metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr, workers: usize, explain: bool) ?usize {
    var largest: usize = 0;
    for (work) |item| {
        const input = args.inputs[item.file];
        if (!explain) if (input.dict_wait) |wait| wait.wait(wait.ctx, item.rg) catch return null;
        const src = if (explain) input.plan_bytes orelse input.bytes else input.bytes;
        var entries: [2]usize = @splat(0);
        for (keys, 0..) |key, i| {
            const cm = metas[item.file].row_groups.items[item.rg].columns.items[key.col_ref.col_idx].meta_data.?;
            const start = std.math.cast(usize, cm.dictionary_page_offset orelse cm.data_page_offset) orelse return null;
            const len = std.math.cast(usize, cm.total_compressed_size) orelse return null;
            if (start > src.len or len > src.len - start) return null;
            var reader = thrift_mod.Reader.init(src[start..][0..@min(len, 4096)]);
            const header = schema.PageHeader.read(&reader) catch return null;
            switch (header.type) {
                .DICTIONARY_PAGE => {
                    const h = header.dictionary_page_header orelse return null;
                    if (h.num_values < 0) return null;
                    entries[i] = @intCast(h.num_values);
                },
                .DATA_PAGE, .DATA_PAGE_V2 => {},
                else => return null,
            }
        }
        const rg = &metas[item.file].row_groups.items[item.rg];
        const f = rg.columns.items[keys[1].col_ref.col_idx].meta_data.?;
        const rows = std.math.cast(usize, rg.num_rows) orelse return null;
        // Whole-row views, definition/code arrays and their arena growth, dictionary slices, and numeric scratch.
        // Retained key payloads are separate. The same fixed credit covers every worker's high-water mark.
        const need = rows *| 128 +| entries[0] *| 64 +| entries[1] *| 32 +|
            column_mod.inflateLimit(f) *| 4 +| (4 << 20);
        largest = @max(largest, need);
    }
    return largest *| workers;
}

pub fn inlineMsdAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    n_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const order = args.result_order;
    if (!args.msd_records or !allRescannable(args.inputs) or
        order.having != null or order.hidden.len != 0 or order.keys.len != 1 or calls.len != 1) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    for (outputs) |out| if (out.derived != null) return null;
    if (keys.len != 2 or keys[0] != .col_ref or keys[1] != .col_ref or outputs.len != 3) return null;
    const key = keys[0].col_ref;
    const f = keys[1].col_ref;
    if (key.physical_type != .BYTE_ARRAY or key.expr_type != .str or f.physical_type != .INT32 or
        f.expr_type != .i64 or f.unsigned_32) return null;
    const call = calls[0];
    if (call.func != .count or call.arg != null or call.where != null or call.offset != 0 or call.state or call.result != .i64)
        return null;
    const limit = order.limit orelse return null;
    const k = std.math.cast(usize, order.offset +| limit) orelse return null;
    if (limit == 0 or k == 0 or k > 1024 or !order.keys[0].descending) return null;
    const ordered = outputs[order.keys[0].column].source;
    if (ordered != .agg or ordered.agg != 0) return null;
    var seen: u8 = 0;
    for (outputs) |out| seen |= @as(u8, 1) << @intCast(switch (out.source) {
        .key => |i| i,
        .agg => @as(usize, 2),
    });
    if (seen != 7) return null;
    var rows: usize = 0;
    var raw: usize = 0;
    var largest_capacity: usize = 0;
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        if (rg.num_rows < 0) return null;
        rows +|= @intCast(rg.num_rows);
        const cm = rg.columns.items[key.col_idx].meta_data orelse return null;
        const f_cm = rg.columns.items[f.col_idx].meta_data orelse return null;
        if (cm.type != .BYTE_ARRAY or f_cm.type != .INT32) return null;
        if (cm.total_uncompressed_size < 0 or cm.total_compressed_size < 0) return null;
        // Expansion encodings have a different physical ownership bound. Decline them before consumption.
        for (cm.encodings.items) |enc| switch (enc) {
            .PLAIN, .RLE, .BIT_PACKED, .RLE_DICTIONARY, .PLAIN_DICTIONARY => {},
            else => return null,
        };
        const capacity = column_mod.inflateLimit(cm) +| @as(usize, @intCast(cm.total_compressed_size));
        if (capacity >= @as(u64, 1) << 32) return null;
        raw +|= capacity;
        largest_capacity = @max(largest_capacity, capacity);
    }
    if (rows < args.msd_records_min_rows or rows >= 1 << 30) return null;
    // Each surviving key is contained in one retained chunk. Repeated dictionary keys can win for different f,
    // so reserve H copies of the largest chunk bound, not merely the sum of unique retained bytes. Decoder
    // workspace is dead before those copies are made and can fund the same credit without another parent draw.
    const names_credit = outputNamesBytes(outputs);
    const output_credit = names_credit +| k *| largest_capacity +|
        k *| (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue));
    const cache_count = msd_groups.cacheRows(args.msd_cache_rows);
    const quote = msd_groups.State.reservation(rows, n_workers, k, raw, work.len, cache_count) +|
        msd_groups.State.phaseFunding(args.msd_working_bytes orelse 0, output_credit);
    if (quote > args.max_memory) return null;
    if (explain_only) return .{ .layout = .msd_count_top_h, .reservation = quote };
    const working = args.msd_working_bytes orelse return null;
    const capacities = arena.alloc(usize, work.len) catch return null;
    const starts = arena.alloc(u32, work.len) catch return null;
    var at: usize = 0;
    for (work, capacities, starts) |item, *capacity, *start| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        const cm = rg.columns.items[key.col_idx].meta_data.?;
        capacity.* = column_mod.inflateLimit(cm) +| @as(usize, @intCast(cm.total_compressed_size));
        start.* = @intCast(at);
        at += @intCast(rg.num_rows);
    }
    const state = (try msd_groups.State.initPrepaid(gpa, args.ledger, rows, n_workers, key.col_idx, f.col_idx, k, capacities, working, output_credit, cache_count)) orelse return null;
    defer {
        const release = profile.Span.begin(profile.threadOf(args.profiler, .main));
        state.deinit();
        release.end(.msd_release, profile.args(.{}));
    }
    const sinks = arena.alloc(consumer.RecordSink, n_workers) catch return null;
    for (sinks, state.locals) |*sink, *local| sink.* = .{ .msd = local };
    if (args.taken) |taken| taken.insert(.msd_records);
    var decoded_args = args;
    decoded_args.ledger = &state.decode_budget;
    decoded_args.decode_limit = working;
    decoded_args.taken = null;
    const run = try runRecordWorkers(
        gpa,
        arena,
        decoded_args,
        metas,
        plan,
        work,
        calls,
        keys,
        sinks,
        starts,
        outputs,
        timings,
    );
    state.releaseDecode();
    const final_start = nowMonoNs();
    const kept = try state.finishSelected(@intCast(run.rows_matched));
    const prof = profile.threadOf(args.profiler, .main);
    profile.record(prof, .msd_fold, final_start, nowMonoNs(), profile.args(.{ state.prefix_len, state.output_credit, cache_count }));
    const materialize = nowMonoNs();
    var answer_bytes = names_credit +| kept.len * (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue));
    for (kept) |group| if (group.record.key()) |bytes| {
        answer_bytes +|= bytes.len;
    };
    // The admission bound includes every surviving key and row. A defensive refusal retries the established
    // executor; it never requests additional parent credit after consumption.
    if (answer_bytes > state.output_credit) return error.ExceededMemoryBudget;
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, types, names) |out, *ty, *name| {
        name.* = try gpa.dupe(u8, out.name);
        ty.* = switch (out.source) {
            .key => |i| .{ .lane = if (i == 0) .string else .i64, .nullable = true, .source = leafSchemaElem(&metas[0], keys[i].col_ref.col_idx) },
            .agg => .{ .lane = .i64, .nullable = false },
        };
    }
    const owned = names_credit +| try buildMsdTopRows(gpa, &output, kept, outputs);
    if (args.ledger != null) state.charged -= owned;
    profile.record(prof, .msd_output, materialize, nowMonoNs(), profile.args(.{}));
    var bytes_in: u64 = 0;
    for (args.inputs) |input| bytes_in += if (input.logical_size != 0) input.logical_size else input.bytes.len;
    return .{
        .files_in = args.inputs.len,
        .rows_in = run.rows_in,
        .rows_scanned = run.rows_scanned,
        .rows_matched = run.rows_matched,
        .bytes_in = bytes_in,
        .row_groups_in = run.row_groups_in,
        .row_groups_pruned = run.row_groups_pruned,
        .row_groups_full_match = run.row_groups_full_match,
        .cols_stat_pruned = plan.cols_stat_pruned,
        .timings = run.timings,
        .output = output,
        .part_agg = true,
        .state = .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types },
    };
}

/// Copy the surviving keys before releasing their retained decoder pages. Partial results own every allocation.
pub fn buildMsdTopRows(gpa: std.mem.Allocator, output: *AggOutput, groups: []const msd_groups.Group, outputs: []const BoundOutputColumn) !usize {
    const rows_out = try gpa.alloc([]const AggValue, groups.len);
    @memset(rows_out, &.{});
    output.group_rows = rows_out;
    const slab = try gpa.alloc(AggValue, groups.len * outputs.len);
    @memset(slab, .null_val);
    output.slab = slab;
    var owned: usize = groups.len * (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue));
    for (groups, rows_out, 0..) |g, *row, i| {
        const cells = slab[i * outputs.len ..][0..outputs.len];
        row.* = cells;
        for (outputs, cells) |out, *cell| cell.* = switch (out.source) {
            .key => |idx| if (idx == 0) if (g.record.key()) |bytes| str: {
                const copy = try gpa.dupe(u8, bytes);
                owned += bytes.len;
                break :str .{ .s = copy };
            } else .null_val else if (g.record.fValue()) |v| .{ .i = v } else .null_val,
            .agg => .{ .i = g.count },
        };
    }
    return owned;
}

pub fn inlineByteTopAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    n_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const order = args.result_order;
    const limit = order.limit orelse return null;
    if (limit == 0) return null;
    if (!args.byte_records or !allRescannable(args.inputs) or
        order.keys.len != 1 or order.having != null or order.hidden.len != 0 or calls.len != 1 or
        n_workers > 64) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    for (outputs) |out| if (out.derived != null) return null;
    if (keys.len != 1 or keys[0] != .col_ref or outputs.len != 2) return null;
    const key = keys[0].col_ref;
    if (key.physical_type != .BYTE_ARRAY or key.expr_type != .str) return null;
    const call = calls[0];
    if (args.filter != null and call.func != .count) return null;
    if (call.where != null or call.offset != 0 or call.state or call.result != .i64) return null;
    // COUNT(*) has no value lane; SUM needs one signed INT64 column.
    var value_col: usize = 0;
    switch (call.func) {
        .count => if (call.arg != null) return null,
        .sum => {
            const value = call.arg orelse return null;
            if (value != .col_ref or value.col_ref.physical_type != .INT64 or value.col_ref.expr_type != .i64 or
                value.col_ref.unsigned_64) return null;
            value_col = value.col_ref.col_idx;
        },
        else => return null,
    }
    const rank = order.keys[0];
    if (rank.column >= outputs.len or outputs[rank.column].source != .agg) return null;
    const h = limit +| order.offset;
    if (h == 0 or h > 1024) return null;
    var seen: u8 = 0;
    var names_bytes: usize = outputs.len * @sizeOf([]const u8);
    for (outputs) |out| {
        seen |= if (out.source == .key) @as(u8, 1) else 2;
        names_bytes +|= out.name.len;
    }
    if (seen != 3) return null;
    const starts = arena.alloc(u32, work.len) catch return null;
    var rows: usize = 0;
    for (work, starts) |item, *start| {
        if (!owner_groups.admitsRows(.bytes, rows)) return null;
        start.* = @intCast(rows);
        const rg = &metas[item.file].row_groups.items[item.rg];
        if (rg.num_rows < 0) return null;
        rows +|= @intCast(rg.num_rows);
        // Stored bounds can only decline. Every consumed key still proves its exact length.
        if (rg.columns.items[key.col_idx].meta_data) |cm| if (cm.statistics) |st| {
            for ([_]?[]const u8{ st.min_value, st.max_value, st.min, st.max }) |bound| if (bound) |b|
                if (b.len > 15) return null;
        };
    }
    if (rows < args.byte_records_min_rows or !owner_groups.admitsRows(.bytes, rows)) return null;
    if (args.byte_records_min_rows != 0) if (ndv.footerEntries(metas, keys)) |guess|
        if (guess < rows / 4) return null;
    const producer_rows = if (args.decode_batch_rows != 0)
        args.decode_batch_rows
    else
        owner_groups.batchRows(args.byte_top_batch_rows);
    const output_credit = names_bytes +| @as(usize, @intCast(h)) *|
        (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue) + 15);
    return switch (call.func) {
        inline .sum, .count => |func| runByteTop(
            if (func == .sum) .sum_top else .count_top,
            gpa,
            arena,
            args,
            metas,
            plan,
            work,
            calls,
            keys,
            outputs,
            n_workers,
            timings,
            explain_only,
            .{ .key = key, .value_col = value_col, .rows = rows, .h = @intCast(h), .descending = rank.descending, .starts = starts, .producer_rows = producer_rows, .output_credit = output_credit, .names_bytes = names_bytes },
        ),
        else => unreachable,
    };
}

const ByteTopShape = struct {
    key: expr_ast.ColRef,
    value_col: usize,
    rows: usize,
    h: usize,
    descending: bool,
    starts: []u32,
    producer_rows: usize,
    output_credit: usize,
    names_bytes: usize,
};

fn runByteTop(
    comptime kind: owner_groups.Kind,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    n_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
    shape: ByteTopShape,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const Topk = owner_groups.Owner(kind, .bytes);
    const key = shape.key;
    const rows = shape.rows;
    const starts = shape.starts;
    const producer_rows = shape.producer_rows;
    const output_credit = shape.output_credit;
    const names_bytes = shape.names_bytes;
    const parts = Topk.partitionCount(rows, n_workers, producer_rows, args.byte_top_partitions);
    const quote = Topk.State.reservation(rows, n_workers, parts, shape.h, output_credit);
    if (quote > args.max_memory) return null;
    if (explain_only) return .{ .layout = if (kind == .sum_top) .byte_sum_top_h else .byte_count_top_h, .reservation = quote };
    if (!try shortSampleMayFit(gpa, args, metas, work, key.col_idx, 15)) return null;
    const s = (Topk.State.init(
        gpa,
        args.ledger,
        rows,
        n_workers,
        key.col_idx,
        shape.value_col,
        .{ .k = shape.h, .descending = shape.descending },
        parts,
        output_credit,
    ) catch return null) orelse
        return null;
    defer s.deinit();
    if (args.taken) |taken| taken.insert(.integer_records);
    const sinks = arena.alloc(consumer.RecordSink, n_workers) catch return error.ExceededMemoryBudget;
    for (sinks, s.locals) |*sink, *local| sink.* = if (kind == .sum_top) .{ .byte_top = local } else .{ .byte_count_top = local };
    var decoded = args;
    decoded.decode_batch_rows = producer_rows;
    const run = runRecordWorkers(
        gpa,
        arena,
        decoded,
        metas,
        plan,
        work,
        calls,
        keys,
        sinks,
        starts,
        outputs,
        timings,
    ) catch |e| {
        if (s.width_refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const start = nowMonoNs();
    s.finishSelected(@intCast(run.rows_matched)) catch |e|
        return if (e == error.OutOfMemory) error.ExceededMemoryBudget else e;
    const prof = profile.threadOf(args.profiler, .main);
    const fold_end = start + @as(i64, @intCast(s.fold_ns));
    const release_end = fold_end + @as(i64, @intCast(s.release_ns));
    profile.record(prof, .byte_top_fold, start, fold_end, profile.args(.{ parts, decoded.decode_batch_rows }));
    profile.record(prof, .byte_release, fold_end, release_end, profile.args(.{}));
    profile.record(prof, .byte_top_select, release_end, nowMonoNs(), profile.args(.{}));
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = gpa.alloc([]const u8, outputs.len) catch return error.ExceededMemoryBudget;
    @memset(names, "");
    output.group_cols = names;
    const types = arena.alloc(GroupColType, outputs.len) catch return error.ExceededMemoryBudget;
    for (outputs, types, names) |out, *ty, *name| {
        name.* = gpa.dupe(u8, out.name) catch return error.ExceededMemoryBudget;
        ty.* = .{
            .lane = if (out.source == .key) .string else .i64,
            .nullable = out.source == .key or kind != .count_top,
            .source = if (out.source == .key) leafSchemaElem(&metas[0], key.col_idx) else null,
        };
    }
    const answer_bytes = buildByteTopRows(kind, gpa, &output, s.top(), outputs) catch |e|
        return if (e == error.OutOfMemory) error.ExceededMemoryBudget else e;
    const owned = answer_bytes +| names_bytes;
    if (owned > s.output_credit) return error.ExceededMemoryBudget;
    // This small answer remains drawn until the query ledger dies, as with the other aggregate answers.
    if (args.ledger != null) s.charged -= owned;
    var bytes_in: u64 = 0;
    for (args.inputs) |in| bytes_in += if (in.logical_size != 0) in.logical_size else in.bytes.len;
    return .{
        .files_in = args.inputs.len,
        .rows_in = run.rows_in,
        .rows_scanned = run.rows_scanned,
        .rows_matched = run.rows_matched,
        .bytes_in = bytes_in,
        .row_groups_in = run.row_groups_in,
        .row_groups_pruned = run.row_groups_pruned,
        .row_groups_full_match = run.row_groups_full_match,
        .cols_stat_pruned = plan.cols_stat_pruned,
        .timings = run.timings,
        .output = output,
        .part_agg = true,
        .state = .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types },
    };
}

pub fn buildByteTopRows(
    comptime kind: owner_groups.Kind,
    gpa: std.mem.Allocator,
    output: *AggOutput,
    groups: []const owner_groups.Full.Group,
    outputs: []const BoundOutputColumn,
) !usize {
    const rows = try gpa.alloc([]const AggValue, groups.len);
    @memset(rows, &.{});
    output.group_rows = rows;
    const slab = try gpa.alloc(AggValue, groups.len * outputs.len);
    @memset(slab, .null_val);
    output.slab = slab;
    var owned = groups.len * (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue));
    for (groups, rows, 0..) |*g, *row, i| {
        const cells = slab[i * outputs.len ..][0..outputs.len];
        row.* = cells;
        for (outputs, cells) |out, *cell| cell.* = switch (out.source) {
            .key => key: {
                const bytes = g.keyBytes() orelse break :key .null_val;
                const copy = try gpa.dupe(u8, bytes);
                owned += bytes.len;
                break :key .{ .s = copy };
            },
            .agg => if (owner_groups.Owner(kind, .bytes).rank(g.*)) |v| .{ .i = v } else .null_val,
        };
    }
    return owned;
}

pub fn inlineByteAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    n_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const order = args.result_order;
    if (!args.byte_records or !allRescannable(args.inputs) or !(args.typed_answer or args.record_answer) or
        args.filter != null or
        order.keys.len != 0 or order.limit != null or order.offset != 0 or order.having != null or
        order.hidden.len != 0 or calls.len != 2) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    for (outputs) |out| if (out.derived != null) return null;
    if (keys.len != 1 or keys[0] != .col_ref or outputs.len != 3) return null;
    const key = keys[0].col_ref;
    if (key.physical_type != .BYTE_ARRAY or key.expr_type != .str) return null;
    var count: ?usize = null;
    var value: ?usize = null;
    for (calls, 0..) |call, i| {
        if (call.where != null or call.offset != 0 or call.state or call.result != .i64) return null;
        switch (call.func) {
            .count => {
                if (call.arg != null or count != null) return null;
                count = i;
            },
            .sum => {
                const arg = call.arg orelse return null;
                if (arg != .col_ref or arg.col_ref.physical_type != .INT64 or arg.col_ref.expr_type != .i64 or
                    arg.col_ref.unsigned_64 or value != null) return null;
                value = arg.col_ref.col_idx;
            },
            else => return null,
        }
    }
    if (count == null or value == null) return null;
    var seen: u8 = 0;
    for (outputs) |out| seen |= @as(u8, 1) << @intCast(switch (out.source) {
        .key => @as(usize, 0),
        .agg => |ai| ai + 1,
    });
    if (seen != 7) return null;
    const starts = try arena.alloc(u32, work.len);
    var rows: usize = 0;
    for (work, starts) |item, *start| {
        if (!owner_groups.admitsRows(.bytes, rows)) return null;
        start.* = @intCast(rows);
        const rg = &metas[item.file].row_groups.items[item.rg];
        if (rg.num_rows < 0) return null;
        rows +|= @intCast(rg.num_rows);
        // Bounds can cheaply decline a long-key shape, but never certify short keys: every decoded key is checked.
        if (rg.columns.items[key.col_idx].meta_data) |cm| if (cm.statistics) |st| {
            for ([_]?[]const u8{ st.min_value, st.max_value, st.min, st.max }) |bound| if (bound) |b|
                if (b.len > 16) return null;
        };
    }
    if (rows < args.byte_records_min_rows or !owner_groups.admitsRows(.bytes, rows) or
        owner_groups.Full.State.reservation(rows, n_workers, owner_groups.full_parts, 0, 0) > args.max_memory) return null;
    if (args.byte_records_min_rows != 0) if (ndv.footerEntries(metas, keys)) |guess|
        if (guess < rows / 4) return null;
    if (explain_only) return .{ .layout = .byte_group_stream, .reservation = owner_groups.Full.State.reservation(rows, n_workers, owner_groups.full_parts, 0, 0) };
    if (!try byteSampleMayFit(gpa, args, metas, work, key.col_idx)) return null;
    const state = (try owner_groups.Full.State.init(gpa, args.ledger, rows, n_workers, key.col_idx, value.?, {}, owner_groups.full_parts, 0)) orelse
        return null;
    state.wide_sums = args.record_answer and !args.typed_answer;
    errdefer state.deinit();
    if (args.taken) |taken| taken.insert(.integer_records);
    const sinks = try arena.alloc(consumer.RecordSink, n_workers);
    for (sinks, state.locals) |*sink, *local| sink.* = .{ .bytes = local };
    const run = runRecordWorkers(
        gpa,
        arena,
        args,
        metas,
        plan,
        work,
        calls,
        keys,
        sinks,
        starts,
        outputs,
        timings,
    ) catch |e| {
        if (state.width_refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const prof = profile.threadOf(args.profiler, .main);
    const begin = nowMonoNs();
    try state.finishSelected(@intCast(run.rows_matched));
    const scatter_end = begin + @as(i64, @intCast(state.partition_ns));
    const fold_end = scatter_end + @as(i64, @intCast(state.fold_ns));
    const release_end = fold_end + @as(i64, @intCast(state.release_ns));
    profile.record(prof, .byte_partition, begin, scatter_end, profile.args(.{}));
    profile.record(prof, .byte_fold, scatter_end, fold_end, profile.args(.{}));
    profile.record(prof, .byte_release, fold_end, release_end, profile.args(.{}));
    profile.record(prof, .byte_order, release_end, nowMonoNs(), profile.args(.{}));
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, types, names) |out, *ty, *name| {
        name.* = try gpa.dupe(u8, out.name);
        ty.* = .{
            .lane = if (out.source == .key) .string else .i64,
            .nullable = out.source == .key or out.source.agg != count.?,
            .source = if (out.source == .key) leafSchemaElem(&metas[0], key.col_idx) else null,
        };
    }
    output.byte_stream = .{ .state = state, .outputs = outputs, .count_call = count.? };
    var bytes_in: u64 = 0;
    for (args.inputs) |in| bytes_in += if (in.logical_size != 0) in.logical_size else in.bytes.len;
    return .{
        .files_in = args.inputs.len,
        .rows_in = run.rows_in,
        .rows_scanned = run.rows_scanned,
        .rows_matched = run.rows_matched,
        .bytes_in = bytes_in,
        .row_groups_in = run.row_groups_in,
        .row_groups_pruned = run.row_groups_pruned,
        .row_groups_full_match = run.row_groups_full_match,
        .cols_stat_pruned = plan.cols_stat_pruned,
        .timings = run.timings,
        .output = output,
        .part_agg = true,
        .state = .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types },
    };
}


fn byteTopRowsTrial(a: std.mem.Allocator) !void {
    var output: AggOutput = .{};
    defer output.deinit(a);
    // Present SUMs (bit 30); key lengths in the high ordinal bits; bit 31 is a NULL key.
    const groups = [_]owner_groups.Full.Group{
        .{ .key = 'a', .sum = @as(i128, std.math.maxInt(i64)) * 2, .count = 1 << 30, .first = 0 | 2 << 27 },
        .{ .key = 0, .sum = -7, .count = 1 << 30, .first = 1 },
        .{ .key = 0, .count = 1 << 31, .first = 2 },
    };
    const outputs = [_]BoundOutputColumn{
        .{ .name = "s", .source = .{ .agg = 0 } },
        .{ .name = "k", .source = .{ .key = 0 } },
    };
    const owned = try buildByteTopRows(.sum_top, a, &output, &groups, &outputs);
    try testing.expect(owned > 2);
    try testing.expectEqual(groups[0].sum, output.group_rows.?[0][0].i);
    try testing.expectEqualStrings("a\x00", output.group_rows.?[0][1].s);
    try testing.expectEqualStrings("", output.group_rows.?[1][1].s);
    try testing.expect(output.group_rows.?[2][0] == .null_val and output.group_rows.?[2][1] == .null_val);
}

test "packed byte SUM answer owns its keys across partial allocation failures" {
    try byteTopRowsTrial(testing.allocator);
    try testing.checkAllAllocationFailures(testing.allocator, byteTopRowsTrial, .{});
}

fn msdOutputAllocTrial(a: std.mem.Allocator) !void {
    var output: AggOutput = .{};
    defer output.deinit(a);
    const key = "a long retained key with an embedded\x00byte";
    const groups = [_]msd_groups.Group{
        .{ .record = .{ .word = 0, .ptr = @intFromPtr(key.ptr), .ordinal = 3, .len = key.len, .f = -3001 }, .count = 23 },
        .{ .record = .{ .word = 0, .ptr = 0, .ordinal = (1 << 31) | (1 << 30) | 7, .len = 0, .f = 0 }, .count = 21 },
    };
    const outputs = [_]BoundOutputColumn{
        .{ .name = "c", .source = .{ .agg = 0 } },
        .{ .name = "k", .source = .{ .key = 0 } },
        .{ .name = "f", .source = .{ .key = 1 } },
    };
    const owned = try buildMsdTopRows(a, &output, &groups, &outputs);
    try testing.expect(owned > key.len);
    try testing.expectEqualStrings(key, output.group_rows.?[0][1].s);
    try testing.expect(@intFromPtr(output.group_rows.?[0][1].s.ptr) != @intFromPtr(key.ptr));
    try testing.expectEqual(@as(i64, -3001), output.group_rows.?[0][2].i);
    try testing.expectEqual(AggValue.null_val, output.group_rows.?[1][1]);
    try testing.expectEqual(AggValue.null_val, output.group_rows.?[1][2]);
}

test "retained MSD top rows own keys across partial allocation failures" {
    try msdOutputAllocTrial(testing.allocator);
    try testing.checkAllAllocationFailures(testing.allocator, msdOutputAllocTrial, .{});
}

