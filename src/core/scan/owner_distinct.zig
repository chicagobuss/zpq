const std = @import("std");
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const ndv = @import("../ndv.zig");
const owner_groups = @import("../owner_groups.zig");
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_result = @import("result.zig");
const ScanState = scan_result.ScanState;
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const record_workers = @import("record_workers.zig");
const runRecordWorkers = record_workers.runRecordWorkers;
const owner_support = @import("owner_support.zig");
const IntegerRangeHint = owner_support.IntegerRangeHint;
const owner_pair = @import("owner_pair.zig");
const countedCalls = owner_pair.countedCalls;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const WorkItem = scan_types.WorkItem;
const output_values = @import("output_values.zig");
const AggOutputItem = output_values.AggOutputItem;

pub fn shortSampleMayFit(
    gpa: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    key_col: usize,
    comptime width: usize,
) !bool {
    for (work) |item| {
        // Readers have not started. Do not fault their mapping for an optional cost probe.
        if (args.inputs[item.file].gate != null) continue;
        const rg = &metas[item.file].row_groups.items[item.rg];
        const cm = rg.columns.items[key_col].meta_data orelse return false;
        if (rg.num_rows == 0) continue;
        if (cm.statistics) |st| if (st.min_value != null or st.max_value != null or
            st.min != null or st.max != null) continue;
        const cap = 4 << 20;
        if (args.ledger) |l| _ = l.drawExact(cap) orelse return false;
        defer if (args.ledger) |l| l.release(cap);
        const buf = gpa.alloc(u8, cap) catch return false;
        defer gpa.free(buf);
        var fixed = std.heap.FixedBufferAllocator.init(buf);
        const a = fixed.allocator();
        var reader = (consumer.SliceReader.init(a, a, rg, &metas[item.file], .{
            .bytes = args.inputs[item.file].plan_bytes orelse args.inputs[item.file].bytes,
            .byte_origin = 0,
        }, key_col, .{ .encoded = args.encoded, .expand_limit = cap }) catch return false) orelse return false;
        defer reader.deinit();
        const sampled: usize = @intCast(@min(rg.num_rows, 64));
        const col = reader.next(a, sampled) catch return false;
        const strings = col.string;
        for (0..sampled) |i| {
            if (strings.def_levels) |d| if (d[i] < strings.max_def) continue;
            const value = if (strings.codes) |codes| strings.dict[codes[i]] else strings.values[i];
            if (value.len > width) return false;
        }
        return true;
    }
    return true;
}

/// Only a cost probe. Its read failure or an oversized header declines before decoded rows are consumed.
pub fn scalarDistinctWideParts(arg: expr_ast.Expr, metas: []const schema.FileMetaData, work: []const WorkItem) bool {
    if (arg.typeOf() != .i64) return true;
    if (arg != .col_ref) return false;
    const c = arg.col_ref;
    const width: usize = switch (c.physical_type) {
        .INT32 => 4,
        .INT64 => 8,
        else => return true,
    };
    var lo: i128 = std.math.maxInt(i128);
    var hi: i128 = std.math.minInt(i128);
    for (work) |item| {
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        if (rg.num_rows == 0) continue;
        const b = @import("../parquet/statistics.zig").chunkStats(rg, c.col_idx, meta, .corroborated).bounds orelse return false;
        if (b.min.len != width or b.max.len != width) return false;
        const low: i128 = if (width == 4)
            if (c.unsigned_32) std.mem.readInt(u32, b.min[0..4], .little) else std.mem.readInt(i32, b.min[0..4], .little)
        else if (c.unsigned_64) std.mem.readInt(u64, b.min[0..8], .little) else std.mem.readInt(i64, b.min[0..8], .little);
        const high: i128 = if (width == 4)
            if (c.unsigned_32) std.mem.readInt(u32, b.max[0..4], .little) else std.mem.readInt(i32, b.max[0..4], .little)
        else if (c.unsigned_64) std.mem.readInt(u64, b.max[0..8], .little) else std.mem.readInt(i64, b.max[0..8], .little);
        lo = @min(lo, low);
        hi = @max(hi, high);
    }
    if (lo > hi) return false;
    const bytes = (@as(u128, @intCast(hi - lo)) + 64) / 64 * @sizeOf(u64);
    return bytes > @import("../hw.zig").l2BytesPerWorkerOr(256 << 10);
}

/// A footer-range cost hint shared by scalar and grouped distinct admission; it never supplies an answer.
// Scalar cardinality reuses count-only owner records and the complete partition fold.
pub fn inlineDistinctAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const order = args.result_order;
    if (args.group_by != null or args.filter != null or !allRescannable(args.inputs) or calls.len != 1 or
        n_workers > 64 or
        order.keys.len != 0 or order.limit != null or order.offset != 0 or order.having != null or
        order.hidden.len != 0) return null;
    const call = calls[0];
    if (call.func != .count_distinct or call.where != null or call.offset != 0 or call.state or
        call.result != .i64) return null;
    const arg = call.arg orelse return null;
    if (arg != .col_ref) return null;
    const key = arg.col_ref;
    const codec: owner_groups.Codec = if (key.physical_type == .INT64 and key.expr_type == .i64 and
        !key.unsigned_64) .int64 else if (key.physical_type == .BYTE_ARRAY and key.expr_type == .str)
        .bytes
    else
        return null;
    if (if (codec == .int64) !args.integer_records else !args.byte_records) return null;
    const threshold = if (codec == .int64) args.integer_records_min_rows else args.byte_records_min_rows;
    const starts = try arena.alloc(u32, work.len);
    var rows: usize = 0;
    var key_range: IntegerRangeHint = .{};
    for (work, starts) |item, *start| {
        if (rows >= std.math.maxInt(u32)) return null;
        start.* = @intCast(rows);
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const cm = rg.columns.items[key.col_idx].meta_data orelse return null;
        if (meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        if (codec == .bytes) if (cm.statistics) |st| {
            for ([_]?[]const u8{ st.min_value, st.max_value, st.min, st.max }) |bound| if (bound) |bytes|
                if (bytes.len > 15) return null;
        };
        const n = rg.num_rows;
        if (n < 0) return null;
        rows +|= @intCast(n);
        if (codec == .int64 and threshold != 0) key_range.include(rg, meta, key.col_idx);
    }
    if (rows == 0 or rows < threshold) return null;
    // A narrow footer range favors D1 even when PLAIN pages make dictionary cardinality unknown.
    // This only declines the owner attempt; ordinary decoding still validates every value.
    if (codec == .int64 and threshold != 0 and key_range.small(rows)) return null;
    if (threshold != 0) if (ndv.footerEntries(metas, &.{arg})) |guess| if (guess < rows / 4) return null;
    switch (codec) {
        inline else => |c| {
            const Owner = owner_groups.Owner(.count_top, c);
            if (!owner_groups.admitsRows(c, rows)) return null;
            const batch_rows = if (args.decode_batch_rows != 0)
                args.decode_batch_rows
            else
                owner_groups.batchRows(args.byte_top_batch_rows);
            const parts = Owner.partitionCount(rows, n_workers, batch_rows, args.byte_top_partitions);
            const credit = @sizeOf(AggOutputItem) +| call.alias.len;
            const quote = Owner.State.reservation(rows, n_workers, parts, 0, credit);
            if (quote > args.max_memory) return null;
            if (explain_only) return .{
                .layout = if (c == .int64) .integer_distinct else .byte_distinct,
                .reservation = quote,
            };
            if (c == .bytes and !try shortSampleMayFit(gpa, args, metas, work, key.col_idx, 15)) return null;
            const state = (try Owner.State.initCardinality(
                gpa,
                args.ledger,
                rows,
                n_workers,
                key.col_idx,
                parts,
                credit,
            )) orelse return null;
            defer state.deinit();
            if (args.taken) |taken| taken.insert(.integer_records);
            const sinks = try arena.alloc(consumer.RecordSink, n_workers);
            for (sinks, state.locals) |*sink, *local|
                sink.* = if (c == .int64) .{ .integer_count_top = .{ .local = local } } else .{ .byte_count_top = local };
            var decoded = args;
            decoded.decode_batch_rows = batch_rows;
            const run = runRecordWorkers(
                gpa,
                arena,
                decoded,
                metas,
                plan,
                work,
                calls,
                &.{arg},
                sinks,
                starts,
                &.{},
                timings,
            ) catch |e| {
                if (state.width_refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
                return e;
            };
            const merge_span = profile.Span.begin(profile.threadOf(args.profiler, .main));
            state.finishSelected(@intCast(run.rows_matched)) catch |e|
                return if (e == error.OutOfMemory) error.ExceededMemoryBudget else e;
            merge_span.end(.merge, profile.args(.{parts}));
            // Scalar writers finalize ScanState, so retain the finished count as well as the JSON value.
            const finished_calls = try countedCalls(arena, calls, 0);
            const finished = try arena.alloc(expr_agg.Accumulator, 1);
            finished[0] = .{ .count = @intCast(state.group_count) };
            const alias = gpa.dupe(u8, call.alias) catch return error.ExceededMemoryBudget;
            errdefer gpa.free(alias);
            const items = gpa.alloc(AggOutputItem, 1) catch return error.ExceededMemoryBudget;
            items[0] = .{ .alias = alias, .value = .{ .i = @intCast(state.group_count) } };
            if (args.ledger != null) state.charged -= credit;
            for (work) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
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
                .output = .{ .aggs = items },
                .state = .{ .accumulators = finished, .agg_calls = finished_calls, .agg_sources = &.{null} },
            };
        },
    }
}

