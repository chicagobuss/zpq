const std = @import("std");
const nowMonoNs = @import("../../clock.zig").monoNs;
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const ndv = @import("../ndv.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const owner_groups = @import("../owner_groups.zig");
const integer_radix = @import("../integer_radix.zig");
const NdvStat = ndv.Stat;
const filter_prune = @import("../filter/prune.zig");
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_result = @import("result.zig");
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const scan_binding = @import("binding.zig");
const leafSchemaElem = scan_binding.leafSchemaElem;
const scan_plan_impl = @import("plan.zig");
const wideKey = scan_plan_impl.wideKey;
const record_workers = @import("record_workers.zig");
const runRecordWorkers = record_workers.runRecordWorkers;
const owner_support = @import("owner_support.zig");
const IntegerRangeHint = owner_support.IntegerRangeHint;
const outputNamesBytes = owner_support.outputNamesBytes;
const input_rewind = @import("input_rewind.zig");
const rewindInputs = input_rewind.rewindInputs;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const integer_stream = @import("integer_stream.zig");
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const testing = std.testing;

pub const IntegerCountBounds = union(enum) {
    pending: *bool,
    refined: ?NdvStat,

    fn guess(bounds: @This(), footer: u64) u64 {
        if (bounds == .refined) if (bounds.refined) |stat| if (stat.union_complete) return stat.hi;
        return footer;
    }
};

/// Whether a raw integer owner should take a count or full answer, from the summed footer dictionaries (`guess`).
/// Summed dictionaries can repeat the same small domain in every RG. A reduction hint defers the decision until the
/// union is known; only a complete union replaces the summed footer estimate. An unfiltered count cut always asks:
/// many small row groups each holding most of a small domain sum to more than a quarter of the rows, and the owner
/// would then displace the dense lanes the union admits.
const IntegerGate = enum { owner, coded, ask_union };

fn integerCountGate(
    guess: u64,
    rows: usize,
    unfiltered_top: bool,
    union_allowed: bool,
    bounds: IntegerCountBounds,
) IntegerGate {
    const refine = union_allowed and (guess < rows / 4 or unfiltered_top);
    if (unfiltered_top and bounds == .pending and refine and guess > agg_dense.max_ids) return .ask_union;
    const prefer_coded = if (unfiltered_top)
        (if (refine) bounds.guess(guess) else guess) <= agg_dense.max_ids
    else
        guess < rows / 4;
    return if (prefer_coded) .coded else .owner;
}

test "regression: an unfiltered integer count cut over a repeated small domain asks the union before an owner" {
    // rg64k_100k: 458 row groups of 64K rows each hold most of 100K keys. Their dictionaries sum past a quarter of
    // the rows and past the dense limit, so the owner took the cut from the dense lanes (6d46d1e2, 1.5x slower).
    const rows: usize = 30_000_000;
    const summed: u64 = 22_000_000;
    var asked = false;
    try testing.expectEqual(.ask_union, integerCountGate(summed, rows, true, true, .{ .pending = &asked }));
    const small: NdvStat = .{ .rows = rows, .hi = 100_000, .union_complete = true };
    try testing.expectEqual(.coded, integerCountGate(summed, rows, true, true, .{ .refined = small }));
    // A union as wide as the footer says keeps the owner; so does a union not complete.
    const wide: NdvStat = .{ .rows = rows, .hi = 10_000_000, .union_complete = true };
    try testing.expectEqual(.owner, integerCountGate(summed, rows, true, true, .{ .refined = wide }));
    const partial: NdvStat = .{ .rows = rows, .hi = 100_000, .union_complete = false };
    try testing.expectEqual(.owner, integerCountGate(summed, rows, true, true, .{ .refined = partial }));
    // Filtered, the footer's reduction hint alone decides, as before.
    try testing.expectEqual(.owner, integerCountGate(summed, rows, false, true, .{ .refined = small }));
    try testing.expectEqual(.coded, integerCountGate(1000, rows, false, true, .{ .refined = null }));
}

pub fn inlineIntegerAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: *MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    requested_workers: usize,
    timings: Timings,
    bounds: IntegerCountBounds,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const n_workers = @min(requested_workers, owner_groups.max_workers);
    const order = args.result_order;
    if (!args.integer_records or !allRescannable(args.inputs) or
        order.having != null or
        order.hidden.len != 0) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    for (outputs) |out| if (out.derived != null) return null;
    const full = order.keys.len == 0 and order.limit == null and order.offset == 0;
    var cut: ?owner_groups.Cut = null;
    if (full) {
        if (!(args.typed_answer or args.record_answer) or calls.len != 2) return null;
    } else {
        const limit = order.limit orelse return null;
        if (limit == 0 or calls.len != 1 or order.keys.len != 1) return null;
        const k = std.math.cast(usize, order.offset +| limit) orelse return null;
        if (k == 0 or k > 1024) return null;
        const source = outputs[order.keys[0].column].source;
        if (source != .agg or source.agg != 0) return null;
        cut = .{ .k = k, .descending = order.keys[0].descending };
    }
    if (keys.len != 1 or keys[0] != .col_ref or outputs.len != calls.len + 1) return null;
    const key = keys[0].col_ref;
    if (key.physical_type != .INT64 or key.expr_type != .i64 or key.unsigned_64) return null;
    var count: ?usize = null;
    var value: ?usize = null;
    var distinct = false;
    for (calls, 0..) |call, i| {
        if (call.where != null or call.offset != 0 or call.state or call.result != .i64) return null;
        switch (call.func) {
            .count => {
                if (call.arg != null or count != null) return null;
                count = i;
            },
            .sum, .count_distinct => {
                if (call.func == .count_distinct) {
                    if (full) return null;
                    // Grouped DISTINCT keeps its established optional output schema, unlike bare COUNT.
                    distinct = true;
                }
                const arg = call.arg orelse return null;
                if (arg != .col_ref or arg.col_ref.physical_type != .INT64 or arg.col_ref.expr_type != .i64 or
                    arg.col_ref.unsigned_64 or value != null) return null;
                value = arg.col_ref.col_idx;
            },
            else => return null,
        }
    }
    const count_top = !full and count != null and !distinct;
    if ((full and count == null) or (!count_top and value == null)) return null;
    if (args.filtered() and !count_top) return null;
    const ordered_count = count_top and orderedIntegerHint(metas, work, key.col_idx);
    // Raw integer identities need no retained dictionary union. The cost gates below leave small domains on
    // their coded folds, while wide count cuts avoid a sketch and its certification rescan.
    // Leave compressed/skew keys on the established distinct certifier. This is a cost hint over all work,
    // never a bound on identity or a reason to skip a decoded value.
    if (distinct and (args.distinct_pairs == .on or
        (args.integer_records_min_rows != 0 and !wideKey(metas, work, key.col_idx)))) return null;
    var owner_args = args.*;
    if (distinct and owner_args.decode_batch_rows == 0)
        owner_args.decode_batch_rows = owner_groups.batchRows(args.byte_top_batch_rows);
    var seen: u8 = 0;
    for (outputs) |out| seen |= @as(u8, 1) << @intCast(switch (out.source) {
        .key => @as(usize, 0),
        .agg => |ai| ai + 1,
    });
    if (seen != (if (full) @as(u8, 7) else @as(u8, 3))) return null;
    const starts = try arena.alloc(u32, work.len);
    var rows: usize = 0;
    var key_range: IntegerRangeHint = .{};
    for (work, starts) |item, *start| {
        if (!owner_groups.admitsRows(.int64, rows)) return null;
        start.* = @intCast(rows);
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const n = rg.num_rows;
        if (n < 0) return null;
        rows +|= @intCast(n);
        if (distinct and args.integer_records_min_rows != 0) key_range.include(rg, meta, key.col_idx);
    }
    if (distinct and args.integer_records_min_rows != 0 and key_range.small(rows)) return null;
    if (rows < args.integer_records_min_rows or !owner_groups.admitsRows(.int64, rows)) return null;
    const radix_sum = !full and !count_top and !distinct and !orderedIntegerHint(metas, work, key.col_idx) and
        integer_radix.admits(rows, n_workers, ndv.footerEntries(metas, keys));
    if (radix_sum and owner_args.decode_batch_rows == 0)
        owner_args.decode_batch_rows = owner_groups.batchRows(null);
    const radix_parts = if (radix_sum)
        integer_radix.partitionCount(rows, n_workers, owner_args.decode_batch_rows, args.byte_top_partitions)
    else
        2048;
    const quote = if (radix_sum)
        integer_radix.State.reservation(rows, n_workers, radix_parts, cut.?.k, outputNamesBytes(outputs))
    else
        integerQuote(rows, n_workers, cut, owner_args, count_top, ordered_count, work.len, outputNamesBytes(outputs));
    if (quote > args.max_memory) return null;
    // A small stored-dictionary estimate favors the established coded fold. Compression can make this estimate low:
    // it is only a cost decision, never a proof about key identity, bounds, or which rows must be evaluated.
    if (args.integer_records_min_rows != 0) {
        if (ndv.footerEntries(metas, keys)) |guess| {
            const unfiltered_top = count_top and !ordered_count and !args.filtered();
            switch (integerCountGate(guess, rows, unfiltered_top, args.ndv_policy and args.ndv_union, bounds)) {
                .owner => {},
                .coded => return null,
                .ask_union => {
                    bounds.pending.* = true;
                    return null;
                },
            }
        } else if (count_top and !ordered_count) {
            var survivors: f64 = 0;
            var range: IntegerRangeHint = .{};
            for (work) |item| {
                const meta = &metas[item.file];
                const rg = &meta.row_groups.items[item.rg];
                const decision = plan.decide(rg, meta);
                if (decision == .skip) continue;
                const share = if (decision == .full_match or plan.query.filter == null)
                    1
                else
                    filter_prune.passFraction(plan.query.filter.?, rg, meta);
                survivors += @as(f64, @floatFromInt(@max(0, rg.num_rows))) * share;
                range.include(rg, meta, key.col_idx);
            }
            // An estimated survivor count also caps its domain for costing. Never bounds or skips decoded identities.
            const kept: usize = @intFromFloat(survivors);
            if (kept < args.integer_records_min_rows or range.small(kept)) return null;
        }
    }
    if (explain_only) return .{
        .layout = if (distinct) .integer_distinct_top_h else if (count_top)
            .integer_count_top_h
        else if (cut != null) .integer_sum_top_h else .integer_group_stream,
        .reservation = quote,
        .partitions = radix_parts,
    };
    if (cut) |c| {
        if (distinct) return runIntegerOwner(
            .sum_top,
            false,
            gpa,
            arena,
            &owner_args,
            metas,
            plan,
            work,
            calls,
            keys,
            outputs,
            n_workers,
            timings,
            rows,
            key.col_idx,
            value.?,
            count,
            starts,
            c,
            false,
        );
        if (count_top) return runIntegerOwner(
            .count_top,
            false,
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
            rows,
            key.col_idx,
            0,
            count,
            starts,
            c,
            ordered_count,
        );
        if (orderedIntegerHint(metas, work, key.col_idx)) if (try runIntegerOwner(
            .sum_top,
            false,
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
            rows,
            key.col_idx,
            value.?,
            count,
            starts,
            c,
            true,
        )) |answer| return answer;
        inline for (.{ true, false }) |radix| {
            if (radix == radix_sum) return try runIntegerOwner(
                .sum_top,
                radix,
                gpa,
                arena,
                if (radix) &owner_args else args,
                metas,
                plan,
                work,
                calls,
                keys,
                outputs,
                n_workers,
                timings,
                rows,
                key.col_idx,
                value.?,
                count,
                starts,
                c,
                false,
            );
        }
        unreachable;
    }
    if (orderedIntegerHint(metas, work, key.col_idx)) if (try runIntegerOwner(
        .full,
        false,
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
        rows,
        key.col_idx,
        value.?,
        count,
        starts,
        {},
        true,
    )) |answer| return answer;
    return try runIntegerOwner(
        .full,
        false,
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
        rows,
        key.col_idx,
        value.?,
        count,
        starts,
        {},
        false,
    );
}

/// Footer bounds select an attempt; every decoded key and every source seam must still prove the order.
fn orderedIntegerHint(metas: []const schema.FileMetaData, work: []const WorkItem, column: usize) bool {
    const statistics = @import("../parquet/statistics.zig");
    var previous: ?i64 = null;
    for (work) |item| {
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        if (rg.num_rows == 0) continue;
        const stats = statistics.chunkStats(rg, column, meta, .corroborated);
        // Bounds exclude NULLs. A run cannot represent them; unknown nullability cannot admit a run either.
        if ((stats.null_count orelse -1) != 0) return false;
        const b = stats.bounds orelse return false;
        if (b.min.len != 8 or b.max.len != 8) return false;
        const lo = std.mem.readInt(i64, b.min[0..8], .little);
        const hi = std.mem.readInt(i64, b.max[0..8], .little);
        if (previous) |p| if (lo < p) return false;
        previous = hi;
    }
    return previous != null;
}

/// The reservation of an admitted integer record layout: the full answer's established quote, or a top-H attempt's.
pub fn integerQuote(
    rows: usize,
    n_workers: usize,
    cut: ?owner_groups.Cut,
    args: MultiAggArgs,
    count_top: bool,
    ordered: bool,
    items: usize,
    names_credit: usize,
) usize {
    const c = cut orelse return owner_groups.IntFull.State.reservation(
        rows,
        n_workers,
        owner_groups.full_parts,
        0,
        names_credit,
    );
    switch (if (count_top) owner_groups.Kind.count_top else owner_groups.Kind.sum_top) {
        inline else => |kind| {
            const Owner = owner_groups.Owner(kind, .int64);
            const parts = Owner.partitionCount(rows, n_workers, args.decode_batch_rows, args.byte_top_partitions);
            return if (ordered and kind == .count_top and args.sorted_count_stream)
                Owner.State.streamReservation(n_workers, items, c.k, names_credit)
            else if (ordered)
                Owner.State.runReservation(rows, n_workers, parts, c.k, names_credit)
            else
                Owner.State.reservation(rows, n_workers, parts, c.k, names_credit);
        },
    }
}

fn runIntegerOwner(
    comptime kind: owner_groups.Kind,
    comptime radix: bool,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: *MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    n_workers: usize,
    timings: Timings,
    rows: usize,
    key_col: usize,
    value_col: usize,
    count: ?usize,
    starts: []u32,
    cut: if (kind == .full) void else owner_groups.Cut,
    ordered: bool,
) !?MultiAggResult {
    const Owner = if (radix) integer_radix else owner_groups.Owner(kind, .int64);
    const parts = if (kind == .full)
        owner_groups.full_parts
    else
        Owner.partitionCount(rows, n_workers, args.decode_batch_rows, args.byte_top_partitions);
    const distinct = kind == .sum_top and calls.len == 1 and calls[0].func == .count_distinct;
    const names_credit = outputNamesBytes(outputs);
    const state = (if (radix)
        try Owner.State.init(gpa, args.ledger, rows, n_workers, key_col, value_col, cut, parts, names_credit)
    else if (kind == .sum_top and distinct)
        try Owner.State.initDistinctTop(
            gpa,
            args.ledger,
            rows,
            n_workers,
            key_col,
            value_col,
            cut,
            parts,
            names_credit,
        )
    else if (ordered and kind == .count_top and args.sorted_count_stream)
        try Owner.State.initStreamRuns(gpa, args.ledger, rows, n_workers, key_col, cut, parts, work.len, names_credit)
    else if (ordered)
        try Owner.State.initRuns(
            gpa,
            args.ledger,
            rows,
            n_workers,
            key_col,
            value_col,
            cut,
            parts,
            work.len,
            names_credit,
        )
    else
        try Owner.State.init(
            gpa,
            args.ledger,
            rows,
            n_workers,
            key_col,
            value_col,
            cut,
            parts,
            names_credit,
        )) orelse return null;
    if (kind == .full) state.wide_sums = args.record_answer and !args.typed_answer;
    var state_live = true;
    errdefer if (state_live) state.deinit();
    if (args.taken) |taken| taken.insert(.integer_records);
    const sinks = try arena.alloc(consumer.RecordSink, n_workers);
    for (sinks, state.locals) |*sink, *local| sink.* = if (radix) .{ .selected = .{
        .ctx = local,
        .startFn = integer_radix.Local.startErased,
        .batchFn = integer_radix.Local.batchErased,
    } } else switch (kind) {
        .full => .{ .integer = local },
        .sum_top => .{ .integer_top = local },
        .count_top => .{ .integer_count_top = .{ .local = local } },
    };
    const run = try runRecordWorkers(
        gpa,
        arena,
        args.*,
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
    const prof = profile.threadOf(args.profiler, .main);
    const final_start = nowMonoNs();
    state.finishSelected(@intCast(run.rows_matched)) catch |err| {
        if (err != error.RunOrderDeclined) return err;
        try rewindInputs(arena, args);
        state.deinit();
        state_live = false;
        if (kind == .count_top) return runIntegerOwner(
            kind,
            false,
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
            rows,
            key_col,
            value_col,
            count,
            starts,
            cut,
            false,
        );
        if (args.taken) |taken| taken.remove(.integer_records);
        return null;
    };
    const scatter_end = final_start + @as(i64, @intCast(state.partition_ns));
    const fold_end = scatter_end + @as(i64, @intCast(state.fold_ns));
    profile.record(prof, .integer_scatter, final_start, scatter_end, profile.args(.{}));
    profile.record(prof, .integer_fold, scatter_end, fold_end, profile.args(.{}));
    profile.record(prof, .integer_order, fold_end, nowMonoNs(), profile.args(.{}));
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, types, names) |out, *ty, *name| {
        name.* = try gpa.dupe(u8, out.name);
        ty.* = .{ .lane = .i64, .nullable = switch (out.source) {
            .key => true,
            .agg => |ai| count == null or ai != count.?,
        }, .source = if (out.source == .key) leafSchemaElem(&metas[0], key_col) else null };
    }
    if (kind != .full) {
        const kept = state.top();
        try buildIntegerTopRows(kind, gpa, &output, kept, outputs);
        // As with the general GROUP BY answer, keep its row storage drawn until the query ledger dies. Transfer
        // that tiny part of the reservation before releasing the records; no owning state remains in the answer.
        if (args.ledger != null) state.charged -= @min(state.charged, names_credit + kept.len *
            (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue)));
        state.deinit();
    } else output.integer_stream = .{ .state = state, .outputs = outputs, .count_call = count.? };
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

fn buildIntegerTopRows(
    comptime kind: owner_groups.Kind,
    gpa: std.mem.Allocator,
    output: *AggOutput,
    groups: []const owner_groups.IntSumTop.Group,
    outputs: []const BoundOutputColumn,
) !void {
    const rows = try gpa.alloc([]const AggValue, groups.len);
    @memset(rows, &.{});
    output.group_rows = rows;
    const slab = try gpa.alloc(AggValue, groups.len * outputs.len);
    @memset(slab, .null_val);
    output.slab = slab;
    for (groups, rows, 0..) |g, *row, i| {
        const cells = slab[i * outputs.len ..][0..outputs.len];
        row.* = cells;
        for (outputs, cells) |out, *cell| {
            const v: ?i128 = switch (out.source) {
                .key => if (g.keyValue()) |x| @as(i128, x) else null,
                .agg => owner_groups.Owner(kind, .int64).rank(g),
            };
            cell.* = if (v) |x| .{ .i = x } else .null_val;
        }
    }
}

fn integerTopRowsTrial(a: std.mem.Allocator) !void {
    var output: AggOutput = .{};
    defer output.deinit(a);
    // Integer groups keep SUM-present in count bit 31 and key-NULL in first bit 31.
    const groups = [_]owner_groups.IntSumTop.Group{
        .{ .sum = @as(i128, std.math.maxInt(i64)) * 2, .key = @as(u64, @bitCast(@as(i64, -7))), .count = 1 << 31, .first = 0 },
        .{ .sum = 0, .key = 0, .count = 0, .first = (1 << 31) | 1 },
    };
    const outputs = [_]BoundOutputColumn{
        .{ .name = "s", .source = .{ .agg = 0 } },
        .{ .name = "k", .source = .{ .key = 0 } },
    };
    try buildIntegerTopRows(.sum_top, a, &output, &groups, &outputs);
    try testing.expectEqual(groups[0].sum, output.group_rows.?[0][0].i);
    try testing.expectEqual(@as(i128, -7), output.group_rows.?[0][1].i);
    try testing.expect(output.group_rows.?[1][0] == .null_val and output.group_rows.?[1][1] == .null_val);
}

test "inline integer SUM top rows preserve wide values and free partial allocations" {
    try testing.checkAllAllocationFailures(testing.allocator, integerTopRowsTrial, .{});
}

test "integer COUNT domain cost trusts only a complete dictionary union" {
    const summed: u64 = 29 * 100000;
    try testing.expectEqual(summed, (IntegerCountBounds{ .refined = null }).guess(summed));
    try testing.expectEqual(summed, (IntegerCountBounds{ .refined = .{
        .hi = 100000,
        .union_complete = false,
    } }).guess(summed));
    try testing.expectEqual(@as(u64, 100000), (IntegerCountBounds{ .refined = .{
        .hi = 100000,
        .union_complete = true,
    } }).guess(summed));
    try testing.expectEqual(summed, (IntegerCountBounds{ .refined = .{
        .hi = summed,
        .union_complete = true,
    } }).guess(summed));
}

