const std = @import("std");
const nowMonoNs = @import("../../clock.zig").monoNs;
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const ndv = @import("../ndv.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const key_pack = @import("../expr/key_pack.zig");
const owner_groups = @import("../owner_groups.zig");
const NdvStat = ndv.Stat;
const key_decode = @import("key_decode.zig");
const KeyType = key_decode.KeyType;
const keyTypeFromExpr = key_decode.keyTypeFromExpr;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const scan_materialize = @import("materialize.zig");
const materializeOne = scan_materialize.materializeOne;
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const scan_result = @import("result.zig");
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const scan_binding = @import("binding.zig");
const leafSchemaElem = scan_binding.leafSchemaElem;
const scan_plan_impl = @import("plan.zig");
const selectedGroupBytes = scan_plan_impl.selectedGroupBytes;
const scan_rows = @import("rows.zig");
const RowSource = scan_rows.RowSource;
const buildGroupRows = scan_rows.buildGroupRows;
const record_workers = @import("record_workers.zig");
const runRecordWorkers = record_workers.runRecordWorkers;
const owner_support = @import("owner_support.zig");
const pairRawIdentity = owner_support.pairRawIdentity;
const outputNamesBytes = owner_support.outputNamesBytes;
const ownerIdentity = owner_support.ownerIdentity;
const passThroughSource = owner_support.passThroughSource;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const RecordOrdinals = scan_types.RecordOrdinals;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;

pub fn inlineSelectedCount(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    domain: ?*const agg_dense.Domain,
    stat: ?NdvStat,
    pass_share: f64,
    requested_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    if (!args.integer_records or !allRescannable(args.inputs)) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    const program = agg_plan.OwnerProgram.init(calls) orelse return null;
    if (program.distinct_arg != null or calls.len == 0) return null;
    if (program.len != 0 and keys.len < 2) return null;
    for (outputs) |out| if (out.derived != null) return null;
    // Preserve P1's nullable INT64 sidecar and 16-byte producer while extending physical INT32 and tuples.
    if (keys.len == 1 and keys[0] == .col_ref and keys[0].col_ref.physical_type == .INT64 and
        !keys[0].col_ref.unsigned_64) return null;
    const order = args.result_order;
    if (order.keys.len != 1 or order.having != null or order.hidden.len != 0) return null;
    if (order.keys[0].column >= outputs.len or outputs[order.keys[0].column].source != .agg) return null;
    const rank_call = calls[outputs[order.keys[0].column].source.agg];
    if (rank_call.func != .count or rank_call.arg != null) return null;
    const limit = order.limit orelse return null;
    const k = std.math.cast(usize, limit +| order.offset) orelse return null;
    if (limit == 0 or k == 0 or k > 1024 or work.len == 0) return null;
    const input_rows = RecordOrdinals.rows(metas) orelse return null;
    if (input_rows >= @as(usize, 1) << 31) return null;
    var rows: usize = 0;
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        rows = std.math.add(usize, rows, std.math.cast(usize, rg.num_rows) orelse return null) catch return null;
        for (keys) |key| {
            if (key != .col_ref) return null;
            const cm = rg.columns.items[key.col_ref.col_idx].meta_data orelse return null;
            if (metas[item.file].getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        }
    }
    if (rows < args.integer_records_min_rows) return null;
    var identity = (try ownerIdentity(gpa, keys, domain)) orelse return null;
    defer identity.deinit(gpa);
    const workers = @min(requested_workers, owner_groups.max_workers);
    if (program.len != 0) {
        // The retained domain supplies dictionary identities, not payload or ordinal validity. A checked raw
        // identity uses the same selected sink and whole-attempt replay, including speculative range mismatch.
        if (!identity.fitsBits(u64)) {
            var narrow = (try pairRawIdentity(gpa, arena, metas, keys, args.scan_all)) orelse return null;
            if (!narrow.fitsBits(u64)) {
                narrow.deinit(gpa);
                return null;
            }
            identity.deinit(gpa);
            identity = narrow;
        }
        // OwnerProgram sorts its four slots by kind, retaining distinct columns of the same kind.
        @setEvalBranchQuota(10000);
        inline for (0..5) |sums| inline for (0..5 - sums) |mins|
            inline for (0..5 - sums - mins) |maxes| inline for (0..5 - sums - mins - maxes) |counts| {
                const kinds = comptime blk: {
                    var values: [sums + mins + maxes + counts]owner_groups.Lane = undefined;
                    var at: usize = 0;
                    for (std.enums.values(owner_groups.Lane), [4]usize{ sums, mins, maxes, counts }) |kind, n| {
                        for (0..n) |_| {
                            values[at] = kind;
                            at += 1;
                        }
                    }
                    break :blk values;
                };
                if (comptime kinds.len == 0) continue;
                if (program.matches(&kinds)) return selectedCountBits(
                    u64,
                    &kinds,
                    gpa,
                    arena,
                    args,
                    metas,
                    plan,
                    work,
                    calls,
                    keys,
                    outputs,
                    &identity,
                    if (domain) |d| d.bytes else 0,
                    program,
                    stat,
                    pass_share,
                    input_rows,
                    rows,
                    workers,
                    k,
                    timings,
                    explain_only,
                );
            };
        return null;
    }
    switch (identity.kind()) {
        .dense_u32, .packed_u64 => return selectedCountBits(
            u64,
            &.{},
            gpa,
            arena,
            args,
            metas,
            plan,
            work,
            calls,
            keys,
            outputs,
            &identity,
            if (domain) |d| d.bytes else 0,
            program,
            stat,
            pass_share,
            input_rows,
            rows,
            workers,
            k,
            timings,
            explain_only,
        ),
        .packed_u128 => return selectedCountBits(
            u128,
            &.{},
            gpa,
            arena,
            args,
            metas,
            plan,
            work,
            calls,
            keys,
            outputs,
            &identity,
            if (domain) |d| d.bytes else 0,
            program,
            stat,
            pass_share,
            input_rows,
            rows,
            workers,
            k,
            timings,
            explain_only,
        ),
        .decline => return null,
    }
}

fn selectedCountBits(
    comptime Bits: type,
    comptime kinds: []const owner_groups.Lane,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    identity: *const key_pack.Plan,
    domain_credit: usize,
    program: agg_plan.OwnerProgram,
    stat: ?NdvStat,
    pass_share: f64,
    input_rows: usize,
    rows: usize,
    workers: usize,
    k: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const O = owner_groups.Packed(Bits, kinds, u32);
    const S = owner_groups.SelectedOwner(Bits, kinds, u32);
    const T = @import("../owner_result.zig").Top(O.Group, bool, struct {
        fn worse(desc: bool, x: O.Group, y: O.Group) std.math.Order {
            if (x.weight != y.weight) return if (desc)
                std.math.order(x.weight, y.weight)
            else
                std.math.order(y.weight, x.weight);
            return std.math.order(y.first, x.first);
        }
    }.worse);
    var key_bytes: usize = 0;
    if (identity.domain) |d| for (outputs) |out| if (out.source == .key)
        switch (d.axes[out.source.key].values) {
            .bytes => |values| {
                var widest: usize = 0;
                for (values) |v| widest = @max(widest, v.len);
                key_bytes +|= std.mem.alignForward(usize, widest, 16) +| 16;
            },
            else => {},
        };
    const names_credit = outputNamesBytes(outputs);
    const per_row = @sizeOf([]const AggValue) +| outputs.len *| @sizeOf(AggValue) +| key_bytes;
    const output_credit = names_credit +| k *| per_row;
    const pack_rows = owner_groups.batchRows(null);
    const scratch_credit = workers *| pack_rows *| @sizeOf(Bits) +| workers *| (keys.len *| @sizeOf(key_pack.Plan.Binding) +|
        @sizeOf(S) +| @sizeOf(consumer.RecordSink) +| @sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable)) +|
        work.len *| (@sizeOf(S.Source) +| @sizeOf(u32)) +| identity.retainedBytes() +| domain_credit +|
        k *| @sizeOf(MergedSlot) +| keys.len *| @sizeOf(KeyType) +| outputs.len *| @sizeOf(GroupColType);
    const result_credit = T.bytes(workers, k) +| scratch_credit +| output_credit;
    const selected: usize = @intFromFloat(@ceil(@as(f64, @floatFromInt(rows)) * std.math.clamp(pass_share, 0, 1)));
    var q = @import("../owner_cost.zig").quote(
        Bits,
        kinds,
        u32,
        rows,
        selected,
        workers,
        result_credit,
        args.byte_top_partitions,
    ) orelse return null;
    var compact = false;
    if (program.commonColumn()) if (@import("../owner_cost.zig").quoteStorage(
        Bits,
        kinds,
        u32,
        rows,
        selected,
        workers,
        result_credit,
        args.byte_top_partitions,
        true,
    )) |small| if (small.peak < q.peak) {
        q = small;
        compact = true;
    };
    if (q.peak > args.max_memory) return null;
    if (stat) |s| if (args.plain_rules and s.confidence == .upper_bound and
        q.work > selectedGroupBytes(s, calls.len, workers, pass_share)) return null;
    if (explain_only) return .{ .layout = .packed_count_top_h, .reservation = q.peak, .partitions = q.parts };
    var private_ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &private_ledger;
    const extra_credit = scratch_credit +| output_credit;
    // No input has been consumed: setup funding refusal leaves the ordinary grouping plan available.
    _ = ledger.drawExact(extra_credit) orelse return null;
    var held: usize = 0;
    defer ledger.release(extra_credit - held);
    const state = (try O.State.initStorage(gpa, ledger, input_rows, rows, workers, q.parts, compact)) orelse
        return null;
    var leave_records = false;
    defer if (!leave_records) state.deinit();
    var top = T.init(gpa, ledger, workers, k, args.result_order.keys[0].descending) catch |err| {
        if (err == error.ExceededMemoryBudget) return null;
        return err;
    };
    defer top.deinit();
    var temporary = std.heap.ArenaAllocator.init(gpa);
    defer temporary.deinit();
    const scratch = temporary.allocator();
    const starts = try scratch.alloc(u32, work.len);
    if (!RecordOrdinals.fill(metas, work, starts)) return error.BadColumn;
    const sources = try scratch.alloc(S.Source, work.len);
    for (sources, work) |*s, item| s.* = .{ .file = item.file, .rg = item.rg };
    const adapters = try scratch.alloc(S, workers);
    const sinks = try scratch.alloc(consumer.RecordSink, workers);
    var refused: std.atomic.Value(bool) = .init(false);
    for (adapters, sinks, state.locals) |*adapter, *sink, *local| {
        adapter.* = .{
            .local = local,
            .identity = identity,
            .program = program,
            .sources = sources,
            .bindings = try scratch.alloc(key_pack.Plan.Binding, keys.len),
            .pack_buffer = try scratch.alloc(Bits, pack_rows),
            .refused = &refused,
        };
        sink.* = .{ .selected = .{ .ctx = adapter, .startFn = S.startErased, .batchFn = S.batchErased } };
    }
    if (args.taken) |taken| taken.insert(.integer_records);
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
        if (refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const prof = profile.threadOf(args.profiler, .main);
    const begin = nowMonoNs();
    try state.finish(top.receivers, T.Receiver.receive);
    const fold_end = nowMonoNs();
    profile.record(prof, .integer_fold, begin, fold_end, profile.args(.{}));
    const groups = top.finish();
    profile.record(prof, .integer_order, fold_end, nowMonoNs(), profile.args(.{}));
    const slots = try scratch.alloc(MergedSlot, groups.len);
    for (groups, slots, 0..) |g, *slot, i| slot.* = .{ .key = &.{}, .first = g.first, .idx = i };
    const View = struct {
        groups: []const O.Group,
        program: agg_plan.OwnerProgram,
        calls: []const expr_agg.AggCall,
        fn key(ctx: *const anyopaque, i: usize) u128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return v.groups[i].key;
        }
        fn integer(ctx: *const anyopaque, i: usize, ai: usize) anyerror!?i128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return v.program.integer(v.groups[i], v.calls[ai], null);
        }
        fn value(ctx: *const anyopaque, a: std.mem.Allocator, i: usize, ai: usize) anyerror!AggValue {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return materializeOne(a, v.calls[ai], try v.program.accumulator(v.groups[i], v.calls[ai]));
        }
    };
    const view: View = .{ .groups = groups, .program = program, .calls = calls };
    const key_types = try scratch.alloc(KeyType, keys.len);
    for (keys, key_types) |key, *ty| ty.* = keyTypeFromExpr(key);
    const row_source: RowSource = .{
        .slots = slots,
        .outputs = outputs,
        .calls = calls,
        .key_types = key_types,
        .key_exprs = keys,
        .accs = &.{},
        .folded = false,
        .fixed_keys = false,
        .identity = identity,
        .owner = .{ .ctx = &view, .key = View.key, .integer = View.integer, .value = View.value },
    };
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const types = try arena.alloc(GroupColType, outputs.len);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    for (outputs, names, types) |out, *name, *ty| {
        name.* = try gpa.dupe(u8, out.name);
        ty.* = .{
            .lane = if (out.source == .key and key_types[out.source.key] == .string) .string else if (out.source == .agg and calls[out.source.agg].func == .avg) .f64 else .i64,
            .nullable = out.source == .key or calls[out.source.agg].func != .count,
            .source = if (out.source == .key)
                leafSchemaElem(&metas[0], keys[out.source.key].col_ref.col_idx)
            else
                passThroughSource(calls[out.source.agg], &metas[0]),
        };
    }
    const out_rows = try gpa.alloc([]const AggValue, groups.len);
    @memset(out_rows, &.{});
    output.group_rows = out_rows;
    try buildGroupRows(gpa, &output.slab, row_source, out_rows, workers);
    // FailureProbe borrows Worker metadata through canonical diagnostics in prepareAggregate. Keep that query
    // arena credit beside output schema/rows; the temporary bindings, sources and finish views are gone.
    if (args.ledger != null) held = workers *| (@sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable)) +|
        outputs.len *| @sizeOf(GroupColType) +| names_credit +| groups.len *| per_row;
    // As for leave_tables, a successful tiny answer in an exiting process leaves consumed records to exit.
    // Their full ledger credit stays held beside encoding; refusals and reusable/library contexts free normally.
    leave_records = kinds.len != 0 and args.leave_regions and !std.debug.runtime_safety;
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

/// Retained value-equal axes form a complete tuple identity inside the existing integer COUNT owner.
pub fn inlinePackedAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    domain: *const agg_dense.Domain,
    requested_workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const n_workers = @min(requested_workers, owner_groups.max_workers);
    const order = args.result_order;
    if (!args.integer_records or !args.encoded or !allRescannable(args.inputs) or
        calls.len != 1 or
        order.having != null or order.hidden.len != 0 or order.keys.len != 1 or !order.keys[0].descending)
        return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    if (keys.len < 2 or keys.len != domain.axes.len or domain.packedIds() == null) return null;
    const call = calls[0];
    if (call.func != .count or call.arg != null or call.where != null or call.offset != 0 or call.state or
        call.result != .i64) return null;
    for (outputs) |out| if (out.derived != null) return null;
    const ranked = outputs[order.keys[0].column].source;
    if (ranked != .agg or ranked.agg != 0) return null;
    const limit = order.limit orelse return null;
    const k = std.math.cast(usize, order.offset +| limit) orelse return null;
    if (limit == 0 or k == 0 or k > 1024) return null;
    var rows: usize = 0;
    for (work) |item| {
        const n = std.math.cast(usize, metas[item.file].row_groups.items[item.rg].num_rows) orelse return null;
        rows = std.math.add(usize, rows, n) catch return null;
    }
    if (rows == 0 or rows < args.integer_records_min_rows or !owner_groups.admitsRows(.int64, rows)) return null;
    const Owner = owner_groups.IntCountTop;
    const parts = Owner.partitionCount(rows, n_workers, args.decode_batch_rows, args.byte_top_partitions);
    var key_credit: usize = 0;
    for (outputs) |out| if (out.source == .key) switch (domain.axes[out.source.key].values) {
        .bytes => |values| {
            var widest: usize = 0;
            for (values) |value| widest = @max(widest, value.len);
            key_credit +|= widest;
        },
        else => {},
    };
    const names_credit = outputNamesBytes(outputs);
    const out_credit = names_credit +| k *|
        (@sizeOf([]const AggValue) +| outputs.len *| @sizeOf(AggValue) +| key_credit);
    const scratch = n_workers *| (keys.len *| @sizeOf(agg_dense.CountAxis) +| @sizeOf(consumer.PackedCount)) +|
        work.len *| (@sizeOf(consumer.PackedCount.Source) +| @sizeOf(u32));
    const credit = domain.bytes +| scratch +| out_credit;
    const quote = Owner.State.reservation(rows, n_workers, parts, k, credit);
    if (quote > args.max_memory) return null;
    if (explain_only) return .{ .layout = .integer_count_top_h, .reservation = quote };
    const state = (try Owner.State.init(
        gpa,
        args.ledger,
        rows,
        n_workers,
        keys[0].col_ref.col_idx,
        0,
        .{ .k = k, .descending = true },
        parts,
        credit,
    )) orelse return null;
    defer state.deinit();
    const sources = try arena.alloc(consumer.PackedCount.Source, work.len);
    const starts = try arena.alloc(u32, work.len);
    var at: usize = 0;
    for (sources, starts, work) |*source, *start, item| {
        source.* = .{ .file = item.file, .rg = item.rg };
        start.* = @intCast(at);
        at += @intCast(metas[item.file].row_groups.items[item.rg].num_rows);
    }
    var refused: std.atomic.Value(bool) = .init(false);
    const sinks = try arena.alloc(consumer.RecordSink, n_workers);
    const packings = try arena.alloc(consumer.PackedCount, n_workers);
    for (sinks, packings, state.locals) |*sink, *packing, *local| {
        packing.* = .{
            .domain = domain,
            .keys = keys,
            .sources = sources,
            .bindings = try arena.alloc(agg_dense.CountAxis, keys.len),
            .refused = &refused,
        };
        sink.* = .{ .integer_count_top = .{ .local = local, .packing = packing } };
    }
    if (args.taken) |taken| taken.insert(.integer_records);
    const run = try runRecordWorkers(
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
    );
    // A late PLAIN/mismatched page leaves only private records. Joined workers have released their source pages;
    // the established narrowing chain disables this attempt and rewinds stable mappings before exact fallback.
    if (refused.load(.monotonic)) return error.ExceededMemoryBudget;
    const prof = profile.threadOf(args.profiler, .main);
    const finish_start = nowMonoNs();
    try state.finishSelected(@intCast(run.rows_matched));
    const scatter_end = finish_start + @as(i64, @intCast(state.partition_ns));
    const fold_end = scatter_end + @as(i64, @intCast(state.fold_ns));
    profile.record(prof, .integer_scatter, finish_start, scatter_end, profile.args(.{}));
    profile.record(prof, .integer_fold, scatter_end, fold_end, profile.args(.{}));
    profile.record(prof, .integer_order, fold_end, nowMonoNs(), profile.args(.{}));
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, names, types) |out, *name, *ty| {
        name.* = try gpa.dupe(u8, out.name);
        ty.* = switch (out.source) {
            .key => |ki| .{
                .lane = if (keys[ki].col_ref.expr_type == .str) .string else .i64,
                .nullable = true,
                .source = leafSchemaElem(&metas[0], keys[ki].col_ref.col_idx),
            },
            .agg => .{ .lane = .i64, .nullable = false },
        };
    }
    const kept = state.top();
    const out_rows = try gpa.alloc([]const AggValue, kept.len);
    @memset(out_rows, &.{});
    output.group_rows = out_rows;
    const slab = try gpa.alloc(AggValue, kept.len * outputs.len);
    @memset(slab, .null_val);
    output.slab = slab;
    const indices = try arena.alloc(usize, keys.len);
    var held = names_credit + kept.len * (@sizeOf([]const AggValue) + outputs.len * @sizeOf(AggValue));
    for (kept, out_rows, 0..) |group, *row, ri| {
        var rem: u64 = group.key;
        for (domain.axes, indices) |*axis, *index| {
            index.* = @intCast(rem % axis.ids());
            rem /= axis.ids();
        }
        row.* = slab[ri * outputs.len ..][0..outputs.len];
        for (outputs, slab[ri * outputs.len ..][0..outputs.len]) |out, *cell| switch (out.source) {
            .agg => cell.* = .{ .i = group.countValue() },
            .key => |ki| {
                const axis = &domain.axes[ki];
                const id = indices[ki];
                if (id == axis.len()) continue;
                cell.* = switch (axis.values) {
                    .bytes => |values| b: {
                        const copy = try gpa.dupe(u8, values[id]);
                        held += copy.len;
                        break :b .{ .s = copy };
                    },
                    .int32 => |values| .{ .i = if (keys[ki].col_ref.unsigned_32)
                        @as(u32, @truncate(values[id]))
                    else
                        @as(i64, @bitCast(values[id])) },
                    .int64 => |values| .{ .i = if (keys[ki].col_ref.unsigned_64)
                        @as(i128, values[id])
                    else
                        @as(i64, @bitCast(values[id])) },
                };
            },
        };
    }
    if (args.ledger != null) state.charged -= @min(state.charged, held);
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

