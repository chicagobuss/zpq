const std = @import("std");
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const ndv = @import("../ndv.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const key_pack = @import("../expr/key_pack.zig");
const owner_groups = @import("../owner_groups.zig");
const grouped_bitmap = @import("../grouped_distinct_bitmap.zig");
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const key_decode = @import("key_decode.zig");
const KeyType = key_decode.KeyType;
const keyTypeFromExpr = key_decode.keyTypeFromExpr;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const CountCut = scan_count_cut.CountCut;
const countCut = scan_count_cut.countCut;
const scan_result = @import("result.zig");
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const scan_binding = @import("binding.zig");
const leafSchemaElem = scan_binding.leafSchemaElem;
const scan_rows = @import("rows.zig");
const RowSource = scan_rows.RowSource;
const buildGroupRows = scan_rows.buildGroupRows;
const answerBytesParallel = scan_rows.answerBytesParallel;
const buildGroupColumns = scan_rows.buildGroupColumns;
const slot_sort = @import("slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
const record_workers = @import("record_workers.zig");
const runRecordWorkers = record_workers.runRecordWorkers;
const pairResult = record_workers.pairResult;
const owner_support = @import("owner_support.zig");
const IntegerRangeHint = owner_support.IntegerRangeHint;
const pairRawIdentity = owner_support.pairRawIdentity;
const outputNamesBytes = owner_support.outputNamesBytes;
const owner_integer = @import("owner_integer.zig");
const inlineIntegerAnswer = owner_integer.inlineIntegerAnswer;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const RecordOrdinals = scan_types.RecordOrdinals;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const AggOutputItem = output_values.AggOutputItem;

const BitmapSink = struct {
    const Source = struct { file: usize, rg: usize };
    state: *grouped_bitmap.State,
    worker: usize,
    domain: *const agg_dense.Domain,
    keys: []const expr_ast.Expr,
    sources: []const Source,
    value_column: usize,
    item: ?usize = null,
    refused: *std.atomic.Value(bool),

    fn startErased(ctx: *anyopaque, _: u64, item: usize) void {
        const s: *BitmapSink = @ptrCast(@alignCast(ctx));
        s.item = item;
    }

    fn batchErased(ctx: *anyopaque, source: consumer.SelectedBatch, lookup: []const ?usize) anyerror!void {
        const s: *BitmapSink = @ptrCast(@alignCast(ctx));
        s.batch(source, lookup) catch |e| {
            if (e != error.BitmapDomainDeclined) return e;
            s.refused.store(true, .monotonic);
            return error.ExceededMemoryBudget;
        };
    }

    fn batch(s: *BitmapSink, source: consumer.SelectedBatch, lookup: []const ?usize) !void {
        try source.validate();
        const item = s.item orelse return error.BadColumn;
        if (item >= s.sources.len or s.value_column >= lookup.len) return error.BadColumn;
        const at = s.sources[item];
        var bindings: [1]agg_dense.CountAxis = undefined;
        if (!try s.domain.bind(s.keys, at.file, at.rg, source.batch, lookup, &bindings))
            return error.BitmapDomainDeclined;
        const pos = lookup[s.value_column] orelse return error.BadColumn;
        if (pos >= source.batch.cols.len) return error.BadColumn;
        const col = switch (source.batch.cols[pos]) {
            .i64 => |c| c,
            else => return error.BitmapDomainDeclined,
        };
        if (col.levels_only_rows != 0) return error.BitmapDomainDeclined;
        if (col.max_rep != 0 or col.rowCount() < source.batch.num_rows) return error.BadColumn;
        if (col.def_levels) |dl| if (dl.len < source.batch.num_rows) return error.BadColumn;
        for (0..source.batch.num_rows) |row| {
            if (source.selected) |selected| if (!selected.isActive(row)) continue;
            const group = try agg_dense.productId(&bindings, row, true);
            const is_null = if (col.def_levels) |dl| dl[row] < col.max_def else false;
            const value: ?i64 = if (is_null) null else if (col.codes) |codes| blk: {
                const code = codes[row];
                if (code >= col.dict.len) return error.BadColumn;
                break :blk col.dict[code];
            } else col.values[row];
            const index = try s.state.geometry.index(value);
            const input_row: u64 = if (source.input_rows) |rows| rows[row] else @intCast(row);
            const ordinal = std.math.cast(u32, try std.math.add(u64, source.input_base, input_row)) orelse
                return error.BadColumn;
            try s.state.add(s.worker, @intCast(group), index, ordinal);
        }
    }
};

fn inlineBitmapAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    requested: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    if (args.grouped_distinct_bitmap == .off or !args.integer_records or !args.encoded or !args.ndv_union or
        args.distinct_pairs != .auto or !allRescannable(args.inputs) or work.len == 0 or calls.len != 1) return null;
    const call = calls[0];
    if (call.func != .count_distinct or call.arg == null or call.arg.? != .col_ref or call.where != null or
        call.state or call.offset != 0 or call.result != .i64) return null;
    const x = call.arg.?.col_ref;
    if (x.physical_type != .INT64 or x.expr_type != .i64 or x.unsigned_64) return null;
    const keys = keys_opt orelse return null;
    if (keys.len != 1 or keys[0] != .col_ref or keys[0].col_ref.expr_type != .i64) return null;
    const key = keys[0].col_ref;
    if (key.physical_type != .INT32 and key.physical_type != .INT64) return null;
    const outputs = outputs_opt orelse return null;
    for (outputs) |out| if (out.derived != null) return null;
    const input_rows = RecordOrdinals.rows(metas) orelse return null;
    if (input_rows >= @as(usize, 1) << 31) return null;
    const hw = @import("../hw.zig");
    const l2 = hw.l2BytesPerWorkerOr(hw.default_l2_bytes_per_worker);
    if (args.grouped_distinct_bitmap == .auto) {
        // Reject expensive discovery from footers alone: probing hundreds of cold headers can delay a decline.
        var dictionary_bytes: u64 = 0;
        for (metas) |meta| for (meta.row_groups.items) |rg| {
            if (rg.num_rows == 0) continue;
            if (key.col_idx >= rg.columns.items.len) return null;
            const cm = rg.columns.items[key.col_idx].meta_data orelse return null;
            if (!ndv.fullCoverage(&cm)) return null;
            const start = cm.dictionary_page_offset orelse return null;
            if (start < 0 or cm.data_page_offset <= start) return null;
            dictionary_bytes +|= @intCast(cm.data_page_offset - start);
            if (dictionary_bytes > l2) return null;
        };
    }
    var rows: usize = 0;
    var range: IntegerRangeHint = .{};
    for (work) |item| {
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const cm = rg.columns.items[x.col_idx].meta_data orelse return null;
        if (meta.getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        rows +|= @intCast(rg.num_rows);
        range.include(rg, meta, x.col_idx);
    }
    if (rows == 0 or !range.known or range.lo > range.hi or
        (args.grouped_distinct_bitmap == .auto and rows < args.integer_records_min_rows)) return null;
    const workers = @max(1, @min(requested, work.len));
    // Where no L3 is known, the workers' L2 shares together stand for the last level: on Apple Silicon, whose
    // sysctl reports none, a cluster's shared L2 is that level.
    const known_l3 = hw.l3BytesPerActiveWorker(workers) *| workers;
    const l3 = if (known_l3 != 0) known_l3 else l2 *| workers;
    const minimum = grouped_bitmap.Geometry.plan(1, range.lo, range.hi, workers, l2, l3, args.grouped_distinct_bitmap) orelse return null;
    var private_ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &private_ledger;
    var charged: core_memory.Charged = .{ .child = gpa, .budget = ledger };
    var armed = false;
    return bitmapAnswer(gpa, arena, charged.allocator(), ledger, args, metas, plan, work, calls, keys, outputs, rows, workers, l2, l3, minimum, timings, &armed, explain_only) catch |e| {
        if (e == error.OutOfMemory and charged.refused.load(.monotonic)) {
            if (armed) return error.ExceededMemoryBudget;
            return null;
        }
        return e;
    };
}

fn bitmapAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    charged: std.mem.Allocator,
    ledger: *core_memory.SharedBudget,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    rows: usize,
    workers: usize,
    l2: usize,
    l3: usize,
    minimum: grouped_bitmap.Geometry,
    timings: Timings,
    armed: *bool,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    var temporary = std.heap.ArenaAllocator.init(charged);
    defer temporary.deinit();
    const a = temporary.allocator();
    const files = try a.alloc(ndv.File, args.inputs.len);
    for (files, args.inputs) |*f, in| f.* = .{
        .bytes = in.plan_bytes orelse in.bytes,
        .local = !std.mem.startsWith(u8, in.name, "s3://"),
    };
    const row_bytes = minimum.words * @sizeOf(u64);
    // Header bounds reject expensive identity probes; they are only a cost gate, never group presence evidence.
    const stat = try ndv.collect(charged, a, files, metas, keys, workers, ledger.available(), false);
    if (args.grouped_distinct_bitmap == .auto and (stat.entries == null or stat.covered_rows != stat.rows or
        stat.dictionary_bytes > l2 or stat.lo *| row_bytes > l3 / 2)) return null;
    const identity_limit = @min(rows + 1, (if (args.grouped_distinct_bitmap == .auto) l3 / 2 else ledger.available()) / row_bytes);
    var domain = (try ndv.retainIdentity(charged, a, files, metas, keys, workers, ledger.available(), identity_limit)) orelse return null;
    defer domain.deinit(charged);
    const geometry = grouped_bitmap.Geometry.plan(domain.ids(), minimum.lo, @intCast(@as(i128, minimum.lo) + minimum.width - 1), workers, l2, l3, args.grouped_distinct_bitmap) orelse return null;
    const state_bytes = geometry.bytes() orelse return null;
    if (state_bytes > ledger.available()) return null;
    if (explain_only) return .{
        .layout = .grouped_distinct_bitmap,
        .reservation = state_bytes +| domain.bytes,
        .partitions = geometry.parts,
    };
    var identity = (try key_pack.Plan.init(charged, keys, &domain)) orelse return null;
    defer identity.deinit(charged);
    if (identity.encoding.source != .dictionary) return null;
    var state = try grouped_bitmap.State.init(charged, geometry);
    defer state.deinit();
    const starts = try a.alloc(u32, work.len);
    if (!RecordOrdinals.fill(metas, work, starts)) return error.BadColumn;
    const sources = try a.alloc(BitmapSink.Source, work.len);
    for (sources, work) |*s, item| s.* = .{ .file = item.file, .rg = item.rg };
    const adapters = try a.alloc(BitmapSink, workers);
    const sinks = try a.alloc(consumer.RecordSink, workers);
    var refused: std.atomic.Value(bool) = .init(false);
    for (adapters, sinks, 0..) |*s, *sink, i| {
        s.* = .{ .state = &state, .worker = i, .domain = &domain, .keys = keys, .sources = sources, .value_column = calls[0].arg.?.col_ref.col_idx, .refused = &refused };
        sink.* = .{ .selected = .{ .ctx = s, .startFn = BitmapSink.startErased, .batchFn = BitmapSink.batchErased } };
    }
    // FailureProbe retains worker diagnostics until prepareAggregate has replayed its canonical error prefix.
    var worker_credit = workers *| (@sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable));
    _ = ledger.drawExact(worker_credit) orelse return null;
    defer ledger.release(worker_credit);
    armed.* = true;
    if (args.taken) |taken| taken.insert(.grouped_distinct_bitmap);
    var scan_args = args;
    scan_args.ledger = ledger;
    if (scan_args.decode_batch_rows == 0) scan_args.decode_batch_rows = owner_groups.batchRows(null);
    const run = runRecordWorkers(gpa, arena, scan_args, metas, plan, work, calls, keys, sinks, starts, outputs, timings) catch |e| {
        if (refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const span = profile.Span.begin(profile.threadOf(args.profiler, .main));
    state.finish(WORKER_STACK_SIZE);
    span.end(.merge, profile.args(.{geometry.parts}));
    var n: usize = 0;
    for (state.first[0..geometry.groups]) |first| if (first != grouped_bitmap.none) {
        n += 1;
    };
    const slots = try a.alloc(MergedSlot, n);
    var at: usize = 0;
    for (state.first[0..geometry.groups], 0..) |first, id| if (first != grouped_bitmap.none) {
        slots[at] = .{ .key = &.{}, .first = first, .idx = id };
        at += 1;
    };
    if (!args.unordered) try sortSlotsByFirst(a, slots, workers);
    const View = struct {
        fn key(_: *const anyopaque, i: usize) u128 {
            return i;
        }
        fn integer(ctx: *const anyopaque, i: usize, _: usize) anyerror!?i128 {
            const s: *const grouped_bitmap.State = @ptrCast(@alignCast(ctx));
            return s.counts[i];
        }
        fn value(ctx: *const anyopaque, _: std.mem.Allocator, i: usize, call: usize) anyerror!AggValue {
            return .{ .i = (try integer(ctx, i, call)).? };
        }
    };
    const types = try arena.alloc(GroupColType, outputs.len);
    const key_types = try a.alloc(KeyType, keys.len);
    for (keys, key_types) |key, *ty| ty.* = keyTypeFromExpr(key);
    for (outputs, types) |out, *ty| ty.* = .{
        .lane = .i64,
        .nullable = true,
        .source = if (out.source == .key) leafSchemaElem(&metas[0], keys[out.source.key].col_ref.col_idx) else null,
    };
    const row_source: RowSource = .{ .slots = slots, .outputs = outputs, .calls = calls, .key_types = key_types, .key_exprs = keys, .accs = &.{}, .folded = false, .fixed_keys = false, .identity = &identity, .owner = .{ .ctx = &state, .key = View.key, .integer = View.integer, .value = View.value } };
    const typed = args.typed_answer and args.result_order.keys.len == 0 and args.result_order.limit == null and
        args.result_order.offset == 0 and args.result_order.having == null and args.result_order.hidden.len == 0;
    var output_charge = answerBytesParallel(slots, types, key_types, typed, workers) +| outputNamesBytes(outputs);
    _ = ledger.drawExact(output_charge) orelse return error.ExceededMemoryBudget;
    defer ledger.release(output_charge);
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    for (outputs, names) |out, *name| name.* = try gpa.dupe(u8, out.name);
    if (typed) {
        output.typed = .{ .rows = 0, .columns = &.{} };
        try buildGroupColumns(gpa, &output.typed.?, row_source, types, workers);
    } else {
        const out_rows = try gpa.alloc([]const AggValue, n);
        @memset(out_rows, &.{});
        output.group_rows = out_rows;
        try buildGroupRows(gpa, &output.slab, row_source, out_rows, workers);
    }
    for (work) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
    if (args.ledger != null) {
        output_charge = 0;
        worker_credit = 0;
    }
    var result = pairResult(args, plan, run, output, .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types });
    result.distinct_pairs = false;
    result.grouped_distinct_bitmap = true;
    return result;
}

/// Pair counts and plain integer aggregates share group identities, not DISTINCT duplicate weights.
pub fn inlinePairAnswer(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    requested: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    if (try inlineBitmapAnswer(gpa, arena, args, metas, plan, work, calls, keys_opt, outputs_opt, requested, timings, explain_only)) |answer| return answer;
    if (!args.integer_records or args.distinct_pairs == .off or !allRescannable(args.inputs) or work.len == 0)
        return null;
    var distinct: ?usize = null;
    var companions = false;
    for (calls, 0..) |call, i| {
        if (call.where != null or call.state or call.offset != 0) return null;
        if (call.func == .count_distinct) {
            if (distinct != null or call.arg == null or call.arg.? != .col_ref or call.result != .i64) return null;
            distinct = i;
        } else if (call.func != .count or call.arg != null) {
            companions = true;
        } else if (call.result != .i64) return null;
    }
    if (companions and (agg_plan.OwnerProgram.init(calls) == null or !args.grouped() or
        args.result_order.having != null or args.result_order.hidden.len != 0)) return null;
    const di = distinct orelse return null;
    const keys = keys_opt orelse &.{};
    const outputs = outputs_opt orelse &.{};
    const candidate_cut = if (keys_opt != null) countCut(outputs, calls, args.result_order) else null;
    const top = if (candidate_cut) |cut|
        (if (cut.a_idx == di or calls[cut.a_idx].arg == null) candidate_cut else null)
    else
        null;
    // A full-output companion pass would replicate every group's states in every worker.
    if (companions and (top == null or top.?.k == 0)) return null;
    if (args.distinct_pairs == .auto and keys.len == 0) return null;
    // Single-key owners use a checked raw certificate, so dictionary fallback pages cannot force a replay.
    // String identities keep the existing sets/sketch choice; forced owners still exercise retained dictionaries.
    const automatic_single = args.distinct_pairs == .auto and keys.len == 1;
    for (outputs) |out| if (out.derived != null) return null;
    const input_rows = RecordOrdinals.rows(metas) orelse return null;
    if (input_rows >= @as(usize, 1) << 31) return null;
    var rows: usize = 0;
    for (work) |item| rows +|= @intCast(metas[item.file].row_groups.items[item.rg].num_rows);
    if (rows == 0 or (rows < args.integer_records_min_rows and args.distinct_pairs != .on)) return null;
    const axes = try arena.alloc(expr_ast.Expr, keys.len + 1);
    @memcpy(axes[0..keys.len], keys);
    axes[keys.len] = calls[di].arg.?;
    for (axes) |axis| {
        if (axis != .col_ref) return null;
        const c = axis.col_ref;
        if (c.expr_type != .i64 and c.expr_type != .str) return null;
        for (work) |item| {
            const cm = metas[item.file].row_groups.items[item.rg].columns.items[c.col_idx].meta_data orelse return null;
            if (metas[item.file].getColumnLevels(cm.path_in_schema.items).max_rep != 0) return null;
        }
    }
    // Rescannable readers retain a complete mapping for planning/replay. Union only dictionary pages from that
    // mapping; it never borrows a released remote window or depends on the streaming reader's current position.
    const files = try arena.alloc(ndv.File, args.inputs.len);
    for (args.inputs, files) |input, *file| {
        const bytes = input.plan_bytes orelse input.bytes;
        if (input.logical_size != 0 and input.logical_size != bytes.len) return null;
        file.* = .{ .bytes = bytes, .local = !std.mem.startsWith(u8, input.name, "s3://") };
    }
    var domain: ?agg_dense.Domain = null;
    defer if (domain) |*d| d.deinit(gpa);
    var raw = if (automatic_single) try pairRawIdentity(gpa, arena, metas, axes, args.scan_all) else null;
    errdefer if (raw) |*p| p.deinit(gpa);
    if (automatic_single and raw == null) return null;
    const use_siblings = if (std.c.getenv("ZPQ_CD_SMT")) |v| std.mem.eql(u8, std.mem.span(v), "1") else false;
    const owner_workers = if (use_siblings or raw == null) requested else @import("../hw.zig").independentOwnerWorkers(requested);
    const workers = @min(owner_workers, owner_groups.max_workers);
    // The retained dictionary identity cannot bind a chunk containing non-dictionary data. Decline known
    // fallback before building its union; missing encoding_stats remain speculative, and forced owners replay.
    if (raw == null and args.distinct_pairs == .auto) for (axes) |axis| for (work) |item| {
        const cm = metas[item.file].row_groups.items[item.rg].columns.items[axis.col_ref.col_idx].meta_data.?;
        if (cm.encoding_stats) |stats| for (stats.items) |e| {
            if (e.count <= 0 or (e.page_type != @backingInt(schema.PageType.DATA_PAGE) and
                e.page_type != @backingInt(schema.PageType.DATA_PAGE_V2))) continue;
            if (e.encoding != @backingInt(schema.Encoding.PLAIN_DICTIONARY) and
                e.encoding != @backingInt(schema.Encoding.RLE_DICTIONARY)) return null;
        };
    };
    if (raw == null and args.encoded and args.ndv_union)
        domain = try ndv.retainIdentity(gpa, arena, files, metas, axes, workers, args.max_memory, input_rows + 1);
    var identity = if (raw) |p| p else (try key_pack.Plan.init(gpa, axes, if (domain) |*d| d else null)) orelse return null;
    raw = null;
    defer identity.deinit(gpa);
    if (identity.fitsBits(u64)) {
        if (top) |cut| if (cut.k > 0) return pairAnswerBits(
            u64,
            true,
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
            di,
            rows,
            input_rows,
            workers,
            cut,
            timings,
            explain_only,
        );
        return pairAnswerBits(
            u64,
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
            &identity,
            di,
            rows,
            input_rows,
            workers,
            null,
            timings,
            explain_only,
        );
    }
    if (top) |cut| if (cut.k > 0) return pairAnswerBits(
        u128,
        true,
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
        di,
        rows,
        input_rows,
        workers,
        cut,
        timings,
        explain_only,
    );
    return pairAnswerBits(
        u128,
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
        &identity,
        di,
        rows,
        input_rows,
        workers,
        null,
        timings,
        explain_only,
    );
}

/// A covered local key dictionary bounds the number of per-RG sets. A compact argument range makes the
/// existing sets/bitmaps a credible alternative; unknown or wide domains keep the pair owner's admission.
fn pairLocalSetWork(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    distinct: usize,
    rows: usize,
    workers: usize,
) !?usize {
    if (!args.plain_rules or args.distinct_pairs != .auto or args.integer_records_min_rows == 0 or args.scan_all or
        !args.ndv_union or keys.len != 1 or plan.query.filter != null) return null;
    const arg = calls[distinct].arg.?.col_ref;
    if (arg.physical_type != .INT64 or arg.unsigned_64) return null;
    var range: IntegerRangeHint = .{};
    for (work) |item| range.include(&metas[item.file].row_groups.items[item.rg], &metas[item.file], arg.col_idx);
    if (!range.small(rows)) return null;
    const files = try arena.alloc(ndv.File, args.inputs.len);
    for (args.inputs, files) |input, *file| file.* = .{
        .bytes = input.plan_bytes orelse input.bytes,
        .local = !std.mem.startsWith(u8, input.name, "s3://"),
    };
    // Headers only: neither decompress nor build another union to decide whether to avoid this owner.
    const stat = try ndv.collect(gpa, arena, files, metas, keys, workers, args.max_memory, false);
    const entries = std.math.cast(usize, stat.entries orelse return null) orelse return null;
    const key_bytes: usize = 1 + if (keys[0].col_ref.physical_type == .INT32) @as(usize, 4) else 8;
    const key_table_bytes = expr_agg.GroupTable.variableEntryBytes(key_bytes, calls.len);
    // A cache-resident local group index also permits effective pair-cache coalescing. Its joint value NDV is
    // unknown: comparing worst-case owner rows with just the set objects would systematically underprice sets.
    // Keep the existing pair trial in that case; only price a table that exceeds its active workers' L2 shares here.
    const l2 = @import("../hw.zig").l2BytesPerWorkerOr(640 << 10);
    if (entries *| key_table_bytes <= workers *| l2) return null;
    const set_bytes = key_table_bytes +| @sizeOf(expr_agg.DistinctSet);
    // Read the key/value and table pointers, then visit the local distinct values in the final set merge.
    return entries *| set_bytes +| rows *| (key_bytes + @sizeOf(u64) * 2 + @sizeOf(usize) * 2);
}

const PairQuote = struct {
    pairs: @import("../owner_cost.zig").Quote,
    groups: @import("../owner_cost.zig").Quote,
    group_records: usize,
};

fn quotePairStages(
    comptime Bits: type,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    identity: *const key_pack.Plan,
    rows: usize,
    workers: usize,
) ?PairQuote {
    const cost = @import("../owner_cost.zig");
    const pq = cost.quote(Bits, &.{}, u32, rows, rows, workers, 0, args.byte_top_partitions) orelse return null;
    var null_rows: usize = 0;
    for (work) |item| {
        const rg = metas[item.file].row_groups.items[item.rg];
        const cm = rg.columns.items[identity.keys[identity.keys.len - 1].col_idx].meta_data.?;
        const n = if (cm.statistics) |st| st.null_count orelse rg.num_rows else rg.num_rows;
        if (n < 0 or n > rg.num_rows) return null;
        null_rows +|= @intCast(n);
    }
    var pair_bound: usize = rows;
    if (identity.encoding.source == .dictionary) {
        var product: u128 = 1;
        for (identity.radices) |r| product = std.math.mul(u128, product, r) catch rows;
        pair_bound = @intCast(@min(rows, product));
    }
    // A NULL presence row or one completed pair is the only possible second-stage contribution. Its duplicate
    // weight still uses the original row bound. Global DISTINCT therefore reserves by values, never rows twice.
    // Footer NULL counts are speculative: the weighted owner requests whole-attempt replay if physical records
    // exceed this quote, even when scatter tail capacity happened to accommodate them.
    const group_records = @min(rows, pair_bound +| null_rows);
    const gq = cost.quote(
        Bits,
        &.{.present_count},
        u32,
        group_records,
        group_records,
        workers,
        0,
        args.byte_top_partitions,
    ) orelse return null;
    return .{ .pairs = pq, .groups = gq, .group_records = group_records };
}

const PairFinish = struct {
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    a: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    identity: *const key_pack.Plan,
    distinct: usize,
    workers: usize,
    slots: []MergedSlot,
    n: usize,
    ledger: *core_memory.SharedBudget,
    run: record_workers.RecordRun,
    names_credit: usize,
};

fn finishPairOutput(
    comptime top: bool,
    context: PairFinish,
    view: anytype,
    output: *AggOutput,
    output_charge: *usize,
    held: *usize,
    leave_records: *bool,
) !MultiAggResult {
    const View = @TypeOf(view.*);
    const gpa = context.gpa;
    const arena = context.arena;
    const a = context.a;
    const args = context.args;
    const metas = context.metas;
    const plan = context.plan;
    const work = context.work;
    const calls = context.calls;
    const keys = context.keys;
    const outputs = context.outputs;
    const identity = context.identity;
    const distinct = context.distinct;
    const workers = context.workers;
    const slots = context.slots;
    const n = context.n;
    const ledger = context.ledger;
    const run = context.run;
    const names_credit = context.names_credit;
    if (!args.grouped()) {
        output_charge.* = calls.len *| @sizeOf(AggOutputItem);
        for (calls) |c| output_charge.* +|= c.alias.len;
        _ = ledger.drawExact(output_charge.*) orelse {
            output_charge.* = 0;
            return error.ExceededMemoryBudget;
        };
        const items = try gpa.alloc(AggOutputItem, calls.len);
        @memset(items, .{ .alias = "", .value = .null_val });
        output.aggs = items;
        const accs = try arena.alloc(expr_agg.Accumulator, calls.len);
        for (items, calls, accs, 0..) |*item, call, *acc, ci| {
            const count: u64 = if (n == 0) 0 else @intCast((try View.integer(view, slots[0].idx, ci)).?);
            item.* = .{ .alias = try gpa.dupe(u8, call.alias), .value = .{ .i = count } };
            acc.* = .{ .count = count };
        }
        for (work) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
        // The retained scalar output is released by the enclosing query ledger with its output.
        const finished = try countedCalls(arena, calls, distinct);
        const sources_out = try arena.alloc(?schema.SchemaElement, calls.len);
        @memset(sources_out, null);
        if (args.ledger != null) {
            held.* = workers *| (@sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable));
            output_charge.* = 0;
        }
        return pairResult(args, plan, run, output.*, .{
            .accumulators = accs,
            .agg_calls = finished,
            .agg_sources = sources_out,
        });
    }
    const types = try arena.alloc(GroupColType, outputs.len);
    const key_types = try a.alloc(KeyType, keys.len);
    for (keys, key_types) |key, *ty| ty.* = keyTypeFromExpr(key);
    var key_bytes: usize = 0;
    for (outputs, types) |out, *ty| {
        ty.* = .{
            .lane = if (out.source == .key and key_types[out.source.key] == .string) .string else if (out.source == .agg and calls[out.source.agg].func == .avg) .f64 else .i64,
            .nullable = out.source == .key or calls[out.source.agg].func != .count,
            .source = if (out.source == .key) leafSchemaElem(&metas[0], keys[out.source.key].col_ref.col_idx) else owner_support.passThroughSource(calls[out.source.agg], &metas[0]),
        };
        if (out.source == .key) if (identity.domain) |d| switch (d.axes[out.source.key].values) {
            .bytes => |values| {
                var widest: usize = 0;
                for (values) |v| widest = @max(widest, v.len);
                key_bytes +|= widest;
            },
            else => {},
        };
    }
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
        .owner = .{ .ctx = view, .key = View.key, .integer = View.integer, .value = View.value },
    };
    const typed = args.typed_answer and args.result_order.keys.len == 0 and args.result_order.limit == null and
        args.result_order.offset == 0 and args.result_order.having == null and args.result_order.hidden.len == 0;
    output_charge.* = answerBytesParallel(slots, types, key_types, typed, workers) +| names_credit +| n *| key_bytes;
    _ = ledger.drawExact(output_charge.*) orelse {
        output_charge.* = 0;
        return error.ExceededMemoryBudget;
    };
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    for (outputs, names) |out, *name| name.* = try gpa.dupe(u8, out.name);
    if (typed) {
        output.typed = .{ .rows = 0, .columns = &.{} };
        try buildGroupColumns(gpa, &output.typed.?, row_source, types, workers);
    } else {
        const out_rows = try gpa.alloc([]const AggValue, n);
        @memset(out_rows, &.{});
        output.group_rows = out_rows;
        try buildGroupRows(gpa, &output.slab, row_source, out_rows, workers);
    }
    for (work) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
    if (args.ledger != null) {
        held.* = workers *| (@sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable));
        output_charge.* = 0; // retain the successful output credit in the query's enclosing ledger
    }
    // Keep the records and their full charge until process exit, as the ordinary table path does. A top-H
    // answer owns its copied keys and never reads these states again; all failure paths still deinitialize them.
    leave_records.* = top and args.leave_regions and !std.debug.runtime_safety;
    return pairResult(args, plan, run, output.*, .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types });
}

fn pairAnswerBits(
    comptime Bits: type,
    comptime top: bool,
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
    distinct: usize,
    rows: usize,
    input_rows: usize,
    workers: usize,
    cut: ?CountCut,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const P = @import("../owner_distinct.zig").Pipeline(Bits);
    const G = P.Groups;
    const F = @import("../owner_result.zig").Full(G.Group);
    const T = @import("../owner_result.zig").Top(G.Group, bool, struct {
        fn worse(by_distinct: bool, a: G.Group, b: G.Group) std.math.Order {
            const x = if (by_distinct) a.integer(0).? else a.weight;
            const y = if (by_distinct) b.integer(0).? else b.weight;
            return if (x != y) std.math.order(x, y) else std.math.order(b.first, a.first);
        }
    }.worse);
    const prefix = try P.Prefix.init(identity);
    const quote = quotePairStages(Bits, args, metas, work, identity, rows, workers) orelse return null;
    const pq = quote.pairs;
    const gq = quote.groups;
    const group_records = quote.group_records;
    if (top and args.plain_rules and args.distinct_pairs == .auto and args.integer_records_min_rows != 0) {
        // Ask the existing admission, including its compression/range, schema, top-H and memory checks. This
        // prices a real alternative, not a raw integer plan that cannot execute this query.
        var legacy_args = args;
        if (try inlineIntegerAnswer(gpa, arena, &legacy_args, metas, plan, work, calls, keys, outputs, workers, timings, .{ .refined = null }, true)) |legacy| {
            if (legacy.layout == .integer_distinct_top_h) {
                const O = owner_groups.IntSumTop;
                // One key-owner reduction sorts its complete (key,value) records and counts directly into H.
                // The packed route materializes pair groups and then reduces them by group a second time.
                const legacy_work = rows *| (@sizeOf(O.Record) + @sizeOf(O.Group) + @sizeOf(u64)) +|
                    workers *| cut.?.k *| @sizeOf(O.Group);
                const pair_work = pq.work +| gq.work +| workers *| cut.?.k *| @sizeOf(G.Group);
                if (legacy_work <= pair_work) {
                    if (!explain_only) if (args.taken) |taken| taken.insert(.plain_rule);
                    return null;
                }
            }
        }
    }
    if (try pairLocalSetWork(gpa, arena, args, metas, plan, work, calls, keys, distinct, rows, workers)) |sets| {
        if (sets <= pq.work +| gq.work) {
            if (!explain_only) if (args.taken) |taken| taken.insert(.plain_rule);
            return null;
        }
    }
    if (top and calls.len == 1 and keys.len == 1 and cut.?.a_idx == distinct and
        args.result_order.having == null and args.result_order.hidden.len == 0)
    {
        if (@import("../owner_distinct.zig").CompactGeometry.plan(identity)) |geometry| {
            if (geometry.bits <= 64) return compactPairAnswer(2, gpa, arena, args, metas, plan, work, calls, keys, outputs, identity, rows, input_rows, workers, cut.?, geometry, pq.parts, timings, explain_only);
            return compactPairAnswer(3, gpa, arena, args, metas, plan, work, calls, keys, outputs, identity, rows, input_rows, workers, cut.?, geometry, pq.parts, timings, explain_only);
        }
    }
    const result_credit = if (top) T.bytes(workers, cut.?.k) else F.bytes(workers, gq.parts);
    const domain_bytes = if (identity.domain) |d| d.bytes else 0;
    const pack_rows = owner_groups.batchRows(null);
    const scratch = domain_bytes +| identity.retainedBytes() +| workers *| pack_rows *| @sizeOf(Bits) +|
        workers *| (identity.keys.len *| @sizeOf(key_pack.Plan.Binding) +| @sizeOf(P.Sink) +|
            @sizeOf(P.Reducer) +| @sizeOf(consumer.RecordSink) +| @sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable)) +|
        work.len *| (@sizeOf(P.Sink.Source) +| @sizeOf(u32));
    const companion_bytes = if (needsCompanions(calls)) PairCompanions.bytes(cut.?.k, calls.len, workers, false) +|
        work.len *| @sizeOf(WorkItem) +| 3 *| plan.columns.read.len +| args.inputs.len *| @sizeOf(scan_types.Input) else 0;
    const peak = scratch +| pq.records +| gq.records +| @max(pq.finish, gq.finish) +| result_credit +| companion_bytes;
    if (peak > args.max_memory) return null;
    if (explain_only) return .{ .layout = .pair_distinct, .reservation = peak };
    var private_ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &private_ledger;
    _ = ledger.drawExact(scratch) orelse return null;
    var held: usize = 0;
    defer ledger.release(scratch - held);
    const pairs = (try P.Pairs.State.init(gpa, ledger, input_rows, rows, workers, pq.parts)) orelse return null;
    var leave_records = false;
    defer if (!leave_records) pairs.deinit();
    const groups = (try G.State.initWeighted(
        gpa,
        ledger,
        input_rows,
        rows,
        group_records,
        workers,
        gq.parts,
    )) orelse return null;
    defer if (!leave_records) groups.deinit();
    const result = if (top)
        T.init(gpa, ledger, workers, cut.?.k, cut.?.a_idx == distinct)
    else
        F.init(gpa, ledger, workers, gq.parts);
    var receiver = result catch |e| {
        if (e == error.ExceededMemoryBudget) return null;
        return e;
    };
    defer receiver.deinit();
    var temporary = std.heap.ArenaAllocator.init(gpa);
    defer temporary.deinit();
    const a = temporary.allocator();
    const starts = try a.alloc(u32, work.len);
    if (!RecordOrdinals.fill(metas, work, starts)) return error.BadColumn;
    const sources = try a.alloc(P.Sink.Source, work.len);
    for (sources, work) |*s, item| s.* = .{ .file = item.file, .rg = item.rg };
    const adapters = try a.alloc(P.Sink, workers);
    const sinks = try a.alloc(consumer.RecordSink, workers);
    const reducers = try a.alloc(P.Reducer, workers);
    var refused: std.atomic.Value(bool) = .init(false);
    for (adapters, sinks, reducers, pairs.locals, groups.locals) |*s, *sink, *r, *pl, *gl| {
        s.* = .{
            .pairs = pl,
            .groups = gl,
            .identity = identity,
            .prefix = prefix,
            .sources = sources,
            .bindings = try a.alloc(key_pack.Plan.Binding, identity.keys.len),
            .pack_buffer = try a.alloc(Bits, pack_rows),
            .refused = &refused,
        };
        sink.* = .{ .selected = .{ .ctx = s, .startFn = P.Sink.startErased, .batchFn = P.Sink.batchErased } };
        r.* = .{ .local = gl, .prefix = prefix, .count_rows = calls.len > 1 };
    }
    if (args.taken) |taken| taken.insert(.integer_records);
    const pair_keys = try a.alloc(expr_ast.Expr, identity.keys.len);
    for (pair_keys, identity.keys) |*k, ref| k.* = .{ .col_ref = ref };
    const run = runRecordWorkers(
        gpa,
        arena,
        args,
        metas,
        plan,
        work,
        calls,
        pair_keys,
        sinks,
        starts,
        outputs,
        timings,
    ) catch |e| {
        if (refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const span = profile.Span.begin(profile.threadOf(args.profiler, .main));
    try pairs.finish(reducers, P.Reducer.receive);
    try groups.finish(receiver.receivers, if (top) T.Receiver.receive else F.Receiver.receive);
    span.end(.merge, profile.args(.{gq.parts}));
    const top_groups = if (top) receiver.finish() else &.{};
    const n = if (top) top_groups.len else receiver.count();
    const slots = try a.alloc(MergedSlot, n);
    if (top) {
        for (top_groups, slots, 0..) |g, *s, i| s.* = .{ .key = &.{}, .first = g.first, .idx = i };
    } else try receiver.fill(MergedSlot, slots, struct {
        fn make(g: G.Group, index: u64) MergedSlot {
            return .{ .key = &.{}, .first = g.first, .idx = @intCast(index) };
        }
    }.make);
    if (!args.unordered) try sortSlotsByFirst(a, slots, workers);
    var companion_charge: core_memory.Charged = .{ .child = gpa, .budget = ledger };
    var companion_arena = std.heap.ArenaAllocator.init(companion_charge.allocator());
    defer companion_arena.deinit();
    const ca = companion_arena.allocator();
    var companion: ?PairCompanions = null;
    if (needsCompanions(calls)) {
        const group_keys = try ca.alloc(u128, slots.len);
        for (slots, group_keys) |slot, *key| key.* = if (top) top_groups[slot.idx].key else receiver.at(slot.idx).key;
        companion = PairCompanions.init(ca, companion_charge.allocator(), identity, group_keys, calls, workers, false) catch |e| {
            if (e == error.OutOfMemory and companion_charge.refused.load(.monotonic)) return error.ExceededMemoryBudget;
            return e;
        };
        companion.?.run(gpa, args, metas, plan, work, keys, starts, outputs, run.timings, ledger) catch |e| {
            if (e == error.OutOfMemory and companion_charge.refused.load(.monotonic)) return error.ExceededMemoryBudget;
            return e;
        };
    }
    const View = struct {
        groups: []const G.Group,
        full: ?*F,
        distinct: usize,
        companion: ?*const PairCompanions,
        calls: []const expr_agg.AggCall,
        fn at(v: *const @This(), i: usize) *const G.Group {
            return if (v.full) |f| f.at(i) else &v.groups[i];
        }
        fn key(ctx: *const anyopaque, i: usize) u128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return v.at(i).key;
        }
        fn integer(ctx: *const anyopaque, i: usize, call: usize) anyerror!?i128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            if (call == v.distinct) return v.at(i).integer(0);
            if (v.companion) |c| return switch (c.accumulator(v.at(i).key, call)) {
                .count => |number| number,
                .sum_i, .min_i, .max_i => |number| number,
                else => error.BadAggArg,
            };
            return v.at(i).weight;
        }
        fn value(ctx: *const anyopaque, allocator: std.mem.Allocator, i: usize, call: usize) anyerror!AggValue {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            if (call != v.distinct) if (v.companion) |c|
                return @import("materialize.zig").materializeOne(allocator, v.calls[call], c.accumulator(v.at(i).key, call));
            return .{ .i = (try integer(ctx, i, call)).? };
        }
    };
    const view: View = .{ .groups = top_groups, .full = if (top) null else receiver, .distinct = distinct, .companion = if (companion) |*c| c else null, .calls = calls };
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names_credit = outputNamesBytes(outputs);
    var output_charge: usize = 0;
    defer ledger.release(output_charge);
    return try finishPairOutput(top, .{
        .gpa = gpa,
        .arena = arena,
        .a = a,
        .args = args,
        .metas = metas,
        .plan = plan,
        .work = work,
        .calls = calls,
        .keys = keys,
        .outputs = outputs,
        .identity = identity,
        .distinct = distinct,
        .workers = workers,
        .slots = slots,
        .n = n,
        .ledger = ledger,
        .run = run,
        .names_credit = names_credit,
    }, &view, &output, &output_charge, &held, &leave_records);
}

pub fn countedCalls(arena: std.mem.Allocator, calls: []const expr_agg.AggCall, i: usize) ![]const expr_agg.AggCall {
    std.debug.assert(calls[i].func == .count_distinct);
    const out = try arena.dupe(expr_agg.AggCall, calls);
    out[i].func = .count;
    return out;
}

fn needsCompanions(calls: []const expr_agg.AggCall) bool {
    for (calls) |call| if (call.func != .count_distinct and (call.func != .count or call.arg != null)) return true;
    return false;
}

/// The count cut is final before this pass: only retained groups need plain payload state.
const PairCompanions = struct {
    a: std.mem.Allocator,
    batch_allocator: std.mem.Allocator,
    identity: key_pack.Plan,
    index: std.AutoHashMap(u128, u32),
    calls: []expr_agg.AggCall,
    states: []expr_agg.Accumulator,
    workers: usize,
    firsts: []std.atomic.Value(u32) = &.{},

    fn bytes(groups: usize, calls: usize, workers: usize, ordinals: bool) usize {
        // Hash capacity is a power of two with spare load; four slots per retained key is an upper bound.
        return @sizeOf(PairCompanions) +| 128 +| groups *| 4 *| (@sizeOf(u128) +| @sizeOf(u32) +| 1) +|
            calls *| @sizeOf(expr_agg.AggCall) +| (if (ordinals) groups *| @sizeOf(std.atomic.Value(u32)) else workers *| groups *| calls *| @sizeOf(expr_agg.Accumulator)) +|
            workers *| (@sizeOf(Sink) +| @sizeOf(consumer.RecordSink) +| @sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable));
    }

    fn init(a: std.mem.Allocator, batch_allocator: std.mem.Allocator, identity: *const key_pack.Plan, keys: []const u128, calls: []const expr_agg.AggCall, workers: usize, ordinals: bool) !PairCompanions {
        var group_identity = identity.*;
        group_identity.keys = identity.keys[0 .. identity.keys.len - 1];
        if (identity.fields.len != 0) group_identity.fields = identity.fields[0 .. identity.fields.len - 1];
        if (identity.radices.len != 0) group_identity.radices = identity.radices[0 .. identity.radices.len - 1];
        var index = std.AutoHashMap(u128, u32).init(a);
        for (keys, 0..) |key, i| try index.put(key, @intCast(i));
        const plain = try a.dupe(expr_agg.AggCall, calls);
        for (plain) |*call| if (call.func == .count_distinct) {
            call.func = .count;
            call.arg = null;
        };
        const states = try a.alloc(expr_agg.Accumulator, if (ordinals) 0 else workers * keys.len * calls.len);
        for (states, 0..) |*state, i| state.* = .init(plain[i % plain.len]);
        const firsts = try a.alloc(std.atomic.Value(u32), if (ordinals) keys.len else 0);
        for (firsts) |*first| first.* = .init(std.math.maxInt(u32));
        return .{ .a = a, .batch_allocator = batch_allocator, .identity = group_identity, .index = index, .calls = plain, .states = states, .workers = workers, .firsts = firsts };
    }

    fn accumulator(c: *const PairCompanions, key: u128, call: usize) expr_agg.Accumulator {
        return c.states[c.index.get(key).? * c.calls.len + call];
    }

    fn prefixMayImprove(c: *const PairCompanions, base: u64) bool {
        for (c.firsts) |first| if (first.load(.monotonic) > base) return true;
        return false;
    }

    fn firstRow(c: *PairCompanions, id: usize, ordinal: u32, contiguous: bool) bool {
        _ = c.firsts[id].fetchMin(ordinal, .monotonic);
        if (!contiguous) return false;
        // Gathered rows may arrive out of source order. Only a contiguous suffix proves no later row is earlier.
        return !c.prefixMayImprove(ordinal);
    }

    const Sink = struct {
        pass: *PairCompanions,
        states: []expr_agg.Accumulator,
        sources: []const WorkItem,
        item: usize = 0,
        fn start(ctx: *anyopaque, _: u64, item: usize) void {
            const s: *Sink = @ptrCast(@alignCast(ctx));
            s.item = item;
        }
        fn batch(ctx: *anyopaque, source: consumer.SelectedBatch, lookup: []const ?usize) anyerror!void {
            const s: *Sink = @ptrCast(@alignCast(ctx));
            try source.validate();
            if (s.pass.firsts.len != 0 and !s.pass.prefixMayImprove(source.input_base)) return;
            var temporary = std.heap.ArenaAllocator.init(s.pass.batch_allocator);
            defer temporary.deinit();
            const a = temporary.allocator();
            const at = s.sources[s.item];
            const bound = (try s.pass.identity.bind(a, source, lookup, at.file, at.rg)) orelse
                return error.ExceededMemoryBudget;
            for (0..source.batch.num_rows) |row| {
                if (source.selected) |selection| if (!selection.isActive(row)) continue;
                const key = try bound.packAs(u128, row);
                const id = s.pass.index.get(key) orelse continue;
                if (s.pass.firsts.len != 0) {
                    const first = std.math.cast(u32, try source.ordinal(row)) orelse return error.BadColumn;
                    if (s.pass.firstRow(id, first, source.input_rows == null)) return;
                    continue;
                }
                for (s.pass.calls, s.states[id * s.pass.calls.len ..][0..s.pass.calls.len]) |call, *state| {
                    if (call.arg == null) {
                        state.count += 1;
                        continue;
                    }
                    const ref = call.arg.?.col_ref;
                    const pos = lookup[ref.col_idx] orelse return error.BadColumn;
                    const col = source.batch.cols[pos];
                    const ni = col.nullInfo();
                    if (ni.def_levels) |levels| if (levels[row] < ni.max_def) continue;
                    if (call.func == .count) {
                        state.count += 1;
                        continue;
                    }
                    const value: i128 = switch (col) {
                        inline .i32, .i64 => |lane| value: {
                            const index = if (lane.codes) |codes| codes[row] else row;
                            const values = if (lane.codes != null) lane.dict else lane.values;
                            if (index >= values.len) return error.BadColumn;
                            const v: i64 = values[index];
                            break :value if (ref.unsigned_32) @as(u32, @truncate(@as(u64, @bitCast(v)))) else v;
                        },
                        else => return error.BadColumn,
                    };
                    switch (state.*) {
                        .sum_i => |*sum| sum.* = (sum.* orelse 0) + value,
                        .min_i => |*minimum| minimum.* = if (minimum.*) |v| @min(v, value) else value,
                        .max_i => |*maximum| maximum.* = if (maximum.*) |v| @max(v, value) else value,
                        .avg => |*avg| {
                            avg.isum += value;
                            avg.count += 1;
                        },
                        else => return error.BadAggArg,
                    }
                }
            }
        }
    };

    fn run(c: *PairCompanions, gpa: std.mem.Allocator, args: MultiAggArgs, metas: []const schema.FileMetaData, plan: *const agg_plan.Plan, work: []const WorkItem, keys: []const expr_ast.Expr, starts: []const u32, outputs: []const BoundOutputColumn, timings: Timings, ledger: *core_memory.SharedBudget) !void {
        _ = ledger;
        // Rescan metadata dies with this pass, including on a later output refusal in the caller.
        var scratch = std.heap.ArenaAllocator.init(c.batch_allocator);
        defer scratch.deinit();
        const a = scratch.allocator();
        const adapters = try a.alloc(Sink, c.workers);
        const sinks = try a.alloc(consumer.RecordSink, c.workers);
        const stride = if (c.firsts.len == 0) c.index.count() * c.calls.len else 0;
        for (adapters, sinks, 0..) |*adapter, *sink, i| {
            adapter.* = .{ .pass = c, .states = c.states[i * stride ..][0..stride], .sources = work };
            sink.* = .{ .selected = .{ .ctx = adapter, .startFn = Sink.start, .batchFn = Sink.batch } };
        }
        var rescan = args;
        try @import("input_rewind.zig").rewindInputs(a, &rescan);
        const columns = try plan.forCalls(a, c.calls);
        const rescan_work = try a.dupe(WorkItem, work);
        for (rescan_work) |*item| item.columns = columns;
        _ = try runRecordWorkers(gpa, a, rescan, metas, plan, rescan_work, c.calls, keys, sinks, starts, outputs, timings);
        for (1..c.workers) |w| for (c.states[0..stride], c.states[w * stride ..][0..stride]) |*dst, src|
            try dst.merge(src, c.a);
    }
};

fn compactPairAnswer(
    comptime words: usize,
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
    rows: usize,
    input_rows: usize,
    workers: usize,
    cut: CountCut,
    geometry: @import("../owner_distinct.zig").CompactGeometry,
    parts: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const O = @import("../owner_distinct.zig").Compact(words);
    const G = O.Groups;
    const F = @import("../owner_result.zig").Full(G.Group);
    const T = @import("../owner_result.zig").Top(G.Group, void, struct {
        fn worse(_: void, a: G.Group, b: G.Group) std.math.Order {
            return std.math.order(a.integer(0).?, b.integer(0).?);
        }
    }.worse);
    const group_bound = if (geometry.group_bits >= @bitSizeOf(usize)) rows else @min(rows, @as(usize, 1) << @as(std.math.Log2Int(usize), @intCast(geometry.group_bits)));
    const candidate_bound = @min(group_bound, @max(cut.k, @import("../hw.zig").l2BytesPerWorkerOr(640 << 10) / @sizeOf(G.Group)));
    // Count every retained store beside the compact scatter and its worst-case concurrent fold.
    const retained = F.bytes(workers, parts) +| group_bound *| @sizeOf(G.Group) +|
        T.bytes(workers, cut.k) +| PairCompanions.bytes(candidate_bound, calls.len, workers, true) +|
        candidate_bound *| (2 * @sizeOf(G.Group) +| 2 * @sizeOf(u128) +| @sizeOf(MergedSlot) +|
            @sizeOf([]const AggValue) +| outputs.len *| @sizeOf(AggValue)) +| outputNamesBytes(outputs);
    const scratch = identity.retainedBytes() +| work.len *| (2 * @sizeOf(u32) +| 3 * @sizeOf(usize)) +|
        args.inputs.len *| @sizeOf(scan_types.Input) +| 3 *| plan.columns.read.len +|
        workers *| (owner_groups.batchRows(null) *| @sizeOf(u128) +| @sizeOf(O.Sink) +|
            @sizeOf(O.Reducer) +| @sizeOf(consumer.RecordSink) +| 2 * @sizeOf(usize) +|
            @sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable) +| 2 *| @sizeOf(WorkItem) +| @sizeOf(u32));
    const peak = O.bytes(rows, workers, parts) +| G.State.charge(rows, workers, parts) +|
        O.finishBytes(rows, workers) +| retained +| scratch;
    if (peak > args.max_memory) return null;
    if (explain_only) return .{ .layout = .pair_distinct, .reservation = peak, .partitions = parts };
    var private_ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &private_ledger;
    var charged: core_memory.Charged = .{ .child = gpa, .budget = ledger };
    return compactPairRun(O, F, T, gpa, arena, charged.allocator(), ledger, args, metas, plan, work, calls, keys, outputs, identity, rows, input_rows, workers, cut, geometry, parts, timings) catch |e| {
        if (e == error.OutOfMemory and charged.refused.load(.monotonic)) return error.ExceededMemoryBudget;
        return e;
    };
}

fn compactPairRun(
    comptime O: type,
    comptime F: type,
    comptime T: type,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    charged: std.mem.Allocator,
    ledger: *core_memory.SharedBudget,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    identity: *const key_pack.Plan,
    rows: usize,
    input_rows: usize,
    workers: usize,
    cut: CountCut,
    geometry: @import("../owner_distinct.zig").CompactGeometry,
    parts: usize,
    timings: Timings,
) !?MultiAggResult {
    const state = try O.init(charged, ledger, rows, workers, parts);
    defer state.deinit();
    const groups = (try O.Groups.State.init(gpa, ledger, input_rows, rows, workers, parts)) orelse {
        return null;
    };
    defer groups.deinit();
    const full = try F.init(gpa, ledger, workers, parts);
    defer full.deinit();
    var top = try T.init(gpa, ledger, workers, cut.k, {});
    defer top.deinit();
    var temporary = std.heap.ArenaAllocator.init(charged);
    defer temporary.deinit();
    const a = temporary.allocator();
    const starts = try a.alloc(u32, work.len);
    if (!RecordOrdinals.fill(metas, work, starts)) return error.BadColumn;
    const sources = try a.alloc(@import("../owner_distinct.zig").Pipeline(u128).Sink.Source, work.len);
    for (sources, work) |*source, item| source.* = .{ .file = item.file, .rg = item.rg };
    const adapters = try a.alloc(O.Sink, workers);
    const sinks = try a.alloc(consumer.RecordSink, workers);
    const reducers = try a.alloc(O.Reducer, workers);
    var refused: std.atomic.Value(bool) = .init(false);
    for (adapters, sinks, reducers, state.locals, groups.locals) |*s, *sink, *r, *local, *gl| {
        s.* = .{ .local = local, .identity = identity, .sources = sources, .images = try a.alloc(u128, owner_groups.batchRows(null)), .input_rows = input_rows, .refused = &refused };
        sink.* = .{ .selected = .{ .ctx = s, .startFn = O.Sink.start, .batchFn = O.Sink.batch } };
        r.* = .{ .local = gl, .geometry = geometry };
    }
    const credit = workers *| (@sizeOf(Worker) +| @sizeOf(expr_agg.GroupTable));
    _ = ledger.drawExact(credit) orelse return null;
    var held: usize = 0;
    defer ledger.release(credit - held);
    if (args.taken) |taken| taken.insert(.integer_records);
    const run = runRecordWorkers(gpa, arena, args, metas, plan, work, calls, keys, sinks, starts, outputs, timings) catch |e| {
        if (refused.load(.monotonic)) if (args.taken) |taken| taken.remove(.decode_memory);
        return e;
    };
    const span = profile.Span.begin(profile.threadOf(args.profiler, .main));
    try state.finish(reducers);
    const Receiver = struct {
        full: *F.Receiver,
        top: *T.Receiver,
        fn receive(r: *@This(), p: usize, complete: []const O.Groups.Group) !void {
            try r.full.receive(p, complete);
            try r.top.receive(p, complete);
        }
    };
    const receivers = try a.alloc(Receiver, workers);
    for (receivers, full.receivers, top.receivers) |*r, *f, *t| r.* = .{ .full = f, .top = t };
    try groups.finish(receivers, Receiver.receive);
    const best = top.finish();
    const floor: i128 = if (best.len < cut.k) 0 else best[best.len - 1].integer(0).?;
    var candidates: std.ArrayList(O.Groups.Group) = .empty;
    const l2 = @import("../hw.zig").l2BytesPerWorkerOr(640 << 10);
    const cap = @max(cut.k, l2 / @sizeOf(O.Groups.Group));
    for (full.pieces) |piece| for (piece.groups) |g| if (g.integer(0).? >= floor) {
        // A huge boundary tie privately replays the ordinal-carrying route rather than retaining all groups.
        if (candidates.items.len == cap) return error.ExceededMemoryBudget;
        try candidates.append(a, g);
    };
    std.mem.sortUnstable(O.Groups.Group, candidates.items, {}, struct {
        fn less(_: void, x: O.Groups.Group, y: O.Groups.Group) bool {
            return x.integer(0).? > y.integer(0).?;
        }
    }.less);
    if (std.c.getenv("ZPQ_CD_TRACE")) |v| if (std.mem.eql(u8, std.mem.span(v), "1")) {
        var records: usize = 0;
        var hits: usize = 0;
        var bypass: usize = 0;
        for (state.locals) |local| {
            for (local.frontier.histogram) |count| records += count;
            hits += local.hits;
            bypass += @intFromBool(local.bypass);
        }
        std.debug.print("distinct pairs: records={d} trial_hits={d} bypass_workers={d}\n", .{ records, hits, bypass });
    };
    span.end(.merge, profile.args(.{parts}));
    var tied: std.ArrayList(u128) = .empty;
    for (candidates.items, 0..) |g, i| {
        const count = g.integer(0).?;
        if ((i > 0 and candidates.items[i - 1].integer(0).? == count) or
            (i + 1 < candidates.items.len and candidates.items[i + 1].integer(0).? == count))
            try tied.append(a, g.key);
    }
    if (tied.items.len != 0) {
        var pass = try PairCompanions.init(a, charged, identity, tied.items, calls, workers, true);
        const order = try a.alloc(usize, work.len);
        for (order, 0..) |*item, i| item.* = i;
        std.mem.sortUnstable(usize, order, starts, struct {
            fn less(ordinals: []const u32, x: usize, y: usize) bool {
                return ordinals[x] < ordinals[y];
            }
        }.less);
        const prefix_work = try a.alloc(WorkItem, workers);
        const prefix_starts = try a.alloc(u32, workers);
        // Parallel source prefixes keep the early-stop proof while allowing late ties to use every worker.
        var at: usize = 0;
        while (at < order.len) {
            const width = @min(workers, order.len - at);
            for (order[at..][0..width], prefix_work[0..width], prefix_starts[0..width]) |item, *w, *start| {
                w.* = work[item];
                start.* = starts[item];
            }
            try pass.run(gpa, args, metas, plan, prefix_work[0..width], keys, prefix_starts[0..width], outputs, run.timings, ledger);
            at += width;
            if (at == order.len or !pass.prefixMayImprove(starts[order[at]])) break;
        }
        for (pass.firsts) |first| if (first.load(.monotonic) == std.math.maxInt(u32))
            return error.ExceededMemoryBudget;
        for (candidates.items) |*g| if (pass.index.get(g.key)) |i| {
            g.first = pass.firsts[i].load(.monotonic);
        };
    }
    const slots = try a.alloc(MergedSlot, candidates.items.len);
    for (candidates.items, slots, 0..) |g, *slot, i| slot.* = .{ .key = &.{}, .first = g.first, .idx = i };
    try sortSlotsByFirst(a, slots, workers);
    const View = struct {
        groups: []const O.Groups.Group,
        fn key(ctx: *const anyopaque, i: usize) u128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return v.groups[i].key;
        }
        fn integer(ctx: *const anyopaque, i: usize, _: usize) anyerror!?i128 {
            const v: *const @This() = @ptrCast(@alignCast(ctx));
            return v.groups[i].integer(0);
        }
        fn value(ctx: *const anyopaque, _: std.mem.Allocator, i: usize, call: usize) anyerror!AggValue {
            return .{ .i = (try integer(ctx, i, call)).? };
        }
    };
    const view: View = .{ .groups = candidates.items };
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    var output_charge: usize = 0;
    defer ledger.release(output_charge);
    var leave = false;
    return try finishPairOutput(true, .{ .gpa = gpa, .arena = arena, .a = a, .args = args, .metas = metas, .plan = plan, .work = work, .calls = calls, .keys = keys, .outputs = outputs, .identity = identity, .distinct = 0, .workers = workers, .slots = slots, .n = slots.len, .ledger = ledger, .run = run, .names_credit = outputNamesBytes(outputs) }, &view, &output, &output_charge, &held, &leave);
}

test "compact tied prefix keeps original minima across reordered gathered batches" {
    var firsts = [_]std.atomic.Value(u32){ .init(std.math.maxInt(u32)), .init(std.math.maxInt(u32)) };
    var pass: PairCompanions = undefined;
    pass.firsts = &firsts;
    try std.testing.expect(!pass.firstRow(0, 9, false));
    try std.testing.expect(!pass.firstRow(1, 2, false));
    try std.testing.expect(!pass.firstRow(0, 7, false));
    try std.testing.expect(!pass.firstRow(1, 0, false));
    try std.testing.expectEqual(@as(u32, 7), firsts[0].load(.monotonic));
    try std.testing.expectEqual(@as(u32, 0), firsts[1].load(.monotonic));
    try std.testing.expect(pass.prefixMayImprove(0));
    try std.testing.expect(!pass.prefixMayImprove(8));
    try std.testing.expect(!pass.firstRow(1, 0, true));
    try std.testing.expect(pass.firstRow(0, 1, true));
}
