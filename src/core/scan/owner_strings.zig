const std = @import("std");
const nowMonoNs = @import("../../clock.zig").monoNs;
const profile = @import("../../profile.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const partagg = @import("../partagg.zig");
const string_table = @import("../string_table.zig");
const ndv = @import("../ndv.zig");
const owned_merge = @import("owned_merge.zig");
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const key_decode = @import("key_decode.zig");
const KeyType = key_decode.KeyType;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const plan_types = @import("plan_types.zig");
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
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
const outputNamesBytes = owner_support.outputNamesBytes;
const passThroughSource = owner_support.passThroughSource;
const owner_pair = @import("owner_pair.zig");
const countedCalls = owner_pair.countedCalls;
const partition_fill = @import("partition_fill.zig");
const fillFromPartitions = partition_fill.fillFromPartitions;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const allRescannable = scan_types.allRescannable;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const RecordOrdinals = scan_types.RecordOrdinals;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const AggOutputItem = output_values.AggOutputItem;

pub fn inlineSharedStrings(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    workers: usize,
    timings: Timings,
    comptime explain_only: bool,
) !if (explain_only) ?RecordAdmission else ?MultiAggResult {
    const order = args.result_order;
    if (!args.shared_strings or !allRescannable(args.inputs) or !args.plain_rules or
        order.keys.len != 0 or order.limit != null or order.offset != 0 or order.having != null or
        order.hidden.len != 0) return null;
    const scalar = keys_opt == null;
    const key = if (keys_opt) |keys| blk: {
        if (keys.len != 1) return null;
        for (calls) |call| {
            if (call.where != null or call.func == .count_distinct or call.state or call.offset != 0) return null;
            if (call.arg) |arg| {
                if (arg != .col_ref or arg.col_ref.expr_type != .i64 or arg.isBoolean()) return null;
            } else if (call.func != .count) return null;
        }
        for (outputs_opt.?) |out| if (out.derived != null) return null;
        break :blk keys[0];
    } else blk: {
        if (calls.len != 1 or calls[0].func != .count_distinct or calls[0].where != null or calls[0].state)
            return null;
        break :blk calls[0].arg orelse return null;
    };
    if (key != .col_ref or key.col_ref.expr_type != .str or key.col_ref.physical_type != .BYTE_ARRAY)
        return null;
    // Complete dictionaries already avoid row-wise byte hashing. Route wide PLAIN/fallback bytes directly;
    // short keys keep their existing fixed/coded owner. Encoded width is a cost hint, never an identity bound.
    var coded = true;
    var key_bytes: u128 = 0;
    var key_rows: u128 = 0;
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        const cm = rg.columns.items[key.col_ref.col_idx].meta_data orelse return null;
        coded = coded and ndv.fullCoverage(&cm);
        key_bytes += @intCast(@max(0, cm.total_uncompressed_size));
        key_rows += @intCast(@max(0, rg.num_rows));
    }
    if (coded or key_bytes < key_rows * (2 * @sizeOf([]const u8))) return null;
    // Packed record scans use an original flat ordinal. Width refusal leaves the established string route intact.
    const rows = RecordOrdinals.rows(metas) orelse return null;
    if (rows >= @as(usize, 1) << 31) return null;
    const folded_calls: []const expr_agg.AggCall = if (scalar) &.{} else calls;
    const worker_credit = workers * (@sizeOf(Worker) + @sizeOf(expr_agg.GroupTable));
    const setup = worker_credit + work.len * @sizeOf(u32) + workers * @sizeOf(consumer.RecordSink) +
        (if (outputs_opt) |outs| outs.len * @sizeOf(GroupColType) else 0);
    const quote = setup +| string_table.State.setupBytes(workers, args.max_memory);
    if (quote > args.max_memory / 4 or
        (args.ledger != null and quote > args.ledger.?.available())) return null;
    if (explain_only) return .{
        .layout = .shared_string_table,
        .reservation = quote,
        .partitions = @as(usize, 1) << partagg.partBits(args.max_memory),
    };
    var own_ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &own_ledger;
    _ = ledger.drawExact(setup) orelse return null;
    var held: usize = 0;
    defer ledger.release(setup - held);
    var state = (try string_table.State.init(
        gpa,
        ledger,
        folded_calls,
        key.col_ref.col_idx,
        scalar,
        workers,
    )) orelse return null;
    defer state.deinit();
    state.bind();
    const starts = try arena.alloc(u32, work.len);
    if (!RecordOrdinals.fill(metas, work, starts)) return null;
    const sinks = try arena.alloc(consumer.RecordSink, workers);
    for (sinks, state.locals) |*sink, *local| sink.* = .{ .selected = .{
        .ctx = local,
        .startFn = string_table.Sink.startErased,
        .batchFn = string_table.Sink.batchErased,
    } };
    const previous_block = ledger.block;
    ledger.block = expr_agg.SharedBudget.blockFor(args.max_memory, workers + state.parts.len);
    defer ledger.block = previous_block;
    if (args.taken) |taken| taken.insert(.shared_strings);
    var decoded = args;
    // Record callbacks bypass automatic expression slicing. Bound their producer to the same cache-sized window
    // as the sink, rather than retaining a whole RG's decompressed string pages beside the shared identity.
    if (decoded.auto_decode_batches and decoded.decode_batch_rows == 0 and decoded.expr_batch_rows != 0)
        decoded.decode_batch_rows = string_table.batchRows();
    const run = try runRecordWorkers(
        gpa,
        arena,
        decoded,
        metas,
        plan,
        work,
        calls,
        &.{key},
        sinks,
        starts,
        outputs_opt orelse &.{},
        timings,
    );
    const prof = profile.threadOf(args.profiler, .main);
    const begin = nowMonoNs();
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    var answer_charge: usize = 0;
    defer ledger.release(answer_charge);
    if (scalar) {
        answer_charge = @sizeOf(AggOutputItem) + calls[0].alias.len;
        _ = ledger.drawExact(answer_charge) orelse {
            answer_charge = 0;
            return error.ExceededMemoryBudget;
        };
        const items = try gpa.alloc(AggOutputItem, 1);
        items[0] = .{ .alias = "", .value = .{ .i = state.count() } };
        output.aggs = items;
        items[0].alias = try gpa.dupe(u8, calls[0].alias);
        const accs = try arena.alloc(expr_agg.Accumulator, 1);
        accs[0] = .{ .count = @intCast(state.count()) };
        const finished = try countedCalls(arena, calls, 0);
        const sources = try arena.alloc(?schema.SchemaElement, 1);
        sources[0] = null;
        for (work) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
        if (args.ledger != null) {
            held = worker_credit;
            answer_charge = 0;
        }
        var result = pairResult(args, plan, run, output, .{
            .accumulators = accs,
            .agg_calls = finished,
            .agg_sources = sources,
        });
        result.distinct_pairs = false;
        profile.record(prof, .merge, begin, nowMonoNs(), profile.args(.{}));
        return result;
    }
    const outputs = outputs_opt.?;
    var merge_charge: core_memory.Charged = .{ .child = gpa, .budget = ledger };
    const lists = merge_charge.allocator();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(lists);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(lists);
    try fillFromPartitions(
        gpa,
        lists,
        arena,
        state.parts,
        calls,
        outputs,
        order,
        args.unordered,
        null,
        workers,
        &slots,
        &accs,
    );
    if (!args.unordered) try sortSlotsByFirst(lists, slots.items, workers);
    const key_types = [_]KeyType{.string};
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, types) |out, *ty| ty.* = .{
        .lane = if (out.source == .key) .string else if (calls[out.source.agg].func == .avg) .f64 else .i64,
        .nullable = out.source == .key or calls[out.source.agg].func != .count,
        .source = if (out.source == .key)
            leafSchemaElem(&metas[0], key.col_ref.col_idx)
        else
            passThroughSource(calls[out.source.agg], &metas[0]),
    };
    const tables = try lists.alloc(*expr_agg.GroupTable, state.parts.len);
    defer lists.free(tables);
    for (tables, state.parts) |*out, *part| out.* = &part.table;
    output.arena_credit = try output.takeTableArenas(gpa, tables, ledger);
    output.arena_ledger = args.ledger;
    const source: RowSource = .{
        .slots = slots.items,
        .outputs = outputs,
        .calls = calls,
        .key_types = &key_types,
        .key_exprs = &.{key},
        .accs = accs.items,
        .folded = true,
        .fixed_keys = false,
        .borrowed_strings = output.borrowed_strings,
    };
    answer_charge = answerBytesParallel(slots.items, types, &.{}, args.typed_answer, workers) +|
        outputNamesBytes(outputs);
    _ = ledger.drawExact(answer_charge) orelse {
        answer_charge = 0;
        return error.ExceededMemoryBudget;
    };
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    for (outputs, names) |out, *name| name.* = try gpa.dupe(u8, out.name);
    if (args.typed_answer) {
        output.typed = .{ .rows = 0, .columns = &.{} };
        try buildGroupColumns(gpa, &output.typed.?, source, types, workers);
    } else {
        const out_rows = try gpa.alloc([]const AggValue, slots.items.len);
        @memset(out_rows, &.{});
        output.group_rows = out_rows;
        try buildGroupRows(gpa, &output.slab, source, out_rows, workers);
    }
    if (args.ledger != null) {
        held = worker_credit + outputs.len * @sizeOf(GroupColType);
        answer_charge = 0;
    }
    var result = pairResult(args, plan, run, output, .{
        .accumulators = &.{},
        .agg_calls = calls,
        .group_col_types = types,
    });
    result.distinct_pairs = false;
    profile.record(prof, .merge, begin, nowMonoNs(), profile.args(.{}));
    return result;
}

