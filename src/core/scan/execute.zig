const outputNamesBytes = @import("owner_support.zig").outputNamesBytes;
const CountCut = @import("count_cut.zig").CountCut;
const filter_prune = @import("../filter/prune.zig");
const row_subgroups = @import("../row_subgroups.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const BoundAggregate = @import("types.zig").BoundAggregate;
const std = @import("std");
const testing = std.testing;
const nowMonoNs = @import("../../clock.zig").monoNs;
const profile = @import("../../profile.zig");
const spawn_util = @import("../spawn.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_parser = @import("../expr/parser.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const partagg = @import("../partagg.zig");
const ndv = @import("../ndv.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const key_pack = @import("../expr/key_pack.zig");
const owner_groups = @import("../owner_groups.zig");
const NdvStat = ndv.Stat;
const filter_ast = @import("../filter/ast.zig");
const filter_parser = @import("../filter/parser.zig");
const membership = @import("../filter/membership.zig");
const nanprobe = @import("../filter/nanprobe.zig");
const Diag = @import("../diag.zig").Diag;
const scan_block_merge = @import("block_merge.zig");
const BlockMerge = scan_block_merge.BlockMerge;
const owned_merge = @import("owned_merge.zig");
const mergeAccOwned = owned_merge.mergeAccOwned;
const mergeOwnedAccumulator = owned_merge.mergeOwnedAccumulator;
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const key_decode = @import("key_decode.zig");
const KeyType = key_decode.KeyType;
const keyTypeFromExpr = key_decode.keyTypeFromExpr;
const scan_footers = @import("footers.zig");
const parseFooters = scan_footers.parseFooters;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const pairs_policy = @import("pairs_policy.zig");
const distinctPairsCall = pairs_policy.distinctPairsCall;
const pairsPay = pairs_policy.pairsPay;
const pairsPayRuntime = pairs_policy.pairsPayRuntime;
const scan_worker = @import("worker.zig");
const canonicalFailurePrefix = scan_worker.canonicalFailurePrefix;
const firstWorkerFailure = scan_worker.firstWorkerFailure;
const ownedWinnerBytes = scan_worker.ownedWinnerBytes;
const decodeHeld = scan_worker.decodeHeld;
const releaseRegions = scan_worker.releaseRegions;
const releaseRegionsParallel = scan_worker.releaseRegionsParallel;
const runWorkers = scan_worker.runWorkers;
const scan_materialize = @import("materialize.zig");
const materializeOne = scan_materialize.materializeOne;
const plan_types = @import("plan_types.zig");
const PlanInputs = plan_types.PlanInputs;
const AggregateDispatch = plan_types.AggregateDispatch;
const PlanFacts = plan_types.PlanFacts;
const Planning = plan_types.Planning;
const AggregateExplanation = plan_types.AggregateExplanation;
const RecordAdmission = plan_types.RecordAdmission;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const countCut = scan_count_cut.countCut;
const keepCountTop = scan_count_cut.keepCountTop;
const scan_rank_cut = @import("rank_cut.zig");
const resultCut = scan_rank_cut.resultCut;
const resultLimitGroups = scan_rank_cut.resultLimitGroups;
const keepResultTop = scan_rank_cut.keepResultTop;
const scan_result = @import("result.zig");
const Timings = scan_result.Timings;
const MultiAggResult = scan_result.MultiAggResult;
const scan_binding = @import("binding.zig");
const KeyNames = scan_binding.KeyNames;
const dependentGroupKeys = scan_binding.dependentGroupKeys;
const nameUnaliasedAggs = scan_binding.nameUnaliasedAggs;
const checkAggAliases = scan_binding.checkAggAliases;
const bindGroupOutputs = scan_binding.bindGroupOutputs;
const checkHavingLanes = scan_binding.checkHavingLanes;
const sqlBound = scan_binding.sqlBound;
const leafSchemaElem = scan_binding.leafSchemaElem;
const scan_plan_impl = @import("plan.zig");
const partAggSupported = scan_plan_impl.partAggSupported;
const fewEntries = scan_plan_impl.fewEntries;
const provenPassShare = scan_plan_impl.provenPassShare;
const passShare = scan_plan_impl.passShare;
const selective_share = scan_plan_impl.selective_share;
const few_union = scan_plan_impl.few_union;
const orderSensitive = scan_plan_impl.orderSensitive;
const group_blocks_max = scan_plan_impl.group_blocks_max;
const keysLookUnique = scan_plan_impl.keysLookUnique;
const sketchEligible = scan_plan_impl.sketchEligible;
const filterSmall = scan_plan_impl.filterSmall;
const firstCutEligible = scan_plan_impl.firstCutEligible;
const rankingCall = scan_plan_impl.rankingCall;

const planAggregate = scan_plan_impl.planAggregate;
const sampledPassShare = scan_plan_impl.sampledPassShare;
const selectedGroupBytes = scan_plan_impl.selectedGroupBytes;
const few_entries = scan_plan_impl.few_entries;
const scan_rows = @import("rows.zig");
const RowSource = scan_rows.RowSource;
const buildGroupRows = scan_rows.buildGroupRows;
const answerBytesParallel = scan_rows.answerBytesParallel;
const buildGroupColumns = scan_rows.buildGroupColumns;
const slot_sort = @import("slot_sort.zig");
const sortSlotsByFirst = slot_sort.sortSlotsByFirst;
const pair_fold = @import("pair_fold.zig");
const drainDistinctPairs = pair_fold.drainDistinctPairs;
const foldDistinctPairs = pair_fold.foldDistinctPairs;
const scan_census = @import("census.zig");
const streamProbeBytes = scan_census.streamProbeBytes;
const WinnerCharge = scan_census.WinnerCharge;
const denseLimit = scan_census.denseLimit;
const certifiedTopK = scan_census.certifiedTopK;
const firstAppearanceCut = scan_census.firstAppearanceCut;
const owner_support = @import("owner_support.zig");
const passThroughSource = owner_support.passThroughSource;
const boolean_answer = owner_support.boolean_answer;
const date_answer = owner_support.date_answer;
const input_rewind = @import("input_rewind.zig");
const rewindInputs = input_rewind.rewindInputs;
const owner_integer = @import("owner_integer.zig");
const IntegerCountBounds = owner_integer.IntegerCountBounds;
const inlineIntegerAnswer = owner_integer.inlineIntegerAnswer;
const owner_pair = @import("owner_pair.zig");
const inlinePairAnswer = owner_pair.inlinePairAnswer;
const countedCalls = owner_pair.countedCalls;
const scan_owner_distinct = @import("owner_distinct.zig");
const scalarDistinctWideParts = scan_owner_distinct.scalarDistinctWideParts;
const inlineDistinctAnswer = scan_owner_distinct.inlineDistinctAnswer;
const owner_bytes = @import("owner_bytes.zig");
const msdProbeHeaders = owner_bytes.msdProbeHeaders;
const msdWorkspace = owner_bytes.msdWorkspace;
const inlineMsdAnswer = owner_bytes.inlineMsdAnswer;
const inlineByteTopAnswer = owner_bytes.inlineByteTopAnswer;
const inlineByteAnswer = owner_bytes.inlineByteAnswer;
const partition_fill = @import("partition_fill.zig");
const fillFromPartitions = partition_fill.fillFromPartitions;
const owner_strings = @import("owner_strings.zig");
const inlineSharedStrings = owner_strings.inlineSharedStrings;
const owner_packed = @import("owner_packed.zig");
const inlineSelectedCount = owner_packed.inlineSelectedCount;
const inlinePackedAnswer = owner_packed.inlinePackedAnswer;
const scan_merge = @import("merge.zig");
const MergeGroupRef = scan_merge.MergeGroupRef;
const mergeCandidates = scan_merge.mergeCandidates;
const mergesInPlace = scan_merge.mergesInPlace;
const mergeInPlace = scan_merge.mergeInPlace;
const finishGroupRanges = scan_merge.finishGroupRanges;
const freeAccumulatorRanges = scan_merge.freeAccumulatorRanges;
const mergeFoldedRefs = scan_merge.mergeFoldedRefs;
const firstCandidatesParallel = scan_merge.firstCandidatesParallel;
const premergeLargeDistinctSets = scan_merge.premergeLargeDistinctSets;
const topKCandidates = scan_merge.topKCandidates;
const DistinctMergeTask = scan_merge.DistinctMergeTask;
const scan_private_decode = @import("private_decode.zig");
const PrivateDecode = scan_private_decode.PrivateDecode;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const Input = scan_types.Input;
const allRescannable = scan_types.allRescannable;
const FastPath = scan_types.FastPath;
const GroupColType = scan_types.GroupColType;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const typed_answer = @import("typed_answer.zig");
const scan_output = @import("output.zig");
const AggOutput = scan_output.AggOutput;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const AggOutputItem = output_values.AggOutputItem;

/// Whether `runMultiAggregate` probes Bloom filters itself: only for footers it parses from the whole file (every
/// input mapped), never for `metas` the caller passes, which the caller has probed through its own reader.
fn probesMembership(args: MultiAggArgs) bool {
    return args.metas == null and !args.scan_all;
}

/// Run a multi-file aggregate.
///
/// `gpa` is used for thread-shared scratch (per-RG arenas, per-worker
/// state). `arena` is used for results that outlive the function call:
/// `MultiAggResult.state` and the parsed metas live in this arena. The
/// answer, `MultiAggResult.output`, is gpa-owned; `MultiAggResult.deinit`
/// frees both.
pub fn runMultiAggregate(gpa: std.mem.Allocator, arena: std.mem.Allocator, args: MultiAggArgs) !MultiAggResult {
    // A budget the established path fits must not fail the query, nor may whether it fails depend on timing. A refused
    // scan runs again, exactly, each time on a narrower path: without every fast path it armed (they charge more of
    // their peak than the paths they stand in for, `FastPath`); then, where several workers' tables were refused, on
    // the forced partitions, a partition's groups folded at a time; then on one worker, which claims row groups in
    // order and so peaks the same every run, but only where one worker may fit (`FastPath.one_table_may_fit`): else
    // its one table must hold all the refused tables held and more, and it only refuses, many times slower. Decode
    // memory the query's ledger refused also runs on one worker (`FastPath.decode_memory`), and there once more on a
    // small decode budget, so the tables have nearly all of the ledger. A plain grouping a cost rule chose over a cut
    // gives way to that cut, whose refusal is final. Each step only narrows (`narrowed`): at most seven general scans,
    // plus the shared-string prefix, an admitted record scan and its serial decode retry before the general chain,
    // and each arms its paths only over inputs a scan reads again, whole, from the start.
    var attempt = args;
    var chain: Chain = .{ .parts_tried = args.partagg };
    const ledger_before = if (args.ledger) |l| l.used.load(.monotonic) else 0;
    var may_fit = false;
    while (true) {
        var taken: std.EnumSet(FastPath) = .{};
        attempt.taken = &taken;
        var merge_memory: core_memory.Charged = undefined;
        if (args.ledger) |l| {
            merge_memory = .{ .child = gpa, .budget = l };
            attempt.merge_memory = &merge_memory;
        }
        if (prepareAggregate(gpa, arena, attempt, false)) |r| return r else |e0| {
            const e = if (args.ledger != null) merge_memory.refusal(e0) else e0;
            if (e == error.CountRankedDistinctDeclined) {
                attempt.count_ranked_distinct = .off;
                try rewindInputs(arena, &attempt);
                continue;
            }
            if (e != error.ExceededMemoryBudget) return e;
            if (args.ledger != null and merge_memory.refused.load(.monotonic)) taken.insert(.merge_memory);
            // What a refused scan freed goes back to the OS too, or the next scan's peak stands on its leftovers.
            core_memory.trimHeap();
            may_fit = may_fit or taken.contains(.one_table_may_fit);
            var next = narrowed(args, attempt, taken, &chain, may_fit) orelse return e;
            if (attempt.inputs.ptr == args.inputs.ptr) try rewindInputs(arena, &next);
            // A refused scan, and the reader it abandoned, gave back all they drew: the next runs on the whole of what
            // the query has left.
            if (args.ledger) |l| std.debug.assert(l.used.load(.monotonic) == ledger_before);
            attempt = next;
        }
    }
}

/// Whether several tables hold copies of the same keys: of up to 256 keys spread over each, an eighth or more are in
/// another. A sample, and so only a cap on the reruns a refusal tries (`narrowed`), never a proof; no keys at all (an
/// early refusal) says nothing, and counts as copies.
fn holdCopies(tables: []const expr_agg.GroupTable) bool {
    var seen: usize = 0;
    var shared: usize = 0;
    for (tables, 0..) |*t, ti| {
        const n = t.groupCount();
        var g: usize = 0;
        while (g < n) : (g += @max(1, n / 256)) {
            seen += 1;
            const key = t.keyAt(g);
            for (tables, 0..) |*o, oi| if (oi != ti and o.findKey(key) != null) {
                shared += 1;
                break;
            };
        }
    }
    return seen == 0 or shared *| 8 >= seen;
}

/// Whether the query, planned as the one-worker rerun plans it (on one worker, its inputs mapped, no forced
/// partitions or cost-rule plain grouping), takes a cut, which holds far less than the tables a refusal held: one
/// worker's cells fit where several workers' did not, and a mapped input admits the first keys' cut.
fn oneWorkerCuts(arena: std.mem.Allocator, in: PlanInputs) !bool {
    var one = in;
    one.workers = 1;
    one.readers = false;
    one.args.parallelism = 1;
    one.args.partagg = false;
    one.args.plain_rules = false;
    const inputs = try arena.dupe(Input, in.args.inputs);
    for (inputs) |*input| if (input.plan_bytes) |b| {
        input.bytes = b;
        input.gate = null;
    };
    one.args.inputs = inputs;
    return switch (planAggregate(one).dispatch.strategy) {
        .sketch, .first_keys => true,
        .scalar, .plain_hash, .partitioned_hash => false,
    };
}

/// What a refusal chain has run so far (`narrowed`).
pub const Chain = struct {
    /// The forced partitions (`MultiAggArgs.partagg`), which append to runs; the cost rules' shared partitions
    /// (`FastPath.partitioned`) are not those, and leave it.
    parts_tried: bool,
    /// The one-worker scan has run again on a small decode budget (`narrowed`).
    lean_tried: bool = false,
};

/// The scan to run after `attempt` was refused with `taken` armed, or null when no narrower one can answer and the
/// refusal is the query's. `may_fit`: some refusal so far showed one worker's table may fit (`one_table_may_fit`).
pub fn narrowed(
    args: MultiAggArgs,
    attempt: MultiAggArgs,
    taken: std.EnumSet(FastPath),
    chain: *Chain,
    may_fit: bool,
) ?MultiAggArgs {
    // A finished windowed source may already have discarded its pages. No budget retry can read it again.
    if (!allRescannable(attempt.inputs)) return null;
    var next = attempt;
    if (taken.contains(.grouped_distinct_bitmap)) {
        next.grouped_distinct_bitmap = .off;
        return next;
    }
    if (taken.contains(.shared_strings)) {
        next.shared_strings = false;
        return next;
    }
    if (taken.contains(.msd_records)) {
        // Retained pages and decoder scratch have been released. Admission required complete rescannability,
        // so a bounded-decoder or unsupported-page refusal can enter the established chain from its start.
        next.msd_records = false;
        return next;
    }
    if (taken.contains(.integer_records)) {
        // Both owned-record layouts use this refusal/restart slot; a query's key type chooses only one layout.
        // Concurrent decode draws can race with one another. Try the same layout serially before giving it up,
        // so a budget its single decoder fits always answers. A width refusal is independent of decoder scheduling.
        if (taken.contains(.decode_memory) and attempt.parallelism != 1) {
            next.parallelism = 1;
            return next;
        }
        next.integer_records = false;
        next.byte_records = false;
        next.msd_records = false;
        next.parallelism = args.parallelism;
        return next;
    }
    if (taken.contains(.private_runs)) {
        next.private_runs = false;
        return next;
    }
    // A rerun never trades a cut for a plain table again. After a cost rule's plain grouping that is the cut it stood
    // in for, planned as if the rule had not run: the chain from here is the cut's.
    next.plain_rules = false;
    if (taken.contains(.plain_rule)) return next;
    if (taken.contains(.dense)) next.dense = false;
    if (taken.contains(.spill)) next.spill = false;
    if (taken.contains(.pairs)) next.distinct_pairs = .off;
    if (taken.contains(.partitioned)) next.partition_cost = false;
    if (taken.contains(.dense) or taken.contains(.spill) or taken.contains(.pairs) or taken.contains(.partitioned))
        return next;
    // Decode memory the ledger refused is least on one worker, whatever the tables held; and least of all there on a
    // ledger of `lean_decode` bytes of its own (the ceiling's slack), which leaves the tables all of the query's, as
    // they had beside an uncharged decode. What does not decode in that refuses.
    const one_decoder = (taken.contains(.decode_memory) or taken.contains(.merge_memory)) and attempt.parallelism != 1;
    if (taken.contains(.decode_memory) and attempt.parallelism == 1 and !chain.lean_tried) if (args.decode_limit) |l| {
        chain.lean_tried = true;
        // Half the ledger as the decode budget: a row group past it slices, on the room the other half leaves.
        next.decode_limit = @min(l, core_memory.lean_decode / 2);
        next.charge_decode = false;
        return next;
    };
    if (!taken.contains(.worker_tables) and !one_decoder) return null;
    if (taken.contains(.worker_tables) and taken.contains(.partitions_fit) and !chain.parts_tried) {
        next.partagg = true;
        chain.parts_tried = true;
        return next;
    }
    if (!may_fit and !one_decoder) return null;
    next.parallelism = 1;
    next.partagg = args.partagg;
    return next;
}

/// Shares all binding and work sizing with execution, but stops before allocating workers or aggregate tables.
pub fn explainAggregate(gpa: std.mem.Allocator, arena: std.mem.Allocator, args: MultiAggArgs) !AggregateExplanation {
    return prepareAggregate(gpa, arena, args, true);
}

pub fn prepareAggregate(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    comptime explain: bool,
) !if (explain) AggregateExplanation else MultiAggResult {
    if (args.inputs.len == 0) return error.NoInputs;

    // Parallel because footer parsing scales with file count and schema width and can dominate short multi-file scans.
    // `meta_arenas` must outlive this call: `metas` points into them.
    var meta_arenas: []std.heap.ArenaAllocator = &.{};
    defer for (meta_arenas) |*a| a.deinit();

    const t_parse = nowMonoNs();
    const metas: []const schema.FileMetaData = if (args.metas) |m| blk: {
        if (m.len != args.inputs.len) return error.SchemaMismatch;
        break :blk m;
    } else blk: {
        switch (try parseFooters(arena, gpa, args.inputs, args.parallelism, &meta_arenas, args.profiler)) {
            .ok => |parsed| break :blk parsed,
            .failed => |f| {
                Diag.setInput(args.diag, args.inputs[f.index].name, f.err);
                return switch (f.err) {
                    error.EncryptedFile => error.EncryptedFile,
                    error.ExternalColumnChunk => error.ExternalColumnChunk,
                    error.InvalidDecimalType => error.InvalidDecimalType,
                    else => error.NotParquet,
                };
            },
        }
    };
    consumer.setDecodeFailure(null);

    var probe: FailureProbe = .{ .limit = args.max_memory };
    defer probe.releaseGates();
    var execution = args;
    execution.failure_probe = &probe;
    return prepareAggregateWithMetas(gpa, arena, execution, explain, metas, t_parse) catch |err| {
        if (!consumer.isDecodeError(err)) return err;
        defer consumer.resolveDecodeFailure(args.diag, metas, args.inputs);
        const p = probe.context orelse return err;
        // Local read windows can return their entire charge before the bounded probe reads the mapping.
        if (args.start_reads) |start| if (start.abandon) |abandon| abandon(start.ctx);
        var own: core_memory.SharedBudget = .{ .limit = args.max_memory };
        const ledger = args.ledger orelse &own;
        // Independent of worker count and cache windows. The outer ledger accounts for any still-live input bytes.
        var bound: core_memory.SharedBudget = .{ .limit = args.max_memory, .parent = ledger };
        var charged: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &bound };
        const failure = canonicalFailurePrefix(charged.allocator(), p);
        consumer.setDecodeFailure(failure.site);
        return failure.err;
    };
}

const BoundQueryPhase = struct {
    calls: []expr_agg.AggCall,
    filter: ?filter_ast.Filter,
    keys: ?[]const expr_ast.Expr,
    outputs: ?[]const BoundOutputColumn,
    error_keys: ?[]const expr_ast.Expr,
    error_outputs: ?[]const BoundOutputColumn,
    sources: []?schema.SchemaElement,
};

fn validateAggregateSchemas(args: MultiAggArgs, metas: []const schema.FileMetaData) !void {
    // Validate schemas match the first file's: same number of leaves
    // and same names *case-insensitively*. Real-world datasets drift on
    // capitalization (NYC Taxi flips `Airport_fee` ↔ `airport_fee`
    // mid-2023). Strict-position-strict-name is too strict; full schema
    // evolution is a bigger lift we defer.
    const meta0 = &metas[0];
    // Columns are bound by leaf index against file 0, so the nesting must match too: the same names in the same order
    // can still put a leaf under a group in one file and at the top level in another.
    for (metas[1..], 1..) |*m, i| {
        const at = metadata.schemaMismatch(meta0.schema.items, m.schema.items, .values) orelse continue;
        if (at >= m.schema.items.len or at >= meta0.schema.items.len) return error.SchemaMismatch;
        Diag.setInput(args.diag, args.inputs[i].name, null);
        Diag.set(args.diag, meta0.schema.items[at].name, .{ .schema_mismatch = .{
            .file = i,
            .first_input = args.inputs[0].name,
            .other = .of(m.schema.items[at].name),
        } });
        return error.SchemaMismatch;
    }
}

fn bindAggregateQuery(arena: std.mem.Allocator, args: MultiAggArgs, meta0: *const schema.FileMetaData) !BoundQueryPhase {
    // 2. Parse aggregate + filter against meta0 (column indexes resolved
    //    against file 0; valid for all files because schemas match).
    const agg_calls: []expr_agg.AggCall = if (args.bound_aggregate) |bound|
        try arena.dupe(expr_agg.AggCall, bound.calls)
    else if (args.group_by != null and args.aggregate.len == 0)
        &.{}
    else
        try expr_parser.parseAggList(arena, args.aggregate, meta0, args.diag);
    const grouped = args.grouped();
    if (agg_calls.len == 0 and !grouped) return error.EmptyAggregate;

    var filter_opt: ?filter_ast.Filter = if (args.bound_aggregate) |bound| bound.filter else null;
    if (args.bound_aggregate == null) if (args.filter) |expr_str| {
        if (expr_str.len > 0) {
            filter_opt = try filter_parser.parse(arena, expr_str, meta0, args.diag);
            // `--trust-stats`: leaves trust plausible statistics; a float chunk with no NaN count prunes as NaN-free.
            if (args.trust_stats) filter_opt = try filter_ast.trustStats(arena, filter_opt.?);
        }
    };
    const group_by_items = if (args.bound_aggregate != null) null else if (args.group_by) |gb_str|
        try expr_parser.parseGroupByOf(arena, gb_str, meta0, args.diag, args.select_cols orelse &.{})
    else
        null;
    var group_by_keys: ?[]const expr_ast.Expr = if (args.bound_aggregate) |bound| bound.keys else if (group_by_items) |items| blk: {
        const exprs = try arena.alloc(expr_ast.Expr, items.len);
        for (items, 0..) |item, i| exprs[i] = item.expr;
        break :blk exprs;
    } else null;
    try refuseRepeated(arena, meta0, agg_calls, group_by_keys, args.diag);
    var key_names = try KeyNames.init(arena, meta0, group_by_items orelse &.{});
    key_names.sql = sqlBound(args.select_cols);
    if (args.bound_aggregate == null) try nameUnaliasedAggs(arena, agg_calls, &key_names);
    const agg_sources = try arena.alloc(?schema.SchemaElement, agg_calls.len);
    for (agg_calls, agg_sources) |call, *src| src.* = passThroughSource(call, meta0);
    var group_outputs: ?[]const BoundOutputColumn = if (args.bound_aggregate) |bound| bound.outputs else if (group_by_items != null)
        try bindGroupOutputs(arena, &key_names, agg_calls, meta0, args)
    else blk: {
        try checkAggAliases(agg_calls, args.diag);
        break :blk null;
    };
    if (args.result_order.having) |h| try checkHavingLanes(arena, h.*, group_outputs, agg_calls, group_by_keys);
    const error_group_keys = group_by_keys;
    const error_outputs = group_outputs;
    if (args.dependent_keys and !args.unordered) if (group_by_keys) |keys| {
        const dependencies = try dependentGroupKeys(arena, keys, group_outputs.?);
        group_by_keys = dependencies.keys;
        group_outputs = dependencies.outputs;
    };
    return .{
        .calls = agg_calls,
        .filter = filter_opt,
        .keys = group_by_keys,
        .outputs = group_outputs,
        .error_keys = error_group_keys,
        .error_outputs = error_outputs,
        .sources = agg_sources,
    };
}

const WorkPhase = struct {
    items: std.ArrayList(WorkItem),
    total_rgs: usize,
    requested: usize,
    chunks_per_rg: usize,
    by_bytes: usize,
    workers: usize,
};

fn buildWorkPhase(
    comptime explain: bool,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: agg_plan.Plan,
    agg_calls: []const expr_agg.AggCall,
    group_by_keys: ?[]const expr_ast.Expr,
) !WorkPhase {
    // 4. Build the flat row-group work list. If we have fewer row groups
    //    than the requested parallelism (e.g. wide scans on few large row groups),
    //    we split each row group's aggregates into chunks (horizontal scaling/sub-RG).
    var raw_work_items: std.ArrayList(struct { file: usize, rg: usize }) = .empty;
    for (metas, 0..) |*m, fi| {
        for (0..m.row_groups.items.len) |ri| try raw_work_items.append(arena, .{ .file = fi, .rg = ri });
    }
    const total_rgs = raw_work_items.items.len;

    const cpu_count = std.Thread.getCpuCount() catch 1;
    const requested = if (args.parallelism == 0) cpu_count else args.parallelism;

    // No row groups (a valid empty file) is `total_rgs == 0`: no work items, and nothing to divide by.
    var chunks_per_rg = if (total_rgs == 0 or total_rgs >= requested) @as(usize, 1) else (requested + total_rgs - 1) / total_rgs;
    if (group_by_keys != null) {
        chunks_per_rg = 1;
    } else {
        if (chunks_per_rg > agg_calls.len) chunks_per_rg = agg_calls.len;
        if (chunks_per_rg < 1) chunks_per_rg = 1;
    }

    // Each chunk of aggregates reads its own columns, the same in every row group, and so does its full-match subset.
    const chunk_columns = try arena.alloc(agg_plan.Columns, chunks_per_rg);
    const base_chunk_size = agg_calls.len / chunks_per_rg;
    const remainder = agg_calls.len % chunks_per_rg;
    const chunk_starts = try arena.alloc(usize, chunks_per_rg + 1);
    for (chunk_starts, 0..) |*start, chunk_idx| start.* = chunk_idx * base_chunk_size + @min(chunk_idx, remainder);
    for (chunk_columns, 0..) |*cols, chunk_idx| {
        cols.* = if (chunks_per_rg == 1)
            plan.columns
        else
            try plan.forCalls(arena, agg_calls[chunk_starts[chunk_idx]..chunk_starts[chunk_idx + 1]]);
    }

    var work_items: std.ArrayList(WorkItem) = .empty;
    // Workers fold string min/max winners into the items' accumulators, and the items own count(DISTINCT) sets from
    // here on; merging below moves both out, so this frees only what a failure leaves behind.
    errdefer for (work_items.items) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
    for (raw_work_items.items) |raw| {
        for (chunk_columns, 0..) |cols, chunk_idx| {
            const agg_start = chunk_starts[chunk_idx];
            const agg_len = chunk_starts[chunk_idx + 1] - agg_start;

            // Initialize accumulators for this chunk
            const sub_accs = if (explain or group_by_keys != null)
                try arena.alloc(expr_agg.Accumulator, 0)
            else blk: {
                const arr = try arena.alloc(expr_agg.Accumulator, agg_len);
                for (agg_calls[agg_start .. agg_start + agg_len], arr) |call, *acc| acc.* = expr_agg.Accumulator.init(call);
                break :blk arr;
            };

            try work_items.append(arena, .{
                .file = raw.file,
                .rg = raw.rg,
                .agg_start = agg_start,
                .agg_len = agg_len,
                .columns = cols,
                .accumulators = sub_accs,
            });
            // The sets once the item owns them, so a failure making one frees the others.
            for (agg_calls[agg_start..][0..sub_accs.len], sub_accs) |call, *acc| if (call.func == .count_distinct) {
                const ds = try gpa.create(expr_agg.DistinctSet);
                ds.* = .{};
                acc.* = .{ .distinct = .fromSet(ds) };
            };
        }
    }

    // Size the worker set by BYTES to decode, not row-group count: thread creation has a fixed cost and a query may
    // touch only a small part of a wide schema. 2 MiB per worker (`min_bytes_per_worker`) is a conservative floor.
    var fetch_bytes: u64 = 0;
    for (work_items.items) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        for (rg.columns.items, 0..) |col, ci| {
            if (ci < item.columns.read.len and item.columns.read[ci]) {
                // Larger of two proxies: uncompressed bytes makes BOOLEAN (8 rows per byte) and dictionary-encoded
                // strings (index stream only) look nearly free, so `rows x 4` floors the cost they pay when
                // materialized.
                if (col.meta_data) |cm| {
                    const uncompressed: u64 = @intCast(cm.total_uncompressed_size);
                    const by_rows: u64 = @as(u64, @intCast(rg.num_rows)) *| 4;
                    fetch_bytes +|= @max(uncompressed, by_rows);
                }
            }
        }
    }
    const by_bytes = @max(@as(usize, 1), @as(usize, @intCast(fetch_bytes / @max(1, args.min_bytes_per_worker))));
    const n_workers = @max(@as(usize, 1), @min(@min(requested, work_items.items.len), by_bytes));
    return .{
        .items = work_items,
        .total_rgs = total_rgs,
        .requested = requested,
        .chunks_per_rg = chunks_per_rg,
        .by_bytes = by_bytes,
        .workers = n_workers,
    };
}

const EarlyOwnerPhase = struct {
    answer: ?MultiAggResult = null,
    string_path: ?RecordAdmission = null,
    pair_path: ?RecordAdmission = null,
    integer_needs_union: bool = false,
};

fn tryEarlyOwners(
    comptime explain: bool,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: *MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work_items: []const WorkItem,
    agg_calls: []const expr_agg.AggCall,
    group_by_keys: ?[]const expr_ast.Expr,
    group_outputs: ?[]const BoundOutputColumn,
    n_workers: usize,
    t: Timings,
) !EarlyOwnerPhase {
    var string_record_path: ?RecordAdmission = null;
    if (explain) {
        string_record_path = try inlineSharedStrings(
            gpa,
            arena,
            args.*,
            metas,
            plan,
            work_items,
            agg_calls,
            group_by_keys,
            group_outputs,
            n_workers,
            t,
            true,
        );
    } else if (try inlineSharedStrings(
        gpa,
        arena,
        args.*,
        metas,
        plan,
        work_items,
        agg_calls,
        group_by_keys,
        group_outputs,
        n_workers,
        t,
        false,
    )) |result| return .{ .answer = result };

    if (!explain) if (try inlineDistinctAnswer(
        gpa,
        arena,
        args.*,
        metas,
        plan,
        work_items,
        agg_calls,
        n_workers,
        t,
        false,
    )) |result| return .{ .answer = result };

    var pair_record_path: ?RecordAdmission = null;
    if (explain) {
        pair_record_path = try inlinePairAnswer(
            gpa,
            arena,
            args.*,
            metas,
            plan,
            work_items,
            agg_calls,
            group_by_keys,
            group_outputs,
            n_workers,
            t,
            true,
        );
    } else if (try inlinePairAnswer(
        gpa,
        arena,
        args.*,
        metas,
        plan,
        work_items,
        agg_calls,
        group_by_keys,
        group_outputs,
        n_workers,
        t,
        false,
    )) |result| return .{ .answer = result };

    var integer_needs_union = false;
    if (!explain) if (try inlineIntegerAnswer(
        gpa,
        arena,
        args,
        metas,
        plan,
        work_items,
        agg_calls,
        group_by_keys,
        group_outputs,
        n_workers,
        t,
        .{ .pending = &integer_needs_union },
        false,
    )) |result| return .{ .answer = result };

    if (!explain) if (try inlineByteTopAnswer(
        gpa,
        arena,
        args.*,
        metas,
        plan,
        work_items,
        agg_calls,
        group_by_keys,
        group_outputs,
        n_workers,
        t,
        false,
    )) |result| return .{ .answer = result };

    if (!explain) if (try inlineByteAnswer(
        gpa,
        arena,
        args.*,
        metas,
        plan,
        work_items,
        agg_calls,
        group_by_keys,
        group_outputs,
        n_workers,
        t,
        false,
    )) |result| return .{ .answer = result };

    return .{
        .string_path = string_record_path,
        .pair_path = pair_record_path,
        .integer_needs_union = integer_needs_union,
    };
}

const ProbePhase = struct {
    ndv_stat: ?NdvStat,
    facts: PlanFacts,
    planning_released: bool,
};

fn probeAggregateInputs(
    comptime explain: bool,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    agg_calls: []const expr_agg.AggCall,
    group_by_keys: ?[]const expr_ast.Expr,
    n_workers: usize,
    plan_inputs: *const PlanInputs,
    msd_probe: ?u64,
    domain: *?agg_dense.Domain,
) !ProbePhase {
    var planning_released = false;
    var ndv_stat: ?NdvStat = null;
    var probe_facts: PlanFacts = .{};
    var planning_why: ?@FieldType(Planning, "why") = null;
    if (args.ndv_policy) if (group_by_keys) |keys| {
        // Prefix discovery takes precedence; refining a global group bound cannot improve that decision.
        const first = planAggregate(plan_inputs.*).dispatch.strategy;
        const want = args.dense and agg_dense.shapeOk(agg_calls, keys);
        // A reader's union is read before its scan can start: only where some choice can turn on it (a cut the bound
        // can drop, the dense fold's domain, count(DISTINCT) pairs). Mapped inputs union as they always have.
        const union_matters = first == .sketch or want or for (agg_calls) |c| {
            if (c.func == .count_distinct) break true;
        } else false;
        // A global group bound refines nothing for prefix discovery, unless the footers say the codes may resolve few
        // entries in all: the cost rules may then group plainly (`fewEntries`), and the union decides it.
        const guess = ndv.footerEntries(metas, keys) orelse std.math.maxInt(u64);
        const prefix = first == .first_keys and guess > few_entries;
        const allow_union = args.ndv_union and !prefix and (!plan_inputs.readers or union_matters);
        planning_why = if (!args.ndv_union)
            .union_disabled
        else if (prefix) .prefix_cut else if (!allow_union) .reader_no_choice else null;
        // Beside a reader the union may be read for the fold alone: then not where the fold would decline it.
        const fold_only = plan_inputs.readers and first != .sketch and for (agg_calls) |c| {
            if (c.func == .count_distinct) break false;
        } else true;
        const fold: ndv.FoldOnly = if (fold_only) n_workers else null;
        const ranges = try ndv.probeRanges(arena, metas, keys, args.max_memory, true, allow_union, want, fold);
        // A reader holds what it probes beside its window, page by page as it reads it: past the cap its inputs plan
        // from their footers, as they did with no probe. A mapped input reads its dictionaries as it always has.
        const page = std.heap.pageSize();
        var probe_bytes: u64 = msd_probe orelse 0;
        for (ranges) |r| if (args.inputs[r.file].readerBacked()) {
            probe_bytes +|= std.mem.alignForward(u64, r.end, page) - std.mem.alignBackward(u64, r.start, page);
        };
        const probe_ok = probe_bytes <= args.probe_cap orelse ndv.probeCap(args.max_memory);
        if (plan_inputs.readers) {
            probe_facts.probe_ranges = ranges.len;
            probe_facts.probe_bytes = probe_bytes;
            probe_facts.probe_capped = !probe_ok;
        }
        const files = try arena.alloc(ndv.File, args.inputs.len);
        // Dictionaries are read from a mapped file, or from a reader's buffer once its probe of them is in. Explain
        // reads a reader's input through its mapping, as the probe would read it. Without either (S3, a probe past
        // its cap, or a reader no probe was asked of) an input plans from its footer.
        for (args.inputs, files) |input, *file| {
            const waits = input.readerBacked() and input.dict_wait != null;
            const probed = input.readerBacked() and probe_ok and (waits or explain);
            const bytes = if (waits) input.bytes else input.plan_bytes orelse input.bytes;
            file.* = .{
                .bytes = bytes,
                .local = !std.mem.startsWith(u8, input.name, "s3://") and (input.gate == null or probed) and
                    (input.logical_size == 0 or input.logical_size == bytes.len),
                .probed = probed,
                .wait = if (probed and waits) input.dict_wait else null,
            };
        }
        if (!explain) if (args.dict_probe) |dp| for (ranges) |r| {
            const in = args.inputs[r.file];
            if (in.gate == null or (in.readerBacked() and probe_ok)) try dp.add(dp.ctx, r.file, r.rg, r.start, r.end);
        };
        // The reader reads the probes first, then streams row groups into its window while the dictionaries union.
        if (!explain) if (args.start_reads) |s| try s.start(s.ctx);
        ndv_stat = try ndv.collectRetaining(
            gpa,
            arena,
            files,
            metas,
            keys,
            n_workers,
            args.max_memory,
            true,
            allow_union,
            if (want) domain else null,
            // A prefix plan reads the union only for `fewEntries`: none past its entries, and none past its values.
            .{ .fold_only = fold_only, .limit = if (first == .first_keys) few_union else null },
        );
        if (!explain) if (args.start_reads) |s| if (s.planned) |planned| {
            planned(s.ctx);
            planning_released = true;
        };
        const st = ndv_stat.?;
        probe_facts.planning = .{
            .source = switch (st.source) {
                .unknown => .footers,
                .dictionary_headers => .dictionary_headers,
                .dictionary_union => .dictionary_union,
            },
            .why = if (st.union_complete)
                (if (plan_inputs.readers) .reader_probe else .mapped)
            else if (planning_why) |w|
                w
            else if (plan_inputs.readers and !probe_ok)
                .probe_capped
            else if (st.fold_declined)
                .fold_declines
            else if (st.union_budget_exceeded)
                .union_budget
            else if (st.union_attempted) .union_incomplete else .no_dictionaries,
        };
    };
    if (!explain) if (args.start_reads) |s| try s.start(s.ctx);
    return .{ .ndv_stat = ndv_stat, .facts = probe_facts, .planning_released = planning_released };
}

fn prepareAggregateWithMetas(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    initial_args: MultiAggArgs,
    comptime explain: bool,
    metas: []const schema.FileMetaData,
    t_parse: i64,
) !if (explain) AggregateExplanation else MultiAggResult {
    var args = initial_args;
    var t: Timings = .{};
    const prof = profile.threadOf(args.profiler, .main);

    try validateAggregateSchemas(args, metas);
    const meta0 = &metas[0];
    const bound = try bindAggregateQuery(arena, args, meta0);
    const agg_calls = bound.calls;
    const filter_opt = bound.filter;
    const group_by_keys = bound.keys;
    const group_outputs = bound.outputs;
    const error_group_keys = bound.error_keys;
    const error_outputs = bound.error_outputs;
    const agg_sources = bound.sources;
    if (!explain) if (args.diag) |diag| {
        diag.execution_started = true;
    };
    const t_parse_end = nowMonoNs();
    t.parse_ns = @intCast(t_parse_end - t_parse);
    profile.record(prof, .parse_query, t_parse, t_parse_end, profile.args(.{}));
    if (args.profiler) |p| p.setColumnsFromMeta(meta0);
    const plan_span = profile.Span.begin(prof);

    if (!explain) if (try countRankedDistinct(gpa, arena, args, metas, agg_calls, group_by_keys, group_outputs, filter_opt)) |r|
        return r;

    // 3. What the query reads, and which columns statistics answer outright. The S3 planner makes the same plan
    //    from the same footers, so every column read here was fetched.
    const plan: agg_plan.Plan = try .init(arena, .{
        .filter = filter_opt,
        .calls = agg_calls,
        .group_keys = group_by_keys,
    }, metas, .fromFlags(args.scan_all, args.trust_stats));

    // 3b. Bloom filters, before any worker decides a row group: every decision then reads the same footers.
    var membership_stats: membership.Stats = .{};
    if (!explain) if (filter_opt) |f| if (probesMembership(args)) {
        const files = try arena.alloc([]const u8, args.inputs.len);
        for (args.inputs, files) |in, *b| b.* = in.bytes;
        const mapped: membership.MappedSource = .{ .files = files };
        membership_stats = try membership.load(arena, gpa, f, metas, mapped.source(), .{ .threads = args.parallelism });
        _ = try nanprobe.load(gpa, f, metas, mapped.source(), .{ .threads = args.parallelism, .compressed = false });
    };

    const work_phase = try buildWorkPhase(explain, gpa, arena, args, metas, plan, agg_calls, group_by_keys);
    const work_items = work_phase.items;
    errdefer for (work_items.items) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);
    const total_rgs = work_phase.total_rgs;
    const requested = work_phase.requested;
    const chunks_per_rg = work_phase.chunks_per_rg;
    const by_bytes = work_phase.by_bytes;
    var n_workers = work_phase.workers;
    plan_span.end(.plan, profile.args(.{ work_items.items.len, n_workers, total_rgs }));

    const early = try tryEarlyOwners(explain, gpa, arena, &args, metas, &plan, work_items.items, agg_calls, group_by_keys, group_outputs, n_workers, t);
    if (!explain) if (early.answer) |result| return result;
    const string_record_path = early.string_path;
    const pair_record_path = early.pair_path;
    const integer_needs_union = early.integer_needs_union;

    var plan_inputs: PlanInputs = .{
        .args = args,
        .metas = metas,
        .work = work_items.items,
        .calls = agg_calls,
        .keys = group_by_keys,
        .outputs = group_outputs,
        .filter_cols = plan.filter_cols,
        .workers = n_workers,
        .filter = filter_opt,
        .readers = for (args.inputs) |in| {
            if (in.readerBacked()) break true;
        } else false,
    };
    const msd_candidate = (try inlineMsdAnswer(gpa, arena, args, metas, &plan, work_items.items, agg_calls, group_by_keys, group_outputs, n_workers, t, true)) != null;
    const msd_probe = if (msd_candidate) try msdProbeHeaders(args, metas, work_items.items, group_by_keys.?, explain) else null;
    // The retained domain belongs to the outer execution phase, including failures during probing.
    var domain: ?agg_dense.Domain = null;
    defer if (domain) |*d| d.deinit(gpa);
    const probe = try probeAggregateInputs(explain, gpa, arena, args, metas, agg_calls, group_by_keys, n_workers, &plan_inputs, msd_probe, &domain);
    const ndv_stat = probe.ndv_stat;
    const probe_facts = probe.facts;
    const planning_released = probe.planning_released;
    plan_inputs.ndv_stat = ndv_stat;
    if (!explain and integer_needs_union) {
        const bounds: IntegerCountBounds = .{ .refined = ndv_stat };
        if (try inlineIntegerAnswer(
            gpa,
            arena,
            &args,
            metas,
            &plan,
            work_items.items,
            agg_calls,
            group_by_keys,
            group_outputs,
            n_workers,
            t,
            bounds,
            true,
        )) |_| {
            // This owner does not retain the union. Release it before its independently quoted records draw.
            if (domain) |*d| d.deinit(gpa);
            domain = null;
            if (try inlineIntegerAnswer(
                gpa,
                arena,
                &args,
                metas,
                &plan,
                work_items.items,
                agg_calls,
                group_by_keys,
                group_outputs,
                n_workers,
                t,
                bounds,
                false,
            )) |answer| return answer;
        }
    }
    // Generic composite states share their cache-bound admission with execution and EXPLAIN.
    const dense_supported = if (domain) |*d|
        d.axes.len == 1 or agg_dense.countOnly(agg_calls) or agg_dense.sumCall(agg_calls) != null or
            (!agg_dense.orderSensitive(agg_calls) and agg_dense.numericFits(d.ids(), agg_calls))
    else
        false;
    var dense_profitable = true;
    if (args.encoded and dense_supported) if (domain) |*d| {
        const initial = planAggregate(plan_inputs).dispatch;
        if (d.globalFits(n_workers) and allRescannable(args.inputs) and !initial.use_blocks) {
            const stride = expr_agg.fixedGroupStride(group_by_keys.?) orelse 0;
            const quote = agg_dense.charge(
                d,
                n_workers,
                agg_calls,
                ndv_stat.?.rows,
                stride,
                denseLimit(args, agg_calls, group_outputs),
            ).total;
            // Uniform bounds can hide skew. Before replacing dense by survivor-sized state, require a proven
            // share or count a sample. A budget retry disables this cost rule and can restore the lanes.
            if (args.plain_rules and filter_opt != null and passShare(plan_inputs) <= selective_share and
                denseLimit(args, agg_calls, group_outputs) != null and
                quote > selectedGroupBytes(ndv_stat.?, agg_calls.len, n_workers, passShare(plan_inputs)))
            {
                var share = provenPassShare(plan_inputs);
                if (share > selective_share) {
                    plan_inputs.sampled_pass = try sampledPassShare(
                        gpa,
                        arena,
                        &plan,
                        metas,
                        args.inputs,
                        work_items.items,
                        .{
                            .fast_levels = args.fast_levels,
                            .level_runs = args.level_runs,
                            .encoded = args.encoded,
                            .expand_limit = args.max_memory,
                        },
                        args.decode_limit,
                    );
                    plan_inputs.sample_unread = plan_inputs.sampled_pass == null;
                    share = if (plan_inputs.sampled_pass) |sampled| share * sampled else 1;
                }
                if (share <= selective_share)
                    dense_profitable = quote <= selectedGroupBytes(ndv_stat.?, agg_calls.len, n_workers, share);
            }
            if (dense_profitable) plan_inputs.dense_bytes = quote;
        }
    };
    var scan_plan = planAggregate(plan_inputs);
    var selected_record_path: ?RecordAdmission = null;
    // Keep the accepted dense choice. Sparse typed identities otherwise have one shared record/finish quote.
    const payload_top = if (agg_plan.OwnerProgram.init(agg_calls)) |p|
        p.len != 0 and p.distinct_arg == null and args.result_order.limit != null
    else
        false;
    if (plan_inputs.dense_bytes == null or !dense_profitable or payload_top) {
        if (explain) {
            selected_record_path = try inlineSelectedCount(
                gpa,
                arena,
                args,
                metas,
                &plan,
                work_items.items,
                agg_calls,
                group_by_keys,
                group_outputs,
                if (domain) |*d| d else null,
                ndv_stat,
                if (plan_inputs.sampled_pass) |sampled|
                    provenPassShare(plan_inputs) * sampled
                else
                    passShare(plan_inputs),
                n_workers,
                t,
                true,
            );
        } else if (try inlineSelectedCount(
            gpa,
            arena,
            args,
            metas,
            &plan,
            work_items.items,
            agg_calls,
            group_by_keys,
            group_outputs,
            if (domain) |*d| d else null,
            ndv_stat,
            if (plan_inputs.sampled_pass) |sampled|
                provenPassShare(plan_inputs) * sampled
            else
                passShare(plan_inputs),
            n_workers,
            t,
            false,
        )) |answer| return answer;
    }
    var packed_record_path: ?RecordAdmission = null;
    // Keep the established small-domain table/dense choice; owners replace a multi-pass count cut.
    if (plan_inputs.readers or scan_plan.dispatch.strategy == .sketch or !dense_profitable) if (domain) |*d| {
        if (explain) {
            packed_record_path = try inlinePackedAnswer(
                gpa,
                arena,
                args,
                metas,
                &plan,
                work_items.items,
                agg_calls,
                group_by_keys,
                group_outputs,
                d,
                n_workers,
                t,
                true,
            );
        } else if (try inlinePackedAnswer(
            gpa,
            arena,
            args,
            metas,
            &plan,
            work_items.items,
            agg_calls,
            group_by_keys,
            group_outputs,
            d,
            n_workers,
            t,
            false,
        )) |answer| return answer;
    };
    var msd_args = args;
    msd_args.msd_records = msd_probe != null;
    // Low-NDV composite keys benefit from reducing rows directly. This existing bound only declines a layout.
    if (args.msd_records_min_rows != 0) if (ndv_stat) |st| if (st.hi < st.rows / 4) {
        msd_args.msd_records = false;
    };
    if (msd_args.msd_records) msd_args.msd_working_bytes = msdWorkspace(args, metas, work_items.items, group_by_keys.?, n_workers, explain);
    if (msd_args.msd_working_bytes == null) msd_args.msd_records = false;
    if (planning_released) {
        if (msd_args.start_reads) |*starter| starter.planned = null;
    }
    if (!explain) if (inlineMsdAnswer(gpa, arena, msd_args, metas, &plan, work_items.items, agg_calls, group_by_keys, group_outputs, n_workers, t, false) catch |e| switch (e) {
        error.OutOfMemory, error.RetainedEncodingUnsupported, error.RetainedOwnershipViolation => return error.ExceededMemoryBudget,
        else => return e,
    }) |result| return result;
    if (!explain and !dense_profitable) if (args.taken) |taken| taken.insert(.plain_rule);
    if (scan_plan.wants_sample) {
        const options: consumer.DecodeOptions = .{
            .fast_levels = args.fast_levels,
            .level_runs = args.level_runs,
            .encoded = args.encoded,
            .expand_limit = args.max_memory,
        };
        const work = work_items.items;
        const limit = args.decode_limit;
        plan_inputs.sampled_pass = try sampledPassShare(gpa, arena, &plan, metas, args.inputs, work, options, limit);
        plan_inputs.sample_unread = plan_inputs.sampled_pass == null;
        scan_plan = planAggregate(plan_inputs);
    }
    scan_plan.facts.probe_ranges = probe_facts.probe_ranges;
    scan_plan.facts.probe_bytes = probe_facts.probe_bytes;
    scan_plan.facts.probe_capped = probe_facts.probe_capped;
    scan_plan.facts.planning = probe_facts.planning;
    if (@import("builtin").is_test and !args.partagg and ndv_stat == null)
        try testing.expectEqualDeep(referenceDispatch(plan_inputs), scan_plan.dispatch);
    const decision = scan_plan.dispatch;
    const part_on = decision.strategy == .partitioned_hash;
    const count_columns: ?agg_plan.Columns = if (decision.count_columns)
        try plan.forCalls(arena, &.{})
    else
        null;
    if (explain) {
        const record_path = string_record_path orelse pair_record_path orelse selected_record_path orelse packed_record_path orelse
            try inlineDistinctAnswer(
                gpa,
                arena,
                args,
                metas,
                &plan,
                work_items.items,
                agg_calls,
                n_workers,
                t,
                true,
            ) orelse try inlineMsdAnswer(
            gpa,
            arena,
            msd_args,
            metas,
            &plan,
            work_items.items,
            agg_calls,
            group_by_keys,
            group_outputs,
            n_workers,
            t,
            true,
        ) orelse try inlineIntegerAnswer(
            gpa,
            arena,
            &args,
            metas,
            &plan,
            work_items.items,
            agg_calls,
            group_by_keys,
            group_outputs,
            n_workers,
            t,
            .{ .refined = ndv_stat },
            true,
        ) orelse
            try inlineByteTopAnswer(
                gpa,
                arena,
                args,
                metas,
                &plan,
                work_items.items,
                agg_calls,
                group_by_keys,
                group_outputs,
                n_workers,
                t,
                true,
            ) orelse
            try inlineByteAnswer(gpa, arena, args, metas, &plan, work_items.items, agg_calls, group_by_keys, group_outputs, n_workers, t, true);
        var explained_items = work_items.items.len;
        // The ordinary owner must quote the same independent ranges it will actually schedule. Inline owners
        // have already quoted their own work layout and do not use this row-subgroup plan.
        if (record_path == null and group_by_keys != null and !orderSensitive(agg_calls) and
            !decision.use_blocks and domain == null and
            (decision.strategy == .plain_hash or decision.strategy == .partitioned_hash))
        {
            var quote_budget: core_memory.SharedBudget = .{ .limit = args.max_memory };
            var quote_charge: core_memory.Charged = .{ .child = gpa, .budget = args.ledger orelse &quote_budget };
            var quote_arena = std.heap.ArenaAllocator.init(quote_charge.allocator());
            defer quote_arena.deinit();
            if (splitRowGroups(quote_arena.allocator(), args, metas, &plan, work_items.items, @min(requested, by_bytes)) catch null) |split| {
                explained_items = split.len;
                n_workers = @min(@min(requested, by_bytes), split.len);
                scan_plan.dispatch.tables = n_workers;
            }
        }
        const names = try arena.alloc([]const u8, plan.columns.read.len);
        const labels = try metadata.leafLabels(arena, meta0);
        for (labels.names, names, 0..) |label, *name, i| {
            name.* = if (label) |n| try arena.dupe(u8, n) else try std.fmt.allocPrint(arena, "#{d}", .{i});
        }
        return .{
            .plan = scan_plan,
            .column_names = names,
            .columns = plan.columns,
            .count_columns = count_columns,
            .workers = if (record_path) |path| switch (path.layout) {
                .integer_group_stream,
                .integer_sum_top_h,
                .integer_count_top_h,
                .packed_count_top_h,
                .integer_distinct_top_h,
                .pair_distinct,
                => @min(n_workers, owner_groups.max_workers),
                else => n_workers,
            } else n_workers,
            .work_items = explained_items,
            .record_path = record_path,
            .dense = record_path == null and decision.strategy == .plain_hash and
                if (plan_inputs.dense_bytes) |bytes|
                    bytes <= if (args.ledger) |l| l.available() else args.max_memory
                else
                    false,
        };
    }
    // Tables still share one pool. Only a prepaid private plan's decoders protect funded worker floors.
    var own_budget = expr_agg.SharedBudget{ .limit = args.max_memory };
    const group_budget = args.ledger orelse &own_budget;
    // Non-rescannable sources must diagnose while their bytes are held, beside live peer workers.
    // Reserve the error-only quota before any of those workers or tables can draw.
    if (!allRescannable(args.inputs)) if (args.failure_probe) |p| {
        try p.reserveStream(group_budget, streamProbeBytes(args.inputs, metas, &plan));
    };
    defer if (args.failure_probe) |p| p.releaseStream();
    // Header walks and dictionary backing are charged to the plan, and released only after every range joins.
    // Optional-plan refusal never poisons worker decode allocators or changes the established fallback.
    var subgroup_charge: core_memory.Charged = .{ .child = gpa, .budget = group_budget };
    var subgroup_arena = std.heap.ArenaAllocator.init(subgroup_charge.allocator());
    defer subgroup_arena.deinit();
    var worker_work = work_items.items;
    if (group_by_keys != null and !orderSensitive(agg_calls) and !decision.use_blocks and domain == null and
        (decision.strategy == .plain_hash or decision.strategy == .partitioned_hash))
    {
        const split_span = profile.Span.begin(prof);
        if (splitRowGroups(subgroup_arena.allocator(), args, metas, &plan, work_items.items, @min(requested, by_bytes)) catch null) |split| {
            worker_work = split;
            n_workers = @min(@min(requested, by_bytes), worker_work.len);
        } else _ = subgroup_arena.reset(.free_all);
        // `plan` is the scan-count marker. Bill this work to planning without inventing another scan or I/O job.
        split_span.end(.plan_ranges, profile.args(.{}));
    }
    // Shared optional header memo: charged separately so a declined hint cannot poison decoder flags.
    var hint_charge: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = group_budget };
    var cache_hints: consumer.CacheHints = .{ .allocator = hint_charge.allocator() };
    defer cache_hints.deinit();
    if (args.auto_decode_batches and args.decode_batch_rows == 0 and args.expr_batch_rows != 0 and
        consumer.cacheHintsWanted(agg_calls)) cache_hints.initShards(n_workers) catch {};
    var private_decode: PrivateDecode = .{};
    // Declared before region cleanup: their actual allocations go before their funding returns to the query.
    defer private_decode.release();
    const workers = try arena.alloc(Worker, n_workers);
    for (workers) |*w| {
        w.region = null;
        w.scratch_region = null;
    }
    // A process about to exit leaves the regions to it: unmapping them delays the answer. A failed scan may be run
    // again (`runMultiAggregate`), so it gives them back.
    defer if (!args.leave_regions) releaseRegions(workers);
    errdefer releaseRegions(workers);
    var decode_budget: ?consumer.DecodeBudget = if (args.decode_limit) |l|
        .{ .limit = l, .workers = n_workers }
    else
        null;
    // One budget shared by every worker table, not per-worker quotas: work is claimed off a shared cursor, so which
    // worker meets the group-heavy row groups is unknowable in advance — fixed quotas made `-j2` reject queries `-j1`
    // ran on identical data. Fully initialized before any worker exists, so none can insert against an unpublished
    // budget.
    // A refusal chain's last scan decodes beside the query's ledger, on a ledger of its own (`narrowed`).
    var lean_budget = expr_agg.SharedBudget{ .limit = core_memory.lean_decode };
    // Shared partitions retain their own reservation slack alongside the workers' local tables.
    group_budget.block = expr_agg.SharedBudget.blockFor(
        args.max_memory,
        n_workers + if (part_on) partagg.n_parts else @as(usize, 0),
    );
    const distinct_wide_parts = try arena.alloc(bool, agg_calls.len);
    for (agg_calls, distinct_wide_parts) |call, *wide| wide.* = call.func == .count_distinct and
        scalarDistinctWideParts(call.arg.?, metas, work_items.items);
    for (work_items.items) |item| {
        for (item.accumulators, 0..) |*acc, ai| {
            if (acc.* == .distinct) if (acc.distinct.heap()) |ds| {
                if (group_by_keys == null and n_workers > 1) {
                    if (distinct_wide_parts[item.agg_start + ai])
                        try ds.initStringPartitions(gpa, group_budget)
                    else
                        try ds.initPartitions(gpa, group_budget);
                } else {
                    ds.shared = group_budget;
                }
            };
        }
    }

    // GROUP BY tables. One per worker, unless results depend on the order partials merge in and order is asked for:
    // then one per fixed block of the work list, so which rows fold into which table, and the order the tables merge
    // in, doesn't depend on the number of workers or their timing (see `BlockMerge`).
    var block_bounds: ?[]usize = null;
    const use_blocks = decision.use_blocks;
    const n_tables = if (worker_work.ptr != work_items.items.ptr) n_workers else decision.tables;
    if (use_blocks) {
        const bounds = try arena.alloc(usize, n_tables + 1);
        for (bounds, 0..) |*b, i| b.* = i * work_items.items.len / n_tables;
        block_bounds = bounds;
    }
    // A dense fold (below) needs one table more, after the workers': the one its lanes merge into.
    const dense_wanted = dense_supported and dense_profitable and domain != null and domain.?.globalFits(n_workers) and
        decision.strategy == .plain_hash and !use_blocks and !part_on;
    const all_tables = try arena.alloc(expr_agg.GroupTable, n_tables + @intFromBool(dense_wanted));
    const group_tables = all_tables[0..n_tables];
    // Integer and float keys are a fixed-width image: the per-row lookup mixes those bytes in an open-addressed
    // table instead of copying each new key into a string map. String and decimal keys stay on that map.
    const fixed_stride: u8 = if (group_by_keys) |ks| expr_agg.fixedGroupStride(ks) orelse 0 else 0;
    for (all_tables) |*gt| {
        // Zero local ceiling: everything is drawn from the shared pool, so an idle table reserves nothing others
        // could use.
        gt.* = expr_agg.GroupTable.init(gpa, 0);
        gt.shared = group_budget;
        gt.fixed_stride = fixed_stride;
        // A lone SUM's ranking merge paid more to unpack its state than the smaller scan saved.
        if ((!dense_wanted or agg_dense.countOnly(agg_calls)) and !use_blocks and
            (agg_calls.len != 1 or agg_calls[0].func == .count))
            gt.compact.configure(agg_calls);
    }
    // A process about to exit (`leave_regions`) leaves the tables to it: freeing tens of millions of keys one by one
    // is serial time the answer would wait for. Builds that check for leaks free them, so a real leak still shows.
    // Cleared once an answer is built that the query encodes next and the ledger has no room for beside the tables.
    var leave_tables = args.leave_regions and !std.debug.runtime_safety;
    defer if (!leave_tables) for (all_tables) |*gt| gt.deinit();
    errdefer if (leave_tables) for (all_tables) |*gt| gt.deinit();
    var block_cursor = std.atomic.Value(usize).init(0);
    var merged_table = expr_agg.GroupTable.init(gpa, 0);
    merged_table.shared = group_budget;
    merged_table.fixed_stride = fixed_stride;
    defer if (!leave_tables) merged_table.deinit();
    errdefer if (leave_tables) merged_table.deinit();
    var part_state: ?partagg.PartAgg = null;
    defer if (!leave_tables) if (part_state) |*p| p.deinit();
    // A refused scan runs again: what it held goes back, to the OS and to the query's ledger.
    errdefer if (leave_tables) if (part_state) |*p| p.deinit();
    defer if (part_state) |*p| {
        if (p.private_reserved) if (args.taken) |taken| taken.insert(.private_runs);
    };
    // Fixed-width keys fold into an open-addressed table cheap enough that moving near-unique ones to runs cost more
    // than it saved; string keys, copied into a byte map per worker, gain.
    const rescannable = allRescannable(args.inputs);
    // Exact dictionary domains fold by global id when the plan groups in per-worker
    // tables: no group lookups per row group and no hash merge. Taken only when walking every worker's lanes costs no
    // more than the lookups it replaces (one per dictionary entry per row group), and when the domain and every lane
    // fit the shared budget, drawn before any worker starts.
    const dense_cut = denseLimit(args, agg_calls, group_outputs);
    const dense_rule = if (plan_inputs.dense_bytes != null and decision.strategy == .plain_hash) rule: {
        var ordinary = plan_inputs;
        ordinary.dense_bytes = null;
        break :rule planAggregate(ordinary).dispatch.strategy != .plain_hash;
    } else false;
    if (dense_rule) if (args.taken) |taken| taken.insert(.plain_rule);
    var dense_identity: ?key_pack.Plan = null;
    defer if (dense_identity) |*identity| identity.deinit(gpa);
    var dense_lanes: []agg_dense.Lanes = &.{};
    var dense_charge: usize = 0;
    defer group_budget.release(dense_charge);
    defer for (dense_lanes) |*l| l.deinit(gpa);
    // The table the lanes merge into is reserved with them, and handed to it as it is built.
    var dense_table_charge: usize = 0;
    if (dense_wanted) dense: {
        const d = &domain.?;
        if (!rescannable) break :dense;
        const quote = agg_dense.charge(d, n_workers, agg_calls, ndv_stat.?.rows, fixed_stride, dense_cut);
        dense_table_charge = quote.table;
        dense_charge = group_budget.drawExact(quote.total) orelse {
            // Only the lanes justified this plain plan: retry the displaced strategy before consuming rows.
            if (dense_rule) return error.ExceededMemoryBudget;
            break :dense;
        };
        var positions: ?agg_dense.Ordinals = null;
        if (d.axes.len > 1 and agg_dense.sumCall(agg_calls) != null) {
            dense_identity = try key_pack.Plan.init(gpa, group_by_keys.?, d);
            if (dense_identity == null or dense_identity.?.kind() != .dense_u32) break :dense;
            if (ndv_stat.?.rows < @as(u64, 1) << 31) {
                if (work_items.items.len > d.axes[0].trans.len) break :dense;
                const starts = try arena.alloc(u32, work_items.items.len + 1);
                starts[0] = 0;
                var total: u64 = 0;
                for (work_items.items, 0..) |item, i| {
                    const rows = metas[item.file].row_groups.items[item.rg].num_rows;
                    total = std.math.add(u64, total, @intCast(@max(0, rows))) catch break :dense;
                    if (total >= @as(u64, 1) << 31) break :dense;
                    starts[i + 1] = @intCast(total);
                }
                positions = agg_dense.Ordinals.init(starts) orelse break :dense;
                if (positions.?.rows() != ndv_stat.?.rows) break :dense;
            }
        }
        dense_lanes = try arena.alloc(agg_dense.Lanes, n_workers);
        for (dense_lanes) |*l| l.* = .{
            .domain = d,
            .calls = agg_calls,
            .rows = ndv_stat.?.rows,
            .positions = positions,
        };
        if (args.taken) |taken| taken.insert(.dense);
    }
    if (dense_lanes.len == 0) {
        group_budget.release(dense_charge);
        dense_charge = 0;
        dense_table_charge = 0;
        if (dense_identity) |*identity| identity.deinit(gpa);
        dense_identity = null;
        if (dense_rule) return error.ExceededMemoryBudget;
        if (domain) |*d| {
            d.deinit(gpa);
            domain = null;
        }
    }
    const spill_on = dense_lanes.len == 0 and !part_on and args.spill and
        decision.strategy == .plain_hash and !use_blocks and fixed_stride == 0 and partAggSupported(args, agg_calls, group_by_keys != null) and rescannable;
    if (args.taken) |taken| {
        if (spill_on) taken.insert(.spill);
        // Chosen by the cost rules rather than asked for, a partitioned plan has the plain one to fall back to.
        if (part_on and !args.partagg and rescannable) taken.insert(.partitioned);
        const shared = decision.strategy == .plain_hash or part_on;
        if (shared and n_workers > 1 and rescannable) taken.insert(.worker_tables);
        if (decision.strategy == .plain_hash and !use_blocks and partAggSupported(args, agg_calls, true) and
            (if (group_outputs) |outs| countCut(outs, agg_calls, args.result_order) == null else true))
            taken.insert(.partitions_fit);
        if (rescannable and (scan_plan.reasons.contains(.few_dictionary_entries) or
            scan_plan.reasons.contains(.selective_filter)))
        {
            var cut_inputs = plan_inputs;
            cut_inputs.args.plain_rules = false;
            if (!std.meta.eql(planAggregate(cut_inputs).dispatch, decision)) taken.insert(.plain_rule);
        }
    }
    // A partitioned plan the cost rules chose (long strings, worker-copy pressure) merges into shared partition
    // tables; `ZPQ_PARTAGG=1` and a worker that moves at run time append to runs.
    const armed = spill_on;
    if (part_on or spill_on) {
        part_state = try partagg.PartAgg.init(
            gpa,
            group_budget,
            agg_calls,
            fixed_stride,
            mergeAccOwned,
            args.part_cap,
            args.part_probe,
            n_workers,
            part_on and !args.partagg,
        );
        part_state.?.compact = group_tables[0].compact.stride != 0;
        if (part_state.?.compact) for (part_state.?.parts) |*p| p.table.compact.configure(agg_calls);
        if (part_on and !args.partagg and args.private_runs and rescannable and n_workers > 1 and
            agg_calls.len == 1 and agg_calls[0].func == .count)
        {
            if (group_by_keys) |keys| if (keys.len > 1) if (ndv_stat) |stat| {
                const run_width = @max(stat.key_bytes, stat.run_key_bytes);
                if (stat.string_keys and stat.key_bytes >= 32 and stat.rows >= 65536) {
                    private_decode = try PrivateDecode.init(
                        arena,
                        group_budget,
                        n_workers,
                        args.decode_limit,
                        stat.run_dictionary_peak,
                    );
                    if (private_decode.held > 0 and part_state.?.reservePrivate(stat.rows, run_width) and
                        try private_decode.fundSamples(arena, &part_state.?, run_width))
                    {
                        part_state.?.private_rows = stat.rows;
                        part_state.?.private_key_bytes = run_width;
                        part_state.?.decoder_floors = private_decode.workers;
                        for (group_tables, private_decode.samples) |*table, *sample| table.shared = sample;
                    } else {
                        if (part_state.?.private_reserved) part_state.?.cancelPrivate();
                        private_decode.release();
                    }
                }
            };
        }
        part_state.?.spill_groups = args.spill_groups;
        part_state.?.spill_rows = args.spill_rows;
    }
    // `.auto` without a dictionary bound lets each worker decide from its first rows (`pairsPayRuntime`).
    const pairs_from_start = switch (args.distinct_pairs) {
        .auto => pairsPay(ndv_stat),
        .on => true,
        .off => false,
    };
    const pairs_call: ?usize = if (args.distinct_pairs != .off and decision.strategy == .plain_hash and !use_blocks and
        rescannable)
        distinctPairsCall(agg_calls, fixed_stride)
    else
        null;
    if (pairs_call != null) if (args.taken) |taken| taken.insert(.pairs);
    var pair_sinks: []expr_agg.DistinctPairs = &.{};
    defer for (pair_sinks) |*ps| ps.deinit();
    if (pairs_call) |ai| {
        pair_sinks = try arena.alloc(expr_agg.DistinctPairs, n_tables);
        for (pair_sinks, group_tables) |*ps, *gt| {
            ps.* = .{
                .gpa = gpa,
                .budget = group_budget,
                .table = gt,
                .agg_idx = ai,
                .active = pairs_from_start,
                .allow_coded_sets = args.distinct_pairs == .auto,
            };
        }
    }
    var block_merge: ?BlockMerge = if (use_blocks) .{
        .tables = group_tables,
        .merged = &merged_table,
        .calls = agg_calls,
        .gpa = gpa,
        .done = blk: {
            const d = try arena.alloc(std.atomic.Value(bool), n_tables);
            @memset(d, .init(false));
            break :blk d;
        },
        .ahead = n_workers,
    } else null;

    var winners: WinnerCharge = .{ .ledger = if (group_by_keys == null) args.ledger else group_budget };
    errdefer winners.settle(0);
    defer if (group_by_keys != null and !leave_tables) winners.settle(0);
    var work_cursor = std.atomic.Value(usize).init(0);
    for (workers, 0..) |*w, wi| {
        w.* = .{
            .gpa = gpa,
            .inputs = args.inputs,
            .metas = metas,
            .work = worker_work,
            .cursor = &work_cursor,
            .agg_calls = agg_calls,
            .plan = &plan,
            .decode_options = .{
                .fast_levels = args.fast_levels,
                .level_runs = args.level_runs,
                .encoded = args.encoded,
                .expand_limit = args.max_memory,
            },
            .expr_batch_rows = args.expr_batch_rows,
            .decode_batch_rows = args.decode_batch_rows,
            .auto_decode_batches = args.auto_decode_batches,
            .cache_hints = &cache_hints,
            .compaction = args.compaction,
            .scan_workers = n_workers,
            .retire_after_run = args.leave_regions and (args.ledger != null or private_decode.held > 0) and
                switch (decision.strategy) {
                    .scalar, .plain_hash, .partitioned_hash => true,
                    .first_keys, .sketch => false,
                },
            .error_outputs = error_outputs,
            .error_group_keys = error_group_keys,
            .failure_probe = args.failure_probe,
            .profiler = args.profiler,
            .index = wi,
            .group_by_keys = group_by_keys,
            .group_tables = group_tables,
            .part = if (part_state) |*p| p else null,
            .pairs = if (pair_sinks.len > 0) &pair_sinks[wi] else null,
            .dense = if (dense_lanes.len > 0) &dense_lanes[wi] else null,
            .pairs_pilot_rows = args.pairs_pilot_rows,
            .pairs_rechecks = args.pairs_rechecks,
            .local = .{ .runs = if (part_state) |*p| &p.runs[wi] else null, .armed = armed },
            .blocks = block_bounds,
            .block_cursor = &block_cursor,
            .block_merge = if (block_merge) |*m| m else null,
            .items_per_rg = chunks_per_rg,
            .count_columns = count_columns,
            .decode_budget = if (decode_budget) |*b| b else null,
            .ledger = if (private_decode.held > 0)
                &private_decode.workers[wi]
            else if (args.charge_decode or args.ledger == null) args.ledger else &lean_budget,
            .winners = if (winners.ledger != null) &winners else null,
        };
    }

    // 5. Spawn, join, propagate first error. Time the whole parallel
    //    region as one wall-clock span (real elapsed decode), separate
    //    from the per-worker CPU sum in `core.decode_ns`.
    const t_decode = nowMonoNs();
    if (decision.count_agg == null) if (args.sketch_topk) |cfg| if (cfg.outcome) |o| {
        o.* = .ineligible;
    };
    const order = args.result_order;
    const passes: anyerror!void = switch (decision.strategy) {
        .first_keys => firstAppearanceCut(gpa, arena, workers, &work_cursor, group_tables, order, args.sketch_topk.?),
        .sketch => certifiedTopK(
            gpa,
            arena,
            workers,
            &work_cursor,
            group_tables,
            agg_calls,
            order,
            decision.count_agg.?,
            args.sketch_topk.?,
            decision,
        ),
        .scalar, .plain_hash, .partitioned_hash => runWorkers(workers),
    };
    // A cut's passes return their refusal themselves: what decode memory the workers drew is noted on the way out.
    passes catch |original| {
        const e = if (firstWorkerFailure(workers, args.failure_probe)) |f| chosen: {
            consumer.setDecodeFailure(f.site);
            break :chosen f.err;
        } else original;
        if (e == error.ExceededMemoryBudget) if (args.taken) |taken| {
            for (workers) |*w| if (decodeHeld(w) and rescannable) taken.insert(.decode_memory);
        };
        return e;
    };
    const t_decode_end = nowMonoNs();
    t.decode_wall_ns = @intCast(t_decode_end - t_decode);
    profile.record(prof, .scan, t_decode, t_decode_end, profile.args(.{n_workers}));
    const decode_held = for (workers) |*w| {
        if (decodeHeld(w)) break true;
    } else false;
    // The last pass is done: nothing decodes into the regions again, so the merge does not hold them. Under a ledger
    // they go even for a process about to exit, as their charge would sit beside the merge and the answer.
    if (!args.leave_regions or args.ledger != null or private_decode.held > 0) releaseRegionsParallel(workers);
    private_decode.release();
    cache_hints.deinit();
    if (firstWorkerFailure(workers, args.failure_probe)) |failure| {
        const e = failure.err;
        if (e == error.ExceededMemoryBudget) if (args.taken) |taken| {
            if (decode_held and rescannable) taken.insert(.decode_memory);
            if (taken.contains(.worker_tables)) {
                if ((decision.strategy == .plain_hash and holdCopies(group_tables)) or
                    try oneWorkerCuts(arena, plan_inputs)) taken.insert(.one_table_may_fit);
            }
        };
        consumer.setDecodeFailure(failure.site);
        return e;
    }
    // Past the scan a refusal is the merge's, which one worker's table goes without (a scan that answers drops this).
    if (args.taken) |taken| taken.insert(.one_table_may_fit);
    // The merge and the answer may hold a fixed allowance past the budget, beside the tables they are built from:
    // the ceiling's slack (`memory.finalize_allowance`), so a small answer never refuses a query its tables fit.
    if (args.ledger != null) group_budget.limit += core_memory.finalize_allowance;
    defer if (args.ledger != null) {
        group_budget.limit -= core_memory.finalize_allowance;
    };
    const merge_span = profile.Span.begin(prof);
    // The lanes merge by id into the spare table; the lanes and the domain go as soon as it is built (to the OS with
    // the process, when it is about to exit, as the tables do). Row groups no lane took sit in the worker tables, which
    // then merge with it as usual.
    var dense_merged = false;
    var dense_verified = false;
    var dense_direct: ?*agg_dense.Lanes = null;
    // Left to the exit with the tables, but freed if a later refusal runs the scan again (`runMultiAggregate`).
    var left_lanes: []agg_dense.Lanes = &.{};
    var left_domain: ?agg_dense.Domain = null;
    errdefer {
        for (left_lanes) |*l| l.deinit(gpa);
        if (left_domain) |*d| d.deinit(gpa);
    }
    if (dense_lanes.len > 0) {
        dense_verified = for (group_tables) |gt| {
            if (gt.groupCount() != 0) break false;
        } else true;
        const dense_table = &all_tables[n_tables];
        const dense_keys = group_by_keys.?;
        var dense_lookup: usize = 0;
        for (dense_keys) |key| dense_lookup = @max(dense_lookup, key.col_ref.col_idx + 1);
        group_budget.release(dense_table_charge);
        dense_charge -= dense_table_charge;
        const cut = if (dense_cut) |h| only_lanes: {
            if (!agg_dense.countOnly(agg_calls) and dense_keys.len == 1) break :only_lanes null;
            for (group_tables) |*table| if (table.groupCount() != 0) break :only_lanes null;
            break :only_lanes @as(?usize, h);
        } else null;
        const dense_start = if (prof != null) nowMonoNs() else 0;
        const direct = only_lanes: {
            if (agg_dense.sumCall(agg_calls) == null or dense_keys.len < 2) break :only_lanes false;
            for (group_outputs.?) |out| if (out.derived != null) break :only_lanes false;
            for (group_tables) |gt| if (gt.groupCount() != 0) break :only_lanes false;
            break :only_lanes true;
        };
        if (direct) {
            dense_direct = try agg_dense.merge(gpa, arena, dense_lanes, n_workers);
        } else try agg_dense.intoTable(gpa, arena, dense_lanes, dense_keys, dense_table, dense_lookup, n_workers, cut);
        if (prof != null) profile.record(
            prof,
            .dense_fold,
            dense_start,
            nowMonoNs(),
            profile.args(.{ if (dense_direct) |d| d.groupCount() else dense_table.groupCount(), n_workers }),
        );
        // Direct answers borrow the lanes and domain until this frame returns.
        if (dense_direct == null) {
            if (leave_tables) {
                left_lanes = dense_lanes;
                left_domain = domain;
            } else {
                for (dense_lanes) |*l| l.deinit(gpa);
                domain.?.deinit(gpa);
            }
            dense_lanes = &.{};
            domain = null;
            group_budget.release(dense_charge);
            dense_charge = 0;
        }
        dense_merged = true;
    }
    // Coded sets stay on their ordinary union when no worker emitted a pair. Otherwise every complete identity
    // joins the same pair fold, including sets a coded chunk retained beside an active sink.
    var pairs_used = false;
    for (pair_sinks) |*ps| pairs_used = pairs_used or
        (ps.active and (!ps.allow_coded_sets or ps.hasPairs()));
    if (pairs_used) {
        try drainDistinctPairs(gpa, pair_sinks, group_tables, n_workers);
        try foldDistinctPairs(gpa, group_budget, pair_sinks, group_tables, pairs_call.?, agg_calls.len, n_workers);
    }
    // Once one worker has moved to the partition runs every table goes there, so each key is in one partition.
    const spilled = part_on or (armed and part_state.?.spilled.load(.monotonic));
    if (spilled) {
        const ps = &part_state.?;
        if (armed) for (workers) |*w| if (w.local.armed) try ps.flush(&w.local, &group_tables[w.index]);
        try ps.foldRuns(n_workers);
    }

    // 6. Merge per-work-item accumulators into one final slice.
    var accumulators = try arena.alloc(expr_agg.Accumulator, agg_calls.len);
    for (agg_calls, accumulators) |call, *acc| acc.* = expr_agg.Accumulator.init(call);
    errdefer freeOwnedAccumulatorStrings(gpa, accumulators);
    for (agg_calls, accumulators, 0..) |call, *acc, ci| if (call.func == .count_distinct) {
        const ds = try gpa.create(expr_agg.DistinctSet);
        ds.* = .{};
        acc.* = .{ .distinct = .fromSet(ds) };
        if (group_by_keys == null and n_workers > 1) {
            if (distinct_wide_parts[ci])
                try ds.initStringPartitions(gpa, group_budget)
            else
                try ds.initPartitions(gpa, group_budget);
        } else {
            ds.shared = group_budget;
        }
    };
    // These sets outlive `group_budget`, which lives in this frame: they give their reservations back to it before
    // it goes, and forget it.
    defer for (accumulators) |*acc| if (acc.* == .distinct) acc.distinct.detach();
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    var answer_charge: usize = 0;
    errdefer group_budget.release(answer_charge);
    var rows_in: i64 = 0;
    var rows_scanned: i64 = 0;
    var rows_matched: i64 = 0;
    var rgs_in: usize = 0;
    var rgs_pruned: usize = 0;
    var rgs_full_match: usize = 0;

    var group_col_types: ?[]const GroupColType = null;

    if (group_by_keys) |keys| {
        for (workers) |w| {
            rows_in += w.rows_in;
            rows_scanned += w.rows_scanned;
            rows_matched += w.rows_matched;
            rgs_in += w.rgs_in;
            rgs_pruned += w.rgs_pruned;
            rgs_full_match += w.rgs_full_match;
            t.core.decode_ns += w.timings.decode_ns;
            t.core.eval_ns += w.timings.eval_ns;
            t.core.encode_ns += w.timings.encode_ns;
        }

        // `group_refs` is the whole merge input, in table order: a filter that drops refs before this point still
        // folds only what remains. Blocks have already merged, in input order, into one table.
        //
        // Equal keys meet in a hash table instead of a sort on the key bytes. Long keys that share a prefix make
        // those comparisons the query. Worker maps store an 8-bit fingerprint, not the hash, so each ref is hashed
        // once here. One table already has distinct keys and only needs first-appearance order.
        //
        // Merge maps borrow source keys. Under a ledger the merge's maps, lists and scratch are drawn from it
        // (`MultiAggArgs.merge_memory`), the full merge's and a result cut's alike, so a cut never refuses a budget
        // the merge it stands in for answers; the accumulators' own values stay on `gpa`. A refusal here is the
        // merge's, which the one-worker rerun goes without.
        const lists = if (args.merge_memory) |m| m.allocator() else gpa;
        const result_cut = resultCut(group_outputs.?, agg_calls, args.result_order, args.unordered);
        const out_tables: []expr_agg.GroupTable = if (use_blocks)
            (&merged_table)[0..1]
        else if (dense_merged) blk: {
            for (group_tables) |gt| if (gt.groupCount() > 0) break :blk all_tables;
            break :blk all_tables[n_tables..];
        } else group_tables;
        // An answer cut to its first rows by a COUNT, descending, merges only the groups that could make the cut.
        if (result_cut == null or !result_cut.?.ranking_first)
            try premergeLargeDistinctSets(gpa, arena, out_tables, agg_calls, n_workers);
        // COUNT-only ranks go straight to parallel partition heaps. DISTINCT cardinality bounds can discard
        // expensive losing sets before the payload merge; that merge still runs on the requested workers.
        const first_parallel = n_workers > 1 and args.result_order.keys.len == 0 and
            args.result_order.having == null and args.result_order.hidden.len == 0;
        const first_k = if (args.result_order.limit) |limit|
            std.math.cast(usize, args.result_order.offset +| limit)
        else
            null;
        const candidates = if (first_parallel and first_k != null)
            try firstCandidatesParallel(lists, out_tables, first_k.?, n_workers)
        else if (n_workers == 1 or (result_cut != null and
            agg_calls[result_cut.?.a_idx].func == .count_distinct))
            try topKCandidates(arena, out_tables, group_outputs.?, agg_calls, args.result_order)
        else
            null;
        defer if (first_parallel) if (candidates) |kept| lists.free(kept);
        // One table already holds complete ranks; its top-H heap avoids materializing every losing group.
        const cut: ?[]const u32 = if (dense_direct) |d|
            try agg_dense.reached(arena, d, dense_cut)
        else if (n_workers == 1 and candidates == null and out_tables.len == 1)
            try resultLimitGroups(
                arena,
                &out_tables[0],
                group_outputs.?,
                agg_calls,
                args.result_order,
                args.unordered,
                null,
            )
        else
            null;
        var total_group_refs: usize = 0;
        if (cut) |ids| {
            total_group_refs = ids.len;
        } else if (candidates) |keys_kept| {
            total_group_refs = keys_kept.len * out_tables.len;
        } else for (out_tables) |gt| {
            total_group_refs += gt.groupCount();
        }
        const group_refs_buf = try lists.alloc(MergeGroupRef, total_group_refs);
        defer lists.free(group_refs_buf);
        var ref_i: usize = 0;
        if (cut == null and candidates == null) {
            finishGroupRanges(out_tables, group_refs_buf, null, n_workers, false);
            ref_i = total_group_refs;
        } else for (out_tables, 0..) |gt, ti| {
            if (cut) |ids| {
                if (ti == 0) for (ids) |gi| {
                    group_refs_buf[ref_i] = .{ .table = 0, .group = gi };
                    ref_i += 1;
                };
            } else if (candidates) |keys_kept| {
                for (keys_kept) |key| if (gt.findKey(key)) |gi| {
                    group_refs_buf[ref_i] = .{ .table = @intCast(ti), .group = gi };
                    ref_i += 1;
                };
            }
        }
        const group_refs = group_refs_buf[0..ref_i];

        const outputs = group_outputs.?;

        var key_types = try gpa.alloc(KeyType, keys.len);
        defer gpa.free(key_types);
        for (keys, 0..) |key_expr, idx| {
            key_types[idx] = keyTypeFromExpr(key_expr);
        }

        const col_types = try arena.alloc(GroupColType, outputs.len);
        for (outputs, col_types) |out, *ct| ct.* = switch (out.source) {
            .key => |k_idx| .{
                .nullable = true,
                .lane = switch (key_types[k_idx]) {
                    .i64, .u64 => .i64,
                    .f64 => .f64,
                    .string => .string,
                    .dec => .dec,
                },
                .dec = if (key_types[k_idx] == .dec) key_types[k_idx].dec else null,
                .source = if (out.derived != null) null else if (keys[k_idx] == .col_ref)
                    leafSchemaElem(meta0, keys[k_idx].col_ref.col_idx)
                else if (keys[k_idx].isBoolean()) boolean_answer else if (keys[k_idx].temporal() == .date) date_answer else null,
            },
            .agg => |a_idx| .{
                .nullable = agg_calls[a_idx].func != .count,
                .lane = switch (agg_calls[a_idx].result) {
                    .i64 => .i64,
                    .f64 => .f64,
                    .bytes => .string,
                    .avg_f64 => if (agg_calls[a_idx].state) .avg else .f64,
                    .dec => .dec,
                    .avg_dec => if (agg_calls[a_idx].state) .avg_dec else .dec,
                },
                .dec = switch (agg_calls[a_idx].result) {
                    .dec, .avg_dec => expr_agg.resultDec(agg_calls[a_idx]),
                    else => null,
                },
                .source = agg_sources[a_idx],
            },
        };
        group_col_types = col_types;

        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(lists);
        // Accumulators moved out of the source tables when several tables share keys. Empty when there is one table:
        // its groups are already distinct, so rows read that table directly.
        var merged_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer {
            freeAccumulatorRanges(gpa, merged_accs.items, n_workers);
            merged_accs.deinit(lists);
        }
        var folded = out_tables.len > 1 or (n_workers > 1 and result_cut != null and dense_direct == null);
        // Set when the merge left each group's state in its source table (`mergeInPlace`).
        var in_place_tables: []const expr_agg.GroupTable = &.{};
        // Partition tables already hold each key once. Their accumulators move into `merged_accs`; the keys stay
        // borrowed until the rows are built.
        if (dense_direct) |d| {
            try slots.ensureTotalCapacity(lists, group_refs.len);
            for (group_refs) |ref| slots.appendAssumeCapacity(.{
                .key = &.{},
                .first = d.position(ref.group),
                .idx = ref.group,
            });
            folded = false;
        } else if (spilled) {
            const ps = &part_state.?;
            try fillFromPartitions(
                gpa,
                lists,
                arena,
                ps.parts,
                agg_calls,
                group_outputs.?,
                args.result_order,
                args.unordered,
                result_cut,
                n_workers,
                &slots,
                &merged_accs,
            );
            folded = true;
        } else if (folded) {
            if (candidates) |keys_kept| {
                try mergeCandidates(gpa, lists, out_tables, keys_kept, agg_calls, n_workers, &slots, &merged_accs);
            } else {
                // Pairs left their count(DISTINCT) a count in every table: it merges as one, in place or under a cut.
                const merge_calls = if (pairs_used) try countedCalls(arena, agg_calls, pairs_call.?) else agg_calls;
                const in_place = mergesInPlace(merge_calls, result_cut);
                try mergeFoldedRefs(
                    gpa,
                    lists,
                    out_tables,
                    group_refs,
                    merge_calls,
                    n_workers,
                    result_cut,
                    in_place,
                    &slots,
                    &merged_accs,
                );
                if (in_place) in_place_tables = out_tables;
            }
        } else {
            const gt = &out_tables[0];
            try slots.ensureTotalCapacity(lists, group_refs.len);
            if (cut == null and candidates == null) {
                slots.items.len = group_refs.len;
                finishGroupRanges(out_tables, null, slots.items, n_workers, false);
            } else for (group_refs) |ref| {
                slots.appendAssumeCapacity(.{
                    .key = gt.keyAt(ref.group),
                    .first = gt.first.items[ref.group],
                    .idx = ref.group,
                });
            }
        }
        if (folded) if (result_cut) |rc| {
            if (rc.ranking_first) {
                try keepResultTop(gpa, lists, agg_calls, rc, &slots, &merged_accs);
            } else try keepCountTop(gpa, lists, agg_calls, .{ .a_idx = rc.a_idx, .k = rc.k }, &slots, &merged_accs);
        };
        if (!args.unordered) try sortSlotsByFirst(lists, slots.items, n_workers);

        // Owned by `output` from allocation on, every slot valid at every step, so its `deinit` frees a partly built
        // answer: unbuilt values NULL. One slab holds every row's cells (`AggOutput.slab`), not an allocation a row.
        var row_src: RowSource = .{
            .slots = slots.items,
            .outputs = outputs,
            .calls = agg_calls,
            .key_types = key_types,
            .key_exprs = keys,
            .accs = if (folded) merged_accs.items else out_tables[0].accumulators.items,
            .folded = folded,
            .in_place = in_place_tables,
            .single = if (folded) null else &out_tables[0],
            .fixed_keys = if (spilled) fixed_stride != 0 else out_tables[0].fixed_stride != 0,
            .dense = dense_direct,
            .identity = if (dense_identity) |*identity| identity else null,
        };
        // A small cut owns its few surviving strings instead of retaining every source arena.
        if (dense_direct == null and order.keys.len == 0 and order.limit == null and order.offset == 0 and
            order.having == null and order.hidden.len == 0)
        {
            const source_tables = try arena.alloc(*expr_agg.GroupTable, if (spilled) part_state.?.parts.len else out_tables.len);
            if (spilled) {
                for (part_state.?.parts, source_tables) |*p, *dest| dest.* = &p.table;
            } else for (out_tables, source_tables) |*gt, *dest| dest.* = gt;
            output.arena_credit = try output.takeTableArenas(gpa, source_tables, group_budget);
            output.arena_ledger = args.ledger;
            row_src.borrowed_strings = output.borrowed_strings;
        }
        const as_is = order.keys.len == 0 and order.limit == null and order.offset == 0 and order.having == null and
            order.hidden.len == 0;
        const typed = args.typed_answer and as_is;
        // The answer is drawn from the ledger before it is built, beside the tables and merge it is built from, and
        // stays drawn while the query holds it (the engine's ledger outlives it). Refused, it is the merge's refusal:
        // on one worker the tables it reads are one.
        const winner_copies = row_src.winnerStringBytes();
        if (args.ledger != null or winner_copies != 0) {
            const bytes = (if (args.ledger != null)
                answerBytesParallel(slots.items, col_types, if (row_src.borrowed_strings.len != 0) &.{} else key_types, typed, n_workers) +|
                    row_src.denseStringBytes()
            else
                0) +| winner_copies;
            answer_charge += group_budget.drawExact(bytes) orelse {
                if (args.taken) |taken| taken.insert(.merge_memory);
                return error.ExceededMemoryBudget;
            };
        }
        if (typed) {
            output.typed = .{ .rows = 0, .columns = &.{} };
            try buildGroupColumns(gpa, &output.typed.?, row_src, col_types, n_workers);
            // Its encoding draws up to three times its bytes (`consumer.aggEncodeCharge`) from the ledger.
            if (leave_tables and args.ledger != null and group_budget.available() < answer_charge *| 3) {
                leave_tables = false;
                for (left_lanes) |*l| l.deinit(gpa);
                if (left_domain) |*d| d.deinit(gpa);
                left_lanes = &.{};
                left_domain = null;
            }
        } else {
            const rows = try gpa.alloc([]const AggValue, slots.items.len);
            @memset(rows, &.{});
            output.group_rows = rows;
            try buildGroupRows(gpa, &output.slab, row_src, rows, n_workers);
        }

        const cols = try gpa.alloc([]const u8, outputs.len);
        @memset(cols, "");
        output.group_cols = cols;
        for (outputs, cols) |out, *col| col.* = try gpa.dupe(u8, out.name);

        // Result strings either own their copy or borrow the arenas now held by output. The table
        // payload/storage can be released before returning; the deferred deinit then finds it empty.
        if (!leave_tables) {
            finishGroupRanges(out_tables, null, null, n_workers, true);
            for (out_tables) |*gt| {
                gt.deinitStorage();
                gt.* = expr_agg.GroupTable.init(gpa, 0);
            }
        }
    } else {
        // Merge each work item exactly ONCE: `work` is shared and claimed dynamically, so folding it per worker would
        // merge every item n_workers times — inflating sums and double-freeing the string min/max winner.
        for (work_items.items) |item| {
            for (item.accumulators, 0..) |*sub_acc, i| {
                const call = agg_calls[item.agg_start + i];
                if (call.func != .count_distinct) {
                    try mergeOwnedAccumulator(&accumulators[item.agg_start + i], sub_acc, gpa);
                }
            }
        }
        for (agg_calls, 0..) |call, ci| {
            if (call.func != .count_distinct) continue;
            const dst_acc = &accumulators[ci];
            const dst_ds = dst_acc.distinct.heap() orelse continue;
            if (dst_ds.parts) |dst_parts| {
                // Under pressure, finish and release one source partition at a time instead of growing several
                // destination maps concurrently against the last shared credit.
                const n_threads = if (group_budget.tight()) 1 else @min(n_workers, dst_parts.len);
                const tasks = try arena.alloc(DistinctMergeTask, n_threads);
                var cursor = std.atomic.Value(usize).init(0);
                for (tasks) |*task| task.* = .{
                    .gpa = gpa,
                    .dst_parts = dst_parts,
                    .work_items = work_items.items,
                    .agg_idx = ci,
                    .cursor = &cursor,
                };
                const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
                if (!spawn_util.runTree(DistinctMergeTask, tasks, config, DistinctMergeTask.run)) {
                    DistinctMergeTask.run(&tasks[0]);
                }
                for (tasks) |task| if (task.err) |e| return e;

                for (work_items.items) |item| {
                    if (ci >= item.agg_start and ci < item.agg_start + item.agg_len) {
                        const sub_acc = &item.accumulators[ci - item.agg_start];
                        if (sub_acc.* == .distinct) if (sub_acc.distinct.heap()) |ds| {
                            ds.deinit(gpa);
                            gpa.destroy(ds);
                            sub_acc.* = .{ .distinct = .{} };
                        };
                    }
                }
            } else {
                for (work_items.items) |item| {
                    if (ci >= item.agg_start and ci < item.agg_start + item.agg_len) {
                        const sub_acc = &item.accumulators[ci - item.agg_start];
                        try mergeOwnedAccumulator(dst_acc, sub_acc, gpa);
                    }
                }
            }
        }
        for (workers) |w| {
            rows_in += w.rows_in;
            rows_scanned += w.rows_scanned;
            rows_matched += w.rows_matched;
            rgs_in += w.rgs_in;
            rgs_pruned += w.rgs_pruned;
            rgs_full_match += w.rgs_full_match;
            t.core.decode_ns += w.timings.decode_ns;
            t.core.eval_ns += w.timings.eval_ns;
            t.core.encode_ns += w.timings.encode_ns;
        }
    }

    merge_span.end(.merge, profile.args(.{}));

    // 7. Materialize into gpa, so the answer outlives the arena the caller will eventually deinit.
    const finalize_span = profile.Span.begin(prof);
    defer finalize_span.end(.finalize, profile.args(.{}));
    if (group_by_keys == null) {
        // The merge freed every losing winner; the answer copies the ones left.
        const kept = ownedWinnerBytes(accumulators);
        winners.settle(kept);
        try winners.adjust(kept, kept * 2);
        const items = try gpa.alloc(AggOutputItem, agg_calls.len);
        @memset(items, .{ .alias = "", .value = .null_val });
        output.aggs = items;
        for (agg_calls, items, accumulators) |call, *item, acc| {
            item.alias = try gpa.dupe(u8, call.alias);
            item.value = try materializeOne(gpa, call, acc);
        }
    }

    var bytes_in: u64 = 0;
    for (args.inputs) |in| bytes_in += if (in.logical_size != 0) in.logical_size else in.bytes.len;

    return .{
        .files_in = args.inputs.len,
        .rows_in = rows_in,
        .rows_scanned = rows_scanned,
        .rows_matched = rows_matched,
        .bytes_in = bytes_in,
        .row_groups_in = rgs_in,
        .row_groups_pruned = rgs_pruned,
        .row_groups_full_match = rgs_full_match,
        .cols_stat_pruned = plan.cols_stat_pruned,
        .membership = membership_stats,
        .timings = t,
        .output = output,
        .part_agg = spilled,
        .distinct_pairs = pairs_used,
        .dense_aggregate = dense_merged and dense_verified,
        .reasons = scan_plan.reasons,
        .visible_cols = if (args.result_order.hidden.len > 0) args.select_cols.?.len else null,
        .state = .{
            .accumulators = accumulators,
            .agg_calls = agg_calls,
            .group_col_types = group_col_types,
            .agg_sources = agg_sources,
        },
    };
}

/// Each temporary query owns a child ledger. Existing executor reservations and actual allocations both draw
/// conservatively; the whole phase retires before its remaining diagnostic/output credit returns to the parent.
const RankedPhase = struct {
    budget: core_memory.SharedBudget = undefined,
    charged: core_memory.Charged = undefined,
    arena_budget: core_memory.SharedBudget = undefined,
    arena_charged: core_memory.Charged = undefined,
    arena: std.heap.ArenaAllocator = undefined,

    fn init(p: *RankedPhase, gpa: std.mem.Allocator, parent: *core_memory.SharedBudget, limit: usize) void {
        p.budget = .{ .limit = limit, .parent = parent };
        p.charged = .{ .child = gpa, .budget = &p.budget };
        p.arena_budget = .{ .limit = limit, .parent = parent };
        p.arena_charged = .{ .child = gpa, .budget = &p.arena_budget };
        p.arena = .init(p.arena_charged.allocator());
    }

    fn deinit(p: *RankedPhase) void {
        p.arena.deinit();
        p.budget.release(p.budget.used.load(.monotonic));
    }
};

fn rankedColumn(col: expr_ast.ColRef) bool {
    return col.expr_type == .i64 and !col.unsigned_32 and !col.unsigned_64 and
        (col.physical_type == .INT32 or col.physical_type == .INT64);
}

fn sameRankedKey(a: AggValue, b: AggValue) bool {
    return switch (a) {
        .i => |v| b == .i and v == b.i,
        .null_val => b == .null_val,
        else => false,
    };
}

fn countRankedDistinct(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    calls: []const expr_agg.AggCall,
    keys_opt: ?[]const expr_ast.Expr,
    outputs_opt: ?[]const BoundOutputColumn,
    filter: ?filter_ast.Filter,
) anyerror!?MultiAggResult {
    if (args.count_ranked_distinct == .off or args.scan_all or args.unordered or !args.dense or
        !args.ndv_union or !allRescannable(args.inputs)) return null;
    const keys = keys_opt orelse return null;
    const outputs = outputs_opt orelse return null;
    if (keys.len != 1 or keys[0] != .col_ref or !rankedColumn(keys[0].col_ref) or args.result_order.having != null or
        args.result_order.hidden.len != 0) return null;
    for (outputs) |out| if (out.derived != null) return null;
    const cut = countCut(outputs, calls, args.result_order) orelse return null;
    if (cut.k == 0 or calls[cut.a_idx].func != .count or calls[cut.a_idx].arg != null) return null;
    var distinct: ?usize = null;
    var payload = false;
    for (calls, 0..) |call, ai| {
        if (call.where != null or call.state or call.offset != 0) return null;
        switch (call.func) {
            .count => if (call.arg != null) return null,
            .count_distinct => {
                if (distinct != null or call.arg == null) return null;
                distinct = ai;
            },
            .sum, .avg => payload = true,
            else => return null,
        }
        if (call.arg) |arg| if (arg != .col_ref or !rankedColumn(arg.col_ref)) return null;
    }
    const di = distinct orelse return null;
    if (!payload) return null;

    var own: core_memory.SharedBudget = .{ .limit = args.max_memory };
    const ledger = args.ledger orelse &own;
    var phase: RankedPhase = undefined;
    phase.init(gpa, ledger, args.max_memory);
    defer phase.deinit();
    return countRankedAttempt(gpa, arena, args, metas, calls, keys, outputs, filter, cut, di, ledger, &phase) catch |e| {
        return switch (e) {
            error.OutOfMemory, error.ExceededMemoryBudget, error.CountRankedDistinctDeclined => error.CountRankedDistinctDeclined,
            else => e,
        };
    };
}

fn countRankedAttempt(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    filter: ?filter_ast.Filter,
    cut: CountCut,
    di: usize,
    ledger: *core_memory.SharedBudget,
    phase: *RankedPhase,
) anyerror!?MultiAggResult {
    const a = phase.arena.allocator();
    const ordinary = try a.alloc(expr_agg.AggCall, calls.len - 1);
    const ordinary_outputs = try a.alloc(BoundOutputColumn, calls.len);
    ordinary_outputs[0] = .{ .source = .{ .key = 0 }, .name = "__ranked_key" };
    var oi: usize = 0;
    for (calls, 0..) |call, ai| {
        if (ai == di) continue;
        ordinary[oi] = call;
        ordinary[oi].alias = try std.fmt.allocPrint(a, "__ranked_agg_{d}", .{oi});
        ordinary[oi].default_name = null;
        ordinary_outputs[oi + 1] = .{ .source = .{ .agg = oi }, .name = ordinary[oi].alias };
        oi += 1;
    }
    const ordinary_bound: BoundAggregate = .{ .calls = ordinary, .keys = keys, .outputs = ordinary_outputs, .filter = filter };
    var first = args;
    first.inputs = try a.dupe(Input, args.inputs);
    for (@constCast(first.inputs)) |*input| {
        const bytes = input.plan_bytes orelse input.bytes;
        if (input.logical_size != 0 and bytes.len != input.logical_size) return null;
        input.bytes = bytes;
        input.gate = null;
        input.dict_wait = null;
    }
    first.metas = metas;
    first.start_reads = null;
    first.dict_probe = null;
    first.count_ranked_distinct = .off;
    first.bound_aggregate = &ordinary_bound;
    first.select_cols = null;
    first.column_order = null;
    first.sketch_topk = null;
    first.partagg = false;
    first.partition_cost = false;
    first.plain_rules = false;
    first.typed_answer = false;
    first.record_answer = false;
    first.leave_regions = false;
    first.ledger = &phase.budget;
    const rank = [_]expr_ast.OrderKey{.{ .column = 1 + cut.a_idx - @intFromBool(cut.a_idx > di), .descending = true }};
    first.result_order = .{ .keys = &rank, .limit = cut.k };
    const explanation = try explainAggregate(phase.charged.allocator(), a, first);
    if (!explanation.dense) return null;
    // The mapping is complete before abandoning a streaming reader; any subsequent refusal replays that mapping.
    if (args.start_reads) |start| if (start.abandon) |abandon| abandon(start.ctx);
    var counted = try runMultiAggregate(phase.charged.allocator(), a, first);
    defer counted.deinit(phase.charged.allocator());
    if (!counted.dense_aggregate) return error.CountRankedDistinctDeclined;
    const original = try a.dupe([]const AggValue, counted.output.group_rows orelse return error.CountRankedDistinctDeclined);
    try counted.output.applyResultOrder(phase.charged.allocator(), first.result_order);
    const retained = counted.output.group_rows.?;
    const predicates = try a.alloc(filter_ast.Filter, retained.len);
    const key = keys[0].col_ref;
    for (retained, predicates) |row, *predicate| {
        predicate.* = switch (row[0]) {
            .i => |v| if (key.physical_type == .INT32)
                .{ .int32 = .{ .col_idx = key.col_idx, .op = .Eq, .value = std.math.cast(i32, v) orelse return error.CountRankedDistinctDeclined, .stored = true } }
            else
                .{ .int64 = .{ .col_idx = key.col_idx, .op = .Eq, .value = std.math.cast(i64, v) orelse return error.CountRankedDistinctDeclined, .stored = true } },
            .null_val => .{ .null_check = .{ .col_idx = key.col_idx, .is_not = false } },
            else => return error.CountRankedDistinctDeclined,
        };
    }
    const winners_filter: filter_ast.Filter = .{ .or_filter = .{ .children = predicates } };
    const retained_filter: filter_ast.Filter = if (filter) |f|
        .{ .and_filter = .{ .children = try a.dupe(filter_ast.Filter, &.{ f, winners_filter }) } }
    else
        winners_filter;
    var second_phase: RankedPhase = undefined;
    second_phase.init(gpa, ledger, args.max_memory);
    defer second_phase.deinit();
    var second = first;
    const distinct_bound: BoundAggregate = .{
        .calls = calls[di..][0..1],
        .keys = keys,
        .outputs = &.{ .{ .source = .{ .key = 0 }, .name = "__ranked_key" }, .{ .source = .{ .agg = 0 }, .name = "__ranked_distinct" } },
        .filter = retained_filter,
    };
    second.bound_aggregate = &distinct_bound;
    second.result_order = .{};
    second.ledger = &second_phase.budget;
    var reduced: ?MultiAggResult = null;
    defer if (reduced) |*r| r.deinit(second_phase.charged.allocator());
    const distinct_rows = if (retained.len == 0) &.{} else blk: {
        reduced = try runMultiAggregate(second_phase.charged.allocator(), second_phase.arena.allocator(), second);
        break :blk reduced.?.output.group_rows orelse return error.CountRankedDistinctDeclined;
    };
    if (distinct_rows.len != retained.len) return error.CountRankedDistinctDeclined;

    const bytes = retained.len *| (outputs.len *| @sizeOf(AggValue) +| @sizeOf([]const AggValue)) +|
        outputs.len *| (@sizeOf([]const u8) +| @sizeOf(GroupColType)) +| outputNamesBytes(outputs);
    _ = ledger.drawExact(bytes) orelse return error.CountRankedDistinctDeclined;
    var credit = bytes;
    defer if (credit != 0) ledger.release(credit);
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
    const names = try gpa.alloc([]const u8, outputs.len);
    @memset(names, "");
    output.group_cols = names;
    for (outputs, names) |out, *name| name.* = try gpa.dupe(u8, out.name);
    const rows = try gpa.alloc([]const AggValue, retained.len);
    @memset(rows, &.{});
    output.group_rows = rows;
    output.slab = try gpa.alloc(AggValue, retained.len * outputs.len);
    const types = try arena.alloc(GroupColType, outputs.len);
    for (outputs, types) |out, *ty| ty.* = switch (out.source) {
        .key => .{ .lane = .i64, .nullable = true, .source = leafSchemaElem(&metas[0], keys[0].col_ref.col_idx) },
        .agg => |ai| .{ .lane = if (calls[ai].func == .avg) .f64 else .i64, .nullable = calls[ai].func != .count, .source = passThroughSource(calls[ai], &metas[0]) },
    };
    var ri: usize = 0;
    // The result program still receives first-appearance order before applying its own COUNT sort and offset.
    for (original) |prior| for (retained) |winner| {
        if (prior.ptr != winner.ptr) continue;
        const drow = found: for (distinct_rows) |dr| {
            if (sameRankedKey(winner[0], dr[0])) break :found dr;
        } else return error.CountRankedDistinctDeclined;
        const row = output.slab[ri * outputs.len ..][0..outputs.len];
        rows[ri] = row;
        for (outputs, row) |out, *value| value.* = switch (out.source) {
            .key => winner[0],
            .agg => |ai| if (ai == di) drow[1] else winner[1 + ai - @intFromBool(ai > di)],
        };
        ri += 1;
    };
    if (ri != retained.len) return error.CountRankedDistinctDeclined;
    var result = counted;
    result.output = output;
    result.state = .{ .accumulators = &.{}, .agg_calls = calls, .group_col_types = types };
    result.count_ranked_distinct = true;
    result.dense_aggregate = false;
    if (reduced) |r| result.timings.decode_wall_ns +|= r.timings.decode_wall_ns;
    if (args.ledger) |parent| {
        result.output.arena_ledger = parent;
        result.output.arena_credit = bytes;
        credit = 0;
    }
    return result;
}

/// An aggregate argument or GROUP BY key that reads a repeated leaf (in a LIST or MAP) is refused while planning, as a
/// filter on one is: it is an unsupported query (exit 2), not a decode failure found after reading the column.
pub fn refuseRepeated(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
    diag: ?*Diag,
) !void {
    for ([_]bool{ false, true }) |in_keys| {
        const reads = try arena.alloc(bool, meta.schema.items.len);
        @memset(reads, false);
        if (in_keys) {
            for (keys orelse &.{}) |k| k.collectColumns(reads);
        } else for (calls) |c| if (c.arg) |a| a.collectColumns(reads);
        var leaves: schema.LeafIterator = .init(meta.schema.items);
        while (leaves.next()) |leaf| if (leaf.max_rep > 0 and leaf.index < reads.len and reads[leaf.index]) {
            Diag.setColumn(diag, try std.mem.join(arena, ".", leaf.path));
            return if (in_keys) error.NestedGroupKeyNotSupported else error.NestedAggregateNotSupported;
        };
    }
}

/// Optional scheduling plan for too few surviving groups. Byte weight bounds both task count and plan overhead;
/// a stable flat chunk supplies independent seeks, while one-shot streams and ordered reductions keep their path.
fn splitRowGroups(
    a: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    workers: usize,
) !?[]WorkItem {
    if (workers < 2) return null;
    var alive: usize = 0;
    var bytes: u64 = 0;
    var sample: ?WorkItem = null;
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        const decision = plan.decide(rg, &metas[item.file]);
        if (decision == .skip or rg.num_rows == 0) continue;
        if (!args.inputs[item.file].rescannable() or rg.num_rows < 0 or rg.num_rows > std.math.maxInt(u32)) return null;
        if (!row_subgroups.supported(rg, &metas[item.file], item.columns.forRowGroup(decision))) return null;
        // The indexed filter path compacts survivors and can leave rejected page bodies unread. Keep that
        // contract rather than replacing it with ranges that would decode those pages again.
        if (decision == .filter) for (rg.columns.items, 0..) |col, ci| {
            const read = item.columns.forRowGroup(decision);
            if (ci < read.len and read[ci] and col.column_index_offset != null) return null;
        };
        alive += 1;
        if (sample == null) sample = item;
        bytes +|= subgroupBytes(rg, item.columns.forRowGroup(decision));
    }
    if (alive == 0 or alive >= workers) return null;
    const target = @min(workers *| 2, bytes / @max(1, args.min_bytes_per_worker));
    if (target <= alive) return null;
    if (plan.query.filter) |filter| {
        const item = sample.?;
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const source = args.inputs[item.file].plan_bytes orelse args.inputs[item.file].bytes;
        // Unknown fixed-width selectivity can leave little grouping behind a large decoded-byte estimate.
        // Costing only: a prefix never bounds rows or skips evaluation; probe refusal retains today's split.
        const share = subgroupPassShare(a, source, rg, meta, filter, args.min_bytes_per_worker, workers) catch 1;
        if (@as(f64, @floatFromInt(bytes)) * share <
            @as(f64, @floatFromInt(@max(1, args.min_bytes_per_worker))) * @as(f64, @floatFromInt(workers)))
            return null;
    }
    // A cold header walk must not serialize the chunk reads ahead of the worker pool. Both the walk and
    // mapped decoders reuse these file-backed pages; no extra input buffer is retained by this hint.
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        const decision = plan.decide(rg, &metas[item.file]);
        if (decision == .skip or rg.num_rows == 0) continue;
        const source = args.inputs[item.file].plan_bytes orelse args.inputs[item.file].bytes;
        try row_subgroups.adviseGroup(source, rg, item.columns.forRowGroup(decision));
    }
    var split: std.ArrayList(WorkItem) = .empty;
    var changed = false;
    for (work, 0..) |item, wi| {
        var original = item;
        original.position = expr_agg.firstPosition(wi, 0);
        const rg = &metas[item.file].row_groups.items[item.rg];
        const decision = plan.decide(rg, &metas[item.file]);
        if (decision == .skip or rg.num_rows == 0) {
            try split.append(a, original);
            continue;
        }
        const read = item.columns.forRowGroup(decision);
        const weight = subgroupBytes(rg, read);
        const parts: usize = @intCast(@max(1, (target *| weight +| bytes - 1) / bytes));
        const source = args.inputs[item.file].plan_bytes orelse args.inputs[item.file].bytes;
        const group = if (try row_subgroups.Group.init(a, source, rg, &metas[item.file], read, parts)) |g| blk: {
            const ptr = try a.create(row_subgroups.Group);
            ptr.* = g;
            break :blk ptr;
        } else {
            try split.append(a, original);
            continue;
        };
        changed = true;
        for (group.ranges) |range| {
            var part = original;
            part.subgroup = .{ .group = group, .rows = range };
            part.position = expr_agg.firstPosition(wi, range.lo);
            try split.append(a, part);
        }
    }
    return if (changed) split.items else null;
}

fn subgroupProbeLeaves(
    a: std.mem.Allocator,
    f: filter_ast.Filter,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    leaves: *std.ArrayList(filter_ast.Filter),
) !void {
    if (f == .and_filter) {
        if (f.and_filter.in_set != null or f.and_filter.null_member) return;
        for (f.and_filter.children) |child| try subgroupProbeLeaves(a, child, rg, meta, leaves);
        return;
    }
    switch (f) {
        .int32, .int64, .float, .double, .boolean => {},
        else => return,
    }
    if (filter_prune.pruneRowGroup(rg, f, meta) == .always_match or
        filter_prune.passFraction(f, rg, meta) != 1) return;
    try leaves.append(a, f);
}

fn subgroupPassShare(
    a: std.mem.Allocator,
    source: []const u8,
    rg: *const schema.RowGroup,
    meta: *const schema.FileMetaData,
    filter: filter_ast.Filter,
    min_bytes: u64,
    workers: usize,
) !f64 {
    var leaves: std.ArrayList(filter_ast.Filter) = .empty;
    try subgroupProbeLeaves(a, filter, rg, meta, &leaves);
    if (leaves.items.len == 0) return 1;
    const n: usize = @intCast(@min(@as(u64, @intCast(rg.num_rows)), @max(@import("../hw.zig").cacheLineBytes(), min_bytes / @max(1, workers) / @sizeOf(i64))));
    const lookup = try a.alloc(?usize, rg.columns.items.len);
    @memset(lookup, null);
    var columns: std.ArrayList(filter_eval.Batch.Column) = .empty;
    for (leaves.items) |leaf| {
        const ci = filter_ast.leafColumn(leaf).?;
        if (ci >= lookup.len) return 1;
        if (lookup[ci] != null) continue;
        var reader = (try consumer.SliceReader.init(a, a, rg, meta, .{ .bytes = source, .byte_origin = 0 }, ci, .{ .encoded = false })) orelse return 1;
        defer reader.deinit();
        const col = try reader.next(a, n);
        lookup[ci] = columns.items.len;
        try columns.append(a, col);
    }
    var selected = try filter_selection.SelectionVector.init(a, n);
    defer selected.deinit();
    const batch: filter_eval.Batch = .{ .cols = columns.items, .num_rows = n };
    for (leaves.items) |leaf| try filter_eval.evaluate(leaf, &batch, &selected, lookup, a);
    return @as(f64, @floatFromInt(selected.count())) / @as(f64, @floatFromInt(n));
}

fn subgroupBytes(rg: *const schema.RowGroup, read: []const bool) u64 {
    var bytes: u64 = 0;
    for (rg.columns.items, 0..) |col, ci| {
        if (ci < read.len and read[ci]) if (col.meta_data) |cm| {
            bytes +|= @max(@as(u64, @intCast(@max(0, cm.total_uncompressed_size))), @as(u64, @intCast(@max(0, rg.num_rows))) *| 4);
        };
    }
    return bytes;
}

/// Frozen pre-planner dispatch, kept only for decision-equivalence tests. In particular, the census check is the
/// executor's old check rather than a call back into the new chooser.
fn referenceDispatch(in: PlanInputs) AggregateDispatch {
    const args = in.args;
    const use_blocks = in.keys != null and !args.unordered and orderSensitive(in.calls);
    const blocks = @max(1, @min(in.work.len, group_blocks_max));
    const tables: usize = if (in.keys == null) 0 else if (!use_blocks) in.workers else blocks;
    const sketch_fits = if (args.sketch_topk) |cfg|
        (in.workers *| (@as(usize, 4) << cfg.cell_bits)) *| 8 <= args.max_memory
    else
        false;
    const keys_unique = args.sketch_topk != null and keysLookUnique(in.metas, in.work, in.keys orelse &.{});
    const sketch_count: ?usize = if (args.sketch_topk) |cfg| if (sketch_fits)
        sketchEligible(cfg, args, in.outputs, in.calls, in.keys orelse &.{}, in.filter_cols, use_blocks, keys_unique or
            filterSmall(in.metas, in.work, in.keys orelse &.{}, in.filter_cols))
    else
        null else null;
    const first_cut = sketch_count == null and args.sketch_topk != null and args.sketch_topk.?.census and
        firstCutEligible(args, in.calls, in.keys orelse &.{}, use_blocks, in.work.len, tables);
    const census_fits = blk: {
        if (in.work.len >= expr_agg.Census.max_items) break :blk false;
        var rows: u64 = 0;
        for (in.work) |item| {
            if (item.agg_start != 0) break :blk false;
            rows +|= @intCast(@max(0, in.metas[item.file].row_groups.items[item.rg].num_rows));
        }
        const open = in.workers * expr_agg.Census.N_PARTS * @sizeOf(expr_agg.Census.Chunk);
        break :blk (rows *| @sizeOf(u32)) +| open <= args.max_memory / 4;
    };
    const distinct_count: ?usize = if (sketch_count == null and sketch_fits and census_fits) if (args.sketch_topk) |cfg|
        if (cfg.census) rankingCall(.count_distinct, cfg, args, in.outputs, in.calls, in.keys orelse &.{}, in.filter_cols, use_blocks, keys_unique or
            filterSmall(in.metas, in.work, in.keys orelse &.{}, in.filter_cols)) else null
    else
        null else null;
    const census_allowed = (sketch_count != null and args.sketch_topk.?.census and census_fits) or distinct_count != null;
    const fallback: @FieldType(AggregateDispatch, "strategy") = if (in.keys != null) .plain_hash else .scalar;
    return .{
        .strategy = if (first_cut) .first_keys else if (sketch_count != null or distinct_count != null) .sketch else fallback,
        .use_blocks = use_blocks,
        .tables = tables,
        .count_columns = args.sketch_topk != null and in.keys != null,
        .sketch_fits = sketch_fits,
        .keys_unique = keys_unique,
        .filter_small = args.sketch_topk != null and
            filterSmall(in.metas, in.work, in.keys orelse &.{}, in.filter_cols),
        .count_agg = sketch_count orelse distinct_count,
        .census_allowed = census_allowed,
        .census_fused = distinct_count != null or (census_allowed and (args.sketch_topk.?.fuse_census orelse keys_unique)),
        .distinct_census = distinct_count != null,
    };
}

test "bound plans preserve scalar absence and grouped output without query text" {
    const a = std.testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/full_match.parquet", a);
    defer a.free(bytes);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const ar = arena.allocator();
    const meta = try metadata.open(ar, bytes);
    const calls = try expr_parser.parseAggList(ar, "count(*) AS n", &meta, null);
    var bound: BoundAggregate = .{ .calls = calls, .filter = null };
    const args: MultiAggArgs = .{
        .inputs = &.{.{ .name = "bound", .bytes = bytes }},
        .aggregate = "this must never be parsed",
        .bound_aggregate = &bound,
        .parallelism = 1,
    };
    var scalar = try runMultiAggregate(a, ar, args);
    defer scalar.deinit(a);
    try std.testing.expectEqual(meta.num_rows, scalar.output.aggs[0].value.i);
    try std.testing.expectEqual(@as(?[]const []const AggValue, null), scalar.output.group_rows);
    const items = try expr_parser.parseGroupByOf(ar, "b", &meta, null, &.{});
    bound.keys = &.{items[0].expr};
    bound.outputs = &.{
        .{ .source = .{ .key = 0 }, .name = "b" },
        .{ .source = .{ .agg = 0 }, .name = "n" },
    };
    var grouped = try runMultiAggregate(a, ar, args);
    defer grouped.deinit(a);
    try std.testing.expect(grouped.output.group_rows != null);
    var count: i128 = 0;
    for (grouped.output.group_rows.?) |row| count += row[1].i;
    try std.testing.expectEqual(@as(i128, meta.num_rows), count);
}
