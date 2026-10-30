const std = @import("std");
const testing = std.testing;
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const filter_prune = @import("../filter/prune.zig");
const partagg = @import("../partagg.zig");
const ndv = @import("../ndv.zig");
const NdvStat = ndv.Stat;
const scan_types = @import("types.zig");
const Input = scan_types.Input;
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const plan_types = @import("plan_types.zig");
const PlanInputs = plan_types.PlanInputs;
const Reason = plan_types.Reason;
const AggregateDispatch = plan_types.AggregateDispatch;
const PlanFacts = plan_types.PlanFacts;
const ScanPlan = plan_types.ScanPlan;
const countCut = @import("count_cut.zig").countCut;
const resultCutReason = @import("rank_cut.zig").resultCutReason;
const loadFixture = @import("test_support.zig").loadFixture;

pub fn planAggregate(in: PlanInputs) ScanPlan {
    const args = in.args;
    const keys = in.keys orelse &.{};
    const part_on = partAggWanted(args, in.calls, in.keys != null, in.outputs);
    const use_blocks = in.keys != null and !args.unordered and orderSensitive(in.calls);
    const blocks = @max(1, @min(in.work.len, group_blocks_max));
    const tables: usize = if (in.keys == null) 0 else if (!use_blocks) in.workers else blocks;
    const sketch_fits = if (args.sketch_topk) |cfg|
        (in.workers *| (@as(usize, 4) << cfg.cell_bits)) *| 8 <= args.max_memory
    else
        false;
    const keys_unique = args.sketch_topk != null and keysLookUnique(in.metas, in.work, keys);
    const filter_small = args.sketch_topk != null and filterSmall(in.metas, in.work, keys, in.filter_cols);
    const count_agg = if (args.sketch_topk) |cfg| if (sketch_fits)
        // With NDV statistics, the plain-width guess only decides where a census counts (`applyCostRules`), never
        // whether an expensive filter is decoded twice for a sketch.
        sketchEligible(cfg, args, in.outputs, in.calls, keys, in.filter_cols, use_blocks, (keys_unique and in.ndv_stat == null) or filter_small)
    else
        null else null;
    const first_cut = count_agg == null and args.sketch_topk != null and args.sketch_topk.?.census and
        firstCutEligible(args, in.calls, in.keys orelse &.{}, use_blocks, in.work.len, tables);
    const distinct_agg = if (count_agg == null)
        distinctCensusEligible(in, use_blocks, sketch_fits, (keys_unique and in.ndv_stat == null) or filter_small)
    else
        null;
    const distinct_census = distinct_agg != null;
    const rank_agg = count_agg orelse distinct_agg;
    const census_allowed = distinct_census or (count_agg != null and args.sketch_topk.?.census and
        censusFits(in.metas, in.work, in.workers, args.max_memory));
    const census_fused = distinct_census or (census_allowed and (args.sketch_topk.?.fuse_census orelse keys_unique));
    const fallback: @FieldType(AggregateDispatch, "strategy") = if (in.keys != null) .plain_hash else .scalar;
    var result: ScanPlan = .{ .dispatch = .{
        .strategy = if (part_on) .partitioned_hash else if (first_cut)
            .first_keys
        else if (rank_agg != null) .sketch else fallback,
        .use_blocks = use_blocks,
        .tables = tables,
        .count_columns = args.sketch_topk != null and in.keys != null,
        .sketch_fits = sketch_fits,
        .keys_unique = keys_unique,
        .filter_small = filter_small,
        .count_agg = rank_agg,
        .census_allowed = census_allowed,
        .census_fused = census_fused,
        .distinct_census = distinct_census,
    } };
    const sketch_reason: Reason = blk: {
        if (args.sketch_topk == null) break :blk .sketch_disabled;
        if (!sketch_fits) break :blk .sketch_memory_limit;
        break :blk if (count_agg == null) .sketch_ineligible else .sketch_count_cut;
    };
    result.reasons.insert(sketch_reason);
    if (in.keys != null) if (in.outputs) |outputs| {
        if (args.result_order.limit != null and args.result_order.keys.len > 0)
            result.reasons.insert(resultCutReason(outputs, in.calls, args.result_order, args.unordered));
    };
    if (args.partagg) result.reasons.insert(if (part_on) .partitioned_forced else .partitioned_ineligible);
    if (keys_unique) result.reasons.insert(.keys_look_unique);
    if (filter_small) result.reasons.insert(.filter_small);
    if (use_blocks) result.reasons.insert(.ordered_blocks);
    if (first_cut) result.reasons.insert(.first_keys_cut);
    if (rank_agg == null and !first_cut) result.reasons.insert(.plain_scan);
    if (distinct_census) result.reasons.insert(.census_distinct);
    if (count_agg != null) {
        const census_reason: Reason = blk: {
            if (!args.sketch_topk.?.census) break :blk .census_disabled;
            if (!census_allowed) break :blk .census_memory_or_work_limit;
            break :blk if (census_fused) .census_fused else .census_on_sketch_bail;
        };
        result.reasons.insert(census_reason);
    }
    result.facts = planFacts(in, rank_agg != null, first_cut or (count_agg == null and args.sketch_topk != null and
        args.sketch_topk.?.census and
        firstCutEligible(args, in.calls, in.keys orelse &.{}, use_blocks, in.work.len, tables)));
    if (in.ndv_stat) |stat| applyCostRules(in, stat, &result);
    return result;
}

/// The rule that decided the plan's strategy, among its reasons: the cost rules first (they override eligibility),
/// then the eligibility that left the strategy standing.
pub fn decidingRule(plan: ScanPlan) Reason {
    for ([_]Reason{
        .partitioned_forced, .few_dictionary_entries,   .selective_filter,   .repeated_dictionary_entries,
        .small_ndv,          .high_cardinality_strings, .worker_copy_memory, .first_keys_cut,
        .census_distinct,    .sketch_count_cut,         .unknown_ndv,
    }) |r| if (plan.reasons.contains(r)) return r;
    return .plain_scan;
}

fn planFacts(in: PlanInputs, rank_cut: bool, first_cut: bool) PlanFacts {
    var f: PlanFacts = .{
        .readers = in.readers,
        .pass_share = if (in.filter != null) passShare(in) else null,
        .sampled_pass = in.sampled_pass,
    };
    for (in.work) |item| if (item.agg_start == 0) {
        f.row_groups += 1;
        f.rows +|= @intCast(@max(0, in.metas[item.file].row_groups.items[item.rg].num_rows));
    };
    if (in.keys == null) {
        f.admissible.insert(.scalar);
        return f;
    }
    f.admissible.insert(.plain_hash);
    if (partAggSupported(in.args, in.calls, true)) f.admissible.insert(.partitioned_hash);
    if (rank_cut) f.admissible.insert(.sketch);
    if (first_cut) f.admissible.insert(.first_keys);
    return f;
}

fn plainDispatch(plan: *ScanPlan, strategy: @FieldType(AggregateDispatch, "strategy")) void {
    plan.dispatch.strategy = strategy;
    plan.dispatch.count_agg = null;
    plan.dispatch.count_columns = false;
    plan.dispatch.census_allowed = false;
    plan.dispatch.census_fused = false;
    plan.dispatch.census_cell_repeat_gate = false;
    plan.dispatch.distinct_census = false;
    for ([_]Reason{
        .sketch_count_cut,              .first_keys_cut,                   .census_fused,
        .census_on_sketch_bail,         .census_needs_uniqueness_evidence, .census_fixed_width_bail,
        .census_short_string_composite, .census_short_string_payload,      .census_distinct,
    }) |r|
        plan.reasons.remove(r);
    plan.reasons.insert(.plain_scan);
}

pub const selective_share = 0.05;

/// The share of rows the filter passes, estimated from row-group bounds (`filter_prune.passFraction`); 1 without a
/// filter or rows. An estimate: it only steers a cost rule between exact strategies.
pub fn passShare(in: PlanInputs) f64 {
    const f = in.filter orelse return 1;
    var rows: f64 = 0;
    var pass: f64 = 0;
    for (in.work) |item| {
        if (item.agg_start != 0) continue;
        const m = &in.metas[item.file];
        const rg = &m.row_groups.items[item.rg];
        const n: f64 = @floatFromInt(@max(0, rg.num_rows));
        rows += n;
        pass += n * filter_prune.passFraction(f, rg, m);
    }
    return if (rows == 0) 1 else pass / rows;
}

/// A proven upper bound on the share of rows the filter passes: the rows of the row groups its statistics leave in
/// (`filter_prune.pruneRowGroup`). 1 without a filter or rows.
pub fn provenPassShare(in: PlanInputs) f64 {
    const f = in.filter orelse return 1;
    if (in.args.scan_all) return 1;
    var rows: f64 = 0;
    var kept: f64 = 0;
    for (in.work) |item| {
        if (item.agg_start != 0) continue;
        const m = &in.metas[item.file];
        const rg = &m.row_groups.items[item.rg];
        const n: f64 = @floatFromInt(@max(0, rg.num_rows));
        rows += n;
        if (filter_prune.pruneRowGroup(rg, f, m) != .skip) kept += n;
    }
    return if (rows == 0) 1 else kept / rows;
}

/// The share of rows the filter passes in the first row group the scan reads (in input order, so the same at every
/// `-j`), counted by decoding that row group's filter columns once; 1 where its decode does not fit `decode_limit`, and
/// null where the input's bytes are not the whole file (an S3 input's fetched ranges).
pub fn sampledPassShare(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    plan: *const agg_plan.Plan,
    metas: []const schema.FileMetaData,
    inputs: []const Input,
    work: []const WorkItem,
    options: consumer.DecodeOptions,
    decode_limit: ?usize,
) !?f64 {
    for (work) |item| {
        if (item.agg_start != 0) continue;
        const meta = &metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        const decision = plan.decide(rg, meta);
        if (decision == .skip) continue;
        const in = inputs[item.file];
        const bytes = in.plan_bytes orelse in.bytes;
        if (std.mem.startsWith(u8, in.name, "s3://") or (in.logical_size != 0 and in.logical_size != bytes.len))
            return null;
        if (decision == .full_match or rg.num_rows <= 0) return 1;
        const read = try arena.alloc(bool, plan.filter_cols.len);
        for (read, plan.filter_cols, plan.columns.read) |*r, f, whole| r.* = f and whole;
        const consumed = try arena.alloc(bool, plan.filter_cols.len);
        @memset(consumed, false);
        var rg_arena = consumer.RowGroupArena.init(gpa);
        defer rg_arena.deinit();
        var scratch = consumer.DecodeScratch.init(gpa);
        defer scratch.deinit();
        var timings: consumer.Timings = .{};
        var budget: ?consumer.DecodeBudget = if (decode_limit) |l| .{ .limit = l } else null;
        const calls = [_]expr_agg.AggCall{
            .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 },
        };
        var passed = [_]expr_agg.Accumulator{.{ .count = 0 }};
        _ = consumer.scanRGForAgg(gpa, .{
            .rg = rg,
            .file_index = item.file,
            .rg_index = item.rg,
            .meta = meta,
            .src = .{ .bytes = in.plan_bytes orelse in.bytes, .byte_origin = 0 },
            .filter = plan.filterFor(decision),
            .read = read,
            .consumed = consumed,
        }, plan.stats, options, .{ .scalar = .{ .calls = &calls, .accumulators = &passed } }, .{
            .defer_decode_errors = true,
            .scratch = &scratch,
            .rg_arena = &rg_arena,
            .timings = &timings,
            .budget = if (budget) |*b| b else null,
        }) catch |e| {
            // Optional costing reads only filter columns, so it cannot choose the query's canonical failure.
            if (e == error.ExceededMemoryBudget or consumer.isDecodeError(e)) return 1;
            return e;
        };
        return @as(f64, @floatFromInt(passed[0].count)) / @as(f64, @floatFromInt(rg.num_rows));
    }
    return 0;
}

/// A lone key whose dictionaries, summed over row groups, hold few entries: grouping by their codes is cheap.
pub const few_entries = 1 << 23;
/// The union `fewEntries` needs: a small domain, or one an eighth of the entries or less.
pub const few_union: ndv.UnionLimit = .{ .entries = few_entries, .values = 1 << 17, .recur = 8 };

/// Grouping a lone key's codes plainly is cheap where they resolve few entries in all and the union proves either a
/// small domain or keys that recur across row groups (the dense fold's case); a large domain whose keys each sit in
/// one or two row groups (clustered) pays for a union and declines the fold.
pub fn fewEntries(stat: NdvStat, keys: []const expr_ast.Expr) bool {
    const n = stat.entries orelse return false;
    if (keys.len != 1 or !stat.union_complete or n > few_union.entries) return false;
    return stat.hi <= few_union.values or n >= stat.hi *| few_union.recur;
}

fn plainGroupRowBytes(stat: NdvStat, calls: usize) u64 {
    return (stat.key_bytes +| (@as(u64, @sizeOf(expr_agg.Accumulator)) *| calls) +| 32) *| 2;
}

/// Profitability only: selected rows cap how much ordinary group state this pass can touch. Dense lanes still
/// reserve their full domain; neither this estimate nor an NDV hint may lower a correctness/memory bound.
pub fn selectedGroupBytes(stat: NdvStat, calls: usize, workers: usize, share: f64) u64 {
    const fraction = std.math.clamp(share, 0, 1);
    const raw: f64 = @floatFromInt(stat.rows);
    const estimate = raw * fraction;
    const selected: u64 = if (estimate >= raw) stat.rows else if (estimate <= 0) 0 else @intFromFloat(estimate);
    return @min(selected, stat.hi *| workers) *| plainGroupRowBytes(stat, calls);
}

pub fn applyCostRules(in: PlanInputs, stat: NdvStat, plan: *ScanPlan) void {
    plan.ndv_stat = stat;
    if (stat.union_complete) {
        plan.reasons.insert(.dictionary_union);
    } else if (stat.union_attempted) {
        plan.reasons.insert(.dictionary_union_incomplete);
    }
    if (stat.union_budget_exceeded) plan.reasons.insert(.dictionary_union_budget);
    var state_unknown = false;
    for (in.calls) |call| {
        state_unknown = state_unknown or call.func == .count_distinct or call.result == .bytes;
    }
    const per_group = plainGroupRowBytes(stat, in.calls.len);
    const single = stat.hi *| per_group;
    const copies = single *| plan.dispatch.tables;
    if (!state_unknown) plan.plain_bytes = copies else plan.reasons.insert(.state_memory_unknown);
    if (plan.dispatch.distinct_census) {
        // A proven small key domain puts most rows under the admitted keys: the census would cost a pass and save
        // little. Unbounded or large domains keep it; skew is judged from the count pass (`Census.distinct`).
        if (stat.confidence == .upper_bound and stat.hi < distinctCensusMinGroups(in.args)) {
            plainDispatch(plan, .plain_hash);
            plan.reasons.insert(.small_ndv);
        }
        return;
    }
    const group_bytes = if (in.args.plain_rules) in.dense_bytes orelse copies else copies;
    if (!state_unknown) plan.group_bytes = group_bytes;
    const small = stat.confidence == .upper_bound and stat.hi <= 1 << 20 and
        !state_unknown and group_bytes <= in.args.max_memory / 8;
    const bounded_census = stat.confidence == .upper_bound and stat.hi <= 1 << 20;
    const tiny = bounded_census and stat.hi <= 1 << 14;
    const keys = in.keys orelse &.{};
    // Few local dictionary entries make clustered/skewed R1 scans cheap. Composite marginal bounds cannot
    // establish that cost; the census repeat cap can still admit their proven moderate tuple bound.
    // A small domain whose every key recurs across row groups (their dictionaries sum to 8x the union or more) spreads
    // the sketch's cells evenly, while its codes resolve few entries: one plain pass wins with payloads too (measured
    // 1.3-1.9x, i32_100k, files50); clustered keys, whose dictionaries barely overlap, keep the sketch.
    const repeated = small and stat.union_complete and fewEntries(stat, keys) and stat.entries.? >= stat.hi *| 8;
    const cheap_r1 = small and (tiny or repeated or (keys.len == 1 and (in.calls.len == 1 or
        stat.dictionary_bytes <= (stat.hi *| stat.key_bytes) *| 4)));
    // Short string tuples may have sparse repeats even when their string marginal repeats heavily. Keep
    // payloads and long keys on the existing path; filtered, only the runtime sparse-repeat hint can justify this
    // census.
    const short_composite = !small and stat.string_keys and keys.len > 1 and
        stat.key_bytes < 32 and in.calls.len == 1;
    // A short byte key plus total payloads can avoid almost all losing-group state. The encoded average width
    // is a cost hint, bounded by the packed key's 15 bytes plus its length word; complete keys still decide equality.
    const short_payload = stat.string_keys and keys.len == 1 and keys[0] == .col_ref and
        keys[0].col_ref.expr_type == .str and in.calls.len > 1 and !in.args.filtered() and
        stat.key_bytes <= 15 + @sizeOf(u32);
    const bail_census = plan.dispatch.strategy == .sketch and plan.dispatch.census_allowed and
        (expr_agg.fixedGroupStride(keys) != null or short_composite or short_payload) and !tiny and !cheap_r1;
    // Dictionary bounds never authorize automatic fusion. A census after a bail is a separate, certified cost bet; it
    // rides along the count pass when a fixed-width key is stored at plain width (`keysLookUnique`), a guess that only
    // decides where the census counts: each worker still drops it once its own cells separate the cut.
    const explicit_census = if (in.args.sketch_topk) |cfg| cfg.fuse_census != null else false;
    if (!explicit_census) {
        const looks_unique = plan.dispatch.keys_unique and bail_census and !short_composite;
        // A fixed-width composite's cells are spread by every combination its marginals make, so they bail far more
        // often than one key's; counting again after the bail cost more than riding along and being dropped.
        const fixed_composite = bail_census and !short_composite and keys.len > 1 and
            expr_agg.fixedGroupStride(keys) != null;
        // Unfiltered, a short string composite's count pass reads every row the census would, and its cells, spread
        // by every combination the marginals make, bail like a fixed-width composite's: the census rides along and
        // each worker still drops it once its own cells separate the cut. Filtered, the bet waits for the count.
        const unfiltered_short = bail_census and short_composite and !in.args.filtered();
        plan.dispatch.keys_unique = looks_unique;
        plan.dispatch.census_allowed = bail_census;
        plan.dispatch.census_fused = looks_unique or fixed_composite or unfiltered_short or
            (bail_census and short_payload);
        plan.dispatch.census_cell_repeat_gate = bail_census and !unfiltered_short and
            (short_composite or !bounded_census);
        plan.reasons.remove(.keys_look_unique);
        plan.reasons.remove(.census_fused);
        plan.reasons.remove(.census_on_sketch_bail);
        if (looks_unique) plan.reasons.insert(.keys_look_unique);
        if (plan.dispatch.census_fused) plan.reasons.insert(.census_fused);
        if (plan.dispatch.strategy == .sketch) {
            if (bail_census) {
                plan.reasons.insert(if (short_composite) .census_short_string_composite else if (short_payload)
                    .census_short_string_payload
                else
                    .census_fixed_width_bail);
                plan.reasons.insert(.census_on_sketch_bail);
            } else plan.reasons.insert(.census_needs_uniqueness_evidence);
        }
    }
    if (plan.dispatch.strategy == .partitioned_hash) return; // Explicit force has precedence over profitability.
    if (plan.dispatch.strategy == .first_keys) {
        // Prefix discovery avoids grouping every key, but its admit pass fingerprints every row: where the codes
        // resolve few dictionary entries in all, grouping them all is cheaper (measured 1.5-3x, card10 to files50).
        if (in.args.plain_rules and fewEntries(stat, keys)) {
            plainDispatch(plan, .plain_hash);
            plan.reasons.insert(.few_dictionary_entries);
        }
        return;
    }
    // A count cut reads the filter and keys twice to skip grouping most rows; a filter its row groups' bounds say
    // passes few rows leaves little grouping to skip (measured 1.2-2.1x at 1%, while the cut still wins at 30%). The
    // even-spread estimate alone never picks the plain table, whose size it cannot bound (on skewed values it put nine
    // rows in ten at a thousandth): the row groups the statistics leave in must hold few rows, or the groups' bound
    // fit the sketch's own share of the budget, or the first row group the scan reads pass few rows, counted. A
    // refusal of the plain table still runs the cut (`FastPath.plain_rule`).
    const groups_fit = stat.confidence == .upper_bound and !state_unknown and group_bytes <= in.args.max_memory / 8;
    if (in.args.plain_rules and plan.dispatch.strategy == .sketch and !plan.dispatch.distinct_census and
        passShare(in) <= selective_share)
    select: {
        const kept = provenPassShare(in);
        if (!groups_fit and kept > selective_share) {
            if (in.sampled_pass) |sampled| {
                if (kept * sampled > selective_share) break :select;
            } else if (!in.sample_unread) {
                plan.wants_sample = true;
                break :select;
            }
        }
        plainDispatch(plan, .plain_hash);
        plan.reasons.insert(.selective_filter);
    }
    if (small and !bail_census) {
        plainDispatch(plan, .plain_hash);
        plan.reasons.insert(.small_ndv);
        if (repeated) plan.reasons.insert(.repeated_dictionary_entries);
        return;
    }
    if (stat.source == .unknown or stat.covered_rows != stat.rows) {
        plan.reasons.insert(.unknown_ndv);
        // Numeric unknown keys retain the sketch (zipf11); a census bail also needs a sparse-repeat cell hint.
        // Recounting unknown string marginals can lose even when grouping is expensive. Keep only the short-tuple
        // sparse-repeat bet here; partial coverage may still retain a useful sketch for skew (ClickBench).
        if (stat.string_keys and stat.covered_rows == 0 and stat.encoding_stats_rows == stat.rows and
            plan.dispatch.strategy == .sketch and !((short_composite or short_payload) and bail_census))
            plainDispatch(plan, .plain_hash);
    }
    const long_strings = stat.string_keys and stat.key_bytes >= 32 and stat.rows >= 65536;
    // Upper bounds can certify small tables, but cannot establish pressure. A single-key union is a cost hint;
    // composite products remain bounds only. Execution still enforces the budget on either path.
    const pressure_groups = @max(stat.lo, stat.estimate orelse 0);
    const pressure_copies = (pressure_groups *| per_group) *| plan.dispatch.tables;
    // Not beside a reader: its window takes memory too, and the shared partitions are not held to the budget (their
    // RSS ran far past it, and slower, on cold reader scans). Reader inputs keep the plain plan they ran before.
    const memory_pressure = !in.readers and !state_unknown and pressure_copies > in.args.max_memory / 2 and
        single <= in.args.max_memory;
    const partition_ok = in.args.partition_cost and partAggSupported(in.args, in.calls, in.keys != null);
    if ((long_strings or memory_pressure) and partition_ok) {
        // A known union can expose worker-copy pressure even in a sketch's eventual plain fallback.
        if (plan.dispatch.strategy == .plain_hash or (memory_pressure and plan.dispatch.strategy == .sketch) or
            (long_strings and stat.covered_rows == 0 and stat.encoding_stats_rows == stat.rows and
                plan.dispatch.strategy == .sketch))
        {
            plainDispatch(plan, .partitioned_hash);
            plan.reasons.insert(if (long_strings) .high_cardinality_strings else .worker_copy_memory);
        }
    }
}

/// Ordered GROUP BY folds the work list in at most this many fixed blocks, each into a table of its own (see
/// `MultiAggArgs.unordered`). Fixed, not the worker count: the blocks, and so the order partial sums meet in, must not
/// depend on `-j`. Enough that 32 workers share them evenly; few enough that their tables cost little more than one
/// per worker.
pub const group_blocks_max = 64;

/// Plain GROUP BY that the shared partitions can answer exactly, with the same bits at every `-j`.
fn partAggWanted(
    args: MultiAggArgs,
    calls: []const expr_agg.AggCall,
    grouped: bool,
    outputs: ?[]const BoundOutputColumn,
) bool {
    if (!args.partagg or !partAggSupported(args, calls, grouped)) return false;
    if (outputs) |outs| if (countCut(outs, calls, args.result_order) != null) return false;
    return true;
}

pub fn partAggSupported(args: MultiAggArgs, calls: []const expr_agg.AggCall, grouped: bool) bool {
    if (!grouped or orderSensitive(calls)) return false;
    for (calls) |c| if (c.func == .count_distinct) return false;
    return args.result_order.limit == null or args.result_order.keys.len > 0;
}

/// Whether merging partial results in another order can change their bits: float sums and averages round
/// differently, and a float min or max may come out -0 or +0. Integer and string results never do.
pub fn orderSensitive(calls: []const expr_agg.AggCall) bool {
    for (calls) |c| {
        if (c.func == .count or c.func == .count_distinct) continue;
        if (c.result == .f64) return true;
        // An avg sums integer values exactly (`AvgState.isum`); only a float argument's sum depends on order.
        if (c.result == .avg_f64 and (c.arg == null or c.arg.?.typeOf() != .i64)) return true;
    }
    return false;
}

/// The COUNT call a sketched scan can certify the cut by: the first sort key is that call, descending, under a LIMIT.
/// Per-worker tables only (not ordered blocks), and only inputs read whole up front: a gated input reads each row
/// group once, and the sketch reads them twice.
pub fn sketchEligible(
    cfg: SketchTopK,
    args: MultiAggArgs,
    outputs: ?[]const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    filter_cols: []const bool,
    use_blocks: bool,
    filter_ok: bool,
) ?usize {
    return rankingCall(.count, cfg, args, outputs, calls, keys, filter_cols, use_blocks, filter_ok);
}

/// The `func` call ranking a GROUP BY first, descending, under a LIMIT: see `sketchEligible`.
pub fn rankingCall(
    func: expr_agg.AggFunc,
    cfg: SketchTopK,
    args: MultiAggArgs,
    outputs: ?[]const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    filter_cols: []const bool,
    use_blocks: bool,
    filter_ok: bool,
) ?usize {
    const outs = outputs orelse return null;
    const order = args.result_order;
    const limit = order.limit orelse return null;
    if (use_blocks or order.keys.len == 0 or !order.keys[0].descending) return null;
    const a_idx = switch (outs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (calls[a_idx].func != func) return null;
    if (!callsTotal(calls)) return null;
    // The admitted cells must be few beside all of them, or admitting is grouping nearly everything.
    const k = order.offset +| limit;
    if (k == 0 or cfg.cell_bits < 4 or k *| cfg.admit_per_row > (@as(u64, 1) << cfg.cell_bits) / 16) return null;
    for (args.inputs) |in| if (in.gate != null) return null;
    // Both passes decode every column the filter reads. Beyond the group keys, which both passes decode anyway, that
    // is paid twice, and a filter over a long column (`URL LIKE '%google%'`) costs more than the cut saves. Not so for
    // a filter over columns small beside the keys, or against grouping keys that look unique (`keysLookUnique`),
    // which costs far more than a second decode (`filter_ok`).
    if (!filter_ok) for (filter_cols, 0..) |reads, col| if (reads) {
        const is_key = for (keys) |key| {
            if (key == .col_ref and key.col_ref.col_idx == col) break true;
        } else false;
        if (!is_key) return null;
    };
    return a_idx;
}

/// The count(DISTINCT) call a census can certify a cut by (`certifiedTopK`): the first sort key is that call,
/// descending, under a LIMIT, over inputs read whole, and a census of every row fits. A key has no more distinct
/// values than rows, so the census's row counts bound them as they bound a COUNT.
fn distinctCensusEligible(in: PlanInputs, use_blocks: bool, sketch_fits: bool, filter_ok: bool) ?usize {
    const cfg = in.args.sketch_topk orelse return null;
    const keys = in.keys orelse return null;
    if (!cfg.census or !sketch_fits) return null;
    const a_idx = rankingCall(.count_distinct, cfg, in.args, in.outputs, in.calls, keys, in.filter_cols, use_blocks, filter_ok) orelse
        return null;
    return if (censusFits(in.metas, in.work, in.workers, in.args.max_memory)) a_idx else null;
}

/// Whether folding `calls` can never fail on a row. An argument computed from the row can (`1e12 / q` divides by
/// zero, arithmetic overflows), and so can a DECIMAL sum or mean, past its precision. The sketch, census and
/// first-keys cuts never read some rows' aggregate inputs, and would drop their errors and answer where the full scan
/// fails, so they take only these calls. Keys and the outer filter are read on every row by the count or discovery
/// pass, and filter leaves never fail.
fn callsTotal(calls: []const expr_agg.AggCall) bool {
    for (calls) |call| {
        if (call.arg) |arg| switch (arg) {
            .col_ref, .literal => {},
            else => return false,
        };
        if (call.result == .avg_dec or (call.result == .dec and call.func == .sum)) return false;
    }
    return true;
}

/// Fewer groups than this, proven, and the keys a count(DISTINCT) census admits hold much of the input.
fn distinctCensusMinGroups(args: MultiAggArgs) u64 {
    const order = args.result_order;
    const admit = if (args.sketch_topk) |cfg| cfg.admit_per_row else 16;
    return (order.offset +| (order.limit orelse 0)) *| admit *| 64;
}

/// Whether a census should ride along the sketch's cell count pass: some fixed-width key column is stored at nearly its
/// plain width in the first row group read, so its values barely repeat there. Keys that barely repeat leave the cells
/// too even to certify a cut. Otherwise the cells count alone, and the census counts in a pass of its own only if
/// they bail.
pub fn keysLookUnique(metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr) bool {
    if (work.len == 0) return false;
    for (keys) |key| if (key == .col_ref and wideKey(metas, work[0..1], key.col_ref.col_idx)) return true;
    return false;
}

/// Stored width is a cost proxy for wide/near-unique fixed-width keys, without reading optional pages.
pub fn wideKey(metas: []const schema.FileMetaData, work: []const WorkItem, col: usize) bool {
    var raw: u64 = 0;
    var stored: u64 = 0;
    for (work) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        if (rg.num_rows <= 0) continue;
        if (col >= rg.columns.items.len) return false;
        const cm = rg.columns.items[col].meta_data orelse return false;
        const width: u64 = switch (cm.type) {
            .INT32, .FLOAT => 4,
            .INT64, .DOUBLE => 8,
            else => return false,
        };
        raw +|= @as(u64, @intCast(rg.num_rows)) *| width;
        stored +|= @intCast(@max(0, cm.total_uncompressed_size));
    }
    return raw != 0 and stored *| 4 >= raw *| 3;
}

/// Whether the columns a filter reads beyond the group keys are small beside the keys in the first row group read:
/// at most four times their bytes, uncompressed. A sketched scan decodes those columns twice.
pub fn filterSmall(metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr, filter_cols: []const bool) bool {
    if (work.len == 0) return false;
    const rg = &metas[work[0].file].row_groups.items[work[0].rg];
    const size = struct {
        fn of(r: *const schema.RowGroup, col: usize) i64 {
            if (col >= r.columns.items.len) return 0;
            const md = r.columns.items[col].meta_data orelse return 0;
            return @max(0, md.total_uncompressed_size);
        }
    }.of;
    var key_bytes: i64 = 0;
    for (keys) |key| {
        if (key != .col_ref) return false;
        key_bytes +|= size(rg, key.col_ref.col_idx);
    }
    var extra: i64 = 0;
    for (filter_cols, 0..) |reads, col| if (reads) {
        const is_key = for (keys) |key| {
            if (key.col_ref.col_idx == col) break true;
        } else false;
        if (!is_key) extra +|= size(rg, col);
    };
    return extra <= key_bytes *| 4;
}

/// Four-byte identity records plus producer frontiers must leave room for decode, reduction and exact residual
/// state. An item must be a whole row group, so each row is counted once.
fn censusFits(metas: []const schema.FileMetaData, work: []const WorkItem, n_workers: usize, max_memory: usize) bool {
    if (work.len >= expr_agg.Census.max_items) return false;
    var rows: u64 = 0;
    for (work) |item| {
        if (item.agg_start != 0) return false;
        rows +|= @intCast(@max(0, metas[item.file].row_groups.items[item.rg].num_rows));
    }
    const open = n_workers * expr_agg.Census.N_PARTS * @sizeOf(expr_agg.Census.Chunk);
    return (rows *| @sizeOf(u32)) +| open <= max_memory / 4;
}

/// A LIMIT with no ORDER BY over a grouped scan of whole inputs (`firstAppearanceCut`).
pub fn firstCutEligible(
    args: MultiAggArgs,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    use_blocks: bool,
    n_items: usize,
    n_tables: usize,
) bool {
    const order = args.result_order;
    // Rows after the prefix are read only for the keys it found: a failing call on another key's row must not vanish.
    if (!callsTotal(calls)) return false;
    // Discovery omits payloads; a fallible key expression must retain the ordinary projection/error order.
    for (keys) |key| if (key != .col_ref and key != .literal) return false;
    const limit = order.limit orelse return false;
    if (use_blocks or n_tables == 0 or order.keys.len != 0 or order.having != null or
        order.offset +| limit == 0) return false;
    if (n_items == 0 or n_items >= expr_agg.Census.max_items) return false;
    for (args.inputs) |in| if (in.gate != null) return false;
    return true;
}

test "planAggregate: forced partitions obey aggregate and cut eligibility" {
    const calls = [_]expr_agg.AggCall{.{
        .func = .count,
        .arg = null,
        .where = null,
        .alias = "c",
        .result = .i64,
    }};
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .BYTE_ARRAY,
        .expr_type = .str,
    } }};
    var in: PlanInputs = .{
        .args = .{ .inputs = &.{}, .aggregate = "count(*)", .partagg = true },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = &keys,
        .outputs = &outputs,
        .filter_cols = &.{false},
        .workers = 3,
    };
    var got = planAggregate(in);
    try testing.expectEqual(.partitioned_hash, got.dispatch.strategy);
    try testing.expect(got.reasons.contains(.partitioned_forced));
    in.args.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 };
    got = planAggregate(in);
    try testing.expectEqual(.plain_hash, got.dispatch.strategy);
    try testing.expect(got.reasons.contains(.partitioned_ineligible));
    in.args.result_order = .{ .limit = 3 };
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy);
    in.args.result_order = .{};
    in.keys = null;
    try testing.expectEqual(.scalar, planAggregate(in).dispatch.strategy);
}

test "planAggregate: NDV precedence, census evidence and worker-copy pressure" {
    const calls = [_]expr_agg.AggCall{.{
        .func = .count,
        .arg = null,
        .where = null,
        .alias = "c",
        .result = .i64,
    }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } }};
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var in: PlanInputs = .{
        .args = .{
            .inputs = &.{},
            .aggregate = "count(*)",
            .sketch_topk = .{},
            .max_memory = 16 << 30,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = &keys,
        .outputs = &outputs,
        .filter_cols = &.{false},
        .workers = 16,
        .ndv_stat = .{
            .source = .dictionary_union,
            .confidence = .upper_bound,
            .rows = 30_000_000,
            .covered_rows = 30_000_000,
            .hi = 1000,
            .key_bytes = 9,
            .union_complete = true,
        },
    };
    var plan = planAggregate(in);
    try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
    try testing.expect(plan.reasons.contains(.small_ndv));
    try testing.expect(!plan.dispatch.count_columns and !plan.dispatch.census_allowed);

    in.args.result_order = .{ .limit = 10 };
    // First-prefix discovery must keep precedence even with a proven tiny upper bound.
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/partagg_budget.parquet", testing.allocator);
    defer testing.allocator.free(bytes);
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    in.metas = &.{meta};
    in.work = &.{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} },
        .accumulators = &.{},
    }};
    try testing.expectEqual(.first_keys, planAggregate(in).dispatch.strategy);
    // Unless the headers prove the codes resolve few entries in all, and the union completed: one plain pass is
    // then cheaper.
    in.ndv_stat.?.entries = 29_000;
    plan = planAggregate(in);
    try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
    try testing.expect(plan.reasons.contains(.few_dictionary_entries) and !plan.reasons.contains(.first_keys_cut));
    in.ndv_stat.?.entries = (1 << 23) + 1;
    try testing.expectEqual(.first_keys, planAggregate(in).dispatch.strategy);
    in.ndv_stat.?.entries = null;
    in.work = &.{};
    in.metas = &.{};
    in.args.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    in.ndv_stat = .{ .rows = 30_000_000, .encoding_stats_rows = 30_000_000, .hi = 30_000_000, .key_bytes = 9 };
    plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy); // Unknown numeric zipf11.
    try testing.expect(plan.dispatch.census_allowed and !plan.dispatch.keys_unique);
    try testing.expect(plan.dispatch.census_cell_repeat_gate and !plan.dispatch.census_fused);
    in.ndv_stat.?.string_keys = true;
    in.ndv_stat.?.key_bytes = 15;
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy); // Short fallback strings.
    in.ndv_stat.?.covered_rows = 100_000;
    try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy); // Partial ClickBench coverage.
    in.ndv_stat.?.covered_rows = 0;
    in.ndv_stat.?.key_bytes = 128;
    try testing.expectEqual(.partitioned_hash, planAggregate(in).dispatch.strategy);
    in.args.result_order = .{};
    in.args.max_memory = 128 << 20;
    in.ndv_stat = .{
        .source = .dictionary_union,
        .confidence = .upper_bound,
        .hi = 100_000,
        .estimate = 100_000,
        .rows = 30_000_000,
        .covered_rows = 30_000_000,
        .key_bytes = 9,
        .union_complete = true,
    };
    plan = planAggregate(in);
    try testing.expectEqual(.partitioned_hash, plan.dispatch.strategy);
    try testing.expect(plan.reasons.contains(.worker_copy_memory));
    in.args.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    try testing.expectEqual(.partitioned_hash, planAggregate(in).dispatch.strategy);
    in.ndv_stat.?.hi = 30_000_000;
    in.ndv_stat.?.estimate = null;
    in.ndv_stat.?.key_bytes = 18;
    in.args.max_memory = 16 << 30;
    // A composite dictionary product is only an upper bound, even when every union completed.
    try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy);
}

test "planAggregate: a plain-width key may fuse a census but never sketch through a wide filter" {
    const path = "ci/fixtures/parquet/census_widefilter.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" } };
    const work = [_]WorkItem{.{
        .file = 0,
        .rg = 0,
        .agg_start = 0,
        .agg_len = 1,
        .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} },
        .accumulators = &.{},
    }};
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    // The filter reads only `p`, which is far wider than the key.
    for ([_]bool{ false, true }) |census| {
        var in: PlanInputs = .{
            .args = .{
                .inputs = &.{},
                .aggregate = "count(*)",
                .sketch_topk = .{ .census = census },
                .max_memory = 64 << 20,
                .result_order = order,
            },
            .metas = &.{meta},
            .work = &work,
            .calls = &calls,
            .keys = &keys,
            .outputs = &outputs,
            .filter_cols = &.{ false, true },
            .workers = 1,
            .ndv_stat = .{ .rows = 12000, .encoding_stats_rows = 12000, .hi = 12000, .key_bytes = 9 },
        };
        const plan = planAggregate(in);
        try testing.expect(plan.dispatch.strategy != .sketch);
        try testing.expect(!plan.dispatch.census_fused);
        // Without NDV statistics the guess keeps its old role of admitting the sketch through the filter.
        in.ndv_stat = null;
        try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy);
    }
}

test "planAggregate: census bail cost bets preserve R1 and gate key types" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = .{ .col_ref = .{
            .col_idx = 0,
            .physical_type = .INT64,
            .expr_type = .i64,
        } }, .where = null, .alias = "s", .result = .i64 },
    };
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } },
    };
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "c" }};
    var in: PlanInputs = .{
        .args = .{
            .inputs = &.{},
            .aggregate = "count(*)",
            .sketch_topk = .{},
            .max_memory = 12 << 30,
            .result_order = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 10 },
        },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = keys[0..1],
        .outputs = &outputs,
        .filter_cols = &.{false},
        .workers = 16,
        .ndv_stat = .{
            .source = .dictionary_union,
            .confidence = .upper_bound,
            .hi = 100_000,
            .rows = 30_000_000,
            .covered_rows = 30_000_000,
            .key_bytes = 9,
            .dictionary_bytes = 23 << 20,
            .union_complete = true,
        },
    };
    var plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy);
    try testing.expect(plan.dispatch.census_allowed and !plan.dispatch.census_fused);
    // Barely recurring keys (clustered dictionaries) keep the sketch; keys that recur across row groups, whose codes
    // resolve few entries in all, group in one plain pass, payload and all.
    in.ndv_stat.?.entries = 110_000;
    try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy);
    in.ndv_stat.?.entries = 2_900_000;
    plan = planAggregate(in);
    try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
    try testing.expect(plan.reasons.contains(.repeated_dictionary_entries) and plan.reasons.contains(.small_ndv));
    in.ndv_stat.?.entries = (1 << 23) + 1;
    try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy);
    in.ndv_stat.?.entries = null;
    // Both fit a small global bound, but tiny/skewed/clustered dictionaries make the plain scan cheap.
    for ([_]struct { hi: u64, bytes: u64 }{
        .{ .hi = 1000, .bytes = 233_000 },
        .{ .hi = 100_000, .bytes = 857_000 },
        .{ .hi = 129_231, .bytes = 3_292_000 },
    }) |c| {
        in.ndv_stat.?.hi = c.hi;
        in.ndv_stat.?.dictionary_bytes = c.bytes;
        plan = planAggregate(in);
        try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
        try testing.expect(!plan.dispatch.census_allowed);
    }
    in.calls = calls[0..1];
    in.ndv_stat.?.hi = 100_000;
    in.ndv_stat.?.dictionary_bytes = 23 << 20;
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy);
    in.keys = &keys;
    in.ndv_stat.?.hi = 1_000_000;
    in.ndv_stat.?.key_bytes = 18;
    plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy);
    // A fixed-width composite rides its census along the count pass; it is no guess at uniqueness.
    try testing.expect(plan.dispatch.census_allowed and plan.dispatch.census_fused and !plan.dispatch.keys_unique);
    in.args.sketch_topk.?.census = false;
    try testing.expect(!planAggregate(in).dispatch.census_allowed);
    in.args.sketch_topk.?.census = true;
    in.args.max_memory = 64 << 20;
    try testing.expect(!planAggregate(in).dispatch.census_allowed);
    in.args.max_memory = 12 << 30;
    const string_key = [_]expr_ast.Expr{.{
        .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str },
    }};
    in.keys = &string_key;
    in.ndv_stat = .{
        .rows = 30_000_000,
        .hi = 30_000_000,
        .string_keys = true,
        .key_bytes = 14,
        .encoding_stats_rows = 30_000_000,
    };
    plan = planAggregate(in);
    try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
    try testing.expect(!plan.dispatch.census_allowed);

    // Short string composites can have a sparse repeat tail even when the marginal NDV is unknown. Unfiltered
    // (ClickBench q18's shape), the census rides the count pass, whose cells bail on so spread a key, with no hint.
    const short_composite = [_]expr_ast.Expr{ string_key[0], keys[1] };
    in.keys = &short_composite;
    in.ndv_stat.?.key_bytes = 24;
    plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy);
    try testing.expect(plan.dispatch.census_allowed and plan.dispatch.census_fused);
    try testing.expect(!plan.dispatch.keys_unique and !plan.dispatch.census_cell_repeat_gate);
    try testing.expect(plan.reasons.contains(.census_fused) and plan.reasons.contains(.census_short_string_composite));
    // Filtered, the count pass decides whether a census is worth a second pass, behind the repeat hint.
    in.args.filter = "s > 0";
    plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy);
    try testing.expect(plan.dispatch.census_allowed and plan.dispatch.census_cell_repeat_gate);
    try testing.expect(!plan.dispatch.keys_unique and !plan.dispatch.census_fused);
    in.args.filter = null;
    in.args.sketch_topk.?.census = false;
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy);
    in.args.sketch_topk.?.census = true;
    in.ndv_stat.?.key_bytes = 64;
    try testing.expectEqual(.partitioned_hash, planAggregate(in).dispatch.strategy);
    in.ndv_stat.?.key_bytes = 24;
    in.ndv_stat.?.confidence = .upper_bound;
    in.ndv_stat.?.hi = 100_000;
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy); // Preserve proven-small R1.
    in.ndv_stat.?.confidence = .unknown;
    in.ndv_stat.?.hi = 30_000_000;
    in.calls = &calls;
    try testing.expectEqual(.plain_hash, planAggregate(in).dispatch.strategy); // Payloads keep the string guard.
}

test "planAggregate: a filter its row groups' bounds say passes few rows drops a count cut for one plain pass" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(gpa);
    defer meta_arena.deinit();
    const ma = meta_arena.allocator();
    const meta = try metadata.open(ma, bytes);
    const work = try ma.alloc(WorkItem, meta.row_groups.items.len);
    for (work, 0..) |*w, rg| w.* = .{
        .file = 0,
        .rg = rg,
        .agg_start = 0,
        .agg_len = 1,
        .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} },
        .accumulators = &.{},
    };
    var lo: i64 = std.math.maxInt(i64);
    for (meta.row_groups.items) |*rg| {
        const b = @import("../parquet/statistics.zig").chunkStats(rg, 1, &meta, .corroborated).bounds orelse
            return error.SkipZigTest;
        lo = @min(lo, std.mem.readInt(i64, b.min[0..8], .little));
    }
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var in: PlanInputs = .{
        .args = .{
            .inputs = &.{},
            .aggregate = "count(*)",
            .sketch_topk = .{},
            .max_memory = 16 << 30,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        },
        .metas = &.{meta},
        .work = work,
        .calls = &calls,
        .keys = &keys,
        .outputs = &outputs,
        .filter_cols = &.{ false, true },
        .workers = 4,
        // Partial coverage keeps a string key's sketch (ClickBench), whatever the filter.
        .ndv_stat = .{ .rows = 20_000, .covered_rows = 10_000, .encoding_stats_rows = 20_000, .hi = 20_000 },
        .filter = .{ .int64 = .{ .col_idx = 1, .op = .GtEq, .value = lo } },
    };
    try testing.expectEqual(.sketch, planAggregate(in).dispatch.strategy);
    in.filter = .{ .int64 = .{ .col_idx = 1, .op = .Lt, .value = lo } };
    const plan = planAggregate(in);
    try testing.expectEqual(.plain_hash, plan.dispatch.strategy);
    try testing.expect(plan.reasons.contains(.selective_filter) and !plan.reasons.contains(.sketch_count_cut));
}

test "dense profitability costs selected rows without discounting allocated lanes" {
    const stat: NdvStat = .{ .rows = 30_000_000, .hi = 1_000_000, .key_bytes = 18 };
    const dense_bytes = 395_185_450;
    try testing.expect(selectedGroupBytes(stat, 1, 16, 0.01) < dense_bytes);
    try testing.expect(selectedGroupBytes(stat, 1, 16, 0.30) > dense_bytes);
    try testing.expectEqual(@as(u64, 0), selectedGroupBytes(stat, 1, 16, 0));
    try testing.expectEqual(plainGroupRowBytes(stat, 1) * stat.hi * 16, selectedGroupBytes(stat, 1, 16, 1));
    const wide: NdvStat = .{ .rows = std.math.maxInt(u64), .hi = std.math.maxInt(u64), .key_bytes = 18 };
    try testing.expectEqual(std.math.maxInt(u64), selectedGroupBytes(wide, 1, 16, 1));
}

test "callsTotal: bare columns and literals cannot fail, computed arguments and DECIMAL sums can" {
    const col: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const ok = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .arg = null, .where = null },
        .{ .func = .count_distinct, .alias = "d", .result = .i64, .arg = col, .where = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = col, .where = null },
        .{ .func = .max, .alias = "m", .result = .dec, .arg = col, .where = null },
    };
    try testing.expect(callsTotal(&ok));
    for ([_]expr_agg.AggCall{
        .{ .func = .sum, .alias = "s", .result = .dec, .arg = col, .where = null },
        .{ .func = .avg, .alias = "a", .result = .avg_dec, .arg = col, .where = null },
    }) |bad| try testing.expect(!callsTotal(&.{ ok[0], bad }));
    var left = col;
    var right: expr_ast.Expr = .{ .literal = .{ .i64 = 0 } };
    const div: expr_ast.Expr = .{ .binop = .{ .op = .div, .left = &left, .right = &right, .result_type = .i64, .depth = 2 } };
    try testing.expect(!callsTotal(&.{.{ .func = .count_distinct, .alias = "d", .result = .i64, .arg = div, .where = null }}));
}

test "sketchEligible: a filter may read the group keys, not other columns" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" } };
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } }};
    const args: MultiAggArgs = .{
        .inputs = &.{},
        .aggregate = "count(*) AS c",
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
    };
    const cfg: SketchTopK = .{};
    // No filter, or one over the key column: eligible. One that also reads column 0: not.
    try testing.expectEqual(@as(?usize, 0), sketchEligible(cfg, args, &outputs, &calls, &keys, &.{ false, false }, false, false));
    try testing.expectEqual(@as(?usize, 0), sketchEligible(cfg, args, &outputs, &calls, &keys, &.{ false, true }, false, false));
    try testing.expectEqual(@as(?usize, null), sketchEligible(cfg, args, &outputs, &calls, &keys, &.{ true, true }, false, false));
    // Keys that look unique take the cut even through a filter on another column.
    try testing.expectEqual(@as(?usize, 0), sketchEligible(cfg, args, &outputs, &calls, &keys, &.{ true, true }, false, true));
}

test "bounded first discovery declines HAVING before choosing its candidate prefix" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const keys = [_]expr_ast.Expr{.{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } }};
    var args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "count(*) AS c", .result_order = .{ .limit = 3 } };
    try testing.expect(firstCutEligible(args, &calls, &keys, false, 1, 1));
    const having: expr_ast.Having = .{ .null_check = .{ .column = 0, .is_not = true } };
    args.result_order.having = &having;
    try testing.expect(!firstCutEligible(args, &calls, &keys, false, 1, 1));
}

