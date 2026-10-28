const std = @import("std");
const nowMonoNs = @import("../../clock.zig").monoNs;
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const types = @import("types.zig");
const MultiAggArgs = types.MultiAggArgs;
const WorkItem = types.WorkItem;
const BoundOutputColumn = types.BoundOutputColumn;
const Worker = @import("worker_contracts.zig").Worker;
const worker = @import("worker.zig");
const releaseRegions = worker.releaseRegions;
const releaseRegionsParallel = worker.releaseRegionsParallel;
const runWorkers = worker.runWorkers;
const firstWorkerFailure = worker.firstWorkerFailure;
const result = @import("result.zig");
const Timings = result.Timings;
const ScanState = result.ScanState;
const MultiAggResult = result.MultiAggResult;
const AggOutput = @import("output.zig").AggOutput;

pub fn pairResult(
    args: MultiAggArgs,
    plan: *const agg_plan.Plan,
    run: RecordRun,
    output: AggOutput,
    state: ScanState,
) MultiAggResult {
    var bytes_in: u64 = 0;
    for (args.inputs) |input| bytes_in +|= if (input.logical_size != 0) input.logical_size else input.bytes.len;
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
        .distinct_pairs = true,
        .part_agg = true,
        .state = state,
    };
}

pub const RecordRun = struct {
    timings: Timings,
    rows_in: i64 = 0,
    rows_scanned: i64 = 0,
    rows_matched: i64 = 0,
    row_groups_in: usize = 0,
    row_groups_pruned: usize = 0,
    row_groups_full_match: usize = 0,
};

pub fn runRecordWorkers(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    plan: *const agg_plan.Plan,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    sinks: []const consumer.RecordSink,
    starts: []const u32,
    outputs: []const BoundOutputColumn,
    timings: Timings,
) !RecordRun {
    const n = sinks.len;
    const workers = try arena.alloc(Worker, n);
    @memset(workers, undefined);
    const tables = try arena.alloc(expr_agg.GroupTable, n);
    for (tables) |*table| table.* = .init(gpa, 0);
    defer for (tables) |*table| table.deinit();
    var decode_budget: ?consumer.DecodeBudget = if (args.decode_limit) |l| .{ .limit = l, .workers = n } else null;
    var cursor = std.atomic.Value(usize).init(0);
    // An indirect handle keeps the ordinary worker/target layout unchanged; only record paths dispatch the tag.
    for (workers, 0..) |*w, i| w.* = .{
        .gpa = gpa,
        .inputs = args.inputs,
        .metas = metas,
        .work = work,
        .cursor = &cursor,
        .agg_calls = calls,
        .plan = plan,
        .decode_options = .{
            .fast_levels = args.fast_levels,
            .level_runs = args.level_runs,
            .encoded = args.encoded,
            .expand_limit = args.max_memory,
        },
        .expr_batch_rows = args.expr_batch_rows,
        .decode_batch_rows = args.decode_batch_rows,
        .auto_decode_batches = args.auto_decode_batches,
        .scan_workers = workers.len,
        .error_outputs = outputs,
        .failure_probe = args.failure_probe,
        .group_by_keys = keys,
        .group_tables = tables,
        .index = i,
        .records = &sinks[i],
        .record_starts = starts,
        .ledger = args.ledger,
        .decode_budget = if (decode_budget) |*b| b else null,
        .profiler = args.profiler,
    };
    defer releaseRegions(workers);
    if (args.start_reads) |s| {
        try s.start(s.ctx);
        if (s.planned) |planned| planned(s.ctx);
    }
    const t0 = nowMonoNs();
    runWorkers(workers);
    releaseRegionsParallel(workers);
    if (firstWorkerFailure(workers, args.failure_probe)) |failure| {
        const e = failure.err;
        // Only an effectively parallel attempt can benefit from the deterministic serial retry.
        if (e == error.ExceededMemoryBudget and n > 1) if (args.taken) |taken| taken.insert(.decode_memory);
        consumer.setDecodeFailure(failure.site);
        if (e == error.RetainedEncodingUnsupported) return error.ExceededMemoryBudget;
        return e;
    }
    var run: RecordRun = .{ .timings = timings };
    run.timings.decode_wall_ns = @intCast(nowMonoNs() - t0);
    for (workers) |w| {
        run.rows_in += w.rows_in;
        run.rows_scanned += w.rows_scanned;
        run.rows_matched += w.rows_matched;
        run.row_groups_in += w.rgs_in;
        run.row_groups_pruned += w.rgs_pruned;
        run.row_groups_full_match += w.rgs_full_match;
        run.timings.core.decode_ns += w.timings.decode_ns;
        run.timings.core.eval_ns += w.timings.eval_ns;
        run.timings.core.encode_ns += w.timings.encode_ns;
    }
    return run;
}

