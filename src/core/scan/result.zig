const std = @import("std");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const membership = @import("../filter/membership.zig");
const expr_agg = @import("../expr/agg.zig");
const GroupColType = @import("types.zig").GroupColType;
const AggOutput = @import("output.zig").AggOutput;
const Reason = @import("plan_types.zig").Reason;
const freeOwnedAccumulatorStrings = @import("owned_merge.zig").freeOwnedAccumulatorStrings;

/// What writing the answer as Parquet needs beyond `AggOutput`. Borrowed from the `arena` passed to
/// `runMultiAggregate`, except the string min/max winners in `accumulators`, which are in its `gpa`: `deinit` frees
/// those.
pub const ScanState = struct {
    /// Final accumulator per aggregate call; empty states under GROUP BY.
    accumulators: []expr_agg.Accumulator,
    /// Aggregate calls (parsed AST).
    agg_calls: []const expr_agg.AggCall,
    /// Value lane of each `AggOutput.group_cols` entry, so a writer can type a column that is NULL in every row.
    group_col_types: ?[]const GroupColType = null,
    /// Per aggregate call: the source column of a min/max, which passes values through (see `passThroughSource`).
    agg_sources: []const ?schema.SchemaElement = &.{},

    pub fn deinit(self: *ScanState, gpa: std.mem.Allocator) void {
        freeOwnedAccumulatorStrings(gpa, self.accumulators);
        self.accumulators = &.{};
    }
};

/// Per-phase wall-clock counts. `core` is the time spent inside
/// `consumer.scanRGForAgg` (decode + eval + per-RG aggregator update);
/// the orchestrator-level fields cover everything around it.
pub const Timings = struct {
    parse_ns: u64 = 0,
    core: consumer.Timings = .{},
    /// Wall-clock of the parallel scan region (spawn → join), i.e. the
    /// real elapsed decode+eval time. Distinct from `core.decode_ns`,
    /// which is the *sum* of per-worker CPU time (so wall < core.decode_ns
    /// whenever >1 worker ran). `core.decode_ns / decode_wall_ns` is the
    /// effective decode parallelism.
    decode_wall_ns: u64 = 0,
};

pub const MultiAggResult = struct {
    files_in: usize,
    /// Rows in every row group of the inputs.
    rows_in: i64,
    /// Rows in the row groups read: not ruled out by statistics or membership.
    rows_scanned: i64,
    /// Rows of those the outer filter kept: every one without a filter. The aggregates fold these.
    rows_matched: i64,
    bytes_in: u64,
    row_groups_in: usize,
    row_groups_pruned: usize,
    /// Row groups whose statistics proved every row matches the filter, so it was never evaluated there and
    /// columns only the filter reads were neither fetched nor decoded.
    row_groups_full_match: usize = 0,
    /// Columns dropped from the fetch set because every aggregate
    /// referencing them was provably stats-answerable across every RG.
    /// 0 when no aggs are stat-eligible, an outer filter is present,
    /// or any RG lacks the relevant stat field.
    cols_stat_pruned: usize,
    /// Bloom filter probing this call did (none when the caller passed `metas`).
    membership: membership.Stats = .{},
    timings: Timings,
    /// The answer. A caller keeping it takes it and leaves `state` to `ScanState.deinit`.
    output: AggOutput,
    /// The shared-partition GROUP BY executor ran. False when the query kept the per-worker tables.
    part_agg: bool = false,
    /// Grouped count(DISTINCT) was answered through pairs (`foldDistinctPairs`).
    distinct_pairs: bool = false,
    grouped_distinct_bitmap: bool = false,
    count_ranked_distinct: bool = false,
    dense_aggregate: bool = false,
    /// Why the scan ran as it did (`ScanPlan.reasons`), and what its dictionary bound came from.
    reasons: std.EnumSet(Reason) = .{},
    /// How many of each GROUP BY row's leading columns are shown; the rest are hidden ORDER BY inputs
    /// (`ResultOrder.hidden`) the result program drops once it has ranked the rows. Null shows every column.
    visible_cols: ?usize = null,
    state: ScanState,

    pub fn deinit(self: *MultiAggResult, gpa: std.mem.Allocator) void {
        self.output.deinit(gpa);
        self.state.deinit(gpa);
    }
};

