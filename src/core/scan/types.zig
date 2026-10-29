const std = @import("std");
const FailureProbe = @import("worker_contracts.zig").FailureProbe;
const core_memory = @import("../memory.zig");
const grouped_bitmap = @import("../grouped_distinct_bitmap.zig");
const profile = @import("../../profile.zig");
const Diag = @import("../diag.zig").Diag;
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const filter_ast = @import("../filter/ast.zig");
const testing = std.testing;

pub const Error = error{
    NoInputs,
    /// A serialized group key did not match the framing the writer used.
    BadGroupKey,
    SchemaMismatch,
    EmptyAggregate,
    AggSumOverflow,
    GroupKeyAliasRequired,
    UnknownColumn,
    ExceededMemoryBudget,
    /// Two output columns (aggregate aliases, GROUP BY keys, or one of each) share a name.
    DuplicateOutputColumn,
    /// An output column name refers to both a GROUP BY key and an aggregate.
    AmbiguousOutputColumn,
    /// A SQL result column outside an aggregate is no GROUP BY key.
    ColumnMustBeGrouped,
    /// A HAVING literal its result column cannot compare with (a string against a number, or the reverse).
    HavingTypeMismatch,
} || std.mem.Allocator.Error;

pub const Input = @import("contracts.zig").Input;

pub fn allRescannable(inputs: []const Input) bool {
    for (inputs) |in| if (!in.rescannable()) return false;
    return true;
}

/// A fast path that charges its peak to the query's budget where the established path it stands in for does not
/// charge the same way. Each is armed only over rescannable inputs, and a scan it took that the budget refused runs
/// again without it (`runMultiAggregate`): a budget the established path fits never fails the query.
pub const FastPath = enum {
    grouped_distinct_bitmap,
    shared_strings,
    integer_records,
    msd_records,
    dense,
    spill,
    pairs,
    partitioned,
    private_runs,
    /// A plain grouping the cost rules chose over a bounded cut (`fewEntries`, `selective_share`) from facts that
    /// cannot bound its table: its refusal runs the cut instead.
    plain_rule,
    /// Not fast paths but what the scan held: tables filled by several workers off a shared cursor, whose peak depends
    /// on which worker claimed which row groups (a table past a power of two doubles), so whether the budget refuses
    /// them depends on timing; and whether shared partitions, one copy of each group, could answer instead.
    worker_tables,
    partitions_fit,
    /// What the refusal says of one worker: several workers' tables held copies of the same keys, the merge refused,
    /// or on one worker the query plans a cut (`oneWorkerCuts`), so one worker may fit the budget they did not.
    /// Without it a single table must hold what they held and the rows they had not reached, and the scan on one
    /// worker is not run (it would refuse, many times slower).
    one_table_may_fit,
    /// The workers' decode memory came out of the query's ledger beside the rest: one worker, decoding one row group
    /// at a time, holds the least of it.
    decode_memory,
    /// The merge of several workers' tables was refused its maps and lists: one worker's table needs none.
    merge_memory,
};

pub const RowGroupGate = @import("contracts.zig").RowGroupGate;

/// Asks for the bytes planning reads from input `file` (`ndv.probeRanges`) to be read first: the reader reads them ahead
/// of every row group (`Input.dict_wait`); a mapped file is advised to read them in at once rather than fault them one
/// by one. Called for every probe range before `ReadStart`.
pub const DictProbe = struct {
    ctx: *anyopaque,
    add: *const fn (ctx: *anyopaque, file: usize, rg: usize, start: u64, end: u64) anyerror!void,
};

/// Starts the reads the scan's gates wait on (`Input.gate`). Called once every `DictProbe` range is asked for and before
/// planning reads any of them: planning never reads a reader's input through its mapping, so its reads queue behind
/// nothing but the probes. Idempotent; a scan given none reads as its inputs are.
pub const ReadStart = struct {
    ctx: *anyopaque,
    start: *const fn (ctx: *anyopaque) anyerror!void,
    /// Stops the reads and gives their buffers back before a scan runs again over the mappings (`runMultiAggregate`).
    abandon: ?*const fn (ctx: *anyopaque) void = null,
    /// Planning has read every probe it will: what was probed only for planning can go back.
    planned: ?*const fn (ctx: *anyopaque) void = null,
};

pub const MultiAggArgs = struct {
    /// Internal error handoff; prepareAggregate owns it until all scan state has been released.
    failure_probe: ?*FailureProbe = null,
    inputs: []const Input,
    /// See `ReadStart`.
    start_reads: ?ReadStart = null,
    /// See `DictProbe`.
    dict_probe: ?DictProbe = null,
    /// Tests: the reader's probe cap in place of `ndv.probeCap`.
    probe_cap: ?u64 = null,
    /// Optional metadata parsed by the I/O layer. When null, metadata is
    /// parsed from each Input.bytes as before.
    metas: ?[]const schema.FileMetaData = null,
    filter: ?[]const u8 = null,
    aggregate: []const u8,
    group_by: ?[]const u8 = null,
    /// Internal child execution reuses already bound expressions instead of round-tripping SQL text.
    bound_aggregate: ?*const BoundAggregate = null,
    max_memory: usize = 512 * 1024 * 1024,
    /// The query's ledger (`memory.zig`): its limit is `max_memory`, and the scan's tables, decode memory, merge and
    /// answer draw from it, beside whatever else the query holds. Null: a budget of `max_memory` the scan's tables
    /// draw from alone, as tests that size a budget to their groups expect.
    ledger: ?*expr_agg.SharedBudget = null,
    /// The allocator the merge's own maps and lists come from, drawing from `ledger` (`runMultiAggregate` makes one
    /// per scan, and turns its refusal into the query's). Null: `gpa`, uncharged.
    merge_memory: ?*core_memory.Charged = null,
    /// Whether the workers' decode memory is drawn from `ledger`. A refusal chain's last scan draws it instead from a
    /// ledger of its own of `memory.lean_decode` bytes (`narrowed`), which the ceiling's slack holds, so the tables
    /// have all of the query's.
    charge_decode: bool = true,
    /// What the workers' decoded row groups may take together (`consumer.DecodeBudget`); null leaves decode unbounded.
    decode_limit: ?usize = null,
    /// The GROUP BY output columns as a select list writes them (SQL).
    select_cols: ?[]const expr_ast.SelectColumn = null,
    /// The cut the caller applies to the GROUP BY rows (`AggOutput.applyResultOrder`): a COUNT-descending cut lets the
    /// merge skip groups that cannot make it. The rows still arrive uncut, in their usual order.
    result_order: expr_ast.ResultOrder = .{},
    /// Comma-separated output column names. Only used with GROUP BY when
    /// `select_cols` is unset (e.g. CLI `--column-order`). Reorders the
    /// default key-then-aggregate layout.
    column_order: ?[]const u8 = null,
    /// 0 = use cpu_count. 1 = serial. Row-group granularity: work is the
    /// flat list of all row groups across all inputs, distributed across
    /// `min(this, total_row_groups)` workers — so one big file uses all cores.
    parallelism: usize = 0,
    /// Leave each worker's decode region mapped after the scan, for the process exit to release (see the engine's
    /// `Context.exit_after`): unmapping them otherwise delays the answer.
    leave_regions: bool = false,
    /// `--scan-all`: disable every stats shortcut — no row-group pruning,
    /// no stats-driven column drop, no aggregate stat short-circuit. Forces
    /// a full page/byte decode. The paranoid / untrusted-writer-stats path.
    scan_all: bool = false,
    /// `--trust-stats`: answer min/max/sum from file statistics (the fast
    /// path) instead of decoding. Off by default — file stats can be wrong.
    /// `count(*)` is always answered from num_rows regardless.
    trust_stats: bool = false,
    fast_levels: bool = false,
    level_runs: ?bool = null,
    /// Query-local computed DOUBLE batch override; 0 keeps the legacy evaluator.
    expr_batch_rows: ?usize = null,
    /// Drop fault-free integer expressions fixed by retained bare GROUP BY keys; false forces legacy grouping.
    dependent_keys: bool = true,
    /// Forced flat decode windows; zero keeps ordinary memory admission.
    decode_batch_rows: usize = 0,
    auto_decode_batches: bool = false,
    compaction: ?*consumer.CompactionControl = null,
    /// See `consumer.DecodeOptions.encoded`.
    encoded: bool = true,
    /// Query-wide raw string identity; disabled after a rescannable budget refusal.
    shared_strings: bool = true,
    /// Build a GROUP BY answer the result program would leave as it is (no ORDER BY, LIMIT, OFFSET, HAVING or hidden
    /// column) as typed columns for a Parquet write (`AggOutput.typed`), never as rows.
    typed_answer: bool = false,
    /// A synchronous text consumer can drain full owners without narrowing their exact SUMs to Parquet lanes.
    record_answer: bool = false,
    /// Exact inline records for integer COUNT/SUM answers, SUM/DISTINCT cuts, and scalar distinct, when prepaid.
    integer_records: bool = true,
    /// Stream ordered COUNT top-H runs; false retains the materialized run owner.
    sorted_count_stream: bool = true,
    /// Tests lower the cost threshold; ordinary execution keeps small answers on the established path.
    integer_records_min_rows: usize = 1 << 20,
    /// Exact retained-page radix grouping for a bounded string/INT32 COUNT cut.
    msd_records: bool = true,
    msd_records_min_rows: usize = 1 << 20,
    /// Scratch-window override: null uses the per-worker cache budget, zero retains the legacy layout.
    msd_cache_rows: ?usize = null,
    /// Fixed decoder credit determined from the prefetched first-page headers.
    msd_working_bytes: ?usize = null,
    byte_records: bool = false,
    byte_records_min_rows: usize = 1 << 20,
    /// Zero retains the established partition count or ordinary decode admission, respectively.
    byte_top_partitions: ?usize = null,
    byte_top_batch_rows: ?usize = null,
    /// `--unordered`: GROUP BY output in no particular order, with floating-point aggregates summed in whatever order
    /// workers finish. Ordered (the default) lists groups where they first appear in the input and gives the same bits
    /// at every `-j`; see `group_blocks_max`.
    unordered: bool = false,
    /// Workers are sized so each gets at least this many bytes to decode; tests lower it to run several workers on
    /// small fixtures.
    min_bytes_per_worker: u64 = 2 * 1024 * 1024,
    /// Receives what a failure was about; see `diag.zig`.
    diag: ?*Diag = null,
    /// Records the scan's spans when set; see `profile.zig`.
    profiler: ?*profile.Profiler = null,
    /// Answer a COUNT-descending cut from a count pass and an admit pass where a count sketch certifies it; see
    /// `certifiedTopK`. Null runs the plain scan.
    sketch_topk: ?SketchTopK = null,
    /// Plain GROUP BY through shared partition tables (`partagg.zig`). The CLI sets this from `ZPQ_PARTAGG=1`.
    /// A count cut, sketch, census, or first-keys cut ignores it and keeps the per-worker tables.
    partagg: bool = false,
    /// Admitted private runs for poorly reducing long composite count keys; a refusal retries shared partitions.
    private_runs: bool = true,
    /// Grouped count(DISTINCT) through sorted pairs (`distinctPairsCall`): where they pay (`pairsPay`), always, or
    /// never. The CLI takes `.on` from `ZPQ_DISTINCT_PAIRS=1` and `.off` from `=0`.
    distinct_pairs: enum { auto, on, off } = .auto,
    /// Exact bounded-domain grouped DISTINCT; forced layouts keep all identity, domain and ledger checks.
    grouped_distinct_bitmap: grouped_bitmap.Mode = .auto,
    /// Separate exact ordinary aggregates from DISTINCT under an ordinary COUNT-ranked cut.
    count_ranked_distinct: enum { auto, on, off } = .auto,
    /// Rows a worker reads on sets before judging them (`pairsPayRuntime`) under `.auto`. A verdict that may still
    /// turn is taken again at twice the rows, `pairs_rechecks` times at most.
    pairs_pilot_rows: u64 = 1 << 18,
    pairs_rechecks: u8 = 3,
    /// Disable profitability rules when exercising the legacy cut executors.
    ndv_policy: bool = true,
    /// Budgeted exact dictionary union; coverage and input cost are gated from footers.
    ndv_union: bool = true,
    /// Keep a lone integer key's union as an exact domain and fold by its ids (`agg_dense`) where the plan allows.
    dense: bool = true,
    /// The fast paths the scan armed, so a budget refusal can run again without them (`FastPath`).
    taken: ?*std.EnumSet(FastPath) = null,
    /// Let the cost rules move a plain GROUP BY to partitioned tables (worker-copy pressure, long strings).
    partition_cost: bool = true,
    /// Let the cost rules group plainly in place of a first-keys or count cut (`FastPath.plain_rule`).
    plain_rules: bool = true,
    /// Tests: local-table cap and the row window that decides whether pre-aggregation is paying off. Zero keeps
    /// `partagg.local_cap` and `partagg.probe_rows`.
    part_cap: usize = 0,
    part_probe: u64 = 0,
    /// A plain GROUP BY worker whose table holds `spill_groups` groups or more at fewer than `spill_rows` rows a
    /// group moves to the partition runs (`partagg.zig`) for the rest of its scan. The CLI clears this from
    /// `ZPQ_SPILL=0`.
    spill: bool = true,
    spill_groups: usize = 1 << 18,
    spill_rows: u64 = 4,
};

/// Tuning of `certifiedTopK`.
pub const SketchTopK = struct {
    /// Hot-state producer; tests can force the identity-bound producer instead.
    hot: bool = true,
    hot_outcome: ?*HotOutcome = null,
    /// Cells admitted per row of the cut (`offset + limit`).
    admit_per_row: usize = 16,
    /// log2 of the cells per worker: 2^18 four-byte cells fit a core's L2.
    cell_bits: u6 = 18,
    /// Report each decision on stderr.
    trace: bool = false,
    /// Tests: false never trusts the bound, so the scan falls back after the admit pass.
    certify: bool = true,
    /// Tests: where the scan went.
    outcome: ?*Outcome = null,
    /// When the cells bail, try a census of key fingerprints (`identityBoundPlan`) before the plain scan.
    census: bool = true,
    /// Tests: whether the census rides along the cell count pass, in place of `keysLookUnique`.
    fuse_census: ?bool = null,
    /// Tests: rows a worker records before its cells judge whether a riding census is needed.
    census_check_rows: u64 = expr_agg.Census.check_rows,
    /// Tests: identity bits below the owner partition (at most 32), fewer to force collisions.
    census_fp_bits: u6 = 40,

    pub const Outcome = enum { ineligible, bailed, certified, fell_back, census, census_short };
    pub const HotOutcome = enum { declined, certified, residual };
};

pub const BorrowedSpan = struct {
    lo: usize,
    hi: usize,

    pub fn contains(spans: []const BorrowedSpan, value: []const u8) bool {
        const at = @intFromPtr(value.ptr);
        var lo: usize = 0;
        var hi = spans.len;
        while (lo < hi) {
            const mid = lo + (hi - lo) / 2;
            if (spans[mid].lo <= at) lo = mid + 1 else hi = mid;
        }
        return lo > 0 and at < spans[lo - 1].hi and value.len <= spans[lo - 1].hi - at;
    }
};

/// What one GROUP BY result column holds. `nullable` is false only for counts, matching the 1-row output.
pub const GroupColType = struct {
    lane: enum { i64, f64, string, avg, dec, avg_dec },
    /// The DECIMAL type of a `dec` column, or of an `avg_dec` mean.
    dec: ?expr_ast.Dec = null,
    nullable: bool,
    /// The column whose values pass through unchanged (a bare key, min/max of a column), to type the output by.
    source: ?schema.SchemaElement = null,
};

pub const WorkItem = @import("contracts.zig").WorkItem;

pub const RecordOrdinals = struct {
    pub fn rows(metas: []const schema.FileMetaData) ?usize {
        var n: usize = 0;
        for (metas) |meta| for (meta.row_groups.items) |rg| {
            n = std.math.add(usize, n, std.math.cast(usize, rg.num_rows) orelse return null) catch return null;
        };
        return n;
    }

    /// Work is in input/RG order, but may omit pruned sources. Prefixes include those omissions and empty groups;
    /// neither a survivor count nor gathered position can become an original ordinal.
    pub fn fill(metas: []const schema.FileMetaData, work: []const WorkItem, starts: []u32) bool {
        if (starts.len != work.len or (rows(metas) orelse return false) >= @as(usize, 1) << 31) return false;
        var base: usize = 0;
        var at: usize = 0;
        for (metas, 0..) |meta, fi| for (meta.row_groups.items, 0..) |rg, ri| {
            while (at < work.len and work[at].file == fi and work[at].rg == ri) : (at += 1)
                starts[at] = @intCast(base);
            base += @intCast(rg.num_rows);
        };
        return at == work.len;
    }
};

pub const ColSource = @import("contracts.zig").ColSource;
pub const BoundOutputColumn = @import("contracts.zig").BoundOutputColumn;

/// Zig's 16 MiB default makes worker creation expensive for short scans. Safe to shrink: these workers decode, and
/// every nesting path here is bounded.
pub const WORKER_STACK_SIZE: usize = 1 << 20;

/// Rows a build task claims at a time.
pub const row_block = 4096;

test "owner original ordinals include pruned and empty row groups across files before compact narrowing" {
    const Make = struct {
        fn rg(n: i64) schema.RowGroup {
            return .{ .num_rows = n, .columns = .empty, .total_byte_size = 0 };
        }
        fn meta(rgs: []schema.RowGroup) schema.FileMetaData {
            var list: std.ArrayListUnmanaged(schema.RowGroup) = .initBuffer(rgs);
            list.items = rgs;
            return .{
                .version = 1,
                .schema = .empty,
                .num_rows = 0,
                .created_by = null,
                .row_groups = list,
            };
        }
        fn work(file: usize, rg_index: usize) WorkItem {
            return .{
                .file = file,
                .rg = rg_index,
                .agg_start = 0,
                .agg_len = 1,
                .accumulators = &.{},
                .columns = .{ .read = &.{}, .consumed = &.{}, .preserve = &.{} },
            };
        }
    };
    var rgs0 = [_]schema.RowGroup{ Make.rg(10), Make.rg(0), Make.rg(20) };
    var rgs1 = [_]schema.RowGroup{ Make.rg(5), Make.rg(7) };
    var metas = [_]schema.FileMetaData{ Make.meta(&rgs0), Make.meta(&rgs1) };
    const work = [_]WorkItem{ Make.work(0, 2), Make.work(1, 1) };
    var starts: [2]u32 = undefined;
    try testing.expectEqual(@as(?usize, 42), RecordOrdinals.rows(&metas));
    try testing.expect(RecordOrdinals.fill(&metas, &work, &starts));
    try testing.expectEqualDeep([_]u32{ 10, 35 }, starts);
    try testing.expect(!RecordOrdinals.fill(&metas, &.{ work[1], work[0] }, &starts));
    try testing.expect(!RecordOrdinals.fill(&metas, &work, starts[0..1]));
    rgs0[0].num_rows = 1 << 31;
    try testing.expect(!RecordOrdinals.fill(&metas, &work, &starts));
    rgs0[0].num_rows = -1;
    try testing.expectEqual(null, RecordOrdinals.rows(&metas));
    rgs0[0].num_rows = std.math.maxInt(i64);
    rgs0[2].num_rows = std.math.maxInt(i64);
    try testing.expectEqual(null, RecordOrdinals.rows(&metas));
}


pub const BoundAggregate = struct {
    calls: []const expr_agg.AggCall,
    keys: []const expr_ast.Expr,
    outputs: []const BoundOutputColumn,
    filter: ?filter_ast.Filter,
};
