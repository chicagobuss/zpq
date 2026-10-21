//! Multi-file aggregate orchestrator. Source-agnostic: takes a list of
//! `Input { name, bytes }` already-fetched-or-mmap'd files and runs an
//! aggregate query across all of them. Both the CLI (mmap → bytes) and
//! the Lambda (S3 fetch → bytes) feed the same orchestrator.
//!
//! Sans-IO. Allocates only what the caller's allocators allow; never
//! does syscalls or network I/O of its own. The bytes are someone else's
//! problem.
//!
//! What lives here:
//!   - `Input`             one file's pre-fetched bytes + display name
//!   - `MultiAggArgs`      query spec (filter, aggregate, parallelism)
//!   - `MultiAggResult`    materialized aggregate values + per-stage timings
//!   - `runMultiAggregate` the orchestrator
//!
//! What does NOT live here:
//!   - mmap / file I/O      (cli/query.zig's mmapFile)
//!   - S3 fetch             (io/s3.zig + future io/s3_source.zig)
//!   - 1-row parquet output (cli/query.zig's writeOneRowParquet)

const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");

const spawn_util = @import("spawn.zig");
const huge_pages = @import("huge_pages.zig");
const schema = @import("schema.zig");
const consumer = @import("consumer.zig");
const agg_plan = @import("agg_plan.zig");
const metadata = @import("parquet/metadata.zig");
const expr_ast = @import("expr/ast.zig");
const expr_parser = @import("expr/parser.zig");
const expr_agg = @import("expr/agg.zig");
const core_memory = @import("memory.zig");
const partagg = @import("partagg.zig");
const ndv = @import("ndv.zig");
const agg_dense = @import("expr/agg_dense.zig");
pub const NdvStat = ndv.Stat;
const filter_ast = @import("filter/ast.zig");
const filter_parser = @import("filter/parser.zig");
const filter_prune = @import("filter/prune.zig");
const membership = @import("filter/membership.zig");
const nanprobe = @import("filter/nanprobe.zig");
const Diag = @import("diag.zig").Diag;
const core_bytes = @import("bytes.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;

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

/// One file's contribution to a multi-file scan. Bytes can come from
/// mmap (CLI), an S3 range-fetch into a buffer (Lambda / S3-source CLI),
/// or a literal slice (tests). The orchestrator does not care.
pub const Input = struct {
    /// Display name used in error messages. Path or URL.
    name: []const u8,
    bytes: []const u8,
    /// Logical source size. S3 inputs may compact fetched ranges into
    /// `bytes`; keep reporting the object size in query results.
    logical_size: u64 = 0,
    /// Set while `bytes` are still being read in (`io/local_reader.zig`): a worker waits on it before touching a row
    /// group, and reports each finished work item so a done row group's pages can go back.
    gate: ?RowGroupGate = null,
    /// The whole file as mapped, while `bytes` are read in behind `gate`: what a planning read made before any gate
    /// opens takes (the NDV union's dictionaries, which read buffers the reader had not filled, so the plan hung on
    /// I/O progress and read mode), and what a scan run again after a fast path's refusal reads, the reader's window
    /// having moved on. Null without a gate, and for S3 inputs.
    plan_bytes: ?[]const u8 = null,
    /// Beside a gate: the dictionary bytes `MultiAggArgs.dict_probe` asked for are read into `bytes` ahead of every row
    /// group, and planning reads them there once this says they are in, so a reader's input plans from the same
    /// dictionaries a mapped one does.
    dict_wait: ?ndv.Wait = null,

    /// A local file read in behind gates (`io/local_reader.zig`), its mapping beside it; not an S3 input's window.
    pub fn readerBacked(self: Input) bool {
        return self.gate != null and self.plan_bytes != null and !std.mem.startsWith(u8, self.name, "s3://");
    }

    /// Readable again from the start, as a scan run again after a refusal must read it (`runMultiAggregate`).
    pub fn rescannable(self: Input) bool {
        return self.gate == null or self.plan_bytes != null;
    }
};

fn allRescannable(inputs: []const Input) bool {
    for (inputs) |in| if (!in.rescannable()) return false;
    return true;
}

/// A fast path that charges its peak to the query's budget where the established path it stands in for does not
/// charge the same way. Each is armed only over rescannable inputs, and a scan it took that the budget refused runs
/// again without it (`runMultiAggregate`): a budget the established path fits never fails the query.
pub const FastPath = enum {
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

pub const RowGroupGate = struct {
    ctx: *anyopaque,
    wait: *const fn (ctx: *anyopaque, rg: usize) anyerror!void,
    /// One of the row group's `items` work items is finished, read or skipped.
    done: *const fn (ctx: *anyopaque, rg: usize, items: usize) void,
};

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
    /// See `consumer.DecodeOptions.encoded`.
    encoded: bool = true,
    /// Build a GROUP BY answer the result program would leave as it is (no ORDER BY, LIMIT, OFFSET, HAVING or hidden
    /// column) as typed columns for a Parquet write (`AggOutput.typed`), never as rows.
    typed_answer: bool = false,
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
    /// `sketchTopK`. Null runs the plain scan.
    sketch_topk: ?SketchTopK = null,
    /// Plain GROUP BY through shared partition tables (`partagg.zig`). The CLI sets this from `ZPQ_PARTAGG=1`.
    /// A count cut, sketch, census, or first-keys cut ignores it and keeps the per-worker tables.
    partagg: bool = false,
    /// Admitted private runs for poorly reducing long composite count keys; a refusal retries shared partitions.
    private_runs: bool = true,
    /// Grouped count(DISTINCT) through sorted pairs (`distinctPairsCall`): where they pay (`pairsPay`), always, or
    /// never. The CLI takes `.on` from `ZPQ_DISTINCT_PAIRS=1` and `.off` from `=0`.
    distinct_pairs: enum { auto, on, off } = .auto,
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

/// Tuning of `sketchTopK`.
pub const SketchTopK = struct {
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
    /// When the cells bail, try a census of key fingerprints (`censusTopK`) before the plain scan.
    census: bool = true,
    /// Tests: whether the census rides along the cell count pass, in place of `keysLookUnique`.
    fuse_census: ?bool = null,
    /// Tests: rows a worker records before its cells judge whether a riding census is needed.
    census_check_rows: u64 = expr_agg.Census.check_rows,
    /// Tests: fingerprint bits below the partition's (40), fewer to make keys collide.
    census_fp_bits: u6 = 40,

    pub const Outcome = enum { ineligible, bailed, certified, fell_back, census, census_short };
};

/// Whether `runMultiAggregate` probes Bloom filters itself: only for footers it parses from the whole file (every
/// input mapped), never for `metas` the caller passes, which the caller has probed through its own reader.
fn probesMembership(args: MultiAggArgs) bool {
    return args.metas == null and !args.scan_all;
}

pub const AggValue = union(enum) {
    /// Single integer result (count, integer sum/min/max). i128 so a sum that
    /// overflows i64 — or an unsigned-64 value/extremum up to 2^64-1 — is
    /// representable (DuckDB likewise widens int SUM to HUGEINT). Counts and
    /// signed i64 values widen in losslessly.
    i: i128,
    /// Single f64 result (float sum/min/max, avg's mean).
    f: f64,
    /// avg_state's split-output: ship sum + count, caller divides at end. The sum
    /// is NULL when count is 0, as sum() over no values is.
    avg: struct { sum: ?f64, count: i64 },
    /// String (bytewise/unsigned) min/max result or string group key. Owned by the `AggOutput` holding it.
    s: []const u8,
    /// Exact DECIMAL: unscaled value and its type. Printed as exact text, written as DECIMAL. Also avg's mean.
    dec: DecValue,
    /// avg_state over a DECIMAL: the exact sum and count, which re-aggregate, and the mean rounded to the argument's
    /// type, which only ranks and filters (HAVING) the rows, never printed.
    /// The sum may pass 38 digits while the mean fits (two values near 10^38); JSON prints it whole, and only `-o`,
    /// whose `__sum` column is DECIMAL(38, s), refuses it. `sum` and `mean` are NULL when count is 0.
    avg_dec: struct { sum: ?i192, count: i64, mean: ?i128, spec: expr_ast.Dec },
    null_val: void,
};

pub const DecValue = struct { v: i128, spec: expr_ast.Dec };

pub const AggOutputItem = struct {
    alias: []const u8,
    value: AggValue,
};

/// The aggregate answer, owned in the `gpa` passed to `runMultiAggregate`; `deinit` frees all of it, including a
/// partly built one. Without GROUP BY, `aggs` holds one value per aggregate; with it, `group_rows` holds one row per
/// group, its values in `group_cols` order.
pub const AggOutput = struct {
    aggs: []AggOutputItem = &.{},
    group_cols: ?[]const []const u8 = null,
    group_rows: ?[]const []const AggValue = null,
    /// Without GROUP BY: HAVING failed the one group, so the answer has no row (`aggs` still holds its values).
    no_row: bool = false,
    /// One allocation the GROUP BY rows are views into, instead of one allocation a row. A row outside it (built
    /// alone, as tests build them) is its own allocation; `releaseRow` frees a row either way. The strings values own
    /// are always their own allocations.
    slab: []AggValue = &.{},
    /// The GROUP BY answer as typed columns instead of rows (`MultiAggArgs.typed_answer`); `group_rows` is then null.
    typed: ?TypedAnswer = null,

    pub fn deinit(self: *AggOutput, gpa: std.mem.Allocator) void {
        for (self.aggs) |item| {
            gpa.free(item.alias);
            freeValue(gpa, item.value);
        }
        gpa.free(self.aggs);
        if (self.group_rows) |rows| {
            for (rows) |row| self.releaseRow(gpa, row);
            gpa.free(rows);
        }
        gpa.free(self.slab);
        if (self.typed) |*t| t.deinit(gpa);
        if (self.group_cols) |cols| {
            for (cols) |c| gpa.free(c);
            gpa.free(cols);
        }
        self.* = .{};
    }

    /// Free what row `row` owns: its values' strings, and the row itself unless it lies in `slab`.
    pub fn releaseRow(self: *const AggOutput, gpa: std.mem.Allocator, row: []const AggValue) void {
        for (row) |v| freeValue(gpa, v);
        self.releaseRowSlice(gpa, row);
    }

    /// Free row `row`'s own allocation, its values left alone (taken over elsewhere or already freed).
    pub fn releaseRowSlice(self: *const AggOutput, gpa: std.mem.Allocator, row: []const AggValue) void {
        if (!self.inSlab(row)) gpa.free(row);
    }

    fn inSlab(self: *const AggOutput, row: []const AggValue) bool {
        if (row.len == 0 or self.slab.len == 0) return row.len == 0;
        const lo = @intFromPtr(self.slab.ptr);
        const at = @intFromPtr(row.ptr);
        return at >= lo and at < lo + self.slab.len * @sizeOf(AggValue);
    }

    /// Move the rows into a slab sized to them when the one they lie in is mostly rows a cut dropped (or they lie in
    /// none), so a short answer does not hold every group's memory. All or nothing: on a failure `self` is unchanged.
    pub fn compactRows(self: *AggOutput, gpa: std.mem.Allocator) error{OutOfMemory}!void {
        const rows = self.group_rows orelse return;
        var cells: usize = 0;
        var loose = false;
        for (rows) |row| {
            cells += row.len;
            loose = loose or !self.inSlab(row);
        }
        if (!loose and self.slab.len <= 2 * cells + 4096) return;
        const fresh = try gpa.alloc(AggValue, cells);
        errdefer gpa.free(fresh);
        const views = try gpa.alloc([]const AggValue, rows.len);
        var at: usize = 0;
        for (rows, views) |row, *v| {
            @memcpy(fresh[at..][0..row.len], row);
            v.* = fresh[at..][0..row.len];
            at += row.len;
        }
        for (rows) |row| self.releaseRowSlice(gpa, row);
        gpa.free(rows);
        gpa.free(self.slab);
        self.slab = fresh;
        self.group_rows = views;
    }

    fn freeValue(gpa: std.mem.Allocator, v: AggValue) void {
        switch (v) {
            .s => |s| gpa.free(s),
            else => {},
        }
    }

    /// Sort, then cut, the GROUP BY rows per `order`. Stable, so ties keep first-appearance order and the answer is
    /// the same at every `-j`. The dropped rows are freed here.
    pub fn applyResultOrder(self: *AggOutput, gpa: std.mem.Allocator, order: expr_ast.ResultOrder) error{OutOfMemory}!void {
        const rows = self.group_rows orelse return;
        if (order.limit) |n| if (n > 0 and order.offset +| n < rows.len / 2) return self.keepFirst(gpa, order, @intCast(order.offset +| n));
        const sorted = try gpa.alloc([]const AggValue, rows.len);
        defer gpa.free(sorted);
        @memcpy(sorted, rows);
        std.sort.block([]const AggValue, sorted, order.keys, rowLessThan);
        const start: usize = @intCast(@min(order.offset, sorted.len));
        const end: usize = if (order.limit) |n| @intCast(@min(sorted.len, start +| n)) else sorted.len;
        // Allocated before anything is freed, so a failure leaves the answer whole.
        const kept = try gpa.dupe([]const AggValue, sorted[start..end]);
        for (sorted[0..start]) |row| self.releaseRow(gpa, row);
        for (sorted[end..]) |row| self.releaseRow(gpa, row);
        gpa.free(rows);
        self.group_rows = kept;
    }

    /// `applyResultOrder` for a cut of `m` rows well short of all of them: the `m` least rows by the order, ties by
    /// their place in the answer (as the stable sort breaks them), kept in a heap of `m` instead of sorting everything.
    fn keepFirst(self: *AggOutput, gpa: std.mem.Allocator, order: expr_ast.ResultOrder, m: usize) error{OutOfMemory}!void {
        const rows = self.group_rows.?;
        const Item = struct { row: []const AggValue, i: usize };
        const Ctx = struct {
            keys: []const expr_ast.OrderKey,
            fn before(keys: []const expr_ast.OrderKey, a: Item, b: Item) bool {
                if (rowLessThan(keys, a.row, b.row)) return true;
                if (rowLessThan(keys, b.row, a.row)) return false;
                return a.i < b.i;
            }
            // The heap's top is the last row kept, so a row that sorts before it replaces it.
            fn later(ctx: @This(), a: Item, b: Item) std.math.Order {
                if (before(ctx.keys, b, a)) return .lt;
                if (before(ctx.keys, a, b)) return .gt;
                return .eq;
            }
        };
        var heap: std.PriorityQueue(Item, Ctx, Ctx.later) = .initContext(.{ .keys = order.keys });
        defer heap.deinit(gpa);
        try heap.ensureTotalCapacity(gpa, m);
        for (rows, 0..) |row, i| {
            const item: Item = .{ .row = row, .i = i };
            if (heap.count() == m) {
                if (!Ctx.before(order.keys, item, heap.peek().?)) continue;
                _ = heap.pop();
            }
            heap.push(gpa, item) catch unreachable; // within the capacity reserved above
        }
        const items = heap.items;
        std.mem.sortUnstable(Item, items, order.keys, Ctx.before);
        const start: usize = @intCast(@min(order.offset, items.len));
        const keep = try gpa.alloc(bool, rows.len);
        defer gpa.free(keep);
        const kept = try gpa.alloc([]const AggValue, items.len - start);
        @memset(keep, false);
        for (items[start..], kept) |item, *k| {
            k.* = item.row;
            keep[item.i] = true;
        }
        for (rows, keep) |row, kp| if (!kp) self.releaseRow(gpa, row);
        gpa.free(rows);
        self.group_rows = kept;
    }

    fn rowLessThan(keys: []const expr_ast.OrderKey, a: []const AggValue, b: []const AggValue) bool {
        for (keys) |key| {
            const ord = orderValues(a[key.column], b[key.column], key.descending);
            if (ord != .eq) return ord == .lt;
        }
        return false;
    }
};

/// How two result values of one column sort. NULL last in either direction; NaN above every number, as zpq's float
/// min/max order it; strings bytewise; DECIMAL exactly.
fn orderValues(a: AggValue, b: AggValue, descending: bool) std.math.Order {
    return orderSortKeys(sortKey(a), sortKey(b), descending);
}

fn orderSortKeys(a: SortKey, b: SortKey, descending: bool) std.math.Order {
    const a_null = a == .none;
    const b_null = b == .none;
    if (a_null or b_null) return if (a_null == b_null) .eq else if (a_null) .gt else .lt;
    const ord = compareKeys(a, b);
    return if (descending) ord.invert() else ord;
}

const SortKey = union(enum) { none, int: i128, float: f64, dec: DecValue, bytes: []const u8 };

fn sortKey(v: AggValue) SortKey {
    return switch (v) {
        .i => |i| .{ .int = i },
        .f => |f| .{ .float = f },
        .avg => |a| if (a.sum) |s| .{ .float = s / @as(f64, @floatFromInt(a.count)) } else .none,
        .dec => |d| .{ .dec = d },
        .avg_dec => |a| if (a.mean) |m| .{ .dec = .{ .v = m, .spec = a.spec } } else .none,
        .s => |s| .{ .bytes = s },
        .null_val => .none,
    };
}

fn compareKeys(a: SortKey, b: SortKey) std.math.Order {
    return switch (a) {
        .bytes => |x| if (b == .bytes) std.mem.order(u8, x, b.bytes) else .gt,
        .dec => |x| if (b == .dec and b.dec.spec.scale == x.spec.scale)
            std.math.order(x.v, b.dec.v)
        else if (b == .bytes)
            .lt
        else
            compareFloats(asFloat(a), asFloat(b)),
        .int => |x| if (b == .int) std.math.order(x, b.int) else if (b == .bytes) .lt else compareFloats(asFloat(a), asFloat(b)),
        .float => if (b == .bytes) .lt else compareFloats(asFloat(a), asFloat(b)),
        .none => unreachable,
    };
}

fn asFloat(k: SortKey) f64 {
    return switch (k) {
        .int => |i| @floatFromInt(i),
        .float => |f| f,
        .dec => |d| @as(f64, @floatFromInt(d.v)) / std.math.pow(f64, 10, @floatFromInt(d.spec.scale)),
        .none, .bytes => unreachable,
    };
}

fn compareFloats(a: f64, b: f64) std.math.Order {
    if (std.math.isNan(a) or std.math.isNan(b)) {
        return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
    }
    return std.math.order(a, b);
}

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
    rows_in: i64,
    rows_kept: i64,
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

/// What one GROUP BY result column holds. `nullable` is false only for counts, matching the 1-row output.
pub const GroupColType = struct {
    lane: enum { i64, f64, string, avg, dec, avg_dec },
    /// The DECIMAL type of a `dec` column, or of an `avg_dec` mean.
    dec: ?expr_ast.Dec = null,
    nullable: bool,
    /// The column whose values pass through unchanged (a bare key, min/max of a column), to type the output by.
    source: ?schema.SchemaElement = null,
};

/// One unit of parallel work: a single row group within one input file,
/// optionally restricted to a subset of the query's aggregates/columns.
const WorkItem = struct {
    file: usize,
    rg: usize,
    agg_start: usize,
    agg_len: usize,
    /// What this item's aggregates read; a row group statistics prove fully matching reads only `columns.consumed`.
    columns: agg_plan.Columns,
    accumulators: []expr_agg.Accumulator,
};

/// Bound query and footer facts only: choosing a strategy neither allocates nor reads the input.
pub const PlanInputs = struct {
    ndv_stat: ?NdvStat = null,
    args: MultiAggArgs,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
    outputs: ?[]const BoundOutputColumn,
    filter_cols: []const bool,
    workers: usize,
    /// Some input is read in behind the reader's gates (`Input.readerBacked`), which holds a window of its own.
    readers: bool = false,
    filter: ?filter_ast.Filter = null,
    /// The share of rows the filter passes in the first row group the scan reads, counted (`sampledPassShare`).
    sampled_pass: ?f64 = null,
    /// The count was asked for but the inputs' bytes do not hold that row group (S3, explained from its footer): the
    /// estimate decides alone, as the execution reading them would not.
    sample_unread: bool = false,
};

pub const Reason = enum {
    small_ndv,
    unknown_ndv,
    census_needs_uniqueness_evidence,
    census_fixed_width_bail,
    census_short_string_composite,
    high_cardinality_strings,
    worker_copy_memory,
    dictionary_union,
    dictionary_union_budget,
    dictionary_union_incomplete,
    state_memory_unknown,
    partitioned_forced,
    partitioned_ineligible,
    sketch_disabled,
    sketch_memory_limit,
    sketch_ineligible,
    sketch_count_cut,
    keys_look_unique,
    filter_small,
    ordered_blocks,
    first_keys_cut,
    plain_scan,
    census_disabled,
    census_memory_or_work_limit,
    census_fused,
    census_on_sketch_bail,
    census_distinct,
    result_cut,
    result_cut_unordered,
    result_cut_secondary_keys,
    result_cut_float,
    result_cut_unsupported,
    result_cut_fallible_payload,
    few_dictionary_entries,
    repeated_dictionary_entries,
    selective_filter,
};

pub const AggregateDispatch = struct {
    strategy: enum { scalar, plain_hash, partitioned_hash, sketch, first_keys },
    use_blocks: bool,
    tables: usize,
    count_columns: bool,
    sketch_fits: bool,
    keys_unique: bool,
    filter_small: bool,
    count_agg: ?usize,
    census_allowed: bool,
    census_fused: bool,
    census_cell_repeat_gate: bool = false,
    /// `count_agg` is a count(DISTINCT) the census certifies (`censusDistinctTopK`), not a COUNT.
    distinct_census: bool = false,
};

/// What a plan was made from, for `--explain`. Rows and row groups are exact; the pass share is an estimate.
pub const PlanFacts = struct {
    rows: u64 = 0,
    row_groups: usize = 0,
    /// The share of rows the filter passes, from row-group bounds (`passShare`); null without a filter.
    pass_share: ?f64 = null,
    /// The share it passes in the first row group the scan reads, counted there; null where no rule asked.
    sampled_pass: ?f64 = null,
    /// Some input is read behind the reader's gates (`Input.readerBacked`).
    readers: bool = false,
    /// The dictionary probe asked of the reader: its ranges, their page-aligned bytes, and whether its cap sent the
    /// reader's inputs to their footers instead.
    probe_ranges: usize = 0,
    probe_bytes: u64 = 0,
    probe_capped: bool = false,
    /// Where the group bound came from, and why; null without a GROUP BY bound to plan from.
    planning: ?Planning = null,
    /// The strategies whose exactness preconditions hold for this query; the cost rules chose `dispatch.strategy`.
    admissible: std.EnumSet(@FieldType(AggregateDispatch, "strategy")) = .{},
};

/// The facts a GROUP BY was planned from: the dictionaries' union, their headers, or the footers alone, and why. A plan
/// can differ by read mode (`--explain` says which it made): a reader's input unions only where a choice turns on it.
pub const Planning = struct {
    source: enum { footers, dictionary_headers, dictionary_union },
    why: enum {
        /// The union ran over mapped inputs.
        mapped,
        /// The union ran over the dictionaries a reader's probe read first.
        reader_probe,
        /// `ZPQ_NDV_UNION=0`.
        union_disabled,
        /// A first-keys cut whose footers say its codes resolve many entries: no bound can change it.
        prefix_cut,
        /// Beside a reader, no choice turns on the union (no count cut, dense fold or count(DISTINCT)).
        reader_no_choice,
        /// The union was wanted for the dense fold alone, which the footers or headers say would decline it.
        fold_declines,
        /// The reader's probe of the dictionaries passed its cap.
        probe_capped,
        /// The union's input or memory gates declined it.
        union_budget,
        /// The union began and gave up: a key past its gates, or a union no choice could use past its size.
        union_incomplete,
        /// Some key has no dictionary in every row group, or the input is remote: footers and headers only.
        no_dictionaries,
    },
};

/// Fixed-size decision; census and plain fallback remain conditional runtime transitions.
pub const ScanPlan = struct {
    ndv_stat: ?NdvStat = null,
    plain_bytes: ?u64 = null,
    dispatch: AggregateDispatch,
    reasons: std.EnumSet(Reason) = .{},
    facts: PlanFacts = .{},
    /// The filter rule's estimate wants the measured pass share (`PlanInputs.sampled_pass`) before it commits.
    wants_sample: bool = false,
};

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
        firstCutEligible(args, in.calls, use_blocks, in.work.len, tables);
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
        args.sketch_topk.?.census and firstCutEligible(args, in.calls, use_blocks, in.work.len, tables)));
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
        .census_short_string_composite, .census_distinct,
    }) |r|
        plan.reasons.remove(r);
    plan.reasons.insert(.plain_scan);
}

const selective_share = 0.05;

/// The share of rows the filter passes, estimated from row-group bounds (`filter_prune.passFraction`); 1 without a
/// filter or rows. An estimate: it only steers a cost rule between exact strategies.
fn passShare(in: PlanInputs) f64 {
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
fn provenPassShare(in: PlanInputs) f64 {
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
fn sampledPassShare(
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
        if (std.mem.startsWith(u8, in.name, "s3://") or (in.logical_size != 0 and in.logical_size != in.bytes.len))
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
        consumer.scanRGForAgg(gpa, .{
            .rg = rg,
            .file_index = item.file,
            .rg_index = item.rg,
            .meta = meta,
            .src = .{ .bytes = inputs[item.file].bytes, .byte_origin = 0 },
            .filter = plan.filterFor(decision),
            .read = read,
            .consumed = consumed,
        }, plan.stats, options, .{ .scalar = .{ .calls = &calls, .accumulators = &passed } }, .{
            .scratch = &scratch,
            .rg_arena = &rg_arena,
            .timings = &timings,
            .budget = if (budget) |*b| b else null,
        }) catch |e| return if (e == error.ExceededMemoryBudget) 1 else e;
        return @as(f64, @floatFromInt(passed[0].count)) / @as(f64, @floatFromInt(rg.num_rows));
    }
    return 0;
}

/// A lone key whose dictionaries, summed over row groups, hold few entries: grouping by their codes is cheap.
const few_entries = 1 << 23;
/// The union `fewEntries` needs: a small domain, or one an eighth of the entries or less.
const few_union: ndv.UnionLimit = .{ .entries = few_entries, .values = 1 << 17, .recur = 8 };

/// Grouping a lone key's codes plainly is cheap where they resolve few entries in all and the union proves either a
/// small domain or keys that recur across row groups (the dense fold's case); a large domain whose keys each sit in
/// one or two row groups (clustered) pays for a union and declines the fold.
fn fewEntries(stat: NdvStat, keys: []const expr_ast.Expr) bool {
    const n = stat.entries orelse return false;
    if (keys.len != 1 or !stat.union_complete or n > few_union.entries) return false;
    return stat.hi <= few_union.values or n >= stat.hi *| few_union.recur;
}

fn applyCostRules(in: PlanInputs, stat: NdvStat, plan: *ScanPlan) void {
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
    const per_group = (stat.key_bytes +| (@as(u64, @sizeOf(expr_agg.Accumulator)) *| in.calls.len) +| 32) *| 2;
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
    const small = stat.confidence == .upper_bound and stat.hi <= 1 << 20 and
        !state_unknown and copies <= in.args.max_memory / 8;
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
    // payloads and long keys on the existing path; only the runtime sparse-repeat hint can justify this census.
    const short_composite = !small and stat.string_keys and keys.len > 1 and
        stat.key_bytes < 32 and in.calls.len == 1;
    const bail_census = plan.dispatch.strategy == .sketch and plan.dispatch.census_allowed and
        (expr_agg.fixedGroupStride(keys) != null or short_composite) and !tiny and !cheap_r1;
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
        plan.dispatch.keys_unique = looks_unique;
        plan.dispatch.census_allowed = bail_census;
        plan.dispatch.census_fused = looks_unique or fixed_composite;
        plan.dispatch.census_cell_repeat_gate = bail_census and (short_composite or !bounded_census);
        plan.reasons.remove(.keys_look_unique);
        plan.reasons.remove(.census_fused);
        plan.reasons.remove(.census_on_sketch_bail);
        if (looks_unique) plan.reasons.insert(.keys_look_unique);
        if (looks_unique or fixed_composite) plan.reasons.insert(.census_fused);
        if (plan.dispatch.strategy == .sketch) {
            if (bail_census) {
                plan.reasons.insert(if (short_composite) .census_short_string_composite else .census_fixed_width_bail);
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
    const groups_fit = stat.confidence == .upper_bound and !state_unknown and copies <= in.args.max_memory / 8;
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
            plan.dispatch.strategy == .sketch and !(short_composite and bail_census))
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

pub const AggregateExplanation = struct {
    plan: ScanPlan,
    /// Names copied into the caller's arena: parsed footer scratch may already be gone.
    column_names: []const []const u8,
    columns: agg_plan.Columns,
    count_columns: ?agg_plan.Columns,
    workers: usize,
    work_items: usize,
};

/// Ordered GROUP BY folds the work list in at most this many fixed blocks, each into a table of its own (see
/// `MultiAggArgs.unordered`). Fixed, not the worker count: the blocks, and so the order partial sums meet in, must not
/// depend on `-j`. Enough that 32 workers share them evenly; few enough that their tables cost little more than one
/// per worker.
const group_blocks_max = 64;

/// Zig's 16 MiB default makes worker creation expensive for short scans. Safe to shrink: these workers decode, and
/// every nesting path here is bounded.
pub const WORKER_STACK_SIZE: usize = 1 << 20;

/// Address space each scan worker reserves for its decode memory; only what a row group touches is committed, and a
/// worker needing more spills to the general allocator.
const WORKER_REGION_RESERVE: usize = 1 << 30;

/// One footer-parsing worker. Files are claimed dynamically so wildly uneven file sizes don't leave threads idle.
const ParseCtx = struct {
    inputs: []const Input,
    out: []schema.FileMetaData,
    cursor: *std.atomic.Value(usize),
    /// Private to this worker: ArenaAllocator is not thread-safe, so no two threads may ever share one ParseCtx.
    arena: *std.heap.ArenaAllocator,
    err: ?anyerror = null,
    bad_index: usize = 0,
    /// Entries into `parseWorker` with this context; must never exceed 1. Asserted after join rather than watching for
    /// the race itself, which is UB and unreliable to observe.
    runs: std.atomic.Value(u32) = .init(0),
    profiler: ?*profile.Profiler = null,
};

fn parseWorker(c: *ParseCtx) void {
    _ = c.runs.fetchAdd(1, .monotonic);
    const prof = profile.threadOf(c.profiler, .parse);
    while (true) {
        const i = c.cursor.fetchAdd(1, .monotonic);
        if (i >= c.inputs.len) break;
        const span = profile.Span.begin(prof);
        defer span.end(.footer_parse, profile.args(.{ i, footerLen(c.inputs[i].bytes) }));
        c.out[i] = metadata.open(c.arena.allocator(), c.inputs[i].bytes) catch |e| {
            if (c.err == null) {
                c.err = e;
                c.bad_index = i;
            }
            // Don't stop: the reported failure must be the lowest-indexed one.
            continue;
        };
    }
}

/// Outcome of parsing every input's footer. Failure reports the LOWEST failing index, not whichever worker lost the
/// race, so runs stay reproducible.
const FooterParse = union(enum) {
    ok: []schema.FileMetaData,
    failed: struct { err: anyerror, index: usize },
};

/// Parse all input footers, in parallel when there is more than one file.
///
/// Ownership of the per-worker arenas passes to the caller via `meta_arenas_out`, which must outlive the returned
/// metadata: the returned slice points into them.
fn parseFooters(
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    inputs: []const Input,
    parallelism: usize,
    meta_arenas_out: *[]std.heap.ArenaAllocator,
    profiler: ?*profile.Profiler,
) std.mem.Allocator.Error!FooterParse {
    const parsed = try arena.alloc(schema.FileMetaData, inputs.len);
    const parse_threads = @min(
        inputs.len,
        if (parallelism == 0) (std.Thread.getCpuCount() catch 1) else parallelism,
    );

    if (parse_threads <= 1) {
        const prof = profile.threadOf(profiler, .main);
        for (inputs, 0..) |in, i| {
            const span = profile.Span.begin(prof);
            defer span.end(.footer_parse, profile.args(.{ i, footerLen(in.bytes) }));
            parsed[i] = metadata.open(arena, in.bytes) catch |err| {
                return .{ .failed = .{ .err = err, .index = i } };
            };
        }
        return .{ .ok = parsed };
    }

    const meta_arenas = try arena.alloc(std.heap.ArenaAllocator, parse_threads);
    for (meta_arenas) |*a| a.* = std.heap.ArenaAllocator.init(gpa);
    meta_arenas_out.* = meta_arenas;

    var cursor = std.atomic.Value(usize).init(0);
    const ctxs = try arena.alloc(ParseCtx, parse_threads);
    for (ctxs, 0..) |*c, i| c.* = .{
        .inputs = inputs,
        .out = parsed,
        .cursor = &cursor,
        .arena = &meta_arenas[i],
        .profiler = profiler,
    };

    const batch_span = profile.Span.begin(profile.threadOf(profiler, .main));
    defer batch_span.end(.footer_batch, profile.args(.{inputs.len}));
    const threads = try arena.alloc(std.Thread, parse_threads);
    var spawned: usize = 0;
    for (ctxs) |*c| {
        threads[spawned] = spawn_util.spawn(.{ .stack_size = WORKER_STACK_SIZE }, parseWorker, .{c}) catch break;
        spawned += 1;
    }
    // Must use a context no thread owns — see `ParseCtx.arena`; reusing ctxs[0] would race live worker 0. One inline
    // pass drains whatever is left however many spawns failed, since work comes off a shared cursor.
    if (spawned < parse_threads) parseWorker(&ctxs[spawned]);
    // Join before reading anything the threads wrote, and before the caller can tear `meta_arenas` down underneath
    // them.
    for (threads[0..spawned]) |th| th.join();

    // Historical bug: the inline pass ran on `ctxs[0]` while worker 0 was still live, putting two threads in one arena.
    for (ctxs) |*c| std.debug.assert(c.runs.load(.monotonic) <= 1);

    var worst: ?ParseCtx = null;
    for (ctxs) |c| {
        if (c.err == null) continue;
        if (worst == null or c.bad_index < worst.?.bad_index) worst = c;
    }
    if (worst) |w| return .{ .failed = .{ .err = w.err.?, .index = w.bad_index } };
    return .{ .ok = parsed };
}

/// Ordered GROUP BY whose results depend on the order partials meet in (`orderSensitive`): each finished block merges
/// into one table, strictly in block order, as soon as every block before it has, and its own table is freed. A worker
/// starts block `b` only once `b` is fewer than `ahead` blocks past the first unmerged one, so at most `ahead` block
/// tables are alive beside the merged one, about what one table per worker holds.
///
/// A worker ahead of the window sleeps on `moved` rather than spinning: with more workers than free CPUs (an
/// oversubscribed or virtualised host) a yielding waiter takes the CPU the merging worker needs to let it go.
const BlockMerge = struct {
    tables: []expr_agg.GroupTable,
    merged: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    gpa: std.mem.Allocator,
    done: []std.atomic.Value(bool),
    /// The first block not yet merged.
    next: std.atomic.Value(usize) = .init(0),
    ahead: usize,
    lock: std.Io.Mutex = .init,
    failed: std.atomic.Value(bool) = .init(false),
    /// Bumped whenever `next` advances or a worker fails: the word `awaitTurn` sleeps on.
    moved: std.atomic.Value(u32) = .init(0),

    /// Only the OS futex is used, which no `Io` instance holds state for; the scan has no `Io` of its own.
    const futex_io = std.Io.Threaded.global_single_threaded.io();

    /// Wait until block `b` may start; false once another worker has failed.
    fn awaitTurn(self: *BlockMerge, b: usize) bool {
        while (true) {
            // Read `moved` before the condition: a merge or failure between the two changes it, and the wait returns.
            const seen = self.moved.load(.acquire);
            if (self.failed.load(.acquire)) return false;
            if (b < self.next.load(.acquire) + self.ahead) return true;
            futex_io.futexWaitUncancelable(u32, &self.moved.raw, seen);
        }
    }

    /// Block `b` is complete: merge it, and any finished after it, once every earlier block has merged. Whoever holds
    /// the lock merges every block ready by then, so none is left behind.
    fn finish(self: *BlockMerge, b: usize) !void {
        self.done[b].store(true, .release);
        self.lock.lockUncancelable(futex_io);
        defer self.lock.unlock(futex_io);
        var n = self.next.load(.monotonic);
        while (n < self.tables.len and self.done[n].load(.acquire)) : (n += 1) {
            const src = &self.tables[n];
            const len = self.calls.len;
            for (0..src.groupCount()) |g| {
                const id = try self.merged.getOrInsert(src.keyAt(g), self.calls, src.first.items[g]);
                const dst = self.merged.accumulators.items[@as(usize, id) * len ..][0..len];
                for (dst, src.accumulators.items[g * len ..][0..len]) |*d, *sa| {
                    try mergeOwnedAccumulator(d, sa, self.gpa);
                }
            }
            src.deinit();
            src.* = expr_agg.GroupTable.init(self.gpa, 0);
            self.next.store(n + 1, .release);
            self.wake();
        }
    }

    fn fail(self: *BlockMerge) void {
        self.failed.store(true, .release);
        self.wake();
    }

    fn wake(self: *BlockMerge) void {
        _ = self.moved.fetchAdd(1, .release);
        futex_io.futexWake(u32, &self.moved.raw, std.math.maxInt(u32));
    }
};

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

fn partAggSupported(args: MultiAggArgs, calls: []const expr_agg.AggCall, grouped: bool) bool {
    if (!grouped or orderSensitive(calls)) return false;
    for (calls) |c| if (c.func == .count_distinct) return false;
    return args.result_order.limit == null or args.result_order.keys.len > 0;
}

/// Whether merging partial results in another order can change their bits: float sums and averages round
/// differently, and a float min or max may come out -0 or +0. Integer and string results never do.
fn orderSensitive(calls: []const expr_agg.AggCall) bool {
    for (calls) |c| {
        if (c.func == .count or c.func == .count_distinct) continue;
        if (c.result == .f64) return true;
        // An avg sums integer values exactly (`AvgState.isum`); only a float argument's sum depends on order.
        if (c.result == .avg_f64 and (c.arg == null or c.arg.?.typeOf() != .i64)) return true;
    }
    return false;
}

/// One worker thread's slice of work.
const Worker = struct {
    gpa: std.mem.Allocator,
    inputs: []const Input,
    metas: []const schema.FileMetaData,
    /// The FULL work list, shared by every worker; items are claimed through `cursor`. Row-group cost varies too much
    /// — size, encoding, whether a filter prunes them — for a static split not to leave workers idle.
    work: []const WorkItem,
    /// Relaxed ordering suffices: this only hands out disjoint indices, and the post-join merge is the real
    /// synchronization edge.
    cursor: *std.atomic.Value(usize),
    /// Items a census admit pass passes over (`censusTopK`).
    skip: ?[]const bool = null,
    /// Set once a pass is abandoned (`Census.distinct`): the worker claims no more items.
    stop: ?*const std.atomic.Value(bool) = null,
    /// What a sketch's count pass reads: the outer filter's columns and the keys, no aggregate's (`Plan.forCalls`).
    count_columns: ?agg_plan.Columns = null,
    agg_calls: []const expr_agg.AggCall,
    plan: *const agg_plan.Plan,
    decode_options: consumer.DecodeOptions,
    timings: consumer.Timings = .{},
    rows_in: i64 = 0,
    rows_kept: i64 = 0,
    rgs_in: usize = 0,
    rgs_pruned: usize = 0,
    rgs_full_match: usize = 0,
    group_by_keys: ?[]const expr_ast.Expr = null,
    /// GROUP BY tables, shared by every worker: one per worker when unordered (this worker's is `index`), else one per
    /// block of `blocks`.
    group_tables: []expr_agg.GroupTable = &.{},
    /// Shared partitioned tables, with this worker's pre-aggregation state. Null keeps today's per-worker table.
    part: ?*partagg.PartAgg = null,
    local: partagg.Local = .{},

    /// Grouped count(DISTINCT) as pairs instead of sets (`expr_agg.DistinctPairs`), for this worker's table.
    pairs: ?*expr_agg.DistinctPairs = null,
    /// This worker's lanes over a retained key domain (`agg_dense`); row groups it cannot translate use the table.
    dense: ?*agg_dense.Lanes = null,
    /// Whether this worker has decided if its sets move to pairs.
    pairs_decided: bool = false,
    /// Values its sets took (`DistinctPairs.seen`) at which they are judged next, and how many more judgments a
    /// `.wait` may buy.
    pairs_pilot_rows: u64 = 0,
    pairs_rechecks: u8 = 0,
    /// Ordered GROUP BY: the work list split into fixed blocks, block `b` = `work[blocks[b]..blocks[b + 1]]`, claimed
    /// whole through `block_cursor` and folded in order into `group_tables[b]`.
    blocks: ?[]const usize = null,
    block_cursor: ?*std.atomic.Value(usize) = null,
    /// With `blocks`: where finished blocks merge, in block order.
    block_merge: ?*BlockMerge = null,
    /// Work items per row group (every row group gets the same number), for `Input.gate`.
    items_per_rg: usize = 1,
    /// This worker's decode memory, mapped by its first pass and kept across the passes over `workers` (a sketch's
    /// count and admit passes, a census), so a later pass reuses faulted pages instead of zeroing fresh ones. Only the
    /// owning thread touches it during a pass; `prepareAggregate` unmaps it after the last.
    region: ?huge_pages.Region = null,
    /// The decode scratch's (page and dictionary buffers, kept across row groups), beside `region`, which the row
    /// group arena has to itself (`consumer.RowGroupArena.initOn`). Drawn from `ledger` as `region` is.
    scratch_region: ?huge_pages.Region = null,
    decode_budget: ?*consumer.DecodeBudget = null,
    /// The query's ledger, which the region and `charged` draw from; null draws nothing.
    ledger: ?*expr_agg.SharedBudget = null,
    /// Memory a row group decodes on beside the region (staged, sliced), drawn from `ledger`.
    charged: ?core_memory.Charged = null,
    /// A scalar query's string min/max winners, each copy drawn from the query's ledger before it is made.
    winners: ?*WinnerCharge = null,
    err: ?anyerror = null,
    profiler: ?*profile.Profiler = null,
    index: usize = 0,
};

fn workerRun(w: *Worker) void {
    const prof = profile.threadOf(w.profiler, .scan);
    const span = profile.Span.begin(prof);
    const usage0: profile.Usage = if (prof != null) profile.usage(.thread) else .{};
    var items: usize = 0;
    defer if (prof != null) {
        const u = profile.usage(.thread).sub(usage0);
        span.end(.scan_worker, profile.args(.{ w.index, items, u.user_ns, u.sys_ns, u.minor_faults, u.major_faults }));
    };
    w.timings.prof = prof;
    expr_agg.distinct_pairs = w.pairs;
    defer expr_agg.distinct_pairs = null;
    workerRunErr(w, prof, &items) catch |e| {
        w.err = workerRefusal(w, e);
        if (w.block_merge) |m| m.fail();
    };
    // Holding a drawn block until teardown let a barely-grouping worker strand a small budget and fail the worker
    // doing the real work. (Ordered blocks release theirs as each finishes.)
    if (w.blocks == null and w.group_tables.len > 0) w.group_tables[w.index].releaseSlack();
}

const WinnerCharge = expr_agg.WinnerCharge;

fn ownedWinnerBytes(accs: []const expr_agg.Accumulator) usize {
    var n: usize = 0;
    for (accs) |acc| switch (acc) {
        .min_bytes, .max_bytes => |s| if (s) |v| {
            n += v.len;
        },
        else => {},
    };
    return n;
}

/// Whether the worker drew decode memory from the query's ledger, past its region's allowance, or was refused it.
fn decodeHeld(w: *const Worker) bool {
    if (w.region) |r| if (r.charged > 0 or r.spilled > 0 or r.refused) return true;
    if (w.scratch_region) |r| if (r.charged > 0 or r.spilled > 0 or r.refused) return true;
    if (w.charged) |c| return c.drew.load(.monotonic) or c.refused.load(.monotonic);
    return false;
}

/// `err`, or `error.ExceededMemoryBudget` when it is the out-of-memory a refused draw from the ledger raised.
fn workerRefusal(w: *const Worker, err: anyerror) anyerror {
    if (err != error.OutOfMemory) return err;
    if (w.region) |r| if (r.refused) return error.ExceededMemoryBudget;
    if (w.scratch_region) |r| if (r.refused) return error.ExceededMemoryBudget;
    if (w.charged) |c| return c.refusal(err);
    return err;
}

fn workerRunErr(w: *Worker, prof: ?*profile.Thread, items: *usize) !void {
    // Declared first so it runs last: the release span covers freeing the arena and scratch below.
    var release_span: profile.Span = .{ .t = null, .start = 0 };
    defer release_span.end(.worker_release, profile.args(.{w.index}));
    // One decode arena for the whole run: scanRGForAgg rewinds it per row group instead of regrowing a fresh one.
    // It and the scratch are written front to back and reused, each on a huge-page region of the worker's (see
    // `huge_pages.Region`), rewound here for the next pass. The two share their first huge page, the worker's slack
    // (`Region.initPair`): a small scan faults in one huge page per worker, not one for each region.
    if (w.region == null) {
        std.debug.assert(w.scratch_region == null);
        const pair = huge_pages.Region.initPair(w.gpa, WORKER_REGION_RESERVE);
        w.region = pair.arena;
        w.region.?.budget = w.ledger;
        w.scratch_region = pair.scratch;
        w.scratch_region.?.budget = w.ledger;
    }
    if (w.ledger) |l| if (w.charged == null) {
        // Straight from the OS, not the general heap: a sliced row group's arenas come and go, and freed heap blocks
        // stay resident in its per-thread arenas, held but no longer charged.
        w.charged = .{ .child = std.heap.page_allocator, .budget = l };
    };
    const region = &w.region.?;
    defer region.rewind();
    var rg_decode_arena = consumer.RowGroupArena.initOn(region);
    defer rg_decode_arena.deinit();
    const scratch_region = &w.scratch_region.?;
    defer scratch_region.rewind();
    var scratch = consumer.DecodeScratch.init(scratch_region.allocator());
    defer scratch.deinit();
    scratch.prof = prof;

    var next: usize = 0;
    var block_end: usize = 0;
    var block: ?usize = null;
    var table: ?*expr_agg.GroupTable = if (w.blocks == null and w.group_tables.len > 0) &w.group_tables[w.index] else null;
    while (true) {
        const claimed: ?usize = if (w.blocks) |bounds| claim: {
            while (next == block_end) {
                if (block) |done| try w.block_merge.?.finish(done);
                block = null;
                const b = w.block_cursor.?.fetchAdd(1, .monotonic);
                if (b + 1 >= bounds.len) break :claim null;
                if (!w.block_merge.?.awaitTurn(b)) break :claim null;
                next = bounds[b];
                block_end = bounds[b + 1];
                block = b;
                table = &w.group_tables[b];
            }
            next += 1;
            break :claim next - 1;
        } else claim: {
            if (w.stop) |st| if (st.load(.monotonic)) break :claim null;
            const i = w.cursor.fetchAdd(1, .monotonic);
            break :claim if (i < w.work.len) i else null;
        };
        const idx = claimed orelse {
            release_span = .begin(prof);
            break;
        };
        if (w.skip) |skip| if (skip[idx]) continue;
        items.* += 1;
        const item = w.work[idx];
        const gate = w.inputs[item.file].gate;
        defer if (gate) |g| g.done(g.ctx, item.rg, w.items_per_rg);
        const meta = &w.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        // File bytes are shared read-only across workers (one mmap / one
        // in-memory buffer); concurrent ranged reads are safe.
        const rg_src: consumer.RGSrc = .{ .bytes = w.inputs[item.file].bytes, .byte_origin = 0 };
        if (item.agg_start == 0) {
            w.rgs_in += 1;
            w.rows_in +|= rg.num_rows;
        }
        const rg_span = profile.Span.begin(prof);
        const decision = w.plan.decide(rg, meta);
        defer if (rg_span.t != null) rg_span.end(.row_group, profile.args(.{
            item.file,
            item.rg,
            @backingInt(decision),
            rg.num_rows,
            if (decision == .skip) skipReason(w.plan.query.filter.?, rg, meta) else 0,
            if (decision == .skip) 0 else std.mem.count(bool, item.columns.forRowGroup(decision), &.{true}),
        }));
        switch (decision) {
            .skip => {
                if (item.agg_start == 0) w.rgs_pruned += 1;
                continue;
            },
            .full_match => if (item.agg_start == 0) {
                w.rgs_full_match += 1;
            },
            .filter => {},
        }
        if (item.agg_start == 0) {
            w.rows_kept +|= rg.num_rows;
        }
        if (gate) |g| try g.wait(g.ctx, item.rg);
        const sub_agg_calls = w.agg_calls[item.agg_start .. item.agg_start + item.agg_len];
        // A sketch's count pass reads only the filter's columns and the keys.
        const columns = if (table != null and table.?.countOnly()) w.count_columns.? else item.columns;
        // A census notes the item each row was read in, and how many rows each item gave it.
        const census: ?*expr_agg.Census = if (table) |gt| if (gt.sketch) |s| s.census else null else null;
        const recorded_before = if (census) |c| c.recorded else 0;
        if (census) |c| c.item = idx;
        defer if (census) |c| if (c.admitted == null) {
            c.item_rows[idx] = c.recorded - recorded_before;
        };
        const target: consumer.AggTarget = if (table) |gt|
            .{ .grouped = .{
                .calls = sub_agg_calls,
                .keys = w.group_by_keys.?,
                .table = gt,
                .pos_base = expr_agg.firstPosition(idx, 0),
                .part = w.part,
                .local = if (w.part != null) &w.local else null,
                .dense = w.dense,
            } }
        else
            .{ .scalar = .{ .calls = sub_agg_calls, .accumulators = item.accumulators } };
        expr_agg.winner_charge = if (table == null) w.winners else null;
        defer expr_agg.winner_charge = null;
        try consumer.scanRGForAgg(w.gpa, .{
            .rg = rg,
            .file_index = item.file,
            .rg_index = item.rg,
            .meta = meta,
            .src = rg_src,
            .filter = w.plan.filterFor(decision),
            .read = columns.forRowGroup(decision),
            .consumed = columns.consumed,
            .preserve = columns.preserve,
        }, w.plan.stats, w.decode_options, target, .{
            .scratch = &scratch,
            .rg_arena = &rg_decode_arena,
            .timings = &w.timings,
            .budget = w.decode_budget,
            .region = region,
            .charged = if (w.charged) |*c| c.allocator() else null,
            .ledger = w.ledger,
        });
        if (w.pairs) |dp| if (!dp.active and !w.pairs_decided) if (table) |gt| {
            if (dp.seen >= w.pairs_pilot_rows) switch (pairsPayRuntime(gt, dp.agg_idx, dp.seen)) {
                .pay => {
                    w.pairs_decided = true;
                    try dp.drainTable(gt);
                    dp.active = true;
                },
                .wait => if (w.pairs_rechecks > 0) {
                    w.pairs_rechecks -= 1;
                    w.pairs_pilot_rows = dp.seen *| 2;
                } else {
                    w.pairs_decided = true;
                },
                .never => w.pairs_decided = true,
            };
        };
    }
    // An armed worker that never moved keeps its table for the usual merge, unless another worker moved (`spilled`).
    if (w.part) |p| if (!w.local.armed and w.blocks == null and w.group_tables.len > 0)
        try p.flush(&w.local, &w.group_tables[w.index]);
}

fn releaseRegions(workers: []Worker) void {
    for (workers) |*w| {
        // The scratch first: the arena's region owns the mapping the two share.
        if (w.scratch_region) |*r| r.deinit();
        if (w.region) |*r| r.deinit();
        w.region = null;
        w.scratch_region = null;
    }
}

fn runWorkers(workers: []Worker) void {
    if (workers.len > 1) {
        // Started as a tree, so the last worker is not a millisecond late. Spawn failure never returns early (running
        // threads hold pointers into `arena` and `group_tables`); workers that never start are idle, and with no
        // thread at all one inline runner drains the shared cursor.
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (!spawn_util.runTree(Worker, workers, config, workerRun)) workerRun(&workers[0]);
    } else {
        // Single-threaded fast path. Avoids std.Thread overhead.
        workerRun(&workers[0]);
    }
}

/// The COUNT call a sketched scan can certify the cut by: the first sort key is that call, descending, under a LIMIT.
/// Per-worker tables only (not ordered blocks), and only inputs read whole up front: a gated input reads each row
/// group once, and the sketch reads them twice.
fn sketchEligible(
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

/// The `func` call ranking a GROUP BY first, descending, under a LIMIT, where a count pass and an admit pass can cut
/// it: see `sketchEligible`.
fn rankingCall(
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

/// The count(DISTINCT) call a census can certify a cut by (`censusDistinctTopK`): the first sort key is that call,
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

/// A COUNT-descending cut of `k = offset + limit` groups, grouping only the keys that can make it. The count pass
/// gives no key a group: each row adds one to its key's hash cell. The cells, summed over workers, bound every key's
/// total. The admit pass groups, exactly and with first appearance, every key of the `T` fullest cells, and drops the
/// rest. A dropped key totals at most the fullest cell not admitted, `bound`; when `bound` is below the k-th largest
/// admitted total, every group the cut keeps, ties at the cut included, is admitted, and the tables answer exactly.
/// Otherwise (or when the cells are too even for that to happen) the tables start over and the scan runs as usual.
fn sketchTopK(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    a_idx: usize,
    cfg: SketchTopK,
    decision: AggregateDispatch,
) !void {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const n_cells = @as(usize, 1) << cfg.cell_bits;
    const sketches = try arena.alloc(expr_agg.CountSketch, workers.len);
    var made: usize = 0;
    defer for (sketches[0..made]) |s| gpa.free(s.cells);
    for (sketches) |*s| {
        s.* = .{
            .cells = try gpa.alloc(u32, n_cells),
            .shift = @intCast(@as(u7, 64) - cfg.cell_bits),
            .whole_keys = decision.distinct_census,
        };
        made += 1;
        @memset(s.cells, 0);
    }
    for (tables, sketches) |*t, *s| t.sketch = s;
    defer for (tables) |*t| {
        t.sketch = null;
    };
    const admit_n = @max(k * cfg.admit_per_row, @min(64, n_cells / 16));
    // A census (`censusTopK`) rides along the count pass when it fits and the keys look unique (`keysLookUnique`), so
    // cells too even to bound anything need no second count pass. It fills the cells too, and stops early wherever a
    // worker's own cells already separate the cut, as the cells will then do without it.
    var dropped: std.atomic.Value(bool) = .init(false);
    const censuses: []expr_agg.Census = if (decision.census_allowed) blk: {
        const item_rows = try arena.alloc(u64, workers[0].work.len);
        @memset(item_rows, 0);
        const cs = try arena.alloc(expr_agg.Census, workers.len);
        for (cs) |*c| c.* = .{
            .gpa = gpa,
            .item_rows = item_rows,
            .dropped = &dropped,
            .cut_k = k,
            .admit_n = admit_n,
            .distinct = decision.distinct_census,
            .check_after = if (decision.distinct_census)
                @min(cfg.census_check_rows, expr_agg.Census.distinct_check_rows)
            else
                cfg.census_check_rows,
            .fp_mask = censusMask(cfg),
        };
        break :blk cs;
    } else &.{};
    defer for (censuses) |*c| c.deinit();
    const fused = decision.census_fused;
    if (fused) for (sketches, censuses) |*s, *c| {
        s.census = c;
    };
    defer for (sketches) |*s| {
        s.census = null;
    };
    const saved = try arena.dupe(Worker, workers);
    const t0 = nowMonoNs();
    // A count(DISTINCT) census that would admit most rows is abandoned as soon as one worker sees it.
    if (decision.distinct_census) for (workers) |*w| {
        w.stop = &dropped;
    };
    runPhase(workers, cursor) catch |e| {
        if (e != error.CensusAbandoned) return e;
        try clearWorkerErrors(workers, error.CensusAbandoned);
    };
    for (workers) |*w| w.stop = null;
    var t1 = nowMonoNs();
    for (sketches) |*s| s.census = null;
    if (decision.distinct_census) {
        const abandoned = dropped.load(.monotonic);
        if (cfg.trace) std.debug.print("census distinct: count_ms={d} abandoned={}\n", .{
            @divTrunc(t1 - t0, 1_000_000), abandoned,
        });
        if (!abandoned and try censusDistinctTopK(gpa, arena, workers, cursor, tables, sketches, censuses, calls, order, a_idx, cfg)) return;
        if (abandoned) if (cfg.outcome) |o| {
            o.* = .bailed;
        };
        for (censuses) |*c| c.deinit();
        for (tables) |*t| t.sketch = null;
        restoreCounts(workers, saved);
        try runPhase(workers, cursor);
        return;
    }

    const sum = sketches[0].cells;
    for (sketches[1..]) |s| for (sum, s.cells) |*a, b| {
        a.* +|= b;
    };
    const ranked = try gpa.dupe(u32, sum);
    defer gpa.free(ranked);
    std.mem.sortUnstable(u32, ranked, {}, std.sort.desc(u32));
    // Admitting every cell fuller than `bound` keeps ties at it out, so `bound` is the fullest cell not admitted.
    const bound = ranked[admit_n];
    const viable = expr_agg.cellsSeparate(ranked, k, admit_n);
    if (cfg.trace) std.debug.print("sketch: count_ms={d} kth_cell={d} bound={d} viable={} census_dropped={}\n", .{
        @divTrunc(t1 - t0, 1_000_000), ranked[k - 1], bound, viable, dropped.load(.monotonic),
    });
    if (!viable) if (cfg.outcome) |o| {
        o.* = .bailed;
    };
    const census_try = !viable and censuses.len > 0 and
        (!decision.census_cell_repeat_gate or censusCellBet(sum, calls.len > 1));
    if (cfg.trace and !viable and decision.census_cell_repeat_gate)
        std.debug.print("census: cell_repeat_load={d} try={}\n", .{ cellRepeatLoad(sum), census_try });
    if (census_try) {
        if (!fused or dropped.load(.monotonic)) {
            // The census did not ride along, or a worker's cells misjudged the whole: count again, census only.
            for (censuses) |*c| {
                c.deinit();
                c.* = .{ .gpa = gpa, .item_rows = c.item_rows, .fp_mask = censusMask(cfg) };
            }
            @memset(censuses[0].item_rows, 0);
            for (sketches, censuses) |*s, *c| s.census = c;
            restoreCounts(workers, saved);
            const t2 = nowMonoNs();
            try runPhase(workers, cursor);
            t1 = nowMonoNs();
            for (sketches) |*s| s.census = null;
            if (cfg.trace) std.debug.print("census: recount_ms={d}\n", .{@divTrunc(t1 - t2, 1_000_000)});
        }
        if (try censusTopK(gpa, arena, workers, cursor, tables, sketches, censuses, calls, order, a_idx, cfg, t1 - t0)) return;
    }
    for (censuses) |*c| c.deinit();
    if (viable) {
        const admit = try arena.alloc(u64, (n_cells + 63) / 64);
        @memset(admit, 0);
        for (sum, 0..) |c, i| if (c > bound) {
            admit[i / 64] |= @as(u64, 1) << @intCast(i % 64);
        };
        for (sketches) |*s| s.admit = admit;
        restoreCounts(workers, saved);
        try runPhase(workers, cursor);
        const kth = try kthTotal(arena, tables, calls.len, a_idx, k);
        const certified = cfg.certify and (bound == 0 or bound < kth);
        if (cfg.trace) std.debug.print("sketch: admit_ms={d} kth_total={d} certified={}\n", .{
            @divTrunc(nowMonoNs() - t1, 1_000_000), kth, certified,
        });
        if (certified) {
            if (cfg.outcome) |o| o.* = .certified;
            return;
        }
        if (cfg.outcome) |o| o.* = .fell_back;
        clearTables(gpa, tables);
    }
    for (tables) |*t| t.sketch = null;
    restoreCounts(workers, saved);
    try runPhase(workers, cursor);
}

/// Whether a census should ride along the sketch's cell count pass: some fixed-width key column is stored at nearly its
/// plain width in the first row group read, so its values barely repeat there. Keys that barely repeat leave the cells
/// too even to certify a cut. Otherwise the cells count alone, and the census counts in a pass of its own only if
/// they bail.
fn keysLookUnique(metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr) bool {
    if (work.len == 0) return false;
    const rg = &metas[work[0].file].row_groups.items[work[0].rg];
    if (rg.num_rows <= 0) return false;
    for (keys) |key| {
        if (key != .col_ref) continue;
        const col = key.col_ref.col_idx;
        if (col >= rg.columns.items.len) continue;
        const md = rg.columns.items[col].meta_data orelse continue;
        const width: i64 = switch (md.type) {
            .INT32, .FLOAT => 4,
            .INT64, .DOUBLE => 8,
            else => continue,
        };
        if (md.total_uncompressed_size *| 4 >= rg.num_rows *| width *| 3) return true;
    }
    return false;
}

/// Cost hint only: with B uniform hash cells, E[sum((cell - mean)^2)] = (1 - 1/B) * sum(key_count^2).
/// Dividing by row count approaches one for sparse repeats. Hash noise/collisions never certify an answer;
/// the census still uses its exact admission certificate and plain fallback. Saturated/empty cells give no hint.
fn cellRepeatLoad(cells: []const u32) f64 {
    if (cells.len < 2) return std.math.inf(f64);
    var rows: f64 = 0;
    for (cells) |c| {
        if (c == std.math.maxInt(u32)) return std.math.inf(f64);
        rows += @floatFromInt(c);
    }
    if (rows == 0) return std.math.inf(f64);
    const bins: f64 = @floatFromInt(cells.len);
    const mean = rows / bins;
    var dispersion: f64 = 0;
    for (cells) |c| {
        const delta = @as(f64, @floatFromInt(c)) - mean;
        dispersion += delta * delta;
    }
    return dispersion * (bins / (bins - 1)) / rows;
}

/// Whether a census after a sketch bail is worth recounting for. Its repeats no longer bound its use (the census keeps
/// each partition's most frequent and certifies against the rest), so the only reason left not to is a selective
/// narrow COUNT, which merges directly more cheaply than recounting every filter column.
fn censusCellBet(cells: []const u32, payload: bool) bool {
    if (payload) return true;
    var rows: f64 = 0;
    for (cells) |c| rows += @floatFromInt(c);
    return rows > 1 << 20;
}

/// Whether the columns a filter reads beyond the group keys are small beside the keys in the first row group read:
/// at most four times their bytes, uncompressed. A sketched scan decodes those columns twice.
fn filterSmall(metas: []const schema.FileMetaData, work: []const WorkItem, keys: []const expr_ast.Expr, filter_cols: []const bool) bool {
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

/// Empties `tables` for a pass that starts over, keeping their budget, key width and sketch (all three).
fn clearTables(gpa: std.mem.Allocator, tables: []expr_agg.GroupTable) void {
    for (tables) |*t| {
        const shared = t.shared;
        const stride = t.fixed_stride;
        const sketch = t.sketch;
        const layout = t.compact;
        t.deinit();
        t.* = expr_agg.GroupTable.init(gpa, 0);
        t.shared = shared;
        t.fixed_stride = stride;
        t.sketch = sketch;
        t.compact = layout;
        t.compact.words = .empty;
    }
}

/// Whether a census of every row the work list can give (`censusTopK`) is a small part of `max_memory`: eight bytes a
/// row, and a chunk per partition per worker being filled. Its entries name work items in 24 bits, and an item must
/// be a whole row group (as a grouped scan's are), so each row is counted once.
fn censusFits(metas: []const schema.FileMetaData, work: []const WorkItem, n_workers: usize, max_memory: usize) bool {
    if (work.len >= expr_agg.Census.max_items) return false;
    var rows: u64 = 0;
    for (work) |item| {
        if (item.agg_start != 0) return false;
        rows +|= @intCast(@max(0, metas[item.file].row_groups.items[item.rg].num_rows));
    }
    const open = n_workers * expr_agg.Census.N_PARTS * @sizeOf(expr_agg.Census.Chunk);
    return (rows *| 8) +| open <= max_memory / 4;
}

fn censusMask(cfg: SketchTopK) u64 {
    return (@as(u64, 1) << @min(cfg.census_fp_bits, 40)) - 1;
}

/// A fingerprint seen more than once, and the work items its rows were read in: `items[at..][0..n]` of the task that
/// found it.
const Repeat = struct { fp: u64, count: u64, task: u32, at: u32, n: u32 };

/// Folds the census partitions claimed from `cursor`: each partition's fingerprints from every worker, sorted, give
/// the fingerprints seen more than once. Every copy of a fingerprint lands in one partition, so its count there is
/// complete. A partition keeps at most `keep` of them, its most frequent, and records in `omitted` the largest count
/// it dropped: every fingerprint not kept has a count at most that, a bound the cut must clear. Stops once all tasks
/// together have found more than `cap`.
const CensusTask = struct {
    gpa: std.mem.Allocator,
    censuses: []expr_agg.Census,
    cursor: *std.atomic.Value(usize),
    found: *std.atomic.Value(usize),
    cap: usize,
    index: u32 = 0,
    keep: usize = std.math.maxInt(usize),
    /// The largest count of a repeated fingerprint some partition dropped; 1 when none was (a single row each).
    omitted: u64 = 1,
    out: std.ArrayList(Repeat) = .empty,
    items: std.ArrayList(u32) = .empty,
    sorted: std.ArrayList(u64) = .empty,
    table: std.ArrayList(u64) = .empty,
    /// One partition's repeats, before the most frequent `keep` are emitted.
    pending: std.ArrayList(Pending) = .empty,
    err: ?anyerror = null,

    /// A repeated fingerprint whose rows are `sorted.items[lo..hi]`.
    const Pending = struct { fp: u64, count: u64, lo: usize, hi: usize };

    fn run(t: *CensusTask) void {
        t.fold() catch |e| {
            t.err = e;
        };
    }

    fn fold(t: *CensusTask) !void {
        while (t.found.load(.monotonic) <= t.cap) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= expr_agg.Census.N_PARTS) return;
            // Every worker's entries for the partition, bucketed by the top byte straight from the chunks, so each
            // bucket is small enough to stay in cache.
            var starts: [257]usize = @splat(0);
            for (t.censuses) |*c| {
                for (c.full[p].items) |chunk| for (chunk) |e| {
                    starts[(e >> 56) + 1] += 1;
                };
                if (c.open[p]) |chunk| for (chunk[0..c.fill[p]]) |e| {
                    starts[(e >> 56) + 1] += 1;
                };
            }
            for (1..257) |b| starts[b] += starts[b - 1];
            try t.sorted.resize(t.gpa, starts[256]);
            const entries = t.sorted.items;
            var next = starts;
            for (t.censuses) |*c| {
                for (c.full[p].items) |chunk| scatter(entries, &next, chunk);
                if (c.open[p]) |chunk| scatter(entries, &next, chunk[0..c.fill[p]]);
                c.freeChunks(&c.full[p]);
                c.open[p] = null;
                c.fill[p] = 0;
            }
            var found: usize = 0;
            t.pending.clearRetainingCapacity();
            for (0..256) |b| found += try t.repeatsIn(p, entries, starts[b], starts[b + 1]);
            if (t.pending.items.len > 0) found += try t.emitMostFrequent(p);
            _ = t.found.fetchAdd(found, .monotonic);
        }
    }

    /// Emit the partition's `keep` most frequent pending repeats, recording the largest count left out.
    fn emitMostFrequent(t: *CensusTask, p: usize) !usize {
        const IB = expr_agg.Census.ITEM_BITS;
        const all = t.pending.items;
        // Most frequent first, ties by fingerprint so the choice does not depend on scheduling.
        std.mem.sortUnstable(Pending, all, {}, struct {
            fn more(_: void, a: Pending, b: Pending) bool {
                return a.count > b.count or (a.count == b.count and a.fp < b.fp);
            }
        }.more);
        const kept = all[0..@min(all.len, t.keep)];
        if (kept.len < all.len) t.omitted = @max(t.omitted, all[kept.len].count);
        for (kept) |r| {
            const at = t.items.items.len;
            for (t.sorted.items[r.lo..r.hi]) |e| try t.items.append(t.gpa, @intCast(e & ((1 << IB) - 1)));
            try t.emit(p, r.fp, r.count, at);
        }
        return kept.len;
    }

    fn scatter(entries: []u64, next: *[257]usize, chunk: []const u64) void {
        for (chunk) |e| {
            const b: usize = @intCast(e >> 56);
            entries[next[b]] = e;
            next[b] += 1;
        }
    }

    /// The fingerprints of `entries[lo..hi]` seen more than once. A table of its fingerprints finds them in one pass;
    /// a bucket with more than a few is sorted instead, so their rows come together and wait in `pending`. Returns
    /// how many it emitted itself.
    fn repeatsIn(t: *CensusTask, p: usize, entries: []u64, lo: usize, hi: usize) !usize {
        const IB = expr_agg.Census.ITEM_BITS;
        const bucket = entries[lo..hi];
        if (bucket.len < 2) return 0;
        const size = std.math.ceilPowerOfTwoAssert(usize, bucket.len * 2);
        try t.table.resize(t.gpa, size);
        const table = t.table.items;
        @memset(table, 0);
        const used: u64 = 1 << 63;
        const seen_twice: u64 = 1 << 62;
        var reps: [8]u64 = undefined;
        var n_reps: usize = 0;
        for (bucket) |e| {
            const fp = e >> IB;
            var i: usize = @intCast(fp & (size - 1));
            while (true) : (i = (i + 1) & (size - 1)) {
                const slot = table[i];
                if (slot == 0) {
                    table[i] = fp | used;
                    break;
                }
                if (slot & ~seen_twice != fp | used) continue;
                if (slot & seen_twice == 0) {
                    if (n_reps == reps.len) return t.repeatsSorted(entries, lo, hi);
                    table[i] |= seen_twice;
                    reps[n_reps] = fp;
                    n_reps += 1;
                }
                break;
            }
        }
        for (reps[0..n_reps]) |fp| {
            const at = t.items.items.len;
            var count: u64 = 0;
            for (bucket) |e| if (e >> IB == fp) {
                count += 1;
                try t.items.append(t.gpa, @intCast(e & ((1 << IB) - 1)));
            };
            try t.emit(p, fp, count, at);
        }
        return n_reps;
    }

    /// The repeats of a bucket with many, sorted so each one's rows are a run; they wait in `pending` for the
    /// partition's most frequent to be chosen.
    fn repeatsSorted(t: *CensusTask, entries: []u64, lo: usize, hi: usize) !usize {
        const IB = expr_agg.Census.ITEM_BITS;
        std.mem.sortUnstable(u64, entries[lo..hi], {}, std.sort.asc(u64));
        var i = lo;
        while (i < hi) {
            const fp = entries[i] >> IB;
            var j = i + 1;
            while (j < hi and entries[j] >> IB == fp) j += 1;
            if (j - i > 1) try t.pending.append(t.gpa, .{ .fp = fp, .count = j - i, .lo = i, .hi = j });
            i = j;
        }
        return 0;
    }

    /// Records a repeated fingerprint whose rows' items were appended from `at`, kept once each, in order.
    fn emit(t: *CensusTask, p: usize, fp: u64, count: u64, at: usize) !void {
        const its = t.items.items[at..];
        std.mem.sortUnstable(u32, its, {}, std.sort.asc(u32));
        var n: usize = 0;
        for (its) |it| if (n == 0 or its[n - 1] != it) {
            its[n] = it;
            n += 1;
        };
        t.items.shrinkRetainingCapacity(at + n);
        try t.out.append(t.gpa, .{
            .fp = (@as(u64, p) << 40) | fp,
            .count = count,
            .task = t.index,
            .at = @intCast(at),
            .n = @intCast(n),
        });
    }
};

/// A census's repeated fingerprints from every partition (`CensusTask`), most frequent first, ties by fingerprint, and
/// the largest count a partition dropped (1 when none was). The tasks own the item lists the repeats name.
const CensusRepeats = struct {
    tasks: []CensusTask,
    repeats: std.ArrayList(Repeat) = .empty,
    omitted: u64 = 1,

    fn deinit(self: *CensusRepeats, gpa: std.mem.Allocator) void {
        self.repeats.deinit(gpa);
        for (self.tasks) |*t| {
            t.out.deinit(gpa);
            t.items.deinit(gpa);
            t.sorted.deinit(gpa);
            t.table.deinit(gpa);
            t.pending.deinit(gpa);
        }
    }
};

/// Folds every census partition, each keeping its `keep` most frequent repeats. Null when more than `cap` repeat.
fn reduceCensus(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    n_workers: usize,
    censuses: []expr_agg.Census,
    keep: usize,
    cap: usize,
) !?CensusRepeats {
    var cursor_p: std.atomic.Value(usize) = .init(0);
    var found: std.atomic.Value(usize) = .init(0);
    var out: CensusRepeats = .{ .tasks = try arena.alloc(CensusTask, @min(n_workers, expr_agg.Census.N_PARTS)) };
    for (out.tasks, 0..) |*t, ti| t.* = .{
        .gpa = gpa,
        .censuses = censuses,
        .cursor = &cursor_p,
        .found = &found,
        .cap = cap,
        .index = @intCast(ti),
        // Dense repeats are reduced to the most frequent per partition, never all emitted (`CensusTask`).
        .keep = keep,
    };
    errdefer out.deinit(gpa);
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (out.tasks.len == 1 or !spawn_util.runTree(CensusTask, out.tasks, config, CensusTask.run)) out.tasks[0].run();
    for (out.tasks) |t| if (t.err) |e| return e;
    if (found.load(.monotonic) > cap) {
        out.deinit(gpa);
        return null;
    }
    try out.repeats.ensureTotalCapacity(gpa, found.load(.monotonic));
    for (out.tasks) |t| out.repeats.appendSliceAssumeCapacity(t.out.items);
    std.mem.sortUnstable(Repeat, out.repeats.items, {}, struct {
        fn more(_: void, a: Repeat, b: Repeat) bool {
            return a.count > b.count or (a.count == b.count and a.fp < b.fp);
        }
    }.more);
    for (out.tasks) |task| out.omitted = @max(out.omitted, task.omitted);
    return out;
}

/// The sketch's fallback when its cells are too even to bound anything, as with near-unique keys. A census of every
/// row's key fingerprint (`expr_agg.Census`) bounds each key's total almost exactly. The admit pass groups each key
/// whose fingerprint totals at least `t`, the k-th largest fingerprint total. A key left out totals below `t`, so
/// when the k-th largest grouped total reaches `t`, every group the cut keeps, ties at the cut included, is grouped.
///
/// When `t` is 2 and the cut orders by a plain count(*) alone, the admit pass also groups every key asked for in a prefix of
/// the input holding at least k rows of keys seen once. Every key left out is then a single row asked for later
/// than those, and a tie at the cut keeps the first to appear, so the cut is certified at a tie of 1 too.
/// Grouping a key from the prefix is exact, as it has one row. Returns false, with the tables empty, when the census
/// cannot certify the cut.
fn censusTopK(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    sketches: []expr_agg.CountSketch,
    censuses: []expr_agg.Census,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    a_idx: usize,
    cfg: SketchTopK,
    count_ns: i128,
) !bool {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const work = workers[0].work;
    const item_rows = censuses[0].item_rows;
    const t1 = nowMonoNs();
    const count_ms = @divTrunc(count_ns, 1_000_000);

    const cap = @max(k *| cfg.admit_per_row *| 64, 1 << 20);
    var reduced = (try reduceCensus(gpa, arena, workers.len, censuses, @max(k *| cfg.admit_per_row, 1), cap)) orelse {
        if (cfg.trace) std.debug.print("census: count_ms={d} repeated>{d}, too many\n", .{ count_ms, cap });
        return false;
    };
    defer reduced.deinit(gpa);
    if (cfg.trace) std.debug.print("census: reduce_ms={d}\n", .{@divTrunc(nowMonoNs() - t1, 1_000_000)});
    const tasks = reduced.tasks;
    const items = reduced.repeats.items;
    const t: u64 = if (items.len >= k) items[k - 1].count else 2;
    // A fingerprint a partition dropped totals at most `omitted`, and a key at most its fingerprint (a collision only
    // adds rows to it). Only with every dropped one below `t` does admitting the fingerprints totalling `t` or more
    // admit every key that can reach the cut; the exact k-th total must still reach `t` to certify it.
    const omitted = reduced.omitted;
    if (omitted >= t) {
        if (cfg.trace) std.debug.print("census: count_ms={d} repeated={d} t={d} omitted={d}, cannot cut\n", .{
            count_ms, items.len, t, omitted,
        });
        return false;
    }
    // The census counts rows, so only a plain count(*) gives each early row's key exactly 1; a count of a column or
    // under FILTER can give it 0, and must certify by the totals.
    const call = calls[a_idx];
    const prefix = t == 2 and order.keys.len == 1 and call.arg == null and call.where == null;
    var n_admit: usize = 0;
    var admit_rows: u64 = 0;
    while (n_admit < items.len and items[n_admit].count >= t) : (n_admit += 1) admit_rows += items[n_admit].count;
    const admit_cap = @max(k *| cfg.admit_per_row *| 16, 1 << 16);
    if (n_admit > admit_cap or (items.len < k and !prefix)) {
        if (cfg.trace) std.debug.print("census: count_ms={d} repeated={d} t={d} admit={d}, cannot cut\n", .{
            count_ms, items.len, t, n_admit,
        });
        return false;
    }

    var set: expr_agg.FingerprintSet = .{ .filter = undefined, .map = .empty };
    defer set.map.deinit(gpa);
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    defer gpa.free(filter);
    @memset(filter, 0);
    try set.map.ensureTotalCapacity(gpa, @intCast(n_admit));
    for (items[0..n_admit]) |r| {
        set.map.putAssumeCapacity(r.fp, {});
        const b = r.fp & ((1 << expr_agg.FingerprintSet.filter_bits) - 1);
        filter[b / 64] |= @as(u64, 1) << @intCast(b % 64);
    }
    set.filter = filter;

    // The prefix ends where the rows given to the census pass `need`. In a row group whose rows all reached the
    // census, at that row; otherwise at the end of it. `k` rows beyond every admitted key's rows always hold k keys
    // seen once, but heavy keys can make that most of the input. So a fixed-width table first tries a prefix sized by
    // the admitted keys' share of the rows, certified only when the admit pass grouped k keys it did not admit.
    var total_rows: u64 = 0;
    for (item_rows) |n| total_rows += n;
    const sure_need = k + admit_rows;
    const short_need: u64 = if (total_rows > admit_rows and tables.len > 0 and tables[0].fixed_stride != 0)
        @min(sure_need, (2 *| @as(u64, k) *| total_rows) / (total_rows - admit_rows) +| 1)
    else
        sure_need;
    const counted = try arena.dupe(Worker, workers);
    defer for (workers) |*w| {
        w.skip = null;
    };
    const skip = try arena.alloc(bool, work.len);
    var need: u64 = if (prefix) short_need else 0;
    var short = false;
    const certified = while (true) {
        var lo: u64 = 0;
        var row: u64 = 0;
        if (prefix) {
            lo = std.math.maxInt(u32);
            var seen: u64 = 0;
            for (item_rows, 0..) |n, i| {
                if (seen + n < need) {
                    seen += n;
                    continue;
                }
                const rg_rows = workers[0].metas[work[i].file].row_groups.items[work[i].rg].num_rows;
                lo = i;
                row = if (rg_rows >= 0 and n == @as(u64, @intCast(rg_rows))) need - seen else std.math.maxInt(u64);
                break;
            }
        }
        for (censuses) |*c| {
            c.admitted = &set;
            c.prefix_item = lo;
            c.prefix_row = row;
        }
        for (sketches, censuses) |*s, *c| s.census = c;
        // The admit pass reads only the prefix and the items holding an admitted key's rows. The count pass's row
        // counts stand.
        for (skip, 0..) |*sk, i| sk.* = i > lo or (i == lo and row == 0);
        for (items[0..n_admit]) |r| for (tasks[r.task].items.items[r.at..][0..r.n]) |item| {
            skip[item] = false;
        };
        for (workers) |*w| w.skip = skip;
        try runPhase(workers, cursor);
        restoreCounts(workers, counted);
        if (!prefix or need >= sure_need) {
            const kth = try kthTotal(arena, tables, calls.len, a_idx, k);
            const sure = cfg.certify and (prefix or kth >= t);
            if (cfg.trace) std.debug.print("census: count_ms={d} repeated={d} t={d} admit={d} prefix={} need={d} admit_ms={d} kth_total={d} certified={}\n", .{
                count_ms, items.len, t, n_admit, prefix, need, @divTrunc(nowMonoNs() - t1, 1_000_000), kth, sure,
            });
            break sure;
        }
        // A key grouped though not admitted had its one row in the prefix: one group in one table, earlier than any
        // key left out. k of them, with the admitted keys counted exactly, certify the cut.
        var fresh: usize = 0;
        for (tables) |*tb| fresh += tb.unadmittedGroups(&censuses[0]);
        if (cfg.trace) std.debug.print("census: short prefix need={d} fresh={d} admit_ms={d} certified={}\n", .{
            need, fresh, @divTrunc(nowMonoNs() - t1, 1_000_000), cfg.certify and fresh >= k,
        });
        if (fresh >= k) {
            short = true;
            break cfg.certify;
        }
        clearTables(gpa, tables);
        need = sure_need;
    };
    if (certified) {
        if (cfg.outcome) |o| o.* = if (short) .census_short else .census;
        return true;
    }
    if (cfg.outcome) |o| o.* = .fell_back;
    for (workers) |*w| w.skip = null;
    clearTables(gpa, tables);
    return false;
}

/// A count(DISTINCT) cut of `k = offset + limit` groups from the census of a count pass (`Census.distinct`). A key has
/// no more distinct values than rows, nor more rows than its fingerprint's total, so admitting the most frequent
/// fingerprints with every row of theirs leaves out only keys whose distinct count is at most `bound`, the largest total
/// not admitted. The admit pass counts the admitted keys exactly, and when the k-th largest distinct count exceeds
/// `bound` every key the cut keeps was admitted, ties at the cut included: strictly, as a key left out may tie at
/// `bound` and appear earlier. Short of that, four times the fingerprints are admitted once, their keys grouped beside
/// the first ones'. Admitted keys holding over a quarter of the rows, or a bound never beaten, empty the tables and
/// return false.
fn censusDistinctTopK(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    sketches: []expr_agg.CountSketch,
    censuses: []expr_agg.Census,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    a_idx: usize,
    cfg: SketchTopK,
) !bool {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const t1 = nowMonoNs();
    const admit_n = @max(k *| cfg.admit_per_row, 1);
    const wide_n = admit_n *| 4;
    var reduced = (try reduceCensus(gpa, arena, workers.len, censuses, wide_n, @max(admit_n *| 64, 1 << 20))) orelse {
        if (cfg.trace) std.debug.print("census distinct: too many repeats\n", .{});
        return false;
    };
    defer reduced.deinit(gpa);
    const items = reduced.repeats.items;
    var total_rows: u64 = 0;
    for (censuses[0].item_rows) |n| total_rows += n;

    const work = workers[0].work;
    const skip = try arena.alloc(bool, work.len);
    const counted = try arena.dupe(Worker, workers);
    defer for (workers) |*w| {
        w.skip = null;
    };
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    defer gpa.free(filter);
    var set: expr_agg.FingerprintSet = .{ .filter = filter, .map = .empty };
    defer set.map.deinit(gpa);
    var lo: usize = 0;
    var hi = @min(items.len, admit_n);
    var admit_rows: u64 = 0;
    const certified = while (true) {
        const bound = @max(reduced.omitted, if (hi < items.len) items[hi].count else 1);
        for (items[lo..hi]) |r| admit_rows += r.count;
        if (hi < k or admit_rows *| 4 > total_rows) break false;
        // Only the fingerprints new to this pass: keys admitted before keep their groups and are not read again.
        @memset(filter, 0);
        set.map.clearRetainingCapacity();
        try set.map.ensureTotalCapacity(gpa, @intCast(hi - lo));
        @memset(skip, true);
        for (items[lo..hi]) |r| {
            set.map.putAssumeCapacity(r.fp, {});
            const b = r.fp & ((1 << expr_agg.FingerprintSet.filter_bits) - 1);
            filter[b / 64] |= @as(u64, 1) << @intCast(b % 64);
            for (reduced.tasks[r.task].items.items[r.at..][0..r.n]) |item| skip[item] = false;
        }
        for (sketches, censuses) |*s, *c| {
            c.admitted = &set;
            s.census = c;
        }
        for (workers) |*w| w.skip = skip;
        // The trial's tables and the threshold's copies of their sets are memory the exact scan never holds: a
        // budget they exceed is the certificate failing, never the query.
        runPhase(workers, cursor) catch |e| {
            if (e != error.ExceededMemoryBudget) return e;
            try clearWorkerErrors(workers, error.ExceededMemoryBudget);
            if (cfg.trace) std.debug.print("census distinct: trial over budget, exact scan\n", .{});
            break false;
        };
        restoreCounts(workers, counted);
        const tau = kthDistinct(gpa, arena, tables, calls.len, a_idx, k) catch |e| {
            if (e != error.ExceededMemoryBudget) return e;
            if (cfg.trace) std.debug.print("census distinct: trial over budget, exact scan\n", .{});
            break false;
        };
        if (cfg.trace) std.debug.print("census distinct: repeated={d} omitted={d} admit={d} admit_rows={d} bound={d} kth_distinct={d} admit_ms={d}\n", .{
            items.len, reduced.omitted, hi, admit_rows, bound, tau, @divTrunc(nowMonoNs() - t1, 1_000_000),
        });
        if (cfg.certify and tau > bound) break true;
        if (lo > 0 or hi == items.len) break false;
        lo = hi;
        hi = @min(items.len, wide_n);
    };
    for (sketches) |*s| s.census = null;
    if (certified) {
        if (cfg.outcome) |o| o.* = .census;
        return true;
    }
    if (cfg.outcome) |o| o.* = .fell_back;
    for (workers) |*w| w.skip = null;
    clearTables(gpa, tables);
    return false;
}

/// The k-th largest count(DISTINCT) of call `a_idx` over the keys of `tables`, each key's values merged across the
/// tables holding it; 0 with fewer than k keys. A merged copy is charged like the set it copies, and freed.
fn kthDistinct(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    stride: usize,
    a_idx: usize,
    k: usize,
) !u64 {
    const Seen = struct { table: usize, group: usize, merged: ?expr_agg.DistinctAcc = null };
    var keys: std.StringArrayHashMapUnmanaged(Seen) = .empty;
    defer for (keys.values()) |*e| if (e.merged) |*m| m.deinit(gpa);
    for (tables, 0..) |*t, ti| for (0..t.groupCount()) |g| {
        const gop = try keys.getOrPut(arena, t.keyAt(g));
        if (!gop.found_existing) {
            gop.value_ptr.* = .{ .table = ti, .group = g };
            continue;
        }
        const e = gop.value_ptr;
        if (e.merged == null) {
            e.merged = .{};
            try e.merged.?.mergeFrom(tables[e.table].accumulators.items[e.group * stride + a_idx].distinct, gpa);
        }
        try e.merged.?.mergeFrom(t.readAcc(g, a_idx, stride).distinct, gpa);
    };
    if (keys.count() < k) return 0;
    const counts = try arena.alloc(u64, keys.count());
    for (keys.values(), counts) |e, *c| c.* = if (e.merged) |m|
        m.count()
    else
        tables[e.table].accumulators.items[e.group * stride + a_idx].distinct.count();
    std.mem.sortUnstable(u64, counts, {}, std.sort.desc(u64));
    return counts[k - 1];
}

/// A LIMIT with no ORDER BY over a grouped scan of whole inputs (`firstKeysCut`).
fn firstCutEligible(args: MultiAggArgs, calls: []const expr_agg.AggCall, use_blocks: bool, n_items: usize, n_tables: usize) bool {
    const order = args.result_order;
    // Rows after the prefix are read only for the keys it found: a failing call on another key's row must not vanish.
    if (!callsTotal(calls)) return false;
    const limit = order.limit orelse return false;
    if (use_blocks or n_tables == 0 or order.keys.len != 0 or order.offset +| limit == 0) return false;
    if (n_items == 0 or n_items >= expr_agg.Census.max_items) return false;
    for (args.inputs) |in| if (in.gate != null) return false;
    return true;
}

/// A LIMIT with no ORDER BY returns the first `k = offset + limit` groups to appear, so only those need their
/// aggregates. A discovery pass groups the first work items, twice as many each time, until k keys have appeared; the
/// k-th key's first row ends a prefix, which holds exactly the first k keys. The admit pass then groups every row of
/// the prefix, and every later row whose key's fingerprint is one of the k (`expr_agg.Census`, admitting). A key that
/// shares a fingerprint with one of them gets a group too, but it first appears after the prefix, so the cut never
/// keeps it. When the discovery has read everything, its tables are the answer.
fn firstKeysCut(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    order: expr_ast.ResultOrder,
    cfg: SketchTopK,
) !void {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const work = workers[0].work;
    const saved = try arena.dupe(Worker, workers);
    const skip = try arena.alloc(bool, work.len);
    defer for (workers) |*w| {
        w.skip = null;
    };
    const First = struct { first: u64, table: usize, g: u32 };
    var firsts: std.ArrayList(First) = .empty;
    defer firsts.deinit(gpa);
    var m: usize = 1;
    while (true) : (m *|= 2) {
        for (skip, 0..) |*sk, i| sk.* = i >= m;
        for (workers) |*w| w.skip = if (m >= work.len) null else skip;
        restoreCounts(workers, saved);
        try runPhase(workers, cursor);
        if (m >= work.len) {
            if (cfg.trace) std.debug.print("first: discovery read everything\n", .{});
            return;
        }
        // Each key's first appearance over every table.
        var seen: std.StringArrayHashMapUnmanaged(First) = .empty;
        defer seen.deinit(gpa);
        for (tables, 0..) |*t, ti| for (0..t.groupCount()) |g| {
            const e = try seen.getOrPut(gpa, t.keyAt(g));
            const f: First = .{ .first = t.first.items[g], .table = ti, .g = @intCast(g) };
            if (!e.found_existing or f.first < e.value_ptr.first) e.value_ptr.* = f;
        };
        if (seen.count() < k) {
            clearTables(gpa, tables);
            continue;
        }
        firsts.clearRetainingCapacity();
        try firsts.appendSlice(gpa, seen.values());
        break;
    }
    std.mem.sortUnstable(First, firsts.items, {}, struct {
        fn earlier(_: void, a: First, b: First) bool {
            return a.first < b.first;
        }
    }.earlier);
    const last = firsts.items[k - 1].first;

    const censuses = try arena.alloc(expr_agg.Census, workers.len);
    for (censuses) |*c| c.* = .{ .gpa = gpa, .item_rows = &.{}, .prefix_item = last >> 32, .prefix_row = (last & 0xffff_ffff) + 1 };
    defer for (censuses) |*c| c.deinit();
    var set: expr_agg.FingerprintSet = .{ .filter = undefined, .map = .empty };
    defer set.map.deinit(gpa);
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    defer gpa.free(filter);
    @memset(filter, 0);
    try set.map.ensureTotalCapacity(gpa, @intCast(k));
    for (firsts.items[0..k]) |f| {
        const fp = censuses[0].fingerprint(tables[f.table].routeHash(f.g));
        set.map.putAssumeCapacity(fp, {});
        const b = fp & ((1 << expr_agg.FingerprintSet.filter_bits) - 1);
        filter[b / 64] |= @as(u64, 1) << @intCast(b % 64);
    }
    set.filter = filter;
    for (censuses) |*c| c.admitted = &set;

    var no_cells = [1]u32{0};
    const sketches = try arena.alloc(expr_agg.CountSketch, workers.len);
    for (sketches, censuses) |*s, *c| s.* = .{ .cells = &no_cells, .shift = 63, .census = c };
    clearTables(gpa, tables);
    for (tables, sketches) |*t, *s| t.sketch = s;
    defer for (tables) |*t| {
        t.sketch = null;
    };
    for (workers) |*w| w.skip = null;
    restoreCounts(workers, saved);
    const t0 = nowMonoNs();
    try runPhase(workers, cursor);
    if (cfg.trace) std.debug.print("first: discovery_items={d} admit_ms={d}\n", .{ m, @divTrunc(nowMonoNs() - t0, 1_000_000) });
}

/// Puts back the row and row-group counts a discarded pass added.
fn restoreCounts(workers: []Worker, saved: []const Worker) void {
    for (workers, saved) |*w, s| {
        w.rows_in = s.rows_in;
        w.rows_kept = s.rows_kept;
        w.rgs_in = s.rgs_in;
        w.rgs_pruned = s.rgs_pruned;
        w.rgs_full_match = s.rgs_full_match;
    }
}

/// One pass of `workers` over every work item, from a fresh cursor.
/// After a phase that failed with `expected`: clears every worker's `expected`, then returns the first other error a
/// worker met, which the exact path would meet too.
fn clearWorkerErrors(workers: []Worker, expected: anyerror) !void {
    for (workers) |*w| if (w.err) |we| if (we == expected) {
        w.err = null;
    };
    for (workers) |w| if (w.err) |other| return other;
}

fn runPhase(workers: []Worker, cursor: *std.atomic.Value(usize)) !void {
    cursor.store(0, .monotonic);
    runWorkers(workers);
    for (workers) |w| if (w.err) |e| return e;
}

/// The one count(DISTINCT) call `foldDistinctPairs` can answer: a plain numeric column, the only count(DISTINCT) of
/// the query, grouped by a fixed-width key of at most 16 bytes. Null otherwise.
fn distinctPairsCall(calls: []const expr_agg.AggCall, fixed_stride: u8) ?usize {
    if (fixed_stride == 0 or fixed_stride > 16) return null;
    var found: ?usize = null;
    for (calls, 0..) |call, i| {
        if (call.func != .count_distinct) continue;
        if (found != null) return null;
        const arg = call.arg orelse return null;
        if (arg != .col_ref) return null;
        switch (arg.col_ref.expr_type) {
            .i64, .f64 => {},
            else => return null,
        }
        found = i;
    }
    return found;
}

/// Whether pairs beat per-group sets: only for a group count the dictionaries bound to at most 2^20, each group
/// taking 64 rows or more. Near-unique keys keep their few values inline in the sets, cheaper than sorting a pair a
/// row and looking every group up afterwards; there, and wherever nothing bounds the groups, the sets stay.
fn pairsPay(stat: ?NdvStat) bool {
    const st = stat orelse return false;
    const est = st.estimate orelse return false;
    return est > 0 and est <= 1 << 20 and st.rows / est >= 64;
}

/// Whether a worker's sets, after taking `rows` values (`DistinctPairs.seen`), are the kind pairs beat: groups of 8
/// rows or more on average, nearly every row a value its group has not seen (nine in ten). Near-unique keys keep a few
/// values inline in each set, and input whose values repeat (skew, or a few huge groups) keeps small or cache-resident
/// sets; the sets win there. Rows a group gathers only grow, so too few of them is `.wait`: small row groups, or files
/// each holding most of the keys, reach eight a group only after several items. The share of new values only falls,
/// so too few is final. The row test runs first: it is free, and walking every set at each check cost near-unique
/// keys ~4%.
fn pairsPayRuntime(table: *const expr_agg.GroupTable, agg_idx: usize, rows: u64) enum { pay, wait, never } {
    const groups = table.groupCount();
    if (groups == 0 or rows < 8 * @as(u64, groups)) return .wait;
    const n_calls = table.accumulators.items.len / groups;
    var values: u64 = 0;
    for (0..groups) |g| {
        const acc = table.accumulators.items[g * n_calls + agg_idx];
        if (acc == .distinct) values += acc.distinct.count();
    }
    return if (10 * values >= 9 * rows) .pay else .never;
}

/// Per partition: every worker's pairs sorted, then each key's distinct values counted. Sorted by key for lookup.
const KeyDistinct = struct { k0: u64, k1: u64, count: u64, claimed: std.atomic.Value(bool) = .init(false) };

const PairFoldTask = struct {
    gpa: std.mem.Allocator,
    budget: *expr_agg.SharedBudget,
    sinks: []expr_agg.DistinctPairs,
    out: [][]KeyDistinct,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    fn run(t: *PairFoldTask) void {
        while (true) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= expr_agg.DistinctPairs.N) return;
            t.fold(p) catch |e| {
                t.err = e;
                return;
            };
        }
    }

    fn fold(t: *PairFoldTask, p: usize) !void {
        const Pair = expr_agg.DistinctPairs.Pair;
        var n: usize = 0;
        for (t.sinks) |*s| n += s.parts[p].items.len;
        const bytes = n * (@sizeOf(Pair) + @sizeOf(KeyDistinct));
        const got = t.budget.drawExact(bytes) orelse return error.ExceededMemoryBudget;
        defer t.budget.release(got);
        const all = try t.gpa.alloc(Pair, n);
        defer t.gpa.free(all);
        var at: usize = 0;
        for (t.sinks) |*s| {
            @memcpy(all[at..][0..s.parts[p].items.len], s.parts[p].items);
            at += s.parts[p].items.len;
            s.parts[p].clearAndFree(t.gpa);
        }
        std.mem.sortUnstable(Pair, all, {}, struct {
            fn lt(_: void, a: Pair, b: Pair) bool {
                if (a.k0 != b.k0) return a.k0 < b.k0;
                if (a.k1 != b.k1) return a.k1 < b.k1;
                return a.x < b.x;
            }
        }.lt);
        var keys: std.ArrayList(KeyDistinct) = .empty;
        errdefer keys.deinit(t.gpa);
        var i: usize = 0;
        while (i < all.len) {
            var count: u64 = 1;
            var j = i + 1;
            while (j < all.len and all[j].k0 == all[i].k0 and all[j].k1 == all[i].k1) : (j += 1) {
                if (all[j].x != all[j - 1].x) count += 1;
            }
            try keys.append(t.gpa, .{ .k0 = all[i].k0, .k1 = all[i].k1, .count = count });
            i = j;
        }
        t.out[p] = try keys.toOwnedSlice(t.gpa);
    }
};

const PairApplyTask = struct {
    tables: []expr_agg.GroupTable,
    keyed: []const []KeyDistinct,
    agg_idx: usize,
    agg_len: usize,
    gpa: std.mem.Allocator,
    cursor: *std.atomic.Value(usize),

    fn run(t: *PairApplyTask) void {
        const Pairs = expr_agg.DistinctPairs;
        while (true) {
            const ti = t.cursor.fetchAdd(1, .monotonic);
            if (ti >= t.tables.len) return;
            const table = &t.tables[ti];
            for (0..table.groupCount()) |g| {
                const probe = Pairs.pairOf(table.keyAt(g), 0);
                const acc = &table.accumulators.items[g * t.agg_len + t.agg_idx];
                var n: u64 = 0;
                // Only the first table to claim a key carries its count, so the usual merge's sum is exact.
                if (findKeyDistinct(t.keyed[Pairs.partOf(probe)], probe.k0, probe.k1)) |e| {
                    if (e.claimed.cmpxchgStrong(false, true, .monotonic, .monotonic) == null) n = e.count;
                }
                if (acc.* == .distinct) acc.distinct.deinit(t.gpa);
                acc.* = .{ .count = n };
            }
        }
    }
};

fn findKeyDistinct(list: []KeyDistinct, k0: u64, k1: u64) ?*KeyDistinct {
    var lo: usize = 0;
    var hi: usize = list.len;
    while (lo < hi) {
        const mid = lo + (hi - lo) / 2;
        const m = &list[mid];
        if (m.k0 == k0 and m.k1 == k1) return m;
        if (m.k0 < k0 or (m.k0 == k0 and m.k1 < k1)) lo = mid + 1 else hi = mid;
    }
    return null;
}

/// Turn every worker's pairs into exact count(DISTINCT) states on the worker tables: each key's count on the first
/// table to claim it, 0 on the others. NULL values sent no pair, so a key seen only with them counts 0.
fn foldDistinctPairs(
    gpa: std.mem.Allocator,
    budget: *expr_agg.SharedBudget,
    sinks: []expr_agg.DistinctPairs,
    tables: []expr_agg.GroupTable,
    agg_idx: usize,
    agg_len: usize,
    threads: usize,
) !void {
    const N = expr_agg.DistinctPairs.N;
    const keyed = try gpa.alloc([]KeyDistinct, N);
    @memset(keyed, &.{});
    defer {
        for (keyed) |k| gpa.free(k);
        gpa.free(keyed);
    }
    var cursor: std.atomic.Value(usize) = .init(0);
    const n_tasks = @max(1, @min(threads, N, 64));
    var folds: [64]PairFoldTask = undefined;
    for (folds[0..n_tasks]) |*f| f.* = .{
        .gpa = gpa,
        .budget = budget,
        .sinks = sinks,
        .out = keyed,
        .cursor = &cursor,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (n_tasks == 1 or !spawn_util.runTree(PairFoldTask, folds[0..n_tasks], config, PairFoldTask.run)) folds[0].run();
    for (folds[0..n_tasks]) |f| if (f.err) |e| return e;
    for (sinks) |*s| s.deinit();
    // Tasks whose spawn failed leave their share to the ones that started, through the shared cursor.
    var table_cursor: std.atomic.Value(usize) = .init(0);
    const n_apply = @max(1, @min(threads, tables.len, 64));
    var applies: [64]PairApplyTask = undefined;
    for (applies[0..n_apply]) |*a| a.* = .{
        .tables = tables,
        .keyed = keyed,
        .agg_idx = agg_idx,
        .agg_len = agg_len,
        .gpa = gpa,
        .cursor = &table_cursor,
    };
    if (n_apply == 1 or !spawn_util.runTree(PairApplyTask, applies[0..n_apply], config, PairApplyTask.run))
        applies[0].run();
}

/// The k-th largest total of the COUNT at `a_idx` over every key of `tables`, 0 with fewer than `k` keys.
fn kthTotal(arena: std.mem.Allocator, tables: []const expr_agg.GroupTable, stride: usize, a_idx: usize, k: usize) !u64 {
    var totals: std.StringArrayHashMapUnmanaged(u64) = .empty;
    for (tables) |*t| for (0..t.groupCount()) |g| {
        const e = try totals.getOrPutValue(arena, t.keyAt(g), 0);
        e.value_ptr.* +|= t.readAcc(g, a_idx, stride).count;
    };
    const vals = totals.values();
    if (vals.len < k) return 0;
    std.mem.sortUnstable(u64, vals, {}, std.sort.desc(u64));
    return vals[k - 1];
}

/// The filter column whose statistics alone exclude `rg`, as leaf index + 1 (0 when only a combination does), for the
/// profile's "skipped by" counts. Profiling only: it re-runs the pruning check per conjunct.
fn skipReason(f: filter_ast.Filter, rg: *const schema.RowGroup, meta: *const schema.FileMetaData) usize {
    switch (f) {
        .and_filter => |c| {
            for (c.children) |child| {
                const r = skipReason(child, rg, meta);
                if (r != 0) return r;
            }
            return 0;
        },
        .or_filter => return 0,
        inline else => |leaf| {
            if (filter_prune.pruneRowGroup(rg, f, meta) != .skip) return 0;
            return leaf.col_idx + 1;
        },
    }
}

/// Thrift footer length from a file's trailer, 0 when it has none.
fn footerLen(bytes: []const u8) usize {
    if (bytes.len < 8) return 0;
    return std.mem.readInt(u32, bytes[bytes.len - 8 ..][0..4], .little);
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
    // gives way to that cut, whose refusal is final. Each step only narrows (`narrowed`), so at most seven scans run,
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
            if (e != error.ExceededMemoryBudget) return e;
            if (args.ledger != null and merge_memory.refused.load(.monotonic)) taken.insert(.merge_memory);
            // What a refused scan freed goes back to the OS too, or the next scan's peak stands on its leftovers.
            core_memory.trimHeap();
            may_fit = may_fit or taken.contains(.one_table_may_fit);
            var next = narrowed(args, attempt, taken, &chain, may_fit) orelse return e;
            if (attempt.inputs.ptr == args.inputs.ptr) {
                // The rerun reads the mappings from the start: an exact restart, which reuses none of the reader's
                // bytes and so must not hold its window beside its own.
                if (args.start_reads) |s| if (s.abandon) |abandon| abandon(s.ctx);
                next.start_reads = null;
                next.dict_probe = null;
                const inputs = try arena.dupe(Input, args.inputs);
                for (inputs) |*in| if (in.plan_bytes) |b| {
                    in.bytes = b;
                    in.gate = null;
                    in.dict_wait = null;
                };
                next.inputs = inputs;
            }
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
const Chain = struct {
    /// The forced partitions (`MultiAggArgs.partagg`), which append to runs; the cost rules' shared partitions
    /// (`FastPath.partitioned`) are not those, and leave it.
    parts_tried: bool,
    /// The one-worker scan has run again on a small decode budget (`narrowed`).
    lean_tried: bool = false,
};

/// Fund a decoder floor before workers start. Whole row groups may borrow headroom; slices retain protected room
/// when that admission refuses. Cancelling the private sample returns the floors beside still-charged live values.
const PrivateDecode = struct {
    held: usize = 0,
    workers: []core_memory.SharedBudget = &.{},
    samples: []core_memory.SharedBudget = &.{},

    fn init(
        a: std.mem.Allocator,
        parent: *core_memory.SharedBudget,
        n: usize,
        limit: ?usize,
        dictionary_peak: u64,
    ) !PrivateDecode {
        const share = @min(limit orelse parent.limit / 2, parent.limit / 4) / n;
        // A floor consumed by a dictionary strands the sample on tiny slices even when it later cancels.
        // Decline before reserving runs or reading rows; leave room for a slice beside the fixed dictionary.
        if (share < @max(huge_pages.huge_page_size, dictionary_peak +| parent.block)) return .{};
        const held = parent.drawExact(share * n) orelse return .{};
        errdefer parent.release(held);
        const workers = try a.alloc(core_memory.SharedBudget, n);
        for (workers) |*w| w.* = .{ .limit = parent.limit, .parent = parent, .funded = share };
        return .{ .held = held, .workers = workers };
    }

    fn fundSamples(self: *PrivateDecode, a: std.mem.Allocator, part: *partagg.PartAgg, width: u64) !bool {
        // Decode loans must not strand every worker before its first flush. Transfer part of the prepaid run
        // ceiling into bounded local-table floors; it is the same charged credit, not another reservation.
        const share = (width +| 64) *| part.cap *| 4 +| part.budget.block;
        const total = share *| self.workers.len;
        const credit = part.run_credit.load(.monotonic);
        if (total > credit) return false;
        const samples = try a.alloc(core_memory.SharedBudget, self.workers.len);
        part.run_credit.store(credit - @as(usize, @intCast(total)), .monotonic);
        for (samples) |*b| b.* = .{ .limit = part.budget.limit, .parent = part.budget, .funded = @intCast(share) };
        self.samples = samples;
        part.sample_floors = samples;
        return true;
    }

    fn release(self: *PrivateDecode) void {
        for (self.workers) |*w| {
            std.debug.assert(w.used.load(.monotonic) == 0);
            w.releaseFloor();
        }
        // A failed worker can still hold a local table; cancellation keeps its live bytes charged until deinit.
        for (self.samples) |*b| b.releaseFloor();
        self.held = 0;
        self.workers = &.{};
        self.samples = &.{};
    }
};

/// The scan to run after `attempt` was refused with `taken` armed, or null when no narrower one can answer and the
/// refusal is the query's. `may_fit`: some refusal so far showed one worker's table may fit (`one_table_may_fit`).
fn narrowed(
    args: MultiAggArgs,
    attempt: MultiAggArgs,
    taken: std.EnumSet(FastPath),
    chain: *Chain,
    may_fit: bool,
) ?MultiAggArgs {
    var next = attempt;
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

fn prepareAggregate(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
    comptime explain: bool,
) !if (explain) AggregateExplanation else MultiAggResult {
    if (args.inputs.len == 0) return error.NoInputs;

    var t: Timings = .{};
    const prof = profile.threadOf(args.profiler, .main);

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
                return if (f.err == error.EncryptedFile) error.EncryptedFile else error.NotParquet;
            },
        }
    };

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

    // 2. Parse aggregate + filter against meta0 (column indexes resolved
    //    against file 0; valid for all files because schemas match).
    const agg_calls: []expr_agg.AggCall = if (args.group_by != null and args.aggregate.len == 0)
        &.{}
    else
        try expr_parser.parseAggList(arena, args.aggregate, meta0, args.diag);
    if (agg_calls.len == 0 and args.group_by == null) return error.EmptyAggregate;

    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |expr_str| {
        if (expr_str.len > 0) {
            filter_opt = try filter_parser.parse(arena, expr_str, meta0, args.diag);
            // `--trust-stats`: a float chunk without a NaN count prunes as if it had none.
            if (args.trust_stats) filter_opt = try filter_ast.trustMissingNanCounts(arena, filter_opt.?);
        }
    }
    const group_by_items = if (args.group_by) |gb_str|
        try expr_parser.parseGroupByOf(arena, gb_str, meta0, args.diag, args.select_cols orelse &.{})
    else
        null;
    const group_by_keys = if (group_by_items) |items| blk: {
        const exprs = try arena.alloc(expr_ast.Expr, items.len);
        for (items, 0..) |item, i| exprs[i] = item.expr;
        break :blk exprs;
    } else null;
    var key_names = try KeyNames.init(arena, meta0, group_by_items orelse &.{});
    key_names.sql = sqlBound(args.select_cols);
    try nameUnaliasedAggs(arena, agg_calls, &key_names);
    const agg_sources = try arena.alloc(?schema.SchemaElement, agg_calls.len);
    for (agg_calls, agg_sources) |call, *src| src.* = passThroughSource(call, meta0);
    const group_outputs: ?[]const BoundOutputColumn = if (group_by_items != null)
        try bindGroupOutputs(arena, &key_names, agg_calls, meta0, args)
    else blk: {
        try checkAggAliases(agg_calls, args.diag);
        break :blk null;
    };
    if (args.result_order.having) |h| try checkHavingLanes(arena, h.*, group_outputs, agg_calls, group_by_keys);
    if (!explain) if (args.diag) |diag| {
        diag.execution_started = true;
    };
    const t_parse_end = nowMonoNs();
    t.parse_ns = @intCast(t_parse_end - t_parse);
    profile.record(prof, .parse_query, t_parse, t_parse_end, profile.args(.{}));
    if (args.profiler) |p| p.setColumnsFromMeta(meta0);
    const plan_span = profile.Span.begin(prof);

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
    plan_span.end(.plan, profile.args(.{ work_items.items.len, n_workers, total_rgs }));

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
    var ndv_stat: ?NdvStat = null;
    var probe_facts: PlanFacts = .{};
    var planning_why: ?@FieldType(Planning, "why") = null;
    // Freed once the dense tables are built, or as soon as the plan cannot use it.
    var domain: ?agg_dense.Domain = null;
    defer if (domain) |*d| d.deinit(gpa);
    if (args.ndv_policy) if (group_by_keys) |keys| {
        // Prefix discovery takes precedence; refining a global group bound cannot improve that decision.
        const first = planAggregate(plan_inputs).dispatch.strategy;
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
        var probe_bytes: u64 = 0;
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
            if (want) &domain else null,
            // A prefix plan reads the union only for `fewEntries`: none past its entries, and none past its values.
            .{ .fold_only = fold_only, .limit = if (first == .first_keys) few_union else null },
        );
        if (!explain) if (args.start_reads) |s| if (s.planned) |planned| planned(s.ctx);
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
    plan_inputs.ndv_stat = ndv_stat;
    var scan_plan = planAggregate(plan_inputs);
    if (scan_plan.wants_sample) {
        const options: consumer.DecodeOptions = .{
            .fast_levels = args.fast_levels,
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
            .workers = n_workers,
            .work_items = work_items.items.len,
        };
    }
    // Tables still share one pool. Only a prepaid private plan's decoders protect funded worker floors.
    var own_budget = expr_agg.SharedBudget{ .limit = args.max_memory };
    const group_budget = args.ledger orelse &own_budget;
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
    for (work_items.items) |item| {
        for (item.accumulators) |*acc| {
            if (acc.* == .distinct) if (acc.distinct.heap()) |ds| {
                if (group_by_keys == null and n_workers > 1) {
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
    const n_tables = decision.tables;
    if (use_blocks) {
        const bounds = try arena.alloc(usize, n_tables + 1);
        for (bounds, 0..) |*b, i| b.* = i * work_items.items.len / n_tables;
        block_bounds = bounds;
    }
    // A dense fold (below) needs one table more, after the workers': the one its lanes merge into.
    const dense_wanted = domain != null and decision.strategy == .plain_hash and !use_blocks and !part_on;
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
        if (!dense_wanted and !use_blocks and (agg_calls.len != 1 or agg_calls[0].func == .count))
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
    const spill_on = !part_on and args.spill and decision.strategy == .plain_hash and !use_blocks and
        fixed_stride == 0 and partAggSupported(args, agg_calls, group_by_keys != null) and rescannable;
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
            ps.* = .{ .gpa = gpa, .budget = group_budget, .table = gt, .agg_idx = ai, .active = pairs_from_start };
        }
    }
    // A lone integer key whose exact domain the union kept folds by global id when the plan groups it in per-worker
    // tables: no group lookups per row group and no hash merge. Taken only when walking every worker's lanes costs no
    // more than the lookups it replaces (one per dictionary entry per row group), and when the domain and every lane
    // fit the shared budget, drawn before any worker starts.
    var dense_lanes: []agg_dense.Lanes = &.{};
    var dense_charge: usize = 0;
    defer group_budget.release(dense_charge);
    defer for (dense_lanes) |*l| l.deinit(gpa);
    // The table the lanes merge into is reserved with them, and handed to it as it is built.
    var dense_table_charge: usize = 0;
    if (dense_wanted) dense: {
        const d = &domain.?;
        const lanes_bytes = n_workers *| agg_dense.laneBytes(d.ids(), agg_calls.len);
        if (!rescannable) break :dense;
        if (spill_on or pairs_call != null or n_workers *| d.ids() > d.entries()) break :dense;
        dense_table_charge = agg_dense.tableBytes(d.ids(), fixed_stride, agg_calls.len);
        dense_charge = group_budget.drawExact(d.bytes +| lanes_bytes +| dense_table_charge) orelse break :dense;
        dense_lanes = try arena.alloc(agg_dense.Lanes, n_workers);
        for (dense_lanes) |*l| l.* = .{ .domain = d, .calls = agg_calls };
        if (args.taken) |taken| taken.insert(.dense);
    }
    if (dense_lanes.len == 0) if (domain) |*d| {
        d.deinit(gpa);
        domain = null;
    };
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

    var winners: WinnerCharge = .{ .ledger = if (group_by_keys == null) args.ledger else null };
    errdefer winners.settle(0);
    var work_cursor = std.atomic.Value(usize).init(0);
    for (workers, 0..) |*w, wi| {
        w.* = .{
            .gpa = gpa,
            .inputs = args.inputs,
            .metas = metas,
            .work = work_items.items,
            .cursor = &work_cursor,
            .agg_calls = agg_calls,
            .plan = &plan,
            .decode_options = .{
                .fast_levels = args.fast_levels,
                .encoded = args.encoded,
                .expand_limit = args.max_memory,
            },
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
        .first_keys => firstKeysCut(gpa, arena, workers, &work_cursor, group_tables, order, args.sketch_topk.?),
        .sketch => sketchTopK(
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
    passes catch |e| {
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
    if (!args.leave_regions or args.ledger != null or private_decode.held > 0) releaseRegions(workers);
    private_decode.release();
    for (workers) |w| if (w.err) |e| {
        if (e == error.ExceededMemoryBudget) if (args.taken) |taken| {
            if (decode_held and rescannable) taken.insert(.decode_memory);
            if (taken.contains(.worker_tables)) {
                if ((decision.strategy == .plain_hash and holdCopies(group_tables)) or
                    try oneWorkerCuts(arena, plan_inputs)) taken.insert(.one_table_may_fit);
            }
        };
        return e;
    };
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
    // Left to the exit with the tables, but freed if a later refusal runs the scan again (`runMultiAggregate`).
    var left_lanes: []agg_dense.Lanes = &.{};
    var left_domain: ?agg_dense.Domain = null;
    errdefer {
        for (left_lanes) |*l| l.deinit(gpa);
        if (left_domain) |*d| d.deinit(gpa);
    }
    if (dense_lanes.len > 0) {
        const dense_table = &all_tables[n_tables];
        const key = group_by_keys.?[0].col_ref;
        group_budget.release(dense_table_charge);
        dense_charge -= dense_table_charge;
        try agg_dense.intoTable(gpa, arena, dense_lanes, key, dense_table, key.col_idx + 1, n_workers);
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
        dense_merged = true;
    }
    // Once any worker sends pairs every table does, so each key's values meet in one partition.
    var pairs_used = false;
    for (pair_sinks) |*ps| pairs_used = pairs_used or ps.active;
    if (pairs_used) {
        for (pair_sinks, group_tables) |*ps, *gt| if (!ps.active) try ps.drainTable(gt);
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
    for (agg_calls, accumulators) |call, *acc| if (call.func == .count_distinct) {
        const ds = try gpa.create(expr_agg.DistinctSet);
        ds.* = .{};
        acc.* = .{ .distinct = .fromSet(ds) };
        if (group_by_keys == null and n_workers > 1) {
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
    var rows_kept: i64 = 0;
    var rgs_in: usize = 0;
    var rgs_pruned: usize = 0;
    var rgs_full_match: usize = 0;

    var group_col_types: ?[]const GroupColType = null;

    if (group_by_keys) |keys| {
        for (workers) |w| {
            rows_in += w.rows_in;
            rows_kept += w.rows_kept;
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
        const candidates = try topKCandidates(arena, out_tables, group_outputs.?, agg_calls, args.result_order);
        // One table already holds complete ranks; its top-H heap avoids materializing every losing group.
        const cut: ?[]const u32 = if (candidates == null and out_tables.len == 1)
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
        for (out_tables, 0..) |gt, ti| {
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
            } else for (0..gt.groupCount()) |gi| {
                group_refs_buf[ref_i] = .{ .table = @intCast(ti), .group = @intCast(gi) };
                ref_i += 1;
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
                .source = if (keys[k_idx] == .col_ref)
                    leafSchemaElem(meta0, keys[k_idx].col_ref.col_idx)
                else if (keys[k_idx].isBoolean()) boolean_answer else null,
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
            freeOwnedAccumulatorStrings(gpa, merged_accs.items);
            merged_accs.deinit(lists);
        }
        var folded = out_tables.len > 1;
        // Set when the merge left each group's state in its source table (`mergeInPlace`).
        var in_place_tables: []const expr_agg.GroupTable = &.{};
        // Partition tables already hold each key once. Their accumulators move into `merged_accs`; the keys stay
        // borrowed until the rows are built.
        if (spilled) {
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
            for (group_refs) |ref| {
                slots.appendAssumeCapacity(.{
                    .key = gt.keyAt(ref.group),
                    .first = gt.first.items[ref.group],
                    .idx = ref.group,
                });
            }
        }
        if (folded) if (result_cut) |rc| {
            if (rc.ranking_first) try keepResultTop(gpa, lists, agg_calls, rc, &slots, &merged_accs);
        };
        if (!args.unordered) try sortSlotsByFirst(lists, slots.items, n_workers);

        // Owned by `output` from allocation on, every slot valid at every step, so its `deinit` frees a partly built
        // answer: unbuilt values NULL. One slab holds every row's cells (`AggOutput.slab`), not an allocation a row.
        const row_src: RowSource = .{
            .slots = slots.items,
            .outputs = outputs,
            .calls = agg_calls,
            .key_types = key_types,
            .accs = if (folded) merged_accs.items else out_tables[0].accumulators.items,
            .folded = folded,
            .in_place = in_place_tables,
            .single = if (folded) null else &out_tables[0],
            .fixed_keys = if (spilled) fixed_stride != 0 else out_tables[0].fixed_stride != 0,
        };
        const as_is = order.keys.len == 0 and order.limit == null and order.offset == 0 and order.having == null and
            order.hidden.len == 0;
        const typed = args.typed_answer and as_is;
        // The answer is drawn from the ledger before it is built, beside the tables and merge it is built from, and
        // stays drawn while the query holds it (the engine's ledger outlives it). Refused, it is the merge's refusal:
        // on one worker the tables it reads are one.
        if (args.ledger != null) {
            const bytes = answerBytes(slots.items, col_types, key_types, typed);
            answer_charge = group_budget.drawExact(bytes) orelse {
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

        // Result rows own any strings they need; table keys and accumulator
        // winners can now be released before returning (the deferred deinit
        // then finds them empty).
        if (!leave_tables) {
            for (out_tables) |*gt| {
                gt.deinit();
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
                const n_threads = @min(n_workers, expr_agg.NUM_PARTS);
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
            rows_kept += w.rows_kept;
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
        .rows_kept = rows_kept,
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

/// min/max of a bare column returns one of that column's values, so its output can carry the column's type.
fn passThroughSource(call: expr_agg.AggCall, meta: *const schema.FileMetaData) ?schema.SchemaElement {
    if (call.func != .min and call.func != .max) return null;
    const arg = call.arg orelse return null;
    if (arg != .col_ref) return if (arg.isBoolean()) boolean_answer else null;
    return leafSchemaElem(meta, arg.col_ref.col_idx);
}

/// The type a computed BOOLEAN answer column (a key, min or max of a BOOLEAN expression) passes through as, as a bare
/// BOOLEAN column's does: printed true/false and written BOOLEAN, though its lane holds 0/1.
const boolean_answer: schema.SchemaElement = .{
    .type = .BOOLEAN,
    .type_length = null,
    .repetition_type = .OPTIONAL,
    .name = "",
    .num_children = null,
    .scale = null,
    .precision = null,
    .field_id = null,
};

const MergeGroupRef = struct {
    table: u32,
    group: u32,
};

const MergedSlot = struct {
    /// Borrowed from a source table. Valid until that table is freed.
    key: []const u8,
    first: u64,
    /// Start of this group's accumulators in the merged list, or the source group id when nothing was folded.
    idx: usize,
};

/// The widest digit a pass sorts by: 8192 buckets a chunk.
const radix_bits_max = 13;
/// Fewer slots than this per chunk sort on fewer threads: a chunk's pass over its buckets would cost more than its
/// share of the slots.
const radix_chunk_min = 1 << 16;

/// Sort `slots` by `first`, the input row that created each group, so unique per group: answers list groups in
/// first-appearance order, the same at every `-j`. An LSD radix sort, so every pass splits into chunks of slots each
/// worker counts and scatters on its own. `first` packs a row group ordinal above a row index (`firstPosition`), so
/// most of its 64 bits are the same in every slot: the sort key is `first` with the row index's unused high bits
/// squeezed out (`SlotRadix.key`, order-preserving), and its varying bits are split into as few passes as cover them.
fn sortSlotsByFirst(gpa: std.mem.Allocator, slots: []MergedSlot, n_workers: usize) error{OutOfMemory}!void {
    if (slots.len < 2) return;
    const n_chunks = @max(1, @min(n_workers, slots.len / radix_chunk_min));
    const scratch = try gpa.alloc(MergedSlot, slots.len);
    defer gpa.free(scratch);
    const counts = try gpa.alloc(usize, n_chunks << radix_bits_max);
    defer gpa.free(counts);
    const spans = try gpa.alloc(SlotRadix.Span, n_chunks);
    defer gpa.free(spans);
    const tasks = try gpa.alloc(SlotRadix.Task, n_chunks);
    defer gpa.free(tasks);

    var sort: SlotRadix = .{ .src = slots, .dst = scratch, .counts = counts, .spans = spans, .n_chunks = n_chunks };
    for (tasks) |*t| t.* = .{ .sort = &sort };
    sort.runPhase(tasks, .span);
    var any: u64 = 0;
    var all: u64 = std.math.maxInt(u64);
    for (spans) |s| {
        any |= s.any;
        all &= s.all;
    }
    // Every slot's row index is below 2^row_bits. `key` moves bits, so it maps an OR or an AND of firsts to the same
    // of their keys.
    sort.row_bits = @intCast(64 - @clz(any & 0xffff_ffff));
    const varying = sort.key(any) ^ sort.key(all);
    if (varying == 0) return;
    const lo: usize = @ctz(varying);
    const span_bits = 64 - @clz(varying) - lo;
    const passes = std.math.divCeil(usize, span_bits, radix_bits_max) catch unreachable;
    sort.bits = @intCast(std.math.divCeil(usize, span_bits, passes) catch unreachable);
    const buckets = @as(usize, 1) << sort.bits;

    var shift: usize = lo;
    while (shift < lo + span_bits) : (shift += sort.bits) {
        sort.shift = @intCast(shift);
        if ((varying >> sort.shift) & (buckets - 1) == 0) continue;
        sort.runPhase(tasks, .count);
        // Bucket b starts after every chunk's slots of the buckets below it; within b, chunk c's after chunks < c, so
        // each pass is stable, as LSD needs.
        var at: usize = 0;
        for (0..buckets) |b| {
            for (0..n_chunks) |c| {
                const n = counts[c * buckets + b];
                counts[c * buckets + b] = at;
                at += n;
            }
        }
        sort.runPhase(tasks, .scatter);
        std.mem.swap([]MergedSlot, &sort.src, &sort.dst);
    }
    if (sort.src.ptr != slots.ptr) {
        sort.dst = slots;
        sort.runPhase(tasks, .copy);
    }
}

const SlotRadix = struct {
    src: []MergedSlot,
    dst: []MergedSlot,
    /// Per chunk, a count per bucket of the current digit, then where the chunk writes its next slot of that bucket.
    counts: []usize,
    spans: []Span,
    n_chunks: usize,
    shift: u6 = 0,
    /// The digit's width, at most `radix_bits_max`.
    bits: u4 = radix_bits_max,
    /// Bits of the row index in `key`: as many as the largest row index of any slot takes.
    row_bits: u6 = 32,
    phase: Phase = .span,
    cursor: std.atomic.Value(usize) = .init(0),

    /// The bits set in any of a chunk's `first`s, and in all of them.
    const Span = struct { any: u64, all: u64 };
    const Phase = enum { span, count, scatter, copy };
    const Task = struct { sort: *SlotRadix };

    fn runPhase(s: *SlotRadix, tasks: []Task, phase: Phase) void {
        s.phase = phase;
        s.cursor.store(0, .monotonic);
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, config, run)) run(&tasks[0]);
    }

    /// Claim chunks until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
    fn run(t: *Task) void {
        const s = t.sort;
        while (true) {
            const c = s.cursor.fetchAdd(1, .monotonic);
            if (c >= s.n_chunks) return;
            const lo = s.src.len * c / s.n_chunks;
            const hi = s.src.len * (c + 1) / s.n_chunks;
            const buckets = @as(usize, 1) << s.bits;
            const mine = s.counts[c * buckets ..][0..buckets];
            switch (s.phase) {
                .span => {
                    var span: Span = .{ .any = 0, .all = std.math.maxInt(u64) };
                    for (s.src[lo..hi]) |slot| {
                        span.any |= slot.first;
                        span.all &= slot.first;
                    }
                    s.spans[c] = span;
                },
                .count => {
                    @memset(mine, 0);
                    for (s.src[lo..hi]) |slot| mine[s.digit(slot.first)] += 1;
                },
                .scatter => for (s.src[lo..hi]) |slot| {
                    const d = s.digit(slot.first);
                    s.dst[mine[d]] = slot;
                    mine[d] += 1;
                },
                .copy => @memcpy(s.dst[lo..hi], s.src[lo..hi]),
            }
        }
    }

    /// `first`'s row group ordinal right above its row index's `row_bits`: the same order on fewer bits.
    fn key(s: *const SlotRadix, first: u64) u64 {
        const row_mask = (@as(u64, 1) << s.row_bits) - 1;
        return (first >> 32) << s.row_bits | (first & row_mask);
    }

    fn digit(s: *const SlotRadix, first: u64) usize {
        return @intCast((s.key(first) >> s.shift) & ((@as(u64, 1) << s.bits) - 1));
    }
};

/// What the GROUP BY answer's rows are built from: the merged groups and the output columns they fill.
const RowSource = struct {
    slots: []const MergedSlot,
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    key_types: []const KeyType,
    /// The merged accumulators, a group's starting at `slot.idx`, when worker tables were folded; else the one
    /// table's, starting at `slot.idx * calls.len`.
    accs: []const expr_agg.Accumulator,
    folded: bool,
    /// Non-empty when the merge left each group's state in these tables (`mergeInPlace`): `slot.idx` names it, and
    /// `accs` is unused.
    in_place: []const expr_agg.GroupTable = &.{},
    single: ?*const expr_agg.GroupTable = null,
    /// The keys are fixed-width images, not the serialized framing `deserializeKeyColumn` reads.
    fixed_keys: bool,

    /// Fill `vals`, the row's slab cells, all NULL, for `slot`. Each value is set only once built, so on an error the
    /// row holds what was built and NULLs, which `AggOutput.releaseRow` frees.
    fn buildRow(s: *const RowSource, gpa: std.mem.Allocator, slot: MergedSlot, vals: []AggValue) !void {
        const at = if (s.folded) slot.idx else slot.idx * s.calls.len;
        const table: ?*const expr_agg.GroupTable = if (s.in_place.len > 0)
            &s.in_place[slot.idx >> 32]
        else
            s.single;
        const group = if (s.in_place.len > 0) @as(u32, @truncate(slot.idx)) else slot.idx;
        const compact = table != null and table.?.compact.stride != 0;
        const accs = if (compact) &.{} else if (s.in_place.len > 0)
            table.?.accumulators.items[@as(usize, group) * s.calls.len ..][0..s.calls.len]
        else
            s.accs[at..][0..s.calls.len];
        // A fixed image pads a null column with zeros; the reader wants that payload omitted.
        var ser_buf: [48]u8 = undefined;
        const key_bytes = if (s.fixed_keys) expr_agg.canonicalKey(slot.key, &ser_buf) else slot.key;
        for (s.outputs, vals) |out, *val| val.* = switch (out.source) {
            .key => |k_idx| try deserializeKeyColumn(gpa, key_bytes, k_idx, s.key_types),
            .agg => |a_idx| blk: {
                const acc = if (compact) table.?.readAcc(group, a_idx, s.calls.len) else accs[a_idx];
                break :blk try materializeOne(gpa, s.calls[a_idx], if (s.in_place.len > 0)
                    try foldedAcc(gpa, s.calls[a_idx], acc)
                else
                    acc);
            },
        };
    }
};

/// Rows a build task claims at a time.
const row_block = 4096;

/// Build `rows[i]` for `src.slots[i]`, on up to `n_workers` threads. `rows` must start empty; on an error each row
/// holds what was built of it (see `RowSource.buildRow`), so the `AggOutput` owning it frees it whole, and the error
/// is the one the lowest failing row gave, as a serial build would return.
fn buildGroupRows(
    gpa: std.mem.Allocator,
    /// Receives the slab the rows are views into, owned by the caller (`AggOutput.slab`) from allocation on.
    slab: *[]AggValue,
    src: RowSource,
    rows: [][]const AggValue,
    n_workers: usize,
) !void {
    std.debug.assert(rows.len == src.slots.len);
    if (rows.len == 0) return;
    const width = src.outputs.len;
    const cells = try gpa.alloc(AggValue, rows.len * width);
    @memset(cells, .null_val);
    slab.* = cells;
    for (rows, 0..) |*row, i| row.* = cells[i * width ..][0..width];
    const n_threads = @max(1, @min(n_workers, std.math.divCeil(usize, rows.len, row_block) catch unreachable));
    const tasks = try gpa.alloc(RowBuildTask, n_threads);
    defer gpa.free(tasks);
    var cursor: std.atomic.Value(usize) = .init(0);
    for (tasks) |*t| t.* = .{ .gpa = gpa, .src = &src, .rows = rows, .cursor = &cursor };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (tasks.len == 1 or !spawn_util.runTree(RowBuildTask, tasks, config, RowBuildTask.run)) RowBuildTask.run(&tasks[0]);
    // Every block below the lowest failing row was claimed and built whole, so that row's error is a serial build's.
    var failed: ?*const RowBuildTask = null;
    for (tasks) |*t| if (t.err != null and (failed == null or t.err_row < failed.?.err_row)) {
        failed = t;
    };
    if (failed) |t| return t.err.?;
}

/// What a GROUP BY answer of `slots` holds once built (`buildGroupColumns` or `buildGroupRows`): its values, their
/// lanes and NULL flags, and its string keys' heap blocks. A string aggregate's own value is not counted.
fn answerBytes(slots: []const MergedSlot, cols: []const GroupColType, key_types: []const KeyType, typed: bool) usize {
    const rows = slots.len;
    // Each string key value is a heap block of its own: rounded to 16 bytes, with a header.
    var string_keys: usize = 0;
    for (key_types) |kt| string_keys += @intFromBool(kt == .string);
    var strings: usize = 0;
    if (string_keys > 0) for (slots) |sl| {
        strings +|= std.mem.alignForward(usize, sl.key.len, 16) + string_keys * 16;
    };
    if (!typed) return rows *| (cols.len *| @sizeOf(AggValue) +| @sizeOf([]const AggValue)) +| strings;
    var per_row: usize = 0;
    for (cols) |c| per_row += @sizeOf(u32) + switch (c.lane) {
        .i64, .f64 => @as(usize, 8),
        .string => @sizeOf([]const u8),
        .dec => 16,
        .avg => 16,
        .avg_dec => 24,
    };
    return rows *| per_row +| strings;
}

/// A GROUP BY answer as typed columns, one per output column in `group_cols` order, built straight from the merged
/// groups for a Parquet write, so no row of `AggValue`s (112 bytes a value) is ever held. Values are made by the same
/// functions the rows are (`RowSource`) and narrowed by the rules the row writer applies, so the file and the error a
/// query stops with are the same either way. Strings are owned, a present one each.
pub const TypedAnswer = struct {
    rows: usize,
    columns: []TypedColumn,

    pub fn Lane(comptime T: type) type {
        return struct {
            values: []T,
            /// 1 present, 0 NULL.
            defs: []u32,
            nulls: bool = false,
        };
    }

    pub const TypedColumn = union(enum) {
        i64: Lane(i64),
        f64: Lane(f64),
        string: Lane([]const u8),
        dec: Lane(i128),
        /// avg_state's re-aggregatable pair, written as `<name>__sum` and `<name>__count`.
        avg: struct { sum: Lane(f64), count: []i64 },
        avg_dec: struct { sum: Lane(i128), count: []i64 },
    };

    pub fn deinit(self: *TypedAnswer, gpa: std.mem.Allocator) void {
        for (self.columns) |*col| switch (col.*) {
            .i64 => |l| freeLane(gpa, i64, l),
            .f64 => |l| freeLane(gpa, f64, l),
            .dec => |l| freeLane(gpa, i128, l),
            .string => |l| {
                for (l.values, l.defs) |v, d| if (d == 1) gpa.free(v);
                freeLane(gpa, []const u8, l);
            },
            .avg => |a| {
                freeLane(gpa, f64, a.sum);
                gpa.free(a.count);
            },
            .avg_dec => |a| {
                freeLane(gpa, i128, a.sum);
                gpa.free(a.count);
            },
        };
        gpa.free(self.columns);
        self.* = .{ .rows = 0, .columns = &.{} };
    }

    fn freeLane(gpa: std.mem.Allocator, comptime T: type, l: Lane(T)) void {
        gpa.free(l.values);
        gpa.free(l.defs);
    }

    /// Cleared by `clear`, but a string lane's defs at once: its `deinit` reads them, should a later lane fail.
    fn lane(gpa: std.mem.Allocator, comptime T: type, n: usize) !Lane(T) {
        const values = try gpa.alloc(T, n);
        errdefer gpa.free(values);
        const defs = try gpa.alloc(u32, n);
        if (T == []const u8) @memset(defs, 0);
        return .{ .values = values, .defs = defs };
    }

    fn clearLane(comptime T: type, l: Lane(T), lo: usize, hi: usize) void {
        @memset(l.values[lo..hi], std.mem.zeroes(T));
        @memset(l.defs[lo..hi], 0);
    }

    /// Rows `lo..hi` of every column NULL, values zero.
    fn clear(self: *const TypedAnswer, lo: usize, hi: usize) void {
        for (self.columns) |col| switch (col) {
            .i64 => |l| clearLane(i64, l, lo, hi),
            .f64 => |l| clearLane(f64, l, lo, hi),
            .string => |l| clearLane([]const u8, l, lo, hi),
            .dec => |l| clearLane(i128, l, lo, hi),
            .avg => |a| {
                clearLane(f64, a.sum, lo, hi);
                @memset(a.count[lo..hi], 0);
            },
            .avg_dec => |a| {
                clearLane(i128, a.sum, lo, hi);
                @memset(a.count[lo..hi], 0);
            },
        };
    }

    /// Rows a `clear` task claims at a time.
    const clear_block = 1 << 16;

    /// `clear` of every row, on up to `n_workers` threads: an answer of millions of rows is cleared, and its pages
    /// first touched, on every core rather than one. Cannot fail: rows a thread that never started left are cleared by
    /// one that did, or here.
    fn clearAll(self: *const TypedAnswer, n_workers: usize) void {
        const Task = struct {
            answer: *const TypedAnswer,
            cursor: *std.atomic.Value(usize),

            fn run(t: *@This()) void {
                while (true) {
                    const lo = t.cursor.fetchAdd(clear_block, .monotonic);
                    if (lo >= t.answer.rows) return;
                    t.answer.clear(lo, @min(t.answer.rows, lo + clear_block));
                }
            }
        };
        var tasks: [32]Task = undefined;
        const n_threads = @min(tasks.len, n_workers, self.rows / clear_block);
        if (n_threads < 2) return self.clear(0, self.rows);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks[0..n_threads]) |*t| t.* = .{ .answer = self, .cursor = &cursor };
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (!spawn_util.runTree(Task, tasks[0..n_threads], config, Task.run)) Task.run(&tasks[0]);
    }

    /// Columns typed by `types`, every cell NULL, owned by the caller's answer from the first allocation on. Cleared on
    /// up to `n_workers` threads.
    fn init(gpa: std.mem.Allocator, types: []const GroupColType, n: usize, out: *TypedAnswer, n_workers: usize) !void {
        out.* = .{ .rows = n, .columns = try gpa.alloc(TypedColumn, types.len) };
        // Each column is valid (empty) until allocated, so a failure leaves `deinit` something to free.
        for (out.columns) |*c| c.* = .{ .i64 = .{ .values = &.{}, .defs = &.{} } };
        for (types, out.columns) |ty, *col| col.* = switch (ty.lane) {
            .i64 => .{ .i64 = try lane(gpa, i64, n) },
            .f64 => .{ .f64 = try lane(gpa, f64, n) },
            .string => .{ .string = try lane(gpa, []const u8, n) },
            .dec => .{ .dec = try lane(gpa, i128, n) },
            .avg => blk: {
                const sum = try lane(gpa, f64, n);
                const count = gpa.alloc(i64, n) catch |e| {
                    freeLane(gpa, f64, sum);
                    return e;
                };
                break :blk .{ .avg = .{ .sum = sum, .count = count } };
            },
            .avg_dec => blk: {
                const sum = try lane(gpa, i128, n);
                const count = gpa.alloc(i64, n) catch |e| {
                    freeLane(gpa, i128, sum);
                    return e;
                };
                break :blk .{ .avg_dec = .{ .sum = sum, .count = count } };
            },
        };
        out.clearAll(n_workers);
    }

    /// Store value `v` of row `i` in column `col`, narrowed as the row writer narrows it. A string's ownership passes
    /// to the column at once, so an error after leaves it freed with the rest. The error is the row writer's.
    fn put(col: *TypedColumn, ty: GroupColType, i: usize, v: AggValue) !void {
        if (v == .null_val) return;
        switch (col.*) {
            .i64 => |*l| {
                const x = switch (v) {
                    .i => |x| x,
                    else => return error.TypeMismatch,
                };
                const raw_u64 = if (ty.source) |src| schema.isUnsignedInt64(src) else false;
                l.values[i] = if (raw_u64 and x >= 0 and x <= std.math.maxInt(u64))
                    expr_agg.u64Lane(x)
                else
                    std.math.cast(i64, x) orelse return error.AggIntTooWide;
                l.defs[i] = 1;
            },
            .f64 => |*l| {
                l.values[i] = switch (v) {
                    .f => |x| x,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .string => |*l| {
                l.values[i] = switch (v) {
                    .s => |x| x,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .dec => |*l| {
                l.values[i] = switch (v) {
                    .dec => |x| x.v,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .avg => |*a| switch (v) {
                .avg => |p| {
                    a.count[i] = p.count;
                    if (p.sum) |s| {
                        a.sum.values[i] = s;
                        a.sum.defs[i] = 1;
                    }
                },
                else => return error.TypeMismatch,
            },
            .avg_dec => |*a| switch (v) {
                .avg_dec => |p| {
                    a.count[i] = p.count;
                    // The `__sum` column is DECIMAL(38, s): a wider exact sum cannot be stored, and is not rounded.
                    if (p.sum) |x| {
                        if (@abs(x) >= @as(u192, @intCast(ty.dec.?.sum().bound()))) return error.DecimalOverflow;
                        a.sum.values[i] = @intCast(x);
                        a.sum.defs[i] = 1;
                    }
                },
                else => return error.TypeMismatch,
            },
        }
    }

    /// Flag the columns that hold a NULL, and refuse one its type says cannot: the row writer's check.
    fn seal(self: *TypedAnswer, types: []const GroupColType) !void {
        for (self.columns, types) |*col, ty| switch (col.*) {
            inline .i64, .f64, .string, .dec => |*l| {
                l.nulls = std.mem.indexOfScalar(u32, l.defs, 0) != null;
                if (l.nulls and !ty.nullable) return error.UnexpectedNull;
            },
            inline .avg, .avg_dec => |*a| a.sum.nulls = std.mem.indexOfScalar(u32, a.sum.defs, 0) != null,
        };
    }
};

/// Build the GROUP BY answer as typed columns (`TypedAnswer`), on up to `n_workers` threads. The error is the row
/// path's: a value that fails to build at the lowest such row; else, by column, the first row that does not narrow.
fn buildGroupColumns(
    gpa: std.mem.Allocator,
    out: *TypedAnswer,
    src: RowSource,
    types: []const GroupColType,
    n_workers: usize,
) !void {
    try TypedAnswer.init(gpa, types, src.slots.len, out, n_workers);
    const n = src.slots.len;
    if (n == 0) return;
    const n_threads = @max(1, @min(n_workers, std.math.divCeil(usize, n, row_block) catch unreachable));
    const tasks = try gpa.alloc(ColumnBuildTask, n_threads);
    defer gpa.free(tasks);
    var cursor: std.atomic.Value(usize) = .init(0);
    const vals = try gpa.alloc(AggValue, n_threads * src.outputs.len);
    defer gpa.free(vals);
    for (tasks, 0..) |*t, ti| t.* = .{
        .gpa = gpa,
        .src = &src,
        .types = types,
        .answer = out,
        .cursor = &cursor,
        .vals = vals[ti * src.outputs.len ..][0..src.outputs.len],
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (tasks.len == 1 or !spawn_util.runTree(ColumnBuildTask, tasks, config, ColumnBuildTask.run)) ColumnBuildTask.run(&tasks[0]);
    var failed: ?*const ColumnBuildTask = null;
    for (tasks) |*t| if (t.err != null and (failed == null or t.err_row < failed.?.err_row)) {
        failed = t;
    };
    if (failed) |t| return t.err.?;
    // Narrowing errors in column order, each column's first failing row, as the row writer meets them.
    var narrow: ?struct { col: usize, row: usize, err: anyerror } = null;
    for (tasks) |t| if (t.narrow_err) |e| {
        if (narrow == null or t.narrow_col < narrow.?.col or (t.narrow_col == narrow.?.col and t.narrow_row < narrow.?.row))
            narrow = .{ .col = t.narrow_col, .row = t.narrow_row, .err = e };
    };
    if (narrow) |e| return e.err;
    try out.seal(types);
}

const ColumnBuildTask = struct {
    gpa: std.mem.Allocator,
    src: *const RowSource,
    types: []const GroupColType,
    answer: *TypedAnswer,
    cursor: *std.atomic.Value(usize),
    /// One row's values, built then stored a row at a time.
    vals: []AggValue,
    err: ?anyerror = null,
    err_row: usize = 0,
    narrow_err: ?anyerror = null,
    narrow_col: usize = 0,
    narrow_row: usize = 0,

    fn run(t: *ColumnBuildTask) void {
        const n = t.src.slots.len;
        while (true) {
            const lo = t.cursor.fetchAdd(row_block, .monotonic);
            if (lo >= n) return;
            for (lo..@min(n, lo + row_block)) |i| {
                @memset(t.vals, .null_val);
                const built = t.src.buildRow(t.gpa, t.src.slots[i], t.vals);
                // Every value built passes to its column, whatever happens to the rest of the row.
                for (t.answer.columns, t.types, t.vals, 0..) |*col, ty, v, ci| {
                    TypedAnswer.put(col, ty, i, v) catch |e| {
                        if (v == .s) t.gpa.free(v.s);
                        if (t.narrow_err == null or ci < t.narrow_col or (ci == t.narrow_col and i < t.narrow_row)) {
                            t.narrow_err = e;
                            t.narrow_col = ci;
                            t.narrow_row = i;
                        }
                    };
                }
                built catch |e| {
                    t.err = e;
                    t.err_row = i;
                    return;
                };
            }
        }
    }
};

const RowBuildTask = struct {
    gpa: std.mem.Allocator,
    src: *const RowSource,
    rows: [][]const AggValue,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,
    err_row: usize = 0,

    /// Claim blocks of rows until none remain or one fails. A thread that never starts leaves its blocks on the
    /// cursor, so a failed spawn cannot leave a row unbuilt.
    fn run(t: *RowBuildTask) void {
        while (true) {
            const lo = t.cursor.fetchAdd(row_block, .monotonic);
            if (lo >= t.rows.len) return;
            for (lo..@min(t.rows.len, lo + row_block)) |i| {
                t.src.buildRow(t.gpa, t.src.slots[i], @constCast(t.rows[i])) catch |e| {
                    t.err = e;
                    t.err_row = i;
                    return;
                };
            }
        }
    }
};

/// Calls `f(ctx, v)` for every value of a set of u64 values, partitioned or not.
fn eachDistinctU64(ds: *const expr_agg.DistinctSet, ctx: anytype, comptime f: fn (@TypeOf(ctx), u64) void) void {
    const one = struct {
        fn set(d: *const expr_agg.DistinctSet, c: @TypeOf(ctx)) void {
            switch (d.val_set) {
                .inline_u64 => |s| for (s.vals[0..s.len]) |v| f(c, v),
                .u64 => |s| {
                    var it = s.keyIterator();
                    while (it.next()) |k| f(c, k.*);
                },
                else => {},
            }
        }
    };
    one.set(ds, ctx);
    if (ds.parts) |parts| for (parts) |*part| one.set(part, ctx);
}

fn distinctSlice(v: u64) usize {
    return @intCast((v *% 11400714819323198485) >> 60);
}

const Phase1Task = struct {
    gpa: std.mem.Allocator,
    sources: []const *const expr_agg.DistinctSet,
    slices: []std.ArrayListUnmanaged(u64),
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    /// Split each claimed source set's values into its 16 slices: counted first, so each slice is reserved at
    /// exactly its size before it is filled.
    fn run(t: *Phase1Task) void {
        while (t.err == null) {
            const m = t.cursor.fetchAdd(1, .monotonic);
            if (m >= t.sources.len) return;
            const row = t.slices[m * 16 .. (m + 1) * 16];
            var sizes: [16]usize = @splat(0);
            eachDistinctU64(t.sources[m], &sizes, struct {
                fn f(n: *[16]usize, v: u64) void {
                    n[distinctSlice(v)] += 1;
                }
            }.f);
            for (row, sizes) |*cell, n| cell.ensureTotalCapacityPrecise(t.gpa, n) catch |e| {
                t.err = e;
                return;
            };
            eachDistinctU64(t.sources[m], row, struct {
                fn f(r: []std.ArrayListUnmanaged(u64), v: u64) void {
                    r[distinctSlice(v)].appendAssumeCapacity(v);
                }
            }.f);
        }
    }
};

const Phase2Task = struct {
    gpa: std.mem.Allocator,
    slices: []std.ArrayListUnmanaged(u64),
    M: usize,
    counts: []usize,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    fn run(t: *Phase2Task) void {
        while (t.err == null) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= 16) return;
            var total: usize = 0;
            for (0..t.M) |m| total += t.slices[m * 16 + p].items.len;
            if (total == 0) {
                t.counts[p] = 0;
                continue;
            }
            if (total == 1) {
                t.counts[p] = 1;
                continue;
            }
            var local_map: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer local_map.deinit(t.gpa);
            local_map.ensureTotalCapacity(t.gpa, @intCast(total)) catch |err| {
                t.err = err;
                return;
            };
            for (0..t.M) |m| {
                for (t.slices[m * 16 + p].items) |v| {
                    local_map.putAssumeCapacity(v, {});
                }
            }
            t.counts[p] = local_map.count();
        }
    }
};

fn parallelDistinctCountU64(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    sources: []const *const expr_agg.DistinctSet,
    n_workers: usize,
) !u64 {
    const M = sources.len;
    const slices = try arena.alloc(std.ArrayListUnmanaged(u64), M * 16);
    for (slices) |*cell| cell.* = .empty;
    defer for (slices) |*cell| cell.deinit(gpa);

    const n_threads = @min(n_workers, @max(M, 16));
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };

    {
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = try arena.alloc(Phase1Task, n_threads);
        for (tasks) |*task| task.* = .{
            .gpa = gpa,
            .sources = sources,
            .slices = slices,
            .cursor = &cursor,
        };
        if (!spawn_util.runTree(Phase1Task, tasks, config, Phase1Task.run)) {
            Phase1Task.run(&tasks[0]);
        }
        for (tasks) |task| if (task.err) |e| return e;
    }

    var counts: [16]usize = undefined;
    {
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = try arena.alloc(Phase2Task, n_threads);
        for (tasks) |*task| task.* = .{
            .gpa = gpa,
            .slices = slices,
            .M = M,
            .counts = &counts,
            .cursor = &cursor,
        };
        if (!spawn_util.runTree(Phase2Task, tasks, config, Phase2Task.run)) {
            Phase2Task.run(&tasks[0]);
        }
        for (tasks) |task| if (task.err) |e| return e;
    }

    var total: u64 = 0;
    for (counts) |c| total += c;
    return total;
}

fn mergeCandidates(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    keys: []const []const u8,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    _ = n_workers;
    try slots.ensureTotalCapacity(lists, keys.len);
    try accs.ensureTotalCapacity(lists, keys.len * calls.len);

    for (keys) |key| {
        var first: u64 = std.math.maxInt(u64);
        var key_borrowed: []const u8 = key;
        var found_any = false;

        for (tables) |gt| {
            if (gt.findKey(key)) |gi| {
                found_any = true;
                if (gt.first.items[gi] < first) {
                    first = gt.first.items[gi];
                    key_borrowed = gt.keyAt(gi);
                }
            }
        }
        if (!found_any) continue;

        const slot_idx = accs.items.len;
        for (calls, 0..) |call, ci| {
            // Appended before merging, so state moved out of a source is owned by `accs` even if a later merge fails.
            accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
            const dst = &accs.items[accs.items.len - 1];
            for (tables) |gt| {
                if (gt.findKey(key)) |gi| {
                    var local = gt.readAcc(gi, ci, calls.len);
                    const src = if (gt.compact.stride != 0)
                        &local
                    else
                        &gt.accumulators.items[@as(usize, gi) * calls.len + ci];
                    try mergeOwnedAccumulator(dst, src, gpa);
                }
            }
        }

        slots.appendAssumeCapacity(.{
            .key = key_borrowed,
            .first = first,
            .idx = slot_idx,
        });
    }
}

const HashKey = struct { bytes: []const u8, hash: u64 };

const HashKeyCtx = struct {
    pub fn hash(_: @This(), k: HashKey) u64 {
        return k.hash;
    }
    pub fn eql(_: @This(), k: HashKey, stored: []const u8) bool {
        return keyEql(k.bytes, stored);
    }
};

/// A merge's key -> merged group index, sized up front for every key it will see. Fixed-width keys (every table with
/// the same `fixed_stride`) may be copied into the slots of an open-addressed table, so a probe compares the slot's
/// own bytes instead of following a slice into a source table; other keys go through a map borrowing them (`Inline`).
const MergeIndex = union(enum) {
    bytes: KeyHashMap(u32),
    fixed9: FixedMergeIndex(9),
    fixed18: FixedMergeIndex(18),
    fixed27: FixedMergeIndex(27),

    const Entry = struct { found: bool, value: *u32 };

    /// Which fixed-width keys go inline. An inline slot of a 27-byte key is 32 bytes, against the map's 21 per slot.
    const Inline = enum {
        /// Every stride, at most 2/3 full: the in-place merge, which copies no group state, so its index is the most
        /// it holds per group (up to three times the map's bytes at 27-byte keys).
        any,
        /// 9-byte keys only, at the map's own capacity: 16-byte slots, so never more bytes than the map would take. The
        /// merges that copy state, where the index sits beside the state.
        within_map,
    };

    fn init(allocator: std.mem.Allocator, tables: []const expr_agg.GroupTable, n: usize, mode: Inline) !MergeIndex {
        const n32: u32 = std.math.cast(u32, n) orelse return error.OutOfMemory;
        var stride: u8 = if (tables.len > 0) tables[0].fixed_stride else 0;
        for (tables) |gt| if (gt.fixed_stride != stride) {
            stride = 0;
        };
        if (mode == .within_map and stride != 9) stride = 0;
        // `std.HashMap.capacityForSize` at its default 80% load, for `within_map`.
        const load = std.hash_map.default_max_load_percentage;
        const map_cap = std.math.ceilPowerOfTwo(u64, @as(u64, n32) * 100 / load + 1) catch return error.OutOfMemory;
        const cap: u64 = switch (mode) {
            .any => @as(u64, n32) + n32 / 2 + 1,
            .within_map => map_cap,
        };
        return switch (stride) {
            9 => .{ .fixed9 = try .init(allocator, cap) },
            18 => .{ .fixed18 = try .init(allocator, cap) },
            27 => .{ .fixed27 = try .init(allocator, cap) },
            else => blk: {
                var map = KeyHashMap(u32).init(allocator);
                errdefer map.deinit();
                try map.ensureTotalCapacity(n32);
                break :blk .{ .bytes = map };
            },
        };
    }

    fn deinit(self: *MergeIndex) void {
        switch (self.*) {
            .bytes => |*m| m.deinit(),
            inline else => |*f| f.allocator.free(f.slots),
        }
    }

    /// `hash` is `std.hash.Wyhash.hash(0, key)`, the map's own hash. A new entry's value is the caller's to set.
    inline fn getOrPut(self: *MergeIndex, key: []const u8, hash: u64) Entry {
        switch (self.*) {
            .bytes => |*m| {
                const got = m.getOrPutAssumeCapacityAdapted(HashKey{ .bytes = key, .hash = hash }, HashKeyCtx{});
                if (!got.found_existing) got.key_ptr.* = key;
                return .{ .found = got.found_existing, .value = got.value_ptr };
            },
            inline else => |*f| return f.getOrPut(key, hash),
        }
    }
};

fn FixedMergeIndex(comptime stride: usize) type {
    return struct {
        allocator: std.mem.Allocator,
        slots: []Slot,

        /// 16, 24 or 32 bytes: a stride-9 slot is a quarter of a cache line.
        const Slot = extern struct { key: [stride]u8, id: u32 };
        const empty = std.math.maxInt(u32);

        /// At least `min_cap` slots, more than the keys it will hold, so a probe always ends at an empty slot.
        fn init(allocator: std.mem.Allocator, min_cap: u64) !@This() {
            const want = std.math.cast(usize, @max(16, min_cap)) orelse return error.OutOfMemory;
            const cap = std.math.ceilPowerOfTwo(usize, want) catch return error.OutOfMemory;
            const slots = try allocator.alloc(Slot, cap);
            for (slots) |*s| s.id = empty;
            return .{ .allocator = allocator, .slots = slots };
        }

        /// Never more keys than `init` was given, so an empty slot is always found. Ids are below that count, so none
        /// is `empty`.
        inline fn getOrPut(self: *@This(), key: []const u8, hash: u64) MergeIndex.Entry {
            std.debug.assert(key.len == stride);
            const mask = self.slots.len - 1;
            var i: usize = @intCast(hash & mask);
            while (true) : (i = (i + 1) & mask) {
                const s = &self.slots[i];
                if (s.id == empty) {
                    s.key = key[0..stride].*;
                    return .{ .found = false, .value = &s.id };
                }
                if (keyEql(&s.key, key)) return .{ .found = true, .value = &s.id };
            }
        }
    };
}

/// Fold `refs` into distinct groups. `slots` borrows keys from `tables`; `accs` owns any string winners moved out of
/// those tables. The map is sized for every ref, so a repeated key does not rehash it. `hashes`, when set, is the
/// Wyhash already computed for each ref (the same one `StringHashMap` uses) and is not computed again.
/// `in_place` (`mergesInPlace`): `accs` stays empty and each group's state is merged into its first ref's own
/// accumulators instead, which `slot.idx` names (`packRef`).
fn hashMergeGroupRefs(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ?ResultCut,
    in_place: bool,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    if (in_place) return mergeInPlace(gpa, lists, tables, refs, hashes, calls, slots);
    if (cut) |c| {
        if (c.ranking_first) return mergeResultCut(gpa, lists, tables, refs, hashes, calls, c, slots, accs);
        if (calls[c.a_idx].func == .count) {
            const count_cut: CountCut = .{ .a_idx = c.a_idx, .k = c.k };
            return mergeCountCut(gpa, lists, tables, refs, hashes, calls, count_cut, slots, accs);
        }
    }
    var index = try MergeIndex.init(lists, tables, refs.len, .within_map);
    defer index.deinit();
    try slots.ensureTotalCapacity(lists, refs.len);

    for (refs, 0..) |ref, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = gt.readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        if (gop.found) {
            const slot = &slots.items[gop.value.*];
            slot.first = @min(slot.first, gt.first.items[ref.group]);
            const dst = accs.items[slot.idx..][0..calls.len];
            for (dst, src) |*d, *s| try mergeOwnedAccumulator(d, s, gpa);
        } else {
            gop.value.* = @intCast(slots.items.len);
            const acc_at = accs.items.len;
            try accs.ensureUnusedCapacity(lists, calls.len);
            for (calls) |call| accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
            const dst = accs.items[acc_at..][0..calls.len];
            for (dst, src) |*d, *s| try mergeOwnedAccumulator(d, s, gpa);
            slots.appendAssumeCapacity(.{
                .key = key,
                .first = gt.first.items[ref.group],
                .idx = acc_at,
            });
        }
    }
    // A partition holds every copy of its keys, so its counts are final and a count cut applies here.
    if (cut) |c| try keepCountTop(gpa, lists, calls, .{ .a_idx = c.a_idx, .k = c.k }, slots, accs);
}

/// Whether a full merge may leave each group's state in the source table that first holds it (`mergeInPlace`): no
/// cut, which keeps and reorders merged state, and no count(DISTINCT), whose sets a read-only copy cannot merge.
fn mergesInPlace(calls: []const expr_agg.AggCall, cut: ?ResultCut) bool {
    if (cut != null) return false;
    for (calls) |call| if (call.func == .count_distinct) return false;
    return true;
}

/// `calls` with call `i`, a count(DISTINCT) whose every state `foldDistinctPairs` made a count, merged as the COUNT
/// it now is: a fresh count(DISTINCT) state takes a count as it is, so the merge's every value is the same.
fn countedCalls(arena: std.mem.Allocator, calls: []const expr_agg.AggCall, i: usize) ![]const expr_agg.AggCall {
    std.debug.assert(calls[i].func == .count_distinct);
    const out = try arena.dupe(expr_agg.AggCall, calls);
    out[i].func = .count;
    return out;
}

/// A merged-in-place slot's `idx`: its source table above its group.
fn packRef(ref: MergeGroupRef) usize {
    return @as(usize, ref.table) << 32 | ref.group;
}

fn inPlaceAccs(
    tables: []const expr_agg.GroupTable,
    idx: usize,
    n_calls: usize,
    scratch: *[16]expr_agg.Accumulator,
) []expr_agg.Accumulator {
    const g: usize = @as(u32, @truncate(idx));
    return tables[idx >> 32].readGroup(g, n_calls, scratch);
}

/// `acc` as the copying merge leaves it after merging it into a fresh accumulator. Merging into a fresh one neither
/// allocates nor frees (`gpa` goes unused), and a string winner stays borrowed. Applying it twice changes nothing: a
/// fresh sum adds into +0.0, so never ends -0.0.
fn foldedAcc(gpa: std.mem.Allocator, call: expr_agg.AggCall, acc: expr_agg.Accumulator) !expr_agg.Accumulator {
    // Only a count pairs folded merges in place (`countedCalls`); a fresh count(DISTINCT) state takes it as it is.
    if (call.func == .count_distinct) {
        std.debug.assert(acc == .count);
        return acc;
    }
    if (foldsToItself(acc)) return acc;
    var out = expr_agg.Accumulator.init(call);
    try out.merge(acc, gpa);
    return out;
}

/// Whether merging `acc` into a fresh accumulator gives every bit of it back. Not a float sum of -0.0 (it becomes
/// +0.0) or NaN (left to the addition), and not a count past i64 (`Accumulator.merge` refuses it); integer and
/// DECIMAL sums add into zero exactly, and an extremum or a string winner is taken as is.
fn foldsToItself(acc: expr_agg.Accumulator) bool {
    const plainFloat = struct {
        fn f(x: f64) bool {
            return !std.math.isNan(x) and !(x == 0 and std.math.signbit(x));
        }
    }.f;
    return switch (acc) {
        .count => |c| c <= std.math.maxInt(i64),
        .distinct => false,
        .sum_f => |x| if (x) |v| plainFloat(v) else true,
        .avg => |a| plainFloat(a.sum),
        .sum_i, .min_i, .min_f, .max_i, .max_f, .min_bytes, .max_bytes, .sum_d, .avg_d, .min_d, .max_d => true,
    };
}

/// `hashMergeGroupRefs` with no copy of a group's state: a key's first ref keeps it, and each later ref of the key is
/// merged into it there. Before each such merge the kept state is first made `foldedAcc` of itself, so the sequence of
/// merges, and every bit of the result, is the copying merge's; rows read a group seen once through `foldedAcc`. A key
/// is in one partition only, so no two threads write the same state.
fn mergeInPlace(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    slots: *std.ArrayList(MergedSlot),
) !void {
    var index = try MergeIndex.init(lists, tables, refs.len, .any);
    defer index.deinit();
    try slots.ensureTotalCapacity(lists, slots.items.len + refs.len);
    for (refs, 0..) |ref, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const first = gt.first.items[ref.group];
        if (!gop.found) {
            gop.value.* = @intCast(slots.items.len);
            slots.appendAssumeCapacity(.{ .key = key, .first = first, .idx = packRef(ref) });
            continue;
        }
        const slot = &slots.items[gop.value.*];
        slot.first = @min(slot.first, first);
        var dst_scratch: [16]expr_agg.Accumulator = undefined;
        const dst = inPlaceAccs(tables, slot.idx, calls.len, &dst_scratch);
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = gt.readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src, calls) |*d, *s, call| {
            d.* = try foldedAcc(gpa, call, d.*);
            try mergeOwnedAccumulator(d, s, gpa);
        }
        tables[slot.idx >> 32].writeGroup(@as(u32, @truncate(slot.idx)), dst);
    }
}

/// `hashMergeGroupRefs` under a cut by a COUNT: the counts are summed first, alone, to pick the `cut.k` groups the cut
/// keeps (largest count, earlier first appearance on a tie); only those groups' accumulators are then merged. A key
/// seen once (the common case on a near-unique key) costs a count and a position, not a merged copy of its state.
fn mergeCountCut(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: CountCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    var index = try MergeIndex.init(lists, tables, refs.len, .within_map);
    defer index.deinit();
    const Group = struct { count: u64, first: u64, key: []const u8 };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(lists);
    try groups.ensureTotalCapacity(lists, refs.len);
    const group_of = try lists.alloc(u32, refs.len);
    defer lists.free(group_of);
    for (refs, group_of, 0..) |ref, *gi, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const count = gt.readAcc(@as(usize, ref.group), cut.a_idx, calls.len).count;
        const first = gt.first.items[ref.group];
        if (gop.found) {
            const g = &groups.items[gop.value.*];
            g.count +|= count;
            g.first = @min(g.first, first);
        } else {
            gop.value.* = @intCast(groups.items.len);
            groups.appendAssumeCapacity(.{ .count = count, .first = first, .key = key });
        }
        gi.* = gop.value.*;
    }

    const Item = struct { c: u64, first: u64, g: u32 };
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    defer heap.deinit(lists);
    const k = @min(cut.k, groups.items.len);
    try heap.ensureTotalCapacity(lists, k);
    if (k > 0) for (groups.items, 0..) |g, gi| {
        const item: Item = .{ .c = g.count, .first = g.first, .g = @intCast(gi) };
        if (heap.count() == k) {
            const w = heap.peek().?;
            if (!(item.c > w.c or (item.c == w.c and item.first < w.first))) continue;
            _ = heap.pop();
        }
        heap.push(lists, item) catch unreachable; // within the capacity reserved above
    };

    // The kept groups' state, merged from just their refs.
    const none = std.math.maxInt(u32);
    const out_of = try lists.alloc(u32, groups.items.len);
    defer lists.free(out_of);
    @memset(out_of, none);
    try slots.ensureTotalCapacity(lists, slots.items.len + heap.count());
    try accs.ensureUnusedCapacity(lists, heap.count() * calls.len);
    for (heap.items) |item| {
        const g = groups.items[item.g];
        out_of[item.g] = @intCast(slots.items.len);
        slots.appendAssumeCapacity(.{ .key = g.key, .first = g.first, .idx = accs.items.len });
        for (calls) |call| accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
    }
    for (refs, group_of) |ref, gi| {
        const out = out_of[gi];
        if (out == none) continue;
        const dst = accs.items[slots.items[out].idx..][0..calls.len];
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = tables[ref.table].readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src) |*d, *sa| try mergeOwnedAccumulator(d, sa, gpa);
    }
}

/// Complete groups are ranked before any owning payload moves; each partition's top-H contains its global winners.
fn mergeResultCut(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    // Charged as the full merge's maps are, through `lists`: see the merge in `prepareAggregate`.
    var scratch: CutScratch = .{ .parent = cut.memory orelse lists, .shared = null };
    mergeResultCutInner(gpa, lists, tables, refs, hashes, calls, cut, &scratch, slots, accs) catch |e|
        return if (scratch.denied.load(.monotonic)) error.ExceededMemoryBudget else e;
}

fn mergeResultCutInner(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    scratch: *CutScratch,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    const sa = scratch.allocator();
    const ma = cut.memory orelse lists;
    var index = try MergeIndex.init(sa, tables, refs.len, .within_map);
    defer index.deinit();
    const Group = struct { rank: expr_agg.Accumulator, first: u64, key: []const u8 };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(sa);
    try groups.ensureTotalCapacity(sa, refs.len);
    const group_of = try sa.alloc(u32, refs.len);
    defer sa.free(group_of);
    for (refs, group_of, 0..) |ref, *gi, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const rank = gt.readAcc(@as(usize, ref.group), cut.a_idx, calls.len);
        const first = gt.first.items[ref.group];
        if (gop.found) {
            const g = &groups.items[gop.value.*];
            try g.rank.merge(rank, sa);
            g.first = @min(g.first, first);
        } else {
            gop.value.* = @intCast(groups.items.len);
            groups.appendAssumeCapacity(.{ .rank = rank, .first = first, .key = key });
        }
        gi.* = gop.value.*;
    }

    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(sa);
    const k = @min(cut.k, groups.items.len);
    try heap.ensureTotalCapacity(sa, k);
    for (groups.items, 0..) |g, gi| {
        const value = try rankValue(g.rank, calls[cut.a_idx]);
        pushRank(&heap, sa, k, .{ .value = value, .first = g.first, .g = @intCast(gi) });
    }

    // The kept groups' state, merged from just their refs.
    const none = std.math.maxInt(u32);
    const out_of = try sa.alloc(u32, groups.items.len);
    defer sa.free(out_of);
    @memset(out_of, none);
    try slots.ensureTotalCapacity(ma, slots.items.len + heap.count());
    try accs.ensureUnusedCapacity(ma, heap.count() * calls.len);
    for (heap.items) |item| {
        const g = groups.items[item.g];
        out_of[item.g] = @intCast(slots.items.len);
        slots.appendAssumeCapacity(.{ .key = g.key, .first = g.first, .idx = accs.items.len });
        for (calls, 0..) |call, i|
            accs.appendAssumeCapacity(if (i == cut.a_idx) g.rank else expr_agg.Accumulator.init(call));
    }
    for (refs, group_of) |ref, gi| {
        const out = out_of[gi];
        if (out == none) continue;
        const dst = accs.items[slots.items[out].idx..][0..calls.len];
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = tables[ref.table].readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src, 0..) |*d, *src_acc, i| if (i != cut.a_idx) try mergeOwnedAccumulator(d, src_acc, gpa);
    }
}

/// How many independent partitions the merge is worth. One partition below a few thousand refs, or with one worker:
/// the hash table is then smaller than the cost of splitting it. Otherwise a power of two, so a key's high bits name
/// its partition, and no larger than the worker count.
/// Bytes of merge state a full-merge partition aims for: a core's share of its chiplet's L3.
const merge_part_bytes = 4 << 20;
const merge_parts_max = 4096;

/// `entry_bytes`: what one reference may add to a partition's map, slots and accumulators.
fn mergePartitionCount(n_workers: usize, n_refs: usize, cut: bool, entry_bytes: usize) usize {
    if (n_workers < 2 or n_refs < 16) return 1;
    var p: usize = 1;
    while (p * 2 <= n_workers and p < 32) p *= 2;
    // Under a count cut each partition shrinks to its top groups as it finishes, so many small partitions keep only a
    // few in flight at once instead of half the groups. Even with small n_refs (e.g. after top-k candidate pruning),
    // each remaining group may hold large distinct sets that benefit from parallel workers.
    if (cut) {
        const cut_parts = std.math.ceilPowerOfTwoAssert(usize, n_refs / 65536 + 1);
        return @min(512, @max(p, cut_parts));
    }
    if (n_refs < 8192) return 1;
    // A full merge of near-unique keys holds about a group a reference: one partition a worker leaves each map far
    // larger than cache, and lookups miss. Sized by bytes instead, a partition's state stays cache-resident.
    const fit = std.math.ceilPowerOfTwoAssert(usize, (n_refs *| entry_bytes) / merge_part_bytes + 1);
    return @min(merge_parts_max, @max(p, fit));
}

/// What one merged reference may cost a partition when it starts a group: its map entry, its slot and its
/// accumulators.
fn mergeEntryBytes(n_calls: usize) usize {
    return @sizeOf([]const u8) + @sizeOf(u32) + 1 + @sizeOf(MergedSlot) + n_calls * @sizeOf(expr_agg.Accumulator);
}

const MergePart = struct {
    refs: []const MergeGroupRef,
    hashes: []const u64,
    slots: std.ArrayList(MergedSlot) = .empty,
    accs: std.ArrayList(expr_agg.Accumulator) = .empty,
    /// Where this partition's slots and accumulators start in the combined lists.
    slot_at: usize = 0,
    acc_at: usize = 0,
};

const MergeTask = struct {
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    cut: ?ResultCut,
    in_place: bool,
    parts: []MergePart,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    /// Claim partitions until none remain. A thread that never starts leaves its share on the cursor, so a failed
    /// spawn cannot drop a partition.
    fn run(t: *MergeTask) void {
        while (t.err == null) {
            const i = t.cursor.fetchAdd(1, .monotonic);
            if (i >= t.parts.len) return;
            const p = &t.parts[i];
            const slots = &p.slots;
            hashMergeGroupRefs(t.gpa, t.lists, t.tables, p.refs, p.hashes, t.calls, t.cut, t.in_place, slots, &p.accs) catch |e| {
                t.err = e;
                return;
            };
        }
    }
};

/// Several worker tables: hash-merge `refs`. Large merges split by the key hash and run a partition per worker; the
/// partitions are disjoint, so none writes a group another writes. `hashes` are Wyhash with seed 0, matching the map.
fn mergeFoldedRefs(
    gpa: std.mem.Allocator,
    /// The merge's own maps, lists and scratch; `gpa` holds the accumulators' values.
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    cut: ?ResultCut,
    in_place: bool,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    std.debug.assert(!in_place or mergesInPlace(calls, cut));
    const ma = if (cut) |c| c.memory orelse lists else lists;
    const split_arena = ma;
    const nparts = mergePartitionCount(n_workers, refs.len, cut != null, mergeEntryBytes(calls.len));
    if (nparts == 1) return hashMergeGroupRefs(gpa, ma, tables, refs, null, calls, cut, in_place, slots, accs);

    // Partitions come from the middle of the hash: the map inside each takes its fingerprint from the top bits and
    // its slot from the low ones, and a partition that fixed either would collide more.
    const mask: u64 = nparts - 1;
    const hashes = try split_arena.alloc(u64, refs.len);
    defer split_arena.free(hashes);
    const ordered = try split_arena.alloc(MergeGroupRef, refs.len);
    defer split_arena.free(ordered);
    const ordered_hash = try split_arena.alloc(u64, refs.len);
    defer split_arena.free(ordered_hash);
    const starts = try split_arena.alloc(usize, nparts + 1);
    defer split_arena.free(starts);
    // Hashed and split into partitions by chunks of refs on every worker: each chunk counts its own partitions, the
    // counts give each chunk where it writes in each, and each chunk scatters its own refs. A partition holds its refs
    // in input order, as one thread splitting them all would leave them.
    const n_chunks = @max(1, @min(n_workers, refs.len / 65536));
    const chunk_counts = try split_arena.alloc(usize, n_chunks * nparts);
    defer split_arena.free(chunk_counts);
    @memset(chunk_counts, 0);
    const Split = struct {
        tables: []const expr_agg.GroupTable,
        refs: []const MergeGroupRef,
        hashes: []u64,
        ordered: []MergeGroupRef,
        ordered_hash: []u64,
        counts: []usize,
        nparts: usize,
        mask: u64,
        n_chunks: usize,
        cursor: *std.atomic.Value(usize),
        phase: enum { count, scatter },

        /// Claim chunks until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
        fn run(t: *@This()) void {
            while (true) {
                const c = t.cursor.fetchAdd(1, .monotonic);
                if (c >= t.n_chunks) return;
                const lo = t.refs.len * c / t.n_chunks;
                const hi = t.refs.len * (c + 1) / t.n_chunks;
                const mine = t.counts[c * t.nparts ..][0..t.nparts];
                switch (t.phase) {
                    .count => for (t.refs[lo..hi], t.hashes[lo..hi]) |ref, *h| {
                        h.* = std.hash.Wyhash.hash(0, t.tables[ref.table].keyAt(ref.group));
                        mine[(h.* >> 32) & t.mask] += 1;
                    },
                    // `mine` now holds where this chunk writes next in each partition.
                    .scatter => for (t.refs[lo..hi], t.hashes[lo..hi]) |ref, h| {
                        const at = mine[(h >> 32) & t.mask];
                        t.ordered[at] = ref;
                        t.ordered_hash[at] = h;
                        mine[(h >> 32) & t.mask] = at + 1;
                    },
                }
            }
        }
    };
    var split_cursor = std.atomic.Value(usize).init(0);
    const splits = try split_arena.alloc(Split, n_chunks);
    defer split_arena.free(splits);
    const split_config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    for ([_]@TypeOf(splits[0].phase){ .count, .scatter }) |phase| {
        if (phase == .scatter) {
            // Partition p starts after every chunk's refs of the partitions before it; within p, chunk c after
            // chunks < c.
            var at: usize = 0;
            for (0..nparts) |p| {
                starts[p] = at;
                for (0..n_chunks) |c| {
                    const n = chunk_counts[c * nparts + p];
                    chunk_counts[c * nparts + p] = at;
                    at += n;
                }
            }
            starts[nparts] = at;
        }
        split_cursor.store(0, .monotonic);
        for (splits) |*t| t.* = .{
            .tables = tables,
            .refs = refs,
            .hashes = hashes,
            .ordered = ordered,
            .ordered_hash = ordered_hash,
            .counts = chunk_counts,
            .nparts = nparts,
            .mask = mask,
            .n_chunks = n_chunks,
            .cursor = &split_cursor,
            .phase = phase,
        };
        if (splits.len == 1 or !spawn_util.runTree(Split, splits, split_config, Split.run)) splits[0].run();
    }

    const parts = try split_arena.alloc(MergePart, nparts);
    defer split_arena.free(parts);
    for (parts, 0..) |*p, i| {
        p.* = .{
            .refs = ordered[starts[i]..starts[i + 1]],
            .hashes = ordered_hash[starts[i]..starts[i + 1]],
        };
    }
    defer for (parts) |*p| {
        freeOwnedAccumulatorStrings(gpa, p.accs.items);
        p.accs.deinit(ma);
        p.slots.deinit(ma);
    };

    const n_threads = @min(n_workers, nparts);
    const tasks = try split_arena.alloc(MergeTask, n_threads);
    defer split_arena.free(tasks);
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*task| task.* = .{
        .gpa = gpa,
        .lists = ma,
        .tables = tables,
        .calls = calls,
        .cut = cut,
        .in_place = in_place,
        .parts = parts,
        .cursor = &cursor,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (!spawn_util.runTree(MergeTask, tasks, config, MergeTask.run)) MergeTask.run(&tasks[0]);
    for (tasks) |task| if (task.err) |e| return e;

    // Each partition is copied to its place in the combined lists on every worker: on one thread the copy of a large
    // merge costs more than sorting it.
    var nslots: usize = slots.items.len;
    var naccs: usize = accs.items.len;
    for (parts) |*p| {
        p.slot_at = nslots;
        p.acc_at = naccs;
        nslots += p.slots.items.len;
        naccs += p.accs.items.len;
    }
    try slots.ensureTotalCapacity(ma, nslots);
    try accs.ensureTotalCapacity(ma, naccs);
    const copies = try split_arena.alloc(ConcatTask, n_threads);
    defer split_arena.free(copies);
    // Nothing fails from here on, so the lists never hold an entry no partition wrote.
    slots.items.len = nslots;
    accs.items.len = naccs;
    cursor.store(0, .monotonic);
    for (copies) |*c| c.* = .{
        .parts = parts,
        .slots = slots.items,
        .accs = accs.items,
        .in_place = in_place,
        .cursor = &cursor,
    };
    if (!spawn_util.runTree(ConcatTask, copies, config, ConcatTask.run)) ConcatTask.run(&copies[0]);
    // The combined list now owns these winners.
    for (parts) |*p| p.accs.clearRetainingCapacity();
}

/// Copies the partitions it claims to their place in the combined lists, offsetting each slot's accumulators.
const ConcatTask = struct {
    parts: []const MergePart,
    slots: []MergedSlot,
    accs: []expr_agg.Accumulator,
    /// Slots name their source groups (`packRef`), not merged accumulators, so none is offset.
    in_place: bool,
    cursor: *std.atomic.Value(usize),

    fn run(t: *ConcatTask) void {
        while (true) {
            const i = t.cursor.fetchAdd(1, .monotonic);
            if (i >= t.parts.len) return;
            const p = &t.parts[i];
            for (p.slots.items, t.slots[p.slot_at..][0..p.slots.items.len]) |s, *d| d.* = .{
                .key = s.key,
                .first = s.first,
                .idx = if (t.in_place) s.idx else s.idx + p.acc_at,
            };
            @memcpy(t.accs[p.acc_at..][0..p.accs.items.len], p.accs.items);
        }
    }
};

fn accCount(acc: expr_agg.Accumulator) u64 {
    return switch (acc) {
        .count => acc.count,
        .distinct => |d| @intCast(d.count()),
        else => 0,
    };
}

/// Group indices of the only rows a single table's `ORDER BY` one COUNT DESC LIMIT/OFFSET can keep. Null when that
/// is not the order, or when the limit does not drop anyone. Ties break by earlier `first`, matching the stable sort
/// the full merge would do.
/// An answer ordered by one COUNT, descending, and cut to its first `k` rows: the `k` groups with the largest counts,
/// earlier first appearance winning a tie, are all it can return.
const CountCut = struct { a_idx: usize, k: usize };

const ResultCut = struct {
    a_idx: usize,
    k: usize,
    descending: bool = true,
    ranking_first: bool = false,
    memory: ?std.mem.Allocator = null,
};

fn resultCutReason(
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
) Reason {
    if (countCut(outputs, calls, order) != null) return .result_cut;
    if (unordered) return .result_cut_unordered;
    if (order.keys.len != 1) return .result_cut_secondary_keys;
    const a = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return .result_cut_unsupported,
    };
    const call = calls[a];
    if (call.result == .f64 or (call.result == .avg_f64 and
        (call.arg == null or call.arg.?.typeOf() != .i64))) return .result_cut_float;
    const numeric = switch (call.func) {
        .sum, .min, .max => call.result == .i64 or call.result == .dec,
        .avg => call.result == .avg_dec or (call.result == .avg_f64 and call.arg != null and
            call.arg.?.typeOf() == .i64),
        else => false,
    };
    if (!numeric) return .result_cut_unsupported;
    // Finalizing a losing DECIMAL sum/mean can error. Until its lane is also reduced, keep the full path.
    for (calls, 0..) |payload, i| if (i != a and
        (payload.result == .avg_dec or (payload.result == .dec and payload.func == .sum)))
        return .result_cut_fallible_payload;
    return .result_cut;
}

fn resultCut(
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
) ?ResultCut {
    // COUNT's established paths, including unordered mode, remain unchanged.
    if (countCut(outputs, calls, order)) |c| return .{ .a_idx = c.a_idx, .k = c.k };
    const limit = order.limit orelse return null;
    if (order.keys.len == 0 or resultCutReason(outputs, calls, order, unordered) != .result_cut) return null;
    return .{
        .a_idx = outputs[order.keys[0].column].source.agg,
        .k = std.math.cast(usize, order.offset +| limit) orelse return null,
        .descending = order.keys[0].descending,
        .ranking_first = true,
    };
}

fn rankValue(acc: expr_agg.Accumulator, call: expr_agg.AggCall) !SortKey {
    return switch (acc) {
        .sum_i, .min_i, .max_i => |v| if (v) |i| .{ .int = i } else .none,
        // Match sortKey(.avg): rounding the exact integer sum once, then dividing, also defines its ties.
        .avg => |a| if (expr_agg.avgSum(a)) |sum|
            .{ .float = sum / @as(f64, @floatFromInt(a.count)) }
        else
            .none,
        .sum_d => |d| decimalRank(try d.result(), call),
        .avg_d => |d| decimalRank(try expr_agg.avgDecMean(call, d), call),
        .min_d, .max_d => |v| decimalRank(v, call),
        else => unreachable,
    };
}

fn decimalRank(v: ?i128, call: expr_agg.AggCall) SortKey {
    return if (v) |i| .{ .dec = .{ .v = i, .spec = expr_agg.resultDec(call) } } else .none;
}

const RankItem = struct { value: SortKey, first: u64, g: u32 };
fn worseRank(descending: bool, a: RankItem, b: RankItem) std.math.Order {
    const ord = orderSortKeys(a.value, b.value, descending);
    return if (ord != .eq) ord.invert() else std.math.order(b.first, a.first);
}
const RankHeap = std.PriorityQueue(RankItem, bool, worseRank);

fn pushRank(heap: *RankHeap, allocator: std.mem.Allocator, k: usize, item: RankItem) void {
    if (k == 0) return;
    if (heap.count() == k) {
        if (worseRank(heap.context, item, heap.peek().?) != .gt) return;
        _ = heap.pop();
    }
    heap.push(allocator, item) catch unreachable; // capacity reserved before ranking
}

/// Scratch lives only during a cut, so quota follows actual allocation capacities, including reallocation overlap.
/// Source tables and their distinct sets keep their original allocator and ownership throughout the ranking pass.
const CutScratch = struct {
    parent: std.mem.Allocator,
    shared: ?*expr_agg.SharedBudget,
    denied: std.atomic.Value(bool) = .init(false),
    live: std.atomic.Value(usize) = .init(0),

    fn allocator(self: *CutScratch) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = noResize, .remap = noRemap, .free = free } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *CutScratch = @ptrCast(@alignCast(ctx));
        const charged = if (self.shared) |b| b.draw(len) orelse {
            self.denied.store(true, .monotonic);
            return null;
        } else 0;
        const ptr = self.parent.rawAlloc(len, alignment, ret) orelse {
            if (self.shared) |b| b.release(charged);
            return null;
        };
        if (self.shared) |b| b.release(charged - len);
        _ = self.live.fetchAdd(len, .monotonic);
        return ptr;
    }

    fn finish(self: *CutScratch) void {
        if (self.shared) |b| b.release(self.live.load(.monotonic));
    }

    fn noResize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn noRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *CutScratch = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret);
        _ = self.live.fetchSub(memory.len, .monotonic);
        if (self.shared) |b| b.release(memory.len);
    }
};

fn resultLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
    memory: ?std.mem.Allocator,
) !?[]u32 {
    var cut = resultCut(outputs, calls, order, unordered) orelse return null;
    if (!cut.ranking_first) return countLimitGroups(arena, table, outputs, calls, order);
    cut.memory = memory;
    var scratch: CutScratch = .{ .parent = table.allocator, .shared = null };
    return rankLimitGroups(arena, table, calls, cut, &scratch) catch |e|
        return if (scratch.denied.load(.monotonic)) error.ExceededMemoryBudget else e;
}

fn rankLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    scratch: *CutScratch,
) !?[]u32 {
    if (table.groupCount() <= cut.k) return null;
    const sa = scratch.allocator();
    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(sa);
    const k = @min(cut.k, table.groupCount());
    try heap.ensureTotalCapacity(sa, k);
    for (0..table.groupCount()) |g| {
        // Validate every rank, including losers and LIMIT 0, before dropping groups.
        const value = try rankValue(table.readAcc(g, cut.a_idx, calls.len), calls[cut.a_idx]);
        pushRank(&heap, sa, k, .{ .value = value, .first = table.first.items[g], .g = @intCast(g) });
    }
    const ids = try (cut.memory orelse arena).alloc(u32, heap.count());
    for (ids, heap.items) |*id, item| id.* = item.g;
    return ids;
}

fn keepResultTop(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    if (slots.items.len <= cut.k) return;
    const ma = cut.memory orelse lists;
    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(ma);
    try heap.ensureTotalCapacity(ma, cut.k);
    for (slots.items, 0..) |s, i| pushRank(&heap, ma, cut.k, .{
        .value = try rankValue(accs.items[s.idx + cut.a_idx], calls[cut.a_idx]),
        .first = s.first,
        .g = @intCast(i),
    });
    const kept = try ma.alloc(expr_agg.Accumulator, cut.k * calls.len);
    defer ma.free(kept);
    const keep = try ma.alloc(bool, slots.items.len);
    defer ma.free(keep);
    @memset(keep, false);
    for (heap.items) |item| keep[item.g] = true;
    var n: usize = 0;
    for (slots.items, keep) |s, kp| {
        const group = accs.items[s.idx..][0..calls.len];
        if (!kp) {
            freeOwnedAccumulatorStrings(gpa, group);
            continue;
        }
        @memcpy(kept[n * calls.len ..][0..calls.len], group);
        slots.items[n] = .{ .key = s.key, .first = s.first, .idx = n * calls.len };
        n += 1;
    }
    slots.shrinkRetainingCapacity(n);
    accs.clearRetainingCapacity();
    accs.appendSliceAssumeCapacity(kept);
}

fn countCut(outputs: []const BoundOutputColumn, agg_calls: []const expr_agg.AggCall, order: expr_ast.ResultOrder) ?CountCut {
    const limit = order.limit orelse return null;
    if (order.keys.len != 1 or !order.keys[0].descending) return null;
    const a_idx = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (agg_calls[a_idx].func != .count and agg_calls[a_idx].func != .count_distinct) return null;
    return .{ .a_idx = a_idx, .k = std.math.cast(usize, order.offset +| limit) orelse return null };
}

/// Drop every merged group but the `cut.k` a count cut keeps (`CountCut`), freeing what the dropped ones own. The
/// counts are final: a merge partition holds every copy of its keys.
fn keepCountTop(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    cut: CountCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    const n = slots.items.len;
    if (n <= cut.k) return;
    if (cut.k == 0) {
        freeOwnedAccumulatorStrings(gpa, accs.items);
        accs.clearRetainingCapacity();
        slots.clearRetainingCapacity();
        return;
    }
    const Item = struct { c: u64, first: u64, s: u32 };
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    defer heap.deinit(lists);
    try heap.ensureTotalCapacity(lists, cut.k);
    const kept_accs = try lists.alloc(expr_agg.Accumulator, cut.k * calls.len);
    defer lists.free(kept_accs);
    for (slots.items, 0..) |slot, si| {
        const item: Item = .{ .c = accCount(accs.items[slot.idx + cut.a_idx]), .first = slot.first, .s = @intCast(si) };
        if (heap.count() == cut.k) {
            const w = heap.peek().?;
            if (!(item.c > w.c or (item.c == w.c and item.first < w.first))) continue;
            _ = heap.pop();
        }
        heap.push(lists, item) catch unreachable; // within the capacity reserved above
    }
    // Nothing fails from here on: the kept groups' state moves, the rest is freed.
    const keep = try lists.alloc(bool, n);
    defer lists.free(keep);
    @memset(keep, false);
    for (heap.items) |item| keep[item.s] = true;
    var kept: usize = 0;
    for (slots.items, keep) |slot, kp| {
        const group = accs.items[slot.idx..][0..calls.len];
        if (!kp) {
            freeOwnedAccumulatorStrings(gpa, group);
            continue;
        }
        @memcpy(kept_accs[kept * calls.len ..][0..calls.len], group);
        slots.items[kept] = .{ .key = slot.key, .first = slot.first, .idx = kept * calls.len };
        kept += 1;
    }
    slots.shrinkRetainingCapacity(kept);
    accs.clearRetainingCapacity();
    accs.appendSliceAssumeCapacity(kept_accs[0 .. kept * calls.len]);
}

fn countLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
) error{OutOfMemory}!?[]u32 {
    const cut = countCut(outputs, agg_calls, order) orelse return null;
    const a_idx = cut.a_idx;
    const k = cut.k;
    if (k == 0) return &.{};
    const n = table.groupCount();
    if (n <= k) return null;

    const stride = agg_calls.len;
    const Item = struct { c: u64, first: u64, g: u32 };
    // The worst kept group is the smallest count, and within that the latest first appearance. It sits at the
    // front of the queue so a better group can replace it.
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    try heap.ensureTotalCapacity(arena, k);
    for (0..n) |g| {
        const item: Item = .{
            .c = accCount(table.readAcc(g, a_idx, stride)),
            .first = table.first.items[g],
            .g = @intCast(g),
        };
        if (heap.count() == k) {
            const worst = heap.peek().?;
            const better = item.c > worst.c or
                (item.c == worst.c and item.first < worst.first);
            if (!better) continue;
            _ = heap.pop();
        }
        heap.push(arena, item) catch unreachable; // within the capacity reserved above
    }
    const ids = try arena.alloc(u32, heap.count());
    for (ids, heap.items) |*id, item| id.* = item.g;
    return ids;
}

/// The keys of the only groups a LIMIT with no ORDER BY can return: groups come out in first-appearance order, so it
/// keeps the `k` that appeared first. Each of those is among the `k` earliest groups of the table that first saw it,
/// since any group that table saw earlier appeared earlier overall, so the union of every table's `k` earliest holds
/// them all. Null when that would not shrink the merge.
fn firstCandidates(arena: std.mem.Allocator, tables: []const expr_agg.GroupTable, k: usize) error{OutOfMemory}!?[]const []const u8 {
    if (k == 0) return &.{};
    var groups: usize = 0;
    for (tables) |t| groups += t.groupCount();
    if (groups <= k *| 4) return null;
    const Item = struct { first: u64, g: u32 };
    const later = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, later) = .empty;
    try heap.ensureTotalCapacity(arena, k);
    var kept: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (tables) |*t| {
        heap.clearRetainingCapacity();
        for (t.first.items[0..t.groupCount()], 0..) |first, g| {
            if (heap.count() == k) {
                if (first >= heap.peek().?.first) continue;
                _ = heap.pop();
            }
            heap.push(arena, .{ .first = first, .g = @intCast(g) }) catch unreachable; // within the capacity reserved above
        }
        for (heap.items) |item| try kept.put(arena, t.keyAt(item.g), {});
    }
    return kept.keys();
}

/// The keys of the only groups that can reach the first `offset + limit` rows of an answer ordered by a COUNT,
/// descending, first; null when the order is not such a cut. Exact, after the threshold algorithm of distributed
/// top-k (Cao and Wang, 2004): every table nominates its own top groups, and the k-th largest exact total among the
/// nominees, `tau`, is a floor on the k-th largest total of all. A key whose total reaches `tau` across `m` tables has
/// at least `tau / m` in one of them, so the keys some table counts that high are all that can make the cut, ties at
/// the cut included; the rest are never merged or materialized. Typically a few hundred groups of millions survive.
fn premergeLargeDistinctSets(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    tables: []expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
) !void {
    if (tables.len <= 1 or n_workers <= 1) return;

    for (calls, 0..) |call, ci| {
        if (call.func != .count_distinct) continue;

        var large_keys: KeyHashMapUnmanaged(void) = .empty;
        defer large_keys.deinit(gpa);

        for (tables) |gt| {
            for (0..gt.groupCount()) |gi| {
                const acc = gt.accumulators.items[gi * calls.len + ci];
                if (acc == .distinct and acc.distinct.count() >= 16384) {
                    try large_keys.put(gpa, gt.keyAt(gi), {});
                }
            }
        }

        if (large_keys.count() == 0) continue;

        var it = large_keys.keyIterator();
        while (it.next()) |k_ptr| {
            const key = k_ptr.*;
            var sources: std.ArrayListUnmanaged(*const expr_agg.DistinctSet) = .empty;
            var is_u64 = true;

            for (tables) |*gt| {
                if (gt.findKey(key)) |gi| {
                    const acc = &gt.accumulators.items[gi * calls.len + ci];
                    if (acc.* != .distinct or acc.distinct.count() == 0) continue;
                    // A sibling that never spilled still holds its values inline; the union count needs them too.
                    const ds = try acc.distinct.ensureHeap(gpa);
                    try sources.append(arena, ds);
                    // Only sets of integers split by value here; a partition holding anything else keeps the whole set
                    // on the merge's own path.
                    const u64_only = struct {
                        fn of(d: *const expr_agg.DistinctSet) bool {
                            return d.val_set == .none or d.val_set == .inline_u64 or d.val_set == .u64;
                        }
                    }.of;
                    if (!u64_only(ds)) is_u64 = false;
                    if (ds.parts) |parts| for (parts) |*part| {
                        if (!u64_only(part)) is_u64 = false;
                    };
                }
            }

            if (!is_u64 or sources.items.len <= 1) continue;

            const total = try parallelDistinctCountU64(gpa, arena, sources.items, n_workers);

            var first = true;
            for (tables) |*gt| {
                if (gt.findKey(key)) |gi| {
                    const acc_ptr = &gt.accumulators.items[gi * calls.len + ci];
                    if (acc_ptr.* == .distinct) acc_ptr.distinct.deinit(gpa);
                    if (first) {
                        acc_ptr.* = .{ .count = total };
                        first = false;
                    } else {
                        acc_ptr.* = .{ .count = 0 };
                    }
                }
            }
        }
    }
}

fn topKCandidates(
    arena: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
) error{OutOfMemory}!?[]const []const u8 {
    const limit = order.limit orelse return null;
    if (order.keys.len == 0) return firstCandidates(arena, tables, std.math.cast(usize, order.offset +| limit) orelse return null);
    if (!order.keys[0].descending) return null;
    const a_idx = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (agg_calls[a_idx].func != .count and agg_calls[a_idx].func != .count_distinct) return null;
    const is_distinct = agg_calls[a_idx].func == .count_distinct;
    const k = std.math.cast(usize, order.offset +| limit) orelse return null;
    if (k == 0) return &.{};
    var groups: usize = 0;
    for (tables) |t| groups += t.groupCount();
    if (groups <= k *| 4) return null;

    const stride = agg_calls.len;
    const Count = struct {
        c: u64,
        g: u32,
        fn of(t: *const expr_agg.GroupTable, g: usize, at: usize, n: usize) u64 {
            return accCount(t.readAcc(g, at, n));
        }
        fn byCount(_: void, a: @This(), b: @This()) std.math.Order {
            return std.math.order(a.c, b.c);
        }
    };

    // Nominees: each table's own k largest counts, by key.
    var nominees: KeyHashMapUnmanaged(void) = .empty;
    var heap: std.PriorityQueue(Count, void, Count.byCount) = .empty;
    try heap.ensureTotalCapacity(arena, k);
    for (tables) |*t| {
        heap.clearRetainingCapacity();
        for (0..t.groupCount()) |g| {
            const entry: Count = .{ .c = Count.of(t, g, a_idx, stride), .g = @intCast(g) };
            if (heap.count() == k) {
                if (entry.c <= heap.peek().?.c) continue;
                _ = heap.pop();
            }
            heap.push(arena, entry) catch unreachable; // within the capacity reserved above
        }
        for (heap.items) |entry| try nominees.put(arena, t.keyAt(entry.g), {});
    }
    if (nominees.count() < k) return null;

    // tau: a lower bound on the k-th largest total of all.
    // For plain count, sum_i c_i is exact. For count_distinct, max_i c_i is a lower bound on the true distinct count.
    const totals = try arena.alloc(u64, nominees.count());
    var it = nominees.keyIterator();
    for (totals) |*total| {
        const key = it.next().?.*;
        total.* = 0;
        for (tables) |*t| if (t.findKey(key)) |g| {
            const c = Count.of(t, g, a_idx, stride);
            if (is_distinct) {
                total.* = @max(total.*, c);
            } else {
                total.* +|= c;
            }
        };
    }
    std.mem.sortUnstable(u64, totals, {}, std.sort.desc(u64));
    const tau = totals[k - 1];
    if (tau == 0) return null;

    const m: u64 = tables.len;
    // When most groups pass (counts nearly all equal), the plain merge is cheaper than collecting them.
    var passing: usize = 0;
    for (tables) |*t| {
        for (0..t.groupCount()) |g| passing += @intFromBool(Count.of(t, g, a_idx, stride) *| m >= tau);
    }
    if (passing > groups / 4) return null;
    var kept: std.StringArrayHashMapUnmanaged(void) = .empty;
    try kept.ensureTotalCapacity(arena, passing);
    for (tables) |*t| {
        for (0..t.groupCount()) |g| {
            if (Count.of(t, g, a_idx, stride) *| m >= tau) kept.putAssumeCapacity(t.keyAt(g), {});
        }
    }
    // Candidate merging probes every table for the first position and each aggregate, plus ref construction.
    // Leave broad sets to the parallel count-cut merge before doing another serial DISTINCT pass.
    if ((kept.count() *| tables.len) *| (agg_calls.len +| 2) > groups / 2) return null;
    if (is_distinct) {
        // Second-stage filter for distinct: sum_i c_i is an exact upper bound on the union cardinality.
        // Any key where sum_i c_i < tau provably cannot reach tau.
        var i: usize = 0;
        while (i < kept.count()) {
            const key = kept.keys()[i];
            var sum_c: u64 = 0;
            for (tables) |*t| if (t.findKey(key)) |g| {
                sum_c +|= Count.of(t, g, a_idx, stride);
            };
            if (sum_c < tau) {
                _ = kept.swapRemove(key);
            } else {
                i += 1;
            }
        }
    }
    return kept.keys();
}

fn mergeDistinctPart(
    dst: *expr_agg.DistinctSet,
    src: *expr_agg.DistinctSet,
    allocator: std.mem.Allocator,
) !void {
    if (src.count() == 0) {
        src.deinit(allocator);
        src.* = .{ .shared = dst.shared };
        return;
    }
    if (dst.count() == 0) {
        dst.* = src.*;
        src.* = .{ .shared = dst.shared };
        return;
    }
    if (dst.count() < src.count()) {
        const tmp = dst.*;
        dst.* = src.*;
        src.* = tmp;
    }
    defer {
        src.deinit(allocator);
        src.* = .{ .shared = dst.shared };
    }
    return dst.merge(src, allocator);
}

const DistinctMergeTask = struct {
    gpa: std.mem.Allocator,
    dst_parts: []expr_agg.DistinctSet,
    work_items: []const WorkItem,
    agg_idx: usize,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    fn run(t: *DistinctMergeTask) void {
        while (t.err == null) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= t.dst_parts.len) return;
            const dst_part = &t.dst_parts[p];
            for (t.work_items) |item| {
                if (t.agg_idx < item.agg_start or t.agg_idx >= item.agg_start + item.agg_len) continue;
                const src_acc = &item.accumulators[t.agg_idx - item.agg_start];
                if (src_acc.* == .distinct) if (src_acc.distinct.heap()) |ds| {
                    if (ds.parts) |src_parts| {
                        const src_part = &src_parts[p];
                        mergeDistinctPart(dst_part, src_part, t.gpa) catch |e| {
                            t.err = e;
                            return;
                        };
                    }
                };
            }
        }
    }
};

/// Merge an accumulator while transferring ownership of any string winner
/// out of the source. `Accumulator.merge` intentionally consumes its
/// source's owned string; clear the source so its owner (a GroupTable, a
/// work item) cannot free it a second time.
fn mergeAccOwned(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, allocator: std.mem.Allocator) anyerror!void {
    return mergeOwnedAccumulator(dst, src, allocator);
}

fn detachOwned(acc: *expr_agg.Accumulator) void {
    switch (acc.*) {
        .min_bytes => acc.* = .{ .min_bytes = null },
        .max_bytes => acc.* = .{ .max_bytes = null },
        .distinct => acc.* = .{ .distinct = .{} },
        else => {},
    }
}

/// Move each partition's groups into `slots` / `accs`. Keys stay borrowed from the partition tables.
fn fillFromPartitions(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    arena: std.mem.Allocator,
    parts: []partagg.Part,
    calls: []const expr_agg.AggCall,
    outputs: []const BoundOutputColumn,
    order: expr_ast.ResultOrder,
    unordered: bool,
    result_cut: ?ResultCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    _ = gpa;
    const ma = if (result_cut) |c| c.memory orelse lists else lists;
    var total: usize = 0;
    for (parts) |*p| total += if (resultCut(outputs, calls, order, unordered) != null)
        @min(p.table.groupCount(), order.limit.? +| order.offset)
    else
        p.table.groupCount();
    try slots.ensureTotalCapacity(ma, total);
    try accs.ensureTotalCapacity(ma, total * calls.len);
    const len = calls.len;
    for (parts) |*p| {
        const gt = &p.table;
        const cut = try resultLimitGroups(
            arena,
            gt,
            outputs,
            calls,
            order,
            unordered,
            if (result_cut) |c| c.memory else null,
        );
        defer if (cut) |ids| if (result_cut != null and result_cut.?.memory != null) ma.free(ids);
        const n = if (cut) |ids| ids.len else gt.groupCount();
        for (0..n) |i| {
            const g = if (cut) |ids| ids[i] else i;
            var src_scratch: [16]expr_agg.Accumulator = undefined;
            const src = gt.readGroup(g, len, &src_scratch);
            const idx = accs.items.len;
            try accs.appendSlice(ma, src);
            for (src) |*a| detachOwned(a);
            slots.appendAssumeCapacity(.{
                .key = gt.keyAt(g),
                .first = gt.first.items[g],
                .idx = idx,
            });
        }
    }
}

fn mergeOwnedAccumulator(
    dst: *expr_agg.Accumulator,
    src: *expr_agg.Accumulator,
    allocator: std.mem.Allocator,
) !void {
    if (src.* == .count and dst.* == .count) {
        dst.count += src.count;
        return;
    }
    if (src.* == .count and dst.* == .distinct) {
        const n = dst.distinct.count() + src.count;
        dst.distinct.deinit(allocator);
        dst.* = .{ .count = n };
        return;
    }
    if (src.* == .distinct and dst.* == .count) {
        dst.count += src.distinct.count();
        src.distinct.deinit(allocator);
        src.* = .{ .distinct = .{} };
        return;
    }
    if (src.* == .distinct and dst.* == .distinct) {
        // The larger spilled set is handed over whole and the smaller folded into it.
        return dst.distinct.takeFrom(&src.distinct, allocator);
    }
    const moved = src.*;
    try dst.merge(moved, allocator);
    switch (src.*) {
        .min_bytes => src.* = .{ .min_bytes = null },
        .max_bytes => src.* = .{ .max_bytes = null },
        else => {},
    }
}

fn freeOwnedAccumulatorStrings(
    allocator: std.mem.Allocator,
    accumulators: []expr_agg.Accumulator,
) void {
    for (accumulators) |*acc| switch (acc.*) {
        .min_bytes => |s| if (s) |owned| {
            allocator.free(owned);
            acc.min_bytes = null;
        },
        .max_bytes => |s| if (s) |owned| {
            allocator.free(owned);
            acc.max_bytes = null;
        },
        .distinct => |*d| d.deinit(allocator),
        else => {},
    };
}

/// How one group-key column is framed in a serialized composite key.
///
/// These are the only three shapes `expr_agg.serializeRowKey` can write.
/// `evalGroupKeyExpr` builds key columns from the EXPRESSION type (three
/// lanes), and `serializeRowKey` widens BOOLEAN into the i64 lane so a
/// caller that skips eval still matches `deserializeKeyColumn`. Deriving
/// framing from the physical Parquet type instead let the two disagree: a
/// BOOLEAN column evaluates to i64 (8 bytes) while the physical type said
/// `boolean` (1 byte) — silently truncating every later column in a
/// composite key.
/// `u64` frames exactly like `i64`; only the reported value differs, read as the unsigned column it came from.
/// `dec` frames 16 bytes, an exact DECIMAL key's unscaled value, reported at its type.
const KeyType = union(enum) { i64, u64, f64, string, dec: expr_ast.Dec };

/// The names a query's GROUP BY keys go by, each worked out once on first use: a column key's label walks the
/// schema, and binding output columns asks for every key's names once per output column.
const KeyNames = struct {
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    items: []const expr_ast.SelectItem,
    labels: []?[]const u8,
    sources: []?[]const u8,
    /// SQL names its result columns from the SELECT list; an unaliased expression key it does not select has no name
    /// to give and needs none.
    sql: bool = false,

    fn init(arena: std.mem.Allocator, meta: *const schema.FileMetaData, items: []const expr_ast.SelectItem) !KeyNames {
        const labels = try arena.alloc(?[]const u8, items.len);
        @memset(labels, null);
        const sources = try arena.alloc(?[]const u8, items.len);
        @memset(sources, null);
        return .{ .arena = arena, .meta = meta, .items = items, .labels = labels, .sources = sources };
    }

    /// Key `k`'s output label: its alias, else its column's label. A key that is neither needs an alias.
    fn label(self: *KeyNames, k: usize) Error![]const u8 {
        if (self.labels[k]) |l| return l;
        const item = self.items[k];
        const l = item.alias orelse switch (item.expr) {
            .col_ref => |ref| try leafLabel(self.arena, self.meta, ref.col_idx),
            else => if (self.sql) "" else return error.GroupKeyAliasRequired,
        };
        self.labels[k] = l;
        return l;
    }

    /// The label of the column a bare column key reads; `label(k)` for any other key, and for an unaliased column key,
    /// whose label already is its column's.
    fn source(self: *KeyNames, k: usize) Error![]const u8 {
        if (self.sources[k]) |src| return src;
        const item = self.items[k];
        const src = if (item.expr != .col_ref or item.alias == null)
            try self.label(k)
        else
            try leafLabel(self.arena, self.meta, item.expr.col_ref.col_idx);
        self.sources[k] = src;
        return src;
    }
};

fn splitColumnList(arena: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |part| {
        const name = std.mem.trim(u8, part, " \t\r\n");
        if (name.len > 0) try names.append(arena, name);
    }
    return names.items;
}

/// The GROUP BY output columns as written: the select list (SQL), else `column_order`, else every key then every
/// aggregate, each under its name.
fn groupOutputSpecs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    select_cols: ?[]const expr_ast.SelectColumn,
    column_order: ?[]const u8,
) ![]const expr_ast.SelectColumn {
    if (select_cols) |cols| {
        if (cols.len > 0) return cols;
    }
    if (column_order) |order| {
        const names = try splitColumnList(arena, order);
        if (names.len == 0) return error.UnknownColumn;
        const specs = try arena.alloc(expr_ast.SelectColumn, names.len);
        for (names, specs) |name, *spec| spec.* = .parse(name);
        return specs;
    }
    const specs = try arena.alloc(expr_ast.SelectColumn, keys.items.len + agg_calls.len);
    for (specs[0..keys.items.len], 0..) |*spec, k| spec.* = .{ .expr = try keys.label(k) };
    for (specs[keys.items.len..], agg_calls) |*spec, call| spec.* = .{ .expr = call.alias };
    return specs;
}

const ColSource = union(enum) {
    key: usize,
    agg: usize,
};

/// A GROUP BY output column, bound once per query: the key or aggregate that fills it and the name it prints under.
/// The JSON and Parquet outputs both take their columns from these.
const BoundOutputColumn = struct {
    source: ColSource,
    name: []const u8,
};

/// An unaliased aggregate is named after its function (`sum`), as it always has been, unless that name is taken by
/// another aggregate or a GROUP BY key; then it takes its call text (`sum(x)`), as DuckDB names it. So
/// `sum(x), sum(f)` gives `sum(x)` and `sum(f)`, while a lone `sum(x)` still prints as `sum`.
fn nameUnaliasedAggs(arena: std.mem.Allocator, agg_calls: []expr_agg.AggCall, keys: *KeyNames) Error!void {
    const taken = try arena.alloc(bool, agg_calls.len);
    for (agg_calls, taken, 0..) |call, *t, i| {
        t.* = false;
        if (call.default_name == null) continue;
        for (agg_calls, 0..) |other, j| {
            if (i != j and std.ascii.eqlIgnoreCase(call.alias, other.alias)) t.* = true;
        }
        for (0..keys.items.len) |k| {
            const label = try keys.label(k);
            const source = try keys.source(k);
            if (std.ascii.eqlIgnoreCase(call.alias, label) or std.ascii.eqlIgnoreCase(call.alias, source)) t.* = true;
        }
    }
    // Decided before renaming any, so one rename can't make another name look unique.
    for (agg_calls, taken) |*call, t| if (t) {
        call.alias = call.default_name.?;
    };
}

/// Reject two output columns sharing a name, compared case-insensitively: both would land under one output name.
/// Names the later of the pair. Every output that names its columns (aggregates, GROUP BY rows, `--select`) checks
/// through this one rule.
pub fn checkDistinctOutputNames(
    comptime T: type,
    items: []const T,
    comptime nameOf: fn (T) []const u8,
    diag: ?*Diag,
) error{DuplicateOutputColumn}!void {
    for (items, 0..) |item, i| for (items[0..i]) |prev| {
        if (std.ascii.eqlIgnoreCase(nameOf(item), nameOf(prev))) {
            Diag.setColumn(diag, nameOf(item));
            return error.DuplicateOutputColumn;
        }
    };
}

fn aggAlias(call: expr_agg.AggCall) []const u8 {
    return call.alias;
}

fn boundName(col: BoundOutputColumn) []const u8 {
    return col.name;
}

/// Reject two aggregates sharing an alias: both would land under one output name.
fn checkAggAliases(agg_calls: []const expr_agg.AggCall, diag: ?*Diag) Error!void {
    return checkDistinctOutputNames(expr_agg.AggCall, agg_calls, aggAlias, diag);
}

/// Bind the GROUP BY output columns and check their names up front, before any scanning: no aggregate alias twice,
/// no output name twice, and no name that matches both a key and an aggregate. A clash would otherwise surface as one
/// column silently showing another's values.
fn bindGroupOutputs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    meta: *const schema.FileMetaData,
    args: MultiAggArgs,
) ![]const BoundOutputColumn {
    try checkAggAliases(agg_calls, args.diag);
    for (keys.items, 0..) |item, k| if (item.alias == null and item.expr == .col_ref) {
        _ = keys.label(k) catch |err| {
            if (err == error.DuplicateOutputColumn) {
                const segments = (try metadata.leafPathSegments(arena, meta, item.expr.col_ref.col_idx)).?;
                Diag.setColumn(args.diag, try metadata.quotePath(arena, segments));
            }
            return err;
        };
    };
    if (sqlBound(args.select_cols)) return bindSqlOutputs(arena, keys, agg_calls, args);
    const specs = try groupOutputSpecs(arena, keys, agg_calls, args.select_cols, args.column_order);
    const bound = try arena.alloc(BoundOutputColumn, specs.len);
    for (specs, bound) |spec, *out| {
        const src = try resolveOutputColumn(keys, agg_calls, spec, args.diag);
        out.* = .{ .source = src, .name = try groupOutputName(keys, spec, src) };
    }
    try checkDistinctOutputNames(BoundOutputColumn, bound, boundName, args.diag);
    return bound;
}

/// Refuse a HAVING literal its result column cannot compare with, before any row is read.
fn checkHavingLanes(
    arena: std.mem.Allocator,
    having: expr_ast.Having,
    outputs: ?[]const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
) !void {
    const result_program = @import("result_program.zig");
    const Lane = result_program.Lane;
    const aggLane = struct {
        fn of(call: expr_agg.AggCall) Lane {
            return if (call.result == .bytes) .string else .number;
        }
    }.of;
    const lanes = if (outputs) |outs| blk: {
        const l = try arena.alloc(Lane, outs.len);
        for (outs, l) |o, *lane| lane.* = switch (o.source) {
            .key => |k| if (keyTypeFromExpr(keys.?[k]) == .string) .string else .number,
            .agg => |a| aggLane(agg_calls[a]),
        };
        break :blk l;
    } else blk: {
        const l = try arena.alloc(Lane, agg_calls.len);
        for (agg_calls, l) |call, *lane| lane.* = aggLane(call);
        break :blk l;
    };
    try result_program.checkHaving(having, lanes);
}

/// The cut a GROUP BY's order allows that reads its input twice, so a read policy can keep the input mapped for it:
/// a plain COUNT ranking first, descending (the sketch and census), a count(DISTINCT) likewise (its census), or a
/// LIMIT with no ORDER BY (first keys). Decided
/// from the query bound to `meta` exactly as the scan binds it, never from its text. A query that does not bind is
/// `.none` here; the scan reports why.
pub const CutShape = enum { none, count, count_distinct, first_keys };

pub fn cutShape(arena: std.mem.Allocator, meta: *const schema.FileMetaData, args: MultiAggArgs) CutShape {
    const order = args.result_order;
    const limit = order.limit orelse return .none;
    const gb = args.group_by orelse return .none;
    if (order.keys.len == 0) return if (order.offset +| limit > 0) .first_keys else .none;
    if (!order.keys[0].descending) return .none;
    return countFirst(arena, meta, args, gb) catch .none;
}

fn countFirst(arena: std.mem.Allocator, meta: *const schema.FileMetaData, args: MultiAggArgs, gb: []const u8) !CutShape {
    const calls: []expr_agg.AggCall = if (args.aggregate.len == 0)
        &.{}
    else
        try expr_parser.parseAggList(arena, args.aggregate, meta, null);
    const items = try expr_parser.parseGroupByOf(arena, gb, meta, null, args.select_cols orelse &.{});
    var keys = try KeyNames.init(arena, meta, items);
    keys.sql = sqlBound(args.select_cols);
    try nameUnaliasedAggs(arena, calls, &keys);
    var quiet = args;
    quiet.diag = null;
    const outputs = try bindGroupOutputs(arena, &keys, calls, meta, quiet);
    const first = args.result_order.keys[0].column;
    if (first >= outputs.len) return .none;
    return switch (outputs[first].source) {
        .agg => |a| switch (calls[a].func) {
            .count => .count,
            .count_distinct => .count_distinct,
            else => .none,
        },
        .key => .none,
    };
}

/// Whether `select_cols` are a SQL select list, which binds by role (`SelectColumn.Role`) rather than by name.
fn sqlBound(select_cols: ?[]const expr_ast.SelectColumn) bool {
    const cols = select_cols orelse return false;
    for (cols) |col| if (col.role != .named) return true;
    return false;
}

/// Bind a SQL select list, then the hidden ORDER BY terms (`ResultOrder.hidden`) after it, to slots: each aggregate
/// item is the next aggregate call, in order (the hidden ones are the calls after the SELECT list's); any other item
/// is the GROUP BY key it equals as a bound expression (`Expr.eql`: `"r"."v"` and `r.v` are one key exactly when
/// they bind one column). Visible names: an alias, else a column key's label, else the item as written. Hidden
/// columns have no name and are dropped before any output sees them.
fn bindSqlOutputs(
    arena: std.mem.Allocator,
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    args: MultiAggArgs,
) ![]const BoundOutputColumn {
    const visible = args.select_cols.?;
    const hidden = args.result_order.hidden;
    const bound = try arena.alloc(BoundOutputColumn, visible.len + hidden.len);
    var next_agg: usize = 0;
    for (bound, 0..) |*out, i| {
        const spec = if (i < visible.len) visible[i] else hidden[i - visible.len];
        const source: ColSource = switch (spec.role) {
            .agg => blk: {
                if (next_agg >= agg_calls.len) return error.EmptyAggregate;
                next_agg += 1;
                break :blk .{ .agg = next_agg - 1 };
            },
            .key, .named => blk: {
                const e = try expr_parser.parseExprOnly(arena, spec.expr, keys.meta, args.diag);
                for (keys.items, 0..) |item, k| if (item.expr.eql(e)) break :blk .{ .key = k };
                Diag.setColumn(args.diag, exprName(spec));
                return error.ColumnMustBeGrouped;
            },
        };
        const name: []const u8 = if (i >= visible.len) "" else if (spec.alias != null) aliasName(spec) else switch (source) {
            .key => |k| if (keys.items[k].expr == .col_ref) try keys.label(k) else exprName(spec),
            .agg => exprName(spec),
        };
        out.* = .{ .source = source, .name = name };
    }
    try checkDistinctOutputNames(BoundOutputColumn, bound[0..visible.len], boundName, args.diag);
    return bound;
}

/// The name output column `spec` prints under: an `AS` alias it gives; else a GROUP BY key's label, however `spec`
/// spells the key (`SELECT "r"."v"` and `--group-by v` both print `r.v`, see `metadata.leafLabel`); else the
/// aggregate's name as `spec` gives it.
fn groupOutputName(keys: *KeyNames, spec: expr_ast.SelectColumn, src: ColSource) Error![]const u8 {
    return switch (src) {
        .key => |k| if (spec.alias != null) aliasName(spec) else keys.label(k),
        .agg => aliasName(spec),
    };
}

/// Which GROUP BY key or aggregate an output column names. A key matches by its output label or, for a bare column
/// key, its source column path (`r.key` for a nested leaf); an aggregate by its alias. A name matching both is an
/// error rather than a guess: with `--group-by 's AS k'` and `... AS s`, the key used to win and `s` silently showed
/// the key's values.
fn resolveOutputColumn(
    keys: *KeyNames,
    agg_calls: []const expr_agg.AggCall,
    spec: expr_ast.SelectColumn,
    diag: ?*Diag,
) Error!ColSource {
    const expr_name = exprName(spec);
    const alias_name = aliasName(spec);
    // A key by the item's expression first, then by its alias: in `g AS k ... GROUP BY k, g` the column is `g`,
    // named `k`, and a match on the alias alone would show the key `k` instead.
    var key: ?usize = null;
    for ([_][]const u8{ expr_name, alias_name }) |name| {
        for (0..keys.items.len) |k_idx| {
            if (std.ascii.eqlIgnoreCase(name, try keys.label(k_idx)) or std.ascii.eqlIgnoreCase(name, try keys.source(k_idx))) {
                key = k_idx;
                break;
            }
        }
        if (key != null) break;
    }
    var agg: ?usize = null;
    for (agg_calls, 0..) |call, a_idx| {
        if (std.ascii.eqlIgnoreCase(alias_name, call.alias) or std.ascii.eqlIgnoreCase(expr_name, call.alias)) {
            agg = a_idx;
            break;
        }
        // An unaliased SQL select item (`count(*)`) names its aggregate by call text, whatever the alias became.
        if (call.default_name) |text| if (std.ascii.eqlIgnoreCase(expr_name, text)) {
            agg = a_idx;
            break;
        };
    }
    if (key == null and agg == null) {
        // A select-list column (SQL `SELECT b ... GROUP BY b`) names a bare key by any name that binds its column, as
        // the key itself was bound; its label may be a path (`r.b`) or quoted path the select list does not spell.
        if (metadata.resolveColumn(keys.meta, expr_name) catch null) |leaf| {
            for (keys.items, 0..) |item, k_idx| if (item.expr == .col_ref and item.expr.col_ref.col_idx == leaf) {
                key = k_idx;
                break;
            };
        }
    }
    if (key != null and agg != null) {
        // Name the aggregate alias: it is the clashing name whichever side of `AS` matched, and the one to rename.
        Diag.setColumn(diag, agg_calls[agg.?].alias);
        return error.AmbiguousOutputColumn;
    }
    if (key) |k| return .{ .key = k };
    if (agg) |a| return .{ .agg = a };
    Diag.setColumn(diag, alias_name);
    return error.UnknownColumn;
}

/// Framing for a group-key column, taken from the expression's own type.
///
/// Must agree with the lane `evalGroupKeyExpr` produces and `expr_agg.serializeRowKey` then writes: i32 and i64 both
/// serialize as an 8-byte i64, f32 and f64 as an 8-byte f64, so these three cover every column variant.
/// Deliberately does NOT consult the Parquet schema — a leaf index does not address it (see `leafLabel`).
/// An unsigned 64-bit column rides the i64 lane as raw bits, so a bare one is read back as `u64`: as `i64`, values of
/// 2^63 and up came out negative.
fn keyTypeFromExpr(expr: expr_ast.Expr) KeyType {
    if (expr == .col_ref and expr.col_ref.unsigned_64) return .u64;
    return switch (expr.typeOf()) {
        .i64 => .i64,
        .f64 => .f64,
        .str => .string,
        .dec => .{ .dec = expr.decSpec().? },
    };
}

/// The output column name for the `leaf_idx`-th primitive column: `metadata.leafLabel`, the name the row formats
/// print it under, so a nested leaf (`r.key`) is not labelled like a top-level column that shares its leaf name
/// (`key`) or its dotted path (a top-level `r.key`; the nested one is then `"r"."key"`).
///
/// `col_idx` counts LEAVES, but `meta.schema` is a flattened DFS including group nodes, so `leaf_idx + 1` is correct
/// only for files with no nested types. Used for output column NAMES; key framing must not depend on it.
fn leafLabel(arena: std.mem.Allocator, meta: *const schema.FileMetaData, leaf_idx: usize) Error![]const u8 {
    // Callers resolved `leaf_idx` from this same schema. Falling back to
    // another element would just print the wrong column name.
    return (try metadata.leafLabel(arena, meta, leaf_idx)) orelse
        std.debug.panic("leaf_idx {d} not in schema", .{leaf_idx});
}

/// The schema element for the `leaf_idx`-th primitive column, for the type an output column carries over from its
/// source. Names come from `leafLabel`; key framing must not depend on this.
fn leafSchemaElem(meta: *const schema.FileMetaData, leaf_idx: usize) schema.SchemaElement {
    // Callers resolved `leaf_idx` from this same schema.
    return metadata.leafSchemaElement(meta, leaf_idx) orelse
        std.debug.panic("leaf_idx {d} not in schema", .{leaf_idx});
}

/// The name an output column's expression gives: its text, unquoted unless it is a quoted path.
fn exprName(spec: expr_ast.SelectColumn) []const u8 {
    return stripQuotes(std.mem.trim(u8, spec.expr, " "));
}

/// The name an output column asks to print under: its alias, else its expression's name. An alias is the name as the
/// parser decoded it, spaces inside a quoted one included (`AS " n "` names ` n `); the flag form trims its own text.
fn aliasName(spec: expr_ast.SelectColumn) []const u8 {
    return spec.alias orelse exprName(spec);
}

/// A quoted path (`"a"."b"`, a nested leaf's label) is a name in its own right and keeps its quotes.
fn stripQuotes(name: []const u8) []const u8 {
    if (name.len >= 2 and name[0] == '"' and metadata.unquoteIdent(name).len == name.len) return name;
    if (name.len >= 2 and ((name[0] == '\'' and name[name.len - 1] == '\'') or (name[0] == '"' and name[name.len - 1] == '"'))) {
        return name[1 .. name.len - 1];
    }
    return name;
}

/// Pull one column out of a serialized composite group key: a 1-byte present flag, then what
/// `expr_agg.serializeRowKey` wrote for that lane. Bounds-checked rather than trusting the framing: a writer/reader
/// disagreement turns a length field into the previous column's payload bytes, panicking or allocating wildly.
pub fn deserializeKeyColumn(
    allocator: std.mem.Allocator,
    key_bytes: []const u8,
    target_idx: usize,
    key_types: []const KeyType,
) !AggValue {
    var cursor: usize = 0;
    var current_idx: usize = 0;
    while (current_idx <= target_idx) : (current_idx += 1) {
        if (current_idx >= key_types.len) return error.BadGroupKey;
        if (cursor >= key_bytes.len) return error.BadGroupKey;
        const is_present = key_bytes[cursor];
        cursor += 1;

        if (is_present == 0) {
            if (current_idx == target_idx) return .null_val;
            continue;
        }

        switch (key_types[current_idx]) {
            .i64, .u64, .f64 => {
                if (cursor + 8 > key_bytes.len) return error.BadGroupKey;
                const raw = std.mem.readInt(u64, key_bytes[cursor..][0..8], .little);
                cursor += 8;
                if (current_idx == target_idx) {
                    return switch (key_types[current_idx]) {
                        .i64 => .{ .i = @as(i64, @bitCast(raw)) },
                        .u64 => .{ .i = raw },
                        .f64 => .{ .f = @bitCast(raw) },
                        .string, .dec => unreachable,
                    };
                }
            },
            .dec => |spec| {
                if (cursor + 16 > key_bytes.len) return error.BadGroupKey;
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                if (current_idx == target_idx) return .{ .dec = .{ .v = v, .spec = spec } };
            },
            .string => {
                if (cursor + 4 > key_bytes.len) return error.BadGroupKey;
                const len = std.mem.readInt(u32, key_bytes[cursor..][0..4], .little);
                cursor += 4;
                if (cursor + len > key_bytes.len) return error.BadGroupKey;
                if (current_idx == target_idx) {
                    const buf = try allocator.alloc(u8, len);
                    @memcpy(buf, key_bytes[cursor .. cursor + len]);
                    return .{ .s = buf };
                }
                cursor += len;
            },
        }
    }
    return error.BadGroupKey;
}

fn decValue(v: ?i128, spec: expr_ast.Dec) AggValue {
    return if (v) |x| .{ .dec = .{ .v = x, .spec = spec } } else .null_val;
}

/// Convert one agg's final accumulator state into a wire-friendly value.
pub fn materializeOne(gpa: std.mem.Allocator, call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    return switch (call.func) {
        .count => .{ .i = @intCast(state.count) },
        .count_distinct => switch (state) {
            .count => |cnt| .{ .i = @intCast(cnt) },
            .distinct => |d| .{ .i = @intCast(d.count()) },
            else => .{ .i = 0 },
        },
        // i128 carries the full sum (incl. i64 overflow + unsigned-64);
        // the JSON layer prints it directly, no narrowing/overflow error.
        // sum/min/max over no values are NULL, never a 0 or "" stand-in.
        .sum => switch (call.result) {
            .i64 => if (state.sum_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.sum_f) |v| .{ .f = v } else .null_val,
            .dec => decValue(try state.sum_d.result(), expr_agg.resultDec(call)),
            .bytes, .avg_f64, .avg_dec => unreachable,
        },
        .min => switch (call.result) {
            .i64 => if (state.min_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.min_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.min_bytes) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .dec => decValue(state.min_d, expr_agg.resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .max => switch (call.result) {
            .i64 => if (state.max_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.max_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.max_bytes) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .dec => decValue(state.max_d, expr_agg.resultDec(call)),
            .avg_f64, .avg_dec => unreachable,
        },
        .avg => if (call.state)
            try avgStateValue(call, state)
        else if (call.result == .avg_dec)
            decValue(try expr_agg.avgDecMean(call, state.avg_d), expr_agg.resultDec(call))
        else if (expr_agg.avgMean(state.avg)) |m| .{ .f = m } else .null_val,
    };
}

/// avg_state's mergeable pair. A DECIMAL one keeps its mean at the argument's scale only to rank and filter by.
fn avgStateValue(call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    if (call.result != .avg_dec)
        return .{ .avg = .{ .sum = expr_agg.avgSum(state.avg), .count = @intCast(state.avg.count) } };
    return .{ .avg_dec = .{
        .sum = state.avg_d.wide(),
        .count = @intCast(state.avg_d.n),
        .mean = try state.avg_d.mean(),
        .spec = expr_agg.resultDec(call),
    } };
}

// ============================================================
// Compile-time API check. Behavioral coverage is via the CLI
// regression suite + lambda integration tests.
// ============================================================
/// Five GROUP BY rows (name, count, mean) covering NULL, NaN, -inf and ties, in first-appearance order.
fn testOrderRows(gpa: std.mem.Allocator) !AggOutput {
    const nan = std.math.nan(f64);
    const names = [_]?[]const u8{ "b", "a", null, "c", "a2" };
    const counts = [_]?i128{ 3, 5, 5, 3, null };
    const means = [_]?f64{ nan, 1.0, null, -std.math.inf(f64), 2.0 };
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, names.len);
    for (rows) |*r| r.* = &.{};
    out.group_rows = rows;
    for (rows, names, counts, means) |*row, name, count, mean| {
        const vals = try gpa.alloc(AggValue, 3);
        @memset(vals, .null_val);
        row.* = vals;
        // Duplicated before the union is assigned: a failed dupe must not leave a `.s` holding garbage.
        if (name) |n| {
            const dup = try gpa.dupe(u8, n);
            vals[0] = .{ .s = dup };
        }
        if (count) |n| vals[1] = .{ .i = n };
        if (mean) |m| vals[2] = .{ .f = m };
    }
    return out;
}

fn expectOrderedNames(order: expr_ast.ResultOrder, want: []const ?[]const u8) !void {
    const gpa = testing.allocator;
    var out = try testOrderRows(gpa);
    defer out.deinit(gpa);
    try out.applyResultOrder(gpa, order);
    const rows = out.group_rows.?;
    try testing.expectEqual(want.len, rows.len);
    for (rows, want) |row, name| {
        if (name) |n| try testing.expectEqualStrings(n, row[0].s) else try testing.expect(row[0] == .null_val);
    }
}

test "an ordered merge's waiting worker sleeps until the block before its window merges, and wakes on failure" {
    const gpa = testing.allocator;
    var tables: [3]expr_agg.GroupTable = .{ .init(gpa, 0), .init(gpa, 0), .init(gpa, 0) };
    defer for (&tables) |*t| t.deinit();
    var merged = expr_agg.GroupTable.init(gpa, 0);
    defer merged.deinit();
    var done: [3]std.atomic.Value(bool) = @splat(.init(false));
    var m: BlockMerge = .{ .tables = &tables, .merged = &merged, .calls = &.{}, .gpa = gpa, .done = &done, .ahead = 1 };
    const Waiter = struct {
        fn run(merge: *BlockMerge, b: usize, got: *std.atomic.Value(u8)) void {
            got.store(if (merge.awaitTurn(b)) 1 else 2, .release);
        }
    };
    try testing.expect(m.awaitTurn(0));
    var got: std.atomic.Value(u8) = .init(0);
    const t = try std.Thread.spawn(.{}, Waiter.run, .{ &m, 1, &got });
    // Block 1 may not start while block 0 is unmerged.
    for (0..20) |_| {
        try testing.io.sleep(.fromMilliseconds(1), .awake);
        try testing.expectEqual(@as(u8, 0), got.load(.acquire));
    }
    try m.finish(0);
    t.join();
    try testing.expectEqual(@as(u8, 1), got.load(.acquire));
    try testing.expectEqual(@as(usize, 1), m.next.load(.acquire));

    var failed: std.atomic.Value(u8) = .init(0);
    const t2 = try std.Thread.spawn(.{}, Waiter.run, .{ &m, 2, &failed });
    try testing.io.sleep(.fromMilliseconds(2), .awake);
    try testing.expectEqual(@as(u8, 0), failed.load(.acquire));
    m.fail();
    t2.join();
    try testing.expectEqual(@as(u8, 2), failed.load(.acquire));
}

test "applyResultOrder: NULLs last both ways, NaN above numbers, stable ties, offset then limit" {
    // count DESC, name: the two 5s tie on count and the NULL name goes last; the NULL count goes last despite DESC.
    try expectOrderedNames(.{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } } }, &.{ "a", null, "b", "c", "a2" });
    // count alone: ties keep first appearance.
    try expectOrderedNames(.{ .keys = &.{.{ .column = 1, .descending = false }} }, &.{ "b", "c", "a", null, "a2" });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = false }} }, &.{ "c", "a", "a2", "b", null });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = true }} }, &.{ "b", "a2", "a", "c", null });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = false }}, .limit = 2, .offset = 1 }, &.{ "a", "a2" });
    try expectOrderedNames(.{ .limit = 0 }, &.{});
    try expectOrderedNames(.{ .offset = 9 }, &.{});
    try expectOrderedNames(.{ .offset = 3, .limit = std.math.maxInt(u64) }, &.{ "c", "a2" });
}

test "applyResultOrder: an allocation failure leaves the rows whole and leaks nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try testOrderRows(gpa);
            defer out.deinit(gpa);
            const before = out.group_rows.?.len;
            out.applyResultOrder(gpa, .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 2 }) catch |err| {
                try testing.expectEqual(before, out.group_rows.?.len);
                return err;
            };
            try testing.expectEqual(@as(usize, 2), out.group_rows.?.len);
        }
    }.run, .{});
}

fn testTieRows(gpa: std.mem.Allocator, n: usize) !AggOutput {
    var prng = std.Random.DefaultPrng.init(42);
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, n);
    for (rows) |*r| r.* = &.{};
    out.group_rows = rows;
    for (rows, 0..) |*row, i| {
        const vals = try gpa.alloc(AggValue, 3);
        @memset(vals, .null_val);
        row.* = vals;
        vals[0] = .{ .i = @intCast(i) }; // where the row was, to compare orders
        const r = prng.random().uintLessThan(u32, 12);
        if (r != 11) vals[1] = .{ .i = r }; // many ties, some NULL
        vals[2] = .{ .f = @floatFromInt(prng.random().uintLessThan(u32, 3)) };
    }
    return out;
}

test "applyResultOrder: a short cut keeps the rows and order a full sort would" {
    const gpa = testing.allocator;
    for ([_][]const expr_ast.OrderKey{
        &.{.{ .column = 1, .descending = true }},
        &.{.{ .column = 1, .descending = false }},
        &.{ .{ .column = 2, .descending = true }, .{ .column = 1, .descending = false } },
    }) |keys| for ([_][2]u64{ .{ 10, 0 }, .{ 7, 30 }, .{ 1, 0 }, .{ 99, 100 } }) |lo| {
        var whole = try testTieRows(gpa, 500);
        defer whole.deinit(gpa);
        var cut = try testTieRows(gpa, 500);
        defer cut.deinit(gpa);
        try whole.applyResultOrder(gpa, .{ .keys = keys });
        try cut.applyResultOrder(gpa, .{ .keys = keys, .limit = lo[0], .offset = lo[1] });
        const want = whole.group_rows.?[lo[1]..][0..lo[0]];
        const got = cut.group_rows.?;
        try testing.expectEqual(want.len, got.len);
        for (want, got) |w, g| try testing.expectEqual(w[0].i, g[0].i);
    };
}

test "applyResultOrder: a short cut that fails to allocate leaves the rows whole and leaks nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try testTieRows(gpa, 40);
            defer out.deinit(gpa);
            out.applyResultOrder(gpa, .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 }) catch |err| {
                try testing.expectEqual(@as(usize, 40), out.group_rows.?.len);
                return err;
            };
            try testing.expectEqual(@as(usize, 3), out.group_rows.?.len);
        }
    }.run, .{});
}

test "scan: API is well-typed" {
    _ = runMultiAggregate;
    _ = materializeOne;
}

// ============================================================ Partial thread-spawn failure.
//
// `Thread.spawn` can fail partway through a loop (EAGAIN); leftover work must still run and started threads must still
// be joined. Driven through the seam in spawn.zig, since real spawn failure is not reproducible.
// ============================================================

const testing = std.testing;
const test_fixtures = @import("parquet/test_fixtures.zig");
const spawn_test_fixture = "data/parquet-testing/data/nan_in_stats.parquet";
/// The padding columns exist only to push fetch volume past the 2 MiB-per-worker threshold, so workers really spawn.
const SPAWN_FIXTURE_AGG =
    "count(*) AS n" ++
    ", sum(pad0) AS s0" ++ ", sum(pad1) AS s1" ++ ", sum(pad2) AS s2" ++
    ", sum(pad3) AS s3" ++ ", sum(pad4) AS s4" ++ ", sum(pad5) AS s5" ++
    ", sum(pad6) AS s6" ++ ", sum(pad7) AS s7" ++ ", sum(pad8) AS s8" ++
    ", sum(pad9) AS s9" ++ ", sum(pad10) AS s10";
/// Rows in `spawn_test_fixture` — proves a footer was really parsed rather than left as undefined memory.
const spawn_test_fixture_rows: i64 = 2;

/// 64 bytes that are definitively not parquet, so `metadata.open` rejects them at the magic check.
const not_parquet: [64]u8 = @splat('x');

test "parseFooters: every file is parsed even when spawns fail partway" {
    const bytes = metadata.readFileSlice(spawn_test_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{spawn_test_fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);

    const n_files = 8;
    const parse_threads = 4;
    // The last entry is the control: `parse_threads` successes means no injected failure at all.
    for ([_]usize{ 0, 1, parse_threads - 1, parse_threads }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        var meta_arenas: []std.heap.ArenaAllocator = &.{};
        defer for (meta_arenas) |*a| a.deinit();

        var inputs: [n_files]Input = undefined;
        for (&inputs) |*in| in.* = .{ .name = spawn_test_fixture, .bytes = bytes };

        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();

        const res = try parseFooters(
            arena_state.allocator(),
            testing.allocator,
            &inputs,
            parse_threads,
            &meta_arenas,
            null,
        );

        switch (res) {
            .failed => |f| {
                std.debug.print(
                    "fail_after={d}: unexpected parse failure at index {d} ({s})\n",
                    .{ fail_after, f.index, @errorName(f.err) },
                );
                return error.TestUnexpectedResult;
            },
            .ok => |parsed| {
                try testing.expectEqual(@as(usize, n_files), parsed.len);
                // A worker whose thread never started would leave its share of `parsed` untouched.
                for (parsed) |m| {
                    try testing.expectEqual(spawn_test_fixture_rows, m.num_rows);
                }
            },
        }
    }
}

test "parseFooters: lowest-index failure is reported whatever the spawn outcome" {
    const bytes = metadata.readFileSlice(spawn_test_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{spawn_test_fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);

    const n_files = 8;
    const parse_threads = 4;
    // Whichever worker claims which file, the reported index must be the lower of the two.
    const first_bad = 2;
    const second_bad = 5;

    for ([_]usize{ 0, 1, parse_threads - 1, parse_threads }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        var meta_arenas: []std.heap.ArenaAllocator = &.{};
        defer for (meta_arenas) |*a| a.deinit();

        var inputs: [n_files]Input = undefined;
        for (&inputs, 0..) |*in, i| in.* = .{
            .name = spawn_test_fixture,
            .bytes = if (i == first_bad or i == second_bad) &not_parquet else bytes,
        };

        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();

        const res = try parseFooters(
            arena_state.allocator(),
            testing.allocator,
            &inputs,
            parse_threads,
            &meta_arenas,
            null,
        );

        switch (res) {
            .ok => {
                std.debug.print("fail_after={d}: malformed inputs were not detected\n", .{fail_after});
                return error.TestUnexpectedResult;
            },
            .failed => |f| try testing.expectEqual(@as(usize, first_bad), f.index),
        }
    }
}

test "runMultiAggregate: injected spawn failures don't change the answer" {
    // The scan workers, unlike the footer parsers, are the loop whose old `try spawn` returned while earlier threads
    // still read the arena the caller frees. Sizing is by decode volume, so a small fixture would test nothing.
    const fixture = "data/bench_types.parquet";
    const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);

    const parallelism = 4;
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Answer = struct { sum: i128, rows: i64 };
    var baseline: ?Answer = null;
    for ([_]?usize{ null, 0, 1, parallelism - 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(id) AS s",
            .parallelism = parallelism,
            // Force a real decode; otherwise the sum is answered from statistics and no worker ever runs.
            .scan_all = true,
        });
        defer freeResult(res);

        try testing.expectEqual(@as(usize, 1), res.output.aggs.len);
        const got: Answer = .{ .sum = res.output.aggs[0].value.i, .rows = res.rows_in };
        // Work dropped by a failed spawn and never picked up inline would show as a short row count.
        try testing.expect(got.rows > 0);

        if (baseline) |b| {
            try testing.expectEqual(b.sum, got.sum);
            try testing.expectEqual(b.rows, got.rows);
            // Prove the run took the failure path; one worker means nothing was tested.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "spawn injection never fired (fail_after={?d}) — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline = got;
        }
    }
}

test "partagg: same GROUP BY answer as the per-worker tables at every -j" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Q = struct {
        aggregate: []const u8,
        group_by: []const u8,
        /// False: the query must stay on the per-worker tables (float merge, or count distinct).
        part: bool,
        cap: usize = 0,
        probe: u64 = 0,
    };
    const queries = [_]Q{
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm, min(ts) AS mn, max(ts) AS mx, avg(ts) AS a", .group_by = "b", .part = true },
        .{ .aggregate = "count(*) AS c, min(s) AS mn, max(s) AS mx", .group_by = "b", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "s", .part = true, .cap = 30, .probe = 50 },
        .{ .aggregate = "count(*) AS c, sum(ts) AS sm", .group_by = "n", .part = true, .cap = 40, .probe = 80 },
        .{ .aggregate = "sum(x) AS sx", .group_by = "b", .part = false },
        .{ .aggregate = "count(DISTINCT ts) AS d", .group_by = "b", .part = false },
    };
    for (queries) |q| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const base: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .scan_all = true,
        };
        const want = try runMultiAggregate(testing.allocator, arena_state.allocator(), base);
        defer freeResult(want);
        var args = base;
        args.partagg = true;
        args.part_cap = q.cap;
        args.part_probe = q.probe;
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        try testing.expectEqual(q.part, got.part_agg);
        expectSameAnswer(want, got) catch |err| {
            std.debug.print("partagg -j{d} {s} GROUP BY {s}\n", .{ j, q.aggregate, q.group_by });
            return err;
        };
    };
}

test "partagg: shared partitions fit the small GROUP BY budget" {
    // Round 6 repro: one worker plus 32 shared tables. Worker-only block sizing strands quota during the flush.
    const path = "ci/fixtures/parquet/partagg_budget.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .group_by = "k",
        .aggregate = "count(*) AS c",
        .partagg = true,
        .parallelism = 1,
        .max_memory = 256000,
    });
    defer freeResult(res);
    try testing.expect(res.part_agg);
    const rows = res.output.group_rows.?;
    try testing.expectEqual(@as(usize, 1000), rows.len);
    for (rows, 0..) |row, i| {
        try testing.expectEqual(@as(i64, @intCast(i)), row[0].i);
        try testing.expectEqual(@as(i64, 1), row[1].i);
    }
}

test "partagg: a spawn failure does not change the answer" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    // The baseline's accumulators borrow its arena, so that arena outlives the comparisons.
    var base_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer base_arena.deinit();
    const baseline = try runMultiAggregate(testing.allocator, base_arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
        .group_by = "n",
        .parallelism = 4,
        .min_bytes_per_worker = 1,
        .partagg = true,
        .part_cap = 40,
        .part_probe = 80,
        .scan_all = true,
    });
    defer freeResult(baseline);
    try testing.expect(baseline.part_agg);
    for ([_]usize{ 0, 1, 3 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(ts) AS sm, min(s) AS mn",
            .group_by = "n",
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .partagg = true,
            .part_cap = 40,
            .part_probe = 80,
            .scan_all = true,
        });
        defer freeResult(got);
        expectSameAnswer(baseline, got) catch |err| {
            std.debug.print("partagg spawn fail_after={d}\n", .{fail_after});
            return err;
        };
        if (spawn_util.failuresInjected() == 0) {
            std.debug.print("partagg spawn injection never fired (fail_after={d})\n", .{fail_after});
            return error.TestUnexpectedResult;
        }
    }
}

test "runMultiAggregate: skewed work does not let idle workers strand the budget" {
    // Blocks sized as a fraction of the whole budget rather than of each worker's share let fifteen barely-grouping
    // workers strand most of the pool at -j16. Admission cannot depend on worker count when live state fits the budget.
    const heavy = "ci/fixtures/parquet/spawn_budget_a.parquet";
    const light = "ci/fixtures/parquet/spawn_budget_b.parquet";
    var bytes: [2][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for ([_][]const u8{ heavy, light }, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [16]Input = undefined;
    inputs[0] = .{ .name = heavy, .bytes = bytes[0] };
    for (inputs[1..]) |*in| in.* = .{ .name = light, .bytes = bytes[1] };

    // The fixed budget includes modest headroom for the surviving groups and must be enough at any -j.
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key < 1001",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = 710000,
            // Per-worker tables: ordered float results need room for a block's partials too (the next test).
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: skewed work rejected a query with 10% headroom: {s}\n",
                .{ parallelism, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1001), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
}

test "runMultiAggregate: --max-memory means the same thing at every -j" {
    // One integer group in the fixed table: 16 slots, the 9-byte key, and the accumulator and first arrays at
    // their first growth step. That footprint has to be enough at -j1 and, because the tables share one budget,
    // at every larger -j too.
    const one_group = 1361;
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }
    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();

        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "group_key = 0",
            .aggregate = SPAWN_FIXTURE_AGG,
            .group_by = "group_key",
            .parallelism = parallelism,
            .max_memory = one_group,
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: a GROUP BY that fits {d} bytes at -j1 was rejected: {s}\n",
                .{ parallelism, one_group, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);
        try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
    }
    // Ordered float sums keep a block's partials beside the merged sums: twice one group's room is enough at every
    // -j. Integer results merge in any order, so they need no more than unordered (the min/max query below).
    for ([_]usize{ 1, 2, 4 }) |parallelism| {
        for ([_]struct { agg: []const u8, budget: usize }{
            .{ .agg = SPAWN_FIXTURE_AGG, .budget = one_group * 2 },
            .{ .agg = "count(*) AS n, min(group_key) AS lo, max(group_key) AS hi", .budget = 800 },
        }) |c| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = "group_key = 0",
                .aggregate = c.agg,
                .group_by = "group_key",
                .parallelism = parallelism,
                .max_memory = c.budget,
            }) catch |err| {
                std.debug.print("-j{d}: ordered {s} rejected at {d} bytes: {s}\n", .{
                    parallelism, c.agg[0..@min(c.agg.len, 24)], c.budget, @errorName(err),
                });
                return err;
            };
            defer freeResult(res);
            try testing.expectEqual(@as(usize, 1), (res.output.group_rows orelse return error.TestUnexpectedResult).len);
        }
    }
}

fn sameValue(a: AggValue, b: AggValue) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .s => |x| std.mem.eql(u8, x, b.s),
        else => std.meta.eql(a, b),
    };
}

test "topKCandidates: keeps every key whose total can reach the cut, even one no table nominates" {
    // Four tables. h0..h4 count 30 in each (120 in all); s0 and s1 count 26 in each (104), so a cut at 6 ends inside
    // their tie, and each table nominates only one of them; 200 tail keys count 1. The floor is 104, a quarter of it
    // per table: no table alone reaches 104 for any key, so the bound must be divided among the tables.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [16]u8 = undefined;
    for (&tables, 0..) |*t, ti| {
        const add = struct {
            fn add(table: *expr_agg.GroupTable, key: []const u8, n: u64, c: []const expr_agg.AggCall) !void {
                const g = try table.getOrInsert(key, c, 0);
                table.accumulators.items[g].count += n;
            }
        }.add;
        for (0..5) |h| try add(t, try std.fmt.bufPrint(&name_buf, "h{d}", .{h}), 30, &calls);
        // Each table meets the tied pair in a different order, so which one it nominates differs.
        const pair = [2][]const u8{ "s0", "s1" };
        try add(t, pair[ti % 2], 26, &calls);
        try add(t, pair[1 - ti % 2], 26, &calls);
        for (0..50) |i| try add(t, try std.fmt.bufPrint(&name_buf, "t{d}-{d}", .{ ti, i }), 1, &calls);
    }
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" } };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 6 };
    const kept = (try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, order)).?;
    try testing.expectEqual(@as(usize, 7), kept.len);
    for ([_][]const u8{ "h0", "h1", "h2", "h3", "h4", "s0", "s1" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
    // Ascending, a key column first, or no LIMIT: not a cut this applies to.
    for ([_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 6 },
        .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 6 },
        .{ .keys = &.{.{ .column = 1, .descending = true }} },
    }) |other| try testing.expect(try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, other) == null);
}

test "topKCandidates: dense candidate probes use the ordinary merge" {
    // A heavy table passes a quarter of all refs, but probing those keys across every table saves no merge work.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [32]u8 = undefined;
    for (&tables, 0..) |*t, ti| for (0..100) |i| {
        const key = try std.fmt.bufPrint(&name_buf, "{d}-{d}", .{ ti, i });
        const g = try t.getOrInsert(key, &calls, ti * 100 + i);
        t.accumulators.items[g].count = if (ti == 0) 10 else 1;
    };
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    try testing.expect(try topKCandidates(arena.allocator(), &tables, &outputs, &calls, order) == null);
}

test "topKCandidates: count_distinct keeps keys that can reach the cut and prunes rest" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [16]u8 = undefined;
    for (&tables, 0..) |*t, ti| {
        for (0..5) |h| {
            const key = try std.fmt.bufPrint(&name_buf, "h{d}", .{h});
            const g = try t.getOrInsert(key, &calls, 0);
            var slot = &t.accumulators.items[g].distinct;
            for (0..30) |v| try slot.insertU64(testing.allocator, @intCast(ti * 100 + v));
        }
        for (0..50) |i| {
            const key = try std.fmt.bufPrint(&name_buf, "t{d}-{d}", .{ ti, i });
            const g = try t.getOrInsert(key, &calls, 0);
            try t.accumulators.items[g].distinct.insertU64(testing.allocator, @intCast(i));
        }
    }
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "u" } };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 };
    const kept = (try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, order)).?;
    try testing.expectEqual(@as(usize, 5), kept.len);
    for ([_][]const u8{ "h0", "h1", "h2", "h3", "h4" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
}

test "mergeCandidates: a merge that fails part-way leaks none of the sets already moved out of the tables" {
    // Three tables share keys "a" and "b"; each key's count(DISTINCT) set is moved out of the first table and the
    // others are folded into it, which allocates, so a failure can land after a set has left its table.
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            const calls = [_]expr_agg.AggCall{
                .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
                .{ .func = .count_distinct, .arg = null, .where = null, .alias = "w", .result = .i64 },
            };
            var tables: [3]expr_agg.GroupTable = undefined;
            for (&tables) |*t| t.* = .init(gpa, 1 << 30);
            defer for (&tables) |*t| t.deinit();
            for (&tables, 0..) |*t, ti| for ([_][]const u8{ "a", "b" }) |key| {
                const g = try t.getOrInsert(key, &calls, 0);
                for (0..2) |ci| {
                    var slot = &t.accumulators.items[g * calls.len + ci].distinct;
                    for (0..40) |v| try slot.insertU64(gpa, @intCast(ti * 30 + v));
                }
            };
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(gpa);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer {
                freeOwnedAccumulatorStrings(gpa, accs.items);
                accs.deinit(gpa);
            }
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            try mergeCandidates(gpa, gpa, &tables, &.{ "a", "b" }, &calls, 1, &slots, &accs);
            try testing.expectEqual(@as(usize, 2), slots.items.len);
            for (accs.items) |acc| try testing.expectEqual(@as(usize, 100), acc.distinct.count());
        }
    }.run, .{});
}

test "mergeOwnedAccumulator: distinct sets merge whichever side is larger, own every value once, and leak nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn set(gpa: std.mem.Allocator, vals: []const u64) !*expr_agg.DistinctSet {
            const ds = try gpa.create(expr_agg.DistinctSet);
            ds.* = .{};
            errdefer {
                ds.deinit(gpa);
                gpa.destroy(ds);
            }
            for (vals) |v| try ds.insertU64(gpa, v);
            return ds;
        }
        fn run(gpa: std.mem.Allocator) !void {
            for ([_][2][]const u64{
                .{ &.{ 1, 2 }, &.{ 2, 3, 4, 5 } }, // theirs larger: handed over
                .{ &.{ 1, 2, 3, 4 }, &.{ 4, 5 } }, // ours larger
                .{ &.{}, &.{ 1, 2, 3, 4, 5 } },
            }) |c| {
                var dst: expr_agg.Accumulator = .{ .distinct = .{} };
                defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
                if (c[0].len > 0) dst = .{ .distinct = .fromSet(try set(gpa, c[0])) };
                var src: expr_agg.Accumulator = .{ .distinct = .fromSet(try set(gpa, c[1])) };
                defer freeOwnedAccumulatorStrings(gpa, (&src)[0..1]);
                try mergeOwnedAccumulator(&dst, &src, gpa);
                try testing.expect(src.distinct.heap() == null);
                try testing.expectEqual(@as(usize, 5), dst.distinct.count());
            }
            // Nothing seen on the source side: the destination is untouched.
            var dst: expr_agg.Accumulator = .{ .distinct = .fromSet(try set(gpa, &.{7})) };
            defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
            var none: expr_agg.Accumulator = .{ .distinct = .{} };
            try mergeOwnedAccumulator(&dst, &none, gpa);
            try testing.expectEqual(@as(usize, 1), dst.distinct.count());
        }
    }.run, .{});
}

test "count limit on one table is the stable top, ties included" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    const spec = [_]struct { key: []const u8, n: u64 }{
        .{ .key = "a", .n = 1 },
        .{ .key = "b", .n = 3 },
        .{ .key = "c", .n = 3 },
        .{ .key = "d", .n = 2 },
        .{ .key = "e", .n = 1 },
    };
    for (spec, 0..) |s, i| {
        const g = try table.getOrInsert(s.key, &calls, i);
        table.accumulators.items[g].count = s.n;
    }
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" },
        .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const top2: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 };
    const kept2 = (try countLimitGroups(arena, &table, &outputs, &calls, top2)).?;
    try testing.expectEqual(@as(usize, 2), kept2.len);
    var saw_b = false;
    var saw_c = false;
    for (kept2) |g| {
        saw_b = saw_b or std.mem.eql(u8, table.keyAt(g), "b");
        saw_c = saw_c or std.mem.eql(u8, table.keyAt(g), "c");
    }
    try testing.expect(saw_b and saw_c);

    // Every count ties: the cut is the earliest groups, which is what a stable sort keeps.
    for (table.accumulators.items) |*acc| acc.count = 1;
    const tied: expr_ast.ResultOrder = .{
        .keys = &.{.{ .column = 1, .descending = true }},
        .limit = 2,
        .offset = 1,
    };
    const kept_tie = (try countLimitGroups(arena, &table, &outputs, &calls, tied)).?;
    try testing.expectEqual(@as(usize, 3), kept_tie.len);
    var earliest: [3]bool = @splat(false);
    for (kept_tie) |g| earliest[g] = true;
    try testing.expect(earliest[0] and earliest[1] and earliest[2]);
    try testing.expect(try countLimitGroups(arena, &table, &outputs, &calls, .{ .keys = top2.keys }) == null);
}

test "DistinctMergeTask: partitioned distinct sets merge and handle allocation failure without leaks" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var dst: expr_agg.DistinctSet = .{};
            defer dst.deinit(gpa);
            try dst.initPartitions(gpa, null);

            var src1: expr_agg.DistinctSet = .{};
            defer src1.deinit(gpa);
            try src1.initPartitions(gpa, null);
            for ([_]u64{ 1, 2, 3, 100, 200, 300 }) |v| try src1.insertU64(gpa, v);

            var src2: expr_agg.DistinctSet = .{};
            defer src2.deinit(gpa);
            try src2.initPartitions(gpa, null);
            for ([_]u64{ 2, 3, 4, 200, 400 }) |v| try src2.insertU64(gpa, v);

            for (0..expr_agg.NUM_PARTS) |p| {
                try mergeDistinctPart(&dst.parts.?[p], &src1.parts.?[p], gpa);
                try mergeDistinctPart(&dst.parts.?[p], &src2.parts.?[p], gpa);
            }
            try testing.expectEqual(@as(usize, 8), dst.count());
        }
    }.run, .{});
}

test "runMultiAggregate: a COUNT-descending cut merges only groups that can make it, and answers the same" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Cut = struct { order: expr_ast.ResultOrder, prunes: bool };
    const cuts = [_]Cut{
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 }, .prunes = true },
        .{ .order = .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = true } }, .limit = 3, .offset = 5 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 }, .prunes = true },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 0 }, .prunes = true },
        // No ORDER BY: the first groups to appear.
        .{ .order = .{ .limit = 10 }, .prunes = true },
        .{ .order = .{ .limit = 4, .offset = 7 }, .prunes = true },
        // Not a COUNT-descending cut: merged in full.
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }} }, .prunes = false },
    };
    // `v` is unique per row: every count ties at 1, the threshold cannot prune, and the cut falls to the merge's
    // per-partition count heap (or the one-table heap), which must break the ties as the full sort does.
    for ([_][]const u8{ "k", "n", "v" }) |key| {
        for ([_]usize{ 1, 2, 4, 8 }) |parallelism| {
            for (cuts) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "max(v) AS hi" });
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = "count(*) AS c, min(v) AS lo, max(v) AS hi",
                    .group_by = key,
                    .select_cols = select_cols,
                    .parallelism = parallelism,
                };
                var whole = try runMultiAggregate(testing.allocator, arena, args);
                defer whole.deinit(testing.allocator);
                args.result_order = cut.order;
                var cut_res = try runMultiAggregate(testing.allocator, arena, args);
                defer cut_res.deinit(testing.allocator);

                const all_groups = whole.output.group_rows.?.len;
                const merged = cut_res.output.group_rows.?.len;
                // On unique keys only a cut by the COUNT alone shrinks the merge: a second key needs every tie.
                const prunes = cut.prunes and !(std.mem.eql(u8, key, "v") and cut.order.keys.len > 1);
                if (prunes) try testing.expect(merged < all_groups / 10) else try testing.expectEqual(all_groups, merged);
                try whole.output.applyResultOrder(testing.allocator, cut.order);
                try cut_res.output.applyResultOrder(testing.allocator, cut.order);
                const want = whole.output.group_rows.?;
                const got = cut_res.output.group_rows.?;
                try testing.expectEqual(want.len, got.len);
                for (want, got) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            }
        }
    }
}

test "runMultiAggregate: a sketched COUNT cut answers as the full scan, certified or not" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
        // Ties at the cut broken by the key, past an offset.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 4, .offset = 2 },
        // Not a COUNT-descending cut: never sketched.
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Few cells: collisions inflate the bound.
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .certify = false },
        // When the cells bail: the census riding along the count pass, then dropped early by a worker's cells, then
        // counting in a pass of its own, and none.
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = true, .census_check_rows = 64 },
        .{ .cell_bits = 8, .admit_per_row = 1, .fuse_census = false },
        .{ .cell_bits = 8, .admit_per_row = 1, .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    // `k` is skewed and has nulls, `n` is a fixed-width key, `v` is unique (every count ties at 1).
    for ([_][]const u8{ "k", "n", "v", "k, n" }) |key| for ([_]?[]const u8{ null, "n < 12" }) |filter| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const first_key = key[0 .. std.mem.indexOfScalar(u8, key, ',') orelse key.len];
            const select_cols = try selectList(arena, &.{ first_key, "count(*) AS c", "min(v) AS lo" });
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = filter,
                .aggregate = "count(*) AS c, min(v) AS lo",
                .group_by = key,
                .select_cols = if (std.mem.eql(u8, key, first_key)) select_cols else null,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_kept, got.rows_kept);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .ineligible, .bailed, .certified, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a census cut answers as the full scan, certified or not" {
    // Near-unique keys in shuffled order (tools/gen_census_fixture.py): the cells bail, and the census decides.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        // `u` and `s` tie at 1 inside the cut, `t` at 3.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        // Colliding fingerprints inflate every total: the census must not trust a cut they make up.
        .{ .fuse_census = true, .census_fp_bits = 3 },
        .{ .fuse_census = false, .census_fp_bits = 0 },
        .{ .census = false },
        // Each partition keeps only k fingerprints: the census must certify against the largest count it dropped, or
        // refuse, and collisions on top only make it refuse more.
        .{ .fuse_census = false, .admit_per_row = 1 },
        .{ .fuse_census = true, .admit_per_row = 1, .census_fp_bits = 6 },
    };
    var seen = std.EnumSet(Outcome){};
    // A count of a column, or under FILTER, can give the first keys to appear 0 (`n` is NULL through the first row
    // group): the census counts rows, an upper bound only.
    const counts = [_][]const u8{ "count(*) AS c", "count(n) AS c", "count(*) FILTER (WHERE n = 1) AS c" };
    for ([_][]const u8{ "u", "t", "s" }) |key| for ([_]?[]const u8{ null, "u > 300000000000" }) |filter| for (counts) |count| {
        for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, count, "min(s) AS lo", "sum(t) AS st" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, min(s) AS lo, sum(t) AS st", .{count});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .filter = if (filter != null and std.mem.eql(u8, key, "u")) filter else null,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try testing.expectEqual(want.rows_kept, got.rows_kept);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .bailed, .fell_back, .census }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census cut answers as the full scan, certified or not" {
    // tools/gen_census_fixture.py. By `t` four keys hold five rows and twelve hold three, so a cut of 10 ties at 3 and
    // each key's distinct `u` is its rows; by `u` three keys hold two rows, so every distinct count ties at the
    // census bound and nothing certifies; `s` is `u` as a string, hashed whole; `n` is NULL through the first row
    // group, so keys read only there count 0 distinct.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        // Ties broken by the key, which first appearance does not follow.
        .{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } }, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 30 },
    };
    const configs = [_]SketchTopK{
        .{},
        // Ten fingerprints admitted tie at the bound; four times as many certify.
        .{ .admit_per_row = 1 },
        // Colliding fingerprints inflate every bound, and with none left the admitted rows are most of the input.
        .{ .census_fp_bits = 3 },
        .{ .census_fp_bits = 0 },
        // Workers judge after 64 rows that the fullest cells hold too much, and abandon mid row group.
        .{ .census_check_rows = 64 },
        .{ .certify = false },
        .{ .census = false },
    };
    var seen = std.EnumSet(Outcome){};
    const distincts = [_][]const u8{ "count(DISTINCT u) AS d", "count(DISTINCT n) AS d", "count(DISTINCT t) AS d" };
    for ([_][]const u8{ "t", "u", "s" }) |key| for (distincts) |distinct| {
        if (std.mem.eql(u8, key, "t") == std.mem.eql(u8, distinct, "count(DISTINCT t) AS d")) continue;
        for ([_]usize{ 1, 3, 16 }) |parallelism| for (cuts) |cut| for (configs) |cfg_in| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            const select_cols = try selectList(arena, &.{ key, distinct, "count(*) AS c", "min(s) AS lo" });
            const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c, min(s) AS lo", .{distinct});
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = aggregate,
                .group_by = key,
                .select_cols = select_cols,
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            var cfg = cfg_in;
            cfg.outcome = &outcome;
            args.sketch_topk = cfg;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try testing.expectEqual(want.rows_in, got.rows_in);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        };
    };
    for ([_]Outcome{ .census, .fell_back, .bailed, .ineligible }) |o| try testing.expect(seen.contains(o));
}

test "runMultiAggregate: a count(DISTINCT) census never drops the error a computed argument raises" {
    // u = 368403641522 is the first row's, under a key of `t` seen once: a key the cut leaves out.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(DISTINCT 100 / (u - 368403641522)) AS d",
        "count(DISTINCT u) AS d, sum(100 / (u - 368403641522)) AS s",
    };
    for (aggregates) |aggregate| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expect(!ex.plan.dispatch.distinct_census);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
}

test "runMultiAggregate: count and first-keys cuts never drop the error a computed argument raises" {
    // u = 573290335422 is the last row's, under a key of `t` seen once: neither a key the count cut admits nor one in
    // the first keys' prefix.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const Case = struct { aggregate: []const u8, order: expr_ast.ResultOrder };
    const cases = [_]Case{
        .{ .aggregate = "count(*) AS c, sum(100 / (u - 573290335422)) AS s", .order = top },
        .{ .aggregate = "sum(100 / (u - 573290335422)) AS s", .order = .{ .limit = 3 } },
    };
    for (cases) |case| for ([_]?bool{ true, false }) |fuse| for ([_]usize{ 1, 3, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = case.aggregate,
            .group_by = "t",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
            .result_order = case.order,
            .sketch_topk = .{ .fuse_census = fuse },
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(.plain_hash, ex.plan.dispatch.strategy);
        try testing.expectError(error.DivisionByZero, runMultiAggregate(testing.allocator, arena, args));
    };
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

test "planAggregate: a count(DISTINCT) census needs a LIMIT HAVING leaves, a census that fits and a large domain" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const having: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "1" } } };
    var with_having = top;
    with_having.having = &having;
    const Case = struct { order: expr_ast.ResultOrder, max_memory: usize = 512 << 20, census: bool };
    const cases = [_]Case{
        .{ .order = top, .census = true },
        // The engine hands the scan HAVING's cut order (`ResultProgram.cutOrder`): no limit, so no cut.
        .{ .order = @import("result_program.zig").ResultProgram.cutOrder(with_having), .census = false },
        // A census of every row must fit a quarter of the budget, or the scan runs as usual.
        .{ .order = top, .max_memory = 40 << 10, .census = false },
    };
    for (cases) |case| {
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .select_cols = try selectList(arena, &.{ "t", "count(DISTINCT u) AS d" }),
            .result_order = case.order,
            .max_memory = case.max_memory,
            .sketch_topk = .{},
        };
        const ex = try explainAggregate(testing.allocator, arena, args);
        try testing.expectEqual(case.census, ex.plan.dispatch.distinct_census);
        try testing.expectEqual(case.census, ex.plan.reasons.contains(.census_distinct));
    }
    // A proven domain too small for the census to leave most rows out keeps the plain scan.
    const plan: ScanPlan = .{ .dispatch = .{
        .strategy = .sketch,
        .use_blocks = false,
        .tables = 4,
        .count_columns = true,
        .sketch_fits = true,
        .keys_unique = false,
        .filter_small = true,
        .count_agg = 0,
        .census_allowed = true,
        .census_fused = true,
        .distinct_census = true,
    } };
    const calls = [_]expr_agg.AggCall{.{ .func = .count_distinct, .alias = "d", .result = .i64, .arg = null, .where = null }};
    const in: PlanInputs = .{
        .args = .{ .inputs = &inputs, .aggregate = "", .result_order = top, .sketch_topk = .{} },
        .metas = &.{},
        .work = &.{},
        .calls = &calls,
        .keys = &.{},
        .outputs = null,
        .filter_cols = &.{},
        .workers = 4,
    };
    for ([_]u64{ 10_239, 10_240 }, [_]bool{ false, true }) |hi, kept| {
        var p = plan;
        applyCostRules(in, .{ .source = .dictionary_union, .lo = 0, .hi = hi, .rows = 1 << 24, .covered_rows = 1 << 24, .confidence = .upper_bound }, &p);
        try testing.expectEqual(kept, p.dispatch.distinct_census);
        try testing.expectEqual(kept, p.dispatch.strategy == .sketch);
    }
}

test "runMultiAggregate: behind heavy keys a census cut certifies on a short prefix, and answers as the full scan" {
    // tools/gen_census_fixture.py: three keys hold 4500 of 8000 rows, the rest are unique, so a cut of 10 ties at 1.
    const path = "ci/fixtures/parquet/census_heavy.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Outcome = SketchTopK.Outcome;
    var seen = std.EnumSet(Outcome){};
    const cuts = [_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4, .offset = 3 },
        // More than the short prefix can hold: it must widen to the sure one, not certify short.
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 400 },
    };
    for ([_][]const u8{ "h", "g" }) |key| for ([_]usize{ 1, 3 }) |parallelism| for (cuts) |cut| {
        for ([_]bool{ true, false }) |fuse| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .ndv_policy = false,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(v) AS sv",
                .group_by = key,
                .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "min(v) AS lo", "sum(v) AS sv" }),
                .parallelism = parallelism,
                .min_bytes_per_worker = 1,
                .result_order = cut,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            var outcome: Outcome = .ineligible;
            args.sketch_topk = .{ .fuse_census = fuse, .outcome = &outcome };
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            seen.insert(outcome);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const w = want.output.group_rows.?;
            const g = got.output.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
        }
    };
    try testing.expect(seen.contains(.census_short));
    try testing.expect(seen.contains(.census));
}

fn runSketchedAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, certify: bool) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        // Parsed up front: a footer parse reports running out of memory as a file that is not Parquet.
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "s",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 1 },
        .sketch_topk = .{ .cell_bits = 10, .admit_per_row = 4, .certify = certify },
    });
    res.deinit(gpa);
}

test "runMultiAggregate: a sketched COUNT cut frees everything on any allocation failure" {
    // One row group, string keys that own memory, one past the 16 bytes an unframed key needs.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]bool{ true, false }) |certify| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runSketchedAndFree, .{ &inputs, &meta, certify });
    }
}

test "runMultiAggregate: a LIMIT with no ORDER BY answers as the full scan through the first-keys cut" {
    for ([_][]const u8{ "ci/fixtures/parquet/topk_skew.parquet", "ci/fixtures/parquet/census_topk.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        const census = std.mem.indexOf(u8, path, "census") != null;
        const keys: []const []const u8 = if (census) &.{ "u", "t", "s", "t, u" } else &.{ "k", "n", "v", "k, n" };
        const filter: []const u8 = if (census) "u > 300000000000" else "n < 12";
        for (keys) |key| for ([_]?[]const u8{ null, filter }) |f| for ([_]usize{ 1, 3 }) |parallelism| {
            // Within one row group, past it (the discovery doubles), past every key, and none.
            for ([_]expr_ast.ResultOrder{
                .{ .limit = 10 },
                .{ .limit = 4, .offset = 7 },
                .{ .limit = 1500 },
                .{ .limit = 30000 },
                .{ .limit = 0 },
            }) |cut| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const agg = if (census) "count(*) AS c, min(s) AS lo, sum(t) AS st" else "count(*) AS c, min(v) AS lo";
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .filter = f,
                    .aggregate = agg,
                    .group_by = key,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .result_order = cut,
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.sketch_topk = .{};
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try testing.expectEqual(want.rows_in, got.rows_in);
                try testing.expectEqual(want.rows_kept, got.rows_kept);
                try want.output.applyResultOrder(testing.allocator, cut);
                try got.output.applyResultOrder(testing.allocator, cut);
                const w = want.output.group_rows.?;
                const g = got.output.group_rows.?;
                try testing.expectEqual(w.len, g.len);
                for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
            }
        };
    }
}

fn runFirstCutAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, limit: u64) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "n",
        .parallelism = 1,
        .result_order = .{ .limit = limit },
        .sketch_topk = .{},
    });
    res.deinit(gpa);
}

test "runMultiAggregate: a first-keys cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // Three row groups of key `n`: NULL through the first, 1 after. A cut of 1 ends inside the first; one of 2 makes
    // the discovery read two, then the admit pass reads the third; one of 3 has the discovery read everything.
    meta.row_groups.items.len = 3;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    for ([_]u64{ 1, 2, 3 }) |limit| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runFirstCutAndFree, .{ &inputs, &meta, limit });
    }
}

fn runCensusAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, cfg: SketchTopK) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(*) AS c, min(s) AS lo",
        .group_by = "s",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4 },
        .sketch_topk = cfg,
    });
    res.deinit(gpa);
}

fn runDistinctCensusAndFree(gpa: std.mem.Allocator, inputs: []const Input, meta: *const schema.FileMetaData, cfg: SketchTopK) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = "count(DISTINCT u) AS d, min(s) AS lo",
        .group_by = "t",
        .parallelism = 1,
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 4 },
        .sketch_topk = cfg,
    });
    res.deinit(gpa);
}

test "runMultiAggregate: a count(DISTINCT) census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Certified, widened once, abandoned mid row group, and falling back after the admit passes.
    for ([_]SketchTopK{ .{}, .{ .admit_per_row = 1 }, .{ .census_check_rows = 64 }, .{ .certify = false } }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runDistinctCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a census cut frees everything on any allocation failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    var meta = try metadata.open(meta_arena.allocator(), bytes);
    // The first row group alone: every failure point runs the query again.
    meta.row_groups.items.len = 1;
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    // Riding along the count pass and dropped early, riding along, alone, and falling back after the admit pass.
    for ([_]SketchTopK{
        .{ .fuse_census = true, .census_check_rows = 64 },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .fuse_census = false, .certify = false },
    }) |cfg| {
        try testing.checkAllAllocationFailures(fixed.allocator(), runCensusAndFree, .{ &inputs, &meta, cfg });
    }
}

test "runMultiAggregate: a GROUP BY that fits its budget still fits when spawns fail" {
    // A spawn failure must not turn a query that fits `max_memory` into ExceededMemoryBudget. Reaching that path
    // needs four things at once:
    //
    //  1. GROUP BY, or there is no group table and no budget to get wrong.
    //  2. n_workers >= 2, hence twelve fetched columns to clear the 2 MiB-per-worker sizing threshold.
    //  3. Group keys disjoint per work item, so one worker holding the union really needs ~2x; shared keys hide it.
    //  4. A budget that fits only when the fallback worker gets the full shared allowance, not one worker's share.
    //
    // Metadata is passed in so every injected failure is necessarily a scan-worker spawn; otherwise footer-parse spawns
    // satisfy the "injection fired" check while the scan loop quietly runs single-threaded.
    //
    // Tracked fixtures, not data/ (gitignored, so a test pointed there would skip in a clean clone): regenerate with
    // tools/gen_spawn_budget_fixture.py.
    const files = [_][]const u8{
        "ci/fixtures/parquet/spawn_budget_a.parquet",
        "ci/fixtures/parquet/spawn_budget_b.parquet",
    };
    const group_by = "group_key";
    const aggregate = SPAWN_FIXTURE_AGG;
    // A finished block stays live while it folds into the merge. Two thousand groups, with that overlap, sit a
    // little over 2 MiB once keys are a slot array instead of a per-entry charge.
    const budget = 2300 * 1024;
    const parallelism = 2;

    var bytes: [files.len][]u8 = undefined;
    var loaded: usize = 0;
    defer for (bytes[0..loaded]) |b| testing.allocator.free(b);
    for (files, 0..) |f, i| {
        bytes[i] = metadata.readFileSlice(f, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{f});
                return error.SkipZigTest;
            }
            return err;
        };
        loaded += 1;
    }

    var inputs: [files.len]Input = undefined;
    for (files, 0..) |f, i| inputs[i] = .{ .name = f, .bytes = bytes[i] };

    var baseline_groups: ?usize = null;
    for ([_]?usize{ null, 0, 1 }) |fail_after| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        var metas: [files.len]schema.FileMetaData = undefined;
        for (bytes[0..], 0..) |b, i| metas[i] = try metadata.open(arena, b);

        if (fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();

        const res = runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .metas = &metas,
            .aggregate = aggregate,
            .group_by = group_by,
            .parallelism = parallelism,
            .max_memory = budget,
        }) catch |err| {
            std.debug.print(
                "fail_after={?d}: a GROUP BY that fits {d} KiB was rejected: {s}\n",
                .{ fail_after, budget / 1024, @errorName(err) },
            );
            return err;
        };
        defer freeResult(res);

        const groups = (res.output.group_rows orelse return error.TestUnexpectedResult).len;
        if (baseline_groups) |b| {
            try testing.expectEqual(b, groups);
            // Metadata was supplied, so this can only be a scan-worker spawn.
            if (spawn_util.failuresInjected() == 0) {
                std.debug.print(
                    "fail_after={?d}: no scan-worker spawn was refused — test is vacuous\n",
                    .{fail_after},
                );
                return error.TestUnexpectedResult;
            }
        } else {
            baseline_groups = groups;
        }
    }
}

// ============================================================ Group-key framing.
//
// Leaf-indexed framing read the wrong schema element for every column after the first nested one (see
// `leafLabel`). The fixture puts a MAP ahead of ordinary scalars to reproduce that shape.
// ============================================================

const nested_key_fixture = "ci/fixtures/parquet/nested_key_shape.parquet";

fn loadNestedKeyFixture() !?[]u8 {
    return metadata.readFileSlice(nested_key_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{nested_key_fixture});
            return null;
        }
        return err;
    };
}

fn freeResult(res: MultiAggResult) void {
    var owned = res;
    owned.deinit(testing.allocator);
}

test "group key: a scalar column after a MAP is framed by its own type" {
    // `ts` is INT64 but leaf-indexed lookup resolved a BYTE_ARRAY element, so deserialization read the timestamp bytes
    // as a string length: "index out of bounds: index 2690342917, len 9".
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "ts",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 5), rows.len);
    var total: i128 = 0;
    for (rows) |r| total += r[1].i;
    try testing.expectEqual(@as(i128, 60), total);
    for (rows) |r| try testing.expect(r[0] == .i);

    // The output column name also depends on resolving the right leaf.
    const cols = res.output.group_cols orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("ts", cols[0]);
}

test "group key: a BOOLEAN first column does not truncate the rest" {
    // BOOLEAN evaluates to the i64 lane (8 bytes) but the physical type said `boolean` and deserialization consumed 1,
    // so every later key column was read from the wrong offset: `name` came back null while counts looked fine.
    const bytes = (try loadNestedKeyFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();

    const inputs = [_]Input{.{ .name = nested_key_fixture, .bytes = bytes }};
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "flag, name",
        .parallelism = 1,
    });
    defer freeResult(res);

    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    // 2 flags x 4 names, and every name must survive.
    try testing.expectEqual(@as(usize, 8), rows.len);
    for (rows) |r| {
        try testing.expect(r[0] == .i);
        try testing.expect(r[1] == .s);
        try testing.expect(std.mem.startsWith(u8, r[1].s, "name"));
    }
}

test "deserializeKeyColumn: an unsigned 64-bit key reads back unsigned" {
    var key: [9]u8 = undefined;
    key[0] = 1;
    std.mem.writeInt(u64, key[1..9], std.math.maxInt(u64), .little);
    const v = try deserializeKeyColumn(testing.allocator, &key, 0, &.{.u64});
    try testing.expectEqual(@as(i128, std.math.maxInt(u64)), v.i);
    const signed = try deserializeKeyColumn(testing.allocator, &key, 0, &.{.i64});
    try testing.expectEqual(@as(i128, -1), signed.i);
}

test "deserializeKeyColumn: corrupt framing errors instead of panicking" {
    const a = testing.allocator;

    // A length field claiming far more than the buffer holds — previously a panic or a huge alloc.
    var runaway: [5]u8 = .{ 1, 0xFF, 0xFF, 0xFF, 0xFF };
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &runaway, 0, &.{.string}),
    );

    // Truncated mid-payload.
    var short_i64: [4]u8 = .{ 1, 0, 0, 0 };
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &short_i64, 0, &.{.i64}),
    );

    // Asking for a column beyond what the key holds.
    var one_null: [1]u8 = .{0};
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &one_null, 1, &.{ .i64, .i64 }),
    );

    // Empty input.
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &.{}, 0, &.{.i64}),
    );

    // A well-formed key still round-trips: null, then i64, then string.
    var ok_key: std.ArrayList(u8) = .empty;
    defer ok_key.deinit(a);
    try ok_key.append(a, 0); // null
    try ok_key.append(a, 1);
    try ok_key.appendSlice(a, &std.mem.toBytes(@as(i64, -7)));
    try ok_key.append(a, 1);
    try ok_key.appendSlice(a, &std.mem.toBytes(@as(u32, 3)));
    try ok_key.appendSlice(a, "abc");
    const types = [_]KeyType{ .i64, .i64, .string };
    try testing.expect((try deserializeKeyColumn(a, ok_key.items, 0, &types)) == .null_val);
    try testing.expectEqual(@as(i128, -7), (try deserializeKeyColumn(a, ok_key.items, 1, &types)).i);
    const s = try deserializeKeyColumn(a, ok_key.items, 2, &types);
    defer a.free(s.s);
    try testing.expectEqualStrings("abc", s.s);
}

test "parseFooters: serial path reports the lowest bad index too" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var meta_arenas: []std.heap.ArenaAllocator = &.{};
    defer for (meta_arenas) |*a| a.deinit();

    var inputs = [_]Input{
        .{ .name = "bad0", .bytes = &not_parquet },
        .{ .name = "bad1", .bytes = &not_parquet },
    };
    // parallelism 1 takes the serial branch, which never spawns.
    const res = try parseFooters(arena_state.allocator(), testing.allocator, &inputs, 1, &meta_arenas, null);
    switch (res) {
        .ok => return error.TestUnexpectedResult,
        .failed => |f| try testing.expectEqual(@as(usize, 0), f.index),
    }
}

// ============================================================ Full-match row groups.
//
// Where statistics prove every row of a row group passes the filter, the scan neither evaluates the filter there nor
// fetches/decodes columns only the filter reads. Every answer is checked against `--scan-all`, which disables all
// statistics use and evaluates the filter on every row, so the two paths share nothing but the decoders and folds.
// Fixture layout (tools/gen_full_match_fixture.py): 8 row groups x 200 rows, `ts` = 0..1599 sorted, so `ts >= 500`
// skips row groups 0-1, leaves 2 partial and proves 3-7.
// ============================================================

const full_match_fixture = "ci/fixtures/parquet/full_match.parquet";

const FullMatchQuery = struct {
    filter: ?[]const u8,
    aggregate: []const u8,
    group_by: ?[]const u8 = null,
    /// Row groups the proof must claim; checked so a silently disabled optimization cannot pass as a correct answer.
    full_match: usize,
};

fn runFullMatchQuery(
    arena: std.mem.Allocator,
    inputs: []const Input,
    q: FullMatchQuery,
    parallelism: usize,
    scan_all: bool,
) !MultiAggResult {
    return runMultiAggregate(testing.allocator, arena, .{
        .inputs = inputs,
        .filter = q.filter,
        .aggregate = q.aggregate,
        .group_by = q.group_by,
        .parallelism = parallelism,
        .scan_all = scan_all,
    });
}

fn expectSameAggValue(want: AggValue, got: AggValue) !void {
    try testing.expectEqual(std.meta.activeTag(want), std.meta.activeTag(got));
    switch (want) {
        .i => |v| try testing.expectEqual(v, got.i),
        // Fixture doubles are multiples of 0.5 with small sums, so every summation order is exact.
        .f => |v| try testing.expectEqual(v, got.f),
        .avg => |v| {
            try testing.expectEqual(v.sum, got.avg.sum);
            try testing.expectEqual(v.count, got.avg.count);
        },
        .s => |v| try testing.expectEqualStrings(v, got.s),
        .dec => |v| try testing.expectEqual(v, got.dec),
        .avg_dec => |v| try testing.expectEqual(v, got.avg_dec),
        .null_val => {},
    }
}

fn expectSameAnswer(want: MultiAggResult, got: MultiAggResult) !void {
    try testing.expectEqual(want.output.aggs.len, got.output.aggs.len);
    for (want.output.aggs, got.output.aggs) |w, g| try expectSameAggValue(w.value, g.value);
    const want_rows = want.output.group_rows orelse &.{};
    const got_rows = got.output.group_rows orelse &.{};
    try testing.expectEqual(want_rows.len, got_rows.len);
    for (want_rows, got_rows) |wr, gr| {
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |w, g| try expectSameAggValue(w, g);
    }
}

/// Runs `q` with and without statistics at several -j (16 splits each row group's aggregates across work items, each
/// with its own proven fetch set) and requires identical answers plus the expected number of proven row groups.
fn checkFullMatchQuery(inputs: []const Input, q: FullMatchQuery) !MultiAggResult {
    var oracle_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer oracle_arena.deinit();
    const oracle = runFullMatchQuery(oracle_arena.allocator(), inputs, q, 1, true) catch |err| {
        std.debug.print("full match: --scan-all failed for filter={?s} agg={s}: {s}\n", .{
            q.filter, q.aggregate, @errorName(err),
        });
        return err;
    };
    defer freeResult(oracle);
    try testing.expectEqual(@as(usize, 0), oracle.row_groups_full_match);

    var last: ?MultiAggResult = null;
    errdefer if (last) |r| freeResult(r);
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var got = try runFullMatchQuery(arena_state.allocator(), inputs, q, parallelism, false);
        // Only the answer outlives this arena.
        got.state.deinit(testing.allocator);
        if (last) |r| freeResult(r);
        last = got;
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("full match: -j{d} disagrees with --scan-all for filter={?s} agg={s} group_by={?s}\n", .{
                parallelism, q.filter, q.aggregate, q.group_by,
            });
            return err;
        };
        testing.expectEqual(q.full_match, got.row_groups_full_match) catch |err| {
            std.debug.print("full match: filter={?s} proved {d} row groups, want {d}\n", .{
                q.filter, got.row_groups_full_match, q.full_match,
            });
            return err;
        };
    }
    return last.?;
}

fn loadFullMatchFixture() !?[]u8 {
    return metadata.readFileSlice(full_match_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{full_match_fixture});
            return null;
        }
        return err;
    };
}

test "min-fill: a column the filter also reads under an OR decodes the pages its conjunct is proven on" {
    // ci/fixtures/parquet/min_fill_or.parquet (tools/gen_min_fill_fixture.py): c alternates 1, 2 with no nulls, in
    // pages of 50 with a page index; b is 1 on every seventh row. Each page proves `c IS NOT NULL` (and, trusting
    // statistics, `c >= 0`); standing the page minimum in for c would drop every c = 2 row the OR keeps.
    const path = "ci/fixtures/parquet/min_fill_or.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const Case = struct { filter: []const u8, trust: bool, n: i128, sx: i128 };
    const cases = [_]Case{
        .{ .filter = "c IS NOT NULL AND (c = 2 OR b = 1)", .trust = false, .n = 572, .sx = 285784 },
        .{ .filter = "c >= 0 AND (c = 2 OR b = 1)", .trust = true, .n = 572, .sx = 285784 },
        .{ .filter = "(c = 2 OR b = 1) AND c IS NOT NULL", .trust = true, .n = 572, .sx = 285784 },
        // A column only mandatory conjuncts read may still be filled: the answer is the same either way.
        .{ .filter = "c IS NOT NULL AND b = 1", .trust = false, .n = 143, .sx = 71071 },
    };
    for (cases) |case| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = case.filter,
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .parallelism = j,
            .trust_stats = case.trust,
            .min_bytes_per_worker = 1,
        });
        defer r.deinit(testing.allocator);
        testing.expectEqual(case.n, r.output.aggs[0].value.i) catch |err| {
            std.debug.print("min-fill: {s} at -j {d}\n", .{ case.filter, j });
            return err;
        };
        try testing.expectEqual(case.sx, r.output.aggs[1].value.i);
    };
}

test "SQL GROUP BY binds SELECT items to keys as bound expressions, not text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};
    const Col = expr_ast.SelectColumn;
    const Case = struct { group_by: []const u8, select: []const Col, names: []const []const u8, groups: usize };
    const cases = [_]Case{
        // An unaliased expression key, named as written (ClickBench Q35's shape).
        .{
            .group_by = "b, b - 1",
            .select = &.{ .{ .expr = "b - 1", .role = .key }, .{ .expr = "\"b\"", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
            .names = &.{ "b - 1", "b", "count(*)" },
            .groups = 16,
        },
        // A key the SELECT list names by its alias (`SELECT b * 2 AS k ... GROUP BY k`), and a constant key.
        .{
            .group_by = "k, 1",
            .select = &.{ .{ .expr = "1", .role = .key }, .{ .expr = "b * 2", .alias = "k", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } },
            .names = &.{ "1", "k", "n" },
            .groups = 16,
        },
    };
    for (cases) |case| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var r = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*)",
            .group_by = case.group_by,
            .select_cols = case.select,
        });
        defer r.deinit(testing.allocator);
        const cols = r.output.group_cols.?;
        try testing.expectEqual(case.names.len, cols.len);
        for (case.names, cols) |want, got| try testing.expectEqualStrings(want, got);
        try testing.expectEqual(case.groups, r.output.group_rows.?.len);
        var total: i128 = 0;
        for (r.output.group_rows.?) |row| total += row[2].i;
        try testing.expectEqual(@as(i128, 1600), total);
    }
    // `b + 1` is no key of `GROUP BY b - 1`, however alike the text.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    try testing.expectError(error.ColumnMustBeGrouped, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*)",
        .group_by = "b - 1",
        .select_cols = &.{ .{ .expr = "b + 1", .role = .key }, .{ .expr = "count(*)", .role = .agg } },
    }));
}

test "cutShape names the cut a GROUP BY order allows from the bound query, never its text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const meta = try metadata.open(a, bytes);
    const Col = expr_ast.SelectColumn;
    const desc1: []const expr_ast.OrderKey = &.{.{ .column = 1, .descending = true }};
    const Case = struct { aggregate: []const u8, select: []const Col, order: expr_ast.ResultOrder, want: CutShape };
    const cases = [_]Case{
        .{ .aggregate = "COUNT(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "COUNT(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .count },
        // A hidden ORDER BY count binds like a selected one.
        .{ .aggregate = "count(*) AS __h0", .select = &.{.{ .expr = "b", .role = .key }}, .order = .{ .keys = desc1, .limit = 5, .hidden = &.{.{ .expr = "count(*)", .role = .agg }} }, .want = .count },
        .{ .aggregate = "count(DISTINCT x) AS d", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(DISTINCT x)", .alias = "d", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .count_distinct },
        .{ .aggregate = "sum(x) AS s", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "sum(x)", .alias = "s", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .none },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 5 }, .want = .none },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .limit = 5 }, .want = .first_keys },
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "b", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .limit = 0 }, .want = .none },
        // A query that does not bind steers nothing.
        .{ .aggregate = "count(*) AS n", .select = &.{ .{ .expr = "nope", .role = .key }, .{ .expr = "count(*)", .alias = "n", .role = .agg } }, .order = .{ .keys = desc1, .limit = 5 }, .want = .none },
    };
    for (cases) |case| {
        const got = cutShape(a, &meta, .{ .inputs = &.{}, .aggregate = case.aggregate, .group_by = "b", .select_cols = case.select, .result_order = case.order });
        try testing.expectEqual(case.want, got);
    }
}

test "full match: proven row groups answer like --scan-all" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const queries = [_]FullMatchQuery{
        // count(*) alone: proven row groups need no column at all.
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n", .full_match = 5 },
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n, sum(x) AS sx, min(x) AS mn", .full_match = 5 },
        // The filter column is also an aggregate argument / per-agg FILTER input, so it must still be decoded.
        .{ .filter = "ts >= 500", .aggregate = "sum(ts) AS st, max(ts) AS mt, count(*) AS n", .full_match = 5 },
        .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) FILTER (WHERE ts < 1000) AS sx",
            .full_match = 5,
        },
        .{ .filter = "ts >= 500", .aggregate = "avg(x) AS a, count(x) AS cx", .full_match = 5 },
        // Strings: a min-side proof needs no exactness flag; a max-side one relies on is_max_value_exact.
        .{ .filter = "s >= 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 5 },
        .{ .filter = "s < 'k000500'", .aggregate = "count(*) AS n, sum(x) AS sx", .full_match = 2 },
        // Nullable: group 1 is all null (skip), 0/2/3 have nulls (partial), 4-7 have none (proven).
        .{ .filter = "n IS NOT NULL", .aggregate = "count(*) AS c, sum(x) AS sx, count(n) AS cn", .full_match = 4 },
        .{ .filter = "n IS NULL", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "n >= 0", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 4 },
        .{ .filter = "ts >= 500 AND n IS NOT NULL", .aggregate = "count(*) AS c, sum(n) AS sn", .full_match = 4 },
        .{ .filter = "ts >= 1400 OR x > 1000", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 1 },
        .{ .filter = "NOT ts < 500", .aggregate = "count(*) AS c, sum(x) AS sx", .full_match = 5 },
        // Floats: NaN could hide outside [min, max].
        .{ .filter = "x >= -3", .aggregate = "count(*) AS n", .full_match = 0 },
    };
    for (queries) |q| {
        const res = try checkFullMatchQuery(&inputs, q);
        freeResult(res);
    }

    // Hand-known values, so agreement with --scan-all is not agreement on a shared mistake.
    const res = try checkFullMatchQuery(&inputs, queries[2]);
    defer freeResult(res);
    try testing.expectEqual(@as(i128, (500 + 1599) * 1100 / 2), res.output.aggs[0].value.i);
    try testing.expectEqual(@as(i128, 1599), res.output.aggs[1].value.i);
    try testing.expectEqual(@as(i128, 1100), res.output.aggs[2].value.i);
    try testing.expectEqual(@as(usize, 2), res.row_groups_pruned);
}

test "a file with no row groups aggregates to the empty answer" {
    // A valid empty table (pyarrow's ParquetWriter closed without writing). Splitting aggregates across workers
    // divided by the row-group count, and the leaf count was read from row group 0.
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/no_row_groups.parquet", testing.allocator);
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "no_row_groups.parquet", .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .filter = "a > 1",
            .aggregate = "count(*) AS n",
            .parallelism = parallelism,
        });
        defer freeResult(res);
        try testing.expectEqual(@as(i128, 0), res.output.aggs[0].value.i);
    }
}

test "full match: per-row-group decisions on a real footer" {
    // Unsigned columns prune in unsigned order but are never proven fully matching.
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    // pyarrow declares TYPE_DEFINED_ORDER for every leaf and marks untruncated string bounds exact.
    const orders = meta.column_orders orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 6), orders.items.len);
    for (orders.items) |o| try testing.expectEqual(schema.COLUMN_ORDER_TYPE_DEFINED, o);
    const s_stats = meta.row_groups.items[0].columns.items[2].meta_data.?.statistics.?;
    try testing.expectEqual(@as(?bool, true), s_stats.is_max_value_exact);
    try testing.expectEqual(@as(?bool, true), s_stats.is_min_value_exact);

    const D = filter_prune.Decision;
    const all: D = .always_match;
    const cases = [_]struct { filter: []const u8, want: [8]D }{
        .{ .filter = "ts >= 600", .want = .{ .skip, .skip, .skip, all, all, all, all, all } },
        .{ .filter = "u >= 600", .want = .{ .skip, .skip, .skip, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "x >= -3", .want = .{ .keep, .keep, .keep, .keep, .keep, .keep, .keep, .keep } },
        .{ .filter = "n IS NOT NULL", .want = .{ .keep, .skip, .keep, .keep, all, all, all, all } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "full match: GROUP BY on the filter column" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    // b = ts / 100: group 2 holds b = 4, 5 (partial); groups 3-7 hold b = 6..15 (proven).
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "b >= 5",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    defer freeResult(res);
    const rows = res.output.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 11), rows.len);
    for (rows) |r| try testing.expectEqual(@as(i128, 100), r[1].i);

    const keyed = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    freeResult(keyed);
}

test "full match: several files keep their own decisions" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
        .{ .name = full_match_fixture, .bytes = bytes },
    };
    const res = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(ts) AS st",
        .full_match = 15,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(i128, 3 * 1100), res.output.aggs[0].value.i);
}

/// Overwrite column `col`'s chunk in each listed row group with bytes no decoder accepts.
fn poisonChunks(bytes: []u8, meta: *const schema.FileMetaData, col: usize, rgs: []const usize) void {
    for (rgs) |rg_i| {
        const cm = meta.row_groups.items[rg_i].columns.items[col].meta_data.?;
        const start: usize = @intCast(cm.dictionary_page_offset orelse cm.data_page_offset);
        const len: usize = @intCast(cm.total_compressed_size);
        @memset(bytes[start .. start + len], 0xA5);
    }
}

test "full match: filter-only chunks of proven row groups are never read" {
    const clean = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(clean);

    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), clean);

    const cases = [_]struct { col: usize, rgs: []const usize, q: FullMatchQuery }{
        .{ .col = 0, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "ts >= 500",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 2, .rgs = &.{ 3, 4, 5, 6, 7 }, .q = .{
            .filter = "s >= 'k000500'",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 5,
        } },
        .{ .col = 4, .rgs = &.{ 4, 5, 6, 7 }, .q = .{
            .filter = "n IS NOT NULL",
            .aggregate = "count(*) AS n, sum(x) AS sx",
            .full_match = 4,
        } },
    };
    for (cases) |c| {
        const poisoned = try testing.allocator.dupe(u8, clean);
        defer testing.allocator.free(poisoned);
        poisonChunks(poisoned, &meta, c.col, c.rgs);
        const poisoned_in = [_]Input{.{ .name = "poisoned", .bytes = poisoned }};

        var want_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer want_arena.deinit();
        const clean_in = [_]Input{.{ .name = "clean", .bytes = clean }};
        const want = try runFullMatchQuery(want_arena.allocator(), &clean_in, c.q, 1, false);
        defer freeResult(want);

        for ([_]usize{ 1, 16 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const got = try runFullMatchQuery(arena_state.allocator(), &poisoned_in, c.q, parallelism, false);
            defer freeResult(got);
            try expectSameAnswer(want, got);
            try testing.expectEqual(c.q.full_match, got.row_groups_full_match);
        }

        // Control: the poison is detectable once the column is read, or the checks above prove nothing.
        var ctl_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer ctl_arena.deinit();
        var ctl_q = c.q;
        ctl_q.aggregate = "count(*) AS n";
        if (runFullMatchQuery(ctl_arena.allocator(), &poisoned_in, ctl_q, 1, true)) |r| {
            freeResult(r);
            std.debug.print("full match: poisoned column {d} decoded cleanly under --scan-all\n", .{c.col});
            return error.TestUnexpectedResult;
        } else |_| {}
    }
}

// ------------------------------------------------------------
// Statistics order: filters and pruning on columns whose order is not the signed one their bytes suggest.
// Fixtures from tools/gen_stats_order_fixtures.py; expected answers are hand-computed from the generator's formulas.
// ------------------------------------------------------------

const unsigned_order_fixture = "ci/fixtures/parquet/unsigned_order.parquet";

fn loadFixture(path: []const u8) !?[]u8 {
    return metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{path});
            return null;
        }
        return err;
    };
}

const CountSumCase = struct { filter: []const u8, n: i128, sum_i: i128 };

/// `count(*)` and `sum(i)` for each filter, with statistics (pruning, page index) and under --scan-all, at -j1 and -j4.
fn expectCountSum(path: []const u8, cases: []const CountSumCase) !void {
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for (cases) |c| {
        for ([_]bool{ false, true }) |scan_all| for ([_]usize{ 1, 4 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = c.filter,
                .aggregate = "count(*) AS n, sum(i) AS s",
                .parallelism = parallelism,
                .scan_all = scan_all,
            }) catch |err| {
                std.debug.print("{s}: filter {s} failed: {s}\n", .{ path, c.filter, @errorName(err) });
                return err;
            };
            defer freeResult(res);
            const got_sum: i128 = switch (res.output.aggs[1].value) {
                .i => |v| v,
                else => 0, // sum over no rows
            };
            if (res.output.aggs[0].value.i != c.n or got_sum != c.sum_i) {
                std.debug.print("{s}: filter {s} (scan_all={}, -j{d}) gave n={d} sum={d}, want n={d} sum={d}\n", .{
                    path, c.filter, scan_all, parallelism, res.output.aggs[0].value.i, got_sum, c.n, c.sum_i,
                });
                return error.TestUnexpectedResult;
            }
        };
    }
}

test "unsigned columns filter and prune in unsigned order" {
    try expectCountSum(unsigned_order_fixture, &.{
        .{ .filter = "u32 >= 2147483648", .n = 640, .sum_i = 450240 },
        .{ .filter = "u32 = 2147483648", .n = 1, .sum_i = 384 },
        .{ .filter = "u32 < 2147483648", .n = 384, .sum_i = 73536 },
        .{ .filter = "u32 != 3000000000", .n = 1023, .sum_i = 523264 },
        .{ .filter = "u32 IN (0, 4294967295)", .n = 2, .sum_i = 1023 },
        .{ .filter = "u64 > 9223372036854775807", .n = 640, .sum_i = 450240 },
        .{ .filter = "u64 = 18446744073709551615", .n = 1, .sum_i = 1023 },
        .{ .filter = "u8 >= 128", .n = 512, .sum_i = 294656 },
        .{ .filter = "u16 BETWEEN 32768 AND 65535", .n = 512, .sum_i = 293888 },
        .{ .filter = "n32 > 2147483647", .n = 256, .sum_i = 147456 },
        // Literals outside the column's range have a constant answer.
        .{ .filter = "u32 > -1", .n = 1024, .sum_i = 523776 },
        .{ .filter = "u32 = -5", .n = 0, .sum_i = 0 },
        .{ .filter = "u64 < 18446744073709551616", .n = 1024, .sum_i = 523776 },
        .{ .filter = "NOT u64 <= 18446744073709551615", .n = 0, .sum_i = 0 },
    });
}

test "unsigned columns: row-group decisions read bounds unsigned" {
    const bytes = (try loadFixture(unsigned_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);

    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [4]D }{
        .{ .filter = "u32 >= 3000000000", .want = .{ .skip, .skip, .keep, .keep } },
        .{ .filter = "u32 < 256", .want = .{ .keep, .skip, .skip, .skip } },
        .{ .filter = "u64 >= 18446744073709551360", .want = .{ .skip, .skip, .skip, .keep } },
        .{ .filter = "u64 = 9223372036854775808", .want = .{ .skip, .keep, .skip, .skip } },
        .{ .filter = "n32 IS NULL", .want = .{ .keep, .keep, .keep, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

const deprecated_stats_fixture = "ci/fixtures/parquet/deprecated_stats.parquet";

test "deprecated min/max prune only where their signed order is the column's order" {
    try expectCountSum(deprecated_stats_fixture, &.{
        // Byte arrays, DECIMAL and unsigned: the signed pair excludes a stored value from its own row group.
        .{ .filter = "s = 'a'", .n = 1, .sum_i = 3 },
        .{ .filter = "s < 'b'", .n = 1, .sum_i = 3 },
        .{ .filter = "dec = 1.00", .n = 1, .sum_i = 1 },
        .{ .filter = "dec < 1.28", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 1", .n = 1, .sum_i = 1 },
        .{ .filter = "u = 3000000000", .n = 1, .sum_i = 3 },
        // Signed integers and doubles: the pair is in their order and still prunes.
        .{ .filter = "i = 2", .n = 1, .sum_i = 2 },
        .{ .filter = "d >= 2.5", .n = 4, .sum_i = 309 },
        .{ .filter = "idec = 1.28", .n = 1, .sum_i = 2 },
    });

    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'a'", .want = .{ .unknown, .unknown } },
        .{ .filter = "dec = 6.00", .want = .{ .unknown, .unknown } },
        .{ .filter = "u = 11", .want = .{ .unknown, .unknown } },
        .{ .filter = "i > 100", .want = .{ .skip, .keep } },
        .{ .filter = "d < 1", .want = .{ .keep, .skip } },
        // DECIMAL over INT32 compares as its signed unscaled integer: the pair is in numeric order.
        .{ .filter = "idec > 6.50", .want = .{ .skip, .keep } },
        .{ .filter = "idec < 3", .want = .{ .keep, .skip } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

test "deprecated min/max never answer a DECIMAL aggregate under --trust-stats" {
    const bytes = (try loadFixture(deprecated_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = deprecated_stats_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "min(dec) AS lo, max(dec) AS hi, max(i) AS mi",
        .parallelism = 1,
        .trust_stats = true,
    });
    defer freeResult(res);
    // DECIMAL(9,2): exact unscaled values.
    const d92: expr_ast.Dec = .{ .precision = 9, .scale = 2 };
    try testing.expectEqual(DecValue{ .v = 100, .spec = d92 }, res.output.aggs[0].value.dec);
    try testing.expectEqual(DecValue{ .v = 700, .spec = d92 }, res.output.aggs[1].value.dec);
    try testing.expectEqual(@as(i128, 103), res.output.aggs[2].value.i);
}

const column_order_fixture = "ci/fixtures/parquet/column_order.parquet";

test "bounds in a declared column order zpq does not implement never prune" {
    // `s` declares an unknown order whose bounds, read bytewise, exclude 'B' and 'Y' from their row groups and pages.
    try expectCountSum(column_order_fixture, &.{
        .{ .filter = "s = 'B'", .n = 1, .sum_i = 1 },
        .{ .filter = "s = 'Y'", .n = 1, .sum_i = 102 },
        .{ .filter = "s < 'a'", .n = 2, .sum_i = 103 },
        .{ .filter = "s >= 'a'", .n = 4, .sum_i = 209 },
        .{ .filter = "i > 100", .n = 3, .sum_i = 306 },
    });

    const bytes = (try loadFixture(column_order_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const D = filter_prune.Decision;
    const cases = [_]struct { filter: []const u8, want: [2]D }{
        .{ .filter = "s = 'B'", .want = .{ .unknown, .unknown } },
        .{ .filter = "s >= 'zz'", .want = .{ .unknown, .unknown } },
        // `i` keeps TYPE_DEFINED_ORDER.
        .{ .filter = "i > 100", .want = .{ .skip, .always_match } },
    };
    for (cases) |c| {
        const f = try filter_parser.parse(arena, c.filter, &meta, null);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = filter_prune.pruneRowGroup(rg, f, &meta);
            testing.expectEqual(want, got) catch |err| {
                std.debug.print("{s}: row group {d} decided {s}, want {s}\n", .{
                    c.filter, i, @tagName(got), @tagName(want),
                });
                return err;
            };
        }
    }
}

const nan_stats_fixture = "ci/fixtures/parquet/nan_stats.parquet";

test "!= on a float chunk whose bounds are the literal skips only when nan_count proves no NaN" {
    // Every row group records [-0, 0]; row groups 0 and 2 hold a NaN, which `!= 0` keeps.
    try expectCountSum(nan_stats_fixture, &.{
        .{ .filter = "d != 0", .n = 2, .sum_i = 8 },
        .{ .filter = "NOT d = 0", .n = 2, .sum_i = 8 },
        .{ .filter = "d = 0", .n = 7, .sum_i = 28 },
    });

    const bytes = (try loadFixture(nan_stats_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const meta = try metadata.open(arena, bytes);
    const rgs = meta.row_groups.items;
    try testing.expectEqual(@as(?i64, null), rgs[0].columns.items[0].meta_data.?.statistics.?.nan_count);
    try testing.expectEqual(@as(?i64, 0), rgs[1].columns.items[0].meta_data.?.statistics.?.nan_count);
    const f = try filter_parser.parse(arena, "d != 0", &meta, null);
    const D = filter_prune.Decision;
    for (meta.row_groups.items, [_]D{ .keep, .skip, .keep }, 0..) |*rg, want, i| {
        const got = filter_prune.pruneRowGroup(rg, f, &meta);
        testing.expectEqual(want, got) catch |err| {
            std.debug.print("d != 0: row group {d} decided {s}, want {s}\n", .{ i, @tagName(got), @tagName(want) });
            return err;
        };
    }
}

test "clashing output column names are rejected and named" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: anyerror,
        column: []const u8,
    };
    const cases = [_]Case{
        // A key renamed away from its source column and an aggregate named after it: `s` used to show the key.
        .{ .group_by = "s AS k", .aggregate = "sum(x) AS s", .want = error.AmbiguousOutputColumn, .column = "s" },
        .{ .group_by = "b", .aggregate = "count(*) AS b", .want = error.AmbiguousOutputColumn, .column = "b" },
        .{
            .group_by = "b",
            .aggregate = "sum(x) AS t, min(x) AS T",
            .want = error.DuplicateOutputColumn,
            .column = "T",
        },
        .{ .aggregate = "sum(x) AS t, min(x) AS t", .want = error.DuplicateOutputColumn, .column = "t" },
        .{
            .group_by = "b AS k, ts AS k",
            .aggregate = "count(*) AS c",
            .want = error.DuplicateOutputColumn,
            .column = "k",
        },
        // The SQL front end's select list: `SELECT s AS k, sum(x) AS s ... GROUP BY s`.
        .{
            .group_by = "s",
            .aggregate = "sum(x) AS s",
            .select_cols = &.{ "s AS k", "sum(x) AS s" },
            .want = error.AmbiguousOutputColumn,
            .column = "s",
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var diag: Diag = .{};
        const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
            .diag = &diag,
        });
        try testing.expectError(c.want, res);
        try testing.expectEqualStrings(c.column, diag.column.get());
    }
}

test "unaliased aggregates keep the function name unless it clashes, then take the call text" {
    const bytes = (try loadFullMatchFixture()) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = full_match_fixture, .bytes = bytes }};

    const Case = struct {
        group_by: ?[]const u8 = null,
        aggregate: []const u8,
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .aggregate = "sum(x)", .want = &.{"sum"} },
        .{ .aggregate = "sum(x), sum(ts), count(*)", .want = &.{ "sum(x)", "sum(ts)", "count" } },
        .{ .aggregate = "count(*) AS count, count(n)", .want = &.{ "count", "count(n)" } },
        .{
            .aggregate = "sum(x) FILTER (WHERE ts > 5), sum(x)",
            .want = &.{ "sum(x) FILTER (WHERE ts > 5)", "sum(x)" },
        },
        .{ .group_by = "b", .aggregate = "min(x), min(ts)", .want = &.{ "b", "min(x)", "min(ts)" } },
        // The SQL front end lists unaliased aggregates by call text, which used to resolve to nothing.
        .{
            .group_by = "b",
            .aggregate = "count(*), sum(x)",
            .select_cols = &.{ "b", "count(*)", "sum(x)" },
            .want = &.{ "b", "count(*)", "sum(x)" },
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
        });
        defer freeResult(res);
        if (res.output.group_cols) |cols| {
            try testing.expectEqual(c.want.len, cols.len);
            for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
        } else {
            try testing.expectEqual(c.want.len, res.output.aggs.len);
            for (c.want, res.output.aggs) |w, g| try testing.expectEqualStrings(w, g.alias);
        }
    }
}

test "page index: null pages a footer contradicts do not drop rows" {
    // These files flag every page of their two REQUIRED columns all-null in the ColumnIndex, while every page holds
    // values. Pruning on that claim answered 0 for filters every other row satisfies.
    const files = [_][]const u8{
        "data/parquet-testing/data/datapage_v1-snappy-compressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-uncompressed-checksum.parquet",
        "data/parquet-testing/data/datapage_v1-corrupt-checksum.parquet",
    };
    const Query = struct { filter: []const u8, want: i128 };
    const queries = [_]Query{
        .{ .filter = "a > -66052", .want = 2560 },
        .{ .filter = "a IS NOT NULL", .want = 5120 },
        .{ .filter = "b < 0 OR b >= 0", .want = 5120 },
    };
    for (files) |fixture| {
        const bytes = metadata.readFileSlice(fixture, testing.allocator) catch |err| {
            if (err == error.FileNotFound) {
                std.debug.print("skipping: {s} not present\n", .{fixture});
                return error.SkipZigTest;
            }
            return err;
        };
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};
        for (queries) |q| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = q.filter,
                .aggregate = "count(*) AS n",
                .parallelism = 1,
            });
            defer freeResult(res);
            testing.expectEqual(q.want, res.output.aggs[0].value.i) catch |err| {
                std.debug.print("{s}: filter {s}\n", .{ fixture, q.filter });
                return err;
            };
        }
    }
}

test "runMultiAggregate: inputs whose nesting differs are a schema mismatch" {
    // Columns bind by leaf index against file 0. Here file 1 has the same element names in the same order, but
    // `name` is a top-level column rather than a field of `r`, so leaf 1 means different columns in the two files.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const m0 = try test_fixtures.sharedLeafNameMeta(a);
    var m1 = try test_fixtures.sharedLeafNameMeta(a);
    m1.schema.items[0].num_children = 6;
    m1.schema.items[1].num_children = 1;

    const inputs = [_]Input{ .{ .name = "f0", .bytes = "" }, .{ .name = "f1", .bytes = "" } };
    const metas = [_]schema.FileMetaData{ m0, m1 };
    var diag: Diag = .{};
    try testing.expectError(error.SchemaMismatch, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .metas = &metas,
        .aggregate = "sum(key) AS s",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("f1", diag.input.?);
    const mismatch = diag.detail.schema_mismatch;
    try testing.expectEqual(@as(usize, 1), mismatch.file);
    try testing.expectEqualStrings("f0", mismatch.first_input);
    try testing.expectEqualStrings(m0.schema.items[0].name, diag.column.get());
    try testing.expectEqualStrings(m1.schema.items[0].name, mismatch.other.get());
}

test "runMultiAggregate: an input that is not Parquet is named with the reason" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const inputs = [_]Input{.{ .name = "notes.txt", .bytes = "not a parquet file at all" }};
    var diag: Diag = .{};
    try testing.expectError(error.NotParquet, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .parallelism = 1,
        .diag = &diag,
    }));
    try testing.expectEqualStrings("notes.txt", diag.input.?);
    try testing.expect(diag.cause != null);
}

// ============================================================ All-null pages.
//
// The fixture's ColumnIndex flags whole pages of `v` and `s.b` all-null (see tools/gen_null_pages_fixture.py). A
// null check proves such a page always-match; its rows must still read as null wherever the filter is evaluated.
// ============================================================

const null_pages_fixture = "ci/fixtures/parquet/null_pages.parquet";

test "all-null pages: null checks and their combinations answer like --scan-all" {
    const bytes = metadata.readFileSlice(null_pages_fixture, testing.allocator) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{null_pages_fixture});
            return error.SkipZigTest;
        }
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};

    const Q = struct { filter: []const u8, aggregate: []const u8 = "count(*) AS n, sum(id) AS si", group_by: ?[]const u8 = null };
    const queries = [_]Q{
        .{ .filter = "v IS NULL" },
        .{ .filter = "v IS NOT NULL" },
        .{ .filter = "NOT v IS NULL" },
        .{ .filter = "NOT v IS NOT NULL" },
        .{ .filter = "s.b IS NULL" },
        .{ .filter = "s.b IS NOT NULL" },
        .{ .filter = "s.a IS NULL" },
        .{ .filter = "v IS NULL AND id >= 700" },
        .{ .filter = "id >= 700 AND v IS NULL" },
        .{ .filter = "s.b IS NULL AND id < 1500" },
        .{ .filter = "v IS NULL OR v > 1500" },
        .{ .filter = "v IS NOT NULL AND v > 1200" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n, count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "v IS NOT NULL", .aggregate = "count(v) AS c, sum(v) AS s, min(v) AS mn, max(v) AS mx" },
        .{ .filter = "s.b IS NULL", .aggregate = "count(s.b) AS c, count(s.a) AS ca, sum(s.a) AS sa" },
        .{ .filter = "v IS NULL", .aggregate = "count(*) AS n", .group_by = "v" },
        .{ .filter = "v IS NOT NULL AND v < 50", .aggregate = "count(*) AS n", .group_by = "v" },
    };
    for ([_]bool{ false, true }) |trust_stats| for (queries) |q| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .filter = q.filter,
            .aggregate = q.aggregate,
            .group_by = q.group_by,
            .parallelism = 1,
            .trust_stats = trust_stats,
        };
        var oracle_args = args;
        oracle_args.scan_all = true;
        const oracle = try runMultiAggregate(testing.allocator, arena_state.allocator(), oracle_args);
        defer freeResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("all-null pages: filter={s} agg={s} trust_stats={} disagrees with --scan-all\n", .{
                q.filter, q.aggregate, trust_stats,
            });
            return err;
        };
        // Hand-known counts, so agreement is not agreement on a shared mistake.
        if (q.group_by == null and std.mem.eql(u8, q.aggregate, "count(*) AS n, sum(id) AS si")) {
            const want: ?i128 = if (std.mem.eql(u8, q.filter, "v IS NULL")) 650 else if (std.mem.eql(u8, q.filter, "s.b IS NULL")) 997 else null;
            if (want) |w| try testing.expectEqual(w, got.output.aggs[0].value.i);
        }
    };
}

test "GROUP BY labels a nested leaf whose dotted path a top-level column takes by its quoted path" {
    // Top-level `a.b` (1, null, 3) beside the group `a`'s field `b` (10, 20, null).
    const fixture = "ci/fixtures/parquet/dotted_twin.parquet";
    const bytes = (try loadFixture(fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = fixture, .bytes = bytes }};

    const Case = struct {
        group_by: []const u8,
        aggregate: []const u8 = "count(*) AS n",
        select_cols: ?[]const []const u8 = null,
        want: []const []const u8,
    };
    const cases = [_]Case{
        .{ .group_by = "b", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a\".\"b\"", .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "\"a.b\"", .want = &.{ "a.b", "n" } },
        // Both keys at once used to be one name twice.
        .{ .group_by = "b, \"a.b\"", .want = &.{ "\"a\".\"b\"", "a.b", "n" } },
        // The label binds back as an output column name, and is not mistaken for a quoted `a"."b`.
        .{
            .group_by = "b, a.b",
            .select_cols = &.{ "n", "\"a\".\"b\"", "a.b" },
            .want = &.{ "n", "\"a\".\"b\"", "a.b" },
        },
        .{ .group_by = "b AS k", .want = &.{ "k", "n" } },
        // A select list (SQL) names a bare key by any name that binds its column; the output keeps the label
        // unless the select list renames it.
        .{ .group_by = "b", .select_cols = &.{ "b", "n" }, .want = &.{ "\"a\".\"b\"", "n" } },
        .{ .group_by = "b", .select_cols = &.{ "b AS k", "n" }, .want = &.{ "k", "n" } },
        .{
            .group_by = "\"a\".\"b\", \"a.b\"",
            .select_cols = &.{ "\"a.b\"", "b" },
            .want = &.{ "a.b", "\"a\".\"b\"" },
        },
    };
    for (cases) |c| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .select_cols = try selectList(arena_state.allocator(), c.select_cols),
            .parallelism = 1,
        });
        defer freeResult(res);
        const cols = res.output.group_cols.?;
        try testing.expectEqual(c.want.len, cols.len);
        for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
    }

    // The duplicate-name check sees the label, not the dotted path both columns share.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var diag: Diag = .{};
    const res = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "b, \"a\".\"b\" AS \"a\".\"b\"",
        .parallelism = 1,
        .diag = &diag,
    });
    try testing.expectError(error.DuplicateOutputColumn, res);
    try testing.expectEqualStrings("\"a\".\"b\"", diag.column.get());
}

/// A select list given as the SQL front end writes one (`expr AS alias`), split as `SelectColumn.parse` splits it.
fn selectList(arena: std.mem.Allocator, cols: ?[]const []const u8) !?[]const expr_ast.SelectColumn {
    const texts = cols orelse return null;
    const out = try arena.alloc(expr_ast.SelectColumn, texts.len);
    for (texts, out) |text, *col| col.* = .parse(text);
    return out;
}

/// One query against `meta`, freeing its result through the destructors alone.
fn runAndFree(
    gpa: std.mem.Allocator,
    inputs: []const Input,
    meta: *const schema.FileMetaData,
    aggregate: []const u8,
    group_by: ?[]const u8,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var res = try runMultiAggregate(gpa, arena_state.allocator(), .{
        .inputs = inputs,
        .metas = meta[0..1],
        .aggregate = aggregate,
        .group_by = group_by,
        .parallelism = 1,
        .scan_all = true,
    });
    res.deinit(gpa);
}

/// Forwards to `child` but never grows, shrinks or moves an allocation in place. `testing.allocator` resizes in place
/// only while the allocation is the newest in its bucket and the bucket has room, which depends on everything the
/// thread allocated before; a refused resize makes the caller allocate instead, so the allocation count
/// `checkAllAllocationFailures` replays would change from run to run (on Apple silicon's 16 KiB pages, every time).
/// Refusing them all makes every growth an allocation of its own, and so one more failure the check injects.
const NoInPlaceResize = struct {
    child: std.mem.Allocator,

    fn allocator(self: *NoInPlaceResize) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *NoInPlaceResize = @ptrCast(@alignCast(ctx));
        return self.child.rawAlloc(len, alignment, ret_addr);
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *NoInPlaceResize = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

test "runMultiAggregate: the result destructors free a partly built answer" {
    // Strings are what own memory: string min/max winners, string group keys, output names. Every allocation is
    // failed in turn, so each partial state the answer passes through must be one `deinit` frees.
    const path = "ci/fixtures/parquet/json_escape.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    const args = .{ &inputs, &meta, "min(s) AS m, max(s) AS x, count(*) AS n", null };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, args);
    const grouped = .{ &inputs, &meta, "min(s) AS m, count(*) AS n", @as(?[]const u8, "s") };
    try testing.checkAllAllocationFailures(fixed.allocator(), runAndFree, grouped);
}

test "ordered GROUP BY: groups as they first appear, and the same float bits at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // Keys first appear zeta, mu, alpha, omega, beta, kappa (24 row groups, mixed in as they go); `x` mixes
    // magnitudes from 1e-3 to 1e16, so where partial sums meet changes the bits of every float sum.
    const want_order = [_][]const u8{ "zeta", "mu", "alpha", "omega", "beta", "kappa" };
    for ([_]bool{ false, true }) |encoded| for ([_][]const u8{ "k", "i" }) |key| {
        // Results keep state in the arena, so it outlives every one of them.
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 2, 3, 8, 32, 32, 7 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "sum(x) AS s, avg(x) AS a, count(*) AS n, min(k) AS m",
                .group_by = key,
                .parallelism = j,
                .encoded = encoded,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            try testing.expectEqual(want_order.len, rows.len);
            for (rows, want_order) |r, k| try testing.expectEqualStrings(k, r[4].s);
            if (first) |f| {
                for (rows, f.output.group_rows.?) |r, fr| {
                    try testing.expectEqual(@as(u64, @bitCast(fr[1].f)), @as(u64, @bitCast(r[1].f)));
                    try testing.expectEqual(@as(u64, @bitCast(fr[2].f)), @as(u64, @bitCast(r[2].f)));
                    try testing.expectEqual(fr[3].i, r[3].i);
                }
                freeResult(res);
            } else first = res;
        }
    };
}

test "regression: float sums and avgs keep their bits at every -j, budget, decoder and filter" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // 24 row groups of 500 rows, `x` dictionary-encoded and mixing magnitudes from 1e-3 to 1e16: where partial sums
    // meet changes the bits. A small budget reads each row group in slices; the coded path folds over the
    // dictionary; a filter leaves blocks partly selected.
    const Shape = struct { group_by: ?[]const u8, filter: ?[]const u8 };
    for ([_]Shape{
        .{ .group_by = null, .filter = null },
        .{ .group_by = null, .filter = "i > 7" },
        .{ .group_by = null, .filter = "k = 'zeta'" },
        .{ .group_by = "k", .filter = null },
        .{ .group_by = "k", .filter = "i > 7" },
    }) |shape| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var first: ?MultiAggResult = null;
        defer if (first) |f| freeResult(f);
        for ([_]usize{ 1, 3, 8 }) |j| for ([_]?usize{ null, 8 << 10, 32 << 10, 128 << 10 }) |budget| {
            for ([_]bool{ true, false }) |encoded| for ([_]bool{ true, false }) |scan_all| {
                const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    // Computed arguments over the coded column: one gathered under a sparse filter, one that may be
                    // NULL and so evaluates in place.
                    .aggregate = "sum(x) AS s, avg(x) AS a, sum(x * 2) AS t, sum(CASE WHEN i > 3 THEN x END) AS u, " ++
                        "avg_state(x) AS st",
                    .group_by = shape.group_by,
                    .filter = shape.filter,
                    .parallelism = j,
                    .decode_limit = budget,
                    .encoded = encoded,
                    .scan_all = scan_all,
                    .min_bytes_per_worker = 1,
                });
                const f = first orelse {
                    first = res;
                    continue;
                };
                defer freeResult(res);
                if (shape.group_by == null) {
                    for (res.output.aggs, f.output.aggs) |g, w| try expectSameBits(w.value, g.value);
                } else for (res.output.group_rows.?, f.output.group_rows.?) |r, w| {
                    for (r, w) |g, x| try expectSameBits(x, g);
                }
            };
        };
    }
}

/// `expectSameAggValue`, floats compared bit for bit (a -0 is no 0, and every rounding shows).
fn expectSameBits(want: AggValue, got: AggValue) !void {
    switch (want) {
        .f => |v| try testing.expectEqual(@as(u64, @bitCast(v)), @as(u64, @bitCast(got.f))),
        .avg => |v| {
            const want_sum: ?u64 = if (v.sum) |x| @bitCast(x) else null;
            const got_sum: ?u64 = if (got.avg.sum) |x| @bitCast(x) else null;
            try testing.expectEqual(want_sum, got_sum);
            try testing.expectEqual(v.count, got.avg.count);
        },
        else => try expectSameAggValue(want, got),
    }
}

test "regression: count(col) answers, or fails, the same at every budget and -j" {
    const path = "ci/fixtures/parquet/plain_values_corrupt.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // One PLAIN length prefix in `s` runs past its page: count(s) reads only its levels when the row group decodes
    // whole, and read the values (and failed) when a small budget sliced it.
    var want: ?anyerror!i128 = null;
    for ([_]usize{ 1, 3 }) |j| for ([_]?usize{ null, 64 << 10, 4 << 10, 512 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const got: anyerror!i128 = if (runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(s) AS n",
            .parallelism = j,
            .decode_limit = budget,
            .min_bytes_per_worker = 1,
        })) |res| blk: {
            defer freeResult(res);
            break :blk res.output.aggs[0].value.i;
        } else |err| err;
        const w = want orelse {
            want = got;
            continue;
        };
        if (w) |v| try testing.expectEqual(v, try got) else |e| try testing.expectError(e, got);
    };
    try testing.expectEqual(@as(i128, 216), try want.?);
}

test "GROUP BY hash merge: same groups, counts and string mins at every -j" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // count and string min are not order-sensitive, so workers keep separate tables and the final merge hashes.
    for ([_][]const u8{ "k", "i" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        for ([_]usize{ 1, 2, 8, 32 }) |j| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS n, min(k) AS mk",
                .group_by = key,
                .parallelism = j,
                .scan_all = true,
                .min_bytes_per_worker = 1,
            });
            const rows = res.output.group_rows.?;
            if (base) |b| {
                const want = b.output.group_rows.?;
                try testing.expectEqual(want.len, rows.len);
                for (rows, want) |row, w| {
                    try testing.expectEqual(w[1].i, row[1].i);
                    try testing.expectEqualStrings(w[2].s, row[2].s);
                    switch (w[0]) {
                        .s => |s| try testing.expectEqualStrings(s, row[0].s),
                        .i => |v| try testing.expectEqual(v, row[0].i),
                        else => return error.TestUnexpectedResult,
                    }
                }
                freeResult(res);
            } else base = res;
        }
    }
}

fn mergeOverlapAndFree(allocator: std.mem.Allocator) !void {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "m", .result = .bytes, .arg = null, .where = null },
    };
    var a = expr_agg.GroupTable.init(allocator, 1 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(allocator, 1 << 20);
    defer b.deinit();
    _ = try a.getOrInsert("alpha", &calls, 10);
    a.accumulators.items[0].count = 2;
    a.accumulators.items[1] = .{ .min_bytes = try allocator.dupe(u8, "mango") };
    _ = try a.getOrInsert("beta", &calls, 40);
    a.accumulators.items[2].count = 1;
    a.accumulators.items[3] = .{ .min_bytes = try allocator.dupe(u8, "pear") };
    _ = try b.getOrInsert("alpha", &calls, 3);
    b.accumulators.items[0].count = 5;
    b.accumulators.items[1] = .{ .min_bytes = try allocator.dupe(u8, "apple") };
    _ = try b.getOrInsert("gamma", &calls, 20);
    b.accumulators.items[2].count = 7;
    b.accumulators.items[3] = .{ .min_bytes = try allocator.dupe(u8, "fig") };

    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = [_]MergeGroupRef{
        .{ .table = 0, .group = 0 },
        .{ .table = 0, .group = 1 },
        .{ .table = 1, .group = 0 },
        .{ .table = 1, .group = 1 },
    };
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer {
        freeOwnedAccumulatorStrings(allocator, accs.items);
        accs.deinit(allocator);
    }
    try hashMergeGroupRefs(allocator, allocator, &tables, &refs, null, &calls, null, false, &slots, &accs);
    std.sort.pdq(MergedSlot, slots.items, {}, struct {
        fn lt(_: void, x: MergedSlot, y: MergedSlot) bool {
            return x.first < y.first;
        }
    }.lt);
    try testing.expectEqual(@as(usize, 3), slots.items.len);
    const want_key = [_][]const u8{ "alpha", "gamma", "beta" };
    const want_first = [_]u64{ 3, 20, 40 };
    const want_count = [_]u64{ 7, 7, 1 };
    const want_min = [_][]const u8{ "apple", "fig", "pear" };
    for (slots.items, want_key, want_first, want_count, want_min) |slot, key, first, count, min| {
        try testing.expectEqualStrings(key, slot.key);
        try testing.expectEqual(first, slot.first);
        try testing.expectEqual(count, accs.items[slot.idx].count);
        try testing.expectEqualStrings(min, accs.items[slot.idx + 1].min_bytes.?);
    }
}

fn fillCountTables(
    a: *expr_agg.GroupTable,
    b: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    n: usize,
) !void {
    var buf: [32]u8 = undefined;
    for (0..n) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{i}) catch unreachable;
        const id = try a.getOrInsert(key, calls, i);
        a.accumulators.items[@as(usize, id) * calls.len].count = 1;
    }
    for (0..n) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{i + n / 2}) catch unreachable;
        const id = try b.getOrInsert(key, calls, 1_000_000 + i);
        b.accumulators.items[@as(usize, id) * calls.len].count = 10;
    }
}

fn countSum(accs: []const expr_agg.Accumulator, slots: []const MergedSlot) u64 {
    var sum: u64 = 0;
    for (slots) |s| sum += accs[s.idx].count;
    return sum;
}

test "parallel hash merge matches one thread, including a failed spawn" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.keys.items.len + b.keys.items.len);
    defer testing.allocator.free(refs);
    for (a.keys.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.keys.items, 0..) |_, i| refs[a.keys.items.len + i] = .{ .table = 1, .group = @intCast(i) };

    var serial_slots: std.ArrayList(MergedSlot) = .empty;
    defer serial_slots.deinit(testing.allocator);
    var serial_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer serial_accs.deinit(testing.allocator);
    // Counts are copied by value and the source is left intact, so a second merge sees the same partials.
    const ta = testing.allocator;
    try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 1, null, false, &serial_slots, &serial_accs);
    try testing.expectEqual(n + n / 2, serial_slots.items.len);
    const half: u64 = @intCast(n / 2);
    try testing.expectEqual(half + 11 * half + 10 * half, countSum(serial_accs.items, serial_slots.items));

    const Less = struct {
        fn lt(_: void, x: MergedSlot, y: MergedSlot) bool {
            return std.mem.order(u8, x.key, y.key) == .lt;
        }
    };
    std.sort.pdq(MergedSlot, serial_slots.items, {}, Less.lt);

    for ([_]usize{ 0, 1, 2 }) |fail_after| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        spawn_util.injectFailureAfter(fail_after);
        defer spawn_util.resetFailure();
        try mergeFoldedRefs(ta, ta, &tables, refs, &calls, 8, null, false, &slots, &accs);
        spawn_util.resetFailure();
        try testing.expectEqual(serial_slots.items.len, slots.items.len);
        std.sort.pdq(MergedSlot, slots.items, {}, Less.lt);
        for (slots.items, serial_slots.items) |got, want| {
            try testing.expectEqualStrings(want.key, got.key);
            try testing.expectEqual(want.first, got.first);
            try testing.expectEqual(serial_accs.items[want.idx].count, accs.items[got.idx].count);
        }
    }
}

test "a merge drawn from the query's ledger gives all of it back, and past it is refused, holding nothing" {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    };
    const n: usize = 10_000;
    var a = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer a.deinit();
    var b = expr_agg.GroupTable.init(testing.allocator, 64 << 20);
    defer b.deinit();
    try fillCountTables(&a, &b, &calls, n);
    const tables = [_]expr_agg.GroupTable{ a, b };
    const refs = try testing.allocator.alloc(MergeGroupRef, a.keys.items.len + b.keys.items.len);
    defer testing.allocator.free(refs);
    for (a.keys.items, 0..) |_, i| refs[i] = .{ .table = 0, .group = @intCast(i) };
    for (b.keys.items, 0..) |_, i| refs[a.keys.items.len + i] = .{ .table = 1, .group = @intCast(i) };
    for ([_]usize{ 1, 8 }) |workers| for ([_]bool{ false, true }) |in_place| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var charged: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
        const lists = charged.allocator();
        {
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(lists);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer accs.deinit(lists);
            const ta = testing.allocator;
            try mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
            try testing.expectEqual(n + n / 2, slots.items.len);
            try testing.expect(budget.used.load(.monotonic) >= slots.capacity * @sizeOf(MergedSlot));
        }
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        budget.limit = 64 << 10;
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(lists);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(lists);
        const ta = testing.allocator;
        const refused = mergeFoldedRefs(ta, lists, &tables, refs, &calls, workers, null, in_place, &slots, &accs);
        try testing.expectError(error.OutOfMemory, refused);
        try testing.expectEqual(error.ExceededMemoryBudget, charged.refusal(error.OutOfMemory));
        slots.clearAndFree(lists);
        accs.clearAndFree(lists);
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    };
}

test "hash merge of worker tables: overlapping keys, first appearance, allocation failure" {
    try mergeOverlapAndFree(testing.allocator);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), mergeOverlapAndFree, .{});
}

test "MergeIndex: inline fixed-width keys find what the byte map finds, at every stride and either size" {
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rand = prng.random();
    inline for (.{ 9, 18, 27 }) |stride| {
        const pool = 1500;
        var images: [pool][stride]u8 = undefined;
        for (&images, 0..) |*img, i| {
            rand.bytes(img);
            // Pairs that differ only in the last byte, past the first 8-byte word the compare loads.
            if (i % 2 == 1) {
                img.* = images[i - 1];
                img[stride - 1] +%= 1;
            }
        }
        var t = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
        t.fixed_stride = stride;
        const tables = [_]expr_agg.GroupTable{t};
        const n = 5000;
        for ([_]MergeIndex.Inline{ .any, .within_map }) |mode| {
            var fixed = try MergeIndex.init(testing.allocator, &tables, n, mode);
            defer fixed.deinit();
            var bytes: MergeIndex = .{ .bytes = KeyHashMap(u32).init(testing.allocator) };
            defer bytes.deinit();
            try bytes.bytes.ensureTotalCapacity(n);
            if (mode == .within_map) {
                // 9-byte keys only, and in no more bytes than the map's keys, values and metadata.
                try testing.expectEqual(stride == 9, fixed != .bytes);
                if (stride == 9) try testing.expect(fixed.fixed9.slots.len * 16 <= bytes.bytes.capacity() * 21);
            } else try testing.expect(fixed != .bytes);
            var next: u32 = 0;
            for (0..n) |_| {
                const key: []const u8 = &images[rand.uintLessThan(usize, pool)];
                const h = std.hash.Wyhash.hash(0, key);
                const f = fixed.getOrPut(key, h);
                const b = bytes.getOrPut(key, h);
                try testing.expectEqual(b.found, f.found);
                if (b.found) {
                    try testing.expectEqual(b.value.*, f.value.*);
                } else {
                    b.value.* = next;
                    f.value.* = next;
                    next += 1;
                }
            }
        }
    }
}

fn fillFixedPair(
    allocator: std.mem.Allocator,
    fixed: *[2]expr_agg.GroupTable,
    bytes: *[2]expr_agg.GroupTable,
    stride: u8,
    calls: []const expr_agg.AggCall,
    n: usize,
) !void {
    for (fixed, bytes, 0..) |*f, *b, ti| {
        f.* = expr_agg.GroupTable.init(allocator, 1 << 30);
        f.fixed_stride = stride;
        b.* = expr_agg.GroupTable.init(allocator, 1 << 30);
        // Table 1 repeats the upper half of table 0's keys, asked from earlier positions for odd keys.
        for (0..n) |r| {
            const i = r + ti * n / 2;
            var image: [27]u8 = @splat(0);
            var c: usize = 0;
            while (c < stride) : (c += 9) {
                image[c] = 1;
                std.mem.writeInt(u64, image[c + 1 ..][0..8], (i *% 0x9E3779B97F4A7C15) >> @intCast(c), .little);
            }
            const pos: u64 = if (ti == 1 and i % 2 == 1) i / 4 else 1000 * ti + i;
            const fid = try f.getOrInsertFixed(image[0..stride], calls, pos);
            const bid = try b.getOrInsert(image[0..stride], calls, pos);
            for ([_]*expr_agg.GroupTable{ f, b }, [_]u32{ fid, bid }) |t, id| {
                const accs = t.accumulators.items[@as(usize, id) * calls.len ..][0..calls.len];
                accs[0].count = i % 7 + 1;
                accs[1] = .{ .sum_i = @as(i128, @intCast(i)) - 300 };
            }
        }
    }
}

fn expectSameMerge(
    allocator: std.mem.Allocator,
    fixed: []const expr_agg.GroupTable,
    bytes: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    cut: ?ResultCut,
) !void {
    var arena_state = std.heap.ArenaAllocator.init(allocator);
    defer arena_state.deinit();
    const Lists = struct {
        slots: std.ArrayList(MergedSlot) = .empty,
        accs: std.ArrayList(expr_agg.Accumulator) = .empty,
    };
    var lists: [2]Lists = .{ .{}, .{} };
    defer for (&lists) |*l| {
        l.slots.deinit(allocator);
        l.accs.deinit(allocator);
    };
    for ([_][]const expr_agg.GroupTable{ fixed, bytes }, &lists) |tables, *l|
        try mergeFoldedRefs(allocator, allocator, tables, refs, calls, n_workers, cut, false, &l.slots, &l.accs);
    try testing.expectEqual(lists[1].slots.items.len, lists[0].slots.items.len);
    for (lists[0].slots.items, lists[1].slots.items) |f, b| {
        try testing.expectEqualSlices(u8, b.key, f.key);
        try testing.expectEqual(b.first, f.first);
        try testing.expectEqual(lists[1].accs.items[b.idx].count, lists[0].accs.items[f.idx].count);
        try testing.expectEqual(lists[1].accs.items[b.idx + 1].sum_i, lists[0].accs.items[f.idx + 1].sum_i);
    }
}

fn mergeFixedAsBytes(allocator: std.mem.Allocator, comptime n: usize, stride: u8, n_workers: usize) !void {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = null, .where = null },
    };
    var fixed: [2]expr_agg.GroupTable = .{ .init(allocator, 0), .init(allocator, 0) };
    var bytes: [2]expr_agg.GroupTable = .{ .init(allocator, 0), .init(allocator, 0) };
    defer for (&fixed, &bytes) |*f, *b| {
        f.deinit();
        b.deinit();
    };
    try fillFixedPair(allocator, &fixed, &bytes, stride, &calls, n);
    var refs: [2 * n]MergeGroupRef = undefined;
    for (&refs, 0..) |*r, i| r.* = .{ .table = @intCast(i / n), .group = @intCast(i % n) };
    // Whole merges, a count cut and a ranking cut, each in one partition and in several.
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, null);
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, .{ .a_idx = 0, .k = 25 });
    try expectSameMerge(allocator, &fixed, &bytes, &refs, &calls, n_workers, .{
        .a_idx = 1,
        .k = 25,
        .ranking_first = true,
    });
}

test "hash merge of fixed-width tables: the byte map's groups, firsts, order and state, cut or not, at every -j" {
    for ([_]u8{ 9, 18, 27 }) |stride| for ([_]usize{ 1, 3, 8 }) |j| {
        try mergeFixedAsBytes(testing.allocator, 6000, stride, j);
    };
    spawn_util.injectFailureAfter(1);
    defer spawn_util.resetFailure();
    try mergeFixedAsBytes(testing.allocator, 6000, 9, 8);
}

const in_place_calls = [_]expr_agg.AggCall{
    .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
    .{ .func = .sum, .alias = "si", .result = .i64, .arg = null, .where = null },
    .{ .func = .sum, .alias = "sf", .result = .f64, .arg = null, .where = null },
    .{ .func = .min, .alias = "mb", .result = .bytes, .arg = null, .where = null },
    .{ .func = .avg, .alias = "av", .result = .avg_f64, .arg = null, .where = null },
    .{ .func = .max, .alias = "mf", .result = .f64, .arg = null, .where = null },
};

/// Three tables over `n` keys, each holding about two thirds of them, with float sums of -0.0 alone, -0.0 meeting
/// -0.0 or +0.0, NULL sums and extrema, and owned string minima.
fn fillInPlaceTables(gpa: std.mem.Allocator, tables: *[3]expr_agg.GroupTable, fixed: bool, n: usize) !void {
    const calls = &in_place_calls;
    for (tables, 0..) |*t, ti| {
        t.* = expr_agg.GroupTable.init(gpa, 1 << 30);
        if (fixed) t.fixed_stride = 9;
        for (0..n) |r| {
            const i = (r * 7 + ti * 3) % n;
            if ((i + ti) % 3 == 0) continue;
            var image: [9]u8 = @splat(0);
            image[0] = 1;
            std.mem.writeInt(u64, image[1..9], i, .little);
            const id = try t.getOrInsert(&image, calls, ti * n + r);
            const accs = t.accumulators.items[@as(usize, id) * calls.len ..][0..calls.len];
            const v: f64 = @floatFromInt(@as(i64, @intCast(i % 11)) - 5);
            accs[0].count = i % 4 + 1;
            accs[1] = .{ .sum_i = if (i % 13 == 0) null else @as(i128, @intCast(i)) * 3 - 700 };
            accs[2] = .{ .sum_f = switch (i % 5) {
                0 => -0.0,
                1 => if (ti == 0) 0.0 else -0.0,
                2 => null,
                else => v * 0.1,
            } };
            const tag = (i * 31 + ti * 5) % 97;
            const min: ?[]const u8 = if (i % 6 == 0) null else try std.fmt.allocPrint(gpa, "s{d}", .{tag});
            accs[3] = .{ .min_bytes = min };
            accs[4] = .{ .avg = .{ .sum = v, .count = 1, .isum = @as(i128, @intCast(i)) + ti } };
            accs[5] = .{ .max_f = if (i % 4 == 0) null else v };
        }
    }
}

const in_place_outputs = blk: {
    var outs: [in_place_calls.len]BoundOutputColumn = undefined;
    for (&outs, 0..) |*o, i| o.* = .{ .source = .{ .agg = i }, .name = in_place_calls[i].alias };
    break :blk outs;
};

/// Merge, then build rows, from fresh tables, copying the state or in place.
fn mergeAndBuildRows(gpa: std.mem.Allocator, fixed: bool, n: usize, n_workers: usize, in_place: bool) !AggOutput {
    var tables: [3]expr_agg.GroupTable = .{ .init(gpa, 0), .init(gpa, 0), .init(gpa, 0) };
    defer for (&tables) |*t| t.deinit();
    try fillInPlaceTables(gpa, &tables, fixed, n);
    var refs: std.ArrayList(MergeGroupRef) = .empty;
    defer refs.deinit(gpa);
    for (tables, 0..) |t, ti| for (0..t.groupCount()) |g| {
        try refs.append(gpa, .{ .table = @intCast(ti), .group = @intCast(g) });
    };
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(gpa);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer {
        freeOwnedAccumulatorStrings(gpa, accs.items);
        accs.deinit(gpa);
    }
    const calls = &in_place_calls;
    try mergeFoldedRefs(gpa, gpa, &tables, refs.items, calls, n_workers, null, in_place, &slots, &accs);
    if (in_place) try testing.expectEqual(@as(usize, 0), accs.items.len);
    try sortSlotsByFirst(gpa, slots.items, n_workers);
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, slots.items.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    try buildGroupRows(gpa, &out.slab, .{
        .slots = slots.items,
        .outputs = &in_place_outputs,
        .calls = calls,
        .key_types = &.{.i64},
        .accs = accs.items,
        .folded = true,
        .in_place = if (in_place) &tables else &.{},
        .fixed_keys = fixed,
    }, rows, n_workers);
    return out;
}

test "in-place merge: the copying merge's rows, bit for bit, at every -j and when spawns fail" {
    const gpa = testing.allocator;
    // Over 8192 refs, so several workers split the merge into partitions.
    const n = 6000;
    for ([_]bool{ false, true }) |fixed| {
        var want = try mergeAndBuildRows(gpa, fixed, n, 1, false);
        defer want.deinit(gpa);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 } }) |run| {
            if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
            defer spawn_util.resetFailure();
            var got = try mergeAndBuildRows(gpa, fixed, n, run.j, true);
            defer got.deinit(gpa);
            const w = want.group_rows.?;
            const g = got.group_rows.?;
            try testing.expectEqual(w.len, g.len);
            var neg_zero: usize = 0;
            for (w, g) |wr, gr| for (wr, gr) |wv, gv| {
                try testing.expectEqual(std.meta.activeTag(wv), std.meta.activeTag(gv));
                switch (wv) {
                    .s => |s| try testing.expectEqualStrings(s, gv.s),
                    .f => |f| {
                        try testing.expectEqual(@as(u64, @bitCast(f)), @as(u64, @bitCast(gv.f)));
                        if (std.math.signbit(f) and f == 0) neg_zero += 1;
                    },
                    .avg => |a| {
                        try testing.expectEqual(a.sum == null, gv.avg.sum == null);
                        if (a.sum) |sum|
                            try testing.expectEqual(@as(u64, @bitCast(sum)), @as(u64, @bitCast(gv.avg.sum.?)));
                        try testing.expectEqual(a.count, gv.avg.count);
                    },
                    else => try testing.expect(std.meta.eql(wv, gv)),
                }
            };
            // A float sum of -0.0 is never an answer: the copying merge adds every one into +0.0.
            try testing.expectEqual(@as(usize, 0), neg_zero);
        }
    }
}

fn sameAccBits(a: expr_agg.Accumulator, b: expr_agg.Accumulator) bool {
    const F = struct {
        fn f(x: ?f64, y: ?f64) bool {
            if (x == null or y == null) return x == null and y == null;
            return @as(u64, @bitCast(x.?)) == @as(u64, @bitCast(y.?));
        }
        fn s(x: ?[]const u8, y: ?[]const u8) bool {
            if (x == null or y == null) return x == null and y == null;
            return x.?.ptr == y.?.ptr and x.?.len == y.?.len;
        }
    };
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .count => |c| c == b.count,
        .distinct => false,
        .sum_i => |v| v == b.sum_i,
        .min_i => |v| v == b.min_i,
        .max_i => |v| v == b.max_i,
        .min_d => |v| v == b.min_d,
        .max_d => |v| v == b.max_d,
        .sum_f => |v| F.f(v, b.sum_f),
        .min_f => |v| F.f(v, b.min_f),
        .max_f => |v| F.f(v, b.max_f),
        .min_bytes => |v| F.s(v, b.min_bytes),
        .max_bytes => |v| F.s(v, b.max_bytes),
        .avg => |v| F.f(v.sum, b.avg.sum) and v.count == b.avg.count and v.isum == b.avg.isum,
        .sum_d => |v| v.lo == b.sum_d.lo and v.hi == b.sum_d.hi and v.n == b.sum_d.n,
        .avg_d => |v| v.lo == b.avg_d.lo and v.hi == b.avg_d.hi and v.n == b.avg_d.n,
    };
}

test "foldsToItself: merging such an accumulator into a fresh one gives every bit back, and only those" {
    const call = struct {
        fn c(func: expr_agg.AggFunc, result: expr_agg.ResultShape) expr_agg.AggCall {
            return .{ .func = func, .alias = "a", .result = result, .arg = null, .where = null };
        }
    }.c;
    const nan: f64 = std.math.nan(f64);
    const inf: f64 = std.math.inf(f64);
    const big = std.math.maxInt(i64);
    var dec: expr_agg.DecSum = .{};
    dec.add(-(@as(i192, 1) << 150) + 7, 3);
    const Case = struct { call: expr_agg.AggCall, acc: expr_agg.Accumulator };
    const cases = [_]Case{
        .{ .call = call(.count, .i64), .acc = .{ .count = 0 } },
        .{ .call = call(.count, .i64), .acc = .{ .count = big } },
        .{ .call = call(.count, .i64), .acc = .{ .count = big + 1 } },
        .{ .call = call(.sum, .i64), .acc = .{ .sum_i = null } },
        .{ .call = call(.sum, .i64), .acc = .{ .sum_i = -(@as(i128, 1) << 100) } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = null } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -0.0 } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = 0.0 } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = nan } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -inf } },
        .{ .call = call(.sum, .f64), .acc = .{ .sum_f = -2.5 } },
        .{ .call = call(.min, .f64), .acc = .{ .min_f = -0.0 } },
        .{ .call = call(.max, .f64), .acc = .{ .max_f = nan } },
        .{ .call = call(.min, .i64), .acc = .{ .min_i = null } },
        .{ .call = call(.max, .i64), .acc = .{ .max_i = 3 } },
        .{ .call = call(.min, .bytes), .acc = .{ .min_bytes = "pear" } },
        .{ .call = call(.max, .bytes), .acc = .{ .max_bytes = null } },
        .{ .call = call(.avg, .avg_f64), .acc = .{ .avg = .{ .sum = -0.0, .count = 2, .isum = 0 } } },
        .{ .call = call(.avg, .avg_f64), .acc = .{ .avg = .{ .sum = 1.5, .count = 2, .isum = -9 } } },
        .{ .call = call(.sum, .dec), .acc = .{ .sum_d = dec } },
        .{ .call = call(.avg, .avg_dec), .acc = .{ .avg_d = dec } },
        .{ .call = call(.min, .dec), .acc = .{ .min_d = -5 } },
        .{ .call = call(.max, .dec), .acc = .{ .max_d = null } },
    };
    var itself: usize = 0;
    for (cases) |c| {
        var out = expr_agg.Accumulator.init(c.call);
        const merged = out.merge(c.acc, testing.allocator);
        if (foldsToItself(c.acc)) {
            try merged;
            try testing.expect(sameAccBits(c.acc, out));
            itself += 1;
        }
        // The rest (-0.0 and NaN float sums, a count past i64) the merge changes, or refuses, and `foldedAcc` with it.
        const folded = foldedAcc(testing.allocator, c.call, c.acc);
        if (merged) {
            try testing.expect(sameAccBits(try folded, out));
        } else |e| try testing.expectError(e, folded);
    }
    try testing.expectEqual(@as(usize, cases.len - 4), itself);
}

test "in-place merge: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            var out = try mergeAndBuildRows(a, true, 12, 1, true);
            out.deinit(a);
        }
    }.run, .{});
}

test "hash merge of fixed-width tables: an allocation failure frees everything" {
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), struct {
        fn run(a: std.mem.Allocator) !void {
            try mergeFixedAsBytes(a, 24, 18, 1);
        }
    }.run, .{});
}

fn slotFirstLess(_: void, a: MergedSlot, b: MergedSlot) bool {
    return a.first < b.first;
}

test "sortSlotsByFirst: a comparison sort's order at every -j, and when spawns fail" {
    var prng = std.Random.DefaultPrng.init(0x51075);
    const random = prng.random();
    const Spread = enum {
        /// `firstPosition`s over 29 row groups: the key squeezes the row index's unused high bits out.
        positions,
        /// Row indexes that take all 32 bits, so the key is `first` itself.
        high_rows,
        /// Row index 0 everywhere: the key is the row group ordinal alone.
        ordinals,
        /// Every bit differs: five passes, the last one short.
        wide,
        /// Below 2^13: one pass, so the answer is left in the scratch and copied back.
        narrow,
    };
    for ([_]usize{ 0, 1, 2, 1000, 300_000 }) |n| for (std.enums.values(Spread)) |spread| {
        if (spread == .narrow and n > 1 << radix_bits_max) continue;
        const input = try testing.allocator.alloc(MergedSlot, n);
        defer testing.allocator.free(input);
        for (input, 0..) |*s, i| s.* = .{
            .key = "",
            .first = switch (spread) {
                .positions => expr_agg.firstPosition(i % 29, i / 29),
                .high_rows => expr_agg.firstPosition(i % 3, 0xffff_0000 - i / 3),
                .ordinals => expr_agg.firstPosition(i, 0),
                // An odd multiplier permutes the u64s, so these stay unique.
                .wide => @as(u64, i) *% 0x9E3779B97F4A7C15,
                .narrow => i,
            },
            .idx = i,
        };
        random.shuffle(MergedSlot, input);
        const want = try testing.allocator.dupe(MergedSlot, input);
        defer testing.allocator.free(want);
        std.sort.pdq(MergedSlot, want, {}, slotFirstLess);

        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 2 } }) |run| {
            const got = try testing.allocator.dupe(MergedSlot, input);
            defer testing.allocator.free(got);
            if (run.fail_after) |f| spawn_util.injectFailureAfter(f);
            defer spawn_util.resetFailure();
            try sortSlotsByFirst(testing.allocator, got, run.j);
            // Only the largest input has the chunks to spread over threads.
            if (run.fail_after != null and n >= 2 * radix_chunk_min) try testing.expect(spawn_util.failuresInjected() > 0);
            for (want, got) |w, g| {
                try testing.expectEqual(w.first, g.first);
                try testing.expectEqual(w.idx, g.idx);
            }
        }
    };
}

test "sortSlotsByFirst: an allocation failure leaves the slots as they were" {
    var slots: [5]MergedSlot = undefined;
    for (&slots, 0..) |*s, i| s.* = .{ .key = "", .first = 40 - i, .idx = i };
    const before = slots;
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
        sortSlotsByFirst(failing.allocator(), &slots, 1) catch |err| {
            try testing.expectEqual(error.OutOfMemory, err);
            for (before, slots) |b, s| try testing.expectEqual(b.idx, s.idx);
            continue;
        };
        break;
    }
    try testing.expect(fail_index > 0);
    for (slots, 0..) |s, i| try testing.expectEqual(slots.len - 1 - i, s.idx);
}

/// `n` groups for the row-build tests, laid out as one table's: group i has the string key `g<i>`, a count of i and
/// a string min of `m<i>`.
const RowFixture = struct {
    keys: [][]u8,
    accs: []expr_agg.Accumulator,
    slots: []MergedSlot,

    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "m", .result = .bytes, .arg = null, .where = null },
    };
    /// Not in key-then-aggregate order, so a row filled by position would show.
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .agg = 1 }, .name = "m" },
        .{ .source = .{ .key = 0 }, .name = "g" },
        .{ .source = .{ .agg = 0 }, .name = "n" },
    };

    fn init(n: usize) !RowFixture {
        const gpa = testing.allocator;
        const keys = try gpa.alloc([]u8, n);
        @memset(keys, &.{});
        const accs = try gpa.alloc(expr_agg.Accumulator, n * calls.len);
        for (0..n) |i| accs[i * calls.len ..][0..2].* = .{ .{ .count = i }, .{ .min_bytes = null } };
        const slots = try gpa.alloc(MergedSlot, n);
        var f: RowFixture = .{ .keys = keys, .accs = accs, .slots = slots };
        errdefer f.deinit();
        var buf: [32]u8 = undefined;
        for (0..n) |i| {
            const name = try std.fmt.bufPrint(&buf, "g{d}", .{i});
            // The serialized framing of one present string column: a flag, the length, the bytes.
            keys[i] = try gpa.alloc(u8, 5 + name.len);
            keys[i][0] = 1;
            std.mem.writeInt(u32, keys[i][1..5], @intCast(name.len), .little);
            @memcpy(keys[i][5..], name);
            accs[i * calls.len + 1] = .{ .min_bytes = try std.fmt.allocPrint(gpa, "m{d}", .{i}) };
            slots[i] = .{ .key = keys[i], .first = i, .idx = i };
        }
        return f;
    }

    fn deinit(f: *RowFixture) void {
        const gpa = testing.allocator;
        for (f.keys) |k| gpa.free(k);
        gpa.free(f.keys);
        freeOwnedAccumulatorStrings(gpa, f.accs);
        gpa.free(f.accs);
        gpa.free(f.slots);
    }

    fn source(f: *const RowFixture) RowSource {
        return .{
            .slots = f.slots,
            .outputs = &outputs,
            .calls = &calls,
            .key_types = &.{.string},
            .accs = f.accs,
            .folded = false,
            .fixed_keys = false,
        };
    }

    /// Build every row into a fresh answer on `gpa`, which owns it whatever happens.
    fn build(f: *const RowFixture, gpa: std.mem.Allocator, out: *AggOutput, n_workers: usize) !void {
        const rows = try gpa.alloc([]const AggValue, f.slots.len);
        @memset(rows, &.{});
        out.group_rows = rows;
        try buildGroupRows(gpa, &out.slab, f.source(), rows, n_workers);
    }

    fn expectRows(rows: []const []const AggValue) !void {
        var buf: [32]u8 = undefined;
        for (rows, 0..) |row, i| {
            try testing.expectEqual(@as(usize, 3), row.len);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "m{d}", .{i}), row[0].s);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "g{d}", .{i}), row[1].s);
            try testing.expectEqual(@as(i128, @intCast(i)), row[2].i);
        }
    }
};

/// Fails every allocation once `left` have succeeded, and any of exactly `poison_len` bytes, on whichever thread asks:
/// `FailingAllocator` counts without atomics, so it cannot fail one allocation among several threads.
const SharedFailing = struct {
    child: std.mem.Allocator,
    left: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    poison_len: ?usize = null,

    fn allocator(self: *SharedFailing) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *SharedFailing = @ptrCast(@alignCast(ctx));
        if (self.poison_len == len) return null;
        var left = self.left.load(.monotonic);
        while (left != 0) left = self.left.cmpxchgWeak(left, left - 1, .monotonic, .monotonic) orelse
            return self.child.rawAlloc(len, alignment, ret_addr);
        return null;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *SharedFailing = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

test "buildGroupRows: the same rows at every -j, and when spawns fail" {
    // Five blocks of rows, so up to five threads build them.
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    const Run = struct { j: usize, fail_after: ?usize = null };
    for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 2 } }) |run| {
        var out: AggOutput = .{};
        defer out.deinit(testing.allocator);
        if (run.fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();
        try f.build(testing.allocator, &out, run.j);
        if (run.fail_after != null) try testing.expect(spawn_util.failuresInjected() > 0);
        try RowFixture.expectRows(out.group_rows.?);
    }
}

test "TypedAnswer.init: every cell NULL and zero at every -j, and when spawns fail" {
    const lanes = [_]@FieldType(GroupColType, "lane"){ .i64, .f64, .string, .dec, .avg, .avg_dec };
    var types: [lanes.len]GroupColType = undefined;
    for (&types, lanes) |*t, l| t.* = .{
        .nullable = true,
        .lane = l,
        .dec = .{ .precision = 30, .scale = 2 },
        .source = null,
    };
    const n = 3 * TypedAnswer.clear_block + 5;
    const Run = struct { j: usize, fail_after: ?usize = null };
    for ([_]Run{ .{ .j = 1 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 } }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var out: TypedAnswer = undefined;
        try TypedAnswer.init(testing.allocator, &types, n, &out, run.j);
        defer out.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, n), out.rows);
        for (out.columns) |col| switch (col) {
            inline .i64, .f64, .string, .dec => |l, tag| {
                for (l.defs) |d| try testing.expectEqual(@as(u32, 0), d);
                const T = @TypeOf(l.values[0]);
                for (l.values) |v| switch (tag) {
                    .string => try testing.expectEqual(@as(usize, 0), v.len),
                    else => try testing.expectEqual(std.mem.zeroes(T), v),
                };
            },
            inline .avg, .avg_dec => |a| {
                for (a.sum.defs) |d| try testing.expectEqual(@as(u32, 0), d);
                for (a.sum.values) |v| try testing.expectEqual(std.mem.zeroes(@TypeOf(v)), v);
                for (a.count) |c| try testing.expectEqual(@as(i64, 0), c);
            },
        };
    }
}

test "buildGroupRows: an allocation failure leaves every row freeable" {
    // One thread: every allocation failed in turn.
    var small = try RowFixture.init(3);
    defer small.deinit();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, f: *const RowFixture) !void {
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            try f.build(gpa, &out, 1);
        }
    }.run, .{&small});

    // Eight threads: the failure lands in whichever row a thread happens to be building.
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    // Rows are slab views now, so what fails is the slab or a row's own strings (at least one a row).
    for ([_]usize{ 0, 1, 2, 3, 100, 3 * row_block, f.slots.len - 1 }) |left| {
        var failing: SharedFailing = .{ .child = testing.allocator, .left = .init(left) };
        const gpa = failing.allocator();
        var out: AggOutput = .{};
        defer out.deinit(gpa);
        try testing.expectError(error.OutOfMemory, f.build(gpa, &out, 8));
    }
}

test "buildGroupColumns: the rows' values as typed columns at every -j; freeable on any allocation failure" {
    const types = [_]GroupColType{
        .{ .lane = .string, .nullable = true },
        .{ .lane = .string, .nullable = true },
        .{ .lane = .i64, .nullable = false },
    };
    var f = try RowFixture.init(3 * row_block + 77);
    defer f.deinit();
    for ([_]usize{ 1, 3, 8 }) |j| {
        var out: AggOutput = .{};
        defer out.deinit(testing.allocator);
        out.typed = .{ .rows = 0, .columns = &.{} };
        try buildGroupColumns(testing.allocator, &out.typed.?, f.source(), &types, j);
        const t = out.typed.?;
        try testing.expectEqual(f.slots.len, t.rows);
        var buf: [32]u8 = undefined;
        for (0..t.rows) |i| {
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "m{d}", .{i}), t.columns[0].string.values[i]);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "g{d}", .{i}), t.columns[1].string.values[i]);
            try testing.expectEqual(@as(i64, @intCast(i)), t.columns[2].i64.values[i]);
        }
        try testing.expect(!t.columns[2].i64.nulls);
    }
    var small = try RowFixture.init(3);
    defer small.deinit();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, fx: *const RowFixture, ty: []const GroupColType) !void {
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            out.typed = .{ .rows = 0, .columns = &.{} };
            try buildGroupColumns(gpa, &out.typed.?, fx.source(), ty, 1);
        }
    }.run, .{ &small, &types });
    // A count column that may not be NULL refuses a NULL as the row writer does; a narrowing error is the first
    // column's, by row.
    const strict = [_]GroupColType{
        .{ .lane = .string, .nullable = false },
        .{ .lane = .string, .nullable = true },
        .{ .lane = .f64, .nullable = true },
    };
    var out: AggOutput = .{};
    defer out.deinit(testing.allocator);
    out.typed = .{ .rows = 0, .columns = &.{} };
    try testing.expectError(error.TypeMismatch, buildGroupColumns(testing.allocator, &out.typed.?, f.source(), &strict, 8));
}

test "buildGroupRows: the error the lowest failing row gives, at every -j" {
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    // A truncated key fails to read back; a min of `poison_len` bytes fails to copy.
    const poison_len = 77;
    const poisoned: [poison_len]u8 = @splat('x');
    const truncated = [_]u8{1};
    for ([_][2]usize{ .{ 15_000, 7_000 }, .{ 7_000, 15_000 }, .{ 9, 4 * row_block + 100 } }) |at| {
        const bad_key, const bad_min = at;
        const old_key = f.slots[bad_key].key;
        defer f.slots[bad_key].key = old_key;
        f.slots[bad_key].key = &truncated;
        const min_acc = &f.accs[bad_min * RowFixture.calls.len + 1];
        const old_min = min_acc.min_bytes;
        defer min_acc.min_bytes = old_min;
        min_acc.min_bytes = &poisoned;
        const want: anyerror = if (bad_key < bad_min) error.BadGroupKey else error.OutOfMemory;
        for ([_]usize{ 1, 3, 8 }) |j| {
            var failing: SharedFailing = .{ .child = testing.allocator, .poison_len = poison_len };
            const gpa = failing.allocator();
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            try testing.expectError(want, f.build(gpa, &out, j));
        }
    }
}

test "runMultiAggregate: a big GROUP BY answers the same rows in the same order at every -j" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    // `v` is unique per row (20000 groups, so several blocks of rows), `n` a fixed-width key, `k` skewed with nulls.
    for ([_][]const u8{ "v", "n", "k, v" }) |key| for ([_]bool{ false, true }) |unordered| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var base: ?MultiAggResult = null;
        defer if (base) |b| freeResult(b);
        const Run = struct { j: usize, fail_after: ?usize = null };
        for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 5 } }) |run| {
            if (run.fail_after) |n| spawn_util.injectFailureAfter(n);
            defer spawn_util.resetFailure();
            var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(*) AS c, min(v) AS lo, sum(n) AS s",
                .group_by = key,
                .parallelism = run.j,
                .min_bytes_per_worker = 1,
                .unordered = unordered,
            });
            if (run.fail_after != null) try testing.expect(spawn_util.failuresInjected() > 0);
            const rows = res.output.group_rows.?;
            if (std.mem.eql(u8, key, "v")) try testing.expectEqual(@as(usize, 20_000), rows.len);
            const b = base orelse {
                base = res;
                continue;
            };
            defer res.deinit(testing.allocator);
            const want = b.output.group_rows.?;
            try testing.expectEqual(want.len, rows.len);
            if (unordered) {
                // Unordered rows follow the merge's partitions, which change with -j: compare as sets.
                const Less = struct {
                    fn lt(_: void, x: []const AggValue, y: []const AggValue) bool {
                        for (x, y) |xv, yv| {
                            const o = orderValues(xv, yv, false);
                            if (o != .eq) return o == .lt;
                        }
                        return false;
                    }
                };
                const w = try testing.allocator.dupe([]const AggValue, want);
                defer testing.allocator.free(w);
                const g = try testing.allocator.dupe([]const AggValue, rows);
                defer testing.allocator.free(g);
                std.sort.pdq([]const AggValue, w, {}, Less.lt);
                std.sort.pdq([]const AggValue, g, {}, Less.lt);
                for (w, g) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
            } else for (want, rows) |wr, gr| for (wr, gr) |x, y| try testing.expect(sameValue(x, y));
        }
    };
}

test "runMultiAggregate: a NULL first key of two fixed-width keys still reads the second back" {
    // A fixed-width key image pads a NULL column's payload with zeros; read as framing, the pad would be taken for
    // the next column's flag and `id` would come back NULL.
    const bytes = (try loadFixture(null_pages_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = null_pages_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "v, id",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        // `id` is unique and never NULL; `v` is NULL in 650 of the 2000 rows.
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 2000), rows.len);
        var null_v: usize = 0;
        var id_sum: i128 = 0;
        for (rows) |row| {
            if (row[0] == .null_val) null_v += 1;
            id_sum += row[1].i;
        }
        try testing.expectEqual(@as(usize, 650), null_v);
        try testing.expectEqual(@as(i128, 1999 * 2000 / 2), id_sum);
    }
}

test "regression: ordered grouping retains the small-query memory budget at one worker" {
    const path = "ci/fixtures/parquet/group_order.parquet";
    const bytes = try metadata.readFileSlice(path, testing.allocator);
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{ .{ .name = path, .bytes = bytes }, .{ .name = path, .bytes = bytes } };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const res = try runMultiAggregate(testing.allocator, arena.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n",
        .group_by = "k",
        .parallelism = 1,
        .max_memory = 4096,
        .scan_all = true,
    });
    defer freeResult(res);
    try testing.expectEqual(@as(usize, 6), res.output.group_rows.?.len);
}

test "statistics never invent a zero of the other sign: --trust-stats answers zero extrema as decoding does" {
    // Arrow records -0 / +0 bounds for any chunk holding a zero, all +0 or all -0 alike.
    const path = "ci/fixtures/parquet/signed_zero.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    for ([_]?[]const u8{ null, "k = 0", "k = 1", "k = 2", "k >= 2" }) |filter| {
        var answers: [2][2]u64 = undefined;
        for ([_]bool{ false, true }, 0..) |trust, t| {
            const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .filter = filter,
                .aggregate = "min(v) AS lo, max(v) AS hi",
                .trust_stats = trust,
                .scan_all = !trust,
            });
            defer freeResult(res);
            answers[t] = .{ @bitCast(res.output.aggs[0].value.f), @bitCast(res.output.aggs[1].value.f) };
        }
        try testing.expectEqual(answers[0], answers[1]);
    }
}

// ============================================================ Exact DECIMAL.
//
// The fixture (tools/gen_decimal_exact_fixture.py) stores decimals as INT32, INT64 and FLBA(16), dictionary-encoded,
// in three row groups. Every expected value is DuckDB's answer for the same query, as unscaled integer and type.

const decimal_exact_fixture = "ci/fixtures/parquet/decimal_exact.parquet";

const DecWant = struct { alias: []const u8, v: i128, p: u8, s: u8 };

fn expectDecimalAnswers(args: MultiAggArgs, want: []const DecWant) !void {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
    defer freeResult(res);
    for (want) |w| {
        const item = for (res.output.aggs) |it| {
            if (std.mem.eql(u8, it.alias, w.alias)) break it;
        } else return error.TestUnexpectedResult;
        const got: DecValue = switch (item.value) {
            .dec => |d| d,
            .avg_dec => |a| .{ .v = a.mean.?, .spec = a.spec },
            else => {
                std.debug.print("{s}: not a DECIMAL answer ({s})\n", .{ w.alias, @tagName(item.value) });
                return error.TestUnexpectedResult;
            },
        };
        testing.expectEqual(DecValue{ .v = w.v, .spec = .{ .precision = w.p, .scale = w.s } }, got) catch |err| {
            std.debug.print("{s}: got {d} DECIMAL({d},{d})\n", .{ w.alias, got.v, got.spec.precision, got.spec.scale });
            return err;
        };
    }
}

test "DECIMAL aggregates and arithmetic are exact, on every decode path and worker count" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx9", .v = 15049, .p = 9, .s = 2 },
        .{ .alias = "s18", .v = 344850, .p = 38, .s = 4 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
        .{ .alias = "sdn", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "sq", .v = 45000, .p = 38, .s = 2 },
        // DECIMAL(9,2) * DECIMAL(18,4) is DECIMAL(27,6); a sum is DECIMAL(38, s).
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        // Mixed with an INT32 column (DECIMAL(10,0)) and with literals, as SQL types them.
        .{ .alias = "di", .v = -30150, .p = 38, .s = 2 },
        .{ .alias = "dti", .v = 227255050, .p = 38, .s = 2 },
        .{ .alias = "dd", .v = 450015000000000, .p = 38, .s = 10 },
        .{ .alias = "dlit", .v = -227250, .p = 38, .s = 3 },
        .{ .alias = "ilit", .v = -2250, .p = 38, .s = 1 },
        .{ .alias = "neg", .v = 15150, .p = 38, .s = 2 },
        .{ .alias = "co", .v = 3670000, .p = 38, .s = 2 },
        .{ .alias = "cc", .v = 3649800, .p = 38, .s = 2 },
        // avg keeps the argument's type, rounded half away from zero: 36700.00 / 133 = 275.939...
        // avg over DECIMAL(15,2) is DECIMAL(19,6); over DECIMAL(38,10) its own type. avg_state ranks by the mean
        // at the argument's scale.
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
        .{ .alias = "a38", .v = 1495000000000, .p = 38, .s = 10 },
        .{ .alias = "sadn", .v = 27594, .p = 15, .s = 2 },
    };
    const aggregate =
        "sum(d9) AS s9, min(d9) AS mn9, max(d9) AS mx9, sum(d18) AS s18, sum(d38) AS s38, min(d38) AS mn38, " ++
        "max(d38) AS mx38, sum(dn) AS sdn, sum(q) AS sq, sum(d9 * d18) AS p, sum(d9 + i) AS di, " ++
        "sum(d9 * i) AS dti, sum(d38 - d9) AS dd, sum(d9 * 1.5) AS dlit, sum(i * 1.5) AS ilit, sum(-d9) AS neg, " ++
        "sum(coalesce(dn, 0)) AS co, sum(coalesce(dn, d9)) AS cc, avg(dn) AS adn, avg(d38) AS a38, " ++
        "avg_state(dn) AS sadn";
    for ([_]bool{ true, false }) |encoded| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .encoded = encoded,
        }, &want);
    };
    // A filter: Q6's shape over the decimals (a sparse selection gathers the exact values with the rows).
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(d9 * d18) AS p, sum(q) AS sq",
        .filter = "q >= 1.00 AND q <= 2.00 AND i < 100",
        .parallelism = 1,
    }, &.{
        .{ .alias = "p", .v = -296209063, .p = 38, .s = 6 },
        .{ .alias = "sq", .v = 18700, .p = 38, .s = 2 },
    });
}

test "a DECIMAL result past its precision is an error, never a wrapped or rounded value" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    // Two values of 10^38 - 1 sum past 38 digits; each row of `big * 10` and `big + big` is past them already; and
    // `big + 0.1` first brings `big` to the common DECIMAL(38,1), where 10^38 - 1 has no room (as the SQL engines
    // refuse that cast).
    for ([_][]const u8{ "sum(big) AS s", "sum(big * 10) AS s", "sum(big + big) AS s", "sum(big + 0.1) AS s" }) |agg| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = agg,
            .parallelism = 1,
        }));
    }
    // The same column without its two wide rows fits.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 298, .p = 38, .s = 0 }});
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "sum(big + 0.1) AS s",
        .filter = "i >= -148",
        .parallelism = 1,
    }, &.{.{ .alias = "s", .v = 3278, .p = 38, .s = 1 }});
    // A mean fits wherever its values do, though their sum does not: avg divides the wide sum. (2 × (10^38 - 1) +
    // 298) / 300, rounded half away from zero.
    try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "avg(big) AS a",
        .parallelism = 1,
    }, &.{.{ .alias = "a", .v = 666666666666666666666666666666666668, .p = 38, .s = 0 }});
}

test "DECIMAL aggregates over no values are NULL, and GROUP BY a DECIMAL key is exact" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Row group 1 holds only NULL `dn`.
    const empty = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "sum(dn) AS s, min(dn) AS m, avg(dn) AS a, avg_state(dn) AS st",
        .filter = "i >= -50 AND i < 50",
        .parallelism = 1,
    });
    defer freeResult(empty);
    try testing.expect(empty.output.aggs[0].value == .null_val);
    try testing.expect(empty.output.aggs[1].value == .null_val);
    try testing.expect(empty.output.aggs[2].value == .null_val);
    const a = empty.output.aggs[3].value.avg_dec;
    try testing.expect(a.sum == null and a.mean == null and a.count == 0);

    const grouped = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
        .inputs = &inputs,
        .aggregate = "count(*) AS n, sum(d18 * q) AS p",
        .group_by = "dn",
        .filter = "i >= -60 AND i <= 60",
        .parallelism = 1,
    });
    defer freeResult(grouped);
    const rows = grouped.output.group_rows.?;
    // 121 rows: 108 have a NULL `dn`, the other 13 a distinct value each.
    try testing.expectEqual(@as(usize, 14), rows.len);
    var nulls: usize = 0;
    for (rows) |row| switch (row[0]) {
        .null_val => {
            nulls += 1;
            try testing.expectEqual(@as(i128, 108), row[1].i);
        },
        .dec => |k| {
            try testing.expectEqual(expr_ast.Dec{ .precision = 15, .scale = 2 }, k.spec);
            try testing.expectEqual(@as(i128, 1), row[1].i);
            try testing.expectEqual(@as(u8, 6), row[2].dec.spec.scale);
        },
        else => return error.TestUnexpectedResult,
    };
    try testing.expectEqual(@as(usize, 1), nulls);
}

test "--trust-stats answers DECIMAL min/max from statistics exactly, as decoding does" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "mn9", .v = -15150, .p = 9, .s = 2 },
        .{ .alias = "mx18", .v = 1299, .p = 18, .s = 4 },
        .{ .alias = "mn38", .v = -123456789012345678901234561234567891, .p = 38, .s = 10 },
        .{ .alias = "mx38", .v = 123456789012345678901237551234567891, .p = 38, .s = 10 },
    };
    for ([_]bool{ false, true }) |trust| try expectDecimalAnswers(.{
        .inputs = &inputs,
        .aggregate = "min(d9) AS mn9, max(d18) AS mx18, min(d38) AS mn38, max(d38) AS mx38",
        .parallelism = 1,
        .trust_stats = trust,
    }, &want);
}

test "a decode budget slices wide row groups, FIXED_LEN_BYTE_ARRAY DECIMAL included, with the same answer" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    const want = [_]DecWant{
        .{ .alias = "s9", .v = -15150, .p = 38, .s = 2 },
        .{ .alias = "s38", .v = 448500000000000, .p = 38, .s = 10 },
        .{ .alias = "p", .v = 209832550, .p = 38, .s = 6 },
        .{ .alias = "adn", .v = 275939850, .p = 19, .s = 6 },
    };
    const aggregate = "sum(d9) AS s9, sum(d38) AS s38, sum(d9 * d18) AS p, avg(dn) AS adn";
    // Unbounded; then budgets too small for a whole row group per worker, down to a few rows a slice.
    for ([_]?usize{ null, 10_000, 1000 }) |limit| for ([_]usize{ 1, 3 }) |j| {
        try expectDecimalAnswers(.{
            .inputs = &inputs,
            .aggregate = aggregate,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .decode_limit = limit,
        }, &want);
    };
    // GROUP BY slices too.
    for ([_]?usize{ null, 16_000, 1000 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "sum(d18 * q) AS p, max(dn) AS mx",
            .group_by = "k",
            .parallelism = 1,
            .decode_limit = limit,
        });
        defer freeResult(res);
        const rows = res.output.group_rows.?;
        try testing.expectEqual(@as(usize, 3), rows.len);
        try testing.expectEqual(@as(i128, 17235000), rows[0][1].dec.v);
    }
}

test "a decode budget that slices row groups keeps where each group first appeared" {
    // Unique keys in shuffled order: a LIMIT with no ORDER BY, or a count tied at 1, returns the first to appear. A
    // cut of 300 ends past the first slices of the first row group.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]expr_ast.ResultOrder{
        .{ .limit = 300 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 300 },
    }) |cut| for ([_][]const u8{ "u", "s" }) |key| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = key,
            .select_cols = try selectList(arena, &.{ key, "count(*) AS c", "sum(t) AS st" }),
            .parallelism = 1,
            .result_order = cut,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.decode_limit = 3000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const w = want.output.group_rows.?;
        const g = got.output.group_rows.?;
        try testing.expectEqual(w.len, g.len);
        for (w, g) |wr, gr| for (wr, gr) |a, b| try testing.expect(sameValue(a, b));
    };
}

test "a first-keys cut under a decode budget ends inside a later slice where the full scan does" {
    // Keys 3-9 count 7; the ninth group to appear is key 8 (tools/gen_census_fixture.py).
    const path = "ci/fixtures/parquet/census_sliced.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]?usize{ 100, 150, null }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .group_by = "k",
            .aggregate = "count(*) AS c",
            .parallelism = 1,
            .decode_limit = limit,
            .result_order = .{ .limit = 1, .offset = 8 },
        };
        for ([_]?SketchTopK{ null, .{} }) |sketch| {
            args.sketch_topk = sketch;
            var res = try runMultiAggregate(testing.allocator, arena, args);
            defer res.deinit(testing.allocator);
            try res.output.applyResultOrder(testing.allocator, args.result_order);
            const rows = res.output.group_rows.?;
            try testing.expectEqual(@as(usize, 1), rows.len);
            try testing.expectEqual(@as(i128, 8), rows[0][0].i);
            try testing.expectEqual(@as(i128, 7), rows[0][1].i);
        }
    }
}

test "a count cut inside the merge keeps exactly the groups the full sort would, ties by first appearance" {
    // Four tables over 40,000 keys, 80,000 refs: enough for the cut to split the merge into partitions. Most keys
    // count 2 (seen by two tables); every 9973rd counts 3, so the top 25 holds the five count-3 keys and a run of the
    // earliest count-2 keys, decided by first appearance.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var buf: [32]u8 = undefined;
    const n_keys: usize = 40_000;
    for (0..n_keys) |i| {
        const key = std.fmt.bufPrint(&buf, "k{d}", .{(i * 7919) % n_keys}) catch unreachable;
        for ([_]usize{ i % 4, (i + 1) % 4 }) |ti| {
            const id = try tables[ti].getOrInsert(key, &calls, i * 4 + ti);
            tables[ti].accumulators.items[id].count += if (i % 9973 == 0 and ti == i % 4) 2 else 1;
        }
    }
    var refs: std.ArrayList(MergeGroupRef) = .empty;
    defer refs.deinit(testing.allocator);
    for (&tables, 0..) |*t, ti| for (0..t.groupCount()) |g| try refs.append(testing.allocator, .{ .table = @intCast(ti), .group = @intCast(g) });
    try testing.expect(mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)) > 1);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const Ranked = struct { c: u64, first: u64, key: []const u8 };
    const rank = struct {
        fn lt(_: void, a: Ranked, b: Ranked) bool {
            return a.c > b.c or (a.c == b.c and a.first < b.first);
        }
    }.lt;
    var lists: [2][]Ranked = undefined;
    for ([_]?ResultCut{ null, .{ .a_idx = 0, .k = 25 } }, &lists) |cut, *out| {
        var slots: std.ArrayList(MergedSlot) = .empty;
        defer slots.deinit(testing.allocator);
        var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer accs.deinit(testing.allocator);
        try mergeFoldedRefs(testing.allocator, testing.allocator, &tables, refs.items, &calls, 8, cut, false, &slots, &accs);
        if (cut != null) try testing.expectEqual(@as(usize, 25) * mergePartitionCount(8, refs.items.len, true, mergeEntryBytes(calls.len)), slots.items.len);
        const ranked = try arena.alloc(Ranked, slots.items.len);
        for (slots.items, ranked) |s, *r| r.* = .{ .c = accs.items[s.idx].count, .first = s.first, .key = s.key };
        std.sort.pdq(Ranked, ranked, {}, rank);
        out.* = ranked[0..25];
    }
    for (lists[0], lists[1]) |want, got| {
        try testing.expectEqual(want.c, got.c);
        try testing.expectEqual(want.first, got.first);
        try testing.expectEqualStrings(want.key, got.key);
    }
}

test "firstCandidates: every table's earliest groups hold the groups that appeared first overall" {
    // Table 0 saw a..h at 10..17; table 1 saw d at 0 and its own keys at 100.. . The two earliest overall are d (first
    // seen by table 1) and a (table 0), each among the two earliest of the table that first saw it.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [2]expr_agg.GroupTable = .{ .init(testing.allocator, 1 << 20), .init(testing.allocator, 1 << 20) };
    defer for (&tables) |*t| t.deinit();
    for ([_][]const u8{ "a", "b", "c", "d", "e", "f", "g", "h" }, 10..) |key, pos| _ = try tables[0].getOrInsert(key, &calls, pos);
    _ = try tables[1].getOrInsert("d", &calls, 0);
    for ([_][]const u8{ "x", "y", "z", "w", "v", "u", "t" }, 100..) |key, pos| _ = try tables[1].getOrInsert(key, &calls, pos);
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const kept = (try firstCandidates(arena_state.allocator(), &tables, 2)).?;
    try testing.expect(kept.len <= 4);
    for ([_][]const u8{ "d", "a" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
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

test "parallelDistinctCountU64 counts the exact union of overlapping sets" {
    const a = testing.allocator;
    var sets: [3]expr_agg.DistinctSet = .{ .{}, .{}, .{} };
    defer for (&sets) |*d| d.deinit(a);
    const ranges = [_][2]u64{ .{ 0, 50_000 }, .{ 25_000, 75_000 }, .{ 60_000, 100_000 } };
    for (&sets, ranges) |*d, r| for (r[0]..r[1]) |v| try d.insertU64(a, v * 7919);
    const sources = [_]*const expr_agg.DistinctSet{ &sets[0], &sets[1], &sets[2] };
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    for ([_]usize{ 2, 4, 16 }) |workers| {
        try testing.expectEqual(@as(u64, 100_000), try parallelDistinctCountU64(a, arena_state.allocator(), &sources, workers));
    }
}

test "runMultiAggregate: returned count(DISTINCT) sets no longer point at the scan's budget" {
    const path = "ci/fixtures/parquet/topk_skew.parquet";
    const bytes = metadata.readFileSlice(path, testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 4 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT k) AS u, count(DISTINCT n) AS w",
            .parallelism = parallelism,
            .min_bytes_per_worker = 1,
        });
        defer res.deinit(testing.allocator);
        for (res.state.accumulators) |acc| {
            const d = acc.distinct.heap() orelse continue;
            try testing.expect(d.shared == null);
            if (d.parts) |parts| for (parts) |p| try testing.expect(p.shared == null);
        }
    }
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
        firstCutEligible(args, in.calls, use_blocks, in.work.len, tables);
    const census_fits = blk: {
        if (in.work.len >= expr_agg.Census.max_items) break :blk false;
        var rows: u64 = 0;
        for (in.work) |item| {
            if (item.agg_start != 0) break :blk false;
            rows +|= @intCast(@max(0, in.metas[item.file].row_groups.items[item.rg].num_rows));
        }
        const open = in.workers * expr_agg.Census.N_PARTS * @sizeOf(expr_agg.Census.Chunk);
        break :blk (rows *| 8) +| open <= args.max_memory / 4;
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

test "planAggregate: frozen dispatch agrees across fixtures, knobs, filters and cuts" {
    // The shared binder compares the frozen dispatch and planner; empty input bytes also forbid reading any pages.
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, filters: []const ?[]const u8 }{
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .keys = &.{ "k", "n", "v", "k, n", "n + 1 AS shifted" },
            .filters = &.{ null, "n < 12", "v > '10'", "k LIKE '%a%'" },
        },
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{ "u", "t", "s", "u, s" },
            .filters = &.{ null, "u > 300000000000", "s LIKE '%x%'" },
        },
    };
    const configs = [_]?SketchTopK{
        null,
        .{},
        .{ .cell_bits = 3 },
        .{ .cell_bits = 8, .admit_per_row = 1 },
        .{ .census = false },
        .{ .fuse_census = true },
        .{ .fuse_census = false },
        .{ .certify = false, .census_fp_bits = 3, .census_check_rows = 64 },
    };
    const orders = [_]expr_ast.ResultOrder{
        .{},
        .{ .limit = 3 },
        .{ .limit = 0 },
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 3 },
        .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 3 },
    };
    var seen = std.EnumSet(@FieldType(AggregateDispatch, "strategy")){};
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = &.{} }};
        for (fixture.keys) |key| for (fixture.filters) |filter| for (configs) |cfg| for (orders) |order| {
            for ([_]usize{ 1, 3 }) |parallelism| for ([_]usize{ 1024, 512 << 20 }) |max_memory| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .ndv_policy = false,
                    .metas = &metas,
                    .aggregate = "count(*) AS c",
                    .group_by = key,
                    .filter = filter,
                    .parallelism = parallelism,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = cfg,
                    .result_order = order,
                    .max_memory = max_memory,
                });
                seen.insert(explained.plan.dispatch.strategy);
            };
        };
    }
    for ([_]@FieldType(AggregateDispatch, "strategy"){ .plain_hash, .sketch, .first_keys }) |strategy|
        try testing.expect(seen.contains(strategy));
    try testing.expectEqual(@as(usize, 0), spawn_util.failuresInjected());
}

test "planAggregate: scalar, ordered blocks, empty work and gated input stay unchanged" {
    const Gate = struct {
        var marker: u8 = 0;
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.UnexpectedPageRead;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    for ([_][]const u8{ "group_order", "no_row_groups" }) |fixture| {
        var meta_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer meta_arena.deinit();
        const path = try std.fmt.allocPrint(meta_arena.allocator(), "ci/fixtures/parquet/{s}.parquet", .{fixture});
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const metas = [_]schema.FileMetaData{try metadata.open(meta_arena.allocator(), bytes)};
        for ([_]bool{ false, true }) |gated| for ([_]bool{ false, true }) |unordered| {
            for ([_]bool{ false, true }) |grouped| for ([_]bool{ false, true }) |scan_all| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const inputs = [_]Input{.{ .name = path, .bytes = &.{}, .gate = if (gated)
                    .{ .ctx = &Gate.marker, .wait = Gate.wait, .done = Gate.done }
                else
                    null }};
                const has_float = std.mem.eql(u8, fixture, "group_order");
                const explained = try explainAggregate(testing.allocator, arena_state.allocator(), .{
                    .inputs = &inputs,
                    .metas = &metas,
                    .aggregate = if (has_float) "count(*) AS c, sum(x) AS s" else "count(*) AS c",
                    .group_by = if (grouped) (if (std.mem.eql(u8, fixture, "group_order")) "k" else "s") else null,
                    .parallelism = 3,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = .{},
                    .result_order = .{ .limit = 3 },
                    .unordered = unordered,
                    .scan_all = scan_all,
                });
                if (!grouped) try testing.expectEqual(.scalar, explained.plan.dispatch.strategy);
                if (gated) try testing.expect(explained.plan.dispatch.count_agg == null);
                if (grouped and !unordered and std.mem.eql(u8, fixture, "group_order"))
                    try testing.expect(explained.plan.dispatch.use_blocks);
            };
        };
    }
}
test "review round3 count budget and path matrix" {
    for ([_][]const u8{ "ci/fixtures/parquet/census_topk.parquet", "ci/fixtures/parquet/census_dictionary_review.parquet" }) |path| {
        const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
        for ([_]?usize{ null, 3000, 20000 }) |limit| for ([_]bool{ false, true }) |encoded|
            for ([_][]const u8{ "u", "s" }) |key| for ([_]?[]const u8{ null, "n = 1", "t > 50000000", "t > 3" }) |filter|
                for ([_]SketchTopK{ .{ .fuse_census = true }, .{ .fuse_census = false }, .{ .certify = false }, .{ .census_fp_bits = 0 } }) |cfg| {
                    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                    defer arena_state.deinit();
                    const arena = arena_state.allocator();
                    var args: MultiAggArgs = .{
                        .inputs = &inputs,
                        .filter = filter,
                        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
                        .group_by = key,
                        .parallelism = 1,
                        .decode_limit = limit,
                        .encoded = encoded,
                        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
                    };
                    var want = try runMultiAggregate(testing.allocator, arena, args);
                    defer want.deinit(testing.allocator);
                    args.sketch_topk = cfg;
                    var got = try runMultiAggregate(testing.allocator, arena, args);
                    defer got.deinit(testing.allocator);
                    try want.output.applyResultOrder(testing.allocator, args.result_order);
                    try got.output.applyResultOrder(testing.allocator, args.result_order);
                    try testing.expectEqual(want.rows_in, got.rows_in);
                    try testing.expectEqual(want.rows_kept, got.rows_kept);
                    const wr = want.output.group_rows.?;
                    const gr = got.output.group_rows.?;
                    try testing.expectEqual(wr.len, gr.len);
                    for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
                };
    }
}

test "runMultiAggregate: workers that move to partition runs mid-scan answer as the plain fold" {
    // census_topk's `s`: near-unique 20-byte strings, 8 row groups of 1000. A threshold of 64 groups moves a worker
    // after its first window; at -j 3 some workers may never move and still join the runs.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const aggregates = [_][]const u8{
        "count(*) AS c, sum(t) AS st, min(s) AS lo, max(u) AS hi",
        "count(n) AS c, avg(t) AS av",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "s", "s, n" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = aggregate,
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .result_order = cut,
            .spill = false,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.spill = true;
        args.spill_groups = 64;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expect(!want.part_agg);
        try testing.expect(got.part_agg);
        try want.output.applyResultOrder(testing.allocator, cut);
        try got.output.applyResultOrder(testing.allocator, cut);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "runMultiAggregate: a budget the plain fold fits in holds every worker's partition runs" {
    // Each of thousands of runs drawing a shared-budget block once failed a budget the plain fold uses a sliver of.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]bool{ false, true }) |spill| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var res = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = 3,
            .min_bytes_per_worker = 1,
            .max_memory = 4 << 20,
            .spill = spill,
            .spill_groups = 64,
        });
        defer res.deinit(testing.allocator);
        try testing.expectEqual(spill, res.part_agg);
        try testing.expectEqual(@as(usize, 7997), res.output.group_rows.?.len);
    }
}

const NoGate = struct {
    var ctx: u8 = 0;
    fn wait(_: *anyopaque, _: usize) anyerror!void {}
    fn done(_: *anyopaque, _: usize, _: usize) void {}
    const gate: RowGroupGate = .{ .ctx = &ctx, .wait = wait, .done = done };
};

/// The query's outcome at each budget, an error or the group count, for comparing fast paths with their fallback.
fn outcomes(args: MultiAggArgs, budgets: []const usize, out: []anyerror!usize) void {
    for (budgets, out) |budget, *o| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var a = args;
        a.max_memory = budget;
        o.* = if (runMultiAggregate(testing.allocator, arena_state.allocator(), a)) |res| blk: {
            var r = res;
            defer freeResult(r);
            r.output.applyResultOrder(testing.allocator, args.result_order) catch |e| break :blk e;
            break :blk r.output.group_rows.?.len;
        } else |e| e;
    }
}

fn expectSameOutcomes(want: []const (anyerror!usize), got: []const (anyerror!usize)) !void {
    for (want, got) |w, g| {
        if (w) |n| try testing.expectEqual(n, try g) else |e| try testing.expectError(e, g);
    }
}

test "regression: string-key spill answers every budget the plain fold answers, gated inputs included" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const budgets = [_]usize{ 96 << 10, 192 << 10, 384 << 10, 768 << 10, 1536 << 10, 4 << 20 };
    // A gate that never waits: with the mapped file beside it the input reads again, without it a spill is not armed.
    const plain_in = [_]Input{.{ .name = path, .bytes = bytes }};
    const gated_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate, .plan_bytes = bytes }};
    const unmapped_in = [_]Input{.{ .name = path, .bytes = bytes, .gate = NoGate.gate }};
    for ([_]usize{ 1, 3 }) |j| {
        const base: MultiAggArgs = .{
            .inputs = &plain_in,
            .aggregate = "count(*) AS c, sum(t) AS st",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .spill_groups = 64,
        };
        var want: [budgets.len]anyerror!usize = undefined;
        var no_spill = base;
        no_spill.spill = false;
        outcomes(no_spill, &budgets, &want);
        for ([_][]const Input{ &plain_in, &gated_in, &unmapped_in }) |inputs| {
            var got: [budgets.len]anyerror!usize = undefined;
            var a = base;
            a.inputs = inputs;
            outcomes(a, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
    }
}

test "regression: a ranking result cut answers every budget the uncut merge answers" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    const budgets = [_]usize{ 96 << 10, 192 << 10, 384 << 10, 768 << 10, 4 << 20 };
    for ([_]usize{ 1, 3 }) |j| {
        const uncut: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "sum(t) AS st, count(*) AS c",
            .group_by = "s",
            .parallelism = j,
            .min_bytes_per_worker = 1,
        };
        var cut = uncut;
        cut.result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
        var want: [budgets.len]anyerror!usize = undefined;
        var got: [budgets.len]anyerror!usize = undefined;
        outcomes(uncut, &budgets, &want);
        outcomes(cut, &budgets, &got);
        for (want, got) |w, g| {
            if (w) |_| try testing.expectEqual(@as(usize, 10), try g) else |e| try testing.expectError(e, g);
        }
    }
}

test "runMultiAggregate: grouped count(DISTINCT) through pairs answers as through sets" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // `t` holds 7 keys, `u` is near unique, `n` is NULL through the first row group: a key's distinct values, ones
    // only NULL counts, a FILTER, other aggregates beside it, and a cut on the distinct count.
    const aggregates = [_][]const u8{
        "count(DISTINCT u) AS d, count(*) AS c",
        "count(DISTINCT n) AS d, sum(u) AS s",
        "count(DISTINCT u) FILTER (WHERE n = 1) AS d, min(u) AS lo",
    };
    const cuts = [_]expr_ast.ResultOrder{
        .{},
        .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 },
    };
    for (aggregates) |aggregate| for ([_][]const u8{ "t", "u" }) |key| for ([_]usize{ 1, 3 }) |j| for (cuts) |cut| {
        for ([_]bool{ false, true }) |encoded| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const arena = arena_state.allocator();
            var args: MultiAggArgs = .{
                .inputs = &inputs,
                .aggregate = aggregate,
                .group_by = key,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .encoded = encoded,
                .result_order = cut,
                .distinct_pairs = .off,
            };
            var want = try runMultiAggregate(testing.allocator, arena, args);
            defer want.deinit(testing.allocator);
            args.distinct_pairs = .on;
            var got = try runMultiAggregate(testing.allocator, arena, args);
            defer got.deinit(testing.allocator);
            try testing.expect(!want.distinct_pairs);
            try testing.expect(got.distinct_pairs);
            try want.output.applyResultOrder(testing.allocator, cut);
            try got.output.applyResultOrder(testing.allocator, cut);
            const wr = want.output.group_rows.?;
            const gr = got.output.group_rows.?;
            try testing.expectEqual(wr.len, gr.len);
            for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
        }
    };
}

test "runMultiAggregate: workers judge their sets after a pilot and move them to pairs only where pairs pay" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // By `t` (7 keys) every group gains new values: its sets move, at -j 3 alongside workers still on sets. By `u`
    // (near unique) a group is a row or two: the sets stay.
    for ([_][]const u8{ "t", "u" }, [_]bool{ true, false }) |key, moves| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = key,
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(moves, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "mergePartitionCount sizes a full merge's partitions by their state, never below one a worker" {
    const entry = mergeEntryBytes(2);
    // Few references keep one partition a worker; near-unique millions split until a partition's state fits.
    try testing.expectEqual(@as(usize, 1), mergePartitionCount(16, 4000, false, entry));
    try testing.expectEqual(@as(usize, 16), mergePartitionCount(16, 16_000, false, entry));
    const many = mergePartitionCount(16, 27_000_000, false, entry);
    try testing.expect(many > 16 and many <= merge_parts_max);
    try testing.expect(27_000_000 * entry / many <= merge_part_bytes);
    try testing.expectEqual(@as(usize, merge_parts_max), mergePartitionCount(16, 1 << 40, false, entry));
    // A cut keeps its own rule.
    try testing.expectEqual(@as(usize, 512), mergePartitionCount(16, 27_000_000, true, entry));
}

test "runMultiAggregate: a pilot short of rows a group judges again at twice the rows" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // 200 groups of nearly unique values over 1000-row groups: five rows a group after the first row group, ten after
    // the second. Without re-checks the sets stay; with them every worker reading two row groups or more moves, and at
    // -j 3 some worker reads three of the eight while another may read one.
    for ([_]u8{ 0, 3 }) |rechecks| for ([_]usize{ 1, 3 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .ndv_policy = false,
            .aggregate = "count(DISTINCT u) AS d, count(*) AS c",
            .group_by = "u / 5000000000 AS b",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .distinct_pairs = .off,
        };
        var want = try runMultiAggregate(testing.allocator, arena, args);
        defer want.deinit(testing.allocator);
        args.distinct_pairs = .auto;
        args.pairs_pilot_rows = 1000;
        args.pairs_rechecks = rechecks;
        var got = try runMultiAggregate(testing.allocator, arena, args);
        defer got.deinit(testing.allocator);
        try testing.expectEqual(rechecks > 0, got.distinct_pairs);
        const wr = want.output.group_rows.?;
        const gr = got.output.group_rows.?;
        try testing.expectEqual(wr.len, gr.len);
        for (wr, gr) |a, b| for (a, b) |av, bv| try testing.expect(sameValue(av, bv));
    };
}

test "regression: count(DISTINCT) pairs answer every budget the set path answers, as the set path does" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var answered: usize = 0;
    for ([_]usize{ 64 << 10, 256 << 10, 1 << 20, 512 << 20 }) |budget| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]?MultiAggResult = .{ null, null };
        defer for (results) |r| if (r) |res| freeResult(res);
        var errs: [2]?anyerror = .{ null, null };
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, 0..) |pairs, i| {
            results[i] = runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = "count(DISTINCT u) AS d",
                .group_by = "t",
                .parallelism = 3,
                .min_bytes_per_worker = 1,
                .max_memory = budget,
                .distinct_pairs = pairs,
            }) catch |e| blk: {
                errs[i] = e;
                break :blk null;
            };
        }
        // Pairs refused budgets the set path answers: the refusal now runs the set path.
        try testing.expectEqual(errs[0], errs[1]);
        const off = results[0] orelse continue;
        answered += 1;
        const rows = off.output.group_rows.?;
        try testing.expectEqual(rows.len, results[1].?.output.group_rows.?.len);
        for (rows, results[1].?.output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    }
    try testing.expect(answered > 0);
}

test "count-only: charged bytes equals keys and filter estimate alone" {
    // Under a sketch count-only pass, depth-2 sum(t + t) must not be charged.
    // keys+filter estimate is 16,189; uncorrected calls charged 48,189, which forced slicing under limit 20,000.
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .decode_limit = 20000,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
}

test "count-only: no filter-only column materialized" {
    // Under a sketch count-only pass on dictionary-encoded column t with filter t > 3,
    // t is not consumed by aggregates and must remain coded without value materialization.
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const args: MultiAggArgs = .{
        .inputs = &inputs,
        .filter = "t > 3",
        .aggregate = "count(*) AS c, sum(t + t) AS st, min(s) AS lo, count(DISTINCT t) AS d, count(*) FILTER (WHERE n = 1) AS f",
        .group_by = "u",
        .parallelism = 1,
        .encoded = true,
        .decode_limit = null,
        .sketch_topk = .{},
        .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3, .offset = 2 },
    };
    var res = try runMultiAggregate(testing.allocator, arena, args);
    defer res.deinit(testing.allocator);
    try res.output.applyResultOrder(testing.allocator, args.result_order);
    const rows = res.output.group_rows.?;
    try testing.expect(rows.len > 0);
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

    // Short string composites can have a sparse repeat tail even when the marginal NDV is unknown.
    const short_composite = [_]expr_ast.Expr{ string_key[0], keys[1] };
    in.keys = &short_composite;
    in.ndv_stat.?.key_bytes = 24;
    plan = planAggregate(in);
    try testing.expectEqual(.sketch, plan.dispatch.strategy);
    try testing.expect(plan.dispatch.census_allowed and plan.dispatch.census_cell_repeat_gate);
    try testing.expect(!plan.dispatch.keys_unique and !plan.dispatch.census_fused);
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

test "census after a sketch bail: worth recounting unless a selective narrow COUNT" {
    var cells: [1 << 12]u32 = @splat(0);
    for (0..1 << 16) |i| {
        const key: u64 = i;
        const hash = std.hash.Wyhash.hash(0, std.mem.asBytes(&key));
        cells[hash >> 52] += 1;
    }
    // Few rows: a payload still pays for a census, a bare COUNT merges directly.
    try testing.expect(censusCellBet(&cells, true));
    try testing.expect(!censusCellBet(&cells, false));
    // Dense repeats no longer argue against it: the census keeps the most frequent and certifies against the rest.
    for (&cells) |*c| c.* *= 32;
    try testing.expect(censusCellBet(&cells, false));
    @memset(&cells, 0);
    try testing.expect(std.math.isInf(cellRepeatLoad(&cells)));
    cells[0] = std.math.maxInt(u32);
    try testing.expect(std.math.isInf(cellRepeatLoad(&cells)));
    try testing.expect(std.math.isInf(cellRepeatLoad(&.{1})));
}

test "result cut SUM: complete signed ranks, NULLs, ties, offsets and owned payload failures" {
    spawn_util.injectFailureAfter(0);
    defer spawn_util.resetFailure();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            const calls = [_]expr_agg.AggCall{
                .{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 },
                .{ .func = .min, .arg = null, .where = null, .alias = "m", .result = .bytes },
            };
            var tables: [2]expr_agg.GroupTable = undefined;
            for (&tables) |*t| t.* = .init(gpa, 1 << 20);
            defer for (&tables) |*t| t.deinit();
            var refs: std.ArrayList(MergeGroupRef) = .empty;
            defer refs.deinit(gpa);
            var buf: [16]u8 = undefined;
            for (0..40) |g| for (&tables, 0..) |*t, ti| {
                const key = try std.fmt.bufPrint(&buf, "k{d}", .{g});
                const id = try t.getOrInsert(key, &calls, g * 2 + 1 - ti);
                t.accumulators.items[id * 2].sum_i = if (g == 39)
                    null
                else if (ti == 0) 1000 else @as(i128, @intCast(g % 9)) - 1004;
                t.accumulators.items[id * 2 + 1].min_bytes = try gpa.dupe(u8, if (ti == 0) "z" else "a");
                try refs.append(gpa, .{ .table = @intCast(ti), .group = id });
            };
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(gpa);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer {
                freeOwnedAccumulatorStrings(gpa, accs.items);
                accs.deinit(gpa);
            }
            const cut: ResultCut = .{ .a_idx = 0, .k = 3, .descending = true, .ranking_first = true };
            try mergeFoldedRefs(gpa, gpa, &tables, refs.items, &calls, 3, cut, false, &slots, &accs);
            try keepResultTop(gpa, gpa, &calls, cut, &slots, &accs);
            try sortSlotsByFirst(gpa, slots.items, 1);
            try testing.expectEqual(@as(usize, 3), slots.items.len);
            for (slots.items, [_]u64{ 16, 34, 52 }) |s, first| {
                try testing.expectEqual(first, s.first);
                try testing.expectEqual(@as(?i128, 4), accs.items[s.idx].sum_i);
                try testing.expectEqualStrings("a", accs.items[s.idx + 1].min_bytes.?);
            }
        }
    }.run, .{});
}

test "result cut SUM: scan and shared partitions match full ordered answers at j1 j3 j8 and spawn failure" {
    const path = "ci/fixtures/parquet/census_topk.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]bool{ false, true }) |part| {
        for ([_]bool{ false, true }) |descending| {
            for ([_]u64{ 0, 1, 5, 500 }) |limit| for ([_]bool{ false, true }) |fail_spawn| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const distinct = if (part) "count(*) AS d" else "count(DISTINCT t) AS d";
                const aggregate = try std.fmt.allocPrint(
                    arena,
                    "sum(t - 30) FILTER (WHERE n = 1) AS total, min(s) AS lo, {s}",
                    .{distinct},
                );
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = "s",
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{
                        "s", "sum(t - 30) FILTER (WHERE n = 1) AS total", "min(s) AS lo", distinct,
                    }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{
                    .keys = &.{.{ .column = 1, .descending = descending }},
                    .limit = limit,
                    .offset = 2,
                };
                if (fail_spawn) spawn_util.injectFailureAfter(0);
                var got = runMultiAggregate(testing.allocator, arena, args) catch |e| {
                    spawn_util.resetFailure();
                    return e;
                };
                spawn_util.resetFailure();
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                const wr = want.output.group_rows.?;
                const gr = got.output.group_rows.?;
                try testing.expectEqual(wr.len, gr.len);
                for (wr, gr) |w, g| for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        }
    };
}

test "result cut SUM: the merge's scratch is uncharged, as the full merge's is" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    const id = try table.getOrInsert("a", &calls, 0);
    table.accumulators.items[id].sum_i = 4;
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null; // This test only charges merge scratch, not the previously private source table.
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    // A budget the full merge ignores no longer refuses the cut that stands in for it.
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{.{ .table = 0, .group = id }},
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(@as(usize, 1), slots.items.len);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "result cut SUM: planner reports every explicit fallback" {
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "s" }};
    var calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 0, .descending = false }}, .limit = 2 };
    try testing.expectEqual(.result_cut, resultCutReason(&outputs, &calls, order, false));
    try testing.expectEqual(.result_cut_unordered, resultCutReason(&outputs, &calls, order, true));
    try testing.expect(resultCut(&outputs, &calls, order, true) == null);
    const secondary: expr_ast.ResultOrder = .{
        .keys = &.{ .{ .column = 0, .descending = true }, .{ .column = 0, .descending = false } },
        .limit = 2,
    };
    try testing.expectEqual(.result_cut_secondary_keys, resultCutReason(&outputs, &calls, secondary, false));
    try testing.expect(resultCut(&outputs, &calls, secondary, false) == null);
    calls[0].result = .f64;
    try testing.expectEqual(.result_cut_float, resultCutReason(&outputs, &calls, order, false));
    try testing.expect(resultCut(&outputs, &calls, order, false) == null);
    calls[0].result = .i64;
    const payload = [_]expr_agg.AggCall{
        calls[0], .{ .func = .sum, .arg = null, .where = null, .alias = "d", .result = .dec },
    };
    try testing.expectEqual(.result_cut_fallible_payload, resultCutReason(&outputs, &payload, order, false));
    try testing.expect(resultCut(&outputs, &payload, order, false) == null);
    try testing.expect(resultCut(&outputs, &calls, .{ .keys = order.keys }, false) == null);
}

test "result cut SUM: the one-table cut holds no charge; retained winner capacities share and return the budget" {
    const calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "s" }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    for ([_][]const u8{ "a", "b" }, 0..) |key, i| {
        const id = try table.getOrInsert(key, &calls, i);
        table.accumulators.items[id].sum_i = @intCast(i);
    }
    var budget: expr_agg.SharedBudget = .{ .limit = 1, .block = 1 };
    table.shared = &budget;
    defer table.shared = null;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 1 };
    // The full merge it stands in for answers whatever the budget, so the cut does too (it refused before).
    const top = (try resultLimitGroups(
        arena_state.allocator(),
        &table,
        &outputs,
        &calls,
        order,
        false,
        null,
    )).?;
    try testing.expectEqualSlices(u32, &.{1}, top);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    budget.limit = 1 << 20;
    var memory: CutScratch = .{ .parent = testing.allocator, .shared = &budget };
    defer memory.finish();
    var slots: std.ArrayList(MergedSlot) = .empty;
    defer slots.deinit(testing.allocator);
    var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer accs.deinit(testing.allocator);
    try hashMergeGroupRefs(
        testing.allocator,
        testing.allocator,
        (&table)[0..1],
        &.{ .{ .table = 0, .group = 0 }, .{ .table = 0, .group = 1 } },
        null,
        &calls,
        .{ .a_idx = 0, .k = 1, .ranking_first = true, .memory = memory.allocator() },
        false,
        &slots,
        &accs,
    );
    try testing.expectEqual(
        slots.capacity * @sizeOf(MergedSlot) + accs.capacity * @sizeOf(expr_agg.Accumulator),
        budget.used.load(.monotonic),
    );
}

test "result cut extrema AVG DECIMAL: typed final ranks equal emitted sort keys including rounded ties" {
    const int_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const dec_arg: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .dec,
        .dec = .{ .precision = 18, .scale = 4 },
    } };
    const states = [_]expr_agg.Accumulator{
        .{ .min_i = -(@as(i128, 1) << 100) }, .{ .max_i = (@as(i128, 1) << 100) },
        .{ .avg = .{ .isum = 9007199254740993, .sum = 0, .count = 1 } }, // Distinct exact sums tie after f64 rounding.
        .{ .avg = .{ .isum = 9007199254740992, .sum = 0, .count = 1 } },
        .{ .avg = .{ .sum = 0, .count = 0 } },
        .{ .min_d = -10001 },
        .{ .max_d = null },
        .{ .sum_d = .{ .lo = 12345, .n = 2 } },
        .{ .avg_d = .{ .lo = @bitCast(@as(i128, -5)), .hi = -1, .n = 2 } },
    };
    for (states) |state| for ([_]bool{ false, true }) |avg_state| {
        const dec = state == .min_d or state == .max_d or state == .sum_d or state == .avg_d;
        const func: expr_agg.AggFunc = switch (state) {
            .min_i, .min_d => .min,
            .max_i, .max_d => .max,
            .sum_d => .sum,
            else => .avg,
        };
        const call: expr_agg.AggCall = .{
            .func = func,
            .arg = if (dec) dec_arg else int_arg,
            .where = null,
            .alias = "v",
            .result = if (func == .avg)
                (if (dec) .avg_dec else .avg_f64)
            else
                (if (dec) .dec else .i64),
            .state = avg_state and func == .avg,
        };
        const got = try rankValue(state, call);
        const want = sortKey(try materializeOne(testing.allocator, call, state));
        for ([_]bool{ false, true }) |desc| try testing.expectEqual(.eq, orderSortKeys(want, got, desc));
    };
    const avg_call: expr_agg.AggCall = .{
        .func = .avg,
        .arg = int_arg,
        .where = null,
        .alias = "a",
        .result = .avg_f64,
    };
    try testing.expectEqual(.eq, orderSortKeys(
        try rankValue(states[2], avg_call),
        try rankValue(states[3], avg_call),
        true,
    ));
    const dec_call: expr_agg.AggCall = .{
        .func = .avg,
        .arg = dec_arg,
        .where = null,
        .alias = "a",
        .result = .avg_dec,
    };
    // -0.00025: avg's mean at DECIMAL(22, 8); avg_state ranks by it rounded to the argument's scale.
    const exact = try rankValue(states[8], dec_call);
    try testing.expectEqual(@as(i128, -25000), exact.dec.v);
    try testing.expectEqual(@as(u8, 8), exact.dec.spec.scale);
    var state_call = dec_call;
    state_call.state = true;
    const rounded = try rankValue(states[8], state_call);
    try testing.expectEqual(@as(i128, -3), rounded.dec.v);
    try testing.expectEqual(@as(u8, 4), rounded.dec.spec.scale);
}

test "result cut extrema AVG DECIMAL: scans match full stable order at j1 j3 j8" {
    const fixtures = [_]struct { path: []const u8, keys: []const []const u8, aggs: []const []const u8 }{
        .{
            .path = "ci/fixtures/parquet/census_topk.parquet",
            .keys = &.{"s"},
            .aggs = &.{
                "min(t - 30) FILTER (WHERE n = 1) AS rank", "max(t - 30) FILTER (WHERE n = 1) AS rank",
                "avg(t - 30) FILTER (WHERE n = 1) AS rank", "avg_state(t - 30) FILTER (WHERE n = 1) AS rank",
            },
        },
        .{
            .path = decimal_exact_fixture,
            .keys = &.{ "k", "i" },
            .aggs = &.{
                "sum(d38) AS rank", "min(dn) AS rank", "max(dn) AS rank", "avg(dn) AS rank", "avg_state(dn) AS rank",
            },
        },
    };
    for (fixtures) |fixture| {
        const bytes = (try loadFixture(fixture.path)) orelse return error.SkipZigTest;
        defer testing.allocator.free(bytes);
        const inputs = [_]Input{.{ .name = fixture.path, .bytes = bytes }};
        for (fixture.keys) |key| for (fixture.aggs) |ranking| for ([_]usize{ 1, 3, 8 }) |jobs| {
            for ([_]bool{ false, true }) |part| for ([_]bool{ false, true }) |desc| {
                var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
                defer arena_state.deinit();
                const arena = arena_state.allocator();
                const aggregate = try std.fmt.allocPrint(arena, "{s}, count(*) AS c", .{ranking});
                var args: MultiAggArgs = .{
                    .inputs = &inputs,
                    .aggregate = aggregate,
                    .group_by = key,
                    .parallelism = jobs,
                    .partagg = part,
                    .min_bytes_per_worker = 1,
                    .sketch_topk = null,
                    .select_cols = try selectList(arena, &.{ key, ranking, "count(*) AS c" }),
                };
                var want = try runMultiAggregate(testing.allocator, arena, args);
                defer want.deinit(testing.allocator);
                args.result_order = .{ .keys = &.{.{ .column = 1, .descending = desc }}, .limit = 1, .offset = 1 };
                var got = try runMultiAggregate(testing.allocator, arena, args);
                defer got.deinit(testing.allocator);
                try want.output.applyResultOrder(testing.allocator, args.result_order);
                try got.output.applyResultOrder(testing.allocator, args.result_order);
                try testing.expectEqual(want.output.group_rows.?.len, got.output.group_rows.?.len);
                for (want.output.group_rows.?, got.output.group_rows.?) |w, g|
                    for (w, g) |a, b| try testing.expect(sameValue(a, b));
            };
        };
    }
}

test "result cut DECIMAL: loser and zero-limit overflow stays observable, payload falls back" {
    const bytes = (try loadFixture(decimal_exact_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = decimal_exact_fixture, .bytes = bytes }};
    for ([_]usize{ 1, 3, 8 }) |jobs| for ([_]u64{ 0, 1 }) |limit| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const arena = arena_state.allocator();
        // c's sum fits and ranks first ASC; a and b overflow but still have to be finalized.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "sum(big) AS rank",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "sum(big) AS rank" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = limit },
        }));
        // c also ranks first by min(i) DESC; the losing SUM(big) payload must keep the full-path error.
        try testing.expectError(error.DecimalOverflow, runMultiAggregate(testing.allocator, arena, .{
            .inputs = &inputs,
            .aggregate = "min(i) AS rank, sum(big) AS payload",
            .group_by = "k",
            .parallelism = jobs,
            .min_bytes_per_worker = 1,
            .select_cols = try selectList(arena, &.{ "k", "min(i) AS rank", "sum(big) AS payload" }),
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = limit },
        }));
    };
}

test "runMultiAggregate: a gated input plans from the dictionaries its probe reads first, as a mapped one does" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    var meta_arena = std.heap.ArenaAllocator.init(gpa);
    defer meta_arena.deinit();
    const meta = try metadata.open(meta_arena.allocator(), bytes);
    // A reader's buffer: zero until the probe copies what it asks for, the rest once the first gate is waited on.
    const Fake = struct {
        src: []const u8,
        buf: []u8,
        probes: usize = 0,
        started: bool = false,
        filled: bool = false,
        fn add(ctx: *anyopaque, _: usize, _: usize, lo: u64, hi: u64) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(!self.started);
            @memcpy(self.buf[lo..hi], self.src[lo..hi]);
            self.probes += 1;
        }
        fn start(ctx: *anyopaque) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.started = true;
        }
        fn probed(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            try testing.expect(self.started);
        }
        fn wait(ctx: *anyopaque, _: usize) anyerror!void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            if (!self.filled) @memcpy(self.buf, self.src);
            self.filled = true;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {}
    };
    const buf = try gpa.alloc(u8, bytes.len);
    defer gpa.free(buf);
    // A lone integer key, whose union the dense fold may use: a reader's input reads it only where a choice can turn on
    // it. Past the probe's cap a reader's input plans from its footer, as it did with no probe.
    for ([_]?u64{ null, 4096 }) |cap| for ([_][]const u8{ "count(*) AS c", "count(*) AS c, sum(n) AS s" }) |aggregate| {
        @memset(buf, 0);
        var fake: Fake = .{ .src = bytes, .buf = buf };
        var a1 = std.heap.ArenaAllocator.init(gpa);
        defer a1.deinit();
        var mapped = try runMultiAggregate(gpa, a1.allocator(), .{
            .inputs = &.{.{ .name = "m", .bytes = bytes }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
        });
        defer mapped.deinit(gpa);
        var a2 = std.heap.ArenaAllocator.init(gpa);
        defer a2.deinit();
        var gated = try runMultiAggregate(gpa, a2.allocator(), .{
            .inputs = &.{.{
                .name = "g",
                .bytes = buf,
                .gate = .{ .ctx = &fake, .wait = Fake.wait, .done = Fake.done },
                .plan_bytes = bytes,
                .dict_wait = .{ .ctx = &fake, .wait = Fake.probed },
            }},
            .metas = &.{meta},
            .aggregate = aggregate,
            .group_by = "n",
            .parallelism = 1,
            .probe_cap = cap,
            .start_reads = .{ .ctx = &fake, .start = Fake.start },
            .dict_probe = .{ .ctx = &fake, .add = Fake.add },
        });
        defer gated.deinit(gpa);
        try testing.expect(mapped.reasons.contains(.dictionary_union));
        if (cap == null) {
            try testing.expect(fake.probes > 0);
            try testing.expectEqual(mapped.reasons, gated.reasons);
        } else {
            try testing.expectEqual(@as(usize, 0), fake.probes);
            try testing.expect(!gated.reasons.contains(.dictionary_union) and gated.reasons.contains(.unknown_ndv));
        }
        try expectSameAnswer(mapped, gated);
    };
}

test "runMultiAggregate: a LIMIT with no ORDER BY over few dictionary entries groups in one plain pass, same answer" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/topk_skew.parquet", gpa);
    defer gpa.free(bytes);
    for ([_]usize{ 1, 4 }) |j| {
        var arenas = [2]std.heap.ArenaAllocator{ .init(gpa), .init(gpa) };
        defer for (&arenas) |*a| a.deinit();
        var answers: [2]MultiAggResult = undefined;
        for (&answers, &arenas, [_]bool{ false, true }) |*ans, *arena, policy| {
            ans.* = try runMultiAggregate(gpa, arena.allocator(), .{
                .inputs = &.{.{ .name = "t", .bytes = bytes }},
                .aggregate = "count(*) AS c, sum(n) AS s",
                .group_by = "k",
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{},
                .result_order = .{ .limit = 5 },
                // Without the cost rules the first keys are cut as before.
                .ndv_policy = policy,
            });
        }
        defer for (&answers) |*a| a.deinit(gpa);
        try testing.expect(answers[0].reasons.contains(.first_keys_cut));
        try testing.expect(answers[1].reasons.contains(.few_dictionary_entries));
        // Rows arrive uncut, in first-appearance order: the first five are the cut's.
        const cut = answers[0].output.group_rows.?;
        const all = answers[1].output.group_rows.?;
        try testing.expect(all.len >= cut.len and cut.len >= 5);
        for (cut[0..5], all[0..5]) |w, g| for (w, g) |a, b| try expectSameAggValue(a, b);
    }
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
        const b = @import("parquet/statistics.zig").chunkBounds(rg, 1, &meta) orelse return error.SkipZigTest;
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

test "runMultiAggregate: whether a budget answers does not depend on the workers or their timing" {
    const gpa = testing.allocator;
    const bytes = try metadata.readFileSlice("ci/fixtures/parquet/repeat_keys.parquet", gpa);
    defer gpa.free(bytes);
    // Every key is in every row group: each of four workers' tables holds every key whichever row groups it claims,
    // one table each key once. Partitions answer sum; count(DISTINCT) has none, so it runs on one worker.
    for ([_][]const u8{ "sum(v) AS s", "count(DISTINCT v) AS s" }) |aggregate| {
        var args: MultiAggArgs = .{
            .inputs = &.{.{ .name = "r", .bytes = bytes }},
            .aggregate = aggregate,
            .group_by = "k",
            .result_order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 },
            .min_bytes_per_worker = 1,
            .dense = false,
            .parallelism = 1,
        };
        var budgets: [40]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = (256 << 10) + i * (96 << 10);
        var want: [budgets.len](anyerror!usize) = undefined;
        outcomes(args, &budgets, &want);
        args.parallelism = 4;
        var rescued = false;
        for (0..8) |_| {
            var got: [budgets.len](anyerror!usize) = undefined;
            outcomes(args, &budgets, &got);
            try expectSameOutcomes(&want, &got);
        }
        // Some budget the four workers' own tables refuse, which the narrower reruns answer.
        for (budgets, want) |b, w| {
            _ = w catch continue;
            var arena = std.heap.ArenaAllocator.init(gpa);
            defer arena.deinit();
            var first = args;
            first.max_memory = b;
            var taken: std.EnumSet(FastPath) = .{};
            first.taken = &taken;
            if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                try testing.expectEqual(error.ExceededMemoryBudget, e);
                rescued = true;
            }
        }
        try testing.expect(rescued);
    }
}

test "regression: a plain grouping a cost rule chose over a cut answers every budget the cut answers" {
    const gpa = testing.allocator;
    const Case = struct {
        path: []const u8,
        aggregate: []const u8,
        filter: ?[]const u8 = null,
        order: expr_ast.ResultOrder,
        /// The rule that groups plainly; null where the facts cannot bound its table and the cut stands.
        rule: ?Reason,
        /// Some budget refuses the rule's plain scan, which the cut answers.
        rescues: bool,
        budget0: usize,
        step: usize,
    };
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    const skew = "ci/fixtures/parquet/skew_filter.parquet";
    const cases = [_]Case{
        // Few dictionary entries: plain grouping in place of the first keys' cut. Its small domain fits wherever the
        // union runs, so nothing refuses.
        .{
            .path = "ci/fixtures/parquet/topk_skew.parquet",
            .aggregate = "count(*) AS c, sum(n) AS s",
            .order = .{ .limit = 5 },
            .rule = .few_dictionary_entries,
            .rescues = false,
            .budget0 = 512 << 10,
            .step = 512 << 10,
        },
        // The bounds say `s = 5` passes a thousandth of the rows; nine in ten pass, over near-unique keys. Nothing
        // proves the filter selective or the groups few, and the first row group read passes nine in ten: the count
        // cut stays.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "s = 5",
            .order = top10,
            .rule = null,
            .rescues = false,
            .budget0 = 64 << 10,
            .step = 32 << 10,
        },
        // The bounds put `f > 994` at under 1%, and the first row group the scan reads passes that few, counted.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f > 994",
            .order = top10,
            .rule = .selective_filter,
            .rescues = false,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
        // The bounds prove `f = 0` passes one row group in 24, whose near-unique keys a tight budget refuses plainly.
        .{
            .path = skew,
            .aggregate = "count(*) AS c",
            .filter = "f = 0",
            .order = top10,
            .rule = .selective_filter,
            .rescues = true,
            .budget0 = 32 << 10,
            .step = 8 << 10,
        },
    };
    for (cases) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var budgets: [24]usize = undefined;
        for (&budgets, 0..) |*b, i| b.* = case.budget0 + i * case.step;
        for ([_]usize{ 1, 4 }) |j| {
            const args: MultiAggArgs = .{
                .inputs = &.{.{ .name = case.path, .bytes = bytes }},
                .aggregate = case.aggregate,
                .group_by = "k",
                .filter = case.filter,
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .sketch_topk = .{ .cell_bits = 8, .admit_per_row = 1 },
            };
            {
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var at = args;
                at.max_memory = budgets[budgets.len - 1];
                const plan = (try explainAggregate(gpa, arena.allocator(), at)).plan;
                if (case.rule) |r| try testing.expect(plan.reasons.contains(r)) else {
                    try testing.expect(!plan.reasons.contains(.selective_filter));
                }
            }
            var cut = args;
            cut.plain_rules = false;
            var want: [budgets.len]anyerror!usize = undefined;
            outcomes(cut, &budgets, &want);
            // The plain grouping may also answer budgets the cut refuses. On several workers whether the cut itself
            // answers depends on which worker claimed which row groups, and its refusal is final, so the outcomes are
            // compared on one; there both are the same every run.
            if (j == 1) {
                var got: [budgets.len]anyerror!usize = undefined;
                outcomes(args, &budgets, &got);
                for (want, got) |w, g| if (w) |n| try testing.expectEqual(n, try g) else |_| {};
            }
            if (!case.rescues) continue;
            var rescued = false;
            for (budgets, want) |b, w| {
                _ = w catch continue;
                var arena = std.heap.ArenaAllocator.init(gpa);
                defer arena.deinit();
                var first = args;
                first.max_memory = b;
                var taken: std.EnumSet(FastPath) = .{};
                first.taken = &taken;
                if (prepareAggregate(gpa, arena.allocator(), first, false)) |r| freeResult(r) else |e| {
                    try testing.expectEqual(error.ExceededMemoryBudget, e);
                    try testing.expect(taken.contains(.plain_rule));
                    rescued = true;
                }
            }
            try testing.expect(rescued);
        }
    }
}

test "runMultiAggregate: every refusal narrows, admitted runs retry shared partitions, within seven scans" {
    const S = std.EnumSet(FastPath);
    const Walk = struct {
        /// `plain`: a per-worker plain scan has run. `scans`: how many have.
        const State = struct { a: MultiAggArgs, chain: Chain, may_fit: bool, cut: bool, plain: bool, scans: usize };

        /// Whether a scan of `st` can arm `t`, as `prepareAggregate` arms paths: a cut arms none; shared partitions
        /// (the cost rules' or forced) arm no per-worker path; the dense fold excludes spill and pairs. A plan's
        /// facts are the same at every rerun (they read the mappings) but may differ from the first scan's (a
        /// reader's), so the cost rules' partitions come only in the first two scans, and a per-worker path armable
        /// at all was armed by the first per-worker plain scan.
        fn armable(st: State, t: S) bool {
            const a = st.a;
            if (st.cut) return t.count() == 0;
            if (t.contains(.partitioned) and st.scans > 1) return false;
            if (st.plain and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.plain_rule) and !a.plain_rules) return false;
            if (t.contains(.partitioned) and (!a.partition_cost or a.partagg)) return false;
            if (t.contains(.private_runs) and (!a.private_runs or !t.contains(.partitioned))) return false;
            const shared = t.contains(.partitioned) or a.partagg;
            if (shared and (t.contains(.dense) or t.contains(.spill) or t.contains(.pairs) or
                t.contains(.partitions_fit))) return false;
            if (t.contains(.dense) and (!a.dense or t.contains(.spill) or t.contains(.pairs))) return false;
            if (t.contains(.spill) and !a.spill) return false;
            if (t.contains(.pairs) and a.distinct_pairs == .off) return false;
            if (t.contains(.worker_tables) and a.parallelism == 1) return false;
            if (t.contains(.merge_memory) and a.parallelism == 1) return false;
            return true;
        }

        /// What `narrowed` reads and changes of a state, as bits: equal keys walk the same.
        fn key(st: State) u16 {
            const a = st.a;
            var k: u16 = 0;
            for ([_]bool{
                a.dense,             a.spill,            a.distinct_pairs != .off, a.partition_cost,
                a.partagg,           a.parallelism == 1, a.plain_rules,            st.chain.parts_tried,
                st.may_fit,          st.cut,             st.plain,                 st.scans > 1,
                st.chain.lean_tried, a.private_runs,
            }, 0..) |b, i| k |= @as(u16, @intFromBool(b)) << @intCast(i);
            return k;
        }

        /// The most scans any sequence of refusals from `st` runs; every one of them ends.
        fn longest(args: MultiAggArgs, st: State, depth: usize) !usize {
            try testing.expect(depth < 9);
            var seen: [1 << 14]bool = @splat(false);
            var most: usize = 1;
            const all = std.enums.values(FastPath);
            for (0..@as(usize, 1) << @intCast(all.len)) |bits| {
                var t: S = .{};
                for (all, 0..) |path, i| if (bits >> @intCast(i) & 1 != 0) t.insert(path);
                if (!armable(st, t)) continue;
                var chain = st.chain;
                const may_fit = st.may_fit or t.contains(.one_table_may_fit);
                const next = narrowed(args, st.a, t, &chain, may_fit) orelse continue;
                const after: State = .{
                    .a = next,
                    .chain = chain,
                    .may_fit = may_fit,
                    .cut = st.cut or (t.contains(.plain_rule) and !t.contains(.private_runs)),
                    .plain = st.plain or !(t.contains(.partitioned) or st.a.partagg or t.contains(.plain_rule)),
                    .scans = st.scans + 1,
                };
                if (seen[key(after)]) continue;
                seen[key(after)] = true;
                most = @max(most, 1 + try longest(args, after, depth + 1));
            }
            return most;
        }
    };
    const args: MultiAggArgs = .{ .inputs = &.{}, .aggregate = "", .parallelism = 4, .decode_limit = 256 << 20 };
    const start: Walk.State = .{
        .a = args,
        .chain = .{ .parts_tried = false },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    };
    // The cost rules' shared partitions, a fast path's mask, the plain tables, the forced partitions, one worker, and
    // one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 7), try Walk.longest(args, start, 0));
    var forced = args;
    forced.partagg = true;
    // Forced partitions, one worker, one worker on a small decode budget.
    try testing.expectEqual(@as(usize, 3), try Walk.longest(forced, .{
        .a = forced,
        .chain = .{ .parts_tried = true },
        .may_fit = false,
        .cut = false,
        .plain = false,
        .scans = 0,
    }, 0));
    // Refused shared partitions leave the forced ones still to try, before one worker.
    var chain: Chain = .{ .parts_tried = false };
    const shared_retry = narrowed(args, args, S.initMany(&.{ .private_runs, .partitioned, .plain_rule }), &chain, true).?;
    try testing.expect(!shared_retry.private_runs and shared_retry.plain_rules and shared_retry.partition_cost);
    try testing.expectEqual(args.parallelism, shared_retry.parallelism);
    try testing.expect(!chain.parts_tried);
    const plain = narrowed(args, args, S.initMany(&.{ .partitioned, .worker_tables }), &chain, true).?;
    try testing.expect(!plain.partition_cost and !plain.partagg and plain.parallelism == 4);
    const parts = narrowed(args, plain, S.initMany(&.{ .worker_tables, .partitions_fit }), &chain, true).?;
    try testing.expect(parts.partagg and parts.parallelism == 4);
    const one = narrowed(args, parts, S.initMany(&.{.worker_tables}), &chain, true).?;
    try testing.expect(!one.partagg and one.parallelism == 1);
    try testing.expect(narrowed(args, one, .{}, &chain, true) == null);
    // Without copies of the same keys, one worker is not tried.
    chain = .{ .parts_tried = true };
    try testing.expect(narrowed(args, args, S.initMany(&.{.worker_tables}), &chain, false) == null);
    // A cost rule's plain grouping gives way to its cut, and nothing else changes.
    chain = .{ .parts_tried = false };
    const cut = narrowed(args, args, S.initMany(&.{ .plain_rule, .dense, .worker_tables }), &chain, false).?;
    try testing.expect(!cut.plain_rules and cut.dense and cut.parallelism == 4 and !chain.parts_tried);
    // Refused decode memory: one worker, without copies of the keys or any table at all; then a small decode budget
    // there; then the refusal is the query's.
    chain = .{ .parts_tried = true };
    const decoder = narrowed(args, args, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(decoder.parallelism == 1 and decoder.decode_limit.? == args.decode_limit.?);
    const lean = narrowed(args, decoder, S.initMany(&.{.decode_memory}), &chain, false).?;
    try testing.expect(lean.parallelism == 1 and !lean.charge_decode);
    try testing.expectEqual(core_memory.lean_decode / 2, lean.decode_limit.?);
    try testing.expect(chain.lean_tried);
    try testing.expect(narrowed(args, lean, S.initMany(&.{.decode_memory}), &chain, false) == null);
    // A refused merge: one worker, whose one table needs none; its own refusal is the query's.
    chain = .{ .parts_tried = true };
    const merged = narrowed(args, args, S.initMany(&.{.merge_memory}), &chain, false).?;
    try testing.expect(merged.parallelism == 1 and merged.charge_decode);
    try testing.expect(narrowed(args, merged, .{}, &chain, false) == null);
}

test "runMultiAggregate: a refusal says one worker may fit only where tables held copies or one worker plans a cut" {
    const gpa = testing.allocator;
    const top10: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    for ([_]struct { path: []const u8, order: expr_ast.ResultOrder = .{}, may_fit: bool }{
        // Every key in every row group.
        .{ .path = "ci/fixtures/parquet/repeat_keys.parquet", .may_fit = true },
        // Near-unique keys.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .may_fit = false },
        // The same, cut by their count: one worker's sketch cells fit where four workers' did not.
        .{ .path = "ci/fixtures/parquet/skew_filter.parquet", .order = top10, .may_fit = true },
    }) |case| {
        const bytes = (try loadFixture(case.path)) orelse return error.SkipZigTest;
        defer gpa.free(bytes);
        var arena = std.heap.ArenaAllocator.init(gpa);
        defer arena.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        const refused = prepareAggregate(gpa, arena.allocator(), .{
            .inputs = &.{.{ .name = case.path, .bytes = bytes }},
            .aggregate = "count(*) AS c",
            .group_by = "k",
            .result_order = case.order,
            .sketch_topk = .{ .cell_bits = 12 },
            .parallelism = 4,
            .min_bytes_per_worker = 1,
            .max_memory = 256 << 10,
            .dense = false,
            .spill = false,
            .partition_cost = false,
            .taken = &taken,
        }, false);
        if (refused) |r| freeResult(r) else |e| try testing.expectEqual(error.ExceededMemoryBudget, e);
        try testing.expect(taken.contains(.worker_tables));
        try testing.expectEqual(case.may_fit, taken.contains(.one_table_may_fit));
    }
}

test "regression: counts pairs folded merge in place or under a count cut, as the sets' merge leaves them" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // A full merge (in place), a cut by the count(DISTINCT) (`mergeCountCut`), and payloads beside it.
    const top: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 7 };
    for ([_]struct { aggregate: []const u8, order: expr_ast.ResultOrder }{
        .{ .aggregate = "count(DISTINCT u) AS d", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d, count(*) AS c, sum(u) AS s", .order = .{} },
        .{ .aggregate = "count(DISTINCT u) AS d", .order = top },
        .{ .aggregate = "count(DISTINCT u) AS d, min(u) AS m", .order = top },
    }) |case| for ([_]usize{ 1, 3, 8 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var results: [2]MultiAggResult = undefined;
        for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .on }, &results) |pairs, *res| {
            res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
                .inputs = &inputs,
                .aggregate = case.aggregate,
                .group_by = "t",
                .result_order = case.order,
                .parallelism = j,
                .min_bytes_per_worker = 1,
                .distinct_pairs = pairs,
            });
        }
        defer for (results) |r| freeResult(r);
        try testing.expect(!results[0].distinct_pairs and results[1].distinct_pairs);
        const sets = results[0].output.group_rows.?;
        const counted = results[1].output.group_rows.?;
        try testing.expectEqual(sets.len, counted.len);
        for (sets, counted) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
    };
}

test "clearWorkerErrors: a phase's expected failure is cleared on every worker, any other is returned" {
    var ws: [3]Worker = undefined;
    for (&ws) |*w| w.err = null;
    // The census trial's budget on one worker, a corrupt page on a later one: the page's error is the query's.
    ws[0].err = error.ExceededMemoryBudget;
    ws[2].err = error.CorruptPage;
    try testing.expectError(error.CorruptPage, clearWorkerErrors(&ws, error.ExceededMemoryBudget));
    try testing.expectEqual(@as(?anyerror, null), ws[0].err);
    ws[2].err = error.ExceededMemoryBudget;
    ws[1].err = error.ExceededMemoryBudget;
    try clearWorkerErrors(&ws, error.ExceededMemoryBudget);
    for (ws) |w| try testing.expectEqual(@as(?anyerror, null), w.err);
}

test "regression: count(DISTINCT) pairs pay by the values the sets took, a filter's dropped rows left out" {
    const path = "ci/fixtures/parquet/census_dictionary_review.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    // Seven groups of near-unique values; the filter drops one row in eight, which the rows read counted.
    var results: [2]MultiAggResult = undefined;
    for ([_]@FieldType(MultiAggArgs, "distinct_pairs"){ .off, .auto }, &results) |pairs, *res| {
        res.* = try runMultiAggregate(testing.allocator, arena_state.allocator(), .{
            .inputs = &inputs,
            .aggregate = "count(DISTINCT u) AS d",
            .group_by = "t",
            .filter = "n = 1",
            .parallelism = 1,
            .min_bytes_per_worker = 1,
            .distinct_pairs = pairs,
            .pairs_pilot_rows = 1000,
            .ndv_policy = false,
        });
    }
    defer for (results) |r| freeResult(r);
    try testing.expect(results[1].distinct_pairs);
    const sets = results[0].output.group_rows.?;
    try testing.expectEqual(sets.len, results[1].output.group_rows.?.len);
    for (sets, results[1].output.group_rows.?) |r, p_row| for (r, p_row) |x, y| try expectSameAggValue(x, y);
}

/// Counts the bytes live through it, and their high-water mark.
const LivePeak = struct {
    child: std.mem.Allocator,
    live: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),

    fn allocator(self: *LivePeak) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn grew(self: *LivePeak, n: usize) void {
        const now = self.live.fetchAdd(n, .monotonic) + n;
        _ = self.peak.fetchMax(now, .monotonic);
    }

    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        const p = self.child.rawAlloc(len, a, ret) orelse return null;
        self.grew(len);
        return p;
    }

    fn resize(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, new_len: usize, ret: usize) bool {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        if (!self.child.rawResize(m, a, new_len, ret)) return false;
        if (new_len > m.len) self.grew(new_len - m.len) else _ = self.live.fetchSub(m.len - new_len, .monotonic);
        return true;
    }

    fn remap(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, new_len: usize, ret: usize) ?[*]u8 {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        const p = self.child.rawRemap(m, a, new_len, ret) orelse return null;
        if (new_len > m.len) self.grew(new_len - m.len) else _ = self.live.fetchSub(m.len - new_len, .monotonic);
        return p;
    }

    fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ret: usize) void {
        const self: *LivePeak = @ptrCast(@alignCast(ctx));
        self.child.rawFree(m, a, ret);
        _ = self.live.fetchSub(m.len, .monotonic);
    }
};

test "a scalar query's string winner copy is drawn before it is made, so a refusal holds no copy past the ledger" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    // Each row group's 1 MiB value is a new winner of all three aggregates: three copies, one fold. Drawn only after
    // the fold, the third copy was made before the ledger refused it (and at 40 MiB a value, past the host's memory).
    for ([_]usize{ 1, 3 }) |j| {
        var ledger: core_memory.SharedBudget = .{ .limit = (5 << 20) / 2 };
        var gpa: LivePeak = .{ .child = testing.allocator };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS a, max(s) AS b, min(s) AS c",
            .parallelism = j,
            .max_memory = ledger.limit,
            .ledger = &ledger,
            .decode_limit = ledger.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(gpa.allocator(), arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
        try testing.expectEqual(@as(usize, 0), gpa.live.load(.monotonic));
        if (j == 1) try testing.expect(gpa.peak.load(.monotonic) < 3 << 20);
    }
}

test "a scalar query's string winners are drawn from the query's ledger: past it the query refuses, holding nothing" {
    const path = "ci/fixtures/parquet/scalar_strings.parquet";
    const bytes = (try loadFixture(path)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    const inputs = [_]Input{.{ .name = path, .bytes = bytes }};
    for ([_]usize{ 1, 3 }) |j| {
        // Four row groups' winners of 1 MiB each, held until the merge, do not fit 3 MiB beside each other.
        var tight: core_memory.SharedBudget = .{ .limit = 3 << 20 };
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "min(s) AS m",
            .parallelism = j,
            .max_memory = tight.limit,
            .ledger = &tight,
            .decode_limit = tight.limit / 2,
            .min_bytes_per_worker = 1,
        };
        const refused = runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        try testing.expectError(error.ExceededMemoryBudget, refused);
        try testing.expectEqual(@as(usize, 0), tight.used.load(.monotonic));
        // With room for them the answer is the least string, and what stays drawn is its winner and its copy.
        var roomy: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var wide = args;
        wide.max_memory = roomy.limit;
        wide.ledger = &roomy;
        wide.decode_limit = roomy.limit / 2;
        const res = try runMultiAggregate(testing.allocator, arena_state.allocator(), wide);
        defer freeResult(res);
        const got = res.output.aggs[0].value.s;
        try testing.expectEqual(@as(usize, 1 << 20), got.len);
        try testing.expectEqual(@as(u8, 'a'), got[0]);
        try testing.expect(roomy.used.load(.monotonic) >= 2 << 20);
    }
}

test "partagg: admitted long composite counts match shared partitions after a spawn failure" {
    const bytes = (try loadFixture(full_match_fixture)) orelse return error.SkipZigTest;
    defer testing.allocator.free(bytes);
    // Repeat a small fixture to cross the planning row gate; a short probe makes the first local window unique.
    var inputs: [48]Input = undefined;
    for (&inputs) |*in| in.* = .{ .name = full_match_fixture, .bytes = bytes };
    for ([_]usize{ 3, 16 }) |j| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var taken: std.EnumSet(FastPath) = .{};
        var args: MultiAggArgs = .{
            .inputs = &inputs,
            .aggregate = "count(*) AS c",
            .group_by = "s, ts",
            .parallelism = j,
            .min_bytes_per_worker = 1,
            .max_memory = 128 << 20,
            .part_cap = 8,
            .part_probe = 8,
            .private_runs = false,
        };
        const want = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(want);
        args.private_runs = true;
        args.taken = &taken;
        const got = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(got);
        try testing.expect(taken.contains(.private_runs));
        try expectSameAnswer(want, got);
        spawn_util.injectFailureAfter(1);
        defer spawn_util.resetFailure();
        const failed = try prepareAggregate(testing.allocator, arena_state.allocator(), args, false);
        defer freeResult(failed);
        try testing.expect(spawn_util.failuresInjected() > 0);
        try expectSameAnswer(want, failed);
    }
}

test "private decoder floors stay funded and cannot consume another worker's credit" {
    const a = testing.allocator;
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var decoders = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, 0);
    defer decoders.release();
    const rest = parent.drawExact(parent.available()).?;
    defer parent.release(rest);
    const first = &decoders.workers[0];
    const full = first.drawExact(first.funded).?;
    defer first.release(full);
    try testing.expect(first.drawExact(1) == null);
    const second = &decoders.workers[1];
    const independent = second.drawExact(second.funded).?;
    defer second.release(independent);
    try testing.expectEqual(parent.limit, parent.used.load(.monotonic));
    try testing.expectEqual(@as(usize, parent.limit / 4), decoders.held);
}

fn privateDecodeAllocationFailure(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var decoders = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, 0);
    defer decoders.release();
    var charged: core_memory.Charged = .{ .child = a, .budget = &decoders.workers[0] };
    const memory = try charged.allocator().alloc(u8, 4096);
    charged.allocator().free(memory);
}

test "private decoder funding returns after allocation failures or declined admission" {
    try testing.checkAllAllocationFailures(testing.allocator, privateDecodeAllocationFailure, .{});
    var parent: core_memory.SharedBudget = .{ .limit = 128 << 20 };
    const held = parent.drawExact(parent.limit).?;
    defer parent.release(held);
    var refused = try PrivateDecode.init(testing.allocator, &parent, 4, null, 0);
    defer refused.release();
    try testing.expectEqual(@as(usize, 0), refused.held);
    try testing.expectEqual(parent.limit, parent.used.load(.monotonic));
}

fn privateSampleAllocationFailure(a: std.mem.Allocator) !void {
    var parent: core_memory.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(parent.used.load(.monotonic) == 0);
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    const Merge = struct {
        fn count(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) !void {
            dst.count += src.count;
        }
    };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try partagg.PartAgg.init(a, &parent, &calls, 0, Merge.count, 4, 4, 2, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    var funding = try PrivateDecode.init(arena.allocator(), &parent, 2, parent.limit / 2, 0);
    defer funding.release();
    try testing.expect(part.reservePrivate(65536, 128));
    const before = parent.used.load(.monotonic);
    const credit = part.run_credit.load(.monotonic);
    try testing.expect(try funding.fundSamples(arena.allocator(), &part, 128));
    part.decoder_floors = funding.workers;
    try testing.expectEqual(before, parent.used.load(.monotonic));
    try testing.expectEqual(credit, part.run_credit.load(.monotonic) +
        funding.samples[0].funded + funding.samples[1].funded);
    const rest = parent.drawExact(parent.available()).?;
    defer parent.release(rest);
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &funding.samples[0];
    table.compact.configure(&calls);
    defer table.deinit();
    for ([_][]const u8{ "alpha", "bravo", "charlie", "delta" }, 0..) |key, i| {
        _ = try table.getOrInsert(key, &calls, i);
    }
    try testing.expect(funding.samples[0].used.load(.monotonic) > 0);
    part.cancelPrivate();
    for (funding.samples) |*b| try testing.expectEqual(@as(usize, 0), b.funded);
    try testing.expect(funding.samples[0].used.load(.monotonic) > 0);
}

test "private decoder loans cannot starve local samples: prepaid tables survive a full ledger and allocation failures" {
    try testing.checkAllAllocationFailures(testing.allocator, privateSampleAllocationFailure, .{});
}

test "private decoder floors decline a dictionary that leaves no sliced sample room before charging" {
    var parent: core_memory.SharedBudget = .{ .limit = 2 << 30 };
    const dictionary_peak = 2 * (80_000_000 + 900_000 * @sizeOf([]const u8));
    var small = try PrivateDecode.init(testing.allocator, &parent, 4, parent.limit / 2, dictionary_peak);
    defer small.release();
    try testing.expectEqual(@as(usize, 0), small.held);
    try testing.expectEqual(@as(usize, 0), parent.used.load(.monotonic));
    parent.limit = 4 << 30;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var ample = try PrivateDecode.init(arena.allocator(), &parent, 4, parent.limit / 2, dictionary_peak);
    defer ample.release();
    try testing.expectEqual(@as(usize, parent.limit / 4), ample.held);
}

test "regression: a decoded alias keeps its surrounding spaces; an expression's name is trimmed" {
    try std.testing.expectEqualStrings(" n ", aliasName(.{ .expr = "count(*)", .alias = " n " }));
    try std.testing.expectEqualStrings("x", aliasName(.{ .expr = "x", .alias = "x" }));
    try std.testing.expectEqualStrings("k", aliasName(.{ .expr = " k " }));
    // The flag form's `--column-order "k AS  n "` trims what it was given, as before.
    try std.testing.expectEqualStrings("n", aliasName(.parse(" k AS  n ")));
}
