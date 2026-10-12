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
const filter_ast = @import("filter/ast.zig");
const filter_parser = @import("filter/parser.zig");
const filter_prune = @import("filter/prune.zig");
const membership = @import("filter/membership.zig");
const Diag = @import("diag.zig").Diag;

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
};

pub const RowGroupGate = struct {
    ctx: *anyopaque,
    wait: *const fn (ctx: *anyopaque, rg: usize) anyerror!void,
    /// One of the row group's `items` work items is finished, read or skipped.
    done: *const fn (ctx: *anyopaque, rg: usize, items: usize) void,
};

pub const MultiAggArgs = struct {
    inputs: []const Input,
    /// Optional metadata parsed by the I/O layer. When null, metadata is
    /// parsed from each Input.bytes as before.
    metas: ?[]const schema.FileMetaData = null,
    filter: ?[]const u8 = null,
    aggregate: []const u8,
    group_by: ?[]const u8 = null,
    max_memory: usize = 512 * 1024 * 1024,
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
    /// Leave each worker's decode region mapped when it finishes, for the process exit to release (see the engine's
    /// `Context.exit_after`): a worker's unmap otherwise delays its join, and with it the answer.
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
    /// Single f64 result (float sum/min/max).
    f: f64,
    /// Avg's split-output: ship sum + count, caller divides at end. The sum is
    /// NULL when count is 0, as sum() over no values is.
    avg: struct { sum: ?f64, count: i64 },
    /// String (bytewise/unsigned) min/max result or string group key. Owned by the `AggOutput` holding it.
    s: []const u8,
    /// Exact DECIMAL: unscaled value and its type. Printed as exact text, written as DECIMAL.
    dec: DecValue,
    /// avg over a DECIMAL: the exact sum and count, which re-aggregate, and the mean rounded to the argument's type.
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

    pub fn deinit(self: *AggOutput, gpa: std.mem.Allocator) void {
        for (self.aggs) |item| {
            gpa.free(item.alias);
            freeValue(gpa, item.value);
        }
        gpa.free(self.aggs);
        if (self.group_rows) |rows| {
            for (rows) |row| freeRow(gpa, row);
            gpa.free(rows);
        }
        if (self.group_cols) |cols| {
            for (cols) |c| gpa.free(c);
            gpa.free(cols);
        }
        self.* = .{};
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
        const sorted = try gpa.alloc([]const AggValue, rows.len);
        defer gpa.free(sorted);
        @memcpy(sorted, rows);
        std.sort.block([]const AggValue, sorted, order.keys, rowLessThan);
        const start: usize = @intCast(@min(order.offset, sorted.len));
        const end: usize = if (order.limit) |n| @intCast(@min(sorted.len, start +| n)) else sorted.len;
        // Allocated before anything is freed, so a failure leaves the answer whole.
        const kept = try gpa.dupe([]const AggValue, sorted[start..end]);
        for (sorted[0..start]) |row| freeRow(gpa, row);
        for (sorted[end..]) |row| freeRow(gpa, row);
        gpa.free(rows);
        self.group_rows = kept;
    }

    fn freeRow(gpa: std.mem.Allocator, row: []const AggValue) void {
        for (row) |v| freeValue(gpa, v);
        gpa.free(row);
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
    const a_null = sortKey(a) == .none;
    const b_null = sortKey(b) == .none;
    if (a_null or b_null) return if (a_null == b_null) .eq else if (a_null) .gt else .lt;
    const ord = compareKeys(sortKey(a), sortKey(b));
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
const BlockMerge = struct {
    tables: []expr_agg.GroupTable,
    merged: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    gpa: std.mem.Allocator,
    done: []std.atomic.Value(bool),
    /// The first block not yet merged.
    next: std.atomic.Value(usize) = .init(0),
    ahead: usize,
    lock: std.atomic.Value(bool) = .init(false),
    failed: std.atomic.Value(bool) = .init(false),

    /// Wait until block `b` may start; false once another worker has failed.
    fn awaitTurn(self: *BlockMerge, b: usize) bool {
        while (b >= self.next.load(.acquire) + self.ahead) {
            if (self.failed.load(.acquire)) return false;
            std.Thread.yield() catch {};
        }
        return !self.failed.load(.acquire);
    }

    /// Block `b` is complete: merge it, and any finished after it, once every earlier block has merged. Whoever holds
    /// the lock merges every block ready by then, so none is left behind.
    fn finish(self: *BlockMerge, b: usize) !void {
        self.done[b].store(true, .release);
        while (self.lock.cmpxchgWeak(false, true, .acquire, .monotonic) != null) std.Thread.yield() catch {};
        defer self.lock.store(false, .release);
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
        }
    }

    fn fail(self: *BlockMerge) void {
        self.failed.store(true, .release);
    }
};

/// Whether merging partial results in another order can change their bits: float sums and averages round
/// differently, and a float min or max may come out -0 or +0. Integer and string results never do.
fn orderSensitive(calls: []const expr_agg.AggCall) bool {
    for (calls) |c| {
        if (c.func == .count or c.func == .count_distinct) continue;
        if (c.result == .f64 or c.result == .avg_f64) return true;
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
    /// Ordered GROUP BY: the work list split into fixed blocks, block `b` = `work[blocks[b]..blocks[b + 1]]`, claimed
    /// whole through `block_cursor` and folded in order into `group_tables[b]`.
    blocks: ?[]const usize = null,
    block_cursor: ?*std.atomic.Value(usize) = null,
    /// With `blocks`: where finished blocks merge, in block order.
    block_merge: ?*BlockMerge = null,
    /// Work items per row group (every row group gets the same number), for `Input.gate`.
    items_per_rg: usize = 1,
    leave_region: bool = false,
    decode_budget: ?*consumer.DecodeBudget = null,
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
    workerRunErr(w, prof, &items) catch |e| {
        w.err = e;
        if (w.block_merge) |m| m.fail();
    };
    // Holding a drawn block until teardown let a barely-grouping worker strand a small budget and fail the worker
    // doing the real work. (Ordered blocks release theirs as each finishes.)
    if (w.blocks == null and w.group_tables.len > 0) w.group_tables[w.index].releaseSlack();
}

fn workerRunErr(w: *Worker, prof: ?*profile.Thread, items: *usize) !void {
    // Declared first so it runs last: the release span covers freeing the arena and scratch below.
    var release_span: profile.Span = .{ .t = null, .start = 0 };
    defer release_span.end(.worker_release, profile.args(.{w.index}));
    // One decode arena for the whole run: scanRGForAgg rewinds it per row group instead of regrowing a fresh one.
    // Both it and the scratch are written front to back and reused, and both live in one huge-page region that a
    // single unmap releases (see `huge_pages.Region`).
    var region = huge_pages.Region.init(w.gpa, WORKER_REGION_RESERVE);
    defer if (!w.leave_region) region.deinit();
    var rg_decode_arena = consumer.RowGroupArena.init(region.allocator());
    defer rg_decode_arena.deinit();
    var scratch = consumer.DecodeScratch.init(region.allocator());
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
            const i = w.cursor.fetchAdd(1, .monotonic);
            break :claim if (i < w.work.len) i else null;
        };
        const idx = claimed orelse {
            release_span = .begin(prof);
            break;
        };
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
        const target: consumer.AggTarget = if (table) |gt|
            .{ .grouped = .{
                .calls = sub_agg_calls,
                .keys = w.group_by_keys.?,
                .table = gt,
                .pos_base = expr_agg.firstPosition(idx, 0),
            } }
        else
            .{ .scalar = .{ .calls = sub_agg_calls, .accumulators = item.accumulators } };
        try consumer.scanRGForAgg(w.gpa, .{
            .rg = rg,
            .file_index = item.file,
            .rg_index = item.rg,
            .meta = meta,
            .src = rg_src,
            .filter = w.plan.filterFor(decision),
            .read = item.columns.forRowGroup(decision),
            .consumed = item.columns.consumed,
        }, w.plan.stats, w.decode_options, target, .{
            .scratch = &scratch,
            .rg_arena = &rg_decode_arena,
            .timings = &w.timings,
            .budget = w.decode_budget,
        });
    }
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
pub fn runMultiAggregate(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
) !MultiAggResult {
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
                return error.NotParquet;
            },
        }
    };

    // Validate schemas match the first file's: same number of leaves
    // and same names *case-insensitively*. Real-world datasets drift on
    // capitalization (NYC Taxi flips `Airport_fee` ↔ `airport_fee`
    // mid-2023). Strict-position-strict-name is too strict; full schema
    // evolution is a bigger lift we defer.
    const meta0 = &metas[0];
    for (metas[1..], 1..) |*m, i| {
        if (m.schema.items.len != meta0.schema.items.len)
            return error.SchemaMismatch;
        for (m.schema.items, meta0.schema.items) |a_elem, b_elem| {
            // Columns are bound by leaf index against file 0, so the nesting must match too: the same names in the
            // same order can still put a leaf under a group in one file and at the top level in another.
            if (!std.ascii.eqlIgnoreCase(a_elem.name, b_elem.name) or
                (a_elem.num_children orelse 0) != (b_elem.num_children orelse 0))
            {
                Diag.setInput(args.diag, args.inputs[i].name, null);
                Diag.set(args.diag, b_elem.name, .{ .schema_mismatch = .{
                    .file = i,
                    .first_input = args.inputs[0].name,
                    .other = .of(a_elem.name),
                } });
                return error.SchemaMismatch;
            }
        }
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
        if (expr_str.len > 0) filter_opt = try filter_parser.parse(arena, expr_str, meta0, args.diag);
    }
    const group_by_items = if (args.group_by) |gb_str|
        try expr_parser.parseGroupBy(arena, gb_str, meta0, args.diag)
    else
        null;
    const group_by_keys = if (group_by_items) |items| blk: {
        const exprs = try arena.alloc(expr_ast.Expr, items.len);
        for (items, 0..) |item, i| exprs[i] = item.expr;
        break :blk exprs;
    } else null;
    var key_names = try KeyNames.init(arena, meta0, group_by_items orelse &.{});
    try nameUnaliasedAggs(arena, agg_calls, &key_names);
    const agg_sources = try arena.alloc(?schema.SchemaElement, agg_calls.len);
    for (agg_calls, agg_sources) |call, *src| src.* = passThroughSource(call, meta0);
    const group_outputs: ?[]const BoundOutputColumn = if (group_by_items != null)
        try bindGroupOutputs(arena, &key_names, agg_calls, meta0, args)
    else blk: {
        try checkAggAliases(agg_calls, args.diag);
        break :blk null;
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
    if (filter_opt) |f| if (probesMembership(args)) {
        const files = try arena.alloc([]const u8, args.inputs.len);
        for (args.inputs, files) |in, *b| b.* = in.bytes;
        const mapped: membership.MappedSource = .{ .files = files };
        membership_stats = try membership.load(arena, gpa, f, metas, mapped.source(), .{});
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
    for (raw_work_items.items) |raw| {
        for (chunk_columns, 0..) |cols, chunk_idx| {
            const agg_start = chunk_starts[chunk_idx];
            const agg_len = chunk_starts[chunk_idx + 1] - agg_start;

            // Initialize accumulators for this chunk
            const sub_accs = if (group_by_keys != null)
                try arena.alloc(expr_agg.Accumulator, 0)
            else blk: {
                const arr = try arena.alloc(expr_agg.Accumulator, agg_len);
                for (agg_calls[agg_start .. agg_start + agg_len], 0..) |call, ci| {
                    var acc = expr_agg.Accumulator.init(call);
                    if (call.func == .count_distinct) {
                        const ds = try gpa.create(expr_agg.DistinctSet);
                        ds.* = .{};
                        acc = .{ .distinct = ds };
                    }
                    arr[ci] = acc;
                }
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

    var workers = try arena.alloc(Worker, n_workers);
    var decode_budget: ?consumer.DecodeBudget = if (args.decode_limit) |l|
        .{ .limit = l, .workers = n_workers }
    else
        null;
    // One budget shared by every worker table, not per-worker quotas: work is claimed off a shared cursor, so which
    // worker meets the group-heavy row groups is unknowable in advance — fixed quotas made `-j2` reject queries `-j1`
    // ran on identical data. Fully initialized before any worker exists, so none can insert against an unpublished
    // budget.
    var group_budget = expr_agg.SharedBudget{
        .limit = args.max_memory,
        .block = expr_agg.SharedBudget.blockFor(args.max_memory, n_workers),
    };
    for (work_items.items) |item| {
        for (item.accumulators) |*acc| {
            if (acc.* == .distinct and acc.distinct != null) {
                acc.distinct.?.shared = &group_budget;
            }
        }
    }

    // GROUP BY tables. One per worker, unless results depend on the order partials merge in and order is asked for:
    // then one per fixed block of the work list, so which rows fold into which table, and the order the tables merge
    // in, doesn't depend on the number of workers or their timing (see `BlockMerge`).
    var block_bounds: ?[]usize = null;
    const use_blocks = group_by_keys != null and !args.unordered and orderSensitive(agg_calls);
    const n_tables: usize = if (group_by_keys == null) 0 else if (!use_blocks) n_workers else blk: {
        const n_blocks = @max(1, @min(work_items.items.len, group_blocks_max));
        const bounds = try arena.alloc(usize, n_blocks + 1);
        for (bounds, 0..) |*b, i| b.* = i * work_items.items.len / n_blocks;
        block_bounds = bounds;
        break :blk n_blocks;
    };
    const group_tables = try arena.alloc(expr_agg.GroupTable, n_tables);
    // Integer and float keys are a fixed-width image: the per-row lookup mixes those bytes in an open-addressed
    // table instead of copying each new key into a string map. String and decimal keys stay on that map.
    const fixed_stride: u8 = if (group_by_keys) |ks| expr_agg.fixedGroupStride(ks) orelse 0 else 0;
    for (group_tables) |*gt| {
        // Zero local ceiling: everything is drawn from the shared pool, so an idle table reserves nothing others
        // could use.
        gt.* = expr_agg.GroupTable.init(gpa, 0);
        gt.shared = &group_budget;
        gt.fixed_stride = fixed_stride;
    }
    defer for (group_tables) |*gt| gt.deinit();
    var block_cursor = std.atomic.Value(usize).init(0);
    var merged_table = expr_agg.GroupTable.init(gpa, 0);
    merged_table.shared = &group_budget;
    merged_table.fixed_stride = fixed_stride;
    defer merged_table.deinit();
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
            .decode_options = .{ .fast_levels = args.fast_levels, .encoded = args.encoded },
            .profiler = args.profiler,
            .index = wi,
            .group_by_keys = group_by_keys,
            .group_tables = group_tables,
            .blocks = block_bounds,
            .block_cursor = &block_cursor,
            .block_merge = if (block_merge) |*m| m else null,
            .items_per_rg = chunks_per_rg,
            .leave_region = args.leave_regions,
            .decode_budget = if (decode_budget) |*b| b else null,
        };
    }

    // Workers fold string min/max winners into the items' accumulators; merging below moves them out, so this frees
    // only what a failure leaves behind.
    errdefer for (work_items.items) |item| freeOwnedAccumulatorStrings(gpa, item.accumulators);

    // 5. Spawn, join, propagate first error. Time the whole parallel
    //    region as one wall-clock span (real elapsed decode), separate
    //    from the per-worker CPU sum in `core.decode_ns`.
    const t_decode = nowMonoNs();
    if (n_workers > 1) {
        // Started as a tree, so the last worker is not a millisecond late. Spawn failure never returns early (running
        // threads hold pointers into `arena` and `group_tables`); workers that never start are idle, and with no
        // thread at all one inline runner drains the shared cursor.
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (!spawn_util.runTree(Worker, workers, config, workerRun)) workerRun(&workers[0]);
    } else {
        // Single-threaded fast path. Avoids std.Thread overhead.
        workerRun(&workers[0]);
    }
    const t_decode_end = nowMonoNs();
    t.decode_wall_ns = @intCast(t_decode_end - t_decode);
    profile.record(prof, .scan, t_decode, t_decode_end, profile.args(.{n_workers}));
    for (workers) |w| if (w.err) |e| return e;
    const merge_span = profile.Span.begin(prof);

    // 6. Merge per-work-item accumulators into one final slice.
    var accumulators = try arena.alloc(expr_agg.Accumulator, agg_calls.len);
    for (agg_calls, 0..) |call, i| {
        var acc = expr_agg.Accumulator.init(call);
        if (call.func == .count_distinct) {
            const ds = try gpa.create(expr_agg.DistinctSet);
            ds.* = .{ .shared = &group_budget };
            acc = .{ .distinct = ds };
        }
        accumulators[i] = acc;
    }
    errdefer freeOwnedAccumulatorStrings(gpa, accumulators);
    var output: AggOutput = .{};
    errdefer output.deinit(gpa);
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
        // A second GroupTable would draw another --max-memory reservation while the sources still hold theirs, so a
        // query at the ceiling would fail. The merge map borrows source keys and does not touch that budget.
        const out_tables: []expr_agg.GroupTable = if (use_blocks) (&merged_table)[0..1] else group_tables;
        // An answer cut to its first rows by a COUNT, descending, merges only the groups that could make the cut.
        const candidates = try topKCandidates(arena, out_tables, group_outputs.?, agg_calls, args.result_order);
        var total_group_refs: usize = 0;
        for (out_tables) |gt| total_group_refs += if (candidates) |keys_kept| keys_kept.len else gt.groupCount();
        const group_refs_buf = try gpa.alloc(MergeGroupRef, total_group_refs);
        defer gpa.free(group_refs_buf);
        var ref_i: usize = 0;
        for (out_tables, 0..) |gt, ti| {
            if (candidates) |keys_kept| {
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
                .source = if (keys[k_idx] == .col_ref) leafSchemaElem(meta0, keys[k_idx].col_ref.col_idx) else null,
            },
            .agg => |a_idx| .{
                .nullable = agg_calls[a_idx].func != .count,
                .lane = switch (agg_calls[a_idx].result) {
                    .i64 => .i64,
                    .f64 => .f64,
                    .bytes => .string,
                    .avg_f64 => .avg,
                    .dec => .dec,
                    .avg_dec => .avg_dec,
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
        defer slots.deinit(gpa);
        // Accumulators moved out of the source tables when several tables share keys. Empty when there is one table:
        // its groups are already distinct, so rows read that table directly.
        var merged_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
        defer {
            freeOwnedAccumulatorStrings(gpa, merged_accs.items);
            merged_accs.deinit(gpa);
        }
        const folded = out_tables.len > 1;
        if (folded) {
            try mergeFoldedRefs(gpa, arena, out_tables, group_refs, agg_calls, n_workers, &slots, &merged_accs);
        } else {
            const gt = &out_tables[0];
            try slots.ensureTotalCapacity(gpa, group_refs.len);
            for (group_refs) |ref| {
                slots.appendAssumeCapacity(.{
                    .key = gt.keyAt(ref.group),
                    .first = gt.first.items[ref.group],
                    .idx = ref.group,
                });
            }
        }
        // `first` is the input row that created the group, so it is unique per group: an unstable sort is enough.
        if (!args.unordered) {
            std.sort.pdq(MergedSlot, slots.items, {}, struct {
                fn lt(_: void, a: MergedSlot, b: MergedSlot) bool {
                    return a.first < b.first;
                }
            }.lt);
        }

        // Owned by `output` from allocation on, every slot valid at every step, so its `deinit` frees a partly built
        // answer: unbuilt rows are empty, unbuilt values NULL.
        const rows = try gpa.alloc([]const AggValue, slots.items.len);
        @memset(rows, &.{});
        output.group_rows = rows;
        const acc_base = if (folded) merged_accs.items else out_tables[0].accumulators.items;
        for (slots.items, rows) |slot, *row| {
            const row_vals = try gpa.alloc(AggValue, outputs.len);
            @memset(row_vals, .null_val);
            row.* = row_vals;
            const acc_at: usize = if (folded) slot.idx else slot.idx * agg_calls.len;
            const accs = acc_base[acc_at..][0..agg_calls.len];
            for (outputs, row_vals) |out, *val| {
                switch (out.source) {
                    .key => |k_idx| {
                        // A fixed image pads a null column with zeros; the reader wants that payload omitted.
                        var ser_buf: [48]u8 = undefined;
                        val.* = try deserializeKeyColumn(gpa, expr_agg.canonicalKey(slot.key, &ser_buf), k_idx, key_types);
                    },
                    .agg => |a_idx| {
                        val.* = try materializeOne(gpa, agg_calls[a_idx], accs[a_idx]);
                    },
                }
            }
        }

        const cols = try gpa.alloc([]const u8, outputs.len);
        @memset(cols, "");
        output.group_cols = cols;
        for (outputs, cols) |out, *col| col.* = try gpa.dupe(u8, out.name);

        // Result rows own any strings they need; table keys and accumulator
        // winners can now be released before returning (the deferred deinit
        // then finds them empty).
        for (out_tables) |*gt| {
            gt.deinit();
            gt.* = expr_agg.GroupTable.init(gpa, 0);
        }
    } else {
        // Merge each work item exactly ONCE: `work` is shared and claimed dynamically, so folding it per worker would
        // merge every item n_workers times — inflating sums and double-freeing the string min/max winner.
        for (work_items.items) |item| {
            for (item.accumulators, 0..) |*sub_acc, i| {
                try mergeOwnedAccumulator(&accumulators[item.agg_start + i], sub_acc, gpa);
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
    if (arg != .col_ref) return null;
    return leafSchemaElem(meta, arg.col_ref.col_idx);
}

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

const HashKey = struct { bytes: []const u8, hash: u64 };

const HashKeyCtx = struct {
    pub fn hash(_: @This(), k: HashKey) u64 {
        return k.hash;
    }
    pub fn eql(_: @This(), k: HashKey, stored: []const u8) bool {
        return std.mem.eql(u8, k.bytes, stored);
    }
};

/// Fold `refs` into distinct groups. `slots` borrows keys from `tables`; `accs` owns any string winners moved out of
/// those tables. The map is sized for every ref, so a repeated key does not rehash it. `hashes`, when set, is the
/// Wyhash already computed for each ref (the same one `StringHashMap` uses) and is not computed again.
fn hashMergeGroupRefs(
    gpa: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    var map = std.StringHashMap(u32).init(gpa);
    defer map.deinit();
    const n: u32 = std.math.cast(u32, refs.len) orelse return error.OutOfMemory;
    try map.ensureTotalCapacity(n);
    try slots.ensureTotalCapacity(gpa, refs.len);

    for (refs, 0..) |ref, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = if (hashes) |hs| blk: {
            const got = map.getOrPutAssumeCapacityAdapted(HashKey{ .bytes = key, .hash = hs[ri] }, HashKeyCtx{});
            if (!got.found_existing) got.key_ptr.* = key;
            break :blk got;
        } else map.getOrPutAssumeCapacity(key);
        const src = gt.accumulators.items[@as(usize, ref.group) * calls.len ..][0..calls.len];
        if (gop.found_existing) {
            const slot = &slots.items[gop.value_ptr.*];
            slot.first = @min(slot.first, gt.first.items[ref.group]);
            const dst = accs.items[slot.idx..][0..calls.len];
            for (dst, src) |*d, *s| try mergeOwnedAccumulator(d, s, gpa);
        } else {
            gop.value_ptr.* = @intCast(slots.items.len);
            const acc_at = accs.items.len;
            try accs.ensureUnusedCapacity(gpa, calls.len);
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
}

/// How many independent partitions the merge is worth. One partition below a few thousand refs, or with one worker:
/// the hash table is then smaller than the cost of splitting it. Otherwise a power of two, so a key's high bits name
/// its partition, and no larger than the worker count.
fn mergePartitionCount(n_workers: usize, n_refs: usize) usize {
    if (n_workers < 2 or n_refs < 8192) return 1;
    var p: usize = 1;
    while (p * 2 <= n_workers and p < 32) p *= 2;
    return p;
}

const MergePart = struct {
    refs: []const MergeGroupRef,
    hashes: []const u64,
    slots: std.ArrayList(MergedSlot) = .empty,
    accs: std.ArrayList(expr_agg.Accumulator) = .empty,
};

const MergeTask = struct {
    gpa: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
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
            hashMergeGroupRefs(t.gpa, t.tables, p.refs, p.hashes, t.calls, &p.slots, &p.accs) catch |e| {
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
    arena: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    const nparts = mergePartitionCount(n_workers, refs.len);
    if (nparts == 1) return hashMergeGroupRefs(gpa, tables, refs, null, calls, slots, accs);

    const hashes = try arena.alloc(u64, refs.len);
    const counts = try arena.alloc(usize, nparts);
    @memset(counts, 0);
    // `64 - u6` does not fit in a u6 when the width is zero, so the difference stays a usize.
    const bits: usize = std.math.log2_int(usize, nparts);
    const shift: u6 = @intCast(@as(usize, 64) - bits);
    for (refs, hashes) |ref, *h| {
        h.* = std.hash.Wyhash.hash(0, tables[ref.table].keyAt(ref.group));
        counts[h.* >> shift] += 1;
    }
    const starts = try arena.alloc(usize, nparts + 1);
    starts[0] = 0;
    for (counts, 0..) |c, i| starts[i + 1] = starts[i] + c;
    const ordered = try arena.alloc(MergeGroupRef, refs.len);
    const ordered_hash = try arena.alloc(u64, refs.len);
    const fill = try arena.alloc(usize, nparts);
    @memcpy(fill, starts[0..nparts]);
    for (refs, hashes) |ref, h| {
        const at = fill[h >> shift];
        ordered[at] = ref;
        ordered_hash[at] = h;
        fill[h >> shift] = at + 1;
    }

    const parts = try arena.alloc(MergePart, nparts);
    for (parts, 0..) |*p, i| {
        p.* = .{
            .refs = ordered[starts[i]..starts[i + 1]],
            .hashes = ordered_hash[starts[i]..starts[i + 1]],
        };
    }
    defer for (parts) |*p| {
        freeOwnedAccumulatorStrings(gpa, p.accs.items);
        p.accs.deinit(gpa);
        p.slots.deinit(gpa);
    };

    const n_threads = @min(n_workers, nparts);
    const tasks = try arena.alloc(MergeTask, n_threads);
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*t| t.* = .{
        .gpa = gpa,
        .tables = tables,
        .calls = calls,
        .parts = parts,
        .cursor = &cursor,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (!spawn_util.runTree(MergeTask, tasks, config, MergeTask.run)) MergeTask.run(&tasks[0]);
    for (tasks) |t| if (t.err) |e| return e;

    var nslots: usize = 0;
    var naccs: usize = 0;
    for (parts) |p| {
        nslots += p.slots.items.len;
        naccs += p.accs.items.len;
    }
    try slots.ensureTotalCapacity(gpa, nslots);
    try accs.ensureTotalCapacity(gpa, naccs);
    var acc_at: usize = 0;
    for (parts) |*p| {
        for (p.slots.items) |s| slots.appendAssumeCapacity(.{
            .key = s.key,
            .first = s.first,
            .idx = s.idx + acc_at,
        });
        accs.appendSliceAssumeCapacity(p.accs.items);
        acc_at += p.accs.items.len;
        // The combined list now owns these winners.
        p.accs.clearRetainingCapacity();
    }
}

/// The keys of the only groups that can reach the first `offset + limit` rows of an answer ordered by a COUNT,
/// descending, first; null when the order is not such a cut. Exact, after the threshold algorithm of distributed
/// top-k (Cao and Wang, 2004): every table nominates its own top groups, and the k-th largest exact total among the
/// nominees, `tau`, is a floor on the k-th largest total of all. A key whose total reaches `tau` across `m` tables has
/// at least `tau / m` in one of them, so the keys some table counts that high are all that can make the cut, ties at
/// the cut included; the rest are never merged or materialized. Typically a few hundred groups of millions survive.
fn topKCandidates(
    arena: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
) error{OutOfMemory}!?[]const []const u8 {
    const limit = order.limit orelse return null;
    if (order.keys.len == 0 or !order.keys[0].descending) return null;
    const a_idx = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (agg_calls[a_idx].func != .count) return null;
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
            return t.accumulators.items[g * n + at].count;
        }
        fn byCount(_: void, a: @This(), b: @This()) std.math.Order {
            return std.math.order(a.c, b.c);
        }
    };

    // Nominees: each table's own k largest counts, by key.
    var nominees: std.StringHashMapUnmanaged(void) = .empty;
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

    // tau: the k-th largest exact total among the nominees.
    const totals = try arena.alloc(u64, nominees.count());
    var it = nominees.keyIterator();
    for (totals) |*total| {
        const key = it.next().?.*;
        total.* = 0;
        for (tables) |*t| if (t.findKey(key)) |g| {
            total.* +|= Count.of(t, g, a_idx, stride);
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
    return kept.keys();
}

/// Merge an accumulator while transferring ownership of any string winner
/// out of the source. `Accumulator.merge` intentionally consumes its
/// source's owned string; clear the source so its owner (a GroupTable, a
/// work item) cannot free it a second time.
fn mergeOwnedAccumulator(
    dst: *expr_agg.Accumulator,
    src: *expr_agg.Accumulator,
    allocator: std.mem.Allocator,
) !void {
    if (src.* == .distinct) {
        // The larger set is handed over whole and the smaller folded into it, so a merge copies the fewest values.
        const theirs = src.distinct orelse return;
        src.* = .{ .distinct = null };
        const ours = dst.distinct orelse {
            dst.* = .{ .distinct = theirs };
            return;
        };
        const big, const small = if (ours.count() >= theirs.count()) .{ ours, theirs } else .{ theirs, ours };
        dst.* = .{ .distinct = big };
        defer {
            small.deinit(allocator);
            allocator.destroy(small);
        }
        return big.merge(small, allocator);
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
        .distinct => |*d| if (d.*) |set| {
            set.deinit(allocator);
            allocator.destroy(set);
            d.* = null;
        },
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
            else => return error.GroupKeyAliasRequired,
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
) Error![]const BoundOutputColumn {
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
    const specs = try groupOutputSpecs(arena, keys, agg_calls, args.select_cols, args.column_order);
    const bound = try arena.alloc(BoundOutputColumn, specs.len);
    for (specs, bound) |spec, *out| {
        const src = try resolveOutputColumn(keys, agg_calls, spec, args.diag);
        out.* = .{ .source = src, .name = try groupOutputName(keys, spec, src) };
    }
    try checkDistinctOutputNames(BoundOutputColumn, bound, boundName, args.diag);
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
    var key: ?usize = null;
    for (0..keys.items.len) |k_idx| {
        const label = try keys.label(k_idx);
        const source = try keys.source(k_idx);
        if (std.ascii.eqlIgnoreCase(expr_name, label) or std.ascii.eqlIgnoreCase(alias_name, label) or
            std.ascii.eqlIgnoreCase(expr_name, source) or std.ascii.eqlIgnoreCase(alias_name, source))
        {
            key = k_idx;
            break;
        }
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

/// The name an output column asks to print under: its alias, else its expression's name.
fn aliasName(spec: expr_ast.SelectColumn) []const u8 {
    const alias = spec.alias orelse return exprName(spec);
    return std.mem.trim(u8, alias, " ");
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
        .count_distinct => .{ .i = if (state.distinct) |d| @intCast(d.count()) else 0 },
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
        .avg => if (call.result == .avg_dec) .{ .avg_dec = .{
            .sum = state.avg_d.wide(),
            .count = @intCast(state.avg_d.n),
            .mean = try state.avg_d.mean(),
            .spec = expr_agg.resultDec(call),
        } } else .{ .avg = .{ .sum = expr_agg.avgSum(state.avg), .count = @intCast(state.avg.count) } },
    };
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
    // 800 bytes fits exactly one group — enough at -j1, and the shared budget must make it enough at every -j.
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
            .max_memory = 800,
            .unordered = true,
        }) catch |err| {
            std.debug.print(
                "-j{d}: a GROUP BY that fits 800 bytes at -j1 was rejected: {s}\n",
                .{ parallelism, @errorName(err) },
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
            .{ .agg = SPAWN_FIXTURE_AGG, .budget = 1600 },
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
                var dst: expr_agg.Accumulator = .{ .distinct = null };
                defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
                if (c[0].len > 0) dst = .{ .distinct = try set(gpa, c[0]) };
                var src: expr_agg.Accumulator = .{ .distinct = try set(gpa, c[1]) };
                defer freeOwnedAccumulatorStrings(gpa, (&src)[0..1]);
                try mergeOwnedAccumulator(&dst, &src, gpa);
                try testing.expect(src.distinct == null);
                try testing.expectEqual(@as(usize, 5), dst.distinct.?.count());
            }
            // Nothing seen on the source side: the destination is untouched.
            var dst: expr_agg.Accumulator = .{ .distinct = try set(gpa, &.{7}) };
            defer freeOwnedAccumulatorStrings(gpa, (&dst)[0..1]);
            var none: expr_agg.Accumulator = .{ .distinct = null };
            try mergeOwnedAccumulator(&dst, &none, gpa);
            try testing.expectEqual(@as(usize, 1), dst.distinct.?.count());
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
        // Not a COUNT-descending cut: merged in full.
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 2, .descending = true }}, .limit = 10 }, .prunes = false },
        .{ .order = .{ .keys = &.{.{ .column = 1, .descending = true }} }, .prunes = false },
    };
    for ([_][]const u8{ "k", "n" }) |key| {
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
                if (cut.prunes) try testing.expect(merged < all_groups / 10) else try testing.expectEqual(all_groups, merged);
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
    const budget = 2 * 1024 * 1024;
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
                    try testing.expectEqual(@as(u64, @bitCast(fr[2].avg.sum.?)), @as(u64, @bitCast(r[2].avg.sum.?)));
                    try testing.expectEqual(fr[3].i, r[3].i);
                }
                freeResult(res);
            } else first = res;
        }
    };
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
    try hashMergeGroupRefs(allocator, &tables, &refs, null, &calls, &slots, &accs);
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

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var serial_slots: std.ArrayList(MergedSlot) = .empty;
    defer serial_slots.deinit(testing.allocator);
    var serial_accs: std.ArrayList(expr_agg.Accumulator) = .empty;
    defer serial_accs.deinit(testing.allocator);
    // Counts are copied by value and the source is left intact, so a second merge sees the same partials.
    try mergeFoldedRefs(testing.allocator, arena, &tables, refs, &calls, 1, &serial_slots, &serial_accs);
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
        try mergeFoldedRefs(testing.allocator, arena, &tables, refs, &calls, 8, &slots, &accs);
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

test "hash merge of worker tables: overlapping keys, first appearance, allocation failure" {
    try mergeOverlapAndFree(testing.allocator);
    var fixed: NoInPlaceResize = .{ .child = testing.allocator };
    try testing.checkAllAllocationFailures(fixed.allocator(), mergeOverlapAndFree, .{});
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
        .{ .alias = "adn", .v = 27594, .p = 15, .s = 2 },
        .{ .alias = "a38", .v = 1495000000000, .p = 38, .s = 10 },
    };
    const aggregate =
        "sum(d9) AS s9, min(d9) AS mn9, max(d9) AS mx9, sum(d18) AS s18, sum(d38) AS s38, min(d38) AS mn38, " ++
        "max(d38) AS mx38, sum(dn) AS sdn, sum(q) AS sq, sum(d9 * d18) AS p, sum(d9 + i) AS di, " ++
        "sum(d9 * i) AS dti, sum(d38 - d9) AS dd, sum(d9 * 1.5) AS dlit, sum(i * 1.5) AS ilit, sum(-d9) AS neg, " ++
        "sum(coalesce(dn, 0)) AS co, sum(coalesce(dn, d9)) AS cc, avg(dn) AS adn, avg(d38) AS a38";
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
        .aggregate = "sum(dn) AS s, min(dn) AS m, avg(dn) AS a",
        .filter = "i >= -50 AND i < 50",
        .parallelism = 1,
    });
    defer freeResult(empty);
    try testing.expect(empty.output.aggs[0].value == .null_val);
    try testing.expect(empty.output.aggs[1].value == .null_val);
    const a = empty.output.aggs[2].value.avg_dec;
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
        .{ .alias = "adn", .v = 27594, .p = 15, .s = 2 },
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
