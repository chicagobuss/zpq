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

const spawn_util = @import("spawn.zig");
const huge_pages = @import("huge_pages.zig");
const schema = @import("schema.zig");
const consumer = @import("consumer.zig");
const metadata = @import("parquet/metadata.zig");
const expr_ast = @import("expr/ast.zig");
const expr_parser = @import("expr/parser.zig");
const expr_agg = @import("expr/agg.zig");
const filter_ast = @import("filter/ast.zig");
const filter_parser = @import("filter/parser.zig");
const filter_prune = @import("filter/prune.zig");

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

/// The output column an error is about, for callers that report it. Only meaningful after a
/// `DuplicateOutputColumn` / `AmbiguousOutputColumn` / `UnknownColumn` error from `runMultiAggregate`, or an
/// `UnknownColumn` / `AmbiguousColumn` error for a --columns name from `engine`.
pub const Diag = struct {
    buf: [256]u8 = undefined,
    len: usize = 0,

    pub fn column(self: *const Diag) []const u8 {
        return self.buf[0..self.len];
    }

    pub fn setColumn(diag: ?*Diag, name: []const u8) void {
        const d = diag orelse return;
        d.len = @min(name.len, d.buf.len);
        @memcpy(d.buf[0..d.len], name[0..d.len]);
    }
};

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
    select_cols: ?[]const []const u8 = null,
    /// Comma-separated output column names. Only used with GROUP BY when
    /// `select_cols` is unset (e.g. CLI `--column-order`). Reorders the
    /// default key-then-aggregate layout.
    column_order: ?[]const u8 = null,
    /// 0 = use cpu_count. 1 = serial. Row-group granularity: work is the
    /// flat list of all row groups across all inputs, distributed across
    /// `min(this, total_row_groups)` workers — so one big file uses all cores.
    parallelism: usize = 0,
    /// `--scan-all`: disable every stats shortcut — no row-group pruning,
    /// no stats-driven column drop, no aggregate stat short-circuit. Forces
    /// a full page/byte decode. The paranoid / untrusted-writer-stats path.
    scan_all: bool = false,
    /// `--trust-stats`: answer min/max/sum from file statistics (the fast
    /// path) instead of decoding. Off by default — file stats can be wrong.
    /// `count(*)` is always answered from num_rows regardless.
    trust_stats: bool = false,
    fast_levels: bool = false,
    /// Receives the column name behind an output-naming error.
    diag: ?*Diag = null,
};

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
    /// String (bytewise/unsigned) min/max result. Borrowed from the
    /// accumulator's persist allocator (query-lifetime `gpa`).
    s: []const u8,
    null_val: void,
};

pub const AggOutputItem = struct {
    alias: []const u8,
    value: AggValue,
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
    /// Owned in the caller-provided gpa; caller frees `aggs` and each
    /// item's `alias`. Values themselves are POD.
    aggs: []AggOutputItem,
    /// Raw accumulators in the orchestrator's arena lifetime — useful
    /// for the optional 1-row parquet output the CLI / Lambda produce.
    /// Lifetime ends with the outer arena that the caller passed to
    /// `runMultiAggregate` via the `arena` argument.
    accumulators: []const expr_agg.Accumulator,
    /// Aggregate calls (parsed AST). Same lifetime as `accumulators`.
    agg_calls: []const expr_agg.AggCall,
    timings: Timings,
    group_cols: ?[]const []const u8 = null,
    group_rows: ?[]const []const AggValue = null,
    /// Value lane of each `group_cols` entry, so a writer can type a column that is NULL in every row. Allocated in
    /// the `arena` passed to `runMultiAggregate`, like `accumulators`.
    group_col_types: ?[]const GroupColType = null,
    /// Per aggregate call: the source column of a min/max, which passes values through (see `passThroughSource`).
    /// Same lifetime as `accumulators`.
    agg_sources: []const ?schema.SchemaElement = &.{},
};

/// What one GROUP BY result column holds. `nullable` is false only for counts, matching the 1-row output.
pub const GroupColType = struct {
    lane: enum { i64, f64, string, avg },
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
    fetch_arr: []const bool,
    /// Fetch set for a row group statistics prove fully matching: `fetch_arr` minus the columns only the filter
    /// reads. Anything an aggregate argument, per-agg FILTER, or GROUP BY key reads stays, and is decoded in full.
    proven_fetch_arr: []const bool,
    accumulators: []expr_agg.Accumulator,
};

/// Zig's 16 MiB default makes worker creation expensive for short scans. Safe to shrink: these workers decode, and
/// every nesting path here is bounded.
pub const WORKER_STACK_SIZE: usize = 1 << 20;

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
};

fn parseWorker(c: *ParseCtx) void {
    _ = c.runs.fetchAdd(1, .monotonic);
    while (true) {
        const i = c.cursor.fetchAdd(1, .monotonic);
        if (i >= c.inputs.len) break;
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
) std.mem.Allocator.Error!FooterParse {
    const parsed = try arena.alloc(schema.FileMetaData, inputs.len);
    const parse_threads = @min(
        inputs.len,
        if (parallelism == 0) (std.Thread.getCpuCount() catch 1) else parallelism,
    );

    if (parse_threads <= 1) {
        for (inputs, 0..) |in, i| {
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
    };

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
    filter_opt: ?filter_ast.Filter,
    scan_all: bool,
    trust_stats: bool,
    decode_options: consumer.DecodeOptions,
    timings: consumer.Timings = .{},
    rows_in: i64 = 0,
    rows_kept: i64 = 0,
    rgs_in: usize = 0,
    rgs_pruned: usize = 0,
    rgs_full_match: usize = 0,
    group_by_keys: ?[]const expr_ast.Expr = null,
    group_table: ?expr_agg.GroupTable = null,
    err: ?anyerror = null,
};

fn workerRun(w: *Worker) void {
    workerRunErr(w) catch |e| {
        w.err = e;
    };
    // Holding a drawn block until teardown let a barely-grouping worker strand a small budget and fail the worker
    // doing the real work.
    if (w.group_table) |*gt| gt.releaseSlack();
}

fn workerRunErr(w: *Worker) !void {
    // One decode arena for the whole run: scanRGForAgg rewinds it per row group instead of regrowing a fresh one.
    // Both it and the scratch are written front to back and reused, the case huge pages pay off for.
    var thp: huge_pages.HugePageAdvisor = .{ .child = w.gpa };
    var rg_decode_arena = std.heap.ArenaAllocator.init(thp.allocator());
    defer rg_decode_arena.deinit();
    var scratch = consumer.DecodeScratch.init(thp.allocator());
    defer scratch.deinit();
    var decode_options = w.decode_options;
    decode_options.scratch = &scratch;

    while (true) {
        const idx = w.cursor.fetchAdd(1, .monotonic);
        if (idx >= w.work.len) break;
        const item = w.work[idx];
        const meta = &w.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        // File bytes are shared read-only across workers (one mmap / one
        // in-memory buffer); concurrent ranged reads are safe.
        const rg_src: consumer.RGSrc = .{ .bytes = w.inputs[item.file].bytes, .byte_origin = 0 };
        if (item.agg_start == 0) {
            w.rgs_in += 1;
            w.rows_in += rg.num_rows;
        }
        var rg_filter = w.filter_opt;
        var fetch_arr = item.fetch_arr;
        if (w.filter_opt) |f| if (!w.scan_all) {
            // pruneRowGroup needs a transient allocator; per-RG arena
            // keeps the working set small. Skipped under --scan-all.
            var rg_arena = std.heap.ArenaAllocator.init(w.gpa);
            defer rg_arena.deinit();
            switch (try filter_prune.pruneRowGroup(rg, f, rg_arena.allocator(), meta)) {
                .skip => {
                    if (item.agg_start == 0) {
                        w.rgs_pruned += 1;
                    }
                    continue;
                },
                // Every row matches, so the row group is scanned as if unfiltered. The S3 planner makes this same
                // call from the same footer and leaves filter-only chunks unfetched, so the two must not diverge.
                .always_match => {
                    rg_filter = null;
                    fetch_arr = item.proven_fetch_arr;
                    if (item.agg_start == 0) w.rgs_full_match += 1;
                },
                .keep, .unknown => {},
            }
        };
        if (item.agg_start == 0) {
            w.rows_kept += rg.num_rows;
        }
        const sub_agg_calls = w.agg_calls[item.agg_start .. item.agg_start + item.agg_len];
        if (w.group_table) |*gt| {
            try consumer.scanRGForAgg(
                w.gpa,
                rg,
                meta,
                rg_src,
                rg_filter,
                w.scan_all,
                w.trust_stats,
                decode_options,
                fetch_arr,
                sub_agg_calls,
                &[_]expr_agg.Accumulator{},
                w.group_by_keys,
                gt,
                &rg_decode_arena,
                &w.timings,
            );
        } else {
            try consumer.scanRGForAgg(
                w.gpa,
                rg,
                meta,
                rg_src,
                rg_filter,
                w.scan_all,
                w.trust_stats,
                decode_options,
                fetch_arr,
                sub_agg_calls,
                item.accumulators,
                null,
                null,
                &rg_decode_arena,
                &w.timings,
            );
        }
    }
}

/// Run a multi-file aggregate.
///
/// `gpa` is used for thread-shared scratch (per-RG arenas, per-worker
/// state). `arena` is used for results that outlive the function call:
/// `MultiAggResult.accumulators`, `agg_calls`, and the parsed metas live
/// in this arena. The result's `aggs` slice is gpa-owned (caller frees).
pub fn runMultiAggregate(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    args: MultiAggArgs,
) !MultiAggResult {
    if (args.inputs.len == 0) return error.NoInputs;

    var t: Timings = .{};

    // Parallel because footer parsing scales with file count and schema width and can dominate short multi-file scans.
    // `meta_arenas` must outlive this call: `metas` points into them.
    var meta_arenas: []std.heap.ArenaAllocator = &.{};
    defer for (meta_arenas) |*a| a.deinit();

    const t_parse = nowMonoNs();
    const metas: []const schema.FileMetaData = if (args.metas) |m| blk: {
        if (m.len != args.inputs.len) return error.SchemaMismatch;
        break :blk m;
    } else blk: {
        switch (try parseFooters(arena, gpa, args.inputs, args.parallelism, &meta_arenas)) {
            .ok => |parsed| break :blk parsed,
            .failed => |f| {
                std.debug.print(
                    "zpq query: input file {s} is not a valid Parquet file ({s})\n",
                    .{ args.inputs[f.index].name, @errorName(f.err) },
                );
                return error.AlreadyReported;
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
                std.debug.print(
                    "schema mismatch: file 0 ({s}) vs file {d} ({s}): '{s}' vs '{s}'\n",
                    .{ args.inputs[0].name, i, args.inputs[i].name, b_elem.name, a_elem.name },
                );
                return error.SchemaMismatch;
            }
        }
    }

    // 2. Parse aggregate + filter against meta0 (column indexes resolved
    //    against file 0; valid for all files because schemas match).
    const agg_calls: []expr_agg.AggCall = if (args.group_by != null and args.aggregate.len == 0)
        &.{}
    else
        try expr_parser.parseAggList(arena, args.aggregate, meta0);
    if (agg_calls.len == 0 and args.group_by == null) return error.EmptyAggregate;

    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |expr_str| {
        if (expr_str.len > 0) filter_opt = try filter_parser.parse(arena, expr_str, meta0);
    }
    const group_by_items = if (args.group_by) |gb_str|
        try expr_parser.parseGroupBy(arena, gb_str, meta0)
    else
        null;
    const group_by_keys = if (group_by_items) |items| blk: {
        const exprs = try arena.alloc(expr_ast.Expr, items.len);
        for (items, 0..) |item, i| exprs[i] = item.expr;
        break :blk exprs;
    } else null;
    try nameUnaliasedAggs(arena, agg_calls, group_by_items, meta0);
    const agg_sources = try arena.alloc(?schema.SchemaElement, agg_calls.len);
    for (agg_calls, agg_sources) |call, *src| src.* = passThroughSource(call, meta0);
    // Before any scanning: a clash would otherwise surface as one column silently showing another's values.
    const group_select_cols: ?[]const []const u8 = if (group_by_items) |items|
        try checkGroupOutputNames(arena, items, agg_calls, meta0, args)
    else blk: {
        try checkAggAliases(agg_calls, args.diag);
        break :blk null;
    };
    t.parse_ns = @intCast(nowMonoNs() - t_parse);

    // 3. fetch_set: union of outer-filter columns + each agg's arg
    //    columns + each agg's per-agg WHERE columns + GROUP BY columns. Same set applies
    //    to every file (schemas match).
    const num_leaves = metadata.leafCount(meta0);
    const fetch_arr = try arena.alloc(bool, num_leaves);
    @memset(fetch_arr, false);
    if (filter_opt) |f| {
        var cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&cols, arena);
        for (cols.items) |ci| if (ci < num_leaves) {
            fetch_arr[ci] = true;
        };
    }
    for (agg_calls) |call| {
        if (call.arg) |arg_expr| arg_expr.collectColumns(fetch_arr);
        if (call.where) |w_expr| {
            var cols: std.ArrayList(usize) = .empty;
            try w_expr.collectColumns(&cols, arena);
            for (cols.items) |ci| if (ci < num_leaves) {
                fetch_arr[ci] = true;
            };
        }
    }
    if (group_by_keys) |keys| {
        for (keys) |key_expr| {
            key_expr.collectColumns(fetch_arr);
        }
    }

    // 3b. Stats-driven fetch pruning. For each column in the fetch
    //     set: if EVERY aggregate that references it can be answered
    //     from row-group statistics on EVERY RG of EVERY input, the
    //     column never decodes. Drop it from the fetch set so the
    //     network/range layer doesn't read its bytes either.
    //
    //     Skipped entirely when an outer filter is present (stats
    //     reflect all rows; we can't trust them once a predicate
    //     prunes the population). Filter-referenced columns also
    //     remain (they need raw values for evaluation).
    //
    //     Per-agg WHERE makes a call ineligible for stats period —
    //     `statsCoverageComplete` enforces that via canStatShortCircuit.
    var cols_stat_pruned: usize = 0;
    if (filter_opt == null and group_by_keys == null and !args.scan_all) {
        var per_agg_cols = try arena.alloc(bool, num_leaves);
        for (fetch_arr, 0..) |needed, ci| {
            if (!needed) continue;

            var prunable = true;
            for (agg_calls) |call| {
                @memset(per_agg_cols, false);
                if (call.arg) |arg_expr| arg_expr.collectColumns(per_agg_cols);
                if (call.where) |w_expr| {
                    var wcols: std.ArrayList(usize) = .empty;
                    w_expr.collectColumns(&wcols, arena) catch {
                        prunable = false;
                        break;
                    };
                    for (wcols.items) |wci| if (wci < num_leaves) {
                        per_agg_cols[wci] = true;
                    };
                }
                if (!per_agg_cols[ci]) continue;
                if (!expr_agg.statsCoverageComplete(call, metas, ci, args.trust_stats)) {
                    prunable = false;
                    break;
                }
            }
            if (prunable) {
                fetch_arr[ci] = false;
                cols_stat_pruned += 1;
            }
        }
    }

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

    // Precompute filter columns set to intersect with each chunk's references
    const filter_cols = try arena.alloc(bool, num_leaves);
    @memset(filter_cols, false);
    if (filter_opt) |f| {
        var cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&cols, arena);
        for (cols.items) |ci| if (ci < num_leaves) {
            filter_cols[ci] = true;
        };
    }

    var work_items: std.ArrayList(WorkItem) = .empty;
    for (raw_work_items.items) |raw| {
        const base_chunk_size = agg_calls.len / chunks_per_rg;
        const remainder = agg_calls.len % chunks_per_rg;

        var chunk_idx: usize = 0;
        while (chunk_idx < chunks_per_rg) : (chunk_idx += 1) {
            const agg_start = chunk_idx * base_chunk_size + @min(chunk_idx, remainder);
            const agg_len = base_chunk_size + if (chunk_idx < remainder) @as(usize, 1) else @as(usize, 0);

            // Build specialized fetch_arr for this chunk. `proven_fetch_arr` is what it reads when the outer filter is
            // proven to pass every row: only its own aggregates' and the GROUP BY's columns.
            const proven_fetch_arr = try arena.alloc(bool, num_leaves);
            @memset(proven_fetch_arr, false);
            for (agg_calls[agg_start .. agg_start + agg_len]) |call| {
                if (call.arg) |arg_expr| arg_expr.collectColumns(proven_fetch_arr);
                if (call.where) |w_expr| {
                    var wcols: std.ArrayList(usize) = .empty;
                    try w_expr.collectColumns(&wcols, arena);
                    for (wcols.items) |wci| if (wci < num_leaves) {
                        proven_fetch_arr[wci] = true;
                    };
                }
            }
            if (group_by_keys) |keys| {
                for (keys) |key_expr| key_expr.collectColumns(proven_fetch_arr);
            }
            const chunk_fetch_arr = try arena.alloc(bool, num_leaves);
            for (0..num_leaves) |ci| {
                chunk_fetch_arr[ci] = fetch_arr[ci] and (filter_cols[ci] or proven_fetch_arr[ci]);
                proven_fetch_arr[ci] = fetch_arr[ci] and proven_fetch_arr[ci];
            }

            // Initialize accumulators for this chunk
            const sub_accs = if (group_by_keys != null)
                try arena.alloc(expr_agg.Accumulator, 0)
            else blk: {
                const arr = try arena.alloc(expr_agg.Accumulator, agg_len);
                for (agg_calls[agg_start .. agg_start + agg_len], 0..) |call, ci| {
                    arr[ci] = expr_agg.Accumulator.init(call);
                }
                break :blk arr;
            };

            try work_items.append(arena, .{
                .file = raw.file,
                .rg = raw.rg,
                .agg_start = agg_start,
                .agg_len = agg_len,
                .fetch_arr = chunk_fetch_arr,
                .proven_fetch_arr = proven_fetch_arr,
                .accumulators = sub_accs,
            });
        }
    }

    // Size the worker set by BYTES to decode, not row-group count: thread creation has a fixed cost and a query may
    // touch only a small part of a wide schema. 2 MiB per worker is a conservative floor.
    const MIN_BYTES_PER_WORKER: u64 = 2 * 1024 * 1024;
    var fetch_bytes: u64 = 0;
    for (work_items.items) |item| {
        const rg = &metas[item.file].row_groups.items[item.rg];
        for (rg.columns.items, 0..) |col, ci| {
            if (ci < item.fetch_arr.len and item.fetch_arr[ci]) {
                // Larger of two proxies: uncompressed bytes makes BOOLEAN (8 rows per byte) and dictionary-encoded
                // strings (index stream only) look nearly free, so `rows x 4` floors the cost they pay when
                // materialized.
                if (col.meta_data) |cm| {
                    const uncompressed: u64 = @intCast(cm.total_uncompressed_size);
                    const by_rows: u64 = @as(u64, @intCast(rg.num_rows)) * 4;
                    fetch_bytes += @max(uncompressed, by_rows);
                }
            }
        }
    }
    const by_bytes = @max(@as(usize, 1), @as(usize, @intCast(fetch_bytes / MIN_BYTES_PER_WORKER)));
    const n_workers = @max(@as(usize, 1), @min(@min(requested, work_items.items.len), by_bytes));

    var workers = try arena.alloc(Worker, n_workers);
    // One budget shared by every worker table, not per-worker quotas: work is claimed off a shared cursor, so which
    // worker meets the group-heavy row groups is unknowable in advance — fixed quotas made `-j2` reject queries `-j1`
    // ran on identical data. Fully initialized before any worker exists, so none can insert against an unpublished
    // budget.
    var group_budget = expr_agg.SharedBudget{
        .limit = args.max_memory,
        .block = expr_agg.SharedBudget.blockFor(args.max_memory, n_workers),
    };

    var work_cursor = std.atomic.Value(usize).init(0);
    for (workers, 0..) |*w, wi| {
        _ = wi;
        w.* = .{
            .gpa = gpa,
            .inputs = args.inputs,
            .metas = metas,
            .work = work_items.items,
            .cursor = &work_cursor,
            .agg_calls = agg_calls,
            .filter_opt = filter_opt,
            .scan_all = args.scan_all,
            .trust_stats = args.trust_stats,
            .decode_options = .{ .fast_levels = args.fast_levels },
            .group_by_keys = group_by_keys,
            .group_table = if (group_by_keys != null) blk_gt: {
                // Zero local ceiling: everything is drawn from the shared pool, so an idle worker reserves nothing
                // others could use.
                var gt = expr_agg.GroupTable.init(gpa, 0);
                gt.shared = &group_budget;
                break :blk_gt gt;
            } else null,
        };
    }
    defer if (group_by_keys != null) {
        for (workers) |*w| {
            if (w.group_table) |*gt| gt.deinit();
        }
    };

    // 5. Spawn, join, propagate first error. Time the whole parallel
    //    region as one wall-clock span (real elapsed decode), separate
    //    from the per-worker CPU sum in `core.decode_ns`.
    const t_decode = nowMonoNs();
    if (n_workers > 1) {
        var threads = try arena.alloc(std.Thread, n_workers);
        var spawned: usize = 0;
        for (workers, 0..) |*w, i| {
            // Deliberately not `try`: running threads hold pointers into `arena` and their own `group_table`, both
            // freed by the caller, so an early return here is a use-after-free, not just a leak.
            threads[i] = spawn_util.spawn(.{ .stack_size = WORKER_STACK_SIZE }, workerRun, .{w}) catch break;
            spawned += 1;
        }
        // Run the work that never got a thread; one runner drains the shared cursor however many spawns failed.
        if (spawned < n_workers) workerRun(&workers[spawned]);
        for (threads[0..spawned]) |th| th.join();
    } else {
        // Single-threaded fast path. Avoids std.Thread overhead.
        workerRun(&workers[0]);
    }
    t.decode_wall_ns = @intCast(nowMonoNs() - t_decode);
    for (workers) |w| if (w.err) |e| return e;

    // 6. Merge per-work-item accumulators into one final slice.
    var accumulators = try arena.alloc(expr_agg.Accumulator, agg_calls.len);
    for (agg_calls, 0..) |call, i| accumulators[i] = expr_agg.Accumulator.init(call);
    var rows_in: i64 = 0;
    var rows_kept: i64 = 0;
    var rgs_in: usize = 0;
    var rgs_pruned: usize = 0;
    var rgs_full_match: usize = 0;

    var group_cols: ?[]const []const u8 = null;
    var group_rows: ?[]const []const AggValue = null;
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

        // Merge worker groups without building a second GroupTable. The old
        // coordinator copied every key while the source table was still live,
        // so a one-worker query admitted at the max-memory ceiling briefly
        // held almost twice that much table state during finalization.
        //
        // Sorting lightweight references lets equal keys from different
        // workers meet next to each other. Their accumulator ownership is
        // folded directly into one temporary row state, then materialized;
        // the worker tables remain the only GROUP BY tables in memory.
        const GroupRef = struct {
            worker: u32,
            group: u32,
        };
        var total_group_refs: usize = 0;
        for (workers) |w| {
            if (w.group_table) |gt| total_group_refs += gt.keys.items.len;
        }
        const group_refs = try gpa.alloc(GroupRef, total_group_refs);
        defer gpa.free(group_refs);
        var ref_i: usize = 0;
        for (workers, 0..) |w, wi| {
            if (w.group_table) |gt| {
                for (0..gt.keys.items.len) |gi| {
                    group_refs[ref_i] = .{
                        .worker = @intCast(wi),
                        .group = @intCast(gi),
                    };
                    ref_i += 1;
                }
            }
        }

        const GroupRefSorter = struct {
            workers: []const Worker,

            fn key(ctx: @This(), ref: GroupRef) []const u8 {
                return ctx.workers[ref.worker].group_table.?.keys.items[ref.group];
            }

            pub fn lessThan(ctx: @This(), lhs: GroupRef, rhs: GroupRef) bool {
                return std.mem.lessThan(u8, ctx.key(lhs), ctx.key(rhs));
            }
        };
        const ref_sorter = GroupRefSorter{ .workers = workers };
        std.mem.sort(GroupRef, group_refs, ref_sorter, GroupRefSorter.lessThan);

        var distinct_groups: usize = 0;
        var previous_key: ?[]const u8 = null;
        for (group_refs) |ref| {
            const key = ref_sorter.key(ref);
            if (previous_key == null or !std.mem.eql(u8, previous_key.?, key)) {
                distinct_groups += 1;
                previous_key = key;
            }
        }

        const select_cols = group_select_cols.?;
        const col_sources = try gpa.alloc(ColSource, select_cols.len);
        defer gpa.free(col_sources);
        for (select_cols, col_sources) |col_name, *src| {
            src.* = try resolveOutputColumn(arena, group_by_items.?, agg_calls, meta0, col_name, args.diag);
        }

        var key_types = try gpa.alloc(KeyType, keys.len);
        defer gpa.free(key_types);
        for (keys, 0..) |key_expr, idx| {
            key_types[idx] = keyTypeFromExpr(key_expr);
        }

        const col_types = try arena.alloc(GroupColType, col_sources.len);
        for (col_sources, col_types) |src, *ct| ct.* = switch (src) {
            .key => |k_idx| .{
                .nullable = true,
                .lane = switch (key_types[k_idx]) {
                    .i64, .u64 => .i64,
                    .f64 => .f64,
                    .string => .string,
                },
                .source = if (keys[k_idx] == .col_ref) leafSchemaElem(meta0, keys[k_idx].col_ref.col_idx) else null,
            },
            .agg => |a_idx| .{
                .nullable = agg_calls[a_idx].func != .count,
                .lane = switch (agg_calls[a_idx].result) {
                    .i64 => .i64,
                    .f64 => .f64,
                    .bytes => .string,
                    .avg_f64 => .avg,
                },
                .source = agg_sources[a_idx],
            },
        };
        group_col_types = col_types;

        const rows = try gpa.alloc([]const AggValue, distinct_groups);
        var rows_built: usize = 0;
        errdefer {
            for (rows[0..rows_built]) |r| {
                for (r) |v| {
                    switch (v) {
                        .s => |s| gpa.free(s),
                        else => {},
                    }
                }
                gpa.free(r);
            }
            gpa.free(rows);
        }

        var group_start: usize = 0;
        while (group_start < group_refs.len) {
            const key_bytes = ref_sorter.key(group_refs[group_start]);
            var group_end = group_start + 1;
            while (group_end < group_refs.len and
                std.mem.eql(u8, key_bytes, ref_sorter.key(group_refs[group_end])))
            {
                group_end += 1;
            }

            const merged = try gpa.alloc(expr_agg.Accumulator, agg_calls.len);
            for (agg_calls, 0..) |call, i| merged[i] = expr_agg.Accumulator.init(call);
            defer {
                freeOwnedAccumulatorStrings(gpa, merged);
                gpa.free(merged);
            }

            for (group_refs[group_start..group_end]) |ref| {
                const gt = &workers[ref.worker].group_table.?;
                const acc_start = @as(usize, ref.group) * agg_calls.len;
                for (merged, gt.accumulators.items[acc_start..][0..agg_calls.len]) |*dst, *src| {
                    mergeOwnedAccumulator(dst, src, gpa);
                }
            }

            const row_vals = try gpa.alloc(AggValue, select_cols.len);
            var row_cols_built: usize = 0;
            errdefer {
                for (row_vals[0..row_cols_built]) |v| switch (v) {
                    .s => |s| gpa.free(s),
                    else => {},
                };
                gpa.free(row_vals);
            }
            for (col_sources, 0..) |src, col_idx| {
                switch (src) {
                    .key => |k_idx| {
                        row_vals[col_idx] = try deserializeKeyColumn(gpa, key_bytes, k_idx, key_types);
                    },
                    .agg => |a_idx| {
                        row_vals[col_idx] = try materializeOne(gpa, agg_calls[a_idx], merged[a_idx]);
                    },
                }
                row_cols_built += 1;
            }
            rows[rows_built] = row_vals;
            rows_built += 1;
            group_start = group_end;
        }

        group_rows = rows;

        const cols = try gpa.alloc([]const u8, select_cols.len);
        var cols_built: usize = 0;
        errdefer {
            for (cols[0..cols_built]) |c| gpa.free(c);
            gpa.free(cols);
        }
        for (select_cols, col_sources, 0..) |col, src, idx| {
            cols[idx] = try gpa.dupe(u8, try groupOutputName(arena, group_by_items.?, meta0, col, src));
            cols_built += 1;
        }
        group_cols = cols;

        // Result rows own any strings they need; worker table keys and
        // accumulator winners can now be released before returning.
        for (workers) |*w| {
            if (w.group_table) |*gt| {
                gt.deinit();
                w.group_table = null;
            }
        }
    } else {
        // Merge each work item exactly ONCE: `work` is shared and claimed dynamically, so folding it per worker would
        // merge every item n_workers times — inflating sums and double-freeing the string min/max winner.
        for (work_items.items) |item| {
            for (item.accumulators, 0..) |sub_acc, i| {
                accumulators[item.agg_start + i].merge(sub_acc, gpa);
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

    // 7. Materialize. Allocate results from gpa so they outlive the
    //    arena the caller will eventually deinit.
    var items: []AggOutputItem = &[_]AggOutputItem{};
    if (group_by_keys == null) {
        items = try gpa.alloc(AggOutputItem, agg_calls.len);
        errdefer gpa.free(items);
        for (agg_calls, 0..) |call, i| {
            items[i] = .{
                .alias = try gpa.dupe(u8, call.alias),
                .value = try materializeOne(gpa, call, accumulators[i]),
            };
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
        .cols_stat_pruned = cols_stat_pruned,
        .aggs = items,
        .accumulators = accumulators,
        .agg_calls = agg_calls,
        .timings = t,
        .group_cols = group_cols,
        .group_rows = group_rows,
        .group_col_types = group_col_types,
        .agg_sources = agg_sources,
    };
}

/// min/max of a bare column returns one of that column's values, so its output can carry the column's type.
fn passThroughSource(call: expr_agg.AggCall, meta: *const schema.FileMetaData) ?schema.SchemaElement {
    if (call.func != .min and call.func != .max) return null;
    const arg = call.arg orelse return null;
    if (arg != .col_ref) return null;
    return leafSchemaElem(meta, arg.col_ref.col_idx);
}

/// Merge an accumulator while transferring ownership of any string winner
/// out of the source table. `Accumulator.merge` intentionally consumes its
/// source's owned string; clear the source so GroupTable.deinit cannot free it
/// a second time.
fn mergeOwnedAccumulator(
    dst: *expr_agg.Accumulator,
    src: *expr_agg.Accumulator,
    allocator: std.mem.Allocator,
) void {
    const moved = src.*;
    dst.merge(moved, allocator);
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
    for (accumulators) |acc| switch (acc) {
        .min_bytes => |s| if (s) |owned| allocator.free(owned),
        .max_bytes => |s| if (s) |owned| allocator.free(owned),
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
const KeyType = enum { i64, u64, f64, string };

fn groupKeyLabel(arena: std.mem.Allocator, item: expr_ast.SelectItem, meta: *const schema.FileMetaData) Error![]const u8 {
    if (item.alias) |a| return a;
    switch (item.expr) {
        .col_ref => |ref| return leafLabel(arena, meta, ref.col_idx),
        else => return error.GroupKeyAliasRequired,
    }
}

fn splitColumnList(arena: std.mem.Allocator, csv: []const u8) ![]const []const u8 {
    var names: std.ArrayList([]const u8) = .empty;
    var it = std.mem.splitScalar(u8, csv, ',');
    while (it.next()) |part| {
        const name = std.mem.trim(u8, part, " \t\r\n");
        if (name.len > 0) try names.append(arena, name);
    }
    return names.items;
}

fn resolveGroupSelectCols(
    arena: std.mem.Allocator,
    group_items: []const expr_ast.SelectItem,
    agg_calls: []const expr_agg.AggCall,
    meta: *const schema.FileMetaData,
    select_cols: ?[]const []const u8,
    column_order: ?[]const u8,
) ![]const []const u8 {
    if (select_cols) |cols| {
        if (cols.len > 0) return cols;
    }
    if (column_order) |order| {
        const names = try splitColumnList(arena, order);
        if (names.len == 0) return error.UnknownColumn;
        const owned = try arena.alloc([]const u8, names.len);
        for (names, 0..) |name, i| owned[i] = try arena.dupe(u8, name);
        return owned;
    }
    const owned = try arena.alloc([]const u8, group_items.len + agg_calls.len);
    var idx: usize = 0;
    for (group_items) |item| {
        owned[idx] = try arena.dupe(u8, try groupKeyLabel(arena, item, meta));
        idx += 1;
    }
    for (agg_calls) |call| {
        owned[idx] = try arena.dupe(u8, call.alias);
        idx += 1;
    }
    return owned;
}

const ColSource = union(enum) {
    key: usize,
    agg: usize,
};

/// An unaliased aggregate is named after its function (`sum`), as it always has been, unless that name is taken by
/// another aggregate or a GROUP BY key; then it takes its call text (`sum(x)`), as DuckDB names it. So
/// `sum(x), sum(f)` gives `sum(x)` and `sum(f)`, while a lone `sum(x)` still prints as `sum`.
fn nameUnaliasedAggs(
    arena: std.mem.Allocator,
    agg_calls: []expr_agg.AggCall,
    group_items: ?[]const expr_ast.SelectItem,
    meta: *const schema.FileMetaData,
) Error!void {
    const taken = try arena.alloc(bool, agg_calls.len);
    for (agg_calls, taken, 0..) |call, *t, i| {
        t.* = false;
        if (call.default_name == null) continue;
        for (agg_calls, 0..) |other, j| {
            if (i != j and std.ascii.eqlIgnoreCase(call.alias, other.alias)) t.* = true;
        }
        for (group_items orelse &.{}) |item| {
            const label = try groupKeyLabel(arena, item, meta);
            const source = if (item.expr == .col_ref) try leafLabel(arena, meta, item.expr.col_ref.col_idx) else label;
            if (std.ascii.eqlIgnoreCase(call.alias, label) or std.ascii.eqlIgnoreCase(call.alias, source)) t.* = true;
        }
    }
    // Decided before renaming any, so one rename can't make another name look unique.
    for (agg_calls, taken) |*call, t| if (t) {
        call.alias = call.default_name.?;
    };
}

/// Reject two aggregates sharing an alias: both would land under one output name.
fn checkAggAliases(agg_calls: []const expr_agg.AggCall, diag: ?*Diag) Error!void {
    for (agg_calls, 0..) |call, i| for (agg_calls[0..i]) |prev| {
        if (std.ascii.eqlIgnoreCase(call.alias, prev.alias)) {
            Diag.setColumn(diag, call.alias);
            return error.DuplicateOutputColumn;
        }
    };
}

/// Resolve the GROUP BY output columns and check their names up front: no aggregate alias twice, no output name
/// twice, and no name that matches both a key and an aggregate. Returns the output column list.
fn checkGroupOutputNames(
    arena: std.mem.Allocator,
    group_items: []const expr_ast.SelectItem,
    agg_calls: []const expr_agg.AggCall,
    meta: *const schema.FileMetaData,
    args: MultiAggArgs,
) Error![]const []const u8 {
    try checkAggAliases(agg_calls, args.diag);
    for (group_items) |item| if (item.alias == null and item.expr == .col_ref) {
        _ = leafLabel(arena, meta, item.expr.col_ref.col_idx) catch |err| {
            if (err == error.DuplicateOutputColumn) {
                const segments = (try metadata.leafPathSegments(arena, meta, item.expr.col_ref.col_idx)).?;
                Diag.setColumn(args.diag, try metadata.quotePath(arena, segments));
            }
            return err;
        };
    };
    const cols = try resolveGroupSelectCols(arena, group_items, agg_calls, meta, args.select_cols, args.column_order);
    const names = try arena.alloc([]const u8, cols.len);
    for (cols, names, 0..) |col, *name, i| {
        const src = try resolveOutputColumn(arena, group_items, agg_calls, meta, col, args.diag);
        name.* = try groupOutputName(arena, group_items, meta, col, src);
        for (names[0..i]) |prev| if (std.ascii.eqlIgnoreCase(name.*, prev)) {
            Diag.setColumn(args.diag, name.*);
            return error.DuplicateOutputColumn;
        };
    }
    return cols;
}

/// The name output column `col` prints under: an `AS` alias it gives; else a GROUP BY key's label, however `col`
/// spells the key (`SELECT "r"."v"` and `--group-by v` both print `r.v`, see `metadata.leafLabel`); else the
/// aggregate's name as `col` gives it.
fn groupOutputName(
    arena: std.mem.Allocator,
    group_items: []const expr_ast.SelectItem,
    meta: *const schema.FileMetaData,
    col: []const u8,
    src: ColSource,
) Error![]const u8 {
    const renamed = std.mem.lastIndexOf(u8, std.mem.trim(u8, col, " "), " AS ") != null;
    return switch (src) {
        .key => |k| if (renamed) selectColumnName(col) else groupKeyLabel(arena, group_items[k], meta),
        .agg => selectColumnName(col),
    };
}

/// Which GROUP BY key or aggregate an output column names. A key matches by its output label or, for a bare column
/// key, its source column path (`r.key` for a nested leaf); an aggregate by its alias. A name matching both is an
/// error rather than a guess: with `--group-by 's AS k'` and `... AS s`, the key used to win and `s` silently showed
/// the key's values.
fn resolveOutputColumn(
    arena: std.mem.Allocator,
    group_items: []const expr_ast.SelectItem,
    agg_calls: []const expr_agg.AggCall,
    meta: *const schema.FileMetaData,
    col_name: []const u8,
    diag: ?*Diag,
) Error!ColSource {
    const expr_name = selectColumnExpr(col_name);
    const alias_name = selectColumnName(col_name);
    var key: ?usize = null;
    for (group_items, 0..) |item, k_idx| {
        const label = try groupKeyLabel(arena, item, meta);
        const source = if (item.expr == .col_ref) try leafLabel(arena, meta, item.expr.col_ref.col_idx) else label;
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
        if (metadata.resolveColumn(meta, expr_name) catch null) |leaf| {
            for (group_items, 0..) |item, k_idx| if (item.expr == .col_ref and item.expr.col_ref.col_idx == leaf) {
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

fn selectColumnName(col_name: []const u8) []const u8 {
    const clean_col = std.mem.trim(u8, col_name, " ");
    if (std.mem.lastIndexOf(u8, clean_col, " AS ")) |as_idx| {
        return std.mem.trim(u8, clean_col[as_idx + 4 ..], " ");
    }
    return stripQuotes(clean_col);
}

fn selectColumnExpr(col_name: []const u8) []const u8 {
    const clean_col = std.mem.trim(u8, col_name, " ");
    if (std.mem.lastIndexOf(u8, clean_col, " AS ")) |as_idx| {
        return stripQuotes(std.mem.trim(u8, clean_col[0..as_idx], " "));
    }
    return stripQuotes(clean_col);
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
                        .string => unreachable,
                    };
                }
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

/// Convert one agg's final accumulator state into a wire-friendly value.
pub fn materializeOne(gpa: std.mem.Allocator, call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
    return switch (call.func) {
        .count => .{ .i = @intCast(state.count) },
        // i128 carries the full sum (incl. i64 overflow + unsigned-64);
        // the JSON layer prints it directly, no narrowing/overflow error.
        // sum/min/max over no values are NULL, never a 0 or "" stand-in.
        .sum => switch (call.result) {
            .i64 => if (state.sum_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.sum_f) |v| .{ .f = v } else .null_val,
            .bytes, .avg_f64 => unreachable,
        },
        .min => switch (call.result) {
            .i64 => if (state.min_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.min_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.min_bytes) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .avg_f64 => unreachable,
        },
        .max => switch (call.result) {
            .i64 => if (state.max_i) |v| .{ .i = v } else .null_val,
            .f64 => if (state.max_f) |v| .{ .f = v } else .null_val,
            .bytes => if (state.max_bytes) |b| .{ .s = try gpa.dupe(u8, b) } else .null_val,
            .avg_f64 => unreachable,
        },
        .avg => .{ .avg = .{ .sum = expr_agg.avgSum(state.avg), .count = @intCast(state.avg.count) } },
    };
}

// ============================================================
// Compile-time API check. Behavioral coverage is via the CLI
// regression suite + lambda integration tests.
// ============================================================
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
        defer {
            for (res.aggs) |a| testing.allocator.free(a.alias);
            testing.allocator.free(res.aggs);
        }

        try testing.expectEqual(@as(usize, 1), res.aggs.len);
        const got: Answer = .{ .sum = res.aggs[0].value.i, .rows = res.rows_in };
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
        }) catch |err| {
            std.debug.print(
                "-j{d}: skewed work rejected a query with 10% headroom: {s}\n",
                .{ parallelism, @errorName(err) },
            );
            return err;
        };
        defer {
            for (res.aggs) |a| testing.allocator.free(a.alias);
            testing.allocator.free(res.aggs);
            if (res.group_rows) |rows| {
                for (rows) |r| {
                    for (r) |v| switch (v) {
                        .s => |str| testing.allocator.free(str),
                        else => {},
                    };
                    testing.allocator.free(r);
                }
                testing.allocator.free(rows);
            }
            if (res.group_cols) |cols| {
                for (cols) |c| testing.allocator.free(c);
                testing.allocator.free(cols);
            }
        }
        try testing.expectEqual(@as(usize, 1001), (res.group_rows orelse return error.TestUnexpectedResult).len);
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
        }) catch |err| {
            std.debug.print(
                "-j{d}: a GROUP BY that fits 800 bytes at -j1 was rejected: {s}\n",
                .{ parallelism, @errorName(err) },
            );
            return err;
        };
        defer {
            for (res.aggs) |a| testing.allocator.free(a.alias);
            testing.allocator.free(res.aggs);
            if (res.group_rows) |rows| {
                for (rows) |r| {
                    for (r) |v| switch (v) {
                        .s => |str| testing.allocator.free(str),
                        else => {},
                    };
                    testing.allocator.free(r);
                }
                testing.allocator.free(rows);
            }
            if (res.group_cols) |cols| {
                for (cols) |c| testing.allocator.free(c);
                testing.allocator.free(cols);
            }
        }
        try testing.expectEqual(@as(usize, 1), (res.group_rows orelse return error.TestUnexpectedResult).len);
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
        defer {
            for (res.aggs) |a| testing.allocator.free(a.alias);
            testing.allocator.free(res.aggs);
            if (res.group_rows) |rows| {
                for (rows) |r| {
                    for (r) |v| switch (v) {
                        .s => |s| testing.allocator.free(s),
                        else => {},
                    };
                    testing.allocator.free(r);
                }
                testing.allocator.free(rows);
            }
            if (res.group_cols) |cols| {
                for (cols) |c| testing.allocator.free(c);
                testing.allocator.free(cols);
            }
        }

        const groups = (res.group_rows orelse return error.TestUnexpectedResult).len;
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

fn freeGroupResult(res: MultiAggResult) void {
    for (res.aggs) |a| testing.allocator.free(a.alias);
    testing.allocator.free(res.aggs);
    if (res.group_rows) |rows| {
        for (rows) |r| {
            for (r) |v| switch (v) {
                .s => |str| testing.allocator.free(str),
                else => {},
            };
            testing.allocator.free(r);
        }
        testing.allocator.free(rows);
    }
    if (res.group_cols) |cols| {
        for (cols) |c| testing.allocator.free(c);
        testing.allocator.free(cols);
    }
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
    defer freeGroupResult(res);

    const rows = res.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 5), rows.len);
    var total: i128 = 0;
    for (rows) |r| total += r[1].i;
    try testing.expectEqual(@as(i128, 60), total);
    for (rows) |r| try testing.expect(r[0] == .i);

    // The output column name also depends on resolving the right leaf.
    const cols = res.group_cols orelse return error.TestUnexpectedResult;
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
    defer freeGroupResult(res);

    const rows = res.group_rows orelse return error.TestUnexpectedResult;
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
    const res = try parseFooters(arena_state.allocator(), testing.allocator, &inputs, 1, &meta_arenas);
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
        .null_val => {},
    }
}

fn expectSameAnswer(want: MultiAggResult, got: MultiAggResult) !void {
    try testing.expectEqual(want.aggs.len, got.aggs.len);
    for (want.aggs, got.aggs) |w, g| try expectSameAggValue(w.value, g.value);
    const want_rows = want.group_rows orelse &.{};
    const got_rows = got.group_rows orelse &.{};
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
    defer freeGroupResult(oracle);
    try testing.expectEqual(@as(usize, 0), oracle.row_groups_full_match);

    var last: ?MultiAggResult = null;
    errdefer if (last) |r| freeGroupResult(r);
    for ([_]usize{ 1, 4, 16 }) |parallelism| {
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        const got = try runFullMatchQuery(arena_state.allocator(), inputs, q, parallelism, false);
        if (last) |r| freeGroupResult(r);
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
        freeGroupResult(res);
    }

    // Hand-known values, so agreement with --scan-all is not agreement on a shared mistake.
    const res = try checkFullMatchQuery(&inputs, queries[2]);
    defer freeGroupResult(res);
    try testing.expectEqual(@as(i128, (500 + 1599) * 1100 / 2), res.aggs[0].value.i);
    try testing.expectEqual(@as(i128, 1599), res.aggs[1].value.i);
    try testing.expectEqual(@as(i128, 1100), res.aggs[2].value.i);
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
        defer freeGroupResult(res);
        try testing.expectEqual(@as(i128, 0), res.aggs[0].value.i);
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
        const f = try filter_parser.parse(arena, c.filter, &meta);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = try filter_prune.pruneRowGroup(rg, f, arena, &meta);
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
    defer freeGroupResult(res);
    const rows = res.group_rows orelse return error.TestUnexpectedResult;
    try testing.expectEqual(@as(usize, 11), rows.len);
    for (rows) |r| try testing.expectEqual(@as(i128, 100), r[1].i);

    const keyed = try checkFullMatchQuery(&inputs, .{
        .filter = "ts >= 500",
        .aggregate = "count(*) AS n, sum(x) AS sx",
        .group_by = "b",
        .full_match = 5,
    });
    freeGroupResult(keyed);
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
    defer freeGroupResult(res);
    try testing.expectEqual(@as(i128, 3 * 1100), res.aggs[0].value.i);
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
        defer freeGroupResult(want);

        for ([_]usize{ 1, 16 }) |parallelism| {
            var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena_state.deinit();
            const got = try runFullMatchQuery(arena_state.allocator(), &poisoned_in, c.q, parallelism, false);
            defer freeGroupResult(got);
            try expectSameAnswer(want, got);
            try testing.expectEqual(c.q.full_match, got.row_groups_full_match);
        }

        // Control: the poison is detectable once the column is read, or the checks above prove nothing.
        var ctl_arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer ctl_arena.deinit();
        var ctl_q = c.q;
        ctl_q.aggregate = "count(*) AS n";
        if (runFullMatchQuery(ctl_arena.allocator(), &poisoned_in, ctl_q, 1, true)) |r| {
            freeGroupResult(r);
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
            defer freeGroupResult(res);
            const got_sum: i128 = switch (res.aggs[1].value) {
                .i => |v| v,
                else => 0, // sum over no rows
            };
            if (res.aggs[0].value.i != c.n or got_sum != c.sum_i) {
                std.debug.print("{s}: filter {s} (scan_all={}, -j{d}) gave n={d} sum={d}, want n={d} sum={d}\n", .{
                    path, c.filter, scan_all, parallelism, res.aggs[0].value.i, got_sum, c.n, c.sum_i,
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
        const f = try filter_parser.parse(arena, c.filter, &meta);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = try filter_prune.pruneRowGroup(rg, f, arena, &meta);
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
        const f = try filter_parser.parse(arena, c.filter, &meta);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = try filter_prune.pruneRowGroup(rg, f, arena, &meta);
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
    defer freeGroupResult(res);
    try testing.expectEqual(@as(f64, 1.0), res.aggs[0].value.f);
    try testing.expectEqual(@as(f64, 7.0), res.aggs[1].value.f);
    try testing.expectEqual(@as(i128, 103), res.aggs[2].value.i);
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
        const f = try filter_parser.parse(arena, c.filter, &meta);
        for (meta.row_groups.items, c.want, 0..) |*rg, want, i| {
            const got = try filter_prune.pruneRowGroup(rg, f, arena, &meta);
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
    const f = try filter_parser.parse(arena, "d != 0", &meta);
    const D = filter_prune.Decision;
    for (meta.row_groups.items, [_]D{ .keep, .skip, .keep }, 0..) |*rg, want, i| {
        const got = try filter_prune.pruneRowGroup(rg, f, arena, &meta);
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
            .select_cols = c.select_cols,
            .parallelism = 1,
            .diag = &diag,
        });
        try testing.expectError(c.want, res);
        try testing.expectEqualStrings(c.column, diag.column());
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
            .select_cols = c.select_cols,
            .parallelism = 1,
        });
        defer freeGroupResult(res);
        if (res.group_cols) |cols| {
            try testing.expectEqual(c.want.len, cols.len);
            for (c.want, cols) |w, g| try testing.expectEqualStrings(w, g);
        } else {
            try testing.expectEqual(c.want.len, res.aggs.len);
            for (c.want, res.aggs) |w, g| try testing.expectEqualStrings(w, g.alias);
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
            defer {
                for (res.aggs) |a| testing.allocator.free(a.alias);
                testing.allocator.free(res.aggs);
            }
            testing.expectEqual(q.want, res.aggs[0].value.i) catch |err| {
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
    const m0 = try metadata.sharedLeafNameMetaForTest(a);
    var m1 = try metadata.sharedLeafNameMetaForTest(a);
    m1.schema.items[0].num_children = 6;
    m1.schema.items[1].num_children = 1;

    const inputs = [_]Input{ .{ .name = "f0", .bytes = "" }, .{ .name = "f1", .bytes = "" } };
    const metas = [_]schema.FileMetaData{ m0, m1 };
    try testing.expectError(error.SchemaMismatch, runMultiAggregate(testing.allocator, a, .{
        .inputs = &inputs,
        .metas = &metas,
        .aggregate = "sum(key) AS s",
        .parallelism = 1,
    }));
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
        defer freeGroupResult(oracle);
        const got = try runMultiAggregate(testing.allocator, arena_state.allocator(), args);
        defer freeGroupResult(got);
        expectSameAnswer(oracle, got) catch |err| {
            std.debug.print("all-null pages: filter={s} agg={s} trust_stats={} disagrees with --scan-all\n", .{
                q.filter, q.aggregate, trust_stats,
            });
            return err;
        };
        // Hand-known counts, so agreement is not agreement on a shared mistake.
        if (q.group_by == null and std.mem.eql(u8, q.aggregate, "count(*) AS n, sum(id) AS si")) {
            const want: ?i128 = if (std.mem.eql(u8, q.filter, "v IS NULL")) 650 else if (std.mem.eql(u8, q.filter, "s.b IS NULL")) 997 else null;
            if (want) |w| try testing.expectEqual(w, got.aggs[0].value.i);
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
            .select_cols = c.select_cols,
            .parallelism = 1,
        });
        defer freeGroupResult(res);
        const cols = res.group_cols.?;
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
    try testing.expectEqualStrings("\"a\".\"b\"", diag.column());
}
