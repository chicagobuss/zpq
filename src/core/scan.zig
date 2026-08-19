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

const spawn_util = @import("spawn.zig");
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
};

pub const AggValue = union(enum) {
    /// Single integer result (count, integer sum/min/max). i128 so a sum that
    /// overflows i64 — or an unsigned-64 value/extremum up to 2^64-1 — is
    /// representable (DuckDB likewise widens int SUM to HUGEINT). Counts and
    /// signed i64 values widen in losslessly.
    i: i128,
    /// Single f64 result (float sum/min/max).
    f: f64,
    /// Avg's split-output: ship sum + count, caller divides at end.
    avg: struct { sum: f64, count: i64 },
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
};

/// One unit of parallel work: a single row group within one input file,
/// optionally restricted to a subset of the query's aggregates/columns.
const WorkItem = struct {
    file: usize,
    rg: usize,
    agg_start: usize,
    agg_len: usize,
    fetch_arr: []const bool,
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
    timings: consumer.Timings = .{},
    rows_in: i64 = 0,
    rows_kept: i64 = 0,
    rgs_in: usize = 0,
    rgs_pruned: usize = 0,
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
    // One decode arena for the whole run: scanRGForAgg resets it per row group instead of regrowing a fresh one.
    var rg_decode_arena = std.heap.ArenaAllocator.init(w.gpa);
    defer rg_decode_arena.deinit();

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
        if (w.filter_opt) |f| if (!w.scan_all) {
            // pruneRowGroup needs a transient allocator; per-RG arena
            // keeps the working set small. Skipped under --scan-all.
            var rg_arena = std.heap.ArenaAllocator.init(w.gpa);
            defer rg_arena.deinit();
            if ((try filter_prune.pruneRowGroup(rg, f, rg_arena.allocator(), meta)) == .skip) {
                if (item.agg_start == 0) {
                    w.rgs_pruned += 1;
                }
                continue;
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
                w.filter_opt,
                w.scan_all,
                w.trust_stats,
                item.fetch_arr,
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
                w.filter_opt,
                w.scan_all,
                w.trust_stats,
                item.fetch_arr,
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
            if (!std.ascii.eqlIgnoreCase(a_elem.name, b_elem.name)) {
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
    const agg_calls = if (args.group_by != null and args.aggregate.len == 0)
        &[_]expr_agg.AggCall{}
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
    t.parse_ns = @intCast(nowMonoNs() - t_parse);

    // 3. fetch_set: union of outer-filter columns + each agg's arg
    //    columns + each agg's per-agg WHERE columns + GROUP BY columns. Same set applies
    //    to every file (schemas match).
    const num_leaves = meta0.row_groups.items[0].columns.items.len;
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

    var chunks_per_rg = if (total_rgs >= requested) @as(usize, 1) else (requested + total_rgs - 1) / total_rgs;
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

            // Build specialized fetch_arr for this chunk
            const chunk_fetch_arr = try arena.alloc(bool, num_leaves);
            @memset(chunk_fetch_arr, false);
            for (agg_calls[agg_start .. agg_start + agg_len]) |call| {
                if (call.arg) |arg_expr| arg_expr.collectColumns(chunk_fetch_arr);
                if (call.where) |w_expr| {
                    var wcols: std.ArrayList(usize) = .empty;
                    try w_expr.collectColumns(&wcols, arena);
                    for (wcols.items) |wci| if (wci < num_leaves) {
                        chunk_fetch_arr[wci] = true;
                    };
                }
            }
            if (group_by_keys) |keys| {
                for (keys) |key_expr| key_expr.collectColumns(chunk_fetch_arr);
            }
            for (0..num_leaves) |ci| {
                chunk_fetch_arr[ci] = fetch_arr[ci] and (filter_cols[ci] or chunk_fetch_arr[ci]);
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
    var assignments = try arena.alloc(std.ArrayList(WorkItem), n_workers);
    for (assignments) |*a| a.* = .empty;
    _ = &assignments; // superseded by dynamic claiming via Worker.cursor

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

    var group_cols: ?[]const []const u8 = null;
    var group_rows: ?[]const []const AggValue = null;

    if (group_by_keys) |keys| {
        var coord_table = expr_agg.GroupTable.init(gpa, args.max_memory);
        errdefer coord_table.deinit();

        for (workers) |*w| {
            if (w.group_table) |*gt| {
                try coord_table.mergeTable(gt, agg_calls);
                // Deallocate worker table immediately to reclaim memory before merging
                // the next worker. Set to null so the defer block doesn't double-free.
                gt.deinit();
                w.group_table = null;
            }
            rows_in += w.rows_in;
            rows_kept += w.rows_kept;
            rgs_in += w.rgs_in;
            rgs_pruned += w.rgs_pruned;
            t.core.decode_ns += w.timings.decode_ns;
            t.core.eval_ns += w.timings.eval_ns;
            t.core.encode_ns += w.timings.encode_ns;
        }

        const group_indices = try gpa.alloc(u32, coord_table.keys.items.len);
        defer gpa.free(group_indices);
        for (group_indices, 0..) |*idx, j| idx.* = @intCast(j);

        const KeySorter = struct {
            keys: []const []const u8,
            pub fn lessThan(ctx: @This(), lhs: u32, rhs: u32) bool {
                return std.mem.lessThan(u8, ctx.keys[lhs], ctx.keys[rhs]);
            }
        };
        std.mem.sort(u32, group_indices, KeySorter{ .keys = coord_table.keys.items }, KeySorter.lessThan);

        const select_cols = try resolveGroupSelectCols(
            arena,
            group_by_items.?,
            agg_calls,
            meta0,
            args.select_cols,
            args.column_order,
        );
        const ColSource = union(enum) {
            key: usize,
            agg: usize,
        };
        var col_sources = try gpa.alloc(ColSource, select_cols.len);
        defer gpa.free(col_sources);

        for (select_cols, 0..) |col_name, idx| {
            var resolved = false;
            const expr_name = selectColumnExpr(col_name);
            const alias_name = selectColumnName(col_name);
            for (group_by_items.?, 0..) |item, k_idx| {
                const label = try groupKeyLabel(item, meta0);
                if (std.ascii.eqlIgnoreCase(expr_name, label) or
                    std.ascii.eqlIgnoreCase(alias_name, label))
                {
                    col_sources[idx] = .{ .key = k_idx };
                    resolved = true;
                    break;
                }
                if (item.expr == .col_ref) {
                    const k_name = leafSchemaElem(meta0, item.expr.col_ref.col_idx).name;
                    if (std.ascii.eqlIgnoreCase(expr_name, k_name) or
                        std.ascii.eqlIgnoreCase(alias_name, k_name))
                    {
                        col_sources[idx] = .{ .key = k_idx };
                        resolved = true;
                        break;
                    }
                }
            }

            if (!resolved) {
                for (agg_calls, 0..) |call, a_idx| {
                    if (std.ascii.eqlIgnoreCase(alias_name, call.alias) or
                        std.ascii.eqlIgnoreCase(expr_name, call.alias))
                    {
                        col_sources[idx] = .{ .agg = a_idx };
                        resolved = true;
                        break;
                    }
                }
            }

            if (!resolved) return error.UnknownColumn;
        }

        var key_types = try gpa.alloc(KeyType, keys.len);
        defer gpa.free(key_types);
        for (keys, 0..) |key_expr, idx| {
            key_types[idx] = keyTypeFromExpr(key_expr);
        }

        var rows = try gpa.alloc([]const AggValue, coord_table.keys.items.len);
        errdefer {
            for (rows) |r| {
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

        for (group_indices, 0..) |g_idx, r_idx| {
            const key_bytes = coord_table.keys.items[g_idx];
            var row_vals = try gpa.alloc(AggValue, select_cols.len);
            errdefer gpa.free(row_vals);

            for (col_sources, 0..) |src, col_idx| {
                switch (src) {
                    .key => |k_idx| {
                        row_vals[col_idx] = try deserializeKeyColumn(gpa, key_bytes, k_idx, key_types);
                    },
                    .agg => |a_idx| {
                        const state = coord_table.accumulators.items[g_idx * agg_calls.len + a_idx];
                        row_vals[col_idx] = try materializeOne(gpa, agg_calls[a_idx], state);
                    },
                }
            }
            rows[r_idx] = row_vals;
        }

        group_rows = rows;

        var cols = try gpa.alloc([]const u8, select_cols.len);
        errdefer {
            for (cols) |c| gpa.free(c);
            gpa.free(cols);
        }
        for (select_cols, 0..) |col, idx| {
            cols[idx] = try gpa.dupe(u8, selectColumnName(col));
        }
        group_cols = cols;

        coord_table.deinit();
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
        .cols_stat_pruned = cols_stat_pruned,
        .aggs = items,
        .accumulators = accumulators,
        .agg_calls = agg_calls,
        .timings = t,
        .group_cols = group_cols,
        .group_rows = group_rows,
    };
}

/// How one group-key column is framed in a serialized composite key.
///
/// Framing from the physical Parquet type let writer and reader disagree: a BOOLEAN serialized 8 bytes but
/// deserialized 1, silently truncating every later column of the key.
const KeyType = enum { i64, f64, string };

fn groupKeyLabel(item: expr_ast.SelectItem, meta: *const schema.FileMetaData) Error![]const u8 {
    if (item.alias) |a| return a;
    switch (item.expr) {
        .col_ref => |ref| return leafSchemaElem(meta, ref.col_idx).name,
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
        owned[idx] = try arena.dupe(u8, try groupKeyLabel(item, meta));
        idx += 1;
    }
    for (agg_calls) |call| {
        owned[idx] = try arena.dupe(u8, call.alias);
        idx += 1;
    }
    return owned;
}

/// Framing for a group-key column, taken from the expression's own type.
///
/// Must agree with the lane `evalGroupKeyExpr` produces and `expr_agg.serializeRowKey` then writes: i32 and i64 both
/// serialize as an 8-byte i64, f32 and f64 as an 8-byte f64, so these three cover every column variant.
/// Deliberately does NOT consult the Parquet schema — a leaf index does not address it (see `leafSchemaElem`).
fn keyTypeFromExpr(expr: expr_ast.Expr) KeyType {
    return switch (expr.typeOf()) {
        .i64 => .i64,
        .f64 => .f64,
        .str => .string,
    };
}

/// The schema element for the `leaf_idx`-th primitive column.
///
/// `col_idx` counts LEAVES, but `meta.schema` is a flattened DFS including group nodes, so `leaf_idx + 1` is correct
/// only for files with no nested types. Used for output column NAMES; key framing must not depend on it.
fn leafSchemaElem(meta: *const schema.FileMetaData, leaf_idx: usize) *const schema.SchemaElement {
    var seen: usize = 0;
    // Element 0 is the root, which is always a group.
    for (meta.schema.items[1..]) |*elem| {
        const is_leaf = elem.num_children == null or elem.num_children.? == 0;
        if (!is_leaf) continue;
        if (seen == leaf_idx) return elem;
        seen += 1;
    }
    // Callers resolved `leaf_idx` from this same schema, so it exists.
    return &meta.schema.items[meta.schema.items.len - 1];
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

fn stripQuotes(name: []const u8) []const u8 {
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
            .i64, .f64 => {
                if (cursor + 8 > key_bytes.len) return error.BadGroupKey;
                const raw = std.mem.readInt(u64, key_bytes[cursor..][0..8], .little);
                cursor += 8;
                if (current_idx == target_idx) {
                    return switch (key_types[current_idx]) {
                        .i64 => .{ .i = @as(i64, @bitCast(raw)) },
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
        .sum => switch (call.result) {
            // i128 carries the full sum (incl. i64 overflow + unsigned-64);
            // the JSON layer prints it directly, no narrowing/overflow error.
            .i64 => .{ .i = state.sum_i },
            .f64 => .{ .f = state.sum_f },
            .bytes, .avg_f64 => unreachable,
        },
        .min => switch (call.result) {
            .i64 => .{ .i = state.min_i orelse 0 },
            .f64 => .{ .f = state.min_f orelse 0 },
            .bytes => .{ .s = if (state.min_bytes) |b| try gpa.dupe(u8, b) else try gpa.dupe(u8, "") },
            .avg_f64 => unreachable,
        },
        .max => switch (call.result) {
            .i64 => .{ .i = state.max_i orelse 0 },
            .f64 => .{ .f = state.max_f orelse 0 },
            .bytes => .{ .s = if (state.max_bytes) |b| try gpa.dupe(u8, b) else try gpa.dupe(u8, "") },
            .avg_f64 => unreachable,
        },
        .avg => .{ .avg = .{ .sum = state.avg.sum, .count = @intCast(state.avg.count) } },
    };
}

fn nowMonoNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
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
const not_parquet = [_]u8{'x'} ** 64;

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
// `leafSchemaElem`). The fixture puts a MAP ahead of ordinary scalars to reproduce that shape.
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
