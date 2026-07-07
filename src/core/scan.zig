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
    SchemaMismatch,
    EmptyAggregate,
    AggSumOverflow,
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

/// One worker thread's slice of work.
const Worker = struct {
    gpa: std.mem.Allocator,
    inputs: []const Input,
    metas: []const schema.FileMetaData,
    work: []const WorkItem,
    agg_calls: []const expr_agg.AggCall,
    filter_opt: ?filter_ast.Filter,
    scan_all: bool,
    trust_stats: bool,
    timings: consumer.Timings = .{},
    rows_in: i64 = 0,
    rows_kept: i64 = 0,
    rgs_in: usize = 0,
    rgs_pruned: usize = 0,
    err: ?anyerror = null,
};

fn workerRun(w: *Worker) void {
    workerRunErr(w) catch |e| {
        w.err = e;
    };
}

fn workerRunErr(w: *Worker) !void {
    for (w.work) |item| {
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
            &w.timings,
        );
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

    // 1. Parse metadata for every input. Cheap (~ms per file); serial
    //    keeps the schema-validation order deterministic.
    const t_parse = nowMonoNs();
    const metas: []const schema.FileMetaData = if (args.metas) |m| blk: {
        if (m.len != args.inputs.len) return error.SchemaMismatch;
        break :blk m;
    } else blk: {
        var parsed = try arena.alloc(schema.FileMetaData, args.inputs.len);
        for (args.inputs, 0..) |in, i| {
            parsed[i] = metadata.open(arena, in.bytes) catch |err| {
                std.debug.print("zpq query: input file {s} is not a valid Parquet file ({s})\n", .{ in.name, @errorName(err) });
                return error.AlreadyReported;
            };
        }
        break :blk parsed;
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
    const agg_calls = try expr_parser.parseAggList(arena, args.aggregate, meta0);
    if (agg_calls.len == 0) return error.EmptyAggregate;

    var filter_opt: ?filter_ast.Filter = null;
    if (args.filter) |expr_str| {
        if (expr_str.len > 0) filter_opt = try filter_parser.parse(arena, expr_str, meta0);
    }
    t.parse_ns = @intCast(nowMonoNs() - t_parse);

    // 3. fetch_set: union of outer-filter columns + each agg's arg
    //    columns + each agg's per-agg WHERE columns. Same set applies
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
    if (filter_opt == null and !args.scan_all) {
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
    if (chunks_per_rg > agg_calls.len) chunks_per_rg = agg_calls.len;
    if (chunks_per_rg < 1) chunks_per_rg = 1;

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
            for (0..num_leaves) |ci| {
                chunk_fetch_arr[ci] = fetch_arr[ci] and (filter_cols[ci] or chunk_fetch_arr[ci]);
            }

            // Initialize accumulators for this chunk
            const sub_accs = try arena.alloc(expr_agg.Accumulator, agg_len);
            for (agg_calls[agg_start .. agg_start + agg_len], 0..) |call, ci| {
                sub_accs[ci] = expr_agg.Accumulator.init(call);
            }

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

    const n_workers = @max(@as(usize, 1), @min(requested, work_items.items.len));

    var workers = try arena.alloc(Worker, n_workers);
    var assignments = try arena.alloc(std.ArrayList(WorkItem), n_workers);
    for (assignments) |*a| a.* = .empty;
    for (work_items.items, 0..) |item, k| try assignments[k % n_workers].append(arena, item);

    for (workers, 0..) |*w, wi| {
        w.* = .{
            .gpa = gpa,
            .inputs = args.inputs,
            .metas = metas,
            .work = assignments[wi].items,
            .agg_calls = agg_calls,
            .filter_opt = filter_opt,
            .scan_all = args.scan_all,
            .trust_stats = args.trust_stats,
        };
    }

    // 5. Spawn, join, propagate first error. Time the whole parallel
    //    region as one wall-clock span (real elapsed decode), separate
    //    from the per-worker CPU sum in `core.decode_ns`.
    const t_decode = nowMonoNs();
    if (n_workers > 1) {
        var threads = try arena.alloc(std.Thread, n_workers);
        for (workers, 0..) |*w, i| {
            threads[i] = try std.Thread.spawn(.{}, workerRun, .{w});
        }
        for (threads) |th| th.join();
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
    for (workers) |w| {
        for (w.work) |item| {
            for (item.accumulators, 0..) |sub_acc, i| {
                accumulators[item.agg_start + i].merge(sub_acc);
            }
        }
        rows_in += w.rows_in;
        rows_kept += w.rows_kept;
        rgs_in += w.rgs_in;
        rgs_pruned += w.rgs_pruned;
        // Sum core CPU time across workers — wall is dominated by the
        // slowest worker, but sum-of-CPU is the more interpretable
        // number for "where did the work go."
        t.core.decode_ns += w.timings.decode_ns;
        t.core.eval_ns += w.timings.eval_ns;
        t.core.encode_ns += w.timings.encode_ns;
    }

    // 7. Materialize. Allocate results from gpa so they outlive the
    //    arena the caller will eventually deinit.
    var items = try gpa.alloc(AggOutputItem, agg_calls.len);
    errdefer gpa.free(items);
    for (agg_calls, 0..) |call, i| {
        items[i] = .{
            .alias = try gpa.dupe(u8, call.alias),
            .value = try materializeOne(call, accumulators[i]),
        };
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
    };
}

/// Convert one agg's final accumulator state into a wire-friendly value.
pub fn materializeOne(call: expr_agg.AggCall, state: expr_agg.Accumulator) !AggValue {
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
            .bytes => .{ .s = state.min_bytes orelse "" },
            .avg_f64 => unreachable,
        },
        .max => switch (call.result) {
            .i64 => .{ .i = state.max_i orelse 0 },
            .f64 => .{ .f = state.max_f orelse 0 },
            .bytes => .{ .s = state.max_bytes orelse "" },
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
