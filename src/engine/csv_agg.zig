//! An aggregate over a CSV file, folded during the parse: the query is bound as a Parquet aggregate query is, against
//! the file's fields as a schema (`csv_write.fileSchema`); each chunk parses only the fields it reads and folds its
//! rows into its own aggregate states, which merge in chunk order into one answer (`Ordered`). Types are settled as a
//! filter's are: a query that does not bind to the sample's types binds to the file's, and a value wider than its
//! column's type starts the fold again on the widened types.
//!
//! The chunks are cut as for one worker at any parallelism, so which rows fold into which state, and the order the
//! states merge in, are the file's and not the host's: the answer is the same at every `-j`.

const std = @import("std");
const builtin = @import("builtin");
const csv_write = @import("csv_write.zig");
const aggregate = @import("aggregate.zig");
const engine_types = @import("types.zig");
const text = @import("text.zig");
const schema = @import("../core/schema.zig");
const consumer = @import("../core/consumer.zig");
const core_memory = @import("../core/memory.zig");
const blocking = @import("../core/blocking.zig");
const system = @import("../core/system.zig");
const csv_scan = @import("../core/csv/scan.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_eval = @import("../core/filter/eval.zig");
const filter_selection = @import("../core/filter/selection.zig");
const expr_ast = @import("../core/expr/ast.zig");
const expr_agg = @import("../core/expr/agg.zig");
const expr_eval = @import("../core/expr/eval.zig");
const agg_coded = @import("../core/expr/agg_coded.zig");
const scan = @import("../core/scan.zig");
const execute = @import("../core/scan/execute.zig");
const scan_types = @import("../core/scan/types.zig");
const scan_rows = @import("../core/scan/rows.zig");
const key_decode = @import("../core/scan/key_decode.zig");
const owned_merge = @import("../core/scan/owned_merge.zig");
const slot_sort = @import("../core/scan/slot_sort.zig");
const binding = @import("../core/scan/binding.zig");
const owner_support = @import("../core/scan/owner_support.zig");
const output_values = @import("../core/scan/output_values.zig");
const MergedSlot = @import("../core/scan/count_cut.zig").MergedSlot;
const local_fs = @import("../local_fs.zig");
const nowMonoNs = @import("../clock.zig").monoNs;

const Context = engine_types.Context;
const QueryArgs = engine_types.QueryArgs;
const AggResult = engine_types.AggResult;
const Plan = csv_write.Plan;
const Source = csv_write.Source;
const Type = csv_write.Type;

/// Whether `args` asks for an aggregate this path folds: anything that needs the rows or the groups in an order (ORDER
/// BY, LIMIT, OFFSET), a join, or a column order is refused by name.
pub fn refused(args: QueryArgs) bool {
    if (args.column_order != null or args.join != null) return true;
    const order = args.result_order;
    if (order.keys.len > 0 or order.rows.len > 0 or order.limit != null or order.offset != 0 or order.hidden.len > 0)
        return true;
    if (args.select != null or args.columns != null) return true;
    if (args.plan_source) |ps| {
        const q = @import("syntax_plan.zig").syntaxOf(ps) orelse return true;
        if (q.join != null or q.row_keys.len > 0 or q.hidden.len > 0) return true;
    }
    return false;
}

/// The aggregate `args` asks of the CSV file at `path`, its refusal on `args.diag` (`csv_write.describe`).
pub fn query(ctx: Context, args: QueryArgs, path: []const u8) !AggResult {
    if (refused(args)) return error.CsvQueryNotSupported;
    var local: csv_write.Report = .{};
    const report = args.csv_report orelse &local;
    var scratch = std.heap.ArenaAllocator.init(ctx.gpa);
    defer scratch.deinit();
    const alloc = args.csv_report_alloc orelse scratch.allocator();
    if (args.diag) |d| d.execution_started = true;
    return run(ctx, args, path, report, alloc) catch |e| {
        if (args.diag) |d| csv_write.describe(d, args, path, e, report);
        return e;
    };
}

/// How a CSV aggregate runs, as `--explain` reports it: planned and bound as the query is, nothing folded. The types are
/// the sample's, or the file's where binding settled them; a value past the sample can still widen one during the fold.
pub const Explanation = struct {
    grouped: bool,
    calls: usize,
    keys: usize,
    filter: bool,
    having: bool,
    workers: usize,
    chunks: usize,
    chunk_bytes: usize,
    max_memory: usize,
    settled: bool,
    /// The fields each chunk parses, in file order, and their types.
    fields: []const []const u8,
    types: []const Type,

    pub fn writeJson(e: Explanation, writer: anytype) !void {
        try writer.print(
            "{{\"ok\":true,\"explain\":true,\"input\":\"csv\",\"strategy\":\"csv_fold\",\"merge\":\"ordered_chunks\"," ++
                "\"grouped\":{},\"calls\":{d},\"keys\":{d},\"filter\":{},\"having\":{},\"workers\":{d},\"chunks\":{d}," ++
                "\"chunk_bytes\":{d},\"max_memory\":{d},\"types_settled\":{},\"fields\":[",
            .{ e.grouped, e.calls, e.keys, e.filter, e.having, e.workers, e.chunks, e.chunk_bytes, e.max_memory, e.settled },
        );
        for (e.fields, e.types, 0..) |name, t, i| {
            if (i > 0) try writer.writeByte(',');
            var buf: [64]u8 = undefined;
            try writer.writeAll("{\"name\":\"");
            try text.writeJsonString(writer, name);
            try writer.writeAll("\",\"type\":\"");
            try text.writeJsonString(writer, t.name(&buf));
            try writer.writeAll("\"}");
        }
        try writer.writeAll("]}\n");
    }
};

/// The plan of the aggregate `args` asks of the CSV file at `path`, on `arena`; its refusal on `args.diag`.
pub fn explain(ctx: Context, arena: std.mem.Allocator, args: QueryArgs, path: []const u8) !Explanation {
    if (refused(args)) return error.CsvQueryNotSupported;
    var report: csv_write.Report = .{};
    return explainOver(ctx, arena, args, path, &report) catch |e| {
        if (args.diag) |d| csv_write.describe(d, args, path, e, &report);
        return e;
    };
}

fn explainOver(ctx: Context, arena: std.mem.Allocator, args: QueryArgs, path: []const u8, report: *csv_write.Report) !Explanation {
    const mapped = try mapInput(path, &report.failure);
    defer mapped.unmap();
    var planned = try Planned.of(ctx, arena, args, mapped.bytes, &report.failure);
    const p = &planned.p;
    report.fields = p.fields;
    const b = try Bound.of(arena, args, &planned.all, p);
    // Names are slices of the mapping, which goes when this returns.
    const fields = try arena.alloc([]const u8, p.names.len);
    for (fields, p.names) |*d, n| d.* = try arena.dupe(u8, n);
    const data = mapped.bytes.len - p.data_start;
    return .{
        .grouped = b.q.keys != null,
        .calls = b.q.calls.len,
        .keys = if (b.q.keys) |k| k.len else 0,
        .filter = b.q.filter != null,
        .having = args.result_order.having != null,
        .workers = planned.workers,
        .chunks = @max(1, std.math.divCeil(usize, data, planned.chunk) catch 1),
        .chunk_bytes = planned.chunk,
        .max_memory = args.max_memory,
        .settled = planned.settled,
        .fields = fields,
        .types = try arena.dupe(Type, p.types),
    };
}

/// The query bound to the file's fields: its calls, filter, keys and outputs, and per field whether a chunk parses it.
const Bound = struct {
    meta: *const schema.FileMetaData,
    q: execute.BoundQueryPhase,
    fetch: []bool,

    fn of(a: std.mem.Allocator, args: QueryArgs, all: *const Plan, p: ?*const Plan) !Bound {
        return ofMeta(a, args, all.fields.len, try csv_write.fileSchema(a, all, p));
    }

    fn ofMeta(a: std.mem.Allocator, args: QueryArgs, n: usize, meta: *const schema.FileMetaData) !Bound {
        const cache = try a.create(engine_types.bound_plan.Cache);
        cache.* = .{};
        const with_cache = args.withPlanCache(cache);
        var margs: scan.MultiAggArgs = .{
            .inputs = &.{},
            .aggregate = args.aggregate orelse "",
            .filter = args.filter,
            .group_by = args.group_by,
            .select_cols = args.select_cols,
            .result_order = args.result_order,
            .diag = args.diag,
            // A key a GROUP BY holds that another determines is still a key here: the fold has no plan that skips it.
            .dependent_keys = false,
        };
        margs.bound_aggregate = with_cache.boundAggregate(a, meta) catch |e| return switch (e) {
            error.InvalidBoundPlan => error.CsvQueryNotSupported,
            else => e,
        };
        const q = try execute.bindAggregateQuery(a, margs, meta);
        const fetch = try a.alloc(bool, n);
        @memset(fetch, false);
        var list: std.ArrayList(usize) = .empty;
        for (q.calls) |call| {
            if (call.arg) |e| e.collectColumns(fetch);
            if (call.arg2) |e| e.collectColumns(fetch);
            if (call.where) |w| try w.collectColumns(&list, a);
        }
        for (q.keys orelse &.{}) |k| k.collectColumns(fetch);
        if (q.filter) |f| try f.collectColumns(&list, a);
        for (list.items) |ci| if (ci < n) {
            fetch[ci] = true;
        };
        // COUNT(*) alone reads no field; the parse still frames every record, and counts them by its first.
        if (std.mem.indexOfScalar(bool, fetch, true) == null and n > 0) fetch[0] = true;
        return .{ .meta = meta, .q = q, .fetch = fetch };
    }

    fn fetchedNames(b: *const Bound, a: std.mem.Allocator, fields: []const []const u8) ![]const []const u8 {
        var out: std.ArrayListUnmanaged([]const u8) = .empty;
        for (fields, b.fetch) |name, f| if (f) try out.append(a, name);
        return out.items;
    }
};

/// The plans a CSV aggregate runs on: every field's (`all`, the query's schema) and the fetched fields' (`p`, what each
/// chunk parses), with the types settled from the whole file when the sample's did not bind (`settled`).
const Planned = struct {
    all: Plan,
    p: Plan,
    chunk: usize,
    workers: usize,
    settled: bool,

    fn of(ctx: Context, arena: std.mem.Allocator, args: QueryArgs, bytes: []const u8, failure: *csv_write.Failure) !Planned {
        const opts = args.csv;
        const workers = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
        // Cut as for one worker at any `-j` (see the top of the file).
        const chunk = csv_write.chunkBytes(opts, args, 1, bytes.len);
        var every = opts;
        every.columns = null;
        var all = try csv_write.plan(arena, bytes, every, failure);
        var settled = false;
        const first = Bound.of(arena, args, &all, null) catch |e| retry: {
            // As a filter settles them (`csv_write.run`): the sample's types are provisional, the file's are not.
            if (e == error.CsvQueryNotSupported or e == error.OutOfMemory or opts.strict) return e;
            const seen = try csv_write.classifyFile(ctx, arena, &all, chunk, workers, failure);
            for (all.types, seen, all.pinned) |*t, s, pinned| if (!pinned) {
                t.* = t.join(s);
            };
            settled = true;
            break :retry try Bound.of(arena, args, &all, null);
        };
        var parsed = opts;
        parsed.columns = try first.fetchedNames(arena, all.fields);
        var p = try csv_write.plan(arena, bytes, parsed, failure);
        if (settled) for (p.slot, 0..) |slot, ci| if (slot) |o| {
            p.types[o] = all.types[ci];
        };
        return .{ .all = all, .p = p, .chunk = chunk, .workers = workers, .settled = settled };
    }
};

fn mapInput(path: []const u8, failure: *csv_write.Failure) !local_fs.Mapped {
    if (std.mem.startsWith(u8, path, "s3://")) return error.CsvRemoteInput;
    return local_fs.mapFile(path) catch |e| switch (e) {
        error.EmptyFile => {
            failure.* = .{ .kind = .empty, .line = 1 };
            return error.CsvEmpty;
        },
        else => return e,
    };
}

fn run(ctx: Context, args: QueryArgs, path: []const u8, report: *csv_write.Report, report_alloc: std.mem.Allocator) !AggResult {
    const opts = args.csv;
    const failure = &report.failure;
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const t_open = nowMonoNs();
    const mapped = try mapInput(path, failure);
    defer mapped.unmap();
    var planned = try Planned.of(ctx, arena, args, mapped.bytes, failure);
    const all = &planned.all;
    const p = &planned.p;
    const chunk = planned.chunk;
    const workers = planned.workers;
    const fields = try report_alloc.alloc([]const u8, p.fields.len);
    for (fields, p.fields) |*d, n| d.* = try report_alloc.dupe(u8, n);
    report.fields = fields;

    while (true) {
        var pass_state = std.heap.ArenaAllocator.init(ctx.gpa);
        defer pass_state.deinit();
        const pass = pass_state.allocator();
        // Bound again each pass: a widened column changes what its calls and keys are.
        const b = try Bound.of(pass, args, all, p);
        return foldOnce(ctx, pass, args, path, &b, p, chunk, workers, mapped, report, t_open) catch |e| {
            if (e != error.CsvTypeChanged or opts.strict) return e;
            const seen = try csv_write.classifyFile(ctx, pass, p, chunk, workers, failure);
            for (p.types, seen, p.pinned) |*t, s, pinned| if (!pinned) {
                t.* = t.join(s);
            };
            report.restarts += 1;
            if (report.restarts > 2) return e; // the join of the whole file holds every value
            continue;
        };
    }
}

/// Chunk states merged strictly in chunk order, each as soon as every chunk before it has (`scan.BlockMerge`'s
/// discipline, for scalar states as for group tables): a chunk starts only within `ahead` chunks of the first unmerged
/// one, so at most `ahead` chunks' states are alive beside the merged one.
const Fold = struct {
    gpa: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    keys: ?[]const expr_ast.Expr,
    filter: ?filter_ast.Filter,
    /// Per field: its position among the parsed columns.
    lookup: []const ?usize,
    evaluated: []const bool,
    matched: std.atomic.Value(u64) = .init(0),
    /// Scalar: each chunk's states, and the merged ones.
    chunk_accs: [][]expr_agg.Accumulator,
    accs: []expr_agg.Accumulator,
    /// Grouped: each chunk's table, and the merged one.
    tables: []expr_agg.GroupTable,
    merged: *expr_agg.GroupTable,
    done: []std.atomic.Value(bool),
    next: std.atomic.Value(usize) = .init(0),
    ahead: usize,
    lock: std.Io.Mutex = .init,
    failed_from: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    moved: std.atomic.Value(u32) = .init(0),
    /// The query's ledger, which a count(DISTINCT)'s set spills against, and the charge string min/max winners draw.
    ledger: *core_memory.SharedBudget,
    winners: *expr_agg.WinnerCharge,
    /// Per chunk, an error that stopped it other than a refusal of its text (`Source.failures` holds those).
    errs: []?anyerror,
    /// The chunks' scratch arenas, on `scratch` (the ledger's): every one made, and those free for the next chunk.
    scratch: std.mem.Allocator,
    pool_lock: std.Io.Mutex = .init,
    pool: std.ArrayList(*std.heap.ArenaAllocator) = .empty,
    all_arenas: std.ArrayList(*std.heap.ArenaAllocator) = .empty,
    /// Worker time, summed: each chunk's whole (`produce`), and its filter and fold (with the merge) within it.
    chunk_ns: std.atomic.Value(u64) = .init(0),
    filter_ns: std.atomic.Value(u64) = .init(0),
    fold_ns: std.atomic.Value(u64) = .init(0),

    fn awaitTurn(f: *Fold, b: usize) bool {
        while (true) {
            const seen = f.moved.load(.acquire);
            if (b >= f.failed_from.load(.acquire)) return false;
            if (b < f.next.load(.acquire) + f.ahead) return true;
            blocking.io.futexWaitUncancelable(u32, &f.moved.raw, seen);
        }
    }

    fn failFrom(f: *Fold, b: usize) void {
        _ = f.failed_from.fetchMin(b, .acq_rel);
        f.wake();
    }

    fn wake(f: *Fold) void {
        _ = f.moved.fetchAdd(1, .release);
        blocking.io.futexWake(u32, &f.moved.raw, std.math.maxInt(u32));
    }

    /// Chunk `b`'s states are complete: merge them, and any finished after them, once every earlier chunk has merged.
    fn finish(f: *Fold, b: usize) !void {
        f.done[b].store(true, .release);
        f.lock.lockUncancelable(blocking.io);
        defer f.lock.unlock(blocking.io);
        var n = f.next.load(.monotonic);
        while (n < f.done.len and f.done[n].load(.acquire)) : (n += 1) {
            if (f.keys == null) {
                for (f.accs, f.chunk_accs[n]) |*d, *s| {
                    const before = ownedWinner(d.*) +| ownedWinner(s.*);
                    try owned_merge.mergeOwnedAccumulator(d, s, f.gpa);
                    try f.winners.adjust(before, ownedWinner(d.*) +| ownedWinner(s.*));
                }
                owned_merge.freeOwnedAccumulatorStrings(f.gpa, f.chunk_accs[n]);
                f.gpa.free(f.chunk_accs[n]);
                f.chunk_accs[n] = &.{};
            } else {
                const src = &f.tables[n];
                const len = f.calls.len;
                for (0..src.groupCount()) |g| {
                    const id = try f.merged.getOrInsert(src.keyAt(g), f.calls, src.first.items[g]);
                    const dst = f.merged.accumulators.items[@as(usize, id) * len ..][0..len];
                    for (dst, src.accumulators.items[g * len ..][0..len]) |*d, *sa| {
                        const before = ownedWinner(d.*) +| ownedWinner(sa.*);
                        try expr_agg.mergePersistent(d, sa, f.gpa, owned_merge.mergeOwnedAccumulator);
                        try f.winners.adjust(before, ownedWinner(d.*) +| ownedWinner(sa.*));
                    }
                }
                src.deinit();
                src.* = expr_agg.GroupTable.init(f.gpa, 0);
            }
            f.next.store(n + 1, .release);
            f.wake();
        }
    }

    /// Chunk `i`'s parsed fields (`Source.Fold`), on `a`, freed when it returns.
    fn foldErased(ctx: *anyopaque, p: *const Plan, parsed: *const consumer.OutputAggregator, a: std.mem.Allocator, i: usize) anyerror!void {
        const f: *Fold = @ptrCast(@alignCast(ctx));
        const cb = try csv_write.ChunkBatch.of(p, parsed, f.lookup, f.evaluated, true, a);
        const t_filter = nowMonoNs();
        var batch: *const filter_eval.Batch = &cb.batch;
        const rows = parsed.num_rows;
        var sel = try filter_selection.SelectionVector.init(a, rows);
        if (f.filter) |flt| try filter_eval.evaluate(flt, batch, &sel, f.lookup, a);
        const t_fold = nowMonoNs();
        _ = f.filter_ns.fetchAdd(@intCast(t_fold - t_filter), .monotonic);
        _ = f.matched.fetchAdd(sel.count(), .monotonic);
        if (f.keys) |keys| {
            // The grouped fold reads values: a string column parsed as dictionary codes alone is expanded first, as a
            // row group's is (`consumer.scanRGForAgg`).
            const cols = try a.dupe(filter_eval.Batch.Column, batch.cols);
            for (cols) |*c| if (switch (c.*) {
                inline else => |x| x.codesOnly(),
            }) {
                c.* = try agg_coded.materialize(a, c.*);
            };
            const flat: filter_eval.Batch = .{ .cols = cols, .num_rows = batch.num_rows };
            batch = &flat;
            const key_cols = try a.alloc(filter_eval.Batch.Column, keys.len);
            const restricted = batch.restrictedTo(&sel);
            for (keys, key_cols) |k, *c| c.* = try expr_eval.evalGroupKeyExpr(a, &restricted, f.lookup, k);
            var resolver = expr_agg.GroupKeyResolver.init(key_cols);
            defer resolver.deinit(a);
            const ids = try a.alloc(u32, rows);
            @memset(ids, 0);
            const table = &f.tables[i];
            const pos = expr_agg.firstPosition(i, 0);
            const fold_ctx: expr_agg.FoldContext = .{ .winner_charge = f.winners };
            if (try denseIds(a, keys, cb.batch.cols, f.lookup, key_cols, &sel, &resolver, table, f.calls, pos, ids)) {
                for (f.calls, 0..) |_, ci| try expr_agg.updateTableGroupedWithContext(a, f.gpa, table, ids, ci, f.calls, batch, f.lookup, &sel, fold_ctx);
            } else {
                // Gather the kept rows first only when that writes fewer bytes than folding under the mask reads past
                // them: per chunk, from its kept count and its columns' widths.
                const kept = sel.count();
                const skipped_reads = (rows - kept) *| rowBytes(batch.cols);
                const gather_writes = kept *| (rowBytes(batch.cols) +| rowBytes(key_cols));
                try consumer.foldGroupedBatch(a, f.gpa, &resolver, table, f.calls, key_cols, batch, f.lookup, &sel, ids, pos, fold_ctx, gather_writes < skipped_reads);
            }
        } else {
            const accs = try f.gpa.alloc(expr_agg.Accumulator, f.calls.len);
            for (accs, f.calls) |*acc, call| acc.* = initAcc(call, f.ledger);
            f.chunk_accs[i] = accs;
            const handled = try a.alloc(bool, f.calls.len);
            @memset(handled, false);
            if (!try expr_agg.updateSharedInteger(a, f.calls, accs, handled, batch, f.lookup, &sel)) {
                for (f.calls, accs) |call, *acc| try expr_agg.updateOneWithCharge(a, f.gpa, acc, call, batch, f.lookup, &sel, f.winners);
            }
        }
        try f.finish(i);
        _ = f.fold_ns.fetchAdd(@intCast(nowMonoNs() - t_fold), .monotonic);
    }

    /// A worker's scratch arena for a chunk's parse: one returned by an earlier chunk, reset, or a new one.
    fn take(ctx: *anyopaque) anyerror!*std.heap.ArenaAllocator {
        const f: *Fold = @ptrCast(@alignCast(ctx));
        {
            f.pool_lock.lockUncancelable(blocking.io);
            defer f.pool_lock.unlock(blocking.io);
            if (f.pool.pop()) |ar| return ar;
        }
        const ar = try f.gpa.create(std.heap.ArenaAllocator);
        ar.* = .init(f.scratch);
        f.pool_lock.lockUncancelable(blocking.io);
        defer f.pool_lock.unlock(blocking.io);
        f.all_arenas.append(f.gpa, ar) catch |e| {
            ar.deinit();
            f.gpa.destroy(ar);
            return e;
        };
        return ar;
    }

    /// Back to the pool, its pages kept (`retain_capacity`): the next chunk parses into them without faulting.
    fn give(ctx: *anyopaque, ar: *std.heap.ArenaAllocator) void {
        const f: *Fold = @ptrCast(@alignCast(ctx));
        _ = ar.reset(.retain_capacity);
        f.pool_lock.lockUncancelable(blocking.io);
        defer f.pool_lock.unlock(blocking.io);
        // `all_arenas` holds it either way; a full pool only means the next chunk makes another.
        f.pool.append(f.gpa, ar) catch {};
    }

    fn deinit(f: *Fold) void {
        for (f.all_arenas.items) |ar| {
            ar.deinit();
            f.gpa.destroy(ar);
        }
        f.all_arenas.deinit(f.gpa);
        f.pool.deinit(f.gpa);
        for (f.chunk_accs) |accs| {
            owned_merge.freeOwnedAccumulatorStrings(f.gpa, accs);
            f.gpa.free(accs);
        }
        for (f.tables) |*t| t.deinit();
    }
};

/// The bytes a row of `c` takes: its value, and a DECIMAL view's exact integer beside it.
fn colBytes(c: filter_eval.Batch.Column) usize {
    return switch (c) {
        .f64 => |x| 8 + @as(usize, if (x.unscaled) |u| switch (u) {
            .i32 => 4,
            .i64 => 8,
            .i128 => 16,
        } else 0),
        inline else => |x| @sizeOf(std.meta.Elem(@TypeOf(x.values))),
    };
}

fn rowBytes(cols: []const filter_eval.Batch.Column) usize {
    var n: usize = 0;
    for (cols) |c| n +|= colBytes(c);
    return n;
}

/// Group ids without a key serialized or hashed per row: when every key is a plain column the parse coded against
/// its chunk's dictionary, and their code combinations (each dictionary and NULL) are no more than the chunk's rows,
/// each combination is resolved once, at its first row, and every row takes its id from the combination's slot.
/// False, nothing resolved, when the keys are not that.
fn denseIds(
    a: std.mem.Allocator,
    keys: []const expr_ast.Expr,
    parsed: []const filter_eval.Batch.Column,
    lookup: []const ?usize,
    key_cols: []const filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    resolver: *expr_agg.GroupKeyResolver,
    table: *expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    pos: u64,
    ids: []u32,
) !bool {
    if (table.sketch != null or table.countOnly()) return false;
    const codes = try a.alloc([]const u32, keys.len);
    const radix = try a.alloc(usize, keys.len);
    const stride = try a.alloc(usize, keys.len);
    var total: usize = 1;
    for (keys, codes, radix, stride) |k, *c, *r, *st| {
        if (k != .col_ref) return false;
        const at = lookup[k.col_ref.col_idx] orelse return false;
        if (parsed[at] != .string) return false;
        const s = parsed[at].string;
        c.* = s.codes orelse return false;
        r.* = s.dict.len + 1; // the last: NULL
        st.* = total;
        total = std.math.mul(usize, total, r.*) catch return false;
        if (total > sel.len) return false;
    }
    const slot = try a.alloc(u32, total);
    @memset(slot, std.math.maxInt(u32));
    for (0..sel.len) |row| if (sel.isActive(row)) {
        var combo: usize = 0;
        for (codes, radix, stride, key_cols) |c, r, st, kc| {
            combo += (if (expr_agg.isRowNull(kc, row)) r - 1 else c[row]) * st;
        }
        if (slot[combo] == std.math.maxInt(u32)) slot[combo] = try resolver.resolve(a, table, key_cols, row, calls, pos);
        ids[row] = slot[combo];
    };
    return true;
}

/// A call's empty state. A count(DISTINCT)'s set spills against `ledger`, as a Parquet scan's does
/// (`runMultiAggregate`): a set past `--max-memory` is refused, not grown.
fn initAcc(call: expr_agg.AggCall, ledger: *core_memory.SharedBudget) expr_agg.Accumulator {
    const acc: expr_agg.Accumulator = .init(call);
    return if (acc == .distinct) .{ .distinct = .initEmpty(ledger) } else acc;
}

/// The bytes a string min/max winner owns, as `WinnerCharge` drew them; a merge gives back the losers'.
fn ownedWinner(acc: expr_agg.Accumulator) usize {
    return switch (acc) {
        .min_bytes, .max_bytes => |v| if (v) |s| expr_agg.winnerBytes(s.len) else 0,
        else => 0,
    };
}

/// The query bound again at each DECIMAL's data precision, as the footer of the Parquet this file writes to carries it
/// (`csv_write.run`), once the fold has seen every value: an avg's extra digits (`avgDec`) and a min/max's type follow
/// it, as over that Parquet. `b` itself when no precision moved, or when a call's state would not read the same.
fn atDataPrecision(a: std.mem.Allocator, args: QueryArgs, b: *const Bound, p: *const Plan, source: *const Source) !Bound {
    const items = try a.dupe(schema.SchemaElement, b.meta.schema.items);
    var moved = false;
    for (p.slot, 0..) |slot, ci| if (slot) |o| {
        const t = p.types[o];
        if (t.kind != .decimal) continue;
        var max: u128 = 0;
        for (0..source.count) |i| max = @max(max, source.max_abs[i * p.names.len + o]);
        const prec = csv_write.decimalPrecisionOf(t, p.pinned[o], max);
        const e = &items[ci + 1];
        if (e.precision == prec) continue;
        e.precision = prec;
        e.logical_type = .{ .DECIMAL = .{ .scale = t.scale, .precision = prec } };
        moved = true;
    };
    if (!moved) return b.*;
    const meta = try a.create(schema.FileMetaData);
    meta.* = b.meta.*;
    meta.schema = .fromOwnedSlice(items);
    const again = try Bound.ofMeta(a, args, b.fetch.len, meta);
    if (again.q.calls.len != b.q.calls.len) return b.*;
    for (again.q.calls, b.q.calls) |x, y| if (x.func != y.func or x.result != y.result)
        return b.*;
    return again;
}

fn foldOnce(
    ctx: Context,
    pass: std.mem.Allocator,
    args: QueryArgs,
    path: []const u8,
    b: *const Bound,
    p: *const Plan,
    chunk: usize,
    workers: usize,
    mapped: local_fs.Mapped,
    report: *csv_write.Report,
    t_open: i64,
) !AggResult {
    const gpa = ctx.gpa;
    var t: engine_types.Timings = .{};
    t.read_ns = @intCast(nowMonoNs() - t_open);
    var ledger: core_memory.SharedBudget = .{ .limit = args.max_memory };
    defer if (args.profiler) |pr| pr.noteLedger(ledger.peak.load(.monotonic), args.max_memory);
    var scratch_charge: core_memory.Charged = .{ .child = gpa, .budget = &ledger };
    var winners: expr_agg.WinnerCharge = .{ .ledger = &ledger };

    var source = try Source.init(pass, p, chunk);
    source.opts = args.csv;
    report.chunks = source.count;
    const n = b.meta.schema.items.len - 1;
    const lookup = try pass.alloc(?usize, n);
    for (lookup, p.slot) |*l, s| l.* = if (s) |o| o else null;
    const calls = b.q.calls;
    const keys = b.q.keys;
    const fixed_stride: u8 = if (keys) |ks| expr_agg.fixedGroupStride(ks) orelse 0 else 0;
    var merged = expr_agg.GroupTable.init(gpa, 0);
    merged.shared = &ledger;
    merged.fixed_stride = fixed_stride;
    defer merged.deinit();
    const tables = try pass.alloc(expr_agg.GroupTable, if (keys != null) source.count else 0);
    for (tables) |*gt| {
        gt.* = expr_agg.GroupTable.init(gpa, 0);
        gt.shared = &ledger;
        gt.fixed_stride = fixed_stride;
    }
    const chunk_accs = try pass.alloc([]expr_agg.Accumulator, if (keys == null) source.count else 0);
    @memset(chunk_accs, &.{});
    const accs = try pass.alloc(expr_agg.Accumulator, if (keys == null) calls.len else 0);
    for (accs, 0..) |*acc, ci| acc.* = initAcc(calls[ci], &ledger);
    var accs_owned = true;
    defer if (accs_owned) owned_merge.freeOwnedAccumulatorStrings(gpa, accs);
    const done = try pass.alloc(std.atomic.Value(bool), source.count);
    @memset(done, .init(false));
    const errs = try pass.alloc(?anyerror, source.count);
    @memset(errs, null);
    // Every field a chunk parses is one its calls, keys or filter read: each DECIMAL gets its scaled view.
    const evaluated = try pass.alloc(bool, n);
    for (evaluated, lookup) |*e, l| e.* = l != null;
    var fold: Fold = .{
        .gpa = gpa,
        .calls = calls,
        .keys = keys,
        .filter = b.q.filter,
        .lookup = lookup,
        .evaluated = evaluated,
        .chunk_accs = chunk_accs,
        .accs = accs,
        .tables = tables,
        .merged = &merged,
        .done = done,
        .ahead = @max(1, workers),
        .ledger = &ledger,
        .winners = &winners,
        .errs = errs,
        .scratch = scratch_charge.allocator(),
    };
    defer fold.deinit();
    source.fold = .{ .ctx = &fold, .take = Fold.take, .give = Fold.give, .fold = Fold.foldErased };
    // A cold file is read ahead of the parse through the local reader, as a CSV write reads it (`startReader`); a
    // resident one through its mapping, each chunk's pages dropped from it once parsed (`dropBehind`).
    const reader = try csv_write.startReader(ctx, pass, args, &source, mapped, &ledger, report);
    defer if (reader) |rd| rd.destroy();

    const t_scan = nowMonoNs();
    {
        var threaded: std.Io.Threaded = .init(gpa, .{ .async_limit = .limited(@max(1, workers)) });
        defer threaded.deinit();
        const io = threaded.io();
        const Task = struct {
            // Each chunk counts its own quotes as it starts (`Source.records`); chunks start in order, so a chunk
            // waits only on those already counting.
            // One task per worker, each claiming the next chunk in order: no chunk waits on another's dispatch, and
            // none runs on the dispatching thread while workers sit idle.
            fn worker(s: *Source, f: *Fold, t_io: std.Io, cursor: *std.atomic.Value(usize)) void {
                while (true) {
                    const i = cursor.fetchAdd(1, .monotonic);
                    if (i >= s.count or !f.awaitTurn(i)) return;
                    const t0 = nowMonoNs();
                    defer _ = f.chunk_ns.fetchAdd(@intCast(nowMonoNs() - t0), .monotonic);
                    defer if (s.gate) |g| g.done(g.ctx, i, 1) else dropBehind(s, i);
                    s.produce(t_io, null, i) catch |e| {
                        if (!s.failed[i]) f.errs[i] = e;
                        f.failFrom(i);
                    };
                }
            }
        };
        var cursor: std.atomic.Value(usize) = .init(0);
        var group: std.Io.Group = .init;
        for (0..@min(@max(1, workers), source.count)) |_| group.async(io, Task.worker, .{ &source, &fold, io, &cursor });
        group.await(io) catch {};
    }
    t.decode_wall_ns = @intCast(nowMonoNs() - t_scan);
    // Summed over the workers, as a Parquet scan's are (`--summary=json` worker_ms): parsing a chunk (framing, typing and
    // its DECIMAL views) as decode, the WHERE as filter, and the fold with its ordered merge as fold.
    t.core.eval_ns = fold.filter_ns.load(.monotonic);
    t.core.encode_ns = fold.fold_ns.load(.monotonic);
    t.core.decode_ns = fold.chunk_ns.load(.monotonic) -| (t.core.eval_ns +| t.core.encode_ns);
    // The earliest chunk to stop says why, a refusal of its text or another error.
    for (0..source.count) |i| {
        if (source.failed[i]) {
            report.failure = source.failures[i];
            return switch (source.failures[i].kind) {
                .malformed => error.CsvMalformed,
                .field_count => error.CsvFieldCount,
                .type_changed => error.CsvTypeChanged,
                .pinned_type => error.CsvPinnedType,
                .utf16 => error.CsvUtf16,
                .empty => error.CsvEmpty,
            };
        }
        if (errs[i]) |e| return scratch_charge.refusal(e);
    }
    // The fold has merged every chunk: what is left is the answer. As past a Parquet scan (`runMultiAggregate`), it
    // may hold the ceiling's slack past the limit (`memory.finalize_allowance`), so an answer of groups the fold
    // admitted never refuses the query; the slack is taken back before the answer is encoded or printed, which adds
    // its own (`writeAggregateParquet`).
    ledger.limit += core_memory.finalize_allowance;
    const ab = try atDataPrecision(pass, args, b, p, &source);
    const out_calls = ab.q.calls;

    var output: scan.AggOutput = .{};
    errdefer output.deinit(gpa);
    var group_col_types: ?[]const scan.GroupColType = null;
    if (ab.q.keys) |ks| {
        const outputs = ab.q.outputs.?;
        const key_types = try pass.alloc(key_decode.KeyType, ks.len);
        for (ks, key_types) |k, *kt| kt.* = key_decode.keyTypeFromExpr(k);
        const col_types = try pass.alloc(scan.GroupColType, outputs.len);
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
                .source = if (out.derived != null) null else if (ks[k_idx] == .col_ref)
                    binding.leafSchemaElem(ab.meta, ks[k_idx].col_ref.col_idx)
                else
                    owner_support.computedAnswer(ks[k_idx]),
            },
            .agg => |a_idx| scan.GroupColType.ofAgg(out_calls[a_idx], ab.q.sources[a_idx]),
        };
        group_col_types = col_types;
        const slots = try pass.alloc(MergedSlot, merged.groupCount());
        for (slots, 0..) |*s, g| s.* = .{ .key = merged.keyAt(g), .first = merged.first.items[g], .idx = g };
        if (!args.unordered) try slot_sort.sortSlotsByFirst(pass, slots, @max(1, workers));
        var row_src: scan_rows.RowSource = .{
            .slots = slots,
            .outputs = outputs,
            .calls = out_calls,
            .key_types = key_types,
            .key_exprs = ks,
            .accs = merged.accumulators.items,
            .folded = false,
            .single = &merged,
            .fixed_keys = merged.fixed_stride != 0,
        };
        const kept = scan_rows.pruneHaving(&row_src, slots, args.result_order.having);
        row_src.slots = slots[0..kept];
        // The answer is drawn from the ledger before it is built, beside the merged table it is built from, as a
        // Parquet aggregate's is (`runMultiAggregate`), within the finalize allowance added above, and stays drawn while
        // the query holds it. A group count past `--max-memory` is refused while the fold admits its groups.
        const answer_bytes = scan_rows.answerBytesParallel(row_src.slots, col_types, key_types, false, @max(1, workers)) +|
            row_src.winnerStringBytes();
        _ = ledger.drawExact(answer_bytes) orelse return error.ExceededMemoryBudget;
        const rows = try gpa.alloc([]const output_values.AggValue, kept);
        @memset(rows, &.{});
        output.group_rows = rows;
        try scan_rows.buildGroupRows(gpa, &output.slab, row_src, rows, @max(1, workers), null);
        const cols = try gpa.alloc([]const u8, outputs.len);
        @memset(cols, "");
        output.group_cols = cols;
        for (outputs, cols) |out, *col| col.* = try gpa.dupe(u8, out.name);
    } else {
        const items = try gpa.alloc(output_values.AggOutputItem, out_calls.len);
        @memset(items, .{ .alias = "", .value = .null_val });
        output.aggs = items;
        for (out_calls, items, accs) |call, *item, acc| {
            item.alias = try gpa.dupe(u8, call.alias);
            item.value = try scan.materializeOne(gpa, call, acc);
        }
    }

    const rows_in: i64 = @intCast(source.rows.load(.monotonic));
    var r: scan.MultiAggResult = .{
        .files_in = 1,
        .rows_in = rows_in,
        .rows_scanned = rows_in,
        .rows_matched = @intCast(fold.matched.load(.monotonic)),
        .bytes_in = mapped.bytes.len,
        .row_groups_in = 0,
        .row_groups_pruned = 0,
        .cols_stat_pruned = 0,
        .timings = .{},
        .output = output,
        .state = .{
            .accumulators = accs,
            .agg_calls = out_calls,
            .group_col_types = group_col_types,
            .agg_sources = ab.q.sources,
        },
    };
    accs_owned = false;
    const Check = struct {
        mapped: local_fs.Mapped,
        path: []const u8,
        fn check(c: *const anyopaque) anyerror!void {
            const self: *const @This() = @ptrCast(@alignCast(c));
            try self.mapped.intact(self.path);
        }
    };
    const check: Check = .{ .mapped = mapped, .path = path };
    ledger.limit -= core_memory.finalize_allowance;
    return aggregate.answerAggregate(ctx, pass, args, &r, &ledger, &t, .{}, .{ .ctx = &check, .check = Check.check }, null);
}

/// Chunk `i`'s own pages, dropped from the mapping once it is parsed: they stay in the page cache, so a later read (a
/// neighbour's straddling record, a restart's pass) faults them back as they were. The pages it shares with its
/// neighbours stay. One call a chunk; without it the process maps every page of the file until it exits.
fn dropBehind(s: *const Source, i: usize) void {
    if (builtin.os.tag != .linux) return;
    const page = std.heap.pageSize();
    const base = @intFromPtr(s.plan.bytes.ptr);
    const lo = std.mem.alignForward(usize, base + s.bound(i), page);
    const hi = std.mem.alignBackward(usize, base + s.bound(i + 1), page);
    if (hi > lo) _ = std.os.linux.madvise(@ptrFromInt(lo), hi - lo, std.os.linux.MADV.DONTNEED);
}
