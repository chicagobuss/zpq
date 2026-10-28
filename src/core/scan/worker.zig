const row_subgroups = @import("../row_subgroups.zig");
const std = @import("std");
const testing = std.testing;
const consumer = @import("../consumer.zig");
const core_memory = @import("../memory.zig");
const expr_agg = @import("../expr/agg.zig");
const agg_plan = @import("../agg_plan.zig");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_prune = @import("../filter/prune.zig");
const huge_pages = @import("../huge_pages.zig");
const spawn_util = @import("../spawn.zig");
const blocking = @import("../blocking.zig");
const profile = @import("../../profile.zig");
const WORKER_STACK_SIZE = @import("types.zig").WORKER_STACK_SIZE;
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const FailureProbe = worker_contracts.FailureProbe;
const workerErrorRanks = worker_contracts.workerErrorRanks;
const pairsPayRuntime = @import("pairs_policy.zig").pairsPayRuntime;

/// Address space each scan worker reserves for its decode memory; only what a row group touches is committed, and a
/// worker needing more spills to the general allocator.
pub const WORKER_REGION_RESERVE: usize = 1 << 30;

/// Only on failure: count/discovery/admit passes may have omitted arguments or entire work items. Replay the
/// query's ordinary full projection in input/RG order, including those omissions, after all scan storage is gone.
pub fn canonicalFailurePrefix(a: std.mem.Allocator, p: FailureProbe.Context) @import("../decode_order.zig").Failure {
    for (p.metas[0 .. p.file + 1], 0..) |*meta, fi| {
        // Streams already checked the full projection before returning their bytes; optional passes refuse them.
        if (!p.inputs[fi].rescannable()) continue;
        const end = if (fi == p.file) p.rg + 1 else meta.row_groups.items.len;
        for (meta.row_groups.items[0..end]) |*rg| {
            const decision = p.plan.decide(rg, meta);
            if (decision == .skip) continue;
            var in = p.in;
            in.rg = rg;
            in.meta = meta;
            in.src = .{ .bytes = p.inputs[fi].plan_bytes orelse p.inputs[fi].bytes, .byte_origin = 0 };
            in.filter = p.plan.filterFor(decision);
            in.read = p.plan.columns.forRowGroup(decision);
            in.error_read = null;
            if (consumer.probeDecodeFailure(a, in, p.options, p.calls, p.scalar) catch null) |found| return found;
        }
    }
    return p.original;
}

const WorkerFailureKey = struct {
    file: usize,
    rg: usize,
    unit: u1,
    row: usize,
    projection: usize,
    column: usize,

    fn of(w: *const Worker) WorkerFailureKey {
        const unknown = std.math.maxInt(usize);
        const refusal = w.err.? == error.ExceededMemoryBudget;
        var key: WorkerFailureKey = .{
            // An unlocated refusal must replay: it may hide a failure before every known site.
            .file = if (refusal) 0 else unknown,
            .rg = if (refusal) 0 else unknown,
            .unit = if (refusal) 0 else 1,
            .row = if (refusal) 0 else unknown,
            .projection = if (refusal) 0 else unknown,
            .column = if (refusal) 0 else unknown,
        };
        if (w.failure_work) |i| {
            key.file = w.work[i].file;
            key.rg = w.work[i].rg;
        }
        if (consumer.isDecodeError(w.err.?)) if (w.failure) |site| {
            if (w.failure_work == null) outer: for (w.metas, 0..) |meta, fi| {
                for (meta.row_groups.items, 0..) |*rg, ri| if (rg == site.rg) {
                    key.file = fi;
                    key.rg = ri;
                    break :outer;
                };
            };
            key.row = site.first_row;
            key.projection = site.projection;
            key.column = site.column;
        };
        return key;
    }

    fn less(a: WorkerFailureKey, b: WorkerFailureKey) bool {
        inline for (.{ "file", "rg", "unit", "row", "projection", "column" }) |field| {
            if (@field(a, field) != @field(b, field)) return @field(a, field) < @field(b, field);
        }
        return false;
    }
};

/// Collect every failure, including refusals which must replay before a later decode error can be final.
pub fn firstWorkerFailure(
    workers: []const Worker,
    probe: ?*FailureProbe,
) ?struct { err: anyerror, site: ?consumer.DecodeSite } {
    var best: ?usize = null;
    var best_key: WorkerFailureKey = undefined;
    for (workers, 0..) |w, wi| if (w.err) |err| {
        const key = WorkerFailureKey.of(&w);
        if (best == null or key.less(best_key) or
            (!best_key.less(key) and std.mem.order(u8, @errorName(err), @errorName(workers[best.?].err.?)) == .lt))
        {
            best = wi;
            best_key = key;
        }
    };
    if (best) |wi| {
        const w = &workers[wi];
        const site = if (consumer.isDecodeError(w.err.?)) w.failure else null;
        if (probe) |p| {
            p.workers = workers;
            p.context = null;
            if (site != null) p.capture(workers, wi);
        }
        return .{ .err = w.err.?, .site = site };
    }
    return null;
}

pub fn resetAttemptFlags(w: *Worker) void {
    // These classify this attempt only. Retained mappings and charged allocators survive optional phases.
    w.retired_decode_held = false;
    if (w.region) |*r| r.refused = false;
    if (w.scratch_region) |*r| r.refused = false;
    if (w.charged) |*c| {
        c.refused.store(false, .monotonic);
        c.drew.store(false, .monotonic);
    }
}

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
    consumer.setDecodeFailure(null);
    w.failure_work = null;
    resetAttemptFlags(w);
    defer if (w.retire_after_run) {
        // Keep refusal classification after returning the actual pages and their ledger charges.
        w.retired_decode_held = decodeHeld(w);
        const release = profile.Span.begin(prof);
        retireRegions(w);
        release.end(.worker_release, profile.args(.{w.index}));
    };
    workerRunErr(w, prof, &items) catch |e| {
        w.err = workerRefusal(w, e);
        w.failure = consumer.decodeFailure();
        if (consumer.isDecodeError(e) and w.failure != null and w.failed_gate != null) {
            const probe = w.failure_probe.?;
            retireRegions(w);
            probe.lock.lockUncancelable(blocking.io);
            defer probe.lock.unlock(blocking.io);
            const p = FailureProbe.contextOf(w);
            var charged: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = &probe.stream_budget };
            const failure = consumer.canonicalDecodeFailure(
                charged.allocator(),
                p.in,
                p.options,
                p.calls,
                p.scalar,
                p.original,
            );
            w.err = failure.err;
            w.failure = failure.site;
            const gate = w.failed_gate.?;
            gate.done(gate.ctx, w.failed_rg, w.items_per_rg);
            w.failed_gate = null;
        }
        if (w.block_merge) |m| {
            if (w.failure_work) |wi| {
                // A lower block may not have entered awaitTurn yet. It must still decode: a later block's
                // refusal or decode failure cannot hide an earlier row group's failure.
                const block = for (w.blocks.?[1..], 0..) |end, b| {
                    if (wi < end) break b;
                } else 0;
                m.failFrom(block);
            } else m.fail();
        }
    };
    // Holding a drawn block until teardown let a barely-grouping worker strand a small budget and fail the worker
    // doing the real work. (Ordered blocks release theirs as each finishes.)
    if (w.blocks == null and w.group_tables.len > 0) w.group_tables[w.index].releaseSlack();
}

pub fn ownedWinnerBytes(accs: []const expr_agg.Accumulator) usize {
    var n: usize = 0;
    for (accs) |acc| switch (acc) {
        .min_bytes, .max_bytes => |s| if (s) |v| {
            n += expr_agg.winnerBytes(v.len);
        },
        else => {},
    };
    return n;
}

/// Whether the worker drew decode memory from the query's ledger, past its region's allowance, or was refused it.
pub fn decodeHeld(w: *const Worker) bool {
    if (w.retired_decode_held) return true;
    if (w.region) |r| if (r.charged > 0 or r.spilled > 0 or r.refused) return true;
    if (w.scratch_region) |r| if (r.charged > 0 or r.spilled > 0 or r.refused) return true;
    if (w.charged) |c| return c.drew.load(.monotonic) or c.refused.load(.monotonic);
    return false;
}

/// `err`, or `error.ExceededMemoryBudget` when it is the out-of-memory a refused draw from the ledger raised.
pub fn workerRefusal(w: *const Worker, err: anyerror) anyerror {
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
        const pair = huge_pages.takePair(w.gpa, WORKER_REGION_RESERVE);
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
    // Cost hints may decline on a refused allocation. Their flags must not turn a subsequent host OOM into
    // a ledger refusal; the storage is still charged to exactly the same ledger. Fresh for every attempt.
    var unbounded_planning: core_memory.SharedBudget = .{ .limit = std.math.maxInt(usize) };
    var planning_charge: core_memory.Charged = .{ .child = std.heap.page_allocator, .budget = w.ledger orelse &unbounded_planning };
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
        if (table) |gt| if (gt.discoveryDone(expr_agg.firstPosition(idx, 0))) continue;
        errdefer w.failure_work = idx;
        items.* += 1;
        const item = w.work[idx];
        if (@import("builtin").is_test and item.subgroup != null) _ = row_subgroups.test_ranges.fetchAdd(1, .monotonic);
        const gate = w.inputs[item.file].gate;
        defer if (gate) |g| if (w.failed_gate == null) {
            // The producer owns compressed bytes until every range has finished; its contract counts RG items.
            const last = if (item.subgroup) |s| s.group.unfinished.fetchSub(1, .acq_rel) == 1 else true;
            if (last) g.done(g.ctx, item.rg, w.items_per_rg);
        };
        const meta = &w.metas[item.file];
        const rg = &meta.row_groups.items[item.rg];
        // File bytes are shared read-only across workers (one mmap / one
        // in-memory buffer); concurrent ranged reads are safe.
        const rg_src: consumer.RGSrc = .{ .bytes = w.inputs[item.file].bytes, .byte_origin = 0 };
        if (item.agg_start == 0 and (item.subgroup == null or item.subgroup.?.rows.lo == 0)) {
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
            .full_match => if (item.agg_start == 0 and (item.subgroup == null or item.subgroup.?.rows.lo == 0)) {
                w.rgs_full_match += 1;
            },
            .filter => {},
        }
        if (item.agg_start == 0) {
            w.rows_scanned +|= if (item.subgroup) |s| @intCast(s.rows.hi - s.rows.lo) else rg.num_rows;
        }
        if (gate) |g| try g.wait(g.ctx, item.rg);
        const sub_agg_calls = w.agg_calls[item.agg_start .. item.agg_start + item.agg_len];
        // A sketch's count pass reads only the filter's columns and the keys.
        const columns = if (table != null and table.?.countOnly()) w.count_columns.? else item.columns;
        // A census notes the item each row was read in, and how many rows each item gave it.
        const census: ?*expr_agg.Census = if (table) |gt| if (gt.sketch) |s| s.census else null else null;
        const recorded_before = if (census) |c| c.recorded else 0;
        defer if (census) |c| if (c.admitted == null) {
            c.item_rows[idx] = c.recorded - recorded_before;
        };
        const target: consumer.AggTarget = if (table) |gt|
            .{ .grouped = .{
                .calls = sub_agg_calls,
                .keys = w.group_by_keys.?,
                .table = gt,
                .pos_base = item.position orelse expr_agg.firstPosition(idx, 0),
                .part = w.part,
                .local = if (w.part != null) &w.local else null,
                .dense = w.dense,
                .records = w.records,
            } }
        else
            .{ .scalar = .{ .calls = sub_agg_calls, .accumulators = item.accumulators } };
        expr_agg.winner_charge = w.winners;
        if (w.records) |records| switch (records.*) {
            .selected => |arm| arm.start(w.record_starts[idx], idx),
            else => records.start(w.record_starts[idx], idx),
        };
        defer expr_agg.winner_charge = null;
        const matched = consumer.scanRGForAgg(w.gpa, .{
            .subgroup = item.subgroup,
            .rg = rg,
            .file_index = item.file,
            .rg_index = item.rg,
            .row_base = if (w.records != null) w.record_starts[idx] else 0,
            .meta = meta,
            .src = rg_src,
            .filter = w.plan.filterFor(decision),
            .read = columns.forRowGroup(decision),
            .consumed = columns.consumed,
            .preserve = columns.preserve,
            .error_ranks = .{ .ctx = w, .make = workerErrorRanks },
            .error_read = w.plan.columns.forRowGroup(decision),
            .error_calls = w.agg_calls,
        }, w.plan.stats, w.decode_options, target, .{
            .scratch = &scratch,
            .rg_arena = &rg_decode_arena,
            .timings = &w.timings,
            .budget = w.decode_budget,
            .region = region,
            .charged = if (w.charged) |*c| c.allocator() else null,
            .planning_charged = planning_charge.allocator(),
            .ledger = w.ledger,
            .expr_batch_rows = w.expr_batch_rows,
            .decode_batch_rows = w.decode_batch_rows,
            .auto_decode_batches = w.auto_decode_batches,
            .cache_hints = w.cache_hints,
            .scan_workers = w.scan_workers,
            .defer_decode_errors = w.failure_probe != null,
        }) catch |err| {
            if (consumer.isDecodeError(err)) {
                w.failed_item = idx;
                w.failed_rg = item.rg;
                if (w.failure_probe != null and w.inputs[item.file].plan_bytes == null) w.failed_gate = gate;
            }
            return err;
        };

        if (item.agg_start == 0) w.rows_matched +|= @intCast(matched);
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

pub fn releaseRegions(workers: []Worker) void {
    for (workers) |*w| retireRegions(w);
}

/// Multipass scans keep their regions until the certificate is final. Retire the remaining pairs in parallel
/// only at that barrier; returning pages after an intermediate pass would fault them back in on the next pass.
pub fn releaseRegionsParallel(workers: []Worker) void {
    if (workers.len > 1) _ = spawn_util.runTree(Worker, workers, .{ .stack_size = WORKER_STACK_SIZE }, struct {
        fn run(w: *Worker) void {
            const span = profile.Span.begin(profile.threadOf(w.profiler, .scan));
            retireRegions(w);
            span.end(.worker_release, profile.args(.{w.index}));
        }
    }.run);
    // Partial spawn failure leaves unrun contexts. Already retired pairs are NULL and cost no syscall.
    releaseRegions(workers);
}

/// Hand the worker's region pair back for the next pass or query (`huge_pages.retirePair`).
fn retireRegions(w: *Worker) void {
    if (w.region) |arena| {
        var pair: huge_pages.Region.Pair = .{ .scratch = w.scratch_region.?, .arena = arena };
        huge_pages.retirePair(&pair);
    }
    w.region = null;
    w.scratch_region = null;
}

pub fn runWorkers(workers: []Worker) void {
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

test "cost hint and previous attempt refusals never classify a later host OOM" {
    var budget: core_memory.SharedBudget = .{ .limit = 0 };
    var w: Worker = undefined;
    w.region = null;
    w.scratch_region = null;
    w.charged = .{ .child = testing.allocator, .budget = &budget };
    try testing.expectError(error.OutOfMemory, w.charged.?.allocator().alloc(u8, 1));
    try testing.expectEqual(error.ExceededMemoryBudget, workerRefusal(&w, error.OutOfMemory));
    resetAttemptFlags(&w);
    var hint: core_memory.Charged = .{ .child = testing.allocator, .budget = &budget };
    try testing.expectError(error.OutOfMemory, hint.allocator().alloc(u8, 1));
    try testing.expectEqual(error.OutOfMemory, workerRefusal(&w, error.OutOfMemory));
    try testing.expect(!decodeHeld(&w));
    // A real refusal in this attempt still classifies normally.
    try testing.expectError(error.OutOfMemory, w.charged.?.allocator().alloc(u8, 1));
    try testing.expectEqual(error.ExceededMemoryBudget, workerRefusal(&w, error.OutOfMemory));
}

test "exiting final-pass workers retire charged regions before join and preserve refusal history" {
    const gpa = testing.allocator;
    const plan: agg_plan.Plan = undefined; // Empty work never consults the query plan.
    for ([_]bool{ false, true }) |retire| {
        var budget: core_memory.SharedBudget = .{ .limit = 64 << 20 };
        var cursor: std.atomic.Value(usize) = .init(0);
        var workers: [4]Worker = undefined;
        for (&workers, 0..) |*w, i| {
            var pair = huge_pages.takePair(gpa, WORKER_REGION_RESERVE);
            pair.arena.budget = &budget;
            pair.scratch.budget = &budget;
            const pages = try pair.arena.allocator().alloc(u8, 4 << 20);
            @memset(pages, 17);
            w.* = .{
                .gpa = gpa,
                .inputs = &.{},
                .metas = &.{},
                .work = &.{},
                .cursor = &cursor,
                .agg_calls = &.{},
                .plan = &plan,
                .decode_options = .{},
                .ledger = &budget,
                .region = pair.arena,
                .scratch_region = pair.scratch,
                .retire_after_run = retire,
                .index = i,
            };
        }
        defer releaseRegions(&workers);
        try testing.expect(budget.used.load(.monotonic) > 0);
        runWorkers(&workers);
        for (&workers) |*w| {
            try testing.expect(w.err == null);
            try testing.expectEqual(retire, w.region == null);
            try testing.expectEqual(retire, w.scratch_region == null);
            try testing.expect(decodeHeld(w));
            if (retire) {
                resetAttemptFlags(w);
                try testing.expect(!decodeHeld(w));
            }
        }
        if (retire) try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        releaseRegions(&workers);
        try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    }
}

