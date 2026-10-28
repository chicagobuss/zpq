const std = @import("std");
const nowMonoNs = @import("../../clock.zig").monoNs;
const spawn_util = @import("../spawn.zig");
const schema = @import("../schema.zig");
const consumer = @import("../consumer.zig");
const agg_plan = @import("../agg_plan.zig");
const metadata = @import("../parquet/metadata.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const owner_groups = @import("../owner_groups.zig");
const membership = @import("../filter/membership.zig");
const worker_contracts = @import("worker_contracts.zig");
const Worker = worker_contracts.Worker;
const plan_types = @import("plan_types.zig");
const AggregateDispatch = plan_types.AggregateDispatch;
const scan_plan_impl = @import("plan.zig");
const keysLookUnique = scan_plan_impl.keysLookUnique;
const scan_phase = @import("phase.zig");
const restoreCounts = scan_phase.restoreCounts;
const clearWorkerErrors = scan_phase.clearWorkerErrors;
const runPhase = scan_phase.runPhase;
const scan_types = @import("types.zig");
const MultiAggArgs = scan_types.MultiAggArgs;
const SketchTopK = scan_types.SketchTopK;
const Input = scan_types.Input;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const testing = std.testing;

/// Cost quote only: stream pages are not available yet. Bound one probe column from its chunk footprint and
/// a 64-row window, never all projected columns or workers at once. False size metadata can only limit evidence.
pub fn streamProbeBytes(inputs: []const Input, metas: []const schema.FileMetaData, plan: *const agg_plan.Plan) usize {
    var largest: usize = 0;
    for (inputs, metas) |input, *meta| {
        if (input.rescannable()) continue;
        for (meta.row_groups.items) |*rg| {
            const decision = plan.decide(rg, meta);
            if (decision == .skip) continue;
            const read = plan.columns.forRowGroup(decision);
            for (read, 0..) |needed, ci| {
                if (!needed) continue;
                var unit: usize = std.heap.pageSize() * 4 +| read.len *| 16;
                if (rg.columns.items[ci].meta_data) |cm| {
                    const raw = std.math.cast(usize, @max(0, cm.total_uncompressed_size)) orelse
                        std.math.maxInt(usize);
                    const n = std.math.cast(usize, @max(0, cm.num_values)) orelse std.math.maxInt(usize);
                    // Byte windows may copy the same long dictionary string 64 times. Numeric pages retain
                    // decompression/dictionary storage and can expand levels even when values are tightly coded.
                    const copies: usize = switch (cm.type) {
                        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY, .INT96 => 64 + 12,
                        else => 4,
                    };
                    unit +|= raw *| copies +| n *| 4 +| 64 * 32;
                }
                largest = @max(largest, unit);
            }
        }
    }
    return largest;
}

pub const WinnerCharge = expr_agg.WinnerCharge;

/// Complete dense COUNT or signed SUM ranks need no losing groups when no later predicate or expression reads them.
pub fn denseLimit(args: MultiAggArgs, calls: []const expr_agg.AggCall, outputs: ?[]const BoundOutputColumn) ?usize {
    const ai = if (agg_dense.countOnly(calls)) 0 else agg_dense.sumCall(calls) orelse return null;
    const outs = outputs orelse return null;
    const order = args.result_order;
    const limit = order.limit orelse return null;
    if (limit == 0 or order.having != null or order.hidden.len != 0 or order.keys.len != 1 or
        !order.keys[0].descending or order.keys[0].column >= outs.len) return null;
    for (outs) |out| if (out.derived != null) return null;
    const source = outs[order.keys[0].column].source;
    if (source != .agg or source.agg != ai) return null;
    return std.math.cast(usize, std.math.add(u64, order.offset, limit) catch return null);
}

/// Hot membership is sampled; each hot lane is then counted completely and every other row contributes to a
/// saturating cold bound. The exact residual includes equality at the cut, preserving all secondary ordering.
fn hotCertificate(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    cfg: SketchTopK,
) !bool {
    const cert = @import("../count_certificate.zig");
    if (cfg.hot_outcome) |outcome| outcome.* = .declined;
    if (!cfg.hot) return false;
    const keys = workers[0].group_by_keys orelse return false;
    if (keys.len == 0 or keys.len > 2 or calls.len != 1 or calls[0].func != .count or calls[0].arg != null or
        calls[0].where != null or calls[0].offset != 0 or calls[0].state or
        tables[0].fixed_stride != keys.len * 9) return false;
    for (keys) |key| {
        if (key != .col_ref or key.col_ref.expr_type != .i64 or key.col_ref.unsigned_64) return false;
        if (key.col_ref.physical_type != .INT32 and key.col_ref.physical_type != .INT64) return false;
    }
    const h: usize = @intCast(order.offset +| order.limit.?);
    var unlimited: core_memory.SharedBudget = .{ .limit = std.math.maxInt(usize) };
    var charge: core_memory.Charged = .{ .child = gpa, .budget = tables[0].shared orelse &unlimited };
    const a = charge.allocator();
    const sources = a.alloc(cert.Source, workers[0].work.len) catch return false;
    defer a.free(sources);
    for (workers[0].work, sources) |item, *source| source.* = .{
        .rg = &workers[0].metas[item.file].row_groups.items[item.rg],
        .meta = &workers[0].metas[item.file],
        .src = .{ .bytes = workers[0].inputs[item.file].bytes, .byte_origin = 0 },
    };
    // A failed optional sample is never the query's error; the complete scan establishes canonical failures.
    var sample = cert.sample(
        a,
        sources,
        keys[0].col_ref.col_idx,
        if (keys.len == 2) keys[1].col_ref.col_idx else null,
        h,
        workers.len,
        workers[0].decode_options,
    ) catch {
        consumer.setDecodeFailure(null);
        return false;
    };
    defer sample.deinit(a);
    const cell_capacity = @max(16, @import("../hw.zig").l2BytesPerWorkerOr(640 << 10) / (2 * @sizeOf(u32)));
    const bits = @min(cfg.cell_bits, std.math.log2_int(usize, cell_capacity));
    const n_cells = @as(usize, 1) << @intCast(bits);
    var plan = (cert.hotPlan(a, sample, keys.len == 2, h, n_cells) catch return false) orelse return false;
    defer plan.deinit(a);
    sample.deinit(a);
    const hot = a.alloc(cert.Hot, workers.len) catch return false;
    defer a.free(hot);
    var made: usize = 0;
    defer for (hot[0..made]) |local| local.deinit(a);
    for (hot) |*local| {
        local.* = cert.Hot.init(a, &plan) catch return false;
        made += 1;
    }
    const sketches = a.alloc(expr_agg.CountSketch, workers.len) catch return false;
    defer a.free(sketches);
    var cells_made: usize = 0;
    defer for (sketches[0..cells_made]) |s| a.free(s.cells);
    for (sketches, hot) |*s, *local| {
        s.* = .{
            .cells = a.alloc(u32, n_cells) catch return false,
            .shift = @intCast(@as(u7, 64) - bits),
            .hot = local,
        };
        @memset(s.cells, 0);
        cells_made += 1;
    }
    const totals = a.alloc(u64, plan.lanes()) catch return false;
    defer a.free(totals);
    @memset(totals, 0);
    const ranked = a.alloc(u64, plan.lanes()) catch return false;
    defer a.free(ranked);
    const saved = try arena.dupe(Worker, workers);
    var accepted = false;
    defer if (!accepted) restoreCounts(workers, saved);
    for (tables, sketches) |*t, *s| t.sketch = s;
    defer for (tables) |*t| {
        t.sketch = null;
    };
    const t0 = nowMonoNs();
    try runPhase(workers, cursor);
    for (hot) |local| {
        if (local.overflow) return false;
        for (totals, local.counts) |*total, count| total.* += count;
        for (hot[0].first, local.first) |*first, ordinal| first.* = @min(first.*, ordinal);
    }
    @memcpy(ranked, totals);
    std.mem.sortUnstable(u64, ranked, {}, std.sort.desc(u64));
    const cut = ranked[h - 1];
    if (cut == 0) return false;
    const cells = sketches[0].cells;
    for (sketches[1..]) |s| for (cells, s.cells) |*cell, count| {
        cell.* +|= count;
    };
    var residual: u64 = 0;
    for (cells) |cell| if (cell == std.math.maxInt(u32)) {
        // A saturated cell is infinity, including when an exact hot score exceeds u32.
        return false;
    } else if (cell >= cut) {
        residual +|= cell;
    };
    if (cfg.trace) std.debug.print("certificate: hot={d} width={d} cut={d} residual_bound={d} count_ms={d}\n", .{
        plan.keys.len, plan.width, cut, residual, @divTrunc(nowMonoNs() - t0, 1_000_000),
    });
    // The residual counts survivors, so compare it with this pass's selected rows, excluding earlier phases.
    var selected_rows: u64 = 0;
    for (workers, saved) |w, before| selected_rows +|= @intCast(w.rows_matched - before.rows_matched);
    if (residual >= selected_rows / 2 or !cfg.certify) return false;
    if (residual > 0) {
        const admit = try a.alloc(u64, (n_cells + 63) / 64);
        defer a.free(admit);
        @memset(admit, 0);
        for (cells, 0..) |cell, i| if (cell >= cut) {
            admit[i / 64] |= @as(u64, 1) << @intCast(i % 64);
        };
        for (sketches) |*s| s.admit = admit;
        restoreCounts(workers, saved);
        try runPhase(workers, cursor);
    }
    // Each hot key is complete in one lane, even when its cold hash cell was also admitted. The residual drops
    // hot rows; materializing the complete lane once cannot double-count them.
    for (tables) |*t| t.sketch = null;
    for (totals, hot[0].first, 0..) |total, first, id| if (total != 0) {
        var image: [18]u8 = undefined;
        const g = try tables[0].getOrInsert(plan.image(id, &image), calls, first);
        tables[0].writeAcc(g, 0, 1, .{ .count = total });
    };
    if (cfg.outcome) |o| o.* = .certified;
    if (cfg.hot_outcome) |o| o.* = if (residual == 0) .certified else .residual;
    accepted = true;
    return true;
}

/// A COUNT-descending cut of `k = offset + limit` groups, grouping only the keys that can make it. The count pass
/// gives no key a group: each row adds one to its key's hash cell. The cells, summed over workers, bound every key's
/// total. The admit pass groups, exactly and with first appearance, every key of the `T` fullest cells, and drops the
/// rest. A dropped key totals at most the fullest cell not admitted, `bound`; when `bound` is below the k-th largest
/// admitted total, every group the cut keeps, ties at the cut included, is admitted, and the tables answer exactly.
/// Otherwise (or when the cells are too even for that to happen) the tables start over and the scan runs as usual.
pub fn certifiedTopK(
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
    if (!decision.distinct_census) {
        const hot = hotCertificate(gpa, arena, workers, cursor, tables, calls, order, cfg) catch |e| {
            if (e != error.ExceededMemoryBudget) return e;
            // The hot trial has released its lanes/cells and restored scan counts before replay owns the tables.
            try clearWorkerErrors(workers, error.ExceededMemoryBudget);
            clearTables(gpa, tables);
            if (cfg.outcome) |o| o.* = .fell_back;
            try runPhase(workers, cursor);
            return;
        };
        if (hot) return;
    }
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
    // A census (`identityBoundPlan`) rides along when it fits and the keys look unique (`keysLookUnique`), so
    // cells too even to bound anything need no second count pass. It fills the cells too, and stops early wherever a
    // worker's own cells already separate the cut, as the cells will then do without it.
    var dropped: std.atomic.Value(bool) = .init(false);
    // A census riding the count pass drops on the merged cells of every worker with `check_after` rows to judge.
    // Each worker judges at its share of what one worker's cells once judged alone, so the merged cells hold as many
    // rows, drawn from every worker's slice rather than one.
    var merged: expr_agg.Census.MergedCells = .{ .slots = &.{}, .quorum = 0 };
    var merged_check: u64 = cfg.census_check_rows;
    if (decision.census_allowed and decision.census_fused and !decision.distinct_census) {
        var rows: u64 = 0;
        for (workers[0].work) |item| if (item.agg_start == 0) {
            rows +|= @intCast(@max(0, workers[0].metas[item.file].row_groups.items[item.rg].num_rows));
        };
        merged.quorum = workers.len;
        merged_check = @max(1, cfg.census_check_rows / workers.len);
        // Workers that cannot all reach their check never complete the quorum: judge on fewer.
        if (rows / merged_check < workers.len) merged.quorum = @intCast(rows / merged_check);
        merged.slots = try arena.alloc([]u32, workers.len);
        for (merged.slots) |*slot| {
            slot.* = try arena.alloc(u32, n_cells);
            @memset(slot.*, 0);
        }
    }
    const censuses: []expr_agg.Census = if (decision.census_allowed) blk: {
        const item_rows = try arena.alloc(u64, workers[0].work.len);
        @memset(item_rows, 0);
        const cs = try arena.alloc(expr_agg.Census, workers.len);
        for (cs, 0..) |*c, wi| c.* = .{
            .worker = wi,
            .gpa = gpa,
            .item_rows = item_rows,
            .dropped = &dropped,
            .merged = if (merged.slots.len != 0) &merged else null,
            .cut_k = k,
            .admit_n = admit_n,
            .distinct = decision.distinct_census,
            .check_after = if (decision.distinct_census)
                @min(cfg.census_check_rows, expr_agg.Census.distinct_check_rows)
            else
                merged_check,
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
        if (!abandoned) {
            var bound_plan = try identityBoundPlan(
                gpa,
                arena,
                workers,
                tables,
                censuses,
                calls,
                order,
                a_idx,
                cfg,
                t1 - t0,
            );
            for (censuses) |*c| c.deinit();
            if (bound_plan) |*plan| {
                defer plan.deinit(gpa);
                if (try certifiedResidual(
                    gpa,
                    arena,
                    workers,
                    cursor,
                    tables,
                    sketches,
                    censuses,
                    calls,
                    order,
                    a_idx,
                    cfg,
                    plan,
                ))
                    return;
            }
        }
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
        std.debug.print("certificate: hash_identity_try={}\n", .{census_try});
    var bound_plan: ?CutBoundPlan = null;
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
        bound_plan = try identityBoundPlan(gpa, arena, workers, tables, censuses, calls, order, a_idx, cfg, t1 - t0);
    }
    for (censuses) |*c| c.deinit();
    if (viable) {
        const admit = try gpa.alloc(u64, (n_cells + 63) / 64);
        @memset(admit, 0);
        for (sum, 0..) |count, i| if (count > bound) {
            admit[i / 64] |= @as(u64, 1) << @intCast(i % 64);
        };
        bound_plan = .{ .bound = if (bound == std.math.maxInt(u32)) std.math.maxInt(u64) else bound, .cells = admit };
    }
    if (bound_plan) |*plan| {
        defer plan.deinit(gpa);
        if (try certifiedResidual(
            gpa,
            arena,
            workers,
            cursor,
            tables,
            sketches,
            censuses,
            calls,
            order,
            a_idx,
            cfg,
            plan,
        ))
            return;
    }
    for (tables) |*t| t.sketch = null;
    restoreCounts(workers, saved);
    try runPhase(workers, cursor);
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

fn censusMask(cfg: SketchTopK) u64 {
    return (@as(u64, 1) << @min(cfg.census_fp_bits, 32)) - 1;
}

/// A complete identity class seen more than once. Its row count is an upper bound, never key equality.
pub const Repeat = struct { fp: u64, count: u64 };

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
    keep: usize = std.math.maxInt(usize),
    /// The largest count of a repeated fingerprint some partition dropped; 1 when none was (a single row each).
    omitted: u64 = 1,
    out: std.ArrayList(Repeat) = .empty,
    sorted: std.ArrayList(u32) = .empty,
    table: std.ArrayList(u64) = .empty,
    /// One partition's repeats, before the most frequent `keep` are emitted.
    pending: std.ArrayList(Pending) = .empty,
    err: ?anyerror = null,

    /// A repeated fingerprint whose rows are `sorted.items[lo..hi]`.
    const Pending = struct { fp: u64, count: u64 };

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
                    starts[(e >> 24) + 1] += 1;
                };
                if (c.open[p]) |chunk| for (chunk[0..c.fill[p]]) |e| {
                    starts[(e >> 24) + 1] += 1;
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
            try t.emit(p, r.fp, r.count);
        }
        return kept.len;
    }

    fn scatter(entries: []u32, next: *[257]usize, chunk: []const u32) void {
        for (chunk) |e| {
            const b: usize = @intCast(e >> 24);
            entries[next[b]] = e;
            next[b] += 1;
        }
    }

    /// The fingerprints of `entries[lo..hi]` seen more than once. A table of its fingerprints finds them in one pass;
    /// a bucket with more than a few is sorted instead, so their rows come together and wait in `pending`. Returns
    /// how many it emitted itself.
    fn repeatsIn(t: *CensusTask, p: usize, entries: []u32, lo: usize, hi: usize) !usize {
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
            const fp: u64 = e;
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
            var count: u64 = 0;
            for (bucket) |e| if (e == fp) {
                count += 1;
            };
            try t.emit(p, fp, count);
        }
        return n_reps;
    }

    /// The repeats of a bucket with many, sorted so each one's rows are a run; they wait in `pending` for the
    /// partition's most frequent to be chosen.
    fn repeatsSorted(t: *CensusTask, entries: []u32, lo: usize, hi: usize) !usize {
        std.mem.sortUnstable(u32, entries[lo..hi], {}, std.sort.asc(u32));
        var i = lo;
        while (i < hi) {
            const fp = entries[i];
            var j = i + 1;
            while (j < hi and entries[j] == fp) j += 1;
            if (j - i > 1) try t.pending.append(t.gpa, .{ .fp = fp, .count = j - i });
            i = j;
        }
        return 0;
    }

    /// Emit one complete class. The exact residual scans every work item and filters by this identity.
    fn emit(t: *CensusTask, p: usize, fp: u64, count: u64) !void {
        try t.out.append(t.gpa, .{ .fp = (@as(u64, p) << 40) | fp, .count = count });
    }
};

/// A census's repeated fingerprints from every partition (`CensusTask`), most frequent first, ties by fingerprint, and
/// the largest count a partition dropped (1 when none was).
const CensusRepeats = struct {
    tasks: []CensusTask,
    repeats: std.ArrayList(Repeat) = .empty,
    omitted: u64 = 1,

    fn deinit(self: *CensusRepeats, gpa: std.mem.Allocator) void {
        self.repeats.deinit(gpa);
        for (self.tasks) |*t| {
            t.out.deinit(gpa);
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
    for (out.tasks) |*t| t.* = .{
        .gpa = gpa,
        .censuses = censuses,
        .cursor = &cursor_p,
        .found = &found,
        .cap = cap,
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

/// A producer's excluded-class bound and candidate membership. Cell and identity producers share the same
/// exact grouping pass and proof. A prefix additionally proves that excluded singleton ties appear later.
pub const CutBoundPlan = struct {
    bound: u64,
    distinct: bool = false,
    widen: ?struct { identities: expr_agg.FingerprintSet, bound: u64 } = null,
    cells: ?[]u64 = null,
    identities: ?expr_agg.FingerprintSet = null,
    prefix: ?struct { item_rows: []const u64, sure_need: u64, short_need: u64 } = null,

    pub fn proves(p: CutBoundPlan, score: u64) bool {
        return p.bound == 0 or score > p.bound;
    }

    pub fn deinit(p: *CutBoundPlan, a: std.mem.Allocator) void {
        if (p.cells) |cells| a.free(cells);
        if (p.identities) |*set| {
            set.map.deinit(a);
            a.free(set.filter);
        }
        if (p.widen) |*more| more.identities.map.deinit(a);
    }
};

pub fn certifiedResidual(
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
    plan: *CutBoundPlan,
) !bool {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const counted = try arena.dupe(Worker, workers);
    const work = workers[0].work;
    var need: u64 = if (plan.prefix) |p| p.short_need else 0;
    const t0 = nowMonoNs();
    while (true) {
        if (plan.identities) |*set| {
            var last: u64 = 0;
            var row: u64 = 0;
            if (plan.prefix) |prefix| {
                last = std.math.maxInt(u32);
                var seen: u64 = 0;
                for (prefix.item_rows, 0..) |n, i| {
                    if (seen + n < need) {
                        seen += n;
                        continue;
                    }
                    const rg_rows = workers[0].metas[work[i].file].row_groups.items[work[i].rg].num_rows;
                    last = i;
                    row = if (rg_rows >= 0 and n == @as(u64, @intCast(rg_rows))) need - seen else std.math.maxInt(u64);
                    break;
                }
            }
            for (sketches, censuses) |*s, *c| {
                c.admitted = set;
                c.prefix_item = last;
                c.prefix_row = row;
                s.census = c;
            }
        } else {
            for (sketches) |*s| {
                s.census = null;
                s.admit = plan.cells.?;
            }
        }
        runPhase(workers, cursor) catch |e| {
            if (e != error.ExceededMemoryBudget) return e;
            try clearWorkerErrors(workers, error.ExceededMemoryBudget);
            restoreCounts(workers, counted);
            clearTables(gpa, tables);
            if (cfg.outcome) |o| o.* = .fell_back;
            return false;
        };
        restoreCounts(workers, counted);
        var short = false;
        const proven = if (plan.prefix) |prefix| prefix_proof: {
            if (need >= prefix.sure_need) break :prefix_proof true;
            var fresh: usize = 0;
            for (tables) |*table| fresh += table.unadmittedGroups(&censuses[0]);
            if (fresh < k) {
                clearTables(gpa, tables);
                need = prefix.sure_need;
                continue;
            }
            short = true;
            break :prefix_proof true;
        } else count_proof: {
            const kth = if (plan.distinct)
                kthDistinct(gpa, arena, tables, calls.len, a_idx, k) catch |e| {
                    if (e != error.ExceededMemoryBudget) return e;
                    clearTables(gpa, tables);
                    if (cfg.outcome) |o| o.* = .fell_back;
                    return false;
                }
            else
                try kthTotal(arena, tables, calls.len, a_idx, k);
            break :count_proof plan.proves(kth);
        };
        if (cfg.trace) std.debug.print("certificate: residual_ms={d} excluded_bound={d} prefix={} proven={}\n", .{
            @divTrunc(nowMonoNs() - t0, 1_000_000), plan.bound, plan.prefix != null, proven,
        });
        if (cfg.certify and proven) {
            if (cfg.outcome) |o| o.* = if (plan.identities != null)
                (if (short) .census_short else .census)
            else
                .certified;
            return true;
        }
        if (plan.widen) |more| {
            // Keep completed exact groups; the next map contains only newly admitted classes.
            plan.identities.?.map.deinit(gpa);
            plan.identities = more.identities;
            plan.bound = more.bound;
            plan.widen = null;
            continue;
        }
        if (cfg.outcome) |o| o.* = .fell_back;
        clearTables(gpa, tables);
        return false;
    }
}

/// Produce a complete hash-class bound and candidate superset; full-key equality is deferred to the shared
/// residual. Collisions can retain more classes or decline, never exclude a possible winner.
/// The filter may contain a superset of bits when two batches share it; full identity lookup still admits only
/// the map's classes. Separate maps allow widening without counting any previously admitted row twice.
fn identitySet(
    gpa: std.mem.Allocator,
    filter: []u64,
    classes: []const Repeat,
) !expr_agg.FingerprintSet {
    var set: expr_agg.FingerprintSet = .{ .filter = filter, .map = .empty };
    errdefer set.map.deinit(gpa);
    try set.map.ensureTotalCapacity(gpa, @intCast(classes.len));
    for (classes) |class| {
        set.map.putAssumeCapacity(class.fp, {});
        const bit = class.fp & ((1 << expr_agg.FingerprintSet.filter_bits) - 1);
        filter[bit / 64] |= @as(u64, 1) << @intCast(bit % 64);
    }
    return set;
}

fn distinctScorePlan(
    gpa: std.mem.Allocator,
    reduced: *const CensusRepeats,
    item_rows: []const u64,
    k: usize,
    cfg: SketchTopK,
) !?CutBoundPlan {
    const classes = reduced.repeats.items;
    const n = @min(classes.len, @max(k *| cfg.admit_per_row, 1));
    const wider = @min(classes.len, n *| 4);
    var rows: u64 = 0;
    for (item_rows) |count| rows += count;
    var admitted_rows: u64 = 0;
    for (classes[0..n]) |class| admitted_rows += class.count;
    if (n < k or admitted_rows *| 4 > rows) return null;
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    @memset(filter, 0);
    var plan: CutBoundPlan = .{
        .bound = @max(reduced.omitted, if (n < classes.len) classes[n].count else 1),
        .distinct = true,
        .identities = identitySet(gpa, filter, classes[0..n]) catch |e| {
            gpa.free(filter);
            return e;
        },
    };
    errdefer plan.deinit(gpa);
    for (classes[n..wider]) |class| admitted_rows += class.count;
    if (cfg.certify and wider > n and admitted_rows *| 4 <= rows) {
        const more = try identitySet(gpa, filter, classes[n..wider]);
        plan.widen = .{
            .bound = @max(reduced.omitted, if (wider < classes.len) classes[wider].count else 1),
            .identities = more,
        };
    }
    return plan;
}

fn identityBoundPlan(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    tables: []expr_agg.GroupTable,
    censuses: []expr_agg.Census,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    a_idx: usize,
    cfg: SketchTopK,
    count_ns: i128,
) !?CutBoundPlan {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const item_rows = censuses[0].item_rows;
    const t1 = nowMonoNs();
    const count_ms = @divTrunc(count_ns, 1_000_000);

    const distinct = calls[a_idx].func == .count_distinct;
    const cap = @max(k *| cfg.admit_per_row *| 64, 1 << 20);
    const keep = @max(k *| cfg.admit_per_row, 1) *| @as(usize, if (distinct) 4 else 1);
    var reduced = (try reduceCensus(gpa, arena, workers.len, censuses, keep, cap)) orelse {
        if (cfg.trace) std.debug.print("census: count_ms={d} repeated>{d}, too many\n", .{ count_ms, cap });
        return null;
    };
    defer reduced.deinit(gpa);
    if (distinct) return distinctScorePlan(gpa, &reduced, item_rows, k, cfg);
    if (cfg.trace) std.debug.print("census: reduce_ms={d}\n", .{@divTrunc(nowMonoNs() - t1, 1_000_000)});
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
        return null;
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
        return null;
    }

    var set: expr_agg.FingerprintSet = .{ .filter = undefined, .map = .empty };
    errdefer set.map.deinit(gpa);
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    errdefer gpa.free(filter);
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
    return .{
        .bound = t - 1,
        .identities = set,
        .prefix = if (prefix) .{ .item_rows = item_rows, .sure_need = sure_need, .short_need = short_need } else null,
    };
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

/// Discover at most H exact keys per worker. A worker's local Hth first position bounds the global Hth position,
/// so it can stop later discovery; every selected key is still counted through the entire input afterwards.
pub fn firstAppearanceCut(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    workers: []Worker,
    cursor: *std.atomic.Value(usize),
    tables: []expr_agg.GroupTable,
    order: expr_ast.ResultOrder,
    cfg: SketchTopK,
) !void {
    const k: usize = @intCast(order.offset +| order.limit.?);
    const saved = try arena.dupe(Worker, workers);
    var threshold: std.atomic.Value(u64) = .init(std.math.maxInt(u64));
    for (tables) |*table| table.discovery = .{ .limit = k, .threshold = &threshold };
    defer for (tables) |*table| {
        table.discovery = null;
    };
    const SavedWindow = struct { rows: usize, budget: ?*consumer.DecodeBudget };
    const windows = try arena.alloc(SavedWindow, workers.len);
    var discovery_budget: consumer.DecodeBudget = .{
        .limit = if (tables[0].shared) |budget| budget.limit else std.math.maxInt(usize),
        .workers = workers.len,
    };
    for (workers, windows) |*w, *window| {
        window.* = .{ .rows = w.decode_batch_rows, .budget = w.decode_budget };
        w.decode_batch_rows = owner_groups.batchRows(null);
        if (w.decode_budget == null) w.decode_budget = &discovery_budget;
    }
    defer for (workers, windows) |*w, window| {
        w.decode_batch_rows = window.rows;
        w.decode_budget = window.budget;
    };
    try runPhase(workers, cursor);
    for (workers, windows) |*w, window| {
        w.decode_batch_rows = window.rows;
        w.decode_budget = window.budget;
    }
    for (tables) |*table| table.discovery = null;

    const First = struct { first: u64, table: usize, group: usize };
    var seen: std.StringArrayHashMapUnmanaged(First) = .empty;
    defer seen.deinit(gpa);
    for (tables, 0..) |*table, ti| for (0..table.groupCount()) |group| {
        const entry = try seen.getOrPut(gpa, table.keyAt(group));
        const first: First = .{ .first = table.first.items[group], .table = ti, .group = group };
        if (!entry.found_existing or first.first < entry.value_ptr.first) entry.value_ptr.* = first;
    };
    std.mem.sortUnstable(First, seen.values(), {}, struct {
        fn earlier(_: void, x: First, y: First) bool {
            return x.first < y.first;
        }
    }.earlier);
    const n = @min(k, seen.count());
    const filter = try gpa.alloc(u64, (1 << expr_agg.FingerprintSet.filter_bits) / 64);
    defer gpa.free(filter);
    var set: expr_agg.FingerprintSet = .{ .filter = filter, .map = .empty };
    defer set.map.deinit(gpa);
    @memset(filter, 0);
    try set.map.ensureTotalCapacity(gpa, @intCast(n));
    const identity: expr_agg.Census = .{ .gpa = gpa, .item_rows = &.{}, .fp_mask = censusMask(cfg) };
    for (seen.values()[0..n]) |first| {
        const fp = identity.fingerprint(tables[first.table].routeHash(first.group));
        set.map.putAssumeCapacity(fp, {});
        filter[fp % (1 << expr_agg.FingerprintSet.filter_bits) / 64] |=
            @as(u64, 1) << @intCast(fp % 64);
    }
    // The first scan only discovered keys. Replay every row once for complete counts/payloads; hash collisions
    // merely admit extra keys whose first positions are later than the H retained keys.
    clearTables(gpa, tables);
    const censuses = try arena.alloc(expr_agg.Census, workers.len);
    defer for (censuses) |*c| c.deinit();
    for (censuses) |*c| c.* = .{ .gpa = gpa, .item_rows = &.{}, .admitted = &set, .fp_mask = identity.fp_mask };
    var no_cells = [1]u32{0};
    const sketches = try arena.alloc(expr_agg.CountSketch, workers.len);
    for (sketches, censuses, tables) |*s, *c, *table| {
        s.* = .{ .cells = &no_cells, .shift = 63, .census = c };
        table.sketch = s;
    }
    defer for (tables) |*table| {
        table.sketch = null;
    };
    restoreCounts(workers, saved);
    const t0 = nowMonoNs();
    try runPhase(workers, cursor);
    if (cfg.trace) std.debug.print("first: bounded_keys={d} threshold={d} count_ms={d}\n", .{
        n, threshold.load(.monotonic), @divTrunc(nowMonoNs() - t0, 1_000_000),
    });
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
}
test "COUNT certificate exclusion is strict at ties and never trusts a saturated bound" {
    const finite: CutBoundPlan = .{ .bound = 7 };
    try testing.expect(!finite.proves(6));
    try testing.expect(!finite.proves(7));
    try testing.expect(finite.proves(8));
    const saturated: CutBoundPlan = .{ .bound = std.math.maxInt(u64) };
    try testing.expect(!saturated.proves(std.math.maxInt(u64)));
    const empty: CutBoundPlan = .{ .bound = 0 };
    try testing.expect(empty.proves(0));
}
fn distinctBoundPlanTrial(a: std.mem.Allocator) !void {
    var classes = [_]Repeat{
        .{ .fp = 1, .count = 100 }, .{ .fp = 2, .count = 80 }, .{ .fp = 3, .count = 60 },
        .{ .fp = 4, .count = 40 },  .{ .fp = 5, .count = 20 },
    };
    var borrowed = std.ArrayList(Repeat).initBuffer(&classes);
    borrowed.items = &classes;
    const reduced: CensusRepeats = .{ .tasks = &.{}, .repeats = borrowed, .omitted = 1 };
    var plan = (try distinctScorePlan(a, &reduced, &.{4096}, 1, .{ .admit_per_row = 1 })) orelse
        return error.TestUnexpectedResult;
    defer plan.deinit(a);
    try testing.expect(plan.distinct);
    try testing.expectEqual(@as(u64, 80), plan.bound);
    try testing.expect(!plan.proves(80));
    try testing.expect(plan.proves(81));
    try testing.expect(plan.identities.?.contains(1));
    try testing.expect(!plan.identities.?.contains(2));
    const wider = plan.widen.?;
    try testing.expectEqual(@as(u64, 20), wider.bound);
    try testing.expect(!wider.identities.contains(1));
    for (2..5) |fp| try testing.expect(wider.identities.contains(fp));
    try testing.expect(!wider.identities.contains(5));
}

test "DISTINCT shares score bounds and widens only to new identities with allocation-safe ownership" {
    try testing.checkAllAllocationFailures(testing.allocator, distinctBoundPlanTrial, .{});
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

