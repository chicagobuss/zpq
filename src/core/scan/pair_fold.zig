const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const core_memory = @import("../memory.zig");
const spawn_util = @import("../spawn.zig");
const WORKER_STACK_SIZE = @import("types.zig").WORKER_STACK_SIZE;

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

const PairTableTask = struct {
    tables: []expr_agg.GroupTable,
    keyed: []const []KeyDistinct = &.{},
    agg_idx: usize = 0,
    agg_len: usize = 0,
    gpa: std.mem.Allocator,
    cursor: *std.atomic.Value(usize),
    sinks: ?[]expr_agg.DistinctPairs = null,
    err: ?anyerror = null,

    fn run(t: *PairTableTask) void {
        const Pairs = expr_agg.DistinctPairs;
        while (t.err == null) {
            const ti = t.cursor.fetchAdd(1, .monotonic);
            if (ti >= t.tables.len) return;
            const table = &t.tables[ti];
            if (t.sinks) |sinks| {
                const sink = &sinks[ti];
                if (!sink.active or sink.pending_sets) sink.drainTable(table) catch |e| {
                    t.err = e;
                    return;
                };
                continue;
            }
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

/// Each table owns its stored sets and sink. Drain them in parallel before the shared pair fold, just as the
/// scan produced pairs in parallel; serial expansion would strand every decoder behind one large group.
pub fn drainDistinctPairs(
    gpa: std.mem.Allocator,
    sinks: []expr_agg.DistinctPairs,
    tables: []expr_agg.GroupTable,
    threads: usize,
) !void {
    var cursor: std.atomic.Value(usize) = .init(0);
    const n = @max(1, @min(threads, tables.len, 64));
    var tasks: [64]PairTableTask = undefined;
    for (tasks[0..n]) |*task| task.* = .{
        .tables = tables,
        .gpa = gpa,
        .cursor = &cursor,
        .sinks = sinks,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (n == 1 or !spawn_util.runTree(PairTableTask, tasks[0..n], config, PairTableTask.run)) tasks[0].run();
    for (tasks[0..n]) |task| if (task.err) |e| return e;
}

/// Turn every worker's pairs into exact count(DISTINCT) states on the worker tables: each key's count on the first
/// table to claim it, 0 on the others. NULL values sent no pair, so a key seen only with them counts 0.
pub fn foldDistinctPairs(
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
    var applies: [64]PairTableTask = undefined;
    for (applies[0..n_apply]) |*a| a.* = .{
        .tables = tables,
        .keyed = keyed,
        .agg_idx = agg_idx,
        .agg_len = agg_len,
        .gpa = gpa,
        .cursor = &table_cursor,
    };
    if (n_apply == 1 or !spawn_util.runTree(PairTableTask, applies[0..n_apply], config, PairTableTask.run))
        applies[0].run();
}

test "distinct pairs drain and count complete bitmap states after zero or partial worker starts" {
    const gpa = testing.allocator;
    const calls = [_]expr_agg.AggCall{.{
        .func = .count_distinct,
        .arg = null,
        .where = null,
        .alias = "d",
        .result = .i64,
    }};
    for ([_]usize{ 0, 1, 3 }) |starts| {
        var budget: core_memory.SharedBudget = .{ .limit = 8 << 20 };
        defer std.debug.assert(budget.used.load(.monotonic) == 0);
        var tables: [4]expr_agg.GroupTable = undefined;
        for (&tables) |*table| {
            table.* = .init(gpa, 0);
            table.shared = &budget;
        }
        defer for (&tables) |*table| table.deinit();
        var sinks: [4]expr_agg.DistinctPairs = undefined;
        for (&sinks, &tables) |*sink, *table| sink.* = .{
            .gpa = gpa,
            .budget = &budget,
            .table = table,
            .agg_idx = 0,
            .allow_coded_sets = true,
            .pending_sets = true,
        };
        defer for (&sinks) |*sink| sink.deinit();
        for (&tables, 0..) |*table, i| {
            const g = try table.getOrInsert(&.{7}, &calls, i);
            for (0..128) |v| try table.accumulators.items[g].distinct.insertU64(gpa, i * 64 + v);
            try testing.expect(table.accumulators.items[g].distinct.heap().?.val_set == .bitmap);
        }
        spawn_util.injectFailureAfter(starts);
        defer spawn_util.resetFailure();
        try drainDistinctPairs(gpa, &sinks, &tables, 4);
        try testing.expect(spawn_util.failuresInjected() > 0);
        for (&sinks) |*sink| try testing.expect(sink.hasPairs());
        try foldDistinctPairs(gpa, &budget, &sinks, &tables, 0, 1, 4);
        var total: u64 = 0;
        for (tables) |table| total += table.accumulators.items[0].count;
        try testing.expectEqual(@as(u64, 320), total);
    }
}
