//! Shared partitioned GROUP BY for a plain aggregation (no count cut, sketch, census, or first-keys cut).
//!
//! Each worker folds into a small table. When that table fills, or when it is not reducing the rows, its groups are
//! merged into one of `n_parts` shared tables, 2048 at a time under that table's lock. A key's partition is the top
//! bits of its hash, so the tables are disjoint and the answer does not need a cross-worker key merge. First
//! appearance is the minimum position any worker saw, taken while the lock is held, so the output order does not
//! depend on which worker ran.
//!
//! Count(DISTINCT) and float aggregates whose bits depend on merge order stay on the per-worker tables. The caller
//! decides that (`scan.partAggWanted`).

const std = @import("std");
const expr_agg = @import("expr/agg.zig");

pub const n_parts: usize = 32;
/// Groups a worker keeps before flushing. About an L2-sized open table in the layout probe (2^15 slots, half full).
pub const local_cap: usize = 1 << 14;
/// Rows moved under one partition lock. Smaller batches serialize on the lock; the probe's 2048 was the knee.
pub const batch_n: usize = 2048;
/// Rows folded before a strong reduction is trusted enough to stop windowing the row group.
pub const probe_rows: u64 = 16384;

pub const MergeFn = *const fn (*expr_agg.Accumulator, *expr_agg.Accumulator, std.mem.Allocator) anyerror!void;

pub const Local = struct {
    rows_seen: u64 = 0,
    rows_since: u64 = 0,
    /// The last fill reduced by less than 2x: stop aggregating locally and flush every batch.
    abandoned: bool = false,
};

pub const Part = struct {
    lock: std.atomic.Value(bool) align(64) = .init(false),
    table: expr_agg.GroupTable,
};

pub const PartAgg = struct {
    gpa: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    parts: []Part,
    budget: *expr_agg.SharedBudget,
    merge: MergeFn,
    stride: u8,
    /// Top bits of `GroupTable.routeHash` select the partition.
    shift: u6 = 59,
    cap: usize = local_cap,
    probe: u64 = probe_rows,

    pub fn init(
        gpa: std.mem.Allocator,
        budget: *expr_agg.SharedBudget,
        calls: []const expr_agg.AggCall,
        stride: u8,
        merge: MergeFn,
        cap: usize,
        probe: u64,
    ) !PartAgg {
        const parts = try gpa.alloc(Part, n_parts);
        errdefer gpa.free(parts);
        var n: usize = 0;
        errdefer for (parts[0..n]) |*p| p.table.deinit();
        for (parts) |*p| {
            p.* = .{ .table = expr_agg.GroupTable.init(gpa, 0) };
            p.table.shared = budget;
            p.table.fixed_stride = stride;
            n += 1;
        }
        return .{
            .gpa = gpa,
            .calls = calls,
            .parts = parts,
            .budget = budget,
            .merge = merge,
            .stride = stride,
            .cap = if (cap == 0) local_cap else cap,
            .probe = if (probe == 0) probe_rows else probe,
        };
    }

    pub fn deinit(self: *PartAgg) void {
        for (self.parts) |*p| p.table.deinit();
        self.gpa.free(self.parts);
        self.* = undefined;
    }

    pub fn needsFlush(self: *const PartAgg, local: *const Local, table: *const expr_agg.GroupTable) bool {
        const n = table.groupCount();
        if (n == 0) return false;
        return n >= self.cap or local.abandoned;
    }

    /// How many rows of this row group to fold before looking at the local table again. The first window stays
    /// inside `cap` so a unique key cannot allocate the whole row group into one worker. Once a window has shown
    /// the groups are a small fraction of the rows and the table is still under a quarter of its cap, the rest of
    /// the row group is one fold: a low-cardinality key pays one extra probe, not a window per batch.
    pub fn nextSpan(
        self: *const PartAgg,
        local: *const Local,
        table: *const expr_agg.GroupTable,
        remaining: usize,
    ) usize {
        if (remaining == 0) return 0;
        if (local.abandoned) return @min(batch_n, remaining);
        const groups = table.groupCount();
        const room = self.cap -| groups;
        if (local.rows_seen >= self.probe and groups * 8 <= local.rows_seen and groups * 4 < self.cap)
            return remaining;
        const probe_n: usize = @intCast(@min(self.probe, std.math.maxInt(usize)));
        const bounded = @max(@min(room, probe_n), 1);
        return @min(bounded, remaining);
    }

    /// Count `rows` toward the reduction, and flush when the local table is full or no longer worth keeping.
    pub fn pump(self: *PartAgg, local: *Local, table: *expr_agg.GroupTable, rows: u64) !void {
        local.rows_seen += rows;
        local.rows_since += rows;
        if (!self.needsFlush(local, table)) return;
        const groups: u64 = @intCast(table.groupCount());
        const poor = local.rows_since < groups * 2;
        try self.flush(table);
        local.rows_since = 0;
        if (poor) local.abandoned = true;
    }

    /// Merge every local group into its partition, then drop the local table's memory back to the shared budget.
    pub fn flush(self: *PartAgg, local: *expr_agg.GroupTable) !void {
        const n = local.groupCount();
        if (n == 0) return;
        const ids = try self.gpa.alloc(u32, n);
        defer self.gpa.free(ids);
        var counts: [n_parts]u32 = @splat(0);
        for (0..n) |g| counts[self.partOf(local, g)] += 1;
        var start: [n_parts + 1]u32 = undefined;
        start[0] = 0;
        for (0..n_parts) |p| start[p + 1] = start[p] + counts[p];
        var at = start;
        for (0..n) |g| {
            const p = self.partOf(local, g);
            ids[at[p]] = @intCast(g);
            at[p] += 1;
        }
        for (self.parts, 0..) |*part, p| {
            var i: usize = start[p];
            const end_p: usize = start[p + 1];
            while (i < end_p) {
                const end = @min(i + batch_n, end_p);
                try self.mergeBatch(part, local, ids[i..end]);
                i = end;
            }
        }
        self.reset(local);
    }

    pub fn indexOf(self: *const PartAgg, hash: u64) usize {
        return @intCast(hash >> self.shift);
    }

    fn partOf(self: *const PartAgg, table: *const expr_agg.GroupTable, g: usize) usize {
        return self.indexOf(table.routeHash(g));
    }

    pub fn lock(part: *Part) void {
        var spins: u32 = 0;
        while (part.lock.cmpxchgWeak(false, true, .acquire, .monotonic) != null) {
            if (spins < 64) {
                spins += 1;
                std.atomic.spinLoopHint();
            } else std.Thread.yield() catch {};
        }
    }

    pub fn unlock(part: *Part) void {
        part.lock.store(false, .release);
    }

    fn mergeBatch(self: *PartAgg, part: *Part, local: *expr_agg.GroupTable, groups: []const u32) !void {
        lock(part);
        defer unlock(part);
        const len = self.calls.len;
        for (groups) |g| {
            const gi: usize = g;
            const id = try part.table.getOrInsert(local.keyAt(gi), self.calls, local.first.items[gi]);
            const dst = part.table.accumulators.items[@as(usize, id) * len ..][0..len];
            const src = local.accumulators.items[gi * len ..][0..len];
            for (dst, src) |*d, *s| try self.merge(d, s, self.gpa);
        }
    }

    fn reset(self: *PartAgg, table: *expr_agg.GroupTable) void {
        const gpa = table.allocator;
        const shared = table.shared;
        const stride = table.fixed_stride;
        table.deinit();
        table.* = expr_agg.GroupTable.init(gpa, 0);
        table.shared = shared;
        table.fixed_stride = stride;
        _ = self;
    }
};

fn countMerge(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) anyerror!void {
    dst.count += src.count;
    src.count = 0;
}

test "partagg: partial flushes sum and keep the earliest position" {
    const a = std.testing.allocator;
    var budget = expr_agg.SharedBudget{ .limit = 1 << 20, .block = 4096 };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 0, 0);
    defer part.deinit();

    var first_t = expr_agg.GroupTable.init(a, 0);
    first_t.shared = &budget;
    defer first_t.deinit();
    var second = expr_agg.GroupTable.init(a, 0);
    second.shared = &budget;
    defer second.deinit();

    const a_id = try first_t.getOrInsert("alpha", &calls, 10);
    first_t.accumulators.items[a_id].count = 2;
    const b_id = try first_t.getOrInsert("beta", &calls, 40);
    first_t.accumulators.items[b_id].count = 1;
    try part.flush(&first_t);
    try std.testing.expectEqual(@as(usize, 0), first_t.groupCount());

    const a2 = try second.getOrInsert("alpha", &calls, 3);
    second.accumulators.items[a2].count = 5;
    const c_id = try second.getOrInsert("gamma", &calls, 20);
    second.accumulators.items[c_id].count = 4;
    try part.flush(&second);

    var groups: usize = 0;
    var alpha: u64 = 0;
    var alpha_at: u64 = std.math.maxInt(u64);
    var total: u64 = 0;
    for (part.parts) |p| {
        groups += p.table.groupCount();
        for (0..p.table.groupCount()) |g| {
            total += p.table.accumulators.items[g].count;
            if (std.mem.eql(u8, p.table.keyAt(g), "alpha")) {
                alpha = p.table.accumulators.items[g].count;
                alpha_at = p.table.first.items[g];
            }
        }
    }
    try std.testing.expectEqual(@as(usize, 3), groups);
    try std.testing.expectEqual(@as(u64, 7), alpha);
    try std.testing.expectEqual(@as(u64, 3), alpha_at);
    try std.testing.expectEqual(@as(u64, 12), total);
}

test "partagg: an allocation failure leaks nothing" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            var budget = expr_agg.SharedBudget{ .limit = 1 << 20, .block = 1024 };
            const calls = [_]expr_agg.AggCall{.{
                .func = .count,
                .arg = null,
                .where = null,
                .alias = "c",
                .result = .i64,
            }};
            var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 8, 4);
            defer part.deinit();
            var local = expr_agg.GroupTable.init(a, 0);
            local.shared = &budget;
            defer local.deinit();
            for ([_][]const u8{ "a", "b", "c", "d" }, 0..) |key, i| {
                const id = try local.getOrInsert(key, &calls, @intCast(i + 1));
                local.accumulators.items[id].count = @intCast(i + 1);
            }
            try part.flush(&local);
            var total: u64 = 0;
            for (part.parts) |p| for (0..p.table.groupCount()) |g| {
                total += p.table.accumulators.items[g].count;
            };
            try std.testing.expectEqual(@as(u64, 10), total);
        }
    }.run, .{});
}
