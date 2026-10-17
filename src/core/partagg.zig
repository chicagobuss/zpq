//! Partitioned GROUP BY for a plain aggregation (no count cut, sketch, census, or first-keys cut).
//!
//! Each worker folds into a small table. When that table fills, or when it is not reducing the rows, its groups (or,
//! once abandoned, each row) are appended to that worker's run for the key's partition: no lock, no shared table.
//! After the scan, `foldRuns` folds each partition's runs, worker by worker, into a table of its own. A key's partition
//! is the top bits of its hash, so the tables are disjoint and the answer does not need a cross-worker key merge, and
//! each table is small enough to stay in cache while it is folded. First appearance is the minimum position any
//! worker saw, so the output order does not depend on which worker ran.
//!
//! Count(DISTINCT) and float aggregates whose bits depend on merge order stay on the per-worker tables. The caller
//! decides that (`scan.partAggWanted`).

const std = @import("std");
const expr_agg = @import("expr/agg.zig");
const spawn_util = @import("spawn.zig");

/// At most this many partitions: enough that one holds a cache-sized table at tens of millions of groups. A small
/// budget gets fewer (`partBits`), as every partition's table costs a few hundred bytes even when nearly empty.
pub const max_part_bits = 10;
pub const n_parts: usize = 1 << max_part_bits;

/// Partition bits for a budget of `limit` bytes: one partition per 256 KiB, between 32 and `n_parts`.
pub fn partBits(limit: usize) u6 {
    const per = limit / (256 * 1024);
    const bits: usize = if (per <= 1) 0 else std.math.log2_int(usize, per);
    return @intCast(std.math.clamp(bits, 5, max_part_bits));
}
/// Groups a worker keeps before flushing. About an L2-sized open table in the layout probe (2^15 slots, half full).
pub const local_cap: usize = 1 << 14;
/// Rows scattered per window once a worker stops aggregating locally.
pub const batch_n: usize = 2048;
/// Rows folded before a strong reduction is trusted enough to stop windowing the row group.
pub const probe_rows: u64 = 16384;

pub const MergeFn = *const fn (*expr_agg.Accumulator, *expr_agg.Accumulator, std.mem.Allocator) anyerror!void;

/// Partial groups one worker appended for one partition, in the order it met them: keys, where each first appeared,
/// and `calls.len` accumulators each. Never probed, so appending costs a write, not a cache miss.
pub const Run = struct {
    /// Fixed-width images back to back, or variable keys back to back with their ends in `ends`.
    keys: std.ArrayList(u8) = .empty,
    ends: std.ArrayList(u32) = .empty,
    first: std.ArrayList(u64) = .empty,
    accs: std.ArrayList(expr_agg.Accumulator) = .empty,
    compact: @import("expr/agg_state.zig").State = .{},
    /// Bytes drawn from the shared budget for the capacities above.
    charged: usize = 0,

    pub fn len(self: *const Run) usize {
        return self.first.items.len;
    }

    fn key(self: *const Run, stride: u8, i: usize) []const u8 {
        if (stride != 0) return self.keys.items[i * stride ..][0..stride];
        const lo: usize = if (i == 0) 0 else self.ends.items[i - 1];
        return self.keys.items[lo..self.ends.items[i]];
    }

    pub fn readGroup(self: *const Run, g: usize, n: usize, scratch: *[16]expr_agg.Accumulator) []expr_agg.Accumulator {
        if (self.compact.stride == 0) return self.accs.items[g * n ..][0..n];
        for (scratch[0..n], 0..) |*a, i| a.* = self.compact.read(g, i);
        return scratch[0..n];
    }

    pub fn writeGroup(self: *const Run, g: usize, values: []const expr_agg.Accumulator) void {
        if (self.compact.stride != 0) for (values, 0..) |a, i| self.compact.write(g, i, a);
    }

    fn held(self: *const Run) usize {
        return self.keys.capacity + self.ends.capacity * @sizeOf(u32) + self.first.capacity * @sizeOf(u64) +
            self.accs.capacity * @sizeOf(expr_agg.Accumulator) + self.compact.held();
    }

    fn free(self: *Run, gpa: std.mem.Allocator, budget: *expr_agg.SharedBudget) void {
        for (self.accs.items) |*acc| freeOwned(gpa, acc);
        self.keys.deinit(gpa);
        self.ends.deinit(gpa);
        self.first.deinit(gpa);
        self.accs.deinit(gpa);
        self.compact.words.deinit(gpa);
        budget.release(self.charged);
        self.* = .{};
    }
};

pub const Runs = [n_parts]Run;

pub const Local = struct {
    rows_seen: u64 = 0,
    rows_since: u64 = 0,
    /// The last fill reduced by less than 2x: stop aggregating locally and scatter every batch.
    abandoned: bool = false,
    /// This worker's runs, one per partition (`PartAgg.runs`).
    runs: ?*Runs = null,
    /// Still on the plain fold: whole windows of `armed_window` rows into one table that never flushes, until it
    /// holds `spill_groups` groups at fewer than `spill_rows` rows each. Then it flushes once and scatters.
    armed: bool = false,
};

/// Rows an armed worker folds before looking at its table again. Large, as a window's fixed cost (the coded path
/// sizes its work by the dictionary) must stay small beside the rows it folds.
pub const armed_window: usize = 1 << 18;

pub const Part = struct {
    /// Held while a worker merges into `table` (`PartAgg.shared` only).
    lock: std.atomic.Value(bool) align(64) = .init(false),
    table: expr_agg.GroupTable,
};

/// Partitions in shared mode: few, so a flush moves many groups under each lock.
const shared_part_bits = 5;

pub const PartAgg = struct {
    gpa: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    parts: []Part,
    /// One set of runs per worker, folded in worker order.
    runs: []Runs,
    budget: *expr_agg.SharedBudget,
    merge: MergeFn,
    stride: u8,
    /// Top bits of `GroupTable.routeHash` select the partition, one of `parts.len`.
    shift: u6,
    cap: usize = local_cap,
    probe: u64 = probe_rows,
    /// Workers merge straight into the partition tables under their locks instead of appending to runs. Long keys
    /// gain: each is held once, in its partition, rather than once per worker and again in a run.
    shared: bool = false,
    compact: bool = false,
    /// Set by the first worker that moves its plain table here (`scan.MultiAggArgs.spill`).
    spilled: std.atomic.Value(bool) = .init(false),
    spill_groups: usize = 1 << 18,
    spill_rows: u64 = 4,

    pub fn init(
        gpa: std.mem.Allocator,
        budget: *expr_agg.SharedBudget,
        calls: []const expr_agg.AggCall,
        stride: u8,
        merge: MergeFn,
        cap: usize,
        probe: u64,
        n_workers: usize,
        shared: bool,
    ) !PartAgg {
        const bits: u6 = if (shared) shared_part_bits else partBits(budget.limit);
        const parts = try gpa.alloc(Part, @as(usize, 1) << bits);
        errdefer gpa.free(parts);
        for (parts) |*p| {
            p.* = .{ .table = expr_agg.GroupTable.init(gpa, 0) };
            p.table.shared = budget;
            p.table.fixed_stride = stride;
        }
        const runs = try gpa.alloc(Runs, @max(n_workers, 1));
        for (runs) |*r| r.* = @splat(.{});
        return .{
            .gpa = gpa,
            .calls = calls,
            .parts = parts,
            .runs = runs,
            .budget = budget,
            .merge = merge,
            .stride = stride,
            .shared = shared,
            .shift = @intCast(64 - @as(u7, bits)),
            .cap = if (cap == 0) local_cap else cap,
            .probe = if (probe == 0) probe_rows else probe,
        };
    }

    pub fn deinit(self: *PartAgg) void {
        for (self.runs) |*rs| for (rs) |*r| r.free(self.gpa, self.budget);
        self.gpa.free(self.runs);
        for (self.parts) |*p| p.table.deinit();
        self.gpa.free(self.parts);
        self.* = undefined;
    }

    pub fn needsFlush(self: *const PartAgg, local: *const Local, table: *const expr_agg.GroupTable) bool {
        const n = table.groupCount();
        if (n == 0) return false;
        if (local.armed) return n >= self.spill_groups and n * self.spill_rows > local.rows_seen;
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
        if (local.armed) return @min(armed_window, remaining);
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
        if (local.armed) {
            // The table stopped reducing the rows: this worker moves to the runs for the rest of its scan.
            self.spilled.store(true, .monotonic);
            try self.flush(local, table);
            local.armed = false;
            local.abandoned = true;
            return;
        }
        const groups: u64 = @intCast(table.groupCount());
        const poor = local.rows_since < groups * 2;
        try self.flush(local, table);
        local.rows_since = 0;
        if (poor) local.abandoned = true;
    }

    /// Append every local group to this worker's run for its partition, then drop the local table's memory back to the
    /// shared budget.
    pub fn flush(self: *PartAgg, local: *Local, table: *expr_agg.GroupTable) !void {
        const n = table.groupCount();
        if (n == 0) return;
        if (self.shared) return self.flushShared(table);
        const runs = local.runs.?;
        const len = self.calls.len;
        for (0..n) |g| {
            const run = &runs[self.indexOf(table.routeHash(g))];
            const at = try self.append(run, table.keyAt(g), table.first.items[g]);
            var dst_scratch: [16]expr_agg.Accumulator = undefined;
            const dst = run.readGroup(at, len, &dst_scratch);
            var scratch: [16]expr_agg.Accumulator = undefined;
            const src = table.readGroup(g, len, &scratch);
            for (dst, src) |*d, *s| try self.merge(d, s, self.gpa);
            run.writeGroup(at, dst);
        }
        self.reset(table);
    }

    /// `flush` in shared mode: each partition's groups merged under its lock, `batch_n` at a time.
    fn flushShared(self: *PartAgg, local: *expr_agg.GroupTable) !void {
        const n = local.groupCount();
        const ids = try self.gpa.alloc(u32, n);
        defer self.gpa.free(ids);
        var counts: [n_parts]u32 = @splat(0);
        for (0..n) |g| counts[self.indexOf(local.routeHash(g))] += 1;
        var start: [n_parts + 1]u32 = undefined;
        start[0] = 0;
        for (0..self.parts.len) |p| start[p + 1] = start[p] + counts[p];
        var at = start;
        for (0..n) |g| {
            const p = self.indexOf(local.routeHash(g));
            ids[at[p]] = @intCast(g);
            at[p] += 1;
        }
        for (self.parts, 0..) |*part, p| {
            var i: usize = start[p];
            while (i < start[p + 1]) {
                const end = @min(i + batch_n, start[p + 1]);
                try self.mergeBatch(part, local, ids[i..end]);
                i = end;
            }
        }
        self.reset(local);
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
            var dst_scratch: [16]expr_agg.Accumulator = undefined;
            var src_scratch: [16]expr_agg.Accumulator = undefined;
            const dst = part.table.readGroup(id, len, &dst_scratch);
            const src = local.readGroup(gi, len, &src_scratch);
            for (dst, src) |*d, *s| try self.merge(d, s, self.gpa);
            part.table.writeGroup(id, dst);
        }
    }

    /// Append a fresh entry for `key`, first asked for at `first`, to `run`; returns its index. Growth is drawn from
    /// the shared budget before it is made.
    pub fn append(self: *PartAgg, run: *Run, raw_key: []const u8, first: u64) !usize {
        var image: [32]u8 = undefined;
        const key = if (self.stride == 0) raw_key else expr_agg.fixedImage(self.stride, raw_key, &image) orelse
            return error.BadGroupKey;
        const len = self.calls.len;
        if (self.compact and run.compact.stride == 0) run.compact.configure(self.calls);
        if (run.first.items.len == run.first.capacity or run.keys.items.len + key.len > run.keys.capacity or
            (if (run.compact.stride != 0)
                run.compact.words.items.len + run.compact.stride > run.compact.words.capacity
            else
                run.accs.items.len + len > run.accs.capacity) or
            (self.stride == 0 and run.ends.items.len == run.ends.capacity))
        {
            // Every list's new capacity is chosen here and grown to exactly that, so the bytes it will hold are
            // reserved before anything changes: a growth that fits the budget is never refused for an estimate.
            const want = @max(run.first.items.len * 2, 4);
            const first_cap = @max(run.first.capacity, want);
            const keys_cap = if (run.keys.items.len + key.len > run.keys.capacity)
                @max(run.keys.items.len + key.len, run.keys.capacity * 2, 64)
            else
                run.keys.capacity;
            const ends_cap = if (self.stride == 0) @max(run.ends.capacity, want) else run.ends.capacity;
            const accs_cap = if (run.compact.stride != 0) 0 else @max(run.accs.capacity, want * len);
            const compact_cap = @max(run.compact.words.capacity, first_cap * run.compact.stride);
            const target = keys_cap + ends_cap * @sizeOf(u32) + first_cap * @sizeOf(u64) +
                accs_cap * @sizeOf(expr_agg.Accumulator) + compact_cap * @sizeOf(u64);
            if (target > run.charged) {
                run.charged += self.budget.drawExact(target - run.charged) orelse return error.ExceededMemoryBudget;
            }
            // A list that failed to grow leaves the others grown: the charge falls back to what is held.
            defer self.settle(run);
            try run.first.ensureTotalCapacityPrecise(self.gpa, first_cap);
            try run.keys.ensureTotalCapacityPrecise(self.gpa, keys_cap);
            if (self.stride == 0) try run.ends.ensureTotalCapacityPrecise(self.gpa, ends_cap);
            try run.accs.ensureTotalCapacityPrecise(self.gpa, accs_cap);
            try run.compact.words.ensureTotalCapacityPrecise(self.gpa, compact_cap);
        }
        const at = run.first.items.len;
        run.keys.appendSliceAssumeCapacity(key);
        if (self.stride == 0) run.ends.appendAssumeCapacity(@intCast(run.keys.items.len));
        run.first.appendAssumeCapacity(first);
        if (run.compact.stride != 0) {
            run.compact.append();
        } else {
            for (self.calls) |call| run.accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
        }
        return at;
    }

    /// Give back what `run.charged` holds beyond its lists. Growth reserves before it allocates, so the charge never
    /// falls short of what is held.
    fn settle(self: *PartAgg, run: *Run) void {
        const held = run.held();
        std.debug.assert(held <= run.charged);
        if (held < run.charged) {
            self.budget.release(run.charged - held);
            run.charged = held;
        }
    }

    pub fn indexOf(self: *const PartAgg, hash: u64) usize {
        return @intCast(hash >> self.shift);
    }

    /// Fold every worker's runs into the partition tables, partitions claimed off a shared cursor by up to `threads`
    /// threads. Each run is freed, and its budget released, once folded.
    pub fn foldRuns(self: *PartAgg, threads: usize) !void {
        if (self.shared) return;
        var cursor: std.atomic.Value(usize) = .init(0);
        var failed: std.atomic.Value(bool) = .init(false);
        const n = @max(1, @min(threads, self.parts.len, 64));
        var tasks: [64]FoldTask = undefined;
        for (tasks[0..n]) |*t| t.* = .{ .agg = self, .cursor = &cursor, .failed = &failed };
        const config: std.Thread.SpawnConfig = .{ .stack_size = 1 << 20 };
        if (n == 1 or !spawn_util.runTree(FoldTask, tasks[0..n], config, FoldTask.run)) tasks[0].run();
        for (tasks[0..n]) |t| if (t.err) |e| return e;
    }

    fn foldPart(self: *PartAgg, p: usize) !void {
        const table = &self.parts[p].table;
        const len = self.calls.len;
        // Sized for every entry being a group of its own, the near-unique case where growth costs the most.
        if (self.stride != 0) {
            var n: usize = 0;
            for (self.runs) |*rs| n += rs[p].len();
            try table.reserveFixed(n, len);
        }
        for (self.runs) |*rs| {
            const run = &rs[p];
            for (0..run.len()) |i| {
                const id = if (self.stride != 0)
                    try table.getOrInsertFixed(run.key(self.stride, i), self.calls, run.first.items[i])
                else
                    try table.getOrInsert(run.key(self.stride, i), self.calls, run.first.items[i]);
                var scratch: [16]expr_agg.Accumulator = undefined;
                const dst = table.readGroup(id, len, &scratch);
                var src_scratch: [16]expr_agg.Accumulator = undefined;
                const src = run.readGroup(i, len, &src_scratch);
                for (dst, src) |*d, *s| try self.merge(d, s, self.gpa);
                table.writeGroup(id, dst);
            }
            run.free(self.gpa, self.budget);
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
        if (self.compact) table.compact.configure(self.calls);
    }
};

const FoldTask = struct {
    agg: *PartAgg,
    cursor: *std.atomic.Value(usize),
    failed: *std.atomic.Value(bool),
    err: ?anyerror = null,

    fn run(t: *FoldTask) void {
        while (!t.failed.load(.monotonic)) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= t.agg.parts.len) return;
            t.agg.foldPart(p) catch |e| {
                t.err = e;
                t.failed.store(true, .monotonic);
                return;
            };
        }
    }
};

/// Frees what an accumulator owns that `merge` would have moved: the string of a min or max.
fn freeOwned(gpa: std.mem.Allocator, acc: *expr_agg.Accumulator) void {
    switch (acc.*) {
        .min_bytes, .max_bytes => |b| if (b) |s| gpa.free(s),
        else => {},
    }
}

fn countMerge(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, _: std.mem.Allocator) anyerror!void {
    dst.count += src.count;
    src.count = 0;
}

test "partagg: partial flushes sum and keep the earliest position" {
    const a = std.testing.allocator;
    var budget = expr_agg.SharedBudget{ .limit = 1 << 20, .block = 4096 };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 0, 0, 2, false);
    defer part.deinit();
    var w0: Local = .{ .runs = &part.runs[0] };
    var w1: Local = .{ .runs = &part.runs[1] };

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
    try part.flush(&w1, &first_t);
    try std.testing.expectEqual(@as(usize, 0), first_t.groupCount());

    const a2 = try second.getOrInsert("alpha", &calls, 3);
    second.accumulators.items[a2].count = 5;
    const c_id = try second.getOrInsert("gamma", &calls, 20);
    second.accumulators.items[c_id].count = 4;
    try part.flush(&w0, &second);
    try part.foldRuns(3);

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
            var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 8, 4, 1, false);
            defer part.deinit();
            var w: Local = .{ .runs = &part.runs[0] };
            var local = expr_agg.GroupTable.init(a, 0);
            local.shared = &budget;
            defer local.deinit();
            for ([_][]const u8{ "a", "b", "c", "d" }, 0..) |key, i| {
                const id = try local.getOrInsert(key, &calls, @intCast(i + 1));
                local.accumulators.items[id].count = @intCast(i + 1);
            }
            try part.flush(&w, &local);
            try part.foldRuns(1);
            var total: u64 = 0;
            for (part.parts) |p| for (0..p.table.groupCount()) |g| {
                total += p.table.accumulators.items[g].count;
            };
            try std.testing.expectEqual(@as(u64, 10), total);
        }
    }.run, .{});
}

test "partagg: a run's budget charge is exactly what its lists hold after every growth" {
    const a = std.testing.allocator;
    var budget = expr_agg.SharedBudget{ .limit = 1 << 24, .block = 1 << 16 };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    for ([_]u8{ 0, 8 }) |stride| {
        var part = try PartAgg.init(a, &budget, &calls, stride, countMerge, 0, 0, 1, false);
        defer part.deinit();
        const run = &part.runs[0][0];
        var buf: [8]u8 = undefined;
        for (0..5000) |i| {
            std.mem.writeInt(u64, &buf, i, .little);
            // Variable keys of changing length grow the key bytes apart from the other lists.
            const key = if (stride == 0) buf[0 .. 1 + i % 8] else buf[0..];
            _ = try part.append(run, key, i);
            try std.testing.expectEqual(run.held(), run.charged);
        }
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "partagg: a run grows within a budget that holds exactly what it ends up holding" {
    // An estimate drawn before growing, larger than the growth, used to refuse a growth the budget held.
    const a = std.testing.allocator;
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const n = 3000;
    var buf: [24]u8 = undefined;
    // Long keys first, then short: the key bytes and the other lists grow at different times.
    const keyOf = struct {
        fn f(b: *[24]u8, i: usize) []const u8 {
            @memset(b, 'a' + @as(u8, @intCast(i % 26)));
            return b[0..if (i < n / 2) 24 else 2];
        }
    }.f;
    var held: usize = 0;
    {
        var budget = expr_agg.SharedBudget{ .limit = 1 << 30, .block = 1 << 16 };
        var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 0, 0, 1, false);
        defer part.deinit();
        for (0..n) |i| _ = try part.append(&part.runs[0][0], keyOf(&buf, i), i);
        held = part.runs[0][0].held();
    }
    var budget = expr_agg.SharedBudget{ .limit = held, .block = 1 << 16 };
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 0, 0, 1, false);
    defer part.deinit();
    for (0..n) |i| _ = try part.append(&part.runs[0][0], keyOf(&buf, i), i);
    try std.testing.expectEqual(held, part.runs[0][0].charged);
}

fn compactPartitionTrial(backing: std.mem.Allocator, shared: bool) !void {
    // In-place remaps depend on neighboring allocations; force copies so every failure index is reproducible.
    var no_resize = std.testing.FailingAllocator.init(backing, .{ .resize_fail_index = 0 });
    const gpa = no_resize.allocator();
    const arg: @import("expr/ast.zig").Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
    };
    var budget: expr_agg.SharedBudget = .{ .limit = 16 << 20 };
    const merge = struct {
        fn f(dst: *expr_agg.Accumulator, src: *expr_agg.Accumulator, a: std.mem.Allocator) anyerror!void {
            try dst.merge(src.*, a);
        }
    }.f;
    var p = try PartAgg.init(gpa, &budget, &calls, 0, merge, 4, 4, 1, shared);
    defer p.deinit();
    p.compact = true;
    for (p.parts) |*part| part.table.compact.configure(&calls);
    var table = expr_agg.GroupTable.init(gpa, 0);
    table.shared = &budget;
    table.compact.configure(&calls);
    defer table.deinit();
    var local: Local = .{ .runs = &p.runs[0] };
    for (0..48) |r| {
        const key = [_]u8{@intCast(r % 3)};
        const id = try table.getOrInsert(&key, &calls, r);
        var n = table.readAcc(id, 0, calls.len);
        n.count += 1;
        table.writeAcc(id, 0, calls.len, n);
        var avg = table.readAcc(id, 2, calls.len);
        avg.avg.isum += @as(i128, @intCast(r));
        avg.avg.count += 1;
        table.writeAcc(id, 2, calls.len, avg);
        if (r % 8 == 7) try p.flush(&local, &table);
    }
    try p.foldRuns(1);
    var groups: usize = 0;
    for (p.parts) |*part| for (0..part.table.groupCount()) |g| {
        const k: i128 = part.table.keyAt(g)[0];
        try std.testing.expectEqual(@as(u64, 16), part.table.readAcc(g, 0, calls.len).count);
        try std.testing.expectEqual(@as(?i128, 360 + 16 * k), part.table.readAcc(g, 1, calls.len).sum_i);
        const avg = part.table.readAcc(g, 2, calls.len).avg;
        try std.testing.expectEqual(@as(u64, 16), avg.count);
        try std.testing.expectEqual(@as(i128, 360 + 16 * k), avg.isum);
        try std.testing.expectEqual(@as(u64, @intCast(k)), part.table.first.items[g]);
        groups += 1;
    };
    try std.testing.expectEqual(@as(usize, 3), groups);
}

test "compact private runs and shared partitions preserve shared SUM AVG on allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactPartitionTrial, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactPartitionTrial, .{true});
}
