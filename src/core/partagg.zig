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
const core_memory = @import("memory.zig");
const spawn_util = @import("spawn.zig");
const blocking = @import("blocking.zig");

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
    strings: ?*expr_agg.KeyArena = null,
    owner: ?*PartAgg = null,
    /// Bytes drawn from the shared budget for the capacities above.
    charged: usize = 0,

    pub fn len(self: *const Run) usize {
        return self.first.items.len;
    }

    fn keyEnd(current: usize, additional: usize) error{ExceededMemoryBudget}!u32 {
        const end = std.math.add(usize, current, additional) catch return error.ExceededMemoryBudget;
        return std.math.cast(u32, end) orelse error.ExceededMemoryBudget;
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
            self.accs.capacity * @sizeOf(expr_agg.Accumulator) + self.compact.held() +
            if (self.strings) |strings| strings.held() + @sizeOf(expr_agg.KeyArena) else 0;
    }

    fn reserveStrings(ctx: *anyopaque, n: usize) expr_agg.Error!void {
        const run: *Run = @ptrCast(@alignCast(ctx));
        run.charged += try run.owner.?.takeCredit(n);
    }

    fn settleStrings(ctx: *anyopaque) void {
        const run: *Run = @ptrCast(@alignCast(ctx));
        run.owner.?.settle(run);
    }

    fn free(self: *Run, gpa: std.mem.Allocator, budget: *expr_agg.SharedBudget) void {
        for (self.accs.items) |*acc| freeOwned(gpa, acc);
        self.keys.deinit(gpa);
        self.ends.deinit(gpa);
        self.first.deinit(gpa);
        self.accs.deinit(gpa);
        self.compact.words.deinit(gpa);
        if (self.strings) |strings| {
            strings.deinit(gpa);
            gpa.destroy(strings);
        }
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
    private: bool = false,
    private_checked: bool = false,
};

/// Rows an armed worker folds before looking at its table again. Large, as a window's fixed cost (the coded path
/// sizes its work by the dictionary) must stay small beside the rows it folds.
pub const armed_window: usize = 1 << 18;

pub const Part = struct {
    /// Held while a worker merges into `table` (`PartAgg.shared` only).
    lock: std.Io.Mutex align(64) = .init,
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
    /// Prepaid private run capacities, including a growth's old/new buffer overlap; the ledger grants any shortfall.
    run_credit: std.atomic.Value(usize) = .init(0),
    private_metadata: usize = 0,
    private_admitted: bool = false,
    private_reserved: bool = false,
    private_rows: u64 = 0,
    private_key_bytes: u64 = 0,
    /// Offered decoder floors return as soon as a reducing sample keeps the shared path.
    decoder_floors: []expr_agg.SharedBudget = &.{},
    sample_floors: []expr_agg.SharedBudget = &.{},
    private_lock: std.Io.Mutex = .init,
    private_decided: bool = false,
    compact: bool = false,
    /// Set by the first worker that moves its plain table here (`scan.MultiAggArgs.spill`).
    spilled: std.atomic.Value(bool) = .init(false),
    spill_groups: usize = 1 << 18,
    spill_rows: u64 = 4,

    pub fn init(
        input_gpa: std.mem.Allocator,
        budget: *expr_agg.SharedBudget,
        calls: []const expr_agg.AggCall,
        stride: u8,
        merge: MergeFn,
        cap: usize,
        probe: u64,
        n_workers: usize,
        shared: bool,
    ) !PartAgg {
        const gpa = core_memory.groupAllocator(input_gpa);
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
        self.budget.release(self.run_credit.load(.monotonic));
        // Runs and tables are partition-private after the scan/merge joins. Reclaim strings and winners beside
        // their partition; serial destruction otherwise costs a second full group-count tail after typed fill.
        const Task = struct {
            state: *PartAgg,
            cursor: *std.atomic.Value(usize),
            fn run(t: *@This()) void {
                const s = t.state;
                while (true) {
                    const p = t.cursor.fetchAdd(1, .monotonic);
                    if (p >= n_parts) return;
                    for (s.runs) |*rs| rs[p].free(s.gpa, s.budget);
                    if (p < s.parts.len) s.parts[p].table.deinit();
                }
            }
        };
        var storage: [64]Task = undefined;
        const tasks = storage[0..@max(1, @min(storage.len, self.runs.len))];
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*t| t.* = .{ .state = self, .cursor = &cursor };
        if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
        self.gpa.free(self.runs);
        self.gpa.free(self.parts);
        self.budget.release(self.private_metadata);
        self.* = undefined;
    }

    /// Reserve while decode is idle: concurrent decoder reservations must not crowd out a private plan's ceiling.
    /// Width is a hint, not a bound; growth asks the ledger for a shortfall before refusing the attempt.
    pub fn reservePrivate(self: *PartAgg, rows: u64, key_bytes: u64) bool {
        std.debug.assert(self.shared);
        const payload: u64 = if (self.compact)
            self.parts[0].table.compact.stride * 8
        else
            @sizeOf(expr_agg.Accumulator) * self.calls.len;
        const run_bytes = rows *| ((key_bytes +| payload +| 12) *| 2);
        const bits = partBits(self.budget.limit);
        const metadata = (@as(usize, 1) << bits) * @sizeOf(Part) + self.runs.len * @sizeOf(Runs);
        // The initial reservation takes at most half the ceiling; the ledger must grant it beside
        // the query's existing holders. Unused credit returns before the final partitions are folded.
        const total = run_bytes +| metadata;
        if (run_bytes == 0 or total > self.budget.limit / 2) return false;
        const charge = self.budget.drawExact(@intCast(run_bytes + metadata)) orelse return false;
        std.debug.assert(charge == total);
        self.run_credit.store(@intCast(run_bytes), .monotonic);
        self.private_metadata = metadata;
        self.private_reserved = true;
        return true;
    }

    pub fn cancelPrivate(self: *PartAgg) void {
        std.debug.assert(!self.private_admitted);
        self.budget.release(self.run_credit.swap(0, .monotonic) + self.private_metadata);
        self.private_metadata = 0;
        self.private_reserved = false;
        for (self.decoder_floors) |*b| b.releaseFloor();
        for (self.sample_floors) |*b| b.releaseFloor();
    }

    /// The sample decides before moving any group; a reducing window returns the prepaid ceiling unchanged.
    pub fn admitPrivate(self: *PartAgg, rows: u64, key_bytes: u64) !bool {
        if (!self.private_reserved and !self.reservePrivate(rows, key_bytes)) return false;
        const bits = partBits(self.budget.limit);
        const parts = try self.gpa.alloc(Part, @as(usize, 1) << bits);
        for (self.parts) |*p| {
            std.debug.assert(p.table.groupCount() == 0);
            p.table.deinit();
        }
        self.gpa.free(self.parts);
        for (parts) |*p| {
            p.* = .{ .table = expr_agg.GroupTable.init(self.gpa, 0) };
            p.table.shared = self.budget;
            p.table.fixed_stride = self.stride;
            if (self.compact) p.table.compact.configure(self.calls);
        }
        self.parts = parts;
        self.shift = @intCast(64 - @as(u7, bits));
        self.private_admitted = true;
        return true;
    }

    pub fn usesRuns(self: *const PartAgg, local: *const Local) bool {
        return !self.shared or local.private;
    }

    fn choosePrivate(self: *PartAgg, local: *Local, table: *const expr_agg.GroupTable) !void {
        if (self.private_rows == 0 or local.private_checked) return;
        // No worker can touch a partition until its first flush has passed this lock. The winner may replace the
        // still-empty partition array; every later worker sees that choice before routing or scattering a row.
        self.private_lock.lockUncancelable(blocking.io);
        defer self.private_lock.unlock(blocking.io);
        if (!self.private_decided) {
            self.private_decided = true;
            if (local.rows_since >= self.probe and table.groupCount() *| 10 >= local.rows_since *| 9) {
                _ = try self.admitPrivate(self.private_rows, self.private_key_bytes);
            } else self.cancelPrivate();
        }
        local.private_checked = true;
    }

    fn takeCredit(self: *PartAgg, need: usize) !usize {
        if (!self.private_admitted) return self.budget.drawExact(need) orelse error.ExceededMemoryBudget;
        var cur = self.run_credit.load(.monotonic);
        while (true) {
            if (cur >= need) {
                cur = self.run_credit.cmpxchgWeak(cur, cur - need, .monotonic, .monotonic) orelse return need;
                continue;
            }
            const prepaid = @min(cur, need);
            const extra = self.budget.drawExact(need - prepaid) orelse return error.ExceededMemoryBudget;
            if (self.run_credit.cmpxchgWeak(cur, cur - prepaid, .monotonic, .monotonic)) |actual| {
                // A competing draw or returned overlap changed the split; relinquish this top-up before retrying.
                self.budget.release(extra);
                cur = actual;
            } else return need;
        }
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
        try self.flushKeeping(local, table, !poor);
        local.rows_since = 0;
        if (poor) local.abandoned = true;
    }

    /// Append every local group to this worker's run for its partition, then drop the local table's memory back to the
    /// shared budget.
    pub fn flush(self: *PartAgg, local: *Local, table: *expr_agg.GroupTable) !void {
        if (table.groupCount() == 0) {
            self.reset(table, false);
            return;
        }
        return self.flushKeeping(local, table, false);
    }

    fn flushKeeping(self: *PartAgg, local: *Local, table: *expr_agg.GroupTable, retain: bool) !void {
        const n = table.groupCount();
        if (n == 0) return;
        try self.choosePrivate(local, table);
        if (self.private_admitted and local.rows_since >= self.probe and n *| 10 >= local.rows_since *| 9)
            local.private = true;
        if (!self.usesRuns(local)) return self.flushShared(table, retain);
        const runs = local.runs.?;
        const len = self.calls.len;
        for (0..n) |g| {
            const run = &runs[self.indexOf(table.routeHash(g))];
            const at = try self.append(run, table.keyAt(g), table.first.items[g]);
            var dst_scratch: [16]expr_agg.Accumulator = undefined;
            const dst = run.readGroup(at, len, &dst_scratch);
            var scratch: [16]expr_agg.Accumulator = undefined;
            const src = table.readGroup(g, len, &scratch);
            for (dst, src) |*d, *s| try expr_agg.mergePersistent(d, s, self.gpa, self.merge);
            run.writeGroup(at, dst);
        }
        self.reset(table, retain);
    }

    /// `flush` in shared mode: each partition's groups merged under its lock, `batch_n` at a time.
    fn flushShared(self: *PartAgg, local: *expr_agg.GroupTable, retain: bool) !void {
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
        self.reset(local, retain);
    }

    /// A merge holds a partition briefly, so a few spins usually find it free; past them the waiter sleeps rather
    /// than yield the CPU the holder needs.
    pub fn lock(part: *Part) void {
        for (0..64) |_| {
            if (part.lock.tryLock()) return;
            std.atomic.spinLoopHint();
        }
        part.lock.lockUncancelable(blocking.io);
    }

    pub fn unlock(part: *Part) void {
        part.lock.unlock(blocking.io);
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
            for (dst, src) |*d, *s| try expr_agg.mergePersistent(d, s, self.gpa, self.merge);
            part.table.writeGroup(id, dst);
        }
    }

    /// Append a fresh entry for `key`, first asked for at `first`, to `run`; returns its index. Growth is drawn from
    /// the shared budget before it is made.
    pub fn append(self: *PartAgg, run: *Run, raw_key: []const u8, first: u64) !usize {
        var image: [32]u8 = undefined;
        const key = if (self.stride == 0) raw_key else expr_agg.fixedImage(self.stride, raw_key, &image) orelse
            return error.BadGroupKey;
        // A skewed partition can reach the offset limit before the whole query's ledger is full.
        const end = if (self.stride == 0) try Run.keyEnd(run.keys.items.len, key.len) else 0;
        const len = self.calls.len;
        if (run.strings == null) for (self.calls) |call| {
            if (call.result != .bytes or (call.func != .min and call.func != .max)) continue;
            run.charged += try self.takeCredit(@sizeOf(expr_agg.KeyArena));
            const strings = try self.gpa.create(expr_agg.KeyArena);
            run.owner = self;
            strings.* = .{ .allocator = self.gpa, .owner = run, .reserveFn = Run.reserveStrings, .settleFn = Run.settleStrings };
            run.strings = strings;
            break;
        };
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
                accs_cap * @sizeOf(expr_agg.Accumulator) + compact_cap * @sizeOf(u64) +
                if (run.strings) |strings| strings.held() + @sizeOf(expr_agg.KeyArena) else 0;
            if (target > run.charged) {
                run.charged += try self.takeCredit(target - run.charged);
            }
            // A list that failed to grow leaves the others grown: the charge falls back to what is held.
            defer self.settle(run);
            // ArrayList may allocate a replacement before freeing its old buffer. Credit covers that temporary
            // overlap too; it is still part of the prepaid reservation while the allocation is in flight.
            const overlap = if (self.private_admitted) try self.takeCredit(run.held()) else 0;
            defer {
                if (overlap != 0) _ = self.run_credit.fetchAdd(overlap, .monotonic);
            }
            try run.first.ensureTotalCapacityPrecise(self.gpa, first_cap);
            try run.keys.ensureTotalCapacityPrecise(self.gpa, keys_cap);
            if (self.stride == 0) try run.ends.ensureTotalCapacityPrecise(self.gpa, ends_cap);
            try run.accs.ensureTotalCapacityPrecise(self.gpa, accs_cap);
            try run.compact.words.ensureTotalCapacityPrecise(self.gpa, compact_cap);
        }
        const at = run.first.items.len;
        run.keys.appendSliceAssumeCapacity(key);
        if (self.stride == 0) run.ends.appendAssumeCapacity(end);
        run.first.appendAssumeCapacity(first);
        if (run.compact.stride != 0) {
            run.compact.append();
        } else {
            for (self.calls) |call| run.accs.appendAssumeCapacity(if (call.result == .bytes and call.func == .min)
                .{ .min_arena = .{ .store = run.strings.? } }
            else if (call.result == .bytes and call.func == .max)
                .{ .max_arena = .{ .store = run.strings.? } }
            else
                expr_agg.Accumulator.init(call));
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
        if (self.shared and !self.private_admitted) {
            self.cancelPrivate();
            return;
        }
        self.budget.release(self.run_credit.swap(0, .monotonic));
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
                for (dst, src) |*d, *s| try expr_agg.mergePersistent(d, s, self.gpa, self.merge);
                table.writeGroup(id, dst);
            }
            run.free(self.gpa, self.budget);
        }
    }

    fn reset(self: *PartAgg, table: *expr_agg.GroupTable, retain: bool) void {
        // Keep capacity only for useful local reductions; abandoned and final tables return their charge.
        if (retain) {
            table.clearRetainingCapacity();
            return;
        }
        const gpa = table.allocator;
        const shared = table.shared;
        const stride = table.fixed_stride;
        table.deinit();
        if (!retain) if (shared) |b| {
            if (b.parent == self.budget) {
                // Empty workers can finish before the sample decides. Keep their credit prepaid, with returns
                // ordered against admission/cancellation under the same lock as nonempty workers' first flush.
                self.private_lock.lockUncancelable(blocking.io);
                defer self.private_lock.unlock(blocking.io);
                if (self.private_reserved) b.transferIdleFloor(&self.run_credit) else b.releaseFloor();
            } else b.releaseFloor();
        };
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

test "partagg: a partition's lock admits one merging worker at a time" {
    var part: Part = .{ .table = expr_agg.GroupTable.init(std.testing.allocator, 0) };
    defer part.table.deinit();
    const Worker = struct {
        fn run(p: *Part, n: *usize) void {
            for (0..20_000) |_| {
                PartAgg.lock(p);
                defer PartAgg.unlock(p);
                n.* += 1;
            }
        }
    };
    var n: usize = 0;
    var threads: [4]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &part, &n });
    for (threads) |t| t.join();
    try std.testing.expectEqual(@as(usize, 80_000), n);
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

test "partagg: useful fixed flush retains capacity but final flush releases it" {
    const a = std.testing.allocator;
    var budget = expr_agg.SharedBudget{ .limit = 1 << 20, .block = 1024 };
    const calls = [_]expr_agg.AggCall{.{
        .func = .count,
        .arg = null,
        .where = null,
        .alias = "c",
        .result = .i64,
    }};
    var part = try PartAgg.init(a, &budget, &calls, 8, countMerge, 4, 1, 1, false);
    defer part.deinit();
    var w: Local = .{ .runs = &part.runs[0] };
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    table.fixed_stride = 8;
    defer table.deinit();
    var key: [8]u8 = @splat(0);
    for (0..4) |g| {
        std.mem.writeInt(u64, &key, g, .little);
        const id = try table.getOrInsertFixed(&key, &calls, g);
        table.accumulators.items[id].count = 2;
    }
    const held = table.allocated_bytes;
    try part.pump(&w, &table, 8);
    try std.testing.expectEqual(@as(usize, 0), table.groupCount());
    try std.testing.expectEqual(held, table.allocated_bytes);
    try std.testing.expect(!w.abandoned);
    try part.flush(&w, &table);
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.max_memory_bytes);
    const id = try table.getOrInsertFixed(&key, &calls, 9);
    table.accumulators.items[id].count = 1;
    try part.flush(&w, &table);
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.max_memory_bytes);
    try part.foldRuns(1);
    var total: u64 = 0;
    for (part.parts) |p| for (0..p.table.groupCount()) |g| {
        total += p.table.accumulators.items[g].count;
    };
    try std.testing.expectEqual(@as(u64, 9), total);
}

test "partagg: private admission declines before touching shared partitions or their ledger" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 20 };
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
    defer part.deinit();
    const original = part.parts.ptr;
    try std.testing.expect(!try part.admitPrivate(std.math.maxInt(u64), std.math.maxInt(u64)));
    try std.testing.expect(!try part.admitPrivate(0, 64));
    try std.testing.expectEqual(original, part.parts.ptr);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try std.testing.expect(!part.private_admitted);
}

test "partagg: admitted private and shared workers combine complete counts and minimum positions" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 2, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    try std.testing.expect(try part.admitPrivate(128, 64));
    const reserved = budget.used.load(.monotonic);
    try std.testing.expectEqual(reserved, part.run_credit.load(.monotonic) + part.private_metadata);
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    table.compact.configure(&calls);
    defer table.deinit();
    var private: Local = .{ .runs = &part.runs[0] };
    for ([_][]const u8{ "alpha", "beta", "gamma", "delta" }, 0..) |key, i| {
        const g = try table.getOrInsert(key, &calls, i + 10);
        table.compact.write(g, 0, .{ .count = 1 });
    }
    try part.pump(&private, &table, 4);
    try std.testing.expect(private.private);
    try std.testing.expectEqual(@as(usize, 0), table.groupCount());
    var shared: Local = .{ .runs = &part.runs[1] };
    const g = try table.getOrInsert("alpha", &calls, 2);
    table.compact.write(g, 0, .{ .count = 8 });
    try part.pump(&shared, &table, 8);
    try std.testing.expect(!shared.private);
    try part.flush(&shared, &table);
    try part.foldRuns(3);
    var groups: usize = 0;
    for (part.parts) |*p| {
        groups += p.table.groupCount();
        if (p.table.findKey("alpha")) |id| {
            try std.testing.expectEqual(@as(u64, 9), p.table.compact.read(id, 0).count);
            try std.testing.expectEqual(@as(u64, 2), p.table.first.items[id]);
        }
    }
    try std.testing.expectEqual(@as(usize, 4), groups);
    try std.testing.expectEqual(@as(usize, 0), part.run_credit.load(.monotonic));
}

test "partagg: refused private credit leaves a local accumulator unconsumed" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 1, 1, 1, true);
    defer part.deinit();
    try std.testing.expect(try part.admitPrivate(128, 64));
    budget.release(part.run_credit.swap(0, .monotonic));
    var local: Local = .{ .runs = &part.runs[0] };
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    defer table.deinit();
    const g = try table.getOrInsert("alpha", &calls, 7);
    table.accumulators.items[g].count = 1;
    const other = budget.drawExact(budget.available()).?;
    defer budget.release(other);
    try std.testing.expectError(error.ExceededMemoryBudget, part.pump(&local, &table, 1));
    try std.testing.expectEqual(@as(usize, 1), table.groupCount());
    try std.testing.expectEqual(@as(u64, 1), table.accumulators.items[g].count);
    try std.testing.expectEqual(@as(u64, 7), table.first.items[g]);
    for (part.runs[0]) |r| try std.testing.expectEqual(@as(usize, 0), r.len());
}

test "partagg: underestimated private keys grow from ledger room without losing counts or charges" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    try std.testing.expect(try part.admitPrivate(128, 32));
    const credit = part.run_credit.load(.monotonic);
    var key: [2048]u8 = @splat('p');
    for (0..128) |i| {
        std.mem.writeInt(u64, key[0..8], i, .little);
        const run = &part.runs[0][part.indexOf(expr_agg.sketchHash(&key))];
        const g = try part.append(run, &key, i);
        run.compact.write(g, 0, .{ .count = 1 });
    }
    var held: usize = 0;
    for (part.runs[0]) |r| held += r.charged;
    try std.testing.expect(held > credit);
    try std.testing.expectEqual(
        held + part.private_metadata + part.run_credit.load(.monotonic),
        budget.used.load(.monotonic),
    );
    try part.foldRuns(3);
    var groups: usize = 0;
    for (part.parts) |*p| {
        groups += p.table.groupCount();
        for (0..p.table.groupCount()) |g| try std.testing.expectEqual(@as(u64, 1), p.table.compact.read(g, 0).count);
    }
    try std.testing.expectEqual(@as(usize, 128), groups);
}

test "partagg: a refused top-up preserves prepaid credit and the shared ledger" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
    defer part.deinit();
    try std.testing.expect(try part.admitPrivate(128, 32));
    const credit = part.run_credit.load(.monotonic);
    const other = budget.drawExact(budget.available()).?;
    defer budget.release(other);
    try std.testing.expectError(error.ExceededMemoryBudget, part.takeCredit(credit + 1));
    try std.testing.expectEqual(credit, part.run_credit.load(.monotonic));
    try std.testing.expectEqual(budget.limit, budget.used.load(.monotonic));
}

test "partagg: competing private top-ups charge each byte exactly once" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 4, true);
    defer part.deinit();
    try std.testing.expect(try part.admitPrivate(128, 32));
    const credit = part.run_credit.load(.monotonic);
    const Work = struct {
        part: *PartAgg,
        cursor: std.atomic.Value(usize) = .init(0),
        failed: std.atomic.Value(bool) = .init(false),
        fn run(ctx: **@This()) void {
            const w = ctx.*;
            while (w.cursor.fetchAdd(1, .monotonic) < 4096) {
                _ = w.part.takeCredit(32) catch {
                    w.failed.store(true, .monotonic);
                    return;
                };
            }
        }
    };
    var work: Work = .{ .part = &part };
    var workers: [4]*Work = @splat(&work);
    if (!spawn_util.runTree(*Work, &workers, .{}, Work.run)) Work.run(&workers[0]);
    try std.testing.expect(!work.failed.load(.monotonic));
    try std.testing.expect(4096 * 32 > credit);
    try std.testing.expectEqual(
        part.private_metadata + part.run_credit.load(.monotonic) + 4096 * 32,
        budget.used.load(.monotonic),
    );
    budget.release(4096 * 32);
}

test "partagg: every private admission and append allocation failure returns all charged bytes" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
            defer std.debug.assert(budget.used.load(.monotonic) == 0);
            const calls = [_]expr_agg.AggCall{.{
                .func = .count,
                .arg = null,
                .where = null,
                .alias = "c",
                .result = .i64,
            }};
            var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
            defer part.deinit();
            try std.testing.expect(try part.admitPrivate(1, 1));
            var local: Local = .{ .runs = &part.runs[0] };
            var table = expr_agg.GroupTable.init(a, 0);
            table.shared = &budget;
            defer table.deinit();
            for ([_][]const u8{ "alpha", "beta", "gamma", "delta" }, 0..) |key, i| {
                const g = try table.getOrInsert(key, &calls, i);
                table.accumulators.items[g].count = 1;
            }
            try part.pump(&local, &table, 4);
            try part.foldRuns(1);
        }
    }.run, .{});
}

test "partagg: a reducing first window keeps the original shared partitions and reserves no runs" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
    defer part.deinit();
    part.private_rows = 128;
    part.private_key_bytes = 64;
    const original = part.parts.ptr;
    var local: Local = .{ .runs = &part.runs[0] };
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    defer table.deinit();
    const g = try table.getOrInsert("alpha", &calls, 7);
    table.accumulators.items[g].count = 8;
    try part.pump(&local, &table, 8);
    try part.flush(&local, &table);
    try std.testing.expectEqual(original, part.parts.ptr);
    try std.testing.expect(part.private_decided and !part.private_admitted and !local.private);
    try std.testing.expectEqual(@as(usize, 0), part.private_metadata);
    try std.testing.expectEqual(@as(usize, 0), part.run_credit.load(.monotonic));
}

test "partagg: a run refuses an unrepresentable key end before appending" {
    const t = std.testing;
    const last = std.math.maxInt(u32);
    try t.expectEqual(@as(u32, last), try Run.keyEnd(last - 7, 7));
    try t.expectError(error.ExceededMemoryBudget, Run.keyEnd(last, 1));
    try t.expectError(error.ExceededMemoryBudget, Run.keyEnd(std.math.maxInt(usize), 1));
}

test "partagg: a reducing sample returns its prepaid ceiling without replacing shared partitions" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 1, true);
    defer part.deinit();
    const original = part.parts.ptr;
    try std.testing.expect(part.reservePrivate(128, 64));
    try std.testing.expect(part.private_reserved and budget.used.load(.monotonic) > 0);
    try std.testing.expectEqual(original, part.parts.ptr);
    // An empty or fully filtered query never reaches a sample.
    try part.foldRuns(1);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try std.testing.expect(!part.private_reserved);
    try std.testing.expect(part.reservePrivate(128, 64));
    part.private_rows = 128;
    part.private_key_bytes = 64;
    _ = budget.drawExact(2 << 20).?;
    var decoders = [_]expr_agg.SharedBudget{
        .{ .limit = budget.limit, .parent = &budget, .funded = 1 << 20 },
        .{ .limit = budget.limit, .parent = &budget, .funded = 1 << 20 },
    };
    defer for (&decoders) |*d| d.releaseFloor();
    _ = decoders[0].drawExact(4096).?;
    defer decoders[0].release(4096);
    part.decoder_floors = &decoders;
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &budget;
    defer table.deinit();
    const id = try table.getOrInsert("repeated", &calls, 0);
    table.accumulators.items[id].count = 32;
    var local: Local = .{ .runs = &part.runs[0], .rows_since = 32 };
    try part.flush(&local, &table);
    try std.testing.expectEqual(original, part.parts.ptr);
    try std.testing.expect(!part.private_reserved and !part.private_admitted);
    try std.testing.expectEqual(@as(usize, 0), part.run_credit.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), part.private_metadata);
    for (&decoders) |*d| try std.testing.expectEqual(@as(usize, 0), d.funded);
    try std.testing.expectEqual(@as(usize, 4096), decoders[0].used.load(.monotonic));
}

test "partagg: an admitted sample returns idle floors to run credit even with a full parent" {
    const a = std.testing.allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 2, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    try std.testing.expect(try part.admitPrivate(65536, 128));
    const floor = 256 << 10;
    _ = part.run_credit.fetchSub(floor, .monotonic);
    const credit = part.run_credit.load(.monotonic);
    var sample: expr_agg.SharedBudget = .{ .limit = budget.limit, .parent = &budget, .funded = floor };
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &sample;
    table.compact.configure(&calls);
    defer table.deinit();
    _ = try table.getOrInsert("sample key", &calls, 0);
    const rest = budget.drawExact(budget.available()).?;
    defer budget.release(rest);
    part.reset(&table, true);
    try std.testing.expectEqual(floor, sample.funded);
    try std.testing.expectEqual(credit, part.run_credit.load(.monotonic));
    part.reset(&table, false);
    try std.testing.expectEqual(@as(usize, 0), sample.funded);
    try std.testing.expectEqual(budget.limit, budget.used.load(.monotonic));
    try std.testing.expectEqual(credit + floor, part.run_credit.load(.monotonic));
    part.reset(&table, false);
    try std.testing.expectEqual(credit + floor, part.run_credit.load(.monotonic));
    // No parent room exists: the returned credit alone must fund a run, with no top-up race.
    try std.testing.expectEqual(floor, try part.takeCredit(floor));
    budget.release(floor);
}

fn checkEmptySampleBeforeAdmission(a: std.mem.Allocator, admit_first: bool) !void {
    var budget: expr_agg.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var part = try PartAgg.init(a, &budget, &calls, 0, countMerge, 4, 4, 2, true);
    defer part.deinit();
    part.compact = true;
    for (part.parts) |*p| p.table.compact.configure(&calls);
    try std.testing.expect(part.reservePrivate(65536, 128));
    const floor = 256 << 10;
    _ = part.run_credit.fetchSub(floor, .monotonic);
    const credit = part.run_credit.load(.monotonic);
    var sample: expr_agg.SharedBudget = .{ .limit = budget.limit, .parent = &budget, .funded = floor };
    defer sample.releaseFloor();
    var table = expr_agg.GroupTable.init(a, 0);
    table.shared = &sample;
    defer table.deinit();
    var local: Local = .{ .runs = &part.runs[0] };
    if (admit_first) try std.testing.expect(try part.admitPrivate(65536, 128));
    const before = budget.used.load(.monotonic);
    try part.flush(&local, &table);
    try std.testing.expectEqual(before, budget.used.load(.monotonic));
    try std.testing.expectEqual(@as(usize, 0), sample.funded);
    try std.testing.expectEqual(credit + floor, part.run_credit.load(.monotonic));
    if (!admit_first) try std.testing.expect(try part.admitPrivate(65536, 128));
}

test "partagg: an empty worker preserves sample credit before or after the first admission" {
    try checkEmptySampleBeforeAdmission(std.testing.allocator, false);
    try checkEmptySampleBeforeAdmission(std.testing.allocator, true);
}
