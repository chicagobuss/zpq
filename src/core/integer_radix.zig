//! Cache-bounded local SUM tables spill exact partials to radix partitions. A partition is merged by one
//! worker before top-H discards anything; scheduling never changes identities, NULLs or first-ordinal ties.
const std = @import("std");
const hw = @import("hw.zig");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const owners = @import("owner_groups.zig");
const SelectedBatch = @import("selected_batch.zig").SelectedBatch;
const now = @import("../clock.zig").monoNs;
pub const Group = owners.IntSumTop.Group;
const present: u32 = 1 << 31;
const null_key: u32 = 1 << 31;
const ordinal_mask = null_key - 1;
const Partial = extern struct { sum: i64, key: u64, count: u32, first: u32 };
const Records = @import("owner_records.zig").WriterRows(Partial, u32, 128);
const none = @import("owner_records.zig").none;
const Heap = std.PriorityQueue(Group, bool, worse);

fn worse(descending: bool, a: Group, b: Group) std.math.Order {
    const av = a.sumValue();
    const bv = b.sumValue();
    if (av != null and bv == null) return .gt;
    if (av == null and bv != null) return .lt;
    if (av != null) {
        const order = if (descending) std.math.order(av.?, bv.?) else std.math.order(bv.?, av.?);
        if (order != .eq) return order;
    }
    return std.math.order(b.ordinal(), a.ordinal());
}

fn hash(g: Group) u64 {
    if (g.first & null_key != 0) return 0x621985775bcbd930;
    const m = @as(u128, g.key ^ 0x9e3779b97f4a7c15) * 0xd6e8feb86659fd93;
    return @as(u64, @truncate(m)) ^ @as(u64, @truncate(m >> 64));
}
fn same(a: Group, b: Group) bool {
    return a.key == b.key and (a.first & null_key) == (b.first & null_key);
}
fn combine(a: *Group, b: Group) !void {
    const count = std.math.add(u32, a.count & ordinal_mask, b.count & ordinal_mask) catch return error.Overflow;
    if (count > ordinal_mask) return error.Overflow;
    const sum = std.math.add(i128, a.sum, b.sum) catch return error.Overflow;
    a.* = .{
        .key = a.key,
        .sum = sum,
        .count = count | ((a.count | b.count) & present),
        .first = @min(a.first, b.first),
    };
}

pub fn localCapacity(workers: usize) usize {
    const l2 = hw.l2BytesPerWorker();
    const l3 = hw.l3BytesPerActiveWorker(workers);
    if (l2 == 0 or l3 == 0) return 0;
    const entries = @min(l2 / 2, l3 / 4) / @sizeOf(Group);
    if (entries < 16) return 0;
    return @as(usize, 1) << @intCast(std.math.log2_int(usize, entries));
}

/// A costing hint, not a cardinality bound: all decoded rows still contribute. Leave a cache-sized domain and
/// small scans on their established paths; absent cache discovery declines this optional attempt.
pub fn admits(rows: usize, workers: usize, domain_hint: ?u64) bool {
    const cap = localCapacity(workers);
    return cap != 0 and workers > 1 and rows / workers >= cap *| 4 and
        (domain_hint orelse rows) > cap *| workers;
}

pub fn partitionCount(rows: usize, workers: usize, _: usize, forced: ?usize) usize {
    if (forced) |n| return std.math.ceilPowerOfTwo(usize, std.math.clamp(n, 64, 16384)) catch 16384;
    const l2 = hw.l2BytesPerWorkerOr(512 << 10);
    const l3 = hw.l3BytesPerActiveWorker(workers);
    const target = @max(l2, l3 / 2);
    const n = @max(workers * 4, rows *| @sizeOf(Group) / @max(1, target));
    return std.math.ceilPowerOfTwo(usize, std.math.clamp(n, 64, 16384)) catch 16384;
}

pub const Local = struct {
    state: *State,
    records: Records.Frontier = .{},
    entries: []Group = &.{},
    used: usize = 0,
    seen: usize = 0,
    contributed: usize = 0,
    bypass: bool = false,

    pub fn startErased(ctx: *anyopaque, _: u64, _: usize) void {
        const l: *Local = @ptrCast(@alignCast(ctx));
        l.bypass = false;
    }
    pub fn batchErased(ctx: *anyopaque, source: SelectedBatch, lookup: []const ?usize) !void {
        const l: *Local = @ptrCast(@alignCast(ctx));
        try source.validate();
        if (l.state.key_col >= lookup.len or l.state.value_col >= lookup.len) return error.BadColumn;
        const ki = lookup[l.state.key_col] orelse return error.BadColumn;
        const vi = lookup[l.state.value_col] orelse return error.BadColumn;
        if (ki >= source.batch.cols.len or vi >= source.batch.cols.len) return error.BadColumn;
        if (source.batch.cols[ki] != .i64 or source.batch.cols[vi] != .i64) return error.BadColumn;
        const k = source.batch.cols[ki].i64;
        const v = source.batch.cols[vi].i64;
        for (0..source.batch.num_rows) |i| {
            if (source.selected) |sel| if (!sel.isActive(i)) continue;
            const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
            const vn = if (v.def_levels) |d| d[i] < v.max_def else false;
            const key: ?i64 = if (kn) null else if (k.codes) |codes| k.dict[codes[i]] else k.values[i];
            const value: ?i64 = if (vn) null else if (v.codes) |codes| v.dict[codes[i]] else v.values[i];
            const ordinal = std.math.cast(u32, try source.ordinal(i)) orelse return error.Overflow;
            try l.append(key, value, ordinal);
        }
    }
    pub fn append(l: *Local, key: ?i64, value: ?i64, ordinal: u32) !void {
        if (ordinal >= l.state.rows or ordinal >= null_key) return error.BadRowCount;
        const g: Group = .{
            .key = if (key) |k| @bitCast(k) else 0,
            .sum = value orelse 0,
            .count = 1 | @as(u32, if (value != null) present else 0),
            .first = ordinal | @as(u32, if (key == null) null_key else 0),
        };
        const h = hash(g);
        if (l.bypass) {
            try l.emit(g, h);
        } else {
            var at: usize = @as(usize, @truncate(h)) & (l.entries.len - 1);
            while (l.entries[at].count != 0) : (at = (at + 1) & (l.entries.len - 1)) {
                if (same(l.entries[at], g)) {
                    const entry = &l.entries[at];
                    // Keep spill tuples as narrow as raw records. Overflow flushes the old exact partial;
                    // the partition merger widens every partial to checked i128 before adding it.
                    const sum = @addWithOverflow(@as(i64, @intCast(entry.sum)), @as(i64, @intCast(g.sum)));
                    if (sum[1] != 0) {
                        try l.emit(entry.*, h);
                        entry.* = g;
                    } else {
                        entry.sum = sum[0];
                        entry.count = ((entry.count & ordinal_mask) + 1) | ((entry.count | g.count) & present);
                        entry.first = @min(entry.first, g.first);
                    }
                    l.seen += 1;
                    l.contributed += 1;
                    return;
                }
            }
            l.entries[at] = g;
            l.used += 1;
            l.seen += 1;
            if (l.used >= l.entries.len * 3 / 4) {
                // Stop paying for probes when the measured reduction saves less than a quarter of spill
                // traffic; the next source can try its different distribution.
                const bypass = l.used * @sizeOf(Partial) >= l.seen * @sizeOf(owners.IntSumTop.Record) * 3 / 4;
                try l.flush();
                l.bypass = bypass;
            }
        }
        l.contributed += 1;
    }
    fn emit(l: *Local, g: Group, h: u64) !void {
        try Records.append(&l.records, l.state.blocks, &l.state.block_cursor, @intCast(h >> l.state.shift), .{
            .key = g.key,
            .sum = std.math.cast(i64, g.sum) orelse return error.Overflow,
            .count = g.count,
            .first = g.first,
        });
    }
    fn flush(l: *Local) !void {
        for (l.entries) |*g| if (g.count != 0) {
            try l.emit(g.*, hash(g.*));
            g.count = 0;
        };
        l.used = 0;
        l.seen = 0;
    }
};

pub const State = struct {
    gpa: std.mem.Allocator,
    ledger: ?*memory.SharedBudget,
    charged: usize,
    rows: usize,
    key_col: usize,
    value_col: usize,
    parts: usize,
    shift: u6,
    cut: owners.Cut,
    locals: []Local = &.{},
    blocks: []Records.Block = &.{},
    heaps: []Heap = &.{},
    block_cursor: std.atomic.Value(u32) = .init(0),
    partition_ns: u64 = 0,
    fold_ns: u64 = 0,
    group_count: usize = 0,

    pub fn reservation(rows: usize, workers: usize, parts: usize, k: usize, credit: usize) usize {
        // Each task's retained maximum belongs to a different completed partition, so summed merge scratch
        // is bounded by all emitted partials: one group plus <4 u32 slots per partial. Replacements free first.
        return @sizeOf(State) +| credit +| (8 << 20) +| rows *| (@sizeOf(Group) + 16) +|
            Records.blockCount(rows, workers, parts) *| @sizeOf(Records.Block) +|
            Records.frontierBytes(workers, parts) +| workers *| (localCapacity(workers) *| @sizeOf(Group) +|
            (1 << 20) + @sizeOf(Local) + @sizeOf(Heap) + @sizeOf(Task) +| k *| @sizeOf(Group));
    }
    pub fn init(
        a: std.mem.Allocator,
        ledger: ?*memory.SharedBudget,
        rows: usize,
        workers: usize,
        key_col: usize,
        value_col: usize,
        cut: owners.Cut,
        parts: usize,
        credit: usize,
    ) !?*State {
        if (!owners.admitsRows(.int64, rows) or workers == 0 or workers > owners.max_workers or
            cut.k == 0 or cut.k > 1024 or parts < 64 or parts > 16384 or !std.math.isPowerOfTwo(parts) or
            localCapacity(workers) == 0) return null;
        const charge = reservation(rows, workers, parts, cut.k, credit);
        if (ledger) |l| _ = l.drawExact(charge) orelse return null;
        const s = a.create(State) catch |err| {
            if (ledger) |l| l.release(charge);
            return err;
        };
        s.* = .{
            .gpa = a,
            .ledger = ledger,
            .charged = charge,
            .rows = rows,
            .key_col = key_col,
            .value_col = value_col,
            .parts = parts,
            .shift = @intCast(@as(usize, 64) - std.math.log2_int(usize, parts)),
            .cut = cut,
        };
        errdefer s.deinit();
        s.locals = try a.alloc(Local, workers);
        for (s.locals) |*l| l.* = .{ .state = s };
        for (s.locals) |*l| {
            l.entries = try a.alloc(Group, localCapacity(workers));
            @memset(l.entries, .{ .key = 0, .count = 0, .first = 0 });
            l.records = try Records.Frontier.init(a, parts);
        }
        s.heaps = try a.alloc(Heap, workers);
        for (s.heaps) |*h| h.* = .initContext(cut.descending);
        for (s.heaps) |*h| {
            const storage = try a.alloc(Group, cut.k);
            h.items = storage[0..0];
            h.cap = storage.len;
        }
        s.blocks = try a.alloc(Records.Block, Records.blockCount(rows, workers, parts));
        return s;
    }
    pub fn deinit(s: *State) void {
        const a = s.gpa;
        for (s.locals) |l| {
            l.records.deinit(a);
            a.free(l.entries);
        }
        a.free(s.locals);
        for (s.heaps) |*h| h.deinit(a);
        a.free(s.heaps);
        a.free(s.blocks);
        if (s.ledger) |l| l.release(s.charged);
        a.destroy(s);
    }
    fn keep(s: *State, heap: *Heap, g: Group) void {
        if (heap.count() == s.cut.k) {
            if (worse(s.cut.descending, g, heap.peek().?) != .gt) return;
            _ = heap.pop();
        }
        heap.push(s.gpa, g) catch unreachable;
    }
    const Task = struct {
        state: *State,
        heap: *Heap,
        cursor: *std.atomic.Value(usize),
        flush: bool = true,
        err: ?anyerror = null,
        groups: []Group = &.{},
        slots: []u32 = &.{},
        group_count: usize = 0,

        fn run(t: *Task) void {
            while (true) {
                const p = t.cursor.fetchAdd(1, .monotonic);
                if (p >= (if (t.flush) t.state.locals.len else t.state.parts)) return;
                (if (t.flush) t.state.locals[p].flush() else t.fold(p)) catch |err| {
                    t.err = err;
                    return;
                };
            }
        }
        fn fold(t: *Task, p: usize) !void {
            const s = t.state;
            var count: usize = 0;
            for (s.locals) |l| count += l.records.histogram[p];
            if (count == 0) return;
            const cap = try std.math.ceilPowerOfTwo(usize, count * 2);
            if (t.slots.len < cap) {
                s.gpa.free(t.slots);
                t.slots = &.{};
                t.slots = try s.gpa.alloc(u32, cap);
            }
            if (t.groups.len < count) {
                s.gpa.free(t.groups);
                t.groups = &.{};
                t.groups = try s.gpa.alloc(Group, count);
            }
            const slots = t.slots[0..cap];
            @memset(slots, 0);
            var n: usize = 0;
            for (s.locals) |l| {
                var id = l.records.heads[p];
                while (id != none) {
                    const b = &s.blocks[id];
                    if (b.next != none) @prefetch(&s.blocks[b.next], .{ .rw = .read, .locality = 1 });
                    for (b.rows[0..b.len]) |r| {
                        const g: Group = .{ .key = r.key, .sum = r.sum, .count = r.count, .first = r.first };
                        var at: usize = @as(usize, @truncate(hash(g))) & (cap - 1);
                        while (slots[at] != 0) : (at = (at + 1) & (cap - 1)) {
                            if (same(t.groups[slots[at] - 1], g)) break;
                        }
                        if (slots[at] == 0) {
                            t.groups[n] = g;
                            slots[at] = @intCast(n + 1);
                            n += 1;
                        } else try combine(&t.groups[slots[at] - 1], g);
                    }
                    id = b.next;
                }
            }
            for (t.groups[0..n]) |g| s.keep(t.heap, g);
            t.group_count += n;
        }
    };
    pub fn finishSelected(s: *State, selected: usize) !void {
        var contributed: usize = 0;
        for (s.locals) |l| contributed += l.contributed;
        if (contributed != selected or selected > s.rows) return error.BadRowCount;
        const tasks = try s.gpa.alloc(Task, s.locals.len);
        defer s.gpa.free(tasks);
        var cursor: std.atomic.Value(usize) = .init(0);
        for (tasks, s.heaps) |*t, *h| t.* = .{ .state = s, .heap = h, .cursor = &cursor };
        defer for (tasks) |t| {
            s.gpa.free(t.groups);
            s.gpa.free(t.slots);
        };
        var start = now();
        for ([_]bool{ true, false }) |flush| {
            cursor.store(0, .monotonic);
            for (tasks) |*t| t.flush = flush;
            if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run))
                Task.run(&tasks[0]);
            for (tasks) |t| if (t.err) |err| return err;
            if (flush) {
                s.partition_ns = @intCast(now() - start);
                start = now();
            }
        }
        s.fold_ns = @intCast(now() - start);
        for (tasks) |t| s.group_count += t.group_count;
        for (s.heaps[1..]) |h| for (h.items) |g| s.keep(&s.heaps[0], g);
        std.mem.sort(Group, s.heaps[0].items, {}, struct {
            fn less(_: void, a: Group, b: Group) bool {
                return a.ordinal() < b.ordinal();
            }
        }.less);
    }
    pub fn top(s: *const State) []const Group {
        return s.heaps[0].items;
    }
};

fn trial(a: std.mem.Allocator, workers: usize, descending: bool) !void {
    const rows = 4096;
    const ids = 129;
    const k: usize = if (workers == 1) ids else 17;
    var expected: [ids]Group = undefined;
    for (&expected, 0..) |*g, i| g.* = .{
        .key = if (i == 0) 0 else @bitCast(@as(i64, @intCast(i)) - 64),
        .count = 0,
        .first = std.math.maxInt(u32),
    };
    var ledger: memory.SharedBudget = .{ .limit = 64 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const s = (try State.init(a, &ledger, rows, workers, 0, 1, .{ .k = k, .descending = descending }, 64, 0)).?;
    defer s.deinit();
    // Reverse arrival and different source ownership exercise both local and partition merge ordinals.
    for (0..rows) |r| {
        const ordinal: u32 = @intCast(rows - 1 - r);
        const id = ordinal % ids;
        const key: ?i64 = if (id == 0) null else @bitCast(expected[id].key);
        const values = [_]i64{ std.math.minInt(i64), std.math.maxInt(i64), 0, 1, -1 };
        const value: ?i64 = if (id == 1 or ordinal % 7 == 0) null else values[ordinal % 5];
        try s.locals[(r / 37) % workers].append(key, value, ordinal);
        const g = &expected[id];
        g.sum += value orelse 0;
        g.count = ((g.count & ordinal_mask) + 1) | (g.count & present) |
            @as(u32, if (value != null) present else 0);
        g.first = @min(g.first, ordinal | @as(u32, if (key == null) null_key else 0));
    }
    try s.finishSelected(rows);
    try std.testing.expectEqual(@as(usize, ids), s.group_count);
    std.mem.sort(Group, &expected, descending, struct {
        fn less(desc: bool, a_: Group, b: Group) bool {
            const av = a_.sumValue();
            const bv = b.sumValue();
            if (av == null) return bv == null and a_.ordinal() < b.ordinal();
            if (bv == null) return true;
            if (av.? == bv.?) return a_.ordinal() < b.ordinal();
            return if (desc) av.? > bv.? else av.? < bv.?;
        }
    }.less);
    std.mem.sort(Group, expected[0..k], {}, struct {
        fn less(_: void, x: Group, y: Group) bool {
            return x.ordinal() < y.ordinal();
        }
    }.less);
    try std.testing.expectEqualDeep(expected[0..k], s.top());
}

test "integer radix exact NULL keys wide sums all-NULL values and ordinal ties across workers" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 4096 }, .l3 = .{ .bytes = 1 << 20 } };
    for ([_]usize{ 1, 2, 4, 8, 16 }) |workers| for ([_]bool{ true, false }) |descending|
        try trial(std.testing.allocator, workers, descending);
    for ([_]usize{ 0, 1, 3 }) |after| {
        spawn.injectFailureAfter(after);
        defer spawn.resetFailure();
        try trial(std.testing.allocator, 4, true);
    }
}

test "integer radix every failed allocation releases the ledger and all private state" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 4096 }, .l3 = .{ .bytes = 1 << 20 } };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, trial, .{ 1, true });
}

test "integer radix refuses unfunded state and invalid ordinal widths without retaining charges" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 4096 }, .l3 = .{ .bytes = 1 << 20 } };
    var ledger: memory.SharedBudget = .{ .limit = State.reservation(64, 1, 64, 1, 0) - 1 };
    try std.testing.expect((try State.init(
        std.testing.allocator,
        &ledger,
        64,
        1,
        0,
        1,
        .{ .k = 1, .descending = true },
        64,
        0,
    )) == null);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
    ledger.limit = std.math.maxInt(usize);
    try std.testing.expect((try State.init(
        std.testing.allocator,
        &ledger,
        1 << 31,
        1,
        0,
        1,
        .{ .k = 1, .descending = true },
        64,
        0,
    )) == null);
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "integer radix checked overflow is atomic and all-NULL is distinct from zero" {
    var g: Group = .{ .key = 1, .sum = std.math.maxInt(i128), .count = present | 1, .first = 0 };
    const before = g;
    try std.testing.expectError(error.Overflow, combine(&g, .{ .key = 1, .sum = 1, .count = present | 1, .first = 1 }));
    try std.testing.expectEqualDeep(before, g);
    var n: Group = .{ .key = 0, .count = 1, .first = null_key };
    try std.testing.expectEqual(@as(?i128, null), n.sumValue());
    try combine(&n, .{ .key = 0, .sum = 0, .count = present | 1, .first = null_key | 1 });
    try std.testing.expectEqual(@as(?i128, 0), n.sumValue());
}

test "integer radix bypasses nonreducing sources and retries hot NULLs exactly" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1024 }, .l3 = .{ .bytes = 1 << 20 } };
    const s = (try State.init(std.testing.allocator, null, 512, 1, 0, 1, .{ .k = 2, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const l = &s.locals[0];
    for (0..256) |r| try l.append(@intCast(r), 0, @intCast(r));
    try std.testing.expect(l.bypass);
    Local.startErased(l, 256, 1);
    for (256..512) |r| try l.append(null, 1, @intCast(r));
    try std.testing.expect(!l.bypass);
    try s.finishSelected(512);
    try std.testing.expectEqual(@as(usize, 257), s.group_count);
    try std.testing.expectEqual(@as(usize, 2), s.top().len);
    try std.testing.expectEqual(@as(?i64, 0), s.top()[0].keyValue());
    try std.testing.expectEqual(@as(?i64, null), s.top()[1].keyValue());
    try std.testing.expectEqual(@as(?i128, 256), s.top()[1].sumValue());
}

test "integer radix selected dictionary batches preserve original ordinals and ignore NULL payloads" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 4096 }, .l3 = .{ .bytes = 1 << 20 } };
    const s = (try State.init(std.testing.allocator, null, 100, 1, 0, 1, .{ .k = 3, .descending = true }, 64, 0)).?;
    defer s.deinit();
    const eval = @import("filter/eval.zig");
    const batch: eval.Batch = .{ .num_rows = 4, .cols = &.{
        .{ .i64 = .{
            .values = &.{},
            .dict = &.{ 0, -7 },
            .codes = &.{ 999, 0, 1, 0 },
            .def_levels = &.{ 0, 1, 1, 1 },
            .max_def = 1,
        } },
        .{ .i64 = .{
            .values = &.{},
            .dict = &.{ 0, 9 },
            .codes = &.{ 1, 999, 0, 0 },
            .def_levels = &.{ 1, 0, 1, 1 },
            .max_def = 1,
        } },
    } };
    const source: SelectedBatch = .{ .batch = &batch, .input_base = 40, .input_rows = &.{ 1, 3, 5, 7 } };
    try Local.batchErased(&s.locals[0], source, &.{ 0, 1 });
    try s.finishSelected(4);
    const got = s.top();
    try std.testing.expectEqual(@as(usize, 3), got.len);
    try std.testing.expectEqual(@as(?i64, null), got[0].keyValue());
    try std.testing.expectEqual(@as(?i128, 9), got[0].sumValue());
    try std.testing.expectEqual(@as(u32, 41), got[0].ordinal());
    try std.testing.expectEqual(@as(?i64, 0), got[1].keyValue());
    try std.testing.expectEqual(@as(?i128, 0), got[1].sumValue());
    try std.testing.expectEqual(@as(u32, 43), got[1].ordinal());
    try std.testing.expectEqual(@as(?i64, -7), got[2].keyValue());
    try std.testing.expectEqual(@as(u32, 45), got[2].ordinal());
}

test "integer radix capacities and admission follow runtime L2 L3 and work estimates" {
    defer hw.test_cpu = null;
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20 }, .l3 = .{ .bytes = 32 << 20 } };
    try std.testing.expectEqual(@as(usize, 16384), localCapacity(4));
    try std.testing.expect(admits(10_000_000, 4, 1_000_000));
    try std.testing.expect(!admits(10_000_000, 4, 1000));
    try std.testing.expect(admits(10_000_000, 4, null));
    try std.testing.expect(!admits(1000, 4, 1_000_000));
    hw.test_cpu.?.l3.bytes = 1 << 20;
    try std.testing.expectEqual(@as(usize, 2048), localCapacity(4));
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expect(!admits(10_000_000, 4, 1_000_000));
}
