//! Inline integer records, cache-sized radix partitions and exact first-position winners.
//! The caller admits a bare signed integer key with COUNT(*) + SUM, or a bounded cut by one exact integer SUM.
const std = @import("std");
const eval = @import("filter/eval.zig");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const now = @import("../clock.zig").monoNs;

const coarse_bits = 6;
const fine_bits = 5;
const coarse_n = 1 << coarse_bits;
const fine_n = 1 << fine_bits;
const parts_n = coarse_n * fine_n;
const block_rows = 256;
const none = std.math.maxInt(u32);
const flag: u32 = 1 << 31;
const mask = flag - 1;

pub const Record = extern struct { key: i64, value: i64, ordinal: u32, flags: u32 };
const Block = struct { rows: [block_rows]Record, next: u32 = none, len: u32 = 0 };
pub const Group = struct {
    sum: i128,
    key: i64,
    /// High bit means at least one non-NULL SUM value.
    count: u32,
    /// High bit means a NULL key; low bits are the exact input ordinal.
    first: u32,
    pub fn keyValue(g: Group) ?i128 {
        return if (g.first & flag != 0) null else g.key;
    }
    pub fn countValue(g: Group) i128 {
        return g.count & mask;
    }
    pub fn sumValue(g: Group) ?i128 {
        return if (g.count & flag == 0) null else g.sum;
    }
};

pub const Cut = struct { k: usize, descending: bool };

// The minimum heap item is the worst retained row. NULL is last in either direction, and an earlier input ordinal
// wins an equal SUM, exactly as the stable result sort does. The NULL-key flag is not part of the ordinal.
fn worseSum(descending: bool, a: Group, b: Group) std.math.Order {
    const an = a.count & flag == 0;
    const bn = b.count & flag == 0;
    if (an != bn) return if (an) .lt else .gt;
    if (!an) {
        const ord = std.math.order(a.sum, b.sum);
        if (ord != .eq) return if (descending) ord else ord.invert();
    }
    return std.math.order(b.first & mask, a.first & mask);
}

const SumHeap = std.PriorityQueue(Group, bool, worseSum);

fn keepSum(heap: *SumHeap, a: std.mem.Allocator, k: usize, g: Group) void {
    if (heap.count() == k) {
        if (worseSum(heap.context, g, heap.peek().?) != .gt) return;
        _ = heap.pop();
    }
    heap.push(a, g) catch unreachable; // Every worker's bounded capacity is reserved before decoding.
}

const FoldScratch = struct {
    groups: []Group = &.{},
    slots: []u32 = &.{},

    fn deinit(s: *FoldScratch, a: std.mem.Allocator) void {
        a.free(s.groups);
        a.free(s.slots);
    }

    fn ensure(s: *FoldScratch, a: std.mem.Allocator, rows: usize, cap: usize) !void {
        // Free before growing: old/new overlap would exceed the admitted bound on an adversarially skewed partition.
        // Each worker retains at most its largest partition; those maxima name disjoint partitions, totaling <= N.
        if (s.groups.len < rows) {
            a.free(s.groups);
            s.groups = &.{};
            s.groups = try a.alloc(Group, rows);
        }
        if (s.slots.len < cap) {
            a.free(s.slots);
            s.slots = &.{};
            s.slots = try a.alloc(u32, cap);
        }
    }
};

inline fn hash(key: i64, is_null: bool) u64 {
    var h: u64 = if (is_null) 0x621985775bcbd930 else @bitCast(key);
    h ^= h >> 33;
    h *%= 0xff51afd7ed558ccd;
    h ^= h >> 33;
    h *%= 0xc4ceb9fe1a85ec53;
    return h ^ (h >> 33);
}

pub const Local = struct {
    state: *State,
    key_col: usize,
    value_col: usize,
    ordinal_base: u32 = 0,
    heads: [coarse_n]u32 = @splat(none),
    tails: [coarse_n]u32 = @splat(none),
    histogram: [parts_n]u32 = @splat(0),
    nulls: u32 = 0,

    fn append(l: *Local, record: Record) !void {
        l.nulls |= record.flags & 3;
        const h = hash(record.key, record.flags & 1 != 0);
        l.histogram[h >> (64 - coarse_bits - fine_bits)] += 1;
        const p = h >> (64 - coarse_bits);
        var tail = l.tails[p];
        if (tail == none or l.state.blocks[tail].len == block_rows) {
            const fresh = l.state.block_cursor.fetchAdd(1, .monotonic);
            if (fresh >= l.state.blocks.len) return error.ExceededMemoryBudget;
            l.state.blocks[fresh].next = none;
            l.state.blocks[fresh].len = 0;
            if (tail == none) l.heads[p] = fresh else l.state.blocks[tail].next = fresh;
            l.tails[p] = fresh;
            tail = fresh;
        }
        const b = &l.state.blocks[tail];
        b.rows[b.len] = record;
        b.len += 1;
    }

    pub fn batch(l: *Local, batch_view: eval.Batch, lookup: []const ?usize, offset: usize) !void {
        const k = batch_view.cols[lookup[l.key_col].?].i64;
        const v = batch_view.cols[lookup[l.value_col].?].i64;
        for (0..batch_view.num_rows) |i| {
            const kn = if (k.def_levels) |d| d[i] < k.max_def else false;
            const vn = if (v.def_levels) |d| d[i] < v.max_def else false;
            try l.append(.{
                .key = if (kn) 0 else if (k.codes) |codes| k.dict[codes[i]] else k.values[i],
                .value = if (vn) 0 else if (v.codes) |codes| v.dict[codes[i]] else v.values[i],
                .ordinal = l.ordinal_base + @as(u32, @intCast(offset + i)),
                .flags = @as(u32, @intFromBool(kn)) | (@as(u32, @intFromBool(vn)) << 1),
            });
        }
    }
};

pub const State = struct {
    gpa: std.mem.Allocator,
    ledger: ?*memory.SharedBudget,
    charged: usize,
    rows: usize,
    blocks: []Block = &.{},
    block_cursor: std.atomic.Value(u32) = .init(0),
    locals: []Local = &.{},
    groups: []Group = &.{},
    records: []Record = &.{},
    cut: ?Cut = null,
    heaps: []SumHeap = &.{},
    winners: []u32 = &.{},
    bitmap: []u64 = &.{},
    prefix: []u32 = &.{},
    starts: [parts_n + 1]usize = @splat(0),
    counts: [parts_n]usize = @splat(0),
    group_count: usize = 0,
    /// Conservative output NULL possibilities from the exact input flags: key bit 0, SUM bit 1. No input NULLs
    /// proves no output NULLs; a NULL SUM input may still belong to a group with a present value.
    input_nulls: u32 = 0,
    scatter_ns: u64 = 0,
    fold_ns: u64 = 0,
    order_ns: u64 = 0,

    pub fn reservation(rows: usize) usize {
        return rows *| 88 +| (128 << 20);
    }

    pub fn init(
        a: std.mem.Allocator,
        ledger: ?*memory.SharedBudget,
        rows: usize,
        workers: usize,
        key_col: usize,
        value_col: usize,
    ) !?*State {
        if (rows >= flag or workers > 64) return null;
        const charge = reservation(rows);
        if (ledger) |l| _ = l.drawExact(charge) orelse return null;
        const s = a.create(State) catch |e| {
            if (ledger) |l| l.release(charge);
            return e;
        };
        s.* = .{ .gpa = a, .ledger = ledger, .charged = charge, .rows = rows };
        errdefer s.deinit();
        s.blocks = try a.alloc(Block, (rows + block_rows - 1) / block_rows + workers * coarse_n);
        s.locals = try a.alloc(Local, workers);
        for (s.locals) |*l| l.* = .{ .state = s, .key_col = key_col, .value_col = value_col };
        return s;
    }

    pub fn deinit(s: *State) void {
        const a = s.gpa;
        a.free(s.blocks);
        a.free(s.locals);
        a.free(s.groups);
        a.free(s.records);
        for (s.heaps) |*heap| heap.deinit(a);
        a.free(s.heaps);
        a.free(s.winners);
        a.free(s.bitmap);
        a.free(s.prefix);
        if (s.ledger) |l| l.release(s.charged);
        a.destroy(s);
    }

    pub fn top(
        a: std.mem.Allocator,
        ledger: ?*memory.SharedBudget,
        rows: usize,
        workers: usize,
        key_col: usize,
        value_col: usize,
        cut: Cut,
    ) !?*State {
        if (cut.k == 0 or cut.k > 1024 or workers == 0) return null;
        const s = (try init(a, ledger, rows, workers, key_col, value_col)) orelse return null;
        errdefer s.deinit();
        s.cut = cut;
        s.heaps = try a.alloc(SumHeap, workers);
        for (s.heaps) |*heap| heap.* = .initContext(cut.descending);
        for (s.heaps) |*heap| try heap.ensureTotalCapacity(a, cut.k);
        return s;
    }

    const Task = struct {
        s: *State,
        cursor: *std.atomic.Value(usize),
        pass: enum { scatter, fold, top_scatter, top_fold },
        heap: ?*SumHeap = null,
        err: ?anyerror = null,
        fn run(t: *Task) void {
            var scratch: FoldScratch = .{};
            defer scratch.deinit(t.s.gpa);
            const end: usize = if (t.pass == .scatter or t.pass == .top_scatter) coarse_n else parts_n;
            while (true) {
                const p = t.cursor.fetchAdd(1, .monotonic);
                if (p >= end) return;
                (switch (t.pass) {
                    .scatter => t.s.scatter(p, false),
                    .top_scatter => t.s.scatter(p, true),
                    .fold => t.s.fold(p),
                    .top_fold => t.s.foldTop(p, &scratch, t.heap.?),
                }) catch |e| {
                    t.err = e;
                    return;
                };
            }
        }
    };

    fn parallel(s: *State, workers: usize, pass: @FieldType(Task, "pass")) !void {
        const tasks = try s.gpa.alloc(Task, @max(1, workers));
        defer s.gpa.free(tasks);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks, 0..) |*t, i| t.* = .{
            .s = s,
            .cursor = &cursor,
            .pass = pass,
            .heap = if (pass == .top_fold) &s.heaps[i] else null,
        };
        if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run)) Task.run(&tasks[0]);
        for (tasks) |t| if (t.err) |e| return e;
    }

    fn scatter(s: *State, coarse: usize, comptime top_cut: bool) void {
        const base = coarse * fine_n;
        var next: [fine_n]usize = undefined;
        for (&next, 0..) |*n, i| n.* = s.starts[base + i];
        for (s.locals) |l| {
            var id = l.heads[coarse];
            while (id != none) {
                const b = &s.blocks[id];
                for (b.rows[0..b.len]) |r| {
                    const p = (hash(r.key, r.flags & 1 != 0) >> (64 - coarse_bits - fine_bits)) & (fine_n - 1);
                    const start = s.starts[base + p];
                    const records: [*]Record = if (top_cut) s.records.ptr + start else @ptrCast(s.groups.ptr + start);
                    records[next[p] - start] = r;
                    next[p] += 1;
                }
                id = b.next;
            }
        }
    }

    fn foldTop(s: *State, p: usize, scratch: *FoldScratch, heap: *SumHeap) !void {
        const records = s.records[s.starts[p]..s.starts[p + 1]];
        if (records.len == 0) return;
        const cap = try std.math.ceilPowerOfTwo(usize, records.len * 2);
        try scratch.ensure(s.gpa, records.len, cap);
        const slots = scratch.slots[0..cap];
        const groups = scratch.groups[0..records.len];
        @memset(slots, 0);
        var n: usize = 0;
        for (records) |r| {
            const kn = r.flags & 1 != 0;
            var pos: usize = hash(r.key, kn) & (cap - 1);
            while (slots[pos] != 0) : (pos = (pos + 1) & (cap - 1)) {
                const g = &groups[slots[pos] - 1];
                if ((g.first & flag != 0) == kn and (kn or g.key == r.key)) break;
            }
            if (slots[pos] == 0) {
                slots[pos] = @intCast(n + 1);
                groups[n] = .{
                    .sum = 0,
                    .key = r.key,
                    .count = 0,
                    .first = r.ordinal | (if (kn) flag else @as(u32, 0)),
                };
                n += 1;
            }
            const g = &groups[slots[pos] - 1];
            g.first = @min(g.first & mask, r.ordinal) | (g.first & flag);
            if (r.flags & 2 == 0) {
                g.sum += r.value;
                g.count |= flag;
            }
        }
        // All records of a key use the same exact hash partition. Selection starts only after every record in this
        // partition is folded. Keeping top-H across a worker's completed partitions subsumes their individual top-H;
        // the global top-H is therefore in the union of worker heaps, independent of task scheduling or worker count.
        for (groups[0..n]) |g| keepSum(heap, s.gpa, s.cut.?.k, g);
        s.counts[p] = n;
    }

    fn fold(s: *State, p: usize) !void {
        const lo = s.starts[p];
        const hi = s.starts[p + 1];
        if (lo == hi) return;
        const cap = try std.math.ceilPowerOfTwo(usize, (hi - lo) * 2);
        const slots = try s.gpa.alloc(u32, cap);
        defer s.gpa.free(slots);
        @memset(slots, 0);
        var n: usize = 0;
        // Each partition reserves 32 bytes per input row, initially holding packed 24-byte records. Walk backwards
        // and append groups from the end: after consuming row i, at most rows-i groups exist, so each group starts
        // at an index >= i. Its 32-byte write cannot overlap an unread record below 24*i. The current record is
        // captured by value before any group write. No second state allocation or record-buffer teardown is needed.
        comptime std.debug.assert(@sizeOf(Group) == 32 and @sizeOf(Record) == 24);
        const records: [*]const Record = @ptrCast(s.groups.ptr + lo);
        var row = hi - lo;
        while (row != 0) {
            row -= 1;
            const r = records[row];
            const kn = r.flags & 1 != 0;
            var pos: usize = hash(r.key, kn) & (cap - 1);
            while (slots[pos] != 0) : (pos = (pos + 1) & (cap - 1)) {
                const g = &s.groups[slots[pos] - 1];
                if ((g.first & flag != 0) == kn and (kn or g.key == r.key)) break;
            }
            if (slots[pos] == 0) {
                n += 1;
                slots[pos] = @intCast(hi - n + 1);
                s.groups[hi - n] = .{
                    .sum = 0,
                    .key = r.key,
                    .count = 0,
                    .first = r.ordinal | (if (kn) flag else @as(u32, 0)),
                };
            }
            const g = &s.groups[slots[pos] - 1];
            g.count += 1;
            g.first = @min(g.first & mask, r.ordinal) | (g.first & flag);
            if (r.flags & 2 == 0) {
                g.sum += r.value;
                g.count |= flag;
            }
        }
        for (s.groups[hi - n .. hi], hi - n..) |g, i| {
            if (g.count & flag != 0 and std.math.cast(i64, g.sum) == null) return error.AggIntTooWide;
            s.winners[g.first & mask] = @intCast(i + 1);
        }
        s.counts[p] = n;
    }

    pub fn finish(s: *State, workers: usize) !void {
        if (s.cut != null) return s.finishTop(workers);
        var start = now();
        for (s.locals) |l| s.input_nulls |= l.nulls;
        var at: usize = 0;
        // The emission histograms replace a complete count pass over the coarse records.
        for (0..parts_n) |p| {
            s.starts[p] = at;
            for (s.locals) |l| at += l.histogram[p];
        }
        if (at != s.rows) return error.BadRowCount;
        s.starts[parts_n] = at;
        s.groups = try s.gpa.alloc(Group, s.rows);
        try s.parallel(workers, .scatter);
        s.gpa.free(s.blocks);
        s.blocks = &.{};
        s.scatter_ns = @intCast(now() - start);
        start = now();
        s.winners = try s.gpa.alloc(u32, s.rows);
        @memset(s.winners, 0);
        try s.parallel(workers, .fold);
        for (s.counts) |n| s.group_count += n;
        s.fold_ns = @intCast(now() - start);
        start = now();
        s.bitmap = try s.gpa.alloc(u64, (s.rows + 63) / 64);
        s.prefix = try s.gpa.alloc(u32, s.bitmap.len + 1);
        var total: u32 = 0;
        for (s.bitmap, 0..) |*word, wi| {
            var bits: u64 = 0;
            for (wi * 64..@min(s.rows, wi * 64 + 64)) |i| if (s.winners[i] != 0) {
                bits |= @as(u64, 1) << @intCast(i % 64);
            };
            word.* = bits;
            s.prefix[wi] = total;
            total += @popCount(bits);
        }
        s.prefix[s.bitmap.len] = total;
        if (total != s.group_count) return error.BadRowCount;
        s.order_ns = @intCast(now() - start);
    }

    fn finishTop(s: *State, workers: usize) !void {
        var start = now();
        var at: usize = 0;
        for (0..parts_n) |p| {
            s.starts[p] = at;
            for (s.locals) |l| at += l.histogram[p];
        }
        if (at != s.rows) return error.BadRowCount;
        s.starts[parts_n] = at;
        s.records = try s.gpa.alloc(Record, s.rows);
        try s.parallel(workers, .top_scatter);
        s.gpa.free(s.blocks);
        s.blocks = &.{};
        s.scatter_ns = @intCast(now() - start);
        start = now();
        try s.parallel(workers, .top_fold);
        for (s.counts) |n| s.group_count += n;
        s.fold_ns = @intCast(now() - start);
        start = now();
        const heap = &s.heaps[0];
        for (s.heaps[1..]) |other| for (other.items) |g| keepSum(heap, s.gpa, s.cut.?.k, g);
        // The ordinary stable result sort sees these retained rows in their original first-appearance order.
        std.mem.sort(Group, heap.items, {}, struct {
            fn less(_: void, a: Group, b: Group) bool {
                return (a.first & mask) < (b.first & mask);
            }
        }.less);
        s.gpa.free(s.records);
        s.records = &.{};
        s.order_ns = @intCast(now() - start);
    }

    pub fn topGroups(s: *const State) []const Group {
        return s.heaps[0].items;
    }

    /// Ordered references for an output window; neither groups nor 32-byte slots are sorted or copied.
    pub fn window(s: *const State, a: std.mem.Allocator, lo: usize, hi: usize) ![]u32 {
        std.debug.assert(lo <= hi and hi <= s.group_count);
        const ids = try a.alloc(u32, hi - lo);
        if (ids.len == 0) return ids;
        var left: usize = 0;
        var right = s.bitmap.len;
        while (left < right) {
            const mid = left + (right - left) / 2;
            if (s.prefix[mid + 1] <= lo) left = mid + 1 else right = mid;
        }
        var wi = left;
        var skip = lo - s.prefix[wi];
        var n: usize = 0;
        while (n < ids.len) : (wi += 1) {
            var bits = s.bitmap[wi];
            while (bits != 0) {
                const bit = @ctz(bits);
                bits &= bits - 1;
                if (skip != 0) {
                    skip -= 1;
                    continue;
                }
                ids[n] = s.winners[wi * 64 + bit] - 1;
                n += 1;
                if (n == ids.len) break;
            }
        }
        return ids;
    }
};

fn trial(a: std.mem.Allocator, workers: usize) !void {
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const s = (try State.init(a, &budget, 1001, workers, 0, 1)).?;
    defer s.deinit();
    for (0..1001) |i| try s.locals[i % workers].append(.{
        .key = @intCast(i % 71),
        .value = @as(i64, @intCast(i)) - 500,
        .ordinal = @intCast(i),
        .flags = if (i % 71 == 0) 1 else if (i % 71 == 2) 2 else 0,
    });
    try s.finish(workers);
    try std.testing.expectEqual(@as(usize, 71), s.group_count);
    const ids = try s.window(a, 0, s.group_count);
    defer a.free(ids);
    for (ids, 0..) |id, i| {
        const g = s.groups[id];
        try std.testing.expectEqual(@as(u32, @intCast(i)), g.first & mask);
        try std.testing.expectEqual(if (i == 0) null else @as(?i128, @intCast(i)), g.keyValue());
        var sum: i128 = 0;
        var count: i128 = 0;
        var row = i;
        while (row < 1001) : (row += 71) {
            count += 1;
            sum += @as(i128, @intCast(row)) - 500;
        }
        try std.testing.expectEqual(count, g.countValue());
        try std.testing.expectEqual(if (i == 2) null else @as(?i128, sum), g.sumValue());
    }
    const slice = try s.window(a, 7, 69);
    defer a.free(slice);
    try std.testing.expectEqualSlices(u32, ids[7..69], slice);
}

test "inline integer groups preserve exact sums, NULLs and first order" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| try trial(std.testing.allocator, j);
}
test "inline integer groups release every allocation and reservation on failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, trial, .{1});
}

test "inline integer groups reject a wide exact sum before any output window" {
    for ([_]usize{ 1, 2, 8 }) |j| for ([_]i64{ std.math.maxInt(i64), std.math.minInt(i64) }) |value| {
        const s = (try State.init(std.testing.allocator, null, 2, j, 0, 1)).?;
        defer s.deinit();
        for (0..2) |i| try s.locals[i % j].append(.{ .key = 1, .value = value, .ordinal = @intCast(i), .flags = 0 });
        try std.testing.expectError(error.AggIntTooWide, s.finish(j));
    };
}

test "inline integer groups overlap packed records safely for every reduction and partition tail" {
    const a = std.testing.allocator;
    const Ref = struct { key: ?i64, count: i128 = 0, sum: ?i128 = null };
    for ([_]usize{ 1, 2, 3, 4, 7, 31, 64, 65, 127, 257 }) |n| {
        for ([_]usize{ 1, @max(1, n / 2), n }) |ndv| for ([_]usize{ 1, 4, 16 }) |j| {
            const state = (try State.init(a, null, n, j, 0, 1)).?;
            defer state.deinit();
            var expected: std.ArrayList(Ref) = .empty;
            defer expected.deinit(a);
            for (0..n) |i| {
                const key: ?i64 = if (i % 37 == 0) null else @as(i64, @intCast(i % ndv)) - @as(i64, @intCast(ndv / 2));
                const value: ?i64 = if (i % 13 == 0) null else @as(i64, @intCast((i * 730201) % 1000)) - 500;
                var id = expected.items.len;
                for (expected.items, 0..) |r, ri| if (r.key == key) {
                    id = ri;
                    break;
                };
                if (id == expected.items.len) try expected.append(a, .{ .key = key });
                const ref = &expected.items[id];
                ref.count += 1;
                if (value) |v| ref.sum = (ref.sum orelse 0) + v;
                try state.locals[(n - i) % j].append(.{
                    .key = key orelse 0,
                    .value = value orelse 0,
                    .ordinal = @intCast(i),
                    .flags = @as(u32, @intFromBool(key == null)) | (@as(u32, @intFromBool(value == null)) << 1),
                });
            }
            try state.finish(j);
            try std.testing.expectEqual(expected.items.len, state.group_count);
            const ids = try state.window(a, 0, state.group_count);
            defer a.free(ids);
            for (expected.items, ids) |ref, id| {
                const group = state.groups[id];
                try std.testing.expectEqual(if (ref.key) |k| @as(?i128, k) else null, group.keyValue());
                try std.testing.expectEqual(ref.count, group.countValue());
                try std.testing.expectEqual(ref.sum, group.sumValue());
            }
        };
    }
}

fn topTrial(a: std.mem.Allocator, workers: usize, descending: bool, k: usize) !void {
    const ndv = 1021;
    var budget: memory.SharedBudget = .{ .limit = 1 << 30 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    const state = (try State.top(a, &budget, ndv * 3, workers, 0, 1, .{
        .k = k,
        .descending = descending,
    })).?;
    defer state.deinit();
    // Every present SUM is four. Keys, arrival order and first-appearance order differ; duplicates span workers.
    // Group 5 has a NULL sum and group 11 a NULL key. A partial-partition cut would select incorrect intermediate
    // sums, and a key-order tie-break would produce a different answer from these hand-known first ordinals.
    for ([_]usize{ 2, 0, 1 }) |round| for (0..ndv) |i| try state.locals[(ndv - i + round) % workers].append(.{
        .key = @intCast((i * 73) % ndv),
        .value = ([_]i64{ 2, -1, 3 })[round],
        .ordinal = @intCast(round * ndv + i),
        .flags = if (i == 5) 2 else if (i == 11) 1 else 0,
    });
    try state.finish(workers);
    try std.testing.expectEqual(@as(usize, ndv), state.group_count);
    try std.testing.expectEqual(@as(usize, 0), state.groups.len);
    try std.testing.expectEqual(@as(usize, 0), state.winners.len);
    try std.testing.expectEqual(@as(usize, 0), state.bitmap.len);
    try std.testing.expectEqual(@min(k, ndv), state.topGroups().len);
    var ordinal: usize = 0;
    for (state.topGroups()) |g| {
        if (k < ndv and ordinal == 5) ordinal += 1;
        try std.testing.expectEqual(@as(u32, @intCast(ordinal)), g.first & mask);
        const key: ?i128 = if (ordinal == 11) null else @intCast((ordinal * 73) % ndv);
        try std.testing.expectEqual(key, g.keyValue());
        try std.testing.expectEqual(if (ordinal == 5) null else @as(?i128, 4), g.sumValue());
        ordinal += 1;
    }
}

test "inline integer SUM top-H completes keys before stable tie selection at every worker count" {
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| for ([_]bool{ false, true }) |descending|
        for ([_]usize{ 7, 31, 1024 }) |k| try topTrial(std.testing.allocator, j, descending, k);
}

test "inline integer SUM top-H frees every partial heap and scratch allocation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, topTrial, .{ 1, true, 7 });
}

test "inline integer SUM top-H retains wide winning and losing sums without output narrowing" {
    for ([_]bool{ false, true }) |descending| {
        const s = (try State.top(std.testing.allocator, null, 6, 2, 0, 1, .{
            .k = 1,
            .descending = descending,
        })).?;
        defer s.deinit();
        for (0..6) |i| try s.locals[i % 2].append(.{
            .key = @intCast(i / 2),
            .value = if (i < 2) std.math.maxInt(i64) else if (i < 4) std.math.minInt(i64) else 0,
            .ordinal = @intCast(i),
            .flags = 0,
        });
        try s.finish(2);
        const g = s.topGroups()[0];
        const value: i128 = if (descending) std.math.maxInt(i64) else std.math.minInt(i64);
        try std.testing.expectEqual(value * 2, g.sumValue().?);
    }
}
