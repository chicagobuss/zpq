const std = @import("std");
const spawn_util = @import("../spawn.zig");
const expr_ast = @import("../expr/ast.zig");
const expr_agg = @import("../expr/agg.zig");
const core_bytes = @import("../bytes.zig");
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;
const owned_merge = @import("owned_merge.zig");
const mergeOwnedAccumulator = owned_merge.mergeOwnedAccumulator;
const freeOwnedAccumulatorStrings = owned_merge.freeOwnedAccumulatorStrings;
const merge_index = @import("merge_index.zig");
const MergeIndex = merge_index.MergeIndex;
const scan_count_cut = @import("count_cut.zig");
const MergedSlot = scan_count_cut.MergedSlot;
const accCount = scan_count_cut.accCount;
const CountCut = scan_count_cut.CountCut;
const keepCountTop = scan_count_cut.keepCountTop;
const scan_rank_cut = @import("rank_cut.zig");
const ResultCut = scan_rank_cut.ResultCut;
const rankValue = scan_rank_cut.rankValue;
const RankHeap = scan_rank_cut.RankHeap;
const pushRank = scan_rank_cut.pushRank;
const CutScratch = scan_rank_cut.CutScratch;
const folded_acc = @import("folded_acc.zig");
const foldedAcc = folded_acc.foldedAcc;
const parallel_distinct = @import("parallel_distinct.zig");
const parallelDistinctCountU64 = parallel_distinct.parallelDistinctCountU64;
const pair_fold = @import("pair_fold.zig");
const foldDistinctPairs = pair_fold.foldDistinctPairs;
const scan_types = @import("types.zig");
const WorkItem = scan_types.WorkItem;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const testing = std.testing;

pub const MergeGroupRef = struct {
    table: u32,
    group: u32,
};

pub fn mergeCandidates(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    keys: []const []const u8,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    _ = n_workers;
    try slots.ensureTotalCapacity(lists, keys.len);
    try accs.ensureTotalCapacity(lists, keys.len * calls.len);

    for (keys) |key| {
        var first: u64 = std.math.maxInt(u64);
        var key_borrowed: []const u8 = key;
        var found_any = false;

        for (tables) |gt| {
            if (gt.findKey(key)) |gi| {
                found_any = true;
                if (gt.first.items[gi] < first) {
                    first = gt.first.items[gi];
                    key_borrowed = gt.keyAt(gi);
                }
            }
        }
        if (!found_any) continue;

        const slot_idx = accs.items.len;
        for (calls, 0..) |call, ci| {
            // Appended before merging, so state moved out of a source is owned by `accs` even if a later merge fails.
            accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
            const dst = &accs.items[accs.items.len - 1];
            for (tables) |gt| {
                if (gt.findKey(key)) |gi| {
                    var local = gt.readAcc(gi, ci, calls.len);
                    const src = if (gt.compact.stride != 0)
                        &local
                    else
                        &gt.accumulators.items[@as(usize, gi) * calls.len + ci];
                    try mergeOwnedAccumulator(dst, src, gpa);
                }
            }
        }

        slots.appendAssumeCapacity(.{
            .key = key_borrowed,
            .first = first,
            .idx = slot_idx,
        });
    }
}

/// Fold `refs` into distinct groups. `slots` borrows keys from `tables`; `accs` owns any string winners moved out of
/// those tables. The map is sized for every ref, so a repeated key does not rehash it. `hashes`, when set, is the
/// Wyhash already computed for each ref (the same one `StringHashMap` uses) and is not computed again.
/// `in_place` (`mergesInPlace`): `accs` stays empty and each group's state is merged into its first ref's own
/// accumulators instead, which `slot.idx` names (`packRef`).
pub fn hashMergeGroupRefs(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ?ResultCut,
    in_place: bool,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    if (in_place) return mergeInPlace(gpa, lists, tables, refs, hashes, calls, slots);
    if (cut) |c| {
        if (c.ranking_first) return mergeResultCut(gpa, lists, tables, refs, hashes, calls, c, slots, accs);
        if (calls[c.a_idx].func == .count) {
            const count_cut: CountCut = .{ .a_idx = c.a_idx, .k = c.k };
            return mergeCountCut(gpa, lists, tables, refs, hashes, calls, count_cut, slots, accs);
        }
    }
    var index = try MergeIndex.init(lists, tables, refs.len, .within_map);
    defer index.deinit();
    try slots.ensureTotalCapacity(lists, refs.len);

    for (refs, 0..) |ref, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = gt.readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        if (gop.found) {
            const slot = &slots.items[gop.value.*];
            slot.first = @min(slot.first, gt.first.items[ref.group]);
            const dst = accs.items[slot.idx..][0..calls.len];
            for (dst, src) |*d, *s| try mergeOwnedAccumulator(d, s, gpa);
        } else {
            gop.value.* = @intCast(slots.items.len);
            const acc_at = accs.items.len;
            try accs.ensureUnusedCapacity(lists, calls.len);
            for (calls) |call| accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
            const dst = accs.items[acc_at..][0..calls.len];
            for (dst, src) |*d, *s| try mergeOwnedAccumulator(d, s, gpa);
            slots.appendAssumeCapacity(.{
                .key = key,
                .first = gt.first.items[ref.group],
                .idx = acc_at,
            });
        }
    }
    // A partition holds every copy of its keys, so its counts are final and a count cut applies here.
    if (cut) |c| try keepCountTop(gpa, lists, calls, .{ .a_idx = c.a_idx, .k = c.k }, slots, accs);
}

/// Whether a full merge may leave each group's state in the source table that first holds it (`mergeInPlace`): no
/// cut, which keeps and reorders merged state, and no count(DISTINCT), whose sets a read-only copy cannot merge.
pub fn mergesInPlace(calls: []const expr_agg.AggCall, cut: ?ResultCut) bool {
    if (cut != null) return false;
    for (calls) |call| if (call.func == .count_distinct) return false;
    return true;
}

/// `calls` with call `i`, a count(DISTINCT) whose every state `foldDistinctPairs` made a count, merged as the COUNT
/// it now is: a fresh count(DISTINCT) state takes a count as it is, so the merge's every value is the same.
/// A merged-in-place slot's `idx`: its source table above its group.
fn packRef(ref: MergeGroupRef) usize {
    return @as(usize, ref.table) << 32 | ref.group;
}

fn inPlaceAccs(
    tables: []const expr_agg.GroupTable,
    idx: usize,
    n_calls: usize,
    scratch: *[16]expr_agg.Accumulator,
) []expr_agg.Accumulator {
    const g: usize = @as(u32, @truncate(idx));
    return tables[idx >> 32].readGroup(g, n_calls, scratch);
}

/// `hashMergeGroupRefs` with no copy of a group's state: a key's first ref keeps it, and each later ref of the key is
/// merged into it there. Before each such merge the kept state is first made `foldedAcc` of itself, so the sequence of
/// merges, and every bit of the result, is the copying merge's; rows read a group seen once through `foldedAcc`. A key
/// is in one partition only, so no two threads write the same state.
pub fn mergeInPlace(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    slots: *std.ArrayList(MergedSlot),
) !void {
    var index = try MergeIndex.init(lists, tables, refs.len, .any);
    defer index.deinit();
    try slots.ensureTotalCapacity(lists, slots.items.len + refs.len);
    for (refs, 0..) |ref, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const first = gt.first.items[ref.group];
        if (!gop.found) {
            gop.value.* = @intCast(slots.items.len);
            slots.appendAssumeCapacity(.{ .key = key, .first = first, .idx = packRef(ref) });
            continue;
        }
        const slot = &slots.items[gop.value.*];
        slot.first = @min(slot.first, first);
        var dst_scratch: [16]expr_agg.Accumulator = undefined;
        const dst = inPlaceAccs(tables, slot.idx, calls.len, &dst_scratch);
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = gt.readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src, calls) |*d, *s, call| {
            d.* = try foldedAcc(gpa, call, d.*);
            try mergeOwnedAccumulator(d, s, gpa);
        }
        tables[slot.idx >> 32].writeGroup(@as(u32, @truncate(slot.idx)), dst);
    }
}

/// `hashMergeGroupRefs` under a cut by a COUNT: the counts are summed first, alone, to pick the `cut.k` groups the cut
/// keeps (largest count, earlier first appearance on a tie); only those groups' accumulators are then merged. A key
/// seen once (the common case on a near-unique key) costs a count and a position, not a merged copy of its state.
pub fn mergeCountCut(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: CountCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    var index = try MergeIndex.init(lists, tables, refs.len, .within_map);
    defer index.deinit();
    const Group = struct { count: u64, first: u64, key: []const u8 };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(lists);
    try groups.ensureTotalCapacity(lists, refs.len);
    const group_of = try lists.alloc(u32, refs.len);
    defer lists.free(group_of);
    for (refs, group_of, 0..) |ref, *gi, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const count = gt.readAcc(@as(usize, ref.group), cut.a_idx, calls.len).count;
        const first = gt.first.items[ref.group];
        if (gop.found) {
            const g = &groups.items[gop.value.*];
            g.count +|= count;
            g.first = @min(g.first, first);
        } else {
            gop.value.* = @intCast(groups.items.len);
            groups.appendAssumeCapacity(.{ .count = count, .first = first, .key = key });
        }
        gi.* = gop.value.*;
    }

    const Item = struct { c: u64, first: u64, g: u32 };
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    defer heap.deinit(lists);
    const k = @min(cut.k, groups.items.len);
    try heap.ensureTotalCapacity(lists, k);
    if (k > 0) for (groups.items, 0..) |g, gi| {
        const item: Item = .{ .c = g.count, .first = g.first, .g = @intCast(gi) };
        if (heap.count() == k) {
            const w = heap.peek().?;
            if (!(item.c > w.c or (item.c == w.c and item.first < w.first))) continue;
            _ = heap.pop();
        }
        heap.push(lists, item) catch unreachable; // within the capacity reserved above
    };

    // The kept groups' state, merged from just their refs.
    const none = std.math.maxInt(u32);
    const out_of = try lists.alloc(u32, groups.items.len);
    defer lists.free(out_of);
    @memset(out_of, none);
    try slots.ensureTotalCapacity(lists, slots.items.len + heap.count());
    try accs.ensureUnusedCapacity(lists, heap.count() * calls.len);
    for (heap.items) |item| {
        const g = groups.items[item.g];
        out_of[item.g] = @intCast(slots.items.len);
        slots.appendAssumeCapacity(.{ .key = g.key, .first = g.first, .idx = accs.items.len });
        for (calls) |call| accs.appendAssumeCapacity(expr_agg.Accumulator.init(call));
    }
    for (refs, group_of) |ref, gi| {
        const out = out_of[gi];
        if (out == none) continue;
        const dst = accs.items[slots.items[out].idx..][0..calls.len];
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = tables[ref.table].readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src) |*d, *sa| try mergeOwnedAccumulator(d, sa, gpa);
    }
}

/// Complete groups are ranked before any owning payload moves; each partition's top-H contains its global winners.
fn mergeResultCut(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    // Charged as the full merge's maps are, through `lists`: see the merge in `prepareAggregate`.
    var scratch: CutScratch = .{ .parent = cut.memory orelse lists, .shared = null };
    mergeResultCutInner(gpa, lists, tables, refs, hashes, calls, cut, &scratch, slots, accs) catch |e|
        return if (scratch.denied.load(.monotonic)) error.ExceededMemoryBudget else e;
}

fn mergeResultCutInner(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    hashes: ?[]const u64,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    scratch: *CutScratch,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    const sa = scratch.allocator();
    const ma = cut.memory orelse lists;
    var index = try MergeIndex.init(sa, tables, refs.len, .within_map);
    defer index.deinit();
    const Group = struct { rank: expr_agg.Accumulator, first: u64, key: []const u8 };
    var groups: std.ArrayList(Group) = .empty;
    defer groups.deinit(sa);
    try groups.ensureTotalCapacity(sa, refs.len);
    const group_of = try sa.alloc(u32, refs.len);
    defer sa.free(group_of);
    for (refs, group_of, 0..) |ref, *gi, ri| {
        const gt = &tables[ref.table];
        const key = gt.keyAt(ref.group);
        const gop = index.getOrPut(key, if (hashes) |hs| hs[ri] else std.hash.Wyhash.hash(0, key));
        const rank = gt.readAcc(@as(usize, ref.group), cut.a_idx, calls.len);
        const first = gt.first.items[ref.group];
        if (gop.found) {
            const g = &groups.items[gop.value.*];
            try g.rank.merge(rank, sa);
            g.first = @min(g.first, first);
        } else {
            gop.value.* = @intCast(groups.items.len);
            groups.appendAssumeCapacity(.{ .rank = rank, .first = first, .key = key });
        }
        gi.* = gop.value.*;
    }

    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(sa);
    const k = @min(cut.k, groups.items.len);
    try heap.ensureTotalCapacity(sa, k);
    for (groups.items, 0..) |g, gi| {
        const value = try rankValue(g.rank, calls[cut.a_idx]);
        pushRank(&heap, sa, k, .{ .value = value, .first = g.first, .g = @intCast(gi) });
    }

    // The kept groups' state, merged from just their refs.
    const none = std.math.maxInt(u32);
    const out_of = try sa.alloc(u32, groups.items.len);
    defer sa.free(out_of);
    @memset(out_of, none);
    try slots.ensureTotalCapacity(ma, slots.items.len + heap.count());
    try accs.ensureUnusedCapacity(ma, heap.count() * calls.len);
    for (heap.items) |item| {
        const g = groups.items[item.g];
        out_of[item.g] = @intCast(slots.items.len);
        slots.appendAssumeCapacity(.{ .key = g.key, .first = g.first, .idx = accs.items.len });
        for (calls, 0..) |call, i|
            accs.appendAssumeCapacity(if (i == cut.a_idx) g.rank else expr_agg.Accumulator.init(call));
    }
    for (refs, group_of) |ref, gi| {
        const out = out_of[gi];
        if (out == none) continue;
        const dst = accs.items[slots.items[out].idx..][0..calls.len];
        var src_scratch: [16]expr_agg.Accumulator = undefined;
        const src = tables[ref.table].readGroup(@as(usize, ref.group), calls.len, &src_scratch);
        for (dst, src, 0..) |*d, *src_acc, i| if (i != cut.a_idx) try mergeOwnedAccumulator(d, src_acc, gpa);
    }
}

/// How many independent partitions the merge is worth. One partition below a few thousand refs, or with one worker:
/// the hash table is then smaller than the cost of splitting it. Otherwise a power of two, so a key's high bits name
/// its partition, and no larger than the worker count.
/// Bytes of merge state a full-merge partition aims for: a core's share of its chiplet's L3.
pub const merge_part_bytes = 4 << 20;
pub const merge_parts_max = 4096;

/// `entry_bytes`: what one reference may add to a partition's map, slots and accumulators.
pub fn mergePartitionCount(n_workers: usize, n_refs: usize, cut: bool, entry_bytes: usize) usize {
    if (n_workers < 2 or n_refs < 16) return 1;
    var p: usize = 1;
    while (p * 2 <= n_workers and p < 32) p *= 2;
    // Under a count cut each partition shrinks to its top groups as it finishes, so many small partitions keep only a
    // few in flight at once instead of half the groups. Even with small n_refs (e.g. after top-k candidate pruning),
    // each remaining group may hold large distinct sets that benefit from parallel workers.
    if (cut) {
        const cut_parts = std.math.ceilPowerOfTwoAssert(usize, n_refs / 65536 + 1);
        return @min(512, @max(p, cut_parts));
    }
    if (n_refs < 8192) return 1;
    // A full merge of near-unique keys holds about a group a reference: one partition a worker leaves each map far
    // larger than cache, and lookups miss. Sized by bytes instead, a partition's state stays cache-resident.
    const fit = std.math.ceilPowerOfTwoAssert(usize, (n_refs *| entry_bytes) / merge_part_bytes + 1);
    return @min(merge_parts_max, @max(p, fit));
}

/// What one merged reference may cost a partition when it starts a group: its map entry, its slot and its
/// accumulators.
pub fn mergeEntryBytes(n_calls: usize) usize {
    return @sizeOf([]const u8) + @sizeOf(u32) + 1 + @sizeOf(MergedSlot) + n_calls * @sizeOf(expr_agg.Accumulator);
}

/// Preparation and reclamation are part of the finish too: neither may walk millions of groups on the caller.
/// Stable contiguous ranges retain table/input order before the radix scatter, including canonical float partials.
pub fn finishGroupRanges(
    tables: []expr_agg.GroupTable,
    refs: ?[]MergeGroupRef,
    slots: ?[]MergedSlot,
    requested: usize,
    cleanup: bool,
) void {
    var total: usize = 0;
    for (tables) |t| total += t.groupCount();
    if (total == 0) return;
    const Task = struct {
        tables: []expr_agg.GroupTable,
        refs: ?[]MergeGroupRef,
        slots: ?[]MergedSlot,
        total: usize,
        chunks: usize,
        cleanup: bool,
        cursor: *std.atomic.Value(usize),
        fn run(t: *@This()) void {
            const width = std.math.divCeil(usize, t.total, t.chunks) catch unreachable;
            while (true) {
                const chunk = t.cursor.fetchAdd(1, .monotonic);
                if (chunk >= t.chunks) return;
                const lo = chunk * width;
                const hi = @min(t.total, lo + width);
                var base: usize = 0;
                for (t.tables, 0..) |*table, ti| {
                    const end = base + table.groupCount();
                    if (lo < end and hi > base) {
                        const first = @max(lo, base) - base;
                        const last = @min(hi, end) - base;
                        if (t.cleanup) {
                            table.deinitRange(first, last);
                        } else for (first..last) |gi| {
                            if (t.refs) |r| r[base + gi] = .{ .table = @intCast(ti), .group = @intCast(gi) };
                            if (t.slots) |s| s[base + gi] = .{
                                .key = table.keyAt(gi),
                                .first = table.first.items[gi],
                                .idx = gi,
                            };
                        }
                    }
                    base = end;
                    if (base >= hi) break;
                }
            }
        }
    };
    var storage: [64]Task = undefined;
    const count = @min(storage.len, @max(1, @min(requested, total / 8192 + 1)));
    const tasks = storage[0..count];
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*t| t.* = .{
        .tables = tables,
        .refs = refs,
        .slots = slots,
        .total = total,
        .chunks = count,
        .cleanup = cleanup,
        .cursor = &cursor,
    };
    if (count == 1 or !spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run)) tasks[0].run();
}

pub fn freeAccumulatorRanges(gpa: std.mem.Allocator, accs: []expr_agg.Accumulator, workers: usize) void {
    const Task = struct {
        gpa: std.mem.Allocator,
        accs: []expr_agg.Accumulator,
        chunks: usize,
        cursor: *std.atomic.Value(usize),
        fn run(t: *@This()) void {
            const width = std.math.divCeil(usize, t.accs.len, t.chunks) catch unreachable;
            while (true) {
                const chunk = t.cursor.fetchAdd(1, .monotonic);
                if (chunk >= t.chunks) return;
                const lo = @min(t.accs.len, chunk * width);
                freeOwnedAccumulatorStrings(t.gpa, t.accs[lo..@min(t.accs.len, lo + width)]);
            }
        }
    };
    var storage: [64]Task = undefined;
    const count = @min(storage.len, @max(1, @min(workers, accs.len / 8192 + 1)));
    const tasks = storage[0..count];
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*t| t.* = .{ .gpa = gpa, .accs = accs, .chunks = count, .cursor = &cursor };
    if (count == 1 or !spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run)) tasks[0].run();
}

const MergePart = struct {
    refs: []const MergeGroupRef,
    hashes: []const u64,
    slots: std.ArrayList(MergedSlot) = .empty,
    accs: std.ArrayList(expr_agg.Accumulator) = .empty,
    /// Where this partition's slots and accumulators start in the combined lists.
    slot_at: usize = 0,
    acc_at: usize = 0,
};

const MergeTask = struct {
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    cut: ?ResultCut,
    in_place: bool,
    parts: []MergePart,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    /// Claim partitions until none remain. A thread that never starts leaves its share on the cursor, so a failed
    /// spawn cannot drop a partition.
    fn run(t: *MergeTask) void {
        while (t.err == null) {
            const i = t.cursor.fetchAdd(1, .monotonic);
            if (i >= t.parts.len) return;
            const p = &t.parts[i];
            const slots = &p.slots;
            hashMergeGroupRefs(t.gpa, t.lists, t.tables, p.refs, p.hashes, t.calls, t.cut, t.in_place, slots, &p.accs) catch |e| {
                t.err = e;
                return;
            };
        }
    }
};

/// Several worker tables: hash-merge `refs`. Large merges split by the key hash and run a partition per worker; the
/// partitions are disjoint, so none writes a group another writes. `hashes` are Wyhash with seed 0, matching the map.
pub fn mergeFoldedRefs(
    gpa: std.mem.Allocator,
    /// The merge's own maps, lists and scratch; `gpa` holds the accumulators' values.
    lists: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    refs: []const MergeGroupRef,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
    cut: ?ResultCut,
    in_place: bool,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    std.debug.assert(!in_place or mergesInPlace(calls, cut));
    const ma = if (cut) |c| c.memory orelse lists else lists;
    const split_arena = ma;
    const nparts = mergePartitionCount(n_workers, refs.len, cut != null, mergeEntryBytes(calls.len));
    if (nparts == 1) return hashMergeGroupRefs(gpa, ma, tables, refs, null, calls, cut, in_place, slots, accs);

    // Partitions come from the middle of the hash: the map inside each takes its fingerprint from the top bits and
    // its slot from the low ones, and a partition that fixed either would collide more.
    const mask: u64 = nparts - 1;
    const hashes = try split_arena.alloc(u64, refs.len);
    defer split_arena.free(hashes);
    const ordered = try split_arena.alloc(MergeGroupRef, refs.len);
    defer split_arena.free(ordered);
    const ordered_hash = try split_arena.alloc(u64, refs.len);
    defer split_arena.free(ordered_hash);
    const starts = try split_arena.alloc(usize, nparts + 1);
    defer split_arena.free(starts);
    // Hashed and split into partitions by chunks of refs on every worker: each chunk counts its own partitions, the
    // counts give each chunk where it writes in each, and each chunk scatters its own refs. A partition holds its refs
    // in input order, as one thread splitting them all would leave them.
    const n_chunks = @max(1, @min(n_workers, refs.len / 65536));
    const chunk_counts = try split_arena.alloc(usize, n_chunks * nparts);
    defer split_arena.free(chunk_counts);
    @memset(chunk_counts, 0);
    const Split = struct {
        tables: []const expr_agg.GroupTable,
        refs: []const MergeGroupRef,
        hashes: []u64,
        ordered: []MergeGroupRef,
        ordered_hash: []u64,
        counts: []usize,
        nparts: usize,
        mask: u64,
        n_chunks: usize,
        cursor: *std.atomic.Value(usize),
        phase: enum { count, scatter },

        /// Claim chunks until none remain: one a failed spawn left unclaimed is taken by a thread that did start.
        fn run(t: *@This()) void {
            while (true) {
                const c = t.cursor.fetchAdd(1, .monotonic);
                if (c >= t.n_chunks) return;
                const lo = t.refs.len * c / t.n_chunks;
                const hi = t.refs.len * (c + 1) / t.n_chunks;
                const mine = t.counts[c * t.nparts ..][0..t.nparts];
                switch (t.phase) {
                    .count => for (t.refs[lo..hi], t.hashes[lo..hi]) |ref, *h| {
                        h.* = std.hash.Wyhash.hash(0, t.tables[ref.table].keyAt(ref.group));
                        mine[(h.* >> 32) & t.mask] += 1;
                    },
                    // `mine` now holds where this chunk writes next in each partition.
                    .scatter => for (t.refs[lo..hi], t.hashes[lo..hi]) |ref, h| {
                        const at = mine[(h >> 32) & t.mask];
                        t.ordered[at] = ref;
                        t.ordered_hash[at] = h;
                        mine[(h >> 32) & t.mask] = at + 1;
                    },
                }
            }
        }
    };
    var split_cursor = std.atomic.Value(usize).init(0);
    const splits = try split_arena.alloc(Split, n_chunks);
    defer split_arena.free(splits);
    const split_config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    for ([_]@TypeOf(splits[0].phase){ .count, .scatter }) |phase| {
        if (phase == .scatter) {
            // Partition p starts after every chunk's refs of the partitions before it; within p, chunk c after
            // chunks < c.
            var at: usize = 0;
            for (0..nparts) |p| {
                starts[p] = at;
                for (0..n_chunks) |c| {
                    const n = chunk_counts[c * nparts + p];
                    chunk_counts[c * nparts + p] = at;
                    at += n;
                }
            }
            starts[nparts] = at;
        }
        split_cursor.store(0, .monotonic);
        for (splits) |*t| t.* = .{
            .tables = tables,
            .refs = refs,
            .hashes = hashes,
            .ordered = ordered,
            .ordered_hash = ordered_hash,
            .counts = chunk_counts,
            .nparts = nparts,
            .mask = mask,
            .n_chunks = n_chunks,
            .cursor = &split_cursor,
            .phase = phase,
        };
        if (splits.len == 1 or !spawn_util.runTree(Split, splits, split_config, Split.run)) splits[0].run();
    }

    const parts = try split_arena.alloc(MergePart, nparts);
    defer split_arena.free(parts);
    for (parts, 0..) |*p, i| {
        p.* = .{
            .refs = ordered[starts[i]..starts[i + 1]],
            .hashes = ordered_hash[starts[i]..starts[i + 1]],
        };
    }
    defer for (parts) |*p| {
        freeOwnedAccumulatorStrings(gpa, p.accs.items);
        p.accs.deinit(ma);
        p.slots.deinit(ma);
    };

    const n_threads = @min(n_workers, nparts);
    const tasks = try split_arena.alloc(MergeTask, n_threads);
    defer split_arena.free(tasks);
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*task| task.* = .{
        .gpa = gpa,
        .lists = ma,
        .tables = tables,
        .calls = calls,
        .cut = cut,
        .in_place = in_place,
        .parts = parts,
        .cursor = &cursor,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (!spawn_util.runTree(MergeTask, tasks, config, MergeTask.run)) MergeTask.run(&tasks[0]);
    for (tasks) |task| if (task.err) |e| return e;

    // Each partition is copied to its place in the combined lists on every worker: on one thread the copy of a large
    // merge costs more than sorting it.
    var nslots: usize = slots.items.len;
    var naccs: usize = accs.items.len;
    for (parts) |*p| {
        p.slot_at = nslots;
        p.acc_at = naccs;
        nslots += p.slots.items.len;
        naccs += p.accs.items.len;
    }
    try slots.ensureTotalCapacity(ma, nslots);
    try accs.ensureTotalCapacity(ma, naccs);
    const copies = try split_arena.alloc(ConcatTask, n_threads);
    defer split_arena.free(copies);
    // Nothing fails from here on, so the lists never hold an entry no partition wrote.
    slots.items.len = nslots;
    accs.items.len = naccs;
    cursor.store(0, .monotonic);
    for (copies) |*c| c.* = .{
        .parts = parts,
        .slots = slots.items,
        .accs = accs.items,
        .in_place = in_place,
        .cursor = &cursor,
    };
    if (!spawn_util.runTree(ConcatTask, copies, config, ConcatTask.run)) ConcatTask.run(&copies[0]);
    // The combined list now owns these winners.
    for (parts) |*p| p.accs.clearRetainingCapacity();
}

/// Copies the partitions it claims to their place in the combined lists, offsetting each slot's accumulators.
const ConcatTask = struct {
    parts: []const MergePart,
    slots: []MergedSlot,
    accs: []expr_agg.Accumulator,
    /// Slots name their source groups (`packRef`), not merged accumulators, so none is offset.
    in_place: bool,
    cursor: *std.atomic.Value(usize),

    fn run(t: *ConcatTask) void {
        while (true) {
            const i = t.cursor.fetchAdd(1, .monotonic);
            if (i >= t.parts.len) return;
            const p = &t.parts[i];
            for (p.slots.items, t.slots[p.slot_at..][0..p.slots.items.len]) |s, *d| d.* = .{
                .key = s.key,
                .first = s.first,
                .idx = if (t.in_place) s.idx else s.idx + p.acc_at,
            };
            @memcpy(t.accs[p.acc_at..][0..p.accs.items.len], p.accs.items);
        }
    }
};

/// The keys of the only groups a LIMIT with no ORDER BY can return: groups come out in first-appearance order, so it
/// keeps the `k` that appeared first. Each of those is among the `k` earliest groups of the table that first saw it,
/// since any group that table saw earlier appeared earlier overall, so the union of every table's `k` earliest holds
/// them all. Null when that would not shrink the merge.
/// Per-range first-H nominees contain the first H global keys: each winning key's earliest ref is among its
/// range's H earliest distinct keys. Taking minima over those nominees therefore gives exact original ordinals.
/// Only bounded nominees meet on the caller; source keys remain borrowed and the returned outer slice is owned.
pub fn firstCandidatesParallel(
    a: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    k: usize,
    requested: usize,
) !?[]const []const u8 {
    if (k == 0) return &.{};
    var groups: usize = 0;
    for (tables) |t| groups += t.groupCount();
    if (groups <= k *| 4) return null;
    const Item = struct { first: u64, key: []const u8 };
    const later = struct {
        fn cmp(_: void, x: Item, y: Item) std.math.Order {
            return std.math.order(y.first, x.first);
        }
    }.cmp;
    const Heap = std.PriorityQueue(Item, void, later);
    const Task = struct {
        allocator: std.mem.Allocator,
        tables: []const expr_agg.GroupTable,
        chunks: usize,
        k: usize,
        cursor: *std.atomic.Value(usize),
        heap: Heap = .empty,
        keys: KeyHashMapUnmanaged(u64) = .empty,
        err: ?anyerror = null,
        fn offer(queue: *Heap, allocator: std.mem.Allocator, limit: usize, item: Item) void {
            if (queue.count() == limit) {
                if (item.first >= queue.peek().?.first) return;
                _ = queue.pop();
            }
            queue.push(allocator, item) catch unreachable;
        }
        fn run(t: *@This()) void {
            while (t.err == null) {
                const job = t.cursor.fetchAdd(1, .monotonic);
                if (job >= t.tables.len * t.chunks) return;
                const table = &t.tables[job % t.tables.len];
                const width = std.math.divCeil(usize, table.groupCount(), t.chunks) catch unreachable;
                const lo = @min(table.groupCount(), (job / t.tables.len) * width);
                t.heap.clearRetainingCapacity();
                for (lo..@min(table.groupCount(), lo + width)) |gi| offer(
                    &t.heap,
                    t.allocator,
                    t.k,
                    .{ .first = table.first.items[gi], .key = table.keyAt(gi) },
                );
                for (t.heap.items) |item| {
                    const entry = t.keys.getOrPut(t.allocator, item.key) catch |e| {
                        t.err = e;
                        return;
                    };
                    entry.value_ptr.* = if (entry.found_existing) @min(entry.value_ptr.*, item.first) else item.first;
                }
            }
        }
    };
    const chunks = @max(1, @min(64, requested, groups / @max(1, k *| tables.len *| 4)));
    var storage: [64]Task = undefined;
    const tasks = storage[0..@max(1, @min(storage.len, requested, tables.len * chunks))];
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*t| t.* = .{ .allocator = a, .tables = tables, .chunks = chunks, .k = k, .cursor = &cursor };
    defer for (tasks) |*t| {
        t.keys.deinit(a);
        t.heap.deinit(a);
    };
    for (tasks) |*t| try t.heap.ensureTotalCapacity(a, k);
    if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run))
        tasks[0].run();
    var nominees: KeyHashMapUnmanaged(u64) = .empty;
    defer nominees.deinit(a);
    for (tasks) |*t| {
        if (t.err) |e| return e;
        var it = t.keys.iterator();
        while (it.next()) |entry| {
            const to = try nominees.getOrPut(a, entry.key_ptr.*);
            to.value_ptr.* = if (to.found_existing) @min(to.value_ptr.*, entry.value_ptr.*) else entry.value_ptr.*;
        }
    }
    var heap: Heap = .empty;
    defer heap.deinit(a);
    try heap.ensureTotalCapacity(a, k);
    var it = nominees.iterator();
    while (it.next()) |entry|
        Task.offer(&heap, a, k, .{ .first = entry.value_ptr.*, .key = entry.key_ptr.* });
    const keys = try a.alloc([]const u8, heap.count());
    for (keys, heap.items) |*key, item| key.* = item.key;
    return keys;
}

pub fn firstCandidates(arena: std.mem.Allocator, tables: []const expr_agg.GroupTable, k: usize) error{OutOfMemory}!?[]const []const u8 {
    if (k == 0) return &.{};
    var groups: usize = 0;
    for (tables) |t| groups += t.groupCount();
    if (groups <= k *| 4) return null;
    const Item = struct { first: u64, g: u32 };
    const later = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, later) = .empty;
    try heap.ensureTotalCapacity(arena, k);
    var kept: std.StringArrayHashMapUnmanaged(void) = .empty;
    for (tables) |*t| {
        heap.clearRetainingCapacity();
        for (t.first.items[0..t.groupCount()], 0..) |first, g| {
            if (heap.count() == k) {
                if (first >= heap.peek().?.first) continue;
                _ = heap.pop();
            }
            heap.push(arena, .{ .first = first, .g = @intCast(g) }) catch unreachable; // within the capacity reserved above
        }
        for (heap.items) |item| try kept.put(arena, t.keyAt(item.g), {});
    }
    return kept.keys();
}

/// The keys of the only groups that can reach the first `offset + limit` rows of an answer ordered by a COUNT,
/// descending, first; null when the order is not such a cut. Exact, after the threshold algorithm of distributed
/// top-k (Cao and Wang, 2004): every table nominates its own top groups, and the k-th largest exact total among the
/// nominees, `tau`, is a floor on the k-th largest total of all. A key whose total reaches `tau` across `m` tables has
/// at least `tau / m` in one of them, so the keys some table counts that high are all that can make the cut, ties at
/// the cut included; the rest are never merged or materialized. Typically a few hundred groups of millions survive.
pub fn premergeLargeDistinctSets(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    tables: []expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    n_workers: usize,
) !void {
    if (tables.len <= 1 or n_workers <= 1) return;

    for (calls, 0..) |call, ci| {
        if (call.func != .count_distinct) continue;

        var large_keys: KeyHashMapUnmanaged(void) = .empty;
        defer large_keys.deinit(gpa);

        const Task = struct {
            allocator: std.mem.Allocator,
            tables: []const expr_agg.GroupTable,
            stride: usize,
            call: usize,
            chunks: usize,
            cursor: *std.atomic.Value(usize),
            keys: KeyHashMapUnmanaged(void) = .empty,
            err: ?anyerror = null,
            fn run(t: *@This()) void {
                while (t.err == null) {
                    const job = t.cursor.fetchAdd(1, .monotonic);
                    if (job >= t.tables.len * t.chunks) return;
                    const table = &t.tables[job % t.tables.len];
                    const width = std.math.divCeil(usize, table.groupCount(), t.chunks) catch unreachable;
                    const lo = @min(table.groupCount(), (job / t.tables.len) * width);
                    for (lo..@min(table.groupCount(), lo + width)) |gi| {
                        const acc = table.readAcc(gi, t.call, t.stride);
                        if (acc == .distinct and acc.distinct.count() >= 16384)
                            t.keys.put(t.allocator, table.keyAt(gi), {}) catch |e| {
                                t.err = e;
                                return;
                            };
                    }
                }
            }
        };
        var storage: [64]Task = undefined;
        const tasks = storage[0..@max(1, @min(storage.len, n_workers))];
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*task| task.* = .{
            .allocator = gpa,
            .tables = tables,
            .stride = calls.len,
            .call = ci,
            .chunks = tasks.len,
            .cursor = &cursor,
        };
        defer for (tasks) |*task| task.keys.deinit(gpa);
        if (!spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run)) tasks[0].run();
        for (tasks) |*task| {
            if (task.err) |e| return e;
            var keys = task.keys.keyIterator();
            while (keys.next()) |key| try large_keys.put(gpa, key.*, {});
        }

        if (large_keys.count() == 0) continue;

        var it = large_keys.keyIterator();
        while (it.next()) |k_ptr| {
            const key = k_ptr.*;
            var sources: std.ArrayListUnmanaged(*const expr_agg.DistinctSet) = .empty;
            var is_u64 = true;

            for (tables) |*gt| {
                if (gt.findKey(key)) |gi| {
                    const acc = &gt.accumulators.items[gi * calls.len + ci];
                    if (acc.* != .distinct or acc.distinct.count() == 0) continue;
                    // A sibling that never spilled still holds its values inline; the union count needs them too.
                    const ds = try acc.distinct.ensureHeap(gpa);
                    try sources.append(arena, ds);
                    // Only sets of integers split by value here; a partition holding anything else keeps the whole set
                    // on the merge's own path.
                    const u64_only = struct {
                        fn of(d: *const expr_agg.DistinctSet) bool {
                            return d.val_set == .none or d.val_set == .inline_u64 or d.val_set == .u64;
                        }
                    }.of;
                    if (!u64_only(ds)) is_u64 = false;
                    if (ds.parts) |parts| for (parts) |*part| {
                        if (!u64_only(part)) is_u64 = false;
                    };
                }
            }

            if (!is_u64 or sources.items.len <= 1) continue;

            const total = try parallelDistinctCountU64(gpa, arena, sources.items, n_workers);

            var first = true;
            for (tables) |*gt| {
                if (gt.findKey(key)) |gi| {
                    const acc_ptr = &gt.accumulators.items[gi * calls.len + ci];
                    if (acc_ptr.* == .distinct) acc_ptr.distinct.deinit(gpa);
                    if (first) {
                        acc_ptr.* = .{ .count = total };
                        first = false;
                    } else {
                        acc_ptr.* = .{ .count = 0 };
                    }
                }
            }
        }
    }
}

pub fn topKCandidates(
    arena: std.mem.Allocator,
    tables: []const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
) error{OutOfMemory}!?[]const []const u8 {
    const limit = order.limit orelse return null;
    if (order.keys.len == 0) return firstCandidates(arena, tables, std.math.cast(usize, order.offset +| limit) orelse return null);
    if (!order.keys[0].descending) return null;
    const a_idx = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (agg_calls[a_idx].func != .count and agg_calls[a_idx].func != .count_distinct) return null;
    const is_distinct = agg_calls[a_idx].func == .count_distinct;
    const k = std.math.cast(usize, order.offset +| limit) orelse return null;
    if (k == 0) return &.{};
    var groups: usize = 0;
    for (tables) |t| groups += t.groupCount();
    if (groups <= k *| 4) return null;

    const stride = agg_calls.len;
    const Count = struct {
        c: u64,
        g: u32,
        fn of(t: *const expr_agg.GroupTable, g: usize, at: usize, n: usize) u64 {
            return accCount(t.readAcc(g, at, n));
        }
        fn byCount(_: void, a: @This(), b: @This()) std.math.Order {
            return std.math.order(a.c, b.c);
        }
    };

    // Nominees: each table's own k largest counts, by key.
    var nominees: KeyHashMapUnmanaged(void) = .empty;
    var heap: std.PriorityQueue(Count, void, Count.byCount) = .empty;
    try heap.ensureTotalCapacity(arena, k);
    for (tables) |*t| {
        heap.clearRetainingCapacity();
        for (0..t.groupCount()) |g| {
            const entry: Count = .{ .c = Count.of(t, g, a_idx, stride), .g = @intCast(g) };
            if (heap.count() == k) {
                if (entry.c <= heap.peek().?.c) continue;
                _ = heap.pop();
            }
            heap.push(arena, entry) catch unreachable; // within the capacity reserved above
        }
        for (heap.items) |entry| try nominees.put(arena, t.keyAt(entry.g), {});
    }
    if (nominees.count() < k) return null;

    // tau: a lower bound on the k-th largest total of all.
    // For plain count, sum_i c_i is exact. For count_distinct, max_i c_i is a lower bound on the true distinct count.
    const totals = try arena.alloc(u64, nominees.count());
    var it = nominees.keyIterator();
    for (totals) |*total| {
        const key = it.next().?.*;
        total.* = 0;
        for (tables) |*t| if (t.findKey(key)) |g| {
            const c = Count.of(t, g, a_idx, stride);
            if (is_distinct) {
                total.* = @max(total.*, c);
            } else {
                total.* +|= c;
            }
        };
    }
    std.mem.sortUnstable(u64, totals, {}, std.sort.desc(u64));
    const tau = totals[k - 1];
    if (tau == 0) return null;

    const m: u64 = tables.len;
    // When most groups pass (counts nearly all equal), the plain merge is cheaper than collecting them.
    var passing: usize = 0;
    for (tables) |*t| {
        for (0..t.groupCount()) |g| passing += @intFromBool(Count.of(t, g, a_idx, stride) *| m >= tau);
    }
    if (passing > groups / 4) return null;
    var kept: std.StringArrayHashMapUnmanaged(void) = .empty;
    try kept.ensureTotalCapacity(arena, passing);
    for (tables) |*t| {
        for (0..t.groupCount()) |g| {
            if (Count.of(t, g, a_idx, stride) *| m >= tau) kept.putAssumeCapacity(t.keyAt(g), {});
        }
    }
    // Candidate merging probes every table for the first position and each aggregate, plus ref construction.
    // Leave broad sets to the parallel count-cut merge before doing another serial DISTINCT pass.
    if ((kept.count() *| tables.len) *| (agg_calls.len +| 2) > groups / 2) return null;
    if (is_distinct) {
        // Second-stage filter for distinct: sum_i c_i is an exact upper bound on the union cardinality.
        // Any key where sum_i c_i < tau provably cannot reach tau.
        var i: usize = 0;
        while (i < kept.count()) {
            const key = kept.keys()[i];
            var sum_c: u64 = 0;
            for (tables) |*t| if (t.findKey(key)) |g| {
                sum_c +|= Count.of(t, g, a_idx, stride);
            };
            if (sum_c < tau) {
                _ = kept.swapRemove(key);
            } else {
                i += 1;
            }
        }
    }
    return kept.keys();
}

fn mergeDistinctPart(
    dst: *expr_agg.DistinctSet,
    src: *expr_agg.DistinctSet,
    allocator: std.mem.Allocator,
) !void {
    if (src.count() == 0) {
        src.deinit(allocator);
        src.* = .{ .shared = dst.shared };
        return;
    }
    if (dst.count() == 0) {
        dst.* = src.*;
        src.* = .{ .shared = dst.shared };
        return;
    }
    if (dst.count() < src.count()) {
        const tmp = dst.*;
        dst.* = src.*;
        src.* = tmp;
    }
    defer {
        src.deinit(allocator);
        src.* = .{ .shared = dst.shared };
    }
    src.releaseConsumedNumericSurplus();
    return dst.merge(src, allocator);
}

pub const DistinctMergeTask = struct {
    gpa: std.mem.Allocator,
    dst_parts: []expr_agg.DistinctSet,
    work_items: []const WorkItem,
    agg_idx: usize,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    pub fn run(t: *DistinctMergeTask) void {
        while (t.err == null) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= t.dst_parts.len) return;
            const dst_part = &t.dst_parts[p];
            for (t.work_items) |item| {
                if (t.agg_idx < item.agg_start or t.agg_idx >= item.agg_start + item.agg_len) continue;
                const src_acc = &item.accumulators[t.agg_idx - item.agg_start];
                if (src_acc.* == .distinct) if (src_acc.distinct.heap()) |ds| {
                    if (ds.parts) |src_parts| {
                        const src_part = &src_parts[p];
                        mergeDistinctPart(dst_part, src_part, t.gpa) catch |e| {
                            t.err = e;
                            return;
                        };
                    }
                };
            }
        }
    }
};

test "topKCandidates: keeps every key whose total can reach the cut, even one no table nominates" {
    // Four tables. h0..h4 count 30 in each (120 in all); s0 and s1 count 26 in each (104), so a cut at 6 ends inside
    // their tie, and each table nominates only one of them; 200 tail keys count 1. The floor is 104, a quarter of it
    // per table: no table alone reaches 104 for any key, so the bound must be divided among the tables.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [16]u8 = undefined;
    for (&tables, 0..) |*t, ti| {
        const add = struct {
            fn add(table: *expr_agg.GroupTable, key: []const u8, n: u64, c: []const expr_agg.AggCall) !void {
                const g = try table.getOrInsert(key, c, 0);
                table.accumulators.items[g].count += n;
            }
        }.add;
        for (0..5) |h| try add(t, try std.fmt.bufPrint(&name_buf, "h{d}", .{h}), 30, &calls);
        // Each table meets the tied pair in a different order, so which one it nominates differs.
        const pair = [2][]const u8{ "s0", "s1" };
        try add(t, pair[ti % 2], 26, &calls);
        try add(t, pair[1 - ti % 2], 26, &calls);
        for (0..50) |i| try add(t, try std.fmt.bufPrint(&name_buf, "t{d}-{d}", .{ ti, i }), 1, &calls);
    }
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" } };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 6 };
    const kept = (try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, order)).?;
    try testing.expectEqual(@as(usize, 7), kept.len);
    for ([_][]const u8{ "h0", "h1", "h2", "h3", "h4", "s0", "s1" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
    // Ascending, a key column first, or no LIMIT: not a cut this applies to.
    for ([_]expr_ast.ResultOrder{
        .{ .keys = &.{.{ .column = 1, .descending = false }}, .limit = 6 },
        .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 6 },
        .{ .keys = &.{.{ .column = 1, .descending = true }} },
    }) |other| try testing.expect(try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, other) == null);
}

test "topKCandidates: dense candidate probes use the ordinary merge" {
    // A heavy table passes a quarter of all refs, but probing those keys across every table saves no merge work.
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [32]u8 = undefined;
    for (&tables, 0..) |*t, ti| for (0..100) |i| {
        const key = try std.fmt.bufPrint(&name_buf, "{d}-{d}", .{ ti, i });
        const g = try t.getOrInsert(key, &calls, ti * 100 + i);
        t.accumulators.items[g].count = if (ti == 0) 10 else 1;
    };
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 };
    try testing.expect(try topKCandidates(arena.allocator(), &tables, &outputs, &calls, order) == null);
}

test "topKCandidates: count_distinct keeps keys that can reach the cut and prunes rest" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 }};
    var tables: [4]expr_agg.GroupTable = undefined;
    for (&tables) |*t| t.* = .init(testing.allocator, 1 << 30);
    defer for (&tables) |*t| t.deinit();
    var name_buf: [16]u8 = undefined;
    for (&tables, 0..) |*t, ti| {
        for (0..5) |h| {
            const key = try std.fmt.bufPrint(&name_buf, "h{d}", .{h});
            const g = try t.getOrInsert(key, &calls, 0);
            var slot = &t.accumulators.items[g].distinct;
            for (0..30) |v| try slot.insertU64(testing.allocator, @intCast(ti * 100 + v));
        }
        for (0..50) |i| {
            const key = try std.fmt.bufPrint(&name_buf, "t{d}-{d}", .{ ti, i });
            const g = try t.getOrInsert(key, &calls, 0);
            try t.accumulators.items[g].distinct.insertU64(testing.allocator, @intCast(i));
        }
    }
    const outputs = [_]BoundOutputColumn{ .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .agg = 0 }, .name = "u" } };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 5 };
    const kept = (try topKCandidates(arena_state.allocator(), &tables, &outputs, &calls, order)).?;
    try testing.expectEqual(@as(usize, 5), kept.len);
    for ([_][]const u8{ "h0", "h1", "h2", "h3", "h4" }) |want| {
        for (kept) |k| {
            if (std.mem.eql(u8, k, want)) break;
        } else return error.TestExpectedKey;
    }
}

test "mergeCandidates: a merge that fails part-way leaks none of the sets already moved out of the tables" {
    // Three tables share keys "a" and "b"; each key's count(DISTINCT) set is moved out of the first table and the
    // others are folded into it, which allocates, so a failure can land after a set has left its table.
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            const calls = [_]expr_agg.AggCall{
                .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
                .{ .func = .count_distinct, .arg = null, .where = null, .alias = "w", .result = .i64 },
            };
            var tables: [3]expr_agg.GroupTable = undefined;
            for (&tables) |*t| t.* = .init(gpa, 1 << 30);
            defer for (&tables) |*t| t.deinit();
            for (&tables, 0..) |*t, ti| for ([_][]const u8{ "a", "b" }) |key| {
                const g = try t.getOrInsert(key, &calls, 0);
                for (0..2) |ci| {
                    var slot = &t.accumulators.items[g * calls.len + ci].distinct;
                    for (0..40) |v| try slot.insertU64(gpa, @intCast(ti * 30 + v));
                }
            };
            var slots: std.ArrayList(MergedSlot) = .empty;
            defer slots.deinit(gpa);
            var accs: std.ArrayList(expr_agg.Accumulator) = .empty;
            defer {
                freeOwnedAccumulatorStrings(gpa, accs.items);
                accs.deinit(gpa);
            }
            var arena_state = std.heap.ArenaAllocator.init(gpa);
            defer arena_state.deinit();
            try mergeCandidates(gpa, gpa, &tables, &.{ "a", "b" }, &calls, 1, &slots, &accs);
            try testing.expectEqual(@as(usize, 2), slots.items.len);
            for (accs.items) |acc| try testing.expectEqual(@as(usize, 100), acc.distinct.count());
        }
    }.run, .{});
}

test "DistinctMergeTask: partitioned distinct sets merge and handle allocation failure without leaks" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var dst: expr_agg.DistinctSet = .{};
            defer dst.deinit(gpa);
            try dst.initPartitions(gpa, null);

            var src1: expr_agg.DistinctSet = .{};
            defer src1.deinit(gpa);
            try src1.initPartitions(gpa, null);
            for ([_]u64{ 1, 2, 3, 100, 200, 300 }) |v| try src1.insertU64(gpa, v);

            var src2: expr_agg.DistinctSet = .{};
            defer src2.deinit(gpa);
            try src2.initPartitions(gpa, null);
            for ([_]u64{ 2, 3, 4, 200, 400 }) |v| try src2.insertU64(gpa, v);

            for (0..dst.parts.?.len) |p| {
                try mergeDistinctPart(&dst.parts.?[p], &src1.parts.?[p], gpa);
                try mergeDistinctPart(&dst.parts.?[p], &src2.parts.?[p], gpa);
            }
            try testing.expectEqual(@as(usize, 8), dst.count());
        }
    }.run, .{});
}


test "DistinctMergeTask: a tight budget funds live source and destination without overlap" {
    const a = std.heap.page_allocator;
    var budget: expr_agg.SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var dst: expr_agg.DistinctSet = .{ .shared = &budget };
    var src: expr_agg.DistinctSet = .{ .shared = &budget };
    defer dst.deinit(a);
    defer src.deinit(a);
    for (0..1000) |i| {
        try dst.insertU64(a, (@as(u64, @intCast(i)) + 1) << 40);
        try src.insertU64(a, (@as(u64, @intCast(i)) + 1001) << 40);
    }
    dst.releaseSlack();
    src.releaseSlack();
    budget.limit = (54 << 10) + 256;
    try mergeDistinctPart(&dst, &src, a);
    try testing.expectEqual(@as(usize, 2000), dst.count());
    try testing.expectEqual(@as(usize, 0), src.count());
    for (0..2000) |i| try testing.expect(dst.val_set.u64.contains((@as(u64, @intCast(i)) + 1) << 40));
    try testing.expect(budget.used.load(.monotonic) <= budget.limit);
}
