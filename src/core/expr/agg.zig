//! Aggregate kernels — sum / count / min / max / avg with optional
//! per-aggregate `FILTER (WHERE pred)` clause.
//!
//! Architectural shape:
//!
//!   AggCall — one call site (e.g. `sum(cost) FILTER (WHERE foo='bar') AS total`)
//!   Accumulator — running state for one AggCall, updated per-RG
//!   updateOne(state, batch, sel, call) — per-RG update path
//!   finalize(state, call) — fold accumulator into a Batch.Column for emit
//!
//! Conditional aggs (the FILTER clause) compose with the outer query
//! `--filter`: the per-agg SelectionVector starts as a copy of the
//! query's surviving rows, then ANDs in the per-agg predicate.
//!
//! The output of an aggregate workload is a 1-row parquet file, one
//! column per agg; avg is its finalized mean. `avg_state` emits two
//! columns (`<alias>__sum`, `<alias>__count`) so downstream re-
//! aggregation across N files preserves the algebra: a tier-2
//! re-aggregate is `sum(<alias>__sum) / sum(<alias>__count)`. You
//! cannot average means; you can sum sums and counts and divide once
//! at the end.

const std = @import("std");
const count_certificate = @import("../count_certificate.zig");
const schema = @import("../schema.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_eval = @import("../filter/eval.zig");
const filter_selection = @import("../filter/selection.zig");
const expr_ast = @import("ast.zig");
const expr_eval = @import("eval.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const agg_coded = @import("agg_coded.zig");
const statistics = @import("../parquet/statistics.zig");
const core_bytes = @import("../bytes.zig");
const core_memory = @import("../memory.zig");
const keyEql = core_bytes.keyEql;
const KeyHashMap = core_bytes.KeyHashMap;
const KeyHashMapUnmanaged = core_bytes.KeyHashMapUnmanaged;

pub const Error = types.Error;

pub const types = @import("aggregate/types.zig");
pub const AggFunc = types.AggFunc;
pub const AggCall = types.AggCall;
pub const ResultShape = types.ResultShape;
pub const resultDec = types.resultDec;
pub const avgDec = types.avgDec;
pub const avgDecMean = types.avgDecMean;
pub const DecSum = types.DecSum;

pub const avgMean = types.avgMean;

pub const distinct = @import("aggregate/distinct.zig");
pub const NUM_PARTS = distinct.NUM_PARTS;
pub const NUMERIC_PARTS = distinct.NUMERIC_PARTS;
pub const INLINE_CAP = distinct.INLINE_CAP;
pub const InlineU64 = distinct.InlineU64;
pub const InlineI128 = distinct.InlineI128;
pub const InlineBytes = distinct.InlineBytes;
const partOf = distinct.partOf;
const DistinctBytes = distinct.DistinctBytes;

pub const DistinctSet = distinct.DistinctSet;
pub const DistinctAcc = distinct.DistinctAcc;
pub const accumulator_mod = @import("aggregate/accumulator.zig");
pub const Accumulator = accumulator_mod.Accumulator;

pub const mergePersistent = accumulator_mod.mergePersistent;

pub const AvgState = types.AvgState;
pub const ResolveError = types.ResolveError;
pub const resolveResult = types.resolveResult;

pub const canStatShortCircuit = scalar_mod.canStatShortCircuit;
pub const statsCoverageComplete = scalar_mod.statsCoverageComplete;
pub const updateOneFromStats = scalar_mod.updateOneFromStats;
const statPresentCount = scalar_mod.statPresentCount;

pub const scalar_mod = @import("aggregate/scalar.zig");
pub const updateOne = scalar_mod.updateOne;
pub const updateSharedInteger = scalar_mod.updateSharedInteger;
pub const minTotal = scalar_mod.minTotal;
pub const maxTotal = scalar_mod.maxTotal;
pub const foldExtremeF = scalar_mod.foldExtremeF;
pub const prefix16 = scalar_mod.prefix16;
pub const intersectPresentMask = scalar_mod.intersectPresentMask;
pub const sumBlocksF64 = scalar_mod.sumBlocksF64;
pub const sumBlocksGatheredF64 = scalar_mod.sumBlocksGatheredF64;
pub const sumBlocksCodedF64 = scalar_mod.sumBlocksCodedF64;
pub const sumConstantF64 = scalar_mod.sumConstantF64;
const PreparedAggInput = scalar_mod.PreparedAggInput;
const foldMinMaxBytes = scalar_mod.foldMinMaxBytes;
const simdSumI64 = scalar_mod.simdSumI64;
const simdMinMaxF64 = scalar_mod.simdMinMaxF64;
const exactAt = scalar_mod.exactAt;

pub const OutputCol = types.OutputCol;
pub const finalize = scalar_mod.finalize;
pub const u64Lane = scalar_mod.u64Lane;

pub const avgSum = types.avgSum;

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

const testI64Meta = scalar_mod.testI64Meta;

pub const keys_mod = @import("aggregate/keys.zig");
pub const isRowNull = keys_mod.isRowNull;
pub const serializeRowKey = keys_mod.serializeRowKey;
pub const fixedGroupStride = keys_mod.fixedGroupStride;
pub const packFixedKey = keys_mod.packFixedKey;
pub const canonicalKey = keys_mod.canonicalKey;
pub const fixedImage = keys_mod.fixedImage;
const expandFixed = keys_mod.expandFixed;
const hashFixed = keys_mod.hashFixed;

pub const GroupKeyResolver = keys_mod.GroupKeyResolver;

pub const SharedBudget = core_memory.SharedBudget;

pub const census_mod = @import("aggregate/census.zig");
pub const CountSketch = census_mod.CountSketch;
pub const Census = census_mod.Census;
pub const FingerprintSet = census_mod.FingerprintSet;
pub const fullestCells = census_mod.fullestCells;
pub const cellsSeparate = census_mod.cellsSeparate;
pub const sketchHash = census_mod.sketchHash;
const splitMix64 = census_mod.splitMix64;
const rawSketchHash = census_mod.rawSketchHash;
const wholeRawHash = census_mod.wholeRawHash;

pub const sinkSketchRows = group_state.sinkSketchRows;

pub const group_state = @import("aggregate/group_table.zig");
pub const KeyArena = keys_mod.KeyArena;
pub const GroupTable = group_state.GroupTable;

pub const grouped_mod = @import("aggregate/grouped.zig");
pub const firstPosition = grouped_mod.firstPosition;
pub const updateOneGrouped = grouped_mod.updateOneGrouped;
pub const updateTableGrouped = grouped_mod.updateTableGrouped;
pub const updateOneGroupedValues = grouped_mod.updateOneGroupedValues;
pub const GroupedPreps = grouped_mod.GroupedPreps;
pub const prepareGrouped = grouped_mod.prepareGrouped;
pub const DistinctPairs = grouped_mod.DistinctPairs;
const foldMinMaxBytesGroupedOne = grouped_mod.foldMinMaxBytesGroupedOne;

pub const WinnerCharge = accumulator_mod.WinnerCharge;

/// The scan worker's winner charge while it folds a row group; null during merges.
/// A thread-local for the reason `distinct_pairs` is one: the folds that copy winners see only the accumulator.
pub threadlocal var winner_charge: ?*WinnerCharge = null;

pub const winnerBytes = accumulator_mod.winnerBytes;
pub const persistWinner = accumulator_mod.persistWinner;

/// The worker's pair sink while it scans. A thread-local, not a parameter: every grouped count(DISTINCT) fold, coded
/// or not, reaches `foldDistinctU64` with only the table's accumulators, which identify the table the sink serves.
pub threadlocal var distinct_pairs: ?*DistinctPairs = null;

test "variable hash growth bitmap migration preserves supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var buf: [24]u8 = undefined;
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.variableHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.variableHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.probeString(key, true, 0).id);
    }
}

test "distinct partition allocation admits first and restores credit on failure" {
    const a = std.testing.allocator;
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var set: DistinctSet = .{};
    defer set.deinit(a);
    var failing = std.testing.FailingAllocator.init(a, .{ .fail_index = 0 });
    try std.testing.expectError(error.OutOfMemory, set.initPartitions(failing.allocator(), &budget));
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    try std.testing.expect(set.parts == null and set.shared == null);
    budget.limit = 1;
    try std.testing.expectError(error.ExceededMemoryBudget, set.initPartitions(failing.allocator(), &budget));
    try std.testing.expectEqual(@as(usize, 0), failing.allocations);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "distinct string arena charges live backing capacity before allocation" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const ptr = std.testing.allocator.rawAlloc(len, alignment, ra) orelse return null;
            self.live += len;
            self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return ptr;
        }
        fn free(ctx: *anyopaque, mem: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.live -= mem.len;
            std.testing.allocator.rawFree(mem, alignment, ra);
        }
    };
    const value = try std.testing.allocator.alloc(u8, 1 << 20);
    defer std.testing.allocator.free(value);
    @memset(value, 'x');
    var budget: SharedBudget = .{ .limit = 1_153_433, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    const a = counter.allocator();
    var set: DistinctSet = .{ .shared = &budget };
    defer set.deinit(a);
    try std.testing.expectError(error.ExceededMemoryBudget, set.insertBytes(a, value));
    try std.testing.expectEqual(@as(usize, 0), set.count());
    try std.testing.expect(counter.live <= set.allocated_bytes);
    try std.testing.expect(set.allocated_bytes <= budget.used.load(.monotonic));
    try std.testing.expect(budget.used.load(.monotonic) <= budget.limit);

    budget.limit = 2 << 20;
    try set.insertBytes(a, value);
    try std.testing.expectEqual(@as(usize, 1), set.count());
    try std.testing.expect(counter.live <= set.allocated_bytes);
    set.releaseSlack();
    try std.testing.expectEqual(set.allocated_bytes, budget.used.load(.monotonic));
    try set.insertBytes(a, value);
    try std.testing.expectEqual(@as(usize, 1), set.count());
    // Sets can move during a merge; the arena's callback must charge and free through its current owner.
    var moved = set;
    set = .{};
    defer moved.deinit(a);
    value[0] = 'y';
    budget.limit = 8 << 20;
    try moved.insertBytes(a, value);
    try std.testing.expectEqual(@as(usize, 2), moved.count());
    try std.testing.expect(counter.live <= moved.allocated_bytes);
    moved.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    budget.limit = 12 << 20;
    var many: DistinctSet = .{ .shared = &budget };
    defer many.deinit(a);
    var refused = false;
    for (0..8) |i| {
        value[0] = @intCast(i);
        many.insertBytes(a, value) catch |err| switch (err) {
            error.ExceededMemoryBudget => {
                refused = true;
                break;
            },
            else => return err,
        };
        try std.testing.expect(counter.live <= many.allocated_bytes);
        try std.testing.expect(many.allocated_bytes <= budget.used.load(.monotonic));
    }
    try std.testing.expect(refused);
    try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    try std.testing.expect(counter.live <= many.allocated_bytes);
    many.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "distinct hash growth refuses before replacing a full map" {
    const a = std.testing.allocator;
    {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(a);
        var i: u64 = 0;
        while (true) : (i += 1) {
            try set.insertU64(a, (i + 1) * (1 << 40));
            if (set.val_set == .u64 and set.val_set.u64.available == 0) break;
        }
        const capacity = set.val_set.u64.capacity();
        set.releaseSlack();
        budget.limit = budget.used.load(.monotonic) + DistinctSet.u64_entry_bytes;
        try set.insertU64(a, 1 << 40);
        try std.testing.expectError(error.ExceededMemoryBudget, set.insertU64(a, (i + 2) * (1 << 40)));
        try std.testing.expectEqual(capacity, set.val_set.u64.capacity());
    }
    {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(a);
        var i: i128 = 0;
        while (true) : (i += 1) {
            try set.insertI128(a, (i + 1) * (@as(i128, 1) << 80));
            if (set.val_set == .i128 and set.val_set.i128.available == 0) break;
        }
        const capacity = set.val_set.i128.capacity();
        set.releaseSlack();
        budget.limit = budget.used.load(.monotonic) + 32;
        try set.insertI128(a, @as(i128, 1) << 80);
        try std.testing.expectError(error.ExceededMemoryBudget, set.insertI128(a, (i + 2) * (@as(i128, 1) << 80)));
        try std.testing.expectEqual(capacity, set.val_set.i128.capacity());
    }
    {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(a);
        var buf: [32]u8 = undefined;
        var i: usize = 0;
        while (true) : (i += 1) {
            try set.insertBytes(a, try std.fmt.bufPrint(&buf, "distinct-{d}", .{i}));
            if (set.val_set == .bytes and set.val_set.bytes.map.available == 0) break;
        }
        const capacity = set.val_set.bytes.map.capacity();
        set.releaseSlack();
        const next = try std.fmt.bufPrint(&buf, "distinct-{d}", .{i + 1});
        budget.limit = budget.used.load(.monotonic) + DistinctSet.bytes_entry_bytes + next.len;
        try set.insertBytes(a, "distinct-0");
        try std.testing.expectError(error.ExceededMemoryBudget, set.insertBytes(a, next));
        try std.testing.expectEqual(capacity, set.val_set.bytes.map.capacity());
    }
}

test "distinct overlap and merge preallocation are admitted before allocation" {
    const a = std.testing.allocator;
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var set: DistinctSet = .{ .shared = &budget };
    defer set.deinit(a);
    var i: u64 = 0;
    while (true) : (i += 1) {
        try set.insertU64(a, (i + 1) << 40);
        if (set.val_set == .u64 and set.val_set.u64.available == 0) break;
    }
    set.releaseSlack();
    const capacity = set.val_set.u64.capacity();
    const count = set.count();
    budget.limit = budget.used.load(.monotonic) + DistinctSet.u64_entry_bytes;
    var counting = std.testing.FailingAllocator.init(a, .{});
    try std.testing.expectError(error.ExceededMemoryBudget, set.insertU64(counting.allocator(), (i + 2) << 40));
    try std.testing.expectEqual(@as(usize, 0), counting.allocations);
    try std.testing.expectEqual(capacity, set.val_set.u64.capacity());
    try std.testing.expectEqual(count, set.count());
    try std.testing.expectError(error.ExceededMemoryBudget, set.reserveFor(counting.allocator(), .u64, 1000));
    try std.testing.expectEqual(@as(usize, 0), counting.allocations);
    budget.limit = 1 << 20;
    try set.insertU64(a, (i + 2) << 40);
    try std.testing.expectEqual(count + 1, set.count());
    try set.reserveFor(a, .u64, 1000);
    set.releaseSlack();
    try std.testing.expect(set.allocated_bytes >= DistinctSet.mapBytes(u64, set.val_set.u64.capacity()));
    try std.testing.expectEqual(set.allocated_bytes, budget.used.load(.monotonic));
}

test "consumed numeric distinct source releases only nonphysical credit" {
    inline for (.{ u64, i128 }) |T| {
        var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(std.testing.allocator);
        for (0..1000) |i| {
            if (T == u64)
                try set.insertU64(std.testing.allocator, (@as(u64, @intCast(i)) + 1) << 40)
            else
                try set.insertI128(std.testing.allocator, (@as(i128, @intCast(i)) + 1) << 80);
        }
        set.releaseSlack();
        const before = budget.used.load(.monotonic);
        const cap = if (T == u64) set.val_set.u64.capacity() else set.val_set.i128.capacity();
        set.releaseConsumedNumericSurplus();
        try std.testing.expectEqual(DistinctSet.mapBytes(T, cap), budget.used.load(.monotonic));
        try std.testing.expect(budget.used.load(.monotonic) <= before);
        if (T == u64) try std.testing.expect(budget.used.load(.monotonic) < before);
        try std.testing.expectEqual(@as(usize, 1000), set.count());
    }
}

test "distinct hash growth remaps without copy when replacement overlap does not fit" {
    const a = std.heap.page_allocator;
    var budget: SharedBudget = .{ .limit = 16 << 20, .block = 1 };
    var set: DistinctSet = .{ .shared = &budget };
    defer set.deinit(a);
    var i: u64 = 0;
    while (true) : (i += 1) {
        try set.insertU64(a, (i + 1) << 40);
        if (set.val_set == .u64 and set.val_set.u64.capacity() >= 512 and set.val_set.u64.available == 0) break;
    }
    set.releaseSlack();
    const count = set.count();
    const old_cap = set.val_set.u64.capacity();
    const next_cap = try DistinctSet.growthCapacity(old_cap, 0, 1);
    const new_extra = DistinctSet.mapBytes(u64, next_cap) -| count * DistinctSet.u64_entry_bytes;
    budget.limit = budget.used.load(.monotonic) + DistinctSet.u64_entry_bytes + (new_extra -| set.map_extra);
    try set.insertU64(a, (i + 2) << 40);
    try std.testing.expectEqual(count + 1, set.count());
    try std.testing.expectEqual(next_cap, set.val_set.u64.capacity());
    for (0..count + 1) |n| try std.testing.expect(set.val_set.u64.contains((@as(u64, @intCast(n)) + 1) << 40));
    try std.testing.expect(budget.used.load(.monotonic) <= budget.limit);

    set.releaseSlack();
    const future = try DistinctSet.growthCapacity(set.val_set.u64.capacity(), set.val_set.u64.available, 1000);
    const reserve_extra = DistinctSet.mapBytes(u64, future) -| set.count() * DistinctSet.u64_entry_bytes;
    budget.limit = budget.used.load(.monotonic) + (reserve_extra -| set.map_extra);
    try set.reserveFor(a, .u64, 1000);
    try std.testing.expectEqual(future, set.val_set.u64.capacity());
    for (0..count + 1) |n| try std.testing.expect(set.val_set.u64.contains((@as(u64, @intCast(n)) + 1) << 40));
}

test "distinct wide and string maps retain identities after no-copy growth" {
    const a = std.heap.page_allocator;
    {
        var budget: SharedBudget = .{ .limit = 16 << 20, .block = 1 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(a);
        var i: i128 = 0;
        while (true) : (i += 1) {
            try set.insertI128(a, (i + 1) * (@as(i128, 1) << 80));
            if (set.val_set == .i128 and set.val_set.i128.capacity() >= 512 and set.val_set.i128.available == 0) break;
        }
        set.releaseSlack();
        const count = set.count();
        const next = try DistinctSet.growthCapacity(set.val_set.i128.capacity(), 0, 1);
        const extra = DistinctSet.mapBytes(i128, next) -| count * 32;
        budget.limit = budget.used.load(.monotonic) + 32 + (extra -| set.map_extra);
        try set.insertI128(a, (i + 2) * (@as(i128, 1) << 80));
        try std.testing.expectEqual(next, set.val_set.i128.capacity());
        for (0..count + 1) |n| try std.testing.expect(set.val_set.i128.contains((@as(i128, @intCast(n)) + 1) * (@as(i128, 1) << 80)));
    }
    {
        var budget: SharedBudget = .{ .limit = 16 << 20, .block = 1 };
        var set: DistinctSet = .{ .shared = &budget };
        defer set.deinit(a);
        var text: [32]u8 = undefined;
        var i: usize = 0;
        while (true) : (i += 1) {
            try set.insertBytes(a, try std.fmt.bufPrint(&text, "key-{d}", .{i}));
            if (set.val_set == .bytes and set.val_set.bytes.map.capacity() >= 512 and set.val_set.bytes.map.available == 0) break;
        }
        set.releaseSlack();
        const count = set.count();
        const next = try DistinctSet.growthCapacity(set.val_set.bytes.map.capacity(), 0, 1);
        const extra = DistinctSet.mapBytes(DistinctBytes.Key, next) -| count * DistinctSet.bytes_entry_bytes;
        const key = try std.fmt.bufPrint(&text, "key-{d}", .{i + 1});
        budget.limit = budget.used.load(.monotonic) + DistinctSet.bytes_entry_bytes + key.len + (extra -| set.map_extra);
        try set.insertBytes(a, key);
        try std.testing.expectEqual(next, set.val_set.bytes.map.capacity());
        for (0..count + 1) |n| {
            const value = try std.fmt.bufPrint(&text, "key-{d}", .{n});
            try std.testing.expect(set.val_set.bytes.map.contains(.{ .bytes = value, .hash = std.hash.Wyhash.hash(0, value) }));
        }
    }
}

test "fixed hash growth bitmap admission and migration preserve supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixedHashed(&key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    try std.testing.expectEqual(@as(usize, 16), table.fixedSlotCount());
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, 0));
    }
}

test "fixed hash growth is ledger funded and refusal preserves its keys" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        observing: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            if (self.observing)
                self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    var table = GroupTable.init(counter.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    var table_live = true;
    defer if (table_live) table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    budget.limit = budget.used.load(.monotonic);
    std.mem.writeInt(u64, key[1..9], 12, .little);
    try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsertFixed(&key, &calls, 12));
    try std.testing.expectEqual(@as(usize, 12), table.groupCount());
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    counter.observing = true;
    try table.growFixedSlots(32, projected);
    counter.observing = false;
    try std.testing.expectEqual(@as(u8, 1), table.fixed_extra_count);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    budget.limit = 1 << 20;
    for (12..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    for (0..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
    table.deinit();
    table_live = false;
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "fixed hash growth uses funded contiguous replacement when headroom permits" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    try table.admit(projected - table.allocated_bytes);
    try table.growFixedSlots(32, projected);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(u8, 0), table.fixed_extra_count);
    try std.testing.expectEqual(@as(usize, 32), table.firstSlots().len);
    try std.testing.expect(budget.peak.load(.monotonic) >= projected + 16 * @sizeOf(GroupTable.FixedSlot));
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
}

test "group buffer overlap is funded before replacement" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        table.fixed_stride = 9;
        defer table.deinit();
        var key: [9]u8 = @splat(0);
        for (0..500) |i| {
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try table.getOrInsertFixed(&key, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        var key: [32]u8 = undefined;
        for (0..500) |i| {
            const value = try std.fmt.bufPrint(&key, "variable-{d}", .{i});
            _ = try table.getOrInsert(value, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        const key = try std.testing.allocator.alloc(u8, 1 << 20);
        defer std.testing.allocator.free(key);
        @memset(key, 'g');
        budget.limit = 1_153_433;
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        _ = try table.getOrInsert(key, &calls, 0);
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
        try std.testing.expect(counter.live <= budget.used.load(.monotonic));
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
