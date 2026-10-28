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

test "grouped sum/min over a group with only null values stays NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Group 0 holds rows 0 and 2 (both null); group 1 holds row 1 (value 5).
    const vals = [_]i64{ 9, 5, 9 };
    const dls = [_]u32{ 0, 1, 0 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .i64 = .{ .values = &vals, .def_levels = &dls, .max_def = 1, .has_nulls = true } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};
    const sel = try filter_selection.SelectionVector.init(a, 3);
    const gids = [_]u32{ 0, 1, 0 };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .min, .arg = arg, .where = null, .alias = "m", .result = .i64 },
    };
    var accs: [4]Accumulator = undefined;
    for (0..2) |g| for (calls, 0..) |c, i| {
        accs[g * 2 + i] = Accumulator.init(c);
    };
    for (calls, 0..) |c, i| try updateOneGrouped(a, a, &accs, &gids, i, calls.len, c, &batch, &lookup, &sel);
    try testing.expectEqual(@as(?i128, null), accs[0].sum_i);
    try testing.expectEqual(@as(?i128, null), accs[1].min_i);
    try testing.expectEqual(@as(?i128, 5), accs[2].sum_i);
    try testing.expectEqual(@as(?i128, 5), accs[3].min_i);
}

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

/// The rows of `ids` a sketched table gave a cell instead of a group (`CountSketch.sinkId`) leave `sel`, so no
/// aggregate folds them; in the count pass each also adds one to its cell.
pub fn sinkSketchRows(table: *const GroupTable, ids: []const u32, sel: *filter_selection.SelectionVector) !void {
    const s = table.sketch orelse return;
    if (s.census) |c| if (c.admitted == null and !c.off) {
        // Count pass: every row holds a slot naming its hash, which it records and adds to its cell.
        defer c.batch.clearRetainingCapacity();
        // An abandoned count(DISTINCT) census needs nothing more from this pass: stop mid row group.
        if (c.distinct) if (c.dropped) |d| if (d.load(.monotonic)) return error.CensusAbandoned;
        for (ids, 0..) |id, r| if (sel.isActive(r)) {
            // Any worker may abandon it meanwhile; a row group is a million rows, too long to finish first.
            if (c.distinct and r % 4096 == 0) if (c.dropped.?.load(.monotonic)) return error.CensusAbandoned;
            const hash = c.batch.items[std.math.maxInt(u32) - id];
            if (!c.off) {
                try c.record(hash);
                if (!c.checked and c.dropped != null and c.recorded >= c.check_after) try c.judge(s.cells);
            }
            s.cells[s.cellOf(hash)] +|= 1;
            sel.set(r, false);
        };
        if (c.dropped) |d| if (d.load(.monotonic)) {
            if (c.distinct) return error.CensusAbandoned;
            if (!c.off) c.stop();
        };
        return;
    };
    const hot_lanes = if (s.hot) |hot| hot.counts.len else 0;
    const floor = CountSketch.sinkId(@intCast(s.cells.len + hot_lanes - 1));
    for (ids, 0..) |id, r| {
        if (id < floor or !sel.isActive(r)) continue;
        if (s.admit == null and (if (s.census) |c| c.off else true)) {
            const lane = std.math.maxInt(u32) - id;
            if (lane >= s.cells.len) s.hot.?.add(lane - s.cells.len) else s.cells[lane] +|= 1;
        }
        sel.set(r, false);
    }
}

pub const group_state = @import("aggregate/group_table.zig");
pub const KeyArena = keys_mod.KeyArena;
pub const GroupTable = group_state.GroupTable;

/// A row's place in the input, for first-appearance group order: its row group's ordinal across all inputs (files
/// in order, then their row groups) above the row's index within the row group.
pub fn firstPosition(rg_ordinal: usize, row: usize) u64 {
    return (@as(u64, @intCast(rg_ordinal)) << 32) | @as(u64, @intCast(row));
}

/// Grouped counterpart of `updateOne`: fold each row the call selects into its group's accumulator,
/// `accs[group_id_arr[row] * agg_len + agg_idx]`.
pub fn updateOneGrouped(
    arena: std.mem.Allocator,
    persist: std.mem.Allocator,
    accs: []Accumulator,
    group_id_arr: []const u32,
    agg_idx: usize,
    agg_len: usize,
    call: AggCall,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    outer_sel: *const filter_selection.SelectionVector,
) Error!void {
    var input: PreparedAggInput = try .init(arena, call, batch, column_lookup, outer_sel, false);
    defer input.deinit(arena);
    try updateOneGroupedValues(
        arena,
        persist,
        accs,
        group_id_arr,
        agg_idx,
        agg_len,
        call,
        input.values,
        input.selection(),
        input.unsigned_64,
    );
}

pub fn updateTableGrouped(
    arena: std.mem.Allocator,
    persist: std.mem.Allocator,
    table: *GroupTable,
    ids: []const u32,
    i: usize,
    calls: []const AggCall,
    batch: *const filter_eval.Batch,
    lookup: []const ?usize,
    outer: *const filter_selection.SelectionVector,
) Error!void {
    if (table.compact.stride == 0) return updateOneGrouped(
        arena,
        persist,
        table.accumulators.items,
        ids,
        i,
        calls.len,
        calls[i],
        batch,
        lookup,
        outer,
    );
    if (table.compact.fields[i].shared_sum) return;
    var input: PreparedAggInput = try .init(arena, calls[i], batch, lookup, outer, false);
    defer input.deinit(arena);
    table.compact.foldRows(i, input.values, input.selection(), ids, null, calls[i].offset);
}

/// `updateOneGrouped` over an already-prepared argument and selection.
pub fn updateOneGroupedValues(
    arena: std.mem.Allocator,
    persist: std.mem.Allocator,
    accs: []Accumulator,
    group_id_arr: []const u32,
    agg_idx: usize,
    agg_len: usize,
    call: AggCall,
    values: ?filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
    u64_col: bool,
) Error!void {
    _ = arena;
    if (call.func == .count_distinct) {
        return foldDistinctGrouped(persist, accs, group_id_arr, agg_idx, agg_len, values.?, sel);
    }
    var r: usize = 0;
    while (r < sel.len) : (r += 1) {
        if (sel.isActive(r)) {
            const state = &accs[group_id_arr[r] * agg_len + agg_idx];
            try applyGrouped(persist, state, call, values, r, u64_col);
        }
    }
}

fn applyGrouped(
    persist: std.mem.Allocator,
    state: *Accumulator,
    call: AggCall,
    values: ?filter_eval.Batch.Column,
    r: usize,
    u64_col: bool,
) !void {
    switch (call.func) {
        .count => state.count += 1,
        // count(DISTINCT) folds through `foldDistinctGrouped`, whose inline sets this row-at-a-time path does not know;
        // the partitioned executor never takes it.
        .count_distinct => return error.UnsupportedAggType,
        .sum => try foldSumGroupedOne(state, values.?, r, u64_col, call.offset),
        .min => if (call.result == .bytes)
            try foldMinMaxBytesGroupedOne(persist, true, state, values.?, r)
        else
            foldMinGroupedOne(state, values.?, r, u64_col),
        .max => if (call.result == .bytes)
            try foldMinMaxBytesGroupedOne(persist, false, state, values.?, r)
        else
            foldMaxGroupedOne(state, values.?, r, u64_col),
        .avg => try foldAvgGroupedOne(state, values.?, r, u64_col),
    }
}

/// One prepared aggregate call, reused across partition locks so a scattered window does not re-read its columns.
pub const GroupedPreps = struct {
    mem: []PreparedAggInput,
    arena: std.mem.Allocator,

    pub fn deinit(self: *GroupedPreps) void {
        for (self.mem) |*p| p.deinit(self.arena);
        self.arena.free(self.mem);
        self.* = undefined;
    }

    /// Add `rows` into `accs`. A row the call's own filter or a null argument dropped is skipped.
    pub fn fold(
        self: *const GroupedPreps,
        persist: std.mem.Allocator,
        accs: []Accumulator,
        group_id_arr: []const u32,
        rows: []const u32,
        calls: []const AggCall,
        compact: ?*const @import("agg_state.zig").State,
    ) !void {
        for (calls, self.mem, 0..) |call, prep, i| {
            const sel = prep.selection();
            if (compact) |c| {
                c.foldRows(i, prep.values, sel, group_id_arr, rows, call.offset);
                continue;
            }
            for (rows) |r| {
                if (!sel.isActive(r)) continue;
                const state = &accs[group_id_arr[r] * calls.len + i];
                try applyGrouped(persist, state, call, prep.values, r, prep.unsigned_64);
            }
        }
    }
};

pub fn prepareGrouped(
    arena: std.mem.Allocator,
    calls: []const AggCall,
    batch: *const filter_eval.Batch,
    column_lookup: []const ?usize,
    outer_sel: *const filter_selection.SelectionVector,
) !GroupedPreps {
    const mem = try arena.alloc(PreparedAggInput, calls.len);
    errdefer arena.free(mem);
    var n: usize = 0;
    errdefer for (mem[0..n]) |*p| p.deinit(arena);
    for (calls) |call| {
        mem[n] = try PreparedAggInput.init(
            arena,
            call,
            batch,
            column_lookup,
            outer_sel,
            false,
        );
        n += 1;
    }
    return .{ .mem = mem, .arena = arena };
}

/// Grouped count(DISTINCT) as pairs: instead of a set per group, each row's (key, value) goes to one of `N` lists
/// by the key's hash, and `scan.foldDistinctPairs` later sorts each list and counts every key's distinct values
/// exactly. Neither a set per group nor a cross-worker set merge is built. One per worker, for its own table only.
pub const DistinctPairs = struct {
    pub const part_bits = 8;
    pub const N = 1 << part_bits;
    const recent_bits = 12;
    /// A fixed-width key image of up to 16 bytes, zero-padded, then the value's distinct bits.
    pub const Pair = struct { k0: u64, k1: u64, x: u64 };

    gpa: std.mem.Allocator,
    budget: *SharedBudget,
    /// The table whose accumulators this sink stands in for; its count(DISTINCT) states stay empty.
    table: *const GroupTable,
    /// The one count(DISTINCT) call it serves.
    agg_idx: usize,
    /// Rows go to the sets until this is set (`drainTable` moves what they hold so far).
    active: bool = true,
    /// Automatic selection can retain cache-admitted coded sets beside pairs from other chunks.
    allow_coded_sets: bool = false,
    /// Those sets must join emitted pairs before either representation is counted.
    pending_sets: bool = false,
    /// Values the sets took meanwhile: rows that passed the filter, NULLs left out, which `scan.pairsPayRuntime`
    /// judges the sets by.
    seen: u64 = 0,
    parts: [N]std.ArrayList(Pair) = @splat(.empty),
    charged: usize = 0,
    recent: [1 << recent_bits]Pair = undefined,
    /// Which `recent` slots hold a pair.
    recent_valid: [(1 << recent_bits) / 64]u64 = @splat(0),

    pub fn hasPairs(self: *const DistinctPairs) bool {
        for (self.parts) |part| if (part.items.len != 0) return true;
        return false;
    }

    /// Safe to call again.
    pub fn deinit(self: *DistinctPairs) void {
        for (&self.parts) |*p| {
            p.deinit(self.gpa);
            p.* = .empty;
        }
        self.budget.release(self.charged);
        self.charged = 0;
    }

    /// Move every value the table's count(DISTINCT) sets hold into pairs, leaving the sets empty. Exact: each value
    /// went into a set through `insertU64` as the pair would have carried it.
    pub fn drainTable(self: *DistinctPairs, table: *GroupTable) !void {
        const n_calls = table.accumulators.items.len / @max(table.groupCount(), 1);
        for (0..table.groupCount()) |g| {
            const acc = &table.accumulators.items[g * n_calls + self.agg_idx];
            if (acc.* != .distinct) continue;
            const d = acc.distinct;
            if (d.heap()) |ds| try self.drainSet(@intCast(g), ds) else {
                for (d.raw[1..][0..d.count()]) |v| try self.add(@intCast(g), v);
            }
            acc.distinct.deinit(self.gpa);
            acc.* = .{ .distinct = table.freshDistinct() };
        }
    }

    fn drainSet(self: *DistinctPairs, gid: u32, ds: *const DistinctSet) !void {
        if (ds.parts) |parts| {
            for (parts) |*p| try self.drainSet(gid, p);
            return;
        }
        switch (ds.val_set) {
            .none => {},
            .inline_u64 => |v| for (v.vals[0..v.len]) |x| try self.add(gid, x),
            .bitmap => |bits| {
                var it = bits.iterator();
                while (it.next()) |bit| try self.add(gid, bits.base + bit);
            },
            .u64 => |m| {
                var it = m.keyIterator();
                while (it.next()) |x| try self.add(gid, x.*);
            },
            else => return error.UnsupportedAggType,
        }
    }

    pub fn pairOf(key: []const u8, x: u64) Pair {
        var buf: [16]u8 = @splat(0);
        @memcpy(buf[0..key.len], key);
        return .{
            .k0 = std.mem.readInt(u64, buf[0..8], .little),
            .k1 = std.mem.readInt(u64, buf[8..16], .little),
            .x = x,
        };
    }

    pub fn partOf(p: Pair) usize {
        return @intCast(splitMix64(p.k0 ^ splitMix64(p.k1)) >> (64 - part_bits));
    }

    fn add(self: *DistinctPairs, gid: u32, x: u64) !void {
        const pair = pairOf(self.table.keyAt(gid), x);
        // Skewed input repeats a few pairs most of the time: a small direct-mapped memory of recent ones drops them
        // before they are stored and sorted. Exact either way, as the sort removes any duplicate it lets through.
        const h = splitMix64(pair.k0 ^ splitMix64(pair.k1 ^ splitMix64(pair.x)));
        const at: usize = @intCast(h >> (64 - recent_bits));
        const bit = @as(u64, 1) << @intCast(at % 64);
        const slot = &self.recent[at];
        const valid = self.recent_valid[at / 64] & bit != 0;
        if (valid and slot.k0 == pair.k0 and slot.k1 == pair.k1 and slot.x == pair.x) return;
        slot.* = pair;
        self.recent_valid[at / 64] |= bit;
        const list = &self.parts[DistinctPairs.partOf(pair)];
        if (list.items.len == list.capacity) {
            const want = @max(list.capacity * 2, 256);
            const got = self.budget.drawExact((want - list.capacity) * @sizeOf(Pair)) orelse
                return error.ExceededMemoryBudget;
            self.charged += got;
            try list.ensureTotalCapacityPrecise(self.gpa, want);
        }
        list.appendAssumeCapacity(pair);
    }
};

pub const WinnerCharge = accumulator_mod.WinnerCharge;

/// The scan worker's winner charge while it folds a row group; null during merges.
/// A thread-local for the reason `distinct_pairs` is one: the folds that copy winners see only the accumulator.
pub threadlocal var winner_charge: ?*WinnerCharge = null;

pub const winnerBytes = accumulator_mod.winnerBytes;
pub const persistWinner = accumulator_mod.persistWinner;

/// The worker's pair sink while it scans. A thread-local, not a parameter: every grouped count(DISTINCT) fold, coded
/// or not, reaches `foldDistinctU64` with only the table's accumulators, which identify the table the sink serves.
pub threadlocal var distinct_pairs: ?*DistinctPairs = null;

/// One column type for the whole batch. i64/i32/f64 stay inline in the accumulator until a group spills.
fn foldDistinctGrouped(
    persist: std.mem.Allocator,
    accs: []Accumulator,
    group_id_arr: []const u32,
    agg_idx: usize,
    agg_len: usize,
    col: filter_eval.Batch.Column,
    sel: *const filter_selection.SelectionVector,
) Error!void {
    switch (col) {
        .i64 => |c| try foldDistinctU64(
            persist,
            accs,
            group_id_arr,
            agg_idx,
            agg_len,
            c.values,
            c.def_levels,
            c.max_def,
            sel,
            struct {
                fn bit(v: i64) u64 {
                    return @bitCast(v);
                }
            }.bit,
        ),
        .i32 => |c| try foldDistinctU64(
            persist,
            accs,
            group_id_arr,
            agg_idx,
            agg_len,
            c.values,
            c.def_levels,
            c.max_def,
            sel,
            struct {
                fn bit(v: i32) u64 {
                    return @as(u64, @bitCast(@as(i64, v)));
                }
            }.bit,
        ),
        .f64 => |c| try foldDistinctU64(
            persist,
            accs,
            group_id_arr,
            agg_idx,
            agg_len,
            c.values,
            c.def_levels,
            c.max_def,
            sel,
            struct {
                fn bit(v: f64) u64 {
                    var f = v;
                    if (std.math.isNan(f)) f = std.math.nan(f64) else if (f == 0.0) f = 0.0;
                    return @bitCast(f);
                }
            }.bit,
        ),
        .f32 => |c| try foldDistinctU64(
            persist,
            accs,
            group_id_arr,
            agg_idx,
            agg_len,
            c.values,
            c.def_levels,
            c.max_def,
            sel,
            struct {
                fn bit(v: f32) u64 {
                    var f: f64 = v;
                    if (std.math.isNan(f)) f = std.math.nan(f64) else if (f == 0.0) f = 0.0;
                    return @bitCast(f);
                }
            }.bit,
        ),
        .boolean => |c| try foldDistinctU64(
            persist,
            accs,
            group_id_arr,
            agg_idx,
            agg_len,
            c.values,
            c.def_levels,
            c.max_def,
            sel,
            struct {
                fn bit(v: bool) u64 {
                    return if (v) 1 else 0;
                }
            }.bit,
        ),
        .string, .i128 => {
            // The plan sends only fixed-width numeric values to a pair sink; a set here would be counted twice.
            if (distinct_pairs) |dp| if (dp.active and dp.table.accumulators.items.ptr == accs.ptr)
                return error.UnsupportedAggType;
            var r: usize = 0;
            while (r < sel.len) : (r += 1) {
                if (!sel.isActive(r)) continue;
                try accs[group_id_arr[r] * agg_len + agg_idx].distinct.insertColRow(persist, col, r);
            }
        },
    }
}

fn foldDistinctU64(
    persist: std.mem.Allocator,
    accs: []Accumulator,
    group_id_arr: []const u32,
    agg_idx: usize,
    agg_len: usize,
    values: anytype,
    def_levels: ?[]const u32,
    max_def: u32,
    sel: *const filter_selection.SelectionVector,
    comptime bit: fn (@typeInfo(@TypeOf(values)).pointer.child) u64,
) Error!void {
    if (distinct_pairs) |dp| if (dp.active and dp.table.accumulators.items.ptr == accs.ptr) {
        if (agg_idx != dp.agg_idx) return error.UnsupportedAggType;
        var r: usize = 0;
        while (r < sel.len) : (r += 1) {
            if (!sel.isActive(r)) continue;
            if (def_levels) |dl| if (dl[r] < max_def) continue;
            try dp.add(group_id_arr[r], bit(values[r]));
        }
        return;
    };
    var fed: u64 = 0;
    defer if (distinct_pairs) |dp| if (dp.table.accumulators.items.ptr == accs.ptr and dp.agg_idx == agg_idx) {
        dp.seen += fed;
    };
    var r: usize = 0;
    while (r < sel.len) : (r += 1) {
        if (!sel.isActive(r)) continue;
        if (def_levels) |dl| if (dl[r] < max_def) continue;
        try accs[group_id_arr[r] * agg_len + agg_idx].distinct.insertU64(persist, bit(values[r]));
        fed += 1;
    }
}

fn foldSumGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool, offset: i64) Error!void {
    switch (state.*) {
        .sum_i => |*s| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => return error.UnsupportedAggType,
            };
            s.* = (s.* orelse 0) + v + offset;
        },
        .sum_f => |*s| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => return error.UnsupportedAggType,
            };
            s.* = (s.* orelse 0) + v;
        },
        .sum_d => |*d| d.add(exactAt(col, r), 1),
        else => return error.UnsupportedAggType,
    }
}

fn foldMinGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) void {
    switch (state.*) {
        .min_i => |*m| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => unreachable,
            };
            if (m.* == null or v < m.*.?) m.* = v;
        },
        .min_f => |*m| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => unreachable,
            };
            foldExtremeF(true, m, v);
        },
        .min_d => |*m| {
            const v = exactAt(col, r);
            if (m.* == null or v < m.*.?) m.* = v;
        },
        else => unreachable,
    }
}

fn foldMaxGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) void {
    switch (state.*) {
        .max_i => |*m| {
            const v = switch (col) {
                .i32 => |c| @as(i128, c.values[r]),
                .i64 => |c| if (unsigned_64) @as(i128, @intCast(@as(u64, @bitCast(c.values[r])))) else @as(i128, c.values[r]),
                else => unreachable,
            };
            if (m.* == null or v > m.*.?) m.* = v;
        },
        .max_f => |*m| {
            const v = switch (col) {
                .f32 => |c| @as(f64, c.values[r]),
                .f64 => |c| c.values[r],
                else => unreachable,
            };
            foldExtremeF(false, m, v);
        },
        .max_d => |*m| {
            const v = exactAt(col, r);
            if (m.* == null or v > m.*.?) m.* = v;
        },
        else => unreachable,
    }
}

fn foldMinMaxBytesGroupedOne(
    persist: std.mem.Allocator,
    is_min: bool,
    state: *Accumulator,
    col: filter_eval.Batch.Column,
    r: usize,
) !void {
    const sv = col.string.values[r];
    switch (state.*) {
        .min_arena, .max_arena => |*value| return value.consider(sv, is_min),
        else => {},
    }
    const m_ptr = if (is_min) &state.min_bytes else &state.max_bytes;
    if (m_ptr.* == null) {
        try persistWinner(persist, m_ptr, sv);
    } else {
        const ord = std.mem.order(u8, sv, m_ptr.*.?);
        const condition = if (is_min) (ord == .lt) else (ord == .gt);
        if (condition) {
            try persistWinner(persist, m_ptr, sv);
        }
    }
}

fn foldAvgGroupedOne(state: *Accumulator, col: filter_eval.Batch.Column, r: usize, unsigned_64: bool) !void {
    if (state.* == .avg_d) return state.avg_d.add(exactAt(col, r), 1);
    switch (col) {
        .i32 => |c| state.avg.isum += c.values[r],
        .i64 => |c| state.avg.isum += if (unsigned_64) @as(u64, @bitCast(c.values[r])) else c.values[r],
        .f32 => |c| state.avg.sum += c.values[r],
        .f64 => |c| state.avg.sum += c.values[r],
        else => return error.UnsupportedAggType,
    }
    state.avg.count += 1;
}

test "STRGROUP slots grow without key relocation and local clear reuses arena capacity" {
    const a = std.testing.allocator;
    var table = GroupTable.init(a, 64 << 20);
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    var buf: [128]u8 = undefined;
    var first_ptr: ?[*]const u8 = null;
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        const id = try table.getOrInsert(key, &calls, i + 1);
        try std.testing.expectEqual(@as(u32, @intCast(i)), id);
        if (i == 0) first_ptr = table.keyAt(0).ptr;
        try std.testing.expect(first_ptr.? == table.keyAt(0).ptr);
    }
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findKey(key));
        try std.testing.expectEqual(@as(u32, @intCast(i)), try table.getOrInsert(key, &calls, 0));
        try std.testing.expectEqual(@as(u64, 0), table.first.items[i]);
    }
    const blocks = table.strings.?.blocks.items.len;
    const slots = table.slots.ptr;
    table.clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), table.groupCount());
    for (0..1000) |i| {
        const key = try std.fmt.bufPrint(&buf, "reused-{d}", .{i});
        _ = try table.getOrInsert(key, &calls, i);
    }
    try std.testing.expect(slots == table.slots.ptr);
    try std.testing.expectEqual(blocks, table.strings.?.blocks.items.len);
}

test "STRGROUP failed arena inserts and winner allocations preserve ids and ledger cleanup" {
    const Trial = struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: SharedBudget = .{ .limit = 16 << 20 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            defer {
                table.deinit();
                std.debug.assert(budget.used.load(.monotonic) == 0);
            }
            const calls = [_]AggCall{.{ .func = .min, .result = .bytes, .arg = null, .where = null, .alias = "lo" }};
            var buf: [128]u8 = undefined;
            for (0..64) |i| {
                const key = try std.fmt.bufPrint(&buf, "variable-key-{d}", .{i});
                const id = try table.getOrInsert(key, &calls, i);
                try table.accumulators.items[id].min_arena.consider("winner-b", true);
                try table.accumulators.items[id].min_arena.consider("winner-a", true);
                try std.testing.expectEqualStrings("winner-a", table.accumulators.items[id].stringValue().?);
            }
            const before = table.groupCount();
            table.releaseSlack();
            budget.limit = budget.used.load(.monotonic);
            const long = try a.alloc(u8, 131073);
            defer a.free(long);
            @memset(long, 'x');
            try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsert(long, &calls, 0));
            try std.testing.expectEqual(before, table.groupCount());
            try std.testing.expectEqual(@as(?u32, 0), table.findKey("variable-key-0"));
        }
    };
    try Trial.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Trial.run, .{});
}


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


test "SharedBudget: tables share one budget instead of owning fixed slices" {
    const allocator = std.testing.allocator;
    const agg_calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};

    // Two tables under one budget, as parallel workers are: the only busy one may take the whole budget.
    var budget = SharedBudget{ .limit = 4096 };
    var a = GroupTable.init(allocator, 0);
    a.shared = &budget;
    defer a.deinit();
    var b = GroupTable.init(allocator, 0);
    b.shared = &budget;
    defer b.deinit();

    var key_buf: [16]u8 = undefined;
    var placed: usize = 0;
    while (placed < 500) : (placed += 1) {
        const key = std.fmt.bufPrint(&key_buf, "k{d}", .{placed}) catch unreachable;
        _ = b.getOrInsert(key, &agg_calls, 0) catch break;
    }
    try std.testing.expect(b.refs.items.len > 0);
    try std.testing.expect(budget.used.load(.monotonic) > 4096 / 2);
    try std.testing.expect(a.max_memory_bytes == 0); // the idle table reserved nothing

    var full = SharedBudget{ .limit = 8 };
    var c = GroupTable.init(allocator, 0);
    c.shared = &full;
    defer c.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, c.getOrInsert("too-big", &agg_calls, 0));

    // Freed tables return their quota: workers merge, then free.
    var recycle = SharedBudget{ .limit = 4096 };
    {
        var tmp = GroupTable.init(allocator, 0);
        tmp.shared = &recycle;
        _ = try tmp.getOrInsert("k", &agg_calls, 0);
        try std.testing.expect(recycle.used.load(.monotonic) > 0);
        tmp.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), recycle.used.load(.monotonic));
}

test "string min/max by 16-byte prefix agree with a byte-by-byte fold: shared prefixes, zero bytes, nulls" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xb17e);
    const r = prng.random();
    const alphabet = [_]u8{ 0, 1, 'a', 'b', 0xff };
    for (0..200) |_| {
        const n = r.uintLessThan(usize, 300) + 1;
        const vals = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        for (vals, defs) |*v, *d| {
            const len = r.uintLessThan(usize, 22);
            const buf = try a.alloc(u8, len);
            // A long shared prefix in most rows, so the full compare runs too.
            for (buf, 0..) |*c, k| {
                c.* = if (k < 17 and r.uintLessThan(u8, 6) != 0) 'a' else alphabet[r.uintLessThan(usize, alphabet.len)];
            }
            v.* = buf;
            d.* = @intFromBool(r.uintLessThan(u8, 5) != 0);
        }
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| if (r.uintLessThan(u8, 3) == 0) sel.set(i, false);
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = vals, .def_levels = defs, .max_def = 1 } };
        inline for (.{ true, false }) |is_min| {
            var want: ?[]const u8 = null;
            for (vals, defs, 0..) |v, d, i| {
                if (!sel.isActive(i) or d != 1) continue;
                const ord = if (want) |w| std.mem.order(u8, v, w) else .lt;
                if (want == null or (is_min and ord == .lt) or (!is_min and ord == .gt)) want = v;
            }
            var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
            try foldMinMaxBytes(a, is_min, &state, col, &sel);
            const got = if (is_min) state.min_bytes else state.max_bytes;
            if (want) |w| try testing.expectEqualSlices(u8, w, got.?) else try testing.expect(got == null);
        }
    }
}

/// Both NaN, or the same bits (so -0 and +0 differ).
fn sameF(want: f64, got: f64) bool {
    if (std.math.isNan(want)) return std.math.isNan(got);
    return @as(u64, @bitCast(want)) == @as(u64, @bitCast(got));
}

test "float min/max: NaN above every number, -0 below +0, the same in any order and on every path" {
    const nan = std.math.nan(f64);
    const inf = std.math.inf(f64);
    const cases = [_]struct { vals: []const f64, lo: f64, hi: f64 }{
        .{ .vals = &.{ nan, 1, 3 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ 3, nan, 1 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ nan, nan }, .lo = nan, .hi = nan },
        .{ .vals = &.{ 0.0, -0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -0.0, 0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -inf, inf, nan }, .lo = -inf, .hi = nan },
        .{ .vals = &.{ -inf, -inf }, .lo = -inf, .hi = -inf },
    };
    var prng = std.Random.DefaultPrng.init(0x7a7);
    const r = prng.random();
    for (cases) |c| for (0..20) |_| {
        // Values in a random order (each at least once), past 64 rows so the SIMD lanes run; folded one at a time,
        // through the kernel, and as two halves merged: all give the same answer.
        var vals: [130]f64 = undefined;
        for (&vals) |*v| v.* = c.vals[r.uintLessThan(usize, c.vals.len)];
        for (c.vals, 0..) |v, k| vals[k * 7] = v;
        var sel = try filter_selection.SelectionVector.init(std.testing.allocator, vals.len);
        defer sel.deinit();
        inline for (.{ true, false }) |is_min| {
            const want = if (is_min) c.lo else c.hi;
            var one: ?f64 = null;
            for (vals) |v| foldExtremeF(is_min, &one, v);
            var half_a: ?f64 = null;
            var half_b: ?f64 = null;
            for (vals[0..65]) |v| foldExtremeF(is_min, &half_a, v);
            for (vals[65..]) |v| foldExtremeF(is_min, &half_b, v);
            foldExtremeF(is_min, &half_b, half_a.?);
            for ([_]f64{ one.?, half_b.?, simdMinMaxF64(is_min, &vals, &sel).? }) |got| {
                if (!sameF(want, got)) {
                    std.debug.print("{s} of {any}: want {d}, got {d}\n", .{ if (is_min) "min" else "max", c.vals, want, got });
                    return error.TestUnexpectedResult;
                }
            }
        }
    };
}

test "zero float bounds never prove sign even with known zero NaNs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    inline for (.{ schema.Type.FLOAT, schema.Type.DOUBLE }) |physical| {
        const T = if (physical == .FLOAT) f32 else f64;
        const U = if (physical == .FLOAT) u32 else u64;
        var meta = try testI64Meta(a, .empty, 100, .REQUIRED);
        meta.schema.items[1].type = physical;
        const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = physical, .expr_type = .f64 } };
        const Case = struct { lo: T, hi: T, min_folds: bool, max_folds: bool };
        for ([_]Case{
            .{ .lo = -0.0, .hi = 0.0, .min_folds = false, .max_folds = false },
            .{ .lo = 0.0, .hi = 1.0, .min_folds = false, .max_folds = true },
            .{ .lo = -1.0, .hi = -0.0, .min_folds = true, .max_folds = false },
            .{ .lo = 1.5, .hi = 2.5, .min_folds = true, .max_folds = true },
        }) |c| {
            var lo: [@sizeOf(T)]u8 = undefined;
            var hi: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &lo, @bitCast(c.lo), .little);
            std.mem.writeInt(U, &hi, @bitCast(c.hi), .little);
            var path: schema.StringList = .empty;
            try path.append(a, "x");
            var chunks: std.ArrayListUnmanaged(schema.ColumnChunk) = .empty;
            try chunks.append(a, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = physical,
                    .encodings = .empty,
                    .path_in_schema = path,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 0,
                    .total_compressed_size = 0,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                    .statistics = .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 0 },
                },
            });
            const rg: schema.RowGroup = .{ .columns = chunks, .total_byte_size = 0, .num_rows = 100 };
            inline for (.{ AggFunc.min, AggFunc.max }) |func| {
                const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "x", .result = .f64 };
                var state = Accumulator.init(call);
                try testing.expectEqual(if (func == .min) c.min_folds else c.max_folds, try updateOneFromStats(&state, call, &rg, &meta));
            }
        }
    }
}

test "fixed-width group keys match the serialized framing and the byte table" {
    const a = std.testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};

    var ints = [_]i64{ 7, -1, 7 };
    var idef = [_]u32{ 1, 1, 0 };
    const icol: filter_eval.Batch.Column = .{ .i64 = .{
        .values = &ints,
        .def_levels = &idef,
        .max_def = 1,
        .has_nulls = true,
    } };
    var other = [_]i64{4};
    const pair = [_]filter_eval.Batch.Column{ icol, .{ .i64 = .{ .values = &other } } };
    var floats = [_]f64{ -0.0, std.math.nan(f64) };
    const fcol: filter_eval.Batch.Column = .{ .f64 = .{ .values = &floats } };

    var image: [32]u8 = undefined;
    var ser: std.ArrayList(u8) = .empty;
    defer ser.deinit(a);
    const samples = [_]struct { cols: []const filter_eval.Batch.Column, row: usize }{
        .{ .cols = &.{icol}, .row = 0 },
        .{ .cols = &.{icol}, .row = 1 },
        .{ .cols = &.{icol}, .row = 2 },
        .{ .cols = &pair, .row = 0 },
        .{ .cols = &.{fcol}, .row = 0 },
        .{ .cols = &.{fcol}, .row = 1 },
    };
    for (samples) |s| {
        const n = packFixedKey(s.cols, s.row, &image).?;
        try serializeRowKey(&ser, a, s.cols, s.row);
        var back: [32]u8 = undefined;
        const expanded = expandFixed(n, ser.items, &back).?;
        try std.testing.expectEqualSlices(u8, image[0..n], expanded);
        var canon_buf: [48]u8 = undefined;
        try std.testing.expectEqualSlices(u8, ser.items, canonicalKey(image[0..n], &canon_buf));
    }

    var fixed = GroupTable.init(a, 1 << 20);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var bytes = GroupTable.init(a, 1 << 20);
    defer bytes.deinit();
    for ([_]usize{ 0, 2, 1, 0 }, 0..) |row, nvisit| {
        const n = packFixedKey(&.{icol}, row, &image).?;
        try serializeRowKey(&ser, a, &.{icol}, row);
        // The repeated value is asked from an earlier position the second time, and must keep that one.
        const pos: u64 = if (nvisit == 3) 40 else @intCast(100 - row);
        const fid = try fixed.getOrInsert(ser.items, &calls, pos);
        const bid = try bytes.getOrInsert(ser.items, &calls, pos);
        try std.testing.expectEqual(bid, fid);
        try std.testing.expectEqual(bytes.first.items[bid], fixed.first.items[fid]);
        try std.testing.expectEqual(fid, fixed.findKey(image[0..n]).?);
    }
    try std.testing.expectEqual(@as(usize, 3), fixed.groupCount());
    try std.testing.expectEqual(@as(u64, 40), fixed.first.items[0]);

    var tiny = GroupTable.init(a, 0);
    tiny.fixed_stride = 9;
    defer tiny.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, tiny.getOrInsertFixed(image[0..9], &calls, 0));
}

test "fixed-width group insert frees every partial allocation" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const insert = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var t = GroupTable.init(allocator, 1 << 30);
            t.fixed_stride = 9;
            defer t.deinit();
            var i: u64 = 0;
            while (i < 40) : (i += 1) {
                var image: [9]u8 = @splat(0);
                image[0] = 1;
                std.mem.writeInt(u64, image[1..9], i, .little);
                _ = try t.getOrInsertFixed(&image, &calls, i);
                if (i % 3 == 0) _ = try t.getOrInsertFixed(&image, &calls, 1000 + i);
            }
            try std.testing.expectEqual(@as(usize, 40), t.groupCount());
        }
    }.run;
    // In-place resize would make the failure count depend on what else the thread allocated.
    const NoResize = struct {
        child: std.mem.Allocator,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawAlloc(len, alignment, ret_addr);
        }
        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(memory, alignment, ret_addr);
        }
    };
    var fixed: NoResize = .{ .child = std.testing.allocator };
    try std.testing.checkAllAllocationFailures(fixed.allocator(), insert, .{});
}

test "a lone string key reaches the same sketch cell framed or unframed" {
    var prng = std.Random.DefaultPrng.init(0x5ce7c4);
    const rand = prng.random();
    var raw: [200]u8 = undefined;
    var framed: [205]u8 = undefined;
    for (16..raw.len) |len| {
        rand.bytes(raw[0..len]);
        framed[0] = 1;
        std.mem.writeInt(u32, framed[1..5], @intCast(len), .little);
        @memcpy(framed[5..][0..len], raw[0..len]);
        try std.testing.expectEqual(sketchHash(framed[0 .. len + 5]), rawSketchHash(raw[0..len]));
    }
}

test "a sketched table counts every row in the count pass and groups only admitted cells in the admit pass" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    // Long keys take the unframed path, short ones the framed one; a null key is framed too.
    const vals = [_][]const u8{
        "http://example.com/a-long-enough-path", "short",
        "http://example.com/a-long-enough-path", "http://example.com/another/long/path",
        "short",                                 "",
    };
    const defs = [_]u32{ 1, 1, 1, 1, 1, 0 };
    const cols = [_]filter_eval.Batch.Column{.{ .string = .{
        .values = &vals,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const a = std.testing.allocator;
    for ([_]u8{ 0, 9 }) |fixed| {
        var cells: [1 << 10]u32 = @splat(0);
        var sketch: CountSketch = .{ .cells = &cells, .shift = 64 - 10 };
        var table = GroupTable.init(a, 1 << 20);
        defer table.deinit();
        table.sketch = &sketch;
        var ids: [vals.len]u32 = undefined;
        var sel = try filter_selection.SelectionVector.init(a, vals.len);
        defer sel.deinit();
        if (fixed == 0) {
            var resolver = GroupKeyResolver.init(&cols);
            defer resolver.deinit(a);
            for (&ids, 0..) |*id, r| id.* = try resolver.resolve(a, &table, &cols, r, &calls, r);
        } else {
            // A fixed-width table sketches by its own image hash.
            table.fixed_stride = fixed;
            for (&ids, 0..) |*id, r| {
                var image: [9]u8 = @splat(0);
                image[0] = @intFromBool(r != 5);
                image[1] = @intCast(vals[r].len);
                id.* = try table.getOrInsertFixed(&image, &calls, r);
            }
        }
        try sinkSketchRows(&table, &ids, &sel);
        try std.testing.expectEqual(@as(usize, 0), table.groupCount());
        try std.testing.expectEqual(@as(usize, 0), sel.count());
        var total: u64 = 0;
        for (cells) |c| total += c;
        try std.testing.expectEqual(@as(u64, vals.len), total);
        // The same key always lands in the same cell.
        try std.testing.expectEqual(ids[0], ids[2]);
        try std.testing.expectEqual(ids[1], ids[4]);

        // Admit only the first key's cell: its rows get one group, every other row is dropped uncounted.
        var admit: [(1 << 10) / 64]u64 = @splat(0);
        const cell = std.math.maxInt(u32) - ids[0];
        admit[cell / 64] |= @as(u64, 1) << @intCast(cell % 64);
        sketch.admit = &admit;
        @memset(&cells, 0);
        var sel2 = try filter_selection.SelectionVector.init(a, vals.len);
        defer sel2.deinit();
        if (fixed == 0) {
            var resolver = GroupKeyResolver.init(&cols);
            defer resolver.deinit(a);
            for (&ids, 0..) |*id, r| id.* = try resolver.resolve(a, &table, &cols, r, &calls, r);
        } else for (&ids, 0..) |*id, r| {
            var image: [9]u8 = @splat(0);
            image[0] = @intFromBool(r != 5);
            image[1] = @intCast(vals[r].len);
            id.* = try table.getOrInsertFixed(&image, &calls, r);
        }
        try sinkSketchRows(&table, &ids, &sel2);
        try std.testing.expect(table.groupCount() >= 1);
        try std.testing.expectEqual(@as(u32, 0), ids[0]);
        try std.testing.expectEqual(@as(u64, 0), table.first.items[0]);
        for (ids, 0..) |id, r| try std.testing.expectEqual(id < table.groupCount(), sel2.isActive(r));
        for (cells) |c| try std.testing.expectEqual(@as(u32, 0), c);
    }
}

test "fixed-width groups charge their count(DISTINCT) sets to the shared budget, and a failed insert leaves nothing" {
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 },
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.fixed_stride = 9;
    table.shared = &budget;
    defer table.deinit();
    var image: [32]u8 = @splat(0);
    image[0] = 1;
    const g = try table.getOrInsertFixed(image[0..9], &calls, 0);
    const slot = &table.accumulators.items[g * calls.len + 1].distinct;
    try std.testing.expect(slot.heap() == null);
    try std.testing.expect(slot.budgetPtr() == &budget);

    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var b: SharedBudget = .{ .limit = 1 << 20 };
            var t = GroupTable.init(gpa, 0);
            t.fixed_stride = 9;
            t.shared = &b;
            defer t.deinit();
            var img: [32]u8 = @splat(0);
            for (0..3) |i| {
                img[0] = 1;
                img[1] = @intCast(i);
                _ = try t.getOrInsertFixed(img[0..9], &calls, i);
            }
        }
    }.run, .{});
}

test "string min/max folds keep the old winner when copying a new one fails" {
    // The second batch's winner beats the first; its copy is the allocation that fails. The slot must still hold the
    // first winner, live, so freeing the accumulator afterwards frees it exactly once.
    const batches = [2][2][]const u8{ .{ "m", "q" }, .{ "a", "z" } };
    inline for (.{ true, false }) |grouped| inline for (.{ true, false }) |is_min| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{ .fail_index = 1 });
        const persist = failing.allocator();
        var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
        defer if (if (is_min) state.min_bytes else state.max_bytes) |s| persist.free(s);
        for (batches, 0..) |vals, bi| {
            const col: filter_eval.Batch.Column = .{ .string = .{ .values = &vals } };
            const folded = if (grouped)
                foldMinMaxBytesGroupedOne(persist, is_min, &state, col, if (is_min) 0 else 1)
            else blk: {
                var sel = try filter_selection.SelectionVector.init(std.testing.allocator, vals.len);
                defer sel.deinit();
                break :blk foldMinMaxBytes(persist, is_min, &state, col, &sel);
            };
            if (bi == 0) try folded else try std.testing.expectError(error.OutOfMemory, folded);
        }
        const kept = (if (is_min) state.min_bytes else state.max_bytes).?;
        try std.testing.expectEqualStrings(if (is_min) "m" else "q", kept);
    };
}

test "a table charges each group's count(DISTINCT) set object, fixed-width keys or not" {
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
    };
    const n = 500;
    var plain = GroupTable.init(std.testing.allocator, 1 << 30);
    defer plain.deinit();
    var without = GroupTable.init(std.testing.allocator, 1 << 30);
    defer without.deinit();
    var fixed = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var fixed_without = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed_without.fixed_stride = 9;
    defer fixed_without.deinit();
    for (0..n) |i| {
        var image: [9]u8 = undefined;
        image[0] = 1;
        std.mem.writeInt(i64, image[1..9], @intCast(i), .little);
        _ = try plain.getOrInsert(&image, &calls, i);
        _ = try without.getOrInsert(&image, calls[1..], i);
        _ = try fixed.getOrInsertFixed(&image, &calls, i);
        _ = try fixed_without.getOrInsertFixed(&image, calls[1..], i);
    }
    // Variable tables quote actual array capacities; no distinct-set object exists before a spill.
    const without_cells = without.accumulators.capacity * @sizeOf(Accumulator);
    const plain_cells = plain.accumulators.capacity * @sizeOf(Accumulator);
    try std.testing.expectEqual(without.allocated_bytes - without_cells + plain_cells, plain.allocated_bytes);
    for ([_]*const GroupTable{ &plain, &fixed }) |t| {
        for (t.accumulators.items) |acc| {
            if (acc == .distinct) try std.testing.expect(acc.distinct.heap() == null);
        }
    }
}

test "round 4: detach clears an inline budget" {
    const a = testing.allocator;
    var budget = SharedBudget{ .limit = 4096, .block = 1 };
    var acc = DistinctAcc.initEmpty(&budget);
    defer acc.deinit(a);
    try acc.insertU64(a, 1);
    acc.detach();
    // The count stays. A later spill must not draw on the budget this call just gave up.
    try testing.expect(acc.budgetPtr() == null);
    try testing.expectEqual(@as(usize, 1), acc.count());
    try acc.insertU64(a, 2);
    try acc.insertU64(a, 3);
    try acc.insertU64(a, 4);
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "round 4: private table ceiling includes a spilled object" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        // Clamped after the group exists, so the spill itself has to see the live ceiling.
        t.max_memory_bytes = t.allocated_bytes;
        const slot = &t.accumulators.items[gid].distinct;
        for (0..3) |v| try slot.insertU64(a, v);
        try testing.expectError(error.ExceededMemoryBudget, slot.insertU64(a, 3));
        try testing.expect(slot.heap() == null);
        try testing.expectEqual(@as(usize, 3), slot.count());
        try testing.expectEqual(t.max_memory_bytes, t.allocated_bytes);
    }
}

test "round 4: a spilled object stays inside a private ceiling after the next insert" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const before = t.allocated_bytes;
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        const set_credit = t.accumulators.items[gid].distinct.heap().?.max_memory_bytes;
        try testing.expect(set_credit > 0);
        try testing.expectEqual(before + @sizeOf(DistinctSet) + set_credit, t.allocated_bytes);
        image[0] = 2;
        _ = if (stride == 0)
            try t.getOrInsert("b", &calls, 1)
        else
            try t.getOrInsertFixed(&image, &calls, 1);
        try testing.expect(t.allocated_bytes >= before + @sizeOf(DistinctSet));
        const held = t.allocated_bytes;
        t.accumulators.items[gid].distinct.deinit(a);
        try testing.expectEqual(held - @sizeOf(DistinctSet) - set_credit, t.allocated_bytes);
    }
}

test "round 4: shared budget still owns the spilled object, not the table" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 0);
        t.fixed_stride = stride;
        t.shared = &budget;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const table_before = t.allocated_bytes;
        const used_before = budget.used.load(.monotonic);
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        try testing.expectEqual(table_before, t.allocated_bytes);
        try testing.expect(budget.used.load(.monotonic) >= used_before + @sizeOf(DistinctSet));
    }
}

test "round 4: owned merges preserve reservations on allocation failure" {
    const Helper = struct {
        fn run(a: std.mem.Allocator, dn: usize, sn: usize) !void {
            var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
            defer testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic)) catch
                @panic("reservation leaked");
            var src = DistinctAcc.initEmpty(&budget);
            defer src.deinit(a);
            var dst = DistinctAcc.initEmpty(&budget);
            defer dst.deinit(a);
            for (0..dn) |v| try dst.insertU64(a, v);
            for (2..2 + sn) |v| try src.insertU64(a, v);
            try dst.takeFrom(&src, a);
            const overlap = if (dn > 2) @min(dn - 2, sn) else 0;
            try testing.expectEqual(dn + sn - overlap, dst.count());
            try testing.expectEqual(@as(usize, 0), src.count());
            const reserved = if (dst.heap()) |d| d.max_memory_bytes else 0;
            try testing.expectEqual(reserved, budget.used.load(.monotonic));
        }
    };
    // A failed grow for a duplicate can legitimately return the existing hash-map entry.
    for (0..8) |dn| for (0..8) |sn| for (0..16) |fail_index| {
        var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
        Helper.run(failing.allocator(), dn, sn) catch |err| if (err != error.OutOfMemory) return err;
        try testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    };
}

fn compactAllocationTrial(gpa: std.mem.Allocator, fixed: bool) !void {
    // Keep the allocation-failure sweep deterministic: whether an address can grow in place varies across runs.
    // Disabling remap here forces every replacement through the allocating fallback, so every such allocation is
    // still injected and checked; separate tests exercise the page-backed no-copy path.
    const NoRemap = struct {
        child: std.mem.Allocator,
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
            return self.child.rawAlloc(n, alignment, ra);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(bytes, alignment, ra);
        }
    };
    var stable: NoRemap = .{ .child = gpa };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
        .{ .func = .min, .arg = arg, .where = null, .alias = "lo", .result = .i64 },
        .{ .func = .max, .arg = arg, .where = null, .alias = "hi", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(stable.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = if (fixed) 9 else 0;
    table.compact.configure(&calls);
    defer table.deinit();
    try std.testing.expectEqual(@as(usize, 56), table.compact.stride * 8);
    for (0..90) |k| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], k, .little);
        const g = try table.getOrInsert(&key, &calls, k);
        try std.testing.expectEqual(@as(?i128, null), table.readAcc(g, 1, calls.len).sum_i);
        table.writeAcc(g, 0, calls.len, .{ .count = 2 });
        table.writeAcc(g, 2, calls.len, .{ .avg = .{ .sum = 0, .isum = -5, .count = 1 } });
        table.writeAcc(g, 3, calls.len, .{ .min_i = -5 });
        table.writeAcc(g, 4, calls.len, .{ .max_i = -5 });
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 1, calls.len).sum_i);
        try std.testing.expectEqual(@as(?i128, -5), table.compact.integer(g, 1));
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 3, calls.len).min_i);
        try std.testing.expectEqual(g, try table.getOrInsert(&key, &calls, k + 1));
    }
}

test "compact count and integer tuple allocation failures leave tables owned" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{true});
}

test "compact states refuse float tuples and only share identical integer inputs" {
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
    };
    var state: @import("agg_state.zig").State = .{};
    state.configure(calls[0..1]);
    try std.testing.expectEqual(@as(usize, 8), state.stride * 8);
    state.configure(calls[0..2]);
    try std.testing.expectEqual(@as(usize, 32), state.stride * 8);
    state.configure(&calls);
    try std.testing.expect(state.fields[1].shared_sum);
    calls[1].offset = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    calls[1].offset = 0;
    calls[2].arg.?.col_ref.col_idx = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    var reordered = [_]AggCall{ calls[1], calls[0], calls[2] };
    reordered[2].arg = arg;
    state.configure(&reordered);
    try std.testing.expect(!state.fields[0].shared_sum);
    calls[2].arg.?.col_ref.expr_type = .f64;
    state.configure(&calls);
    try std.testing.expectEqual(@as(usize, 0), state.stride);
}

test "batched fixed lookups preserve sparse selections nulls NaNs and signed zeros" {
    const n = 129;
    var values: [n]f64 = undefined;
    var defs: [n]u32 = undefined;
    for (0..n) |r| {
        values[r] = switch (r % 4) {
            0 => -0.0,
            1 => 0.0,
            2 => @bitCast(@as(u64, 0x7ff8000000000001)),
            else => 7.0,
        };
        defs[r] = if (r % 19 == 0) 0 else 1;
    }
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{
        .values = &values,
        .def_levels = &defs,
        .max_def = 1,
        .has_nulls = true,
    } }};
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var sel = try filter_selection.SelectionVector.init(std.testing.allocator, n);
    defer sel.deinit();
    for (0..n) |r| if (r % 11 == 0) {
        sel.set(r, false);
    };
    var scalar = GroupTable.init(std.testing.allocator, 1 << 20);
    defer scalar.deinit();
    var batched = GroupTable.init(std.testing.allocator, 1 << 20);
    defer batched.deinit();
    scalar.fixed_stride = 9;
    batched.fixed_stride = 9;
    var resolver = GroupKeyResolver.init(&cols);
    defer resolver.deinit(std.testing.allocator);
    var ids: [n]u32 = @splat(0);
    try resolver.resolveBatch(std.testing.allocator, &batched, &cols, &calls, 1000, &sel, &ids, null);
    for (0..n) |r| if (sel.isActive(r)) {
        const id = try resolver.resolve(std.testing.allocator, &scalar, &cols, r, &calls, 1000);
        try std.testing.expectEqual(id, ids[r]);
    };
    try std.testing.expectEqual(scalar.groupCount(), batched.groupCount());
    try std.testing.expectEqualSlices(u64, scalar.first.items, batched.first.items);
}

test "resolveBatch on a gathered selection keeps the original first row" {
    const values = [_]i32{ 10, 30 };
    const cols = [_]filter_eval.Batch.Column{.{ .i32 = .{ .values = &values } }};
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var sel = try filter_selection.SelectionVector.init(std.testing.allocator, 2);
    defer sel.deinit();
    var table = GroupTable.init(std.testing.allocator, 1 << 20);
    defer table.deinit();
    var resolver = GroupKeyResolver.init(&cols);
    defer resolver.deinit(std.testing.allocator);
    var ids: [2]u32 = @splat(0);
    const ordinals = [_]u32{ 1, 3 };
    try resolver.resolveBatch(std.testing.allocator, &table, &cols, &calls, 100, &sel, &ids, &ordinals);
    try std.testing.expectEqual(@as(usize, 2), table.first.items.len);
    try std.testing.expectEqual(@as(u64, 101), table.first.items[0]);
    try std.testing.expectEqual(@as(u64, 103), table.first.items[1]);
}

test "compact variable insertion keeps retained capacity charged after allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            const arg: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .INT64,
                .expr_type = .i64,
            } };
            const calls = [_]AggCall{
                .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
                .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
            };
            var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1024 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            table.compact.configure(&calls);
            defer table.deinit();
            _ = table.getOrInsert("key", &calls, 0) catch |err| {
                table.releaseSlack();
                try std.testing.expect(budget.used.load(.monotonic) >= table.compact.held());
                return err;
            };
        }
    }.run, .{});
}

test "boundary: a footer null count outside the row group's rows answers no count from statistics" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    const meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 800, .num_rows = 100 };
    try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    } });
    const cm = &rg.columns.items[0].meta_data.?;
    const Case = struct { nc: i64, want: ?i64 };
    for ([_]Case{
        .{ .nc = 0, .want = 100 },
        .{ .nc = 100, .want = 0 },
        .{ .nc = 37, .want = 63 },
        .{ .nc = 101, .want = null },
        .{ .nc = -1, .want = null },
        .{ .nc = std.math.minInt(i64), .want = null },
        .{ .nc = std.math.maxInt(i64), .want = null },
    }) |case| {
        cm.statistics = .{ .null_count = case.nc };
        try testing.expectEqual(case.want, statPresentCount(&rg, 0, cm, &meta));
    }
}

test "regression: a one-row min/max of a UINT64 column keeps values of 2^63 and up as the column's bits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };
    for ([_]AggFunc{ .min, .max }) |func| {
        const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "m", .result = .i64 };
        var state = Accumulator.init(call);
        if (func == .min) state.min_i = std.math.maxInt(u64) else state.max_i = std.math.maxInt(u64);
        const cols = try finalize(a, call, state);
        try std.testing.expectEqual(@as(i64, -1), cols[0].col.i64.values[0]);
    }
}

test "regression: a count(DISTINCT) merge sizes its tables for the result, so none grows while hash-ordered values arrive" {
    // F3: one worker's set merged into another's slot by slot (hash order) while the destination grew as values came
    // took 40 s at -j 3 against 0.4 s presized. A table that grows mid-merge allocates once per doubling; sized
    // first, each destination table allocates at most once. That allocation count is what this pins.
    const gpa = std.testing.allocator;
    const n: u64 = 60_000;
    const Dest = enum { empty, inline_two, small_map, partitioned };
    inline for (.{ u64, i128 }) |T| for ([_]bool{ false, true }) |other_parted| for (std.enums.values(Dest)) |dest_shape| {
        var other: DistinctSet = .{};
        defer other.deinit(gpa);
        if (other_parted) try other.initPartitions(gpa, null);
        for (0..n) |i| {
            const v: T = @intCast(i * 7919);
            if (T == u64) try other.insertU64(gpa, v) else try other.insertI128(gpa, v);
        }
        var dest: DistinctSet = .{};
        if (dest_shape == .partitioned) try dest.initPartitions(gpa, null);
        // Values both sets hold (the first few multiples), and some only the destination does (odd ones).
        const own: usize = switch (dest_shape) {
            .empty => 0,
            .inline_two => 2,
            .small_map, .partitioned => 50,
        };
        for (0..own) |i| {
            const v: T = @intCast(if (i % 2 == 0) i * 7919 else i * 7919 + 1);
            if (T == u64) try dest.insertU64(gpa, v) else try dest.insertI128(gpa, v);
        }
        const before = dest.count();
        var counting = std.testing.FailingAllocator.init(gpa, .{});
        try dest.merge(&other, counting.allocator());
        try std.testing.expectEqual(@as(usize, n) + before - (own + 1) / 2, dest.count());
        const tables: usize = if (dest_shape == .partitioned) NUMERIC_PARTS else 1;
        std.testing.expect(counting.allocations <= tables) catch |err| {
            std.debug.print("{s} {s} other_parted={}: {d} allocations\n", .{ @typeName(T), @tagName(dest_shape), other_parted, counting.allocations });
            return err;
        };
        dest.deinit(gpa);
    };
}

fn compactBoundsTrial(a: std.mem.Allocator) !void {
    var bounds: Census = .{ .gpa = a, .item_rows = &.{} };
    defer bounds.deinit();
    // Different hashes in ignored bits deliberately collide; each occurrence must survive in the complete class.
    const h: u64 = 0x37ab_1234_5678_0000;
    for (0..1000) |i| try bounds.record(h | @as(u64, @intCast(i)));
    const part: usize = @intCast(bounds.fingerprint(h) >> 40);
    try std.testing.expectEqual(@as(usize, 1000), bounds.partLen(part));
    try std.testing.expectEqual(bounds.fingerprint(h), bounds.fingerprint(h ^ (@as(u64, 255) << 48)));
    for (bounds.full[part].items) |chunk| for (chunk) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    };
    for (bounds.open[part].?[0..bounds.fill[part]]) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    }
    for (0..Census.N_PARTS) |p| try bounds.record(@as(u64, @intCast(p)) << 56);
    try std.testing.expectEqual(@as(u64, 1000 + Census.N_PARTS), bounds.recorded);
}

test "hash identity bounds use four-byte rows and bounded frontiers and free all failed allocations" {
    try std.testing.expectEqual(@as(usize, 512), @sizeOf(Census.Chunk));
    try std.testing.expectEqual(@as(usize, 4), @sizeOf(@TypeOf(@as(Census.Chunk, undefined)[0])));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactBoundsTrial, .{});
}

test "bounded first discovery keeps at most H exact keys and only lowers the shared ordinal" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    for ([_]bool{ false, true }) |fixed| {
        var threshold: std.atomic.Value(u64) = .init(std.math.maxInt(u64));
        var early = GroupTable.init(std.testing.allocator, 1 << 20);
        defer early.deinit();
        var late = GroupTable.init(std.testing.allocator, 1 << 20);
        defer late.deinit();
        early.fixed_stride = if (fixed) 9 else 0;
        late.fixed_stride = early.fixed_stride;
        early.discovery = .{ .limit = 3, .threshold = &threshold };
        late.discovery = early.discovery;
        try std.testing.expect(early.countOnly());
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try late.getOrInsert(&key, &calls, 100 + i);
        }
        try std.testing.expectEqual(@as(u64, 102), threshold.load(.monotonic));
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try early.getOrInsert(&key, &calls, 1 + i);
        }
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
        for (3..1000) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            try std.testing.expectEqual(CountSketch.sinkId(0), try early.getOrInsert(&key, &calls, i + 1));
            try std.testing.expectEqual(CountSketch.sinkId(0), try late.getOrInsert(&key, &calls, i + 100));
        }
        try std.testing.expectEqual(@as(usize, 3), early.groupCount());
        try std.testing.expectEqual(@as(usize, 3), late.groupCount());
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
    }
}

test "distinct bitmaps retain the hash reservation so a late outlier has identical budget admission" {
    const hw = @import("../hw.zig");
    defer hw.test_cpu = null;
    for ([_]usize{ 144, 256, 512, 1024, 4096, 8192 }) |limit| {
        var outcomes: [2]bool = undefined;
        for ([_]usize{ 1, 64 << 10 }, 0..) |cache, mode| {
            hw.test_cpu = .{ .l2 = .{ .bytes = cache, .max_sharing = 1 } };
            var budget: SharedBudget = .{ .limit = limit };
            {
                var set: DistinctSet = .{ .shared = &budget };
                defer set.deinit(testing.allocator);
                outcomes[mode] = true;
                for (0..129) |i| {
                    const v: u64 = if (i == 128) 1 << 48 else i;
                    set.insertU64(testing.allocator, v) catch |e| {
                        try testing.expectEqual(error.ExceededMemoryBudget, e);
                        outcomes[mode] = false;
                        break;
                    };
                }
            }
            try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        }
        try testing.expectEqual(outcomes[0], outcomes[1]);
    }
}

test "shared integer scalar folds preserve exact lanes, NULLs, selections and refusal before mutation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testSharedInteger, .{});
}

fn testSharedInteger(a: std.mem.Allocator) !void {
    const n = 193;
    var values: [n]i64 = undefined;
    var narrow: [n]i32 = undefined;
    var defs: [n]u32 = undefined;
    for (&values, &narrow, &defs, 0..) |*v, *v32, *d, r| {
        v.* = switch (r % 5) {
            0 => std.math.minInt(i64),
            1 => std.math.maxInt(i64),
            else => @as(i64, @intCast(r)) - 97,
        };
        v32.* = @intCast(@as(i64, @intCast(r)) - 97);
        d.* = @intFromBool(r % 11 != 0);
    }
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "all" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "s", .offset = -17 },
        .{ .func = .min, .arg = arg, .result = .i64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = arg, .result = .i64, .where = null, .alias = "hi" },
        .{ .func = .avg, .arg = arg, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = arg, .result = .i64, .where = null, .alias = "present" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "again", .offset = 31 },
    };
    for ([_]bool{ false, true }) |is_narrow| {
        const col: filter_eval.Batch.Column = if (is_narrow)
            .{ .i32 = .{ .values = &narrow, .def_levels = &defs, .max_def = 1, .has_nulls = true } }
        else
            .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..4) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                2 => r % 3 != 0,
                else => r % 71 == 0,
            });
            // Exercise every compile-time operation mask and repeat updates from nonempty states.
            for (0..8) |mask| {
                var handled: [calls.len]bool = @splat(false);
                handled[1] = mask & 1 == 0;
                handled[2] = mask & 2 == 0;
                handled[3] = mask & 4 == 0;
                handled[4] = mask & 1 == 0;
                handled[6] = mask & 1 == 0;
                // At least two value calls are needed to justify the common-input kernel.
                if (mask == 0) handled[2] = false;
                var want: [calls.len]Accumulator = undefined;
                var got: [calls.len]Accumulator = undefined;
                for (calls, &want, &got) |call, *w, *g| {
                    w.* = .init(call);
                    g.* = .init(call);
                }
                for (0..2) |_| {
                    for (calls, &want, handled) |call, *w, h| if (!h) {
                        try updateOne(a, a, w, call, &batch, &.{0}, &sel);
                    };
                    try std.testing.expect(try updateSharedInteger(a, &calls, &got, &handled, &batch, &.{0}, &sel));
                }
                for (want, got) |w, g| try std.testing.expectEqualDeep(w, g);
            }
        }
    }
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var states: [calls.len]Accumulator = undefined;
    for (calls, &states) |call, *state| state.* = .init(call);
    const before = states;
    var unsupported = calls;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = true;
    const batch: filter_eval.Batch = .{ .cols = &.{.{ .i64 = .{ .values = &values } }}, .num_rows = n };
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = false;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_32 = true;
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
}

test "shared integer duplicate counts decline levels-only and short value lanes before mutation" {
    const a = std.testing.allocator;
    const n = 193;
    var defs: [n]u32 = undefined;
    var vals: [n]i64 = @splat(7);
    var narrow: [n]i32 = @splat(7);
    for (&defs, 0..) |*d, i| d.* = @intFromBool(i % 11 != 0);
    for ([_]bool{ false, true }) |is32| for ([_]usize{ 0, n - 1 }) |len| {
        const col: filter_eval.Batch.Column = if (is32)
            .{ .i32 = .{
                .values = narrow[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } }
        else
            .{ .i64 = .{
                .values = vals[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } };
        const arg: expr_ast.Expr = .{ .col_ref = .{
            .col_idx = 0,
            .physical_type = if (is32) .INT32 else .INT64,
            .expr_type = .i64,
            .nullable = true,
        } };
        const calls = [_]AggCall{
            .{ .func = .count, .arg = arg, .where = null, .alias = "a", .result = .i64 },
            .{ .func = .count, .arg = arg, .where = null, .alias = "b", .result = .i64 },
        };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..3) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                else => r % 3 != 0,
            });
            var states: [2]Accumulator = .{ .{ .count = 7 }, .{ .count = 19 } };
            const before = states;
            try std.testing.expect(!try updateSharedInteger(
                a,
                &calls,
                &states,
                &.{ false, false },
                &batch,
                &.{0},
                &sel,
            ));
            try std.testing.expectEqualDeep(before, states);
            if (len == 0) {
                var present: u64 = 0;
                for (defs, 0..) |d, r| if (d != 0 and sel.isActive(r)) {
                    present += 1;
                };
                for (calls, &states) |call, *state| try updateOne(a, a, state, call, &batch, &.{0}, &sel);
                try std.testing.expectEqual(7 + present, states[0].count);
                try std.testing.expectEqual(19 + present, states[1].count);
            }
        }
    };
}

test "M1 parallel DISTINCT source charges and releases preserve the private ceiling" {
    const a = std.testing.allocator;
    var table = GroupTable.init(a, 8 * @sizeOf(DistinctSet));
    defer table.deinit();
    const Task = struct {
        table: *GroupTable,
        allocator: std.mem.Allocator,
        cursor: *std.atomic.Value(usize),
        err: ?anyerror = null,
        fn run(t: *@This()) void {
            while (t.err == null) {
                const job = t.cursor.fetchAdd(1, .monotonic);
                if (job >= 256) return;
                var set = t.table.freshDistinct();
                defer set.deinit(t.allocator);
                for (0..4) |i| set.insertU64(t.allocator, job * 7 + i) catch |e| {
                    t.err = e;
                    return;
                };
                std.debug.assert(set.count() == 4);
            }
        }
    };
    var tasks: [8]Task = undefined;
    var cursor = std.atomic.Value(usize).init(0);
    for (&tasks) |*t| t.* = .{ .table = &table, .allocator = a, .cursor = &cursor };
    const spawn = @import("../spawn.zig");
    if (!spawn.runTree(Task, &tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
    for (tasks) |t| if (t.err) |e| return e;
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.set_objects);
}

test "STRGROUP whole-table quote includes first chunks large records and actual capacities" {
    const a = testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    for ([_]usize{ 0, 127, 33000, 70000 }) |width| for ([_]usize{ 1, 33, 300 }) |groups| {
        var table = GroupTable.init(a, 256 << 20);
        defer table.deinit();
        const key = try a.alloc(u8, width);
        defer a.free(key);
        @memset(key, 'x');
        for (0..groups) |i| {
            if (width >= 4) std.mem.writeInt(u32, key[0..4], @intCast(i), .little);
            _ = try table.getOrInsert(key, &calls, i);
        }
        try testing.expect(table.allocated_bytes <= GroupTable.variableTableBytes(groups, width, calls.len));
    };
}

test "grouped string winner replacement funds both live copies before allocating" {
    const a = testing.allocator;
    const old = try a.alloc(u8, 1 << 20);
    defer a.free(old);
    const next = try a.alloc(u8, 1 << 20);
    defer a.free(next);
    for ([_]bool{ false, true }) |is_min| {
        @memset(old, if (is_min) 'z' else 'a');
        @memset(next, if (is_min) 'a' else 'z');
        const values = [_][]const u8{ old, next };
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = &values } };
        var ledger: SharedBudget = .{ .limit = 3 << 19 };
        var charge: WinnerCharge = .{ .ledger = &ledger };
        var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
        winner_charge = &charge;
        defer winner_charge = null;
        defer {
            const value = if (is_min) state.min_bytes else state.max_bytes;
            if (value) |bytes| a.free(bytes);
            charge.settle(0);
            std.debug.assert(ledger.used.load(.monotonic) == 0);
        }
        try foldMinMaxBytesGroupedOne(a, is_min, &state, col, 0);
        const before = ledger.used.load(.monotonic);
        try testing.expectEqual(winnerBytes(old.len), before);
        try testing.expectError(error.ExceededMemoryBudget, foldMinMaxBytesGroupedOne(a, is_min, &state, col, 1));
        try testing.expectEqual(before, ledger.used.load(.monotonic));
        try testing.expectEqualStrings(old, (if (is_min) state.min_bytes else state.max_bytes).?);
        try testing.expect(ledger.peak.load(.monotonic) <= ledger.limit);
    }
}

fn cachedDistinctTrial(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var left: DistinctSet = .{};
    defer left.deinit(a);
    var right: DistinctSet = .{};
    defer right.deinit(a);
    for (0..4096) |i| {
        const text = try std.fmt.allocPrint(arena.allocator(), "long-shared-prefix-{d:0>10}-suffix", .{i % 1001});
        try left.insertBytes(a, text);
        if (i % 2 == 0) try right.insertBytes(a, text);
    }
    try left.insertBytes(a, "");
    try right.merge(&left, a);
    try testing.expectEqual(@as(usize, 1002), right.count());
    var it = right.val_set.bytes.map.keyIterator();
    while (it.next()) |key| try testing.expectEqual(std.hash.Wyhash.hash(0, key.bytes), key.hash);
}

test "cached string DISTINCT grows and merges exact bytes through allocation refusals" {
    try cachedDistinctTrial(testing.allocator);
    try testing.checkAllAllocationFailures(testing.allocator, cachedDistinctTrial, .{});
}

test "shared string identity frames NULL empty embedded zero and large keys once through stored hash growth" {
    const a = testing.allocator;
    var table = GroupTable.init(a, 32 << 20);
    defer table.deinit();
    const big = try a.alloc(u8, 70001);
    defer a.free(big);
    @memset(big, 'x');
    const keys = [_]?[]const u8{ null, "", "a\x00b", "\x00", big };
    for (0..500) |i| {
        var bytes: [64]u8 = undefined;
        const raw = try std.fmt.bufPrint(&bytes, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(raw, &.{}, 900 + i, 7);
    }
    for (keys, 0..) |key, i| {
        const id = try table.getOrInsertStringHashed(key, &.{}, 800 + i, 7);
        try testing.expectEqual(@as(u32, @intCast(500 + i)), id);
        try testing.expectEqual(id, try table.getOrInsertStringHashed(key, &.{}, i, 7));
        try testing.expectEqual(@as(u64, @intCast(i)), table.first.items[id]);
        const frame = table.keyAt(id);
        if (key) |raw| {
            try testing.expectEqual(@as(u8, 1), frame[0]);
            try testing.expectEqualStrings(raw, frame[5..]);
            try testing.expectEqual(raw.len, std.mem.readInt(u32, frame[1..5], .little));
        } else try testing.expectEqualSlices(u8, &.{0}, frame);
    }
    try testing.expectEqual(@as(usize, 505), table.groupCount());
}

test "a riding census drops on every worker's merged cells, not on one worker's slice" {
    const a = testing.allocator;
    const n_cells = 64;
    const admit_n = 8;
    const k = 2;
    var item_rows = [_]u64{0};
    for ([_]bool{ false, true }) |second_separates| {
        var dropped: std.atomic.Value(bool) = .init(false);
        var slot_cells: [2][n_cells]u32 = undefined;
        var slots = [_][]u32{ &slot_cells[0], &slot_cells[1] };
        var merged: Census.MergedCells = .{ .slots = &slots, .quorum = 2 };
        var censuses: [2]Census = undefined;
        for (&censuses, 0..) |*c, wi| c.* = .{
            .worker = wi,
            .gpa = a,
            .item_rows = &item_rows,
            .dropped = &dropped,
            .merged = &merged,
            .cut_k = k,
            .admit_n = admit_n,
        };
        defer for (&censuses) |*c| c.deinit();
        // The first worker's slice holds two hot cells over a sparse floor: its own cells separate the cut.
        var first: [n_cells]u32 = @splat(1);
        first[0] = 40;
        first[1] = 40;
        try std.testing.expect(cellsSeparate(blk: {
            var r: [admit_n + 1]u32 = undefined;
            fullestCells(&first, &r);
            break :blk &r;
        }, k, admit_n));
        try censuses[0].judge(&first);
        // Alone it decides nothing: the quorum is two.
        try std.testing.expect(!dropped.load(.monotonic) and !censuses[0].off);
        // The second worker's slice is either as decisive, or a dense floor that buries the hot cells once merged.
        var second: [n_cells]u32 = @splat(if (second_separates) 1 else 60);
        if (second_separates) {
            second[0] = 40;
            second[1] = 40;
        }
        try censuses[1].judge(&second);
        try std.testing.expectEqual(second_separates, dropped.load(.monotonic));
    }
    // Without merged cells a worker still judges its own.
    var dropped: std.atomic.Value(bool) = .init(false);
    var alone: Census = .{ .gpa = a, .item_rows = &item_rows, .dropped = &dropped, .cut_k = k, .admit_n = admit_n };
    defer alone.deinit();
    var own: [n_cells]u32 = @splat(1);
    own[0] = 40;
    own[1] = 40;
    try alone.judge(&own);
    try std.testing.expect(dropped.load(.monotonic));
}

test "distinct admission numeric fanout preserves bitmap promotion" {
    const a = testing.allocator;
    var budget: SharedBudget = .{ .limit = 64 << 20 };
    {
        var set: DistinctSet = .{};
        defer set.deinit(a);
        try set.initPartitions(a, &budget);
        try testing.expectEqual(@as(usize, 16), set.parts.?.len);
        for (0..65536) |i| try set.insertU64(a, (i * 97) % 100000);
        try testing.expectEqual(@as(usize, 65536), set.count());
        for (set.parts.?) |part| try testing.expect(part.val_set == .bitmap);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "distinct admission string fanout stays wide and merges across fanouts" {
    const a = testing.allocator;
    var narrow: DistinctSet = .{};
    defer narrow.deinit(a);
    try narrow.initPartitions(a, null);
    var wide: DistinctSet = .{};
    defer wide.deinit(a);
    try wide.initStringPartitions(a, null);
    try testing.expectEqual(@as(usize, 64), wide.parts.?.len);
    try narrow.insertBytes(a, "");
    try wide.insertBytes(a, "long shared string");
    try wide.insertBytes(a, "");
    try narrow.merge(&wide, a);
    try testing.expectEqual(@as(usize, 2), narrow.count());
    try wide.merge(&narrow, a);
    try testing.expectEqual(@as(usize, 2), wide.count());
}
