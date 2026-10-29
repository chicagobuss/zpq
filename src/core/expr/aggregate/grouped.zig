const std = @import("std");
const testing = std.testing;
const filter_eval = @import("../../filter/eval.zig");
const filter_selection = @import("../../filter/selection.zig");
const expr_ast = @import("../ast.zig");
const agg_coded = @import("../agg_coded.zig");
const SharedBudget = @import("../../memory.zig").SharedBudget;
const types = @import("types.zig");
const AggCall = types.AggCall;
const Error = types.Error;
const Accumulator = @import("accumulator.zig").Accumulator;
const persistWinner = @import("accumulator.zig").persistWinner;
const GroupTable = @import("group_table.zig").GroupTable;
const DistinctSet = @import("distinct.zig").DistinctSet;
const scalar = @import("scalar.zig");
const PreparedAggInput = scalar.PreparedAggInput;
const exactAt = scalar.exactAt;
const foldExtremeF = scalar.foldExtremeF;
const isRowNull = @import("contracts.zig").isRowNull;
const splitMix64 = @import("census.zig").splitMix64;
// Known exception: the facade owns writable thread-local state until the later contract pass.
const agg_threadlocals = @import("../agg.zig");

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
        compact: ?*const @import("../agg_state.zig").State,
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
            if (agg_threadlocals.distinct_pairs) |dp| if (dp.active and dp.table.accumulators.items.ptr == accs.ptr)
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
    if (agg_threadlocals.distinct_pairs) |dp| if (dp.active and dp.table.accumulators.items.ptr == accs.ptr) {
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
    defer if (agg_threadlocals.distinct_pairs) |dp| if (dp.table.accumulators.items.ptr == accs.ptr and dp.agg_idx == agg_idx) {
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

pub fn foldMinMaxBytesGroupedOne(
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

