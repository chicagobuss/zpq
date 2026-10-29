const std = @import("std");
const testing = std.testing;
const top_n = @import("top_n.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const BoundOutputColumn = @import("types.zig").BoundOutputColumn;
const Reason = @import("plan_types.zig").Reason;
const SortKey = @import("output_values.zig").SortKey;
const orderSortKeys = @import("output_values.zig").orderSortKeys;
const sortKey = @import("output_values.zig").sortKey;
const materializeOne = @import("materialize.zig").materializeOne;
const freeOwnedAccumulatorStrings = @import("owned_merge.zig").freeOwnedAccumulatorStrings;
const count_cut = @import("count_cut.zig");
const MergedSlot = count_cut.MergedSlot;
const countCut = count_cut.countCut;
const countLimitGroups = count_cut.countLimitGroups;

pub const ResultCut = struct {
    a_idx: usize,
    k: usize,
    descending: bool = true,
    ranking_first: bool = false,
    memory: ?std.mem.Allocator = null,
};

pub fn resultCutReason(
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
) Reason {
    if (countCut(outputs, calls, order) != null) return .result_cut;
    if (unordered) return .result_cut_unordered;
    if (order.keys.len != 1) return .result_cut_secondary_keys;
    const a = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return .result_cut_unsupported,
    };
    const call = calls[a];
    if (call.result == .f64 or (call.result == .avg_f64 and
        (call.arg == null or call.arg.?.typeOf() != .i64))) return .result_cut_float;
    const numeric = switch (call.func) {
        .sum, .min, .max => call.result == .i64 or call.result == .dec,
        .avg => call.result == .avg_dec or (call.result == .avg_f64 and call.arg != null and
            call.arg.?.typeOf() == .i64),
        else => false,
    };
    if (!numeric) return .result_cut_unsupported;
    // Finalizing a losing DECIMAL sum/mean can error. Until its lane is also reduced, keep the full path.
    for (calls, 0..) |payload, i| if (i != a and
        (payload.result == .avg_dec or (payload.result == .dec and payload.func == .sum)))
        return .result_cut_fallible_payload;
    return .result_cut;
}

pub fn resultCut(
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
) ?ResultCut {
    // COUNT's established paths, including unordered mode, remain unchanged.
    if (countCut(outputs, calls, order)) |c| return .{ .a_idx = c.a_idx, .k = c.k };
    const limit = order.limit orelse return null;
    if (order.keys.len == 0 or resultCutReason(outputs, calls, order, unordered) != .result_cut) return null;
    return .{
        .a_idx = outputs[order.keys[0].column].source.agg,
        .k = std.math.cast(usize, order.offset +| limit) orelse return null,
        .descending = order.keys[0].descending,
        .ranking_first = true,
    };
}

pub fn rankValue(acc: expr_agg.Accumulator, call: expr_agg.AggCall) !SortKey {
    return switch (acc) {
        .sum_i, .min_i, .max_i => |v| if (v) |i| .{ .int = i } else .none,
        // Match sortKey(.avg): rounding the exact integer sum once, then dividing, also defines its ties.
        .avg => |a| if (expr_agg.avgSum(a)) |sum|
            .{ .float = sum / @as(f64, @floatFromInt(a.count)) }
        else
            .none,
        .sum_d => |d| decimalRank(try d.result(), call),
        .avg_d => |d| decimalRank(try expr_agg.avgDecMean(call, d), call),
        .min_d, .max_d => |v| decimalRank(v, call),
        else => unreachable,
    };
}

fn decimalRank(v: ?i128, call: expr_agg.AggCall) SortKey {
    return if (v) |i| .{ .dec = .{ .v = i, .spec = expr_agg.resultDec(call) } } else .none;
}

const RankItem = struct { value: SortKey, first: u64, g: u32 };
fn worseRank(descending: bool, a: RankItem, b: RankItem) std.math.Order {
    const ord = orderSortKeys(a.value, b.value, descending);
    return top_n.worstFirst(ord, std.math.order(a.first, b.first));
}
pub const RankHeap = std.PriorityQueue(RankItem, bool, worseRank);

pub fn pushRank(heap: *RankHeap, allocator: std.mem.Allocator, k: usize, item: RankItem) void {
    const compared = if (k != 0 and heap.count() == k) worseRank(heap.context, item, heap.peek().?) else .eq;
    switch (top_n.admission(k, heap.count(), compared)) {
        .skip => return,
        .append => {},
        .replace => _ = heap.pop(),
    }
    heap.push(allocator, item) catch unreachable; // capacity reserved before ranking
}

/// Scratch lives only during a cut, so quota follows actual allocation capacities, including reallocation overlap.
/// Source tables and their distinct sets keep their original allocator and ownership throughout the ranking pass.
pub const CutScratch = struct {
    parent: std.mem.Allocator,
    shared: ?*expr_agg.SharedBudget,
    denied: std.atomic.Value(bool) = .init(false),
    live: std.atomic.Value(usize) = .init(0),

    pub fn allocator(self: *CutScratch) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = noResize, .remap = noRemap, .free = free } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret: usize) ?[*]u8 {
        const self: *CutScratch = @ptrCast(@alignCast(ctx));
        const charged = if (self.shared) |b| b.draw(len) orelse {
            self.denied.store(true, .monotonic);
            return null;
        } else 0;
        const ptr = self.parent.rawAlloc(len, alignment, ret) orelse {
            if (self.shared) |b| b.release(charged);
            return null;
        };
        if (self.shared) |b| b.release(charged - len);
        _ = self.live.fetchAdd(len, .monotonic);
        return ptr;
    }

    pub fn finish(self: *CutScratch) void {
        if (self.shared) |b| b.release(self.live.load(.monotonic));
    }

    fn noResize(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) bool {
        return false;
    }
    fn noRemap(_: *anyopaque, _: []u8, _: std.mem.Alignment, _: usize, _: usize) ?[*]u8 {
        return null;
    }
    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret: usize) void {
        const self: *CutScratch = @ptrCast(@alignCast(ctx));
        self.parent.rawFree(memory, alignment, ret);
        _ = self.live.fetchSub(memory.len, .monotonic);
        if (self.shared) |b| b.release(memory.len);
    }
};

pub fn resultLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
    unordered: bool,
    memory: ?std.mem.Allocator,
) !?[]u32 {
    var cut = resultCut(outputs, calls, order, unordered) orelse return null;
    if (!cut.ranking_first) return countLimitGroups(arena, table, outputs, calls, order);
    cut.memory = memory;
    var scratch: CutScratch = .{ .parent = table.allocator, .shared = null };
    return rankLimitGroups(arena, table, calls, cut, &scratch) catch |e|
        return if (scratch.denied.load(.monotonic)) error.ExceededMemoryBudget else e;
}

fn rankLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    scratch: *CutScratch,
) !?[]u32 {
    if (table.groupCount() <= cut.k) return null;
    const sa = scratch.allocator();
    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(sa);
    const k = @min(cut.k, table.groupCount());
    try heap.ensureTotalCapacity(sa, k);
    for (0..table.groupCount()) |g| {
        // Validate every rank, including losers and LIMIT 0, before dropping groups.
        const value = try rankValue(table.readAcc(g, cut.a_idx, calls.len), calls[cut.a_idx]);
        pushRank(&heap, sa, k, .{ .value = value, .first = table.first.items[g], .g = @intCast(g) });
    }
    const ids = try (cut.memory orelse arena).alloc(u32, heap.count());
    for (ids, heap.items) |*id, item| id.* = item.g;
    return ids;
}

pub fn keepResultTop(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    cut: ResultCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    if (slots.items.len <= cut.k) return;
    const ma = cut.memory orelse lists;
    var heap = RankHeap.initContext(cut.descending);
    defer heap.deinit(ma);
    try heap.ensureTotalCapacity(ma, cut.k);
    for (slots.items, 0..) |s, i| pushRank(&heap, ma, cut.k, .{
        .value = try rankValue(accs.items[s.idx + cut.a_idx], calls[cut.a_idx]),
        .first = s.first,
        .g = @intCast(i),
    });
    const kept = try ma.alloc(expr_agg.Accumulator, cut.k * calls.len);
    defer ma.free(kept);
    const keep = try ma.alloc(bool, slots.items.len);
    defer ma.free(keep);
    @memset(keep, false);
    for (heap.items) |item| keep[item.g] = true;
    var n: usize = 0;
    for (slots.items, keep) |s, kp| {
        const group = accs.items[s.idx..][0..calls.len];
        if (!kp) {
            freeOwnedAccumulatorStrings(gpa, group);
            continue;
        }
        @memcpy(kept[n * calls.len ..][0..calls.len], group);
        slots.items[n] = .{ .key = s.key, .first = s.first, .idx = n * calls.len };
        n += 1;
    }
    slots.shrinkRetainingCapacity(n);
    accs.clearRetainingCapacity();
    accs.appendSliceAssumeCapacity(kept);
}

test "result cut SUM: planner reports every explicit fallback" {
    const outputs = [_]BoundOutputColumn{.{ .source = .{ .agg = 0 }, .name = "s" }};
    var calls = [_]expr_agg.AggCall{.{ .func = .sum, .arg = null, .where = null, .alias = "s", .result = .i64 }};
    const order: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 0, .descending = false }}, .limit = 2 };
    try testing.expectEqual(.result_cut, resultCutReason(&outputs, &calls, order, false));
    try testing.expectEqual(.result_cut_unordered, resultCutReason(&outputs, &calls, order, true));
    try testing.expect(resultCut(&outputs, &calls, order, true) == null);
    const secondary: expr_ast.ResultOrder = .{
        .keys = &.{ .{ .column = 0, .descending = true }, .{ .column = 0, .descending = false } },
        .limit = 2,
    };
    try testing.expectEqual(.result_cut_secondary_keys, resultCutReason(&outputs, &calls, secondary, false));
    try testing.expect(resultCut(&outputs, &calls, secondary, false) == null);
    calls[0].result = .f64;
    try testing.expectEqual(.result_cut_float, resultCutReason(&outputs, &calls, order, false));
    try testing.expect(resultCut(&outputs, &calls, order, false) == null);
    calls[0].result = .i64;
    const payload = [_]expr_agg.AggCall{
        calls[0], .{ .func = .sum, .arg = null, .where = null, .alias = "d", .result = .dec },
    };
    try testing.expectEqual(.result_cut_fallible_payload, resultCutReason(&outputs, &payload, order, false));
    try testing.expect(resultCut(&outputs, &payload, order, false) == null);
    try testing.expect(resultCut(&outputs, &calls, .{ .keys = order.keys }, false) == null);
}

test "result cut extrema AVG DECIMAL: typed final ranks equal emitted sort keys including rounded ties" {
    const int_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const dec_arg: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .dec,
        .dec = .{ .precision = 18, .scale = 4 },
    } };
    const states = [_]expr_agg.Accumulator{
        .{ .min_i = -(@as(i128, 1) << 100) }, .{ .max_i = (@as(i128, 1) << 100) },
        .{ .avg = .{ .isum = 9007199254740993, .sum = 0, .count = 1 } }, // Distinct exact sums tie after f64 rounding.
        .{ .avg = .{ .isum = 9007199254740992, .sum = 0, .count = 1 } },
        .{ .avg = .{ .sum = 0, .count = 0 } },
        .{ .min_d = -10001 },
        .{ .max_d = null },
        .{ .sum_d = .{ .lo = 12345, .n = 2 } },
        .{ .avg_d = .{ .lo = @bitCast(@as(i128, -5)), .hi = -1, .n = 2 } },
    };
    for (states) |state| for ([_]bool{ false, true }) |avg_state| {
        const dec = state == .min_d or state == .max_d or state == .sum_d or state == .avg_d;
        const func: expr_agg.AggFunc = switch (state) {
            .min_i, .min_d => .min,
            .max_i, .max_d => .max,
            .sum_d => .sum,
            else => .avg,
        };
        const call: expr_agg.AggCall = .{
            .func = func,
            .arg = if (dec) dec_arg else int_arg,
            .where = null,
            .alias = "v",
            .result = if (func == .avg)
                (if (dec) .avg_dec else .avg_f64)
            else
                (if (dec) .dec else .i64),
            .state = avg_state and func == .avg,
        };
        const got = try rankValue(state, call);
        const want = sortKey(try materializeOne(testing.allocator, call, state));
        for ([_]bool{ false, true }) |desc| try testing.expectEqual(.eq, orderSortKeys(want, got, desc));
    };
    const avg_call: expr_agg.AggCall = .{
        .func = .avg,
        .arg = int_arg,
        .where = null,
        .alias = "a",
        .result = .avg_f64,
    };
    try testing.expectEqual(.eq, orderSortKeys(
        try rankValue(states[2], avg_call),
        try rankValue(states[3], avg_call),
        true,
    ));
    const dec_call: expr_agg.AggCall = .{
        .func = .avg,
        .arg = dec_arg,
        .where = null,
        .alias = "a",
        .result = .avg_dec,
    };
    // -0.00025: avg's mean at DECIMAL(22, 8); avg_state ranks by it rounded to the argument's scale.
    const exact = try rankValue(states[8], dec_call);
    try testing.expectEqual(@as(i128, -25000), exact.dec.v);
    try testing.expectEqual(@as(u8, 8), exact.dec.spec.scale);
    var state_call = dec_call;
    state_call.state = true;
    const rounded = try rankValue(states[8], state_call);
    try testing.expectEqual(@as(i128, -3), rounded.dec.v);
    try testing.expectEqual(@as(u8, 4), rounded.dec.spec.scale);
}
