const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const BoundOutputColumn = @import("types.zig").BoundOutputColumn;
const freeOwnedAccumulatorStrings = @import("owned_merge.zig").freeOwnedAccumulatorStrings;

pub const MergedSlot = struct {
    /// Borrowed from a source table. Valid until that table is freed.
    key: []const u8,
    first: u64,
    /// Start of this group's accumulators in the merged list, or the source group id when nothing was folded.
    idx: usize,
};

pub fn accCount(acc: expr_agg.Accumulator) u64 {
    return switch (acc) {
        .count => acc.count,
        .distinct => |d| @intCast(d.count()),
        else => 0,
    };
}

/// Group indices of the only rows a single table's `ORDER BY` one COUNT DESC LIMIT/OFFSET can keep. Null when that
/// is not the order, or when the limit does not drop anyone. Ties break by earlier `first`, matching the stable sort
/// the full merge would do.
/// An answer ordered by one COUNT, descending, and cut to its first `k` rows: the `k` groups with the largest counts,
/// earlier first appearance winning a tie, are all it can return.
pub const CountCut = struct { a_idx: usize, k: usize };

pub fn countCut(outputs: []const BoundOutputColumn, agg_calls: []const expr_agg.AggCall, order: expr_ast.ResultOrder) ?CountCut {
    const limit = order.limit orelse return null;
    if (order.keys.len != 1 or !order.keys[0].descending) return null;
    const a_idx = switch (outputs[order.keys[0].column].source) {
        .agg => |a| a,
        .key => return null,
    };
    if (agg_calls[a_idx].func != .count and agg_calls[a_idx].func != .count_distinct) return null;
    return .{ .a_idx = a_idx, .k = std.math.cast(usize, order.offset +| limit) orelse return null };
}

/// Drop every merged group but the `cut.k` a count cut keeps (`CountCut`), freeing what the dropped ones own. The
/// counts are final: a merge partition holds every copy of its keys.
pub fn keepCountTop(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    calls: []const expr_agg.AggCall,
    cut: CountCut,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    const n = slots.items.len;
    if (n <= cut.k) return;
    if (cut.k == 0) {
        freeOwnedAccumulatorStrings(gpa, accs.items);
        accs.clearRetainingCapacity();
        slots.clearRetainingCapacity();
        return;
    }
    const Item = struct { c: u64, first: u64, s: u32 };
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    defer heap.deinit(lists);
    try heap.ensureTotalCapacity(lists, cut.k);
    const kept_accs = try lists.alloc(expr_agg.Accumulator, cut.k * calls.len);
    defer lists.free(kept_accs);
    for (slots.items, 0..) |slot, si| {
        const item: Item = .{ .c = accCount(accs.items[slot.idx + cut.a_idx]), .first = slot.first, .s = @intCast(si) };
        if (heap.count() == cut.k) {
            const w = heap.peek().?;
            if (!(item.c > w.c or (item.c == w.c and item.first < w.first))) continue;
            _ = heap.pop();
        }
        heap.push(lists, item) catch unreachable; // within the capacity reserved above
    }
    // Nothing fails from here on: the kept groups' state moves, the rest is freed.
    const keep = try lists.alloc(bool, n);
    defer lists.free(keep);
    @memset(keep, false);
    for (heap.items) |item| keep[item.s] = true;
    var kept: usize = 0;
    for (slots.items, keep) |slot, kp| {
        const group = accs.items[slot.idx..][0..calls.len];
        if (!kp) {
            freeOwnedAccumulatorStrings(gpa, group);
            continue;
        }
        @memcpy(kept_accs[kept * calls.len ..][0..calls.len], group);
        slots.items[kept] = .{ .key = slot.key, .first = slot.first, .idx = kept * calls.len };
        kept += 1;
    }
    slots.shrinkRetainingCapacity(kept);
    accs.clearRetainingCapacity();
    accs.appendSliceAssumeCapacity(kept_accs[0 .. kept * calls.len]);
}

pub fn countLimitGroups(
    arena: std.mem.Allocator,
    table: *const expr_agg.GroupTable,
    outputs: []const BoundOutputColumn,
    agg_calls: []const expr_agg.AggCall,
    order: expr_ast.ResultOrder,
) error{OutOfMemory}!?[]u32 {
    const cut = countCut(outputs, agg_calls, order) orelse return null;
    const a_idx = cut.a_idx;
    const k = cut.k;
    if (k == 0) return &.{};
    const n = table.groupCount();
    if (n <= k) return null;

    const stride = agg_calls.len;
    const Item = struct { c: u64, first: u64, g: u32 };
    // The worst kept group is the smallest count, and within that the latest first appearance. It sits at the
    // front of the queue so a better group can replace it.
    const worse = struct {
        fn cmp(_: void, a: Item, b: Item) std.math.Order {
            if (a.c != b.c) return std.math.order(a.c, b.c);
            return std.math.order(b.first, a.first);
        }
    }.cmp;
    var heap: std.PriorityQueue(Item, void, worse) = .empty;
    defer heap.deinit(arena);
    try heap.ensureTotalCapacity(arena, k);
    for (0..n) |g| {
        const item: Item = .{
            .c = accCount(table.readAcc(g, a_idx, stride)),
            .first = table.first.items[g],
            .g = @intCast(g),
        };
        if (heap.count() == k) {
            const worst = heap.peek().?;
            const better = item.c > worst.c or
                (item.c == worst.c and item.first < worst.first);
            if (!better) continue;
            _ = heap.pop();
        }
        heap.push(arena, item) catch unreachable; // within the capacity reserved above
    }
    const ids = try arena.alloc(u32, heap.count());
    for (ids, heap.items) |*id, item| id.* = item.g;
    return ids;
}

test "count limit on one table is the stable top, ties included" {
    const calls = [_]expr_agg.AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = expr_agg.GroupTable.init(testing.allocator, 1 << 20);
    defer table.deinit();
    const spec = [_]struct { key: []const u8, n: u64 }{
        .{ .key = "a", .n = 1 },
        .{ .key = "b", .n = 3 },
        .{ .key = "c", .n = 3 },
        .{ .key = "d", .n = 2 },
        .{ .key = "e", .n = 1 },
    };
    for (spec, 0..) |s, i| {
        const g = try table.getOrInsert(s.key, &calls, i);
        table.accumulators.items[g].count = s.n;
    }
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" },
        .{ .source = .{ .agg = 0 }, .name = "c" },
    };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const top2: expr_ast.ResultOrder = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2 };
    const kept2 = (try countLimitGroups(arena, &table, &outputs, &calls, top2)).?;
    try testing.expectEqual(@as(usize, 2), kept2.len);
    var saw_b = false;
    var saw_c = false;
    for (kept2) |g| {
        saw_b = saw_b or std.mem.eql(u8, table.keyAt(g), "b");
        saw_c = saw_c or std.mem.eql(u8, table.keyAt(g), "c");
    }
    try testing.expect(saw_b and saw_c);

    // Every count ties: the cut is the earliest groups, which is what a stable sort keeps.
    for (table.accumulators.items) |*acc| acc.count = 1;
    const tied: expr_ast.ResultOrder = .{
        .keys = &.{.{ .column = 1, .descending = true }},
        .limit = 2,
        .offset = 1,
    };
    const kept_tie = (try countLimitGroups(arena, &table, &outputs, &calls, tied)).?;
    try testing.expectEqual(@as(usize, 3), kept_tie.len);
    var earliest: [3]bool = @splat(false);
    for (kept_tie) |g| earliest[g] = true;
    try testing.expect(earliest[0] and earliest[1] and earliest[2]);
    try testing.expect(try countLimitGroups(arena, &table, &outputs, &calls, .{ .keys = top2.keys }) == null);
}

