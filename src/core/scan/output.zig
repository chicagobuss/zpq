const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const core_memory = @import("../memory.zig");
const BorrowedSpan = @import("types.zig").BorrowedSpan;
const TypedAnswer = @import("typed_answer.zig").TypedAnswer;
const IntegerStream = @import("integer_stream.zig").IntegerStream;
const ByteStream = @import("byte_stream.zig").ByteStream;
const output_values = @import("output_values.zig");
const AggValue = output_values.AggValue;
const AggOutputItem = output_values.AggOutputItem;
const orderValues = output_values.orderValues;

/// The aggregate answer, owned in the `gpa` passed to `runMultiAggregate`; `deinit` frees all of it, including a
/// partly built one. Without GROUP BY, `aggs` holds one value per aggregate; with it, `group_rows` holds one row per
/// group, its values in `group_cols` order.
pub const AggOutput = struct {
    aggs: []AggOutputItem = &.{},
    group_cols: ?[]const []const u8 = null,
    group_rows: ?[]const []const AggValue = null,
    /// Without GROUP BY: HAVING failed the one group, so the answer has no row (`aggs` still holds its values).
    no_row: bool = false,
    /// One allocation the GROUP BY rows are views into, instead of one allocation a row. A row outside it (built
    /// alone, as tests build them) is its own allocation; `releaseRow` frees a row either way. The strings values own
    /// are always their own allocations.
    slab: []AggValue = &.{},
    /// The GROUP BY answer as typed columns instead of rows (`MultiAggArgs.typed_answer`); `group_rows` is then null.
    typed: ?TypedAnswer = null,
    integer_stream: ?IntegerStream = null,
    byte_stream: ?ByteStream = null,
    string_arenas: []*expr_agg.KeyArena = &.{},
    borrowed_strings: []BorrowedSpan = &.{},
    arena_ledger: ?*core_memory.SharedBudget = null,
    arena_credit: usize = 0,

    pub fn rowCount(self: AggOutput) usize {
        if (self.group_rows) |rows| return rows.len;
        if (self.typed) |answer| return answer.rows;
        if (self.integer_stream) |stream| return stream.state.group_count;
        if (self.byte_stream) |stream| return stream.state.group_count;
        return if (self.no_row or self.aggs.len == 0) 0 else 1;
    }

    pub fn deinit(self: *AggOutput, gpa: std.mem.Allocator) void {
        for (self.aggs) |item| {
            gpa.free(item.alias);
            self.freeValue(gpa, item.value);
        }
        gpa.free(self.aggs);
        if (self.group_rows) |rows| {
            for (rows) |row| self.releaseRow(gpa, row);
            gpa.free(rows);
        }
        gpa.free(self.slab);
        if (self.typed) |*t| t.deinit(gpa);
        if (self.integer_stream) |s| s.state.deinit();
        if (self.byte_stream) |s| s.state.deinit();
        if (self.group_cols) |cols| {
            for (cols) |c| gpa.free(c);
            gpa.free(cols);
        }
        for (self.string_arenas) |strings| {
            strings.deinit(gpa);
            gpa.destroy(strings);
        }
        gpa.free(self.string_arenas);
        gpa.free(self.borrowed_strings);
        self.detachArenaLedger();
        self.* = .{};
    }

    /// A returned engine result outlives its query-local ledger. Its arenas remain owned by the result.
    pub fn detachArenaLedger(self: *AggOutput) void {
        if (self.arena_ledger) |budget| budget.release(self.arena_credit);
        self.arena_ledger = null;
        self.arena_credit = 0;
    }

    /// Free what row `row` owns: its values' strings, and the row itself unless it lies in `slab`.
    pub fn releaseRow(self: *const AggOutput, gpa: std.mem.Allocator, row: []const AggValue) void {
        for (row) |v| self.freeValue(gpa, v);
        self.releaseRowSlice(gpa, row);
    }

    /// Free row `row`'s own allocation, its values left alone (taken over elsewhere or already freed).
    pub fn releaseRowSlice(self: *const AggOutput, gpa: std.mem.Allocator, row: []const AggValue) void {
        if (!self.inSlab(row)) gpa.free(row);
    }

    fn inSlab(self: *const AggOutput, row: []const AggValue) bool {
        if (row.len == 0 or self.slab.len == 0) return row.len == 0;
        const lo = @intFromPtr(self.slab.ptr);
        const at = @intFromPtr(row.ptr);
        return at >= lo and at < lo + self.slab.len * @sizeOf(AggValue);
    }

    /// Move the rows into a slab sized to them when the one they lie in is mostly rows a cut dropped (or they lie in
    /// none), so a short answer does not hold every group's memory. All or nothing: on a failure `self` is unchanged.
    pub fn compactRows(self: *AggOutput, gpa: std.mem.Allocator) error{OutOfMemory}!void {
        const rows = self.group_rows orelse return;
        var cells: usize = 0;
        var loose = false;
        for (rows) |row| {
            cells += row.len;
            loose = loose or !self.inSlab(row);
        }
        if (!loose and self.slab.len <= 2 * cells + 4096) return;
        const fresh = try gpa.alloc(AggValue, cells);
        errdefer gpa.free(fresh);
        const views = try gpa.alloc([]const AggValue, rows.len);
        var at: usize = 0;
        for (rows, views) |row, *v| {
            @memcpy(fresh[at..][0..row.len], row);
            v.* = fresh[at..][0..row.len];
            at += row.len;
        }
        for (rows) |row| self.releaseRowSlice(gpa, row);
        gpa.free(rows);
        gpa.free(self.slab);
        self.slab = fresh;
        self.group_rows = views;
    }

    fn freeValue(self: *const AggOutput, gpa: std.mem.Allocator, v: AggValue) void {
        switch (v) {
            .s => |s| if (!BorrowedSpan.contains(self.borrowed_strings, s)) gpa.free(s),
            else => {},
        }
    }

    pub fn takeTableArenas(
        self: *AggOutput,
        gpa: std.mem.Allocator,
        tables: []const *expr_agg.GroupTable,
        budget: *core_memory.SharedBudget,
    ) !usize {
        var n: usize = 0;
        var blocks: usize = 0;
        for (tables) |t| if (t.strings) |strings| {
            n += 1;
            blocks += strings.blocks.items.len;
        };
        if (n == 0) return 0;
        const metadata_bytes = n * @sizeOf(*expr_agg.KeyArena) + blocks * @sizeOf(BorrowedSpan);
        _ = budget.drawExact(metadata_bytes) orelse return error.ExceededMemoryBudget;
        errdefer budget.release(metadata_bytes);
        const arenas = try gpa.alloc(*expr_agg.KeyArena, n);
        errdefer gpa.free(arenas);
        const spans = try gpa.alloc(BorrowedSpan, blocks);
        var at: usize = 0;
        var si: usize = 0;
        var credit = metadata_bytes;
        for (tables) |t| if (t.strings) |strings| {
            arenas[at] = strings;
            at += 1;
            for (strings.blocks.items) |b| {
                spans[si] = .{ .lo = @intFromPtr(b.bytes.ptr), .hi = @intFromPtr(b.bytes.ptr) + b.bytes.len };
                si += 1;
            }
            const held = strings.held() + @sizeOf(expr_agg.KeyArena);
            // Transfer the live allocation's credit rather than drawing for a duplicate string copy. Final
            // partition tables normally use this ledger; a private child first stops protecting its old floor.
            if (t.shared) |source| {
                if (source != budget) {
                    source.releaseFloor();
                    source.release(held);
                    _ = budget.drawExact(held) orelse unreachable; // Joined scan; source just returned these bytes.
                }
                t.max_memory_bytes -= held;
                credit += held;
            }
            t.allocated_bytes -= held;
            t.strings = null;
            strings.owner = null;
            strings.reserveFn = null;
            strings.settleFn = null;
        };
        std.mem.sortUnstable(BorrowedSpan, spans, {}, struct {
            fn less(_: void, a: BorrowedSpan, b: BorrowedSpan) bool {
                return a.lo < b.lo;
            }
        }.less);
        self.string_arenas = arenas;
        self.borrowed_strings = spans;
        return credit;
    }

    /// Sort, then cut, the GROUP BY rows per `order`. Stable, so ties keep first-appearance order and the answer is
    /// the same at every `-j`. The dropped rows are freed here.
    pub fn applyResultOrder(self: *AggOutput, gpa: std.mem.Allocator, order: expr_ast.ResultOrder) error{OutOfMemory}!void {
        const rows = self.group_rows orelse return;
        if (order.limit) |n| if (n > 0 and order.offset +| n < rows.len / 2) return self.keepFirst(gpa, order, @intCast(order.offset +| n));
        const sorted = try gpa.alloc([]const AggValue, rows.len);
        defer gpa.free(sorted);
        @memcpy(sorted, rows);
        std.sort.block([]const AggValue, sorted, order.keys, rowLessThan);
        const start: usize = @intCast(@min(order.offset, sorted.len));
        const end: usize = if (order.limit) |n| @intCast(@min(sorted.len, start +| n)) else sorted.len;
        // Allocated before anything is freed, so a failure leaves the answer whole.
        const kept = try gpa.dupe([]const AggValue, sorted[start..end]);
        for (sorted[0..start]) |row| self.releaseRow(gpa, row);
        for (sorted[end..]) |row| self.releaseRow(gpa, row);
        gpa.free(rows);
        self.group_rows = kept;
    }

    /// `applyResultOrder` for a cut of `m` rows well short of all of them: the `m` least rows by the order, ties by
    /// their place in the answer (as the stable sort breaks them), kept in a heap of `m` instead of sorting everything.
    fn keepFirst(self: *AggOutput, gpa: std.mem.Allocator, order: expr_ast.ResultOrder, m: usize) error{OutOfMemory}!void {
        const rows = self.group_rows.?;
        const Item = struct { row: []const AggValue, i: usize };
        const Ctx = struct {
            keys: []const expr_ast.OrderKey,
            fn before(keys: []const expr_ast.OrderKey, a: Item, b: Item) bool {
                if (rowLessThan(keys, a.row, b.row)) return true;
                if (rowLessThan(keys, b.row, a.row)) return false;
                return a.i < b.i;
            }
            // The heap's top is the last row kept, so a row that sorts before it replaces it.
            fn later(ctx: @This(), a: Item, b: Item) std.math.Order {
                if (before(ctx.keys, b, a)) return .lt;
                if (before(ctx.keys, a, b)) return .gt;
                return .eq;
            }
        };
        var heap: std.PriorityQueue(Item, Ctx, Ctx.later) = .initContext(.{ .keys = order.keys });
        defer heap.deinit(gpa);
        try heap.ensureTotalCapacity(gpa, m);
        for (rows, 0..) |row, i| {
            const item: Item = .{ .row = row, .i = i };
            if (heap.count() == m) {
                if (!Ctx.before(order.keys, item, heap.peek().?)) continue;
                _ = heap.pop();
            }
            heap.push(gpa, item) catch unreachable; // within the capacity reserved above
        }
        const items = heap.items;
        std.mem.sortUnstable(Item, items, order.keys, Ctx.before);
        const start: usize = @intCast(@min(order.offset, items.len));
        const keep = try gpa.alloc(bool, rows.len);
        defer gpa.free(keep);
        const kept = try gpa.alloc([]const AggValue, items.len - start);
        @memset(keep, false);
        for (items[start..], kept) |item, *k| {
            k.* = item.row;
            keep[item.i] = true;
        }
        for (rows, keep) |row, kp| if (!kp) self.releaseRow(gpa, row);
        gpa.free(rows);
        self.group_rows = kept;
    }

    fn rowLessThan(keys: []const expr_ast.OrderKey, a: []const AggValue, b: []const AggValue) bool {
        for (keys) |key| {
            const ord = orderValues(a[key.column], b[key.column], key.descending);
            if (ord != .eq) return ord == .lt;
        }
        return false;
    }
};

/// Five GROUP BY rows (name, count, mean) covering NULL, NaN, -inf and ties, in first-appearance order.
fn testOrderRows(gpa: std.mem.Allocator) !AggOutput {
    const nan = std.math.nan(f64);
    const names = [_]?[]const u8{ "b", "a", null, "c", "a2" };
    const counts = [_]?i128{ 3, 5, 5, 3, null };
    const means = [_]?f64{ nan, 1.0, null, -std.math.inf(f64), 2.0 };
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, names.len);
    for (rows) |*r| r.* = &.{};
    out.group_rows = rows;
    for (rows, names, counts, means) |*row, name, count, mean| {
        const vals = try gpa.alloc(AggValue, 3);
        @memset(vals, .null_val);
        row.* = vals;
        // Duplicated before the union is assigned: a failed dupe must not leave a `.s` holding garbage.
        if (name) |n| {
            const dup = try gpa.dupe(u8, n);
            vals[0] = .{ .s = dup };
        }
        if (count) |n| vals[1] = .{ .i = n };
        if (mean) |m| vals[2] = .{ .f = m };
    }
    return out;
}

fn expectOrderedNames(order: expr_ast.ResultOrder, want: []const ?[]const u8) !void {
    const gpa = testing.allocator;
    var out = try testOrderRows(gpa);
    defer out.deinit(gpa);
    try out.applyResultOrder(gpa, order);
    const rows = out.group_rows.?;
    try testing.expectEqual(want.len, rows.len);
    for (rows, want) |row, name| {
        if (name) |n| try testing.expectEqualStrings(n, row[0].s) else try testing.expect(row[0] == .null_val);
    }
}

test "applyResultOrder: NULLs last both ways, NaN above numbers, stable ties, offset then limit" {
    // count DESC, name: the two 5s tie on count and the NULL name goes last; the NULL count goes last despite DESC.
    try expectOrderedNames(.{ .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } } }, &.{ "a", null, "b", "c", "a2" });
    // count alone: ties keep first appearance.
    try expectOrderedNames(.{ .keys = &.{.{ .column = 1, .descending = false }} }, &.{ "b", "c", "a", null, "a2" });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = false }} }, &.{ "c", "a", "a2", "b", null });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = true }} }, &.{ "b", "a2", "a", "c", null });
    try expectOrderedNames(.{ .keys = &.{.{ .column = 2, .descending = false }}, .limit = 2, .offset = 1 }, &.{ "a", "a2" });
    try expectOrderedNames(.{ .limit = 0 }, &.{});
    try expectOrderedNames(.{ .offset = 9 }, &.{});
    try expectOrderedNames(.{ .offset = 3, .limit = std.math.maxInt(u64) }, &.{ "c", "a2" });
}

test "applyResultOrder: an allocation failure leaves the rows whole and leaks nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try testOrderRows(gpa);
            defer out.deinit(gpa);
            const before = out.group_rows.?.len;
            out.applyResultOrder(gpa, .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 2 }) catch |err| {
                try testing.expectEqual(before, out.group_rows.?.len);
                return err;
            };
            try testing.expectEqual(@as(usize, 2), out.group_rows.?.len);
        }
    }.run, .{});
}

fn testTieRows(gpa: std.mem.Allocator, n: usize) !AggOutput {
    var prng = std.Random.DefaultPrng.init(42);
    var out: AggOutput = .{};
    errdefer out.deinit(gpa);
    const rows = try gpa.alloc([]const AggValue, n);
    for (rows) |*r| r.* = &.{};
    out.group_rows = rows;
    for (rows, 0..) |*row, i| {
        const vals = try gpa.alloc(AggValue, 3);
        @memset(vals, .null_val);
        row.* = vals;
        vals[0] = .{ .i = @intCast(i) }; // where the row was, to compare orders
        const r = prng.random().uintLessThan(u32, 12);
        if (r != 11) vals[1] = .{ .i = r }; // many ties, some NULL
        vals[2] = .{ .f = @floatFromInt(prng.random().uintLessThan(u32, 3)) };
    }
    return out;
}

test "applyResultOrder: a short cut keeps the rows and order a full sort would" {
    const gpa = testing.allocator;
    for ([_][]const expr_ast.OrderKey{
        &.{.{ .column = 1, .descending = true }},
        &.{.{ .column = 1, .descending = false }},
        &.{ .{ .column = 2, .descending = true }, .{ .column = 1, .descending = false } },
    }) |keys| for ([_][2]u64{ .{ 10, 0 }, .{ 7, 30 }, .{ 1, 0 }, .{ 99, 100 } }) |lo| {
        var whole = try testTieRows(gpa, 500);
        defer whole.deinit(gpa);
        var cut = try testTieRows(gpa, 500);
        defer cut.deinit(gpa);
        try whole.applyResultOrder(gpa, .{ .keys = keys });
        try cut.applyResultOrder(gpa, .{ .keys = keys, .limit = lo[0], .offset = lo[1] });
        const want = whole.group_rows.?[lo[1]..][0..lo[0]];
        const got = cut.group_rows.?;
        try testing.expectEqual(want.len, got.len);
        for (want, got) |w, g| try testing.expectEqual(w[0].i, g[0].i);
    };
}

test "applyResultOrder: a short cut that fails to allocate leaves the rows whole and leaks nothing" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out = try testTieRows(gpa, 40);
            defer out.deinit(gpa);
            out.applyResultOrder(gpa, .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 3 }) catch |err| {
                try testing.expectEqual(@as(usize, 40), out.group_rows.?.len);
                return err;
            };
            try testing.expectEqual(@as(usize, 3), out.group_rows.?.len);
        }
    }.run, .{});
}

