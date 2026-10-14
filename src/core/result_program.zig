//! The one stage every aggregate answer passes through after its groups are complete, whatever strategy produced
//! them (scalar, plain or partitioned hash, sketch, census, first-keys, TPUT or ResultCut): the engine runs it on the
//! rows `scan.runMultiAggregate` returns. In SQL order: keep the complete groups HAVING makes TRUE, rank them by
//! ORDER BY, cut them to OFFSET/LIMIT once, then drop the hidden columns HAVING and ranking read
//! (`ResultOrder.hidden`), so no output format ever sees them. Without GROUP BY there is one group, kept or not.
//!
//! The strategies before it may cut rows early only where the cut provably keeps every row this stage would keep;
//! the answer is this stage's either way. `cutOrder` is the one place that decides what they may see of the order.

const std = @import("std");
const scan = @import("scan.zig");
const expr_ast = @import("expr/ast.zig");

pub const ResultProgram = struct {
    order: expr_ast.ResultOrder = .{},
    /// How many leading columns of each GROUP BY row are shown (`MultiAggResult.visible_cols`); null shows all.
    visible: ?usize = null,

    /// The order the strategies before this stage may cut by. A cut ranks groups before HAVING has dropped any, so
    /// under HAVING it would keep the top rows of the wrong set and underfill the answer: the limit is masked there,
    /// the keys and hidden terms kept for binding. The semantic limit stays with this program.
    pub fn cutOrder(order: expr_ast.ResultOrder) expr_ast.ResultOrder {
        var masked = order;
        if (order.having != null) {
            masked.limit = null;
            masked.offset = 0;
        }
        return masked;
    }

    /// Finish `out` in place. `col_types`, one per row column, is cut to the visible columns alongside; it is borrowed
    /// from the scan's arena, so only the slice changes. On failure `out` is whole (unranked or ranked), never torn.
    pub fn finish(
        self: ResultProgram,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) error{OutOfMemory}!void {
        if (self.order.having) |h| try applyHaving(gpa, out, h.*);
        try out.applyResultOrder(gpa, self.order);
        if (out.group_rows == null) return self.projectScalar(gpa, out);
        try project(self.visible, gpa, out, col_types);
        // A cut leaves few of the rows the slab was built for: release the rest. Only memory is at stake, so a
        // refused allocation keeps the larger slab rather than fail an answer already final.
        out.compactRows(gpa) catch {};
    }

    /// Drop each row's columns past the first `visible`, building the narrowed rows in one new slab before anything
    /// is released, so a failure leaves `out` as it was. They take over the visible values (and the strings those
    /// own) from the old rows.
    fn project(
        visible: ?usize,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) error{OutOfMemory}!void {
        const n = visible orelse return;
        const rows = out.group_rows orelse return;
        const cols = out.group_cols.?;
        if (n >= cols.len) return;
        const slab = try gpa.alloc(scan.AggValue, rows.len * n);
        errdefer gpa.free(slab);
        const narrowed = try gpa.alloc([]const scan.AggValue, rows.len);
        errdefer gpa.free(narrowed);
        const names = try gpa.dupe([]const u8, cols[0..n]);
        for (rows, narrowed, 0..) |row, *dst, i| {
            const cells = slab[i * n ..][0..n];
            @memcpy(cells, row[0..n]);
            dst.* = cells;
        }
        for (rows) |row| {
            for (row[n..]) |v| freeValue(gpa, v);
            out.releaseRowSlice(gpa, row);
        }
        gpa.free(rows);
        gpa.free(out.slab);
        for (cols[n..]) |c| gpa.free(c);
        gpa.free(cols);
        out.slab = slab;
        out.group_rows = narrowed;
        out.group_cols = names;
        if (col_types) |ct| if (ct.*) |types| {
            ct.* = types[0..n];
        };
    }

    /// Without GROUP BY the hidden terms are trailing aggregates; drop them from `aggs`.
    fn projectScalar(self: ResultProgram, gpa: std.mem.Allocator, out: *scan.AggOutput) error{OutOfMemory}!void {
        const n = self.visible orelse return;
        if (n >= out.aggs.len) return;
        const kept = try gpa.dupe(scan.AggOutputItem, out.aggs[0..n]);
        for (out.aggs[n..]) |item| {
            gpa.free(item.alias);
            freeValue(gpa, item.value);
        }
        gpa.free(out.aggs);
        out.aggs = kept;
    }

    /// Drop every group `having` does not make TRUE. The kept rows are listed before any is released.
    fn applyHaving(gpa: std.mem.Allocator, out: *scan.AggOutput, having: expr_ast.Having) error{OutOfMemory}!void {
        const rows = out.group_rows orelse {
            const Scalar = struct {
                aggs: []const scan.AggOutputItem,
                fn at(self: @This(), c: usize) scan.AggValue {
                    return self.aggs[c].value;
                }
            };
            out.no_row = truth(having, Scalar{ .aggs = out.aggs }) != true;
            return;
        };
        const Row = struct {
            row: []const scan.AggValue,
            fn at(self: @This(), c: usize) scan.AggValue {
                return self.row[c];
            }
        };
        var n: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row }) == true) n += 1;
        }
        if (n == rows.len) return;
        const kept = try gpa.alloc([]const scan.AggValue, n);
        var k: usize = 0;
        for (rows) |row| {
            if (truth(having, Row{ .row = row }) == true) {
                kept[k] = row;
                k += 1;
            } else out.releaseRow(gpa, row);
        }
        gpa.free(rows);
        out.group_rows = kept;
    }

    /// Three-valued truth of `h` for one result row: null is UNKNOWN.
    fn truth(h: expr_ast.Having, row: anytype) ?bool {
        switch (h) {
            .all => |terms| {
                var unknown = false;
                for (terms) |t| {
                    const v = truth(t, row) orelse {
                        unknown = true;
                        continue;
                    };
                    if (!v) return false;
                }
                return if (unknown) null else true;
            },
            .any => |terms| {
                var unknown = false;
                for (terms) |t| {
                    const v = truth(t, row) orelse {
                        unknown = true;
                        continue;
                    };
                    if (v) return true;
                }
                return if (unknown) null else false;
            },
            .null_check => |nc| return isNull(row.at(nc.column)) != nc.is_not,
            .cmp => |c| {
                const ord = compareLiteral(row.at(c.column), c.literal) orelse return null;
                return c.op.holds(ord);
            },
        }
    }

    fn isNull(v: scan.AggValue) bool {
        return switch (v) {
            .null_val => true,
            .avg => |a| a.count == 0 or a.sum == null,
            .avg_dec => |a| a.count == 0 or a.mean == null,
            else => false,
        };
    }

    fn freeValue(gpa: std.mem.Allocator, v: scan.AggValue) void {
        switch (v) {
            .s => |s| gpa.free(s),
            else => {},
        }
    }
};


/// A HAVING number literal, typed by its text: an integer, an exact decimal (unscaled digits and scale), or a double.
pub const Number = union(enum) {
    int: i128,
    dec: struct { unscaled: i128, scale: u8 },
    float: f64,

    pub fn parse(text: []const u8) ?Number {
        if (std.fmt.parseInt(i128, text, 10)) |v| return .{ .int = v } else |_| {}
        const dot = std.mem.indexOfScalar(u8, text, '.');
        const exp = std.mem.indexOfAny(u8, text, "eE");
        if (dot) |d| if (exp == null) {
            const frac = text[d + 1 ..];
            if (frac.len <= expr_ast.Dec.max_precision) {
                var digits: [80]u8 = undefined;
                if (text.len <= digits.len) {
                    @memcpy(digits[0..d], text[0..d]);
                    @memcpy(digits[d..][0..frac.len], frac);
                    if (std.fmt.parseInt(i128, digits[0 .. d + frac.len], 10)) |u| {
                        return .{ .dec = .{ .unscaled = u, .scale = @intCast(frac.len) } };
                    } else |_| {}
                }
            }
        };
        const f = std.fmt.parseFloat(f64, text) catch return null;
        return .{ .float = f };
    }

    fn asF64(self: Number) f64 {
        return switch (self) {
            .int => |v| @floatFromInt(v),
            .dec => |d| @as(f64, @floatFromInt(d.unscaled)) / std.math.pow(f64, 10, @floatFromInt(d.scale)),
            .float => |f| f,
        };
    }
};

/// How `v` orders against `lit`, or null when the comparison is UNKNOWN: `v` NULL, a NULL literal, or kinds that do
/// not compare (refused when the query binds, see `checkHaving`). Integers and DECIMALs compare exactly against
/// integer and decimal literals; anything with a double compares as doubles, NaN above every number as zpq orders
/// floats; an AVG compares its mean as a double, as SQL types it.
pub fn compareLiteral(v: scan.AggValue, lit: expr_ast.HavingLiteral) ?std.math.Order {
    if (lit == .null) return null;
    if (lit == .string) return switch (v) {
        .s => |s| std.mem.order(u8, s, lit.string),
        else => null,
    };
    const num = Number.parse(lit.number) orelse return null;
    return switch (v) {
        .null_val, .s => null,
        .i => |i| exactOrder(i, 0, num),
        .dec => |d| exactOrder(d.v, d.spec.scale, num),
        .f => |f| floatOrder(f, num.asF64()),
        .avg => |a| if (a.count == 0 or a.sum == null) null else floatOrder(a.sum.? / @as(f64, @floatFromInt(a.count)), num.asF64()),
        .avg_dec => |a| if (a.count == 0 or a.sum == null) null else floatOrder(
            @as(f64, @floatFromInt(a.sum.?)) / std.math.pow(f64, 10, @floatFromInt(a.spec.scale)) /
                @as(f64, @floatFromInt(a.count)),
            num.asF64(),
        ),
    };
}

/// `unscaled / 10^scale` against `num`, exactly unless `num` is a double.
fn exactOrder(unscaled: i128, scale: u8, num: Number) std.math.Order {
    const lit_unscaled: i128, const lit_scale: u8 = switch (num) {
        .int => |i| .{ i, 0 },
        .dec => |d| .{ d.unscaled, d.scale },
        .float => |f| return floatOrder(@as(f64, @floatFromInt(unscaled)) / std.math.pow(f64, 10, @floatFromInt(scale)), f),
    };
    const top = @max(scale, lit_scale);
    const a = @as(i256, unscaled) * @as(i256, expr_ast.pow10(top - scale));
    const b = @as(i256, lit_unscaled) * @as(i256, expr_ast.pow10(top - lit_scale));
    return std.math.order(a, b);
}

fn floatOrder(a: f64, b: f64) std.math.Order {
    if (std.math.isNan(a) or std.math.isNan(b)) {
        return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
    }
    return std.math.order(a, b);
}

/// What a result column holds, for checking HAVING literals against it before any row is read.
pub const Lane = enum { number, string };

/// Refuse a HAVING leaf whose literal cannot compare with its column: a string against a number or the reverse, or a
/// number literal that is none.
pub fn checkHaving(h: expr_ast.Having, lanes: []const Lane) error{HavingTypeMismatch}!void {
    switch (h) {
        .all, .any => |terms| for (terms) |t| try checkHaving(t, lanes),
        .null_check => {},
        .cmp => |c| switch (c.literal) {
            .null => {},
            .string => if (lanes[c.column] != .string) return error.HavingTypeMismatch,
            .number => |text| if (lanes[c.column] != .number or Number.parse(text) == null) return error.HavingTypeMismatch,
        },
    }
}

test "finish ranks by hidden columns, cuts once, then drops them" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    // Rows (k, hidden n): ranked by n DESC, ties by first appearance; k is a string the rows own.
    const ks = [_][]const u8{ "a", "b", "c", "d" };
    const ns = [_]i128{ 1, 5, 5, 3 };
    const rows = try gpa.alloc([]const scan.AggValue, ks.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, ks, ns) |*row, k, n| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .null_val;
        r[1] = .{ .i = n };
        row.* = r;
        const owned = try gpa.dupe(u8, k);
        r[0] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    cols[0] = try gpa.dupe(u8, "k");
    cols[1] = try gpa.dupe(u8, "");
    const types = [_]scan.GroupColType{ .{ .lane = .string, .nullable = true }, .{ .lane = .i64, .nullable = false } };
    var col_types: ?[]const scan.GroupColType = &types;

    const program: ResultProgram = .{
        .order = .{ .keys = &.{.{ .column = 1, .descending = true }}, .limit = 2, .offset = 1 },
        .visible = 1,
    };
    try program.finish(gpa, &out, &col_types);
    try std.testing.expectEqual(@as(usize, 1), out.group_cols.?.len);
    try std.testing.expectEqual(@as(usize, 1), col_types.?.len);
    const got = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    for (got, [_][]const u8{ "c", "d" }) |row, want| {
        try std.testing.expectEqual(@as(usize, 1), row.len);
        try std.testing.expectEqualStrings(want, row[0].s);
    }
}

test "finish leaves the answer whole when it runs out of memory" {
    const Probe = struct {
        fn run(gpa: std.mem.Allocator) !void {
            var out: scan.AggOutput = .{};
            defer out.deinit(gpa);
            const rows = try gpa.alloc([]const scan.AggValue, 3);
            @memset(rows, &.{});
            out.group_rows = rows;
            for (rows, 0..) |*row, i| {
                const r = try gpa.alloc(scan.AggValue, 2);
                r[0] = .{ .i = @intCast(i) };
                r[1] = .null_val;
                row.* = r;
                // Bound first: a union initialized with a `try` inside may take its tag before the payload fails.
                const owned = try gpa.dupe(u8, "hidden");
                r[1] = .{ .s = owned };
            }
            const cols = try gpa.alloc([]const u8, 2);
            @memset(cols, "");
            out.group_cols = cols;
            for (cols) |*c| c.* = try gpa.dupe(u8, "c");
            const program: ResultProgram = .{ .order = .{ .keys = &.{.{ .column = 0, .descending = true }} }, .visible = 1 };
            try program.finish(gpa, &out, null);
            try std.testing.expectEqual(@as(i128, 2), out.group_rows.?[0][0].i);
        }
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Probe.run, .{});
}

test "finish leaves every row as it was when an allocation fails part way" {
    const gpa = std.testing.allocator;
    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var out: scan.AggOutput = .{};
        defer out.deinit(gpa);
        const rows = try gpa.alloc([]const scan.AggValue, 4);
        @memset(rows, &.{});
        out.group_rows = rows;
        for (rows, 0..) |*row, i| {
            const r = try gpa.alloc(scan.AggValue, 2);
            r[0] = .{ .i = @intCast(i) };
            r[1] = .null_val;
            row.* = r;
            const owned = try gpa.dupe(u8, "hidden");
            r[1] = .{ .s = owned };
        }
        const cols = try gpa.alloc([]const u8, 2);
        @memset(cols, "");
        out.group_cols = cols;
        for (cols) |*c| c.* = try gpa.dupe(u8, "c");

        var failing: std.testing.FailingAllocator = .init(gpa, .{ .fail_index = fail_index });
        const program: ResultProgram = .{ .visible = 1 };
        program.finish(failing.allocator(), &out, null) catch |err| {
            try std.testing.expectEqual(error.OutOfMemory, err);
            // Untouched: every row still wide, the hidden strings still owned.
            try std.testing.expectEqual(@as(usize, 2), out.group_cols.?.len);
            for (out.group_rows.?, 0..) |row, i| {
                try std.testing.expectEqual(@as(usize, 2), row.len);
                try std.testing.expectEqual(@as(i128, @intCast(i)), row[0].i);
                try std.testing.expectEqualStrings("hidden", row[1].s);
            }
            continue;
        };
        for (out.group_rows.?) |row| try std.testing.expectEqual(@as(usize, 1), row.len);
        break;
    }
    try std.testing.expect(fail_index > 0);
}

test "HAVING literals compare exactly against integers and DECIMALs, as doubles otherwise" {
    const lit = struct {
        fn n(text: []const u8) expr_ast.HavingLiteral {
            return .{ .number = text };
        }
    }.n;
    const O = std.math.Order;
    const dec: scan.AggValue = .{ .dec = .{ .v = 12345, .spec = .{ .precision = 10, .scale = 2 } } }; // 123.45
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(dec, lit("123.45")));
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(dec, lit("123.4500")));
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(dec, lit("123")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(dec, lit("123.451")));
    // Past 2^53 a double cannot tell these apart; the exact comparison can.
    const big: scan.AggValue = .{ .i = (1 << 60) + 1 };
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(big, lit("1152921504606846976")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(.{ .i = 100000 }, lit("100000.5")));
    try std.testing.expectEqual(@as(?O, .gt), compareLiteral(.{ .f = std.math.nan(f64) }, lit("1e300")));
    try std.testing.expectEqual(@as(?O, .eq), compareLiteral(.{ .avg = .{ .sum = 7, .count = 2 } }, lit("3.5")));
    try std.testing.expectEqual(@as(?O, null), compareLiteral(.{ .avg = .{ .sum = null, .count = 0 } }, lit("0")));
    try std.testing.expectEqual(@as(?O, null), compareLiteral(.null_val, lit("0")));
    try std.testing.expectEqual(@as(?O, .lt), compareLiteral(.{ .s = "abc" }, .{ .string = "abd" }));
    try std.testing.expectError(error.HavingTypeMismatch, checkHaving(.{ .cmp = .{ .column = 0, .op = .eq, .literal = .{ .string = "x" } } }, &.{.number}));
}

test "HAVING keeps only TRUE groups before the cut; without GROUP BY it may leave no row" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    // Rows (k, n): n NULL in one row, which neither `n > 2` nor its negation keeps.
    const ns = [_]?i128{ 1, 5, null, 3, 4 };
    const rows = try gpa.alloc([]const scan.AggValue, ns.len);
    @memset(rows, &.{});
    out.group_rows = rows;
    for (rows, ns, 0..) |*row, n, i| {
        const r = try gpa.alloc(scan.AggValue, 2);
        r[0] = .{ .i = @intCast(i) };
        r[1] = if (n) |v| .{ .i = v } else .null_val;
        row.* = r;
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    const gt2: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "2" } } };
    const program: ResultProgram = .{ .order = .{
        .keys = &.{.{ .column = 1, .descending = false }},
        .limit = 2,
        .having = &gt2,
    } };
    // A cut before HAVING would keep 1 and 3; the masked order keeps nothing from the strategies.
    try std.testing.expectEqual(@as(?u64, null), ResultProgram.cutOrder(program.order).limit);
    try program.finish(gpa, &out, null);
    const got = out.group_rows.?;
    try std.testing.expectEqual(@as(usize, 2), got.len);
    try std.testing.expectEqual(@as(i128, 3), got[0][1].i);
    try std.testing.expectEqual(@as(i128, 4), got[1][1].i);

    var scalar: scan.AggOutput = .{};
    defer scalar.deinit(gpa);
    scalar.aggs = try gpa.alloc(scan.AggOutputItem, 2);
    scalar.aggs[0] = .{ .alias = try gpa.dupe(u8, "c"), .value = .{ .i = 0 } };
    scalar.aggs[1] = .{ .alias = try gpa.dupe(u8, "hidden"), .value = .{ .i = 0 } };
    const pos: expr_ast.Having = .{ .cmp = .{ .column = 1, .op = .gt, .literal = .{ .number = "0" } } };
    const global: ResultProgram = .{ .order = .{ .having = &pos }, .visible = 1 };
    try global.finish(gpa, &scalar, null);
    try std.testing.expect(scalar.no_row);
    try std.testing.expectEqual(@as(usize, 1), scalar.aggs.len);
}

test "slab rows: a cut keeps a few, compaction releases the rest, strings stay owned" {
    const gpa = std.testing.allocator;
    var out: scan.AggOutput = .{};
    defer out.deinit(gpa);
    const n_rows = 10_000;
    const rows = try gpa.alloc([]const scan.AggValue, n_rows);
    @memset(rows, &.{});
    out.group_rows = rows;
    const slab = try gpa.alloc(scan.AggValue, n_rows * 2);
    @memset(slab, .null_val);
    out.slab = slab;
    for (rows, 0..) |*row, i| {
        const cells = slab[i * 2 ..][0..2];
        cells[0] = .{ .i = @intCast(i) };
        row.* = cells;
        const owned = try std.fmt.allocPrint(gpa, "k{d}", .{i});
        cells[1] = .{ .s = owned };
    }
    const cols = try gpa.alloc([]const u8, 2);
    @memset(cols, "");
    out.group_cols = cols;
    for (cols) |*c| c.* = try gpa.dupe(u8, "c");
    const program: ResultProgram = .{ .order = .{ .keys = &.{.{ .column = 0, .descending = true }}, .limit = 3 } };
    try program.finish(gpa, &out, null);
    try std.testing.expectEqual(@as(usize, 3), out.group_rows.?.len);
    // Compacted: the slab holds just the kept rows.
    try std.testing.expectEqual(@as(usize, 6), out.slab.len);
    for (out.group_rows.?, 0..) |row, j| {
        try std.testing.expectEqual(@as(i128, n_rows - 1 - @as(i128, @intCast(j))), row[0].i);
        try std.testing.expect(std.mem.startsWith(u8, row[1].s, "k"));
    }
}

test "slab rows: compaction that runs out of memory leaves the rows as they were and leaks nothing" {
    const gpa = std.testing.allocator;
    for (0..2) |fail_at| {
        var out: scan.AggOutput = .{};
        defer out.deinit(gpa);
        const slab = try gpa.alloc(scan.AggValue, 8192);
        @memset(slab, .null_val);
        out.slab = slab;
        slab[100] = .{ .i = 7 };
        const rows = try gpa.alloc([]const scan.AggValue, 1);
        rows[0] = slab[100..101];
        out.group_rows = rows;
        // The slab far outsizes its one live cell, so compaction runs and fails at its first or second allocation.
        var failing = std.testing.FailingAllocator.init(gpa, .{ .fail_index = fail_at });
        try std.testing.expectError(error.OutOfMemory, out.compactRows(failing.allocator()));
        try std.testing.expectEqual(slab.ptr, out.slab.ptr);
        try std.testing.expectEqual(@as(i128, 7), out.group_rows.?[0][0].i);
    }
}
