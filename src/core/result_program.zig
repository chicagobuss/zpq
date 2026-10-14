//! The one stage every aggregate answer passes through after its groups are complete, whatever strategy produced
//! them (scalar, plain or partitioned hash, sketch, census, first-keys, TPUT or ResultCut): the engine runs it on the
//! rows `scan.runMultiAggregate` returns. In SQL order: rank the rows by ORDER BY, cut them to OFFSET/LIMIT once, then
//! drop the hidden columns ranking read (`ResultOrder.hidden`), so no output format ever sees them.
//!
//! The strategies before it may cut rows early only where the cut provably keeps every row this stage would keep;
//! the answer is this stage's either way.

const std = @import("std");
const scan = @import("scan.zig");
const expr_ast = @import("expr/ast.zig");

pub const ResultProgram = struct {
    order: expr_ast.ResultOrder = .{},
    /// How many leading columns of each GROUP BY row are shown (`MultiAggResult.visible_cols`); null shows all.
    visible: ?usize = null,

    /// Finish `out` in place. `col_types`, one per row column, is cut to the visible columns alongside; it is borrowed
    /// from the scan's arena, so only the slice changes. On failure `out` is whole (unranked or ranked), never torn.
    pub fn finish(
        self: ResultProgram,
        gpa: std.mem.Allocator,
        out: *scan.AggOutput,
        col_types: ?*?[]const scan.GroupColType,
    ) error{OutOfMemory}!void {
        try out.applyResultOrder(gpa, self.order);
        const n = self.visible orelse return;
        const rows = out.group_rows orelse return;
        const cols = out.group_cols.?;
        if (n >= cols.len) return;
        // The narrowed rows are built before anything is released, so a failure leaves `out` as it was. They take
        // over the visible values (and the strings those own) from the old rows.
        const narrowed = try gpa.alloc([]const scan.AggValue, rows.len);
        var built: usize = 0;
        errdefer {
            for (narrowed[0..built]) |row| gpa.free(row);
            gpa.free(narrowed);
        }
        for (rows, narrowed) |row, *dst| {
            dst.* = try gpa.dupe(scan.AggValue, row[0..n]);
            built += 1;
        }
        const names = try gpa.dupe([]const u8, cols[0..n]);
        for (rows) |row| {
            for (row[n..]) |v| freeValue(gpa, v);
            gpa.free(row);
        }
        gpa.free(rows);
        for (cols[n..]) |c| gpa.free(c);
        gpa.free(cols);
        out.group_rows = narrowed;
        out.group_cols = names;
        if (col_types) |ct| if (ct.*) |types| {
            ct.* = types[0..n];
        };
    }

    fn freeValue(gpa: std.mem.Allocator, v: scan.AggValue) void {
        switch (v) {
            .s => |s| gpa.free(s),
            else => {},
        }
    }
};

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
