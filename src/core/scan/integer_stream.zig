const std = @import("std");
const testing = std.testing;
const owner_groups = @import("../owner_groups.zig");
const spawn_util = @import("../spawn.zig");
const scan_types = @import("types.zig");
const BoundOutputColumn = scan_types.BoundOutputColumn;
const GroupColType = scan_types.GroupColType;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const row_block = scan_types.row_block;
const TypedAnswer = @import("typed_answer.zig").TypedAnswer;

pub const IntegerStream = struct {
    state: *owner_groups.IntFull.State,
    outputs: []const BoundOutputColumn,
    count_call: usize,

    /// A bounded piece of the already validated answer. Reuses direct typed narrowing and column ownership.
    pub fn window(
        s: IntegerStream,
        a: std.mem.Allocator,
        types: []const GroupColType,
        lo: usize,
        hi: usize,
        workers: usize,
    ) !TypedAnswer {
        var answer: TypedAnswer = .{ .rows = 0, .columns = &.{} };
        errdefer answer.deinit(a);
        answer.rows = hi - lo;
        answer.columns = try a.alloc(TypedAnswer.TypedColumn, 3);
        @memset(answer.columns, .{ .i64 = .{ .values = &.{}, .defs = &.{} } });
        for (s.outputs, answer.columns) |out, *col| {
            const needs_levels = switch (out.source) {
                .key => s.state.input_nulls & 1 != 0,
                .agg => |ai| ai != s.count_call and s.state.input_nulls & 2 != 0,
            };
            col.* = .{
                .i64 = if (needs_levels)
                    try TypedAnswer.lane(a, i64, hi - lo)
                else
                    .{ .values = try a.alloc(i64, hi - lo), .defs = &.{} },
            };
        }
        const ids = try s.state.window(a, lo, hi);
        defer a.free(ids);
        var key_at: usize = 0;
        var count_at: usize = 0;
        var sum_at: usize = 0;
        for (s.outputs, 0..) |out, i| switch (out.source) {
            .key => key_at = i,
            .agg => |ai| {
                if (ai == s.count_call) count_at = i else sum_at = i;
            },
        };
        const Fill = struct {
            state: *const owner_groups.IntFull.State,
            ids: []const u32,
            key: *TypedAnswer.Lane(i64),
            count: *TypedAnswer.Lane(i64),
            sum: *TypedAnswer.Lane(i64),
            cursor: *std.atomic.Value(usize),
            err: ?anyerror = null,
            fn run(f: *@This()) void {
                const key_values = f.key.values;
                const key_defs = f.key.defs;
                const count_values = f.count.values;
                const sum_values = f.sum.values;
                const sum_defs = f.sum.defs;
                while (true) {
                    const start = f.cursor.fetchAdd(row_block, .monotonic);
                    if (start >= f.ids.len) return;
                    for (start..@min(f.ids.len, start + row_block)) |i| {
                        const g = &f.state.groups[f.ids[i]];
                        if (key_defs.len != 0) {
                            const key_value = g.keyValue();
                            key_values[i] = if (key_value) |x| @intCast(x) else 0;
                            key_defs[i] = @intFromBool(key_value != null);
                        } else key_values[i] = g.keyValue().?;
                        count_values[i] = @intCast(g.countValue());
                        if (sum_defs.len != 0) {
                            sum_values[i] = 0;
                            sum_defs[i] = 0;
                            if (g.sumValue()) |v| TypedAnswer.putIntegerRaw(f.sum, false, i, v) catch |e| {
                                f.err = e;
                                return;
                            };
                        } else {
                            // No input SUM value was NULL; every group is present and finish checked every i64 bound.
                            sum_values[i] = @intCast(g.sum);
                        }
                    }
                }
            }
        };
        const threads = @max(1, @min(workers, (ids.len + row_block - 1) / row_block));
        const tasks = try a.alloc(Fill, threads);
        defer a.free(tasks);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*task| task.* = .{
            .state = s.state,
            .ids = ids,
            .key = &answer.columns[key_at].i64,
            .count = &answer.columns[count_at].i64,
            .sum = &answer.columns[sum_at].i64,
            .cursor = &cursor,
        };
        if (tasks.len == 1 or !spawn_util.runTree(Fill, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Fill.run))
            Fill.run(&tasks[0]);
        for (tasks) |task| if (task.err) |e| return e;
        try answer.seal(types);
        return answer;
    }
};

fn integerWindowAllocTrial(a: std.mem.Allocator, stream: IntegerStream, types: []const GroupColType) !void {
    var answer = try stream.window(a, types, 0, stream.state.group_count, 1);
    defer answer.deinit(a);
    try testing.expectEqualSlices(i64, &.{ 0, 1, 2 }, answer.columns[0].i64.values);
    try testing.expectEqualSlices(i64, &.{ 2, 2, 1 }, answer.columns[1].i64.values);
    try testing.expectEqualSlices(i64, &.{ 3, 5, 0 }, answer.columns[2].i64.values);
    try testing.expectEqual(@as(usize, 0), answer.columns[1].i64.defs.len);
    if (stream.state.input_nulls == 0) {
        for (answer.columns) |col| try testing.expectEqual(@as(usize, 0), col.i64.defs.len);
    } else {
        try testing.expectEqualSlices(u32, &.{ 0, 1, 1 }, answer.columns[0].i64.defs);
        try testing.expectEqualSlices(u32, &.{ 1, 1, 0 }, answer.columns[2].i64.defs);
    }
}

test "inline integer stream windows free every partial typed allocation" {
    for ([_]bool{ false, true }) |nulls| {
        const state = (try owner_groups.IntFull.State.init(testing.allocator, null, 5, 1, 0, 1, {}, owner_groups.full_parts, 0)).?;
        defer state.deinit();
        const k = [_]i64{ 0, 1, 2, 1, 0 };
        const v = [_]i64{ 0, 7, 0, -2, 3 };
        const kd = [_]u32{ 0, 1, 1, 1, 0 };
        const vd = [_]u32{ 0, 1, 0, 1, 1 };
        try state.locals[0].batch(.{ .num_rows = 5, .cols = &.{
            .{ .i64 = .{
                .values = &k,
                .def_levels = if (nulls) &kd else null,
                .max_def = @intFromBool(nulls),
                .has_nulls = nulls,
            } },
            .{ .i64 = .{
                .values = &v,
                .def_levels = if (nulls) &vd else null,
                .max_def = @intFromBool(nulls),
                .has_nulls = nulls,
            } },
        } }, &.{ 0, 1 }, 0);
        try state.finish();
        const outputs = [_]BoundOutputColumn{
            .{ .source = .{ .key = 0 }, .name = "k" },
            .{ .source = .{ .agg = 0 }, .name = "c" },
            .{ .source = .{ .agg = 1 }, .name = "s" },
        };
        const types = [_]GroupColType{
            .{ .lane = .i64, .nullable = true },
            .{ .lane = .i64, .nullable = false },
            .{ .lane = .i64, .nullable = true },
        };
        const stream: IntegerStream = .{ .state = state, .outputs = &outputs, .count_call = 0 };
        try testing.checkAllAllocationFailures(testing.allocator, integerWindowAllocTrial, .{ stream, &types });
    }
}

