const std = @import("std");
const expr_agg = @import("../expr/agg.zig");
const owner_groups = @import("../owner_groups.zig");
const spawn_util = @import("../spawn.zig");
const scan_types = @import("types.zig");
const BoundOutputColumn = scan_types.BoundOutputColumn;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const row_block = scan_types.row_block;

pub const ByteWindow = struct {
    columns: []expr_agg.OutputCol = &.{},
    key_values: [][]const u8 = &.{},
    key_bytes: []u8 = &.{},
    key_defs: []u32 = &.{},
    counts: []i64 = &.{},
    sums: []i64 = &.{},
    sum_defs: []u32 = &.{},

    pub fn deinit(w: *ByteWindow, a: std.mem.Allocator) void {
        a.free(w.columns);
        a.free(w.key_values);
        a.free(w.key_bytes);
        a.free(w.key_defs);
        a.free(w.counts);
        a.free(w.sums);
        a.free(w.sum_defs);
        w.* = .{};
    }
};

pub const ByteStream = struct {
    state: *owner_groups.Full.State,
    outputs: []const BoundOutputColumn,
    count_call: usize,

    pub fn window(s: ByteStream, a: std.mem.Allocator, lo: usize, hi: usize, workers: usize) !ByteWindow {
        var w: ByteWindow = .{};
        errdefer w.deinit(a);
        const n = hi - lo;
        w.columns = try a.alloc(expr_agg.OutputCol, 3);
        w.key_values = try a.alloc([]const u8, n);
        w.key_bytes = try a.alloc(u8, n * 16);
        w.counts = try a.alloc(i64, n);
        w.sums = try a.alloc(i64, n);
        if (s.state.input_nulls & 1 != 0) w.key_defs = try a.alloc(u32, n);
        if (s.state.input_nulls & 2 != 0) w.sum_defs = try a.alloc(u32, n);
        const ids = try s.state.window(a, lo, hi);
        defer a.free(ids);
        const Fill = struct {
            state: *const owner_groups.Full.State,
            ids: []const u32,
            window: *ByteWindow,
            cursor: *std.atomic.Value(usize),
            key_nulls: bool = false,
            sum_nulls: bool = false,
            fn run(f: *@This()) void {
                const win = f.window;
                var kn = false;
                var sn = false;
                while (true) {
                    const start = f.cursor.fetchAdd(row_block, .monotonic);
                    if (start >= f.ids.len) break;
                    for (start..@min(f.ids.len, start + row_block)) |i| {
                        const g = &f.state.groups[f.ids[i]];
                        const bytes = g.keyBytes();
                        const slot = win.key_bytes[i * 16 ..][0..16];
                        slot.* = std.mem.toBytes(g.key);
                        win.key_values[i] = slot[0..if (bytes) |b| b.len else 0];
                        if (win.key_defs.len != 0) {
                            win.key_defs[i] = @intFromBool(bytes != null);
                            kn = kn or bytes == null;
                        }
                        win.counts[i] = g.countValue();
                        if (win.sum_defs.len != 0) {
                            const v = g.sumValue();
                            win.sums[i] = if (v) |x| @intCast(x) else 0;
                            win.sum_defs[i] = @intFromBool(v != null);
                            sn = sn or v == null;
                        } else win.sums[i] = @intCast(g.sum);
                    }
                }
                f.key_nulls = kn;
                f.sum_nulls = sn;
            }
        };
        const threads = @max(1, @min(workers, (n + row_block - 1) / row_block));
        const tasks = try a.alloc(Fill, threads);
        defer a.free(tasks);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*t| t.* = .{ .state = s.state, .ids = ids, .window = &w, .cursor = &cursor };
        if (threads == 1 or !spawn_util.runTree(Fill, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Fill.run))
            Fill.run(&tasks[0]);
        var key_nulls = false;
        var sum_nulls = false;
        for (tasks) |t| {
            key_nulls = key_nulls or t.key_nulls;
            sum_nulls = sum_nulls or t.sum_nulls;
        }
        for (s.outputs, w.columns) |out, *col| col.* = switch (out.source) {
            .key => .{
                .name = out.name,
                .col = .{ .string = .{
                    .values = w.key_values,
                    .def_levels = if (key_nulls) w.key_defs else null,
                    .max_def = 1,
                    .has_nulls = key_nulls,
                } },
                .parquet_type = .BYTE_ARRAY,
                .nullable = true,
                .distinct = true,
            },
            .agg => |ai| .{
                .name = out.name,
                .col = .{ .i64 = .{
                    .values = if (ai == s.count_call) w.counts else w.sums,
                    .def_levels = if (ai != s.count_call and sum_nulls) w.sum_defs else null,
                    .max_def = @intFromBool(ai != s.count_call),
                    .has_nulls = ai != s.count_call and sum_nulls,
                } },
                .parquet_type = .INT64,
                .nullable = ai != s.count_call,
            },
        };
        return w;
    }
};

