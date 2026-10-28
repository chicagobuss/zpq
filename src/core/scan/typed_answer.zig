const std = @import("std");
const testing = std.testing;
const schema = @import("../schema.zig");
const expr_agg = @import("../expr/agg.zig");
const spawn_util = @import("../spawn.zig");
const scan_types = @import("types.zig");
const BorrowedSpan = scan_types.BorrowedSpan;
const GroupColType = scan_types.GroupColType;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const AggValue = @import("output_values.zig").AggValue;

/// A GROUP BY answer as typed columns, one per output column in `group_cols` order, built straight from the merged
/// groups for a Parquet write, so no row of `AggValue`s (112 bytes a value) is ever held. Values are made by the same
/// functions the rows are (`RowSource`) and narrowed by the rules the row writer applies, so the file and the error a
/// query stops with are the same either way. String slices may borrow the arenas retained by AggOutput.
pub const TypedAnswer = struct {
    rows: usize,
    columns: []TypedColumn,
    borrowed_strings: []const BorrowedSpan = &.{},

    pub fn Lane(comptime T: type) type {
        return struct {
            values: []T,
            /// 1 present, 0 NULL.
            defs: []u32,
            nulls: bool = false,
        };
    }

    pub const TypedColumn = union(enum) {
        i64: Lane(i64),
        f64: Lane(f64),
        string: Lane([]const u8),
        dec: Lane(i128),
        /// avg_state's re-aggregatable pair, written as `<name>__sum` and `<name>__count`.
        avg: struct { sum: Lane(f64), count: []i64 },
        avg_dec: struct { sum: Lane(i128), count: []i64 },
    };

    pub fn deinit(self: *TypedAnswer, gpa: std.mem.Allocator) void {
        for (self.columns) |*col| switch (col.*) {
            .i64 => |l| freeLane(gpa, i64, l),
            .f64 => |l| freeLane(gpa, f64, l),
            .dec => |l| freeLane(gpa, i128, l),
            .string => |l| {
                for (l.values, l.defs) |v, d| if (d == 1 and
                    !BorrowedSpan.contains(self.borrowed_strings, v)) gpa.free(v);
                freeLane(gpa, []const u8, l);
            },
            .avg => |a| {
                freeLane(gpa, f64, a.sum);
                gpa.free(a.count);
            },
            .avg_dec => |a| {
                freeLane(gpa, i128, a.sum);
                gpa.free(a.count);
            },
        };
        gpa.free(self.columns);
        self.* = .{ .rows = 0, .columns = &.{} };
    }

    fn freeLane(gpa: std.mem.Allocator, comptime T: type, l: Lane(T)) void {
        gpa.free(l.values);
        gpa.free(l.defs);
    }

    /// Cleared by `clear`, but a string lane's defs at once: its `deinit` reads them, should a later lane fail.
    pub fn lane(gpa: std.mem.Allocator, comptime T: type, n: usize) !Lane(T) {
        const values = try gpa.alloc(T, n);
        errdefer gpa.free(values);
        const defs = try gpa.alloc(u32, n);
        if (T == []const u8) @memset(defs, 0);
        return .{ .values = values, .defs = defs };
    }

    fn clearLane(comptime T: type, l: Lane(T), lo: usize, hi: usize) void {
        @memset(l.values[lo..hi], std.mem.zeroes(T));
        @memset(l.defs[lo..hi], 0);
    }

    /// Rows `lo..hi` of every column NULL, values zero.
    fn clear(self: *const TypedAnswer, lo: usize, hi: usize) void {
        for (self.columns) |col| switch (col) {
            .i64 => |l| clearLane(i64, l, lo, hi),
            .f64 => |l| clearLane(f64, l, lo, hi),
            .string => |l| clearLane([]const u8, l, lo, hi),
            .dec => |l| clearLane(i128, l, lo, hi),
            .avg => |a| {
                clearLane(f64, a.sum, lo, hi);
                @memset(a.count[lo..hi], 0);
            },
            .avg_dec => |a| {
                clearLane(i128, a.sum, lo, hi);
                @memset(a.count[lo..hi], 0);
            },
        };
    }

    /// Rows a `clear` task claims at a time.
    const clear_block = 1 << 16;

    /// `clear` of every row, on up to `n_workers` threads: an answer of millions of rows is cleared, and its pages
    /// first touched, on every core rather than one. Cannot fail: rows a thread that never started left are cleared by
    /// one that did, or here.
    fn clearAll(self: *const TypedAnswer, n_workers: usize) void {
        const Task = struct {
            answer: *const TypedAnswer,
            cursor: *std.atomic.Value(usize),

            fn run(t: *@This()) void {
                while (true) {
                    const lo = t.cursor.fetchAdd(clear_block, .monotonic);
                    if (lo >= t.answer.rows) return;
                    t.answer.clear(lo, @min(t.answer.rows, lo + clear_block));
                }
            }
        };
        var tasks: [32]Task = undefined;
        const n_threads = @min(tasks.len, n_workers, self.rows / clear_block);
        if (n_threads < 2) return self.clear(0, self.rows);
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks[0..n_threads]) |*t| t.* = .{ .answer = self, .cursor = &cursor };
        const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
        if (!spawn_util.runTree(Task, tasks[0..n_threads], config, Task.run)) Task.run(&tasks[0]);
    }

    /// Columns typed by `types`, every cell NULL, owned by the caller's answer from the first allocation on. Cleared on
    /// up to `n_workers` threads.
    pub fn init(gpa: std.mem.Allocator, types: []const GroupColType, n: usize, out: *TypedAnswer, n_workers: usize) !void {
        out.* = .{ .rows = n, .columns = try gpa.alloc(TypedColumn, types.len) };
        // Each column is valid (empty) until allocated, so a failure leaves `deinit` something to free.
        for (out.columns) |*c| c.* = .{ .i64 = .{ .values = &.{}, .defs = &.{} } };
        for (types, out.columns) |ty, *col| {
            // Do not publish the union tag until its fallible payload is complete: the caller may deinit on error.
            const ready: TypedColumn = switch (ty.lane) {
                .i64 => .{ .i64 = try lane(gpa, i64, n) },
                .f64 => .{ .f64 = try lane(gpa, f64, n) },
                .string => .{ .string = try lane(gpa, []const u8, n) },
                .dec => .{ .dec = try lane(gpa, i128, n) },
                .avg => blk: {
                    const sum = try lane(gpa, f64, n);
                    const count = gpa.alloc(i64, n) catch |e| {
                        freeLane(gpa, f64, sum);
                        return e;
                    };
                    break :blk .{ .avg = .{ .sum = sum, .count = count } };
                },
                .avg_dec => blk: {
                    const sum = try lane(gpa, i128, n);
                    const count = gpa.alloc(i64, n) catch |e| {
                        freeLane(gpa, i128, sum);
                        return e;
                    };
                    break :blk .{ .avg_dec = .{ .sum = sum, .count = count } };
                },
            };
            col.* = ready;
        }
        out.clearAll(n_workers);
    }

    /// Store value `v` of row `i` in column `col`, narrowed as the row writer narrows it. A string's ownership passes
    /// to the column at once, so an error after leaves it freed with the rest. The error is the row writer's.
    pub fn put(col: *TypedColumn, ty: GroupColType, i: usize, v: AggValue) !void {
        if (v == .null_val) return;
        switch (col.*) {
            .i64 => |*l| {
                const x = switch (v) {
                    .i => |x| x,
                    else => return error.TypeMismatch,
                };
                try putInteger(l, ty, i, x);
            },
            .f64 => |*l| {
                l.values[i] = switch (v) {
                    .f => |x| x,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .string => |*l| {
                l.values[i] = switch (v) {
                    .s => |x| x,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .dec => |*l| {
                l.values[i] = switch (v) {
                    .dec => |x| x.v,
                    else => return error.TypeMismatch,
                };
                l.defs[i] = 1;
            },
            .avg => |*a| switch (v) {
                .avg => |p| {
                    a.count[i] = p.count;
                    if (p.sum) |s| {
                        a.sum.values[i] = s;
                        a.sum.defs[i] = 1;
                    }
                },
                else => return error.TypeMismatch,
            },
            .avg_dec => |*a| switch (v) {
                .avg_dec => |p| {
                    a.count[i] = p.count;
                    // The `__sum` column is DECIMAL(38, s): a wider exact sum cannot be stored, and is not rounded.
                    if (p.sum) |x| {
                        if (@abs(x) >= @as(u192, @intCast(ty.dec.?.sum().bound()))) return error.DecimalOverflow;
                        a.sum.values[i] = @intCast(x);
                        a.sum.defs[i] = 1;
                    }
                },
                else => return error.TypeMismatch,
            },
        }
    }

    pub inline fn putInteger(l: *Lane(i64), ty: GroupColType, i: usize, x: i128) !void {
        const raw_u64 = if (ty.source) |src| schema.isUnsignedInt64(src) else false;
        return putIntegerRaw(l, raw_u64, i, x);
    }

    pub inline fn putIntegerRaw(l: *Lane(i64), raw_u64: bool, i: usize, x: i128) !void {
        l.values[i] = if (raw_u64 and x >= 0 and x <= std.math.maxInt(u64))
            expr_agg.u64Lane(x)
        else
            std.math.cast(i64, x) orelse return error.AggIntTooWide;
        l.defs[i] = 1;
    }

    /// Flag the columns that hold a NULL, and refuse one its type says cannot: the row writer's check.
    pub fn seal(self: *TypedAnswer, types: []const GroupColType) !void {
        for (self.columns, types) |*col, ty| switch (col.*) {
            inline .i64, .f64, .string, .dec => |*l| {
                l.nulls = std.mem.indexOfScalar(u32, l.defs, 0) != null;
                if (l.nulls and !ty.nullable) return error.UnexpectedNull;
            },
            inline .avg, .avg_dec => |*a| a.sum.nulls = std.mem.indexOfScalar(u32, a.sum.defs, 0) != null,
        };
    }
};

test "TypedAnswer.init: every cell NULL and zero at every -j, and when spawns fail" {
    const lanes = [_]@FieldType(GroupColType, "lane"){ .i64, .f64, .string, .dec, .avg, .avg_dec };
    var types: [lanes.len]GroupColType = undefined;
    for (&types, lanes) |*t, l| t.* = .{
        .nullable = true,
        .lane = l,
        .dec = .{ .precision = 30, .scale = 2 },
        .source = null,
    };
    const n = 3 * TypedAnswer.clear_block + 5;
    const Run = struct { j: usize, fail_after: ?usize = null };
    for ([_]Run{ .{ .j = 1 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 } }) |run| {
        if (run.fail_after) |k| spawn_util.injectFailureAfter(k);
        defer spawn_util.resetFailure();
        var out: TypedAnswer = undefined;
        try TypedAnswer.init(testing.allocator, &types, n, &out, run.j);
        defer out.deinit(testing.allocator);
        try testing.expectEqual(@as(usize, n), out.rows);
        for (out.columns) |col| switch (col) {
            inline .i64, .f64, .string, .dec => |l, tag| {
                for (l.defs) |d| try testing.expectEqual(@as(u32, 0), d);
                const T = @TypeOf(l.values[0]);
                for (l.values) |v| switch (tag) {
                    .string => try testing.expectEqual(@as(usize, 0), v.len),
                    else => try testing.expectEqual(std.mem.zeroes(T), v),
                };
            },
            inline .avg, .avg_dec => |a| {
                for (a.sum.defs) |d| try testing.expectEqual(@as(u32, 0), d);
                for (a.sum.values) |v| try testing.expectEqual(std.mem.zeroes(@TypeOf(v)), v);
                for (a.count) |c| try testing.expectEqual(@as(i64, 0), c);
            },
        };
    }
}

test "TypedAnswer.init preserves empty column on lane allocation failure" {
    const lanes = [_]@FieldType(GroupColType, "lane"){ .i64, .f64, .string, .dec, .avg, .avg_dec };
    for (lanes) |lane_kind| {
        const types = [_]GroupColType{
            .{ .lane = .i64, .nullable = true },
            .{ .lane = lane_kind, .nullable = true },
        };
        // The array and the first column consume three allocations. Fail each allocation of the second lane,
        // including the count allocation for AVG, while the caller owns the partially initialized answer.
        const lane_allocations: usize = if (lane_kind == .avg or lane_kind == .avg_dec) 3 else 2;
        for (3..3 + lane_allocations) |fail_index| {
            var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });
            const a = failing.allocator();
            var out: TypedAnswer = .{ .rows = 0, .columns = &.{} };
            defer out.deinit(a);
            try testing.expectError(error.OutOfMemory, TypedAnswer.init(a, &types, 3, &out, 1));
            try testing.expect(failing.has_induced_failure);
            try testing.expectEqual(@as(usize, 2), out.columns.len);
            try testing.expect(out.columns[0] == .i64);
            try testing.expect(out.columns[1] == .i64);
            try testing.expectEqual(@as(usize, 0), out.columns[1].i64.values.len);
            try testing.expectEqual(@as(usize, 0), out.columns[1].i64.defs.len);
        }
    }
}

