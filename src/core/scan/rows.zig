const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const agg_plan = @import("../agg_plan.zig");
const agg_dense = @import("../expr/agg_dense.zig");
const group_dependencies = @import("../expr/group_dependencies.zig");
const key_pack = @import("../expr/key_pack.zig");
const owner_groups = @import("../owner_groups.zig");
const spawn_util = @import("../spawn.zig");
const scan_types = @import("types.zig");
const BorrowedSpan = scan_types.BorrowedSpan;
const BoundOutputColumn = scan_types.BoundOutputColumn;
const GroupColType = scan_types.GroupColType;
const WORKER_STACK_SIZE = scan_types.WORKER_STACK_SIZE;
const row_block = scan_types.row_block;
const AggValue = @import("output_values.zig").AggValue;
const AggOutput = @import("output.zig").AggOutput;
const TypedAnswer = @import("typed_answer.zig").TypedAnswer;
const MergedSlot = @import("count_cut.zig").MergedSlot;
const key_decode = @import("key_decode.zig");
const KeyType = key_decode.KeyType;
const deserializeKeyColumn = key_decode.deserializeKeyColumn;
const deserializeKeyColumnMode = key_decode.deserializeKeyColumnMode;
const materializeOne = @import("materialize.zig").materializeOne;
const foldedAcc = @import("folded_acc.zig").foldedAcc;
const freeOwnedAccumulatorStrings = @import("owned_merge.zig").freeOwnedAccumulatorStrings;

/// Final groups stay typed through the common row/column writer. Dispatch occurs at output cells, never at records.
const OwnerRows = struct {
    ctx: *const anyopaque,
    key: *const fn (*const anyopaque, usize) u128,
    integer: *const fn (*const anyopaque, usize, usize) anyerror!?i128,
    value: *const fn (*const anyopaque, std.mem.Allocator, usize, usize) anyerror!AggValue,
};

/// What the GROUP BY answer's rows are built from: the merged groups and the output columns they fill.
pub const RowSource = struct {
    slots: []const MergedSlot,
    outputs: []const BoundOutputColumn,
    calls: []const expr_agg.AggCall,
    key_types: []const KeyType,
    key_exprs: []const expr_ast.Expr = &.{},
    /// The merged accumulators, a group's starting at `slot.idx`, when worker tables were folded; else the one
    /// table's, starting at `slot.idx * calls.len`.
    accs: []const expr_agg.Accumulator,
    folded: bool,
    /// Non-empty when the merge left each group's state in these tables (`mergeInPlace`): `slot.idx` names it, and
    /// `accs` is unused.
    in_place: []const expr_agg.GroupTable = &.{},
    single: ?*const expr_agg.GroupTable = null,
    /// The keys are fixed-width images, not the serialized framing `deserializeKeyColumn` reads.
    fixed_keys: bool,
    dense: ?*const agg_dense.Lanes = null,
    owner: ?OwnerRows = null,
    identity: ?*const key_pack.Plan = null,
    borrowed_strings: []const BorrowedSpan = &.{},

    /// Each emitted string aggregate owns a fresh copy beside the still-live source winner.
    pub fn winnerStringBytes(s: *const RowSource) usize {
        var bytes: usize = 0;
        if (s.dense != null or s.owner != null) return 0;
        var strings = false;
        for (s.outputs) |out| if (out.source == .agg and s.calls[out.source.agg].result == .bytes) {
            strings = true;
        };
        if (!strings) return 0;
        for (s.slots) |slot| {
            const row = s.integerRow(slot);
            for (s.outputs) |out| if (out.source == .agg and s.calls[out.source.agg].result == .bytes) {
                const acc = row.accs[out.source.agg];
                const value = switch (acc) {
                    .min_bytes, .max_bytes => |v| v,
                    .min_arena, .max_arena => if (s.borrowed_strings.len == 0) acc.stringValue() else null,
                    else => null,
                };
                if (value) |v| bytes +|= expr_agg.winnerBytes(v.len);
            };
        }
        return bytes;
    }

    pub fn denseStringBytes(s: *const RowSource) usize {
        const domain = if (s.dense) |d| d.domain else if (s.owner != null)
            (s.identity orelse return 0).domain orelse return 0
        else
            return 0;
        var per_row: usize = 0;
        for (s.outputs) |out| if (out.source == .key) {
            switch (domain.axes[out.source.key].values) {
                .bytes => |values| {
                    var widest: usize = 0;
                    for (values) |v| widest = @max(widest, v.len);
                    per_row +|= std.mem.alignForward(usize, widest, 16) +| 16;
                },
                else => {},
            }
        };
        return s.slots.len *| per_row;
    }

    fn outputKey(s: *const RowSource, gpa: std.mem.Allocator, slot: MergedSlot, out: BoundOutputColumn) !AggValue {
        if (s.dense != null or s.owner != null) {
            const identity = s.identity orelse return error.BadGroupKey;
            const key = if (s.owner) |owner|
                try identity.keyValueAs(u128, owner.key(owner.ctx, slot.idx), out.source.key)
            else
                try identity.keyValue(.{ .dense_u32 = @intCast(slot.idx) }, out.source.key);
            return switch (key) {
                .null_val => .null_val,
                .i => |v| .{ .i = v },
                .s => |v| .{ .s = try gpa.dupe(u8, v) },
            };
        }
        var buf: [48]u8 = undefined;
        const bytes = if (s.fixed_keys) expr_agg.canonicalKey(slot.key, &buf) else slot.key;
        return s.keyValue(gpa, bytes, out);
    }

    fn keyValue(s: *const RowSource, gpa: std.mem.Allocator, bytes: []const u8, out: BoundOutputColumn) !AggValue {
        const e = out.derived orelse return deserializeKeyColumnMode(gpa, bytes, out.source.key, s.key_types, s.borrowed_strings.len != 0);
        const Reader = struct {
            src: *const RowSource,
            gpa: std.mem.Allocator,
            bytes: []const u8,

            pub fn column(r: *@This(), ci: usize) !?i64 {
                for (r.src.key_exprs, 0..) |key, i| {
                    if (key != .col_ref or key.col_ref.col_idx != ci) continue;
                    const value = try deserializeKeyColumn(r.gpa, r.bytes, i, r.src.key_types);
                    return switch (value) {
                        .null_val => null,
                        .i => |v| std.math.cast(i64, v) orelse return error.BadGroupKey,
                        else => error.BadGroupKey,
                    };
                }
                return error.BadGroupKey;
            }
        };
        var reader: Reader = .{ .src = s, .gpa = gpa, .bytes = bytes };
        const value = try group_dependencies.evaluate(e, &reader);
        return if (value) |v| .{ .i = v } else .null_val;
    }

    fn integerOutputs(s: *const RowSource, types: []const GroupColType) bool {
        for (s.outputs, types) |out, ty| switch (out.source) {
            .key => {},
            .agg => |ai| {
                if (ty.lane != .i64) return false;
                const call = s.calls[ai];
                switch (call.func) {
                    .count => {},
                    .count_distinct => if (s.owner == null) return false,
                    .sum, .min, .max => if (call.result != .i64) return false,
                    else => return false,
                }
            },
        };
        return true;
    }

    const IntegerRow = struct {
        table: ?*const expr_agg.GroupTable,
        group: usize,
        accs: []const expr_agg.Accumulator,
        folded: bool,
        dense: ?*const agg_dense.Lanes = null,
        owner: ?OwnerRows = null,

        inline fn value(r: IntegerRow, ai: usize, func: expr_agg.AggFunc) !?i128 {
            if (r.owner) |owner| return owner.integer(owner.ctx, r.group, ai);
            if (r.dense) |d| return d.integer(r.group, ai);
            const x: ?i128 = if (r.table != null and r.table.?.compact.stride != 0)
                r.table.?.compact.integer(r.group, ai)
            else switch (func) {
                .count => r.accs[ai].count,
                .sum => r.accs[ai].sum_i,
                .min => r.accs[ai].min_i,
                .max => r.accs[ai].max_i,
                else => unreachable,
            };
            // The copying merge rejects this count before output narrowing, including a group seen only once.
            if (r.folded and func == .count and x.? > std.math.maxInt(i64)) return error.RowCountTooLarge;
            return x;
        }
    };

    fn integerRow(s: *const RowSource, slot: MergedSlot) IntegerRow {
        if (s.owner) |owner| return .{
            .table = null,
            .group = slot.idx,
            .accs = &.{},
            .folded = false,
            .owner = owner,
        };
        if (s.dense) |d| return .{
            .table = null,
            .group = slot.idx,
            .accs = &.{},
            .folded = false,
            .dense = d,
        };
        const table = if (s.in_place.len > 0) &s.in_place[slot.idx >> 32] else s.single;
        const group = if (s.in_place.len > 0) @as(u32, @truncate(slot.idx)) else slot.idx;
        const compact = table != null and table.?.compact.stride != 0;
        const at = if (s.folded) slot.idx else slot.idx * s.calls.len;
        return .{
            .table = table,
            .group = group,
            .accs = if (compact) &.{} else if (s.in_place.len > 0)
                table.?.accumulators.items[@as(usize, group) * s.calls.len ..][0..s.calls.len]
            else
                s.accs[at..][0..s.calls.len],
            .folded = s.in_place.len > 0,
        };
    }

    /// Fill `vals`, the row's slab cells, all NULL, for `slot`. Each value is set only once built, so on an error the
    /// row holds what was built and NULLs, which `AggOutput.releaseRow` frees.
    fn buildRow(s: *const RowSource, gpa: std.mem.Allocator, slot: MergedSlot, vals: []AggValue) !void {
        if (s.owner) |owner| {
            for (s.outputs, vals) |out, *val| val.* = switch (out.source) {
                .key => try s.outputKey(gpa, slot, out),
                .agg => |ai| try owner.value(owner.ctx, gpa, slot.idx, ai),
            };
            return;
        }
        if (s.dense) |d| {
            for (s.outputs, vals) |out, *val| val.* = switch (out.source) {
                .key => try s.outputKey(gpa, slot, out),
                .agg => |ai| try materializeOne(gpa, s.calls[ai], d.accumulator(slot.idx, ai)),
            };
            return;
        }
        const at = if (s.folded) slot.idx else slot.idx * s.calls.len;
        const table: ?*const expr_agg.GroupTable = if (s.in_place.len > 0)
            &s.in_place[slot.idx >> 32]
        else
            s.single;
        const group = if (s.in_place.len > 0) @as(u32, @truncate(slot.idx)) else slot.idx;
        const compact = table != null and table.?.compact.stride != 0;
        const accs = if (compact) &.{} else if (s.in_place.len > 0)
            table.?.accumulators.items[@as(usize, group) * s.calls.len ..][0..s.calls.len]
        else
            s.accs[at..][0..s.calls.len];
        for (s.outputs, vals) |out, *val| val.* = switch (out.source) {
            .key => try s.outputKey(gpa, slot, out),
            .agg => |a_idx| blk: {
                const acc = if (compact) table.?.readAcc(group, a_idx, s.calls.len) else accs[a_idx];
                if (s.borrowed_strings.len != 0 and (acc == .min_arena or acc == .max_arena))
                    break :blk if (acc.stringValue()) |bytes| .{ .s = bytes } else .null_val;
                break :blk try materializeOne(gpa, s.calls[a_idx], if (s.in_place.len > 0)
                    try foldedAcc(gpa, s.calls[a_idx], acc)
                else
                    acc);
            },
        };
    }
};

/// Build `rows[i]` for `src.slots[i]`, on up to `n_workers` threads. `rows` must start empty; on an error each row
/// holds what was built of it (see `RowSource.buildRow`), so the `AggOutput` owning it frees it whole, and the error
/// is the one the lowest failing row gave, as a serial build would return.
pub fn buildGroupRows(
    gpa: std.mem.Allocator,
    /// Receives the slab the rows are views into, owned by the caller (`AggOutput.slab`) from allocation on.
    slab: *[]AggValue,
    src: RowSource,
    rows: [][]const AggValue,
    n_workers: usize,
) !void {
    std.debug.assert(rows.len == src.slots.len);
    if (rows.len == 0) return;
    const width = src.outputs.len;
    const cells = try gpa.alloc(AggValue, rows.len * width);
    @memset(cells, .null_val);
    slab.* = cells;
    for (rows, 0..) |*row, i| row.* = cells[i * width ..][0..width];
    const n_threads = @max(1, @min(n_workers, std.math.divCeil(usize, rows.len, row_block) catch unreachable));
    const tasks = try gpa.alloc(RowBuildTask, n_threads);
    defer gpa.free(tasks);
    var cursor: std.atomic.Value(usize) = .init(0);
    for (tasks) |*t| t.* = .{ .gpa = gpa, .src = &src, .rows = rows, .cursor = &cursor };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (tasks.len == 1 or !spawn_util.runTree(RowBuildTask, tasks, config, RowBuildTask.run)) RowBuildTask.run(&tasks[0]);
    // Every block below the lowest failing row was claimed and built whole, so that row's error is a serial build's.
    var failed: ?*const RowBuildTask = null;
    for (tasks) |*t| if (t.err != null and (failed == null or t.err_row < failed.?.err_row)) {
        failed = t;
    };
    if (failed) |t| return t.err.?;
}

/// What a GROUP BY answer of `slots` holds once built (`buildGroupColumns` or `buildGroupRows`): its values, their
/// lanes and NULL flags, and its string keys' heap blocks. A string aggregate's own value is not counted.
pub fn answerBytes(slots: []const MergedSlot, cols: []const GroupColType, key_types: []const KeyType, typed: bool) usize {
    return answerBytesParallel(slots, cols, key_types, typed, 1);
}

pub fn answerBytesParallel(
    slots: []const MergedSlot,
    cols: []const GroupColType,
    key_types: []const KeyType,
    typed: bool,
    workers: usize,
) usize {
    const rows = slots.len;
    // Each string key value is a heap block of its own: rounded to 16 bytes, with a header.
    var string_keys: usize = 0;
    for (key_types) |kt| string_keys += @intFromBool(kt == .string);
    var strings: usize = 0;
    if (string_keys > 0) {
        const Task = struct {
            slots: []const MergedSlot,
            keys: usize,
            bytes: usize = 0,
            cursor: *std.atomic.Value(usize),
            fn run(t: *@This()) void {
                while (true) {
                    const lo = t.cursor.fetchAdd(row_block, .monotonic);
                    if (lo >= t.slots.len) return;
                    for (t.slots[lo..@min(t.slots.len, lo + row_block)]) |sl|
                        t.bytes +|= std.mem.alignForward(usize, sl.key.len, 16) +| t.keys *| 16;
                }
            }
        };
        var storage: [64]Task = undefined;
        const tasks = storage[0..@max(1, @min(storage.len, workers, rows / row_block + 1))];
        var cursor = std.atomic.Value(usize).init(0);
        for (tasks) |*t| t.* = .{ .slots = slots, .keys = string_keys, .cursor = &cursor };
        if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, .{ .stack_size = WORKER_STACK_SIZE }, Task.run))
            tasks[0].run();
        for (tasks) |t| strings +|= t.bytes;
    }
    if (!typed) return rows *| (cols.len *| @sizeOf(AggValue) +| @sizeOf([]const AggValue)) +| strings;
    var per_row: usize = 0;
    for (cols) |c| per_row += @sizeOf(u32) + switch (c.lane) {
        .i64, .f64 => @as(usize, 8),
        .string => @sizeOf([]const u8),
        .dec => 16,
        .avg => 16,
        .avg_dec => 24,
    };
    return rows *| per_row +| strings;
}

/// Build the GROUP BY answer as typed columns (`TypedAnswer`), on up to `n_workers` threads. The error is the row
/// path's: a value that fails to build at the lowest such row; else, by column, the first row that does not narrow.
pub fn buildGroupColumns(
    gpa: std.mem.Allocator,
    out: *TypedAnswer,
    src: RowSource,
    types: []const GroupColType,
    n_workers: usize,
) !void {
    try TypedAnswer.init(gpa, types, src.slots.len, out, n_workers);
    out.borrowed_strings = src.borrowed_strings;
    const n = src.slots.len;
    if (n == 0) return;
    const n_threads = @max(1, @min(n_workers, std.math.divCeil(usize, n, row_block) catch unreachable));
    const tasks = try gpa.alloc(ColumnBuildTask, n_threads);
    defer gpa.free(tasks);
    var cursor: std.atomic.Value(usize) = .init(0);
    const vals = try gpa.alloc(AggValue, n_threads * src.outputs.len);
    defer gpa.free(vals);
    for (tasks, 0..) |*t, ti| t.* = .{
        .gpa = gpa,
        .src = &src,
        .types = types,
        .answer = out,
        .cursor = &cursor,
        .vals = vals[ti * src.outputs.len ..][0..src.outputs.len],
        .integers = src.integerOutputs(types),
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (tasks.len == 1 or !spawn_util.runTree(ColumnBuildTask, tasks, config, ColumnBuildTask.run)) ColumnBuildTask.run(&tasks[0]);
    var failed: ?*const ColumnBuildTask = null;
    for (tasks) |*t| if (t.err != null and (failed == null or t.err_row < failed.?.err_row)) {
        failed = t;
    };
    if (failed) |t| return t.err.?;
    // Narrowing errors in column order, each column's first failing row, as the row writer meets them.
    var narrow: ?struct { col: usize, row: usize, err: anyerror } = null;
    for (tasks) |t| if (t.narrow_err) |e| {
        if (narrow == null or t.narrow_col < narrow.?.col or (t.narrow_col == narrow.?.col and t.narrow_row < narrow.?.row))
            narrow = .{ .col = t.narrow_col, .row = t.narrow_row, .err = e };
    };
    if (narrow) |e| return e.err;
    try out.seal(types);
}

const ColumnBuildTask = struct {
    gpa: std.mem.Allocator,
    src: *const RowSource,
    types: []const GroupColType,
    answer: *TypedAnswer,
    cursor: *std.atomic.Value(usize),
    /// One row's values, built then stored a row at a time.
    vals: []AggValue,
    integers: bool,
    err: ?anyerror = null,
    err_row: usize = 0,
    narrow_err: ?anyerror = null,
    narrow_col: usize = 0,
    narrow_row: usize = 0,

    fn run(t: *ColumnBuildTask) void {
        const n = t.src.slots.len;
        while (true) {
            const lo = t.cursor.fetchAdd(row_block, .monotonic);
            if (lo >= n) return;
            for (lo..@min(n, lo + row_block)) |i| {
                if (t.integers) {
                    t.buildIntegers(i) catch |e| {
                        t.err = e;
                        t.err_row = i;
                        return;
                    };
                    continue;
                }
                @memset(t.vals, .null_val);
                const built = t.src.buildRow(t.gpa, t.src.slots[i], t.vals);
                // Every value built passes to its column, whatever happens to the rest of the row.
                for (t.answer.columns, t.types, t.vals, 0..) |*col, ty, v, ci| {
                    TypedAnswer.put(col, ty, i, v) catch |e| {
                        if (v == .s and !BorrowedSpan.contains(t.src.borrowed_strings, v.s)) t.gpa.free(v.s);
                        if (t.narrow_err == null or ci < t.narrow_col or (ci == t.narrow_col and i < t.narrow_row)) {
                            t.narrow_err = e;
                            t.narrow_col = ci;
                            t.narrow_row = i;
                        }
                    };
                }
                built catch |e| {
                    t.err = e;
                    t.err_row = i;
                    return;
                };
            }
        }
    }

    fn narrowing(t: *ColumnBuildTask, err: anyerror, ci: usize, row: usize) void {
        if (t.narrow_err == null or ci < t.narrow_col or (ci == t.narrow_col and row < t.narrow_row)) {
            t.narrow_err = err;
            t.narrow_col = ci;
            t.narrow_row = row;
        }
    }

    /// Keep row/output visitation and the materialization-before-narrowing error contract. Only integer states
    /// bypass the generic unions; key strings still acquire the answer's owned copy before being published.
    fn buildIntegers(t: *ColumnBuildTask, i: usize) !void {
        const s = t.src;
        const slot = s.slots[i];
        const row = s.integerRow(slot);
        for (s.outputs, t.types, t.answer.columns, 0..) |out, ty, *col, ci| switch (out.source) {
            .key => {
                const v = try s.outputKey(t.gpa, slot, out);
                TypedAnswer.put(col, ty, i, v) catch |e| {
                    if (v == .s and !BorrowedSpan.contains(t.src.borrowed_strings, v.s)) t.gpa.free(v.s);
                    t.narrowing(e, ci, i);
                };
            },
            .agg => |ai| if (try row.value(ai, s.calls[ai].func)) |x| {
                TypedAnswer.putInteger(&col.i64, ty, i, x) catch |e| t.narrowing(e, ci, i);
            },
        };
    }
};

const RowBuildTask = struct {
    gpa: std.mem.Allocator,
    src: *const RowSource,
    rows: [][]const AggValue,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,
    err_row: usize = 0,

    /// Claim blocks of rows until none remain or one fails. A thread that never starts leaves its blocks on the
    /// cursor, so a failed spawn cannot leave a row unbuilt.
    fn run(t: *RowBuildTask) void {
        while (true) {
            const lo = t.cursor.fetchAdd(row_block, .monotonic);
            if (lo >= t.rows.len) return;
            for (lo..@min(t.rows.len, lo + row_block)) |i| {
                t.src.buildRow(t.gpa, t.src.slots[i], @constCast(t.rows[i])) catch |e| {
                    t.err = e;
                    t.err_row = i;
                    return;
                };
            }
        }
    }
};

/// `n` groups for the row-build tests, laid out as one table's: group i has the string key `g<i>`, a count of i and
/// a string min of `m<i>`.
const RowFixture = struct {
    keys: [][]u8,
    accs: []expr_agg.Accumulator,
    slots: []MergedSlot,

    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "n", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "m", .result = .bytes, .arg = null, .where = null },
    };
    /// Not in key-then-aggregate order, so a row filled by position would show.
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .agg = 1 }, .name = "m" },
        .{ .source = .{ .key = 0 }, .name = "g" },
        .{ .source = .{ .agg = 0 }, .name = "n" },
    };

    fn init(n: usize) !RowFixture {
        const gpa = testing.allocator;
        const keys = try gpa.alloc([]u8, n);
        @memset(keys, &.{});
        const accs = try gpa.alloc(expr_agg.Accumulator, n * calls.len);
        for (0..n) |i| accs[i * calls.len ..][0..2].* = .{ .{ .count = i }, .{ .min_bytes = null } };
        const slots = try gpa.alloc(MergedSlot, n);
        var f: RowFixture = .{ .keys = keys, .accs = accs, .slots = slots };
        errdefer f.deinit();
        var buf: [32]u8 = undefined;
        for (0..n) |i| {
            const name = try std.fmt.bufPrint(&buf, "g{d}", .{i});
            // The serialized framing of one present string column: a flag, the length, the bytes.
            keys[i] = try gpa.alloc(u8, 5 + name.len);
            keys[i][0] = 1;
            std.mem.writeInt(u32, keys[i][1..5], @intCast(name.len), .little);
            @memcpy(keys[i][5..], name);
            const min = try std.fmt.allocPrint(gpa, "m{d}", .{i});
            accs[i * calls.len + 1] = .{ .min_bytes = min };
            slots[i] = .{ .key = keys[i], .first = i, .idx = i };
        }
        return f;
    }

    fn deinit(f: *RowFixture) void {
        const gpa = testing.allocator;
        for (f.keys) |k| gpa.free(k);
        gpa.free(f.keys);
        freeOwnedAccumulatorStrings(gpa, f.accs);
        gpa.free(f.accs);
        gpa.free(f.slots);
    }

    fn source(f: *const RowFixture) RowSource {
        return .{
            .slots = f.slots,
            .outputs = &outputs,
            .calls = &calls,
            .key_types = &.{.string},
            .accs = f.accs,
            .folded = false,
            .fixed_keys = false,
        };
    }

    /// Build every row into a fresh answer on `gpa`, which owns it whatever happens.
    fn build(f: *const RowFixture, gpa: std.mem.Allocator, out: *AggOutput, n_workers: usize) !void {
        const rows = try gpa.alloc([]const AggValue, f.slots.len);
        @memset(rows, &.{});
        out.group_rows = rows;
        try buildGroupRows(gpa, &out.slab, f.source(), rows, n_workers);
    }

    fn expectRows(rows: []const []const AggValue) !void {
        var buf: [32]u8 = undefined;
        for (rows, 0..) |row, i| {
            try testing.expectEqual(@as(usize, 3), row.len);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "m{d}", .{i}), row[0].s);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "g{d}", .{i}), row[1].s);
            try testing.expectEqual(@as(i128, @intCast(i)), row[2].i);
        }
    }
};

/// Fails every allocation once `left` have succeeded, and any of exactly `poison_len` bytes, on whichever thread asks:
/// `FailingAllocator` counts without atomics, so it cannot fail one allocation among several threads.
const SharedFailing = struct {
    child: std.mem.Allocator,
    left: std.atomic.Value(usize) = .init(std.math.maxInt(usize)),
    poison_len: ?usize = null,

    fn allocator(self: *SharedFailing) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = std.mem.Allocator.noResize,
            .remap = std.mem.Allocator.noRemap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *SharedFailing = @ptrCast(@alignCast(ctx));
        if (self.poison_len == len) return null;
        var left = self.left.load(.monotonic);
        while (left != 0) left = self.left.cmpxchgWeak(left, left - 1, .monotonic, .monotonic) orelse
            return self.child.rawAlloc(len, alignment, ret_addr);
        return null;
    }

    fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *SharedFailing = @ptrCast(@alignCast(ctx));
        self.child.rawFree(memory, alignment, ret_addr);
    }
};

test "buildGroupRows: the same rows at every -j, and when spawns fail" {
    // Five blocks of rows, so up to five threads build them.
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    const Run = struct { j: usize, fail_after: ?usize = null };
    for ([_]Run{ .{ .j = 1 }, .{ .j = 3 }, .{ .j = 8 }, .{ .j = 8, .fail_after = 0 }, .{ .j = 8, .fail_after = 1 }, .{ .j = 8, .fail_after = 2 } }) |run| {
        var out: AggOutput = .{};
        defer out.deinit(testing.allocator);
        if (run.fail_after) |n| spawn_util.injectFailureAfter(n);
        defer spawn_util.resetFailure();
        try f.build(testing.allocator, &out, run.j);
        if (run.fail_after != null) try testing.expect(spawn_util.failuresInjected() > 0);
        try RowFixture.expectRows(out.group_rows.?);
    }
}

test "buildGroupRows: an allocation failure leaves every row freeable" {
    // One thread: every allocation failed in turn.
    var small = try RowFixture.init(3);
    defer small.deinit();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, f: *const RowFixture) !void {
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            try f.build(gpa, &out, 1);
        }
    }.run, .{&small});

    // Eight threads: the failure lands in whichever row a thread happens to be building.
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    // Rows are slab views now, so what fails is the slab or a row's own strings (at least one a row).
    for ([_]usize{ 0, 1, 2, 3, 100, 3 * row_block, f.slots.len - 1 }) |left| {
        var failing: SharedFailing = .{ .child = testing.allocator, .left = .init(left) };
        const gpa = failing.allocator();
        var out: AggOutput = .{};
        defer out.deinit(gpa);
        try testing.expectError(error.OutOfMemory, f.build(gpa, &out, 8));
    }
}

fn directIntegerColumnsTrial(gpa: std.mem.Allocator, compact: bool, in_place: bool, n: usize, j: usize) !void {
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .arg = null, .where = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = null, .where = null },
        .{ .func = .min, .alias = "lo", .result = .i64, .arg = null, .where = null },
        .{ .func = .max, .alias = "hi", .result = .i64, .arg = null, .where = null },
    };
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .agg = 1 }, .name = "s" },
        .{ .source = .{ .key = 0 }, .name = "k" },
        .{ .source = .{ .agg = 0 }, .name = "c" },
        .{ .source = .{ .agg = 2 }, .name = "lo" },
        .{ .source = .{ .agg = 3 }, .name = "hi" },
    };
    const types = [_]GroupColType{
        .{ .lane = .i64, .nullable = true },  .{ .lane = .i64, .nullable = true },
        .{ .lane = .i64, .nullable = false }, .{ .lane = .i64, .nullable = true },
        .{ .lane = .i64, .nullable = true },
    };
    var tables = [_]expr_agg.GroupTable{.init(gpa, 64 << 20)};
    defer tables[0].deinit();
    const table = &tables[0];
    table.fixed_stride = 9;
    if (compact) table.compact.configure(&calls);
    const slots = try gpa.alloc(MergedSlot, n);
    defer gpa.free(slots);
    for (0..n) |i| {
        var key: [9]u8 = @splat(0);
        if (i != 0) {
            key[0] = 1;
            std.mem.writeInt(i64, key[1..9], -@as(i64, @intCast(i)), .little);
        }
        const id = try table.getOrInsert(&key, &calls, i);
        table.writeAcc(id, 0, calls.len, .{ .count = i + 1 });
        const x: ?i128 = if (i % 5 == 0) null else -@as(i128, @intCast(i)) * 2;
        table.writeAcc(id, 1, calls.len, .{ .sum_i = x });
        table.writeAcc(id, 2, calls.len, .{ .min_i = x });
        table.writeAcc(id, 3, calls.len, .{ .max_i = x });
        slots[i] = .{ .key = &.{}, .idx = id, .first = i };
    }
    for (slots) |*slot| slot.key = table.keyAt(@intCast(slot.idx));
    const src: RowSource = .{
        .slots = slots,
        .outputs = &outputs,
        .calls = &calls,
        .key_types = &.{.i64},
        .accs = table.accumulators.items,
        .folded = in_place,
        .fixed_keys = true,
        .single = if (in_place) null else table,
        .in_place = if (in_place) &tables else &.{},
    };
    try testing.expect(src.integerOutputs(&types));
    var out: TypedAnswer = .{ .rows = 0, .columns = &.{} };
    defer out.deinit(gpa);
    try buildGroupColumns(gpa, &out, src, &types, j);
    for (slots, 0..) |slot, i| {
        var vals: [outputs.len]AggValue = @splat(.null_val);
        try src.buildRow(gpa, slot, &vals);
        for (out.columns, vals) |col, v| {
            try testing.expectEqual(@as(u32, @intFromBool(v != .null_val)), col.i64.defs[i]);
            if (v != .null_val) try testing.expectEqual(v.i, col.i64.values[i]);
        }
    }
}

fn ownerColumnsTrial(a: std.mem.Allocator, j: usize) !void {
    const L = owner_groups.PackedLayout(u128, &.{ .sum, .present_count }, u32);
    const keys = [_]expr_ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } },
    };
    var identity = (try key_pack.Plan.init(a, &keys, null)).?;
    defer identity.deinit(a);
    const batch: @import("../filter/eval.zig").Batch = .{ .num_rows = 2, .cols = &.{
        .{ .i32 = .{ .values = &.{ -1, 0 }, .def_levels = &.{ 1, 0 }, .max_def = 1 } },
        .{ .i32 = .{ .values = &.{ 0, 5 } } },
    } };
    var bindings: [2]key_pack.Binding = undefined;
    const bound = (try identity.bindInto(&bindings, .{ .batch = &batch }, &.{ 0, 1 }, 0, 0)).?;
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]expr_agg.AggCall{
        .{ .func = .count, .alias = "c", .result = .i64, .where = null, .arg = null },
        .{ .func = .sum, .alias = "s", .result = .i64, .where = null, .arg = arg },
        .{ .func = .avg, .alias = "a", .result = .avg_f64, .where = null, .arg = arg },
        .{ .func = .count, .alias = "n", .result = .i64, .where = null, .arg = arg },
    };
    const groups = [_]L.Group{
        try L.Group.init(.{ .key = try bound.packAs(u128, 0), .ordinal = 2, .weight = 3, .values = .{ 17, 2 } }),
        try L.Group.init(.{
            .key = try bound.packAs(u128, 1),
            .ordinal = 9,
            .weight = 2,
            .values = .{ 0, 0 },
            .argument_nulls = 1,
        }),
    };
    const Ctx = struct {
        groups: []const L.Group,
        calls: []const expr_agg.AggCall,
        program: agg_plan.OwnerProgram,
        fn key(ctx: *const anyopaque, row: usize) u128 {
            const c: *const @This() = @ptrCast(@alignCast(ctx));
            return c.groups[row].key;
        }
        fn integer(ctx: *const anyopaque, row: usize, ai: usize) !?i128 {
            const c: *const @This() = @ptrCast(@alignCast(ctx));
            return c.program.integer(c.groups[row], c.calls[ai], null);
        }
        fn value(ctx: *const anyopaque, gpa: std.mem.Allocator, row: usize, ai: usize) !AggValue {
            const c: *const @This() = @ptrCast(@alignCast(ctx));
            return materializeOne(gpa, c.calls[ai], try c.program.accumulator(c.groups[row], c.calls[ai]));
        }
    };
    const ctx: Ctx = .{ .groups = &groups, .calls = &calls, .program = agg_plan.OwnerProgram.init(&calls).? };
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "k" }, .{ .source = .{ .key = 1 }, .name = "f" },
        .{ .source = .{ .agg = 0 }, .name = "c" }, .{ .source = .{ .agg = 1 }, .name = "s" },
        .{ .source = .{ .agg = 2 }, .name = "a" }, .{ .source = .{ .agg = 3 }, .name = "n" },
    };
    const slots = [_]MergedSlot{ .{ .key = "", .first = 2, .idx = 0 }, .{ .key = "", .first = 9, .idx = 1 } };
    const src: RowSource = .{
        .slots = &slots,
        .outputs = &outputs,
        .calls = &calls,
        .key_types = &.{ .i64, .i64 },
        .accs = &.{},
        .folded = false,
        .fixed_keys = false,
        .identity = &identity,
        .owner = .{ .ctx = &ctx, .key = Ctx.key, .integer = Ctx.integer, .value = Ctx.value },
    };
    const types = [_]GroupColType{
        .{ .lane = .i64, .nullable = true },  .{ .lane = .i64, .nullable = true },
        .{ .lane = .i64, .nullable = false }, .{ .lane = .i64, .nullable = true },
        .{ .lane = .f64, .nullable = true },  .{ .lane = .i64, .nullable = false },
    };
    var answer: TypedAnswer = .{ .rows = 0, .columns = &.{} };
    defer answer.deinit(a);
    try buildGroupColumns(a, &answer, src, &types, j);
    const expected = [_][6]AggValue{
        .{ .{ .i = -1 }, .{ .i = 0 }, .{ .i = 3 }, .{ .i = 17 }, .{ .f = 8.5 }, .{ .i = 2 } },
        .{ .null_val, .{ .i = 5 }, .{ .i = 2 }, .null_val, .null_val, .{ .i = 0 } },
    };
    for (slots, expected, 0..) |slot, want, ri| {
        var row: [6]AggValue = @splat(.null_val);
        try src.buildRow(a, slot, &row);
        try testing.expectEqualDeep(want, row);
        for (answer.columns, row) |column, v| switch (column) {
            .i64 => |col| {
                try testing.expectEqual(@as(u32, @intFromBool(v != .null_val)), col.defs[ri]);
                if (v != .null_val) try testing.expectEqual(v.i, col.values[ri]);
            },
            .f64 => |col| {
                try testing.expectEqual(@as(u32, @intFromBool(v != .null_val)), col.defs[ri]);
                if (v != .null_val) try testing.expectEqual(
                    @as(u64, @bitCast(v.f)),
                    @as(u64, @bitCast(col.values[ri])),
                );
            },
            else => return error.TestUnexpectedResult,
        };
    }
    const integer_src = blk: {
        var s = src;
        s.outputs = outputs[0..4];
        break :blk s;
    };
    var integers: TypedAnswer = .{ .rows = 0, .columns = &.{} };
    defer integers.deinit(a);
    try testing.expect(integer_src.integerOutputs(types[0..4]));
    try buildGroupColumns(a, &integers, integer_src, types[0..4], j);
    for (integers.columns, answer.columns[0..4]) |actual, want|
        try testing.expectEqualDeep(want, actual);
}

test "owner typed finish shares KeyPlan unpack integer AVG NULLs and column narrowing with row output" {
    for ([_]usize{ 1, 3, 16 }) |j| try ownerColumnsTrial(testing.allocator, j);
    try testing.checkAllAllocationFailures(testing.allocator, ownerColumnsTrial, .{1});
}

test "direct integer columns: scalar rows' values with compact/generic states, all thread counts and failures" {
    for ([_]bool{ false, true }) |compact| for ([_]bool{ false, true }) |in_place| {
        for ([_]usize{ 1, 2, 4, 8, 16 }) |j|
            try directIntegerColumnsTrial(testing.allocator, compact, in_place, 2 * row_block + 7, j);
        spawn_util.injectFailureAfter(0);
        defer spawn_util.resetFailure();
        try directIntegerColumnsTrial(testing.allocator, compact, in_place, 2 * row_block + 7, 8);
        spawn_util.resetFailure();
        try testing.checkAllAllocationFailures(testing.allocator, directIntegerColumnsTrial, .{ compact, in_place, 3, 1 });
    };
}

test "direct integer columns: string key ownership and allocation failures" {
    var f = try RowFixture.init(3);
    defer f.deinit();
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .key = 0 }, .name = "g" },
        .{ .source = .{ .agg = 0 }, .name = "n" },
    };
    const types = [_]GroupColType{ .{ .lane = .string, .nullable = true }, .{ .lane = .i64, .nullable = false } };
    var src = f.source();
    src.outputs = &outputs;
    try testing.expect(src.integerOutputs(&types));
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, source: RowSource, ty: []const GroupColType) !void {
            var out: TypedAnswer = .{ .rows = 0, .columns = &.{} };
            defer out.deinit(gpa);
            try buildGroupColumns(gpa, &out, source, ty, 1);
            for (out.columns[0].string.values, source.slots) |key, slot|
                try testing.expectEqualStrings(slot.key[5..], key);
        }
    }.run, .{ src, &types });
}

test "direct integer columns: materialization errors precede narrowing and folded counts keep their error" {
    var f = try RowFixture.init(3);
    defer f.deinit();
    const outputs = [_]BoundOutputColumn{
        .{ .source = .{ .agg = 0 }, .name = "n" },
        .{ .source = .{ .key = 0 }, .name = "g" },
    };
    const types = [_]GroupColType{ .{ .lane = .i64, .nullable = false }, .{ .lane = .string, .nullable = true } };
    f.accs[0] = .{ .count = @as(u64, std.math.maxInt(i64)) + 1 };
    const key = f.slots[1].key;
    f.slots[1].key = &.{1};
    var src = f.source();
    src.outputs = &outputs;
    for ([_]usize{ 1, 2, 4, 8, 16 }) |j| {
        var out: TypedAnswer = .{ .rows = 0, .columns = &.{} };
        defer out.deinit(testing.allocator);
        try testing.expectError(error.BadGroupKey, buildGroupColumns(testing.allocator, &out, src, &types, j));
    }
    f.slots[1].key = key;
    var out: TypedAnswer = .{ .rows = 0, .columns = &.{} };
    defer out.deinit(testing.allocator);
    try testing.expectError(error.AggIntTooWide, buildGroupColumns(testing.allocator, &out, src, &types, 1));
    const row: RowSource.IntegerRow = .{ .table = null, .group = 0, .accs = f.accs, .folded = true };
    try testing.expectError(error.RowCountTooLarge, row.value(0, .count));
}

test "buildGroupColumns: the rows' values as typed columns at every -j; freeable on any allocation failure" {
    const types = [_]GroupColType{
        .{ .lane = .string, .nullable = true },
        .{ .lane = .string, .nullable = true },
        .{ .lane = .i64, .nullable = false },
    };
    var f = try RowFixture.init(3 * row_block + 77);
    defer f.deinit();
    for ([_]usize{ 1, 3, 8 }) |j| {
        var out: AggOutput = .{};
        defer out.deinit(testing.allocator);
        out.typed = .{ .rows = 0, .columns = &.{} };
        try buildGroupColumns(testing.allocator, &out.typed.?, f.source(), &types, j);
        const t = out.typed.?;
        try testing.expectEqual(f.slots.len, t.rows);
        var buf: [32]u8 = undefined;
        for (0..t.rows) |i| {
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "m{d}", .{i}), t.columns[0].string.values[i]);
            try testing.expectEqualStrings(try std.fmt.bufPrint(&buf, "g{d}", .{i}), t.columns[1].string.values[i]);
            try testing.expectEqual(@as(i64, @intCast(i)), t.columns[2].i64.values[i]);
        }
        try testing.expect(!t.columns[2].i64.nulls);
    }
    var small = try RowFixture.init(3);
    defer small.deinit();
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator, fx: *const RowFixture, ty: []const GroupColType) !void {
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            out.typed = .{ .rows = 0, .columns = &.{} };
            try buildGroupColumns(gpa, &out.typed.?, fx.source(), ty, 1);
        }
    }.run, .{ &small, &types });
    // A count column that may not be NULL refuses a NULL as the row writer does; a narrowing error is the first
    // column's, by row.
    const strict = [_]GroupColType{
        .{ .lane = .string, .nullable = false },
        .{ .lane = .string, .nullable = true },
        .{ .lane = .f64, .nullable = true },
    };
    var out: AggOutput = .{};
    defer out.deinit(testing.allocator);
    out.typed = .{ .rows = 0, .columns = &.{} };
    try testing.expectError(error.TypeMismatch, buildGroupColumns(testing.allocator, &out.typed.?, f.source(), &strict, 8));
}

test "buildGroupRows: the error the lowest failing row gives, at every -j" {
    var f = try RowFixture.init(4 * row_block + 123);
    defer f.deinit();
    // A truncated key fails to read back; a min of `poison_len` bytes fails to copy.
    const poison_len = 77;
    const poisoned: [poison_len]u8 = @splat('x');
    const truncated = [_]u8{1};
    for ([_][2]usize{ .{ 15_000, 7_000 }, .{ 7_000, 15_000 }, .{ 9, 4 * row_block + 100 } }) |at| {
        const bad_key, const bad_min = at;
        const old_key = f.slots[bad_key].key;
        defer f.slots[bad_key].key = old_key;
        f.slots[bad_key].key = &truncated;
        const min_acc = &f.accs[bad_min * RowFixture.calls.len + 1];
        const old_min = min_acc.min_bytes;
        defer min_acc.min_bytes = old_min;
        min_acc.min_bytes = &poisoned;
        const want: anyerror = if (bad_key < bad_min) error.BadGroupKey else error.OutOfMemory;
        for ([_]usize{ 1, 3, 8 }) |j| {
            var failing: SharedFailing = .{ .child = testing.allocator, .poison_len = poison_len };
            const gpa = failing.allocator();
            var out: AggOutput = .{};
            defer out.deinit(gpa);
            try testing.expectError(want, f.build(gpa, &out, j));
        }
    }
}

