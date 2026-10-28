const std = @import("std");
const partagg = @import("../partagg.zig");
const expr_agg = @import("../expr/agg.zig");
const expr_ast = @import("../expr/ast.zig");
const spawn_util = @import("../spawn.zig");
const types = @import("types.zig");
const BoundOutputColumn = types.BoundOutputColumn;
const WORKER_STACK_SIZE = types.WORKER_STACK_SIZE;
const MergedSlot = @import("count_cut.zig").MergedSlot;
const rank_cut = @import("rank_cut.zig");
const ResultCut = rank_cut.ResultCut;
const resultLimitGroups = rank_cut.resultLimitGroups;
const detachOwned = @import("owned_merge.zig").detachOwned;

/// Move each partition's groups into `slots` / `accs`. Keys stay borrowed from the partition tables.
pub fn fillFromPartitions(
    gpa: std.mem.Allocator,
    lists: std.mem.Allocator,
    arena: std.mem.Allocator,
    parts: []partagg.Part,
    calls: []const expr_agg.AggCall,
    outputs: []const BoundOutputColumn,
    order: expr_ast.ResultOrder,
    unordered: bool,
    result_cut: ?ResultCut,
    n_workers: usize,
    slots: *std.ArrayList(MergedSlot),
    accs: *std.ArrayList(expr_agg.Accumulator),
) !void {
    _ = gpa;
    _ = arena;
    const ma = if (result_cut) |c| c.memory orelse lists else lists;
    const Piece = struct { ids: ?[]u32 = null, n: usize = 0, offset: usize = 0 };
    const pieces = try ma.alloc(Piece, parts.len);
    @memset(pieces, .{});
    defer {
        for (pieces) |p| if (p.ids) |ids| ma.free(ids);
        ma.free(pieces);
    }
    const Task = struct {
        ma: std.mem.Allocator,
        parts: []partagg.Part,
        pieces: []Piece,
        calls: []const expr_agg.AggCall,
        outputs: []const BoundOutputColumn,
        order: expr_ast.ResultOrder,
        unordered: bool,
        slots: []MergedSlot = &.{},
        accs: []expr_agg.Accumulator = &.{},
        copy: bool = false,
        cursor: *std.atomic.Value(usize),
        err: ?anyerror = null,
        fn run(t: *@This()) void {
            while (t.err == null) {
                const p = t.cursor.fetchAdd(1, .monotonic);
                if (p >= t.parts.len) return;
                const gt = &t.parts[p].table;
                const piece = &t.pieces[p];
                if (!t.copy) {
                    piece.ids = resultLimitGroups(t.ma, gt, t.outputs, t.calls, t.order, t.unordered, t.ma) catch |e| {
                        t.err = e;
                        return;
                    };
                    piece.n = if (piece.ids) |ids| ids.len else gt.groupCount();
                } else for (0..piece.n) |i| {
                    const g = if (piece.ids) |ids| ids[i] else i;
                    var scratch: [16]expr_agg.Accumulator = undefined;
                    const src = gt.readGroup(g, t.calls.len, &scratch);
                    const at = piece.offset + i;
                    @memcpy(t.accs[at * t.calls.len ..][0..t.calls.len], src);
                    for (src) |*a| detachOwned(a);
                    t.slots[at] = .{ .key = gt.keyAt(g), .first = gt.first.items[g], .idx = at * t.calls.len };
                }
            }
        }
    };
    var storage: [64]Task = undefined;
    const tasks = storage[0..@max(1, @min(n_workers, parts.len, storage.len))];
    var cursor = std.atomic.Value(usize).init(0);
    for (tasks) |*t| t.* = .{
        .ma = ma,
        .parts = parts,
        .pieces = pieces,
        .calls = calls,
        .outputs = outputs,
        .order = order,
        .unordered = unordered,
        .cursor = &cursor,
    };
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };
    if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, config, Task.run)) tasks[0].run();
    for (tasks) |t| if (t.err) |e| return e;
    var total: usize = 0;
    for (pieces) |*p| {
        p.offset = total;
        total += p.n;
    }
    try slots.ensureTotalCapacity(ma, total);
    try accs.ensureTotalCapacity(ma, total * calls.len);
    // The copy cannot fail. From this point these lists own every moved winner, even after a failed thread spawn.
    slots.items.len = total;
    accs.items.len = total * calls.len;
    cursor.store(0, .monotonic);
    for (tasks) |*t| {
        t.copy = true;
        t.slots = slots.items;
        t.accs = accs.items;
    }
    if (tasks.len == 1 or !spawn_util.runTree(Task, tasks, config, Task.run)) tasks[0].run();
}

