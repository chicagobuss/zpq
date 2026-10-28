const std = @import("std");
const testing = std.testing;
const expr_agg = @import("../expr/agg.zig");
const spawn_util = @import("../spawn.zig");
const WORKER_STACK_SIZE = @import("types.zig").WORKER_STACK_SIZE;

/// Calls `f(ctx, v)` for every value of a set of u64 values, partitioned or not.
fn eachDistinctU64(ds: *const expr_agg.DistinctSet, ctx: anytype, comptime f: fn (@TypeOf(ctx), u64) void) void {
    const one = struct {
        fn set(d: *const expr_agg.DistinctSet, c: @TypeOf(ctx)) void {
            switch (d.val_set) {
                .inline_u64 => |s| for (s.vals[0..s.len]) |v| f(c, v),
                .bitmap => |bits| {
                    var it = bits.iterator();
                    while (it.next()) |bit| f(c, bits.base + bit);
                },
                .u64 => |s| {
                    var it = s.keyIterator();
                    while (it.next()) |k| f(c, k.*);
                },
                else => {},
            }
        }
    };
    one.set(ds, ctx);
    if (ds.parts) |parts| for (parts) |*part| one.set(part, ctx);
}

fn distinctSlice(v: u64) usize {
    return @intCast((v *% 11400714819323198485) >> 60);
}

const Phase1Task = struct {
    gpa: std.mem.Allocator,
    sources: []const *const expr_agg.DistinctSet,
    slices: []std.ArrayListUnmanaged(u64),
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    /// Split each claimed source set's values into its 16 slices: counted first, so each slice is reserved at
    /// exactly its size before it is filled.
    fn run(t: *Phase1Task) void {
        while (t.err == null) {
            const m = t.cursor.fetchAdd(1, .monotonic);
            if (m >= t.sources.len) return;
            const row = t.slices[m * 16 .. (m + 1) * 16];
            var sizes: [16]usize = @splat(0);
            eachDistinctU64(t.sources[m], &sizes, struct {
                fn f(n: *[16]usize, v: u64) void {
                    n[distinctSlice(v)] += 1;
                }
            }.f);
            for (row, sizes) |*cell, n| cell.ensureTotalCapacityPrecise(t.gpa, n) catch |e| {
                t.err = e;
                return;
            };
            eachDistinctU64(t.sources[m], row, struct {
                fn f(r: []std.ArrayListUnmanaged(u64), v: u64) void {
                    r[distinctSlice(v)].appendAssumeCapacity(v);
                }
            }.f);
        }
    }
};

const Phase2Task = struct {
    gpa: std.mem.Allocator,
    slices: []std.ArrayListUnmanaged(u64),
    M: usize,
    counts: []usize,
    cursor: *std.atomic.Value(usize),
    err: ?anyerror = null,

    fn run(t: *Phase2Task) void {
        while (t.err == null) {
            const p = t.cursor.fetchAdd(1, .monotonic);
            if (p >= 16) return;
            var total: usize = 0;
            for (0..t.M) |m| total += t.slices[m * 16 + p].items.len;
            if (total == 0) {
                t.counts[p] = 0;
                continue;
            }
            if (total == 1) {
                t.counts[p] = 1;
                continue;
            }
            var local_map: std.AutoHashMapUnmanaged(u64, void) = .empty;
            defer local_map.deinit(t.gpa);
            local_map.ensureTotalCapacity(t.gpa, @intCast(total)) catch |err| {
                t.err = err;
                return;
            };
            for (0..t.M) |m| {
                for (t.slices[m * 16 + p].items) |v| {
                    local_map.putAssumeCapacity(v, {});
                }
            }
            t.counts[p] = local_map.count();
        }
    }
};

pub fn parallelDistinctCountU64(
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    sources: []const *const expr_agg.DistinctSet,
    n_workers: usize,
) !u64 {
    const M = sources.len;
    const slices = try arena.alloc(std.ArrayListUnmanaged(u64), M * 16);
    for (slices) |*cell| cell.* = .empty;
    defer for (slices) |*cell| cell.deinit(gpa);

    const n_threads = @min(n_workers, @max(M, 16));
    const config: std.Thread.SpawnConfig = .{ .stack_size = WORKER_STACK_SIZE };

    {
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = try arena.alloc(Phase1Task, n_threads);
        for (tasks) |*task| task.* = .{
            .gpa = gpa,
            .sources = sources,
            .slices = slices,
            .cursor = &cursor,
        };
        if (!spawn_util.runTree(Phase1Task, tasks, config, Phase1Task.run)) {
            Phase1Task.run(&tasks[0]);
        }
        for (tasks) |task| if (task.err) |e| return e;
    }

    var counts: [16]usize = undefined;
    {
        var cursor = std.atomic.Value(usize).init(0);
        const tasks = try arena.alloc(Phase2Task, n_threads);
        for (tasks) |*task| task.* = .{
            .gpa = gpa,
            .slices = slices,
            .M = M,
            .counts = &counts,
            .cursor = &cursor,
        };
        if (!spawn_util.runTree(Phase2Task, tasks, config, Phase2Task.run)) {
            Phase2Task.run(&tasks[0]);
        }
        for (tasks) |task| if (task.err) |e| return e;
    }

    var total: u64 = 0;
    for (counts) |c| total += c;
    return total;
}

test "parallelDistinctCountU64 counts the exact union of overlapping sets" {
    const a = testing.allocator;
    var sets: [3]expr_agg.DistinctSet = .{ .{}, .{}, .{} };
    defer for (&sets) |*d| d.deinit(a);
    const ranges = [_][2]u64{ .{ 0, 50_000 }, .{ 25_000, 75_000 }, .{ 60_000, 100_000 } };
    for (&sets, ranges) |*d, r| for (r[0]..r[1]) |v| try d.insertU64(a, v * 7919);
    const sources = [_]*const expr_agg.DistinctSet{ &sets[0], &sets[1], &sets[2] };
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    for ([_]usize{ 2, 4, 16 }) |workers| {
        try testing.expectEqual(@as(u64, 100_000), try parallelDistinctCountU64(a, arena_state.allocator(), &sources, workers));
    }
}
