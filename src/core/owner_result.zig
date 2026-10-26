//! Partition receivers retain private typed results. Only bounded top-H candidates reach the final serial pick;
//! full results remain in disjoint partition pieces for parallel ordering and typed output.
const std = @import("std");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");

/// `worse` orders the losing row first, including the original-ordinal tie rule.
pub fn Top(
    comptime Group: type,
    comptime Context: type,
    comptime worse: fn (Context, Group, Group) std.math.Order,
) type {
    const Heap = std.PriorityQueue(Group, Context, worse);
    return struct {
        const Self = @This();
        gpa: std.mem.Allocator,
        ledger: *memory.SharedBudget,
        charged: usize,
        receivers: []Receiver = &.{},
        sealed: bool = false,

        pub const Receiver = struct {
            heap: Heap,
            k: usize,
            allocator: std.mem.Allocator,

            fn offer(r: *Receiver, g: Group) void {
                if (r.heap.items.len < r.k) {
                    r.heap.push(r.allocator, g) catch unreachable;
                } else if (worse(r.heap.context, g, r.heap.peek().?) == .gt) {
                    _ = r.heap.pop();
                    r.heap.push(r.allocator, g) catch unreachable;
                }
            }

            pub fn receive(r: *Receiver, _: usize, groups: []const Group) !void {
                for (groups) |g| r.offer(g);
            }
        };

        pub fn bytes(workers: usize, k: usize) usize {
            return workers *| (@sizeOf(Receiver) +| k *| @sizeOf(Group));
        }

        pub fn init(a: std.mem.Allocator, ledger: *memory.SharedBudget, workers: usize, k: usize, ctx: Context) !Self {
            if (workers == 0 or workers > 64 or k == 0 or k > 1024) return error.BadPartial;
            const quote = bytes(workers, k);
            _ = ledger.drawExact(quote) orelse return error.ExceededMemoryBudget;
            var s: Self = .{ .gpa = a, .ledger = ledger, .charged = quote };
            errdefer s.deinit();
            s.receivers = try a.alloc(Receiver, workers);
            for (s.receivers) |*r| r.* = .{ .heap = .initContext(ctx), .k = k, .allocator = a };
            for (s.receivers) |*r| {
                const storage = try a.alloc(Group, k);
                r.heap.items = storage[0..0];
                r.heap.cap = k;
            }
            return s;
        }

        pub fn deinit(s: *Self) void {
            for (s.receivers) |*r| r.heap.deinit(s.gpa);
            s.gpa.free(s.receivers);
            s.ledger.release(s.charged);
            s.receivers = &.{};
            s.charged = 0;
        }

        /// Each worker already saw complete owner groups. The remaining work is bounded by workers * H.
        pub fn finish(s: *Self) []const Group {
            const first = &s.receivers[0];
            if (s.sealed) return first.heap.items;
            for (s.receivers[1..]) |r| for (r.heap.items) |g| first.offer(g);
            std.mem.sortUnstable(Group, first.heap.items, first.heap.context, struct {
                fn less(ctx: Context, a: Group, b: Group) bool {
                    return worse(ctx, a, b) == .gt;
                }
            }.less);
            s.sealed = true;
            return first.heap.items;
        }
    };
}

pub fn Full(comptime Group: type) type {
    return struct {
        const Self = @This();
        pub const Piece = struct { groups: []Group = &.{} };
        pub const Receiver = struct {
            state: *Self,

            pub fn receive(r: *Receiver, p: usize, groups: []const Group) !void {
                const s = r.state;
                if (p >= s.pieces.len or s.pieces[p].groups.len != 0) return error.BadPartial;
                const charge = groups.len *| @sizeOf(Group);
                if (charge == std.math.maxInt(usize)) return error.ExceededMemoryBudget;
                _ = s.ledger.drawExact(charge) orelse return error.ExceededMemoryBudget;
                const owned = s.gpa.dupe(Group, groups) catch |err| {
                    s.ledger.release(charge);
                    return err;
                };
                // One fold task owns p; retained credit is shared across the disjoint partition writers.
                s.pieces[p].groups = owned;
                _ = s.retained.fetchAdd(charge, .monotonic);
            }
        };
        gpa: std.mem.Allocator,
        ledger: *memory.SharedBudget,
        charged: usize,
        retained: std.atomic.Value(usize) = .init(0),
        pieces: []Piece = &.{},
        receivers: []Receiver = &.{},

        pub fn bytes(workers: usize, parts: usize) usize {
            return @sizeOf(Self) +| parts *| @sizeOf(Piece) +| workers *| @sizeOf(Receiver);
        }

        pub fn init(a: std.mem.Allocator, ledger: *memory.SharedBudget, workers: usize, parts: usize) !*Self {
            if (workers == 0 or workers > 64 or parts < 2 or parts > 16384) return error.BadPartial;
            const quote = bytes(workers, parts);
            _ = ledger.drawExact(quote) orelse return error.ExceededMemoryBudget;
            const s = a.create(Self) catch |e| {
                ledger.release(quote);
                return e;
            };
            s.* = .{ .gpa = a, .ledger = ledger, .charged = quote };
            errdefer s.deinit();
            s.pieces = try a.alloc(Piece, parts);
            @memset(s.pieces, .{});
            s.receivers = try a.alloc(Receiver, workers);
            for (s.receivers) |*r| r.* = .{ .state = s };
            return s;
        }

        pub fn deinit(s: *Self) void {
            for (s.pieces) |p| s.gpa.free(p.groups);
            s.gpa.free(s.pieces);
            s.gpa.free(s.receivers);
            s.ledger.release(s.charged + s.retained.load(.monotonic));
            s.gpa.destroy(s);
        }

        /// The only serial enumeration is over partition metadata, never the groups themselves.
        pub fn count(s: *const Self) usize {
            var n: usize = 0;
            for (s.pieces) |p| n += p.groups.len;
            return n;
        }

        pub fn at(s: *const Self, index: u64) *const Group {
            return &s.pieces[index >> 32].groups[@as(u32, @truncate(index))];
        }

        /// The caller funds output storage and worker stacks, as for typed-column fill. Every group reference is
        /// written by a partition task; serial prefix work is bounded by the fixed partition metadata.
        pub fn fill(
            s: *const Self,
            comptime Ref: type,
            output: []Ref,
            comptime make: fn (Group, u64) Ref,
        ) !void {
            if (output.len != s.count()) return error.BadPartial;
            const Task = struct {
                state: *const Self,
                output: []Ref,
                offsets: []const usize,
                cursor: *std.atomic.Value(usize),
                fn run(t: *@This()) void {
                    while (true) {
                        const p = t.cursor.fetchAdd(1, .monotonic);
                        if (p >= t.state.pieces.len) return;
                        for (t.state.pieces[p].groups, 0..) |g, i| {
                            const index = @as(u64, p) << 32 | @as(u32, @intCast(i));
                            t.output[t.offsets[p] + i] = make(g, index);
                        }
                    }
                }
            };
            const charge = s.pieces.len *| @sizeOf(usize) +| s.receivers.len *| @sizeOf(Task);
            _ = s.ledger.drawExact(charge) orelse return error.ExceededMemoryBudget;
            defer s.ledger.release(charge);
            const offsets = try s.gpa.alloc(usize, s.pieces.len);
            defer s.gpa.free(offsets);
            var position: usize = 0;
            for (s.pieces, offsets) |p, *offset| {
                offset.* = position;
                position += p.groups.len;
            }
            const tasks = try s.gpa.alloc(Task, s.receivers.len);
            defer s.gpa.free(tasks);
            var cursor: std.atomic.Value(usize) = .init(0);
            for (tasks) |*t| t.* = .{ .state = s, .output = output, .offsets = offsets, .cursor = &cursor };
            if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = 1 << 20 }, Task.run))
                Task.run(&tasks[0]);
        }
    };
}

fn resultTrial(a: std.mem.Allocator, workers: usize) !void {
    const L = @import("owner_lanes.zig").Layout(u128, &.{.sum}, u64);
    const O = @import("owner_packed.zig").Owner(u128, &.{.sum}, u64);
    const Compare = struct {
        fn worse(desc: bool, x: O.Group, y: O.Group) std.math.Order {
            const xv = x.integer(0);
            const yv = y.integer(0);
            if (xv == null and yv != null) return .lt;
            if (xv != null and yv == null) return .gt;
            if (xv != null and xv.? != yv.?)
                return if (desc) std.math.order(xv.?, yv.?) else std.math.order(yv.?, xv.?);
            return std.math.order(y.first, x.first);
        }
    };
    const T = Top(O.Group, bool, Compare.worse);
    var ledger: memory.SharedBudget = .{ .limit = 256 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const s = (try O.State.init(a, &ledger, @as(usize, 1) << 41, 5000, workers, 64)).?;
    defer s.deinit();
    var truth: [47]?O.Group = @splat(null);
    for (0..5000) |i| {
        const id = i % truth.len;
        const record: L.Record = .{
            .key = @as(u128, id) << 80,
            .ordinal = (@as(u64, 1) << 40) + 4999 - i,
            .weight = 1,
            .values = .{if (id % 3 == 0) -7 else 11},
            .argument_nulls = @intFromBool(id % 7 == 0),
        };
        try s.locals[i % workers].append(record);
        if (truth[id]) |*g| try g.add(record) else truth[id] = try O.Group.init(record);
    }
    var top = try T.init(a, &ledger, workers, 10, true);
    defer top.deinit();
    try s.finish(top.receivers, T.Receiver.receive);
    var wanted: [47]O.Group = undefined;
    for (truth, &wanted) |g, *w| w.* = g.?;
    std.mem.sort(O.Group, &wanted, {}, struct {
        fn less(_: void, x: O.Group, y: O.Group) bool {
            return Compare.worse(true, x, y) == .gt;
        }
    }.less);
    const got = top.finish();
    try std.testing.expectEqualDeep(wanted[0..10], got);
    const full = try Full(O.Group).init(a, &ledger, workers, 64);
    defer full.deinit();
    try s.finish(full.receivers, Full(O.Group).Receiver.receive);
    try std.testing.expectEqual(@as(usize, 47), full.count());
    const Ref = struct { first: u64, index: u64 };
    var refs: [47]Ref = undefined;
    try full.fill(Ref, &refs, struct {
        fn make(g: O.Group, index: u64) Ref {
            return .{ .first = g.first, .index = index };
        }
    }.make);
    var found: [47]bool = @splat(false);
    for (refs) |ref| {
        const g = full.at(ref.index).*;
        const id: usize = @intCast(g.key >> 80);
        try std.testing.expect(!found[id]);
        found[id] = true;
        try std.testing.expectEqual(g.first, ref.first);
        try std.testing.expectEqualDeep(truth[id].?, g);
    }
}

test "owner result parallel partition receivers retain exact top-H and full groups through allocation failures" {
    for ([_]usize{ 1, 3, 16 }) |workers| try resultTrial(std.testing.allocator, workers);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, resultTrial, .{1});
}
