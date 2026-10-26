//! Packed-key owners use the same cache, owned scatter and exact fold as the legacy record adapters.
const std = @import("std");
const lanes = @import("owner_lanes.zig");
const cache = @import("owner_cache.zig");
const records = @import("owner_records.zig");
const memory = @import("memory.zig");
const spawn = @import("spawn.zig");
const hw = @import("hw.zig");
const stack_size = 1 << 20;

pub fn Owner(comptime Bits: type, comptime kinds: []const lanes.Lane, comptime Ordinal: type) type {
    const L = lanes.Layout(Bits, kinds, Ordinal);
    return Framework(L, Ordinal, L.hashRecord);
}

/// DISTINCT pairs specialize the identity policy and partition hash, retaining this same producer/fold machinery.
pub fn Framework(comptime L: type, comptime Ordinal: type, comptime partitionHash: anytype) type {
    const Scatter = records.Writer(L.Record, Ordinal);
    const Cache = cache.Local(L.CachePolicy);
    const Fold = @import("owner_fold.zig").Table(L);
    return struct {
        pub const Record = L.Record;
        pub const Group = L.Group;

        pub const Local = struct {
            state: *State,
            frontier: Scatter.Frontier = .{},
            hot: Cache = .{},
            accepted: usize = 0,
            seen: usize = 0,
            hits: usize = 0,
            bypass: bool = false,

            pub fn start(l: *Local) void {
                l.seen = 0;
                l.hits = 0;
                l.bypass = false;
                l.hot.last = null;
            }

            pub inline fn append(l: *Local, r: Record) !void {
                try L.validate(r);
                if (L.ordinal(r) >= l.state.input_rows or r.weight > l.state.weight_rows -| l.accepted)
                    return error.BadPartial;
                if (!l.bypass and l.hot.entries.len != 0) {
                    const trial = l.seen < l.state.trial_rows;
                    if (trial) l.seen += 1;
                    const hit = try l.hot.push(r, l, emit);
                    if (trial and hit) l.hits += 1;
                    if (trial and l.seen == l.state.trial_rows and l.hits < l.hot.entries.len) {
                        try l.hot.flush(l, emit);
                        l.bypass = true;
                    }
                } else try emit(l, r);
                l.accepted += @intCast(r.weight);
            }

            inline fn emit(l: *Local, r: Record) !void {
                const p: usize = @intCast(partitionHash(r) >> l.state.shift);
                try Scatter.append(&l.frontier, l.state.blocks, &l.state.cursor, p, r);
            }
        };

        pub const State = struct {
            gpa: std.mem.Allocator,
            ledger: *memory.SharedBudget,
            charged: usize,
            input_rows: usize,
            record_rows: usize,
            weight_rows: usize,
            parts: usize,
            shift: u6,
            trial_rows: usize,
            locals: []Local = &.{},
            blocks: []Scatter.Block = &.{},
            entries: []Record = &.{},
            cursor: std.atomic.Value(u32) = .init(0),
            const TaskStorage = struct {
                state: *State,
                cursor: *std.atomic.Value(usize),
                receiver: *anyopaque,
                err: ?anyerror,
            };

            /// Identity, decoded columns and retained answers are quoted by their owners. Records are funded before
            /// decode; the finish draws its exact concurrently live histogram bound before allocating any table.
            pub fn charge(record_rows: usize, workers: usize, parts: usize) usize {
                return Scatter.blockCount(record_rows, workers, parts) *| @sizeOf(Scatter.Block) +|
                    Scatter.frontierBytes(workers, parts) +|
                    workers *| cache.capacity(@sizeOf(Record)) *| @sizeOf(Record) +|
                    workers *| (stack_size +| @sizeOf(Local) +| @sizeOf(TaskStorage)) +| @sizeOf(State);
            }

            pub fn init(
                a: std.mem.Allocator,
                ledger: *memory.SharedBudget,
                input_rows: usize,
                record_rows: usize,
                workers: usize,
                parts: usize,
            ) !?*State {
                return initWeighted(a, ledger, input_rows, record_rows, record_rows, workers, parts);
            }

            /// A completed pair emits one physical COUNT contribution but can carry many duplicate rows for
            /// count(*). The caller proves the contribution bound separately from its total weight/ordinal bound.
            pub fn initWeighted(
                a: std.mem.Allocator,
                ledger: *memory.SharedBudget,
                input_rows: usize,
                weight_rows: usize,
                record_rows: usize,
                workers: usize,
                parts: usize,
            ) !?*State {
                if (workers == 0 or workers > 64 or parts < 2 or parts > 16384 or
                    !std.math.isPowerOfTwo(parts) or record_rows > weight_rows or weight_rows > input_rows) return null;
                if (!L.admitsRows(input_rows)) return null;
                const blocks = Scatter.blockCount(record_rows, workers, parts);
                if (blocks >= std.math.maxInt(u32) - workers * 128 or record_rows > std.math.maxInt(Ordinal))
                    return null;
                const quote = charge(record_rows, workers, parts);
                if (quote == std.math.maxInt(usize)) return null;
                _ = ledger.drawExact(quote) orelse return null;
                const s = a.create(State) catch |e| {
                    ledger.release(quote);
                    return e;
                };
                const slots = cache.capacity(@sizeOf(Record));
                s.* = .{
                    .gpa = a,
                    .ledger = ledger,
                    .charged = quote,
                    .input_rows = input_rows,
                    .record_rows = record_rows,
                    .weight_rows = weight_rows,
                    .parts = parts,
                    .shift = @intCast(@as(usize, 64) - std.math.log2_int(usize, parts)),
                    .trial_rows = cache.trialRows(slots, @sizeOf(Record)),
                };
                errdefer s.deinit();
                s.locals = try a.alloc(Local, workers);
                for (s.locals) |*l| l.* = .{ .state = s };
                s.entries = try a.alloc(Record, workers * slots);
                // Only weight is read before initialization. Avoid touching unused lane/identity cache lines.
                for (s.entries) |*r| r.weight = 0;
                for (s.locals, 0..) |*l, i| {
                    l.hot.entries = s.entries[i * slots ..][0..slots];
                    l.frontier = try Scatter.Frontier.init(a, parts);
                }
                s.blocks = try a.alloc(Scatter.Block, blocks);
                return s;
            }

            pub fn deinit(s: *State) void {
                for (s.locals) |l| l.frontier.deinit(s.gpa);
                s.gpa.free(s.locals);
                s.gpa.free(s.entries);
                s.gpa.free(s.blocks);
                s.ledger.release(s.charged);
                s.gpa.destroy(s);
            }

            /// Each receiver is private to a worker. Consume a complete partition before its table is reused;
            /// retain full groups/top-H or scatter a DISTINCT reduction, publishing output only after success.
            pub fn finish(s: *State, receivers: anytype, comptime receive: anytype) !void {
                std.debug.assert(receivers.len == s.locals.len);
                var accepted: usize = 0;
                for (s.locals) |*l| {
                    try l.hot.flush(l, Local.emit);
                    accepted = std.math.add(usize, accepted, l.accepted) catch return error.BadPartial;
                }
                if (accepted > s.weight_rows) return error.BadPartial;
                if (s.weight_rows != s.record_rows) {
                    var physical: usize = 0;
                    for (s.locals) |l| {
                        for (l.frontier.histogram) |n| physical += n;
                    }
                    if (physical > s.record_rows) return error.BadPartial;
                }
                const scratch = Fold.concurrentBytes(s, s.parts, s.locals.len, partitionRows);
                if (scratch == std.math.maxInt(usize)) return error.ExceededMemoryBudget;
                // Refusal discards private records and requests the established stable-input replay, before any
                // receiver can publish a group. Never reserve all raw rows as concurrently live fold tables.
                _ = s.ledger.drawExact(scratch) orelse return error.ExceededMemoryBudget;
                defer s.ledger.release(scratch);
                const Receiver = @TypeOf(receivers[0]);
                const Task = struct {
                    state: *State,
                    cursor: *std.atomic.Value(usize),
                    receiver: *Receiver,
                    err: ?anyerror = null,
                    fn run(t: *@This()) void {
                        var table: Fold = .{};
                        defer table.deinit(t.state.gpa);
                        while (true) {
                            const p = t.cursor.fetchAdd(1, .monotonic);
                            if (p >= t.state.parts) return;
                            t.partition(&table, p) catch |e| {
                                t.err = e;
                                return;
                            };
                        }
                    }
                    fn partition(t: *@This(), table: *Fold, p: usize) !void {
                        const a = t.state.gpa;
                        const rows = partitionRows(t.state, p);
                        if (rows == 0) return;
                        const l2 = hw.l2BytesPerWorkerOr(640 << 10);
                        try table.reset(a, rows, @max(1, l2 / (@sizeOf(Group) + 16)));
                        for (t.state.locals) |l| {
                            var id = l.frontier.heads[p];
                            while (id != records.none) {
                                const b = &t.state.blocks[id];
                                if (b.next != records.none)
                                    @prefetch(&t.state.blocks[b.next], .{ .rw = .read, .locality = 1 });
                                for (b.rows[0..b.len]) |r| try table.append(a, r);
                                id = b.next;
                            }
                        }
                        try receive(t.receiver, p, table.items());
                    }
                };
                comptime std.debug.assert(@sizeOf(Task) == @sizeOf(TaskStorage));
                const tasks = try s.gpa.alloc(Task, s.locals.len);
                defer s.gpa.free(tasks);
                var cursor: std.atomic.Value(usize) = .init(0);
                for (tasks, receivers) |*t, *receiver| t.* = .{ .state = s, .cursor = &cursor, .receiver = receiver };
                if (tasks.len == 1 or !spawn.runTree(Task, tasks, .{ .stack_size = stack_size }, Task.run))
                    Task.run(&tasks[0]);
                for (tasks) |t| if (t.err) |e| return e;
            }

            fn partitionRows(s: *const State, p: usize) usize {
                var n: usize = 0;
                for (s.locals) |l| n += @intCast(l.frontier.histogram[p]);
                return n;
            }
        };
    };
}

fn packedTrial(a: std.mem.Allocator, workers: usize) !void {
    const O = Owner(u128, &.{ .sum, .min, .max, .present_count }, u64);
    var budget: memory.SharedBudget = .{ .limit = 256 << 20 };
    defer std.debug.assert(budget.used.load(.monotonic) == 0);
    {
        const s = (try O.State.init(a, &budget, 1 << 41, 4096, workers, 64)).?;
        defer s.deinit();
        for (0..4096) |i| {
            const id = i % 17;
            const l = &s.locals[(i / 31) % workers];
            if (i % 31 == 0) l.start();
            try l.append(.{
                .key = @as(u128, id) << 64 | std.math.maxInt(u64),
                .ordinal = (1 << 40) + 4095 - i,
                .weight = 1,
                .values = .{ -7, -7, -7, 1 },
                .argument_nulls = if (id == 0) 15 else 0,
            });
        }
        const Receiver = struct {
            groups: [17]?O.Group = @splat(null),
            fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
                for (groups) |g| {
                    const id: usize = @intCast(g.key >> 64);
                    try std.testing.expect(r.groups[id] == null);
                    r.groups[id] = g;
                }
            }
        };
        const receivers = try a.alloc(Receiver, workers);
        defer a.free(receivers);
        @memset(receivers, .{});
        try s.finish(receivers, Receiver.receive);
        for (0..17) |id| {
            var got: ?O.Group = null;
            for (receivers) |r| if (r.groups[id]) |g| {
                try std.testing.expect(got == null);
                got = g;
            };
            const g = got orelse return error.TestUnexpectedResult;
            const count: usize = @as(usize, 4096 / 17) + @intFromBool(id < 4096 % 17);
            try std.testing.expectEqual(@as(u64, count), g.weight);
            try std.testing.expectEqual(@as(u64, (1 << 40) + (4095 - id) % 17), g.first);
            try std.testing.expectEqual(
                if (id == 0) @as(?i128, null) else -7 * @as(i128, @intCast(count)),
                g.integer(0),
            );
            try std.testing.expectEqual(if (id == 0) @as(?i128, 0) else count, g.integer(3));
        }
    }
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "packed owners share exact weighted lanes, original wide ordinals and parallel finishes" {
    for ([_]usize{ 1, 3, 16 }) |workers| try packedTrial(std.testing.allocator, workers);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, packedTrial, .{1});
}

test "packed owner shared cursor finishes every partition despite failed thread startup" {
    defer spawn.resetFailure();
    for (0..4) |n| {
        spawn.injectFailureAfter(n);
        try packedTrial(std.testing.allocator, 4);
        try std.testing.expect(spawn.failuresInjected() != 0);
    }
}

test "packed owner exact finish funding refuses before publishing and releases every phase charge" {
    const O = Owner(u64, &.{}, u32);
    var ledger: memory.SharedBudget = .{ .limit = 128 << 20 };
    const s = (try O.State.init(std.testing.allocator, &ledger, 4096, 4096, 1, 64)).?;
    for (0..4096) |i| try s.locals[0].append(.{ .key = i % 17, .ordinal = @intCast(i), .weight = 1 });
    const Receiver = struct {
        groups: usize = 0,
        fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
            r.groups += groups.len;
        }
    };
    var receivers = [_]Receiver{.{}};
    ledger.limit = s.charged;
    try std.testing.expectError(error.ExceededMemoryBudget, s.finish(&receivers, Receiver.receive));
    try std.testing.expectEqual(@as(usize, 0), receivers[0].groups);
    try std.testing.expectEqual(s.charged, ledger.used.load(.monotonic));
    ledger.limit = 128 << 20;
    try s.finish(&receivers, Receiver.receive);
    try std.testing.expectEqual(@as(usize, 17), receivers[0].groups);
    try std.testing.expectEqual(s.charged, ledger.used.load(.monotonic));
    s.deinit();
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}
