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
    // Equal row capacity also shrinks each reserved producer/partition tail; equal byte-size blocks would
    // erase the payload saving at high partition counts before any input was decoded.
    const Small = records.WriterRows(lanes.SharedRecord, Ordinal, Scatter.block_rows);
    const small_eligible = if (@hasDecl(L, "sharedEligible")) L.sharedEligible else false;
    const Cache = cache.Local(L.CachePolicy);
    const Fold = @import("owner_fold.zig").Table(L);
    return struct {
        pub const Record = L.Record;
        pub const Group = L.Group;

        const SmallStorage = struct {
            frontiers: []Small.Frontier = &.{},
            blocks: []Small.Block = &.{},
            cursor: std.atomic.Value(u32) = .init(0),

            fn deinit(s: *@This(), a: std.mem.Allocator) void {
                for (s.frontiers) |f| f.deinit(a);
                a.free(s.frontiers);
                a.free(s.blocks);
                a.destroy(s);
            }
        };

        pub const Local = struct {
            state: *State,
            frontier: Scatter.Frontier = .{},
            hot: Cache = .{},
            small: ?*Small.Frontier = null,
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
                try l.appendOwned(r);
                l.accepted += @intCast(r.weight);
            }

            /// False leaves the owner untouched for its existing row fallback. Errors invalidate this private
            /// attempt, as an append/scatter failure already does; no partial window can publish an answer.
            pub fn appendWindow(l: *Local, images: anytype, first: u64, off: usize, payload: anytype) !bool {
                if (images.len == 0) return true;
                if (first >= l.state.input_rows or images.len > l.state.input_rows - @as(usize, @intCast(first)) or
                    images.len > l.state.weight_rows -| l.accepted) return error.BadPartial;
                const window = (try payload.requiredWindow(off, images.len)) orelse return false;
                switch (window) {
                    inline else => |values, tag| {
                        if (l.bypass)
                            try l.appendWindowRows(true, tag, values, images, first)
                        else
                            try l.appendWindowRows(false, tag, values, images, first);
                    },
                }
                l.accepted += images.len;
                return true;
            }

            inline fn appendWindowRows(
                l: *Local,
                comptime direct: bool,
                comptime tag: anytype,
                values: anytype,
                images: anytype,
                first: u64,
            ) !void {
                for (images, 0..) |key, i| {
                    // The window proved the entire ordinal span, code range and all-present payload before mutation.
                    const r = checked: {
                        @setRuntimeSafety(false);
                        const value = switch (tag) {
                            .count_only => 0,
                            .plain => values[i],
                            .coded => values.dict[values.codes[i]],
                        };
                        break :checked L.requiredRecord(key, @intCast(first + i), value);
                    };
                    if (direct) try emit(l, r) else try l.appendOwned(r);
                }
            }

            inline fn appendOwned(l: *Local, r: Record) !void {
                if (!l.bypass and l.hot.entries.len != 0) {
                    const trial = l.seen < l.state.trial_rows;
                    if (trial) l.seen += 1;
                    const hit = try l.hot.push(r, l, emit);
                    if (trial and hit) l.hits += 1;
                    // Wide lane records save more scatter/fold traffic per hit than a bare COUNT partial.
                    const count_bytes = @sizeOf(lanes.Layout(u64, &.{}, u32).Record);
                    const saved_bytes = if (@hasField(Record, "values")) @sizeOf(Record) else count_bytes;
                    if (trial and l.seen == l.state.trial_rows and
                        l.hits *| saved_bytes < l.hot.entries.len *| count_bytes)
                    {
                        try l.hot.flush(l, emit);
                        l.bypass = true;
                    }
                } else try emit(l, r);
            }

            inline fn emit(l: *Local, r: Record) !void {
                const p: usize = @intCast(partitionHash(r) >> l.state.shift);
                if (small_eligible) if (l.small) |frontier| if (r.weight == 1) {
                    const s = l.state.small.?;
                    try Small.append(frontier, s.blocks, &s.cursor, p, try L.compressSingleton(r));
                    return;
                };
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
            small: ?*SmallStorage = null,
            const TaskStorage = struct {
                state: *State,
                cursor: *std.atomic.Value(usize),
                receiver: *anyopaque,
                err: ?anyerror,
            };

            /// Identity, decoded columns and retained answers are quoted by their owners. Records are funded before
            /// decode; the finish draws its exact concurrently live histogram bound before allocating any table.
            pub fn charge(record_rows: usize, workers: usize, parts: usize) usize {
                return chargeStorage(record_rows, workers, parts, false);
            }

            pub fn chargeStorage(record_rows: usize, workers: usize, parts: usize, compact: bool) usize {
                if (compact and !small_eligible) return std.math.maxInt(usize);
                const extra = if (compact) Small.blockCount(record_rows, workers, parts) *| @sizeOf(Small.Block) +|
                    Small.frontierBytes(workers, parts) +| workers *| @sizeOf(Small.Frontier) +|
                    @sizeOf(SmallStorage) else 0;
                // In compact mode every wide record owns >=2 disjoint original rows. Singles use the other list;
                // retaining weighted lanes verbatim avoids inventing a shared v for different SUM/MIN/MAX values.
                const wide_rows = if (compact) record_rows / 2 else record_rows;
                return extra +| Scatter.blockCount(wide_rows, workers, parts) *| @sizeOf(Scatter.Block) +|
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

            pub fn initStorage(
                a: std.mem.Allocator,
                ledger: *memory.SharedBudget,
                input_rows: usize,
                record_rows: usize,
                workers: usize,
                parts: usize,
                compact: bool,
            ) !?*State {
                return initWeightedStorage(a, ledger, input_rows, record_rows, record_rows, workers, parts, compact);
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
                return initWeightedStorage(a, ledger, input_rows, weight_rows, record_rows, workers, parts, false);
            }

            fn initWeightedStorage(
                a: std.mem.Allocator,
                ledger: *memory.SharedBudget,
                input_rows: usize,
                weight_rows: usize,
                record_rows: usize,
                workers: usize,
                parts: usize,
                compact: bool,
            ) !?*State {
                if (workers == 0 or workers > 64 or parts < 2 or parts > 16384 or
                    !std.math.isPowerOfTwo(parts) or record_rows > weight_rows or weight_rows > input_rows) return null;
                if (compact and (!small_eligible or record_rows != weight_rows)) return null;
                if (!L.admitsRows(input_rows)) return null;
                const blocks = Scatter.blockCount(if (compact) record_rows / 2 else record_rows, workers, parts);
                if (blocks >= std.math.maxInt(u32) - workers * 128 or record_rows > std.math.maxInt(Ordinal))
                    return null;
                const small_blocks = if (compact) Small.blockCount(record_rows, workers, parts) else 0;
                if (small_blocks >= std.math.maxInt(u32) - workers * 128) return null;
                const quote = chargeStorage(record_rows, workers, parts, compact);
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
                if (compact) {
                    const extra = try a.create(SmallStorage);
                    extra.* = .{};
                    s.small = extra;
                    extra.frontiers = try a.alloc(Small.Frontier, workers);
                    @memset(extra.frontiers, .{});
                    for (extra.frontiers, s.locals) |*f, *l| {
                        f.* = try Small.Frontier.init(a, parts);
                        l.small = f;
                    }
                    extra.blocks = try a.alloc(Small.Block, small_blocks);
                }
                s.blocks = try a.alloc(Scatter.Block, blocks);
                return s;
            }

            pub fn deinit(s: *State) void {
                if (s.small) |extra| extra.deinit(s.gpa);
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
                        if (l.small) |f| for (f.histogram) |n| {
                            physical += n;
                        };
                    }
                    // A weighted reduction may reserve from speculative footer bounds (for example DISTINCT
                    // NULL-presence rows). Underestimation is a private-attempt budget refusal, not a malformed
                    // decoded partial: replay without the owner before any receiver publishes output.
                    if (physical > s.record_rows) return error.ExceededMemoryBudget;
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
                            if (small_eligible) if (l.small) |f| {
                                const extra = t.state.small.?;
                                id = f.heads[p];
                                while (id != records.none) {
                                    const b = &extra.blocks[id];
                                    if (b.next != records.none)
                                        @prefetch(&extra.blocks[b.next], .{ .rw = .read, .locality = 1 });
                                    for (b.rows[0..b.len]) |r| try table.append(a, try L.expandSingleton(r));
                                    id = b.next;
                                }
                            };
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
                if (s.small) |extra| for (extra.frontiers) |f| {
                    n += @intCast(f.histogram[p]);
                };
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

test "wide payload cache retains a useful trial below the bare COUNT hit threshold" {
    const a = std.testing.allocator;
    const O = Owner(u64, &.{ .sum, .min, .max, .present_count }, u32);
    var ledger: memory.SharedBudget = .{ .limit = 64 << 20 };
    const state = (try O.State.init(a, &ledger, 65536, 65536, 1, 64)).?;
    defer state.deinit();
    const local = &state.locals[0];
    const n = state.trial_rows;
    for (0..n) |i| try local.append(.{
        .key = @intCast(if (i % 4 == 3) i - 1 else i),
        .ordinal = @intCast(i),
        .weight = 1,
        .values = .{ 7, 7, 7, 1 },
    });
    try std.testing.expectEqual(n / 4, local.hits);
    try std.testing.expect(local.hits < local.hot.entries.len);
    try std.testing.expect(!local.bypass);
}

test "owner window append keeps row records, cache trials and exact wide results for plain and coded payloads" {
    const a = std.testing.allocator;
    const ast = @import("expr/ast.zig");
    const Call = @import("expr/agg.zig").AggCall;
    const Program = @import("agg_plan.zig").OwnerProgram;
    const Lane = lanes.Lane;
    const kinds = &.{ Lane.sum, Lane.min, Lane.max, Lane.present_count };
    const L = lanes.Layout(u64, kinds, u32);
    const O = Owner(u64, kinds, u32);
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]Call{
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = arg, .where = null },
        .{ .func = .min, .alias = "lo", .result = .i64, .arg = arg, .where = null },
        .{ .func = .max, .alias = "hi", .result = .i64, .arg = arg, .where = null },
        .{ .func = .count, .alias = "n", .result = .i64, .arg = arg, .where = null },
    };
    const program = Program.init(&calls).?;
    const count = 4096;
    var keys: [count]u64 = undefined;
    var values: [count]i64 = undefined;
    var codes: [count]u32 = undefined;
    const dict = [_]i64{ std.math.minInt(i64), 11, std.math.maxInt(i64) };
    var levels: [count]u32 = @splat(1);
    for (0..count) |i| {
        keys[i] = std.math.maxInt(u64) - i % 17;
        codes[i] = @intCast(i % dict.len);
        values[i] = dict[codes[i]];
    }
    const Receiver = struct {
        groups: [17]?O.Group = @splat(null),
        fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
            for (groups) |g| {
                const id: usize = @intCast(std.math.maxInt(u64) - g.key);
                try std.testing.expect(r.groups[id] == null);
                r.groups[id] = g;
            }
        }
    };
    for ([_]usize{ 1, 4 }) |workers| for ([_]bool{ false, true }) |coded| {
        for ([_]bool{ false, true }) |direct| {
            var budget: memory.SharedBudget = .{ .limit = 128 << 20 };
            {
                const old = (try O.State.init(a, &budget, 1000 + count, count, workers, 64)).?;
                defer old.deinit();
                const bulk = (try O.State.init(a, &budget, 1000 + count, count, workers, 64)).?;
                defer bulk.deinit();
                for (old.locals, bulk.locals) |*x, *y| {
                    x.bypass = direct;
                    y.bypass = direct;
                }
                const batch: @import("filter/eval.zig").Batch = .{ .num_rows = count, .cols = &.{.{ .i64 = .{
                    .values = if (coded) &.{} else &values,
                    .codes = if (coded) &codes else null,
                    .dict = if (coded) &dict else &.{},
                    .def_levels = &levels,
                    .max_def = 1,
                } }} };
                const payload = try L.bind(program, &batch, &.{0});
                var off: usize = 0;
                while (off < count) {
                    const n = @min(count - off, ([_]usize{ 17, 1, 64, 3 })[off % 4]);
                    const wi = off / 64 % workers;
                    for (off..off + n) |i| try old.locals[wi].append(try payload.record(keys[i], i, 1000 + i));
                    try std.testing.expect(try bulk.locals[wi].appendWindow(
                        keys[off..][0..n],
                        1000 + off,
                        off,
                        &payload,
                    ));
                    off += n;
                }
                for (old.locals, bulk.locals) |x, y| {
                    try std.testing.expectEqual(x.accepted, y.accepted);
                    try std.testing.expectEqual(x.bypass, y.bypass);
                    try std.testing.expectEqual(x.seen, y.seen);
                    try std.testing.expectEqual(x.hits, y.hits);
                }
                const x = try a.alloc(Receiver, workers);
                defer a.free(x);
                const y = try a.alloc(Receiver, workers);
                defer a.free(y);
                @memset(x, .{});
                @memset(y, .{});
                try old.finish(x, Receiver.receive);
                try bulk.finish(y, Receiver.receive);
                for (0..17) |key| {
                    var want: ?O.Group = null;
                    var got: ?O.Group = null;
                    for (x) |r| if (r.groups[key]) |g| {
                        want = g;
                    };
                    for (y) |r| if (r.groups[key]) |g| {
                        got = g;
                    };
                    try std.testing.expect(want != null and got != null);
                    try std.testing.expectEqualDeep(want, got);
                }
            }
            try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        }
    };
}

test "owner window validates entire code, ordinal and weight spans before mutating and declines NULL poison" {
    const a = std.testing.allocator;
    const ast = @import("expr/ast.zig");
    const L = lanes.Layout(u64, &.{.sum}, u32);
    const O = Owner(u64, &.{.sum}, u32);
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]@import("expr/agg.zig").AggCall{
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = arg, .where = null },
    };
    const p = @import("agg_plan.zig").OwnerProgram.init(&calls).?;
    var levels = [_]u32{ 1, 1, 1 };
    const cols = [_]@import("filter/eval.zig").Batch.Column{.{ .i64 = .{
        .values = &.{},
        .codes = &.{ 0, 0, 999 },
        .dict = &.{7},
        .def_levels = &levels,
        .max_def = 1,
    } }};
    const batch: @import("filter/eval.zig").Batch = .{ .num_rows = 3, .cols = &cols };
    const payload = try L.bind(p, &batch, &.{0});
    var ledger: memory.SharedBudget = .{ .limit = 32 << 20 };
    {
        const s = (try O.State.init(a, &ledger, 3, 3, 1, 64)).?;
        defer s.deinit();
        const keys = [_]u64{ 9, 9, 9 };
        try std.testing.expectError(error.BadPartial, s.locals[0].appendWindow(&keys, 1, 0, &payload));
        try std.testing.expectError(error.BadColumn, s.locals[0].appendWindow(&keys, 0, 0, &payload));
        try std.testing.expectEqual(@as(usize, 0), s.locals[0].accepted);
        try std.testing.expectEqual(@as(u32, 0), s.cursor.load(.monotonic));
        levels[2] = 0;
        try std.testing.expect(!try s.locals[0].appendWindow(&keys, 0, 0, &payload));
        try std.testing.expectEqual(@as(usize, 0), s.locals[0].accepted);
        for (keys, 0..) |key, i| try s.locals[0].append(try payload.record(key, i, i));
        try std.testing.expectEqual(@as(usize, 3), s.locals[0].accepted);
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "owner window count-only program needs no values and retains contiguous first ordinals" {
    const a = std.testing.allocator;
    const L = lanes.Layout(u64, &.{}, u32);
    const O = Owner(u64, &.{}, u32);
    var ledger: memory.SharedBudget = .{ .limit = 32 << 20 };
    {
        const s = (try O.State.init(a, &ledger, 68, 4, 1, 64)).?;
        defer s.deinit();
        const batch: @import("filter/eval.zig").Batch = .{ .num_rows = 4, .cols = &.{} };
        const p: @import("agg_plan.zig").OwnerProgram = .{};
        const payload = try L.bind(p, &batch, &.{});
        const keys = [_]u64{ 1, 2, 1, 2 };
        try std.testing.expect(try s.locals[0].appendWindow(&keys, 64, 0, &payload));
        const Receiver = struct {
            rows: usize = 0,
            fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
                for (groups) |g| {
                    try std.testing.expectEqual(@as(u32, 2), g.weight);
                    try std.testing.expectEqual(@as(u32, @intCast(63 + g.key)), g.first);
                    r.rows += g.weight;
                }
            }
        };
        var receiver = [_]Receiver{.{}};
        try s.finish(&receiver, Receiver.receive);
        try std.testing.expectEqual(@as(usize, 4), receiver[0].rows);
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

fn mixedStorageTrial(a: std.mem.Allocator, workers: usize, direct: bool) !void {
    const O = Owner(u64, &.{ .sum, .min, .max, .present_count }, u32);
    const count = 4096;
    const Receiver = struct {
        groups: [34]?O.Group = @splat(null),
        fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
            for (groups) |g| {
                const id: usize = if (g.key == 0) 33 else @intCast(std.math.maxInt(u64) - g.key);
                try std.testing.expect(r.groups[id] == null);
                r.groups[id] = g;
            }
        }
    };
    var ledger: memory.SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const old = (try O.State.init(a, &ledger, count + 64, count, workers, 64)).?;
    defer old.deinit();
    const mixed = (try O.State.initStorage(a, &ledger, count + 64, count, workers, 64, true)).?;
    defer mixed.deinit();
    for (0..count) |i| {
        const id = if (i < count - 17) i % 17 else 17 + i - (count - 17);
        const wi = i / 31 % workers;
        if (i % 31 == 0) {
            old.locals[wi].start();
            mixed.locals[wi].start();
            old.locals[wi].bypass = direct;
            mixed.locals[wi].bypass = direct;
        }
        const absent = id == 0 or id == 17;
        const value = ([_]i64{ std.math.minInt(i64), 11, std.math.maxInt(i64) })[i % 3];
        const r: O.Record = .{
            .key = if (id == 33) 0 else std.math.maxInt(u64) - id,
            .ordinal = @intCast(64 + count - 1 - i),
            .weight = 1,
            .values = if (absent) .{ 0, 0, 0, 0 } else .{ value, value, value, 1 },
            .argument_nulls = if (absent) 15 else 0,
        };
        try old.locals[wi].append(r);
        try mixed.locals[wi].append(r);
    }
    const x = try a.alloc(Receiver, workers);
    defer a.free(x);
    const y = try a.alloc(Receiver, workers);
    defer a.free(y);
    @memset(x, .{});
    @memset(y, .{});
    try old.finish(x, Receiver.receive);
    try mixed.finish(y, Receiver.receive);
    var wide_rows: usize = 0;
    var small_rows: usize = 0;
    for (mixed.locals) |l| for (l.frontier.histogram) |n| {
        wide_rows += n;
    };
    for (mixed.small.?.frontiers) |f| for (f.histogram) |n| {
        small_rows += n;
    };
    try std.testing.expect(small_rows != 0);
    if (direct) try std.testing.expectEqual(@as(usize, 0), wide_rows) else try std.testing.expect(wide_rows != 0);
    try std.testing.expect(small_rows + 2 * wide_rows <= count);
    for (0..34) |id| {
        var want: ?O.Group = null;
        var got: ?O.Group = null;
        for (x) |r| if (r.groups[id]) |g| {
            want = g;
        };
        for (y) |r| if (r.groups[id]) |g| {
            got = g;
        };
        try std.testing.expect(want != null and got != null);
        try std.testing.expectEqualDeep(want, got);
    }
}

test "owner shared storage mixes singles and exact weighted lanes at every worker count and allocation failure" {
    for ([_]usize{ 1, 3, 16 }) |workers| for ([_]bool{ false, true }) |direct| {
        try mixedStorageTrial(std.testing.allocator, workers, direct);
    };
    try std.testing.checkAllAllocationFailures(std.testing.allocator, mixedStorageTrial, .{ 1, false });
}

test "owner shared storage recovers partial startup and returns an occupied ledger on setup refusal" {
    defer spawn.resetFailure();
    for (0..4) |n| {
        spawn.injectFailureAfter(n);
        try mixedStorageTrial(std.testing.allocator, 4, false);
    }
    spawn.resetFailure();
    const O = Owner(u64, &.{ .sum, .min, .max, .present_count }, u32);
    var ledger: memory.SharedBudget = .{ .limit = 128 << 20 };
    const occupied = ledger.drawExact(ledger.limit - 1).?;
    try std.testing.expect((try O.State.initStorage(std.testing.allocator, &ledger, 4096, 4096, 1, 64, true)) == null);
    try std.testing.expectEqual(occupied, ledger.used.load(.monotonic));
    ledger.release(occupied);
}
