//! DISTINCT is two reductions in the existing owner framework: exact (group,value), then group COUNT.
//! NULL arguments never enter the pair table. Presence records preserve their groups, first ordinals and count(*).
const std = @import("std");
const owners = @import("owner_packed.zig");
const Plan = @import("expr/key_plan.zig").Plan;
const SelectedBatch = @import("selected_batch.zig").SelectedBatch;

pub fn Pipeline(comptime Bits: type) type {
    return struct {
        pub const Pairs = owners.Owner(Bits, &.{}, u32);
        pub const Groups = owners.Owner(Bits, &.{.present_count}, u32);

        /// Keys occupy the low prefix of the exact pair image. Mixed radix and raw fields share KeyPlan's unpack.
        pub const Prefix = union(enum) {
            modulus: Bits,
            mask: Bits,

            pub fn init(plan: *const Plan) !Prefix {
                const n = plan.keys.len - 1;
                if (plan.encoding.source == .dictionary) {
                    var product: u128 = 1;
                    for (plan.radices[0..n]) |r| product = try std.math.mul(u128, product, r);
                    if (Bits == u64 and product == @as(u128, 1) << 64)
                        return .{ .mask = std.math.maxInt(Bits) };
                    return .{ .modulus = std.math.cast(Bits, product) orelse return error.BadKey };
                }
                var width: usize = 0;
                for (plan.fields[0..n]) |f| width += f.bits + 1;
                if (width >= @bitSizeOf(Bits)) return error.BadKey;
                return .{ .mask = (@as(Bits, 1) << @intCast(width)) - 1 };
            }

            pub inline fn group(p: Prefix, pair: Bits) Bits {
                return switch (p) {
                    .modulus => |m| pair % m,
                    .mask => |m| pair & m,
                };
            }
        };

        pub const Sink = struct {
            pub const Source = struct { file: usize, rg: usize };
            pairs: *Pairs.Local,
            groups: *Groups.Local,
            identity: *const Plan,
            prefix: Prefix,
            sources: []const Source,
            bindings: []Plan.Binding,
            pack_buffer: []Bits = &.{},
            item: ?usize = null,
            input_base: u64 = 0,
            refused: *std.atomic.Value(bool),

            pub fn startErased(ctx: *anyopaque, base: u64, item: usize) void {
                const s: *@This() = @ptrCast(@alignCast(ctx));
                s.item = item;
                s.input_base = base;
                s.pairs.start();
                s.groups.start();
            }

            pub fn batchErased(ctx: *anyopaque, source: SelectedBatch, lookup: []const ?usize) anyerror!void {
                const s: *@This() = @ptrCast(@alignCast(ctx));
                return s.batch(source, lookup);
            }

            pub fn batch(s: *@This(), source: SelectedBatch, lookup: []const ?usize) !void {
                const item = s.item orelse return error.BadColumn;
                if (item >= s.sources.len or source.input_base < s.input_base) return error.BadColumn;
                const at = s.sources[item];
                const bound = (try s.identity.bindInto(s.bindings, source, lookup, at.file, at.rg)) orelse {
                    s.refused.store(true, .monotonic);
                    return error.ExceededMemoryBudget;
                };
                const Callback = struct {
                    sink: *Sink,
                    nulls: @import("filter/eval.zig").Batch.Column,
                    fn append(c: *@This(), row: usize, pair: Bits, ordinal: u64) !void {
                        const pos = std.math.cast(u32, ordinal) orelse return error.BadColumn;
                        const ni = c.nulls.nullInfo();
                        if (ni.def_levels != null and ni.def_levels.?[row] < ni.max_def) {
                            try c.sink.groups.append(.{
                                .key = c.sink.prefix.group(pair),
                                .ordinal = pos,
                                .weight = 1,
                                .values = .{0},
                            });
                        } else try c.sink.pairs.append(.{ .key = pair, .ordinal = pos, .weight = 1 });
                    }
                };
                var callback: Callback = .{ .sink = s, .nulls = bound.bindings[bound.bindings.len - 1].col };
                if (s.pack_buffer.len == 0) {
                    try bound.forEachSelected(Bits, &callback, Callback.append);
                } else {
                    var start: usize = 0;
                    while (start < source.batch.num_rows) {
                        const n = @min(s.pack_buffer.len, source.batch.num_rows - start);
                        const images = s.pack_buffer[0..n];
                        bound.packWindow(Bits, start, images) catch |err| {
                            if (err != error.BadKey and err != error.KeyTooWide) return err;
                            s.refused.store(true, .monotonic);
                            return error.ExceededMemoryBudget;
                        };
                        for (images, 0..) |pair, i| {
                            const row = start + i;
                            if (source.selected) |selected| if (!selected.isActive(row)) continue;
                            try callback.append(row, pair, try source.ordinal(row));
                        }
                        start += n;
                    }
                }
            }
        };

        pub const Reducer = struct {
            local: *Groups.Local,
            prefix: Prefix,
            count_rows: bool = true,
            pub fn receive(r: *@This(), _: usize, pairs: []const Pairs.Group) !void {
                // Every exact pair is complete in this partition. Its duplicate weight supplies count(*), while
                // one supplies DISTINCT; the second owner reunites groups split across pair partitions.
                for (pairs) |p| try r.local.append(.{
                    .key = r.prefix.group(p.key),
                    .ordinal = p.first,
                    .weight = if (r.count_rows) p.weight else 1,
                    .values = .{1},
                });
            }
        };
    };
}

test "owner DISTINCT pair routing preserves NULL presence duplicate weights skew and minimum ordinals" {
    const a = std.testing.allocator;
    const P = Pipeline(u64);
    for ([_]usize{ 1, 16 }) |workers| {
        var ledger: @import("memory.zig").SharedBudget = .{ .limit = 512 << 20 };
        defer std.debug.assert(ledger.used.load(.monotonic) == 0);
        const pairs = (try P.Pairs.State.init(a, &ledger, 4096, 4096, workers, 64)).?;
        defer pairs.deinit();
        const groups = (try P.Groups.State.init(a, &ledger, 4096, 4096, workers, 64)).?;
        defer groups.deinit();
        const prefix: P.Prefix = .{ .modulus = 4 };
        for (0..4096) |i| {
            const key: u64 = if (i % 8 == 0) 1 else 0;
            const pos: u32 = @intCast(4095 - i);
            if (key == 1) {
                try groups.locals[i % workers].append(.{ .key = key, .ordinal = pos, .weight = 1, .values = .{0} });
            } else try pairs.locals[i % workers].append(.{
                .key = key + (i % 7) * 4,
                .ordinal = pos,
                .weight = 1,
            });
        }
        const reducers = try a.alloc(P.Reducer, workers);
        defer a.free(reducers);
        for (reducers, groups.locals) |*r, *l| r.* = .{ .local = l, .prefix = prefix };
        try pairs.finish(reducers, P.Reducer.receive);
        const F = @import("owner_result.zig").Full(P.Groups.Group);
        const full = try F.init(a, &ledger, workers, 64);
        defer full.deinit();
        try groups.finish(full.receivers, F.Receiver.receive);
        try std.testing.expectEqual(@as(usize, 2), full.count());
        for (full.pieces) |piece| for (piece.groups) |g| {
            try std.testing.expectEqual(@as(?i128, if (g.key == 0) 7 else 0), g.integer(0));
            try std.testing.expectEqual(@as(u32, if (g.key == 0) 3584 else 512), g.weight);
            try std.testing.expectEqual(@as(u32, if (g.key == 0) 0 else 7), g.first);
        };
    }
}

test "owner DISTINCT completed-pair records fund physical contributions independently of duplicate weights" {
    const a = std.testing.allocator;
    const P = Pipeline(u64);
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const state = (try P.Groups.State.initWeighted(a, &ledger, 1000000, 1000000, 2, 1, 64)).?;
    defer state.deinit();
    try state.locals[0].append(.{ .key = 9, .ordinal = 17, .weight = 900000, .values = .{1} });
    try state.locals[0].append(.{ .key = 9, .ordinal = 2, .weight = 100000, .values = .{1} });
    const Receiver = struct {
        result: ?P.Groups.Group = null,
        fn receive(r: *@This(), _: usize, groups: []const P.Groups.Group) !void {
            for (groups) |g| {
                try std.testing.expect(r.result == null);
                r.result = g;
            }
        }
    };
    var receivers: [1]Receiver = .{.{}};
    try state.finish(&receivers, Receiver.receive);
    const g = receivers[0].result.?;
    try std.testing.expectEqual(@as(?i128, 2), g.integer(0));
    try std.testing.expectEqual(@as(u32, 1000000), g.weight);
    try std.testing.expectEqual(@as(u32, 2), g.first);
}

test "owner DISTINCT empty argument axis preserves the full 2^64 group-prefix domain" {
    const ast = @import("expr/ast.zig");
    var keys = [_]ast.ColRef{
        .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 },
        .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 },
        .{ .col_idx = 2, .physical_type = .INT32, .expr_type = .i64 },
    };
    var radices = [_]u64{ 1 << 32, 1 << 32, 1 };
    const plan: Plan = .{
        .keys = &keys,
        .radices = &radices,
        .fields = &.{},
        .domain = null,
        .encoding = .{ .width = .word, .source = .{ .dictionary = &radices } },
    };
    const prefix = try Pipeline(u64).Prefix.init(&plan);
    for ([_]u64{ 0, 1, 1 << 32, std.math.maxInt(u64) }) |id|
        try std.testing.expectEqual(id, prefix.group(id));
}

/// The range certificate is checked by KeyPlan on every produced batch, including PLAIN fallback pages.
pub const CompactGeometry = struct {
    group_bits: u7,
    value_null: u7,
    bits: usize,
    pub fn plan(identity: *const Plan) ?CompactGeometry {
        if (identity.encoding.source != .raw or identity.fields.len != 2) return null;
        const g: usize = @as(usize, identity.fields[0].bits) + 1;
        const v: usize = @as(usize, identity.fields[1].bits) + 1;
        if (g > 64 or v > 65 or @min(g, v) > 32 or g + v > 96) return null;
        return .{ .group_bits = @intCast(g), .value_null = @intCast(g + v - 1), .bits = g + v };
    }
    pub inline fn group(s: CompactGeometry, image: u128) u64 {
        return @truncate(image & ((@as(u128, 1) << s.group_bits) - 1));
    }
    pub inline fn present(s: CompactGeometry, image: u128) bool {
        return image & (@as(u128, 1) << s.value_null) == 0;
    }
};

/// The two physical image widths share the scatter, exact fold and group-count reduction.
pub fn Compact(comptime words: usize) type {
    const L = @import("owner_lanes.zig").PairImage(words);
    const records = @import("owner_records.zig");
    const W = records.Writer(L.Record, u32);
    const Fold = @import("owner_fold.zig").Table(L);
    return struct {
        const Self = @This();
        pub const Groups = owners.Owner(u64, &.{.present_count}, u32);
        pub const Local = struct {
            state: *Self,
            frontier: W.Frontier = .{},
            accepted: usize = 0,
            recent: []L.Record = &.{},
            valid: []u64 = &.{},
            seen: usize = 0,
            hits: usize = 0,
            bypass: bool = false,
            pub fn append(l: *Local, image: u128) !void {
                if (l.accepted >= l.state.rows) return error.BadPartial;
                l.accepted += 1;
                const r = L.Record.fromBits(image);
                const hash = L.hashRecord(r);
                const p: usize = @intCast(hash >> l.state.shift);
                if (l.bypass or l.recent.len == 0) return W.append(&l.frontier, l.state.blocks, &l.state.cursor, p, r);
                const at: usize = @as(usize, @truncate(hash)) & (l.recent.len - 1);
                const bit = @as(u64, 1) << @as(u6, @intCast(at % 64));
                const hit = l.valid[at / 64] & bit != 0 and L.same(r, l.recent[at]);
                if (!hit) {
                    try W.append(&l.frontier, l.state.blocks, &l.state.cursor, p, r);
                    // A cache hit can only suppress a pair this worker has already scattered.
                    l.recent[at] = r;
                    l.valid[at / 64] |= bit;
                }
                if (l.seen < l.recent.len * 4) {
                    l.seen += 1;
                    l.hits += @intFromBool(hit);
                    // Stop paying random lookups when the sample saves little scatter and fold traffic.
                    if (l.seen == l.recent.len * 4 and l.hits * 3 < l.seen / 3) l.bypass = true;
                }
            }
        };
        a: std.mem.Allocator,
        ledger: *@import("memory.zig").SharedBudget,
        rows: usize,
        parts: usize,
        shift: u6,
        locals: []Local = &.{},
        blocks: []W.Block = &.{},
        cursor: std.atomic.Value(u32) = .init(0),

        pub fn bytes(rows: usize, workers: usize, parts: usize) usize {
            return W.blockCount(rows, workers, parts) *| @sizeOf(W.Block) +|
                W.frontierBytes(workers, parts) +| workers *| @sizeOf(Local) +| @sizeOf(Self);
        }
        pub fn init(a: std.mem.Allocator, ledger: *@import("memory.zig").SharedBudget, rows: usize, workers: usize, parts: usize) !*Self {
            const s = try a.create(Self);
            s.* = .{ .a = a, .ledger = ledger, .rows = rows, .parts = parts, .shift = @intCast(@as(usize, 64) - std.math.log2_int(usize, parts)) };
            errdefer s.deinit();
            s.locals = try a.alloc(Local, workers);
            for (s.locals) |*l| l.* = .{ .state = s };
            for (s.locals) |*l| l.frontier = try W.Frontier.init(a, parts);
            const enabled = if (std.c.getenv("ZPQ_CD_REPEAT")) |v| !std.mem.eql(u8, std.mem.span(v), "0") else true;
            if (enabled) {
                const l2 = @import("hw.zig").l2BytesPerWorkerOr(640 << 10);
                const slots = std.math.floorPowerOfTwo(usize, @max(1, l2 / (2 * @sizeOf(L.Record))));
                for (s.locals) |*local| {
                    local.recent = try a.alloc(L.Record, slots);
                    local.valid = try a.alloc(u64, (slots + 63) / 64);
                    @memset(local.valid, 0);
                }
            }
            s.blocks = try a.alloc(W.Block, W.blockCount(rows, workers, parts));
            return s;
        }
        pub fn deinit(s: *Self) void {
            for (s.locals) |l| {
                l.frontier.deinit(s.a);
                s.a.free(l.recent);
                s.a.free(l.valid);
            }
            s.a.free(s.locals);
            s.a.free(s.blocks);
            s.a.destroy(s);
        }
        pub const Reducer = struct {
            local: *Groups.Local,
            geometry: CompactGeometry,
            fn receive(r: *Reducer, pairs: []const L.Group) !void {
                for (pairs) |pair| try r.local.append(.{
                    .key = r.geometry.group(pair.bits()),
                    .ordinal = 0,
                    .weight = 1,
                    .values = .{@intFromBool(r.geometry.present(pair.bits()))},
                });
            }
        };
        pub fn finish(s: *Self, reducers: []Reducer) !void {
            const stacks = reducers.len *| (1 << 20);
            _ = s.ledger.drawExact(stacks) orelse return error.ExceededMemoryBudget;
            defer s.ledger.release(stacks);
            const Task = struct {
                state: *Self,
                cursor: *std.atomic.Value(usize),
                reducer: *Reducer,
                err: ?anyerror = null,
                fn run(t: *@This()) void {
                    var table: Fold = .{};
                    defer table.deinit(t.state.a);
                    while (true) {
                        const p = t.cursor.fetchAdd(1, .monotonic);
                        if (p >= t.state.parts) return;
                        t.fold(&table, p) catch |e| {
                            t.err = e;
                            return;
                        };
                    }
                }
                fn fold(t: *@This(), table: *Fold, p: usize) !void {
                    var n: usize = 0;
                    for (t.state.locals) |l| n += l.frontier.histogram[p];
                    if (n == 0) return;
                    const l2 = @import("hw.zig").l2BytesPerWorkerOr(640 << 10);
                    try table.reset(t.state.a, n, @max(1, l2 / (@sizeOf(L.Group) + 8)));
                    for (t.state.locals) |l| {
                        var id = l.frontier.heads[p];
                        while (id != records.none) {
                            const b = &t.state.blocks[id];
                            if (b.next != records.none) @prefetch(&t.state.blocks[b.next], .{ .rw = .read, .locality = 1 });
                            for (b.rows[0..b.len]) |r| try table.append(t.state.a, r);
                            id = b.next;
                        }
                    }
                    try t.reducer.receive(table.items());
                }
            };
            var cursor: std.atomic.Value(usize) = .init(0);
            var tasks: [64]Task = undefined;
            for (tasks[0..reducers.len], reducers) |*t, *r| t.* = .{ .state = s, .cursor = &cursor, .reducer = r };
            if (reducers.len == 1 or !@import("spawn.zig").runTree(Task, tasks[0..reducers.len], .{ .stack_size = 1 << 20 }, Task.run))
                tasks[0].run();
            for (tasks[0..reducers.len]) |t| if (t.err) |e| return e;
        }
        pub const Sink = struct {
            local: *Local,
            identity: *const Plan,
            sources: []const Pipeline(u128).Sink.Source,
            bindings: [2]Plan.Binding = undefined,
            images: []u128,
            item: usize = 0,
            input_rows: usize,
            refused: *std.atomic.Value(bool),
            pub fn start(ctx: *anyopaque, _: u64, item: usize) void {
                const s: *Sink = @ptrCast(@alignCast(ctx));
                s.item = item;
            }
            pub fn batch(ctx: *anyopaque, source: SelectedBatch, lookup: []const ?usize) anyerror!void {
                const s: *Sink = @ptrCast(@alignCast(ctx));
                s.produce(source, lookup) catch |e| {
                    if (e != error.BadKey and e != error.KeyTooWide) return e;
                    s.refused.store(true, .monotonic);
                    return error.ExceededMemoryBudget;
                };
            }
            fn produce(s: *Sink, source: SelectedBatch, lookup: []const ?usize) !void {
                const at = s.sources[s.item];
                const bound = (try s.identity.bindInto(&s.bindings, source, lookup, at.file, at.rg)) orelse
                    return error.ExceededMemoryBudget;
                var start_row: usize = 0;
                while (start_row < source.batch.num_rows) {
                    const n = @min(s.images.len, source.batch.num_rows - start_row);
                    const images = s.images[0..n];
                    try bound.packWindow(u128, start_row, images);
                    for (images, 0..) |image, i| {
                        const row = start_row + i;
                        if (source.selected) |selected| if (!selected.isActive(row)) continue;
                        if (try source.ordinal(row) >= s.input_rows) return error.BadColumn;
                        try s.local.append(image);
                    }
                    start_row += n;
                }
            }
        };
    };
}

test "compact DISTINCT owners match weighted owners on random uniform skew empty and NULL data" {
    const a = std.testing.allocator;
    inline for (.{ 2, 3 }) |words| for (0..5) |mode| for ([_]usize{ 1, 4 }) |workers| {
        var budget: @import("memory.zig").SharedBudget = .{ .limit = 512 << 20 };
        defer std.debug.assert(budget.used.load(.monotonic) == 0);
        var charge: @import("memory.zig").Charged = .{ .child = a, .budget = &budget };
        const C = Compact(words);
        const P = Pipeline(u128);
        const n: usize = if (mode == 2) 0 else 4096;
        const geometry: CompactGeometry = .{ .group_bits = if (words == 2) 12 else 49, .value_null = if (words == 2) 24 else 66, .bits = if (words == 2) 25 else 67 };
        const compact = try C.init(charge.allocator(), &budget, n, workers, 64);
        defer compact.deinit();
        const groups = (try C.Groups.State.init(a, &budget, n, n, workers, 64)).?;
        defer groups.deinit();
        const pairs = (try P.Pairs.State.init(a, &budget, n, n, workers, 64)).?;
        defer pairs.deinit();
        const reference = (try P.Groups.State.init(a, &budget, n, n, workers, 64)).?;
        defer reference.deinit();
        const mask = (@as(u128, 1) << geometry.group_bits) - 1;
        const prefix: P.Prefix = .{ .mask = mask };
        for (0..n) |i| {
            const random = @import("expr/aggregate/census.zig").splitMix64(i + 91);
            const g: u128 = if (mode == 3 or i % 19 == 0) @as(u128, 1) << (geometry.group_bits - 1) else if (mode == 0 and i % 4 != 0) 7 else random % 127;
            const absent = mode == 4 or i % 11 == 0;
            const v: u128 = if (absent) @as(u128, 1) << geometry.value_null else @as(u128, (random >> 32) % 73) << geometry.group_bits;
            const image = g | v;
            try compact.locals[i % workers].append(image);
            if (absent) try reference.locals[i % workers].append(.{
                .key = g,
                .ordinal = @intCast(i),
                .weight = 1,
                .values = .{0},
            }) else try pairs.locals[i % workers].append(.{ .key = image, .ordinal = @intCast(i), .weight = 1 });
        }
        var compact_reducers: [4]C.Reducer = undefined;
        var reference_reducers: [4]P.Reducer = undefined;
        for (compact_reducers[0..workers], groups.locals) |*r, *local| r.* = .{ .local = local, .geometry = geometry };
        for (reference_reducers[0..workers], reference.locals) |*r, *local| r.* = .{ .local = local, .prefix = prefix };
        try compact.finish(compact_reducers[0..workers]);
        try pairs.finish(reference_reducers[0..workers], P.Reducer.receive);
        const F = @import("owner_result.zig").Full(C.Groups.Group);
        const R = @import("owner_result.zig").Full(P.Groups.Group);
        const actual = try F.init(a, &budget, workers, 64);
        defer actual.deinit();
        const expected = try R.init(a, &budget, workers, 64);
        defer expected.deinit();
        try groups.finish(actual.receivers, F.Receiver.receive);
        try reference.finish(expected.receivers, R.Receiver.receive);
        try std.testing.expectEqual(expected.count(), actual.count());
        for (actual.pieces) |piece| for (piece.groups) |g| {
            var found = false;
            for (expected.pieces) |rp| for (rp.groups) |want| if (want.key == g.key) {
                try std.testing.expectEqual(want.integer(0), g.integer(0));
                found = true;
            };
            try std.testing.expect(found);
        };
    };
}

test "compact DISTINCT geometry declines a range whose two axes just exceed 32 bits" {
    const ast = @import("expr/ast.zig");
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var plan = (try Plan.initFields(std.testing.allocator, &.{ key, key }, null, &.{
        .{ .bits = 32, .signed = true }, .{ .bits = 32, .signed = true },
    })).?;
    defer plan.deinit(std.testing.allocator);
    try std.testing.expectEqual(null, CompactGeometry.plan(&plan));
    plan.fields[0].bits = 31;
    try std.testing.expect(CompactGeometry.plan(&plan) != null);
}

test "compact pair geometry keeps full i64 extrema and the NULL tag beside a narrow group" {
    const ast = @import("expr/ast.zig");
    const a = std.testing.allocator;
    const axes = [_]ast.Expr{
        .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } },
        .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } },
    };
    var plan = (try Plan.initFields(a, &axes, null, &.{
        .{ .bits = 18, .signed = true }, .{ .bits = 64, .signed = true },
    })).?;
    defer plan.deinit(a);
    const geometry = CompactGeometry.plan(&plan).?;
    try std.testing.expectEqual(@as(usize, 84), geometry.bits);
    const batch: @import("filter/eval.zig").Batch = .{ .num_rows = 3, .cols = &.{
        .{ .i64 = .{ .values = &.{ 7, undefined, -1 }, .def_levels = &.{ 1, 0, 1 }, .max_def = 1 } },
        .{ .i64 = .{ .values = &.{ std.math.minInt(i64), std.math.maxInt(i64), undefined }, .def_levels = &.{ 1, 1, 0 }, .max_def = 1 } },
    } };
    var bindings: [2]Plan.Binding = undefined;
    const bound = (try plan.bindInto(&bindings, .{ .batch = &batch }, &.{ 0, 1 }, 0, 0)).?;
    var images: [3]u128 = undefined;
    try bound.packWindow(u128, 0, &images);
    const R = @import("owner_lanes.zig").PairImage(3).Record;
    for (images, 0..) |image, row| {
        try std.testing.expectEqual(image, R.fromBits(image).bits());
        try std.testing.expectEqual(row != 2, geometry.present(image));
    }
    try std.testing.expectEqual(@as(u64, 1) << 18, geometry.group(images[1]));
}
