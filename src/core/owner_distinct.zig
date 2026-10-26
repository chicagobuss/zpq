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
                try bound.forEachSelected(Bits, &callback, Callback.append);
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
