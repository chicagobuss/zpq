//! The selected-batch seam borrows decoded inputs only until the callback returns. Identity or payload changes
//! refuse the whole private attempt; no GroupTable contribution may be mixed with these owned records.
const std = @import("std");
const key_plan = @import("expr/key_plan.zig");
const lanes = @import("owner_lanes.zig");
const Program = @import("agg_plan.zig").OwnerProgram;
const SelectedBatch = @import("selected_batch.zig").SelectedBatch;

pub fn Packed(comptime Bits: type, comptime kinds: []const lanes.Lane, comptime Ordinal: type) type {
    const L = lanes.Layout(Bits, kinds, Ordinal);
    const O = @import("owner_packed.zig").Owner(Bits, kinds, Ordinal);
    return struct {
        const Self = @This();
        pub const Source = struct { file: usize, rg: usize };

        local: *O.Local,
        identity: *const key_plan.Plan,
        program: Program,
        sources: []const Source,
        bindings: []key_plan.Plan.Binding,
        item: ?usize = null,
        input_base: u64 = 0,

        pub fn start(s: *Self, original_input_base: u64, item: usize) void {
            s.item = item;
            s.input_base = original_input_base;
            s.local.start();
        }

        pub fn batch(s: *Self, source: SelectedBatch, lookup: []const ?usize) !void {
            const item = s.item orelse return error.BadColumn;
            if (item >= s.sources.len or source.input_base < s.input_base) return error.BadColumn;
            // DISTINCT has its own exact pair identity/reduction. Never accept it as an ordinary lane program.
            if (s.program.distinct_arg != null or !s.identity.fitsBits(Bits)) return error.BadAggArg;
            const at = s.sources[item];
            const keys = (try s.identity.bindInto(s.bindings, source, lookup, at.file, at.rg)) orelse
                return error.ExceededMemoryBudget;
            const payload = try L.bind(s.program, source.batch, lookup);
            const Callback = struct {
                local: *O.Local,
                payload: *const L.Bound,
                fn append(c: *@This(), row: usize, key: Bits, ordinal: u64) !void {
                    try c.local.append(try c.payload.record(key, row, ordinal));
                }
            };
            var callback: Callback = .{ .local = s.local, .payload = &payload };
            try keys.forEachSelected(Bits, &callback, Callback.append);
        }
    };
}

fn selectedTrial(a: std.mem.Allocator, workers: usize) !void {
    const ast = @import("expr/ast.zig");
    const Call = @import("expr/agg.zig").AggCall;
    const O = @import("owner_packed.zig").Owner(u128, &.{ .sum, .present_count }, u64);
    const S = Packed(u128, &.{ .sum, .present_count }, u64);
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const count_arg: ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]Call{
        .{ .func = .sum, .alias = "s", .result = .i64, .where = null, .arg = arg },
        .{ .func = .count, .alias = "n", .result = .i64, .where = null, .arg = count_arg },
    };
    const program = Program.init(&calls).?;
    var identity = (try key_plan.Plan.init(a, &.{key}, null)).?;
    defer identity.deinit(a);
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const owner = (try O.State.init(a, &ledger, @as(usize, 1) << 41, 64, workers, 64)).?;
    defer owner.deinit();
    const view: @import("filter/eval.zig").Batch = .{ .num_rows = 5, .cols = &.{
        .{ .i64 = .{
            .values = &.{ 9, 123, 9, undefined, undefined },
            .def_levels = &.{ 1, 1, 1, 0, 0 },
            .max_def = 1,
        } },
        .{ .i64 = .{
            .values = &.{},
            .dict = &.{ -7, 0 },
            .codes = &.{ 0, 999, 1, 999, 0 },
            .def_levels = &.{ 1, 1, 1, 0, 1 },
            .max_def = 1,
        } },
        .{ .i64 = .{ .values = &.{}, .def_levels = &.{ 1, 1, 0, 0, 1 }, .max_def = 1 } },
    } };
    // A dirty tail must not visit rows outside len; poison row 1 is SQL-rejected, not an input payload.
    var mask = [_]u64{std.math.maxInt(u64) ^ 2};
    const selected: @import("filter/selection.zig").SelectionVector = .{
        .allocator = a,
        .len = 5,
        .mask = &mask,
    };
    const base: u64 = 1 << 40;
    const source: SelectedBatch = .{
        .batch = &view,
        .selected = &selected,
        .input_base = base,
        .input_rows = &.{ 99, 66, 7, 3, 2 },
    };
    var binding: [1]key_plan.Plan.Binding = undefined;
    var sink: S = .{
        .local = &owner.locals[workers - 1],
        .identity = &identity,
        .program = program,
        .sources = &.{.{ .file = 0, .rg = 8 }},
        .bindings = &binding,
    };
    try std.testing.expectError(error.BadColumn, sink.batch(source, &.{ 0, 1, 2 }));
    sink.start(base, 0);
    try sink.batch(source, &.{ 0, 1, 2 });
    const Receiver = struct {
        groups: [2]?O.Group = @splat(null),
        fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
            for (groups) |g| {
                const at: usize = @intFromBool(g.key == @as(u128, 1) << 64);
                try std.testing.expect(r.groups[at] == null);
                r.groups[at] = g;
            }
        }
    };
    const receivers = try a.alloc(Receiver, workers);
    defer a.free(receivers);
    @memset(receivers, .{});
    try owner.finish(receivers, Receiver.receive);
    for (0..2) |at| {
        var result: ?O.Group = null;
        for (receivers) |r| if (r.groups[at]) |g| {
            try std.testing.expect(result == null);
            result = g;
        };
        const g = result orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(@as(u64, 2), g.weight);
        try std.testing.expectEqual(@as(?i128, -7), g.integer(0));
        try std.testing.expectEqual(@as(?i128, 1), g.integer(1));
        try std.testing.expectEqual(base + @as(u64, if (at == 0) 7 else 2), g.first);
    }
}

test "owner sink composes gathered typed keys and lanes with levels-only COUNT and rejected poison" {
    for ([_]usize{ 1, 3 }) |workers| try selectedTrial(std.testing.allocator, workers);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, selectedTrial, .{1});
}

test "owner sink late dictionary decline requests whole-attempt replay without adding a mixed record" {
    const a = std.testing.allocator;
    const dense = @import("expr/agg_dense.zig");
    const ast = @import("expr/ast.zig");
    const O = @import("owner_packed.zig").Owner(u64, &.{}, u32);
    const S = Packed(u64, &.{}, u32);
    var values = [_]u64{9};
    var translation = [_]u32{ 0, 1 };
    var translations = [_][]u32{&translation};
    var files = [_]usize{0};
    var axes = [_]dense.Axis{.{
        .values = .{ .int32 = &values },
        .trans = &translations,
        .file_base = &files,
        .bytes = 0,
    }};
    const domain: dense.Domain = .{ .axes = &axes, .bytes = 0 };
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    var identity = (try key_plan.Plan.init(a, &.{key}, &domain)).?;
    defer identity.deinit(a);
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 128 << 20 };
    defer std.debug.assert(ledger.used.load(.monotonic) == 0);
    const owner = (try O.State.init(a, &ledger, 2, 2, 1, 64)).?;
    defer owner.deinit();
    var columns = [_]@import("filter/eval.zig").Batch.Column{
        .{ .i32 = .{ .values = &.{}, .dict = &.{9}, .codes = &.{0} } },
    };
    const view: @import("filter/eval.zig").Batch = .{ .num_rows = 1, .cols = &columns };
    var binding: [1]key_plan.Plan.Binding = undefined;
    var sink: S = .{
        .local = &owner.locals[0],
        .identity = &identity,
        .program = .{},
        .sources = &.{.{ .file = 0, .rg = 0 }},
        .bindings = &binding,
    };
    sink.start(0, 0);
    try sink.batch(.{ .batch = &view }, &.{0});
    try std.testing.expectEqual(@as(usize, 1), owner.locals[0].accepted);
    columns[0].i32.dict = &.{42};
    try std.testing.expectError(error.ExceededMemoryBudget, sink.batch(.{ .batch = &view, .input_base = 1 }, &.{0}));
    columns[0] = .{ .i32 = .{ .values = &.{9} } };
    try std.testing.expectError(error.ExceededMemoryBudget, sink.batch(.{ .batch = &view, .input_base = 1 }, &.{0}));
    try std.testing.expectEqual(@as(usize, 1), owner.locals[0].accepted);
}
