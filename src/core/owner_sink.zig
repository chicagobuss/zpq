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
        pack_buffer: []Bits = &.{},
        item: ?usize = null,
        input_base: u64 = 0,
        refused: ?*std.atomic.Value(bool) = null,

        pub fn startErased(ctx: *anyopaque, original_input_base: u64, item: usize) void {
            const s: *Self = @ptrCast(@alignCast(ctx));
            s.start(original_input_base, item);
        }

        pub fn batchErased(ctx: *anyopaque, source: SelectedBatch, lookup: []const ?usize) anyerror!void {
            const s: *Self = @ptrCast(@alignCast(ctx));
            return s.batch(source, lookup);
        }

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
            const keys = (try s.identity.bindInto(s.bindings, source, lookup, at.file, at.rg)) orelse {
                if (s.refused) |flag| flag.store(true, .monotonic);
                return error.ExceededMemoryBudget;
            };
            const payload = try L.bind(s.program, source.batch, lookup);
            const contiguous = source.input_rows == null and fullySelected(source);
            const Callback = struct {
                local: *O.Local,
                payload: *const L.Bound,
                fn append(c: *@This(), row: usize, key: Bits, ordinal: u64) !void {
                    try c.local.append(try c.payload.record(key, row, ordinal));
                }
            };
            var callback: Callback = .{ .local = s.local, .payload = &payload };
            if (s.pack_buffer.len == 0) {
                keys.forEachSelected(Bits, &callback, Callback.append) catch |err|
                    return s.identityError(err);
            } else {
                var offset: usize = 0;
                while (offset < source.batch.num_rows) {
                    const n = @min(s.pack_buffer.len, source.batch.num_rows - offset);
                    const images = s.pack_buffer[0..n];
                    keys.packWindow(Bits, offset, images) catch |err| return s.identityError(err);
                    if (contiguous and try s.local.appendWindow(images, try source.ordinal(offset), offset, &payload)) {
                        offset += n;
                        continue;
                    }
                    for (images, 0..) |key, i| {
                        const row = offset + i;
                        if (source.selected) |selected| if (!selected.isActive(row)) continue;
                        try callback.append(row, key, try source.ordinal(row));
                    }
                    offset += n;
                }
            }
        }

        fn identityError(s: *Self, err: anyerror) anyerror {
            if (err == error.BadKey or err == error.KeyTooWide) {
                if (s.refused) |flag| flag.store(true, .monotonic);
                return error.ExceededMemoryBudget;
            }
            return err;
        }

        fn fullySelected(source: SelectedBatch) bool {
            const sel = source.selected orelse return true;
            const words = sel.len / 64;
            for (sel.mask[0..words]) |word| if (word != std.math.maxInt(u64)) return false;
            const tail: u6 = @intCast(sel.len % 64);
            if (tail == 0) return true;
            const mask = (@as(u64, 1) << tail) - 1;
            return sel.mask[words] & mask == mask;
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
    var images: [2]u128 = undefined;
    var sink: S = .{
        .pack_buffer = &images,
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
    var refused: std.atomic.Value(bool) = .init(false);
    var sink: S = .{
        .local = &owner.locals[0],
        .identity = &identity,
        .program = .{},
        .sources = &.{.{ .file = 0, .rg = 0 }},
        .bindings = &binding,
        .refused = &refused,
    };
    sink.start(0, 0);
    try sink.batch(.{ .batch = &view }, &.{0});
    try std.testing.expectEqual(@as(usize, 1), owner.locals[0].accepted);
    columns[0].i32.dict = &.{42};
    try std.testing.expectError(error.ExceededMemoryBudget, sink.batch(.{ .batch = &view, .input_base = 1 }, &.{0}));
    try std.testing.expect(refused.load(.monotonic));
    columns[0] = .{ .i32 = .{ .values = &.{9} } };
    try std.testing.expectError(error.ExceededMemoryBudget, sink.batch(.{ .batch = &view, .input_base = 1 }, &.{0}));
    try std.testing.expectEqual(@as(usize, 1), owner.locals[0].accepted);
}

test "owner sink dirty tail cannot balance a rejected poison row into an all-selected window" {
    const a = std.testing.allocator;
    const ast = @import("expr/ast.zig");
    const O = @import("owner_packed.zig").Owner(u64, &.{.sum}, u32);
    const S = Packed(u64, &.{.sum}, u32);
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]@import("expr/agg.zig").AggCall{
        .{ .func = .sum, .alias = "s", .result = .i64, .where = null, .arg = arg },
    };
    var identity = (try key_plan.Plan.init(a, &.{key}, null)).?;
    defer identity.deinit(a);
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 32 << 20 };
    {
        const state = (try O.State.init(a, &ledger, 5, 5, 1, 64)).?;
        defer state.deinit();
        const batch: @import("filter/eval.zig").Batch = .{ .num_rows = 5, .cols = &.{
            .{ .i32 = .{ .values = &.{ 1, 2, 1, 3, 3 } } },
            .{ .i64 = .{ .values = &.{}, .codes = &.{ 0, 999, 1, 0, 1 }, .dict = &.{ -7, 11 } } },
        } };
        var mask = [_]u64{0b11_1101};
        const sel: @import("filter/selection.zig").SelectionVector = .{ .allocator = a, .len = 5, .mask = &mask };
        const source: SelectedBatch = .{ .batch = &batch, .selected = &sel };
        try std.testing.expectEqual(batch.num_rows, sel.count());
        try std.testing.expect(!S.fullySelected(source));
        var binding: [1]key_plan.Plan.Binding = undefined;
        var images: [3]u64 = undefined;
        var sink: S = .{
            .pack_buffer = &images,
            .local = &state.locals[0],
            .identity = &identity,
            .program = Program.init(&calls).?,
            .sources = &.{.{ .file = 0, .rg = 0 }},
            .bindings = &binding,
        };
        sink.start(0, 0);
        try sink.batch(source, &.{ 0, 1 });
        try std.testing.expectEqual(@as(usize, 4), state.locals[0].accepted);
        const Receiver = struct {
            rows: usize = 0,
            sum: i128 = 0,
            fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
                for (groups) |g| {
                    r.rows += g.weight;
                    r.sum += g.integer(0).?;
                }
            }
        };
        var receiver = [_]Receiver{.{}};
        try state.finish(&receiver, Receiver.receive);
        try std.testing.expectEqual(@as(usize, 4), receiver[0].rows);
        try std.testing.expectEqual(@as(i128, 8), receiver[0].sum);
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}

test "owner sink shared storage keeps NULL keys, NULL singles, weighted NULLs and original ordinals" {
    const a = std.testing.allocator;
    const ast = @import("expr/ast.zig");
    const kinds = &.{ lanes.Lane.sum, lanes.Lane.min, lanes.Lane.max, lanes.Lane.present_count };
    const O = @import("owner_packed.zig").Owner(u64, kinds, u32);
    const S = Packed(u64, kinds, u32);
    const key: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]@import("expr/agg.zig").AggCall{
        .{ .func = .sum, .alias = "s", .result = .i64, .arg = arg, .where = null },
        .{ .func = .min, .alias = "lo", .result = .i64, .arg = arg, .where = null },
        .{ .func = .max, .alias = "hi", .result = .i64, .arg = arg, .where = null },
        .{ .func = .count, .alias = "n", .result = .i64, .arg = arg, .where = null },
    };
    const program = Program.init(&calls).?;
    try std.testing.expect(program.commonColumn());
    var different = program;
    different.args[1].col.col_idx = 0;
    try std.testing.expect(!different.commonColumn());
    var identity = (try key_plan.Plan.init(a, &.{key}, null)).?;
    defer identity.deinit(a);
    const batch: @import("filter/eval.zig").Batch = .{ .num_rows = 7, .cols = &.{
        .{ .i32 = .{
            .values = &.{ 0, 999, 0, 1, 1, 8, 7 },
            .def_levels = &.{ 1, 0, 1, 1, 1, 1, 1 },
            .max_def = 1,
        } },
        .{ .i64 = .{
            .values = &.{},
            .codes = &.{ 0, 1, 999, 999, 999, 2, 999 },
            .dict = &.{ -7, 9, 11 },
            .def_levels = &.{ 1, 1, 0, 0, 0, 1, 0 },
            .max_def = 1,
        } },
    } };
    const Receiver = struct {
        groups: [5]O.Group = undefined,
        len: usize = 0,
        fn receive(r: *@This(), _: usize, groups: []const O.Group) !void {
            for (groups) |g| {
                try std.testing.expect(r.len < r.groups.len);
                r.groups[r.len] = g;
                r.len += 1;
            }
        }
    };
    var ledger: @import("memory.zig").SharedBudget = .{ .limit = 64 << 20 };
    {
        const old = (try O.State.init(a, &ledger, 1007, 7, 1, 64)).?;
        defer old.deinit();
        const mixed = (try O.State.initStorage(a, &ledger, 1007, 7, 1, 64, true)).?;
        defer mixed.deinit();
        var bindings: [2][1]key_plan.Plan.Binding = undefined;
        var images: [2][3]u64 = undefined;
        for ([_]*O.State{ old, mixed }, 0..) |state, i| {
            var sink: S = .{
                .pack_buffer = &images[i],
                .local = &state.locals[0],
                .identity = &identity,
                .program = program,
                .sources = &.{.{ .file = 0, .rg = 0 }},
                .bindings = &bindings[i],
            };
            sink.start(1000, 0);
            try sink.batch(.{ .batch = &batch, .input_base = 1000 }, &.{ 0, 1 });
        }
        var x = [_]Receiver{.{}};
        var y = [_]Receiver{.{}};
        try old.finish(&x, Receiver.receive);
        try mixed.finish(&y, Receiver.receive);
        try std.testing.expectEqual(@as(usize, 5), y[0].len);
        var rows: u64 = 0;
        var sum: i128 = 0;
        for (x[0].groups[0..x[0].len]) |want| {
            var found = false;
            for (y[0].groups[0..y[0].len]) |got| if (got.key == want.key) {
                try std.testing.expectEqualDeep(want, got);
                rows += got.weight;
                sum += got.integer(0) orelse 0;
                found = true;
            };
            try std.testing.expect(found);
        }
        try std.testing.expectEqual(@as(u64, 7), rows);
        try std.testing.expectEqual(@as(i128, 13), sum);
    }
    try std.testing.expectEqual(@as(usize, 0), ledger.used.load(.monotonic));
}
