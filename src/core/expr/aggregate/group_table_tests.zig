const std = @import("std");
const expr_ast = @import("../ast.zig");
const filter_eval = @import("../../filter/eval.zig");
const core_memory = @import("../../memory.zig");
const types = @import("types.zig");
const AggCall = types.AggCall;
const SharedBudget = core_memory.SharedBudget;
const keys_mod = @import("contracts.zig");
const hashFixed = keys_mod.hashFixed;
const expandFixed = keys_mod.expandFixed;
const canonicalKey = keys_mod.canonicalKey;
const serializeRowKey = keys_mod.serializeRowKey;
const census_mod = @import("census.zig");
const CountSketch = census_mod.CountSketch;
const Census = census_mod.Census;
const sketchHash = census_mod.sketchHash;
const rawSketchHash = census_mod.rawSketchHash;
const distinct = @import("distinct.zig");
const NUMERIC_PARTS = distinct.NUMERIC_PARTS;
const DistinctSet = distinct.DistinctSet;
const DistinctAcc = distinct.DistinctAcc;
const Accumulator = @import("accumulator.zig").Accumulator;
const testing = std.testing;
const packFixedKey = keys_mod.packFixedKey;
const fullestCells = census_mod.fullestCells;
const cellsSeparate = census_mod.cellsSeparate;
const ac = @import("group_table.zig");
const GroupTable = ac.GroupTable;

test "GroupTable basic grouping, float normalization, and memory capping" {
    const allocator = std.testing.allocator;

    var gt = GroupTable.init(allocator, 1024);
    defer gt.deinit();

    const agg_calls = &[_]AggCall{
        .{
            .func = .sum,
            .alias = "sum_val",
            .result = .i64,
            .arg = null,
            .where = null,
        },
    };

    const gid1 = try gt.getOrInsert("group_a", agg_calls, 0);
    const gid2 = try gt.getOrInsert("group_b", agg_calls, 1);
    const gid1_dup = try gt.getOrInsert("group_a", agg_calls, 0);

    try std.testing.expect(gid1 == 0);
    try std.testing.expect(gid2 == 1);
    try std.testing.expect(gid1_dup == 0);

    var f32_vals_1 = [_]f32{0.0};
    const f32_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_1 },
    };
    var f32_vals_2 = [_]f32{-0.0};
    const f32_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_vals_2 },
    };
    var key_scratch: std.ArrayList(u8) = .empty;
    defer key_scratch.deinit(allocator);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_1}, 0);
    const key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_col_2}, 0);
    const key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(key2);

    try std.testing.expectEqualSlices(u8, key1, key2);

    var f32_nan_1 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_1 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_1 },
    };
    var f32_nan_2 = [_]f32{std.math.nan(f32)};
    const f32_nan_col_2 = filter_eval.Batch.Column{
        .f32 = .{ .values = &f32_nan_2 },
    };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_1}, 0);
    const nan_key1 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key1);

    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{f32_nan_col_2}, 0);
    const nan_key2 = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(nan_key2);

    try std.testing.expectEqualSlices(u8, nan_key1, nan_key2);

    var bool_true = [_]bool{true};
    const bool_col = filter_eval.Batch.Column{ .boolean = .{ .values = &bool_true } };
    var i64_one = [_]i64{1};
    const i64_col = filter_eval.Batch.Column{ .i64 = .{ .values = &i64_one } };
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{bool_col}, 0);
    const bool_key = try allocator.dupe(u8, key_scratch.items);
    defer allocator.free(bool_key);
    try serializeRowKey(&key_scratch, allocator, &[_]filter_eval.Batch.Column{i64_col}, 0);
    try std.testing.expectEqualSlices(u8, bool_key, key_scratch.items);
    try std.testing.expectEqual(@as(usize, 1 + 8), bool_key.len);

    var small_gt = GroupTable.init(allocator, 10);
    defer small_gt.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, small_gt.getOrInsert("too_large_key_that_exceeds_budget", agg_calls, 0));
}

test "compact fixed slots compare complete keys under projected-hash collisions and growth" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(GroupTable.FixedSlot));
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var table = GroupTable.init(std.testing.allocator, 1 << 20);
    table.fixed_stride = 9;
    defer table.deinit();
    for (0..256) |g| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], g, .little);
        const id = try table.getOrInsertFixedHashed(&key, &calls, 1000 + g, @as(u64, g) << 32);
        try std.testing.expectEqual(@as(u32, @intCast(g)), id);
    }
    for (0..256) |g| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], g, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(g)), table.findFixed(&key, @as(u64, g) << 32));
        const id = try table.getOrInsertFixedHashed(&key, &calls, g, 0);
        try std.testing.expectEqual(@as(u32, @intCast(g)), id);
        try std.testing.expectEqual(@as(u64, g), table.first.items[id]);
    }
}

test "fixed local tables retain charged capacity and discard every old id" {
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
    };
    for ([_]bool{ false, true }) |compact| {
        var budget: SharedBudget = .{ .limit = 1 << 20 };
        var table = GroupTable.init(std.testing.allocator, 0);
        table.shared = &budget;
        table.fixed_stride = 9;
        if (compact) table.compact.configure(&calls);
        defer table.deinit();
        var capacity: usize = 0;
        for (0..4) |epoch| {
            var last: [9]u8 = @splat(0);
            for (0..64) |g| {
                last[0] = 1;
                std.mem.writeInt(u64, last[1..9], epoch * 64 + g, .little);
                const id = try table.getOrInsertFixed(&last, &calls, epoch * 64 + g);
                try std.testing.expectEqual(@as(u32, @intCast(g)), id);
                try std.testing.expectEqual(@as(u64, 0), table.readAcc(id, 0, calls.len).count);
                try std.testing.expectEqual(@as(?i128, null), table.readAcc(id, 1, calls.len).sum_i);
                table.writeAcc(id, 0, calls.len, .{ .count = 1 });
                table.writeAcc(id, 1, calls.len, .{ .sum_i = @intCast(g) });
            }
            if (epoch == 0) capacity = table.allocated_bytes;
            table.clearFixedRetainingCapacity();
            try std.testing.expectEqual(@as(usize, 0), table.groupCount());
            try std.testing.expectEqual(capacity, table.allocated_bytes);
            try std.testing.expectEqual(capacity, budget.used.load(.monotonic));
            try std.testing.expectEqual(@as(?u32, null), table.findFixed(&last, hashFixed(&last)));
        }
    }
}

test "STRGROUP slots grow without key relocation and local clear reuses arena capacity" {
    const a = std.testing.allocator;
    var table = GroupTable.init(a, 64 << 20);
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    var buf: [128]u8 = undefined;
    var first_ptr: ?[*]const u8 = null;
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        const id = try table.getOrInsert(key, &calls, i + 1);
        try std.testing.expectEqual(@as(u32, @intCast(i)), id);
        if (i == 0) first_ptr = table.keyAt(0).ptr;
        try std.testing.expect(first_ptr.? == table.keyAt(0).ptr);
    }
    for (0..20000) |i| {
        const key = try std.fmt.bufPrint(&buf, "long-key-prefix-with-embedded-\x00-byte-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findKey(key));
        try std.testing.expectEqual(@as(u32, @intCast(i)), try table.getOrInsert(key, &calls, 0));
        try std.testing.expectEqual(@as(u64, 0), table.first.items[i]);
    }
    const blocks = table.strings.?.blocks.items.len;
    const slots = table.slots.ptr;
    table.clearRetainingCapacity();
    try std.testing.expectEqual(@as(usize, 0), table.groupCount());
    for (0..1000) |i| {
        const key = try std.fmt.bufPrint(&buf, "reused-{d}", .{i});
        _ = try table.getOrInsert(key, &calls, i);
    }
    try std.testing.expect(slots == table.slots.ptr);
    try std.testing.expectEqual(blocks, table.strings.?.blocks.items.len);
}

test "STRGROUP failed arena inserts and winner allocations preserve ids and ledger cleanup" {
    const Trial = struct {
        fn run(a: std.mem.Allocator) !void {
            var budget: SharedBudget = .{ .limit = 16 << 20 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            defer {
                table.deinit();
                std.debug.assert(budget.used.load(.monotonic) == 0);
            }
            const calls = [_]AggCall{.{ .func = .min, .result = .bytes, .arg = null, .where = null, .alias = "lo" }};
            var buf: [128]u8 = undefined;
            for (0..64) |i| {
                const key = try std.fmt.bufPrint(&buf, "variable-key-{d}", .{i});
                const id = try table.getOrInsert(key, &calls, i);
                try table.accumulators.items[id].min_arena.consider("winner-b", true);
                try table.accumulators.items[id].min_arena.consider("winner-a", true);
                try std.testing.expectEqualStrings("winner-a", table.accumulators.items[id].stringValue().?);
            }
            const before = table.groupCount();
            table.releaseSlack();
            budget.limit = budget.used.load(.monotonic);
            const long = try a.alloc(u8, 131073);
            defer a.free(long);
            @memset(long, 'x');
            try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsert(long, &calls, 0));
            try std.testing.expectEqual(before, table.groupCount());
            try std.testing.expectEqual(@as(?u32, 0), table.findKey("variable-key-0"));
        }
    };
    try Trial.run(std.testing.allocator);
    try std.testing.checkAllAllocationFailures(std.testing.allocator, Trial.run, .{});
}

test "SharedBudget: tables share one budget instead of owning fixed slices" {
    const allocator = std.testing.allocator;
    const agg_calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 }};

    // Two tables under one budget, as parallel workers are: the only busy one may take the whole budget.
    var budget = SharedBudget{ .limit = 4096 };
    var a = GroupTable.init(allocator, 0);
    a.shared = &budget;
    defer a.deinit();
    var b = GroupTable.init(allocator, 0);
    b.shared = &budget;
    defer b.deinit();

    var key_buf: [16]u8 = undefined;
    var placed: usize = 0;
    while (placed < 500) : (placed += 1) {
        const key = std.fmt.bufPrint(&key_buf, "k{d}", .{placed}) catch unreachable;
        _ = b.getOrInsert(key, &agg_calls, 0) catch break;
    }
    try std.testing.expect(b.refs.items.len > 0);
    try std.testing.expect(budget.used.load(.monotonic) > 4096 / 2);
    try std.testing.expect(a.max_memory_bytes == 0); // the idle table reserved nothing

    var full = SharedBudget{ .limit = 8 };
    var c = GroupTable.init(allocator, 0);
    c.shared = &full;
    defer c.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, c.getOrInsert("too-big", &agg_calls, 0));

    // Freed tables return their quota: workers merge, then free.
    var recycle = SharedBudget{ .limit = 4096 };
    {
        var tmp = GroupTable.init(allocator, 0);
        tmp.shared = &recycle;
        _ = try tmp.getOrInsert("k", &agg_calls, 0);
        try std.testing.expect(recycle.used.load(.monotonic) > 0);
        tmp.deinit();
    }
    try std.testing.expectEqual(@as(usize, 0), recycle.used.load(.monotonic));
}

test "fixed-width group keys match the serialized framing and the byte table" {
    const a = std.testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};

    var ints = [_]i64{ 7, -1, 7 };
    var idef = [_]u32{ 1, 1, 0 };
    const icol: filter_eval.Batch.Column = .{ .i64 = .{
        .values = &ints,
        .def_levels = &idef,
        .max_def = 1,
        .has_nulls = true,
    } };
    var other = [_]i64{4};
    const pair = [_]filter_eval.Batch.Column{ icol, .{ .i64 = .{ .values = &other } } };
    var floats = [_]f64{ -0.0, std.math.nan(f64) };
    const fcol: filter_eval.Batch.Column = .{ .f64 = .{ .values = &floats } };

    var image: [32]u8 = undefined;
    var ser: std.ArrayList(u8) = .empty;
    defer ser.deinit(a);
    const samples = [_]struct { cols: []const filter_eval.Batch.Column, row: usize }{
        .{ .cols = &.{icol}, .row = 0 },
        .{ .cols = &.{icol}, .row = 1 },
        .{ .cols = &.{icol}, .row = 2 },
        .{ .cols = &pair, .row = 0 },
        .{ .cols = &.{fcol}, .row = 0 },
        .{ .cols = &.{fcol}, .row = 1 },
    };
    for (samples) |s| {
        const n = packFixedKey(s.cols, s.row, &image).?;
        try serializeRowKey(&ser, a, s.cols, s.row);
        var back: [32]u8 = undefined;
        const expanded = expandFixed(n, ser.items, &back).?;
        try std.testing.expectEqualSlices(u8, image[0..n], expanded);
        var canon_buf: [48]u8 = undefined;
        try std.testing.expectEqualSlices(u8, ser.items, canonicalKey(image[0..n], &canon_buf));
    }

    var fixed = GroupTable.init(a, 1 << 20);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var bytes = GroupTable.init(a, 1 << 20);
    defer bytes.deinit();
    for ([_]usize{ 0, 2, 1, 0 }, 0..) |row, nvisit| {
        const n = packFixedKey(&.{icol}, row, &image).?;
        try serializeRowKey(&ser, a, &.{icol}, row);
        // The repeated value is asked from an earlier position the second time, and must keep that one.
        const pos: u64 = if (nvisit == 3) 40 else @intCast(100 - row);
        const fid = try fixed.getOrInsert(ser.items, &calls, pos);
        const bid = try bytes.getOrInsert(ser.items, &calls, pos);
        try std.testing.expectEqual(bid, fid);
        try std.testing.expectEqual(bytes.first.items[bid], fixed.first.items[fid]);
        try std.testing.expectEqual(fid, fixed.findKey(image[0..n]).?);
    }
    try std.testing.expectEqual(@as(usize, 3), fixed.groupCount());
    try std.testing.expectEqual(@as(u64, 40), fixed.first.items[0]);

    var tiny = GroupTable.init(a, 0);
    tiny.fixed_stride = 9;
    defer tiny.deinit();
    try std.testing.expectError(error.ExceededMemoryBudget, tiny.getOrInsertFixed(image[0..9], &calls, 0));
}

test "fixed-width group insert frees every partial allocation" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    const insert = struct {
        fn run(allocator: std.mem.Allocator) !void {
            var t = GroupTable.init(allocator, 1 << 30);
            t.fixed_stride = 9;
            defer t.deinit();
            var i: u64 = 0;
            while (i < 40) : (i += 1) {
                var image: [9]u8 = @splat(0);
                image[0] = 1;
                std.mem.writeInt(u64, image[1..9], i, .little);
                _ = try t.getOrInsertFixed(&image, &calls, i);
                if (i % 3 == 0) _ = try t.getOrInsertFixed(&image, &calls, 1000 + i);
            }
            try std.testing.expectEqual(@as(usize, 40), t.groupCount());
        }
    }.run;
    // In-place resize would make the failure count depend on what else the thread allocated.
    const NoResize = struct {
        child: std.mem.Allocator,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawAlloc(len, alignment, ret_addr);
        }
        fn free(ctx: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(memory, alignment, ret_addr);
        }
    };
    var fixed: NoResize = .{ .child = std.testing.allocator };
    try std.testing.checkAllAllocationFailures(fixed.allocator(), insert, .{});
}

test "a lone string key reaches the same sketch cell framed or unframed" {
    var prng = std.Random.DefaultPrng.init(0x5ce7c4);
    const rand = prng.random();
    var raw: [200]u8 = undefined;
    var framed: [205]u8 = undefined;
    for (16..raw.len) |len| {
        rand.bytes(raw[0..len]);
        framed[0] = 1;
        std.mem.writeInt(u32, framed[1..5], @intCast(len), .little);
        @memcpy(framed[5..][0..len], raw[0..len]);
        try std.testing.expectEqual(sketchHash(framed[0 .. len + 5]), rawSketchHash(raw[0..len]));
    }
}

test "fixed-width groups charge their count(DISTINCT) sets to the shared budget, and a failed insert leaves nothing" {
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 },
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.fixed_stride = 9;
    table.shared = &budget;
    defer table.deinit();
    var image: [32]u8 = @splat(0);
    image[0] = 1;
    const g = try table.getOrInsertFixed(image[0..9], &calls, 0);
    const slot = &table.accumulators.items[g * calls.len + 1].distinct;
    try std.testing.expect(slot.heap() == null);
    try std.testing.expect(slot.budgetPtr() == &budget);

    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var b: SharedBudget = .{ .limit = 1 << 20 };
            var t = GroupTable.init(gpa, 0);
            t.fixed_stride = 9;
            t.shared = &b;
            defer t.deinit();
            var img: [32]u8 = @splat(0);
            for (0..3) |i| {
                img[0] = 1;
                img[1] = @intCast(i);
                _ = try t.getOrInsertFixed(img[0..9], &calls, i);
            }
        }
    }.run, .{});
}

test "a table charges each group's count(DISTINCT) set object, fixed-width keys or not" {
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "u", .result = .i64 },
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
    };
    const n = 500;
    var plain = GroupTable.init(std.testing.allocator, 1 << 30);
    defer plain.deinit();
    var without = GroupTable.init(std.testing.allocator, 1 << 30);
    defer without.deinit();
    var fixed = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed.fixed_stride = 9;
    defer fixed.deinit();
    var fixed_without = GroupTable.init(std.testing.allocator, 1 << 30);
    fixed_without.fixed_stride = 9;
    defer fixed_without.deinit();
    for (0..n) |i| {
        var image: [9]u8 = undefined;
        image[0] = 1;
        std.mem.writeInt(i64, image[1..9], @intCast(i), .little);
        _ = try plain.getOrInsert(&image, &calls, i);
        _ = try without.getOrInsert(&image, calls[1..], i);
        _ = try fixed.getOrInsertFixed(&image, &calls, i);
        _ = try fixed_without.getOrInsertFixed(&image, calls[1..], i);
    }
    // Variable tables quote actual array capacities; no distinct-set object exists before a spill.
    const without_cells = without.accumulators.capacity * @sizeOf(Accumulator);
    const plain_cells = plain.accumulators.capacity * @sizeOf(Accumulator);
    try std.testing.expectEqual(without.allocated_bytes - without_cells + plain_cells, plain.allocated_bytes);
    for ([_]*const GroupTable{ &plain, &fixed }) |t| {
        for (t.accumulators.items) |acc| {
            if (acc == .distinct) try std.testing.expect(acc.distinct.heap() == null);
        }
    }
}

fn compactAllocationTrial(gpa: std.mem.Allocator, fixed: bool) !void {
    // Keep the allocation-failure sweep deterministic: whether an address can grow in place varies across runs.
    // Disabling remap here forces every replacement through the allocating fallback, so every such allocation is
    // still injected and checked; separate tests exercise the page-backed no-copy path.
    const NoRemap = struct {
        child: std.mem.Allocator,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            return self.child.rawAlloc(n, alignment, ra);
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.child.rawFree(bytes, alignment, ra);
        }
    };
    var stable: NoRemap = .{ .child = gpa };
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
        .{ .func = .min, .arg = arg, .where = null, .alias = "lo", .result = .i64 },
        .{ .func = .max, .arg = arg, .where = null, .alias = "hi", .result = .i64 },
    };
    var budget: SharedBudget = .{ .limit = 1 << 20 };
    var table = GroupTable.init(stable.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = if (fixed) 9 else 0;
    table.compact.configure(&calls);
    defer table.deinit();
    try std.testing.expectEqual(@as(usize, 56), table.compact.stride * 8);
    for (0..90) |k| {
        var key: [9]u8 = @splat(0);
        key[0] = 1;
        std.mem.writeInt(u64, key[1..9], k, .little);
        const g = try table.getOrInsert(&key, &calls, k);
        try std.testing.expectEqual(@as(?i128, null), table.readAcc(g, 1, calls.len).sum_i);
        table.writeAcc(g, 0, calls.len, .{ .count = 2 });
        table.writeAcc(g, 2, calls.len, .{ .avg = .{ .sum = 0, .isum = -5, .count = 1 } });
        table.writeAcc(g, 3, calls.len, .{ .min_i = -5 });
        table.writeAcc(g, 4, calls.len, .{ .max_i = -5 });
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 1, calls.len).sum_i);
        try std.testing.expectEqual(@as(?i128, -5), table.compact.integer(g, 1));
        try std.testing.expectEqual(@as(?i128, -5), table.readAcc(g, 3, calls.len).min_i);
        try std.testing.expectEqual(g, try table.getOrInsert(&key, &calls, k + 1));
    }
}

test "compact count and integer tuple allocation failures leave tables owned" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{false});
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactAllocationTrial, .{true});
}

test "compact states refuse float tuples and only share identical integer inputs" {
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var calls = [_]AggCall{
        .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
        .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
        .{ .func = .avg, .arg = arg, .where = null, .alias = "a", .result = .avg_f64 },
    };
    var state: @import("../agg_state.zig").State = .{};
    state.configure(calls[0..1]);
    try std.testing.expectEqual(@as(usize, 8), state.stride * 8);
    state.configure(calls[0..2]);
    try std.testing.expectEqual(@as(usize, 32), state.stride * 8);
    state.configure(&calls);
    try std.testing.expect(state.fields[1].shared_sum);
    calls[1].offset = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    calls[1].offset = 0;
    calls[2].arg.?.col_ref.col_idx = 1;
    state.configure(&calls);
    try std.testing.expect(!state.fields[1].shared_sum);
    var reordered = [_]AggCall{ calls[1], calls[0], calls[2] };
    reordered[2].arg = arg;
    state.configure(&reordered);
    try std.testing.expect(!state.fields[0].shared_sum);
    calls[2].arg.?.col_ref.expr_type = .f64;
    state.configure(&calls);
    try std.testing.expectEqual(@as(usize, 0), state.stride);
}

test "compact variable insertion keeps retained capacity charged after allocation failure" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(a: std.mem.Allocator) !void {
            const arg: expr_ast.Expr = .{ .col_ref = .{
                .col_idx = 0,
                .physical_type = .INT64,
                .expr_type = .i64,
            } };
            const calls = [_]AggCall{
                .{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 },
                .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 },
            };
            var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1024 };
            var table = GroupTable.init(a, 0);
            table.shared = &budget;
            table.compact.configure(&calls);
            defer table.deinit();
            _ = table.getOrInsert("key", &calls, 0) catch |err| {
                table.releaseSlack();
                try std.testing.expect(budget.used.load(.monotonic) >= table.compact.held());
                return err;
            };
        }
    }.run, .{});
}

test "regression: a count(DISTINCT) merge sizes its tables for the result, so none grows while hash-ordered values arrive" {
    // F3: one worker's set merged into another's slot by slot (hash order) while the destination grew as values came
    // took 40 s at -j 3 against 0.4 s presized. A table that grows mid-merge allocates once per doubling; sized
    // first, each destination table allocates at most once. That allocation count is what this pins.
    const gpa = std.testing.allocator;
    const n: u64 = 60_000;
    const Dest = enum { empty, inline_two, small_map, partitioned };
    inline for (.{ u64, i128 }) |T| for ([_]bool{ false, true }) |other_parted| for (std.enums.values(Dest)) |dest_shape| {
        var other: DistinctSet = .{};
        defer other.deinit(gpa);
        if (other_parted) try other.initPartitions(gpa, null);
        for (0..n) |i| {
            const v: T = @intCast(i * 7919);
            if (T == u64) try other.insertU64(gpa, v) else try other.insertI128(gpa, v);
        }
        var dest: DistinctSet = .{};
        if (dest_shape == .partitioned) try dest.initPartitions(gpa, null);
        // Values both sets hold (the first few multiples), and some only the destination does (odd ones).
        const own: usize = switch (dest_shape) {
            .empty => 0,
            .inline_two => 2,
            .small_map, .partitioned => 50,
        };
        for (0..own) |i| {
            const v: T = @intCast(if (i % 2 == 0) i * 7919 else i * 7919 + 1);
            if (T == u64) try dest.insertU64(gpa, v) else try dest.insertI128(gpa, v);
        }
        const before = dest.count();
        var counting = std.testing.FailingAllocator.init(gpa, .{});
        try dest.merge(&other, counting.allocator());
        try std.testing.expectEqual(@as(usize, n) + before - (own + 1) / 2, dest.count());
        const tables: usize = if (dest_shape == .partitioned) NUMERIC_PARTS else 1;
        std.testing.expect(counting.allocations <= tables) catch |err| {
            std.debug.print("{s} {s} other_parted={}: {d} allocations\n", .{ @typeName(T), @tagName(dest_shape), other_parted, counting.allocations });
            return err;
        };
        dest.deinit(gpa);
    };
}

fn compactBoundsTrial(a: std.mem.Allocator) !void {
    var bounds: Census = .{ .gpa = a, .item_rows = &.{} };
    defer bounds.deinit();
    // Different hashes in ignored bits deliberately collide; each occurrence must survive in the complete class.
    const h: u64 = 0x37ab_1234_5678_0000;
    for (0..1000) |i| try bounds.record(h | @as(u64, @intCast(i)));
    const part: usize = @intCast(bounds.fingerprint(h) >> 40);
    try std.testing.expectEqual(@as(usize, 1000), bounds.partLen(part));
    try std.testing.expectEqual(bounds.fingerprint(h), bounds.fingerprint(h ^ (@as(u64, 255) << 48)));
    for (bounds.full[part].items) |chunk| for (chunk) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    };
    for (bounds.open[part].?[0..bounds.fill[part]]) |fragment| {
        try std.testing.expectEqual(@as(u32, @truncate(bounds.fingerprint(h))), fragment);
    }
    for (0..Census.N_PARTS) |p| try bounds.record(@as(u64, @intCast(p)) << 56);
    try std.testing.expectEqual(@as(u64, 1000 + Census.N_PARTS), bounds.recorded);
}

test "hash identity bounds use four-byte rows and bounded frontiers and free all failed allocations" {
    try std.testing.expectEqual(@as(usize, 512), @sizeOf(Census.Chunk));
    try std.testing.expectEqual(@as(usize, 4), @sizeOf(@TypeOf(@as(Census.Chunk, undefined)[0])));
    try std.testing.checkAllAllocationFailures(std.testing.allocator, compactBoundsTrial, .{});
}

test "bounded first discovery keeps at most H exact keys and only lowers the shared ordinal" {
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    for ([_]bool{ false, true }) |fixed| {
        var threshold: std.atomic.Value(u64) = .init(std.math.maxInt(u64));
        var early = GroupTable.init(std.testing.allocator, 1 << 20);
        defer early.deinit();
        var late = GroupTable.init(std.testing.allocator, 1 << 20);
        defer late.deinit();
        early.fixed_stride = if (fixed) 9 else 0;
        late.fixed_stride = early.fixed_stride;
        early.discovery = .{ .limit = 3, .threshold = &threshold };
        late.discovery = early.discovery;
        try std.testing.expect(early.countOnly());
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try late.getOrInsert(&key, &calls, 100 + i);
        }
        try std.testing.expectEqual(@as(u64, 102), threshold.load(.monotonic));
        for (0..3) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try early.getOrInsert(&key, &calls, 1 + i);
        }
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
        for (3..1000) |i| {
            var key: [9]u8 = @splat(0);
            key[0] = 1;
            std.mem.writeInt(u64, key[1..9], i, .little);
            try std.testing.expectEqual(CountSketch.sinkId(0), try early.getOrInsert(&key, &calls, i + 1));
            try std.testing.expectEqual(CountSketch.sinkId(0), try late.getOrInsert(&key, &calls, i + 100));
        }
        try std.testing.expectEqual(@as(usize, 3), early.groupCount());
        try std.testing.expectEqual(@as(usize, 3), late.groupCount());
        try std.testing.expectEqual(@as(u64, 3), threshold.load(.monotonic));
    }
}

test "distinct bitmaps retain the hash reservation so a late outlier has identical budget admission" {
    const hw = @import("../../hw.zig");
    defer hw.test_cpu = null;
    for ([_]usize{ 144, 256, 512, 1024, 4096, 8192 }) |limit| {
        var outcomes: [2]bool = undefined;
        for ([_]usize{ 1, 64 << 10 }, 0..) |cache, mode| {
            hw.test_cpu = .{ .l2 = .{ .bytes = cache, .max_sharing = 1 } };
            var budget: SharedBudget = .{ .limit = limit };
            {
                var set: DistinctSet = .{ .shared = &budget };
                defer set.deinit(testing.allocator);
                outcomes[mode] = true;
                for (0..129) |i| {
                    const v: u64 = if (i == 128) 1 << 48 else i;
                    set.insertU64(testing.allocator, v) catch |e| {
                        try testing.expectEqual(error.ExceededMemoryBudget, e);
                        outcomes[mode] = false;
                        break;
                    };
                }
            }
            try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
        }
        try testing.expectEqual(outcomes[0], outcomes[1]);
    }
}

test "M1 parallel DISTINCT source charges and releases preserve the private ceiling" {
    const a = std.testing.allocator;
    // Each spilled set owns value and map-capacity credit as well as its object. Measure the reservation including
    // growth, then fund exactly eight such sets: object-only credit can legitimately refuse eight concurrent workers.
    var table = GroupTable.init(a, 1 << 20);
    defer table.deinit();
    var sample = table.freshDistinct();
    for (0..4) |i| try sample.insertU64(a, i);
    const per_set = table.allocated_bytes;
    try std.testing.expect(per_set > @sizeOf(DistinctSet));
    sample.deinit(a);
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    // Private reservations only grow until deinit. Check every worker's values against the measured peak, so the
    // concurrent phase cannot exceed eight reservations even when all workers hold a set at once.
    for (0..256) |job| {
        var set = table.freshDistinct();
        for (0..4) |i| try set.insertU64(a, job * 7 + i);
        try std.testing.expectEqual(per_set, table.allocated_bytes);
        set.deinit(a);
        try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    }
    table.max_memory_bytes = 8 * per_set;

    // Fill the ceiling with eight simultaneously live sets and verify an extra byte cannot be charged.
    var live: [8]DistinctAcc = undefined;
    var initialized: usize = 0;
    defer for (live[0..initialized]) |*set| set.deinit(a);
    for (&live) |*set| {
        set.* = table.freshDistinct();
        initialized += 1;
        for (0..4) |i| try set.insertU64(a, i);
    }
    try std.testing.expectEqual(table.max_memory_bytes, table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.chargePrivateBytes(1));
    for (&live) |*set| set.deinit(a);
    initialized = 0;
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.set_objects);
    const Task = struct {
        table: *GroupTable,
        allocator: std.mem.Allocator,
        cursor: *std.atomic.Value(usize),
        err: ?anyerror = null,
        fn run(t: *@This()) void {
            while (t.err == null) {
                const job = t.cursor.fetchAdd(1, .monotonic);
                if (job >= 256) return;
                var set = t.table.freshDistinct();
                defer set.deinit(t.allocator);
                for (0..4) |i| set.insertU64(t.allocator, job * 7 + i) catch |e| {
                    t.err = e;
                    return;
                };
                std.debug.assert(set.count() == 4);
            }
        }
    };
    var tasks: [8]Task = undefined;
    var cursor = std.atomic.Value(usize).init(0);
    for (&tasks) |*t| t.* = .{ .table = &table, .allocator = a, .cursor = &cursor };
    const spawn = @import("../../spawn.zig");
    if (!spawn.runTree(Task, &tasks, .{ .stack_size = 1 << 20 }, Task.run)) tasks[0].run();
    for (tasks) |t| if (t.err) |e| return e;
    try std.testing.expectEqual(@as(usize, 0), table.allocated_bytes);
    try std.testing.expectEqual(@as(usize, 0), table.set_objects);
}

test "round 4: private table ceiling includes a spilled object" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        // Clamped after the group exists, so the spill itself has to see the live ceiling.
        t.max_memory_bytes = t.allocated_bytes;
        const slot = &t.accumulators.items[gid].distinct;
        for (0..3) |v| try slot.insertU64(a, v);
        try testing.expectError(error.ExceededMemoryBudget, slot.insertU64(a, 3));
        try testing.expect(slot.heap() == null);
        try testing.expectEqual(@as(usize, 3), slot.count());
        try testing.expectEqual(t.max_memory_bytes, t.allocated_bytes);
    }
}

test "round 4: a spilled object stays inside a private ceiling after the next insert" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 1 << 20);
        t.fixed_stride = stride;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const before = t.allocated_bytes;
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        const set_credit = t.accumulators.items[gid].distinct.heap().?.max_memory_bytes;
        try testing.expect(set_credit > 0);
        try testing.expectEqual(before + @sizeOf(DistinctSet) + set_credit, t.allocated_bytes);
        image[0] = 2;
        _ = if (stride == 0)
            try t.getOrInsert("b", &calls, 1)
        else
            try t.getOrInsertFixed(&image, &calls, 1);
        try testing.expect(t.allocated_bytes >= before + @sizeOf(DistinctSet));
        const held = t.allocated_bytes;
        t.accumulators.items[gid].distinct.deinit(a);
        try testing.expectEqual(held - @sizeOf(DistinctSet) - set_credit, t.allocated_bytes);
    }
}

test "round 4: shared budget still owns the spilled object, not the table" {
    const a = testing.allocator;
    const calls = [_]AggCall{
        .{ .func = .count_distinct, .arg = null, .where = null, .alias = "d", .result = .i64 },
    };
    var budget = SharedBudget{ .limit = 1 << 20, .block = 1 };
    for ([_]u8{ 0, 9 }) |stride| {
        var t = GroupTable.init(a, 0);
        t.fixed_stride = stride;
        t.shared = &budget;
        defer t.deinit();
        var image: [9]u8 = @splat(0);
        image[0] = 1;
        const gid = if (stride == 0)
            try t.getOrInsert("a", &calls, 0)
        else
            try t.getOrInsertFixed(&image, &calls, 0);
        const table_before = t.allocated_bytes;
        const used_before = budget.used.load(.monotonic);
        for (0..4) |v| try t.accumulators.items[gid].distinct.insertU64(a, v);
        try testing.expectEqual(table_before, t.allocated_bytes);
        try testing.expect(budget.used.load(.monotonic) >= used_before + @sizeOf(DistinctSet));
    }
}

test "STRGROUP whole-table quote includes first chunks large records and actual capacities" {
    const a = testing.allocator;
    const calls = [_]AggCall{.{ .func = .count, .result = .i64, .arg = null, .where = null, .alias = "c" }};
    for ([_]usize{ 0, 127, 33000, 70000 }) |width| for ([_]usize{ 1, 33, 300 }) |groups| {
        var table = GroupTable.init(a, 256 << 20);
        defer table.deinit();
        const key = try a.alloc(u8, width);
        defer a.free(key);
        @memset(key, 'x');
        for (0..groups) |i| {
            if (width >= 4) std.mem.writeInt(u32, key[0..4], @intCast(i), .little);
            _ = try table.getOrInsert(key, &calls, i);
        }
        try testing.expect(table.allocated_bytes <= GroupTable.variableTableBytes(groups, width, calls.len));
    };
}

fn cachedDistinctTrial(a: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(a);
    defer arena.deinit();
    var left: DistinctSet = .{};
    defer left.deinit(a);
    var right: DistinctSet = .{};
    defer right.deinit(a);
    for (0..4096) |i| {
        const text = try std.fmt.allocPrint(arena.allocator(), "long-shared-prefix-{d:0>10}-suffix", .{i % 1001});
        try left.insertBytes(a, text);
        if (i % 2 == 0) try right.insertBytes(a, text);
    }
    try left.insertBytes(a, "");
    try right.merge(&left, a);
    try testing.expectEqual(@as(usize, 1002), right.count());
    var it = right.val_set.bytes.map.keyIterator();
    while (it.next()) |key| try testing.expectEqual(std.hash.Wyhash.hash(0, key.bytes), key.hash);
}

test "cached string DISTINCT grows and merges exact bytes through allocation refusals" {
    try cachedDistinctTrial(testing.allocator);
    try testing.checkAllAllocationFailures(core_memory.testing_fixed_growth.allocator, cachedDistinctTrial, .{});
}

test "shared string identity frames NULL empty embedded zero and large keys once through stored hash growth" {
    const a = testing.allocator;
    var table = GroupTable.init(a, 32 << 20);
    defer table.deinit();
    const big = try a.alloc(u8, 70001);
    defer a.free(big);
    @memset(big, 'x');
    const keys = [_]?[]const u8{ null, "", "a\x00b", "\x00", big };
    for (0..500) |i| {
        var bytes: [64]u8 = undefined;
        const raw = try std.fmt.bufPrint(&bytes, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(raw, &.{}, 900 + i, 7);
    }
    for (keys, 0..) |key, i| {
        const id = try table.getOrInsertStringHashed(key, &.{}, 800 + i, 7);
        try testing.expectEqual(@as(u32, @intCast(500 + i)), id);
        try testing.expectEqual(id, try table.getOrInsertStringHashed(key, &.{}, i, 7));
        try testing.expectEqual(@as(u64, @intCast(i)), table.first.items[id]);
        const frame = table.keyAt(id);
        if (key) |raw| {
            try testing.expectEqual(@as(u8, 1), frame[0]);
            try testing.expectEqualStrings(raw, frame[5..]);
            try testing.expectEqual(raw.len, std.mem.readInt(u32, frame[1..5], .little));
        } else try testing.expectEqualSlices(u8, &.{0}, frame);
    }
    try testing.expectEqual(@as(usize, 505), table.groupCount());
}

test "a riding census drops on every worker's merged cells, not on one worker's slice" {
    const a = testing.allocator;
    const n_cells = 64;
    const admit_n = 8;
    const k = 2;
    var item_rows = [_]u64{0};
    for ([_]bool{ false, true }) |second_separates| {
        var dropped: std.atomic.Value(bool) = .init(false);
        var slot_cells: [2][n_cells]u32 = undefined;
        var slots = [_][]u32{ &slot_cells[0], &slot_cells[1] };
        var merged: Census.MergedCells = .{ .slots = &slots, .quorum = 2 };
        var censuses: [2]Census = undefined;
        for (&censuses, 0..) |*c, wi| c.* = .{
            .worker = wi,
            .gpa = a,
            .item_rows = &item_rows,
            .dropped = &dropped,
            .merged = &merged,
            .cut_k = k,
            .admit_n = admit_n,
        };
        defer for (&censuses) |*c| c.deinit();
        // The first worker's slice holds two hot cells over a sparse floor: its own cells separate the cut.
        var first: [n_cells]u32 = @splat(1);
        first[0] = 40;
        first[1] = 40;
        try std.testing.expect(cellsSeparate(blk: {
            var r: [admit_n + 1]u32 = undefined;
            fullestCells(&first, &r);
            break :blk &r;
        }, k, admit_n));
        try censuses[0].judge(&first);
        // Alone it decides nothing: the quorum is two.
        try std.testing.expect(!dropped.load(.monotonic) and !censuses[0].off);
        // The second worker's slice is either as decisive, or a dense floor that buries the hot cells once merged.
        var second: [n_cells]u32 = @splat(if (second_separates) 1 else 60);
        if (second_separates) {
            second[0] = 40;
            second[1] = 40;
        }
        try censuses[1].judge(&second);
        try std.testing.expectEqual(second_separates, dropped.load(.monotonic));
    }
    // Without merged cells a worker still judges its own.
    var dropped: std.atomic.Value(bool) = .init(false);
    var alone: Census = .{ .gpa = a, .item_rows = &item_rows, .dropped = &dropped, .cut_k = k, .admit_n = admit_n };
    defer alone.deinit();
    var own: [n_cells]u32 = @splat(1);
    own[0] = 40;
    own[1] = 40;
    try alone.judge(&own);
    try std.testing.expect(dropped.load(.monotonic));
}

test "distinct admission numeric fanout preserves bitmap promotion" {
    const a = testing.allocator;
    var budget: SharedBudget = .{ .limit = 64 << 20 };
    {
        var set: DistinctSet = .{};
        defer set.deinit(a);
        try set.initPartitions(a, &budget);
        try testing.expectEqual(@as(usize, 16), set.parts.?.len);
        for (0..65536) |i| try set.insertU64(a, (i * 97) % 100000);
        try testing.expectEqual(@as(usize, 65536), set.count());
        for (set.parts.?) |part| try testing.expect(part.val_set == .bitmap);
    }
    try testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "distinct admission string fanout stays wide and merges across fanouts" {
    const a = testing.allocator;
    var narrow: DistinctSet = .{};
    defer narrow.deinit(a);
    try narrow.initPartitions(a, null);
    var wide: DistinctSet = .{};
    defer wide.deinit(a);
    try wide.initStringPartitions(a, null);
    try testing.expectEqual(@as(usize, 64), wide.parts.?.len);
    try narrow.insertBytes(a, "");
    try wide.insertBytes(a, "long shared string");
    try wide.insertBytes(a, "");
    try narrow.merge(&wide, a);
    try testing.expectEqual(@as(usize, 2), narrow.count());
    try wide.merge(&narrow, a);
    try testing.expectEqual(@as(usize, 2), wide.count());
}

test "variable hash growth bitmap migration preserves supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var buf: [24]u8 = undefined;
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        _ = try table.getOrInsertStringHashed(key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.variableHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.variableHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        const key = try std.fmt.bufPrint(&buf, "key-{d}", .{i});
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.probeString(key, true, 0).id);
    }
}

test "fixed hash growth bitmap admission and migration preserve supplied hashes" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixedHashed(&key, &calls, i, 0);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    try std.testing.expectError(error.ExceededMemoryBudget, table.growFixedSlotsImpl(32, projected, true));
    try std.testing.expectEqual(@as(usize, 16), table.fixedSlotCount());
    budget.limit += 2;
    try table.growFixedSlotsImpl(32, projected, true);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, 0));
    }
}

test "fixed hash growth is ledger funded and refusal preserves its keys" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        observing: bool = false,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{
                .alloc = alloc,
                .resize = std.mem.Allocator.noResize,
                .remap = std.mem.Allocator.noRemap,
                .free = free,
            } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            if (self.observing)
                self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    var table = GroupTable.init(counter.allocator(), 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    var table_live = true;
    defer if (table_live) table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    budget.limit = budget.used.load(.monotonic);
    std.mem.writeInt(u64, key[1..9], 12, .little);
    try std.testing.expectError(error.ExceededMemoryBudget, table.getOrInsertFixed(&key, &calls, 12));
    try std.testing.expectEqual(@as(usize, 12), table.groupCount());
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    budget.limit = projected;
    try table.admit(projected - table.allocated_bytes);
    counter.observing = true;
    try table.growFixedSlots(32, projected);
    counter.observing = false;
    try std.testing.expectEqual(@as(u8, 1), table.fixed_extra_count);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    budget.limit = 1 << 20;
    for (12..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    for (0..500) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
    table.deinit();
    table_live = false;
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}

test "fixed hash growth uses funded contiguous replacement when headroom permits" {
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var table = GroupTable.init(std.testing.allocator, 0);
    table.shared = &budget;
    table.fixed_stride = 9;
    defer table.deinit();
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    var key: [9]u8 = @splat(0);
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        _ = try table.getOrInsertFixed(&key, &calls, i);
    }
    table.releaseSlack();
    const projected = table.fixedHeld() + 16 * @sizeOf(GroupTable.FixedSlot);
    try table.admit(projected - table.allocated_bytes);
    try table.growFixedSlots(32, projected);
    table.allocated_bytes = table.fixedHeld();
    try std.testing.expectEqual(@as(u8, 0), table.fixed_extra_count);
    try std.testing.expectEqual(@as(usize, 32), table.firstSlots().len);
    try std.testing.expect(budget.peak.load(.monotonic) >= projected + 16 * @sizeOf(GroupTable.FixedSlot));
    try std.testing.expectEqual(projected, budget.used.load(.monotonic));
    for (0..12) |i| {
        std.mem.writeInt(u64, key[1..9], i, .little);
        try std.testing.expectEqual(@as(?u32, @intCast(i)), table.findFixed(&key, hashFixed(&key)));
    }
}

test "group buffer overlap is funded before replacement" {
    const Counter = struct {
        budget: *SharedBudget,
        live: usize = 0,
        unpaid: usize = 0,
        fn allocator(self: *@This()) std.mem.Allocator {
            return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = std.mem.Allocator.noResize, .remap = std.mem.Allocator.noRemap, .free = free } };
        }
        fn alloc(ctx: *anyopaque, n: usize, alignment: std.mem.Alignment, ra: usize) ?[*]u8 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const p = std.testing.allocator.rawAlloc(n, alignment, ra) orelse return null;
            self.live += n;
            self.unpaid = @max(self.unpaid, self.live -| self.budget.used.load(.monotonic));
            return p;
        }
        fn free(ctx: *anyopaque, bytes: []u8, alignment: std.mem.Alignment, ra: usize) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            std.testing.allocator.rawFree(bytes, alignment, ra);
            self.live -= bytes.len;
        }
    };
    var budget: SharedBudget = .{ .limit = 1 << 20, .block = 1 };
    var counter: Counter = .{ .budget = &budget };
    const calls = [_]AggCall{.{ .func = .count, .arg = null, .where = null, .alias = "c", .result = .i64 }};
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        table.fixed_stride = 9;
        defer table.deinit();
        var key: [9]u8 = @splat(0);
        for (0..500) |i| {
            std.mem.writeInt(u64, key[1..9], i, .little);
            _ = try table.getOrInsertFixed(&key, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        var key: [32]u8 = undefined;
        for (0..500) |i| {
            const value = try std.fmt.bufPrint(&key, "variable-{d}", .{i});
            _ = try table.getOrInsert(value, &calls, i);
        }
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
    {
        const key = try std.testing.allocator.alloc(u8, 1 << 20);
        defer std.testing.allocator.free(key);
        @memset(key, 'g');
        budget.limit = 1_153_433;
        var table = GroupTable.init(counter.allocator(), 0);
        table.shared = &budget;
        defer table.deinit();
        _ = try table.getOrInsert(key, &calls, 0);
        try std.testing.expectEqual(@as(usize, 0), counter.unpaid);
        try std.testing.expect(counter.live <= budget.used.load(.monotonic));
    }
    try std.testing.expectEqual(@as(usize, 0), counter.live);
    try std.testing.expectEqual(@as(usize, 0), budget.used.load(.monotonic));
}
