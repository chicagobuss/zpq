const std = @import("std");
const filter_ast = @import("../../filter/ast.zig");
const SharedBudget = @import("../../memory.zig").SharedBudget;
const DistinctSet = @import("distinct.zig").DistinctSet;
const filter_eval = @import("../../filter/eval.zig");
const types = @import("types.zig");
const AggFunc = types.AggFunc;
const testing = std.testing;
const expr_ast = @import("../ast.zig");
const schema = @import("../../schema.zig");
const statistics = @import("../../parquet/statistics.zig");
const AggCall = @import("types.zig").AggCall;
const accumulator_mod = @import("accumulator.zig");
const Accumulator = accumulator_mod.Accumulator;
const filter_selection = @import("../../filter/selection.zig");
const ac = @import("scalar.zig");
const PreparedAggInput = ac.PreparedAggInput;
const canStatShortCircuit = ac.canStatShortCircuit;
const finalize = ac.finalize;
const foldExtremeF = ac.foldExtremeF;
const foldMinMaxBytes = ac.foldMinMaxBytes;
const simdSumI64 = ac.simdSumI64;
const sumBlocksF64 = ac.sumBlocksF64;
const testI64Meta = ac.testI64Meta;
const u64Lane = ac.u64Lane;
const updateOne = ac.updateOne;
const updateSharedInteger = ac.updateSharedInteger;
const simdMinMaxF64 = ac.simdMinMaxF64;
const statPresentCount = ac.statPresentCount;
const statsCoverageComplete = ac.statsCoverageComplete;
const sumBlocksCodedF64 = ac.sumBlocksCodedF64;
const sumBlocksGatheredF64 = ac.sumBlocksGatheredF64;
const sumConstantF64 = ac.sumConstantF64;
const updateOneFromStats = ac.updateOneFromStats;

test "regression: sumBlocksF64 gives the same bits however a row group is cut into 64-row-aligned slices" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(0x5eed);
    const rnd = prng.random();
    const n = 64 * 37 + 21;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 7, 100 }) |keep_one_in| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep_one_in == 1 or rnd.uintLessThan(u32, keep_one_in) == 0);
        const whole = sumBlocksF64(0, vals, &sel);
        for ([_]usize{ 64, 128, 64 * 5, 64 * 36 }) |slice| {
            var acc: f64 = 0;
            var lo: usize = 0;
            while (lo < n) : (lo += slice) {
                const hi = @min(n, lo + slice);
                var part = try filter_selection.SelectionVector.init(gpa, hi - lo);
                defer part.deinit();
                for (lo..hi) |i| part.set(i - lo, sel.isActive(i));
                acc = sumBlocksF64(acc, vals[lo..hi], &part);
            }
            try std.testing.expectEqual(@as(u64, @bitCast(whole)), @as(u64, @bitCast(acc)));
        }
    }
}

test "regression: sumConstantF64 is sumBlocksF64 over the constant's rows" {
    const gpa = std.testing.allocator;
    for ([_]f64{ 0.1, -3.3e-5, 1e300, 7 }) |v| for ([_]u64{ 1, 63, 64, 65, 1000, 64 * 50 }) |n| {
        const vals = try gpa.alloc(f64, n);
        defer gpa.free(vals);
        @memset(vals, v);
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        const want = sumBlocksF64(0.25, vals, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(sumConstantF64(0.25, v, n))));
    };
}

test "regression: sumBlocksCodedF64 is sumBlocksF64 over the column the codes stand for" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(9);
    const rnd = prng.random();
    const dict = [_]f64{ 1e16, -3.25, 0.1, 7e-9, -1e16 };
    const n = 64 * 9 + 13;
    const codes = try gpa.alloc(u32, n);
    defer gpa.free(codes);
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (codes, vals) |*c, *v| {
        c.* = rnd.uintLessThan(u32, dict.len);
        v.* = dict[c.*];
    }
    for ([_]u32{ 1, 3 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksCodedF64(0, codes, &dict, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}

test "regression: sumBlocksGatheredF64 sums gathered values in their rows' blocks" {
    const gpa = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(11);
    const rnd = prng.random();
    const n = 64 * 7 + 29;
    const vals = try gpa.alloc(f64, n);
    defer gpa.free(vals);
    for (vals) |*v| {
        const magnitude = std.math.pow(f64, 10, @floatFromInt(rnd.intRangeAtMost(i32, -3, 16)));
        v.* = (rnd.float(f64) - 0.5) * magnitude;
    }
    for ([_]u32{ 1, 2, 50 }) |keep| {
        var sel = try filter_selection.SelectionVector.init(gpa, n);
        defer sel.deinit();
        for (0..n) |i| sel.set(i, keep == 1 or rnd.uintLessThan(u32, keep) == 0);
        var gathered: std.ArrayList(f64) = .empty;
        defer gathered.deinit(gpa);
        for (vals, 0..) |v, i| if (sel.isActive(i)) try gathered.append(gpa, v);
        const want = sumBlocksF64(0, vals, &sel);
        const got = sumBlocksGatheredF64(0, gathered.items, &sel);
        try std.testing.expectEqual(@as(u64, @bitCast(want)), @as(u64, @bitCast(got)));
    }
}

test "canStatShortCircuit eligibility rules" {
    // count/min/max with no FILTER and no outer filter → eligible
    const a_count: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };
    const a_min_ref: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const a_min: AggCall = .{ .func = .min, .arg = a_min_ref, .where = null, .alias = "lo", .result = .i64 };
    // count(*) is answered from num_rows — eligible regardless of trust_stats.
    try testing.expect(canStatShortCircuit(a_count, false, false));
    try testing.expect(canStatShortCircuit(a_count, false, true));
    // min reads file stats → eligible ONLY under --trust-stats.
    try testing.expect(canStatShortCircuit(a_min, false, true));
    try testing.expect(!canStatShortCircuit(a_min, false, false));

    // Outer filter present → ineligible (stats reflect ALL rows)
    try testing.expect(!canStatShortCircuit(a_count, true, true));
    try testing.expect(!canStatShortCircuit(a_min, true, true));

    // Per-agg WHERE → ineligible
    const a_min_where: AggCall = .{
        .func = .min,
        .arg = a_min_ref,
        .where = .{ .int64 = .{ .col_idx = 0, .op = .GtEq, .value = 0 } },
        .alias = "lo",
        .result = .i64,
    };
    try testing.expect(!canStatShortCircuit(a_min_where, false, true));

    // sum is eligible at this stage: per-RG, updateOneFromStats
    // returns true only when min == max (constant-column case) and
    // returns false otherwise to fall through to decode. avg is
    // still never eligible (needs values to compute a meaningful
    // weighted answer).
    const a_sum: AggCall = .{ .func = .sum, .arg = a_min_ref, .where = null, .alias = "s", .result = .i64 };
    const a_avg: AggCall = .{ .func = .avg, .arg = a_min_ref, .where = null, .alias = "m", .result = .avg_f64 };
    // sum reads stats → eligible only under --trust-stats; avg never.
    try testing.expect(canStatShortCircuit(a_sum, false, true));
    try testing.expect(!canStatShortCircuit(a_sum, false, false));
    try testing.expect(!canStatShortCircuit(a_avg, false, true));

    // Outer filter / per-agg WHERE still disqualify sum.
    try testing.expect(!canStatShortCircuit(a_sum, true, true));
    const a_sum_where: AggCall = .{
        .func = .sum,
        .arg = a_min_ref,
        .where = .{ .int64 = .{ .col_idx = 0, .op = .GtEq, .value = 0 } },
        .alias = "s",
        .result = .i64,
    };
    try testing.expect(!canStatShortCircuit(a_sum_where, false, true));
}

test "boundary: an unsigned 64-bit value's INT64 lane is its low bits, as one-row min/max write it" {
    // Run in ReleaseFast too (see `u64Lane`): the narrowing this replaced answered wrong only when optimized.
    const U = struct { v: i128, lane: i64 };
    for ([_]U{
        .{ .v = 0, .lane = 0 },
        .{ .v = 1, .lane = 1 },
        .{ .v = std.math.maxInt(i64), .lane = std.math.maxInt(i64) },
        .{ .v = @as(i128, std.math.maxInt(i64)) + 1, .lane = std.math.minInt(i64) },
        .{ .v = std.math.maxInt(u64), .lane = -1 },
    }) |u| {
        try std.testing.expectEqual(u.lane, u64Lane(u.v));
        var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
        defer arena.deinit();
        const unsigned_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };
        for ([_]AggFunc{ .min, .max }) |func| {
            const call: AggCall = .{ .func = func, .arg = unsigned_arg, .where = null, .alias = "m", .result = .i64 };
            var state = Accumulator.init(call);
            if (func == .min) state.min_i = u.v else state.max_i = u.v;
            const cols = try finalize(arena.allocator(), call, state);
            try std.testing.expectEqual(u.lane, cols[0].col.i64.values[0]);
        }
    }
    // A signed column's extremes pass through unchanged, minInt and -1 included.
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const signed_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    for ([_]i64{ std.math.minInt(i64), -1, 0, std.math.maxInt(i64) }) |v| {
        const call: AggCall = .{ .func = .max, .arg = signed_arg, .where = null, .alias = "m", .result = .i64 };
        var state = Accumulator.init(call);
        state.max_i = v;
        const cols = try finalize(arena.allocator(), call, state);
        try std.testing.expectEqual(v, cols[0].col.i64.values[0]);
    }
}

test "count over all rows" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cols = [_]filter_eval.Batch.Column{};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 100 };
    const lookup = [_]?usize{};
    var sel = try filter_selection.SelectionVector.init(a, 100);

    const call: AggCall = .{
        .func = .count,
        .arg = null,
        .where = null,
        .alias = "n",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(u64, 100), state.count);
}

test "count_distinct: basic functionality over ints, strings, and nulls" {
    const a = testing.allocator;
    var set_int = DistinctSet{};
    defer set_int.deinit(a);

    const defs = [_]u32{ 1, 1, 0, 1, 1 }; // row 2 is null
    const vals = [_]i64{ 10, 20, 999, 10, 30 };
    const col: filter_eval.Batch.Column = .{
        .i64 = .{ .values = &vals, .def_levels = &defs, .max_def = 1, .has_nulls = true },
    };
    for (0..vals.len) |r| {
        try set_int.insertColRow(a, col, r);
    }
    try testing.expectEqual(@as(usize, 3), set_int.count());

    var set_other = DistinctSet{};
    defer set_other.deinit(a);
    const other_vals = [_]i64{ 30, 40, 50 };
    const col2: filter_eval.Batch.Column = .{
        .i64 = .{ .values = &other_vals },
    };
    for (0..other_vals.len) |r| {
        try set_other.insertColRow(a, col2, r);
    }
    try set_int.merge(&set_other, a);
    try testing.expectEqual(@as(usize, 5), set_int.count());
}

test "count_distinct: allocation failure leaves no leaks" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var set = DistinctSet{};
            defer set.deinit(gpa);
            const vals = [_]i64{ 1, 2, 3, 2, 1, 4, 5 };
            const col: filter_eval.Batch.Column = .{
                .i64 = .{ .values = &vals },
            };
            for (0..vals.len) |r| {
                try set.insertColRow(gpa, col, r);
            }
            try testing.expectEqual(@as(usize, 5), set.count());
        }
    }.run, .{});
}

test "count_distinct: string allocation failure leaves no leaks" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(gpa: std.mem.Allocator) !void {
            var set = DistinctSet{};
            defer set.deinit(gpa);
            const vals = [_][]const u8{ "apple", "banana", "apple", "cherry", "banana" };
            const col: filter_eval.Batch.Column = .{
                .string = .{ .values = &vals },
            };
            for (0..vals.len) |r| {
                try set.insertColRow(gpa, col, r);
            }
            try testing.expectEqual(@as(usize, 3), set.count());
        }
    }.run, .{});
}

test "count_distinct: memory budget enforcement returns ExceededMemoryBudget" {
    const a = testing.allocator;
    var budget = SharedBudget{
        .limit = 64,
        .block = 64,
    };
    var set = DistinctSet{ .shared = &budget };
    defer set.deinit(a);

    var vals: [1000]i64 = undefined;
    for (&vals, 0..) |*v, i| v.* = @intCast(i);
    const col: filter_eval.Batch.Column = .{
        .i64 = .{ .values = &vals },
    };
    var err_encountered = false;
    for (0..vals.len) |r| {
        set.insertColRow(a, col, r) catch |err| {
            try testing.expectEqual(error.ExceededMemoryBudget, err);
            err_encountered = true;
            break;
        };
    }
    try testing.expect(err_encountered);
}

test "sum i64 column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 10, 20, 30, 40, 50 };
    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "total",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(i128, 150), state.sum_i);

    const out = try finalize(a, call, state);
    try testing.expectEqual(@as(usize, 1), out.len);
    try testing.expectEqualStrings("total", out[0].name);
    try testing.expectEqual(@as(i64, 150), out[0].col.i64.values[0]);
}

test "unsigned-64 sum/min/max read the i64 lane as u64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Raw i64 bits for u64 values { 0, i64::MAX, u64::MAX }. As signed i64 the
    // last is -1; the unsigned-64 fold must read it as 18446744073709551615.
    const xs = [_]i64{ 0, std.math.maxInt(i64), -1 };
    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const u64_max_i128: i128 = std.math.maxInt(u64);
    const expectations = .{
        .{ AggFunc.sum, @as(i128, 0) + std.math.maxInt(i64) + u64_max_i128 },
        .{ AggFunc.min, @as(i128, 0) },
        .{ AggFunc.max, u64_max_i128 },
    };
    inline for (expectations) |exp| {
        var sel = try filter_selection.SelectionVector.init(a, 3); // all active
        const call: AggCall = .{
            .func = exp[0],
            .arg = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } },
            .where = null,
            .alias = "u",
            .result = .i64,
        };
        var state = Accumulator.init(call);
        try updateOne(a, a, &state, call, &batch, &lookup, &sel);
        const got: i128 = switch (state) {
            .sum_i => |v| v.?,
            .min_i => |v| v.?,
            .max_i => |v| v.?,
            else => unreachable,
        };
        try testing.expectEqual(@as(i128, exp[1]), got);
    }
}

test "min/max f64 column with selection" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]f64{ 5.0, 2.0, 8.0, 1.0, 9.0 };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);
    sel.set(0, false); // skip 5.0
    sel.set(4, false); // skip 9.0

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .DOUBLE,
        .expr_type = .f64,
    } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .f64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .f64 };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(f64, 1.0), min_state.min_f.?);
    try testing.expectEqual(@as(f64, 8.0), max_state.max_f.?);
}

test "min/max string column (bytewise/unsigned, owned winner)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // bytewise order: "apple" < "banana" < "cherry" < "date"
    const xs = [_][]const u8{ "cherry", "apple", "date", "banana" };
    const cols = [_]filter_eval.Batch.Column{.{ .string = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 4);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .BYTE_ARRAY,
        .expr_type = .str,
    } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .bytes };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .bytes };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqualStrings("apple", min_state.min_bytes.?);
    try testing.expectEqualStrings("date", max_state.max_bytes.?);
    // The winner is OWNED (dup'd into persist), not borrowed from the batch.
    try testing.expect(min_state.min_bytes.?.ptr != xs[1].ptr);
}

test "aggregates over a bool column (widened to 0/1)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]bool{ true, false, true, true, false }; // 3 trues
    const cols = [_]filter_eval.Batch.Column{.{ .boolean = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    // BOOLEAN col_ref typed as i64 (0/1), as the expr parser now resolves it.
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BOOLEAN, .expr_type = .i64 } };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 };
    const min_call: AggCall = .{ .func = .min, .arg = arg, .where = null, .alias = "mn", .result = .i64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg, .where = null, .alias = "mx", .result = .i64 };

    var s = Accumulator.init(sum_call);
    var mn = Accumulator.init(min_call);
    var mx = Accumulator.init(max_call);
    try updateOne(a, a, &s, sum_call, &batch, &lookup, &sel);
    try updateOne(a, a, &mn, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &mx, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(i128, 3), s.sum_i); // sum = #trues
    try testing.expectEqual(@as(i64, 0), mn.min_i.?); // min = 0 (a false present)
    try testing.expectEqual(@as(i64, 1), mx.max_i.?); // max = 1 (a true present)
}

test "prepared aggregate input owns its widened copy and nothing it borrows" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const narrow = [_]i32{ 1, 2, 3 };
    const wide = [_]i64{ 4, 5, 6 };
    const defs = [_]u32{ 1, 0, 1 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .i32 = .{ .values = &narrow } },
        .{ .i64 = .{ .values = &wide, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };
    var sel = try filter_selection.SelectionVector.init(a, 3);

    const narrow_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const sum_narrow: AggCall = .{ .func = .sum, .arg = narrow_arg, .where = null, .alias = "s", .result = .i64 };
    var widened: PreparedAggInput = try .init(a, sum_narrow, &batch, &lookup, &sel, true);
    try testing.expect(widened.values_owned);
    try testing.expect(widened.own_sel == null);
    try testing.expectEqual(&sel, widened.selection());
    const copy = widened.values.?.i64.values.ptr;
    widened.deinit(a);
    // Freed newest first, so the arena hands the same bytes to the next row group's widen.
    try testing.expectEqual(copy, (try a.alloc(i64, 3)).ptr);

    const wide_arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const sum_wide: AggCall = .{ .func = .sum, .arg = wide_arg, .where = null, .alias = "s", .result = .i64 };
    var borrowed: PreparedAggInput = try .init(a, sum_wide, &batch, &lookup, &sel, true);
    defer borrowed.deinit(a);
    try testing.expect(!borrowed.values_owned);
    try testing.expectEqual(@as([*]const i64, &wide), borrowed.values.?.i64.values.ptr);
    // The null row leaves the call's own selection, not the shared one.
    try testing.expect(!borrowed.selection().isActive(1));
    try testing.expect(sel.isActive(1));
}

test "avg emits its mean; avg_state the sum + count pair" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]f64{ 1.0, 2.0, 3.0, 4.0 };
    const cols = [_]filter_eval.Batch.Column{.{ .f64 = .{ .values = &xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 4);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .DOUBLE,
        .expr_type = .f64,
    } };
    const call: AggCall = .{
        .func = .avg,
        .arg = arg_expr,
        .where = null,
        .alias = "mean_x",
        .result = .avg_f64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(f64, 10.0), state.avg.sum);
    try testing.expectEqual(@as(u64, 4), state.avg.count);

    const mean = try finalize(a, call, state);
    try testing.expectEqual(@as(usize, 1), mean.len);
    try testing.expectEqualStrings("mean_x", mean[0].name);
    try testing.expectEqual(schema.Type.DOUBLE, mean[0].parquet_type);
    try testing.expect(mean[0].nullable);
    try testing.expectEqual(@as(f64, 2.5), mean[0].col.f64.values[0]);

    var state_call = call;
    state_call.state = true;
    const out = try finalize(a, state_call, state);
    try testing.expectEqual(@as(usize, 2), out.len);
    try testing.expectEqualStrings("mean_x__sum", out[0].name);
    try testing.expectEqualStrings("mean_x__count", out[1].name);
    try testing.expectEqual(@as(f64, 10.0), out[0].col.f64.values[0]);
    try testing.expectEqual(@as(i64, 4), out[1].col.i64.values[0]);
}

test "updateOneFromStats sum: constant-column RG folds num_rows × value" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Constant column: min == max == 42, 100 rows → sum = 4200.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], 42, .little);
    std.mem.writeInt(i64, max_buf[0..8], 42, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .min_value = min_buf, .max_value = max_buf, .null_count = 0 },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg_expr, .where = null, .alias = "s", .result = .i64 };
    var sum_state = Accumulator.init(sum_call);

    try testing.expect(try updateOneFromStats(&sum_state, sum_call, &rg, &test_meta));
    try testing.expectEqual(@as(i128, 4200), sum_state.sum_i);

    // OPTIONAL constant column with missing null_count is not safe:
    // min == max only describes present values, not how many rows are null.
    rg.columns.items[0].meta_data.?.statistics.?.null_count = null;
    const optional_meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    var optional_state = Accumulator.init(sum_call);
    try testing.expectEqual(false, try updateOneFromStats(&optional_state, sum_call, &rg, &optional_meta));
    rg.columns.items[0].meta_data.?.statistics.?.null_count = 0;

    // Non-constant RG (min != max): updateOneFromStats returns false
    // → caller falls through to decode.
    var hi_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, hi_buf[0..8], 100, .little);
    rg.columns.items[0].meta_data.?.statistics.?.max_value = hi_buf;
    var state2 = Accumulator.init(sum_call);
    try testing.expectEqual(false, try updateOneFromStats(&state2, sum_call, &rg, &test_meta));
    try testing.expectEqual(@as(?i128, null), state2.sum_i);
}

test "updateOneFromStats: an all-null row group leaves sum/min/max NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Bounds a writer left on a chunk whose every row is null must not become a value (or a 0 sum).
    const bound = try a.alloc(u8, 8);
    std.mem.writeInt(i64, bound[0..8], 7, .little);
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var col_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = .empty,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .min_value = bound, .max_value = bound, .null_count = 100 },
        },
    });
    const rg: schema.RowGroup = .{ .columns = col_chunks, .total_byte_size = 0, .num_rows = 100 };
    const meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };

    inline for (.{ AggFunc.sum, AggFunc.min, AggFunc.max }) |func| {
        const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "x", .result = .i64 };
        var state = Accumulator.init(call);
        try testing.expect(try updateOneFromStats(&state, call, &rg, &meta));
        const v: ?i128 = switch (state) {
            .sum_i, .min_i, .max_i => |x| x,
            else => unreachable,
        };
        try testing.expectEqual(@as(?i128, null), v);
    }
}

test "updateOneFromStats decodes typed min/max bytes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Build a synthetic RG with one INT64 column-chunk + stats.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], -42, .little);
    std.mem.writeInt(i64, max_buf[0..8], 1000, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{
                .min_value = min_buf,
                .max_value = max_buf,
            },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .i64 };
    const max_call: AggCall = .{ .func = .max, .arg = arg_expr, .where = null, .alias = "hi", .result = .i64 };

    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);
    try testing.expect(try updateOneFromStats(&min_state, min_call, &rg, &test_meta));
    try testing.expect(try updateOneFromStats(&max_state, max_call, &rg, &test_meta));
    try testing.expectEqual(@as(i64, -42), min_state.min_i.?);
    try testing.expectEqual(@as(i64, 1000), max_state.max_i.?);
}

test "updateOneFromStats returns false when stats absent" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null, // ← no stats
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const min_call: AggCall = .{ .func = .min, .arg = arg_expr, .where = null, .alias = "lo", .result = .i64 };

    var state = Accumulator.init(min_call);
    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);
    try testing.expectEqual(false, try updateOneFromStats(&state, min_call, &rg, &test_meta));
}

test "updateOneFromStats: count(*) uses num_rows; count(col) uses num_rows - null_count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    var col_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{ .null_count = 17 },
        },
    });
    const rg: schema.RowGroup = .{
        .columns = col_chunks,
        .total_byte_size = 800,
        .num_rows = 100,
    };

    const test_meta = try testI64Meta(a, .empty, 100, .REQUIRED);

    // count(*) → 100
    const star_call: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };
    var star_state = Accumulator.init(star_call);
    try testing.expect(try updateOneFromStats(&star_state, star_call, &rg, &test_meta));
    try testing.expectEqual(@as(u64, 100), star_state.count);

    // Counts that add up past i64 across files (each footer is bounded, their sum is not) are an error, not a
    // saturated count nor one count() cannot answer in: from statistics, and when per-file accumulators merge.
    var near_full = Accumulator.init(star_call);
    near_full.count = std.math.maxInt(i64) - 50;
    try testing.expectError(error.RowCountTooLarge, updateOneFromStats(&near_full, star_call, &rg, &test_meta));
    near_full.count = std.math.maxInt(u64) - 50;
    try testing.expectError(error.RowCountTooLarge, updateOneFromStats(&near_full, star_call, &rg, &test_meta));
    near_full.count = std.math.maxInt(i64) - 100;
    try testing.expect(try updateOneFromStats(&near_full, star_call, &rg, &test_meta));
    try testing.expectEqual(@as(u64, std.math.maxInt(i64)), near_full.count);
    var merged = Accumulator.init(star_call);
    merged.count = std.math.maxInt(i64) - 50;
    try testing.expectError(error.RowCountTooLarge, merged.merge(star_state, a));
    merged.count = 7;
    try merged.merge(star_state, a);
    try testing.expectEqual(@as(u64, 107), merged.count);

    // count(x) → 100 - 17 = 83
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const col_call: AggCall = .{ .func = .count, .arg = arg, .where = null, .alias = "n", .result = .i64 };
    var col_state = Accumulator.init(col_call);
    try testing.expect(try updateOneFromStats(&col_state, col_call, &rg, &test_meta));
    try testing.expectEqual(@as(u64, 83), col_state.count);

    // REQUIRED count(x) does not need null_count; every row is present.
    var col_chunks2: std.ArrayList(schema.ColumnChunk) = .empty;
    try col_chunks2.append(a, .{
        .file_path = null,
        .file_offset = 0,
        .meta_data = .{
            .type = .INT64,
            .encodings = encs,
            .path_in_schema = path,
            .codec = .UNCOMPRESSED,
            .num_values = 100,
            .total_uncompressed_size = 800,
            .total_compressed_size = 800,
            .data_page_offset = 0,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = .{}, // present but no null_count
        },
    });
    const rg2: schema.RowGroup = .{
        .columns = col_chunks2,
        .total_byte_size = 800,
        .num_rows = 100,
    };
    var col_state2 = Accumulator.init(col_call);
    try testing.expect(try updateOneFromStats(&col_state2, col_call, &rg2, &test_meta));
    try testing.expectEqual(@as(u64, 100), col_state2.count);
}

test "statsCoverageComplete: prunable iff every RG has the right stat field" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Build a 2-RG file: RG0 has min/max for col 0, RG1 doesn't.
    var min_buf = try a.alloc(u8, 8);
    var max_buf = try a.alloc(u8, 8);
    std.mem.writeInt(i64, min_buf[0..8], 1, .little);
    std.mem.writeInt(i64, max_buf[0..8], 100, .little);

    var path: schema.StringList = .empty;
    try path.append(a, "x");
    var encs: schema.EncodingList = .empty;
    try encs.append(a, .PLAIN);

    const cm_with_stats: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = encs,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = .{ .min_value = min_buf, .max_value = max_buf },
    };
    const cm_without_stats: schema.ColumnMetaData = .{
        .type = .INT64,
        .encodings = encs,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    };

    var rg0_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try rg0_chunks.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = cm_with_stats });
    var rg1_chunks: std.ArrayList(schema.ColumnChunk) = .empty;
    try rg1_chunks.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = cm_without_stats });

    var rgs_complete: std.ArrayList(schema.RowGroup) = .empty;
    try rgs_complete.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    try rgs_complete.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    var rgs_partial: std.ArrayList(schema.RowGroup) = .empty;
    try rgs_partial.append(a, .{ .columns = rg0_chunks, .total_byte_size = 800, .num_rows = 100 });
    try rgs_partial.append(a, .{ .columns = rg1_chunks, .total_byte_size = 800, .num_rows = 100 });

    const meta_complete = try testI64Meta(a, rgs_complete, 200, .REQUIRED);
    const meta_partial = try testI64Meta(a, rgs_partial, 200, .REQUIRED);

    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const max_call: AggCall = .{ .func = .max, .arg = arg, .where = null, .alias = "hi", .result = .i64 };
    const sum_call: AggCall = .{ .func = .sum, .arg = arg, .where = null, .alias = "s", .result = .i64 };
    const star_call: AggCall = .{ .func = .count, .arg = null, .where = null, .alias = "n", .result = .i64 };

    // max with full stats coverage → prunable, but ONLY under --trust-stats
    // (reading min/max stats for the answer). Untrusted → must decode → not
    // prunable, so the column can't be dropped from the fetch set.
    try testing.expect(statsCoverageComplete(max_call, &.{meta_complete}, 0, true));
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_complete}, 0, false));
    // max with one RG missing stats → NOT prunable (even when trusting)
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_partial}, 0, true));
    // sum is only prunable when every RG is constant; these stats are variable.
    try testing.expect(!statsCoverageComplete(sum_call, &.{meta_complete}, 0, true));
    const left = try a.create(expr_ast.Expr);
    left.* = arg;
    const right = try a.create(expr_ast.Expr);
    right.* = .{ .literal = .{ .i64 = 1 } };
    const computed_arg: expr_ast.Expr = .{ .binop = .{
        .op = .add,
        .left = left,
        .right = right,
        .result_type = .i64,
        .depth = 1 + arg.depth(),
    } };
    const computed_sum: AggCall = .{ .func = .sum, .arg = computed_arg, .where = null, .alias = "s", .result = .i64 };
    try testing.expect(!statsCoverageComplete(computed_sum, &.{meta_complete}, 0, true));
    // count(*) is answered from num_rows — always prunable, trust or not.
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, true));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, false));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_partial}, 0, false));

    // `updateOneFromStats` never folds an unsigned column, so its stats cannot let the column go unfetched.
    meta_complete.schema.items[1].converted_type = .UINT_64;
    try testing.expect(!statsCoverageComplete(max_call, &.{meta_complete}, 0, true));
    try testing.expect(statsCoverageComplete(star_call, &.{meta_complete}, 0, true));

    // FLOAT16 decodes to the f64 lane, but no fold reads its 2-byte bounds: it too must stay fetched.
    const leaf = &meta_complete.schema.items[1];
    leaf.converted_type = null;
    leaf.type = .FIXED_LEN_BYTE_ARRAY;
    leaf.type_length = 2;
    leaf.logical_type = .{ .FLOAT16 = .{} };
    const half: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .FIXED_LEN_BYTE_ARRAY,
        .expr_type = .f64,
    } };
    const half_max: AggCall = .{ .func = .max, .arg = half, .where = null, .alias = "hi", .result = .f64 };
    try testing.expect(!statsCoverageComplete(half_max, &.{meta_complete}, 0, true));
}

test "simdSumI64 widens correctly across positive and negative i64 extremes" {
    // Mix of full-range i64 values that cancel close to zero in true
    // arithmetic but would wrap into i64::MAX/MIN territory if
    // accumulated as i64. The expected sum stays well within i64,
    // so finalize won't error — this isolates the kernel itself.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const N: usize = 256;
    const xs = try a.alloc(i64, N);
    var i: usize = 0;
    while (i < N) : (i += 1) {
        // Alternating ±(2^62 + 1). Pairwise sum stays bounded so the
        // i64-accumulator path could pass; but if a kernel does e.g.
        // sum-then-narrow, it'd see (2^62 + 1) + (2^62 + 1) = 2^63 + 2
        // (overflow) on adjacent positives. The widening path stays
        // exact.
        const v: i64 = (@as(i64, 1) << 62) + 1;
        xs[i] = if (i % 2 == 0) v else -v;
    }
    var sel = try filter_selection.SelectionVector.init(a, N);
    const got = simdSumI64(xs, &sel);
    try testing.expectEqual(@as(i128, 0), got);

    // Asymmetric: 200 positives, 56 negatives — true sum 144 × (2^62+1).
    for (xs[0..200]) |*x| x.* = (@as(i64, 1) << 62) + 1;
    for (xs[200..]) |*x| x.* = -((@as(i64, 1) << 62) + 1);
    const got2 = simdSumI64(xs, &sel);
    const expected2: i128 = @as(i128, 144) * (@as(i128, 1 << 62) + 1);
    try testing.expectEqual(expected2, got2);
}

test "sum i64 stays exact on values that would overflow a vector i64 accumulator" {
    // Per-lane overflow regression: 128 copies of (i64::MAX / 2 + 1).
    // True sum is 128 × (i64::MAX / 2 + 1) ≈ 2^69, well outside i64.
    // The previous SIMD path (`@Vector(4, i64)` running accumulator)
    // wrapped after just two iterations on this input. The current
    // path widens every add to i128 so the result is exact.
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const N: usize = 128;
    const big: i64 = (std.math.maxInt(i64) / 2) + 1;
    const xs = try a.alloc(i64, N);
    for (xs) |*x| x.* = big;

    const cols = [_]filter_eval.Batch.Column{.{ .i64 = .{ .values = xs } }};
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = N };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, N);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "s",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);

    const expected: i128 = @as(i128, N) * @as(i128, big);
    try testing.expectEqual(expected, state.sum_i);
    // Also verify the unsigned magnitude exceeds i64::MAX so this
    // really is the overflow regime (defensive — catches the case
    // where a future refactor accidentally narrows somewhere).
    try testing.expect(expected > std.math.maxInt(i64));
}

test "sum/count over OPTIONAL i64 column with real nulls" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Column is OPTIONAL, max_def = 1. Rows 1 and 3 are null
    // (def == 0); rows 0/2/4 are present with values 10/30/50.
    // Placeholder values at null positions are deliberately huge
    // so a buggy fold (one that ignores def_levels) would visibly
    // produce a wrong answer.
    const xs = [_]i64{ 10, 999_999, 30, 999_999, 50 };
    const dls = [_]u32{ 1, 0, 1, 0, 1 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .i64 = .{
            .values = &xs,
            .def_levels = &dls,
            .max_def = 1,
            .has_nulls = true,
        } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 0,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    const sum_call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = null,
        .alias = "s",
        .result = .i64,
    };
    const count_call: AggCall = .{
        .func = .count,
        .arg = arg_expr,
        .where = null,
        .alias = "n",
        .result = .i64,
    };
    const min_call: AggCall = .{
        .func = .min,
        .arg = arg_expr,
        .where = null,
        .alias = "lo",
        .result = .i64,
    };
    const max_call: AggCall = .{
        .func = .max,
        .arg = arg_expr,
        .where = null,
        .alias = "hi",
        .result = .i64,
    };

    var sum_state = Accumulator.init(sum_call);
    var count_state = Accumulator.init(count_call);
    var min_state = Accumulator.init(min_call);
    var max_state = Accumulator.init(max_call);
    try updateOne(a, a, &sum_state, sum_call, &batch, &lookup, &sel);
    try updateOne(a, a, &count_state, count_call, &batch, &lookup, &sel);
    try updateOne(a, a, &min_state, min_call, &batch, &lookup, &sel);
    try updateOne(a, a, &max_state, max_call, &batch, &lookup, &sel);

    try testing.expectEqual(@as(i128, 90), sum_state.sum_i);
    try testing.expectEqual(@as(u64, 3), count_state.count);
    try testing.expectEqual(@as(i64, 10), min_state.min_i.?);
    try testing.expectEqual(@as(i64, 50), max_state.max_i.?);
}

test "conditional agg via FILTER predicate" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // foo: [bar, baz, bar, baz, bar]
    // cost: [10, 20, 30, 40, 50]
    // sum(cost) FILTER (WHERE foo='bar') should be 10+30+50 = 90
    const foo_vals = [_][]const u8{ "bar", "baz", "bar", "baz", "bar" };
    const cost_vals = [_]i64{ 10, 20, 30, 40, 50 };
    const cols = [_]filter_eval.Batch.Column{
        .{ .string = .{ .values = &foo_vals } },
        .{ .i64 = .{ .values = &cost_vals } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{ 0, 1 };
    var sel = try filter_selection.SelectionVector.init(a, 5);

    const arg_expr: expr_ast.Expr = .{ .col_ref = .{
        .col_idx = 1,
        .physical_type = .INT64,
        .expr_type = .i64,
    } };
    // WHERE foo='bar' as a filter AST predicate.
    const where: filter_ast.Filter = .{ .string = .{
        .col_idx = 0,
        .op = .Eq,
        .value = "bar",
    } };
    const call: AggCall = .{
        .func = .sum,
        .arg = arg_expr,
        .where = where,
        .alias = "bar_total",
        .result = .i64,
    };
    var state = Accumulator.init(call);
    try updateOne(a, a, &state, call, &batch, &lookup, &sel);
    try testing.expectEqual(@as(i128, 90), state.sum_i);
}

test "sum/avg/min/max over no values are NULL; count is 0" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Column 0: all null. Column 1: present, but rows are deselected below. Column 2: strings.
    const nulls = [_]f64{ 7, 7, 7 };
    const zeros = [_]u32{ 0, 0, 0 };
    const ints = [_]i64{ 1, 2, 3 };
    const strs = [_][]const u8{ "a", "b", "c" };
    const cols = [_]filter_eval.Batch.Column{
        .{ .f64 = .{ .values = &nulls, .def_levels = &zeros, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = &ints } },
        .{ .string = .{ .values = &strs } },
    };
    const batch: filter_eval.Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1, 2 };
    var all = try filter_selection.SelectionVector.init(a, 3);
    var none = try filter_selection.SelectionVector.init(a, 3);
    for (0..3) |i| none.set(i, false);

    const f_null: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const i_col: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const s_col: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const never: filter_ast.Filter = .{ .int64 = .{ .col_idx = 1, .op = .Lt, .value = 0 } };
    const Case = struct { call: AggCall, sel: *filter_selection.SelectionVector };
    const cases = [_]Case{
        // all-null input
        .{ .call = .{ .func = .sum, .arg = f_null, .where = null, .alias = "a", .result = .f64 }, .sel = &all },
        .{ .call = .{ .func = .min, .arg = f_null, .where = null, .alias = "b", .result = .f64 }, .sel = &all },
        .{ .call = .{ .func = .avg, .arg = f_null, .where = null, .alias = "c", .result = .avg_f64 }, .sel = &all },
        .{
            .call = .{ .func = .avg, .arg = f_null, .where = null, .alias = "c", .result = .avg_f64, .state = true },
            .sel = &all,
        },
        .{ .call = .{ .func = .avg, .arg = i_col, .where = never, .alias = "c", .result = .avg_f64 }, .sel = &all },
        // zero rows selected by the outer filter
        .{ .call = .{ .func = .sum, .arg = i_col, .where = null, .alias = "d", .result = .i64 }, .sel = &none },
        .{ .call = .{ .func = .max, .arg = i_col, .where = null, .alias = "e", .result = .i64 }, .sel = &none },
        .{ .call = .{ .func = .min, .arg = s_col, .where = null, .alias = "f", .result = .bytes }, .sel = &none },
        // a FILTER clause that selects nothing
        .{ .call = .{ .func = .sum, .arg = i_col, .where = never, .alias = "g", .result = .i64 }, .sel = &all },
        .{ .call = .{ .func = .max, .arg = s_col, .where = never, .alias = "h", .result = .bytes }, .sel = &all },
    };
    for (cases) |c| {
        var state = Accumulator.init(c.call);
        try updateOne(a, a, &state, c.call, &batch, &lookup, c.sel);
        // A second, equally empty partial merged in must not turn NULL into 0.
        try state.merge(Accumulator.init(c.call), a);
        const outs = try finalize(a, c.call, state);
        // avg_state's count column stays a REQUIRED 0; every other output, avg's mean too, is a NULL.
        for (outs) |o| {
            if (std.mem.endsWith(u8, o.name, "__count")) {
                try testing.expect(!o.nullable);
                try testing.expectEqual(@as(i64, 0), o.col.i64.values[0]);
                continue;
            }
            try testing.expect(o.nullable);
            switch (o.col) {
                inline else => |col| {
                    try testing.expect(col.has_nulls);
                    try testing.expectEqualSlices(u32, &.{0}, col.def_levels.?);
                },
            }
        }
    }

    // count(*) and count(col) over the same inputs are 0, not NULL.
    const count_col: AggCall = .{ .func = .count, .arg = f_null, .where = null, .alias = "n", .result = .i64 };
    var cs = Accumulator.init(count_col);
    try updateOne(a, a, &cs, count_col, &batch, &lookup, &all);
    const cout = try finalize(a, count_col, cs);
    try testing.expect(!cout[0].nullable);
    try testing.expectEqual(@as(i64, 0), cout[0].col.i64.values[0]);

    // NULL is the identity for merging partials: NULL + 6 = 6.
    const sum_call: AggCall = .{ .func = .sum, .arg = i_col, .where = null, .alias = "s", .result = .i64 };
    var empty = Accumulator.init(sum_call);
    var full = Accumulator.init(sum_call);
    try updateOne(a, a, &full, sum_call, &batch, &lookup, &all);
    try empty.merge(full, a);
    try testing.expectEqual(@as(?i128, 6), empty.sum_i);
}

/// Both NaN, or the same bits (so -0 and +0 differ).
fn sameF(want: f64, got: f64) bool {
    if (std.math.isNan(want)) return std.math.isNan(got);
    return @as(u64, @bitCast(want)) == @as(u64, @bitCast(got));
}

test "string min/max by 16-byte prefix agree with a byte-by-byte fold: shared prefixes, zero bytes, nulls" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var prng = std.Random.DefaultPrng.init(0xb17e);
    const r = prng.random();
    const alphabet = [_]u8{ 0, 1, 'a', 'b', 0xff };
    for (0..200) |_| {
        const n = r.uintLessThan(usize, 300) + 1;
        const vals = try a.alloc([]const u8, n);
        const defs = try a.alloc(u32, n);
        for (vals, defs) |*v, *d| {
            const len = r.uintLessThan(usize, 22);
            const buf = try a.alloc(u8, len);
            // A long shared prefix in most rows, so the full compare runs too.
            for (buf, 0..) |*c, k| {
                c.* = if (k < 17 and r.uintLessThan(u8, 6) != 0) 'a' else alphabet[r.uintLessThan(usize, alphabet.len)];
            }
            v.* = buf;
            d.* = @intFromBool(r.uintLessThan(u8, 5) != 0);
        }
        var sel = try filter_selection.SelectionVector.init(a, n);
        for (0..n) |i| if (r.uintLessThan(u8, 3) == 0) sel.set(i, false);
        const col: filter_eval.Batch.Column = .{ .string = .{ .values = vals, .def_levels = defs, .max_def = 1 } };
        inline for (.{ true, false }) |is_min| {
            var want: ?[]const u8 = null;
            for (vals, defs, 0..) |v, d, i| {
                if (!sel.isActive(i) or d != 1) continue;
                const ord = if (want) |w| std.mem.order(u8, v, w) else .lt;
                if (want == null or (is_min and ord == .lt) or (!is_min and ord == .gt)) want = v;
            }
            var state: Accumulator = if (is_min) .{ .min_bytes = null } else .{ .max_bytes = null };
            try foldMinMaxBytes(a, is_min, &state, col, &sel);
            const got = if (is_min) state.min_bytes else state.max_bytes;
            if (want) |w| try testing.expectEqualSlices(u8, w, got.?) else try testing.expect(got == null);
        }
    }
}

test "float min/max: NaN above every number, -0 below +0, the same in any order and on every path" {
    const nan = std.math.nan(f64);
    const inf = std.math.inf(f64);
    const cases = [_]struct { vals: []const f64, lo: f64, hi: f64 }{
        .{ .vals = &.{ nan, 1, 3 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ 3, nan, 1 }, .lo = 1, .hi = nan },
        .{ .vals = &.{ nan, nan }, .lo = nan, .hi = nan },
        .{ .vals = &.{ 0.0, -0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -0.0, 0.0 }, .lo = -0.0, .hi = 0.0 },
        .{ .vals = &.{ -inf, inf, nan }, .lo = -inf, .hi = nan },
        .{ .vals = &.{ -inf, -inf }, .lo = -inf, .hi = -inf },
    };
    var prng = std.Random.DefaultPrng.init(0x7a7);
    const r = prng.random();
    for (cases) |c| for (0..20) |_| {
        // Values in a random order (each at least once), past 64 rows so the SIMD lanes run; folded one at a time,
        // through the kernel, and as two halves merged: all give the same answer.
        var vals: [130]f64 = undefined;
        for (&vals) |*v| v.* = c.vals[r.uintLessThan(usize, c.vals.len)];
        for (c.vals, 0..) |v, k| vals[k * 7] = v;
        var sel = try filter_selection.SelectionVector.init(std.testing.allocator, vals.len);
        defer sel.deinit();
        inline for (.{ true, false }) |is_min| {
            const want = if (is_min) c.lo else c.hi;
            var one: ?f64 = null;
            for (vals) |v| foldExtremeF(is_min, &one, v);
            var half_a: ?f64 = null;
            var half_b: ?f64 = null;
            for (vals[0..65]) |v| foldExtremeF(is_min, &half_a, v);
            for (vals[65..]) |v| foldExtremeF(is_min, &half_b, v);
            foldExtremeF(is_min, &half_b, half_a.?);
            for ([_]f64{ one.?, half_b.?, simdMinMaxF64(is_min, &vals, &sel).? }) |got| {
                if (!sameF(want, got)) {
                    std.debug.print("{s} of {any}: want {d}, got {d}\n", .{ if (is_min) "min" else "max", c.vals, want, got });
                    return error.TestUnexpectedResult;
                }
            }
        }
    };
}

test "zero float bounds never prove sign even with known zero NaNs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    inline for (.{ schema.Type.FLOAT, schema.Type.DOUBLE }) |physical| {
        const T = if (physical == .FLOAT) f32 else f64;
        const U = if (physical == .FLOAT) u32 else u64;
        var meta = try testI64Meta(a, .empty, 100, .REQUIRED);
        meta.schema.items[1].type = physical;
        const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = physical, .expr_type = .f64 } };
        const Case = struct { lo: T, hi: T, min_folds: bool, max_folds: bool };
        for ([_]Case{
            .{ .lo = -0.0, .hi = 0.0, .min_folds = false, .max_folds = false },
            .{ .lo = 0.0, .hi = 1.0, .min_folds = false, .max_folds = true },
            .{ .lo = -1.0, .hi = -0.0, .min_folds = true, .max_folds = false },
            .{ .lo = 1.5, .hi = 2.5, .min_folds = true, .max_folds = true },
        }) |c| {
            var lo: [@sizeOf(T)]u8 = undefined;
            var hi: [@sizeOf(T)]u8 = undefined;
            std.mem.writeInt(U, &lo, @bitCast(c.lo), .little);
            std.mem.writeInt(U, &hi, @bitCast(c.hi), .little);
            var path: schema.StringList = .empty;
            try path.append(a, "x");
            var chunks: std.ArrayList(schema.ColumnChunk) = .empty;
            try chunks.append(a, .{
                .file_path = null,
                .file_offset = 0,
                .meta_data = .{
                    .type = physical,
                    .encodings = .empty,
                    .path_in_schema = path,
                    .codec = .UNCOMPRESSED,
                    .num_values = 100,
                    .total_uncompressed_size = 0,
                    .total_compressed_size = 0,
                    .data_page_offset = 0,
                    .index_page_offset = null,
                    .dictionary_page_offset = null,
                    .statistics = .{ .min_value = &lo, .max_value = &hi, .null_count = 0, .nan_count = 0 },
                },
            });
            const rg: schema.RowGroup = .{ .columns = chunks, .total_byte_size = 0, .num_rows = 100 };
            inline for (.{ AggFunc.min, AggFunc.max }) |func| {
                const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "x", .result = .f64 };
                var state = Accumulator.init(call);
                try testing.expectEqual(if (func == .min) c.min_folds else c.max_folds, try updateOneFromStats(&state, call, &rg, &meta));
            }
        }
    }
}

test "boundary: a footer null count outside the row group's rows answers no count from statistics" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var path: schema.StringList = .empty;
    try path.append(a, "x");
    const meta = try testI64Meta(a, .empty, 100, .OPTIONAL);
    var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 800, .num_rows = 100 };
    try rg.columns.append(a, .{ .file_path = null, .file_offset = 0, .meta_data = .{
        .type = .INT64,
        .encodings = .empty,
        .path_in_schema = path,
        .codec = .UNCOMPRESSED,
        .num_values = 100,
        .total_uncompressed_size = 800,
        .total_compressed_size = 800,
        .data_page_offset = 0,
        .index_page_offset = null,
        .dictionary_page_offset = null,
        .statistics = null,
    } });
    const cm = &rg.columns.items[0].meta_data.?;
    const Case = struct { nc: i64, want: ?i64 };
    for ([_]Case{
        .{ .nc = 0, .want = 100 },
        .{ .nc = 100, .want = 0 },
        .{ .nc = 37, .want = 63 },
        .{ .nc = 101, .want = null },
        .{ .nc = -1, .want = null },
        .{ .nc = std.math.minInt(i64), .want = null },
        .{ .nc = std.math.maxInt(i64), .want = null },
    }) |case| {
        cm.statistics = .{ .null_count = case.nc };
        try testing.expectEqual(case.want, statPresentCount(&rg, 0, cm, &meta));
    }
}

test "regression: a one-row min/max of a UINT64 column keeps values of 2^63 and up as the column's bits" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };
    for ([_]AggFunc{ .min, .max }) |func| {
        const call: AggCall = .{ .func = func, .arg = arg, .where = null, .alias = "m", .result = .i64 };
        var state = Accumulator.init(call);
        if (func == .min) state.min_i = std.math.maxInt(u64) else state.max_i = std.math.maxInt(u64);
        const cols = try finalize(a, call, state);
        try std.testing.expectEqual(@as(i64, -1), cols[0].col.i64.values[0]);
    }
}

test "shared integer scalar folds preserve exact lanes, NULLs, selections and refusal before mutation" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, testSharedInteger, .{});
}

test "shared integer duplicate counts decline levels-only and short value lanes before mutation" {
    const a = std.testing.allocator;
    const n = 193;
    var defs: [n]u32 = undefined;
    var vals: [n]i64 = @splat(7);
    var narrow: [n]i32 = @splat(7);
    for (&defs, 0..) |*d, i| d.* = @intFromBool(i % 11 != 0);
    for ([_]bool{ false, true }) |is32| for ([_]usize{ 0, n - 1 }) |len| {
        const col: filter_eval.Batch.Column = if (is32)
            .{ .i32 = .{
                .values = narrow[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } }
        else
            .{ .i64 = .{
                .values = vals[0..len],
                .def_levels = &defs,
                .max_def = 1,
                .has_nulls = true,
                .levels_only_rows = if (len == 0) n else 0,
            } };
        const arg: expr_ast.Expr = .{ .col_ref = .{
            .col_idx = 0,
            .physical_type = if (is32) .INT32 else .INT64,
            .expr_type = .i64,
            .nullable = true,
        } };
        const calls = [_]AggCall{
            .{ .func = .count, .arg = arg, .where = null, .alias = "a", .result = .i64 },
            .{ .func = .count, .arg = arg, .where = null, .alias = "b", .result = .i64 },
        };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..3) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                else => r % 3 != 0,
            });
            var states: [2]Accumulator = .{ .{ .count = 7 }, .{ .count = 19 } };
            const before = states;
            try std.testing.expect(!try updateSharedInteger(
                a,
                &calls,
                &states,
                &.{ false, false },
                &batch,
                &.{0},
                &sel,
            ));
            try std.testing.expectEqualDeep(before, states);
            if (len == 0) {
                var present: u64 = 0;
                for (defs, 0..) |d, r| if (d != 0 and sel.isActive(r)) {
                    present += 1;
                };
                for (calls, &states) |call, *state| try updateOne(a, a, state, call, &batch, &.{0}, &sel);
                try std.testing.expectEqual(7 + present, states[0].count);
                try std.testing.expectEqual(19 + present, states[1].count);
            }
        }
    };
}

fn testSharedInteger(a: std.mem.Allocator) !void {
    const n = 193;
    var values: [n]i64 = undefined;
    var narrow: [n]i32 = undefined;
    var defs: [n]u32 = undefined;
    for (&values, &narrow, &defs, 0..) |*v, *v32, *d, r| {
        v.* = switch (r % 5) {
            0 => std.math.minInt(i64),
            1 => std.math.maxInt(i64),
            else => @as(i64, @intCast(r)) - 97,
        };
        v32.* = @intCast(@as(i64, @intCast(r)) - 97);
        d.* = @intFromBool(r % 11 != 0);
    }
    const arg: expr_ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const calls = [_]AggCall{
        .{ .func = .count, .arg = null, .result = .i64, .where = null, .alias = "all" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "s", .offset = -17 },
        .{ .func = .min, .arg = arg, .result = .i64, .where = null, .alias = "lo" },
        .{ .func = .max, .arg = arg, .result = .i64, .where = null, .alias = "hi" },
        .{ .func = .avg, .arg = arg, .result = .avg_f64, .where = null, .alias = "a" },
        .{ .func = .count, .arg = arg, .result = .i64, .where = null, .alias = "present" },
        .{ .func = .sum, .arg = arg, .result = .i64, .where = null, .alias = "again", .offset = 31 },
    };
    for ([_]bool{ false, true }) |is_narrow| {
        const col: filter_eval.Batch.Column = if (is_narrow)
            .{ .i32 = .{ .values = &narrow, .def_levels = &defs, .max_def = 1, .has_nulls = true } }
        else
            .{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1, .has_nulls = true } };
        const batch: filter_eval.Batch = .{ .cols = &.{col}, .num_rows = n };
        for (0..4) |mode| {
            var sel = try filter_selection.SelectionVector.init(a, n);
            defer sel.deinit();
            for (0..n) |r| sel.set(r, switch (mode) {
                0 => true,
                1 => false,
                2 => r % 3 != 0,
                else => r % 71 == 0,
            });
            // Exercise every compile-time operation mask and repeat updates from nonempty states.
            for (0..8) |mask| {
                var handled: [calls.len]bool = @splat(false);
                handled[1] = mask & 1 == 0;
                handled[2] = mask & 2 == 0;
                handled[3] = mask & 4 == 0;
                handled[4] = mask & 1 == 0;
                handled[6] = mask & 1 == 0;
                // At least two value calls are needed to justify the common-input kernel.
                if (mask == 0) handled[2] = false;
                var want: [calls.len]Accumulator = undefined;
                var got: [calls.len]Accumulator = undefined;
                for (calls, &want, &got) |call, *w, *g| {
                    w.* = .init(call);
                    g.* = .init(call);
                }
                for (0..2) |_| {
                    for (calls, &want, handled) |call, *w, h| if (!h) {
                        try updateOne(a, a, w, call, &batch, &.{0}, &sel);
                    };
                    try std.testing.expect(try updateSharedInteger(a, &calls, &got, &handled, &batch, &.{0}, &sel));
                }
                for (want, got) |w, g| try std.testing.expectEqualDeep(w, g);
            }
        }
    }
    var sel = try filter_selection.SelectionVector.init(a, n);
    defer sel.deinit();
    var states: [calls.len]Accumulator = undefined;
    for (calls, &states) |call, *state| state.* = .init(call);
    const before = states;
    var unsupported = calls;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = true;
    const batch: filter_eval.Batch = .{ .cols = &.{.{ .i64 = .{ .values = &values } }}, .num_rows = n };
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_64 = false;
    unsupported[calls.len - 1].arg.?.col_ref.unsigned_32 = true;
    try std.testing.expect(!try updateSharedInteger(
        a,
        &unsupported,
        &states,
        &@as([calls.len]bool, @splat(false)),
        &batch,
        &.{0},
        &sel,
    ));
    try std.testing.expectEqualDeep(before, states);
}
