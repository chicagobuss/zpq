const std = @import("std");
const ast = @import("ast.zig");
const filter_eval = @import("../filter/eval.zig");
const selection = @import("../filter/selection.zig");
const regex = @import("regex.zig");
const Batch = filter_eval.Batch;
const ac = @import("eval.zig");
const Error = ac.Error;
const ExactPlan = ac.ExactPlan;
const FloatPlan = ac.FloatPlan;
const evalExpr = ac.evalExpr;
const testing = std.testing;
const evalGroupKeyExpr = ac.evalGroupKeyExpr;
const floatBatchRows = ac.floatBatchRows;

test "float batch budgets follow L2 sharing and forced paths independently of the host" {
    const hw = @import("../hw.zig");
    defer hw.test_cpu = null;
    defer ac.forced_batch_rows = null;
    try std.testing.expectEqual(@as(usize, 128), floatBatchRows(100, 4096, 130));
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, 0));
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    try std.testing.expectEqual(@as(usize, 3840), floatBatchRows(100, 4096, null));
    hw.test_cpu.?.l2.bytes = 1280 << 10;
    try std.testing.expectEqual(@as(usize, 4864), floatBatchRows(100, 4096, null));
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, null));
    for ([_]usize{ 2048, 4096, 8192, 16384 }) |rows| {
        ac.forced_batch_rows = rows;
        try std.testing.expectEqual(rows, floatBatchRows(100, 4096, null));
    }
    ac.forced_batch_rows = 0;
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, null));
}

fn col_i64(values: []const i64) Batch.Column {
    return .{ .i64 = .{ .values = values } };
}

fn col_i32(values: []const i32) Batch.Column {
    return .{ .i32 = .{ .values = values } };
}

fn col_f64(values: []const f64) Batch.Column {
    return .{ .f64 = .{ .values = values } };
}

test "literal column expands to num_rows" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const cols = [_]Batch.Column{};
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{};

    const e: ast.Expr = .{ .literal = .{ .i64 = 7 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 7, 7, 7, 7 }, out.i64.values);
}

test "col_ref reads i64 column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 10, 20, 30 };
    const cols = [_]Batch.Column{col_i64(&xs)};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const e: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &xs, out.i64.values);
}

test "col_ref widens i32 to i64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i32{ -1, 0, 1 };
    const cols = [_]Batch.Column{col_i32(&xs)};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const e: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ -1, 0, 1 }, out.i64.values);
}

test "binop add i64 + i64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 1, 2, 3 };
    const ys = [_]i64{ 10, 20, 30 };
    const cols = [_]Batch.Column{ col_i64(&xs), col_i64(&ys) };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const e: ast.Expr = .{ .binop = .{ .op = .add, .left = lp, .right = rp, .result_type = .i64, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 11, 22, 33 }, out.i64.values);
}

test "binop mul with literal promotes correctly" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 1, 2, 3 };
    const cols = [_]Batch.Column{col_i64(&xs)};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .literal = .{ .i64 = 10 } };
    const e: ast.Expr = .{ .binop = .{ .op = .mul, .left = lp, .right = rp, .result_type = .i64, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 10, 20, 30 }, out.i64.values);
}

test "mixed-type binop promotes both sides to f64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 1, 2, 3 };
    const ys = [_]f64{ 0.5, 0.5, 0.5 };
    const cols = [_]Batch.Column{ col_i64(&xs), col_f64(&ys) };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const e: ast.Expr = .{ .binop = .{ .op = .add, .left = lp, .right = rp, .result_type = .f64, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqual(@as(usize, 3), out.f64.values.len);
    try testing.expect(@abs(out.f64.values[0] - 1.5) < 1e-9);
    try testing.expect(@abs(out.f64.values[2] - 3.5) < 1e-9);
}

test "integer division truncates" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 7, 8, 9 };
    const ys = [_]i64{ 2, 2, 2 };
    const cols = [_]Batch.Column{ col_i64(&xs), col_i64(&ys) };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const e: ast.Expr = .{ .binop = .{ .op = .div, .left = lp, .right = rp, .result_type = .i64, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 3, 4, 4 }, out.i64.values);
}

/// `x op y` over two INT64 columns.
fn evalI64Binop(a: std.mem.Allocator, op: ast.Op, xs: []const i64, ys: []const i64) Error!Batch.Column {
    const cols = [_]Batch.Column{ col_i64(xs), col_i64(ys) };
    const batch: Batch = .{ .cols = &cols, .num_rows = xs.len };
    const lookup = [_]?usize{ 0, 1 };
    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const e: ast.Expr = .{ .binop = .{ .op = op, .left = lp, .right = rp, .result_type = .i64, .depth = 2 } };
    return evalExpr(a, &batch, &lookup, e);
}

test "integer + - * / that overflow the i64 lane fail the query, in a vector or the tail after it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const max = std.math.maxInt(i64);
    const min = std.math.minInt(i64);
    // 37 rows: whole vectors and a tail. The overflowing row sits in a vector, then in the tail.
    for ([_]usize{ 3, 36 }) |at| {
        var xs: [37]i64 = @splat(5);
        var ys: [37]i64 = @splat(7);
        xs[at] = max;
        try testing.expectError(error.IntegerOverflowAdd, evalI64Binop(a, .add, &xs, &ys));
        try testing.expectError(error.IntegerOverflowMul, evalI64Binop(a, .mul, &xs, &ys));
        xs[at] = min;
        try testing.expectError(error.IntegerOverflowSub, evalI64Binop(a, .sub, &xs, &ys));
        ys[at] = -1;
        try testing.expectError(error.IntegerOverflowDiv, evalI64Binop(a, .div, &xs, &ys));
    }
    // At the edges, exact: nothing wraps and nothing is refused that fits.
    const out = try evalI64Binop(a, .add, &.{ max - 1, min + 1, 0 }, &.{ 1, -1, max });
    try testing.expectEqualSlices(i64, &.{ max, min, max }, out.i64.values);
    const prod = try evalI64Binop(a, .mul, &.{ 3_037_000_499, -1, min }, &.{ 3_037_000_499, max, 1 });
    try testing.expectEqualSlices(i64, &.{ 9_223_372_030_926_249_001, -max, min }, prod.i64.values);
}

test "an integer fault in a null slot or an unselected row is not the query's; on a consumed row it is" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const max = std.math.maxInt(i64);
    // Row 1 is null with maxInt left in its values lane; row 3 is present and holds maxInt.
    const xs = [_]i64{ 1, max, 3, max };
    const defs = [_]u32{ 1, 0, 1, 1 };
    const zeros = [_]i64{ 2, 0, 2, 0 };
    const cols = [_]Batch.Column{
        .{ .i64 = .{ .values = &xs, .def_levels = &defs, .max_def = 1, .has_nulls = true } },
        col_i64(&zeros),
    };
    const lookup = [_]?usize{ 0, 1 };
    const x = try a.create(ast.Expr);
    x.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const two = try a.create(ast.Expr);
    two.* = .{ .literal = .{ .i64 = 2 } };
    const z = try a.create(ast.Expr);
    z.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const times_two: ast.Expr = .{ .binop = .{ .op = .mul, .left = x, .right = two, .result_type = .i64, .depth = 2 } };
    const over_zero: ast.Expr = .{ .binop = .{ .op = .div, .left = x, .right = z, .result_type = .i64, .depth = 2 } };

    // Row 3 consumed: its overflow, and its zero divisor, fail the key. The null row's never would.
    const all: Batch = .{ .cols = &cols, .num_rows = 4 };
    try testing.expectError(error.IntegerOverflowMul, evalGroupKeyExpr(a, &all, &lookup, times_two));
    try testing.expectError(error.DivisionByZero, evalGroupKeyExpr(a, &all, &lookup, over_zero));
    // Row 3 outside the selection: only rows 0 and 2 count, and the null row stays null.
    var sel = try selection.SelectionVector.init(a, 4);
    sel.set(3, false);
    const kept = all.restrictedTo(&sel);
    const doubled = try evalGroupKeyExpr(a, &kept, &lookup, times_two);
    try testing.expectEqual(@as(i64, 2), doubled.i64.values[0]);
    try testing.expectEqual(@as(i64, 6), doubled.i64.values[2]);
    try testing.expectEqual(@as(u32, 0), doubled.i64.def_levels.?[1]);
    const halved = try evalGroupKeyExpr(a, &kept, &lookup, over_zero);
    try testing.expectEqual(@as(i64, 0), halved.i64.values[0]);
    // The same selection reaches the column kernel through evalExpr.
    const plain = [_]i64{ 1, 2, 3, max };
    const plain_cols = [_]Batch.Column{ col_i64(&plain), col_i64(&zeros) };
    const plain_batch: Batch = .{ .cols = &plain_cols, .num_rows = 4 };
    try testing.expectError(error.IntegerOverflowMul, evalExpr(a, &plain_batch, &lookup, times_two));
    try testing.expectError(error.DivisionByZero, evalExpr(a, &plain_batch, &lookup, over_zero));
    var row1_and_3_out = try selection.SelectionVector.init(a, 4);
    row1_and_3_out.set(1, false);
    row1_and_3_out.set(3, false);
    const narrowed = plain_batch.restrictedTo(&row1_and_3_out);
    try testing.expectEqual(@as(i64, 6), (try evalExpr(a, &narrowed, &lookup, times_two)).i64.values[2]);
    try testing.expectEqual(@as(i64, 1), (try evalExpr(a, &narrowed, &lookup, over_zero)).i64.values[2]);
}

test "integer division by zero errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 1, 2, 3 };
    const ys = [_]i64{ 1, 0, 1 };
    const cols = [_]Batch.Column{ col_i64(&xs), col_i64(&ys) };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const e: ast.Expr = .{ .binop = .{ .op = .div, .left = lp, .right = rp, .result_type = .i64, .depth = 2 } };

    try testing.expectError(error.DivisionByZero, evalExpr(a, &batch, &lookup, e));
}

test "OPTIONAL column with all rows present is accepted" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 10, 20, 30 };
    const dls = [_]u32{ 1, 1, 1 }; // all present
    const cols = [_]Batch.Column{.{ .i64 = .{ .values = &xs, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const e: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 10, 20, 30 }, out.i64.values);
}

test "string concat: column || literal" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_][]const u8{ "alpha", "beta", "gamma" };
    const cols = [_]Batch.Column{.{ .string = .{ .values = &xs } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .literal = .{ .str = "_x" } };
    const e: ast.Expr = .{ .binop = .{ .op = .concat, .left = lp, .right = rp, .result_type = .str, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqual(@as(usize, 3), out.string.values.len);
    try testing.expectEqualStrings("alpha_x", out.string.values[0]);
    try testing.expectEqualStrings("beta_x", out.string.values[1]);
    try testing.expectEqualStrings("gamma_x", out.string.values[2]);
}

test "string concat: column || column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_][]const u8{ "Hello, ", "Goodbye, " };
    const ys = [_][]const u8{ "world", "world" };
    const cols = [_]Batch.Column{
        .{ .string = .{ .values = &xs } },
        .{ .string = .{ .values = &ys } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 2 };
    const lookup = [_]?usize{ 0, 1 };

    const lp = try a.create(ast.Expr);
    lp.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const rp = try a.create(ast.Expr);
    rp.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const e: ast.Expr = .{ .binop = .{ .op = .concat, .left = lp, .right = rp, .result_type = .str, .depth = 2 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("Hello, world", out.string.values[0]);
    try testing.expectEqualStrings("Goodbye, world", out.string.values[1]);
}

test "coalesce: i64 column with nulls + i64 default" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // Column with nulls at indices 1 and 3.
    const xs = [_]i64{ 10, 0, 30, 0, 50 };
    const dls = [_]u32{ 1, 0, 1, 0, 1 };
    const cols = [_]Batch.Column{.{ .i64 = .{ .values = &xs, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 5 };
    const lookup = [_]?usize{0};

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .literal = .{ .i64 = -1 } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .i64, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 10, -1, 30, -1, 50 }, out.i64.values);
}

test "coalesce: i32 column widened to i64 with default" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i32{ 1, 0, 3 };
    const dls = [_]u32{ 1, 0, 1 };
    const cols = [_]Batch.Column{.{ .i32 = .{ .values = &xs, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .literal = .{ .i64 = 99 } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .i64, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 1, 99, 3 }, out.i64.values);
}

test "coalesce: string column with empty default" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_][]const u8{ "alpha", "", "gamma" };
    const dls = [_]u32{ 1, 0, 1 };
    const cols = [_]Batch.Column{.{ .string = .{ .values = &xs, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .literal = .{ .str = "<missing>" } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .str, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("alpha", out.string.values[0]);
    try testing.expectEqualStrings("<missing>", out.string.values[1]);
    try testing.expectEqualStrings("gamma", out.string.values[2]);
}

test "coalesce: col + col (str) — b fills a's nulls" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // col_a has null at row 1; col_b has value at row 1.
    const a_vals = [_][]const u8{ "us-west-2", "", "us-east-1" };
    const a_dls = [_]u32{ 1, 0, 1 };
    const b_vals = [_][]const u8{ "eu-west-1", "ap-south-1", "us-east-2" };
    const b_dls = [_]u32{ 1, 1, 1 };
    const cols = [_]Batch.Column{
        .{ .string = .{ .values = &a_vals, .def_levels = &a_dls, .max_def = 1 } },
        .{ .string = .{ .values = &b_vals, .def_levels = &b_dls, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .str, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("us-west-2", out.string.values[0]);
    try testing.expectEqualStrings("ap-south-1", out.string.values[1]);
    try testing.expectEqualStrings("us-east-1", out.string.values[2]);
}

test "regression: coalesce: col + col (str) is NULL where both are" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const a_vals = [_][]const u8{ "alpha", "" };
    const a_dls = [_]u32{ 1, 0 };
    const b_vals = [_][]const u8{ "beta", "" };
    const b_dls = [_]u32{ 1, 0 };
    const cols = [_]Batch.Column{
        .{ .string = .{ .values = &a_vals, .def_levels = &a_dls, .max_def = 1 } },
        .{ .string = .{ .values = &b_vals, .def_levels = &b_dls, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 2 };
    const lookup = [_]?usize{ 0, 1 };

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .str, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("alpha", out.string.values[0]);
    try testing.expectEqualSlices(u32, &.{ 1, 0 }, out.string.def_levels.?);
    try testing.expectEqual(1, out.string.max_def);
}

test "coalesce: col + col (str) — REQUIRED col_a fast-paths to memcpy" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // col_a has no def_levels (REQUIRED). The kernel should never
    // look at col_b — verify by giving col_b nulls that would
    // otherwise leak through.
    const a_vals = [_][]const u8{ "x", "y", "z" };
    const b_vals = [_][]const u8{ "!", "!", "!" };
    const b_dls = [_]u32{ 0, 0, 0 };
    const cols = [_]Batch.Column{
        .{ .string = .{ .values = &a_vals } },
        .{ .string = .{ .values = &b_vals, .def_levels = &b_dls, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .str, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("x", out.string.values[0]);
    try testing.expectEqualStrings("y", out.string.values[1]);
    try testing.expectEqualStrings("z", out.string.values[2]);
}

test "coalesce: col + col (i64) — promotes i32+i64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // a is i32 with a null; b is i64 all-present.
    const a_vals = [_]i32{ 10, 0, 30 };
    const a_dls = [_]u32{ 1, 0, 1 };
    const b_vals = [_]i64{ 100, 200, 300 };
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = &a_vals, .def_levels = &a_dls, .max_def = 1 } },
        .{ .i64 = .{ .values = &b_vals } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .i64, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 10, 200, 30 }, out.i64.values);
}

test "regression: coalesce: col + col (f64) is NULL where both are" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const a_vals = [_]f64{ 1.5, 0, 3.5 };
    const a_dls = [_]u32{ 1, 0, 1 };
    const b_vals = [_]f64{ 0, 0, 4.5 };
    const b_dls = [_]u32{ 0, 0, 1 };
    const cols = [_]Batch.Column{
        .{ .f64 = .{ .values = &a_vals, .def_levels = &a_dls, .max_def = 1 } },
        .{ .f64 = .{ .values = &b_vals, .def_levels = &b_dls, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };

    const arg0 = try a.create(ast.Expr);
    arg0.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const arg1 = try a.create(ast.Expr);
    arg1.* = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const args = try a.alloc(*ast.Expr, 2);
    args[0] = arg0;
    args[1] = arg1;

    const e: ast.Expr = .{ .call = .{ .func = .coalesce, .args = args, .result_type = .f64, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqual(1.5, out.f64.values[0]);
    try testing.expectEqual(3.5, out.f64.values[2]);
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1 }, out.f64.def_levels.?);
}

test "regression: coalesce zero-extends UINT32 operands in the i64 and f64 lanes, with a literal or a column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const big: i32 = @bitCast(@as(u32, 3_000_000_000));
    const u_vals = [_]i32{ big, 0, 0 };
    const u_dls = [_]u32{ 1, 0, 0 };
    const r_vals = [_]i32{ 0, big, 0 };
    const r_dls = [_]u32{ 0, 1, 0 };
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = &u_vals, .def_levels = &u_dls, .max_def = 1 } },
        .{ .i32 = .{ .values = &r_vals, .def_levels = &r_dls, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{ 0, 1 };
    var u: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64, .unsigned_32 = true } };
    var r: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64, .unsigned_32 = true } };
    var seven: ast.Expr = .{ .literal = .{ .i64 = 7 } };
    for ([_]ast.Type{ .i64, .f64 }) |lane| {
        const with_lit = [_]*ast.Expr{ &u, &seven };
        const lit_out = try evalExpr(a, &batch, &lookup, .{ .call = .{
            .func = .coalesce,
            .args = &with_lit,
            .result_type = lane,
            .depth = 2,
        } });
        const with_col = [_]*ast.Expr{ &u, &r };
        const col_out = try evalExpr(a, &batch, &lookup, .{ .call = .{
            .func = .coalesce,
            .args = &with_col,
            .result_type = lane,
            .depth = 2,
        } });
        switch (lane) {
            .i64 => {
                try testing.expectEqualSlices(i64, &.{ 3_000_000_000, 7, 7 }, lit_out.i64.values);
                try testing.expectEqualSlices(i64, &.{ 3_000_000_000, 3_000_000_000 }, col_out.i64.values[0..2]);
                try testing.expectEqualSlices(u32, &.{ 1, 1, 0 }, col_out.i64.def_levels.?);
            },
            else => {
                try testing.expectEqualSlices(f64, &.{ 3e9, 7, 7 }, lit_out.f64.values);
                try testing.expectEqualSlices(f64, &.{ 3e9, 3e9 }, col_out.f64.values[0..2]);
                try testing.expectEqualSlices(u32, &.{ 1, 1, 0 }, col_out.f64.def_levels.?);
            },
        }
    }
}

test "arithmetic over NULLs is NULL, and a NULL, unwanted or placeholder row never faults" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // x: NULL in row 2, whose placeholder would overflow `* 2` and divide by zero; y: NULL in row 0.
    const xs = [_]i64{ 1, 2, std.math.maxInt(i64), 4 };
    const x_def = [_]u32{ 1, 1, 0, 1 };
    const ys = [_]i64{ 0, 5, 0, 3 };
    const y_def = [_]u32{ 0, 1, 1, 1 };
    const strs = [_][]const u8{ "a", "b", "", "d" };
    const nested_rep = [_]u32{ 0, 1, 0, 0 };
    const cols = [_]Batch.Column{
        .{ .i64 = .{ .values = &xs, .def_levels = &x_def, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = &ys, .def_levels = &y_def, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = &strs, .def_levels = &x_def, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = &xs, .max_rep = 1, .rep_levels = &nested_rep } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1, 2, 3 };
    var x: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .nullable = true } };
    var y: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64, .nullable = true } };
    var s_col: ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .BYTE_ARRAY, .expr_type = .str, .nullable = true } };
    var nested: ast.Expr = .{ .col_ref = .{ .col_idx = 3, .physical_type = .INT64, .expr_type = .i64 } };
    var two: ast.Expr = .{ .literal = .{ .i64 = 2 } };
    var half: ast.Expr = .{ .literal = .{ .f64 = 0.5 } };
    const bin = struct {
        fn of(op: ast.Op, l: *ast.Expr, r: *ast.Expr, t: ast.Type) ast.Expr {
            return .{ .binop = .{ .op = op, .left = l, .right = r, .result_type = t, .depth = 2 } };
        }
    }.of;

    // The column alone keeps its levels.
    const alone = try evalExpr(a, &batch, &lookup, x);
    try testing.expectEqualSlices(u32, &x_def, alone.i64.def_levels.?);

    const doubled = try evalExpr(a, &batch, &lookup, bin(.mul, &x, &two, .i64));
    try testing.expectEqualSlices(u32, &.{ 1, 1, 0, 1 }, doubled.i64.def_levels.?);
    try testing.expect(doubled.i64.has_nulls);
    try testing.expectEqual(@as(i64, 8), doubled.i64.values[3]);

    // NULL on either side; `x / y` divides by zero only in row 0, where y is NULL.
    const sum = try evalExpr(a, &batch, &lookup, bin(.add, &x, &y, .i64));
    try testing.expectEqualSlices(u32, &.{ 0, 1, 0, 1 }, sum.i64.def_levels.?);
    try testing.expectEqual(@as(i64, 7), sum.i64.values[1]);
    const quotient = try evalExpr(a, &batch, &lookup, bin(.div, &x, &y, .i64));
    try testing.expectEqualSlices(u32, &.{ 0, 1, 0, 1 }, quotient.i64.def_levels.?);
    try testing.expectEqual(@as(i64, 1), quotient.i64.values[3]);

    // An integer side promoted to the float lane keeps its NULLs.
    const plus_half = try evalExpr(a, &batch, &lookup, bin(.add, &x, &half, .f64));
    try testing.expectEqualSlices(u32, &.{ 1, 1, 0, 1 }, plus_half.f64.def_levels.?);
    try testing.expectEqual(@as(f64, 4.5), plus_half.f64.values[3]);

    // String concatenation.
    const joined = try evalExpr(a, &batch, &lookup, bin(.concat, &s_col, &s_col, .str));
    try testing.expectEqualSlices(u32, &.{ 1, 1, 0, 1 }, joined.string.def_levels.?);
    try testing.expectEqualStrings("dd", joined.string.values[3]);

    // A wanted, present row still faults; one the selection drops does not.
    var y_plus: ast.Expr = bin(.add, &x, &y, .i64);
    var big: ast.Expr = .{ .literal = .{ .i64 = std.math.maxInt(i64) } };
    try testing.expectError(error.IntegerOverflowAdd, evalExpr(a, &batch, &lookup, bin(.add, &y_plus, &big, .i64)));
    var sel = try selection.SelectionVector.init(a, 4);
    sel.set(1, false);
    sel.set(3, false);
    const dropped = try evalExpr(a, &batch.restrictedTo(&sel), &lookup, bin(.add, &y_plus, &big, .i64));
    try testing.expectEqualSlices(u32, &.{ 0, 1, 0, 1 }, dropped.i64.def_levels.?);

    // An OPTIONAL column with no NULL row takes the vector kernels: no levels on the result.
    const no_nulls = [_]u32{ 1, 1, 1, 1 };
    const ones = [_]i64{ 1, 2, 3, 4 };
    const present = [_]Batch.Column{.{ .i64 = .{ .values = &ones, .def_levels = &no_nulls, .max_def = 1 } }};
    const full: Batch = .{ .cols = &present, .num_rows = 4 };
    const fast = try evalExpr(a, &full, &lookup, bin(.mul, &x, &two, .i64));
    try testing.expect(fast.i64.def_levels == null);
    try testing.expectEqualSlices(i64, &.{ 2, 4, 6, 8 }, fast.i64.values);

    // Elements of a list stay refused.
    try testing.expectError(error.NestedNotSupported, evalExpr(a, &batch, &lookup, bin(.mul, &nested, &two, .i64)));
}

test "group key: integer operands convert to the binop's float lane" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    // i8 + 0.5 and v * 2: the integer side used to be read as raw f64 bits (2 → 1e-323).
    const ints = [_]i64{ 1, 2, 3, -1 };
    const dls = [_]u32{ 1, 1, 0, 1 };
    const dbls = [_]f64{ 1.0, 2.0, 3.0, -2.25 };
    const big = [_]i64{ @bitCast(@as(u64, 1 << 63)), 5, 0, 1 };
    const cols = [_]Batch.Column{
        .{ .i64 = .{ .values = &ints, .def_levels = &dls, .max_def = 1, .has_nulls = true } },
        col_f64(&dbls),
        col_i64(&big),
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1, 2 };

    const int_col: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    const dbl_col: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const u64_col: ast.Expr = .{
        .col_ref = .{ .col_idx = 2, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true },
    };
    const half: ast.Expr = .{ .literal = .{ .f64 = 0.5 } };
    const two: ast.Expr = .{ .literal = .{ .i64 = 2 } };
    const one: ast.Expr = .{ .literal = .{ .i64 = 1 } };

    const Case = struct { op: ast.Op, l: ast.Expr, r: ast.Expr, want: []const f64 };
    const cases = [_]Case{
        .{ .op = .add, .l = int_col, .r = half, .want = &.{ 1.5, 2.5, 0, -0.5 } },
        .{ .op = .add, .l = half, .r = int_col, .want = &.{ 1.5, 2.5, 0, -0.5 } },
        .{ .op = .mul, .l = dbl_col, .r = two, .want = &.{ 2, 4, 6, -4.5 } },
        .{ .op = .div, .l = dbl_col, .r = two, .want = &.{ 0.5, 1, 1.5, -1.125 } },
        .{ .op = .mul, .l = int_col, .r = dbl_col, .want = &.{ 1, 4, 0, 2.25 } },
        // Unsigned 64-bit converts by value, not as a negative i64.
        .{ .op = .mul, .l = u64_col, .r = half, .want = &.{ 4611686018427387904.0, 2.5, 0, 0.5 } },
    };
    for (cases) |c| {
        const l = try a.create(ast.Expr);
        const r = try a.create(ast.Expr);
        l.* = c.l;
        r.* = c.r;
        const e: ast.Expr = .{ .binop = .{ .op = c.op, .left = l, .right = r, .result_type = .f64, .depth = 2 } };
        const out = try evalGroupKeyExpr(a, &batch, &lookup, e);
        for (c.want, out.f64.values, 0..) |w, g, i| {
            if (out.f64.def_levels) |d| if (d[i] == 0) continue; // null row
            try testing.expectEqual(w, g);
        }
    }

    // In the signed integer lane an unsigned value of 2^63 or more is rejected, not wrapped negative.
    const l = try a.create(ast.Expr);
    const r = try a.create(ast.Expr);
    l.* = u64_col;
    r.* = one;
    const e: ast.Expr = .{ .binop = .{ .op = .add, .left = l, .right = r, .result_type = .i64, .depth = 2 } };
    try testing.expectError(error.Unsigned64OutOfRange, evalGroupKeyExpr(a, &batch, &lookup, e));
}

test "select/agg expression: unsigned 64-bit operands convert by value or are rejected" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const big = [_]i64{ @bitCast(@as(u64, std.math.maxInt(u64))), 4 };
    const cols = [_]Batch.Column{col_i64(&big)};
    const batch: Batch = .{ .cols = &cols, .num_rows = 2 };
    const lookup = [_]?usize{0};
    const l = try a.create(ast.Expr);
    const r = try a.create(ast.Expr);
    l.* = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64, .unsigned_64 = true } };

    r.* = .{ .literal = .{ .f64 = 0.5 } };
    const half: ast.Expr = .{ .binop = .{ .op = .mul, .left = l, .right = r, .result_type = .f64, .depth = 2 } };
    const out = try evalExpr(a, &batch, &lookup, half);
    try testing.expectEqualSlices(f64, &.{ 9223372036854775807.5, 2 }, out.f64.values);

    r.* = .{ .literal = .{ .i64 = 0 } };
    const plus: ast.Expr = .{ .binop = .{ .op = .add, .left = l, .right = r, .result_type = .i64, .depth = 2 } };
    try testing.expectError(error.Unsigned64OutOfRange, evalExpr(a, &batch, &lookup, plus));
}

test "strlen counts bytes, keeps NULL rows NULL and reads dictionary codes per entry" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const strs = [_][]const u8{ "", "abc", "x", "héllo" };
    const def = [_]u32{ 1, 1, 0, 1 };
    const plain: Batch.Column = .{ .string = .{ .values = &strs, .def_levels = &def, .max_def = 1, .has_nulls = true } };
    const dict = [_][]const u8{ "ab", "héllo" };
    const codes = [_]u32{ 1, 0, 0, 1 };
    const coded: Batch.Column = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } };
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const args = [_]*const ast.Expr{&arg};
    const call: ast.Expr = .{ .call = .{ .func = .strlen, .args = @ptrCast(&args), .result_type = .i64, .depth = 2 } };
    const lookup = [_]?usize{0};

    const got = try evalExpr(a, &.{ .cols = &.{plain}, .num_rows = 4 }, &lookup, call);
    try std.testing.expectEqualSlices(i64, &.{ 0, 3, 1, 6 }, got.i64.values);
    try std.testing.expectEqualSlices(u32, &def, got.i64.def_levels.?);
    const via_codes = try evalExpr(a, &.{ .cols = &.{coded}, .num_rows = 4 }, &lookup, call);
    try std.testing.expectEqualSlices(i64, &.{ 6, 2, 2, 6 }, via_codes.i64.values);
}

test "coalesce over any arguments: first present value, each argument evaluated only where every earlier one is NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    // x NULL in row 1; y NULL in rows 0 and 1, 0 in row 2 (a divisor), REQUIRED neighbours elsewhere.
    const xs = [_]i64{ 3, 0, 5, 7 };
    const x_def = [_]u32{ 1, 0, 1, 1 };
    const ys = [_]i64{ 0, 0, 0, 2 };
    const y_def = [_]u32{ 0, 0, 1, 1 };
    const strs = [_][]const u8{ "a", "", "c", "d" };
    const cols = [_]Batch.Column{
        .{ .i64 = .{ .values = &xs, .def_levels = &x_def, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = &ys, .def_levels = &y_def, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = &strs, .def_levels = &x_def, .max_def = 1, .has_nulls = true } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1, 2 };
    var x: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var y: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    var s: ast.Expr = .{ .col_ref = .{ .col_idx = 2, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    var two: ast.Expr = .{ .literal = .{ .i64 = 2 } };
    var seven: ast.Expr = .{ .literal = .{ .i64 = 7 } };
    var hundred: ast.Expr = .{ .literal = .{ .i64 = 100 } };
    var na: ast.Expr = .{ .literal = .{ .str = "NA" } };
    var suffix: ast.Expr = .{ .literal = .{ .str = "!" } };
    var x2: ast.Expr = .{ .binop = .{ .op = .mul, .left = &x, .right = &two, .result_type = .i64, .depth = 2 } };
    var div: ast.Expr = .{ .binop = .{ .op = .div, .left = &hundred, .right = &y, .result_type = .i64, .depth = 2 } };
    var cat: ast.Expr = .{ .binop = .{ .op = .concat, .left = &s, .right = &suffix, .result_type = .str, .depth = 2 } };
    const call = struct {
        fn of(args: []const *ast.Expr, t: ast.Type) ast.Expr {
            return .{ .call = .{ .func = .coalesce, .args = args, .result_type = t, .depth = 3 } };
        }
    }.of;

    // Arithmetic first, a column, then a literal: three arguments.
    const three = try evalExpr(a, &batch, &lookup, call(&.{ &x2, &y, &seven }, .i64));
    try testing.expectEqualSlices(i64, &.{ 6, 7, 10, 14 }, three.i64.values);
    try testing.expect(three.i64.def_levels == null);
    // `100 / y` divides by zero only in row 2, where `x * 2` already answered; rows 0 and 1 stay NULL there.
    const guarded = try evalExpr(a, &batch, &lookup, call(&.{ &x2, &div }, .i64));
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1, 1 }, guarded.i64.def_levels.?);
    try testing.expectEqualSlices(i64, &.{ 6, 0, 10, 14 }, guarded.i64.values);
    try testing.expectError(error.DivisionByZero, evalExpr(a, &batch, &lookup, call(&.{ &div, &x2 }, .i64)));
    // Strings, an expression first.
    const words = try evalExpr(a, &batch, &lookup, call(&.{ &cat, &na }, .str));
    try testing.expectEqualStrings("a!", words.string.values[0]);
    try testing.expectEqualStrings("NA", words.string.values[1]);
}

test "float DAG batches propagate independent FLOAT and DOUBLE nulls through shared trees" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const n = 257;
    var xs: [n]f32 = undefined;
    var ys: [n]f64 = undefined;
    var xd: [n]u32 = undefined;
    var yd: [n]u32 = undefined;
    for (0..n) |i| {
        xs[i] = @as(f32, @floatFromInt(i % 13)) - 6;
        ys[i] = @as(f64, @floatFromInt(i % 17)) / 8;
        // An entirely null interior batch and independent null patterns at both sides of slice boundaries.
        xd[i] = @intFromBool(i % 11 != 0 and !(i >= 64 and i < 128));
        yd[i] = @intFromBool(i % 7 != 0);
    }
    const cols = [_]Batch.Column{
        .{ .f32 = .{ .values = &xs, .def_levels = &xd, .max_def = 1, .has_nulls = true } },
        .{ .f64 = .{ .values = &ys, .def_levels = &yd, .max_def = 1, .has_nulls = true } },
    };
    const lookup = [_]?usize{ 0, 1 };
    var x: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .FLOAT, .expr_type = .f64 } };
    var y: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var one: ast.Expr = .{ .literal = .{ .f64 = 1 } };
    var shared: ast.Expr = .{ .binop = .{
        .op = .sub,
        .left = &one,
        .right = &y,
        .result_type = .f64,
        .depth = 2,
    } };
    var product: ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &x,
        .right = &shared,
        .result_type = .f64,
        .depth = 3,
    } };
    const quotient: ast.Expr = .{ .binop = .{
        .op = .div,
        .left = &product,
        .right = &shared,
        .result_type = .f64,
        .depth = 4,
    } };
    var sel = try selection.SelectionVector.init(a, n);
    for (0..n) |i| if (i % 5 == 0) sel.set(i, false);
    const batch: Batch = .{ .cols = &cols, .num_rows = n, .sel = &sel };
    var plan: FloatPlan = .{};
    const roots = [_]usize{
        (try plan.add(a, &batch, &lookup, product)).?,
        (try plan.add(a, &batch, &lookup, quotient)).?,
    };
    const want = [_]Batch.Column{
        try evalGroupKeyExpr(a, &batch, &lookup, product),
        try evalGroupKeyExpr(a, &batch, &lookup, quotient),
    };
    var off: usize = 0;
    while (off < n) : (off += 64) {
        const count = @min(64, n - off);
        var sliced: [2]Batch.Column = undefined;
        for (cols, &sliced) |col, *out| out.* = try col.sliceRows(off, count);
        const mask = try sel.alignedWindow(off, count);
        const win: Batch = .{ .cols = &sliced, .num_rows = count, .sel = &mask };
        const got = try plan.evaluate(a, &win, &lookup);
        for (want, roots) |expected, root| for (0..count) |r| {
            const ni = got[root].nullInfo();
            const wi = expected.nullInfo();
            const present = ni.def_levels == null or ni.def_levels.?[r] == ni.max_def;
            const wanted = wi.def_levels == null or wi.def_levels.?[off + r] == wi.max_def;
            try std.testing.expectEqual(wanted, present);
            if (present and mask.isActive(r)) try std.testing.expectEqual(
                @as(u64, @bitCast(expected.f64.values[off + r])),
                @as(u64, @bitCast(got[root].f64.values[r])),
            );
        };
    }
}

test "exact DAG checks physical overflow and shares subexpressions without widening small roots" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, exactDagTrial, .{});
}

fn exactDagTrial(gpa: std.mem.Allocator) !void {
    var arena = std.heap.ArenaAllocator.init(gpa);
    defer arena.deinit();
    const a = arena.allocator();
    var xvals = [_]i64{ 10, 20, 30, 40 };
    var yvals = [_]i64{ 2, 3, 4, 5 };
    const defs = [_]u32{ 1, 1, 0, 1 };
    const cols = [_]Batch.Column{
        .{ .f64 = .{
            .values = &.{ 10, 20, 30, 40 },
            .unscaled = .{ .i64 = &xvals },
            .def_levels = &defs,
            .max_def = 1,
            .has_nulls = true,
        } },
        .{ .f64 = .{ .values = &.{ 2, 3, 4, 5 }, .unscaled = .{ .i64 = &yvals } } },
    };
    var sel = try selection.SelectionVector.init(a, 4);
    defer sel.deinit();
    const batch: Batch = .{ .cols = &cols, .num_rows = 4, .sel = &sel };
    const lookup = [_]?usize{ 0, 1 };
    var x: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .dec, .dec = .{
        .precision = 15,
        .scale = 0,
    } } };
    var y: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .dec, .dec = .{
        .precision = 15,
        .scale = 0,
    } } };
    var product: ast.Expr = .{ .binop = .{
        .op = .mul,
        .left = &x,
        .right = &y,
        .result_type = .dec,
        .dec = .{ .precision = 31, .scale = 0 },
        .depth = 2,
    } };
    const twice: ast.Expr = .{ .binop = .{
        .op = .add,
        .left = &product,
        .right = &product,
        .result_type = .dec,
        .dec = .{ .precision = 32, .scale = 0 },
        .depth = 3,
    } };
    var plan: ExactPlan = .{};
    const root = (try plan.add(a, &batch, &lookup, product)).?;
    const top = (try plan.add(a, &batch, &lookup, twice)).?;
    try std.testing.expectEqual(@as(usize, 4), plan.nodes.items.len);
    const narrow = try plan.evaluate(a, &batch, &lookup, root);
    try std.testing.expect(narrow == .i64);
    try std.testing.expectEqualSlices(i64, &.{ 20, 60, 0, 200 }, narrow.i64.values);
    const both = try plan.evaluate(a, &batch, &lookup, top);
    try std.testing.expectEqualSlices(i64, &.{ 40, 120, 0, 400 }, both.i64.values);
    try std.testing.expectEqual(
        @intFromPtr(narrow.i64.values.ptr),
        @intFromPtr((try plan.evaluate(a, &batch, &lookup, root)).i64.values.ptr),
    );
    // A checked physical overflow retries wide, preserving a logical DECIMAL(31,0) result.
    xvals[0] = 1 << 40;
    yvals[0] = 1 << 30;
    for (plan.nodes.items) |*node| node.value = null;
    const wide = try plan.evaluate(a, &batch, &lookup, root);
    try std.testing.expect(wide == .i128);
    try std.testing.expectEqual(@as(i128, 1) << 70, wide.i128.values[0]);
    const canonical = try evalExpr(a, &batch, &lookup, product);
    try std.testing.expectEqualDeep(canonical, wide);
    // Overflowing unselected and NULL rows cannot become new errors or force the wide fallback.
    sel.set(0, false);
    xvals[2] = std.math.maxInt(i64);
    yvals[2] = std.math.maxInt(i64);
    for (plan.nodes.items) |*node| node.value = null;
    try std.testing.expect((try plan.evaluate(a, &batch, &lookup, root)) == .i64);
    sel.set(0, true);
    xvals[0] = std.math.maxInt(i64);
    yvals[0] = std.math.maxInt(i64);
    for (plan.nodes.items) |*node| node.value = null;
    try std.testing.expectError(error.DecimalOverflow, plan.evaluate(a, &batch, &lookup, root));
    try std.testing.expectError(error.DecimalOverflow, evalExpr(a, &batch, &lookup, product));
}

test "date/time functions keep NULL rows NULL and fault only at rows the batch wants" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // Milliseconds: 2013-07-31 19:59:59.123, 1969-12-31 23:59:59.999, and a NULL row holding a value no TIMESTAMP has.
    const ms = [_]i64{ 1375300799123, -1, std.math.maxInt(i64) };
    const def = [_]u32{ 1, 1, 0 };
    const nullable: Batch.Column = .{ .i64 = .{ .values = &ms, .def_levels = &def, .max_def = 1, .has_nulls = true } };
    const batch: Batch = .{ .cols = &.{nullable}, .num_rows = 3 };
    const lookup = [_]?usize{0};
    const col: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const ts_args = [_]*const ast.Expr{&col};
    const ts: ast.Expr = .{ .call = .{
        .func = .epoch_ms,
        .args = @ptrCast(&ts_args),
        .result_type = .i64,
        .depth = 2,
        .temporal = .ts_us,
    } };
    const got = try evalExpr(a, &batch, &lookup, ts);
    try std.testing.expectEqualSlices(i64, &.{ 1375300799123000, -1000 }, got.i64.values[0..2]);
    try std.testing.expectEqualSlices(u32, &def, got.i64.def_levels.?);

    const minute: ast.Expr = .{ .literal = .{ .str = "minute" } };
    const part_args = [_]*const ast.Expr{ &minute, &ts };
    const part: ast.Expr = .{ .call = .{
        .func = .date_part,
        .args = @ptrCast(&part_args),
        .result_type = .i64,
        .depth = 3,
        .part = .minute,
    } };
    const parts = try evalGroupKeyExpr(a, &batch, &lookup, part);
    try std.testing.expectEqualSlices(i64, &.{ 59, 59 }, parts.i64.values[0..2]);
    var trunc = part;
    trunc.call.func = .date_trunc;
    trunc.call.temporal = .ts_us;
    const truncs = try evalExpr(a, &batch, &lookup, trunc);
    try std.testing.expectEqualSlices(i64, &.{ 1375300740000000, -60000000 }, truncs.i64.values[0..2]);

    // The same out-of-range value at a present row faults, unless the batch does not want that row.
    const present: Batch = .{ .cols = &.{col_i64(&ms)}, .num_rows = 3 };
    try std.testing.expectError(error.TimestampOutOfRange, evalExpr(a, &present, &lookup, ts));
    var sel = try selection.SelectionVector.init(a, 3);
    sel.set(2, false);
    _ = try evalExpr(a, &present.restrictedTo(&sel), &lookup, ts);

    // make_date takes an INTEGER's range; date_trunc of a DATE is a TIMESTAMP, as DuckDB has it.
    const days = [_]i64{ 15900, 1 << 31 };
    const day_batch: Batch = .{ .cols = &.{col_i64(&days)}, .num_rows = 2 };
    var date = ts;
    date.call.func = .make_date;
    date.call.temporal = .date;
    try std.testing.expectError(error.DateOutOfRange, evalExpr(a, &day_batch, &lookup, date));
    const month: ast.Expr = .{ .literal = .{ .str = "month" } };
    const month_args = [_]*const ast.Expr{ &month, &date };
    var by_month = trunc;
    by_month.call.args = @ptrCast(&month_args);
    by_month.call.part = .month;
    const first = try evalExpr(a, &.{ .cols = &.{col_i64(days[0..1])}, .num_rows = 1 }, &lookup, by_month);
    try std.testing.expectEqual(@as(i64, 15887 * 86_400_000_000), first.i64.values[0]);
}

test "regexp_replace replaces a coded column once per entry, and keeps NULL and unmatched rows as they are" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const r = try a.create(regex.Replace);
    r.* = try .compile(a, "^https?://(?:www\\.)?([^/]+)/.*$", "\\1", "");
    const arg: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .BYTE_ARRAY, .expr_type = .str } };
    const pattern: ast.Expr = .{ .literal = .{ .str = "" } };
    const args = [_]*const ast.Expr{ &arg, &pattern, &pattern };
    const call: ast.Expr = .{ .call = .{
        .func = .regexp_replace,
        .args = @ptrCast(&args),
        .result_type = .str,
        .depth = 2,
        .replace = r,
    } };
    const lookup = [_]?usize{0};
    const strs = [_][]const u8{ "http://www.a.ru/x", "", "ftp://b/", "http://www.a.ru/x" };
    const def = [_]u32{ 1, 0, 1, 1 };
    const plain: Batch.Column = .{ .string = .{ .values = &strs, .def_levels = &def, .max_def = 1, .has_nulls = true } };
    const got = try evalExpr(a, &.{ .cols = &.{plain}, .num_rows = 4 }, &lookup, call);
    try std.testing.expectEqualStrings("a.ru", got.string.values[0]);
    try std.testing.expectEqualStrings("ftp://b/", got.string.values[2]);
    try std.testing.expectEqualStrings("a.ru", got.string.values[3]);
    try std.testing.expectEqualSlices(u32, &def, got.string.def_levels.?);
    const dict = [_][]const u8{ "https://c.ru/", "nope" };
    const codes = [_]u32{ 1, 0, 0 };
    const coded: Batch.Column = .{ .string = .{ .values = &.{}, .codes = &codes, .dict = &dict } };
    const via_codes = try evalExpr(a, &.{ .cols = &.{coded}, .num_rows = 3 }, &lookup, call);
    try std.testing.expectEqualStrings("nope", via_codes.string.values[0]);
    try std.testing.expectEqualStrings("c.ru", via_codes.string.values[1]);
    try std.testing.expectEqual(@as(?[]const u32, null), via_codes.string.codes);
    // Lengths alone carry no texts: an engine fault named as one, never an answer over empty strings.
    const lens = [_]u32{ 17, 0, 8, 17 };
    const lengths_only: Batch.Column = .{ .string = .{
        .values = &.{},
        .byte_lengths = &lens,
        .def_levels = &def,
        .max_def = 1,
        .has_nulls = true,
    } };
    try std.testing.expectError(
        error.LengthsOnlyColumn,
        evalExpr(a, &.{ .cols = &.{lengths_only}, .num_rows = 4 }, &lookup, call),
    );
}
