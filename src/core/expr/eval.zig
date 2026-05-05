//! Expression evaluator. Turns an `Expr` AST into a typed
//! `Batch.Column` of length `batch.num_rows`.
//!
//! Strategy: recursive descent over the AST, producing a `.i64` or
//! `.f64` Column at each level. Promotion happens at two boundaries:
//!
//!   1. Column-ref load — INT32/INT64 widen to i64; FLOAT/DOUBLE widen
//!      to f64. The parser pre-computed the target type per col_ref.
//!   2. Binop child eval — both children are forced to the binop's
//!      `result_type` before the kernel runs. So binop kernels see
//!      same-typed slices.
//!
//! Kernels are small and per-(op, T) — comptime-generated to avoid
//! a runtime switch in the hot loop. Four ops × two types = eight
//! specializations.
//!
//! Scope of first slice:
//!   - Required, non-nested numeric columns only. OPTIONAL or LIST/MAP
//!     columns return error.NullableNotSupported. Null-aware arithmetic
//!     lands in a follow-up slice when we know what the encoder side
//!     wants to do with computed nulls.
//!   - Integer division truncates (matches Zig `/` on integers). To
//!     get true-division semantics, write `i / 1.0` to force f64.

const std = @import("std");
const ast = @import("ast.zig");
const filter_eval = @import("../filter/eval.zig");
const schema = @import("../schema.zig");

pub const Error = error{
    BadColumn,
    TypeMismatch,
    NullableNotSupported,
    NestedNotSupported,
    DivisionByZero,
} || std.mem.Allocator.Error;

const Batch = filter_eval.Batch;
const ColumnT = filter_eval.ColumnT;

/// Evaluate `e` against `batch`, returning a freshly-allocated column
/// (owned by `arena`) of length `batch.num_rows`. `column_lookup` maps
/// AST col_ref indices to positions in `batch.cols`, mirroring
/// `filter_eval.evaluate`'s contract.
pub fn evalExpr(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    e: ast.Expr,
) Error!Batch.Column {
    return switch (e) {
        .literal => |lit| literalAsColumn(arena, lit, batch.num_rows),
        .col_ref => |c| try colRefAsColumn(arena, batch, column_lookup, c),
        .binop => |b| try evalBinOp(arena, batch, column_lookup, b),
    };
}

/// Evaluate `e` and coerce the result to `target` if needed. Only
/// widening is supported (i64 → f64); narrowing is rejected.
fn evalAs(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    e: ast.Expr,
    target: ast.Type,
) Error!Batch.Column {
    const result = try evalExpr(arena, batch, column_lookup, e);
    if (e.typeOf() == target) return result;
    // Promotion: i64 → f64. The reverse never happens for valid ASTs
    // (the parser sets binop.result_type via Type.promote).
    if (target == .f64 and e.typeOf() == .i64) {
        const src = result.i64.values;
        const out = try arena.alloc(f64, src.len);
        for (src, 0..) |v, i| out[i] = @floatFromInt(v);
        return .{ .f64 = .{ .values = out } };
    }
    return error.TypeMismatch;
}

fn literalAsColumn(arena: std.mem.Allocator, lit: ast.Literal, num_rows: usize) Error!Batch.Column {
    return switch (lit) {
        .i64 => |v| blk: {
            const out = try arena.alloc(i64, num_rows);
            @memset(out, v);
            break :blk .{ .i64 = .{ .values = out } };
        },
        .f64 => |v| blk: {
            const out = try arena.alloc(f64, num_rows);
            @memset(out, v);
            break :blk .{ .f64 = .{ .values = out } };
        },
        .str => |v| blk: {
            // Constant string column: all rows point at the same backing
            // bytes (literal lifetime is bounded by the parser arena
            // which must outlive the encode call). No per-row alloc.
            const out = try arena.alloc([]const u8, num_rows);
            @memset(out, v);
            break :blk .{ .string = .{ .values = out } };
        },
    };
}

fn colRefAsColumn(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.ColRef,
) Error!Batch.Column {
    const pos = column_lookup[c.col_idx] orelse return error.BadColumn;
    const col = batch.cols[pos];
    // Reject OPTIONAL / nested columns up-front. Null-aware arithmetic
    // is a follow-up slice.
    try rejectNullableOrNested(col);

    return switch (c.expr_type) {
        .i64 => .{ .i64 = .{ .values = try widenToI64(arena, col) } },
        .f64 => .{ .f64 = .{ .values = try widenToF64(arena, col) } },
        .str => .{ .string = .{ .values = try borrowStr(col) } },
    };
}

fn evalBinOp(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!Batch.Column {
    const l_col = try evalAs(arena, batch, column_lookup, b.left.*, b.result_type);
    const r_col = try evalAs(arena, batch, column_lookup, b.right.*, b.result_type);

    return switch (b.result_type) {
        .i64 => .{ .i64 = .{ .values = try applyKernel(i64, arena, b.op, l_col.i64.values, r_col.i64.values) } },
        .f64 => .{ .f64 = .{ .values = try applyKernel(f64, arena, b.op, l_col.f64.values, r_col.f64.values) } },
        .str => .{ .string = .{ .values = try concatKernel(arena, l_col.string.values, r_col.string.values) } },
    };
}

/// String concat kernel: produces `[a[i] ++ b[i]]` for each row. One
/// arena allocation per row's joined bytes; the outer slice is a single
/// allocation. Could be folded later by sharing a bump-pointer in a
/// shared backing buffer if profiling shows the per-row malloc hurts.
fn concatKernel(
    arena: std.mem.Allocator,
    a: []const []const u8,
    b: []const []const u8,
) Error![]const []const u8 {
    std.debug.assert(a.len == b.len);
    const out = try arena.alloc([]const u8, a.len);
    for (a, b, 0..) |x, y, i| {
        const buf = try arena.alloc(u8, x.len + y.len);
        @memcpy(buf[0..x.len], x);
        @memcpy(buf[x.len..], y);
        out[i] = buf;
    }
    return out;
}

/// Hand-coded kernel for binary arithmetic. Comptime-specialized per
/// (T, op) — the inner loop is straight-line scalar code that the
/// optimizer can vectorize. Division by zero in integer types returns
/// an error; in float it yields the IEEE-754 default (Inf / NaN), as
/// is conventional in numeric kernels.
fn applyKernel(
    comptime T: type,
    arena: std.mem.Allocator,
    op: ast.Op,
    a: []const T,
    b: []const T,
) Error![]T {
    std.debug.assert(a.len == b.len);
    const out = try arena.alloc(T, a.len);
    switch (op) {
        .add => for (a, b, 0..) |x, y, i| {
            out[i] = x + y;
        },
        .sub => for (a, b, 0..) |x, y, i| {
            out[i] = x - y;
        },
        .mul => for (a, b, 0..) |x, y, i| {
            out[i] = x * y;
        },
        .div => {
            const is_int = @typeInfo(T) == .int;
            for (a, b, 0..) |x, y, i| {
                if (is_int) {
                    if (y == 0) return error.DivisionByZero;
                    out[i] = @divTrunc(x, y);
                } else {
                    out[i] = x / y;
                }
            }
        },
        // String concat is dispatched via concatKernel — should never
        // reach the numeric kernel.
        .concat => return error.TypeMismatch,
    }
    return out;
}

fn rejectNullableOrNested(col: Batch.Column) Error!void {
    // Reject nested (LIST/MAP) outright. For def-only OPTIONAL: many
    // Parquet writers (notably the benchmark fixture's) emit every
    // column as OPTIONAL even when no rows are actually null. We
    // accept those by scanning def_levels — if every entry == max_def
    // there are zero nulls and arithmetic is well-defined. If any
    // null is present we error: null-aware arithmetic is a follow-up.
    switch (col) {
        inline else => |c| {
            if (c.max_rep > 0) return error.NestedNotSupported;
            if (c.def_levels) |dls| {
                for (dls) |dl| if (dl < c.max_def) return error.NullableNotSupported;
            }
        },
    }
}

fn widenToI64(arena: std.mem.Allocator, col: Batch.Column) Error![]i64 {
    return switch (col) {
        .i32 => |c| blk: {
            const out = try arena.alloc(i64, c.values.len);
            for (c.values, 0..) |v, i| out[i] = v;
            break :blk out;
        },
        .i64 => |c| blk: {
            const out = try arena.alloc(i64, c.values.len);
            @memcpy(out, c.values);
            break :blk out;
        },
        else => return error.TypeMismatch,
    };
}

/// String column borrow — no widening needed (BYTE_ARRAY is the only
/// physical type that maps to `Type.str`). Returns the input slice
/// directly; the caller's arena owns the backing storage and outlives
/// the encode step.
fn borrowStr(col: Batch.Column) Error![]const []const u8 {
    return switch (col) {
        .string => |c| c.values,
        else => return error.TypeMismatch,
    };
}

fn widenToF64(arena: std.mem.Allocator, col: Batch.Column) Error![]f64 {
    return switch (col) {
        .f32 => |c| blk: {
            const out = try arena.alloc(f64, c.values.len);
            for (c.values, 0..) |v, i| out[i] = v;
            break :blk out;
        },
        .f64 => |c| blk: {
            const out = try arena.alloc(f64, c.values.len);
            @memcpy(out, c.values);
            break :blk out;
        },
        // The col_ref's expr_type only resolves to .f64 for FLOAT or
        // DOUBLE physical types — a parser invariant. If we end up
        // here for INT32/INT64 it means evalAs is being called with
        // a target that doesn't match the column.
        else => return error.TypeMismatch,
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

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
    const e: ast.Expr = .{ .binop = .{ .op = .add, .left = lp, .right = rp, .result_type = .i64 } };

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
    const e: ast.Expr = .{ .binop = .{ .op = .mul, .left = lp, .right = rp, .result_type = .i64 } };

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
    const e: ast.Expr = .{ .binop = .{ .op = .add, .left = lp, .right = rp, .result_type = .f64 } };

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
    const e: ast.Expr = .{ .binop = .{ .op = .div, .left = lp, .right = rp, .result_type = .i64 } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualSlices(i64, &.{ 3, 4, 4 }, out.i64.values);
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
    const e: ast.Expr = .{ .binop = .{ .op = .div, .left = lp, .right = rp, .result_type = .i64 } };

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
    const e: ast.Expr = .{ .binop = .{ .op = .concat, .left = lp, .right = rp, .result_type = .str } };

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
    const e: ast.Expr = .{ .binop = .{ .op = .concat, .left = lp, .right = rp, .result_type = .str } };

    const out = try evalExpr(a, &batch, &lookup, e);
    try testing.expectEqualStrings("Hello, world", out.string.values[0]);
    try testing.expectEqualStrings("Goodbye, world", out.string.values[1]);
}

test "nullable column rejected" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const xs = [_]i64{ 1, 2, 3 };
    const def_levels = [_]u32{ 1, 1, 0 };
    const cols = [_]Batch.Column{.{ .i64 = .{ .values = &xs, .def_levels = &def_levels, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};

    const e: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    try testing.expectError(error.NullableNotSupported, evalExpr(a, &batch, &lookup, e));
}
