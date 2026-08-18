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
    UnsupportedCoalesce,
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
        .call => |c| try evalCall(arena, batch, column_lookup, c),
    };
}

/// Evaluate a GROUP BY key expression. Unlike `evalExpr`, nullable
/// source columns are allowed — null rows serialize as absent keys.
/// Binops propagate null when either operand is null.
pub fn evalGroupKeyExpr(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    e: ast.Expr,
) Error!Batch.Column {
    return switch (e) {
        .literal => |lit| literalAsColumn(arena, lit, batch.num_rows),
        .col_ref => |c| try colRefAsGroupKeyColumn(arena, batch, column_lookup, c),
        .binop => |b| try evalGroupKeyBinOp(arena, batch, column_lookup, b),
        .call => |c| try evalCall(arena, batch, column_lookup, c),
    };
}

fn rowIsNull(col: Batch.Column, row: usize) bool {
    return switch (col) {
        inline else => |c| if (c.def_levels) |dl| dl[row] < c.max_def else false,
    };
}

fn colHasNulls(col: Batch.Column) bool {
    return switch (col) {
        inline else => |c| c.def_levels != null and c.max_def > 0,
    };
}

fn colRefAsGroupKeyColumn(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.ColRef,
) Error!Batch.Column {
    const pos = column_lookup[c.col_idx] orelse return error.BadColumn;
    const col = batch.cols[pos];
    switch (col) {
        inline else => |cc| if (cc.max_rep > 0) return error.NestedNotSupported,
    }
    const nulls = col.nullInfo();

    return switch (c.expr_type) {
        .i64 => .{ .i64 = .{
            .values = try widenToI64(arena, col),
            .def_levels = nulls.def_levels,
            .max_def = nulls.max_def,
            .rep_levels = switch (col) {
                inline else => |cc| cc.rep_levels,
            },
            .max_rep = nulls.max_rep,
            .has_nulls = switch (col) {
                inline else => |cc| cc.has_nulls,
            },
        } },
        .f64 => .{ .f64 = .{
            .values = try widenToF64(arena, col),
            .def_levels = nulls.def_levels,
            .max_def = nulls.max_def,
            .rep_levels = switch (col) {
                inline else => |cc| cc.rep_levels,
            },
            .max_rep = nulls.max_rep,
            .has_nulls = switch (col) {
                inline else => |cc| cc.has_nulls,
            },
        } },
        .str => .{ .string = .{
            .values = try borrowStr(col),
            .def_levels = nulls.def_levels,
            .max_def = nulls.max_def,
            .rep_levels = switch (col) {
                inline else => |cc| cc.rep_levels,
            },
            .max_rep = nulls.max_rep,
            .has_nulls = switch (col) {
                inline else => |cc| cc.has_nulls,
            },
        } },
    };
}

fn evalGroupKeyBinOp(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!Batch.Column {
    const l_col = try evalGroupKeyExpr(arena, batch, column_lookup, b.left.*);
    const r_col = try evalGroupKeyExpr(arena, batch, column_lookup, b.right.*);

    return switch (b.result_type) {
        .i64 => evalGroupKeyNumericBinOp(i64, arena, b.op, l_col, r_col),
        .f64 => evalGroupKeyNumericBinOp(f64, arena, b.op, l_col, r_col),
        .str => evalGroupKeyConcatBinOp(arena, l_col, r_col),
    };
}

fn evalGroupKeyNumericBinOp(
    comptime T: type,
    arena: std.mem.Allocator,
    op: ast.Op,
    l_col: Batch.Column,
    r_col: Batch.Column,
) Error!Batch.Column {
    const l_vals = if (T == i64) l_col.i64.values else l_col.f64.values;
    const r_vals = if (T == i64) r_col.i64.values else r_col.f64.values;
    std.debug.assert(l_vals.len == r_vals.len);

    const out = try arena.alloc(T, l_vals.len);
    const needs_nulls = colHasNulls(l_col) or colHasNulls(r_col);
    var def_out: ?[]u32 = null;
    if (needs_nulls) {
        def_out = try arena.alloc(u32, l_vals.len);
    }

    for (l_vals, r_vals, 0..) |lv, rv, i| {
        if (rowIsNull(l_col, i) or rowIsNull(r_col, i)) {
            if (def_out) |dl| dl[i] = 0;
            out[i] = 0;
            continue;
        }
        if (def_out) |dl| dl[i] = 1;
        out[i] = try applyScalarKernel(T, op, lv, rv);
    }

    if (T == i64) {
        return .{ .i64 = .{
            .values = out,
            .def_levels = def_out,
            .max_def = if (needs_nulls) 1 else 0,
        } };
    }
    return .{ .f64 = .{
        .values = out,
        .def_levels = def_out,
        .max_def = if (needs_nulls) 1 else 0,
    } };
}

fn evalGroupKeyConcatBinOp(
    arena: std.mem.Allocator,
    l_col: Batch.Column,
    r_col: Batch.Column,
) Error!Batch.Column {
    const l_vals = l_col.string.values;
    const r_vals = r_col.string.values;
    std.debug.assert(l_vals.len == r_vals.len);

    const out = try arena.alloc([]const u8, l_vals.len);
    const needs_nulls = colHasNulls(l_col) or colHasNulls(r_col);
    var def_out: ?[]u32 = null;
    if (needs_nulls) {
        def_out = try arena.alloc(u32, l_vals.len);
    }

    for (l_vals, r_vals, 0..) |lv, rv, i| {
        if (rowIsNull(l_col, i) or rowIsNull(r_col, i)) {
            if (def_out) |dl| dl[i] = 0;
            out[i] = "";
            continue;
        }
        if (def_out) |dl| dl[i] = 1;
        const buf = try arena.alloc(u8, lv.len + rv.len);
        @memcpy(buf[0..lv.len], lv);
        @memcpy(buf[lv.len..], rv);
        out[i] = buf;
    }

    return .{ .string = .{
        .values = out,
        .def_levels = def_out,
        .max_def = if (needs_nulls) 1 else 0,
    } };
}

fn applyScalarKernel(comptime T: type, op: ast.Op, a: T, b: T) Error!T {
    return switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => blk: {
            if (@typeInfo(T) == .int) {
                if (b == 0) return error.DivisionByZero;
                break :blk @divTrunc(a, b);
            }
            break :blk a / b;
        },
        .concat => return error.TypeMismatch,
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

/// `coalesce(arg1, arg2, ..., default)` — return the first non-null
/// value per row. First-slice scope: only `coalesce(<col_ref>, <literal>)`.
/// Multi-arg with non-literal trailing fallbacks lands when we have
/// a real workload that needs it.
///
/// Semantics:
///   - Walk the column's def_levels per row.
///   - def_level == max_def → row is present → output = col.values[i]
///   - def_level <  max_def → row is null → output = literal default
/// Output column is REQUIRED (no nulls).
fn evalCall(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.Call,
) Error!Batch.Column {
    switch (c.func) {
        .coalesce => return evalCoalesce(arena, batch, column_lookup, c.args, c.result_type),
    }
}

fn evalCoalesce(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    args: []const *ast.Expr,
    result_type: ast.Type,
) Error!Batch.Column {
    if (args.len != 2) return error.UnsupportedCoalesce;
    if (args[0].* != .col_ref) return error.UnsupportedCoalesce;

    // `coalesce(col, X)`. X is either a literal (the original form —
    // "use literal default for null rows") or another column ref
    // ("walk to the second column for null rows"). Cases dispatched
    // separately because the kernels touch different shapes.
    return switch (args[1].*) {
        .literal => |lit| try evalCoalesceColLit(arena, batch, column_lookup, args[0].col_ref, lit, result_type),
        .col_ref => |rhs_ref| try evalCoalesceColCol(arena, batch, column_lookup, args[0].col_ref, rhs_ref, result_type),
        else => error.UnsupportedCoalesce,
    };
}

fn evalCoalesceColLit(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    col_ref: ast.ColRef,
    lit: ast.Literal,
    result_type: ast.Type,
) Error!Batch.Column {
    const pos = column_lookup[col_ref.col_idx] orelse return error.BadColumn;
    const col = batch.cols[pos];

    // Reject nested (LIST/MAP). Null def levels are FINE here — coalesce
    // exists precisely to handle them.
    switch (col) {
        inline else => |cc| if (cc.max_rep > 0) return error.NestedNotSupported,
    }

    return switch (result_type) {
        .i64 => .{ .i64 = .{ .values = try coalesceToI64(arena, col, lit) } },
        .f64 => .{ .f64 = .{ .values = try coalesceToF64(arena, col, lit) } },
        .str => .{ .string = .{ .values = try coalesceToStr(arena, col, lit) } },
    };
}

/// Two-column coalesce: per row, take col_a's value if present;
/// otherwise col_b's value if present; otherwise the type-zero for
/// the result lane (`0`, `0.0`, `""`).
///
/// Type-zero on both-null deviates from SQL's "returns NULL" — ZPQ
/// doesn't emit nullable output columns. Workloads that need strict
/// SQL semantics on the both-null edge should write
/// `coalesce(a, b, <literal>)` (multi-arg form, follow-up feature)
/// or compare against engines using the corresponding 3-arg shape
/// (e.g., DuckDB `coalesce(a, b, '')`).
///
/// Allocation: one output slice (arena-owned). For strings, values
/// are borrowed from the source batch's existing string allocations
/// — no per-row copy. Same model as `coalesceToStr`.
fn evalCoalesceColCol(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    lhs_ref: ast.ColRef,
    rhs_ref: ast.ColRef,
    result_type: ast.Type,
) Error!Batch.Column {
    const pos_a = column_lookup[lhs_ref.col_idx] orelse return error.BadColumn;
    const pos_b = column_lookup[rhs_ref.col_idx] orelse return error.BadColumn;
    const col_a = batch.cols[pos_a];
    const col_b = batch.cols[pos_b];

    // Reject nested. Nullable is FINE (it's the whole point).
    switch (col_a) {
        inline else => |c| if (c.max_rep > 0) return error.NestedNotSupported,
    }
    switch (col_b) {
        inline else => |c| if (c.max_rep > 0) return error.NestedNotSupported,
    }

    return switch (result_type) {
        .i64 => .{ .i64 = .{ .values = try coalesceColColToI64(arena, col_a, col_b) } },
        .f64 => .{ .f64 = .{ .values = try coalesceColColToF64(arena, col_a, col_b) } },
        .str => .{ .string = .{ .values = try coalesceColColToStr(arena, col_a, col_b) } },
    };
}

fn defaultI64(lit: ast.Literal) Error!i64 {
    return switch (lit) {
        .i64 => |v| v,
        else => error.TypeMismatch,
    };
}

fn defaultF64(lit: ast.Literal) Error!f64 {
    return switch (lit) {
        .i64 => |v| @floatFromInt(v),
        .f64 => |v| v,
        else => error.TypeMismatch,
    };
}

fn defaultStr(lit: ast.Literal) Error![]const u8 {
    return switch (lit) {
        .str => |v| v,
        else => error.TypeMismatch,
    };
}

fn coalesceToI64(arena: std.mem.Allocator, col: Batch.Column, lit: ast.Literal) Error![]i64 {
    const def = try defaultI64(lit);
    return switch (col) {
        .i32 => |c| try coalesceWiden(arena, i32, i64, c.values, c.def_levels, c.max_def, def, intWiden(i32, i64)),
        .i64 => |c| try coalesceWiden(arena, i64, i64, c.values, c.def_levels, c.max_def, def, identityI64),
        else => error.TypeMismatch,
    };
}

fn coalesceToF64(arena: std.mem.Allocator, col: Batch.Column, lit: ast.Literal) Error![]f64 {
    const def = try defaultF64(lit);
    return switch (col) {
        .i32 => |c| try coalesceWiden(arena, i32, f64, c.values, c.def_levels, c.max_def, def, intToFloat(i32)),
        .i64 => |c| try coalesceWiden(arena, i64, f64, c.values, c.def_levels, c.max_def, def, intToFloat(i64)),
        .f32 => |c| try coalesceWiden(arena, f32, f64, c.values, c.def_levels, c.max_def, def, floatWiden),
        .f64 => |c| try coalesceWiden(arena, f64, f64, c.values, c.def_levels, c.max_def, def, identityF64),
        else => error.TypeMismatch,
    };
}

fn coalesceToStr(
    arena: std.mem.Allocator,
    col: Batch.Column,
    lit: ast.Literal,
) Error![]const []const u8 {
    const def = try defaultStr(lit);
    return switch (col) {
        .string => |c| blk: {
            const out = try arena.alloc([]const u8, c.values.len);
            if (c.def_levels) |dls| {
                for (c.values, dls, 0..) |v, dl, i| out[i] = if (dl >= c.max_def) v else def;
            } else {
                @memcpy(out, c.values);
            }
            break :blk out;
        },
        else => error.TypeMismatch,
    };
}

/// Generic coalesce kernel. `convert` widens a source value into the
/// result type for non-null rows; null rows fall through to `default`.
/// PLAIN-encoded OPTIONAL columns store null slots as zeroed values,
/// so we still must consult def_levels — we cannot trust the values.
fn coalesceWiden(
    arena: std.mem.Allocator,
    comptime SrcT: type,
    comptime DstT: type,
    values: []const SrcT,
    def_levels: ?[]const u32,
    max_def: u32,
    default: DstT,
    convert: *const fn (SrcT) DstT,
) Error![]DstT {
    const out = try arena.alloc(DstT, values.len);
    if (def_levels) |dls| {
        for (values, dls, 0..) |v, dl, i| out[i] = if (dl >= max_def) convert(v) else default;
    } else {
        for (values, 0..) |v, i| out[i] = convert(v);
    }
    return out;
}

// ============================================================
// Two-column coalesce kernels
// ============================================================
// One per result lane. Each dispatches on the two source columns'
// physical types, then drives a generic walk that picks col_a's
// value when present, col_b's when present, type-zero otherwise.
//
// String special-case: values are borrowed (no per-row copy). The
// source columns' backing storage lives in the same arena as the
// output. Numeric values are widened to the result type at copy
// time per the existing widening helpers.

fn coalesceColColToI64(
    arena: std.mem.Allocator,
    col_a: Batch.Column,
    col_b: Batch.Column,
) Error![]i64 {
    return switch (col_a) {
        .i32 => |a| switch (col_b) {
            .i32 => |b| try walkColColInt(arena, i32, i32, i64, a, b, 0, intWiden(i32, i64), intWiden(i32, i64)),
            .i64 => |b| try walkColColInt(arena, i32, i64, i64, a, b, 0, intWiden(i32, i64), identityI64),
            else => error.TypeMismatch,
        },
        .i64 => |a| switch (col_b) {
            .i32 => |b| try walkColColInt(arena, i64, i32, i64, a, b, 0, identityI64, intWiden(i32, i64)),
            .i64 => |b| try walkColColInt(arena, i64, i64, i64, a, b, 0, identityI64, identityI64),
            else => error.TypeMismatch,
        },
        else => error.TypeMismatch,
    };
}

fn coalesceColColToF64(
    arena: std.mem.Allocator,
    col_a: Batch.Column,
    col_b: Batch.Column,
) Error![]f64 {
    return switch (col_a) {
        .f32 => |a| switch (col_b) {
            .f32 => |b| try walkColColInt(arena, f32, f32, f64, a, b, 0.0, floatWiden, floatWiden),
            .f64 => |b| try walkColColInt(arena, f32, f64, f64, a, b, 0.0, floatWiden, identityF64),
            else => error.TypeMismatch,
        },
        .f64 => |a| switch (col_b) {
            .f32 => |b| try walkColColInt(arena, f64, f32, f64, a, b, 0.0, identityF64, floatWiden),
            .f64 => |b| try walkColColInt(arena, f64, f64, f64, a, b, 0.0, identityF64, identityF64),
            else => error.TypeMismatch,
        },
        // Mixed int+float at the column level — promote both sides
        // through the f64 lane. Less common but the parser sets
        // result_type to f64 when promotion involves a float source.
        .i32 => |a| switch (col_b) {
            .f32 => |b| try walkColColInt(arena, i32, f32, f64, a, b, 0.0, intToFloat(i32), floatWiden),
            .f64 => |b| try walkColColInt(arena, i32, f64, f64, a, b, 0.0, intToFloat(i32), identityF64),
            else => error.TypeMismatch,
        },
        .i64 => |a| switch (col_b) {
            .f32 => |b| try walkColColInt(arena, i64, f32, f64, a, b, 0.0, intToFloat(i64), floatWiden),
            .f64 => |b| try walkColColInt(arena, i64, f64, f64, a, b, 0.0, intToFloat(i64), identityF64),
            else => error.TypeMismatch,
        },
        else => error.TypeMismatch,
    };
}

fn coalesceColColToStr(
    arena: std.mem.Allocator,
    col_a: Batch.Column,
    col_b: Batch.Column,
) Error![]const []const u8 {
    const a = switch (col_a) {
        .string => |c| c,
        else => return error.TypeMismatch,
    };
    const b = switch (col_b) {
        .string => |c| c,
        else => return error.TypeMismatch,
    };
    std.debug.assert(a.values.len == b.values.len);

    const out = try arena.alloc([]const u8, a.values.len);
    // Fast path: col_a is REQUIRED (no def_levels) or all-present.
    // In either case every row's answer is just col_a's value; no
    // need to look at col_b at all.
    if (a.def_levels == null) {
        @memcpy(out, a.values);
        return out;
    }
    const a_dls = a.def_levels.?;

    if (b.def_levels == null) {
        // col_b always present — picks up wherever col_a is null.
        for (a.values, a_dls, b.values, 0..) |va, dla, vb, i| {
            out[i] = if (dla >= a.max_def) va else vb;
        }
        return out;
    }
    const b_dls = b.def_levels.?;
    for (a.values, a_dls, b.values, b_dls, 0..) |va, dla, vb, dlb, i| {
        out[i] = if (dla >= a.max_def)
            va
        else if (dlb >= b.max_def)
            vb
        else
            "";
    }
    return out;
}

/// Walk two columns' (values, def_levels) in parallel, picking the
/// first non-null per row and falling back to `zero_default` when
/// both are null. Each value is run through the supplied widening
/// converter (e.g. i32→i64 or float→f64) at pickup time.
///
/// Comptime-specialized on (SrcAT, SrcBT, DstT) — eight numeric
/// specializations live, all small enough to inline.
fn walkColColInt(
    arena: std.mem.Allocator,
    comptime SrcAT: type,
    comptime SrcBT: type,
    comptime DstT: type,
    a: ColumnT(SrcAT),
    b: ColumnT(SrcBT),
    zero_default: DstT,
    convert_a: *const fn (SrcAT) DstT,
    convert_b: *const fn (SrcBT) DstT,
) Error![]DstT {
    std.debug.assert(a.values.len == b.values.len);
    const out = try arena.alloc(DstT, a.values.len);

    // Two def_level slots are null → both fully present, so the
    // answer is always col_a. Saves a per-row branch in the common
    // "no nulls anywhere" case.
    if (a.def_levels == null) {
        for (a.values, 0..) |va, i| out[i] = convert_a(va);
        return out;
    }
    const a_dls = a.def_levels.?;
    if (b.def_levels == null) {
        for (a.values, a_dls, b.values, 0..) |va, dla, vb, i| {
            out[i] = if (dla >= a.max_def) convert_a(va) else convert_b(vb);
        }
        return out;
    }
    const b_dls = b.def_levels.?;
    for (a.values, a_dls, b.values, b_dls, 0..) |va, dla, vb, dlb, i| {
        out[i] = if (dla >= a.max_def)
            convert_a(va)
        else if (dlb >= b.max_def)
            convert_b(vb)
        else
            zero_default;
    }
    return out;
}

fn intWiden(comptime SrcT: type, comptime DstT: type) *const fn (SrcT) DstT {
    return struct {
        fn f(x: SrcT) DstT {
            return @intCast(x);
        }
    }.f;
}

fn intToFloat(comptime SrcT: type) *const fn (SrcT) f64 {
    return struct {
        fn f(x: SrcT) f64 {
            return @floatFromInt(x);
        }
    }.f;
}

fn floatWiden(x: f32) f64 {
    return x;
}

fn identityI64(x: i64) i64 {
    return x;
}

fn identityF64(x: f64) f64 {
    return x;
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

pub fn widenToI64(arena: std.mem.Allocator, col: Batch.Column) Error![]i64 {
    return switch (col) {
        .i32 => |c| blk: {
            const out = try arena.alloc(i64, c.values.len);
            for (c.values, 0..) |v, i| out[i] = v;
            break :blk out;
        },
        // Already i64 — borrow the underlying values, no copy. Source
        // and destination live in the same arena (the per-RG decode
        // arena), so the borrow lives exactly as long as it needs to.
        // Pre-fix this was alloc + memcpy; profile (2026-05-07) showed
        // 14% of decode-heavy aggregate CPU was spent in this memcpy
        // for queries like `sum(int64_col)`.
        .i64 => |c| @constCast(c.values),
        // BOOLEAN → 0/1 so it flows through the i64 aggregate path
        // (count/sum/min/max). Null slots carry a placeholder the caller's
        // selection mask has already cleared.
        .boolean => |c| blk: {
            const out = try arena.alloc(i64, c.values.len);
            for (c.values, 0..) |v, i| out[i] = @intFromBool(v);
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

pub fn widenToF64(arena: std.mem.Allocator, col: Batch.Column) Error![]f64 {
    return switch (col) {
        .f32 => |c| blk: {
            const out = try arena.alloc(f64, c.values.len);
            for (c.values, 0..) |v, i| out[i] = v;
            break :blk out;
        },
        // Already f64 — borrow, don't copy. See widenToI64 for the
        // why. (Same fix; same 14% memcpy.)
        .f64 => |c| @constCast(c.values),
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

test "coalesce: col + col (str) — both null falls to empty" {
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
    try testing.expectEqualStrings("", out.string.values[1]);
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

test "coalesce: col + col (f64) — both null falls to 0.0" {
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
    try testing.expect(@abs(out.f64.values[0] - 1.5) < 1e-9);
    try testing.expect(@abs(out.f64.values[1] - 0.0) < 1e-9);
    try testing.expect(@abs(out.f64.values[2] - 3.5) < 1e-9);
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
