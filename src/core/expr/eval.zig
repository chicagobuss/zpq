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
//! Scope:
//!   - Flat columns, REQUIRED or OPTIONAL: a NULL operand makes its row's
//!     result NULL (definition levels on the result), and that row is never
//!     computed, so it raises no fault. LIST/MAP columns return
//!     error.NestedNotSupported.
//!   - Integer division truncates (matches Zig `/` on integers). To
//!     get true-division semantics, write `i / 1.0` to force f64.

const std = @import("std");
const ast = @import("ast.zig");
const filter_eval = @import("../filter/eval.zig");
const schema = @import("../schema.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const selection = @import("../filter/selection.zig");
const calendar = @import("calendar.zig");
const regex = @import("regex.zig");
const case_map = @import("case_map.zig");

pub const Error = error{
    BadColumn,
    TypeMismatch,
    NullableNotSupported,
    NestedNotSupported,
    DivisionByZero,
    UnsupportedCoalesce,
    /// An unsigned 64-bit value of 2^63 or more in integer arithmetic, which runs in the signed i64 lane.
    Unsigned64OutOfRange,
    /// An exact DECIMAL result needs more digits than its type's precision (at most 38).
    DecimalOverflow,
    /// An integer `+`, `-`, `*` or `/` whose exact result does not fit the signed 64-bit lane. The query fails rather
    /// than wrap or turn to floating point, as DuckDB does.
    IntegerOverflowAdd,
    IntegerOverflowSub,
    IntegerOverflowMul,
    IntegerOverflowDiv,
    /// `abs` of the minimum of its integer type (DuckDB: "Overflow on abs(-2147483648)").
    IntegerOverflowAbs,
    /// `lpad` / `rpad` with an empty pad of a text that needs padding (DuckDB: "Insufficient padding in LPAD.").
    InsufficientPadding,
    /// A `CAST` whose value does not fit the type it casts to, or is NaN or infinite (DuckDB: "Conversion Error").
    ConversionOutOfRange,
    /// A DECIMAL column reached exact evaluation without its unscaled values (see `ColumnT.unscaled`).
    DecimalUnavailable,
    /// A CASE condition over a repeated (nested) column.
    NestedFilterNotSupported,
    /// A string function reached a column decoded to its byte lengths alone (`ColumnT.byte_lengths`). The decoder
    /// refills every column an aggregate reads before it evaluates, so this is an engine fault, not a query's.
    LengthsOnlyColumn,
} || std.mem.Allocator.Error || calendar.Error || regex.Error;

const Batch = filter_eval.Batch;
const ColumnT = filter_eval.ColumnT;

/// Batch-local DAG for fault-free DOUBLE arithmetic. Checked integer/DECIMAL operations, CASE and functions keep
/// their established expression-major error precedence in the general evaluator. No reassociation occurs here.
pub const FloatPlan = struct {
    nodes: std.ArrayList(Node) = .empty,

    const Node = struct { expr: ast.Expr, children: ?[2]usize };

    pub fn liveBytesPerRow(self: *const FloatPlan) usize {
        // Group IDs and selection included. A nullable result needs levels; FLOAT inputs need both their narrow
        // source and widened lane. Counting borrowed inputs too leaves headroom for the whole expression tree.
        var bytes: usize = 8;
        for (self.nodes.items) |node| bytes += switch (node.expr) {
            .literal => 8,
            .col_ref => |c| if (c.physical_type == .FLOAT) 16 else 12,
            else => 12,
        };
        return bytes;
    }

    pub fn deinit(self: *FloatPlan, a: std.mem.Allocator) void {
        self.nodes.deinit(a);
    }

    /// Null declines the whole fused path before any accumulator is touched. Bound expressions are compared, so
    /// independently parsed occurrences of a common subtree share one result in each batch.
    pub fn add(
        self: *FloatPlan,
        a: std.mem.Allocator,
        batch: *const Batch,
        lookup: []const ?usize,
        e: ast.Expr,
    ) std.mem.Allocator.Error!?usize {
        for (self.nodes.items, 0..) |node, i| if (node.expr.eql(e)) return i;
        const children: ?[2]usize = switch (e) {
            .literal => |l| if (l == .str) return null else null,
            .col_ref => |c| blk: {
                if (c.expr_type != .f64) return null;
                const p = lookup[c.col_idx] orelse return null;
                const col = batch.cols[p];
                if (col.nullInfo().max_rep != 0 or col.codes() != null or col.levelsOnly()) return null;
                if (col != .f64 and col != .f32) return null;
                break :blk null;
            },
            .binop => |b| blk: {
                if (b.result_type != .f64 or b.op == .concat) return null;
                const left = (try self.add(a, batch, lookup, b.left.*)) orelse return null;
                const right = (try self.add(a, batch, lookup, b.right.*)) orelse return null;
                break :blk .{ left, right };
            },
            .call, .case_ => return null,
        };
        try self.nodes.append(a, .{ .expr = e, .children = children });
        return self.nodes.items.len - 1;
    }

    /// Retaining all DAG results bounds scratch by nodes x rows. Inputs are borrowed; literals and arithmetic use
    /// the same conversion and kernels as evalAs/evalBinOp, including NULL propagation and IEEE division.
    pub fn evaluate(
        self: *const FloatPlan,
        a: std.mem.Allocator,
        batch: *const Batch,
        lookup: []const ?usize,
    ) Error![]Batch.Column {
        const values = try a.alloc(Batch.Column, self.nodes.items.len);
        for (self.nodes.items, values) |node, *out| {
            if (node.children) |ch| {
                const l = values[ch[0]];
                const r = values[ch[1]];
                out.* = if (columnHasNullRows(l) or columnHasNullRows(r))
                    try evalGroupKeyNumericBinOp(f64, a, node.expr.binop.op, l, r, .{ .sel = batch.sel })
                else
                    .{ .f64 = .{ .values = try applyKernel(
                        f64,
                        a,
                        node.expr.binop.op,
                        l.f64.values,
                        r.f64.values,
                        .{ .sel = batch.sel },
                    ) } };
            } else if (node.expr == .literal) {
                const v: f64 = switch (node.expr.literal) {
                    .i64 => |x| @floatFromInt(x),
                    .f64 => |x| x,
                    .dec => |x| x.f,
                    .str => unreachable,
                };
                out.* = try literalAsColumn(a, .{ .f64 = v }, batch.num_rows);
            } else {
                out.* = try evalAs(a, batch, lookup, node.expr, .f64);
            }
        }
        return values;
    }
};

/// Test overrides are local to the caller; the environment knob exercises every path on every host.
pub threadlocal var forced_batch_rows: ?usize = null;

pub fn floatBatchRows(live_bytes_per_row: usize, fixed_bytes: usize, option: ?usize) usize {
    const forced = forced_batch_rows orelse option;
    if (forced) |rows| return if (rows == 0) 0 else @max(64, std.mem.alignBackward(usize, rows, 64));
    const budget = @import("../hw.zig").l2BytesPerWorker();
    // Keep a quarter of each sibling's L2 share for the decoder, aggregate state and unrelated hot data.
    const available = (budget - budget / 4) -| fixed_bytes;
    return std.mem.alignBackward(usize, available / @max(1, live_bytes_per_row), 64);
}

test "float batch budgets follow L2 sharing and forced paths independently of the host" {
    const hw = @import("../hw.zig");
    defer hw.test_cpu = null;
    defer forced_batch_rows = null;
    try std.testing.expectEqual(@as(usize, 128), floatBatchRows(100, 4096, 130));
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, 0));
    hw.test_cpu = .{ .l2 = .{ .bytes = 1 << 20, .max_sharing = 2 } };
    try std.testing.expectEqual(@as(usize, 3840), floatBatchRows(100, 4096, null));
    hw.test_cpu.?.l2.bytes = 1280 << 10;
    try std.testing.expectEqual(@as(usize, 4864), floatBatchRows(100, 4096, null));
    hw.test_cpu.?.l2.bytes = 0;
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, null));
    for ([_]usize{ 2048, 4096, 8192, 16384 }) |rows| {
        forced_batch_rows = rows;
        try std.testing.expectEqual(rows, floatBatchRows(100, 4096, null));
    }
    forced_batch_rows = 0;
    try std.testing.expectEqual(@as(usize, 0), floatBatchRows(100, 4096, null));
}

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
        .case_ => |c| try evalCase(arena, batch, column_lookup, c, false),
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
        .case_ => |c| try evalCase(arena, batch, column_lookup, c, true),
    };
}

/// A searched CASE over the batch's wanted rows (`batch.sel`, else all). Each WHEN's condition is evaluated over the
/// rows no earlier WHEN took, its branch over just the rows it takes, so a branch's faults (`1 / 0`) count only where
/// it is chosen; ELSE takes the rest, and without one they are NULL. A row the batch does not want is NULL. Branches
/// evaluate as the caller does: `key` with the null-aware key evaluator.
fn evalCase(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.Case,
    key: bool,
) Error!Batch.Column {
    const n = batch.num_rows;
    var remaining = if (batch.sel) |s| try s.cloneAlloc(arena) else try selection.SelectionVector.init(arena, n);
    var result = try RowAssembly.init(arena, n, c.result_type, c.dec);
    for (0..c.whens.len + 1) |w| {
        if (!remaining.any()) break;
        const is_else = w == c.whens.len;
        var taken = try remaining.cloneAlloc(arena);
        if (!is_else) {
            const unrestricted: Batch = .{ .cols = batch.cols, .num_rows = n };
            try filter_eval.evaluate(c.whens[w].cond, &unrestricted, &taken, column_lookup, arena);
        }
        for (remaining.mask, taken.mask) |*r, t| r.* &= ~t;
        const branch = (if (is_else) c.else_ else c.whens[w].then) orelse continue;
        if (!taken.any()) continue;
        const restricted = batch.restrictedTo(&taken);
        const col = if (key)
            try evalGroupKeyExpr(arena, &restricted, column_lookup, branch.*)
        else
            try evalExpr(arena, &restricted, column_lookup, branch.*);
        try result.take(&taken, col, branch.*);
    }
    return result.finish();
}

/// A result built row by row from columns that each supply some of its rows (CASE's branches, coalesce's arguments),
/// in the result's lane: a value converted exactly (a DECIMAL to the result's scale, an error past its precision), a
/// row no column supplies a value for NULL.
const RowAssembly = struct {
    out: union(enum) { i64: []i64, f64: []f64, str: [][]const u8, dec: []i128 },
    def: []u32,
    dec: ?ast.Dec,

    fn init(arena: std.mem.Allocator, n: usize, result_type: ast.Type, dec: ?ast.Dec) Error!RowAssembly {
        const def = try arena.alloc(u32, n);
        @memset(def, 0);
        return .{ .def = def, .dec = dec, .out = switch (result_type) {
            .i64 => .{ .i64 = blk: {
                const v = try arena.alloc(i64, n);
                @memset(v, 0);
                break :blk v;
            } },
            .f64 => .{ .f64 = blk: {
                const v = try arena.alloc(f64, n);
                @memset(v, 0);
                break :blk v;
            } },
            .str => .{ .str = blk: {
                const v = try arena.alloc([]const u8, n);
                @memset(v, "");
                break :blk v;
            } },
            .dec => .{ .dec = blk: {
                const v = try arena.alloc(i128, n);
                @memset(v, 0);
                break :blk v;
            } },
        } };
    }

    /// `col`'s value, evaluated from `e`, at each row of `rows` where it is present.
    fn take(self: *RowAssembly, rows: *const selection.SelectionVector, col: Batch.Column, e: ast.Expr) Error!void {
        const scale: u8 = if (e.typeOf() == .dec) e.decSpec().?.scale else 0;
        for (0..self.def.len) |i| {
            if (!rows.isActive(i) or rowIsNull(col, i)) continue;
            self.def[i] = 1;
            switch (self.out) {
                .i64 => |v| v[i] = switch (col) {
                    .i64 => |x| x.values[i],
                    else => return error.TypeMismatch,
                },
                .f64 => |v| v[i] = switch (col) {
                    .f64 => |x| x.values[i],
                    .i64 => |x| @floatFromInt(x.values[i]),
                    .i128 => |x| @as(f64, @floatFromInt(x.values[i])) / std.math.pow(f64, 10, @floatFromInt(scale)),
                    else => return error.TypeMismatch,
                },
                .str => |v| v[i] = switch (col) {
                    .string => |x| if (x.codes) |codes| x.dict[codes[i]] else x.values[i],
                    else => return error.TypeMismatch,
                },
                // To the result's scale, exactly; a value its precision cannot hold is an error, never rounded.
                .dec => |v| {
                    const spec = self.dec.?;
                    const raw: i128 = switch (col) {
                        .i128 => |x| x.values[i],
                        .i64 => |x| x.values[i],
                        .i32 => |x| x.values[i],
                        else => return error.TypeMismatch,
                    };
                    const scaled = std.math.mul(i128, raw, ast.pow10(spec.scale - scale)) catch return error.DecimalOverflow;
                    if (@abs(scaled) >= @as(u128, @intCast(spec.bound()))) return error.DecimalOverflow;
                    v[i] = scaled;
                },
            }
        }
    }

    fn finish(self: RowAssembly) Batch.Column {
        const has_nulls = std.mem.indexOfScalar(u32, self.def, 0) != null;
        const levels: ?[]const u32 = if (has_nulls) self.def else null;
        return switch (self.out) {
            .i64 => |v| .{ .i64 = .{ .values = v, .def_levels = levels, .max_def = 1, .has_nulls = has_nulls } },
            .f64 => |v| .{ .f64 = .{ .values = v, .def_levels = levels, .max_def = 1, .has_nulls = has_nulls } },
            .str => |v| .{ .string = .{ .values = v, .def_levels = levels, .max_def = 1, .has_nulls = has_nulls } },
            .dec => |v| .{ .i128 = .{ .values = v, .def_levels = levels, .max_def = 1, .has_nulls = has_nulls } },
        };
    }
};

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
            .values = try widenColRefToI64(arena, col, c),
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
        .dec => .{ .i128 = try unscaledView(arena, col) },
    };
}

fn evalGroupKeyBinOp(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!Batch.Column {
    if (b.result_type == .dec) return .{ .i128 = try decBinOp(arena, batch, column_lookup, b) };
    const l_raw = try evalGroupKeyExpr(arena, batch, column_lookup, b.left.*);
    const r_raw = try evalGroupKeyExpr(arena, batch, column_lookup, b.right.*);
    const l_col = try keyOperandAs(arena, l_raw, b.left.*, b.result_type);
    const r_col = try keyOperandAs(arena, r_raw, b.right.*, b.result_type);

    return switch (b.result_type) {
        .i64 => evalGroupKeyNumericBinOp(i64, arena, b.op, l_col, r_col, .{ .sel = batch.sel }),
        .f64 => evalGroupKeyNumericBinOp(f64, arena, b.op, l_col, r_col, .{ .sel = batch.sel }),
        .str => evalGroupKeyConcatBinOp(arena, l_col, r_col),
        .dec => unreachable,
    };
}

/// A binop operand converted to its result lane, for group keys (and for unsigned 64-bit columns, via `evalAs`). The
/// parser types `i8 + 0.5` as f64 but leaves each side its own type; reading an i64 operand's lane as f64 reinterpreted
/// its bits (2 became 1e-323). An unsigned 64-bit column converts by value, and in the i64 lane must stay below 2^63.
fn keyOperandAs(arena: std.mem.Allocator, col: Batch.Column, e: ast.Expr, target: ast.Type) Error!Batch.Column {
    const unsigned_64 = e == .col_ref and e.col_ref.unsigned_64;
    switch (target) {
        .str => return if (col == .string) col else error.TypeMismatch,
        // Exact operands go through `decColumnAt`, which rescales them to the result.
        .dec => return error.TypeMismatch,
        .i64 => {
            if (col != .i64) return error.TypeMismatch;
            if (unsigned_64) for (col.i64.values, 0..) |v, i| {
                if (v < 0 and !rowIsNull(col, i)) return error.Unsigned64OutOfRange;
            };
            return col;
        },
        .f64 => switch (col) {
            .f64 => return col,
            .i64 => |c| {
                const out = try arena.alloc(f64, c.values.len);
                for (c.values, out) |v, *o| o.* = if (unsigned_64)
                    @floatFromInt(@as(u64, @bitCast(v)))
                else
                    @floatFromInt(v);
                return .{ .f64 = .{
                    .values = out,
                    .def_levels = c.def_levels,
                    .max_def = c.max_def,
                    .has_nulls = c.has_nulls,
                } };
            },
            .i128 => |c| return .{ .f64 = .{
                .values = try decToF64(arena, c.values, e),
                .def_levels = c.def_levels,
                .max_def = c.max_def,
                .has_nulls = c.has_nulls,
            } },
            else => return error.TypeMismatch,
        },
    }
}

fn evalGroupKeyNumericBinOp(
    comptime T: type,
    arena: std.mem.Allocator,
    op: ast.Op,
    l_col: Batch.Column,
    r_col: Batch.Column,
    consumed: Consumed,
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

    var any_null = false;
    for (l_vals, r_vals, 0..) |lv, rv, i| {
        if (rowIsNull(l_col, i) or rowIsNull(r_col, i)) {
            if (def_out) |dl| dl[i] = 0;
            out[i] = 0;
            any_null = true;
            continue;
        }
        if (def_out) |dl| dl[i] = 1;
        out[i] = applyScalarKernel(T, op, lv, rv) catch |err| if (consumed.has(i)) return err else 0;
    }

    // `has_nulls` set from the rows, as readers that narrow a selection by a computed value's NULLs check it.
    if (T == i64) {
        return .{ .i64 = .{
            .values = out,
            .def_levels = def_out,
            .max_def = if (needs_nulls) 1 else 0,
            .has_nulls = any_null,
        } };
    }
    return .{ .f64 = .{
        .values = out,
        .def_levels = def_out,
        .max_def = if (needs_nulls) 1 else 0,
        .has_nulls = any_null,
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

    var any_null = false;
    for (l_vals, r_vals, 0..) |lv, rv, i| {
        if (rowIsNull(l_col, i) or rowIsNull(r_col, i)) {
            if (def_out) |dl| dl[i] = 0;
            out[i] = "";
            any_null = true;
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
        .has_nulls = any_null,
    } };
}

fn applyScalarKernel(comptime T: type, op: ast.Op, a: T, b: T) Error!T {
    if (@typeInfo(T) != .int) return switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => a / b,
        .concat => error.TypeMismatch,
    };
    if (rowFault(T, op, a, b)) |fault| return fault;
    return switch (op) {
        .add => a + b,
        .sub => a - b,
        .mul => a * b,
        .div => @divTrunc(a, b),
        .concat => error.TypeMismatch,
    };
}

/// The rows whose results a query consumes: those in the batch's selection when it has one (a WHERE, an aggregate's
/// FILTER), all of them otherwise. A row left out holds whatever its columns do, so an overflow or a zero divisor on
/// it is not the query's to fail on. Null rows never reach the integer kernels: the vector path refuses nullable
/// operands and the null-aware key path skips them.
const Consumed = struct {
    sel: ?*const selection.SelectionVector = null,

    fn has(self: Consumed, i: usize) bool {
        const s = self.sel orelse return true;
        return s.isActive(i);
    }
};

/// What `a op b` raises on signed integers, if anything: an overflow, or a zero divisor. The one definition the
/// kernels check rows against.
fn rowFault(comptime T: type, op: ast.Op, a: T, b: T) ?Error {
    return switch (op) {
        .add => if (@addWithOverflow(a, b)[1] != 0) error.IntegerOverflowAdd else null,
        .sub => if (@subWithOverflow(a, b)[1] != 0) error.IntegerOverflowSub else null,
        .mul => if (@mulWithOverflow(a, b)[1] != 0) error.IntegerOverflowMul else null,
        .div => if (b == 0)
            error.DivisionByZero
        else if (a == std.math.minInt(T) and b == -1)
            error.IntegerOverflowDiv
        else
            null,
        .concat => error.TypeMismatch,
    };
}

/// After a kernel saw a fault somewhere in `a op b`: the first one on a row the query consumes, if any. Rare, so a
/// plain row loop.
fn firstConsumedFault(comptime T: type, op: ast.Op, a: []const T, b: []const T, consumed: Consumed) Error!void {
    for (a, b, 0..) |x, y, i| {
        if (!consumed.has(i)) continue;
        if (rowFault(T, op, x, y)) |fault| return fault;
    }
}

/// `out = a op b` for `+`, `-` or `*` on signed integers, exact: a vector of lanes at a time with the hardware's
/// overflow flags OR-ed together, and one branch for the whole slice, so the check costs next to nothing. Only when a
/// flag is set are the rows looked at, and only a consumed row's overflow fails the query.
fn checkedIntKernel(
    comptime T: type,
    comptime op: ast.Op,
    out: []T,
    a: []const T,
    b: []const T,
    consumed: Consumed,
) Error!void {
    const L = std.simd.suggestVectorLength(T) orelse 1;
    const V = @Vector(L, T);
    var overflow: @Vector(L, u1) = @splat(0);
    var i: usize = 0;
    while (i + L <= a.len) : (i += L) {
        const x: V = a[i..][0..L].*;
        const y: V = b[i..][0..L].*;
        const r = switch (op) {
            .add => @addWithOverflow(x, y),
            .sub => @subWithOverflow(x, y),
            .mul => @mulWithOverflow(x, y),
            else => @compileError("checkedIntKernel: + - * only"),
        };
        out[i..][0..L].* = r[0];
        overflow |= r[1];
    }
    var tail: u1 = 0;
    while (i < a.len) : (i += 1) {
        const r = switch (op) {
            .add => @addWithOverflow(a[i], b[i]),
            .sub => @subWithOverflow(a[i], b[i]),
            .mul => @mulWithOverflow(a[i], b[i]),
            else => unreachable,
        };
        out[i] = r[0];
        tail |= r[1];
    }
    if ((@reduce(.Or, overflow) | tail) != 0) try firstConsumedFault(T, op, a, b, consumed);
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
    // A DECIMAL operand of a DOUBLE result: a column's f64 view and a literal's double are already the correctly
    // converted values; anything computed converts its exact result once.
    if (target == .f64 and e.typeOf() == .dec) switch (e) {
        .col_ref => |c| {
            const pos = column_lookup[c.col_idx] orelse return error.BadColumn;
            const col = batch.cols[pos];
            try rejectNested(col);
            return withNullsOf(.{ .f64 = .{ .values = try widenToF64(arena, col) } }, col);
        },
        .literal => |l| {
            const out = try arena.alloc(f64, batch.num_rows);
            @memset(out, l.dec.f);
            return .{ .f64 = .{ .values = out } };
        },
        else => {},
    };
    // NULL rows keep their levels through every conversion; `evalBinOp` leaves them uncomputed.
    const result = try evalExpr(arena, batch, column_lookup, e);
    // Only binop operands come through here; an unsigned 64-bit column among them needs the same care as in a key.
    if (e == .col_ref and e.col_ref.unsigned_64) return keyOperandAs(arena, result, e, target);
    if (e.typeOf() == target) return result;
    if (target == .f64 and result == .i128) {
        return withNullsOf(.{ .f64 = .{ .values = try decToF64(arena, result.i128.values, e) } }, result);
    }
    // Promotion: i64 → f64. The reverse never happens for valid ASTs
    // (the parser sets binop.result_type via Type.promote).
    if (target == .f64 and e.typeOf() == .i64) {
        const src = result.i64.values;
        const out = try arena.alloc(f64, src.len);
        for (src, 0..) |v, i| out[i] = @floatFromInt(v);
        return withNullsOf(.{ .f64 = .{ .values = out } }, result);
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
        .dec => |d| blk: {
            const out = try arena.alloc(i128, num_rows);
            @memset(out, d.unscaled);
            break :blk .{ .i128 = .{ .values = out } };
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
    try rejectNested(col);
    // The values as they are (borrowed where the lane matches), under the column's own levels.
    return withNullsOf(switch (c.expr_type) {
        .i64 => .{ .i64 = .{ .values = try widenColRefToI64(arena, col, c) } },
        .f64 => .{ .f64 = .{ .values = try widenToF64(arena, col) } },
        .str => .{ .string = .{ .values = try borrowStr(col) } },
        .dec => .{ .i128 = .{ .values = (try unscaledView(arena, col)).values } },
    }, col);
}

fn evalBinOp(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!Batch.Column {
    if (b.result_type == .dec) return .{ .i128 = try decBinOp(arena, batch, column_lookup, b) };
    const l_col = try evalAs(arena, batch, column_lookup, b.left.*, b.result_type);
    const r_col = try evalAs(arena, batch, column_lookup, b.right.*, b.result_type);

    const consumed: Consumed = .{ .sel = batch.sel };
    // A NULL on either side makes the row NULL, never computed, so its placeholder raises no fault. Only a batch with
    // a NULL row pays for that; the rest run the vector kernels below.
    if (columnHasNullRows(l_col) or columnHasNullRows(r_col)) return switch (b.result_type) {
        .i64 => evalGroupKeyNumericBinOp(i64, arena, b.op, l_col, r_col, consumed),
        .f64 => evalGroupKeyNumericBinOp(f64, arena, b.op, l_col, r_col, consumed),
        .str => evalGroupKeyConcatBinOp(arena, l_col, r_col),
        .dec => unreachable,
    };
    return switch (b.result_type) {
        .i64 => .{ .i64 = .{
            .values = try applyKernel(i64, arena, b.op, l_col.i64.values, r_col.i64.values, consumed),
        } },
        .f64 => .{ .f64 = .{
            .values = try applyKernel(f64, arena, b.op, l_col.f64.values, r_col.f64.values, consumed),
        } },
        .str => .{ .string = .{ .values = try concatKernel(arena, l_col.string.values, r_col.string.values) } },
        .dec => unreachable,
    };
}

// ============================================================
// Exact DECIMAL
// ============================================================

/// Whole-batch exact CSE. Evaluate roots on demand in aggregate order: fallible expression trees cannot be
/// evaluated window-first without changing which error wins. Operand normalization stays left-before-right.
pub const ExactPlan = struct {
    nodes: std.ArrayList(Node) = .empty,
    const Node = struct { expr: ast.Expr, value: ?Batch.Column = null };

    pub fn add(
        self: *ExactPlan,
        a: std.mem.Allocator,
        batch: *const Batch,
        lookup: []const ?usize,
        e: ast.Expr,
    ) std.mem.Allocator.Error!?usize {
        for (self.nodes.items, 0..) |node, i| if (node.expr.eql(e)) return i;
        switch (e) {
            .literal => |l| if (l == .str or l == .f64) return null,
            .col_ref => |c| {
                if (c.expr_type != .dec and c.expr_type != .i64) return null;
                if (c.unsigned_64 or c.unsigned_32) return null;
                const col = batch.cols[lookup[c.col_idx] orelse return null];
                if (col.codes() != null or col.levelsOnly() or col.nullInfo().max_rep != 0) return null;
            },
            .binop => |b| {
                if (b.result_type != .dec or b.op == .div or b.op == .concat) return null;
                _ = (try self.add(a, batch, lookup, b.left.*)) orelse return null;
                _ = (try self.add(a, batch, lookup, b.right.*)) orelse return null;
            },
            else => return null,
        }
        try self.nodes.append(a, .{ .expr = e });
        return self.nodes.items.len - 1;
    }

    pub fn evaluate(
        self: *ExactPlan,
        a: std.mem.Allocator,
        batch: *const Batch,
        lookup: []const ?usize,
        root: usize,
    ) Error!Batch.Column {
        if (self.nodes.items[root].value) |v| return v;
        const e = self.nodes.items[root].expr;
        const v = if (e == .binop)
            try decBinOpLaneCached(a, batch, lookup, e.binop, self)
        else if (e == .col_ref and e.typeOf() == .dec)
            try exactColumn(batch.cols[lookup[e.col_ref.col_idx] orelse return error.BadColumn])
        else
            try evalGroupKeyExpr(a, batch, lookup, e);
        self.nodes.items[root].value = v;
        return v;
    }

    fn operand(
        self: *ExactPlan,
        a: std.mem.Allocator,
        batch: *const Batch,
        lookup: []const ?usize,
        e: ast.Expr,
    ) Error!Batch.Column {
        for (self.nodes.items, 0..) |node, i| if (node.expr.eql(e)) return self.evaluate(a, batch, lookup, i);
        unreachable;
    }
};

/// A decoded DECIMAL column's exact values under its null layout, in the lane of the width they were decoded at
/// (`.i32`, `.i64` or `.i128`, borrowed); refused, not approximated, when it has none.
pub fn exactColumn(col: Batch.Column) Error!Batch.Column {
    switch (col) {
        .i128 => return col,
        inline .f64, .i32, .i64 => |c, physical_tag| {
            if (c.max_rep > 0) return error.NestedNotSupported;
            const u = c.unscaled orelse
                return if (physical_tag == .f64) error.DecimalUnavailable else error.TypeMismatch;
            return switch (u) {
                inline else => |vals, tag| @unionInit(Batch.Column, @tagName(tag), .{
                    .values = vals,
                    .def_levels = c.def_levels,
                    .max_def = c.max_def,
                    .has_nulls = c.has_nulls,
                }),
            };
        },
        else => return error.TypeMismatch,
    }
}

/// `exactColumn` widened to i128 (a copy unless it already is).
pub fn unscaledView(arena: std.mem.Allocator, col: Batch.Column) Error!ColumnT(i128) {
    switch (try exactColumn(col)) {
        .i128 => |c| return c,
        inline .i32, .i64 => |c| {
            const out = try arena.alloc(i128, c.values.len);
            for (c.values, out) |v, *o| o.* = v;
            return .{ .values = out, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls };
        },
        else => unreachable,
    }
}

/// `e`'s exact values (`col`, as evaluated) as unscaled integers at `scale`, which is at least `e`'s own: an integer
/// widens, a DECIMAL at a smaller scale multiplies up. Null layout kept; a null slot holds whatever the decoder left,
/// so it is never multiplied (it must not fail the row group). Errors rather than wraps past i128.
fn decColumnAt(
    arena: std.mem.Allocator,
    col: Batch.Column,
    e: ast.Expr,
    scale: u8,
    /// Rows outside it are not checked (see `Batch.sel`).
    sel: ?*const selection.SelectionVector,
) Error!ColumnT(i128) {
    const own = e.decSpec() orelse return error.TypeMismatch;
    std.debug.assert(scale >= own.scale);
    const factor = ast.pow10(scale - own.scale);
    switch (col) {
        .i128 => |c| {
            if (factor == 1) return c;
            const out = try arena.alloc(i128, c.values.len);
            // |v| <= limit multiplies without leaving i128.
            const limit = @divTrunc(std.math.maxInt(i128), factor);
            for (c.values, out, 0..) |v, *o, i| {
                if (v > limit or v < -limit) {
                    if ((c.def_levels != null and rowIsNull(col, i)) or (sel != null and !sel.?.isActive(i))) {
                        o.* = 0;
                        continue;
                    }
                    return error.DecimalOverflow;
                }
                o.* = v * factor;
            }
            return .{ .values = out, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls };
        },
        inline .i32, .i64, .boolean => |c| {
            const unsigned_32 = e == .col_ref and e.col_ref.unsigned_32;
            const out = try arena.alloc(i128, c.values.len);
            if (factor > ast.pow10(19)) {
                // Only a target scale past 19 gets here, where a 64-bit value times the factor can leave i128.
                for (c.values, out, 0..) |v, *o, i| {
                    if ((c.def_levels != null and rowIsNull(col, i)) or (sel != null and !sel.?.isActive(i))) {
                        o.* = 0;
                        continue;
                    }
                    o.* = std.math.mul(i128, widenInt(v, unsigned_32), factor) catch return error.DecimalOverflow;
                }
            } else for (c.values, out) |v, *o| {
                // |v| < 2^64 and factor <= 10^19: the product stays under 2^127.
                o.* = widenInt(v, unsigned_32) * factor;
            }
            return .{ .values = out, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls };
        },
        else => return error.TypeMismatch,
    }
}

inline fn widenInt(v: anytype, unsigned_32: bool) i128 {
    return switch (@TypeOf(v)) {
        bool => @intFromBool(v),
        i32 => if (unsigned_32) @as(u32, @bitCast(v)) else v,
        else => v,
    };
}

/// One side of an exact binop, read in place: a literal as a scalar, a 64-bit lane (a decoded INT64 decimal, an
/// integer expression) with its rescale folded into the read, and anything else as unscaled i128 at the target scale.
const DecOperand = union(enum) {
    scalar: i128,
    /// Unscaled values that need no rescale.
    i64: []const i64,
    /// Values times `factor` (at most 10^19, so a 64-bit value times it stays inside i128).
    i64s: struct { v: []const i64, factor: i128 },
    i128: []const i128,
};

/// `e` as an exact operand at `scale`, and its null layout.
fn decOperandCached(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    e: ast.Expr,
    scale: u8,
    plan: ?*ExactPlan,
) Error!struct { op: DecOperand, nulls: ?Batch.Column } {
    if (e == .literal) {
        const one = try decColumnAt(arena, try literalAsColumn(arena, e.literal, 1), e, scale, null);
        return .{ .op = .{ .scalar = one.values[0] }, .nulls = null };
    }
    // A DECIMAL column is read from the decoder's integers, a DECIMAL binop at its narrowest lane; anything else is
    // evaluated first.
    var raw: Batch.Column = undefined;
    if (plan) |p| {
        raw = try p.operand(arena, batch, column_lookup, e);
    } else if (e == .col_ref and e.typeOf() == .dec) {
        const col = batch.cols[column_lookup[e.col_ref.col_idx] orelse return error.BadColumn];
        raw = try exactColumn(col);
    } else if (e == .binop and e.binop.result_type == .dec) {
        raw = try decBinOpLane(arena, batch, column_lookup, e.binop);
    } else {
        raw = try evalGroupKeyExpr(arena, batch, column_lookup, e);
    }
    const nulls: ?Batch.Column = if (columnHasNullRows(raw)) raw else null;
    const factor = ast.pow10(scale - e.decSpec().?.scale);
    if (raw == .i64 and !(e == .col_ref and e.col_ref.unsigned_32)) {
        if (factor == 1) return .{ .op = .{ .i64 = raw.i64.values }, .nulls = nulls };
        if (factor <= ast.pow10(19)) {
            return .{ .op = .{ .i64s = .{ .v = raw.i64.values, .factor = factor } }, .nulls = nulls };
        }
    }
    return .{ .op = .{ .i128 = (try decColumnAt(arena, raw, e, scale, batch.sel)).values }, .nulls = nulls };
}

/// Whether some row of `col` is null. An OPTIONAL column with no nulls has levels but none below max, and not every
/// builder of a column sets `has_nulls`, so the levels decide.
fn columnHasNullRows(col: Batch.Column) bool {
    return switch (col) {
        inline else => |c| if (c.def_levels) |dl| for (dl) |d| {
            if (d < c.max_def) break true;
        } else false else false,
    };
}

/// `b` (a DECIMAL `+`, `-` or `*`) over the batch, in the i128 lane every exact expression result takes.
fn decBinOp(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!ColumnT(i128) {
    switch (try decBinOpLane(arena, batch, column_lookup, b)) {
        .i128 => |c| return c,
        .i64 => |c| {
            const out = try arena.alloc(i128, c.values.len);
            for (c.values, out) |v, *o| o.* = v;
            return .{ .values = out, .def_levels = c.def_levels, .max_def = c.max_def, .has_nulls = c.has_nulls };
        },
        else => unreachable,
    }
}

/// `b` over the batch, at the narrowest lane its type allows: a result of at most 18 digits in `.i64`, which an
/// enclosing operation then reads without widening. A sum or difference brings both sides to the result scale first;
/// a product multiplies the operands as they are, its scale being their scales' sum. Each result must stay under
/// 10^precision, so a value too wide for the type is an error, not a wrap. A row null on either side is null, and is
/// never computed: its slots hold whatever the decoder left.
fn decBinOpLane(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
) Error!Batch.Column {
    return decBinOpLaneCached(arena, batch, column_lookup, b, null);
}

fn decBinOpLaneCached(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    b: ast.BinOp,
    plan: ?*ExactPlan,
) Error!Batch.Column {
    const spec = b.dec.?;
    const lhs_scale, const rhs_scale = switch (b.op) {
        .add, .sub => .{ spec.scale, spec.scale },
        .mul => .{ b.left.decSpec().?.scale, b.right.decSpec().?.scale },
        .div, .concat => unreachable,
    };
    const l = try decOperandCached(arena, batch, column_lookup, b.left.*, lhs_scale, plan);
    const r = try decOperandCached(arena, batch, column_lookup, b.right.*, rhs_scale, plan);
    const n = batch.num_rows;
    const bound: u128 = @intCast(spec.bound());
    // `+` and `-` first bring both sides to their common type, as the SQL engines cast them: a value that does not
    // fit it (an integer 1 against DECIMAL(38,38), 10^37 against a scale-1 operand) is an error there, so here too.
    // A product takes its operands as they are.
    // Below the 38-digit cap the common type holds every value either side's type can, so only a capped one checks.
    const operand_bound: u128 = switch (b.op) {
        .add, .sub => blk: {
            const ls = b.left.decSpec().?;
            const rs = b.right.decSpec().?;
            const digits = @as(u16, @max(ls.precision - ls.scale, rs.precision - rs.scale)) + @max(ls.scale, rs.scale);
            break :blk if (digits <= ast.Dec.max_precision)
                std.math.maxInt(u128)
            else
                @intCast(ast.Dec.common(ls, rs).bound());
        },
        else => std.math.maxInt(u128),
    };
    const def: ?[]u32 = if (l.nulls != null or r.nulls != null) try arena.alloc(u32, n) else null;
    if (def) |d| for (d, 0..) |*dv, i| {
        const is_null = (l.nulls != null and rowIsNull(l.nulls.?, i)) or (r.nulls != null and rowIsNull(r.nulls.?, i));
        dv.* = @intFromBool(!is_null);
    };
    // A physical int64 result also fits every logical precision above 18. A failed checked lane retries the
    // canonical wide kernel before any root reaches an aggregate; NULL and inactive overflow remain non-errors.
    if (plan != null and spec.precision > 18) {
        switch (b.op) {
            inline .add, .sub, .mul => |op| switch (l.op) {
                inline .scalar, .i64 => |lv, lt| switch (r.op) {
                    inline .scalar, .i64 => |rv, rt| {
                        const ls = if (lt == .scalar) std.math.cast(i64, lv) else @as(?i64, 0);
                        const rs = if (rt == .scalar) std.math.cast(i64, rv) else @as(?i64, 0);
                        if (ls != null and rs != null) {
                            const out = try arena.alloc(i64, n);
                            if (decChecked64(op, lt, rt, lv, rv, out, def, operand_bound, batch.sel)) {
                                return .{ .i64 = .{
                                    .values = out,
                                    .def_levels = def,
                                    .max_def = if (def != null) 1 else 0,
                                    .has_nulls = def != null,
                                } };
                            }
                            arena.free(out);
                        }
                    },
                    else => {},
                },
                else => {},
            },
            else => unreachable,
        }
    }
    // Under 10^18 every value fits i64, which the bound check enforces.
    const narrow = spec.precision <= 18;
    switch (b.op) {
        inline .add, .sub, .mul => |op| switch (l.op) {
            inline else => |lv, lt| switch (r.op) {
                inline else => |rv, rt| inline for (.{ i64, i128 }) |Out| if ((Out == i64) == narrow) {
                    const out = try arena.alloc(Out, n);
                    if (def) |d|
                        try decKernel(op, lt, rt, true, lv, rv, out, d, bound, operand_bound, batch.sel)
                    else
                        try decKernel(op, lt, rt, false, lv, rv, out, &.{}, bound, operand_bound, batch.sel);
                    const col: ColumnT(Out) = if (def) |d|
                        .{ .values = out, .def_levels = d, .max_def = 1, .has_nulls = true }
                    else
                        .{ .values = out };
                    return if (Out == i64) .{ .i64 = col } else .{ .i128 = col };
                },
            },
        },
        .div, .concat => unreachable,
    }
    unreachable;
}

fn decChecked64(
    comptime op: ast.Op,
    comptime lt: std.meta.Tag(DecOperand),
    comptime rt: std.meta.Tag(DecOperand),
    l: anytype,
    r: anytype,
    out: []i64,
    def: ?[]const u32,
    operand_bound: u128,
    sel: ?*const selection.SelectionVector,
) bool {
    for (out, 0..) |*v, i| {
        if (def != null and def.?[i] == 0) {
            v.* = 0;
            continue;
        }
        const x: i64 = if (lt == .scalar) @intCast(l) else l[i];
        const y: i64 = if (rt == .scalar) @intCast(r) else r[i];
        const result = switch (op) {
            .add => @addWithOverflow(x, y),
            .sub => @subWithOverflow(x, y),
            .mul => @mulWithOverflow(x, y),
            else => comptime unreachable,
        };
        if (result[1] != 0 or (op != .mul and (@as(u128, @abs(x)) >= operand_bound or
            @as(u128, @abs(y)) >= operand_bound)))
        {
            if (sel != null and !sel.?.isActive(i)) {
                v.* = 0;
                continue;
            }
            return false;
        }
        v.* = result[0];
    }
    return true;
}

/// Row `i` of an operand, unscaled at the binop's scale.
inline fn decAt(comptime tag: std.meta.Tag(DecOperand), op: anytype, i: usize) i128 {
    return switch (tag) {
        .scalar => op,
        .i64 => op[i],
        .i64s => @as(i128, op.v[i]) * op.factor,
        .i128 => op[i],
    };
}

fn decKernel(
    comptime op: ast.Op,
    comptime lt: std.meta.Tag(DecOperand),
    comptime rt: std.meta.Tag(DecOperand),
    comptime nullable: bool,
    l: anytype,
    r: anytype,
    out: anytype,
    def: []const u32,
    bound: u128,
    operand_bound: u128,
    sel: ?*const selection.SelectionVector,
) Error!void {
    for (out, 0..) |*o, i| {
        if (nullable and def[i] == 0) {
            o.* = 0;
            continue;
        }
        const v: ?i128 = switch (op) {
            inline .add, .sub => |aop| blk: {
                const x = decAt(lt, l, i);
                const y = decAt(rt, r, i);
                if (@abs(x) >= operand_bound or @abs(y) >= operand_bound) break :blk null;
                break :blk (if (aop == .add) std.math.add(i128, x, y) else std.math.sub(i128, x, y)) catch null;
            },
            // Two unscaled 64-bit values multiply wide, which cannot overflow.
            .mul => if (lt == .i64 and rt == .i64)
                std.math.mulWide(i64, l[i], r[i])
            else
                mulExact(decAt(lt, l, i), decAt(rt, r, i)) catch null,
            else => comptime unreachable,
        };
        if (v == null or @abs(v.?) >= bound) {
            // Only a row the filter keeps is an error; the selection is read on this path alone.
            if (sel != null and !sel.?.isActive(i)) {
                o.* = 0;
                continue;
            }
            return error.DecimalOverflow;
        }
        o.* = @intCast(v.?);
    }
}

/// Operands that fit 64 bits take one widening multiply, which cannot overflow; only wider ones pay for the checked
/// 128-bit product.
inline fn mulExact(x: i128, y: i128) error{Overflow}!i128 {
    if (std.math.cast(i64, x)) |x64| if (std.math.cast(i64, y)) |y64| return std.math.mulWide(i64, x64, y64);
    return std.math.mul(i128, x, y);
}

/// Exact values of `e` (unscaled) as doubles: each converted once, the way decode converts a decimal column.
fn decToF64(arena: std.mem.Allocator, values: []const i128, e: ast.Expr) Error![]f64 {
    const scale = (e.decSpec() orelse return error.TypeMismatch).scale;
    const out = try arena.alloc(f64, values.len);
    for (values, out) |v, *o| o.* = decimal_mod.applyScaleI128(v, scale);
    return out;
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
        .coalesce => return evalCoalesce(arena, batch, column_lookup, c.args, c.result_type, c.dec),
        .strlen => return evalStrlen(arena, batch, column_lookup, c.args[0].*),
        .make_date, .epoch_ms, .date_trunc, .date_part => return evalTemporal(arena, batch, column_lookup, c),
        .regexp_replace => return evalRegexpReplace(arena, batch, column_lookup, c, false),
        .cast_text => return evalCastText(arena, batch, column_lookup, c.args[0].*),
        .abs => return evalAbs(arena, batch, column_lookup, c),
        .length => return evalLength(arena, batch, column_lookup, c.args[0].*),
        .substring, .left, .right, .upper, .lower, .trim, .ltrim, .rtrim, .replace, .lpad, .rpad => return evalStringFn(arena, batch, column_lookup, c),
        .cast_i8, .cast_i16, .cast_i32, .cast_i64, .cast_double, .cast_dec, .cast_date, .cast_timestamp => return evalCast(arena, batch, column_lookup, c),
        .floor, .ceil, .sqrt, .round => return evalMath(arena, batch, column_lookup, c),
        .least, .greatest => return evalLeastGreatest(arena, batch, column_lookup, c),
    }
}

/// The text of a string argument as the batch holds it: a bare column as decoded (codes and all), anything else
/// evaluated first.
fn stringArgument(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg: ast.Expr) Error!struct { col: Batch.Column, s: ColumnT([]const u8) } {
    const col = if (arg == .col_ref)
        batch.cols[column_lookup[arg.col_ref.col_idx] orelse return error.BadColumn]
    else
        try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const s = switch (col) {
        .string => |v| v,
        else => return error.TypeMismatch,
    };
    if (s.max_rep > 0) return error.NestedNotSupported;
    // A column decoded to lengths alone (for `= ''`) has no texts to work on.
    if (s.codes == null and s.values.len != s.rowCount()) return error.LengthsOnlyColumn;
    return .{ .col = col, .s = s };
}

/// Characters of `text`: its UTF-8 code points (a continuation byte starts none).
fn charCount(text: []const u8) usize {
    var n: usize = 0;
    for (text) |b| n += @intFromBool(b & 0xC0 != 0x80);
    return n;
}

/// The byte offset where character `k` (0-based) starts; the length for `k` at or past the end.
fn charOffset(text: []const u8, k: usize) usize {
    if (k == 0) return 0;
    var seen: usize = 0;
    for (text, 0..) |b, i| {
        if (b & 0xC0 != 0x80) {
            if (seen == k) return i;
            seen += 1;
        }
    }
    return text.len;
}

/// Characters `lo..hi` (1-based, inclusive, already clipped to the text) of `text`.
fn charRange(text: []const u8, n: usize, lo: i128, hi: i128) []const u8 {
    const first = @max(lo, 1);
    const last = @min(hi, @as(i128, @intCast(n)));
    if (first > last) return "";
    return text[charOffset(text, @intCast(first - 1))..charOffset(text, @intCast(last))];
}

/// A mapped text: a slice of the input, or `bytes[start..end]` of the buffer results are built in (an offset, as the
/// buffer moves while it grows).
const Mapped = union(enum) { text: []const u8, built: [2]usize };

fn appendMapped(bytes: *std.ArrayList(u8), arena: std.mem.Allocator, parts: []const []const u8) Error!Mapped {
    const start = bytes.items.len;
    for (parts) |p| try bytes.appendSlice(arena, p);
    return .{ .built = .{ start, bytes.items.len } };
}

/// Whether `cp` is one of the space separators (Unicode category Zs) above U+00A0, the characters DuckDB's `trim` strips
/// beside U+0020 and U+00A0. Found by asking DuckDB 2.0.0 about every code point: a tab, a line break, NEL and the
/// zero-width spaces stay.
fn isWideSpace(cp: u21) bool {
    return switch (cp) {
        0x1680, 0x2000...0x200A, 0x202F, 0x205F, 0x3000 => true,
        else => false,
    };
}

/// The byte length of the space that opens `s`, 0 if it does not open with one.
fn spaceAtStart(s: []const u8) usize {
    if (s.len == 0) return 0;
    if (s[0] == ' ') return 1;
    if (s.len >= 2 and s[0] == 0xC2 and s[1] == 0xA0) return 2;
    if (s.len >= 3 and s[0] >= 0xE1 and s[0] <= 0xE3) {
        const cp = std.unicode.utf8Decode(s[0..3]) catch return 0;
        if (isWideSpace(cp)) return 3;
    }
    return 0;
}

/// The byte length of the space that closes `s`, 0 if it does not close with one. A three-byte space starts with a lead
/// byte, which no continuation byte of another character can be.
fn spaceAtEnd(s: []const u8) usize {
    if (s.len == 0) return 0;
    if (s[s.len - 1] == ' ') return 1;
    if (s.len >= 2 and s[s.len - 2] == 0xC2 and s[s.len - 1] == 0xA0) return 2;
    if (s.len >= 3 and s[s.len - 3] >= 0xE1 and s[s.len - 3] <= 0xE3) {
        const cp = std.unicode.utf8Decode(s[s.len - 3 ..]) catch return 0;
        if (isWideSpace(cp)) return 3;
    }
    return 0;
}

/// One text through a string function; `bytes` holds what is built. Characters are code points, positions 1-based.
fn applyStringFn(arena: std.mem.Allocator, bytes: *std.ArrayList(u8), c: ast.Call, text: []const u8) Error!Mapped {
    const int = struct {
        fn at(call: ast.Call, k: usize) i128 {
            return call.args[k].literal.i64;
        }
    }.at;
    switch (c.func) {
        .substring => {
            const n: i128 = @intCast(charCount(text));
            const start = int(c, 1);
            const pos: i128 = if (start > 0) start else if (start < 0) n + start + 1 else 0;
            var lo = pos;
            var hi = n;
            if (c.args.len == 3) {
                const len = int(c, 2);
                if (len >= 0) {
                    hi = pos + len - 1;
                } else {
                    lo = pos + len;
                    hi = pos - 1;
                }
            }
            return .{ .text = charRange(text, @intCast(n), lo, hi) };
        },
        .left, .right => {
            const n: i128 = @intCast(charCount(text));
            const k = int(c, 1);
            return .{ .text = switch (c.func) {
                .left => charRange(text, @intCast(n), 1, if (k >= 0) k else n + k),
                else => charRange(text, @intCast(n), if (k >= 0) n - k + 1 else -k + 1, n),
            } };
        },
        .upper, .lower => {
            // Code points map one to one, and many texts change nothing: those stay a view of the input.
            const start = bytes.items.len;
            var changed = false;
            var at: usize = 0;
            while (at < text.len) {
                const b = text[at];
                if (b < 0x80) {
                    const m = if (c.func == .upper) std.ascii.toUpper(b) else std.ascii.toLower(b);
                    changed = changed or m != b;
                    try bytes.append(arena, m);
                    at += 1;
                    continue;
                }
                const n = std.unicode.utf8ByteSequenceLength(b) catch 1;
                const cp = if (n > 1 and at + n <= text.len) std.unicode.utf8Decode(text[at..][0..n]) catch null else null;
                const mapped = if (cp) |x| (if (c.func == .upper) case_map.upper(x) else case_map.lower(x)) else null;
                if (mapped) |m| {
                    var buf: [4]u8 = undefined;
                    const len = std.unicode.utf8Encode(@intCast(m), &buf) catch unreachable;
                    changed = changed or m != cp.?;
                    try bytes.appendSlice(arena, buf[0..len]);
                    at += n;
                } else {
                    // Bytes that are no UTF-8 code point pass through as they are.
                    try bytes.append(arena, b);
                    at += 1;
                }
            }
            if (!changed) {
                bytes.shrinkRetainingCapacity(start);
                return .{ .text = text };
            }
            return .{ .built = .{ start, bytes.items.len } };
        },
        .trim, .ltrim, .rtrim => {
            var lo: usize = 0;
            var hi: usize = text.len;
            if (c.func != .rtrim) while (lo < hi) {
                const n = spaceAtStart(text[lo..hi]);
                if (n == 0) break;
                lo += n;
            };
            if (c.func != .ltrim) while (hi > lo) {
                const n = spaceAtEnd(text[lo..hi]);
                if (n == 0) break;
                hi -= n;
            };
            return .{ .text = text[lo..hi] };
        },
        .replace => {
            const from = c.args[1].literal.str;
            const to = c.args[2].literal.str;
            if (from.len == 0 or std.mem.indexOf(u8, text, from) == null) return .{ .text = text };
            const start = bytes.items.len;
            var at: usize = 0;
            while (std.mem.indexOfPos(u8, text, at, from)) |hit| {
                try bytes.appendSlice(arena, text[at..hit]);
                try bytes.appendSlice(arena, to);
                at = hit + from.len;
            }
            try bytes.appendSlice(arena, text[at..]);
            return .{ .built = .{ start, bytes.items.len } };
        },
        .lpad, .rpad => {
            const want = int(c, 1);
            if (want <= 0) return .{ .text = "" };
            const n = charCount(text);
            if (n >= want) return .{ .text = charRange(text, n, 1, want) };
            const pad = c.args[2].literal.str;
            const pad_chars = charCount(pad);
            if (pad_chars == 0) return error.InsufficientPadding;
            const need: usize = @intCast(want - @as(i128, @intCast(n)));
            const start = bytes.items.len;
            if (c.func == .rpad) try bytes.appendSlice(arena, text);
            var left_over = need;
            while (left_over > 0) {
                const take = @min(left_over, pad_chars);
                try bytes.appendSlice(arena, charRange(pad, pad_chars, 1, @intCast(take)));
                left_over -= take;
            }
            if (c.func == .lpad) try bytes.appendSlice(arena, text);
            return .{ .built = .{ start, bytes.items.len } };
        },
        else => unreachable,
    }
}

/// The string functions of one text and constants, applied once per distinct text: per dictionary entry where the
/// chunk is coded, per value otherwise (the last text's answer is kept for a run of equal ones). A NULL row stays NULL
/// and only a row the batch wants can fault.
fn evalStringFn(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call) Error!Batch.Column {
    const arg = try stringArgument(arena, batch, column_lookup, c.args[0].*);
    const s = arg.s;
    const inputs = if (s.codes != null) s.dict else s.values;
    var bytes: std.ArrayList(u8) = .empty;
    const mapped = try arena.alloc(Mapped, inputs.len);
    var last: ?usize = null;
    for (inputs, mapped, 0..) |text, *m, i| {
        m.* = .{ .text = "" };
        if (s.codes == null and (rowIsNull(arg.col, i) or (batch.sel != null and !batch.sel.?.isActive(i)))) continue;
        if (last) |l| if (std.mem.eql(u8, inputs[l], text)) {
            m.* = mapped[l];
            continue;
        };
        last = i;
        m.* = try applyStringFn(arena, &bytes, c, text);
    }
    const texts = try arena.alloc([]const u8, inputs.len);
    for (mapped, texts) |m, *t| t.* = switch (m) {
        .text => |x| x,
        .built => |b| bytes.items[b[0]..b[1]],
    };
    const values = if (s.codes) |codes| blk: {
        const out = try arena.alloc([]const u8, codes.len);
        for (codes, out) |code, *o| o.* = if (code < texts.len) texts[code] else "";
        break :blk out;
    } else texts;
    return .{ .string = .{ .values = values, .def_levels = s.def_levels, .max_def = s.max_def, .has_nulls = s.has_nulls } };
}

/// `length(s)`: characters of each text, once per dictionary entry where the chunk is coded.
fn evalLength(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg_expr: ast.Expr) Error!Batch.Column {
    const arg = try stringArgument(arena, batch, column_lookup, arg_expr);
    const s = arg.s;
    const out = try arena.alloc(i64, s.rowCount());
    if (s.codes) |codes| {
        const lens = try arena.alloc(i64, s.dict.len);
        defer arena.free(lens);
        for (s.dict, lens) |v, *l| l.* = @intCast(charCount(v));
        for (codes, out) |code, *o| o.* = if (code < lens.len) lens[code] else 0;
    } else for (s.values, out) |v, *o| {
        o.* = @intCast(charCount(v));
    }
    return .{ .i64 = .{ .values = out, .def_levels = s.def_levels, .max_def = s.max_def, .has_nulls = s.has_nulls } };
}

/// `abs(x)`: the magnitude in the argument's type. A NULL row stays NULL. Only a row the batch wants can fault: an
/// integer whose magnitude its type cannot hold (the column's SQL type for a bare column, BIGINT otherwise).
fn evalAbs(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call) Error!Batch.Column {
    const arg = c.args[0].*;
    if (arg.typeOf() == .dec) return evalAbsDecimal(arena, batch, column_lookup, arg);
    const col = try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    switch (col) {
        .i64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            const out = try arena.alloc(i64, in.values.len);
            const t = c.int orelse .i64;
            for (in.values, out, 0..) |v, *o, i| {
                o.* = v;
                if (v >= 0) continue;
                if (std.math.negate(v)) |m| {
                    if (t.holds(m)) {
                        o.* = m;
                        continue;
                    }
                } else |_| {}
                if (rowIsNull(col, i) or (batch.sel != null and !batch.sel.?.isActive(i))) {
                    o.* = 0;
                    continue;
                }
                return error.IntegerOverflowAbs;
            }
            return .{ .i64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
        },
        .f64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            const out = try arena.alloc(f64, in.values.len);
            for (in.values, out) |v, *o| o.* = @abs(v);
            return .{ .f64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
        },
        else => return error.TypeMismatch,
    }
}

/// `abs` of an exact DECIMAL: its unscaled integers, read as any exact operand is, at their own scale.
fn evalAbsDecimal(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg: ast.Expr) Error!Batch.Column {
    const o = try decOperandCached(arena, batch, column_lookup, arg, arg.decSpec().?.scale, null);
    const n = batch.num_rows;
    const out = try arena.alloc(i128, n);
    switch (o.op) {
        .scalar => |v| @memset(out, @as(i128, @intCast(@abs(v)))),
        .i64 => |vs| for (vs, out) |v, *r| {
            r.* = @intCast(@abs(@as(i128, v)));
        },
        .i64s => |x| for (x.v, out) |v, *r| {
            r.* = @intCast(@abs(@as(i128, v) * x.factor));
        },
        // |v| < 10^38, so the magnitude fits i128 as the value did.
        .i128 => |vs| for (vs, out) |v, *r| {
            r.* = @intCast(@abs(v));
        },
    }
    const def: ?[]u32 = if (o.nulls) |nc| blk: {
        const d = try arena.alloc(u32, n);
        for (d, 0..) |*dv, i| dv.* = @intFromBool(!rowIsNull(nc, i));
        break :blk d;
    } else null;
    return .{ .i128 = .{ .values = out, .def_levels = def, .max_def = if (def != null) 1 else 0, .has_nulls = def != null } };
}

/// 10^n as the doubles DuckDB multiplies and divides by (`NumericHelper::DOUBLE_POWERS_OF_TEN`), parsed, not computed.
const double_powers_of_ten = [_]f64{
    1e0,  1e1,  1e2,  1e3,  1e4,  1e5,  1e6,  1e7,  1e8,  1e9,  1e10, 1e11, 1e12, 1e13, 1e14, 1e15, 1e16, 1e17, 1e18, 1e19,
    1e20, 1e21, 1e22, 1e23, 1e24, 1e25, 1e26, 1e27, 1e28, 1e29, 1e30, 1e31, 1e32, 1e33, 1e34, 1e35, 1e36, 1e37, 1e38,
};

/// `x` rounded to a whole number, a tie to the even one: how DuckDB takes a DOUBLE to an integer.
fn roundHalfEven(x: f64) f64 {
    const t = @trunc(x);
    if (@abs(x - t) != 0.5) return @round(x);
    return if (@mod(t, 2) == 0) t else @round(x);
}

/// `v / d` rounded half away from zero, d > 0: how DuckDB takes a DECIMAL to an integer or to fewer places.
fn divRoundHalfAway(v: i128, d: i128) i128 {
    const q = @divTrunc(v, d);
    const r = @rem(v, d);
    if (@as(u128, @abs(r)) * 2 < @as(u128, @intCast(d))) return q;
    return if (v < 0) q - 1 else q + 1;
}

/// A DOUBLE as the integer type `t`: NaN, an infinity and what rounds outside the type are conversion errors.
fn doubleAsInt(t: ast.IntType, x: f64) Error!i64 {
    if (!std.math.isFinite(x)) return error.ConversionOutOfRange;
    const r = roundHalfEven(x);
    // The type's range as doubles: its minimum, and its maximum plus one (a power of two, so exact).
    const low: f64 = @floatFromInt(t.minValue());
    const high: f64 = @floatFromInt(t.maxValue() + 1);
    if (!(r >= low and r < high)) return error.ConversionOutOfRange;
    return @intFromFloat(r);
}

/// An exact operand's unscaled rows at `scale` (at least its own; an integer expression counts as scale 0), its
/// definition levels, and the column its NULLs are read from.
const DecRows = struct { v: []i128, def: ?[]u32, nulls: ?Batch.Column };

fn decRows(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg: ast.Expr, scale: u8) Error!DecRows {
    const o = try decOperandCached(arena, batch, column_lookup, arg, scale, null);
    const n = batch.num_rows;
    const v = try arena.alloc(i128, n);
    switch (o.op) {
        .scalar => |x| @memset(v, x),
        .i64 => |vs| for (vs, v) |x, *r| {
            r.* = x;
        },
        .i64s => |x| for (x.v, v) |y, *r| {
            r.* = @as(i128, y) * x.factor;
        },
        .i128 => |vs| @memcpy(v, vs),
    }
    const def: ?[]u32 = if (o.nulls) |nc| blk: {
        const d = try arena.alloc(u32, n);
        for (d, 0..) |*dv, i| dv.* = @intFromBool(!rowIsNull(nc, i));
        break :blk d;
    } else null;
    return .{ .v = v, .def = def, .nulls = o.nulls };
}

fn decColumn(rows: DecRows, v: []i128) Batch.Column {
    return .{ .i128 = .{ .values = v, .def_levels = rows.def, .max_def = if (rows.def != null) 1 else 0, .has_nulls = rows.def != null } };
}

fn f64Column(v: []f64, def: ?[]const u32, max_def: u32, has_nulls: bool) Batch.Column {
    return .{ .f64 = .{ .values = v, .def_levels = def, .max_def = max_def, .has_nulls = has_nulls } };
}

/// `floor`, `ceil`, `sqrt` and `round` of a number, row by row as DuckDB does. A DECIMAL floors and ceils to DECIMAL(p, 0)
/// and rounds half away from zero (past its precision is `DecimalOverflow`, in a row that is read); an integer or a DOUBLE
/// floors and ceils to a DOUBLE, `sqrt` is a DOUBLE and NaN below zero.
fn evalMath(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call) Error!Batch.Column {
    const arg = c.args[0].*;
    const n = batch.num_rows;
    const digits: i32 = if (c.args.len == 2) @intCast(c.args[1].literal.i64) else 0;
    if (arg.typeOf() == .dec) {
        const spec = arg.decSpec().?;
        const rows = try decRows(arena, batch, column_lookup, arg, spec.scale);
        if (c.func == .sqrt) {
            const out = try arena.alloc(f64, n);
            for (rows.v, out) |x, *r| r.* = @sqrt(ast.decimalToDouble(x, spec.scale));
            return f64Column(out, rows.def, if (rows.def != null) 1 else 0, rows.def != null);
        }
        const out = try arena.alloc(i128, n);
        const divisor = ast.pow10(spec.scale);
        for (rows.v, out, 0..) |x, *r, i| {
            r.* = switch (c.func) {
                .floor => @divFloor(x, divisor),
                .ceil => -@divFloor(-x, divisor),
                else => ast.roundDecimal(x, spec.scale, digits) catch {
                    if (quietDecimalRow(batch, rows.nulls, i)) {
                        r.* = 0;
                        continue;
                    }
                    return error.DecimalOverflow;
                },
            };
            if (@abs(r.*) >= @as(u128, @intCast(ast.pow10(c.dec.?.precision)))) {
                if (quietDecimalRow(batch, rows.nulls, i)) r.* = 0 else return error.DecimalOverflow;
            }
        }
        return decColumn(rows, out);
    }
    const col = try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    switch (col) {
        .i64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            if (c.func == .round) return col;
            const out = try arena.alloc(f64, n);
            for (in.values, out) |v, *r| {
                const x: f64 = @floatFromInt(v);
                r.* = if (c.func == .sqrt) @sqrt(x) else x;
            }
            return f64Column(out, in.def_levels, in.max_def, in.has_nulls);
        },
        .f64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            const out = try arena.alloc(f64, n);
            for (in.values, out) |v, *r| r.* = switch (c.func) {
                .sqrt => @sqrt(v),
                .floor => @floor(v),
                .ceil => @ceil(v),
                else => ast.roundDouble(v, digits),
            };
            return f64Column(out, in.def_levels, in.max_def, in.has_nulls);
        },
        else => return error.TypeMismatch,
    }
}

/// NaN above every number, as DuckDB orders it: `a` is below `b`.
fn belowNanLast(a: f64, b: f64) bool {
    if (std.math.isNan(a)) return false;
    return std.math.isNan(b) or a < b;
}

/// `least` / `greatest`: per row the smallest or largest argument that is not NULL, NULL where all are. The arguments are
/// read in the result's lane: integers as they are, a DOUBLE result converts integers and DECIMALs, a DECIMAL result
/// reads every argument at its scale.
fn evalLeastGreatest(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call) Error!Batch.Column {
    const n = batch.num_rows;
    const largest = c.func == .greatest;
    const def = try arena.alloc(u32, n);
    @memset(def, 0);
    switch (c.result_type) {
        .i64 => {
            const out = try arena.alloc(i64, n);
            @memset(out, 0);
            for (c.args) |a| {
                const col = try evalGroupKeyExpr(arena, batch, column_lookup, a.*);
                if (col != .i64 or col.i64.max_rep > 0) return error.TypeMismatch;
                for (col.i64.values, out, def, 0..) |v, *o, *d, i| {
                    if (rowIsNull(col, i)) continue;
                    if (d.* == 0 or (if (largest) v > o.* else v < o.*)) o.* = v;
                    d.* = 1;
                }
            }
            return .{ .i64 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = std.mem.indexOfScalar(u32, def, 0) != null } };
        },
        .f64 => {
            const out = try arena.alloc(f64, n);
            @memset(out, 0);
            for (c.args) |a| {
                const vals = try arena.alloc(f64, n);
                var nulls: ?Batch.Column = null;
                if (a.typeOf() == .dec) {
                    const spec = a.decSpec().?;
                    const rows = try decRows(arena, batch, column_lookup, a.*, spec.scale);
                    for (rows.v, vals) |x, *r| r.* = ast.decimalToDouble(x, spec.scale);
                    nulls = rows.nulls;
                } else {
                    const col = try evalGroupKeyExpr(arena, batch, column_lookup, a.*);
                    nulls = col;
                    switch (col) {
                        .i64 => |in| for (in.values, vals) |v, *r| {
                            r.* = @floatFromInt(v);
                        },
                        .f64 => |in| @memcpy(vals, in.values),
                        else => return error.TypeMismatch,
                    }
                }
                for (vals, out, def, 0..) |v, *o, *d, i| {
                    if (nulls) |nc| if (rowIsNull(nc, i)) continue;
                    if (d.* == 0 or (if (largest) belowNanLast(o.*, v) else belowNanLast(v, o.*))) o.* = v;
                    d.* = 1;
                }
            }
            return f64Column(out, def, 1, std.mem.indexOfScalar(u32, def, 0) != null);
        },
        .dec => {
            const scale = c.dec.?.scale;
            const out = try arena.alloc(i128, n);
            @memset(out, 0);
            for (c.args) |a| {
                const rows = try decRows(arena, batch, column_lookup, a.*, scale);
                for (rows.v, out, def, 0..) |v, *o, *d, i| {
                    if (rows.nulls) |nc| if (rowIsNull(nc, i)) continue;
                    if (d.* == 0 or (if (largest) v > o.* else v < o.*)) o.* = v;
                    d.* = 1;
                }
            }
            return .{ .i128 = .{ .values = out, .def_levels = def, .max_def = 1, .has_nulls = std.mem.indexOfScalar(u32, def, 0) != null } };
        },
        .str => return error.TypeMismatch,
    }
}

/// `CAST(x AS TINYINT | SMALLINT | INTEGER | BIGINT | DOUBLE | DECIMAL(p, s))`, converted as DuckDB does: an
/// integer to another by range, a DECIMAL to an integer or to fewer places half away from zero, a DOUBLE to an integer
/// half to even and to a DECIMAL as `round(x * 10^s)` in double. A value that does not
/// fit is `ConversionOutOfRange`, but only in a row the batch wants and the data does not make NULL.
fn evalCast(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call) Error!Batch.Column {
    const arg = c.args[0].*;
    if (arg.typeOf() == .dec) return evalCastDecimal(arena, batch, column_lookup, c, arg);
    const col = try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const target_dec: ast.Dec = c.dec orelse .{ .precision = 38, .scale = 0 };
    switch (col) {
        .i64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            const n = in.values.len;
            switch (c.func) {
                .cast_i64 => return col,
                .cast_i8, .cast_i16, .cast_i32 => {
                    const t = c.int.?;
                    const out = try arena.alloc(i64, n);
                    for (in.values, out, 0..) |v, *o, i| {
                        o.* = v;
                        if (t.holds(v)) continue;
                        if (quietRow(batch, col, i)) o.* = 0 else return error.ConversionOutOfRange;
                    }
                    return .{ .i64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                .cast_double => {
                    const out = try arena.alloc(f64, n);
                    for (in.values, out) |v, *o| o.* = @floatFromInt(v);
                    return .{ .f64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                .cast_date, .cast_timestamp => {
                    const unit = arg.temporal().?;
                    const out = try arena.alloc(i64, n);
                    for (in.values, out, 0..) |v, *o, i| {
                        o.* = temporalCast(c.func == .cast_date, unit, v) catch if (quietRow(batch, col, i)) 0 else return error.ConversionOutOfRange;
                    }
                    return .{ .i64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                .cast_dec => {
                    const out = try arena.alloc(i128, n);
                    const factor = ast.pow10(target_dec.scale);
                    const bound = ast.pow10(target_dec.precision);
                    for (in.values, out, 0..) |v, *o, i| {
                        const scaled = std.math.mul(i128, v, factor) catch null;
                        if (scaled != null and @abs(scaled.?) < @as(u128, @intCast(bound))) {
                            o.* = scaled.?;
                        } else if (quietRow(batch, col, i)) o.* = 0 else return error.ConversionOutOfRange;
                    }
                    return .{ .i128 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                else => unreachable,
            }
        },
        .f64 => |in| {
            if (in.max_rep > 0) return error.NestedNotSupported;
            const n = in.values.len;
            switch (c.func) {
                .cast_double => return col,
                .cast_i8, .cast_i16, .cast_i32, .cast_i64 => {
                    const t = c.int.?;
                    const out = try arena.alloc(i64, n);
                    for (in.values, out, 0..) |v, *o, i| {
                        o.* = doubleAsInt(t, v) catch if (quietRow(batch, col, i)) 0 else return error.ConversionOutOfRange;
                    }
                    return .{ .i64 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                .cast_dec => {
                    const out = try arena.alloc(i128, n);
                    const factor = double_powers_of_ten[target_dec.scale];
                    const bound = ast.pow10(target_dec.precision);
                    for (in.values, out, 0..) |v, *o, i| {
                        const scaled = @round(v * factor);
                        // 2^126 is far above any 38-digit bound and keeps the integer conversion defined.
                        if (std.math.isFinite(scaled) and @abs(scaled) < 0x1p126 and @abs(@as(i128, @intFromFloat(scaled))) < @as(u128, @intCast(bound))) {
                            o.* = @intFromFloat(scaled);
                        } else if (quietRow(batch, col, i)) o.* = 0 else return error.ConversionOutOfRange;
                    }
                    return .{ .i128 = .{ .values = out, .def_levels = in.def_levels, .max_def = in.max_def, .has_nulls = in.has_nulls } };
                },
                else => unreachable,
            }
        },
        else => return error.TypeMismatch,
    }
}

/// A row nobody reads: unwanted by the batch's selection, or NULL in the data.
fn quietRow(batch: *const Batch, col: Batch.Column, i: usize) bool {
    return rowIsNull(col, i) or (batch.sel != null and !batch.sel.?.isActive(i));
}

/// `quietRow` for an exact operand, whose NULLs (if it has any) are in `nulls`.
fn quietDecimalRow(batch: *const Batch, nulls: ?Batch.Column, i: usize) bool {
    if (nulls) |nc| if (rowIsNull(nc, i)) return true;
    return batch.sel != null and !batch.sel.?.isActive(i);
}

const us_per_day: i64 = 86_400_000_000;
const ms_per_day: i64 = 86_400_000;

/// `v` divided by `per`, to the nearest, a tie away from zero.
fn roundDivAway(v: i64, per: i64) i64 {
    const q = @divTrunc(v, per);
    const r = @rem(v, per);
    return if (@as(i128, @abs(r)) * 2 >= per) q + (if (v < 0) @as(i64, -1) else 1) else q;
}

/// `CAST(v AS DATE)` (`to_date`) or `CAST(v AS TIMESTAMP)` of `v` counted in `unit`, as DuckDB 2.0 casts it. A day is the
/// floor of the time over a day (so a time before 1970 is the day before); a TIMESTAMP_NS reaches microseconds to the
/// nearest, a tie away from zero (the DATE of one nanosecond before the epoch is 1970-01-01); a DATE and a TIMESTAMP_MS
/// reach microseconds exactly, and one beyond the range of a TIMESTAMP is `ConversionOutOfRange`. Infinity stays infinity.
/// A UTC-adjusted value is read in UTC, as the date parts do.
fn temporalCast(to_date: bool, unit: ast.Temporal, v: i64) error{ConversionOutOfRange}!i64 {
    const ts_infinity: i64 = std.math.maxInt(i64);
    const date_infinity: i64 = std.math.maxInt(i32);
    if (unit == .date) {
        if (to_date) return v;
        if (v == date_infinity) return ts_infinity;
        if (v == -date_infinity) return -ts_infinity;
        return std.math.mul(i64, v, us_per_day) catch error.ConversionOutOfRange;
    }
    if (v == ts_infinity) return if (to_date) date_infinity else ts_infinity;
    if (v == -ts_infinity) return if (to_date) -date_infinity else -ts_infinity;
    if (unit == .ts_ms and to_date) return @divFloor(v, ms_per_day);
    const us: i64 = switch (unit) {
        .ts_us => v,
        .ts_ms => std.math.mul(i64, v, 1000) catch return error.ConversionOutOfRange,
        .ts_ns => roundDivAway(v, 1000),
        .date => unreachable,
    };
    return if (to_date) @divFloor(us, us_per_day) else us;
}

test "casting between DATE and TIMESTAMP matches DuckDB 2.0.0" {
    const T = std.testing;
    // TIMESTAMP to DATE floors: the last microsecond of 1969 is 1969-12-31, the epoch is 1970-01-01.
    try T.expectEqual(@as(i64, -1), try temporalCast(true, .ts_us, -1));
    try T.expectEqual(@as(i64, 0), try temporalCast(true, .ts_us, 0));
    try T.expectEqual(@as(i64, 0), try temporalCast(true, .ts_us, us_per_day - 1));
    try T.expectEqual(@as(i64, -1), try temporalCast(true, .ts_us, -us_per_day));
    try T.expectEqual(@as(i64, -2), try temporalCast(true, .ts_us, -us_per_day - 1));
    try T.expectEqual(@as(i64, -1), try temporalCast(true, .ts_ms, -1));
    try T.expectEqual(@as(i64, 1), try temporalCast(true, .ts_ms, ms_per_day));
    // A TIMESTAMP_NS reaches microseconds half away from zero first: -1 ns is the epoch, -500 ns the microsecond before.
    try T.expectEqual(@as(i64, 0), try temporalCast(false, .ts_ns, -1));
    try T.expectEqual(@as(i64, -1), try temporalCast(false, .ts_ns, -999));
    try T.expectEqual(@as(i64, -1), try temporalCast(false, .ts_ns, -500));
    try T.expectEqual(@as(i64, 0), try temporalCast(false, .ts_ns, -499));
    try T.expectEqual(@as(i64, 1), try temporalCast(false, .ts_ns, 500));
    try T.expectEqual(@as(i64, 1), try temporalCast(false, .ts_ns, 999));
    try T.expectEqual(@as(i64, 0), try temporalCast(true, .ts_ns, -1));
    try T.expectEqual(@as(i64, -1), try temporalCast(true, .ts_ns, -500));
    // DATE and TIMESTAMP_MS to TIMESTAMP are exact; one past the TIMESTAMP range is an error.
    try T.expectEqual(@as(i64, 19782 * us_per_day), try temporalCast(false, .date, 19782));
    try T.expectEqual(@as(i64, -999_000), try temporalCast(false, .ts_ms, -999));
    try T.expectEqual(@as(i64, 106751991 * us_per_day), try temporalCast(false, .date, 106751991));
    try T.expectError(error.ConversionOutOfRange, temporalCast(false, .date, 106751992));
    try T.expectError(error.ConversionOutOfRange, temporalCast(false, .ts_ms, std.math.maxInt(i64) - 1));
    // Infinity maps to infinity, both ways and in every unit.
    try T.expectEqual(@as(i64, std.math.maxInt(i64)), try temporalCast(false, .date, std.math.maxInt(i32)));
    try T.expectEqual(@as(i64, -std.math.maxInt(i32)), try temporalCast(true, .ts_ns, -std.math.maxInt(i64)));
    try T.expectEqual(@as(i64, std.math.maxInt(i64)), try temporalCast(false, .ts_ms, std.math.maxInt(i64)));
    // Identity casts.
    try T.expectEqual(@as(i64, -165), try temporalCast(true, .date, -165));
    try T.expectEqual(@as(i64, -47524859999999), try temporalCast(false, .ts_us, -47524859999999));
}

/// `evalCast` of an exact DECIMAL argument, read at its own scale.
fn evalCastDecimal(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, c: ast.Call, arg: ast.Expr) Error!Batch.Column {
    const spec = arg.decSpec().?;
    const rows = try decRows(arena, batch, column_lookup, arg, spec.scale);
    const n = batch.num_rows;
    const v = rows.v;
    const def = rows.def;
    const levels: u32 = if (def != null) 1 else 0;
    switch (c.func) {
        .cast_double => {
            const out = try arena.alloc(f64, n);
            for (v, out) |x, *r| r.* = ast.decimalToDouble(x, spec.scale);
            return .{ .f64 = .{ .values = out, .def_levels = def, .max_def = levels, .has_nulls = def != null } };
        },
        .cast_i8, .cast_i16, .cast_i32, .cast_i64 => {
            const t = c.int.?;
            const out = try arena.alloc(i64, n);
            const divisor = ast.pow10(spec.scale);
            for (v, out, 0..) |x, *r, i| {
                const q = divRoundHalfAway(x, divisor);
                if (t.holds(q)) {
                    r.* = @intCast(q);
                } else if (quietDecimalRow(batch, rows.nulls, i)) {
                    r.* = 0;
                } else return error.ConversionOutOfRange;
            }
            return .{ .i64 = .{ .values = out, .def_levels = def, .max_def = levels, .has_nulls = def != null } };
        },
        .cast_dec => {
            const target = c.dec.?;
            const bound: u128 = @intCast(ast.pow10(target.precision));
            const out = try arena.alloc(i128, n);
            for (v, out, 0..) |x, *r, i| {
                const scaled: ?i128 = if (target.scale >= spec.scale)
                    std.math.mul(i128, x, ast.pow10(target.scale - spec.scale)) catch null
                else
                    divRoundHalfAway(x, ast.pow10(spec.scale - target.scale));
                if (scaled != null and @abs(scaled.?) < bound) {
                    r.* = scaled.?;
                } else if (quietDecimalRow(batch, rows.nulls, i)) {
                    r.* = 0;
                } else return error.ConversionOutOfRange;
            }
            return .{ .i128 = .{ .values = out, .def_levels = def, .max_def = levels, .has_nulls = def != null } };
        },
        else => return error.TypeMismatch,
    }
}

/// `CAST(s AS VARCHAR)`: the argument's texts as they are, a bare column read as decoded, codes and all.
fn evalCastText(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg: ast.Expr) Error!Batch.Column {
    const col = if (arg == .col_ref)
        batch.cols[column_lookup[arg.col_ref.col_idx] orelse return error.BadColumn]
    else
        try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const s = switch (col) {
        .string => |s| s,
        else => return error.TypeMismatch,
    };
    if (s.max_rep > 0) return error.NestedNotSupported;
    // A column decoded to lengths alone (for `= ''`) has no texts to pass on.
    if (s.codes == null and s.values.len != s.rowCount()) return error.LengthsOnlyColumn;
    return col;
}

/// Coded grouping consumes this view directly; ordinary expression consumers retain their row value lanes.
pub fn evalCodedKey(arena: std.mem.Allocator, batch: *const Batch, lookup: []const ?usize, e: ast.Expr) Error!Batch.Column {
    if (e == .col_ref) return batch.cols[lookup[e.col_ref.col_idx] orelse return error.BadColumn];
    if (e != .call or e.call.func != .regexp_replace) return error.TypeMismatch;
    return evalRegexpReplace(arena, batch, lookup, e.call, true);
}

/// `regexp_replace` over a string column: a dictionary-coded column is replaced once per entry and read back by code,
/// a plain one row by row, skipping NULL rows and rows the batch does not want. A text without a match is borrowed,
/// not copied; replaced texts share one buffer.
fn evalRegexpReplace(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.Call,
    comptime keep_codes: bool,
) Error!Batch.Column {
    const arg = c.args[0].*;
    const col = if (arg == .col_ref)
        batch.cols[column_lookup[arg.col_ref.col_idx] orelse return error.BadColumn]
    else
        try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const s = switch (col) {
        .string => |v| v,
        else => return error.TypeMismatch,
    };
    if (s.max_rep > 0) return error.NestedNotSupported;
    // A column decoded to lengths alone (for `= ''`) has no texts to replace.
    if (s.codes == null and s.values.len != s.rowCount()) return error.LengthsOnlyColumn;
    const replace = c.replace.?;
    var scratch: regex.Scratch = .init(arena);
    var bytes: std.ArrayList(u8) = .empty;
    const inputs = if (s.codes != null) s.dict else s.values;
    // Ends in `bytes` of each replaced text, recorded as offsets because the buffer moves as it grows.
    const ends = try arena.alloc(regex.Replace.Result, inputs.len);
    // Neighbouring rows often repeat a text (a visit's hits share a Referer), so the last one's answer is kept.
    var last: ?usize = null;
    for (inputs, ends, 0..) |text, *end, i| {
        end.* = .unchanged;
        if (s.codes == null and (rowIsNull(col, i) or (batch.sel != null and !batch.sel.?.isActive(i)))) continue;
        if (last) |l| if (std.mem.eql(u8, inputs[l], text)) {
            end.* = ends[l];
            continue;
        };
        last = i;
        end.* = try replace.applyBorrowing(&scratch, &bytes, text);
    }
    const mapped = try arena.alloc([]const u8, inputs.len);
    for (inputs, ends, mapped) |text, end, *m| m.* = switch (end) {
        .unchanged => text,
        .borrowed => |view| view,
        .built => |bounds| bytes.items[bounds[0]..bounds[1]],
    };
    if (keep_codes and s.codes != null) return .{ .string = .{
        .values = &.{},
        .codes = s.codes,
        .dict = mapped,
        .def_levels = s.def_levels,
        .max_def = s.max_def,
        .has_nulls = s.has_nulls,
    } };
    const values = if (s.codes) |codes| blk: {
        const out = try arena.alloc([]const u8, codes.len);
        for (codes, out) |code, *o| o.* = if (code < mapped.len) mapped[code] else "";
        break :blk out;
    } else mapped;
    return .{ .string = .{
        .values = values,
        .def_levels = s.def_levels,
        .max_def = s.max_def,
        .has_nulls = s.has_nulls,
    } };
}

/// `out[i] = f(t, in[i])` over an i64 column, a fault counting only at a present row `sel` wants.
inline fn mapRows(
    col: Batch.Column,
    sel: ?*const selection.SelectionVector,
    out: []i64,
    comptime f: fn (?ast.Temporal, i64) calendar.Error!i64,
    t: ?ast.Temporal,
) Error!void {
    for (col.i64.values, out, 0..) |v, *o, i| o.* = f(t, v) catch |err| fault: {
        if (rowIsNull(col, i) or (sel != null and !sel.?.isActive(i))) break :fault 0;
        return err;
    };
}

/// The date/time functions, integer to integer row by row: a NULL stays NULL, and only a row the batch wants
/// (`batch.sel`) can fault, as in arithmetic.
fn evalTemporal(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    c: ast.Call,
) Error!Batch.Column {
    const arg = c.args[c.args.len - 1].*;
    const col = try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const in = switch (col) {
        .i64 => |v| v,
        else => return error.TypeMismatch,
    };
    if (in.max_rep > 0) return error.NestedNotSupported;
    const out = try arena.alloc(i64, in.values.len);
    const t = arg.temporal();
    // The function and part are fixed for the call, so each pair gets its own loop with constant divisors.
    switch (c.func) {
        .make_date => try mapRows(col, batch.sel, out, struct {
            fn f(_: ?ast.Temporal, v: i64) calendar.Error!i64 {
                return if (std.math.cast(i32, v) != null) v else error.DateOutOfRange;
            }
        }.f, t),
        .epoch_ms => try mapRows(col, batch.sel, out, struct {
            fn f(_: ?ast.Temporal, v: i64) calendar.Error!i64 {
                return std.math.mul(i64, v, std.time.us_per_ms) catch error.TimestampOutOfRange;
            }
        }.f, t),
        .date_trunc => switch (c.part.?) {
            .dow, .isodow, .doy => unreachable,
            inline else => |p| try mapRows(col, batch.sel, out, struct {
                fn f(tt: ?ast.Temporal, v: i64) calendar.Error!i64 {
                    return calendar.trunc(p, tt.?, v);
                }
            }.f, t),
        },
        .date_part => switch (c.part.?) {
            inline else => |p| try mapRows(col, batch.sel, out, struct {
                fn f(tt: ?ast.Temporal, v: i64) calendar.Error!i64 {
                    return calendar.part(p, tt.?, v);
                }
            }.f, t),
        },
        .coalesce, .strlen, .regexp_replace, .cast_text, .abs, .length, .substring, .left, .right, .upper, .lower, .trim, .ltrim, .rtrim, .replace, .lpad, .rpad, .cast_i8, .cast_i16, .cast_i32, .cast_i64, .cast_double, .cast_dec, .cast_date, .cast_timestamp, .floor, .ceil, .sqrt, .round, .least, .greatest => unreachable,
    }
    return .{ .i64 = .{
        .values = out,
        .def_levels = in.def_levels,
        .max_def = in.max_def,
        .has_nulls = in.has_nulls,
    } };
}

/// Byte length of each string; a NULL string's row stays NULL (its definition level carries over). A dictionary-coded
/// column is measured once per entry and looked up by code.
fn evalStrlen(arena: std.mem.Allocator, batch: *const Batch, column_lookup: []const ?usize, arg: ast.Expr) Error!Batch.Column {
    // A bare column is read as decoded, codes and all; anything else evaluates first.
    const col = if (arg == .col_ref)
        batch.cols[column_lookup[arg.col_ref.col_idx] orelse return error.BadColumn]
    else
        try evalGroupKeyExpr(arena, batch, column_lookup, arg);
    const s = switch (col) {
        .string => |s| s,
        else => return error.TypeMismatch,
    };
    if (s.max_rep > 0) return error.NestedNotSupported;
    const rows = s.rowCount();
    const out = try arena.alloc(i64, rows);
    if (s.codes) |codes| {
        const lens = try arena.alloc(i64, s.dict.len);
        defer arena.free(lens);
        for (s.dict, lens) |v, *l| l.* = @intCast(v.len);
        for (codes, out) |code, *o| o.* = if (code < lens.len) lens[code] else 0;
    } else for (s.values, out) |v, *o| {
        o.* = @intCast(v.len);
    }
    return .{ .i64 = .{ .values = out, .def_levels = s.def_levels, .max_def = s.max_def, .has_nulls = s.has_nulls } };
}

fn evalCoalesce(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    args: []const *ast.Expr,
    result_type: ast.Type,
    c_dec: ?ast.Dec,
) Error!Batch.Column {
    // The kernels below serve the common `coalesce(column, literal | column)`; any other arguments, as many as given,
    // take the general form.
    const fast = args.len == 2 and args[0].* == .col_ref and (args[1].* == .literal or args[1].* == .col_ref);
    if (!fast) return evalCoalesceAny(arena, batch, column_lookup, args, result_type, c_dec);

    // `coalesce(col, X)`. X is either a literal (the original form —
    // "use literal default for null rows") or another column ref
    // ("walk to the second column for null rows"). Cases dispatched
    // separately because the kernels touch different shapes.
    return switch (args[1].*) {
        .literal => |lit| try evalCoalesceColLit(arena, batch, column_lookup, args[0].col_ref, lit, result_type, c_dec),
        .col_ref => |rhs_ref| try evalCoalesceColCol(
            arena,
            batch,
            column_lookup,
            args[0].col_ref,
            rhs_ref,
            result_type,
            c_dec,
        ),
        else => unreachable,
    };
}

/// `coalesce` over any arguments: each evaluated, NULLs kept, over just the rows every earlier one left NULL, so its
/// faults (`coalesce(a, 100 / b)` where `b` is 0) count only where it is the answer; its present values are taken.
fn evalCoalesceAny(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    args: []const *ast.Expr,
    result_type: ast.Type,
    c_dec: ?ast.Dec,
) Error!Batch.Column {
    const n = batch.num_rows;
    var remaining = if (batch.sel) |s| try s.cloneAlloc(arena) else try selection.SelectionVector.init(arena, n);
    var result = try RowAssembly.init(arena, n, result_type, c_dec);
    for (args) |arg| {
        if (!remaining.any()) break;
        const col = try evalGroupKeyExpr(arena, &batch.restrictedTo(&remaining), column_lookup, arg.*);
        try result.take(&remaining, col, arg.*);
        for (result.def, 0..) |d, i| if (d == 1) remaining.set(i, false);
    }
    return result.finish();
}

fn evalCoalesceColLit(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    col_ref: ast.ColRef,
    lit: ast.Literal,
    result_type: ast.Type,
    c_dec: ?ast.Dec,
) Error!Batch.Column {
    const pos = column_lookup[col_ref.col_idx] orelse return error.BadColumn;
    const col = batch.cols[pos];

    // Reject nested (LIST/MAP). Null def levels are FINE here — coalesce
    // exists precisely to handle them.
    switch (col) {
        inline else => |cc| if (cc.max_rep > 0) return error.NestedNotSupported,
    }

    return switch (result_type) {
        .i64 => .{ .i64 = .{
            .values = try firstPresent(i64, arena, col, try coalesceI64(arena, col, col_ref), &.{try defaultI64(lit)}),
        } },
        .f64 => .{ .f64 = .{
            .values = try firstPresent(f64, arena, col, try coalesceF64(arena, col, col_ref), &.{try defaultF64(lit)}),
        } },
        .str => .{ .string = .{
            .values = try firstPresent([]const u8, arena, col, try coalesceStr(col), &.{try defaultStr(lit)}),
        } },
        .dec => blk: {
            const spec = c_dec.?;
            const ref: ast.Expr = .{ .col_ref = col_ref };
            const a = try decColumnAt(arena, try exactView(col, ref), ref, spec.scale, batch.sel);
            const one = try literalAsColumn(arena, lit, 1);
            const fallback = try decColumnAt(arena, one, .{ .literal = lit }, spec.scale, null);
            const out = try firstPresent(i128, arena, .{ .i128 = a }, a.values, fallback.values[0..1]);
            break :blk .{ .i128 = .{ .values = try checkBound(out, spec, batch.sel) } };
        },
    };
}

/// The column a reference evaluates from, exact: a DECIMAL column's unscaled view, any other as decoded.
fn exactView(col: Batch.Column, e: ast.Expr) Error!Batch.Column {
    return if (e.typeOf() == .dec) exactColumn(col) else col;
}

fn checkBound(values: []i128, spec: ast.Dec, sel: ?*const selection.SelectionVector) Error![]i128 {
    const bound = spec.bound();
    for (values, 0..) |v, i| if (v >= bound or v <= -bound) {
        if (sel != null and !sel.?.isActive(i)) continue;
        return error.DecimalOverflow;
    };
    return values;
}

/// Two-column coalesce: per row, col_a's value if present, otherwise col_b's, otherwise NULL.
/// Strings borrow the source batch's values, which live in the same arena as the output.
fn evalCoalesceColCol(
    arena: std.mem.Allocator,
    batch: *const Batch,
    column_lookup: []const ?usize,
    lhs_ref: ast.ColRef,
    rhs_ref: ast.ColRef,
    result_type: ast.Type,
    c_dec: ?ast.Dec,
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

    const levels = try bothNull(arena, col_a, col_b);
    const has_nulls = levels != null;
    return switch (result_type) {
        .i64 => .{ .i64 = .{
            .values = try firstPresent(
                i64,
                arena,
                col_a,
                try coalesceI64(arena, col_a, lhs_ref),
                try coalesceI64(arena, col_b, rhs_ref),
            ),
            .def_levels = levels,
            .max_def = 1,
            .has_nulls = has_nulls,
        } },
        .f64 => .{ .f64 = .{
            .values = try firstPresent(
                f64,
                arena,
                col_a,
                try coalesceF64(arena, col_a, lhs_ref),
                try coalesceF64(arena, col_b, rhs_ref),
            ),
            .def_levels = levels,
            .max_def = 1,
            .has_nulls = has_nulls,
        } },
        .str => .{ .string = .{
            .values = try firstPresent([]const u8, arena, col_a, try coalesceStr(col_a), try coalesceStr(col_b)),
            .def_levels = levels,
            .max_def = 1,
            .has_nulls = has_nulls,
        } },
        .dec => blk: {
            const spec = c_dec.?;
            const ea: ast.Expr = .{ .col_ref = lhs_ref };
            const eb: ast.Expr = .{ .col_ref = rhs_ref };
            const a = try decColumnAt(arena, try exactView(col_a, ea), ea, spec.scale, batch.sel);
            const b = try decColumnAt(arena, try exactView(col_b, eb), eb, spec.scale, batch.sel);
            const out = try firstPresent(i128, arena, .{ .i128 = a }, a.values, b.values);
            // A both-null row holds col_b's placeholder, which the bound check must not read as a value.
            const sel = if (levels) |l| try presentRows(arena, l, batch.sel) else batch.sel;
            break :blk .{ .i128 = .{
                .values = try checkBound(out, spec, sel),
                .def_levels = levels,
                .max_def = 1,
                .has_nulls = has_nulls,
            } };
        },
    };
}

/// Definition levels (max_def 1) for the rows where both operands are null; null when there are none.
fn bothNull(arena: std.mem.Allocator, a: Batch.Column, b: Batch.Column) Error!?[]u32 {
    if (!colHasNulls(a) or !colHasNulls(b)) return null;
    const rows = colLen(a);
    var any = false;
    for (0..rows) |i| if (rowIsNull(a, i) and rowIsNull(b, i)) {
        any = true;
        break;
    };
    if (!any) return null;
    const levels = try arena.alloc(u32, rows);
    for (levels, 0..) |*l, i| l.* = @intFromBool(!(rowIsNull(a, i) and rowIsNull(b, i)));
    return levels;
}

/// `sel` narrowed to the rows `levels` holds present.
fn presentRows(
    arena: std.mem.Allocator,
    levels: []const u32,
    sel: ?*const selection.SelectionVector,
) Error!*const selection.SelectionVector {
    const out = try arena.create(selection.SelectionVector);
    out.* = try selection.SelectionVector.init(arena, levels.len);
    for (levels, 0..) |l, i| if (l == 0 or (sel != null and !sel.?.isActive(i))) out.set(i, false);
    return out;
}

fn colLen(col: Batch.Column) usize {
    return switch (col) {
        inline else => |c| c.values.len,
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
        // A DOUBLE column's default `0.0` is a DECIMAL literal, which DuckDB casts to DOUBLE for the result.
        .dec => |d| d.f,
        else => error.TypeMismatch,
    };
}

fn defaultStr(lit: ast.Literal) Error![]const u8 {
    return switch (lit) {
        .str => |v| v,
        else => error.TypeMismatch,
    };
}

/// A coalesce operand in the i64 lane. An unsigned column in the i32 lane zero-extends: sign-extended, a UINT32
/// value of 2^31 or more came back negative.
fn coalesceI64(arena: std.mem.Allocator, col: Batch.Column, ref: ast.ColRef) Error![]const i64 {
    return switch (col) {
        .i32, .i64 => widenColRefToI64(arena, col, ref),
        else => error.TypeMismatch,
    };
}

fn coalesceF64(arena: std.mem.Allocator, col: Batch.Column, ref: ast.ColRef) Error![]const f64 {
    return switch (col) {
        .i32, .i64 => blk: {
            const ints = try widenColRefToI64(arena, col, ref);
            const out = try arena.alloc(f64, ints.len);
            for (ints, out) |v, *o| o.* = @floatFromInt(v);
            break :blk out;
        },
        .f32 => |c| blk: {
            const out = try arena.alloc(f64, c.values.len);
            for (c.values, out) |v, *o| o.* = v;
            break :blk out;
        },
        .f64 => |c| c.values,
        else => error.TypeMismatch,
    };
}

fn coalesceStr(col: Batch.Column) Error![]const []const u8 {
    return switch (col) {
        .string => |c| c.values,
        else => error.TypeMismatch,
    };
}

/// Per row, `a_vals` where `a` is present, else `b_vals` (one element: a literal). Where `b_vals` is a column's null
/// slot, the caller's levels mark the row NULL. PLAIN-encoded OPTIONAL columns leave null slots as placeholders, so
/// the def levels decide, never the values.
fn firstPresent(
    comptime T: type,
    arena: std.mem.Allocator,
    a: Batch.Column,
    a_vals: []const T,
    b_vals: []const T,
) Error![]T {
    const out = try arena.alloc(T, a_vals.len);
    const step: usize = @intFromBool(b_vals.len > 1);
    for (out, a_vals, 0..) |*o, va, i| o.* = if (!rowIsNull(a, i)) va else b_vals[i * step];
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
    consumed: Consumed,
) Error![]T {
    std.debug.assert(a.len == b.len);
    const out = try arena.alloc(T, a.len);
    if (@typeInfo(T) == .int) switch (op) {
        .add => {
            try checkedIntKernel(T, .add, out, a, b, consumed);
            return out;
        },
        .sub => {
            try checkedIntKernel(T, .sub, out, a, b, consumed);
            return out;
        },
        .mul => {
            try checkedIntKernel(T, .mul, out, a, b, consumed);
            return out;
        },
        else => {},
    };
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
        .div => if (@typeInfo(T) == .int) {
            // A zero divisor or minInt / -1 leaves a 0 and a flag; then, as for + - *, only a consumed row fails.
            var faulted = false;
            for (a, b, 0..) |x, y, i| {
                const bad = y == 0 or (x == std.math.minInt(T) and y == -1);
                faulted = faulted or bad;
                out[i] = if (bad) 0 else @divTrunc(x, y);
            }
            if (faulted) try firstConsumedFault(T, op, a, b, consumed);
        } else for (a, b, 0..) |x, y, i| {
            out[i] = x / y;
        },
        // String concat is dispatched via concatKernel — should never
        // reach the numeric kernel.
        .concat => return error.TypeMismatch,
    }
    return out;
}

/// Refuse a nested (repeated) operand: arithmetic over a list's elements has no row to put its result in.
fn rejectNested(col: Batch.Column) Error!void {
    switch (col) {
        inline else => |c| if (c.max_rep > 0) return error.NestedNotSupported,
    }
}

/// `out` under the null layout of `src`, the column it was converted from row for row.
fn withNullsOf(out: Batch.Column, src: Batch.Column) Batch.Column {
    var r = out;
    switch (r) {
        inline else => |*c| switch (src) {
            inline else => |sc| {
                c.def_levels = sc.def_levels;
                c.max_def = sc.max_def;
                c.has_nulls = sc.has_nulls;
            },
        },
    }
    return r;
}

/// `widenToI64` for a column reference: an unsigned column in the i32 lane zero-extends.
pub fn widenColRefToI64(arena: std.mem.Allocator, col: Batch.Column, c: ast.ColRef) Error![]i64 {
    if (c.unsigned_32 and col == .i32) {
        const out = try arena.alloc(i64, col.i32.values.len);
        for (col.i32.values, 0..) |v, i| out[i] = @as(u32, @bitCast(v));
        return out;
    }
    return widenToI64(arena, col);
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

test "least and greatest skip NULLs, are NULL only where every argument is, and put NaN above every number" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const xs = [_]i64{ 1, 0, 3, 0 };
    const xd = [_]u32{ 1, 0, 1, 0 };
    const ys = [_]i64{ 2, 0, 0, 5 };
    const yd = [_]u32{ 1, 0, 0, 1 };
    const cols = [_]Batch.Column{
        .{ .i64 = .{ .values = &xs, .def_levels = &xd, .max_def = 1 } },
        .{ .i64 = .{ .values = &ys, .def_levels = &yd, .max_def = 1 } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1 };
    var x: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    var y: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT64, .expr_type = .i64 } };
    const args = [_]*ast.Expr{ &x, &y };
    const least = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .least, .args = &args, .result_type = .i64, .depth = 2 } });
    try testing.expectEqualSlices(u32, &.{ 1, 0, 1, 1 }, least.i64.def_levels.?);
    try testing.expectEqual(@as(i64, 1), least.i64.values[0]);
    try testing.expectEqual(@as(i64, 3), least.i64.values[2]);
    try testing.expectEqual(@as(i64, 5), least.i64.values[3]);
    const most = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .greatest, .args = &args, .result_type = .i64, .depth = 2 } });
    try testing.expectEqual(@as(i64, 2), most.i64.values[0]);
    // NaN is above every number: it wins greatest and loses least.
    const nan = std.math.nan(f64);
    try testing.expect(belowNanLast(1, nan) and !belowNanLast(nan, 1) and !belowNanLast(nan, nan));
}

test "floor, ceil, sqrt and round of a DOUBLE column, and NaN below zero" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const vals = [_]f64{ 2.5, -2.5, 2.675, -1 };
    const cols = [_]Batch.Column{.{ .f64 = .{ .values = &vals } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 4 };
    const lookup = [_]?usize{0};
    var col: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    var two: ast.Expr = .{ .literal = .{ .i64 = 2 } };
    const one_arg = [_]*ast.Expr{&col};
    const two_args = [_]*ast.Expr{ &col, &two };
    const floor = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .floor, .args = &one_arg, .result_type = .f64, .depth = 2 } });
    try testing.expectEqualSlices(f64, &.{ 2, -3, 2, -1 }, floor.f64.values);
    const ceil = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .ceil, .args = &one_arg, .result_type = .f64, .depth = 2 } });
    try testing.expectEqualSlices(f64, &.{ 3, -2, 3, -1 }, ceil.f64.values);
    const rounded = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .round, .args = &two_args, .result_type = .f64, .depth = 3 } });
    try testing.expectEqualSlices(f64, &.{ 2.5, -2.5, 2.68, -1 }, rounded.f64.values);
    const root = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .sqrt, .args = &one_arg, .result_type = .f64, .depth = 2 } });
    try testing.expect(std.math.isNan(root.f64.values[3]));
    try testing.expectEqual(@sqrt(@as(f64, 2.5)), root.f64.values[0]);
}

test "casts round as DuckDB does: a DOUBLE to the even integer, a DECIMAL half away from zero" {
    for ([_][2]f64{ .{ 2.5, 2 }, .{ 3.5, 4 }, .{ -2.5, -2 }, .{ -3.5, -4 }, .{ 0.5, 0 }, .{ 2.4999, 2 }, .{ 2.5001, 3 }, .{ -0.5, 0 } }) |c| {
        try testing.expectEqual(c[1], roundHalfEven(c[0]));
    }
    for ([_][3]i128{ .{ 250, 100, 3 }, .{ -250, 100, -3 }, .{ 249, 100, 2 }, .{ 5, 10, 1 }, .{ -5, 10, -1 }, .{ 4, 10, 0 }, .{ 2675, 10, 268 } }) |c| {
        try testing.expectEqual(c[2], divRoundHalfAway(c[0], c[1]));
    }
    // A DOUBLE that rounds outside the type, NaN and the infinities are conversion errors.
    try testing.expectEqual(@as(i64, 2147483647), try doubleAsInt(.i32, 2147483647.0));
    try testing.expectEqual(@as(i64, -2147483648), try doubleAsInt(.i32, -2147483648.4));
    try testing.expectError(error.ConversionOutOfRange, doubleAsInt(.i32, 2147483648.0));
    try testing.expectError(error.ConversionOutOfRange, doubleAsInt(.i8, 127.5));
    try testing.expectError(error.ConversionOutOfRange, doubleAsInt(.i64, 0x1p63));
    try testing.expectEqual(@as(i64, std.math.minInt(i64)), try doubleAsInt(.i64, -0x1p63));
    try testing.expectError(error.ConversionOutOfRange, doubleAsInt(.i64, std.math.nan(f64)));
    try testing.expectError(error.ConversionOutOfRange, doubleAsInt(.i16, std.math.inf(f64)));
}

test "a cast to a narrower integer fails on a value that does not fit, in rows that are read and not NULL" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const vals = [_]i64{ 100, 1000, -128 };
    const dls = [_]u32{ 1, 0, 1 };
    const cols = [_]Batch.Column{.{ .i64 = .{ .values = &vals, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};
    var col: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT64, .expr_type = .i64 } };
    const args = [_]*ast.Expr{&col};
    // 1000 is NULL, so 100 and -128 fit a TINYINT; without the NULL it would not.
    const out = try evalExpr(a, &batch, &lookup, .{ .call = .{ .func = .cast_i8, .args = &args, .result_type = .i64, .depth = 2, .int = .i8 } });
    try testing.expectEqualSlices(i64, &.{ 100, 0, -128 }, out.i64.values);
    const dense = [_]Batch.Column{.{ .i64 = .{ .values = &vals } }};
    const dense_batch: Batch = .{ .cols = &dense, .num_rows = 3 };
    try testing.expectError(error.ConversionOutOfRange, evalExpr(a, &dense_batch, &lookup, .{ .call = .{
        .func = .cast_i8,
        .args = &args,
        .result_type = .i64,
        .depth = 2,
        .int = .i8,
    } }));
}

test "coalesce: a DOUBLE column with a DECIMAL literal default is DOUBLE, as DuckDB types it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const vals = [_]f64{ 2.25, 0, 7 };
    const dls = [_]u32{ 1, 0, 1 };
    const cols = [_]Batch.Column{.{ .f64 = .{ .values = &vals, .def_levels = &dls, .max_def = 1 } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 3 };
    const lookup = [_]?usize{0};
    var col: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .DOUBLE, .expr_type = .f64 } };
    const dec_lit: ast.DecLit = .{ .unscaled = 15, .spec = .{ .precision = 2, .scale = 1 }, .f = 1.5 };
    var one_and_a_half: ast.Expr = .{ .literal = .{ .dec = dec_lit } };
    const args = [_]*ast.Expr{ &col, &one_and_a_half };
    const out = try evalExpr(a, &batch, &lookup, .{ .call = .{
        .func = .coalesce,
        .args = &args,
        .result_type = .f64,
        .depth = 2,
    } });
    try testing.expectEqualSlices(f64, &.{ 2.25, 1.5, 7 }, out.f64.values);
    try testing.expect(out.f64.def_levels == null);
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

/// `func(text, extra...)` through the string kernel, the extra arguments literals.
fn stringFn(func: ast.Func, text: []const u8, extra: []const ast.Expr) ![]const u8 {
    var arena: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const args = try a.alloc(*ast.Expr, extra.len + 1);
    args[0] = try a.create(ast.Expr);
    args[0].* = .{ .literal = .{ .str = text } };
    for (extra, args[1..]) |e, *slot| {
        slot.* = try a.create(ast.Expr);
        slot.*.* = e;
    }
    const call: ast.Call = .{ .func = func, .args = args, .result_type = .str, .depth = 1 };
    var bytes: std.ArrayList(u8) = .empty;
    const mapped = try applyStringFn(a, &bytes, call, text);
    // Copy out of the arena the answer lives in.
    return std.testing.allocator.dupe(u8, switch (mapped) {
        .text => |t| t,
        .built => |b| bytes.items[b[0]..b[1]],
    });
}

fn expectString(want: []const u8, func: ast.Func, text: []const u8, extra: []const ast.Expr) !void {
    const got = try stringFn(func, text, extra);
    defer std.testing.allocator.free(got);
    try std.testing.expectEqualStrings(want, got);
}

fn intArg(v: i64) ast.Expr {
    return .{ .literal = .{ .i64 = v } };
}

fn strArg(v: []const u8) ast.Expr {
    return .{ .literal = .{ .str = v } };
}

test "substring takes characters as DuckDB does: negative starts from the end, a negative length backwards" {
    // DuckDB 2.0.0's answers for 'héllo wörld' (11 characters, 13 bytes): rows are starts, columns the lengths.
    const s = "héllo wörld";
    const lens = [_]?i64{ null, -3, -1, 0, 1, 3, 5, 100 };
    const table = [_]struct { start: i64, want: [8][]const u8 }{
        .{ .start = -12, .want = .{ s, "", "", "", "", "hé", "héll", s } },
        .{ .start = -5, .want = .{ "wörld", "lo ", " ", "", "w", "wör", "wörld", "wörld" } },
        .{ .start = -1, .want = .{ "d", "örl", "l", "", "d", "d", "d", "d" } },
        .{ .start = 0, .want = .{ s, "", "", "", "", "hé", "héll", s } },
        .{ .start = 1, .want = .{ s, "", "", "", "h", "hél", "héllo", s } },
        .{ .start = 2, .want = .{ "éllo wörld", "h", "h", "", "é", "éll", "éllo ", "éllo wörld" } },
        .{ .start = 11, .want = .{ "d", "örl", "l", "", "d", "d", "d", "d" } },
        .{ .start = 12, .want = .{ "", "rld", "d", "", "", "", "", "" } },
        .{ .start = 13, .want = .{ "", "ld", "", "", "", "", "", "" } },
    };
    for (table) |row| for (lens, row.want) |len, want| {
        if (len) |l| try expectString(want, .substring, s, &.{ intArg(row.start), intArg(l) }) else try expectString(want, .substring, s, &.{intArg(row.start)});
    };
    try expectString("", .substring, "", &.{ intArg(1), intArg(3) });
    try expectString("日本", .substring, "日本語", &.{ intArg(1), intArg(2) });
}

test "upper and lower map code points as DuckDB does, and leave what it leaves" {
    try expectString("ABC DÉF 日本", .upper, "abc déf 日本", &.{});
    try expectString("abc déf 日本", .lower, "ABC DÉF 日本", &.{});
    try expectString("STRAẞE", .upper, "straße", &.{}); // ß -> ẞ, one code point to one
    try expectString("ß", .lower, "ẞ", &.{});
    try expectString("i", .lower, "İ", &.{});
    try expectString("I", .upper, "ı", &.{});
    try expectString("ασ", .lower, "ΑΣ", &.{}); // no final-sigma form
    try expectString("Ǆ", .upper, "ǅ", &.{});
    try expectString("ǰ", .upper, "ǰ", &.{}); // no single upper-case code point
    try expectString("ﬁ", .upper, "ﬁ", &.{});
    try expectString("😀X", .upper, "😀x", &.{});
    try expectString("", .upper, "", &.{});
    // Bytes that are not UTF-8 pass through, the rest still maps.
    try expectString("A\xffB", .upper, "a\xffb", &.{});
    // The mapping table's own spot values (stride-2 pairs, the ends).
    try std.testing.expectEqual(@as(u32, 0x101), case_map.lower(0x100));
    try std.testing.expectEqual(@as(u32, 0x100), case_map.upper(0x101));
    try std.testing.expectEqual(@as(u32, 0x101), case_map.lower(0x101));
    try std.testing.expectEqual(@as(u32, 0x10428), case_map.lower(0x10400));
    try std.testing.expectEqual(@as(u32, 0x10FFFF), case_map.lower(0x10FFFF));
    try std.testing.expectEqual(@as(u32, 0), case_map.upper(0));
}

test "left, right, trim, replace and the pads match DuckDB's answers" {
    const s = "héllo wörld";
    try expectString("hél", .left, s, &.{intArg(3)});
    try expectString("héllo wö", .left, s, &.{intArg(-3)});
    try expectString("", .left, s, &.{intArg(0)});
    try expectString(s, .left, s, &.{intArg(100)});
    try expectString("", .left, s, &.{intArg(-100)});
    try expectString("rld", .right, s, &.{intArg(3)});
    try expectString("lo wörld", .right, s, &.{intArg(-3)});
    try expectString("", .right, s, &.{intArg(0)});
    try expectString(s, .right, s, &.{intArg(100)});
    try expectString("😀😀", .left, "😀😀x", &.{intArg(2)});
    // Unicode's space separators trim, as DuckDB 2.0.0's trim does; tabs, line breaks and zero-width spaces do not.
    try expectString("a b", .trim, "  a b  ", &.{});
    try expectString("a ", .ltrim, "  a ", &.{});
    try expectString("  a", .rtrim, "  a ", &.{});
    try expectString("", .trim, "   ", &.{});
    try expectString("a b", .trim, " \u{a0}\u{2003}a b\u{3000}\u{1680} ", &.{});
    try expectString("a\u{202f}", .ltrim, "\u{a0}\u{205f}a\u{202f}", &.{});
    try expectString("\u{a0}a", .rtrim, "\u{a0}a\u{2000}\u{200a}", &.{});
    try expectString("", .trim, "\u{a0}\u{2009}", &.{});
    for ([_][]const u8{ "\t", "\n", "\r", "\x0b", "\x0c", "\u{85}", "\u{180e}", "\u{200b}", "\u{2028}", "\u{feff}", "\u{2060}", "\u{3164}", "\u{ad}" }) |kept| {
        const text = try std.mem.concat(std.testing.allocator, u8, &.{ kept, "a", kept });
        defer std.testing.allocator.free(text);
        try expectString(text, .trim, text, &.{});
    }
    // A text that is no UTF-8 stops at its first byte that is no space: the lead byte of a space cut short is not one.
    try expectString("\xe2\x80", .trim, "\xe2\x80", &.{});
    try expectString("a\xe2\x80", .rtrim, "a\xe2\x80", &.{});
    try expectString("ba", .replace, "aaa", &.{ strArg("aa"), strArg("b") });
    try expectString("aaaaaa", .replace, "aaa", &.{ strArg("a"), strArg("aa") });
    try expectString(s, .replace, s, &.{ strArg(""), strArg("x") });
    try expectString("", .replace, "abcabc", &.{ strArg("abc"), strArg("") });
    try expectString("héllo world", .replace, s, &.{ strArg("ö"), strArg("o") });
    // Padding counts characters, cuts a longer text, and an empty pad only matters when padding is needed.
    try expectString("*-*héllo", .lpad, "héllo", &.{ intArg(8), strArg("*-") });
    try expectString("héllo*-*", .rpad, "héllo", &.{ intArg(8), strArg("*-") });
    try expectString("héllo", .lpad, "héllo", &.{ intArg(5), strArg("*") });
    try expectString("hél", .lpad, "héllo", &.{ intArg(3), strArg("*") });
    try expectString("hél", .rpad, "héllo", &.{ intArg(3), strArg("") });
    try expectString("", .lpad, "héllo", &.{ intArg(0), strArg("*") });
    try expectString("", .lpad, "héllo", &.{ intArg(-1), strArg("*") });
    try expectString("aba", .lpad, "", &.{ intArg(3), strArg("ab") });
    try expectString("日本日héllo", .lpad, "héllo", &.{ intArg(8), strArg("日本") });
    try std.testing.expectError(error.InsufficientPadding, stringFn(.lpad, "héllo", &.{ intArg(8), strArg("") }));
    try std.testing.expectError(error.InsufficientPadding, stringFn(.rpad, "héllo", &.{ intArg(8), strArg("") }));
    // Characters, not bytes, and not grapheme clusters.
    try std.testing.expectEqual(@as(usize, 11), charCount("héllo wörld"));
    try std.testing.expectEqual(@as(usize, 2), charCount("a\u{301}"));
    try std.testing.expectEqual(@as(usize, 5), charCount("👨‍👩‍👧"));
}
