//! Expression AST.
//!
//! Tagged-union shape — small enough that the evaluator can recurse
//! through it directly without a comptime VM. First-slice surface:
//!
//!   expr  := literal | col_ref | (expr op expr) | (expr)
//!   op    := + | - | * | /
//!
//! Plus an `Alias` shell at the top level so `SELECT expr AS name`
//! survives parse → eval → encode without a separate alias map.
//! Strings, booleans, NULL, and function calls are not supported in
//! this slice — they land in C2 follow-ups.
//!
//! Type system (resolved at parse time):
//!   - INT32 / INT64 / FLOAT / DOUBLE column refs are accepted in
//!     arithmetic. BYTE_ARRAY / BOOLEAN are not.
//!   - Promotion: any-int op any-int → i64, anything-with-float → f64.
//!   - Result types of computed columns: INT64 or DOUBLE only.
//!
//! Compared to filter/ast.zig: this one doesn't need a `negate()`
//! method (no boolean composition) but does need `result_type` so the
//! caller can synthesize a schema element for the output column.

const std = @import("std");
const schema = @import("../schema.zig");

pub const Op = enum { add, sub, mul, div };

/// The value type an expression evaluates to. We deliberately keep
/// just two — i64 and f64 — to keep the kernel matrix small in the
/// first slice. Promotion happens at column-ref load time (i32 → i64,
/// f32 → f64) so binops only see these two types.
pub const Type = enum {
    i64,
    f64,

    pub fn promote(a: Type, b: Type) Type {
        if (a == .f64 or b == .f64) return .f64;
        return .i64;
    }

    /// Map back to Parquet physical type for output schema synthesis.
    pub fn toParquet(self: Type) schema.Type {
        return switch (self) {
            .i64 => .INT64,
            .f64 => .DOUBLE,
        };
    }
};

pub const Literal = union(enum) {
    i64: i64,
    f64: f64,

    pub fn typeOf(self: Literal) Type {
        return switch (self) {
            .i64 => .i64,
            .f64 => .f64,
        };
    }
};

/// Column reference — resolved at parse time. `col_idx` indexes into
/// the source file's leaf columns; `physical_type` is the on-disk
/// Parquet type so the evaluator knows how to coerce the decoded
/// values into one of `Type`'s two slots.
pub const ColRef = struct {
    col_idx: usize,
    physical_type: schema.Type,
    /// Promoted type (INT32/INT64 → .i64, FLOAT/DOUBLE → .f64).
    /// Cached at parse time so eval doesn't have to recompute.
    expr_type: Type,
};

pub const BinOp = struct {
    op: Op,
    left: *Expr,
    right: *Expr,
    /// Result type — `Type.promote(left.typeOf(), right.typeOf())`.
    /// Cached at parse time.
    result_type: Type,
};

pub const Expr = union(enum) {
    literal: Literal,
    col_ref: ColRef,
    binop: BinOp,

    pub fn typeOf(self: Expr) Type {
        return switch (self) {
            .literal => |l| l.typeOf(),
            .col_ref => |c| c.expr_type,
            .binop => |b| b.result_type,
        };
    }
};

/// One output column from a `SELECT` clause. The optional alias is the
/// name used in the output schema; when null, the caller falls back to
/// a synthetic name (e.g. `"_expr_3"`) — the parser's job to populate
/// `alias` from `expr AS name` syntax.
pub const SelectItem = struct {
    expr: Expr,
    alias: ?[]const u8,
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Type.promote" {
    try testing.expectEqual(Type.i64, Type.promote(.i64, .i64));
    try testing.expectEqual(Type.f64, Type.promote(.i64, .f64));
    try testing.expectEqual(Type.f64, Type.promote(.f64, .i64));
    try testing.expectEqual(Type.f64, Type.promote(.f64, .f64));
}

test "Type.toParquet" {
    try testing.expectEqual(schema.Type.INT64, Type.i64.toParquet());
    try testing.expectEqual(schema.Type.DOUBLE, Type.f64.toParquet());
}

test "Literal.typeOf" {
    const li: Literal = .{ .i64 = 42 };
    const lf: Literal = .{ .f64 = 3.14 };
    try testing.expectEqual(Type.i64, li.typeOf());
    try testing.expectEqual(Type.f64, lf.typeOf());
}

test "Expr.typeOf composes through binop" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const left = try a.create(Expr);
    left.* = .{ .literal = .{ .i64 = 1 } };
    const right = try a.create(Expr);
    right.* = .{ .literal = .{ .f64 = 2.0 } };

    const e: Expr = .{ .binop = .{
        .op = .add,
        .left = left,
        .right = right,
        .result_type = .f64,
    } };
    try testing.expectEqual(Type.f64, e.typeOf());
}
