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

/// Binary operators. Numeric ops (`add`/`sub`/`mul`/`div`) work on
/// `i64` or `f64`; `concat` works on `str` and is the SQL `||`
/// operator.
pub const Op = enum {
    add,
    sub,
    mul,
    div,
    concat,
};

/// The value type an expression evaluates to. Three lanes — i64, f64,
/// str — keep the kernel matrix tractable. Numeric promotion happens
/// at column-ref load time (i32 → i64, f32 → f64); strings stay as
/// strings and don't mix with numerics.
pub const Type = enum {
    i64,
    f64,
    str,

    /// Result type of `lhs op rhs`. Strings can only combine with
    /// strings (concat); any cross-lane mix returns null and the
    /// caller must surface a TypeMismatch error.
    pub fn promote(a: Type, b: Type) ?Type {
        if (a == .str or b == .str) {
            if (a == .str and b == .str) return .str;
            return null;
        }
        if (a == .f64 or b == .f64) return .f64;
        return .i64;
    }

    /// Map back to Parquet physical type for output schema synthesis.
    pub fn toParquet(self: Type) schema.Type {
        return switch (self) {
            .i64 => .INT64,
            .f64 => .DOUBLE,
            .str => .BYTE_ARRAY,
        };
    }
};

pub const Literal = union(enum) {
    i64: i64,
    f64: f64,
    str: []const u8,

    pub fn typeOf(self: Literal) Type {
        return switch (self) {
            .i64 => .i64,
            .f64 => .f64,
            .str => .str,
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
    /// INT64-physical column annotated UNSIGNED (UINT_64 / INTEGER{64,unsigned}).
    /// The i64 lane holds the raw 64 bits; the aggregate fold reinterprets them
    /// as u64 so sum/min/max are correct (a signed read makes 2^64-1 read -1).
    /// ≤32-bit unsigned ints don't need this — they zero-extend at decode.
    unsigned_64: bool = false,
};

pub const BinOp = struct {
    op: Op,
    left: *Expr,
    right: *Expr,
    /// Result type — `Type.promote(left.typeOf(), right.typeOf())`.
    /// Cached at parse time.
    result_type: Type,
    /// Height of this subtree — `1 + max(left.depth(), right.depth())`. Cached because the parser checks it on every
    /// node it builds; a recursive walk would be O(nodes) per check.
    depth: u32,
};

/// Built-in scalar functions. The first slice ships only `coalesce`
/// (replace nulls with a default). Add cases here as functions land:
/// `abs`, `length`, `lower`, `upper`, etc. The parser routes `IDENT(...)`
/// to whichever Func tag matches the name (case-insensitive).
pub const Func = enum {
    coalesce,
};

pub const Call = struct {
    func: Func,
    args: []const *Expr,
    /// Result type, resolved at parse time from the args' types.
    result_type: Type,
    /// Height of this subtree — `1 + max(arg.depth())`. See `BinOp.depth`.
    depth: u32,
};

pub const Expr = union(enum) {
    literal: Literal,
    col_ref: ColRef,
    binop: BinOp,
    call: Call,

    pub fn typeOf(self: Expr) Type {
        return switch (self) {
            .literal => |l| l.typeOf(),
            .col_ref => |c| c.expr_type,
            .binop => |b| b.result_type,
            .call => |c| c.result_type,
        };
    }

    /// Height of this subtree — leaves are 1. This, not the parser's recursion depth, is what `parser.MAX_EXPR_DEPTH`
    /// bounds: it sets how many full intermediate columns are live at once.
    pub fn depth(self: Expr) u32 {
        return switch (self) {
            .literal, .col_ref => 1,
            .binop => |b| b.depth,
            .call => |c| c.depth,
        };
    }

    /// Walk the expression and mark every referenced column index in
    /// `fetch_arr`. Used by drivers (CLI + Lambda) to compute the set
    /// of columns to decode for a given expression.
    pub fn collectColumns(self: Expr, fetch_arr: []bool) void {
        switch (self) {
            .literal => {},
            .col_ref => |c| if (c.col_idx < fetch_arr.len) {
                fetch_arr[c.col_idx] = true;
            },
            .binop => |b| {
                b.left.collectColumns(fetch_arr);
                b.right.collectColumns(fetch_arr);
            },
            .call => |c| for (c.args) |arg| arg.collectColumns(fetch_arr),
        }
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
    try testing.expectEqual(Type.i64, Type.promote(.i64, .i64).?);
    try testing.expectEqual(Type.f64, Type.promote(.i64, .f64).?);
    try testing.expectEqual(Type.f64, Type.promote(.f64, .i64).?);
    try testing.expectEqual(Type.f64, Type.promote(.f64, .f64).?);
    try testing.expectEqual(Type.str, Type.promote(.str, .str).?);
    // Cross-lane mixes return null (caller surfaces TypeMismatch).
    try testing.expectEqual(@as(?Type, null), Type.promote(.str, .i64));
    try testing.expectEqual(@as(?Type, null), Type.promote(.f64, .str));
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
        .depth = 2,
    } };
    try testing.expectEqual(Type.f64, e.typeOf());
}
