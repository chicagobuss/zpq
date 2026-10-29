//! Remove grouping information that is fixed by retained integer column keys. Only a fault-free whitelist is
//! eligible: a discarded key must never hide a per-row error, including on a group a result cut discards.

const std = @import("std");
const ast = @import("ast.zig");

const Range = struct { lo: i128, hi: i128, column: bool = false };

/// Bare column keys are always retained. Return one dependency's old index for a removable expression; every
/// reference must have its own retained key. Injectivity is unnecessary: the retained tuple fixes the result.
pub fn dependency(expr: ast.Expr, keys: []const ast.Expr) ?usize {
    if (expr != .binop) return null;
    const bounds = range(expr, keys) orelse return null;
    if (!bounds.column) return null;
    return firstColumn(expr, keys);
}

fn firstColumn(expr: ast.Expr, keys: []const ast.Expr) ?usize {
    return switch (expr) {
        .col_ref => |c| for (keys, 0..) |k, i| {
            if (k == .col_ref and k.col_ref.col_idx == c.col_idx) break i;
        } else null,
        .binop => |b| firstColumn(b.left.*, keys) orelse firstColumn(b.right.*, keys),
        else => null,
    };
}

fn range(expr: ast.Expr, keys: []const ast.Expr) ?Range {
    return switch (expr) {
        .literal => |l| if (l == .i64) .{ .lo = l.i64, .hi = l.i64 } else null,
        .col_ref => |c| blk: {
            if (c.expr_type != .i64 or c.unsigned_64 or firstColumn(expr, keys) == null) break :blk null;
            break :blk switch (c.physical_type) {
                .BOOLEAN => .{ .lo = 0, .hi = 1, .column = true },
                .INT32 => if (c.unsigned_32)
                    .{ .lo = 0, .hi = std.math.maxInt(u32), .column = true }
                else
                    .{ .lo = std.math.minInt(i32), .hi = std.math.maxInt(i32), .column = true },
                .INT64 => .{ .lo = std.math.minInt(i64), .hi = std.math.maxInt(i64), .column = true },
                else => null,
            };
        },
        .binop => |b| blk: {
            if (b.result_type != .i64) break :blk null;
            const l = range(b.left.*, keys) orelse break :blk null;
            const r = range(b.right.*, keys) orelse break :blk null;
            // Every child fits i64. Its endpoint products fit i128, including minInt(i64) squared.
            const bounds: Range = switch (b.op) {
                .add => .{ .lo = l.lo + r.lo, .hi = l.hi + r.hi },
                .sub => .{ .lo = l.lo - r.hi, .hi = l.hi - r.lo },
                .mul => .{
                    .lo = @min(l.lo * r.lo, l.lo * r.hi, l.hi * r.lo, l.hi * r.hi),
                    .hi = @max(l.lo * r.lo, l.lo * r.hi, l.hi * r.lo, l.hi * r.hi),
                },
                else => break :blk null,
            };
            if (bounds.lo < std.math.minInt(i64) or bounds.hi > std.math.maxInt(i64)) break :blk null;
            break :blk .{ .lo = bounds.lo, .hi = bounds.hi, .column = l.column or r.column };
        },
        // Calls, CASE, division and non-integer lanes need separate determinism/fault proofs.
        else => null,
    };
}

/// Evaluate only after `dependency` proved the tree. The resolver reads retained integer key values, never rows.
/// Arithmetic has the established NULL propagation and checked error kinds even in release builds.
pub fn evaluate(expr: ast.Expr, reader: anytype) (error{
    BadGroupKey,
    IntegerOverflowAdd,
    IntegerOverflowSub,
    IntegerOverflowMul,
} || @typeInfo(@TypeOf(reader.column(0))).error_union.error_set)!?i64 {
    return switch (expr) {
        .literal => expr.literal.i64,
        .col_ref => |c| reader.column(c.col_idx),
        .binop => |b| blk: {
            const l = try evaluate(b.left.*, reader);
            const r = try evaluate(b.right.*, reader);
            if (l == null or r == null) break :blk null;
            break :blk switch (b.op) {
                .add => std.math.add(i64, l.?, r.?) catch return error.IntegerOverflowAdd,
                .sub => std.math.sub(i64, l.?, r.?) catch return error.IntegerOverflowSub,
                .mul => std.math.mul(i64, l.?, r.?) catch return error.IntegerOverflowMul,
                else => error.BadGroupKey,
            };
        },
        else => error.BadGroupKey,
    };
}

test "dependent group keys: endpoint proof rejects faults and needs every column retained" {
    var k: ast.Expr = .{ .col_ref = .{ .col_idx = 0, .physical_type = .INT32, .expr_type = .i64 } };
    var other: ast.Expr = .{ .col_ref = .{ .col_idx = 1, .physical_type = .INT32, .expr_type = .i64 } };
    var one: ast.Expr = .{ .literal = .{ .i64 = 1 } };
    var e: ast.Expr = .{ .binop = .{ .op = .sub, .left = &k, .right = &one, .result_type = .i64, .depth = 2 } };
    try std.testing.expectEqual(@as(?usize, 0), dependency(e, &.{k}));
    e.binop.op = .mul;
    e.binop.right = &k;
    try std.testing.expect(dependency(e, &.{k}) != null); // Non-injective, including both signs of k.
    e.binop.right = &other;
    try std.testing.expect(dependency(e, &.{k}) == null);
    try std.testing.expect(dependency(e, &.{ k, other }) != null);
    e.binop.op = .div;
    try std.testing.expect(dependency(e, &.{ k, other }) == null);
    e.binop.op = .sub;
    e.binop.right = &one;
    k.col_ref.physical_type = .INT64;
    try std.testing.expect(dependency(e, &.{k}) == null);
    e.binop.op = .mul;
    e.binop.right = &k;
    try std.testing.expect(dependency(e, &.{k}) == null);
    k.col_ref.physical_type = .INT32;
    k.col_ref.unsigned_32 = true;
    try std.testing.expect(dependency(e, &.{k}) == null); // UINT32 squared exceeds INT64.
    e.binop.right = &one;
    try std.testing.expect(dependency(e, &.{k}) != null);
}
