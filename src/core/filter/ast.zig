//! Filter AST + Operator enum.
//!
//! Tagged-union shape ported from the pre-rewrite tree (commit 240fecf
//! et al). Leaf nodes carry a typed value plus the column index they
//! reference; composites carry pointer-to-Filter for left/right.
//!
//! Phase 4 surface: =, !=, <, <=, >, >= with AND/OR. No parens, no NOT.
//! Matches the documented filter syntax from `docs/filter_design.md`.

const std = @import("std");

pub const Operator = enum {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    pub fn negate(self: Operator) Operator {
        return switch (self) {
            .Eq => .NotEq,
            .NotEq => .Eq,
            .Lt => .GtEq,
            .LtEq => .Gt,
            .Gt => .LtEq,
            .GtEq => .Lt,
        };
    }
};

/// Test a comparison `a op b` returning whether the predicate holds.
/// `T` must support all the comparison operators.
pub fn applyOp(comptime T: type, a: T, op: Operator, b: T) bool {
    return switch (op) {
        .Eq => a == b,
        .NotEq => a != b,
        .Lt => a < b,
        .LtEq => a <= b,
        .Gt => a > b,
        .GtEq => a >= b,
    };
}

/// String comparison via lexicographic byte order.
pub fn applyOpStr(a: []const u8, op: Operator, b: []const u8) bool {
    const cmp = std.mem.order(u8, a, b);
    return switch (op) {
        .Eq => cmp == .eq,
        .NotEq => cmp != .eq,
        .Lt => cmp == .lt,
        .LtEq => cmp != .gt,
        .Gt => cmp == .gt,
        .GtEq => cmp != .lt,
    };
}

pub const Composite = struct { left: *Filter, right: *Filter };

pub fn Leaf(comptime T: type) type {
    return struct {
        col_idx: usize,
        op: Operator,
        value: T,
    };
}

pub const Filter = union(enum) {
    int32: Leaf(i32),
    int64: Leaf(i64),
    float: Leaf(f32),
    double: Leaf(f64),
    string: Leaf([]const u8),
    boolean: Leaf(bool),
    and_filter: Composite,
    or_filter: Composite,

    /// Walk the AST collecting every leaf's column index.
    /// Useful for the "fetch only filter columns first" pattern.
    pub fn collectColumns(self: Filter, list: *std.ArrayList(usize), allocator: std.mem.Allocator) std.mem.Allocator.Error!void {
        switch (self) {
            .int32 => |f| try list.append(allocator, f.col_idx),
            .int64 => |f| try list.append(allocator, f.col_idx),
            .float => |f| try list.append(allocator, f.col_idx),
            .double => |f| try list.append(allocator, f.col_idx),
            .string => |f| try list.append(allocator, f.col_idx),
            .boolean => |f| try list.append(allocator, f.col_idx),
            .and_filter, .or_filter => |c| {
                try c.left.collectColumns(list, allocator);
                try c.right.collectColumns(list, allocator);
            },
        }
    }
};

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Operator.negate" {
    try testing.expectEqual(Operator.NotEq, Operator.Eq.negate());
    try testing.expectEqual(Operator.Eq, Operator.NotEq.negate());
    try testing.expectEqual(Operator.GtEq, Operator.Lt.negate());
    try testing.expectEqual(Operator.Lt, Operator.GtEq.negate());
    try testing.expectEqual(Operator.Gt, Operator.LtEq.negate());
    try testing.expectEqual(Operator.LtEq, Operator.Gt.negate());
}

test "applyOp on integers" {
    try testing.expect(applyOp(i32, 5, .Eq, 5));
    try testing.expect(!applyOp(i32, 5, .Eq, 6));
    try testing.expect(applyOp(i32, 5, .Lt, 6));
    try testing.expect(applyOp(i32, 5, .LtEq, 5));
    try testing.expect(applyOp(i32, 5, .GtEq, 5));
    try testing.expect(applyOp(i32, 5, .Gt, 4));
    try testing.expect(applyOp(i32, -3, .Lt, 0));
}

test "applyOpStr lexicographic" {
    try testing.expect(applyOpStr("abc", .Eq, "abc"));
    try testing.expect(applyOpStr("abc", .Lt, "abd"));
    try testing.expect(applyOpStr("abc", .LtEq, "abc"));
    try testing.expect(!applyOpStr("abc", .Gt, "abc"));
    try testing.expect(applyOpStr("alpha", .Lt, "beta"));
}

test "Filter.collectColumns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const left = try a.create(Filter);
    left.* = .{ .int32 = .{ .col_idx = 1, .op = .Eq, .value = 7 } };
    const right = try a.create(Filter);
    right.* = .{ .string = .{ .col_idx = 5, .op = .Lt, .value = "hello" } };
    const composite: Filter = .{ .and_filter = .{ .left = left, .right = right } };

    var cols: std.ArrayList(usize) = .empty;
    defer cols.deinit(a);
    try composite.collectColumns(&cols, a);
    try testing.expectEqualSlices(usize, &.{ 1, 5 }, cols.items);
}
