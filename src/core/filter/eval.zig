//! Value-level filter evaluation against a decoded batch.
//!
//! Inputs:
//!   - A typed slice of decoded values (one per row).
//!   - A SelectionVector tracking which rows are still "alive."
//!   - A leaf predicate or composite Filter.
//!
//! Output: the SelectionVector is mutated in place. Rows that fail
//! the predicate get their bit cleared.
//!
//! AND composes naturally: evaluate left, then right — bits cleared
//! by either stay cleared. OR uses a snapshot/restore + bitwise OR
//! of the two children's results.

const std = @import("std");
const ast = @import("ast.zig");
const selection = @import("selection.zig");

pub const Error = error{
    TypeMismatch,
    BadColumn,
    /// Filter referenced a column with max_rep > 0 (LIST / MAP).
    /// List-element predicates need repetition-aware semantics, so this
    /// evaluator rejects them instead of flattening ambiguously.
    NestedFilterNotSupported,
} || std.mem.Allocator.Error;

inline fn rejectIfNested(col: anytype) Error!void {
    if (col.max_rep > 0) return error.NestedFilterNotSupported;
}

/// SQL three-valued logic: any predicate involving NULL evaluates to
/// NULL → row not selected. `def_levels[i] < max_def` means row i is
/// null for this column. Returns true iff the row should be excluded
/// purely on null grounds (caller skips the value comparison).
inline fn rowIsNull(def_levels: ?[]const u32, max_def: u32, i: usize) bool {
    if (def_levels) |dl| {
        return dl[i] < max_def;
    }
    return false;
}

/// Evaluate a leaf predicate of the given comptime type.
/// `values[i]` corresponds to row `i` in the SelectionVector.
/// When `def_levels` is non-null, rows where def_levels[i] < max_def
/// are treated as null and fail the predicate (SQL three-valued logic).
pub fn evalLeaf(
    comptime T: type,
    values: []const T,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: T,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        if (!ast.applyOp(T, values[i], op, needle)) {
            sel.set(i, false);
        }
    }
}

/// `IS NULL` / `IS NOT NULL` against a column's definition levels.
/// A row is null when `def_levels[i] < max_def`; a REQUIRED column
/// (def_levels == null or max_def == 0) has no nulls, so IS NULL keeps
/// nothing and IS NOT NULL keeps everything. Unlike value predicates,
/// this is the one filter where null rows can SELECT (IS NULL).
pub fn evalNullCheck(
    def_levels: ?[]const u32,
    max_def: u32,
    is_not: bool,
    sel: *selection.SelectionVector,
) void {
    var i: usize = 0;
    while (i < sel.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        const is_null = rowIsNull(def_levels, max_def, i);
        const keep = if (is_not) !is_null else is_null;
        if (!keep) sel.set(i, false);
    }
}

/// `LIKE` / `NOT LIKE` on a string column. Null rows fail (SQL 3VL).
pub fn evalLike(
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    m: ast.LikeMatch,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        const hit = ast.likeMatches(m.kind, m.operand, values[i]);
        if (hit == m.negate) sel.set(i, false); // NOT LIKE flips the keep test
    }
}

/// String predicate: byte-comparison.
pub fn evalLeafStr(
    values: []const []const u8,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: []const u8,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        if (!ast.applyOpStr(values[i], op, needle)) {
            sel.set(i, false);
        }
    }
}

/// Boolean predicate.
pub fn evalLeafBool(
    values: []const bool,
    def_levels: ?[]const u32,
    max_def: u32,
    op: ast.Operator,
    needle: bool,
    sel: *selection.SelectionVector,
) void {
    std.debug.assert(values.len == sel.len);
    var i: usize = 0;
    while (i < values.len) : (i += 1) {
        if (!sel.isActive(i)) continue;
        if (rowIsNull(def_levels, max_def, i)) {
            sel.set(i, false);
            continue;
        }
        const v = values[i];
        // Boolean ordering: false (0) < true (1), matching DuckDB/SQL.
        // (Previously range ops silently returned false — a footgun.)
        const a: u1 = @intFromBool(v);
        const b: u1 = @intFromBool(needle);
        const pass = switch (op) {
            .Eq => a == b,
            .NotEq => a != b,
            .Lt => a < b,
            .LtEq => a <= b,
            .Gt => a > b,
            .GtEq => a >= b,
        };
        if (!pass) sel.set(i, false);
    }
}

/// Per-column view of decoded values + null mask + (for nested
/// columns) repetition levels.
///
/// For flat / struct-of-primitive columns: `rep_levels == null`,
/// `max_rep == 0`. Each leaf value corresponds 1:1 to a logical row.
///
/// For nested columns (list/map): `rep_levels` non-null. Each value
/// is a leaf (which may have multiple leaves per logical row). A
/// logical row starts at every position where `rep_levels[i] == 0`.
/// Higher rep values mean "continuation of this row at depth N."
pub fn ColumnT(comptime T: type) type {
    return struct {
        values: []const T,
        /// Null when no leaf in this column is null — either because
        /// the source is REQUIRED at every level (max_def == 0), or
        /// because `--fast-levels` proved an OPTIONAL column all-present
        /// and never materialised its levels. Both mean the same thing
        /// to every reader: no null rows, so `max_def` may be > 0 here.
        /// Otherwise length == values.len; entry < max_def → leaf null.
        def_levels: ?[]const u32 = null,
        max_def: u32 = 0,
        /// Null when max_rep == 0. Otherwise length == values.len.
        rep_levels: ?[]const u32 = null,
        max_rep: u32 = 0,
        has_nulls: bool = false,
    };
}

/// `Batch` — caller-supplied views of decoded columns.
/// Indexed by `col_idx` from the AST.
pub const Batch = struct {
    /// Per-column views. Only the columns referenced by the filter
    /// need to be present (caller decides what to provide).
    cols: []const Column,
    num_rows: usize,

    pub const Column = union(enum) {
        i32: ColumnT(i32),
        i64: ColumnT(i64),
        f32: ColumnT(f32),
        f64: ColumnT(f64),
        string: ColumnT([]const u8),
        boolean: ColumnT(bool),

        /// Null mask + nesting, independent of the value type — used by
        /// IS NULL / IS NOT NULL, which never touch values.
        pub fn nullInfo(self: Column) struct { def_levels: ?[]const u32, max_def: u32, max_rep: u32 } {
            return switch (self) {
                inline else => |c| .{ .def_levels = c.def_levels, .max_def = c.max_def, .max_rep = c.max_rep },
            };
        }
    };
};

/// Walk the AST and apply each predicate to `sel` against `batch`.
/// `column_lookup[col_idx]` returns the index into `batch.cols` for
/// the column that AST leaf nodes reference, or null if the caller
/// didn't supply that column. Returns error.BadColumn in that case
/// (the caller should fetch + decode the missing column first).
pub fn evaluate(
    filter: ast.Filter,
    batch: *const Batch,
    sel: *selection.SelectionVector,
    column_lookup: []const ?usize,
    arena: std.mem.Allocator,
) Error!void {
    switch (filter) {
        .int32 => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .i32) return error.TypeMismatch;
            try rejectIfNested(col.i32);
            evalLeaf(i32, col.i32.values, col.i32.def_levels, col.i32.max_def, leaf.op, leaf.value, sel);
        },
        .int64 => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .i64) return error.TypeMismatch;
            try rejectIfNested(col.i64);
            evalLeaf(i64, col.i64.values, col.i64.def_levels, col.i64.max_def, leaf.op, leaf.value, sel);
        },
        .float => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .f32) return error.TypeMismatch;
            try rejectIfNested(col.f32);
            evalLeaf(f32, col.f32.values, col.f32.def_levels, col.f32.max_def, leaf.op, leaf.value, sel);
        },
        .double => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .f64) return error.TypeMismatch;
            try rejectIfNested(col.f64);
            evalLeaf(f64, col.f64.values, col.f64.def_levels, col.f64.max_def, leaf.op, leaf.value, sel);
        },
        .string => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .string) return error.TypeMismatch;
            try rejectIfNested(col.string);
            evalLeafStr(col.string.values, col.string.def_levels, col.string.max_def, leaf.op, leaf.value, sel);
        },
        .boolean => |leaf| {
            const col_pos = column_lookup[leaf.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .boolean) return error.TypeMismatch;
            try rejectIfNested(col.boolean);
            evalLeafBool(col.boolean.values, col.boolean.def_levels, col.boolean.max_def, leaf.op, leaf.value, sel);
        },
        .null_check => |nc| {
            const col_pos = column_lookup[nc.col_idx] orelse return error.BadColumn;
            const ni = batch.cols[col_pos].nullInfo();
            if (ni.max_rep > 0) return error.NestedFilterNotSupported;
            evalNullCheck(ni.def_levels, ni.max_def, nc.is_not, sel);
        },
        .like => |m| {
            const col_pos = column_lookup[m.col_idx] orelse return error.BadColumn;
            const col = batch.cols[col_pos];
            if (col != .string) return error.TypeMismatch;
            try rejectIfNested(col.string);
            evalLike(col.string.values, col.string.def_levels, col.string.max_def, m, sel);
        },
        .and_filter => |c| {
            // AND composes naturally: left clears bits, right clears
            // more bits. No state to save.
            try evaluate(c.left.*, batch, sel, column_lookup, arena);
            try evaluate(c.right.*, batch, sel, column_lookup, arena);
        },
        .or_filter => |c| {
            // OR: snapshot the entry state so each branch evaluates
            // against the same input set; then merge.
            var entry = try sel.cloneAlloc(arena);
            defer entry.deinit();

            try evaluate(c.left.*, batch, sel, column_lookup, arena);
            const left_result = try sel.cloneAlloc(arena);
            defer {
                var lr = left_result;
                lr.deinit();
            }

            sel.copyFrom(&entry);
            try evaluate(c.right.*, batch, sel, column_lookup, arena);
            // sel now holds right's result; OR-merge with left.
            sel.unionWith(&left_result);
        },
    }
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "evalLeaf i32 equality" {
    const data = [_]i32{ 1, 2, 3, 2, 5 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, null, 0, .Eq, 2, &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(1));
    try testing.expect(sv.isActive(3));
}

test "evalLeaf i32 range" {
    const data = [_]i32{ 1, 2, 3, 4, 5 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, null, 0, .GtEq, 3, &sv);
    try testing.expectEqual(@as(usize, 3), sv.count()); // 3, 4, 5
}

test "evalLeaf with nulls fails predicate (SQL three-valued logic)" {
    // Rows: 1, NULL, 3, NULL, 5. Filter `>= 2` should match rows 2,4
    // (values 3 and 5). The two nulls fail the predicate via SQL 3VL.
    const data = [_]i32{ 1, 0, 3, 0, 5 };
    const def_levels = [_]u32{ 1, 0, 1, 0, 1 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, &def_levels, 1, .GtEq, 2, &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(2));
    try testing.expect(sv.isActive(4));
    // Even though the null rows have value=0 which would not match
    // anyway, the test would fail if the null check were absent for
    // predicates like `< 1` that nulls' default value would satisfy.
}

test "evalLeaf treats null < anything as fail" {
    // Filter `< 100`. If we ignored nulls, all rows would pass since
    // the default 0 < 100. With proper null handling the two nulls
    // are excluded.
    const data = [_]i32{ 50, 0, 75, 0, 25 };
    const def_levels = [_]u32{ 1, 0, 1, 0, 1 };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeaf(i32, &data, &def_levels, 1, .Lt, 100, &sv);
    try testing.expectEqual(@as(usize, 3), sv.count());
    try testing.expect(sv.isActive(0));
    try testing.expect(!sv.isActive(1)); // null
    try testing.expect(sv.isActive(2));
    try testing.expect(!sv.isActive(3)); // null
    try testing.expect(sv.isActive(4));
}

test "evalNullCheck IS NULL / IS NOT NULL + REQUIRED column" {
    // rows: present, null, present, null, present
    const def = [_]u32{ 1, 0, 1, 0, 1 };
    { // IS NULL → the two null rows
        var sv = try selection.SelectionVector.init(testing.allocator, def.len);
        defer sv.deinit();
        evalNullCheck(&def, 1, false, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
        try testing.expect(sv.isActive(1) and sv.isActive(3));
    }
    { // IS NOT NULL → the three present rows
        var sv = try selection.SelectionVector.init(testing.allocator, def.len);
        defer sv.deinit();
        evalNullCheck(&def, 1, true, &sv);
        try testing.expectEqual(@as(usize, 3), sv.count());
        try testing.expect(sv.isActive(0) and sv.isActive(2) and sv.isActive(4));
    }
    { // REQUIRED column (no def_levels): IS NULL keeps none, IS NOT NULL keeps all
        var a = try selection.SelectionVector.init(testing.allocator, 4);
        defer a.deinit();
        evalNullCheck(null, 0, false, &a);
        try testing.expectEqual(@as(usize, 0), a.count());
        var b = try selection.SelectionVector.init(testing.allocator, 4);
        defer b.deinit();
        evalNullCheck(null, 0, true, &b);
        try testing.expectEqual(@as(usize, 4), b.count());
    }
}

test "evalLeafStr equality" {
    const data = [_][]const u8{ "alpha", "beta", "gamma", "alpha", "beta" };
    var sv = try selection.SelectionVector.init(testing.allocator, data.len);
    defer sv.deinit();

    evalLeafStr(&data, null, 0, .Eq, "alpha", &sv);
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(0));
    try testing.expect(sv.isActive(3));
}

test "evalLeafStr range comparisons (bytewise order)" {
    // bytewise sort: alpha < beta < delta < gamma  ('d' 0x64 < 'g' 0x67)
    const data = [_][]const u8{ "alpha", "beta", "gamma", "delta" };

    { // Lt "delta": alpha, beta
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .Lt, "delta", &sv);
        try testing.expect(sv.isActive(0));
        try testing.expect(sv.isActive(1));
        try testing.expect(!sv.isActive(2)); // gamma > delta
        try testing.expect(!sv.isActive(3)); // delta not < delta
    }
    { // GtEq "beta": beta, gamma, delta
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .GtEq, "beta", &sv);
        try testing.expect(!sv.isActive(0)); // alpha < beta
        try testing.expect(sv.isActive(1));
        try testing.expect(sv.isActive(2));
        try testing.expect(sv.isActive(3));
    }
    { // Gt "beta": gamma, delta (strict)
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafStr(&data, null, 0, .Gt, "beta", &sv);
        try testing.expect(!sv.isActive(0));
        try testing.expect(!sv.isActive(1)); // beta not > beta
        try testing.expect(sv.isActive(2));
        try testing.expect(sv.isActive(3));
    }
}

test "evalLeafBool ordering (false < true, matches DuckDB)" {
    const data = [_]bool{ true, false, true, false };
    { // Eq true → the 2 true rows
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .Eq, true, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
    }
    { // Gt false → true rows (false < true); was silently 0 before
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .Gt, false, &sv);
        try testing.expectEqual(@as(usize, 2), sv.count());
        try testing.expect(sv.isActive(0)); // true > false
        try testing.expect(!sv.isActive(1)); // false not > false
    }
    { // GtEq false → all rows
        var sv = try selection.SelectionVector.init(testing.allocator, data.len);
        defer sv.deinit();
        evalLeafBool(&data, null, 0, .GtEq, false, &sv);
        try testing.expectEqual(@as(usize, 4), sv.count());
    }
}

test "evaluate AND composite — both narrow the selection" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ints = [_]i32{ 10, 20, 30, 40, 50 };
    const strs = [_][]const u8{ "x", "y", "x", "y", "x" };
    const cols = [_]Batch.Column{
        .{ .i32 = .{ .values = &ints } },
        .{ .string = .{ .values = &strs } },
    };
    const batch: Batch = .{ .cols = &cols, .num_rows = 5 };
    var sv = try selection.SelectionVector.init(a, 5);
    defer sv.deinit();

    // col_idx 0 is i32 column at batch.cols[0]; col_idx 1 is string at batch.cols[1].
    const lookup = [_]?usize{ 0, 1 };

    // AST: int_col >= 20 AND str_col = "x"
    const left = try a.create(ast.Filter);
    left.* = .{ .int32 = .{ .col_idx = 0, .op = .GtEq, .value = 20 } };
    const right = try a.create(ast.Filter);
    right.* = .{ .string = .{ .col_idx = 1, .op = .Eq, .value = "x" } };
    const f: ast.Filter = .{ .and_filter = .{ .left = left, .right = right } };

    try evaluate(f, &batch, &sv, &lookup, a);

    // Rows: idx2 (30, "x") and idx4 (50, "x") match.
    try testing.expectEqual(@as(usize, 2), sv.count());
    try testing.expect(sv.isActive(2));
    try testing.expect(sv.isActive(4));
}

test "evaluate OR composite — union of branches" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const ints = [_]i32{ 10, 20, 30, 40, 50 };
    const cols = [_]Batch.Column{.{ .i32 = .{ .values = &ints } }};
    const batch: Batch = .{ .cols = &cols, .num_rows = 5 };
    var sv = try selection.SelectionVector.init(a, 5);
    defer sv.deinit();

    const lookup = [_]?usize{0};

    // AST: int_col = 10 OR int_col >= 40
    const left = try a.create(ast.Filter);
    left.* = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 10 } };
    const right = try a.create(ast.Filter);
    right.* = .{ .int32 = .{ .col_idx = 0, .op = .GtEq, .value = 40 } };
    const f: ast.Filter = .{ .or_filter = .{ .left = left, .right = right } };

    try evaluate(f, &batch, &sv, &lookup, a);

    try testing.expectEqual(@as(usize, 3), sv.count());
    try testing.expect(sv.isActive(0)); // 10
    try testing.expect(sv.isActive(3)); // 40
    try testing.expect(sv.isActive(4)); // 50
}
