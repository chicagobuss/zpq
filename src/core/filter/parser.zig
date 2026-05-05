//! Filter expression parser.
//!
//! Grammar (matches docs/filter_design.md and the prior-art syntax spec):
//!
//!   expr        := disjunction
//!   disjunction := conjunction ( " OR " conjunction )*
//!   conjunction := leaf ( " AND " leaf )*
//!   leaf        := identifier OP value
//!   OP          := "!=" | "<=" | ">=" | "=" | "<" | ">"
//!
//! No parens, no NOT, no nested expressions. Matches what the prior
//! implementation supported; sufficient for the demo + much real-world
//! usage. Add when needed.
//!
//! AND binds tighter than OR — we split on " OR " first, then on " AND ".

const std = @import("std");
const ast = @import("ast.zig");
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");

pub const Error = error{
    EmptyExpr,
    UnknownColumn,
    BadOperator,
    BadValue,
    UnsupportedType,
} || std.mem.Allocator.Error;

/// Parse a filter expression against the file's schema.
///
/// The returned Filter holds slices borrowed from `expr` for string
/// values; caller must keep `expr` alive for as long as the Filter.
/// Composite nodes are arena-allocated; caller-provided arena owns
/// their storage.
pub fn parse(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
) Error!ast.Filter {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;
    return try parseDisjunction(arena, trimmed, file);
}

fn parseDisjunction(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
    if (std.mem.indexOf(u8, expr, " OR ")) |i| {
        const left = try arena.create(ast.Filter);
        const right = try arena.create(ast.Filter);
        left.* = try parseConjunction(arena, std.mem.trim(u8, expr[0..i], " "), file);
        right.* = try parseDisjunction(arena, std.mem.trim(u8, expr[i + 4 ..], " "), file);
        return .{ .or_filter = .{ .left = left, .right = right } };
    }
    return try parseConjunction(arena, expr, file);
}

fn parseConjunction(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
    // Find a top-level " AND " — one that doesn't pair with a BETWEEN.
    // For each " BETWEEN " in the expression, the *next* " AND " after
    // it belongs to the BETWEEN bounds, not to a conjunction. Skip
    // over them.
    if (findTopLevelAnd(expr)) |i| {
        const left = try arena.create(ast.Filter);
        const right = try arena.create(ast.Filter);
        left.* = try parseLeaf(arena, std.mem.trim(u8, expr[0..i], " "), file);
        right.* = try parseConjunction(arena, std.mem.trim(u8, expr[i + 5 ..], " "), file);
        return .{ .and_filter = .{ .left = left, .right = right } };
    }
    return try parseLeaf(arena, expr, file);
}

/// Find the first ` AND ` token in `expr` that is NOT part of a
/// `BETWEEN x AND y` clause. Returns its byte index, or null if every
/// ` AND ` belongs to a BETWEEN (or none exist).
fn findTopLevelAnd(expr: []const u8) ?usize {
    // Walk left-to-right tracking a "skip-next-AND" counter that we
    // bump each time we see a `BETWEEN` keyword. Each subsequent
    // ` AND ` decrements the counter (consumed by that BETWEEN's
    // bounds) until it hits zero — then the next ` AND ` is the
    // top-level conjunction split.
    var i: usize = 0;
    var skip: usize = 0;
    while (i + 5 <= expr.len) {
        // Check for ` BETWEEN ` (case-insensitive). The leading space
        // disambiguates from identifiers like `between_threshold`.
        if (i + " BETWEEN ".len <= expr.len and asciiEqIgnoreCase(expr[i .. i + " BETWEEN ".len], " BETWEEN ")) {
            skip += 1;
            i += " BETWEEN ".len;
            continue;
        }
        if (asciiEqIgnoreCase(expr[i .. i + 5], " AND ")) {
            if (skip > 0) {
                skip -= 1;
                i += 5;
                continue;
            }
            return i;
        }
        i += 1;
    }
    return null;
}

fn asciiEqIgnoreCase(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        const xl = if (x >= 'A' and x <= 'Z') x + 32 else x;
        const yl = if (y >= 'A' and y <= 'Z') y + 32 else y;
        if (xl != yl) return false;
    }
    return true;
}

fn caseInsensitiveIndexOf(haystack: []const u8, needle: []const u8) ?usize {
    if (needle.len == 0 or needle.len > haystack.len) return null;
    var i: usize = 0;
    while (i + needle.len <= haystack.len) : (i += 1) {
        if (asciiEqIgnoreCase(haystack[i .. i + needle.len], needle)) return i;
    }
    return null;
}

fn parseLeaf(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
    // BETWEEN check runs before operator detection — `col BETWEEN x AND y`
    // doesn't contain a binary op the comparison-finder would recognize.
    if (caseInsensitiveIndexOf(expr, " BETWEEN ")) |_| {
        return parseBetween(arena, expr, file);
    }
    return parseComparisonLeaf(arena, expr, file);
}

/// Parse `col BETWEEN x AND y` into the conjunction `(col >= x AND col <= y)`.
/// SQL semantics: BETWEEN is inclusive on both sides.
fn parseBetween(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
    const between_at = caseInsensitiveIndexOf(expr, " BETWEEN ") orelse return error.BadOperator;
    const after_between = between_at + " BETWEEN ".len;
    const and_at = caseInsensitiveIndexOf(expr[after_between..], " AND ") orelse return error.BadOperator;

    const col_name = std.mem.trim(u8, expr[0..between_at], " ");
    const x_str = std.mem.trim(u8, expr[after_between .. after_between + and_at], " ");
    const y_str = std.mem.trim(u8, expr[after_between + and_at + " AND ".len ..], " ");
    if (col_name.len == 0 or x_str.len == 0 or y_str.len == 0) return error.BadOperator;

    const col_idx = metadata.findColumnIndex(file, col_name) orelse return error.UnknownColumn;
    const elem = file.getColumnSchema(&[_][]const u8{col_name}) orelse return error.UnknownColumn;
    const ptype = elem.type orelse return error.UnsupportedType;

    const left = try arena.create(ast.Filter);
    const right = try arena.create(ast.Filter);
    left.* = try buildLeafFilter(col_idx, .GtEq, x_str, ptype);
    right.* = try buildLeafFilter(col_idx, .LtEq, y_str, ptype);
    return .{ .and_filter = .{ .left = left, .right = right } };
}

fn parseComparisonLeaf(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
    _ = arena;
    // Find the operator. Two-char ops checked first so "<=" doesn't
    // match the "<" arm.
    const ops_2c = [_]struct { tok: []const u8, op: ast.Operator }{
        .{ .tok = "!=", .op = .NotEq },
        .{ .tok = "<=", .op = .LtEq },
        .{ .tok = ">=", .op = .GtEq },
    };
    const ops_1c = [_]struct { tok: []const u8, op: ast.Operator }{
        .{ .tok = "=", .op = .Eq },
        .{ .tok = "<", .op = .Lt },
        .{ .tok = ">", .op = .Gt },
    };

    var found_op: ?ast.Operator = null;
    var found_at: usize = 0;
    var found_len: usize = 0;
    for (ops_2c) |o| {
        if (std.mem.indexOf(u8, expr, o.tok)) |i| {
            found_op = o.op;
            found_at = i;
            found_len = o.tok.len;
            break;
        }
    }
    if (found_op == null) {
        for (ops_1c) |o| {
            if (std.mem.indexOf(u8, expr, o.tok)) |i| {
                found_op = o.op;
                found_at = i;
                found_len = o.tok.len;
                break;
            }
        }
    }
    const op = found_op orelse return error.BadOperator;

    const col_name = std.mem.trim(u8, expr[0..found_at], " ");
    const val_str = std.mem.trim(u8, expr[found_at + found_len ..], " ");
    if (col_name.len == 0 or val_str.len == 0) return error.BadOperator;

    const col_idx = metadata.findColumnIndex(file, col_name) orelse return error.UnknownColumn;
    const elem = file.getColumnSchema(&[_][]const u8{col_name}) orelse return error.UnknownColumn;
    const ptype = elem.type orelse return error.UnsupportedType;

    return try buildLeafFilter(col_idx, op, val_str, ptype);
}

fn buildLeafFilter(col_idx: usize, op: ast.Operator, val_str: []const u8, parquet_type: schema.Type) Error!ast.Filter {
    return switch (parquet_type) {
        .INT32 => .{ .int32 = .{
            .col_idx = col_idx,
            .op = op,
            .value = std.fmt.parseInt(i32, val_str, 10) catch return error.BadValue,
        } },
        .INT64 => .{ .int64 = .{
            .col_idx = col_idx,
            .op = op,
            .value = std.fmt.parseInt(i64, val_str, 10) catch return error.BadValue,
        } },
        .FLOAT => .{ .float = .{
            .col_idx = col_idx,
            .op = op,
            .value = std.fmt.parseFloat(f32, val_str) catch return error.BadValue,
        } },
        .DOUBLE => .{ .double = .{
            .col_idx = col_idx,
            .op = op,
            .value = std.fmt.parseFloat(f64, val_str) catch return error.BadValue,
        } },
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => .{ .string = .{
            .col_idx = col_idx,
            .op = op,
            .value = val_str,
        } },
        .BOOLEAN => .{ .boolean = .{
            .col_idx = col_idx,
            .op = op,
            .value = if (std.mem.eql(u8, val_str, "true") or std.mem.eql(u8, val_str, "1"))
                true
            else if (std.mem.eql(u8, val_str, "false") or std.mem.eql(u8, val_str, "0"))
                false
            else
                return error.BadValue,
        } },
        .INT96 => return error.UnsupportedType,
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const thrift = @import("../thrift.zig");

// Mini synthetic FileMetaData for parser tests. Avoids needing a real
// fixture file just to exercise the parser.
fn synthFileMeta(arena: std.mem.Allocator) !schema.FileMetaData {
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    // Root + 3 columns. Schema is in DFS order; first elem is the root.
    try meta.schema.append(arena, .{
        .type = .BOOLEAN, // root's type is unused
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "root",
        .num_children = 3,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try meta.schema.append(arena, .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "id",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try meta.schema.append(arena, .{
        .type = .BYTE_ARRAY,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "status",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    try meta.schema.append(arena, .{
        .type = .DOUBLE,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "score",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    return meta;
}

test "parse simple int equality" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id=42", &meta);
    try testing.expectEqual(@as(usize, 0), f.int32.col_idx);
    try testing.expectEqual(ast.Operator.Eq, f.int32.op);
    try testing.expectEqual(@as(i32, 42), f.int32.value);
}

test "parse string less-than" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "status<active", &meta);
    try testing.expectEqual(@as(usize, 1), f.string.col_idx);
    try testing.expectEqual(ast.Operator.Lt, f.string.op);
    try testing.expectEqualStrings("active", f.string.value);
}

test "parse double >=" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "score>=0.5", &meta);
    try testing.expectEqual(ast.Operator.GtEq, f.double.op);
    try testing.expectApproxEqAbs(@as(f64, 0.5), f.double.value, 1e-9);
}

test "parse AND composite" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id>10 AND status=active", &meta);
    try testing.expect(f == .and_filter);
    try testing.expectEqual(@as(usize, 0), f.and_filter.left.int32.col_idx);
    try testing.expectEqual(ast.Operator.Gt, f.and_filter.left.int32.op);
    try testing.expectEqualStrings("active", f.and_filter.right.string.value);
}

test "parse OR has lower precedence than AND" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // Should bind as: status=error OR (id>=1000 AND score=1.5)
    // In our shape that's an or_filter whose right is an and_filter.
    const f = try parse(a, "status=error OR id>=1000 AND score=1.5", &meta);
    try testing.expect(f == .or_filter);
    try testing.expect(f.or_filter.right.* == .and_filter);
}

test "parse rejects unknown column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    try testing.expectError(error.UnknownColumn, parse(a, "missing=1", &meta));
}

test "parse rejects bad operator" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    try testing.expectError(error.BadOperator, parse(a, "id LIKE '5'", &meta));
}

test "parse BETWEEN expands to >= AND <= conjunction" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id BETWEEN 10 AND 20", &meta);
    try testing.expect(f == .and_filter);
    try testing.expect(f.and_filter.left.* == .int32);
    try testing.expect(f.and_filter.right.* == .int32);
    try testing.expectEqual(ast.Operator.GtEq, f.and_filter.left.int32.op);
    try testing.expectEqual(@as(i32, 10), f.and_filter.left.int32.value);
    try testing.expectEqual(ast.Operator.LtEq, f.and_filter.right.int32.op);
    try testing.expectEqual(@as(i32, 20), f.and_filter.right.int32.value);
}

test "parse BETWEEN composes with outer AND" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // `score BETWEEN 0.5 AND 0.9 AND id=1` should parse as
    // `(score >= 0.5 AND score <= 0.9) AND (id=1)`. The BETWEEN's
    // bound-AND must NOT be the conjunction split point.
    const f = try parse(a, "score BETWEEN 0.5 AND 0.9 AND id=1", &meta);
    try testing.expect(f == .and_filter);
    // Left side is the BETWEEN's expansion (and_filter of two doubles).
    try testing.expect(f.and_filter.left.* == .and_filter);
    // Right side is the trailing id=1.
    try testing.expect(f.and_filter.right.* == .int32);
    try testing.expectEqual(@as(i32, 1), f.and_filter.right.int32.value);
}

test "parse BETWEEN is case-insensitive" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id between 1 and 5", &meta);
    try testing.expect(f == .and_filter);
}
