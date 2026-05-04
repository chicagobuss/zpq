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
    if (std.mem.indexOf(u8, expr, " AND ")) |i| {
        const left = try arena.create(ast.Filter);
        const right = try arena.create(ast.Filter);
        left.* = try parseLeaf(arena, std.mem.trim(u8, expr[0..i], " "), file);
        right.* = try parseConjunction(arena, std.mem.trim(u8, expr[i + 5 ..], " "), file);
        return .{ .and_filter = .{ .left = left, .right = right } };
    }
    return try parseLeaf(arena, expr, file);
}

fn parseLeaf(arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData) Error!ast.Filter {
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
