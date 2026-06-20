//! Hive-partition pruning. Pre-fetch path-based file elimination.
//!
//! Pattern (DuckDB-style): given a list of input URLs containing
//! `key=value` segments, parse the partitions per file. If a filter
//! expression references *only* partition columns, evaluate it against
//! each file's partition values and skip files where the predicate is
//! false. Files that survive the predicate (or all files when no
//! predicate is given) go on to the normal data path.
//!
//! Scope: predicate must be **AND-of-leaves** where
//! every leaf references a partition column. If any leaf references
//! a non-partition column, `parsePredicate` returns null — caller
//! falls back to the normal "fetch every file's footer" path.
//!
//! Mixed predicates (partition AND data) require splitting the text,
//! pruning on the partition half, and handing the data half to
//! `filter/parser.zig` per file.

const std = @import("std");
const ast = @import("ast.zig");

pub const Error = error{
    BadOperator,
    BadValue,
} || std.mem.Allocator.Error;

/// One key=value pair extracted from a path segment.
pub const KV = struct {
    key: []const u8,
    value: []const u8,
};

/// One leaf of a partition-only predicate. Values are kept as strings;
/// numeric coercion happens at eval time using the file's value text.
pub const Leaf = struct {
    key: []const u8,
    op: ast.Operator,
    value: []const u8,
};

/// AND-of-leaves predicate. v1 doesn't model OR; if the filter has OR
/// at the top level we conservatively decline (return null from
/// parsePredicate) and let the normal data path run.
pub const Predicate = struct {
    leaves: []const Leaf,
};

/// Parse `key=value` segments out of a path. Mirrors DuckDB's linear
/// walker: split on `/`, each segment must contain a single `=` and
/// no `?`/newline. Works for both `s3://bucket/k=v/...` and bare paths.
pub fn parsePath(arena: std.mem.Allocator, path: []const u8) std.mem.Allocator.Error![]KV {
    var out: std.ArrayList(KV) = .empty;

    var i: usize = 0;
    var seg_start: usize = 0;
    var eq_idx: ?usize = null;
    var candidate = true;

    while (i <= path.len) : (i += 1) {
        const at_end = i == path.len;
        const ch: u8 = if (at_end) '/' else path[i];

        if (ch == '?' or ch == '\n') {
            candidate = false;
            continue;
        }
        if (ch == '/') {
            if (candidate) {
                if (eq_idx) |e| {
                    if (e > seg_start and i > e + 1) {
                        const key = path[seg_start..e];
                        const value = path[e + 1 .. i];
                        try out.append(arena, .{ .key = key, .value = value });
                    }
                }
            }
            seg_start = i + 1;
            eq_idx = null;
            candidate = true;
            continue;
        }
        if (ch == '=') {
            if (eq_idx != null) {
                // multiple = in segment — not a partition
                candidate = false;
            } else {
                eq_idx = i;
            }
        }
    }
    return out.toOwnedSlice(arena);
}

/// Look up a partition value by key. Linear scan — partition counts are
/// tiny (typically < 5).
pub fn lookup(kvs: []const KV, key: []const u8) ?[]const u8 {
    for (kvs) |kv| {
        if (std.mem.eql(u8, kv.key, key)) return kv.value;
    }
    return null;
}

/// Try to parse `expr` as a partition-only predicate. Returns null if
/// the expression contains OR, or any leaf references a column not in
/// `partition_keys`.
pub fn parsePredicate(
    arena: std.mem.Allocator,
    expr: []const u8,
    partition_keys: []const []const u8,
) Error!?Predicate {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");
    if (trimmed.len == 0) return null;

    // Reject OR — v1 is AND-only.
    if (std.mem.indexOf(u8, trimmed, " OR ") != null) return null;

    var leaves: std.ArrayList(Leaf) = .empty;

    var rest = trimmed;
    while (true) {
        const next_and = std.mem.indexOf(u8, rest, " AND ");
        const leaf_text = if (next_and) |i| std.mem.trim(u8, rest[0..i], " ") else rest;

        const leaf = try parseLeaf(leaf_text);
        if (!isPartitionKey(leaf.key, partition_keys)) {
            // A non-partition column → bail. Caller runs normal path.
            return null;
        }
        try leaves.append(arena, leaf);

        if (next_and) |i| {
            rest = std.mem.trim(u8, rest[i + 5 ..], " ");
        } else break;
    }

    return Predicate{ .leaves = try leaves.toOwnedSlice(arena) };
}

fn isPartitionKey(key: []const u8, partition_keys: []const []const u8) bool {
    for (partition_keys) |pk| {
        if (std.mem.eql(u8, pk, key)) return true;
    }
    return false;
}

fn parseLeaf(expr: []const u8) Error!Leaf {
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

    const key = std.mem.trim(u8, expr[0..found_at], " ");
    const val = std.mem.trim(u8, expr[found_at + found_len ..], " ");
    if (key.len == 0 or val.len == 0) return error.BadOperator;

    return .{ .key = key, .op = op, .value = val };
}

/// Evaluate a predicate against a file's partition values. Returns
/// true iff the file passes (i.e. should NOT be pruned). A leaf whose
/// key is missing from `kvs` evaluates to false (the file isn't in
/// the partition layout the user is querying).
///
/// Numeric values: if both sides parse as i64, compare numerically;
/// otherwise fall back to lexicographic byte order (works for the
/// standard `month=08` / `year=2026` case where padding makes lex order
/// agree with numeric order).
pub fn eval(pred: Predicate, kvs: []const KV) bool {
    for (pred.leaves) |leaf| {
        const file_val = lookup(kvs, leaf.key) orelse return false;
        if (!evalLeaf(file_val, leaf.op, leaf.value)) return false;
    }
    return true;
}

fn evalLeaf(file_val: []const u8, op: ast.Operator, query_val: []const u8) bool {
    if (std.fmt.parseInt(i64, file_val, 10)) |a| {
        if (std.fmt.parseInt(i64, query_val, 10)) |b| {
            return ast.applyOp(i64, a, op, b);
        } else |_| {}
    } else |_| {}
    return ast.applyOpStr(file_val, op, query_val);
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "parsePath extracts hive segments" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const kvs = try parsePath(a, "s3://bucket/zpq/year=2026/month=05/data.parquet");
    try testing.expectEqual(@as(usize, 2), kvs.len);
    try testing.expectEqualStrings("year", kvs[0].key);
    try testing.expectEqualStrings("2026", kvs[0].value);
    try testing.expectEqualStrings("month", kvs[1].key);
    try testing.expectEqualStrings("05", kvs[1].value);
}

test "parsePath ignores non-partition segments" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const kvs = try parsePath(a, "/tmp/folder/year=2026/sub/month=05/file.parquet");
    try testing.expectEqual(@as(usize, 2), kvs.len);
    try testing.expectEqualStrings("year", kvs[0].key);
    try testing.expectEqualStrings("month", kvs[1].key);
}

test "parsePath rejects double-equals segment" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const kvs = try parsePath(a, "/k=a=b/c=d/file");
    try testing.expectEqual(@as(usize, 1), kvs.len);
    try testing.expectEqualStrings("c", kvs[0].key);
}

test "parsePredicate accepts pure-partition AND" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{ "year", "month" };
    const pred = (try parsePredicate(a, "month >= 08 AND year = 2026", &keys)).?;
    try testing.expectEqual(@as(usize, 2), pred.leaves.len);
    try testing.expectEqualStrings("month", pred.leaves[0].key);
    try testing.expectEqual(ast.Operator.GtEq, pred.leaves[0].op);
    try testing.expectEqualStrings("08", pred.leaves[0].value);
}

test "parsePredicate rejects non-partition column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{"month"};
    const pred = try parsePredicate(a, "month = 05 AND int8 > 9999", &keys);
    try testing.expect(pred == null);
}

test "parsePredicate rejects OR" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{"month"};
    const pred = try parsePredicate(a, "month = 05 OR month = 06", &keys);
    try testing.expect(pred == null);
}

test "eval numeric comparison" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{"month"};
    const pred = (try parsePredicate(a, "month >= 08", &keys)).?;

    const kv_pass = [_]KV{.{ .key = "month", .value = "09" }};
    const kv_fail = [_]KV{.{ .key = "month", .value = "07" }};
    const kv_eq = [_]KV{.{ .key = "month", .value = "08" }};

    try testing.expect(eval(pred, &kv_pass));
    try testing.expect(!eval(pred, &kv_fail));
    try testing.expect(eval(pred, &kv_eq));
}

test "eval string fallback" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{"region"};
    const pred = (try parsePredicate(a, "region = us-west-2", &keys)).?;

    const kv_pass = [_]KV{.{ .key = "region", .value = "us-west-2" }};
    const kv_fail = [_]KV{.{ .key = "region", .value = "us-east-1" }};

    try testing.expect(eval(pred, &kv_pass));
    try testing.expect(!eval(pred, &kv_fail));
}

test "eval missing key fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const keys = [_][]const u8{"month"};
    const pred = (try parsePredicate(a, "month = 05", &keys)).?;

    const kv_empty = [_]KV{};
    try testing.expect(!eval(pred, &kv_empty));
}
