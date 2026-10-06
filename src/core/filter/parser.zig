//! Filter expression parser.
//!
//! Grammar:
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
const decimal_mod = @import("../parquet/decimal.zig");
const Diag = @import("../diag.zig").Diag;

pub const Error = error{
    EmptyExpr,
    UnknownColumn,
    /// A bare name with no top-level match names leaves in several groups.
    AmbiguousColumn,
    BadOperator,
    BadValue,
    UnsupportedType,
    /// Grouping parentheses aren't supported — the grammar is flat
    /// AND/OR. (Parens are only valid inside an `IN (...)` list.)
    GroupingNotSupported,
} || std.mem.Allocator.Error;

/// Parse a filter expression against the file's schema.
///
/// The returned Filter holds slices borrowed from `expr` for string
/// values; caller must keep `expr` alive for as long as the Filter.
/// Composite nodes are arena-allocated; caller-provided arena owns
/// their storage. `diag` receives what a failure was about.
pub fn parse(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const trimmed = std.mem.trim(u8, expr, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;
    return try parseDisjunction(arena, trimmed, file, diag);
}

fn parseDisjunction(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    if (std.mem.indexOf(u8, expr, " OR ")) |i| {
        const left = try arena.create(ast.Filter);
        const right = try arena.create(ast.Filter);
        left.* = try parseConjunction(arena, std.mem.trim(u8, expr[0..i], " "), file, diag);
        right.* = try parseDisjunction(arena, std.mem.trim(u8, expr[i + 4 ..], " "), file, diag);
        return .{ .or_filter = .{ .left = left, .right = right } };
    }
    return try parseConjunction(arena, expr, file, diag);
}

fn parseConjunction(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    // Find a top-level " AND " — one that doesn't pair with a BETWEEN.
    // For each " BETWEEN " in the expression, the *next* " AND " after
    // it belongs to the BETWEEN bounds, not to a conjunction. Skip
    // over them.
    if (findTopLevelAnd(expr)) |i| {
        const left = try arena.create(ast.Filter);
        const right = try arena.create(ast.Filter);
        left.* = try parseLeaf(arena, std.mem.trim(u8, expr[0..i], " "), file, diag);
        right.* = try parseConjunction(arena, std.mem.trim(u8, expr[i + 5 ..], " "), file, diag);
        return .{ .and_filter = .{ .left = left, .right = right } };
    }
    return try parseLeaf(arena, expr, file, diag);
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

/// Resolve a (possibly double-quoted) column name to its leaf index. See
/// `metadata.resolveColumn` for how bare and dotted names bind.
fn resolveCol(file: *const schema.FileMetaData, name: []const u8, diag: ?*Diag) Error!usize {
    const ident = metadata.unquoteIdent(name);
    return metadata.resolveColumn(file, ident) catch |err| {
        if (err == error.AmbiguousColumn) Diag.set(diag, ident, .{ .ambiguous_column = .{
            .parser = .filter,
            .hint = metadata.ambiguityHint(file, ident),
        } });
        return err;
    };
}

fn parseLeaf(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    // Leading `NOT <leaf>` → negate the parsed leaf (operator-flip on leaves,
    // De Morgan on composites). `col NOT IN (...)` is handled by the IN path
    // below (its NOT is mid-expression, not leading).
    if (expr.len > 4 and asciiEqIgnoreCase(expr[0..4], "NOT ")) {
        const inner = std.mem.trim(u8, expr[4..], " ");
        if (inner.len == 0) return error.BadOperator;
        return negateFilter(arena, try parseLeaf(arena, inner, file, diag));
    }
    // `col IS NULL` / `col IS NOT NULL` — a def-level predicate, no value.
    // Checked before IN/BETWEEN/comparison since it has no binary operator.
    if (try tryParseNullCheck(expr, file, diag)) |nc| return nc;

    // `col LIKE 'pat'` / `col NOT LIKE 'pat'` — SQL wildcard match.
    // " NOT LIKE " must be checked first (it contains " LIKE ").
    if (caseInsensitiveIndexOf(expr, " NOT LIKE ")) |i| {
        return parseLike(expr[0..i], expr[i + " NOT LIKE ".len ..], true, file, diag);
    }
    if (caseInsensitiveIndexOf(expr, " LIKE ")) |i| {
        return parseLike(expr[0..i], expr[i + " LIKE ".len ..], false, file, diag);
    }

    // `col [NOT] IN (v1, v2, ...)` → OR-of-equalities (or AND-of-inequalities
    // for NOT IN), reusing the typed leaf builder per value.
    if (findInClause(expr)) |inc| {
        return parseInList(arena, expr, inc, file, diag);
    }
    // BETWEEN runs before operator detection — `col BETWEEN x AND y` has no
    // binary op the comparison-finder would recognize.
    if (caseInsensitiveIndexOf(expr, " BETWEEN ")) |_| {
        return parseBetween(arena, expr, file, diag);
    }
    return parseComparisonLeaf(arena, expr, file, diag);
}

/// Recognise a trailing ` IS NULL` / ` IS NOT NULL` (case-insensitive).
/// Returns null when the expression isn't a null check (so the caller
/// falls through to the comparison path). A lone ` IS ` that isn't one
/// of the two valid forms is a clear error, not a silent fallthrough.
fn tryParseNullCheck(expr: []const u8, file: *const schema.FileMetaData, diag: ?*Diag) Error!?ast.Filter {
    const not_null = " IS NOT NULL";
    const null_ = " IS NULL";
    var col_part: []const u8 = undefined;
    var is_not: bool = undefined;
    if (expr.len >= not_null.len and asciiEqIgnoreCase(expr[expr.len - not_null.len ..], not_null)) {
        col_part = expr[0 .. expr.len - not_null.len];
        is_not = true;
    } else if (expr.len >= null_.len and asciiEqIgnoreCase(expr[expr.len - null_.len ..], null_)) {
        col_part = expr[0 .. expr.len - null_.len];
        is_not = false;
    } else if (caseInsensitiveIndexOf(expr, " IS ")) |_| {
        if (diag) |d| d.detail = .is_operand;
        return error.BadOperator;
    } else {
        return null;
    }
    const col_name = std.mem.trim(u8, col_part, " ");
    if (col_name.len == 0) return error.BadOperator;
    const col_idx = try resolveCol(file, col_name, diag);
    return ast.Filter{ .null_check = .{ .col_idx = col_idx, .is_not = is_not } };
}

/// Build a `LIKE` / `NOT LIKE` leaf. The pattern is classified once
/// (DuckDB's trick) so prefix/suffix/contains/exact patterns dodge the
/// general matcher. LIKE is text-only — non-string columns are a clear
/// error rather than a confusing type mismatch later.
fn parseLike(
    col_part: []const u8,
    pat_part: []const u8,
    negate: bool,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const col_name = std.mem.trim(u8, col_part, " ");
    if (col_name.len == 0) return error.BadOperator;
    const pattern = stripStringQuotes(std.mem.trim(u8, pat_part, " "));
    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    if (elem.type != .BYTE_ARRAY) {
        const type_name = @tagName(elem.type orelse .BYTE_ARRAY);
        Diag.set(diag, col_name, .{ .like_non_string = .{ .type_name = type_name } });
        return error.UnsupportedType;
    }
    const c = classifyLike(pattern);
    return ast.Filter{ .like = .{ .col_idx = col_idx, .kind = c.kind, .operand = c.operand, .negate = negate } };
}

/// Classify a LIKE pattern into a fast kind + its literal operand, or
/// `.general` (raw pattern) when `_` or interior `%` are present.
fn classifyLike(pat: []const u8) struct { kind: ast.LikeKind, operand: []const u8 } {
    const has_pct = std.mem.indexOfScalar(u8, pat, '%') != null;
    const has_us = std.mem.indexOfScalar(u8, pat, '_') != null;
    if (!has_pct and !has_us) return .{ .kind = .exact, .operand = pat };
    if (!has_us) {
        // prefix `abc%`: trailing % is the only %, doesn't lead with %.
        if (pat.len >= 1 and pat[pat.len - 1] == '%' and pat[0] != '%' and
            std.mem.indexOfScalar(u8, pat[0 .. pat.len - 1], '%') == null)
            return .{ .kind = .prefix, .operand = pat[0 .. pat.len - 1] };
        // suffix `%abc`: leading % is the only %.
        if (pat.len >= 1 and pat[0] == '%' and
            std.mem.indexOfScalar(u8, pat[1..], '%') == null)
            return .{ .kind = .suffix, .operand = pat[1..] };
        // contains `%abc%`: % at both ends, none between.
        if (pat.len >= 2 and pat[0] == '%' and pat[pat.len - 1] == '%' and
            std.mem.indexOfScalar(u8, pat[1 .. pat.len - 1], '%') == null)
            return .{ .kind = .contains, .operand = pat[1 .. pat.len - 1] };
    }
    return .{ .kind = .general, .operand = pat };
}

const InClause = struct { col_end: usize, vals_start: usize, negated: bool };

fn findInClause(expr: []const u8) ?InClause {
    if (caseInsensitiveIndexOf(expr, " NOT IN ")) |i|
        return .{ .col_end = i, .vals_start = i + " NOT IN ".len, .negated = true };
    if (caseInsensitiveIndexOf(expr, " IN ")) |i|
        return .{ .col_end = i, .vals_start = i + " IN ".len, .negated = false };
    return null;
}

fn parseInList(
    arena: std.mem.Allocator,
    expr: []const u8,
    inc: InClause,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const col_name = std.mem.trim(u8, expr[0..inc.col_end], " ");
    var vals = std.mem.trim(u8, expr[inc.vals_start..], " ");
    if (col_name.len == 0) return error.BadOperator;
    // Require a parenthesized list. (Commas inside quoted string values
    // aren't supported — a documented limitation of the simple split.)
    if (vals.len < 2 or vals[0] != '(' or vals[vals.len - 1] != ')') return error.BadOperator;
    vals = std.mem.trim(u8, vals[1 .. vals.len - 1], " ");
    if (vals.len == 0) return error.BadValue; // empty list

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;

    // IN → OR of `= v`; NOT IN → AND of `!= v` (De Morgan).
    const leaf_op: ast.Operator = if (inc.negated) .NotEq else .Eq;
    var acc: ?ast.Filter = null;
    var it = std.mem.splitScalar(u8, vals, ',');
    while (it.next()) |raw| {
        const v = std.mem.trim(u8, raw, " ");
        if (v.len == 0) return error.BadValue;
        const leaf = try buildTypedComparison(col_idx, &elem, leaf_op, v, diag);
        if (acc) |prev| {
            const l = try arena.create(ast.Filter);
            const r = try arena.create(ast.Filter);
            l.* = prev;
            r.* = leaf;
            acc = if (inc.negated)
                .{ .and_filter = .{ .left = l, .right = r } }
            else
                .{ .or_filter = .{ .left = l, .right = r } };
        } else acc = leaf;
    }
    return acc orelse error.BadValue;
}

/// Negate a filter subtree: flip leaf operators, De Morgan on composites.
/// NULL handling matches ZPQ's "nulls don't pass" convention either way
/// (both `NOT (x > 5)` and `x <= 5` exclude null rows).
fn negateFilter(arena: std.mem.Allocator, f: ast.Filter) Error!ast.Filter {
    return switch (f) {
        .int32 => |l| .{ .int32 = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .int64 => |l| .{ .int64 = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .uint64 => |l| .{ .uint64 = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .float => |l| .{ .float = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .double => |l| .{ .double = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .string => |l| .{ .string = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        .boolean => |l| .{ .boolean = .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value } },
        // NOT (x IS NULL) == x IS NOT NULL, and vice-versa.
        .null_check => |nc| .{ .null_check = .{ .col_idx = nc.col_idx, .is_not = !nc.is_not } },
        // NOT (x LIKE p) == x NOT LIKE p — flip the negate flag.
        .like => |m| .{ .like = .{ .col_idx = m.col_idx, .kind = m.kind, .operand = m.operand, .negate = !m.negate } },
        .and_filter => |c| blk: {
            const l = try arena.create(ast.Filter);
            const r = try arena.create(ast.Filter);
            l.* = try negateFilter(arena, c.left.*);
            r.* = try negateFilter(arena, c.right.*);
            break :blk .{ .or_filter = .{ .left = l, .right = r } };
        },
        .or_filter => |c| blk: {
            const l = try arena.create(ast.Filter);
            const r = try arena.create(ast.Filter);
            l.* = try negateFilter(arena, c.left.*);
            r.* = try negateFilter(arena, c.right.*);
            break :blk .{ .and_filter = .{ .left = l, .right = r } };
        },
    };
}

/// Parse `col BETWEEN x AND y` into the conjunction `(col >= x AND col <= y)`.
/// SQL semantics: BETWEEN is inclusive on both sides.
fn parseBetween(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const between_at = caseInsensitiveIndexOf(expr, " BETWEEN ") orelse return error.BadOperator;
    const after_between = between_at + " BETWEEN ".len;
    const and_at = caseInsensitiveIndexOf(expr[after_between..], " AND ") orelse return error.BadOperator;

    const col_name = std.mem.trim(u8, expr[0..between_at], " ");
    const x_str = std.mem.trim(u8, expr[after_between .. after_between + and_at], " ");
    const y_str = std.mem.trim(u8, expr[after_between + and_at + " AND ".len ..], " ");
    if (col_name.len == 0 or x_str.len == 0 or y_str.len == 0) return error.BadOperator;

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;

    // Bounds bind exactly like `>=`/`<=` would. Binding on the bare physical type instead turned DECIMAL(FLBA)
    // bounds into byte-string leaves (silently wrong) and rejected DATE/TIMESTAMP literals.
    const left = try arena.create(ast.Filter);
    const right = try arena.create(ast.Filter);
    left.* = try buildTypedComparison(col_idx, &elem, .GtEq, x_str, diag);
    right.* = try buildTypedComparison(col_idx, &elem, .LtEq, y_str, diag);
    return .{ .and_filter = .{ .left = left, .right = right } };
}

fn parseComparisonLeaf(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    _ = arena;
    // Stray parens reach here only as grouping attempts — IN-list parens
    // were consumed upstream. Without this, `(a OR b)` splits into the
    // fragment `(a` and fails with a misleading UnknownColumn.
    if (std.mem.indexOfScalar(u8, expr, '(') != null or std.mem.indexOfScalar(u8, expr, ')') != null) {
        return error.GroupingNotSupported;
    }
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

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    return try buildTypedComparison(col_idx, &elem, op, val_str, diag);
}

/// Build a single typed comparison leaf, honoring DECIMAL columns (decoded
/// to f64 → the .double lane) and DATE/TIMESTAMP columns (SQL-style literals
/// → stored epoch ints). Shared by the comparison path and the IN-list
/// expansion so both get identical type handling.
fn buildTypedComparison(
    col_idx: usize,
    elem: *const schema.SchemaElement,
    op: ast.Operator,
    val_str: []const u8,
    diag: ?*Diag,
) Error!ast.Filter {
    if (decimal_mod.kindFromSchema(elem) != null) {
        return buildLeafFilter(col_idx, op, val_str, .DOUBLE);
    }
    if (schema.isUnsignedInt(elem.*)) return buildUnsignedLeaf(col_idx, op, val_str);
    if (temporalKind(elem)) |tk| {
        return buildTemporalLeaf(col_idx, op, val_str, tk);
    }
    // FLOAT16 is physically FIXED_LEN_BYTE_ARRAY(2); without an IEEE-half →
    // f64 lane in the filter/prune path, a numeric predicate falls through to
    // the byte (.string) leaf below and is compared lexicographically against
    // raw half-float stat bytes. That silently mis-prunes row groups (the
    // stats path returns a bogus 0/empty while --scan-all errors at decode).
    // Fail loud and consistently on both paths until the f64 lane is wired.
    if (schema.isFloat16(elem.*)) {
        Diag.set(diag, elem.name, .float16_filter);
        return error.UnsupportedType;
    }
    const ptype = elem.type orelse return error.UnsupportedType;
    return buildLeafFilter(col_idx, op, val_str, ptype);
}

/// Unsigned columns compare as u64. They hold only 0..2^64-1, so a literal outside that range has a constant answer
/// for every non-null row (`u > -1` holds, `u = -1` never does); fold it into a leaf that is always true (`>= 0`) or
/// always false (`< 0`) so evaluation and pruning need no out-of-range case.
fn buildUnsignedLeaf(col_idx: usize, op: ast.Operator, val_str: []const u8) Error!ast.Filter {
    const v = std.fmt.parseInt(i128, val_str, 10) catch return error.BadValue;
    if (std.math.cast(u64, v)) |in_range| return .{ .uint64 = .{ .col_idx = col_idx, .op = op, .value = in_range } };
    const below = v < 0;
    const holds = switch (op) {
        .Eq => false,
        .NotEq => true,
        .Lt, .LtEq => !below,
        .Gt, .GtEq => below,
    };
    return .{ .uint64 = .{ .col_idx = col_idx, .op = if (holds) .GtEq else .Lt, .value = 0 } };
}

/// A column whose INT32/INT64(/INT96) storage carries a temporal logical type. `ns_per_tick` is the stored unit
/// (a whole day for DATE); `lane` is the leaf type its values decode to.
const TemporalKind = struct {
    literal: enum { date_time, time_of_day },
    ns_per_tick: i128,
    lane: Lane,
};
const Lane = enum { int32, int64 };

const ns_per_s: i128 = 1_000_000_000;
const ns_per_day: i128 = 86_400 * ns_per_s;

fn unitNs(unit: schema.TimeUnit) i128 {
    return switch (unit) {
        .MILLIS => 1_000_000,
        .MICROS => 1_000,
        .NANOS => 1,
    };
}

fn temporalKind(elem: *const schema.SchemaElement) ?TemporalKind {
    const date: TemporalKind = .{ .literal = .date_time, .ns_per_tick = ns_per_day, .lane = .int32 };
    // INT96 is a legacy Spark/Impala timestamp — the consumer decodes it to
    // i64 epoch-nanoseconds, so filter literals parse as nanos regardless of
    // any (usually absent) logical type.
    if (elem.type == .INT96) return .{ .literal = .date_time, .ns_per_tick = 1, .lane = .int64 };
    if (elem.logical_type) |lt| switch (lt) {
        .DATE => return date,
        .TIMESTAMP => |ts| return .{ .literal = .date_time, .ns_per_tick = unitNs(ts.unit), .lane = .int64 },
        .TIME => |t| return .{
            .literal = .time_of_day,
            .ns_per_tick = unitNs(t.unit),
            .lane = if (elem.type == .INT32) .int32 else .int64,
        },
        else => {},
    };
    if (elem.converted_type) |ct| switch (ct) {
        .DATE => return date,
        .TIMESTAMP_MILLIS => return .{ .literal = .date_time, .ns_per_tick = 1_000_000, .lane = .int64 },
        .TIMESTAMP_MICROS => return .{ .literal = .date_time, .ns_per_tick = 1_000, .lane = .int64 },
        .TIME_MILLIS => return .{ .literal = .time_of_day, .ns_per_tick = 1_000_000, .lane = .int32 },
        .TIME_MICROS => return .{ .literal = .time_of_day, .ns_per_tick = 1_000, .lane = .int64 },
        else => {},
    };
    return null;
}

fn buildTemporalLeaf(col_idx: usize, op: ast.Operator, val_str_raw: []const u8, tk: TemporalKind) Error!ast.Filter {
    const val_str = stripStringQuotes(val_str_raw);
    // Bare integer = raw ticks in the column's unit (back-compat).
    if (std.fmt.parseInt(i64, val_str, 10)) |ticks| {
        return bindExact(col_idx, op, tk.lane, ticks, false);
    } else |_| {}
    const lit = switch (tk.literal) {
        .date_time => try parseDateTimeNs(val_str),
        .time_of_day => try parseTimeOfDayNs(val_str),
    };
    const inexact = lit.sub_ns or @mod(lit.ns, tk.ns_per_tick) != 0;
    return bindExact(col_idx, op, tk.lane, @divFloor(lit.ns, tk.ns_per_tick), inexact);
}

fn bindExact(col_idx: usize, op: ast.Operator, lane: Lane, floor: i128, inexact: bool) ast.Filter {
    return switch (lane) {
        .int32 => .{ .int32 = exactLeaf(i32, col_idx, op, floor, inexact) },
        .int64 => .{ .int64 = exactLeaf(i64, col_idx, op, floor, inexact) },
    };
}

/// `col op v` as an equivalent comparison against a value of the column's type, where `v` is `floor` when exact and
/// otherwise lies strictly between `floor` and `floor + 1` (or beyond the type's range). Rounding the literal to the
/// column's unit changed answers (`>= 0.0995` s on a millisecond column matched 0.099 s), and because the rewrite is
/// equivalent on every non-null value, eval, page/row-group pruning and the full-match proof stay exact unchanged.
fn exactLeaf(comptime T: type, col_idx: usize, op: ast.Operator, floor: i128, inexact: bool) ast.Leaf(T) {
    const lo = std.math.minInt(T);
    const hi = std.math.maxInt(T);
    // No stored value equals `v`: `=` matches nothing and `!=` every non-null row. `col > maxInt` and
    // `col <= maxInt` say exactly that while still rejecting nulls, and negate into each other for NOT.
    const never: ast.Leaf(T) = .{ .col_idx = col_idx, .op = .Gt, .value = hi };
    const always: ast.Leaf(T) = .{ .col_idx = col_idx, .op = .LtEq, .value = hi };
    const o: ast.Operator = if (!inexact) op else switch (op) {
        .Eq => return never,
        .NotEq => return always,
        .Gt, .GtEq => .Gt,
        .Lt, .LtEq => .LtEq,
    };
    if (floor > hi) return switch (o) {
        .Gt, .GtEq, .Eq => never,
        .Lt, .LtEq, .NotEq => always,
    };
    if (floor < lo) return switch (o) {
        .Lt, .LtEq, .Eq => never,
        .Gt, .GtEq, .NotEq => always,
    };
    return .{ .col_idx = col_idx, .op = o, .value = @intCast(floor) };
}

/// A literal as whole nanoseconds; `sub_ns` means a nonzero fraction digit past the ninth put it strictly between
/// `ns` and `ns + 1`.
const ExactNs = struct { ns: i128, sub_ns: bool };

/// Days since 1970-01-01 for a proleptic-Gregorian (y, m, d). Howard
/// Hinnant's days_from_civil — std has no civil-date conversion, and we
/// need bit-exact epoch days to match DuckDB/Spark-written DATE columns.
fn daysFromCivil(y_in: i64, m: i64, d: i64) i64 {
    const y = y_in - @as(i64, @intFromBool(m <= 2));
    const era = @divFloor(if (y >= 0) y else y - 399, 400);
    const yoe = y - era * 400; // [0, 399]
    const mp = @mod(m + 9, 12); // Mar=0 .. Feb=11
    const doy = @divTrunc(153 * mp + 2, 5) + d - 1; // [0, 365]
    const doe = yoe * 365 + @divTrunc(yoe, 4) - @divTrunc(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

/// Unsigned decimal of 1..`max_len` digits. Stricter than parseInt: no sign, no `_`, and the length cap keeps
/// every later product far from overflow.
fn parseDigits(s: []const u8, max_len: usize) Error!i64 {
    if (s.len == 0 or s.len > max_len) return error.BadValue;
    var v: i64 = 0;
    for (s) |c| {
        if (c < '0' or c > '9') return error.BadValue;
        v = v * 10 + (c - '0');
    }
    return v;
}

fn parseDateDays(s: []const u8) Error!i64 {
    var it = std.mem.splitScalar(u8, s, '-');
    const y = try parseDigits(it.next() orelse return error.BadValue, 6);
    const mo = try parseDigits(it.next() orelse return error.BadValue, 2);
    const d = try parseDigits(it.next() orelse return error.BadValue, 2);
    if (it.next() != null) return error.BadValue; // trailing junk
    if (mo < 1 or mo > 12 or d < 1) return error.BadValue;
    const leap = @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
    const month_days = [12]i64{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (d > month_days[@intCast(mo - 1)]) return error.BadValue;
    return daysFromCivil(y, mo, d);
}

/// `YYYY-MM-DD[( |T)HH:MM:SS[.fraction]]` → nanoseconds since the epoch; no time part means midnight.
fn parseDateTimeNs(s: []const u8) Error!ExactNs {
    const sep = std.mem.indexOfAny(u8, s, " T");
    const days = try parseDateDays(if (sep) |i| s[0..i] else s);
    const tod: ExactNs = if (sep) |i| try parseTimeOfDayNs(s[i + 1 ..]) else .{ .ns = 0, .sub_ns = false };
    return .{ .ns = days * ns_per_day + tod.ns, .sub_ns = tod.sub_ns };
}

/// `HH:MM:SS[.fraction]` → nanoseconds since midnight. Fraction digits past nanoseconds are kept as `sub_ns` rather
/// than dropped: truncating them is what moved literals across stored values.
fn parseTimeOfDayNs(s: []const u8) Error!ExactNs {
    const dot = std.mem.indexOfScalar(u8, s, '.');
    var it = std.mem.splitScalar(u8, if (dot) |d| s[0..d] else s, ':');
    const hh = try parseDigits(it.next() orelse return error.BadValue, 2);
    const mm = try parseDigits(it.next() orelse return error.BadValue, 2);
    const ss = try parseDigits(it.next() orelse return error.BadValue, 2);
    if (it.next() != null or hh > 23 or mm > 59 or ss > 59) return error.BadValue;
    var frac_ns: i64 = 0;
    var sub_ns = false;
    if (dot) |d| {
        const fr = s[d + 1 ..];
        if (fr.len == 0) return error.BadValue;
        for (fr, 0..) |c, i| {
            if (c < '0' or c > '9') return error.BadValue;
            if (i < 9) frac_ns = frac_ns * 10 + (c - '0') else if (c != '0') sub_ns = true;
        }
        for (@min(fr.len, 9)..9) |_| frac_ns *= 10;
    }
    return .{ .ns = ((hh * 60 + mm) * 60 + ss) * ns_per_s + frac_ns, .sub_ns = sub_ns };
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
            .value = stripStringQuotes(val_str),
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

/// Strip one matching pair of surrounding quotes from a string literal so
/// the SQL-style `name = 'row42'` compares against `row42`, not `'row42'`.
/// Bare/unquoted literals pass through unchanged (back-compat with the
/// original syntax). Without this, quoted string filters silently matched
/// nothing — found by the duckdb cross-impl smoke, 2026-06-13.
fn stripStringQuotes(s: []const u8) []const u8 {
    if (s.len >= 2) {
        const q = s[0];
        if ((q == '\'' or q == '"') and s[s.len - 1] == q) {
            return s[1 .. s.len - 1];
        }
    }
    return s;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const thrift = @import("../thrift.zig");
const test_fixtures = @import("../parquet/test_fixtures.zig");

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
        .num_children = 10,
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
    try meta.schema.append(arena, .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "d",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = .{ .DATE = .{} },
    });
    try meta.schema.append(arena, .{
        .type = .INT64,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = "ts",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = .{ .MICROS = .{} } } },
    });
    try meta.schema.append(arena, .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 9,
        .repetition_type = .OPTIONAL,
        .name = "dec",
        .num_children = null,
        .scale = 2,
        .precision = 20,
        .field_id = null,
        .converted_type = .DECIMAL,
        .logical_type = .{ .DECIMAL = .{ .scale = 2, .precision = 20 } },
    });
    const ms: schema.TimeUnit = .{ .MILLIS = .{} };
    const ns: schema.TimeUnit = .{ .NANOS = .{} };
    const temporal = [_]struct { name: []const u8, type: schema.Type, lt: schema.LogicalType }{
        .{ .name = "tsms", .type = .INT64, .lt = .{ .TIMESTAMP = .{ .isAdjustedToUTC = false, .unit = ms } } },
        .{ .name = "tsns", .type = .INT64, .lt = .{ .TIMESTAMP = .{ .isAdjustedToUTC = false, .unit = ns } } },
        .{ .name = "tm", .type = .INT32, .lt = .{ .TIME = .{ .isAdjustedToUTC = false, .unit = ms } } },
        .{ .name = "tmn", .type = .INT64, .lt = .{ .TIME = .{ .isAdjustedToUTC = false, .unit = ns } } },
    };
    for (temporal) |t| try meta.schema.append(arena, .{
        .type = t.type,
        .type_length = null,
        .repetition_type = .OPTIONAL,
        .name = t.name,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = t.lt,
    });
    return meta;
}

test "parse simple int equality" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id=42", &meta, null);
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

    const f = try parse(a, "status<active", &meta, null);
    try testing.expectEqual(@as(usize, 1), f.string.col_idx);
    try testing.expectEqual(ast.Operator.Lt, f.string.op);
    try testing.expectEqualStrings("active", f.string.value);
}

test "parse strips quotes from string literals (sql-style + back-compat)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // SQL-style single + double quotes both strip to the bare value.
    const single = try parse(a, "status = 'active'", &meta, null);
    try testing.expectEqualStrings("active", single.string.value);
    const double = try parse(a, "status = \"active\"", &meta, null);
    try testing.expectEqualStrings("active", double.string.value);
    // Bare/unquoted literal still works (original syntax).
    const bare = try parse(a, "status = active", &meta, null);
    try testing.expectEqualStrings("active", bare.string.value);
}

test "parse DATE/TIMESTAMP literals → epoch ints" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // DATE 'YYYY-MM-DD' → days since epoch (2024-01-01 = 19723).
    const fd = try parse(a, "d >= '2024-01-01'", &meta, null);
    try testing.expectEqual(ast.Operator.GtEq, fd.int32.op);
    try testing.expectEqual(@as(i32, 19723), fd.int32.value);
    // Epoch day 0 = 1970-01-01.
    try testing.expectEqual(@as(i32, 0), (try parse(a, "d = '1970-01-01'", &meta, null)).int32.value);
    // Bare integer still works (raw epoch days).
    try testing.expectEqual(@as(i32, 100), (try parse(a, "d = 100", &meta, null)).int32.value);

    // TIMESTAMP (micros) → micros since epoch.
    // 2024-01-01 00:00:00 UTC = 19723*86400 s = 1_704_067_200_000_000 us.
    const ft = try parse(a, "ts < '2024-01-01 00:00:00'", &meta, null);
    try testing.expectEqual(@as(i64, 1_704_067_200_000_000), ft.int64.value);
    // Date-only literal on a timestamp column → midnight.
    const day = try parse(a, "ts = '2024-01-01'", &meta, null);
    try testing.expectEqual(@as(i64, 1_704_067_200_000_000), day.int64.value);
    // Fractional seconds (micros precision).
    const frac = try parse(a, "ts = '2024-01-01 00:00:00.5'", &meta, null);
    try testing.expectEqual(@as(i64, 1_704_067_200_000_000 + 500_000), frac.int64.value);
}

fn expectLeaf(comptime T: type, f: ast.Filter, op: ast.Operator, value: T) !void {
    const leaf = switch (T) {
        i32 => f.int32,
        i64 => f.int64,
        else => @compileError("int lanes only"),
    };
    try testing.expectEqual(op, leaf.op);
    try testing.expectEqual(value, leaf.value);
}

test "parse binds temporal literals finer than the column's unit exactly" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);
    const max64 = std.math.maxInt(i64);
    const max32 = std.math.maxInt(i32);
    const ms: i64 = 1_704_067_200_000; // 2024-01-01 00:00:00 in each unit
    const us: i64 = ms * 1_000;
    const ns: i64 = us * 1_000;

    // 99.5 ms on a millisecond column: one-sided ops round toward the side that keeps the answer.
    try expectLeaf(i64, try parse(a, "tsms >= '2024-01-01 00:00:00.0995'", &meta, null), .Gt, ms + 99);
    try expectLeaf(i64, try parse(a, "tsms > '2024-01-01 00:00:00.0995'", &meta, null), .Gt, ms + 99);
    try expectLeaf(i64, try parse(a, "tsms < '2024-01-01 00:00:00.0995'", &meta, null), .LtEq, ms + 99);
    try expectLeaf(i64, try parse(a, "tsms <= '2024-01-01 00:00:00.0995'", &meta, null), .LtEq, ms + 99);
    // `=` against an unrepresentable value matches nothing, `!=` every non-null row.
    try expectLeaf(i64, try parse(a, "tsms = '2024-01-01 00:00:00.0995'", &meta, null), .Gt, max64);
    try expectLeaf(i64, try parse(a, "tsms != '2024-01-01 00:00:00.0995'", &meta, null), .LtEq, max64);
    try expectLeaf(i64, try parse(a, "NOT tsms = '2024-01-01 00:00:00.0995'", &meta, null), .LtEq, max64);
    // Trailing zeros are still exact.
    try expectLeaf(i64, try parse(a, "tsms >= '2024-01-01 00:00:00.09900'", &meta, null), .GtEq, ms + 99);
    try expectLeaf(i64, try parse(a, "tsms = '2024-01-01T00:00:00.099'", &meta, null), .Eq, ms + 99);
    // Before the epoch the stored value still floors: -0.5 ms lies between -1 and 0.
    try expectLeaf(i64, try parse(a, "tsms >= '1969-12-31 23:59:59.9995'", &meta, null), .Gt, -1);
    try expectLeaf(i64, try parse(a, "tsms < '1969-12-31 23:59:59.9995'", &meta, null), .LtEq, -1);

    try expectLeaf(i64, try parse(a, "ts >= '2024-01-01 00:00:00.0000015'", &meta, null), .Gt, us + 1);
    try expectLeaf(i64, try parse(a, "ts = '2024-01-01 00:00:00.000001'", &meta, null), .Eq, us + 1);

    // Digits past the ninth: nonzero ones make the literal inexact even at nanosecond resolution.
    try expectLeaf(i64, try parse(a, "tsns >= '2024-01-01 00:00:00.0000000015'", &meta, null), .Gt, ns + 1);
    try expectLeaf(i64, try parse(a, "tsns <= '2024-01-01 00:00:00.0000000015'", &meta, null), .LtEq, ns + 1);
    try expectLeaf(i64, try parse(a, "tsns = '2024-01-01 00:00:00.000000001000'", &meta, null), .Eq, ns + 1);
    try expectLeaf(i64, try parse(a, "tsms >= '2024-01-01 00:00:00.0990000001'", &meta, null), .Gt, ms + 99);
    // Past the i64 nanosecond range (year 2262): every row compares the same way, no overflow.
    try expectLeaf(i64, try parse(a, "tsns >= '2500-01-01'", &meta, null), .Gt, max64);
    try expectLeaf(i64, try parse(a, "tsns < '2500-01-01'", &meta, null), .LtEq, max64);
    try expectLeaf(i64, try parse(a, "tsns > '1600-01-01'", &meta, null), .LtEq, max64);
    try expectLeaf(i64, try parse(a, "tsns <= '1600-01-01'", &meta, null), .Gt, max64);

    // DATE vs a timestamp literal compares as a timestamp: noon is after the date's midnight.
    try expectLeaf(i32, try parse(a, "d >= '2024-01-01 12:00:00'", &meta, null), .Gt, 19723);
    try expectLeaf(i32, try parse(a, "d < '2024-01-01 12:00:00'", &meta, null), .LtEq, 19723);
    try expectLeaf(i32, try parse(a, "d = '2024-01-01 00:00:00.000000001'", &meta, null), .Gt, max32);
    try expectLeaf(i32, try parse(a, "d = '2024-01-01 00:00:00'", &meta, null), .Eq, 19723);

    // TIME32 (millis, INT32) and TIME64 (nanos, INT64).
    try expectLeaf(i32, try parse(a, "tm >= '00:01:40.0005'", &meta, null), .Gt, 100_000);
    try expectLeaf(i32, try parse(a, "tm = '00:01:40'", &meta, null), .Eq, 100_000);
    try expectLeaf(i32, try parse(a, "tm != '00:01:40.0005'", &meta, null), .LtEq, max32);
    try expectLeaf(i32, try parse(a, "tm < 100000", &meta, null), .Lt, 100_000);
    try expectLeaf(i64, try parse(a, "tmn < '00:00:01.0000000001'", &meta, null), .LtEq, 1_000_000_000);
    try expectLeaf(i64, try parse(a, "tmn > '23:59:59.999999999'", &meta, null), .Gt, 86_399_999_999_999);

    // BETWEEN and IN bind each bound/element the same way.
    const btw = try parse(a, "tsms BETWEEN '2024-01-01 00:00:00.0005' AND '2024-01-01 00:00:00.0105'", &meta, null);
    try expectLeaf(i64, btw.and_filter.left.*, .Gt, ms);
    try expectLeaf(i64, btw.and_filter.right.*, .LtEq, ms + 10);
    const in = try parse(a, "tsms IN ('2024-01-01 00:00:00.0995', '2024-01-01 00:00:00.100')", &meta, null);
    try expectLeaf(i64, in.or_filter.left.*, .Gt, max64);
    try expectLeaf(i64, in.or_filter.right.*, .Eq, ms + 100);
    const nin = try parse(a, "tsms NOT IN ('2024-01-01 00:00:00.0995', '2024-01-01 00:00:00.100')", &meta, null);
    try expectLeaf(i64, nin.and_filter.left.*, .LtEq, max64);
    try expectLeaf(i64, nin.and_filter.right.*, .NotEq, ms + 100);

    // Malformed literals fail loudly instead of binding something nearby.
    const bad = [_][]const u8{
        "tm >= '24:00:00'",            "tm >= '00:60:00'",         "tm >= '00:00:00.'",
        "tm >= '00:00:00.5x'",         "d >= '2023-02-29'",        "d >= '2024-04-31'",
        "ts >= '2024-01-01 +1:00:00'", "ts >= '2024-01-01 00:00'", "d >= '2024-1x-01'",
    };
    for (bad) |e| try testing.expectError(error.BadValue, parse(a, e, &meta, null));
}

test "parse IN / NOT IN / NOT" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // id IN (1, 2, 3) → ((id=1 OR id=2) OR id=3)
    const f = try parse(a, "id IN (1, 2, 3)", &meta, null);
    try testing.expect(f == .or_filter);
    try testing.expectEqual(@as(i32, 1), f.or_filter.left.*.or_filter.left.*.int32.value);
    try testing.expectEqual(ast.Operator.Eq, f.or_filter.left.*.or_filter.left.*.int32.op);
    try testing.expectEqual(@as(i32, 3), f.or_filter.right.*.int32.value);

    // id NOT IN (1, 2) → (id!=1 AND id!=2)
    const fni = try parse(a, "id NOT IN (1, 2)", &meta, null);
    try testing.expect(fni == .and_filter);
    try testing.expectEqual(ast.Operator.NotEq, fni.and_filter.left.*.int32.op);
    try testing.expectEqual(ast.Operator.NotEq, fni.and_filter.right.*.int32.op);

    // string IN with quotes
    const fs = try parse(a, "status IN ('active', 'idle')", &meta, null);
    try testing.expect(fs == .or_filter);
    try testing.expectEqualStrings("active", fs.or_filter.left.*.string.value);
    try testing.expectEqualStrings("idle", fs.or_filter.right.*.string.value);

    // NOT id > 5 → id <= 5
    const fnot = try parse(a, "NOT id > 5", &meta, null);
    try testing.expectEqual(ast.Operator.LtEq, fnot.int32.op);
    try testing.expectEqual(@as(i32, 5), fnot.int32.value);

    // NOT score BETWEEN 1 AND 2 → De Morgan → (score < 1 OR score > 2)
    const fb = try parse(a, "NOT score BETWEEN 1 AND 2", &meta, null);
    try testing.expect(fb == .or_filter);
    try testing.expectEqual(ast.Operator.Lt, fb.or_filter.left.*.double.op);
    try testing.expectEqual(ast.Operator.Gt, fb.or_filter.right.*.double.op);
}

test "parse double >=" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "score>=0.5", &meta, null);
    try testing.expectEqual(ast.Operator.GtEq, f.double.op);
    try testing.expectApproxEqAbs(@as(f64, 0.5), f.double.value, 1e-9);
}

test "parse AND composite" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id>10 AND status=active", &meta, null);
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
    const f = try parse(a, "status=error OR id>=1000 AND score=1.5", &meta, null);
    try testing.expect(f == .or_filter);
    try testing.expect(f.or_filter.right.* == .and_filter);
}

test "parse rejects unknown column" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    try testing.expectError(error.UnknownColumn, parse(a, "missing=1", &meta, null));
}

test "parse rejects unsupported syntax with specific errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // Grouping parens get a named error (not the old misleading UnknownColumn).
    try testing.expectError(error.GroupingNotSupported, parse(a, "(id > 1 OR id < 5)", &meta, null));
    try testing.expectError(error.GroupingNotSupported, parse(a, "id > 1 AND (score < 5 OR id > 9)", &meta, null));
    // LIKE on a non-string column is a clear error.
    try testing.expectError(error.UnsupportedType, parse(a, "id LIKE '5%'", &meta, null));
}

test "parse LIKE → classified like node (+ NOT LIKE)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const cases = [_]struct { expr: []const u8, kind: ast.LikeKind, operand: []const u8 }{
        .{ .expr = "status LIKE 'abc'", .kind = .exact, .operand = "abc" },
        .{ .expr = "status LIKE 'abc%'", .kind = .prefix, .operand = "abc" },
        .{ .expr = "status LIKE '%abc'", .kind = .suffix, .operand = "abc" },
        .{ .expr = "status LIKE '%abc%'", .kind = .contains, .operand = "abc" },
        .{ .expr = "status LIKE 'a%c'", .kind = .general, .operand = "a%c" },
        .{ .expr = "status LIKE 'a_c'", .kind = .general, .operand = "a_c" },
    };
    for (cases) |c| {
        const f = try parse(a, c.expr, &meta, null);
        try testing.expect(f == .like);
        try testing.expectEqual(c.kind, f.like.kind);
        try testing.expectEqualStrings(c.operand, f.like.operand);
        try testing.expect(!f.like.negate);
    }
    // NOT LIKE and leading NOT both set negate.
    const nl = try parse(a, "status NOT LIKE 'x%'", &meta, null);
    try testing.expect(nl == .like and nl.like.negate and nl.like.kind == .prefix);
    const lead = try parse(a, "NOT status LIKE 'x%'", &meta, null);
    try testing.expect(lead == .like and lead.like.negate);
}

test "likeGlob backtracking matcher" {
    try testing.expect(ast.likeGlob("a%c", "abc"));
    try testing.expect(ast.likeGlob("a%c", "axxxxc"));
    try testing.expect(ast.likeGlob("a%c", "ac"));
    try testing.expect(!ast.likeGlob("a%c", "abd"));
    try testing.expect(ast.likeGlob("a_c", "abc"));
    try testing.expect(!ast.likeGlob("a_c", "ac")); // _ needs exactly one
    try testing.expect(ast.likeGlob("%", "anything"));
    try testing.expect(ast.likeGlob("%mid%", "a mid z"));
    try testing.expect(!ast.likeGlob("end", "the end x"));
}

test "parse IS NULL / IS NOT NULL → null_check node (+ NOT negation)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f_null = try parse(a, "status IS NULL", &meta, null);
    try testing.expect(f_null == .null_check);
    try testing.expectEqual(@as(usize, 1), f_null.null_check.col_idx); // status is leaf 1
    try testing.expect(!f_null.null_check.is_not);

    const f_nn = try parse(a, "status IS NOT NULL", &meta, null);
    try testing.expect(f_nn == .null_check and f_nn.null_check.is_not);

    // NOT (x IS NULL) folds to IS NOT NULL.
    const f_not = try parse(a, "NOT status IS NULL", &meta, null);
    try testing.expect(f_not == .null_check and f_not.null_check.is_not);

    // case-insensitive; composes inside AND.
    const f_and = try parse(a, "id > 1 AND score is not null", &meta, null);
    try testing.expect(f_and == .and_filter);
    try testing.expect(f_and.and_filter.right.* == .null_check);

    // bare `IS <other>` is a clear error, not a silent fallthrough.
    try testing.expectError(error.BadOperator, parse(a, "id IS 5", &meta, null));
}

test "parse double-quoted identifiers resolve like bare" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // SQL-standard `"col"` quoting must resolve to the same leaf as bare
    // `col`, on both the comparison path (resolveCol) and the schema-lookup
    // path (getColumnSchema) — the two sites the quoting fix had to cover.
    const cmp = try parse(a, "\"id\" = 5", &meta, null);
    try testing.expect(cmp == .int32);
    try testing.expectEqual(@as(i32, 5), cmp.int32.value);

    const btw = try parse(a, "\"id\" BETWEEN 10 AND 20", &meta, null);
    try testing.expect(btw == .and_filter);
    try testing.expectEqual(@as(i32, 10), btw.and_filter.left.int32.value);

    // A quoted *unknown* column still errors cleanly (no quote-swallowing bug).
    try testing.expectError(error.UnknownColumn, parse(a, "\"nope\" = 1", &meta, null));
}

test "parse BETWEEN expands to >= AND <= conjunction" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const f = try parse(a, "id BETWEEN 10 AND 20", &meta, null);
    try testing.expect(f == .and_filter);
    try testing.expect(f.and_filter.left.* == .int32);
    try testing.expect(f.and_filter.right.* == .int32);
    try testing.expectEqual(ast.Operator.GtEq, f.and_filter.left.int32.op);
    try testing.expectEqual(@as(i32, 10), f.and_filter.left.int32.value);
    try testing.expectEqual(ast.Operator.LtEq, f.and_filter.right.int32.op);
    try testing.expectEqual(@as(i32, 20), f.and_filter.right.int32.value);
}

test "parse BETWEEN binds its bounds like the comparison operators" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // DECIMAL bounds take the f64 lane, not a byte-string compare against the FLBA bytes.
    const fd = try parse(a, "dec BETWEEN 1 AND 2.5", &meta, null);
    try testing.expect(fd.and_filter.left.* == .double and fd.and_filter.right.* == .double);
    try testing.expectEqual(@as(f64, 1), fd.and_filter.left.double.value);
    try testing.expectEqual(@as(f64, 2.5), fd.and_filter.right.double.value);

    // DATE/TIMESTAMP bounds accept the same SQL literals `>=`/`<=` do.
    const fdt = try parse(a, "d BETWEEN '2024-01-01' AND '2024-01-31'", &meta, null);
    try testing.expectEqual(@as(i32, 19723), fdt.and_filter.left.int32.value);
    try testing.expectEqual(@as(i32, 19753), fdt.and_filter.right.int32.value);
    const fts = try parse(a, "ts BETWEEN '2024-01-01' AND '2024-01-01 00:00:01'", &meta, null);
    try testing.expectEqual(@as(i64, 1_704_067_200_000_000), fts.and_filter.left.int64.value);
    try testing.expectEqual(@as(i64, 1_704_067_201_000_000), fts.and_filter.right.int64.value);
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
    const f = try parse(a, "score BETWEEN 0.5 AND 0.9 AND id=1", &meta, null);
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

    const f = try parse(a, "id between 1 and 5", &meta, null);
    try testing.expect(f == .and_filter);
}

test "parse unsigned literals: in-range compares as u64, out-of-range folds to a constant leaf" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    const root: schema.SchemaElement = .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = 2,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    try items.append(a, root);
    var u32_elem = root;
    u32_elem.type = .INT32;
    u32_elem.name = "u";
    u32_elem.num_children = null;
    u32_elem.converted_type = .UINT_32;
    try items.append(a, u32_elem);
    var u64_elem = u32_elem;
    u64_elem.type = .INT64;
    u64_elem.name = "w";
    u64_elem.converted_type = null;
    u64_elem.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    try items.append(a, u64_elem);
    const fm: schema.FileMetaData = .{
        .version = 1,
        .schema = items,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };

    const big = try parse(a, "u = 3000000000", &fm, null);
    try testing.expectEqual(@as(u64, 3_000_000_000), big.uint64.value);
    const top = try parse(a, "w >= 18446744073709551615", &fm, null);
    try testing.expectEqual(@as(u64, std.math.maxInt(u64)), top.uint64.value);
    try testing.expectEqual(@as(usize, 1), top.uint64.col_idx);

    // Below zero: `>`/`>=`/`!=` hold for every row, `=`/`<`/`<=` for none. Above 2^64-1 the reverse.
    const cases = [_]struct { expr: []const u8, holds: bool }{
        .{ .expr = "u > -1", .holds = true },
        .{ .expr = "u != -1", .holds = true },
        .{ .expr = "u = -1", .holds = false },
        .{ .expr = "u <= -1", .holds = false },
        .{ .expr = "w < 18446744073709551616", .holds = true },
        .{ .expr = "w = 18446744073709551616", .holds = false },
        .{ .expr = "w > 99999999999999999999999", .holds = false },
    };
    for (cases) |c| {
        const f = try parse(a, c.expr, &fm, null);
        try testing.expectEqual(@as(u64, 0), f.uint64.value);
        try testing.expectEqual(if (c.holds) ast.Operator.GtEq else ast.Operator.Lt, f.uint64.op);
    }
    try testing.expectError(error.BadValue, parse(a, "u = 1.5", &fm, null));
}

test "parse binds a bare name to the top-level column, not a nested leaf sharing it" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const meta = try test_fixtures.sharedLeafNameMeta(a);

    const top = try parse(a, "key > 1005", &meta, null);
    try testing.expect(top == .int64);
    try testing.expectEqual(@as(usize, 2), top.int64.col_idx);
    const nested = try parse(a, "r.key = 5", &meta, null);
    try testing.expectEqual(@as(usize, 0), nested.int64.col_idx);
    const quoted = try parse(a, "\"r.key\" BETWEEN 1 AND 9", &meta, null);
    try testing.expectEqual(@as(usize, 0), quoted.and_filter.left.int64.col_idx);
    const like = try parse(a, "r.name LIKE 'a%'", &meta, null);
    try testing.expectEqual(@as(usize, 1), like.like.col_idx);
    const in_list = try parse(a, "key IN (1005, 1006)", &meta, null);
    try testing.expectEqual(@as(usize, 2), in_list.or_filter.left.int64.col_idx);
    const nc = try parse(a, "key IS NOT NULL", &meta, null);
    try testing.expectEqual(@as(usize, 2), nc.null_check.col_idx);

    var diag: Diag = .{};
    try testing.expectError(error.AmbiguousColumn, parse(a, "x = 1", &meta, &diag));
    try testing.expectEqualStrings("x", diag.column.get());
    try testing.expect(diag.detail.ambiguous_column.parser == .filter);
    try testing.expectEqualStrings(metadata.ambiguityHint(&meta, "x"), diag.detail.ambiguous_column.hint);
    try testing.expectError(error.AmbiguousColumn, parse(a, "x IS NULL", &meta, null));
    try testing.expectEqual(@as(usize, 4), (try parse(a, "a.x = 1", &meta, null)).int32.col_idx);
}

test "parse binds a quoted path to the nested leaf a top-level column's dotted name hides" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try test_fixtures.sharedLeafNameMeta(a);
    meta.schema.items[4].name = "r.key"; // the top-level `key` becomes `r.key`, beside the group r's field key

    try testing.expectEqual(@as(usize, 2), (try parse(a, "r.key > 1005", &meta, null)).int64.col_idx);
    try testing.expectEqual(@as(usize, 2), (try parse(a, "\"r.key\" > 1005", &meta, null)).int64.col_idx);
    try testing.expectEqual(@as(usize, 0), (try parse(a, "\"r\".\"key\" > 1005", &meta, null)).int64.col_idx);
    try testing.expectEqual(@as(usize, 0), (try parse(a, "\"r\".\"key\" IS NULL", &meta, null)).null_check.col_idx);
    const in_list = try parse(a, "\"r\".\"key\" IN (1, 2)", &meta, null);
    try testing.expectEqual(@as(usize, 0), in_list.or_filter.left.int64.col_idx);
    try testing.expectEqual(@as(usize, 1), (try parse(a, "\"r\".\"name\" LIKE 'a%'", &meta, null)).like.col_idx);
}
