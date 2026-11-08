//! Filter expression parser.
//!
//! Grammar (keywords case-insensitive):
//!
//!   expr        := disjunction
//!   disjunction := conjunction ( OR conjunction )*
//!   conjunction := negation ( AND negation )*
//!   negation    := NOT negation | "(" disjunction ")" | leaf
//!   leaf        := identifier OP value | identifier [NOT] IN (v, ...) | identifier [NOT] BETWEEN x AND y
//!                | identifier [NOT] LIKE pattern | identifier IS [NOT] NULL
//!   OP          := "!=" | "<>" | "<=" | ">=" | "=" | "<" | ">"
//!
//! Structure is found by scanning, never by plain substring search: quoted spans ('...' and "...", a doubled quote
//! escaping itself) and parenthesized groups are stepped over, so a keyword or operator inside a string literal, a
//! quoted identifier or an IN list is never mistaken for one. A chain of any length parses in a loop; only grouping
//! parentheses recurse, and they nest at most `MAX_GROUP_DEPTH` deep, which bounds every recursive walk of the tree.

const std = @import("std");
const ast = @import("ast.zig");
const syntax = @import("../expr/syntax.zig");
const in_set = @import("in_set.zig");
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
    /// A parenthesis outside a quoted span without its partner.
    UnbalancedParentheses,
    /// Grouping parentheses nest deeper than `MAX_GROUP_DEPTH`.
    FilterTooDeep,
    /// A quote opens a string literal or quoted name that never closes.
    UnterminatedString,
    /// The column is repeated (in a LIST or MAP, or a legacy REPEATED field): a row holds a list of its values.
    NestedFilterNotSupported,
    /// `%`, an operator zpq does not have, where a column was expected (`x % 2 = 1`).
    ModuloNotSupported,
    /// Arithmetic where a predicate compares a column (`x + 1 > 5`): the left side must be a column.
    ExpressionInPredicate,
} || std.mem.Allocator.Error;

/// How deep grouping parentheses may nest. Each level adds at most an OR and an AND node, and evaluation clones the
/// row selection once per OR level, so this caps the tree height every walker (evaluation, pruning, negation,
/// membership) recurses through and the selections live at once.
pub const MAX_GROUP_DEPTH: u32 = 32;

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
    const trimmed = trimSpace(expr);
    if (trimmed.len == 0) return error.EmptyExpr;
    return try parseDisjunction(true, arena, trimmed, file, diag, 0);
}

fn FilterNode(comptime bound: bool) type {
    return if (bound) ast.Filter else syntax.Predicate;
}

fn joinSyntax(comptime bound: bool, arena: std.mem.Allocator, conjunction: bool, terms: []const FilterNode(bound)) Error!FilterNode(bound) {
    if (bound) return ast.join(arena, conjunction, terms);
    if (terms.len == 1) return terms[0];
    return if (conjunction) .{ .and_ = terms } else .{ .or_ = terms };
}

fn negateSyntax(arena: std.mem.Allocator, inner: syntax.Predicate) Error!syntax.Predicate {
    const out = try arena.create(syntax.Predicate);
    out.* = inner;
    return .{ .not_ = out };
}

fn parseOperandSyntax(text: []const u8) Error!syntax.Operand {
    const open = std.mem.indexOfScalar(u8, text, '(') orelse return .{ .column = text };
    if (!std.ascii.eqlIgnoreCase(trimSpace(text[0..open]), "make_date")) return error.BadOperator;
    const arg = (try groupBody(text[open..])) orelse return error.BadOperator;
    return .{ .column = arg, .make_date = true };
}

pub fn parseSyntax(arena: std.mem.Allocator, expr: []const u8) Error!syntax.Predicate {
    const trimmed = trimSpace(expr);
    if (trimmed.len == 0) return error.EmptyExpr;
    return parseDisjunction(false, arena, trimmed, undefined, null, 0);
}

pub fn bindSyntax(arena: std.mem.Allocator, node: syntax.Predicate, file: *const schema.FileMetaData, diag: ?*Diag) Error!ast.Filter {
    return bindSyntaxAt(arena, node, file, diag, 0);
}

fn bindSyntaxAt(arena: std.mem.Allocator, node: syntax.Predicate, file: *const schema.FileMetaData, diag: ?*Diag, depth: u32) Error!ast.Filter {
    // Each source grouping level can produce AND, OR and NOT wrappers.
    if (depth > MAX_GROUP_DEPTH * 3) return error.FilterTooDeep;
    return switch (node) {
        .and_, .or_ => |terms| blk: {
            if (terms.len == 0) return error.BadOperator;
            const children = try arena.alloc(ast.Filter, terms.len);
            for (terms, children) |term, *out| out.* = try bindSyntaxAt(arena, term, file, diag, depth + 1);
            break :blk ast.join(arena, node == .and_, children);
        },
        .not_ => |inner| negateFilter(arena, try bindSyntaxAt(arena, inner.*, file, diag, depth + 1)),
        .comparison => |c| blk: {
            const operand = try bindOperand(file, c.operand.column, c.operand.make_date, diag);
            break :blk if (isNullLiteral(c.value)) unknown else buildOperandComparison(arena, file, operand, c.op, c.value, diag);
        },
        .null_check => |n| .{ .null_check = .{ .col_idx = try resolveCol(file, n.column, diag), .is_not = n.is_not } },
        .like => |l| parseLike(true, arena, l.column, l.pattern, l.negate, file, diag),
        .boolean => |name| bareBoolean(true, arena, name, file, diag),
        .in_list => |list| blk: {
            if (list.values.len == 0) return error.BadValue;
            const operand = try bindOperand(file, list.operand.column, list.operand.make_date, diag);
            var leaves: std.ArrayList(ast.Filter) = .empty;
            var has_null = false;
            for (list.values) |value| {
                if (value.len == 0) return error.BadValue;
                if (isNullLiteral(value)) has_null = true else try leaves.append(arena, try buildOperandComparison(arena, file, operand, if (list.negate) .NotEq else .Eq, value, diag));
            }
            break :blk bindIn(arena, operand.col_idx, list.negate, leaves.items, has_null);
        },
        .between => |b| blk: {
            const operand = try bindOperand(file, b.operand.column, b.operand.make_date, diag);
            const inner = try ast.join(arena, true, &.{
                if (isNullLiteral(b.low)) unknown else try buildOperandComparison(arena, file, operand, .GtEq, b.low, diag),
                if (isNullLiteral(b.high)) unknown else try buildOperandComparison(arena, file, operand, .LtEq, b.high, diag),
            });
            break :blk if (b.negate) negateFilter(arena, inner) else inner;
        },
    };
}

fn trimSpace(s: []const u8) []const u8 {
    return std.mem.trim(u8, s, " \t\r\n");
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn isWordByte(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

/// The index just past the quoted span opening at `src[i]`. A doubled quote inside escapes itself, as SQL writes
/// `'O''Reilly'`. An unterminated quote is an error: read as an ordinary byte, it would let the rest of a mistyped
/// literal (`'O\'Reilly' AND ...`) split as structure.
fn skipQuoted(src: []const u8, i: usize) Error!usize {
    const q = src[i];
    var j = i + 1;
    while (j < src.len) : (j += 1) {
        if (src[j] != q) continue;
        if (j + 1 < src.len and src[j + 1] == q) {
            j += 1;
            continue;
        }
        return j + 1;
    }
    return error.UnterminatedString;
}

/// A word of the outermost level: letters, digits and `_`, with whitespace, a parenthesis or an end on both sides.
const Word = struct { start: usize, end: usize };

/// Walks the outermost level of `src`, stepping over quoted spans and parenthesized groups.
const Cursor = struct {
    src: []const u8,
    i: usize = 0,
    depth: u32 = 0,

    fn boundary(self: *const Cursor, at: usize) bool {
        return at >= self.src.len or isSpace(self.src[at]) or self.src[at] == '(' or self.src[at] == ')';
    }

    fn nextWord(self: *Cursor) Error!?Word {
        const src = self.src;
        while (self.i < src.len) {
            const c = src[self.i];
            switch (c) {
                '\'', '"' => self.i = try skipQuoted(src, self.i),
                '(' => {
                    self.depth += 1;
                    self.i += 1;
                },
                ')' => {
                    if (self.depth == 0) return error.UnbalancedParentheses;
                    self.depth -= 1;
                    self.i += 1;
                },
                else => {
                    const starts = self.depth == 0 and isWordByte(c) and (self.i == 0 or self.boundary(self.i - 1));
                    if (!starts) {
                        self.i += 1;
                        continue;
                    }
                    const start = self.i;
                    while (self.i < src.len and isWordByte(src[self.i])) self.i += 1;
                    if (self.boundary(self.i)) return .{ .start = start, .end = self.i };
                },
            }
        }
        if (self.depth != 0) return error.UnbalancedParentheses;
        return null;
    }

    /// The next outermost-level position of `needle`, compared case-insensitively.
    fn find(self: *Cursor, needle: []const u8) Error!?usize {
        const src = self.src;
        while (self.i < src.len) {
            const c = src[self.i];
            if (self.depth == 0 and self.i + needle.len <= src.len and
                std.ascii.eqlIgnoreCase(src[self.i .. self.i + needle.len], needle))
            {
                const at = self.i;
                self.i += needle.len;
                return at;
            }
            switch (c) {
                '\'', '"' => self.i = try skipQuoted(src, self.i),
                '(' => {
                    self.depth += 1;
                    self.i += 1;
                },
                ')' => {
                    if (self.depth == 0) return error.UnbalancedParentheses;
                    self.depth -= 1;
                    self.i += 1;
                },
                else => self.i += 1,
            }
        }
        return null;
    }
};

/// The first outermost-level position of `needle` in `src` (case-insensitive), or null.
fn topLevelIndexOf(src: []const u8, needle: []const u8) Error!?usize {
    var cur: Cursor = .{ .src = src };
    return cur.find(needle);
}

fn isKeyword(src: []const u8, w: Word, kw: []const u8) bool {
    return std.ascii.eqlIgnoreCase(src[w.start..w.end], kw);
}

/// Split at every outermost ` OR ` into one flat OR node: a loop, not recursion, so a chain of any length costs no
/// stack.
fn parseDisjunction(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!FilterNode(bound) {
    var terms: std.ArrayList(FilterNode(bound)) = .empty;
    var cur: Cursor = .{ .src = expr };
    var start: usize = 0;
    while (try cur.nextWord()) |w| {
        if (!isKeyword(expr, w, "OR")) continue;
        try terms.append(arena, try parseConjunction(bound, arena, trimSpace(expr[start..w.start]), file, diag, depth));
        start = w.end;
    }
    try terms.append(arena, try parseConjunction(bound, arena, trimSpace(expr[start..]), file, diag, depth));
    return joinSyntax(bound, arena, false, terms.items);
}

/// Split at every outermost ` AND ` that does not pair with a `BETWEEN`: the next AND after each BETWEEN belongs to
/// its bounds.
fn parseConjunction(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!FilterNode(bound) {
    var terms: std.ArrayList(FilterNode(bound)) = .empty;
    var cur: Cursor = .{ .src = expr };
    var start: usize = 0;
    var bounds_pending: usize = 0;
    while (try cur.nextWord()) |w| {
        if (isKeyword(expr, w, "BETWEEN")) {
            bounds_pending += 1;
        } else if (isKeyword(expr, w, "AND")) {
            if (bounds_pending > 0) {
                bounds_pending -= 1;
                continue;
            }
            try terms.append(arena, try parseNegation(bound, arena, trimSpace(expr[start..w.start]), file, diag, depth));
            start = w.end;
        }
    }
    try terms.append(arena, try parseNegation(bound, arena, trimSpace(expr[start..]), file, diag, depth));
    // A BETWEEN's two bounds join the chain they sit in.
    return joinSyntax(bound, arena, true, terms.items);
}

/// A run of leading NOTs, then a parenthesized group or a leaf. The run is counted, not recursed into: only its
/// parity matters, and `NOT NOT ... x` must not cost a stack frame per NOT. NOT is structural (operator flip on
/// leaves, De Morgan on composites), never a complement of the rows that passed, so a row whose operand is NULL
/// (UNKNOWN) fails both `p` and `NOT p`.
fn parseNegation(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!FilterNode(bound) {
    var negated = false;
    var body = expr;
    while (body.len > 3 and std.ascii.eqlIgnoreCase(body[0..3], "NOT") and (isSpace(body[3]) or body[3] == '(')) {
        body = trimSpace(body[3..]);
        negated = !negated;
    }
    if (body.len == 0) return error.BadOperator;
    const inner = if (try groupBody(body)) |group| blk: {
        if (depth + 1 > MAX_GROUP_DEPTH) return error.FilterTooDeep;
        break :blk try parseDisjunction(bound, arena, group, file, diag, depth + 1);
    } else try parseLeafBody(bound, arena, body, file, diag);
    return if (!negated) inner else if (bound) negateFilter(arena, inner) else negateSyntax(arena, inner);
}

/// The inside of `term` when the whole of it is one parenthesized group, else null.
fn groupBody(term: []const u8) Error!?[]const u8 {
    if (term[0] != '(') return null;
    var cur: Cursor = .{ .src = term, .i = 1, .depth = 1 };
    while (cur.i < term.len) {
        switch (term[cur.i]) {
            '\'', '"' => cur.i = try skipQuoted(term, cur.i),
            '(' => {
                cur.depth += 1;
                cur.i += 1;
            },
            ')' => {
                cur.depth -= 1;
                if (cur.depth == 0) {
                    if (cur.i + 1 != term.len) return null;
                    const inner = trimSpace(term[1..cur.i]);
                    if (inner.len == 0) return error.EmptyExpr;
                    return inner;
                }
                cur.i += 1;
            },
            else => cur.i += 1,
        }
    }
    return error.UnbalancedParentheses;
}

/// Resolve a (possibly double-quoted) column name to its leaf index. See
/// `metadata.resolveColumn` for how bare and dotted names bind.
/// A repeated column is refused here, before statistics could answer for it without the decode that refuses it.
pub fn resolveCol(file: *const schema.FileMetaData, name: []const u8, diag: ?*Diag) Error!usize {
    const ident = metadata.unquoteIdent(name);
    const ci = metadata.resolveColumn(file, ident) catch |err| {
        if (err == error.AmbiguousColumn) Diag.set(diag, ident, .{ .ambiguous_column = .{
            .parser = .filter,
            .hint = metadata.ambiguityHint(file, ident),
        } });
        // No column by that name: an operator outside quotes says what it is instead.
        if (err == error.UnknownColumn) if (try operatorOutsideQuotes(name)) |op| {
            Diag.setColumn(diag, name);
            return if (op == '%') error.ModuloNotSupported else error.ExpressionInPredicate;
        };
        return err;
    };
    var walk: schema.LeafIterator = undefined;
    if (file.leafAt(ci, &walk)) |leaf| if (leaf.max_rep > 0) {
        Diag.setColumn(diag, ident);
        return error.NestedFilterNotSupported;
    };
    return ci;
}

/// An arithmetic operator or blank in `name` outside a quoted span, which no unquoted column name holds: `%` if there
/// is one, else the first.
fn operatorOutsideQuotes(name: []const u8) Error!?u8 {
    var first: ?u8 = null;
    var i: usize = 0;
    while (i < name.len) : (i += 1) switch (name[i]) {
        '\'', '"' => i = try skipQuoted(name, i) - 1,
        '%' => return '%',
        '+', '-', '*', '/', ' ', '\t' => |c| if (first == null) {
            first = c;
        },
        else => {},
    };
    return first;
}

fn parseLeafBody(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!FilterNode(bound) {
    // `col IS NULL` / `col IS NOT NULL` — a def-level predicate, no value.
    // Checked before IN/BETWEEN/comparison since it has no binary operator.
    if (try tryParseNullCheck(bound, expr, file, diag)) |nc| return nc;

    // `col LIKE 'pat'` / `col NOT LIKE 'pat'` — SQL wildcard match.
    // " NOT LIKE " must be checked first (it contains " LIKE ").
    if (try topLevelIndexOf(expr, " NOT LIKE ")) |i| {
        return parseLike(bound, arena, expr[0..i], expr[i + " NOT LIKE ".len ..], true, file, diag);
    }
    if (try topLevelIndexOf(expr, " LIKE ")) |i| {
        return parseLike(bound, arena, expr[0..i], expr[i + " LIKE ".len ..], false, file, diag);
    }

    // `col [NOT] IN (v1, v2, ...)` → OR-of-equalities (or AND-of-inequalities
    // for NOT IN), reusing the typed leaf builder per value.
    if (try findInClause(expr)) |inc| {
        return parseInList(bound, arena, expr, inc, file, diag);
    }
    // BETWEEN runs before operator detection — `col BETWEEN x AND y` has no
    // binary op the comparison-finder would recognize. `col NOT BETWEEN x AND y` is its negation.
    if (try topLevelIndexOf(expr, " NOT BETWEEN ")) |i| {
        const inner = try parseBetween(bound, arena, expr, i, " NOT BETWEEN ".len, file, diag);
        return if (bound) negateFilter(arena, inner) else negateSyntax(arena, inner);
    }
    if (try topLevelIndexOf(expr, " BETWEEN ")) |i| {
        return parseBetween(bound, arena, expr, i, " BETWEEN ".len, file, diag);
    }
    return parseComparisonLeaf(bound, arena, expr, file, diag);
}

/// Recognise a trailing ` IS NULL` / ` IS NOT NULL` (case-insensitive).
/// Returns null when the expression isn't a null check (so the caller
/// falls through to the comparison path). A lone ` IS ` that isn't one
/// of the two valid forms is a clear error, not a silent fallthrough.
fn tryParseNullCheck(comptime bound: bool, expr: []const u8, file: *const schema.FileMetaData, diag: ?*Diag) Error!?FilterNode(bound) {
    const not_null = " IS NOT NULL";
    const null_ = " IS NULL";
    var col_part: []const u8 = undefined;
    var is_not: bool = undefined;
    if (expr.len >= not_null.len and std.ascii.eqlIgnoreCase(expr[expr.len - not_null.len ..], not_null)) {
        col_part = expr[0 .. expr.len - not_null.len];
        is_not = true;
    } else if (expr.len >= null_.len and std.ascii.eqlIgnoreCase(expr[expr.len - null_.len ..], null_)) {
        col_part = expr[0 .. expr.len - null_.len];
        is_not = false;
    } else if (try topLevelIndexOf(expr, " IS ")) |_| {
        if (diag) |d| d.detail = .is_operand;
        return error.BadOperator;
    } else {
        return null;
    }
    const col_name = trimSpace(col_part);
    if (col_name.len == 0) return error.BadOperator;
    if (!bound) return .{ .null_check = .{ .column = col_name, .is_not = is_not } };
    const col_idx = try resolveCol(file, col_name, diag);
    return ast.Filter{ .null_check = .{ .col_idx = col_idx, .is_not = is_not } };
}

/// Build a `LIKE` / `NOT LIKE` leaf. The pattern is classified once
/// (DuckDB's trick) so prefix/suffix/contains/exact patterns dodge the
/// general matcher. LIKE is text-only — non-string columns are a clear
/// error rather than a confusing type mismatch later.
fn parseLike(
    comptime bound: bool,
    arena: std.mem.Allocator,
    col_part: []const u8,
    pat_part: []const u8,
    negate: bool,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!FilterNode(bound) {
    const col_name = trimSpace(col_part);
    if (col_name.len == 0) return error.BadOperator;
    if (!bound) return .{ .like = .{ .column = col_name, .pattern = trimSpace(pat_part), .negate = negate } };
    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    if (elem.type != .BYTE_ARRAY) {
        const type_name = @tagName(elem.type orelse .BYTE_ARRAY);
        Diag.set(diag, col_name, .{ .like_non_string = .{ .type_name = type_name } });
        return error.UnsupportedType;
    }
    // `s LIKE NULL` is UNKNOWN for every row, as `s = NULL` is: the bare word is the NULL literal, not the text 'NULL'.
    if (isNullLiteral(trimSpace(pat_part))) return unknown;
    const pattern = try stringValue(arena, file, col_name, trimSpace(pat_part), diag);
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

fn findInClause(expr: []const u8) Error!?InClause {
    if (try topLevelIndexOf(expr, " NOT IN ")) |i|
        return .{ .col_end = i, .vals_start = i + " NOT IN ".len, .negated = true };
    if (try topLevelIndexOf(expr, " IN ")) |i|
        return .{ .col_end = i, .vals_start = i + " IN ".len, .negated = false };
    return null;
}

fn parseInList(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    inc: InClause,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!FilterNode(bound) {
    const col_name = trimSpace(expr[0..inc.col_end]);
    const list = trimSpace(expr[inc.vals_start..]);
    if (col_name.len == 0 or list.len == 0) return error.BadOperator;
    // A parenthesized list, split at its outermost commas: a comma inside a quoted value is part of the value.
    const vals = (try groupBody(list)) orelse return error.BadOperator;

    const operand = if (bound) try resolveOperand(file, col_name, diag) else try parseOperandSyntax(col_name);
    const Item = if (bound) ast.Filter else []const u8;
    var items: std.ArrayList(Item) = .empty;
    var null_member = false;
    var cur: Cursor = .{ .src = vals };
    var start: usize = 0;
    while (true) {
        const comma = try cur.find(",");
        const value = trimSpace(vals[start .. comma orelse vals.len]);
        if (value.len == 0) return error.BadValue;
        if (bound) {
            if (isNullLiteral(value)) null_member = true else {
                const op: ast.Operator = if (inc.negated) .NotEq else .Eq;
                try items.append(arena, try buildOperandComparison(arena, file, operand, op, value, diag));
            }
        } else try items.append(arena, value);
        start = (comma orelse break) + 1;
    }
    if (bound) return bindIn(arena, operand.col_idx, inc.negated, items.items, null_member);
    return .{ .in_list = .{ .operand = operand, .values = items.items, .negate = inc.negated } };
}

fn isNullLiteral(v: []const u8) bool {
    return std.ascii.eqlIgnoreCase(v, "NULL");
}

/// `x op NULL`: UNKNOWN for every row, which neither it nor its negation passes.
pub const unknown: ast.Filter = .{ .or_filter = .{ .children = &.{}, .null_member = true } };

/// The literals of an IN list bound for one-pass evaluation; null when its leaves are of a type the set doesn't hold
/// (boolean) or one of them isn't a plain `=` (`!=` for NOT IN) or a constant, which then evaluate leaf by leaf.
pub fn bindIn(arena: std.mem.Allocator, col_idx: usize, negated: bool, leaves: []const ast.Filter, null_member: bool) Error!ast.Filter {
    // One node for the whole list, however long: OR of `= v` (AND of `!= v`), with its set bound beside the leaves.
    if (leaves.len == 1 and !null_member) return leaves[0];
    const c: ast.Composite = .{
        .children = leaves,
        .in_set = if (leaves.len > 1) try bindInSet(arena, col_idx, negated, leaves) else null,
        .null_member = null_member,
    };
    return if (negated) .{ .and_filter = c } else .{ .or_filter = c };
}

pub fn bindInSet(
    arena: std.mem.Allocator,
    col_idx: usize,
    negated: bool,
    leaves: []const ast.Filter,
) Error!?*const in_set.InSet {
    const want: ast.Operator = if (negated) .NotEq else .Eq;
    const probe: in_set.InSet.Probe = switch (leaves[0]) {
        inline .int32, .int64, .uint64, .float, .double, .string => |_, tag| blk: {
            const T = @FieldType(@FieldType(ast.Filter, @tagName(tag)), "value");
            var values: std.ArrayList(T) = .empty;
            for (leaves) |l| {
                if (std.meta.activeTag(l) != tag) return null;
                const leaf = @field(l, @tagName(tag));
                if (leaf.op == want) {
                    try values.append(arena, leaf.value);
                    continue;
                }
                // A literal no stored value can equal binds to a constant leaf: one that matches nothing adds no
                // member to IN, one that matches every present row adds none to NOT IN. Anything else stays leaves.
                const constant = constantLeaf(T, leaf) orelse return null;
                if (constant != negated) return null;
            }
            if (values.items.len == 0) return null;
            break :blk switch (tag) {
                .int32 => .{ .i32 = try .init(arena, values.items) },
                .int64 => .{ .i64 = try .init(arena, values.items) },
                .uint64 => .{ .u64 = try .init(arena, values.items) },
                .float => .{ .f32 = try .init(arena, values.items) },
                .double => .{ .f64 = try .init(arena, values.items) },
                .string => .{ .string = try .init(arena, values.items) },
                else => unreachable,
            };
        },
        else => return null,
    };
    const set = try arena.create(in_set.InSet);
    set.* = .{ .col_idx = col_idx, .negated = negated, .probe = probe };
    return set;
}

/// What the binder's constant leaves always answer for a present row: true for `exactLeaf`'s `<= maxInt` and
/// `buildUnsignedLeaf`'s `>= 0`, false for `> maxInt` and `< 0`; null for any other leaf.
fn constantLeaf(comptime T: type, leaf: ast.Leaf(T)) ?bool {
    switch (@typeInfo(T)) {
        .int => |info| if (info.signedness == .unsigned) {
            if (leaf.value != 0) return null;
            return switch (leaf.op) {
                .GtEq => true,
                .Lt => false,
                else => null,
            };
        } else {
            if (leaf.value != std.math.maxInt(T)) return null;
            return switch (leaf.op) {
                .LtEq => true,
                .Gt => false,
                else => null,
            };
        },
        else => return null,
    }
}

/// Negate a filter subtree: flip leaf operators, De Morgan on composites.
/// NULL handling matches zpq's "nulls don't pass" convention either way
/// (both `NOT (x > 5)` and `x <= 5` exclude null rows).
pub fn negateFilter(arena: std.mem.Allocator, f: ast.Filter) Error!ast.Filter {
    return switch (f) {
        // Negating keeps the value, so a stored literal stays one (`NOT x != 5` is `x = 5`).
        inline .int32, .int64, .uint64, .float, .double, .decimal, .string, .boolean => |l, tag| blk: {
            var out = l;
            out.op = l.op.negate();
            break :blk @unionInit(ast.Filter, @tagName(tag), out);
        },
        // NOT (x IS NULL) == x IS NOT NULL, and vice-versa.
        .null_check => |nc| .{ .null_check = .{ .col_idx = nc.col_idx, .is_not = !nc.is_not } },
        // NOT (x LIKE p) == x NOT LIKE p — flip the negate flag.
        .like => |m| .{ .like = .{ .col_idx = m.col_idx, .kind = m.kind, .operand = m.operand, .negate = !m.negate } },
        // De Morgan, a node at a time: the tree is at most a few levels deep (see `ast.Composite`).
        .and_filter, .or_filter => |c| blk: {
            const children = try arena.alloc(ast.Filter, c.children.len);
            for (c.children, children) |child, *n| n.* = try negateFilter(arena, child);
            const flipped: ast.Composite = .{
                .children = children,
                .in_set = if (c.in_set) |s| try s.negate(arena) else null,
                .null_member = c.null_member,
            };
            break :blk if (f == .and_filter) .{ .or_filter = flipped } else .{ .and_filter = flipped };
        },
    };
}

/// Parse `col BETWEEN x AND y` (the keyword at `between_at`, `kw_len` long) into the conjunction
/// `(col >= x AND col <= y)`. SQL semantics: BETWEEN is inclusive on both sides.
fn parseBetween(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    between_at: usize,
    kw_len: usize,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!FilterNode(bound) {
    const after_between = between_at + kw_len;
    const and_at = (try topLevelIndexOf(expr[after_between..], " AND ")) orelse return error.BadOperator;

    const col_name = trimSpace(expr[0..between_at]);
    const x_str = trimSpace(expr[after_between .. after_between + and_at]);
    const y_str = trimSpace(expr[after_between + and_at + " AND ".len ..]);
    if (col_name.len == 0 or x_str.len == 0 or y_str.len == 0) return error.BadOperator;

    if (!bound) return .{ .between = .{ .operand = try parseOperandSyntax(col_name), .low = x_str, .high = y_str, .negate = false } };
    const operand = try resolveOperand(file, col_name, diag);

    // Bounds bind exactly like `>=`/`<=` would. Binding on the bare physical type instead turned DECIMAL(FLBA)
    // bounds into byte-string leaves (silently wrong) and rejected DATE/TIMESTAMP literals. A NULL bound is the
    // UNKNOWN `x >= NULL` is, so `x NOT BETWEEN NULL AND 5` still keeps the rows past 5.
    return ast.join(arena, true, &.{
        if (isNullLiteral(x_str)) unknown else try buildOperandComparison(arena, file, operand, .GtEq, x_str, diag),
        if (isNullLiteral(y_str)) unknown else try buildOperandComparison(arena, file, operand, .LtEq, y_str, diag),
    });
}

fn parseComparisonLeaf(
    comptime bound: bool,
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!FilterNode(bound) {
    const found = (try findComparison(expr)) orelse return bareBoolean(bound, arena, expr, file, diag);
    const op = found.op;
    const found_at = found.at;
    const found_len = found.len;

    const col_name = trimSpace(expr[0..found_at]);
    const val_str = trimSpace(expr[found_at + found_len ..]);
    if (col_name.len == 0 or val_str.len == 0) return error.BadOperator;
    if (!bound) return .{ .comparison = .{ .operand = try parseOperandSyntax(col_name), .op = op, .value = val_str } };
    if (isNullLiteral(val_str)) {
        _ = try resolveOperand(file, col_name, diag);
        return unknown;
    }
    return buildOperandComparison(arena, file, try resolveOperand(file, col_name, diag), op, val_str, diag);
}

/// A comparison's column side: a column, or `make_date(c)` over an integer column. That is the DATE its integer
/// counts, the same number, so the leaf compares (and statistics prune) the stored days themselves.
pub const Operand = struct { col_idx: usize, elem: schema.SchemaElement, make_date: bool };

pub fn resolveOperand(file: *const schema.FileMetaData, text: []const u8, diag: ?*Diag) Error!Operand {
    const operand = try parseOperandSyntax(text);
    return bindOperand(file, operand.column, operand.make_date, diag);
}

/// Bind a structured column operand through the same repeated/type guards as fragment predicates.
pub fn bindOperand(file: *const schema.FileMetaData, name: []const u8, make_date: bool, diag: ?*Diag) Error!Operand {
    const col_idx = try resolveCol(file, name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    if (make_date and !integerDays(&elem)) {
        Diag.setColumn(diag, metadata.unquoteIdent(name));
        return error.UnsupportedType;
    }
    return .{ .col_idx = col_idx, .elem = elem, .make_date = make_date };
}

/// An INT32 column DuckDB's `make_date(INTEGER)` takes: plain, or an integer annotation it casts to INTEGER (signed
/// up to 32 bits, unsigned up to 16), never a DATE, TIME or DECIMAL.
pub fn integerDays(elem: *const schema.SchemaElement) bool {
    if (elem.type != .INT32) return false;
    const lt = elem.effectiveLogicalType() orelse return true;
    return lt == .INTEGER and lt.INTEGER.bitWidth <= @as(i8, if (lt.INTEGER.isSigned) 32 else 16);
}

pub fn buildOperandComparison(
    arena: std.mem.Allocator,
    file: *const schema.FileMetaData,
    operand: Operand,
    op: ast.Operator,
    val_str: []const u8,
    diag: ?*Diag,
) Error!ast.Filter {
    if (!operand.make_date) return buildTypedComparison(arena, file, operand.col_idx, &operand.elem, op, val_str, diag);
    // DuckDB casts the literal to the DATE's type, so a time of day is dropped, not compared: `make_date(x) =
    // '2013-07-01 12:00'` holds on that day.
    const lit = try parseDateTimeNs(stripStringQuotes(val_str));
    const lane: Lane = if (schema.isUnsignedInt(operand.elem)) .uint64 else .int32;
    return markStored(bindExact(operand.col_idx, op, lane, @divFloor(lit.ns, ns_per_day), false));
}

/// A leaf that is a column's name alone (`b`, `"b"`, `t.b`): SQL's bare BOOLEAN predicate, the same leaf as `b = true`
/// (a NULL is not true, and under NOT stays out too). A column of another type is no predicate.
fn bareBoolean(comptime bound: bool, arena: std.mem.Allocator, expr: []const u8, file: *const schema.FileMetaData, diag: ?*Diag) Error!FilterNode(bound) {
    if (expr.len == 0) return error.BadOperator;
    const lone = if (expr[0] == '"')
        try skipQuoted(expr, 0) == expr.len
    else for (expr) |c| {
        if (!isWordByte(c) and c != '.') break false;
    } else true;
    if (!lone or isNullLiteral(expr) or std.ascii.eqlIgnoreCase(expr, "true") or std.ascii.eqlIgnoreCase(expr, "false"))
        return error.BadOperator;
    if (!bound) return .{ .boolean = expr };
    const col_idx = try resolveCol(file, expr, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    if (elem.type != .BOOLEAN) {
        Diag.set(diag, metadata.unquoteIdent(expr), .{ .bare_non_boolean = .{ .type_name = @tagName(elem.type orelse .BOOLEAN) } });
        return error.BadOperator;
    }
    return buildTypedComparison(arena, file, col_idx, &elem, .Eq, "true", diag);
}

const Comparison = struct { op: ast.Operator, at: usize, len: usize };

/// The first outermost-level comparison operator in `expr`, the two-byte ones taking precedence at a position.
fn findComparison(expr: []const u8) Error!?Comparison {
    var i: usize = 0;
    var depth: u32 = 0;
    while (i < expr.len) {
        const c = expr[i];
        switch (c) {
            '\'', '"' => {
                i = try skipQuoted(expr, i);
                continue;
            },
            '(' => depth += 1,
            ')' => {
                if (depth == 0) return error.UnbalancedParentheses;
                depth -= 1;
            },
            '!', '<', '>', '=' => if (depth == 0) {
                const next: u8 = if (i + 1 < expr.len) expr[i + 1] else 0;
                const two: ?ast.Operator = switch (c) {
                    '!' => if (next == '=') .NotEq else null,
                    '<' => if (next == '=') .LtEq else if (next == '>') .NotEq else null,
                    '>' => if (next == '=') .GtEq else null,
                    else => null,
                };
                if (two) |op| return .{ .op = op, .at = i, .len = 2 };
                const one: ?ast.Operator = switch (c) {
                    '=' => .Eq,
                    '<' => .Lt,
                    '>' => .Gt,
                    else => null,
                };
                if (one) |op| return .{ .op = op, .at = i, .len = 1 };
            },
            else => {},
        }
        i += 1;
    }
    return null;
}

/// Build a single typed comparison leaf, honoring DECIMAL columns (the
/// exact unscaled lane) and DATE/TIMESTAMP columns (SQL-style literals
/// → stored epoch ints). Shared by the comparison path and the IN-list
/// expansion so both get identical type handling.
fn buildTypedComparison(
    arena: std.mem.Allocator,
    file: *const schema.FileMetaData,
    col_idx: usize,
    elem: *const schema.SchemaElement,
    op: ast.Operator,
    val_str: []const u8,
    diag: ?*Diag,
) Error!ast.Filter {
    // Every leaf that compares a stored value is marked `stored` here and nowhere else, so what membership pruning
    // hashes (filter/membership.zig) can never drift from what evaluation compares.
    // Exact epoch nanoseconds (`decimal.exactKind`), so a literal before 1677 or after 2262 compares as written.
    if (elem.type == .INT96) return .{ .decimal = try int96Leaf(col_idx, op, val_str) };
    if (decimal_mod.kindFromSchema(elem)) |k| {
        // Compared on the unscaled integers: through f64, values past 2^53 matched their neighbours.
        const lit = try scaledLiteral(val_str, k.scale);
        return .{ .decimal = ast.exactLeaf(i128, col_idx, op, lit.floor, lit.inexact) };
    }
    if (schema.isUnsignedInt(elem.*)) return markStored(try buildUnsignedLeaf(col_idx, op, val_str));
    if (temporalKind(elem)) |tk| {
        const leaf = try buildTemporalLeaf(col_idx, op, val_str, tk);
        // INT96 decodes to nanoseconds; every other temporal lane holds the stored ticks.
        return if (elem.type == .INT96) leaf else markStored(leaf);
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
    // A UUID is stored as its 16 bytes; the literal is its text. Compared as text, it never matched.
    if (elem.logical_type != null and elem.logical_type.? == .UUID and elem.type == .FIXED_LEN_BYTE_ARRAY) {
        const bytes = try arena.create([16]u8);
        bytes.* = parseUuid(stripStringQuotes(val_str)) orelse return error.BadValue;
        return markStored(.{ .string = .{ .col_idx = col_idx, .op = op, .value = bytes } });
    }
    const ptype = elem.type orelse return error.UnsupportedType;
    if (ptype == .BYTE_ARRAY or ptype == .FIXED_LEN_BYTE_ARRAY) {
        const value = try stringValue(arena, file, elem.name, val_str, diag);
        return markStored(.{ .string = .{ .col_idx = col_idx, .op = op, .value = value } });
    }
    return markStored(try buildLeafFilter(col_idx, op, val_str, ptype));
}

/// The 16 bytes of a UUID written as 32 hex digits, either bare or hyphenated 8-4-4-4-12.
fn parseUuid(text: []const u8) ?[16]u8 {
    var hex: [32]u8 = undefined;
    switch (text.len) {
        32 => @memcpy(&hex, text),
        36 => {
            var n: usize = 0;
            for (text, 0..) |c, i| {
                const dash = i == 8 or i == 13 or i == 18 or i == 23;
                if (dash != (c == '-')) return null;
                if (dash) continue;
                hex[n] = c;
                n += 1;
            }
        },
        else => return null,
    }
    var out: [16]u8 = undefined;
    _ = std.fmt.hexToBytes(&out, &hex) catch return null;
    return out;
}

fn markStored(f: ast.Filter) ast.Filter {
    var out = f;
    switch (out) {
        inline .int32, .int64, .uint64, .float, .double, .string, .boolean => |*l| l.stored = true,
        else => {},
    }
    return out;
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
/// `.uint64` is the lane an unsigned column's leaves compare in (`buildUnsignedLeaf`).
const Lane = enum { int32, int64, uint64 };

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
    // A DATE takes the literal as a DATE, as DuckDB casts an untyped string to the column's type: its time of day is
    // dropped, not compared (`d = '2024-01-01 12:00:00'` holds on that day), as for `make_date`.
    if (tk.ns_per_tick == ns_per_day) return bindExact(col_idx, op, tk.lane, @divFloor(lit.ns, ns_per_day), false);
    const inexact = lit.sub_ns or @mod(lit.ns, tk.ns_per_tick) != 0;
    return bindExact(col_idx, op, tk.lane, @divFloor(lit.ns, tk.ns_per_tick), inexact);
}

/// An INT96 leaf: a bare integer is nanoseconds since the epoch, as the column reads; anything else a date-time.
fn int96Leaf(col_idx: usize, op: ast.Operator, val_str_raw: []const u8) Error!ast.Leaf(i128) {
    const val_str = stripStringQuotes(val_str_raw);
    if (std.fmt.parseInt(i128, val_str, 10)) |ns| return ast.exactLeaf(i128, col_idx, op, ns, false) else |_| {}
    const lit = try parseDateTimeNs(val_str);
    return ast.exactLeaf(i128, col_idx, op, lit.ns, lit.sub_ns);
}

fn bindExact(col_idx: usize, op: ast.Operator, lane: Lane, floor: i128, inexact: bool) ast.Filter {
    return switch (lane) {
        .int32 => .{ .int32 = ast.exactLeaf(i32, col_idx, op, floor, inexact) },
        .int64 => .{ .int64 = ast.exactLeaf(i64, col_idx, op, floor, inexact) },
        .uint64 => .{ .uint64 = ast.exactLeaf(u64, col_idx, op, floor, inexact) },
    };
}

/// A decimal literal (`-12.50`, `7`, `1.5e3`) times 10^`scale`: its floor and whether that dropped a fraction. A
/// magnitude no DECIMAL holds (past 10^40) stands as ±10^40, which compares with every stored value as it would.
fn scaledLiteral(text: []const u8, scale: i32) Error!struct { floor: i256, inexact: bool } {
    var s = text;
    const neg = s.len > 0 and s[0] == '-';
    if (s.len > 0 and (s[0] == '-' or s[0] == '+')) s = s[1..];
    var exp: i64 = 0;
    if (std.mem.indexOfAny(u8, s, "eE")) |e| {
        exp = std.fmt.parseInt(i32, s[e + 1 ..], 10) catch return error.BadValue;
        s = s[0..e];
    }
    const dot = std.mem.indexOfScalar(u8, s, '.');
    const int_part = if (dot) |d| s[0..d] else s;
    const frac_part = if (dot) |d| s[d + 1 ..] else "";
    var mant: i256 = 0;
    var digits: i64 = 0;
    var any_digit = false;
    var frac_digits: i64 = 0;
    for ([_][]const u8{ int_part, frac_part }, 0..) |part, pi| for (part, 0..) |c, i| {
        // A `_` between digits separates them, as the integer and float lanes read it.
        if (c == '_' and i > 0 and i + 1 < part.len and
            std.ascii.isDigit(part[i - 1]) and std.ascii.isDigit(part[i + 1])) continue;
        if (c < '0' or c > '9') return error.BadValue;
        any_digit = true;
        frac_digits += @intCast(pi);
        if (mant == 0 and c == '0') continue;
        digits += 1;
        if (digits > 76) return error.BadValue;
        mant = mant * 10 + (c - '0');
    };
    if (!any_digit) return error.BadValue;
    if (mant == 0) return .{ .floor = 0, .inexact = false };
    const shift = exp + scale - frac_digits;
    const huge = pow10Wide(40);
    var q: i256 = undefined;
    var r: i256 = 0;
    if (shift >= 0) {
        q = if (digits + shift > 40) huge else mant * pow10Wide(@intCast(shift));
    } else if (-shift > digits) {
        q = 0;
        r = mant;
    } else {
        const p = pow10Wide(@intCast(-shift));
        q = @divTrunc(mant, p);
        r = @rem(mant, p);
    }
    return .{ .floor = if (neg) -q - @intFromBool(r != 0) else q, .inexact = r != 0 };
}

fn pow10Wide(n: u32) i256 {
    var p: i256 = 1;
    for (0..n) |_| p *= 10;
    return p;
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
        .FLOAT => .{ .float = try floatLeaf(col_idx, op, val_str) },
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
        .BOOLEAN => .{
            .boolean = .{
                .col_idx = col_idx,
                .op = op,
                // Any letter case: SQL prints the literal as TRUE.
                .value = if (std.ascii.eqlIgnoreCase(val_str, "true") or std.mem.eql(u8, val_str, "1"))
                    true
                else if (std.ascii.eqlIgnoreCase(val_str, "false") or std.mem.eql(u8, val_str, "0"))
                    false
                else
                    return error.BadValue,
            },
        },
        .INT96 => return error.UnsupportedType,
    };
}

/// A FLOAT literal binds as the f32 it rounds to, the cast SQL makes of it, unless it is finite and past f32's range.
/// Rounded, it became an infinity: `f > 3.5e38` missed a stored +inf and `f = 3.5e38` matched it. Such a literal lies
/// strictly between every finite f32 and the infinity on its side, so each comparison with it is one with a bound
/// f32 has: past the top, `f < v` is `f <= floatMax` and `f > v` is `f > floatMax` (+inf and NaN, above every
/// number); `=` matches nothing (`f < -inf`) and `!=` every present row (`f >= -inf`). Each negates into its
/// complement, so NOT stays exact.
fn floatLeaf(col_idx: usize, op: ast.Operator, val_str: []const u8) Error!ast.Leaf(f32) {
    const rounded = std.fmt.parseFloat(f32, val_str) catch return error.BadValue;
    const wide = std.fmt.parseFloat(f64, val_str) catch return error.BadValue;
    if (!std.math.isInf(rounded) or std.math.isInf(wide)) return .{ .col_idx = col_idx, .op = op, .value = rounded };
    const top = std.math.floatMax(f32);
    const inf = std.math.inf(f32);
    const bound: struct { op: ast.Operator, value: f32 } = if (wide > 0) switch (op) {
        .Eq => .{ .op = .Lt, .value = -inf },
        .NotEq => .{ .op = .GtEq, .value = -inf },
        .Lt, .LtEq => .{ .op = .LtEq, .value = top },
        .Gt, .GtEq => .{ .op = .Gt, .value = top },
    } else switch (op) {
        .Eq => .{ .op = .Lt, .value = -inf },
        .NotEq => .{ .op = .GtEq, .value = -inf },
        .Lt, .LtEq => .{ .op = .Lt, .value = -top },
        .Gt, .GtEq => .{ .op = .GtEq, .value = -top },
    };
    return .{ .col_idx = col_idx, .op = bound.op, .value = bound.value };
}

/// Strip one matching pair of surrounding quotes from a string literal so
/// the SQL-style `name = 'row42'` compares against `row42`, not `'row42'`.
/// Bare/unquoted literals pass through unchanged (back-compat with the
/// original syntax). Without this, quoted string filters silently matched
/// nothing — found by the duckdb cross-impl smoke, 2026-06-13.
/// A string literal's value: `stripStringQuotes`, with a doubled quote inside a quoted literal read as one quote, as
/// SQL writes `'it''s'`. Borrows `s` unless there is a doubled quote to undo.
fn unquoteLiteral(arena: std.mem.Allocator, s: []const u8) Error![]const u8 {
    const inner = stripStringQuotes(s);
    if (inner.len == s.len) return s;
    const q = s[0];
    const doubled = [2]u8{ q, q };
    if (std.mem.indexOf(u8, inner, &doubled) == null) return inner;
    const out = try arena.alloc(u8, inner.len);
    var n: usize = 0;
    var i: usize = 0;
    while (i < inner.len) : (i += 1) {
        out[n] = inner[i];
        n += 1;
        if (inner[i] == q and i + 1 < inner.len and inner[i + 1] == q) i += 1;
    }
    return out[0..n];
}

/// The text a string column `col` is compared with: one quoted literal or, the flag form's back-compat, one bare
/// word. Anything else (`'a' || 'b'`, `upper('a')`, `'a%' ESCAPE '!'`) is an expression the grammar cannot evaluate,
/// and a word naming a column (`s = t`, `s = "t"`) is a column comparison: read as text, each matched incorrect rows.
fn stringValue(
    arena: std.mem.Allocator,
    file: *const schema.FileMetaData,
    col: []const u8,
    val_str: []const u8,
    diag: ?*Diag,
) Error![]const u8 {
    const quoted = val_str[0] == '\'' or val_str[0] == '"';
    const one_token = if (quoted)
        try skipQuoted(val_str, 0) == val_str.len
    else for (val_str) |c| {
        if (isSpace(c) or c == '(' or c == ')' or c == '\'' or c == '"' or c == '|') break false;
    } else true;
    if (!one_token or (val_str[0] != '\'' and namesColumn(file, val_str))) {
        Diag.set(diag, col, .non_literal_value);
        return error.BadValue;
    }
    return unquoteLiteral(arena, val_str);
}

fn namesColumn(file: *const schema.FileMetaData, word: []const u8) bool {
    _ = metadata.resolveColumn(file, metadata.unquoteIdent(word)) catch |err| return err == error.AmbiguousColumn;
    return true;
}

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
