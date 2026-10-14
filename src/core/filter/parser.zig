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
    return try parseDisjunction(arena, trimmed, file, diag, 0);
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
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!ast.Filter {
    var terms: std.ArrayList(ast.Filter) = .empty;
    var cur: Cursor = .{ .src = expr };
    var start: usize = 0;
    while (try cur.nextWord()) |w| {
        if (!isKeyword(expr, w, "OR")) continue;
        try terms.append(arena, try parseConjunction(arena, trimSpace(expr[start..w.start]), file, diag, depth));
        start = w.end;
    }
    try terms.append(arena, try parseConjunction(arena, trimSpace(expr[start..]), file, diag, depth));
    return ast.join(arena, false, terms.items);
}

/// Split at every outermost ` AND ` that does not pair with a `BETWEEN`: the next AND after each BETWEEN belongs to
/// its bounds.
fn parseConjunction(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!ast.Filter {
    var terms: std.ArrayList(ast.Filter) = .empty;
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
            try terms.append(arena, try parseNegation(arena, trimSpace(expr[start..w.start]), file, diag, depth));
            start = w.end;
        }
    }
    try terms.append(arena, try parseNegation(arena, trimSpace(expr[start..]), file, diag, depth));
    // A BETWEEN's two bounds join the chain they sit in.
    return ast.join(arena, true, terms.items);
}

/// A run of leading NOTs, then a parenthesized group or a leaf. The run is counted, not recursed into: only its
/// parity matters, and `NOT NOT ... x` must not cost a stack frame per NOT. NOT is structural (operator flip on
/// leaves, De Morgan on composites), never a complement of the rows that passed, so a row whose operand is NULL
/// (UNKNOWN) fails both `p` and `NOT p`.
fn parseNegation(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    depth: u32,
) Error!ast.Filter {
    var negated = false;
    var body = expr;
    while (body.len > 3 and std.ascii.eqlIgnoreCase(body[0..3], "NOT") and (isSpace(body[3]) or body[3] == '(')) {
        body = trimSpace(body[3..]);
        negated = !negated;
    }
    if (body.len == 0) return error.BadOperator;
    const inner = if (try groupBody(body)) |group| blk: {
        if (depth + 1 > MAX_GROUP_DEPTH) return error.FilterTooDeep;
        break :blk try parseDisjunction(arena, group, file, diag, depth + 1);
    } else try parseLeafBody(arena, body, file, diag);
    return if (negated) negateFilter(arena, inner) else inner;
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

fn parseLeafBody(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    // `col IS NULL` / `col IS NOT NULL` — a def-level predicate, no value.
    // Checked before IN/BETWEEN/comparison since it has no binary operator.
    if (try tryParseNullCheck(expr, file, diag)) |nc| return nc;

    // `col LIKE 'pat'` / `col NOT LIKE 'pat'` — SQL wildcard match.
    // " NOT LIKE " must be checked first (it contains " LIKE ").
    if (try topLevelIndexOf(expr, " NOT LIKE ")) |i| {
        return parseLike(arena, expr[0..i], expr[i + " NOT LIKE ".len ..], true, file, diag);
    }
    if (try topLevelIndexOf(expr, " LIKE ")) |i| {
        return parseLike(arena, expr[0..i], expr[i + " LIKE ".len ..], false, file, diag);
    }

    // `col [NOT] IN (v1, v2, ...)` → OR-of-equalities (or AND-of-inequalities
    // for NOT IN), reusing the typed leaf builder per value.
    if (try findInClause(expr)) |inc| {
        return parseInList(arena, expr, inc, file, diag);
    }
    // BETWEEN runs before operator detection — `col BETWEEN x AND y` has no
    // binary op the comparison-finder would recognize. `col NOT BETWEEN x AND y` is its negation.
    if (try topLevelIndexOf(expr, " NOT BETWEEN ")) |i| {
        return negateFilter(arena, try parseBetween(arena, expr, i, " NOT BETWEEN ".len, file, diag));
    }
    if (try topLevelIndexOf(expr, " BETWEEN ")) |i| {
        return parseBetween(arena, expr, i, " BETWEEN ".len, file, diag);
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
    const col_idx = try resolveCol(file, col_name, diag);
    return ast.Filter{ .null_check = .{ .col_idx = col_idx, .is_not = is_not } };
}

/// Build a `LIKE` / `NOT LIKE` leaf. The pattern is classified once
/// (DuckDB's trick) so prefix/suffix/contains/exact patterns dodge the
/// general matcher. LIKE is text-only — non-string columns are a clear
/// error rather than a confusing type mismatch later.
fn parseLike(
    arena: std.mem.Allocator,
    col_part: []const u8,
    pat_part: []const u8,
    negate: bool,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const col_name = trimSpace(col_part);
    if (col_name.len == 0) return error.BadOperator;
    const pattern = try unquoteLiteral(arena, trimSpace(pat_part));
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

fn findInClause(expr: []const u8) Error!?InClause {
    if (try topLevelIndexOf(expr, " NOT IN ")) |i|
        return .{ .col_end = i, .vals_start = i + " NOT IN ".len, .negated = true };
    if (try topLevelIndexOf(expr, " IN ")) |i|
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
    const col_name = trimSpace(expr[0..inc.col_end]);
    const list = trimSpace(expr[inc.vals_start..]);
    if (col_name.len == 0 or list.len == 0) return error.BadOperator;
    // A parenthesized list, split at its outermost commas: a comma inside a quoted value is part of the value.
    const vals = (try groupBody(list)) orelse return error.BadOperator;

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;

    // IN → OR of `= v`; NOT IN → AND of `!= v` (De Morgan).
    const leaf_op: ast.Operator = if (inc.negated) .NotEq else .Eq;
    var leaves: std.ArrayList(ast.Filter) = .empty;
    var cur: Cursor = .{ .src = vals };
    var start: usize = 0;
    while (true) {
        const comma = try cur.find(",");
        const v = trimSpace(vals[start .. comma orelse vals.len]);
        if (v.len == 0) return error.BadValue;
        try leaves.append(arena, try buildTypedComparison(arena, col_idx, &elem, leaf_op, v, diag));
        start = (comma orelse break) + 1;
    }
    // One node for the whole list, however long: OR of `= v` (AND of `!= v`), with its set bound beside the leaves.
    if (leaves.items.len == 1) return leaves.items[0];
    const c: ast.Composite = .{
        .children = leaves.items,
        .in_set = try bindInSet(arena, col_idx, inc.negated, leaves.items),
    };
    return if (inc.negated) .{ .and_filter = c } else .{ .or_filter = c };
}

/// The literals of an IN list bound for one-pass evaluation; null when its leaves are of a type the set doesn't hold
/// (boolean) or one of them isn't a plain `=` (`!=` for NOT IN) or a constant, which then evaluate leaf by leaf.
fn bindInSet(
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
/// NULL handling matches ZPQ's "nulls don't pass" convention either way
/// (both `NOT (x > 5)` and `x <= 5` exclude null rows).
fn negateFilter(arena: std.mem.Allocator, f: ast.Filter) Error!ast.Filter {
    return switch (f) {
        // Negating keeps the value, so a stored literal stays one (`NOT x != 5` is `x = 5`).
        inline .int32, .int64, .uint64, .float, .double, .string, .boolean => |l, tag| @unionInit(
            ast.Filter,
            @tagName(tag),
            .{ .col_idx = l.col_idx, .op = l.op.negate(), .value = l.value, .stored = l.stored },
        ),
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
            };
            break :blk if (f == .and_filter) .{ .or_filter = flipped } else .{ .and_filter = flipped };
        },
    };
}

/// Parse `col BETWEEN x AND y` (the keyword at `between_at`, `kw_len` long) into the conjunction
/// `(col >= x AND col <= y)`. SQL semantics: BETWEEN is inclusive on both sides.
fn parseBetween(
    arena: std.mem.Allocator,
    expr: []const u8,
    between_at: usize,
    kw_len: usize,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const after_between = between_at + kw_len;
    const and_at = (try topLevelIndexOf(expr[after_between..], " AND ")) orelse return error.BadOperator;

    const col_name = trimSpace(expr[0..between_at]);
    const x_str = trimSpace(expr[after_between .. after_between + and_at]);
    const y_str = trimSpace(expr[after_between + and_at + " AND ".len ..]);
    if (col_name.len == 0 or x_str.len == 0 or y_str.len == 0) return error.BadOperator;

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;

    // Bounds bind exactly like `>=`/`<=` would. Binding on the bare physical type instead turned DECIMAL(FLBA)
    // bounds into byte-string leaves (silently wrong) and rejected DATE/TIMESTAMP literals.
    return ast.join(arena, true, &.{
        try buildTypedComparison(arena, col_idx, &elem, .GtEq, x_str, diag),
        try buildTypedComparison(arena, col_idx, &elem, .LtEq, y_str, diag),
    });
}

fn parseComparisonLeaf(
    arena: std.mem.Allocator,
    expr: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Filter {
    const found = (try findComparison(expr)) orelse return error.BadOperator;
    const op = found.op;
    const found_at = found.at;
    const found_len = found.len;

    const col_name = trimSpace(expr[0..found_at]);
    const val_str = trimSpace(expr[found_at + found_len ..]);
    if (col_name.len == 0 or val_str.len == 0) return error.BadOperator;
    // What parentheses are left here are no grouping: `(a + b) = 1` or `f(x) = 1`, which no leaf compares.
    if (std.mem.indexOfScalar(u8, col_name, '(') != null) return error.BadOperator;

    const col_idx = try resolveCol(file, col_name, diag);
    const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
    return try buildTypedComparison(arena, col_idx, &elem, op, val_str, diag);
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

/// Build a single typed comparison leaf, honoring DECIMAL columns (decoded
/// to f64 → the .double lane) and DATE/TIMESTAMP columns (SQL-style literals
/// → stored epoch ints). Shared by the comparison path and the IN-list
/// expansion so both get identical type handling.
fn buildTypedComparison(
    arena: std.mem.Allocator,
    col_idx: usize,
    elem: *const schema.SchemaElement,
    op: ast.Operator,
    val_str: []const u8,
    diag: ?*Diag,
) Error!ast.Filter {
    // Every leaf that compares a stored value is marked `stored` here and nowhere else, so what membership pruning
    // hashes (filter/membership.zig) can never drift from what evaluation compares.
    if (decimal_mod.kindFromSchema(elem) != null) {
        // Compared after decoding to f64: the literal is no stored value.
        return buildLeafFilter(col_idx, op, val_str, .DOUBLE);
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
        const value = try unquoteLiteral(arena, val_str);
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
            // Any letter case: SQL prints the literal as TRUE.
            .value = if (std.ascii.eqlIgnoreCase(val_str, "true") or std.mem.eql(u8, val_str, "1"))
                true
            else if (std.ascii.eqlIgnoreCase(val_str, "false") or std.mem.eql(u8, val_str, "0"))
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
        .num_children = 11,
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
    try meta.schema.append(arena, .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 16,
        .repetition_type = .OPTIONAL,
        .name = "uid",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
        .logical_type = .{ .UUID = .{} },
    });
    return meta;
}

fn filterDepth(f: ast.Filter) usize {
    return switch (f) {
        .and_filter, .or_filter => |c| blk: {
            var d: usize = 0;
            for (c.children) |child| d = @max(d, filterDepth(child));
            break :blk 1 + d;
        },
        else => 1,
    };
}

test "regression: IN and NOT IN lists of thousands are one node, values in order" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    for ([_][]const u8{ " IN (", " NOT IN (" }) |kw| {
        var expr: std.ArrayList(u8) = .empty;
        try expr.print(a, "id{s}", .{kw});
        for (0..8000) |i| try expr.print(a, "{s}{d}", .{ if (i == 0) "" else ", ", i });
        try expr.append(a, ')');
        const f = try parse(a, expr.items, &meta, null);
        try testing.expectEqual(@as(usize, 2), filterDepth(f));
        // Leaves left to right are the list's values.
        var stack: std.ArrayList(ast.Filter) = .empty;
        try stack.append(a, f);
        var next: i32 = 0;
        while (stack.pop()) |n| switch (n) {
            .and_filter, .or_filter => |c| {
                try testing.expect((n == .and_filter) == (kw.len > 5));
                var k = c.children.len;
                while (k > 0) : (k -= 1) try stack.append(a, c.children[k - 1]);
            },
            else => {
                try testing.expectEqual(next, n.int32.value);
                next += 1;
            },
        };
        try testing.expectEqual(@as(i32, 8000), next);
    }
}

test "regression: chains of a hundred thousand ANDs, ORs and NOTs parse flat and evaluate" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);
    const n_terms = 100_000;

    const ids = [_]i32{ -3, 0, 7, 99_999, 100_000, 5 };
    const cols = [_]eval.Batch.Column{.{ .i32 = .{ .values = &ids } }};
    const lookup = [_]?usize{0};
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = ids.len };

    const Case = struct { join: []const u8, depth: usize, want: []const bool };
    const cases = [_]Case{
        // id >= 0 AND id >= 1 ... AND id >= 99999: only ids of 99999 and up pass.
        .{ .join = " AND ", .depth = 2, .want = &.{ false, false, false, true, true, false } },
        // id = 0 OR id = 1 ... OR id = 99999.
        .{ .join = " OR ", .depth = 2, .want = &.{ false, true, true, true, false, true } },
    };
    for (cases) |case| {
        var expr: std.ArrayList(u8) = .empty;
        for (0..n_terms) |i| try expr.print(a, "{s}id {s} {d}", .{
            if (i == 0) "" else case.join,
            if (std.mem.eql(u8, case.join, " AND ")) ">=" else "=",
            i,
        });
        const f = try parse(a, expr.items, &meta, null);
        try testing.expectEqual(case.depth, filterDepth(f));
        var sel = try selection.SelectionVector.init(a, ids.len);
        try eval.evaluate(f, &batch, &sel, &lookup, a);
        for (case.want, 0..) |w, i| try testing.expectEqual(w, sel.isActive(i));
    }

    // Alternating AND / OR: OR of two-leaf ANDs, three levels.
    var mixed: std.ArrayList(u8) = .empty;
    for (0..n_terms) |i| try mixed.print(a, "{s}id > {d}", .{ if (i == 0) "" else if (i % 2 == 1) " AND " else " OR ", i });
    try testing.expectEqual(@as(usize, 3), filterDepth(try parse(a, mixed.items, &meta, null)));

    // A run of NOTs is its parity: an even one leaves the leaf as it was, an odd one flips it.
    for ([_]usize{ n_terms, n_terms + 1 }) |nots| {
        var expr: std.ArrayList(u8) = .empty;
        for (0..nots) |_| try expr.appendSlice(a, "NOT ");
        try expr.appendSlice(a, "id > 5");
        const f = try parse(a, expr.items, &meta, null);
        try testing.expectEqual(if (nots % 2 == 0) ast.Operator.Gt else ast.Operator.LtEq, f.int32.op);
    }
}

/// `f` with every bound IN set dropped, so it evaluates leaf by leaf.
fn withoutInSets(a: std.mem.Allocator, f: ast.Filter) !ast.Filter {
    return switch (f) {
        inline .and_filter, .or_filter => |c, tag| blk: {
            const children = try a.alloc(ast.Filter, c.children.len);
            for (c.children, children) |child, *n| n.* = try withoutInSets(a, child);
            break :blk @unionInit(ast.Filter, @tagName(tag), .{ .children = children });
        },
        else => f,
    };
}

test "IN lists bound to a set select exactly what their leaves do: nulls, NaN, -0, NOT IN and NOT (IN)" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const n = 777;
    var prng = std.Random.DefaultPrng.init(0x15e7);
    const r = prng.random();
    const ids = try a.alloc(i32, n);
    const strs = try a.alloc([]const u8, n);
    const scores = try a.alloc(f64, n);
    const tss = try a.alloc(i64, n);
    const defs = try a.alloc(u32, n);
    const specials = [_]f64{ 0.0, -0.0, std.math.nan(f64), std.math.inf(f64), 2.5 };
    for (0..n) |i| {
        ids[i] = r.intRangeAtMost(i32, -300, 300);
        strs[i] = try std.fmt.allocPrint(a, "s{d}", .{r.uintLessThan(u32, 400)});
        scores[i] = if (r.boolean())
            specials[r.uintLessThan(usize, specials.len)]
        else
            @floatFromInt(r.intRangeAtMost(i32, -50, 50));
        tss[i] = r.intRangeAtMost(i64, -1 << 40, 1 << 40);
        defs[i] = @intFromBool(r.uintLessThan(u8, 6) != 0);
    }
    for ([_]usize{ 0, 4 }) |k| tss[k] = 123456789;
    const cols = [_]eval.Batch.Column{
        .{ .i32 = .{ .values = ids, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = strs, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .{ .f64 = .{ .values = scores, .def_levels = defs, .max_def = 1, .has_nulls = true } },
        .{ .i64 = .{ .values = tss } },
    };
    var lookup: [11]?usize = @splat(null);
    lookup[0] = 0;
    lookup[1] = 1;
    lookup[2] = 2;
    lookup[4] = 3;
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = n };

    var exprs: std.ArrayList([]const u8) = .empty;
    for ([_]usize{ 2, 5, 8, 9, 40, 300, 2000 }) |k| {
        const heads = [_][]const u8{
            "id IN (",         "id NOT IN (", "status IN (",
            "NOT status IN (", "ts IN (",     "ts NOT IN (",
        };
        for (heads) |head| {
            var e: std.ArrayList(u8) = .empty;
            try e.appendSlice(a, head);
            for (0..k) |i| {
                if (i > 0) try e.appendSlice(a, ", ");
                if (head[0] == 's' or head[4] == 's') {
                    try e.print(a, "'s{d}'", .{r.uintLessThan(u32, 500)});
                } else if (head[0] == 't') {
                    try e.print(a, "{d}", .{if (i == 0) 123456789 else r.intRangeAtMost(i64, -1 << 40, 1 << 40)});
                } else {
                    try e.print(a, "{d}", .{r.intRangeAtMost(i32, -400, 400)});
                }
            }
            try e.append(a, ')');
            try exprs.append(a, e.items);
        }
    }
    try exprs.appendSlice(a, &.{
        "score IN (0, 2.5, 7)",
        "score IN (-0.0, nan, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)",
        "score NOT IN (nan, 0, inf, 3, 4, 5, 6, 7, 8, 9, 10, 11)",
        "NOT score IN (-0.0, -inf, 2.5)",
    });
    for (exprs.items) |expr| {
        const f = try parse(a, expr, &meta, null);
        try testing.expect(switch (f) {
            .and_filter, .or_filter => |c| c.in_set != null,
            else => false,
        });
        var want = try selection.SelectionVector.init(a, n);
        var got = try selection.SelectionVector.init(a, n);
        for (0..n) |i| if (i % 5 == 0) {
            want.set(i, false);
            got.set(i, false);
        };
        try eval.evaluate(try withoutInSets(a, f), &batch, &want, &lookup, a);
        try eval.evaluate(f, &batch, &got, &lookup, a);
        if (!std.mem.eql(u64, want.mask, got.mask)) {
            std.debug.print("IN set disagrees with its leaves on: {s}\n", .{expr[0..@min(expr.len, 120)]});
            return error.TestUnexpectedResult;
        }
    }
}

test "IN lists holding literals no stored value can equal bind only their real members" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // `ts` is TIMESTAMP(MICROS): a nanosecond literal binds to the constant `> maxInt` (`<= maxInt` under NOT IN).
    // A set that took the constant's value as a member would match the row holding maxInt.
    const tss = [_]i64{ std.math.maxInt(i64), 2, 3, 5, std.math.minInt(i64), 0 };
    const cols = [_]eval.Batch.Column{.{ .i64 = .{ .values = &tss } }};
    var lookup: [11]?usize = @splat(null);
    lookup[4] = 0;
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = tss.len };
    const sub_us = "'1970-01-01 00:00:00.000000001'";
    const cases = [_]struct { expr: []const u8, bound: bool }{
        .{ .expr = "ts IN (" ++ sub_us ++ ", 1, 2, 3, 4, 5, 6, 7, 8, 9)", .bound = true },
        .{ .expr = "ts NOT IN (" ++ sub_us ++ ", 1, 2, 3, 4, 5, 6, 7, 8, 9)", .bound = true },
        .{ .expr = "NOT ts IN (" ++ sub_us ++ ", 1, 2, 3, 4, 5, 6, 7, 8, 9)", .bound = true },
        .{ .expr = "ts IN (" ++ sub_us ++ ", '1970-01-01 00:00:00.000000002')", .bound = false },
        .{ .expr = "ts NOT IN (" ++ sub_us ++ ", '1970-01-01 00:00:00.000000002')", .bound = false },
    };
    for (cases) |c| {
        const f = try parse(a, c.expr, &meta, null);
        const bound = switch (f) {
            .and_filter, .or_filter => |comp| comp.in_set != null,
            else => false,
        };
        try testing.expectEqual(c.bound, bound);
        var want = try selection.SelectionVector.init(a, tss.len);
        var got = try selection.SelectionVector.init(a, tss.len);
        try eval.evaluate(try withoutInSets(a, f), &batch, &want, &lookup, a);
        try eval.evaluate(f, &batch, &got, &lookup, a);
        try testing.expectEqualSlices(u64, want.mask, got.mask);
        // The maxInt row: only NOT IN keeps it.
        try testing.expectEqual(std.mem.indexOf(u8, c.expr, "NOT") != null, got.isActive(0));
    }
}

test "unsigned IN lists compare stored bits on both lanes, out-of-range literals folded" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
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
    const fm: schema.FileMetaData = .{ .version = 1, .schema = items, .num_rows = 0, .created_by = null, .row_groups = .empty };

    const u_vals = [_]i32{ @bitCast(@as(u32, 3_000_000_000)), 1, -1, 7, 0, @bitCast(@as(u32, 4_000_000_000)) };
    const w_vals = [_]i64{ @bitCast(@as(u64, std.math.maxInt(u64))), 1, -1, 7, 0, 99 };
    const cols = [_]eval.Batch.Column{ .{ .i32 = .{ .values = &u_vals } }, .{ .i64 = .{ .values = &w_vals } } };
    const lookup = [_]?usize{ 0, 1 };
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = u_vals.len };
    for ([_][]const u8{
        "u IN (3000000000, 1, -1, 2, 3, 4, 5, 6, 8, 9, 4294967296)",
        "u NOT IN (3000000000, 1, -1, 2, 3, 4, 5, 6, 8, 9, 4294967296)",
        "w IN (18446744073709551615, 1, -1, 2, 3, 4, 5, 6, 8, 9, 18446744073709551616)",
        "NOT w IN (18446744073709551615, 1, 2, 3, 4, 5, 6, 8, 9, 11, 12, 13)",
    }) |expr| {
        const f = try parse(a, expr, &fm, null);
        try testing.expect(switch (f) {
            .and_filter, .or_filter => |c| c.in_set != null,
            else => false,
        });
        var want = try selection.SelectionVector.init(a, u_vals.len);
        var got = try selection.SelectionVector.init(a, u_vals.len);
        try eval.evaluate(try withoutInSets(a, f), &batch, &want, &lookup, a);
        try eval.evaluate(f, &batch, &got, &lookup, a);
        try testing.expectEqualSlices(u64, want.mask, got.mask);
    }
}

test "regression: a UUID literal binds to the 16 bytes the column stores" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    const want = [16]u8{
        0x55, 0x0e, 0x84, 0x00, 0xe2, 0x9b, 0x41, 0xd4,
        0xa7, 0x16, 0x44, 0x66, 0x55, 0x44, 0x00, 0x00,
    };
    for ([_][]const u8{
        "uid = '550e8400-e29b-41d4-a716-446655440000'",
        "uid = '550E8400E29B41D4A716446655440000'",
        "uid IN ('550e8400-e29b-41d4-a716-446655440000')",
    }) |expr| {
        const f = try parse(a, expr, &meta, null);
        try testing.expectEqualSlices(u8, &want, f.string.value);
        try testing.expect(f.string.stored);
    }
    for ([_][]const u8{
        "uid = '550e8400-e29b-41d4-a716-44665544000'",
        "uid = '550e8400e-29b-41d4-a716-446655440000'",
        "uid = '550e8400-e29b-41d4-a716-44665544000g'",
        "uid = 'abc'",
    }) |expr| try testing.expectError(error.BadValue, parse(a, expr, &meta, null));
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
    try expectLeaf(i64, btw.and_filter.children[0], .Gt, ms);
    try expectLeaf(i64, btw.and_filter.children[1], .LtEq, ms + 10);
    const in = try parse(a, "tsms IN ('2024-01-01 00:00:00.0995', '2024-01-01 00:00:00.100')", &meta, null);
    try expectLeaf(i64, in.or_filter.children[0], .Gt, max64);
    try expectLeaf(i64, in.or_filter.children[1], .Eq, ms + 100);
    const nin = try parse(a, "tsms NOT IN ('2024-01-01 00:00:00.0995', '2024-01-01 00:00:00.100')", &meta, null);
    try expectLeaf(i64, nin.and_filter.children[0], .LtEq, max64);
    try expectLeaf(i64, nin.and_filter.children[1], .NotEq, ms + 100);

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

    // id IN (1, 2, 3) → (id=1 OR (id=2 OR id=3)), balanced
    const f = try parse(a, "id IN (1, 2, 3)", &meta, null);
    try testing.expect(f == .or_filter);
    try testing.expectEqual(@as(i32, 1), f.or_filter.children[0].int32.value);
    try testing.expectEqual(ast.Operator.Eq, f.or_filter.children[0].int32.op);
    try testing.expectEqual(@as(i32, 2), f.or_filter.children[1].int32.value);
    try testing.expectEqual(@as(i32, 3), f.or_filter.children[2].int32.value);

    // id NOT IN (1, 2) → (id!=1 AND id!=2)
    const fni = try parse(a, "id NOT IN (1, 2)", &meta, null);
    try testing.expect(fni == .and_filter);
    try testing.expectEqual(ast.Operator.NotEq, fni.and_filter.children[0].int32.op);
    try testing.expectEqual(ast.Operator.NotEq, fni.and_filter.children[1].int32.op);

    // string IN with quotes
    const fs = try parse(a, "status IN ('active', 'idle')", &meta, null);
    try testing.expect(fs == .or_filter);
    try testing.expectEqualStrings("active", fs.or_filter.children[0].string.value);
    try testing.expectEqualStrings("idle", fs.or_filter.children[1].string.value);

    // NOT id > 5 → id <= 5
    const fnot = try parse(a, "NOT id > 5", &meta, null);
    try testing.expectEqual(ast.Operator.LtEq, fnot.int32.op);
    try testing.expectEqual(@as(i32, 5), fnot.int32.value);

    // NOT score BETWEEN 1 AND 2 → De Morgan → (score < 1 OR score > 2)
    const fb = try parse(a, "NOT score BETWEEN 1 AND 2", &meta, null);
    try testing.expect(fb == .or_filter);
    try testing.expectEqual(ast.Operator.Lt, fb.or_filter.children[0].double.op);
    try testing.expectEqual(ast.Operator.Gt, fb.or_filter.children[1].double.op);
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
    try testing.expectEqual(@as(usize, 0), f.and_filter.children[0].int32.col_idx);
    try testing.expectEqual(ast.Operator.Gt, f.and_filter.children[0].int32.op);
    try testing.expectEqualStrings("active", f.and_filter.children[1].string.value);
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
    try testing.expect(f.or_filter.children[1] == .and_filter);
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

    try testing.expectError(error.UnbalancedParentheses, parse(a, "(id > 1 OR id < 5", &meta, null));
    try testing.expectError(error.UnbalancedParentheses, parse(a, "id > 1) OR (id < 5", &meta, null));
    try testing.expectError(error.EmptyExpr, parse(a, "id > 1 AND ()", &meta, null));
    // A quote that never closes is an error, never a literal that swallows or splits the rest.
    try testing.expectError(error.UnterminatedString, parse(a, "status = 'O\\'Reilly' AND id = 1", &meta, null));
    try testing.expectError(error.UnterminatedString, parse(a, "status = 'abc", &meta, null));
    try testing.expectError(error.UnterminatedString, parse(a, "\"status = 1", &meta, null));
    // What parentheses remain inside a leaf are no grouping this grammar has.
    try testing.expectError(error.BadOperator, parse(a, "(id + 1) = 2", &meta, null));
    // LIKE on a non-string column is a clear error.
    try testing.expectError(error.UnsupportedType, parse(a, "id LIKE '5%'", &meta, null));

    var deep: std.ArrayList(u8) = .empty;
    for (0..MAX_GROUP_DEPTH) |_| try deep.appendSlice(a, "(");
    try deep.appendSlice(a, "id > 1");
    for (0..MAX_GROUP_DEPTH) |_| try deep.appendSlice(a, ")");
    _ = try parse(a, deep.items, &meta, null);
    const deeper = try std.fmt.allocPrint(a, "NOT ({s})", .{deep.items});
    try testing.expectError(error.FilterTooDeep, parse(a, deeper, &meta, null));
}

test "parse groups with parentheses, quotes and NOT as SQL does" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // `(a OR b) AND c` keeps the OR beneath the AND.
    const f = try parse(a, "(id = 1 OR id = 2) AND status = 'a'", &meta, null);
    try testing.expect(f == .and_filter);
    try testing.expect(f.and_filter.children[0] == .or_filter);
    // Keywords and operators inside quotes are the value's, and a doubled quote is one quote.
    const q = try parse(a, "status = 'x OR y AND z' OR status = 'it''s' OR status = 'a<b'", &meta, null);
    try testing.expectEqual(@as(usize, 3), q.or_filter.children.len);
    try testing.expectEqualStrings("x OR y AND z", q.or_filter.children[0].string.value);
    try testing.expectEqualStrings("it's", q.or_filter.children[1].string.value);
    try testing.expectEqualStrings("a<b", q.or_filter.children[2].string.value);
    const in_quotes = try parse(a, "status IN ('a,b', 'c) OR (d')", &meta, null);
    try testing.expectEqualStrings("a,b", in_quotes.or_filter.children[0].string.value);
    try testing.expectEqualStrings("c) OR (d", in_quotes.or_filter.children[1].string.value);
    // NOT applies to the group: De Morgan, not to its first leaf.
    const n = try parse(a, "NOT (id = 1 AND status = 'a')", &meta, null);
    try testing.expect(n == .or_filter);
    try testing.expectEqual(ast.Operator.NotEq, n.or_filter.children[0].int32.op);
    // Lower-case keywords split as upper-case ones do.
    const lc = try parse(a, "id = 1 or not (id = 2 and id = 3)", &meta, null);
    try testing.expect(lc == .or_filter);
    // NOT BETWEEN is the complement of BETWEEN's range.
    const nb = try parse(a, "id NOT BETWEEN 2 AND 4", &meta, null);
    try testing.expect(nb == .or_filter);
    try testing.expectEqual(ast.Operator.Lt, nb.or_filter.children[0].int32.op);
    try testing.expectEqual(ast.Operator.Gt, nb.or_filter.children[1].int32.op);
}

/// Three-valued truth of a generated predicate for one row: null is UNKNOWN.
const Tri = ?bool;

fn triAnd(x: Tri, y: Tri) Tri {
    if (x == false or y == false) return false;
    if (x == null or y == null) return null;
    return true;
}

fn triOr(x: Tri, y: Tri) Tri {
    if (x == true or y == true) return true;
    if (x == null or y == null) return null;
    return false;
}

fn triNot(x: Tri) Tri {
    return if (x) |v| !v else null;
}

const GenRow = struct { id: ?i32, status: ?[]const u8 };

/// Writes a random predicate over `id` and `status` to `out` and returns its truth for each of `rows`.
fn genPredicate(
    a: std.mem.Allocator,
    rand: std.Random,
    out: *std.ArrayList(u8),
    rows: []const GenRow,
    depth: u32,
) ![]Tri {
    const truth = try a.alloc(Tri, rows.len);
    const kw = struct {
        fn word(r: std.Random, upper: []const u8, lower: []const u8) []const u8 {
            return if (r.boolean()) upper else lower;
        }
    };
    const pick = if (depth >= 4) rand.uintLessThan(u8, 9) else rand.uintLessThan(u8, 13);
    switch (pick) {
        0...8 => {
            const v: i32 = rand.uintLessThan(u8, 4);
            const w: i32 = v + rand.uintLessThan(u8, 3);
            const strs = [_][]const u8{ "a", "it''s", "x OR y", "b" };
            const vals = [_][]const u8{ "a", "it's", "x OR y", "b" };
            const s = rand.uintLessThan(usize, strs.len);
            switch (pick) {
                0 => try out.print(a, "id = {d}", .{v}),
                1 => try out.print(a, "id != {d}", .{v}),
                2 => try out.print(a, "id < {d}", .{v}),
                3 => try out.print(a, "id {s} ({d}, {d})", .{ kw.word(rand, "IN", "in"), v, w }),
                4 => try out.print(a, "id NOT IN ({d}, {d})", .{ v, w }),
                5 => try out.print(a, "id {s} {d} {s} {d}", .{ kw.word(rand, "BETWEEN", "between"), v, kw.word(rand, "AND", "and"), w }),
                6 => try out.print(a, "id {s}", .{kw.word(rand, "IS NULL", "IS NOT NULL")}),
                7 => try out.print(a, "status = '{s}'", .{strs[s]}),
                8 => try out.print(a, "status LIKE '%{s}%'", .{strs[s][0..1]}),
                else => unreachable,
            }
            const is_null_kw = pick == 6 and std.mem.endsWith(u8, out.items, "IS NULL");
            for (rows, truth) |row, *t| t.* = switch (pick) {
                0 => if (row.id) |x| x == v else null,
                1 => if (row.id) |x| x != v else null,
                2 => if (row.id) |x| x < v else null,
                3 => if (row.id) |x| x == v or x == w else null,
                4 => if (row.id) |x| x != v and x != w else null,
                5 => if (row.id) |x| x >= v and x <= w else null,
                6 => if (is_null_kw) row.id == null else row.id != null,
                7 => if (row.status) |x| std.mem.eql(u8, x, vals[s]) else null,
                8 => if (row.status) |x| std.mem.indexOf(u8, x, vals[s][0..1]) != null else null,
                else => unreachable,
            };
        },
        9 => {
            try out.appendSlice(a, kw.word(rand, "NOT ", "not "));
            try out.appendSlice(a, "(");
            const inner = try genPredicate(a, rand, out, rows, depth + 1);
            try out.appendSlice(a, ")");
            for (inner, truth) |x, *t| t.* = triNot(x);
        },
        else => {
            const conj = pick != 12;
            const n = 2 + rand.uintLessThan(usize, 2);
            try out.appendSlice(a, "(");
            for (0..n) |k| {
                if (k > 0) try out.appendSlice(a, if (conj) kw.word(rand, " AND ", " and ") else kw.word(rand, " OR ", " or "));
                const part = try genPredicate(a, rand, out, rows, depth + 1);
                for (part, truth) |x, *t| t.* = if (k == 0) x else if (conj) triAnd(t.*, x) else triOr(t.*, x);
            }
            try out.appendSlice(a, ")");
        },
    }
    return truth;
}

test "fuzz-lite: nested predicates select exactly the rows three-valued logic passes" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    var rows: [64]GenRow = undefined;
    var prng = std.Random.DefaultPrng.init(0x9a7e);
    const rand = prng.random();
    const statuses = [_]?[]const u8{ "a", "it's", "x OR y", "b", "ab", null };
    for (&rows) |*r| r.* = .{
        .id = if (rand.uintLessThan(u8, 5) == 0) null else @as(i32, rand.uintLessThan(u8, 6)),
        .status = statuses[rand.uintLessThan(usize, statuses.len)],
    };
    var ids: [rows.len]i32 = undefined;
    var id_def: [rows.len]u32 = undefined;
    var strs: [rows.len][]const u8 = undefined;
    var str_def: [rows.len]u32 = undefined;
    for (rows, 0..) |r, i| {
        ids[i] = r.id orelse 0;
        id_def[i] = if (r.id == null) 0 else 1;
        strs[i] = r.status orelse "";
        str_def[i] = if (r.status == null) 0 else 1;
    }
    const cols = [_]eval.Batch.Column{
        .{ .i32 = .{ .values = &ids, .def_levels = &id_def, .max_def = 1, .has_nulls = true } },
        .{ .string = .{ .values = &strs, .def_levels = &str_def, .max_def = 1, .has_nulls = true } },
    };
    const lookup = [_]?usize{ 0, 1 };
    const batch: eval.Batch = .{ .cols = &cols, .num_rows = rows.len };

    for (0..400) |_| {
        var text: std.ArrayList(u8) = .empty;
        const want = try genPredicate(a, rand, &text, &rows, 0);
        const f = parse(a, text.items, &meta, null) catch |err| {
            std.debug.print("parse failed: {s}: {s}\n", .{ text.items, @errorName(err) });
            return err;
        };
        var sel = try selection.SelectionVector.init(a, rows.len);
        try eval.evaluate(f, &batch, &sel, &lookup, a);
        for (want, 0..) |w, i| {
            if (sel.isActive(i) != (w == true)) {
                std.debug.print("row {d} {?d} {?s}: {s}\n", .{ i, rows[i].id, rows[i].status, text.items });
                return error.TestUnexpectedResult;
            }
        }
    }
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
    try testing.expect(f_and.and_filter.children[1] == .null_check);

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
    try testing.expectEqual(@as(i32, 10), btw.and_filter.children[0].int32.value);

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
    try testing.expect(f.and_filter.children[0] == .int32);
    try testing.expect(f.and_filter.children[1] == .int32);
    try testing.expectEqual(ast.Operator.GtEq, f.and_filter.children[0].int32.op);
    try testing.expectEqual(@as(i32, 10), f.and_filter.children[0].int32.value);
    try testing.expectEqual(ast.Operator.LtEq, f.and_filter.children[1].int32.op);
    try testing.expectEqual(@as(i32, 20), f.and_filter.children[1].int32.value);
}

test "parse BETWEEN binds its bounds like the comparison operators" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // DECIMAL bounds take the f64 lane, not a byte-string compare against the FLBA bytes.
    const fd = try parse(a, "dec BETWEEN 1 AND 2.5", &meta, null);
    try testing.expect(fd.and_filter.children[0] == .double and fd.and_filter.children[1] == .double);
    try testing.expectEqual(@as(f64, 1), fd.and_filter.children[0].double.value);
    try testing.expectEqual(@as(f64, 2.5), fd.and_filter.children[1].double.value);

    // DATE/TIMESTAMP bounds accept the same SQL literals `>=`/`<=` do.
    const fdt = try parse(a, "d BETWEEN '2024-01-01' AND '2024-01-31'", &meta, null);
    try testing.expectEqual(@as(i32, 19723), fdt.and_filter.children[0].int32.value);
    try testing.expectEqual(@as(i32, 19753), fdt.and_filter.children[1].int32.value);
    const fts = try parse(a, "ts BETWEEN '2024-01-01' AND '2024-01-01 00:00:01'", &meta, null);
    try testing.expectEqual(@as(i64, 1_704_067_200_000_000), fts.and_filter.children[0].int64.value);
    try testing.expectEqual(@as(i64, 1_704_067_201_000_000), fts.and_filter.children[1].int64.value);
}

test "parse BETWEEN composes with outer AND" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);

    // `score BETWEEN 0.5 AND 0.9 AND id=1` is `score >= 0.5 AND score <= 0.9 AND id = 1`, one flat AND: the
    // BETWEEN's bound-AND must NOT be the conjunction split point, and its bounds join the chain.
    const f = try parse(a, "score BETWEEN 0.5 AND 0.9 AND id=1", &meta, null);
    try testing.expect(f == .and_filter);
    try testing.expectEqual(@as(usize, 3), f.and_filter.children.len);
    try testing.expectEqual(ast.Operator.GtEq, f.and_filter.children[0].double.op);
    try testing.expectEqual(ast.Operator.LtEq, f.and_filter.children[1].double.op);
    try testing.expectEqual(@as(i32, 1), f.and_filter.children[2].int32.value);
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
    try testing.expectEqual(@as(usize, 0), quoted.and_filter.children[0].int64.col_idx);
    const like = try parse(a, "r.name LIKE 'a%'", &meta, null);
    try testing.expectEqual(@as(usize, 1), like.like.col_idx);
    const in_list = try parse(a, "key IN (1005, 1006)", &meta, null);
    try testing.expectEqual(@as(usize, 2), in_list.or_filter.children[0].int64.col_idx);
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
    try testing.expectEqual(@as(usize, 0), in_list.or_filter.children[0].int64.col_idx);
    try testing.expectEqual(@as(usize, 1), (try parse(a, "\"r\".\"name\" LIKE 'a%'", &meta, null)).like.col_idx);
}

test "regression: IN sets preserve temporal comparison rewrites" {
    const eval = @import("eval.zig");
    const selection = @import("selection.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var meta = try synthFileMeta(a);
    defer meta.deinit(a);
    meta.schema.items[5].logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = true, .unit = .{ .NANOS = .{} } } };
    const values = [_]i64{ 0, 1, std.math.maxInt(i64), 0 };
    const defs = [_]u32{ 1, 1, 1, 0 };
    const columns = [_]eval.Batch.Column{.{ .i64 = .{ .values = &values, .def_levels = &defs, .max_def = 1, .has_nulls = true } }};
    const batch: eval.Batch = .{ .cols = &columns, .num_rows = values.len };
    var lookup: [11]?usize = @splat(null);
    lookup[4] = 0;
    for ([_][]const u8{
        "ts IN ('1970-01-01 00:00:00.0000000001', '1970-01-01 00:00:00.0000000002')",
        "ts NOT IN ('1970-01-01 00:00:00.0000000001', '1970-01-01 00:00:00.0000000002')",
        "ts IN ('1970-01-01', '1970-01-01 00:00:00.0000000001')",
    }) |expr| {
        const filter = try parse(a, expr, &meta, null);
        var expected = try selection.SelectionVector.init(a, values.len);
        var actual = try selection.SelectionVector.init(a, values.len);
        try eval.evaluate(try withoutInSets(a, filter), &batch, &expected, &lookup, a);
        try eval.evaluate(filter, &batch, &actual, &lookup, a);
        try testing.expectEqualSlices(u64, expected.mask, actual.mask);
    }
}
