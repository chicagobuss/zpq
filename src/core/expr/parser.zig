//! Expression parser. Recursive descent over a tiny tokenizer.
//!
//! Grammar:
//!
//!   select      := select_item ( "," select_item )*
//!   select_item := expr ( "AS" IDENT )?
//!   expr        := term ( ("+" | "-") term )*
//!   term        := factor ( ("*" | "/") factor )*
//!   factor      := "-" factor | NUMBER | IDENT | IDENT "(" args ")" | "(" expr ")"
//!
//! Unary minus folds numeric literals at parse time; non-literal operands become `0 - expr`.
//! `AS` is case-insensitive (`as` works too).
//! Identifiers reference column names; resolution to `(col_idx,
//! physical_type)` happens here so the AST is fully typed.

const std = @import("std");
const ast = @import("ast.zig");
const agg = @import("agg.zig");
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");
const decimal_mod = @import("../parquet/decimal.zig");
const filter_ast = @import("../filter/ast.zig");
const filter_parser = @import("../filter/parser.zig");
const Diag = @import("../diag.zig").Diag;

pub const Error = error{
    EmptyExpr,
    EmptyAggregate,
    UnexpectedChar,
    UnexpectedEnd,
    BadNumber,
    UnknownColumn,
    /// A bare name with no top-level match names leaves in several groups.
    AmbiguousColumn,
    UnsupportedColumnType,
    UnterminatedString,
    TypeMismatch,
    UnknownFunction,
    UnknownAggFunc,
    WrongArity,
    BadAggArg,
    ExpectedLParen,
    ExpectedRParen,
    ExpectedIdentifier,
    ExpectedWhere,
    ExpectedAggFunc,
    StarOnlyValidInCount,
    TrailingTokens,
    ExpressionTooDeep,
    UnsupportedAggType,
    /// A UINT64 column used as an operand. Expressions compute in i64, where values >= 2^63 have no representation,
    /// so arithmetic and functions over such a column are refused rather than answered wrongly.
    Unsigned64Operand,
    /// A product's scale would pass 38 digits, which no DECIMAL holds.
    DecimalScaleTooLarge,
    /// A DECIMAL column whose declared precision/scale no DECIMAL can have (precision 1..38, scale 0..precision).
    InvalidDecimalType,
    /// A GROUP BY term names an aggregate (by a SELECT alias or position).
    AggregateInGroupBy,
    /// A CASE zpq does not evaluate: the simple form (`CASE x WHEN 1 ...`), branches of types with no common one, or
    /// every branch NULL.
    UnsupportedCase,
} || std.mem.Allocator.Error || filter_parser.Error;

/// A bare UINT64 reference is fine on its own (selected, grouped by, aggregated); as an operand it is not.
fn rejectUnsigned64Operand(e: ast.Expr) Error!void {
    if (e == .col_ref and e.col_ref.unsigned_64) return error.Unsigned64Operand;
}

const TokenKind = enum {
    number_int,
    number_float,
    string,
    ident,
    plus,
    minus,
    star,
    slash,
    /// `||` — SQL string concatenation.
    pipe_pipe,
    lparen,
    rparen,
    comma,
    eof,
};

const Token = struct {
    kind: TokenKind,
    /// Lexeme bytes for ident/number tokens; empty for punctuation.
    text: []const u8,
};

/// Bounds AST height and the parser's own recursion alike: `lex.depth` catches paren/unary-minus/call nesting before
/// any node exists, while the tree-height checks bound memory — the evaluator materializes a full intermediate column
/// per AST node in each row group a worker holds in flight, so peak cost is O(workers x depth x rows_per_row_group).
/// `max_memory` is unenforced on this path, so this cap is the only thing bounding the pathological case.
pub const MAX_EXPR_DEPTH: u32 = 32;

const Lexer = struct {
    src: []const u8,
    pos: usize = 0,
    /// Lives on the Lexer because it is already threaded through every parse function.
    depth: u32 = 0,
    /// Receives what a failure was about. On the Lexer for the same reason.
    diag: ?*Diag = null,

    fn peek(self: *Lexer) Error!Token {
        const save = self.pos;
        const t = try self.next();
        self.pos = save;
        return t;
    }

    fn next(self: *Lexer) Error!Token {
        while (self.pos < self.src.len and isSpace(self.src[self.pos])) self.pos += 1;
        if (self.pos >= self.src.len) return .{ .kind = .eof, .text = "" };
        const c = self.src[self.pos];
        switch (c) {
            '+' => {
                self.pos += 1;
                return .{ .kind = .plus, .text = "" };
            },
            '-' => {
                self.pos += 1;
                return .{ .kind = .minus, .text = "" };
            },
            '*' => {
                self.pos += 1;
                return .{ .kind = .star, .text = "" };
            },
            '/' => {
                self.pos += 1;
                return .{ .kind = .slash, .text = "" };
            },
            '(' => {
                self.pos += 1;
                return .{ .kind = .lparen, .text = "" };
            },
            ')' => {
                self.pos += 1;
                return .{ .kind = .rparen, .text = "" };
            },
            ',' => {
                self.pos += 1;
                return .{ .kind = .comma, .text = "" };
            },
            '|' => {
                if (self.pos + 1 < self.src.len and self.src[self.pos + 1] == '|') {
                    self.pos += 2;
                    return .{ .kind = .pipe_pipe, .text = "" };
                }
                return error.UnexpectedChar;
            },
            '\'' => {
                // Single-quoted string literal. No escapes in v1; a
                // literal like `'it\'s'` would need either `''` doubling
                // or `\'` escapes. Future work.
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.src.len and self.src[self.pos] != '\'') self.pos += 1;
                if (self.pos >= self.src.len) return error.UnterminatedString;
                const text = self.src[start..self.pos];
                self.pos += 1; // consume closing quote
                return .{ .kind = .string, .text = text };
            },
            '"' => {
                // Double-quoted identifier (SQL-standard): `"id"`, `"my col"`,
                // a column named like a keyword. The quotes are syntax; the
                // text is the column name, emitted as a plain ident token so
                // resolution treats it identically to a bare identifier. A
                // quoted path (`"a"."b"`, or a doubled quote) keeps its quotes:
                // resolveColumn binds it by segments.
                const start = self.pos;
                while (true) {
                    self.pos += 1;
                    while (self.pos < self.src.len and self.src[self.pos] != '"') self.pos += 1;
                    if (self.pos >= self.src.len) return error.UnterminatedString;
                    self.pos += 1; // consume closing quote
                    if (self.pos < self.src.len and self.src[self.pos] == '"') continue; // doubled quote
                    if (self.pos + 1 < self.src.len and self.src[self.pos] == '.' and self.src[self.pos + 1] == '"') {
                        self.pos += 1;
                        continue;
                    }
                    break;
                }
                return .{ .kind = .ident, .text = metadata.unquoteIdent(self.src[start..self.pos]) };
            },
            else => {},
        }
        if (isDigit(c)) {
            const start = self.pos;
            var saw_dot = false;
            while (self.pos < self.src.len) {
                const d = self.src[self.pos];
                if (isDigit(d)) {
                    self.pos += 1;
                } else if (d == '.' and !saw_dot) {
                    saw_dot = true;
                    self.pos += 1;
                } else break;
            }
            return .{
                .kind = if (saw_dot) .number_float else .number_int,
                .text = self.src[start..self.pos],
            };
        }
        if (isIdentStart(c)) {
            const start = self.pos;
            while (self.pos < self.src.len and isIdentCont(self.src[self.pos])) self.pos += 1;
            // A dotted path (`r.key`) names a nested leaf: one identifier.
            while (self.pos + 1 < self.src.len and self.src[self.pos] == '.' and isIdentStart(self.src[self.pos + 1])) {
                self.pos += 1;
                while (self.pos < self.src.len and isIdentCont(self.src[self.pos])) self.pos += 1;
            }
            return .{ .kind = .ident, .text = self.src[start..self.pos] };
        }
        return error.UnexpectedChar;
    }
};

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}
fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}
fn isIdentStart(c: u8) bool {
    return (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or c == '_';
}
fn isIdentCont(c: u8) bool {
    return isIdentStart(c) or isDigit(c);
}

/// Parse a comma-separated list of `expr [AS alias]` items.
pub fn parseSelect(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error![]ast.SelectItem {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;

    var items: std.ArrayList(ast.SelectItem) = .empty;
    var lex: Lexer = .{ .src = trimmed, .diag = diag };

    while (true) {
        const e = try parseExpr(arena, &lex, file);
        var alias: ?[]const u8 = null;
        const after = try lex.peek();
        if (after.kind == .ident and asciiEqIgnoreCase(after.text, "AS")) {
            _ = try lex.next();
            const id = try lex.next();
            if (id.kind != .ident) return error.ExpectedIdentifier;
            alias = id.text;
        }
        try items.append(arena, .{ .expr = e, .alias = alias });

        const sep = try lex.next();
        switch (sep.kind) {
            .eof => break,
            .comma => continue,
            else => return error.TrailingTokens,
        }
    }
    return items.items;
}

pub fn parseGroupBy(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error![]ast.SelectItem {
    return parseGroupByOf(arena, src, file, diag, &.{});
}

/// `parseGroupBy` for a SQL GROUP BY over `select`: a bare name no column has, but a SELECT item's alias gives
/// (case-insensitively), is that item's expression, as DuckDB binds `SELECT URL AS Dst ... GROUP BY Dst`. A column of
/// the name wins over the alias. An alias of an aggregate cannot be a key.
pub fn parseGroupByOf(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
    select: []const ast.SelectColumn,
) Error![]ast.SelectItem {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;

    var items: std.ArrayList(ast.SelectItem) = .empty;
    var lex: Lexer = .{ .src = trimmed, .diag = diag };

    while (true) {
        const e = if (try aliasKey(arena, &lex, file, select)) |aliased| aliased else try parseExpr(arena, &lex, file);
        var alias: ?[]const u8 = null;
        const after = try lex.peek();
        if (after.kind == .ident and asciiEqIgnoreCase(after.text, "AS")) {
            _ = try lex.next();
            const id = try lex.next();
            if (id.kind != .ident) return error.ExpectedIdentifier;
            alias = id.text;
        }
        try items.append(arena, .{ .expr = e, .alias = alias });

        const sep = try lex.next();
        switch (sep.kind) {
            .eof => break,
            .comma => continue,
            else => return error.TrailingTokens,
        }
    }
    return items.items;
}

/// The SELECT item a GROUP BY term at `lex` names by its alias, consumed: a lone undotted name, no column's, that
/// ends the term. Null (and `lex` untouched) for any other term.
fn aliasKey(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData, select: []const ast.SelectColumn) Error!?ast.Expr {
    if (select.len == 0) return null;
    const save = lex.pos;
    const tok = try lex.next();
    const end = try lex.peek();
    lex.pos = save;
    if (tok.kind != .ident or std.mem.indexOfScalar(u8, tok.text, '.') != null) return null;
    if (end.kind != .comma and end.kind != .eof) return null;
    if (metadata.resolveColumn(file, tok.text)) |_| return null else |err| if (err != error.UnknownColumn) return null;
    for (select) |item| {
        const alias = item.alias orelse continue;
        if (!asciiEqIgnoreCase(std.mem.trim(u8, alias, " "), tok.text)) continue;
        if (item.role == .agg) return error.AggregateInGroupBy;
        _ = try lex.next();
        return try parseExprOnly(arena, item.expr, file, lex.diag);
    }
    return null;
}

/// Parse a single expression. Convenience for callers that already
/// know they only have one (e.g. tests).
pub fn parseExprOnly(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error!ast.Expr {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;
    var lex: Lexer = .{ .src = trimmed, .diag = diag };
    const e = try parseExpr(arena, &lex, file);
    const tail = try lex.next();
    if (tail.kind != .eof) return error.TrailingTokens;
    return e;
}

fn parseExpr(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData) Error!ast.Expr {
    var left = try parseTerm(arena, lex, file);
    while (true) {
        const tk = try lex.peek();
        const op: ast.Op = switch (tk.kind) {
            .plus => .add,
            .minus => .sub,
            // SQL `||` binds at the same precedence as `+`/`-`. Strings
            // and numerics live in disjoint type lanes — the eval layer
            // surfaces TypeMismatch if a user mixes them.
            .pipe_pipe => .concat,
            else => return left,
        };
        _ = try lex.next();
        const right = try parseTerm(arena, lex, file);
        left = try makeBinop(arena, op, left, right);
    }
}

fn parseTerm(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData) Error!ast.Expr {
    var left = try parseFactor(arena, lex, file);
    while (true) {
        const tk = try lex.peek();
        const op: ast.Op = switch (tk.kind) {
            .star => .mul,
            .slash => .div,
            else => return left,
        };
        _ = try lex.next();
        const right = try parseFactor(arena, lex, file);
        left = try makeBinop(arena, op, left, right);
    }
}

fn parseFactor(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData) Error!ast.Expr {
    const tk = try lex.next();
    switch (tk.kind) {
        .minus => {
            // Unary minus: parse the next factor and negate. Folds
            // numeric literals at parse time so `-5` lands as a single
            // literal node; non-literal sub-exprs become `0 - expr`.
            //
            // Self-recursive, so it needs the parse-recursion guard parens get: `- - - x` overflows the parser's own
            // stack before any node exists to measure.
            lex.depth += 1;
            if (lex.depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
            const inner = try parseFactor(arena, lex, file);
            lex.depth -= 1;
            switch (inner) {
                .literal => |lit| switch (lit) {
                    .i64 => |v| return .{ .literal = .{ .i64 = -v } },
                    .f64 => |v| return .{ .literal = .{ .f64 = -v } },
                    .dec => |d| return .{ .literal = .{
                        .dec = .{ .unscaled = -d.unscaled, .spec = d.spec, .f = -d.f },
                    } },
                    .str => return error.TypeMismatch,
                },
                else => {
                    try rejectUnsigned64Operand(inner);
                    const lp = try arena.create(ast.Expr);
                    lp.* = .{ .literal = .{ .i64 = 0 } };
                    const rp = try arena.create(ast.Expr);
                    rp.* = inner;
                    const result_type = ast.Type.promote(.i64, inner.typeOf()) orelse return error.TypeMismatch;
                    if (result_type == .str) return error.TypeMismatch;
                    const depth = 1 + inner.depth();
                    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
                    // Negation can't grow a value, so a DECIMAL keeps its operand's type (as SQL engines type `-x`).
                    return .{ .binop = .{
                        .op = .sub,
                        .left = lp,
                        .right = rp,
                        .result_type = result_type,
                        .depth = depth,
                        .dec = if (result_type == .dec) inner.decSpec() else null,
                    } };
                },
            }
        },
        .number_int => {
            const v = std.fmt.parseInt(i64, tk.text, 10) catch return error.BadNumber;
            return .{ .literal = .{ .i64 = v } };
        },
        .number_float => {
            const v = std.fmt.parseFloat(f64, tk.text) catch return error.BadNumber;
            if (decimalLiteral(tk.text, v)) |d| return .{ .literal = .{ .dec = d } };
            return .{ .literal = .{ .f64 = v } };
        },
        .string => {
            return .{ .literal = .{ .str = tk.text } };
        },
        .ident => {
            // IDENT followed by `(` is a function call; otherwise it's
            // a column reference. Function names are matched
            // case-insensitively against `ast.Func`.
            const after = try lex.peek();
            if (after.kind == .lparen) {
                _ = try lex.next(); // consume '('
                return parseCall(arena, lex, file, tk.text);
            }
            if (asciiEqIgnoreCase(tk.text, "CASE")) return parseCase(arena, lex, file);
            const col_idx = metadata.resolveColumn(file, tk.text) catch |err| {
                if (err == error.AmbiguousColumn) Diag.set(lex.diag, tk.text, .{ .ambiguous_column = .{
                    .parser = .expression,
                    .hint = metadata.ambiguityHint(file, tk.text),
                } });
                return err;
            };
            const elem = metadata.leafSchemaElement(file, col_idx) orelse return error.UnknownColumn;
            const phys = elem.type orelse return error.UnsupportedColumnType;
            // DECIMAL columns are exact: expressions and aggregates read the unscaled integers the consumer decodes
            // beside the f64 view (see core/parquet/decimal.zig `decodeColumnExact`).
            var dec: ?ast.Dec = null;
            if (decimal_mod.exactKind(&elem)) |k| {
                if (!ast.Dec.valid(k.precision, k.scale)) return error.InvalidDecimalType;
                dec = .{ .precision = @intCast(k.precision), .scale = @intCast(k.scale) };
            }
            const expr_type: ast.Type = if (dec != null) .dec else if (schema.isFloat16(elem)) .f64 else switch (phys) {
                .INT32, .INT64 => .i64,
                .FLOAT, .DOUBLE => .f64,
                .BYTE_ARRAY => .str,
                // BOOLEAN aggregates/expressions treat the column as 0/1
                // (false/true), matching SQL: count(flag), sum(flag)=#true,
                // min/max(flag)=0/1. Widened to i64 in colRefForAgg.
                .BOOLEAN => .i64,
                // INT96 decodes to i64 epoch-nanoseconds (legacy timestamp).
                .INT96 => .i64,
                // FIXED_LEN_BYTE_ARRAY (non-decimal) decodes to raw bytes,
                // same runtime lane as BYTE_ARRAY.
                .FIXED_LEN_BYTE_ARRAY => .str,
            };
            return .{
                .col_ref = .{
                    .col_idx = col_idx,
                    .physical_type = phys,
                    .expr_type = expr_type,
                    // INT64-physical unsigned: the agg fold reads the i64 lane as u64 (≤32-bit unsigned already
                    // zero-extends at decode).
                    .unsigned_64 = phys == .INT64 and schema.isUnsignedInt64(elem),
                    .unsigned_32 = phys == .INT32 and schema.isUnsignedIntTo32(elem),
                    .dec = dec,
                    .nullable = blk: {
                        const path = (try metadata.leafPathSegments(arena, file, col_idx)) orelse break :blk true;
                        break :blk file.getColumnLevels(path).max_def > 0;
                    },
                },
            };
        },
        .lparen => {
            lex.depth += 1;
            if (lex.depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
            const inner = try parseExpr(arena, lex, file);
            lex.depth -= 1;
            const close = try lex.next();
            if (close.kind != .rparen) return error.ExpectedRParen;
            return inner;
        },
        .eof => return error.UnexpectedEnd,
        else => return error.UnexpectedChar,
    }
}

/// Parse a function call body: `arg1, arg2, ... )`. The opening `(`
/// has already been consumed. Resolves the function by name
/// (case-insensitive) and validates arity + arg types.
fn parseCall(
    arena: std.mem.Allocator,
    lex: *Lexer,
    file: *const schema.FileMetaData,
    name: []const u8,
) Error!ast.Expr {
    const func = resolveFunc(name) orelse return error.UnknownFunction;

    var args: std.ArrayList(*ast.Expr) = .empty;
    // Empty arg list: `func()`.
    const first = try lex.peek();
    if (first.kind == .rparen) {
        _ = try lex.next();
    } else {
        while (true) {
            const arg = try arena.create(ast.Expr);
            lex.depth += 1;
            if (lex.depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
            arg.* = try parseExpr(arena, lex, file);
            lex.depth -= 1;
            try rejectUnsigned64Operand(arg.*);
            try args.append(arena, arg);
            const sep = try lex.next();
            switch (sep.kind) {
                .comma => continue,
                .rparen => break,
                else => return error.ExpectedRParen,
            }
        }
    }

    const result_type = try resolveCallType(func, args.items);
    var dec: ?ast.Dec = null;
    if (result_type == .dec) {
        dec = args.items[0].decSpec().?;
        for (args.items[1..]) |a| dec = ast.Dec.common(dec.?, a.decSpec().?);
    }
    var deepest: u32 = 0;
    for (args.items) |a| deepest = @max(deepest, a.depth());
    const depth = 1 + deepest;
    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
    return .{ .call = .{
        .func = func,
        .args = args.items,
        .result_type = result_type,
        .depth = depth,
        .dec = dec,
    } };
}

/// A searched `CASE WHEN cond THEN value ... [ELSE value] END`, `CASE` already read. Each condition is a predicate
/// in the filter grammar, bound by the filter parser (its text runs to the THEN that closes it); each value an
/// expression or NULL. The result type is the branches' common one.
fn parseCase(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData) Error!ast.Expr {
    lex.depth += 1;
    defer lex.depth -= 1;
    if (lex.depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
    var whens: std.ArrayList(ast.Case.When) = .empty;
    var first = try lex.next();
    if (first.kind != .ident or !asciiEqIgnoreCase(first.text, "WHEN")) return error.UnsupportedCase;
    var else_: ?*ast.Expr = null;
    while (true) {
        const cond_start = lex.pos;
        const then_at = (try findThen(lex.src, cond_start)) orelse return error.UnsupportedCase;
        const cond = try filter_parser.parse(arena, lex.src[cond_start..then_at], file, lex.diag);
        lex.pos = then_at + "THEN".len;
        try whens.append(arena, .{ .cond = cond, .then = try caseValue(arena, lex, file) });
        first = try lex.next();
        if (first.kind != .ident) return error.UnsupportedCase;
        if (asciiEqIgnoreCase(first.text, "WHEN")) continue;
        if (asciiEqIgnoreCase(first.text, "ELSE")) {
            else_ = try caseValue(arena, lex, file);
            first = try lex.next();
        }
        if (first.kind != .ident or !asciiEqIgnoreCase(first.text, "END")) return error.UnsupportedCase;
        break;
    }
    // The common type of the non-NULL branches: strings with strings, numbers promoted; a DECIMAL result holds every
    // branch's digits (`Dec.common`), integers widened as SQL widens them.
    var result: ?ast.Type = null;
    var dec: ?ast.Dec = null;
    var deepest: u32 = 0;
    const Branches = struct {
        whens: []const ast.Case.When,
        else_: ?*ast.Expr,
        fn at(self: @This(), i: usize) ?*ast.Expr {
            return if (i < self.whens.len) self.whens[i].then else self.else_;
        }
    };
    const branches: Branches = .{ .whens = whens.items, .else_ = else_ };
    for (0..whens.items.len + 1) |i| {
        const v = branches.at(i) orelse continue;
        deepest = @max(deepest, v.depth());
        try rejectUnsigned64Operand(v.*);
        const t = v.typeOf();
        result = if (result) |r| ast.Type.promote(r, t) orelse return error.UnsupportedCase else t;
        if (v.decSpec()) |spec| dec = if (dec) |d| ast.Dec.common(d, spec) else spec;
    }
    const result_type = result orelse return error.UnsupportedCase;
    const depth = 1 + deepest;
    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
    return .{ .case_ = .{
        .whens = whens.items,
        .else_ = else_,
        .result_type = result_type,
        .depth = depth,
        .dec = if (result_type == .dec) dec else null,
    } };
}

/// A CASE branch value: NULL (null), or an expression.
fn caseValue(arena: std.mem.Allocator, lex: *Lexer, file: *const schema.FileMetaData) Error!?*ast.Expr {
    const tk = try lex.peek();
    if (tk.kind == .ident and asciiEqIgnoreCase(tk.text, "NULL")) {
        _ = try lex.next();
        return null;
    }
    const v = try arena.create(ast.Expr);
    v.* = try parseExpr(arena, lex, file);
    return v;
}

/// Where the THEN closing a WHEN condition that starts at `from` sits: the first outermost-level word THEN, quoted
/// spans and parentheses stepped over, and a nested CASE's own THENs skipped until its END.
fn findThen(src: []const u8, from: usize) Error!?usize {
    var i = from;
    var parens: usize = 0;
    var cases: usize = 0;
    while (i < src.len) {
        const c = src[i];
        if (c == '\'' or c == '"') {
            i += 1;
            while (i < src.len) : (i += 1) {
                if (src[i] != c) continue;
                if (i + 1 < src.len and src[i + 1] == c) i += 1 else break;
            }
            if (i >= src.len) return error.UnterminatedString;
            i += 1;
            continue;
        }
        if (c == '(') parens += 1;
        if (c == ')') parens -|= 1;
        if (parens == 0 and isIdentStart(c) and (i == 0 or !isIdentCont(src[i - 1]))) {
            const start = i;
            while (i < src.len and isIdentCont(src[i])) i += 1;
            const word = src[start..i];
            if (asciiEqIgnoreCase(word, "CASE")) cases += 1;
            if (asciiEqIgnoreCase(word, "END")) cases -|= 1;
            if (cases == 0 and asciiEqIgnoreCase(word, "THEN")) return start;
            continue;
        }
        i += 1;
    }
    return null;
}

fn resolveFunc(name: []const u8) ?ast.Func {
    if (asciiEqIgnoreCase(name, "coalesce")) return .coalesce;
    if (asciiEqIgnoreCase(name, "strlen")) return .strlen;
    return null;
}

/// Validate arity + argument types per function and return the result
/// type. coalesce requires ≥2 args, all promotable to a common type.
fn resolveCallType(func: ast.Func, args: []const *ast.Expr) Error!ast.Type {
    switch (func) {
        .coalesce => {
            if (args.len < 2) return error.WrongArity;
            // Reduce arg types via Type.promote — every cross-lane
            // mix is rejected. Result type is the common lane.
            var t = args[0].typeOf();
            for (args[1..]) |a| {
                t = ast.Type.promote(t, a.typeOf()) orelse return error.TypeMismatch;
            }
            return t;
        },
        .strlen => {
            if (args.len != 1) return error.WrongArity;
            if (args[0].typeOf() != .str) return error.TypeMismatch;
            return .i64;
        },
    }
}

fn makeBinop(arena: std.mem.Allocator, op: ast.Op, l: ast.Expr, r: ast.Expr) Error!ast.Expr {
    try rejectUnsigned64Operand(l);
    try rejectUnsigned64Operand(r);
    var result_type = ast.Type.promote(l.typeOf(), r.typeOf()) orelse return error.TypeMismatch;
    // Op/type compatibility first: `concat` is string-only, arithmetic ops numeric-only. A `||` over DECIMAL operands
    // must stop here, before the DECIMAL result spec below.
    if (op == .concat and result_type != .str) return error.TypeMismatch;
    if (op != .concat and result_type == .str) return error.TypeMismatch;
    // Exact division has no closed result scale; DECIMAL `/` is DOUBLE, as DuckDB types it.
    if (op == .div and result_type == .dec) result_type = .f64;
    const dec: ?ast.Dec = if (result_type != .dec) null else switch (op) {
        .add, .sub => ast.Dec.addSub(l.decSpec().?, r.decSpec().?),
        .mul => try ast.Dec.mul(l.decSpec().?, r.decSpec().?),
        .div, .concat => unreachable, // `.div` became f64 and `.concat` returned above
    };

    // Tree height, not parser recursion: `a + 1 + 1 + ...` parses in a loop but still grows the left spine, which the
    // evaluator does recurse over. Checking as we build rejects a runaway chain before the whole AST is allocated.
    const depth = 1 + @max(l.depth(), r.depth());
    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;

    const lp = try arena.create(ast.Expr);
    const rp = try arena.create(ast.Expr);
    lp.* = l;
    rp.* = r;
    return .{ .binop = .{
        .op = op,
        .left = lp,
        .right = rp,
        .result_type = result_type,
        .depth = depth,
        .dec = dec,
    } };
}

/// `text` (digits with one `.`) as the exact DECIMAL SQL gives it: every digit counts toward the precision and those
/// after the point are the scale, so `0.05` is DECIMAL(3,2). Past 38 digits it stays DOUBLE, as DuckDB has it.
fn decimalLiteral(text: []const u8, f: f64) ?ast.DecLit {
    var unscaled: i128 = 0;
    var digits: u8 = 0;
    var scale: u8 = 0;
    var after_point = false;
    for (text) |c| {
        if (c == '.') {
            after_point = true;
            continue;
        }
        if (digits == ast.Dec.max_precision) return null;
        unscaled = unscaled * 10 + (c - '0');
        digits += 1;
        if (after_point) scale += 1;
    }
    return .{ .unscaled = unscaled, .spec = .{ .precision = @max(digits, 1), .scale = scale }, .f = f };
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

// ============================================================
// Aggregate parser — `--aggregate "sum(cost) FILTER (WHERE foo = 'bar') AS x, ..."`
// ============================================================

/// Parse a comma-separated list of aggregate calls.
pub fn parseAggList(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
    diag: ?*Diag,
) Error![]agg.AggCall {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyAggregate;
    var lex: Lexer = .{ .src = trimmed, .diag = diag };
    var items: std.ArrayList(agg.AggCall) = .empty;
    while (true) {
        const call = try parseAggCall(arena, &lex, file);
        try items.append(arena, call);
        const sep = try lex.next();
        switch (sep.kind) {
            .eof => break,
            .comma => continue,
            else => return error.TrailingTokens,
        }
    }
    return items.items;
}

fn parseAggCall(
    arena: std.mem.Allocator,
    lex: *Lexer,
    file: *const schema.FileMetaData,
) Error!agg.AggCall {
    // 1. Function name (sum, count, min, max, avg).
    const name_tok = try lex.next();
    if (name_tok.kind != .ident) return error.ExpectedAggFunc;
    const call_start = @intFromPtr(name_tok.text.ptr) - @intFromPtr(lex.src.ptr);
    var func = resolveAggFunc(name_tok.text) orelse return error.UnknownAggFunc;

    // 2. Open paren.
    const lp = try lex.next();
    if (lp.kind != .lparen) return error.ExpectedLParen;

    // 3. Argument: `*` only valid for count; otherwise a regular expression.
    var arg: ?ast.Expr = null;
    const peek = try lex.peek();
    if (peek.kind == .star) {
        if (func != .count) return error.StarOnlyValidInCount;
        _ = try lex.next();
    } else {
        if (func == .count and peek.kind == .ident and asciiEqIgnoreCase(peek.text, "DISTINCT")) {
            _ = try lex.next();
            func = .count_distinct;
        }
        lex.depth += 1;
        if (lex.depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
        arg = try parseExpr(arena, lex, file);
        lex.depth -= 1;
    }

    const rp = try lex.next();
    if (rp.kind != .rparen) return error.ExpectedRParen;

    // 4. Optional FILTER (WHERE pred). The predicate is parsed by the
    //    `filter` parser (same syntax as `--filter`). We delimit the
    //    body by paren-depth scanning over the source string, then
    //    strip the leading WHERE keyword.
    var where: ?filter_ast.Filter = null;
    const after_call = try lex.peek();
    if (after_call.kind == .ident and asciiEqIgnoreCase(after_call.text, "FILTER")) {
        _ = try lex.next();
        const lp2 = try lex.next();
        if (lp2.kind != .lparen) return error.ExpectedLParen;

        // The matching closing paren, stepping over quoted spans (a doubled quote escaping itself), so a parenthesis
        // inside a string literal or quoted name neither opens nor closes the body.
        const body_start = lex.pos;
        var depth: usize = 1;
        var i = body_start;
        while (i < lex.src.len) : (i += 1) {
            switch (lex.src[i]) {
                '(' => depth += 1,
                ')' => {
                    depth -= 1;
                    if (depth == 0) break;
                },
                '\'', '"' => |q| {
                    i += 1;
                    while (i < lex.src.len) : (i += 1) {
                        if (lex.src[i] != q) continue;
                        if (i + 1 < lex.src.len and lex.src[i + 1] == q) i += 1 else break;
                    }
                },
                else => {},
            }
        }
        if (depth != 0) return error.ExpectedRParen;
        const body = std.mem.trim(u8, lex.src[body_start..i], " \t\r\n");
        lex.pos = i + 1; // skip closing `)`

        if (body.len < 6 or !asciiEqIgnoreCase(body[0..5], "WHERE")) return error.ExpectedWhere;
        const pred_str = std.mem.trim(u8, body[5..], " \t\r\n");
        where = try filter_parser.parse(arena, pred_str, file, lex.diag);
    }

    // 5. Optional AS alias. Default = function name (sum / count / ...), with the call's text kept for when that
    //    name is not unique (see `default_name`).
    var alias: []const u8 = name_tok.text;
    var default_name: ?[]const u8 = std.mem.trim(u8, lex.src[call_start..lex.pos], " \t\r\n");
    const after_filter = try lex.peek();
    if (after_filter.kind == .ident and asciiEqIgnoreCase(after_filter.text, "AS")) {
        _ = try lex.next();
        const alias_tok = try lex.next();
        if (alias_tok.kind != .ident) return error.ExpectedIdentifier;
        alias = alias_tok.text;
        default_name = null;
    }

    // 6. Resolve result shape from func + arg type.
    const result = try agg.resolveResult(func, arg);
    const shifted = if (func != .sum) null else if (arg) |a| sumOffset(a) else null;

    return .{
        .func = func,
        .arg = if (shifted) |s| s.col.* else arg,
        .where = where,
        .alias = alias,
        .default_name = default_name,
        .result = result,
        .offset = if (shifted) |s| s.offset else 0,
    };
}

/// `sum(x + c)`, `sum(c + x)` or `sum(x - c)` as `x` and `c`, for `sum(x) + c·count(x)`: one pass over `x` instead of
/// a column of `x + c` per call. Only where `x + c` cannot overflow a row's 64 bits, so no query that errors today
/// answers: `x` a column of at most 32 bits, `|c|` at most 2^62.
fn sumOffset(arg: ast.Expr) ?struct { col: *ast.Expr, offset: i64 } {
    if (arg != .binop) return null;
    const b = arg.binop;
    if (b.op != .add and b.op != .sub) return null;
    const col, const lit = if (b.left.* == .col_ref) .{ b.left, b.right.* } else if (b.op == .add) .{ b.right, b.left.* } else return null;
    if (col.* != .col_ref or col.col_ref.physical_type != .INT32 or col.col_ref.expr_type != .i64) return null;
    if (lit != .literal or lit.literal != .i64) return null;
    const c = lit.literal.i64;
    if (@abs(c) > 1 << 62) return null;
    return .{ .col = col, .offset = if (b.op == .add) c else -c };
}

fn resolveAggFunc(name: []const u8) ?agg.AggFunc {
    if (asciiEqIgnoreCase(name, "count")) return .count;
    if (asciiEqIgnoreCase(name, "count_distinct")) return .count_distinct;
    if (asciiEqIgnoreCase(name, "sum")) return .sum;
    if (asciiEqIgnoreCase(name, "min")) return .min;
    if (asciiEqIgnoreCase(name, "max")) return .max;
    if (asciiEqIgnoreCase(name, "avg")) return .avg;
    return null;
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;
const test_fixtures = @import("../parquet/test_fixtures.zig");
const filter_eval_mod = @import("../filter/eval.zig");

fn fakeFile(arena: std.mem.Allocator, names: []const []const u8, types: []const schema.Type) !schema.FileMetaData {
    // Build a minimal FileMetaData with a flat schema: one root group +
    // one leaf per name. Enough for findColumnIndex / getColumnSchema.
    var elems: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try elems.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(names.len),
        .scale = null,
        .precision = null,
        .field_id = null,
    });
    for (names, types) |n, t| {
        try elems.append(arena, .{
            .type = t,
            .type_length = null,
            .repetition_type = .REQUIRED,
            .name = n,
            .num_children = 0,
            .scale = null,
            .precision = null,
            .field_id = null,
        });
    }
    return .{
        .version = 1,
        .schema = elems,
        .num_rows = 0,
        .row_groups = .empty,
        .created_by = null,
    };
}

test "parse: nesting past MAX_EXPR_DEPTH is rejected" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});

    // Parenthesis nesting trips the parser-recursion guard, before any AST node exists to measure.
    const n = MAX_EXPR_DEPTH + 1;
    const parens = try a.alloc(u8, n * 2 + 1);
    @memset(parens[0..n], '(');
    parens[n] = '1';
    @memset(parens[n + 1 ..], ')');
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, parens, &file, null));

    // A left-associative chain is parsed by a loop, not recursion, so only the tree-height check catches it.
    var chain: std.ArrayList(u8) = .empty;
    try chain.appendSlice(a, "1");
    for (0..n) |_| try chain.appendSlice(a, " + 1");
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, chain.items, &file, null));

    // A run of unary minus is self-recursive and hits the same guard.
    const minuses = try a.alloc(u8, n + 1);
    @memset(minuses[0..n], '-');
    minuses[n] = '1';
    try testing.expectError(error.ExpressionTooDeep, parseExprOnly(a, minuses, &file, null));

    // Just inside the cap still parses.
    const ok_parens = try a.alloc(u8, (MAX_EXPR_DEPTH - 1) * 2 + 1);
    @memset(ok_parens[0 .. MAX_EXPR_DEPTH - 1], '(');
    ok_parens[MAX_EXPR_DEPTH - 1] = '1';
    @memset(ok_parens[MAX_EXPR_DEPTH ..], ')');
    _ = try parseExprOnly(a, ok_parens, &file, null);
}

test "parse: literal int" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "42", &file, null);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(@as(i64, 42), e.literal.i64);
}

test "parse: literal float" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    // A literal with a decimal point is an exact DECIMAL, as SQL types it, with its double alongside.
    const e = try parseExprOnly(a, "3.14", &file, null);
    try testing.expectEqual(ast.Type.dec, e.typeOf());
    try testing.expectEqual(@as(i128, 314), e.literal.dec.unscaled);
    try testing.expectEqual(ast.Dec{ .precision = 3, .scale = 2 }, e.literal.dec.spec);
    try testing.expectEqual(@as(f64, 3.14), e.literal.dec.f);
}

test "parse: column ref resolves" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT32, .DOUBLE });
    const ex = try parseExprOnly(a, "x", &file, null);
    try testing.expectEqual(ast.Type.i64, ex.typeOf()); // INT32 promoted
    try testing.expectEqual(@as(usize, 0), ex.col_ref.col_idx);
    const ey = try parseExprOnly(a, "y", &file, null);
    try testing.expectEqual(ast.Type.f64, ey.typeOf());
    try testing.expectEqual(@as(usize, 1), ey.col_ref.col_idx);
}

test "parse: precedence — mul before add" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "x + y * z", &file, null);
    // Should parse as `x + (y * z)`.
    try testing.expectEqual(ast.Op.add, e.binop.op);
    try testing.expectEqual(ast.Op.mul, e.binop.right.binop.op);
}

test "parse: parens override precedence" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "(x + y) * z", &file, null);
    try testing.expectEqual(ast.Op.mul, e.binop.op);
    try testing.expectEqual(ast.Op.add, e.binop.left.binop.op);
}

test "parse: select list with alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT64, .INT64 });
    const items = try parseSelect(a, "x AS first, x + y AS sum", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqualStrings("first", items[0].alias.?);
    try testing.expectEqualStrings("sum", items[1].alias.?);
    try testing.expectEqual(ast.Op.add, items[1].expr.binop.op);
}

test "parse: type promotion in binop" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "f" }, &.{ .INT64, .DOUBLE });
    const e = try parseExprOnly(a, "i + f", &file, null);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: unknown column errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "y + 1", &file, null));
}

test "parse: string literal" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "'hello'", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqualStrings("hello", e.literal.str);
}

test "parse: string column ref" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    const e = try parseExprOnly(a, "name", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
}

test "parse: || string concat" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "first", "last" }, &.{ .BYTE_ARRAY, .BYTE_ARRAY });
    const e = try parseExprOnly(a, "first || ' ' || last", &file, null);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqual(ast.Op.concat, e.binop.op);
}

test "parse: type mismatch — string + number errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "name + 1", &file, null));
}

test "parse: `||` over numbers, DECIMAL ones included, is a type error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    // A decimal literal makes the operands DECIMAL; this reached `unreachable` in the DECIMAL result spec.
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1.5 || 2", &file, null));
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1 || 2.25", &file, null));
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "1 || 2", &file, null));
}

test "parse and evaluate: searched CASE takes the first TRUE WHEN and evaluates branches only where taken" {
    const expr_eval = @import("eval.zig");
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const file = try fakeFile(a, &.{ "a", "s" }, &.{ .INT64, .BYTE_ARRAY });
    const av = [_]i64{ 1, 3, 4, 5 };
    const sv = [_][]const u8{ "xa", "b", "c", "xd" };
    const batch: filter_eval_mod.Batch = .{ .cols = &.{ .{ .i64 = .{ .values = &av } }, .{ .string = .{ .values = &sv } } }, .num_rows = 4 };
    const lookup = [_]?usize{ 0, 1 };

    // The row a = 3 divides by zero in the second branch, which the first WHEN takes it from.
    const num = try parseExprOnly(a, "CASE WHEN a = 3 THEN 0 WHEN a > 2 THEN 10 / (a - 3) ELSE -1 END", &file, null);
    const got = try expr_eval.evalExpr(a, &batch, &lookup, num);
    try testing.expectEqualSlices(i64, &.{ -1, 0, 10, 5 }, got.i64.values);
    try testing.expect(got.i64.def_levels == null);
    // Without ELSE a row no WHEN takes is NULL; a string literal joins a string column.
    const str = try parseExprOnly(a, "CASE WHEN s LIKE 'x%' AND (a < 2 OR a > 4) THEN s WHEN a = 3 THEN 'three' END", &file, null);
    const sg = try expr_eval.evalExpr(a, &batch, &lookup, str);
    try testing.expectEqualStrings("xa", sg.string.values[0]);
    try testing.expectEqualStrings("three", sg.string.values[1]);
    try testing.expectEqualSlices(u32, &.{ 1, 1, 0, 1 }, sg.string.def_levels.?);
    try testing.expect(str.mayBeNull());
    try testing.expect(!num.mayBeNull());
    // A DECIMAL and an integer branch meet in the DECIMAL holding both, the integer rescaled exactly; the same CASE
    // parsed twice is one bound expression.
    const mixed = try parseExprOnly(a, "CASE WHEN a > 3 THEN a * 1.5 ELSE 2 END", &file, null);
    try testing.expectEqual(ast.Type.dec, mixed.typeOf());
    const mg = try expr_eval.evalExpr(a, &batch, &lookup, mixed);
    try testing.expectEqualSlices(i128, &.{ 20, 20, 60, 75 }, mg.i128.values);
    try testing.expect(str.eql(try parseExprOnly(a, "case when s like 'x%' and (a < 2 or a > 4) then s when a = 3 then 'three' end", &file, null)));
    try testing.expect(!str.eql(mixed));

    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE a WHEN 1 THEN 2 END", &file, null));
    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE WHEN a > 1 THEN s ELSE 1 END", &file, null));
    try testing.expectError(error.UnsupportedCase, parseExprOnly(a, "CASE WHEN a > 1 THEN NULL END", &file, null));
}

test "parse: coalesce(col, default)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "coalesce(x, 0)", &file, null);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
    try testing.expectEqual(@as(usize, 2), e.call.args.len);
}

test "parse: coalesce is case-insensitive" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "COALESCE(x, 0)", &file, null);
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
}

test "parse: coalesce promotes int+float arg types to f64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "f" }, &.{ .INT64, .DOUBLE });
    const e = try parseExprOnly(a, "coalesce(i, f)", &file, null);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: coalesce arity error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.WrongArity, parseExprOnly(a, "coalesce(x)", &file, null));
}

test "parse: unknown function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownFunction, parseExprOnly(a, "abs(x)", &file, null));
}

test "parse: incomplete trailing operator fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnexpectedEnd, parseExprOnly(a, "x +", &file, null));
}

test "parse: extra trailing token fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.TrailingTokens, parseExprOnly(a, "x 1", &file, null));
}

test "agg: count(*) with default alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const items = try parseAggList(a, "count(*)", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(agg.AggFunc.count, items[0].func);
    try testing.expectEqualStrings("count", items[0].alias);
    try testing.expect(items[0].arg == null);
    try testing.expect(items[0].where == null);
}

test "agg: count(DISTINCT col) and count_distinct(col)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"uid"}, &.{.INT64});
    const items1 = try parseAggList(a, "count(DISTINCT uid) AS u", &file, null);
    try testing.expectEqual(@as(usize, 1), items1.len);
    try testing.expectEqual(agg.AggFunc.count_distinct, items1[0].func);
    try testing.expectEqualStrings("u", items1[0].alias);

    const items2 = try parseAggList(a, "count_distinct(uid)", &file, null);
    try testing.expectEqual(@as(usize, 1), items2.len);
    try testing.expectEqual(agg.AggFunc.count_distinct, items2[0].func);
    try testing.expectEqualStrings("count_distinct", items2[0].alias);
}

test "agg: sum with column ref + alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"cost"}, &.{.DOUBLE});
    const items = try parseAggList(a, "sum(cost) AS total", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(agg.AggFunc.sum, items[0].func);
    try testing.expectEqualStrings("total", items[0].alias);
    try testing.expectEqual(agg.ResultShape.f64, items[0].result);
}

test "agg: avg result shape is avg_f64 (split-output)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"cost"}, &.{.DOUBLE});
    const items = try parseAggList(a, "avg(cost) AS mean", &file, null);
    try testing.expectEqual(agg.ResultShape.avg_f64, items[0].result);
}

test "agg: FILTER (WHERE pred) attaches a predicate" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "sum(cost) FILTER (WHERE service = ec2) AS ec2_cost", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expect(items[0].where != null);
    try testing.expect(items[0].where.? == .string);
    try testing.expectEqualStrings("ec2_cost", items[0].alias);
}

test "agg: multiple FILTER aggs in one list" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "svc", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(
        a,
        "sum(cost) FILTER (WHERE svc = ec2) AS ec2, sum(cost) FILTER (WHERE svc = s3) AS s3, count(*) AS rows",
        &file,
        null,
    );
    try testing.expectEqual(@as(usize, 3), items.len);
    try testing.expectEqualStrings("ec2", items[0].alias);
    try testing.expectEqualStrings("s3", items[1].alias);
    try testing.expectEqualStrings("rows", items[2].alias);
    try testing.expect(items[0].where != null);
    try testing.expect(items[1].where != null);
    try testing.expect(items[2].where == null);
}

test "agg: star only valid in count" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    try testing.expectError(error.StarOnlyValidInCount, parseAggList(a, "sum(*)", &file, null));
}

test "agg: unknown agg function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownAggFunc, parseAggList(a, "median(x)", &file, null));
}

test "agg: case-insensitive keywords" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "SUM(cost) filter (where service = ec2) as total", &file, null);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqualStrings("total", items[0].alias);
    try testing.expect(items[0].where != null);
}

test "agg: double-quoted column identifier resolves like bare" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    // `"cost"` (SQL-standard quoting) must parse identically to bare `cost`.
    const items = try parseAggList(a, "sum(\"cost\") AS c, max(\"cost\") AS m", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqualStrings("c", items[0].alias);
    try testing.expectEqualStrings("m", items[1].alias);
}

test "group-by: bare column and AS alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "region", "kind" }, &.{ .INT32, .BYTE_ARRAY });
    const items = try parseGroupBy(a, "region, kind AS k", &file, null);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqual(@as(?[]const u8, null), items[0].alias);
    try testing.expectEqualStrings("k", items[1].alias.?);
}

test "parse: a UINT64 column may stand alone but is refused as an operand" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "u", "w" }, &.{ .INT64, .INT32 });
    file.schema.items[1].logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };
    file.schema.items[2].converted_type = .UINT_32;

    const bare = try parseSelect(a, "u AS b", &file, null);
    try testing.expect(bare[0].expr.col_ref.unsigned_64);
    _ = try parseAggList(a, "sum(u) AS s, max(u) AS m", &file, null);
    _ = try parseExprOnly(a, "w + 1", &file, null); // UINT32 zero-extends into i64 exactly

    for ([_][]const u8{ "u + 1", "1 * u", "-u", "coalesce(u, 0)", "(u) - 2" }) |src| {
        try testing.expectError(error.Unsigned64Operand, parseExprOnly(a, src, &file, null));
    }
    try testing.expectError(error.Unsigned64Operand, parseAggList(a, "sum(u + 1) AS s", &file, null));
    try testing.expectError(error.Unsigned64Operand, parseGroupBy(a, "u * 2 AS k", &file, null));
}

test "parse: bare names bind top-level columns, dotted paths nested leaves" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try test_fixtures.sharedLeafNameMeta(a);

    const aggs = try parseAggList(
        a,
        "sum(key) AS s, sum(r.key) AS t, count(*) FILTER (WHERE key > 1005) AS n",
        &file,
        null,
    );
    try testing.expectEqual(@as(usize, 2), aggs[0].arg.?.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), aggs[1].arg.?.col_ref.col_idx);

    const sel = try parseSelect(a, "key + r.key AS both, \"r.name\" AS n", &file, null);
    try testing.expectEqual(@as(usize, 2), sel[0].expr.binop.left.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), sel[0].expr.binop.right.col_ref.col_idx);
    try testing.expectEqual(ast.Type.str, sel[1].expr.typeOf());

    const keys = try parseGroupBy(a, "key, r.key", &file, null);
    try testing.expectEqual(@as(usize, 2), keys[0].expr.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), keys[1].expr.col_ref.col_idx);

    try testing.expectError(error.AmbiguousColumn, parseExprOnly(a, "x", &file, null));
    var diag: Diag = .{};
    try testing.expectError(error.AmbiguousColumn, parseAggList(a, "sum(x) AS s", &file, &diag));
    try testing.expectEqualStrings("x", diag.column.get());
    try testing.expect(diag.detail.ambiguous_column.parser == .expression);
    // A per-aggregate FILTER is the filter parser's.
    try testing.expectError(error.AmbiguousColumn, parseAggList(a, "count(*) FILTER (WHERE x = 1) AS n", &file, &diag));
    try testing.expect(diag.detail.ambiguous_column.parser == .filter);
    try testing.expectEqual(@as(usize, 5), (try parseExprOnly(a, "b.x", &file, null)).col_ref.col_idx);
    // A dot that does not continue an identifier is still an error, not part of the name.
    try testing.expectError(error.UnexpectedChar, parseExprOnly(a, "key.", &file, null));
}

test "parse: a quoted path binds the nested leaf a top-level column's dotted name hides" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try test_fixtures.sharedLeafNameMeta(a);
    file.schema.items[4].name = "r.key"; // the top-level `key` becomes `r.key`, beside the group r's field key

    try testing.expectEqual(@as(usize, 2), (try parseExprOnly(a, "r.key", &file, null)).col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), (try parseExprOnly(a, "\"r.key\"", &file, null)).col_ref.col_idx);
    try testing.expectEqual(@as(usize, 0), (try parseExprOnly(a, "\"r\".\"key\"", &file, null)).col_ref.col_idx);
    const sel = try parseSelect(a, "\"r\".\"key\" + \"r.key\" AS s, \"r\".\"name\"", &file, null);
    try testing.expectEqual(@as(usize, 0), sel[0].expr.binop.left.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), sel[0].expr.binop.right.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 1), sel[1].expr.col_ref.col_idx);
    const keys = try parseGroupBy(a, "\"r\".\"key\", \"r.key\"", &file, null);
    try testing.expectEqual(@as(usize, 0), keys[0].expr.col_ref.col_idx);
    try testing.expectEqual(@as(usize, 2), keys[1].expr.col_ref.col_idx);
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "\"r\".\"nope\"", &file, null));
    try testing.expectError(error.UnterminatedString, parseExprOnly(a, "\"r\".\"key", &file, null));
}

test "parse: DECIMAL columns are exact, typed as DuckDB and Trino type their arithmetic" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "p", "d", "i", "b", "f" }, &.{ .INT64, .INT64, .INT32, .INT64, .DOUBLE });
    file.schema.items[1].logical_type = .{ .DECIMAL = .{ .precision = 15, .scale = 2 } };
    file.schema.items[2].logical_type = .{ .DECIMAL = .{ .precision = 15, .scale = 2 } };
    const Case = struct { src: []const u8, t: ast.Type, dec: ?ast.Dec = null };
    const cases = [_]Case{
        .{ .src = "p", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "p * d", .t = .dec, .dec = .{ .precision = 30, .scale = 4 } },
        .{ .src = "1 - d", .t = .dec, .dec = .{ .precision = 16, .scale = 2 } },
        .{ .src = "p * (1 - d) * (1 + d)", .t = .dec, .dec = .{ .precision = 38, .scale = 6 } },
        .{ .src = "p + i", .t = .dec, .dec = .{ .precision = 16, .scale = 2 } },
        .{ .src = "p + b", .t = .dec, .dec = .{ .precision = 22, .scale = 2 } },
        .{ .src = "p * b", .t = .dec, .dec = .{ .precision = 34, .scale = 2 } },
        .{ .src = "p * 0.5", .t = .dec, .dec = .{ .precision = 17, .scale = 3 } },
        .{ .src = "-p", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "coalesce(p, i)", .t = .dec, .dec = .{ .precision = 15, .scale = 2 } },
        .{ .src = "i * 1.5", .t = .dec, .dec = .{ .precision = 12, .scale = 1 } },
        // Division and DOUBLE operands leave exact arithmetic; integers alone stay integers.
        .{ .src = "p / d", .t = .f64 },
        .{ .src = "i / 1.0", .t = .f64 },
        .{ .src = "p + f", .t = .f64 },
        .{ .src = "i + b", .t = .i64 },
    };
    for (cases) |c| {
        const e = try parseExprOnly(a, c.src, &file, null);
        testing.expectEqual(c.t, e.typeOf()) catch |err| {
            std.debug.print("{s}: {s}\n", .{ c.src, @tagName(e.typeOf()) });
            return err;
        };
        if (c.dec) |want| try testing.expectEqual(want, e.decSpec().?);
    }
}

test "parse: DECIMAL limits — declared types, literal width, product scale" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    var file = try fakeFile(a, &.{ "neg", "wide", "s30" }, &.{ .INT64, .FIXED_LEN_BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY });
    // Parquet allows no negative scale and no precision past 38: such a column is refused, not approximated.
    file.schema.items[1].logical_type = .{ .DECIMAL = .{ .precision = 10, .scale = -2 } };
    file.schema.items[2].logical_type = .{ .DECIMAL = .{ .precision = 39, .scale = 0 } };
    file.schema.items[3].logical_type = .{ .DECIMAL = .{ .precision = 38, .scale = 30 } };
    try testing.expectError(error.InvalidDecimalType, parseExprOnly(a, "neg", &file, null));
    try testing.expectError(error.InvalidDecimalType, parseExprOnly(a, "wide + 1", &file, null));
    // Scales add under `*`: 30 + 30 has no DECIMAL.
    try testing.expectError(error.DecimalScaleTooLarge, parseExprOnly(a, "s30 * s30", &file, null));
    _ = try parseExprOnly(a, "s30 * 1.25", &file, null);
    // 38 digits is still exact; a 39th makes the literal DOUBLE, as in DuckDB.
    const d38 = try parseExprOnly(a, "1234567890123456789012345678.9012345678", &file, null);
    try testing.expectEqual(ast.Dec{ .precision = 38, .scale = 10 }, d38.literal.dec.spec);
    const d39 = try parseExprOnly(a, "1234567890123456789012345678.90123456789", &file, null);
    try testing.expectEqual(ast.Type.f64, d39.typeOf());
    const lit = try parseExprOnly(a, "-0.05", &file, null);
    try testing.expectEqual(@as(i128, -5), lit.literal.dec.unscaled);
    try testing.expectEqual(ast.Dec{ .precision = 3, .scale = 2 }, lit.literal.dec.spec);
}

/// Fuzz target: arbitrary text as select, aggregate, group-by, a lone expression and a filter, against a schema of
/// every lane: each parses or fails with an error.
fn fuzzParsers(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [1024]u8 = undefined;
    const text = buf[0..smith.slice(&buf)];
    var arena_state = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const file = try fakeFile(
        a,
        &.{ "a", "b", "c", "s", "f", "fl", "u" },
        &.{ .INT32, .INT64, .DOUBLE, .BYTE_ARRAY, .BOOLEAN, .FLOAT, .FIXED_LEN_BYTE_ARRAY },
    );
    _ = parseSelect(a, text, &file, null) catch {};
    _ = parseAggList(a, text, &file, null) catch {};
    _ = parseGroupBy(a, text, &file, null) catch {};
    _ = parseExprOnly(a, text, &file, null) catch {};
    _ = filter_parser.parse(a, text, &file, null) catch {};
}

test "fuzz: select, aggregate, group-by and filter text parse or fail with an error, never panic" {
    try std.testing.fuzz({}, fuzzParsers, .{ .corpus = &.{
        "a + 1 AS x, s || 'z' AS t",
        "count(*) AS n, sum(b * 2) AS s, avg(c) FILTER (WHERE a > 3) AS v",
        "a, s AS k",
        "a > 5 AND s = 'x' OR c BETWEEN 1.5 AND 2 AND NOT f = true",
        "a IN (1, 2, 3) AND s NOT IN ('p', 'q') AND s LIKE 'ab%' AND b IS NOT NULL",
        "coalesce(a, 0) * -(b - 3) / 2",
    } });
}
