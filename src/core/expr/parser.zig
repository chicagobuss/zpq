//! Expression parser. Recursive descent over a tiny tokenizer.
//!
//! Grammar:
//!
//!   select      := select_item ( "," select_item )*
//!   select_item := expr ( "AS" IDENT )?
//!   expr        := term ( ("+" | "-") term )*
//!   term        := factor ( ("*" | "/") factor )*
//!   factor      := NUMBER | IDENT | "(" expr ")"
//!
//! Negative literals are written as `0 - x` for now — unary minus
//! lands in a follow-up. `AS` is case-insensitive (`as` works too).
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

pub const Error = error{
    EmptyExpr,
    EmptyAggregate,
    UnexpectedChar,
    UnexpectedEnd,
    BadNumber,
    UnknownColumn,
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
} || std.mem.Allocator.Error || filter_parser.Error;

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

/// A memory bound, not a stack one: the evaluator materializes a full intermediate column per AST node, in each of the
/// row groups a worker holds in flight, so peak cost is O(workers x depth x rows_per_row_group). `max_memory` is
/// unenforced on this path, so the cap is the only thing bounding the pathological case; 32 is well above any
/// hand-written expression.
pub const MAX_EXPR_DEPTH: u32 = 32;

const Lexer = struct {
    src: []const u8,
    pos: usize = 0,
    /// Lives on the Lexer because it is already threaded through every parse function.
    depth: u32 = 0,

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
                // resolution treats it identically to a bare identifier.
                self.pos += 1;
                const start = self.pos;
                while (self.pos < self.src.len and self.src[self.pos] != '"') self.pos += 1;
                if (self.pos >= self.src.len) return error.UnterminatedString;
                const text = self.src[start..self.pos];
                self.pos += 1; // consume closing quote
                return .{ .kind = .ident, .text = text };
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
) Error![]ast.SelectItem {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;

    var items: std.ArrayList(ast.SelectItem) = .empty;
    var lex: Lexer = .{ .src = trimmed };

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
) Error![]ast.SelectItem {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;

    var items: std.ArrayList(ast.SelectItem) = .empty;
    var lex: Lexer = .{ .src = trimmed };

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

/// Parse a single expression. Convenience for callers that already
/// know they only have one (e.g. tests).
pub fn parseExprOnly(
    arena: std.mem.Allocator,
    src: []const u8,
    file: *const schema.FileMetaData,
) Error!ast.Expr {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyExpr;
    var lex: Lexer = .{ .src = trimmed };
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
                    .str => return error.TypeMismatch,
                },
                else => {
                    const lp = try arena.create(ast.Expr);
                    lp.* = .{ .literal = .{ .i64 = 0 } };
                    const rp = try arena.create(ast.Expr);
                    rp.* = inner;
                    const result_type = ast.Type.promote(.i64, inner.typeOf()) orelse return error.TypeMismatch;
                    if (result_type == .str) return error.TypeMismatch;
                    const depth = 1 + inner.depth();
                    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
                    return .{ .binop = .{
                        .op = .sub,
                        .left = lp,
                        .right = rp,
                        .result_type = result_type,
                        .depth = depth,
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
            const col_idx = metadata.findColumnIndex(file, tk.text) orelse return error.UnknownColumn;
            const elem = file.getColumnSchema(&[_][]const u8{tk.text}) orelse return error.UnknownColumn;
            const phys = elem.type orelse return error.UnsupportedColumnType;
            // DECIMAL columns are decoded to f64 by the consumer
            // (see core/parquet/decimal.zig) regardless of their
            // physical backing — so expressions reference them as
            // f64, not as the underlying INT32/INT64/FLBA.
            const is_decimal = decimal_mod.kindFromSchema(&elem) != null;
            const expr_type: ast.Type = if (is_decimal or schema.isFloat16(elem)) .f64 else switch (phys) {
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
    var deepest: u32 = 0;
    for (args.items) |a| deepest = @max(deepest, a.depth());
    const depth = 1 + deepest;
    if (depth > MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
    return .{ .call = .{
        .func = func,
        .args = args.items,
        .result_type = result_type,
        .depth = depth,
    } };
}

fn resolveFunc(name: []const u8) ?ast.Func {
    if (asciiEqIgnoreCase(name, "coalesce")) return .coalesce;
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
    }
}

fn makeBinop(arena: std.mem.Allocator, op: ast.Op, l: ast.Expr, r: ast.Expr) Error!ast.Expr {
    const result_type = ast.Type.promote(l.typeOf(), r.typeOf()) orelse return error.TypeMismatch;
    // Op/type compatibility: `concat` is string-only, arithmetic ops
    // are numeric-only.
    if (op == .concat and result_type != .str) return error.TypeMismatch;
    if (op != .concat and result_type == .str) return error.TypeMismatch;

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
    } };
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
) Error![]agg.AggCall {
    const trimmed = std.mem.trim(u8, src, " \t\r\n");
    if (trimmed.len == 0) return error.EmptyAggregate;
    var lex: Lexer = .{ .src = trimmed };
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
    const func = resolveAggFunc(name_tok.text) orelse return error.UnknownAggFunc;

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

        // Scan source for matching closing paren. The filter parser
        // doesn't support nested parens in predicates, but we still
        // walk depth so a future "filter parser with parens" would
        // compose without changes here.
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
                else => {},
            }
        }
        if (depth != 0) return error.ExpectedRParen;
        const body = std.mem.trim(u8, lex.src[body_start..i], " \t\r\n");
        lex.pos = i + 1; // skip closing `)`

        if (body.len < 6 or !asciiEqIgnoreCase(body[0..5], "WHERE")) return error.ExpectedWhere;
        const pred_str = std.mem.trim(u8, body[5..], " \t\r\n");
        where = try filter_parser.parse(arena, pred_str, file);
    }

    // 5. Optional AS alias. Default = function name (sum / count / ...).
    var alias: []const u8 = name_tok.text;
    const after_filter = try lex.peek();
    if (after_filter.kind == .ident and asciiEqIgnoreCase(after_filter.text, "AS")) {
        _ = try lex.next();
        const alias_tok = try lex.next();
        if (alias_tok.kind != .ident) return error.ExpectedIdentifier;
        alias = alias_tok.text;
    }

    // 6. Resolve result shape from func + arg type.
    const result = try agg.resolveResult(func, arg);

    return .{
        .func = func,
        .arg = arg,
        .where = where,
        .alias = alias,
        .result = result,
    };
}

fn resolveAggFunc(name: []const u8) ?agg.AggFunc {
    if (asciiEqIgnoreCase(name, "count")) return .count;
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

test "parse: literal int" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "42", &file);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(@as(i64, 42), e.literal.i64);
}

test "parse: literal float" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "3.14", &file);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
    try testing.expect(@abs(e.literal.f64 - 3.14) < 1e-9);
}

test "parse: column ref resolves" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT32, .DOUBLE });
    const ex = try parseExprOnly(a, "x", &file);
    try testing.expectEqual(ast.Type.i64, ex.typeOf()); // INT32 promoted
    try testing.expectEqual(@as(usize, 0), ex.col_ref.col_idx);
    const ey = try parseExprOnly(a, "y", &file);
    try testing.expectEqual(ast.Type.f64, ey.typeOf());
    try testing.expectEqual(@as(usize, 1), ey.col_ref.col_idx);
}

test "parse: precedence — mul before add" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "x + y * z", &file);
    // Should parse as `x + (y * z)`.
    try testing.expectEqual(ast.Op.add, e.binop.op);
    try testing.expectEqual(ast.Op.mul, e.binop.right.binop.op);
}

test "parse: parens override precedence" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y", "z" }, &.{ .INT64, .INT64, .INT64 });
    const e = try parseExprOnly(a, "(x + y) * z", &file);
    try testing.expectEqual(ast.Op.mul, e.binop.op);
    try testing.expectEqual(ast.Op.add, e.binop.left.binop.op);
}

test "parse: select list with alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "x", "y" }, &.{ .INT64, .INT64 });
    const items = try parseSelect(a, "x AS first, x + y AS sum", &file);
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
    const e = try parseExprOnly(a, "i + f", &file);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: unknown column errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownColumn, parseExprOnly(a, "y + 1", &file));
}

test "parse: string literal" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const e = try parseExprOnly(a, "'hello'", &file);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqualStrings("hello", e.literal.str);
}

test "parse: string column ref" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    const e = try parseExprOnly(a, "name", &file);
    try testing.expectEqual(ast.Type.str, e.typeOf());
}

test "parse: || string concat" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "first", "last" }, &.{ .BYTE_ARRAY, .BYTE_ARRAY });
    const e = try parseExprOnly(a, "first || ' ' || last", &file);
    try testing.expectEqual(ast.Type.str, e.typeOf());
    try testing.expectEqual(ast.Op.concat, e.binop.op);
}

test "parse: type mismatch — string + number errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"name"}, &.{.BYTE_ARRAY});
    try testing.expectError(error.TypeMismatch, parseExprOnly(a, "name + 1", &file));
}

test "parse: coalesce(col, default)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "coalesce(x, 0)", &file);
    try testing.expectEqual(ast.Type.i64, e.typeOf());
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
    try testing.expectEqual(@as(usize, 2), e.call.args.len);
}

test "parse: coalesce is case-insensitive" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    const e = try parseExprOnly(a, "COALESCE(x, 0)", &file);
    try testing.expectEqual(ast.Func.coalesce, e.call.func);
}

test "parse: coalesce promotes int+float arg types to f64" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "i", "f" }, &.{ .INT64, .DOUBLE });
    const e = try parseExprOnly(a, "coalesce(i, f)", &file);
    try testing.expectEqual(ast.Type.f64, e.typeOf());
}

test "parse: coalesce arity error" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.WrongArity, parseExprOnly(a, "coalesce(x)", &file));
}

test "parse: unknown function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownFunction, parseExprOnly(a, "abs(x)", &file));
}

test "parse: incomplete trailing operator fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnexpectedEnd, parseExprOnly(a, "x +", &file));
}

test "parse: extra trailing token fails" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.TrailingTokens, parseExprOnly(a, "x 1", &file));
}

test "agg: count(*) with default alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{}, &.{});
    const items = try parseAggList(a, "count(*)", &file);
    try testing.expectEqual(@as(usize, 1), items.len);
    try testing.expectEqual(agg.AggFunc.count, items[0].func);
    try testing.expectEqualStrings("count", items[0].alias);
    try testing.expect(items[0].arg == null);
    try testing.expect(items[0].where == null);
}

test "agg: sum with column ref + alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"cost"}, &.{.DOUBLE});
    const items = try parseAggList(a, "sum(cost) AS total", &file);
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
    const items = try parseAggList(a, "avg(cost) AS mean", &file);
    try testing.expectEqual(agg.ResultShape.avg_f64, items[0].result);
}

test "agg: FILTER (WHERE pred) attaches a predicate" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "sum(cost) FILTER (WHERE service = ec2) AS ec2_cost", &file);
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
    try testing.expectError(error.StarOnlyValidInCount, parseAggList(a, "sum(*)", &file));
}

test "agg: unknown agg function errors" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{"x"}, &.{.INT64});
    try testing.expectError(error.UnknownAggFunc, parseAggList(a, "median(x)", &file));
}

test "agg: case-insensitive keywords" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "service", "cost" }, &.{ .BYTE_ARRAY, .DOUBLE });
    const items = try parseAggList(a, "SUM(cost) filter (where service = ec2) as total", &file);
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
    const items = try parseAggList(a, "sum(\"cost\") AS c, max(\"cost\") AS m", &file);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqualStrings("c", items[0].alias);
    try testing.expectEqualStrings("m", items[1].alias);
}

test "group-by: bare column and AS alias" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const file = try fakeFile(a, &.{ "region", "kind" }, &.{ .INT32, .BYTE_ARRAY });
    const items = try parseGroupBy(a, "region, kind AS k", &file);
    try testing.expectEqual(@as(usize, 2), items.len);
    try testing.expectEqual(@as(?[]const u8, null), items[0].alias);
    try testing.expectEqualStrings("k", items[1].alias.?);
}
