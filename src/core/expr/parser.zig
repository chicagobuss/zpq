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
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");

pub const Error = error{
    EmptyExpr,
    UnexpectedChar,
    UnexpectedEnd,
    BadNumber,
    UnknownColumn,
    UnsupportedColumnType,
    UnterminatedString,
    TypeMismatch,
    ExpectedRParen,
    ExpectedIdentifier,
    TrailingTokens,
} || std.mem.Allocator.Error;

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

const Lexer = struct {
    src: []const u8,
    pos: usize = 0,

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
            const col_idx = metadata.findColumnIndex(file, tk.text) orelse return error.UnknownColumn;
            const elem = file.getColumnSchema(&[_][]const u8{tk.text}) orelse return error.UnknownColumn;
            const phys = elem.type orelse return error.UnsupportedColumnType;
            const expr_type: ast.Type = switch (phys) {
                .INT32, .INT64 => .i64,
                .FLOAT, .DOUBLE => .f64,
                .BYTE_ARRAY => .str,
                else => return error.UnsupportedColumnType,
            };
            return .{ .col_ref = .{
                .col_idx = col_idx,
                .physical_type = phys,
                .expr_type = expr_type,
            } };
        },
        .lparen => {
            const inner = try parseExpr(arena, lex, file);
            const close = try lex.next();
            if (close.kind != .rparen) return error.ExpectedRParen;
            return inner;
        },
        .eof => return error.UnexpectedEnd,
        else => return error.UnexpectedChar,
    }
}

fn makeBinop(arena: std.mem.Allocator, op: ast.Op, l: ast.Expr, r: ast.Expr) Error!ast.Expr {
    const result_type = ast.Type.promote(l.typeOf(), r.typeOf()) orelse return error.TypeMismatch;
    // Op/type compatibility: `concat` is string-only, arithmetic ops
    // are numeric-only.
    if (op == .concat and result_type != .str) return error.TypeMismatch;
    if (op != .concat and result_type == .str) return error.TypeMismatch;

    const lp = try arena.create(ast.Expr);
    const rp = try arena.create(ast.Expr);
    lp.* = l;
    rp.* = r;
    return .{ .binop = .{
        .op = op,
        .left = lp,
        .right = rp,
        .result_type = result_type,
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
