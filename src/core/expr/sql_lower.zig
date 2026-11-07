//! Copy the SQL parser's borrowed C nodes into owned, schema-independent syntax.
const std = @import("std");
const c = @import("liteparser_c");
const syntax = @import("syntax.zig");
const parser = @import("parser.zig");
const filters = @import("../filter/parser.zig");
const filter = @import("../filter/ast.zig");
const metadata = @import("../parquet/metadata.zig");
const lits = @import("sql_literals.zig");
const stack_guard = @import("../stack_guard.zig");
const Error = parser.Error || error{ SqlParseError, UnparseError, ConstantTooDeep };

/// Whether `node` reads a column anywhere beneath it.
fn readsColumn(node: *const c.LpNode) bool {
    const Find = struct {
        hit: bool = false,
        fn enter(v: [*c]c.LpVisitor, x: [*c]c.LpNode) callconv(.c) c_int {
            if (x.*.kind != c.LP_EXPR_COLUMN_REF) return 0;
            const self: *@This() = @ptrCast(@alignCast(v.*.user_data));
            self.hit = true;
            return 2;
        }
    };
    var find: Find = .{};
    var visitor: c.LpVisitor = .{ .user_data = &find, .enter = Find.enter, .leave = null };
    c.lp_ast_walk(@constCast(node), &visitor);
    return find.hit;
}

pub const Lower = struct {
    arena: std.mem.Allocator,
    lp_arena: *c.arena_t,
    /// The stack a constant's fold may use, set where a fold starts (`folded`): a constant tree is as deep as the query
    /// writes it, and nothing else bounds the recursion over it.
    stack: ?stack_guard.Guard = null,
    /// Bytes of stack a fold may use in place of what the thread has (`stack_guard.Guard.within`): for tests.
    stack_limit: ?usize = null,
    /// The string the rewrite gave the ESCAPE of an ILIKE (`Markers.ilikeEscape`); a user's own ESCAPE is no such
    /// string, whatever it spells, and is refused.
    ilike_escape: []const u8 = "",
    hidden: std.ArrayList(*const c.LpNode) = .empty,
    rows: std.ArrayList(syntax.RowKey) = .empty,
    /// Set while WHERE lowers: a comparison that is not a column against a literal lowers as two expressions
    /// (`expr_compare`). A FILTER or CASE condition, and anything inside an expression, keeps the literal grammar.
    compare_expressions: bool = false,

    fn copy(self: *Lower, text: []const u8) ![]const u8 {
        return self.arena.dupe(u8, text);
    }

    /// Identifiers use the metadata resolver's quoted-path representation, never an expression parser.
    pub fn name(self: *Lower, node: *const c.LpNode) ![]const u8 {
        if (node.kind != c.LP_EXPR_COLUMN_REF) return error.ExpressionInPredicate;
        const col = node.u.column_ref;
        var parts: std.ArrayList([]const u8) = .empty;
        if (col.schema) |v| try parts.append(self.arena, std.mem.span(v));
        if (col.table) |v| try parts.append(self.arena, std.mem.span(v));
        try parts.append(self.arena, std.mem.span(col.column));
        // Bare a.b still prefers a top-level column so named. Quoted segments force the nested path.
        return if (col.quoted == 0) std.mem.join(self.arena, ".", parts.items) else metadata.quotePath(self.arena, parts.items);
    }

    fn ptr(self: *Lower, node: *const c.LpNode, depth: u32) Error!*const syntax.Expr {
        const out = try self.arena.create(syntax.Expr);
        out.* = try self.expression(node, depth);
        return out;
    }

    pub fn expression(self: *Lower, node: *const c.LpNode, depth: u32) Error!syntax.Expr {
        const compare = self.compare_expressions;
        self.compare_expressions = false;
        defer self.compare_expressions = compare;
        return self.expressionAt(node, depth);
    }

    fn expressionAt(self: *Lower, node: *const c.LpNode, depth: u32) Error!syntax.Expr {
        if (depth > parser.MAX_EXPR_DEPTH) return error.ExpressionTooDeep;
        return switch (node.kind) {
            c.LP_RESULT_COLUMN => self.expression(node.u.result_column.expr orelse return error.SqlParseError, depth),
            c.LP_EXPR_COLUMN_REF => .{ .column = try self.name(node) },
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => .{ .number = .{
                .token = try self.copy(std.mem.span(node.u.literal.value)),
                .floating = node.kind == c.LP_EXPR_LITERAL_FLOAT,
            } },
            c.LP_EXPR_LITERAL_STRING => .{ .string = try self.copy(std.mem.span(node.u.literal.value)) },
            c.LP_EXPR_LITERAL_NULL => .{ .column = "NULL" },
            c.LP_EXPR_LITERAL_BOOL => .{ .column = try self.copy(std.mem.span(node.u.literal.value)) },
            c.LP_EXPR_UNARY_OP => switch (node.u.unary.op) {
                c.LP_UOP_MINUS => .{ .negate = try self.ptr(node.u.unary.operand orelse return error.SqlParseError, depth + 1) },
                else => error.UnexpectedChar,
            },
            c.LP_EXPR_BINARY_OP => .{ .binary = .{
                .op = switch (node.u.binary.op) {
                    c.LP_OP_ADD => .add,
                    c.LP_OP_SUB => .sub,
                    c.LP_OP_MUL => .mul,
                    c.LP_OP_DIV => .div,
                    c.LP_OP_CONCAT => .concat,
                    c.LP_OP_MOD => .mod,
                    else => return error.UnexpectedChar,
                },
                .left = try self.ptr(node.u.binary.left orelse return error.SqlParseError, depth + 1),
                .right = try self.ptr(node.u.binary.right orelse return error.SqlParseError, depth + 1),
            } },
            c.LP_EXPR_FUNCTION => blk: {
                const f = node.u.function;
                const name_ = std.mem.span(f.name);
                // `a // b` reaches the parser as `"//"(a, b)` (`sql_parser.integerDivision`): DuckDB's own name for it.
                if ((std.mem.eql(u8, name_, "//") or std.mem.eql(u8, name_, "\"//\"")) and f.args.count == 2 and f.over == null and f.distinct == 0 and
                    f.filter == null and f.order_by.count == 0)
                    break :blk .{ .binary = .{
                        .op = .intdiv,
                        .left = try self.ptr(f.args.items[0] orelse return error.SqlParseError, depth + 1),
                        .right = try self.ptr(f.args.items[1] orelse return error.SqlParseError, depth + 1),
                    } };
                if (try self.sugarCall(f, name_, depth)) |rewritten| break :blk rewritten;
                if (parser.resolveFunc(name_) != null) {
                    if (f.order_by.count != 0) return error.ExpectedRParen;
                    if (f.filter != null) return error.TrailingTokens;
                }
                // Unknown functions refuse in the schema binder, after input errors, as the fragment route does.
                const args = try self.arena.alloc(*const syntax.Expr, if (parser.resolveFunc(name_) != null) @intCast(f.args.count) else 0);
                for (args, 0..) |*out, i| out.* = try self.ptr(f.args.items[i] orelse return error.SqlParseError, depth + 1);
                break :blk .{ .call = .{ .name = try self.copy(name_), .args = args } };
            },
            c.LP_EXPR_CASE => blk: {
                const cs = node.u.case_;
                if (@mod(cs.when_exprs.count, 2) != 0) return error.UnsupportedCase;
                const whens = try self.arena.alloc(syntax.Expr.When, @intCast(@divExact(cs.when_exprs.count, 2)));
                for (whens, 0..) |*w, i| {
                    const when = cs.when_exprs.items[i * 2] orelse return error.SqlParseError;
                    w.* = .{
                        // `CASE x WHEN v` is `CASE WHEN x = v`: a column against a literal, as a predicate is.
                        .cond = if (cs.operand) |operand_node| .{ .comparison = .{
                            .operand = self.operand(operand_node) catch return error.UnsupportedCase,
                            .op = .Eq,
                            .value = try self.literal(when),
                        } } else try self.predicate(when, 0),
                        .then = try self.nullable(cs.when_exprs.items[i * 2 + 1], depth + 1),
                    };
                }
                break :blk .{ .case_ = .{ .whens = whens, .else_ = try self.nullable(cs.else_expr, depth + 1) } };
            },
            c.LP_EXPR_CAST => if (textType(node.u.cast.type_name)) .{
                .cast_text = try self.ptr(node.u.cast.expr orelse return error.SqlParseError, depth + 1),
            } else if (numericTarget(node.u.cast.type_name)) |target| try self.castCall(node, target, depth) else .{
                .call = .{ .name = "CAST", .args = &.{} },
            },
            else => error.UnexpectedChar,
        };
    }

    /// A function that is a spelling of a CASE, which the binder, the batch evaluator and every strategy already run:
    /// `nullif(x, v)` is `CASE WHEN x = v THEN NULL ELSE x END`. Its first argument is a column (the CASE's condition
    /// compares a column with a literal) and its second a literal; any other form is no function zpq has, and refuses.
    fn sugarCall(self: *Lower, f: anytype, func: []const u8, depth: u32) Error!?syntax.Expr {
        const is_concat = std.ascii.eqlIgnoreCase(func, "concat");
        const is_ifnull = std.ascii.eqlIgnoreCase(func, "ifnull");
        const part = datePartAlias(func);
        if (!is_concat and !is_ifnull and part == null and !std.ascii.eqlIgnoreCase(func, "nullif")) return null;
        if (f.over != null or f.distinct != 0 or f.filter != null or f.order_by.count != 0) return error.ExpectedRParen;
        if (part) |part_name| {
            if (f.args.count != 1) return error.WrongArity;
            const args = try self.arena.alloc(*const syntax.Expr, 2);
            const text = try self.arena.create(syntax.Expr);
            text.* = .{ .string = part_name };
            args[0] = text;
            args[1] = try self.ptr(f.args.items[0] orelse return error.SqlParseError, depth + 1);
            return .{ .call = .{ .name = "date_part", .args = args } };
        }
        if (is_concat) return try self.concatCall(f, depth);
        if (f.args.count != 2) return error.WrongArity;
        if (is_ifnull) {
            const args = try self.arena.alloc(*const syntax.Expr, 2);
            for (args, 0..) |*out, i| out.* = try self.ptr(f.args.items[i] orelse return error.SqlParseError, depth + 1);
            return .{ .call = .{ .name = "coalesce", .args = args } };
        }
        const arg_node: *const c.LpNode = f.args.items[0] orelse return error.SqlParseError;
        const other: *const c.LpNode = f.args.items[1] orelse return error.SqlParseError;
        // The column, and the same column as the branch's value.
        const column = self.operand(arg_node) catch return error.ExpressionInPredicate;
        const value = try self.ptr(arg_node, depth + 1);
        const whens = try self.arena.alloc(syntax.Expr.When, 1);
        whens[0] = .{ .cond = .{ .comparison = .{ .operand = column, .op = .Eq, .value = try self.literal(other) } }, .then = null };
        return .{ .case_ = .{ .whens = whens, .else_ = value } };
    }

    /// The part `year(x)`, `month(x)`, ... name: each is `date_part('<part>', x)`, which DuckDB answers as a BIGINT too.
    fn datePartAlias(func: []const u8) ?[]const u8 {
        const aliases = [_]struct { name: []const u8, part: []const u8 }{
            .{ .name = "year", .part = "year" },
            .{ .name = "quarter", .part = "quarter" },
            .{ .name = "month", .part = "month" },
            .{ .name = "week", .part = "week" },
            .{ .name = "day", .part = "day" },
            .{ .name = "dayofmonth", .part = "day" },
            .{ .name = "hour", .part = "hour" },
            .{ .name = "minute", .part = "minute" },
            .{ .name = "second", .part = "second" },
            .{ .name = "dayofweek", .part = "dow" },
            .{ .name = "isodow", .part = "isodow" },
            .{ .name = "dayofyear", .part = "doy" },
        };
        for (aliases) |a| if (std.ascii.eqlIgnoreCase(func, a.name)) return a.part;
        return null;
    }

    /// `concat(a, b, ...)` joins its arguments and reads a NULL as the empty text (unlike `||`, where it makes the result
    /// NULL): `coalesce(a, '') || coalesce(b, '') || ...`. DuckDB turns any argument into text; here they must be text.
    fn concatCall(self: *Lower, f: anytype, depth: u32) Error!syntax.Expr {
        if (f.args.count == 0) return error.WrongArity;
        var joined: ?*const syntax.Expr = null;
        for (0..@intCast(f.args.count)) |i| {
            const args = try self.arena.alloc(*const syntax.Expr, 2);
            args[0] = try self.ptr(f.args.items[i] orelse return error.SqlParseError, depth + 1);
            const empty = try self.arena.create(syntax.Expr);
            empty.* = .{ .string = "" };
            args[1] = empty;
            const part = try self.arena.create(syntax.Expr);
            part.* = .{ .call = .{ .name = "coalesce", .args = args } };
            if (joined) |left| {
                const both = try self.arena.create(syntax.Expr);
                both.* = .{ .binary = .{ .op = .concat, .left = left, .right = part } };
                joined = both;
            } else joined = part;
        }
        return joined.?.*;
    }

    /// What a cast to a number or a DATE is called to the binder, and a DECIMAL's precision and scale.
    const CastTarget = struct { name: []const u8, precision: u8 = 0, scale: u8 = 0, lenient: bool = false };

    /// The marker the `TRY_CAST` rewrite (`sql_parser.tryCasts`) puts before a cast's type name: `TRY_CAST(x AS INT)` is read as
    /// `CAST(x AS zpq_try_INT)`.
    const try_marker = "zpq_try_";

    fn withoutMarker(t: []const u8) struct { text: []const u8, lenient: bool } {
        if (t.len > try_marker.len and std.ascii.eqlIgnoreCase(t[0..try_marker.len], try_marker))
            return .{ .text = t[try_marker.len..], .lenient = true };
        return .{ .text = t, .lenient = false };
    }

    /// `CAST(x AS type)` for the types zpq casts to: the integers, DOUBLE, DECIMAL(p, s), DATE and TIMESTAMP, by every
    /// spelling DuckDB has for them. Anything else (FLOAT, UBIGINT, TIMESTAMPTZ, ...) is no cast zpq makes, and refuses.
    fn numericTarget(type_name: ?[*:0]const u8) ?CastTarget {
        const marked = withoutMarker(std.mem.span(type_name orelse return null));
        var target = numericTargetOf(marked.text) orelse return null;
        target.lenient = marked.lenient;
        return target;
    }

    fn numericTargetOf(t: []const u8) ?CastTarget {
        const open = std.mem.indexOfScalar(u8, t, '(') orelse t.len;
        var buf: [32]u8 = undefined;
        const base = std.mem.trim(u8, t[0..open], " ");
        if (base.len > buf.len) return null;
        const upper = std.ascii.upperString(&buf, base);
        const names = [_]struct { spelling: []const u8, name: []const u8 }{
            .{ .spelling = "TINYINT", .name = "cast$tinyint" },
            .{ .spelling = "INT1", .name = "cast$tinyint" },
            .{ .spelling = "SMALLINT", .name = "cast$smallint" },
            .{ .spelling = "INT2", .name = "cast$smallint" },
            .{ .spelling = "SHORT", .name = "cast$smallint" },
            .{ .spelling = "INTEGER", .name = "cast$integer" },
            .{ .spelling = "INT", .name = "cast$integer" },
            .{ .spelling = "INT4", .name = "cast$integer" },
            .{ .spelling = "SIGNED", .name = "cast$integer" },
            .{ .spelling = "BIGINT", .name = "cast$bigint" },
            .{ .spelling = "INT8", .name = "cast$bigint" },
            .{ .spelling = "LONG", .name = "cast$bigint" },
            .{ .spelling = "DOUBLE", .name = "cast$double" },
            .{ .spelling = "DOUBLE PRECISION", .name = "cast$double" },
            .{ .spelling = "FLOAT8", .name = "cast$double" },
            .{ .spelling = "DATE", .name = "cast$date" },
            .{ .spelling = "TIMESTAMP", .name = "cast$timestamp" },
            .{ .spelling = "DATETIME", .name = "cast$timestamp" },
        };
        for (names) |n| if (std.mem.eql(u8, upper, n.spelling)) return .{ .name = n.name };
        if (!std.mem.eql(u8, upper, "DECIMAL") and !std.mem.eql(u8, upper, "NUMERIC")) return null;
        // DECIMAL alone is DECIMAL(18, 3), DECIMAL(p) is DECIMAL(p, 0).
        if (open == t.len) return .{ .name = "cast$decimal", .precision = 18, .scale = 3 };
        const close = std.mem.indexOfScalar(u8, t, ')') orelse return null;
        var parts = std.mem.splitScalar(u8, t[open + 1 .. close], ',');
        const p = std.fmt.parseInt(u8, std.mem.trim(u8, parts.next() orelse return null, " "), 10) catch return null;
        const sc = if (parts.next()) |text| std.fmt.parseInt(u8, std.mem.trim(u8, text, " "), 10) catch return null else 0;
        if (parts.next() != null) return null;
        return .{ .name = "cast$decimal", .precision = p, .scale = sc };
    }

    fn castCall(self: *Lower, node: *const c.LpNode, target: CastTarget, depth: u32) Error!syntax.Expr {
        const decimal = target.precision != 0;
        const args = try self.arena.alloc(*const syntax.Expr, if (decimal) 3 else 1);
        args[0] = try self.ptr(node.u.cast.expr orelse return error.SqlParseError, depth + 1);
        const call_name = if (target.lenient) try std.fmt.allocPrint(self.arena, "try_{s}", .{target.name}) else target.name;
        if (decimal) for ([_]u8{ target.precision, target.scale }, 1..) |v, i| {
            const digits = try self.arena.create(syntax.Expr);
            digits.* = .{ .number = .{ .token = try std.fmt.allocPrint(self.arena, "{d}", .{v}), .floating = false } };
            args[i] = digits;
        };
        return .{ .call = .{ .name = call_name, .args = args } };
    }

    /// DuckDB's names for its one string type that a cast takes; a length (`VARCHAR(10)`) DuckDB ignores, so zpq does.
    fn textType(type_name: ?[*:0]const u8) bool {
        const t = withoutMarker(std.mem.span(type_name orelse return false)).text;
        const base = std.mem.trim(u8, t[0 .. std.mem.indexOfScalar(u8, t, '(') orelse t.len], " ");
        for ([_][]const u8{ "VARCHAR", "TEXT", "STRING" }) |n| if (std.ascii.eqlIgnoreCase(base, n)) return true;
        return false;
    }

    fn nullable(self: *Lower, node: ?*c.LpNode, depth: u32) !?*const syntax.Expr {
        const n = node orelse return null;
        return if (n.kind == c.LP_EXPR_LITERAL_NULL) null else try self.ptr(n, depth);
    }

    fn operand(self: *Lower, node: *const c.LpNode) !syntax.Operand {
        if (node.kind == c.LP_EXPR_BINARY_OP and node.u.binary.op == c.LP_OP_MOD)
            return error.ModuloNotSupported;
        if (node.kind == c.LP_EXPR_COLUMN_REF) return .{ .column = try self.name(node) };
        if (node.kind == c.LP_EXPR_FUNCTION) {
            const f = node.u.function;
            if (std.ascii.eqlIgnoreCase(std.mem.span(f.name), "make_date") and f.args.count == 1)
                return .{ .column = try self.name(f.args.items[0] orelse return error.BadOperator), .make_date = true };
        }
        return error.ExpressionInPredicate;
    }

    /// Only a standalone literal is spelled for the shared exact/temporal value converters; no query is rebuilt. A
    /// constant (`CAST('1998-09-02' AS date)`, `0.06 - 0.01`) is folded to the literal it equals first, a typed one
    /// spelled `DATE '..'` so the filter binder compares it with the column's own kind.
    fn literal(self: *Lower, node: *const c.LpNode) ![]const u8 {
        switch (node.kind) {
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT, c.LP_EXPR_LITERAL_NULL, c.LP_EXPR_LITERAL_STRING, c.LP_EXPR_LITERAL_BOOL => {},
            c.LP_EXPR_UNARY_OP => {
                if (node.u.unary.op != c.LP_UOP_MINUS and node.u.unary.op != c.LP_UOP_PLUS) return error.BadValue;
                const value: *const c.LpNode = node.u.unary.operand orelse return error.BadValue;
                if (value.kind != c.LP_EXPR_LITERAL_INT and value.kind != c.LP_EXPR_LITERAL_FLOAT) return self.folded(node);
            },
            c.LP_EXPR_BINARY_OP, c.LP_EXPR_CAST => return self.folded(node),
            else => return error.BadValue,
        }
        const value = c.lp_ast_to_sql(@constCast(node), self.lp_arena);
        if (value == null) return error.UnparseError;
        return self.copy(std.mem.span(value));
    }

    /// An integer constant's DuckDB type: INTEGER, BIGINT or HUGEINT.
    const Width = enum {
        i32,
        i64,
        i128,

        /// The narrowest type holding `v`, as DuckDB types an integer literal, signed ones after their sign.
        fn of(v: i128) Width {
            if (v >= std.math.minInt(i32) and v <= std.math.maxInt(i32)) return .i32;
            if (v >= std.math.minInt(i64) and v <= std.math.maxInt(i64)) return .i64;
            return .i128;
        }

        fn holds(self: Width, v: i128) bool {
            return switch (self) {
                .i32 => v >= std.math.minInt(i32) and v <= std.math.maxInt(i32),
                .i64 => v >= std.math.minInt(i64) and v <= std.math.maxInt(i64),
                .i128 => true,
            };
        }
    };

    /// A constant tree's value. Integers keep DuckDB's width (INTEGER, BIGINT, HUGEINT) so an overflow DuckDB would
    /// raise is refused here, not folded to a value; a number with a fraction is an exact decimal; `/` is DOUBLE there
    /// and `%` is no operator here, so both refuse.
    const Const = union(enum) {
        int: struct { v: i128, width: Width },
        dec: lits.Dec,
        text: []const u8,
        date: lits.Moment,
        timestamp: lits.Moment,
        /// `CAST(.. AS DECIMAL(p, s))`: the value at its scale.
        typed_dec: lits.Dec,
    };

    fn number(c_: Const) ?lits.Dec {
        return switch (c_) {
            .int => |i| .{ .mant = i.v, .scale = 0 },
            .dec => |d| d,
            else => null,
        };
    }

    fn constant(self: *Lower, node: *const c.LpNode) error{ BadValue, ConstantTooDeep }!Const {
        // Each level of the tree is a frame: refuse by name before the thread's stack runs out, not after.
        if (!(self.stack orelse return error.BadValue).ok()) return error.ConstantTooDeep;
        return switch (node.kind) {
            c.LP_EXPR_LITERAL_INT => blk: {
                const token = std.mem.span(node.u.literal.value);
                const v = std.fmt.parseInt(i128, token, 10) catch break :blk .{ .dec = lits.Dec.parse(token, false) orelse return error.BadValue };
                break :blk .{ .int = .{ .v = v, .width = Width.of(v) } };
            },
            c.LP_EXPR_LITERAL_FLOAT => .{ .dec = lits.Dec.parse(std.mem.span(node.u.literal.value), false) orelse return error.BadValue },
            c.LP_EXPR_LITERAL_STRING => .{ .text = std.mem.span(node.u.literal.value) },
            c.LP_EXPR_UNARY_OP => blk: {
                const inner = try self.constant(node.u.unary.operand orelse return error.BadValue);
                const negate = switch (node.u.unary.op) {
                    c.LP_UOP_MINUS => true,
                    c.LP_UOP_PLUS => false,
                    else => return error.BadValue,
                };
                if (!negate) break :blk inner;
                break :blk switch (inner) {
                    // `-2147483648` is an INTEGER though `2147483648` is a BIGINT: the type follows the signed value.
                    .int => |i| blk2: {
                        const v = std.math.negate(i.v) catch return error.BadValue;
                        break :blk2 .{ .int = .{ .v = v, .width = Width.of(v) } };
                    },
                    .dec => |d| .{ .dec = .{ .mant = -d.mant, .scale = d.scale } },
                    else => return error.BadValue,
                };
            },
            c.LP_EXPR_BINARY_OP => blk: {
                const op = node.u.binary.op;
                if (op != c.LP_OP_ADD and op != c.LP_OP_SUB and op != c.LP_OP_MUL) return error.BadValue;
                const l = try self.constant(node.u.binary.left orelse return error.BadValue);
                const r = try self.constant(node.u.binary.right orelse return error.BadValue);
                if (l == .int and r == .int) {
                    const v = (switch (op) {
                        c.LP_OP_ADD => std.math.add(i128, l.int.v, r.int.v),
                        c.LP_OP_SUB => std.math.sub(i128, l.int.v, r.int.v),
                        else => std.math.mul(i128, l.int.v, r.int.v),
                    }) catch return error.BadValue;
                    // The result type is the wider operand's: past it DuckDB raises an overflow error.
                    const width: Width = @fromBackingInt(@intCast(@max(@backingInt(l.int.width), @backingInt(r.int.width))));
                    if (!width.holds(v)) return error.BadValue;
                    break :blk .{ .int = .{ .v = v, .width = width } };
                }
                const a = number(l) orelse return error.BadValue;
                const b = number(r) orelse return error.BadValue;
                break :blk .{ .dec = (switch (op) {
                    c.LP_OP_ADD => a.add(b),
                    c.LP_OP_SUB => a.sub(b),
                    else => a.mul(b),
                }) orelse return error.BadValue };
            },
            c.LP_EXPR_CAST => self.castConstant(node),
            else => error.BadValue,
        };
    }

    fn castConstant(self: *Lower, node: *const c.LpNode) error{ BadValue, ConstantTooDeep }!Const {
        const child = try self.constant(node.u.cast.expr orelse return error.BadValue);
        const type_text = std.mem.trim(u8, std.mem.span(node.u.cast.type_name orelse return error.BadValue), " ");
        const open = std.mem.indexOfScalar(u8, type_text, '(');
        const base = std.mem.trim(u8, type_text[0 .. open orelse type_text.len], " ");
        if (std.ascii.eqlIgnoreCase(base, "date") or std.ascii.eqlIgnoreCase(base, "timestamp")) {
            if (open != null) return error.BadValue;
            const is_date = std.ascii.eqlIgnoreCase(base, "date");
            const at: lits.Moment = switch (child) {
                .text => |t| lits.parseMoment(t, is_date) catch return error.BadValue,
                .date, .timestamp => |m| .{ .days = m.days, .micros = if (is_date) 0 else m.micros },
                else => return error.BadValue,
            };
            // A DATE past TIMESTAMP's range does not convert to one.
            if (!is_date and !lits.inTimestampRange(at)) return error.BadValue;
            return if (is_date) .{ .date = at } else .{ .timestamp = at };
        }
        const is_dec = std.ascii.eqlIgnoreCase(base, "decimal") or std.ascii.eqlIgnoreCase(base, "numeric") or std.ascii.eqlIgnoreCase(base, "dec");
        if (!is_dec) return error.BadValue;
        var precision: u32 = 18;
        var scale: u32 = 3;
        if (open) |o| {
            const close = std.mem.indexOfScalar(u8, type_text, ')') orelse return error.BadValue;
            var it = std.mem.splitScalar(u8, type_text[o + 1 .. close], ',');
            precision = std.fmt.parseInt(u32, std.mem.trim(u8, it.next() orelse return error.BadValue, " "), 10) catch return error.BadValue;
            scale = if (it.next()) |sc| std.fmt.parseInt(u32, std.mem.trim(u8, sc, " "), 10) catch return error.BadValue else 0;
            if (it.next() != null) return error.BadValue;
        }
        if (precision < 1 or precision > lits.Dec.max_digits or scale > precision) return error.BadValue;
        // A string takes an exponent; a bare literal with one is a DOUBLE in DuckDB, and no exact decimal.
        const source = switch (child) {
            .text => |t| lits.Dec.parse(t, true) orelse return error.BadValue,
            else => number(child) orelse return error.BadValue,
        };
        const at_scale = source.rescale(scale) orelse return error.BadValue;
        if (at_scale.digits() > precision) return error.BadValue;
        return .{ .typed_dec = at_scale };
    }

    fn folded(self: *Lower, node: *const c.LpNode) ![]const u8 {
        self.stack = if (self.stack_limit) |bytes| .within(bytes) else .start();
        defer self.stack = null;
        return switch (try self.constant(node)) {
            .int => |i| std.fmt.allocPrint(self.arena, "{d}", .{i.v}),
            .dec => |d| d.format(self.arena),
            .typed_dec => |d| blk: {
                const text = try d.format(self.arena);
                break :blk std.fmt.allocPrint(self.arena, "DECIMAL '{s}'", .{text});
            },
            .date => |m| std.fmt.allocPrint(self.arena, "DATE '{s}'", .{try lits.formatDate(self.arena, m.days)}),
            .timestamp => |m| std.fmt.allocPrint(self.arena, "TIMESTAMP '{s}'", .{try lits.formatTimestamp(self.arena, m)}),
            .text => error.BadValue,
        };
    }

    pub fn predicate(self: *Lower, node: *const c.LpNode, depth: u32) Error!syntax.Predicate {
        if (depth > filters.MAX_GROUP_DEPTH) return error.FilterTooDeep;
        return switch (node.kind) {
            c.LP_EXPR_COLUMN_REF => .{ .boolean = try self.name(node) },
            c.LP_EXPR_UNARY_OP => blk: {
                if (node.u.unary.op != c.LP_UOP_NOT) return error.BadOperator;
                const out = try self.arena.create(syntax.Predicate);
                out.* = try self.predicate(node.u.unary.operand orelse return error.BadOperator, depth + 1);
                break :blk .{ .not_ = out };
            },
            c.LP_EXPR_BINARY_OP => blk: {
                const b = node.u.binary;
                const left: *const c.LpNode = b.left orelse return error.BadOperator;
                const right: *const c.LpNode = b.right orelse return error.BadOperator;
                if (b.op == c.LP_OP_AND or b.op == c.LP_OP_OR) {
                    break :blk try self.chain(node, b.op, depth);
                }
                if (b.op == c.LP_OP_IS or b.op == c.LP_OP_ISNOT) {
                    if (right.kind != c.LP_EXPR_LITERAL_NULL) return error.BadOperator;
                    break :blk .{ .null_check = .{ .column = try self.name(left), .is_not = b.op == c.LP_OP_ISNOT } };
                }
                if (b.op == c.LP_OP_LIKE) {
                    // ILIKE reaches here as `LIKE .. ESCAPE '<marker>'` (`sql_literals`); any other ESCAPE is refused.
                    var fold = false;
                    if (b.escape != null) {
                        const esc: *const c.LpNode = b.escape;
                        if (self.ilike_escape.len == 0 or esc.kind != c.LP_EXPR_LITERAL_STRING or
                            !std.mem.eql(u8, std.mem.span(esc.u.literal.value), self.ilike_escape))
                            return error.BadValue;
                        fold = true;
                    }
                    const pattern = try self.literal(right);
                    if (filters.typedLiteral(pattern) != null) return error.BadValue;
                    break :blk .{ .like = .{ .column = try self.name(left), .pattern = pattern, .negate = false, .fold = fold } };
                }
                const op: filter.Operator = switch (b.op) {
                    c.LP_OP_EQ => .Eq,
                    c.LP_OP_NE => .NotEq,
                    c.LP_OP_LT => .Lt,
                    c.LP_OP_LE => .LtEq,
                    c.LP_OP_GT => .Gt,
                    c.LP_OP_GE => .GtEq,
                    else => return error.BadOperator,
                };
                if (!self.compare_expressions)
                    break :blk .{ .comparison = .{ .operand = try self.operand(left), .op = op, .value = try self.literal(right) } };
                // A constant side folds as it always has, refusing what DuckDB refuses (`i > 2147483647 + 1`); a column
                // against it keeps the leaf whose statistics prune.
                if (!readsColumn(right)) {
                    const value = try self.literal(right);
                    if (self.operand(left)) |column| break :blk .{ .comparison = .{ .operand = column, .op = op, .value = value } } else |_| {}
                }
                if (!readsColumn(left)) _ = try self.literal(left);
                const sides = try self.arena.alloc(syntax.Expr, 2);
                sides[0] = try self.expression(left, depth + 1);
                sides[1] = try self.expression(right, depth + 1);
                break :blk .{ .expr_compare = .{ .left = &sides[0], .op = op, .right = &sides[1] } };
            },
            c.LP_EXPR_IN => blk: {
                const list = node.u.in;
                const values = try self.arena.alloc([]const u8, @intCast(list.values.count));
                for (values, 0..) |*out, i| out.* = try self.literal(list.values.items[i] orelse return error.BadValue);
                break :blk .{ .in_list = .{ .operand = try self.operand(list.expr orelse return error.BadOperator), .values = values, .negate = list.is_not != 0 } };
            },
            c.LP_EXPR_BETWEEN => blk: {
                const b = node.u.between;
                break :blk .{ .between = .{
                    .operand = try self.operand(b.expr orelse return error.BadOperator),
                    .low = try self.literal(b.low orelse return error.BadValue),
                    .high = try self.literal(b.high orelse return error.BadValue),
                    .negate = b.is_not != 0,
                } };
            },
            else => error.BadOperator,
        };
    }

    fn chain(self: *Lower, root: *const c.LpNode, op: c.LpBinOp, depth: u32) Error!syntax.Predicate {
        // The C tree is left-deep even for a flat chain. Preserve the fragment grammar's unbounded chain length.
        var pending: std.ArrayList(*const c.LpNode) = .empty;
        var terms: std.ArrayList(syntax.Predicate) = .empty;
        try pending.append(self.arena, root);
        while (pending.pop()) |node| {
            if (node.kind == c.LP_EXPR_BINARY_OP and node.u.binary.op == op) {
                try pending.append(self.arena, node.u.binary.right orelse return error.BadOperator);
                try pending.append(self.arena, node.u.binary.left orelse return error.BadOperator);
            } else {
                // Only an OR inside an AND needs parentheses when spelling this predicate in the legacy grammar.
                const grouped = op == c.LP_OP_AND and node.kind == c.LP_EXPR_BINARY_OP and node.u.binary.op == c.LP_OP_OR;
                try terms.append(self.arena, try self.predicate(node, depth + @as(u32, @intFromBool(grouped))));
            }
        }
        return if (op == c.LP_OP_AND) .{ .and_ = terms.items } else .{ .or_ = terms.items };
    }

    pub fn aggregate(self: *Lower, node: *const c.LpNode, alias: ?[]const u8, display: []const u8) !syntax.Aggregate {
        if (node.kind != c.LP_EXPR_FUNCTION) return error.TrailingTokens;
        const f = node.u.function;
        const name_ = std.mem.span(f.name);
        const state = std.ascii.eqlIgnoreCase(name_, "avg_state");
        const boolean_only = parser.isBooleanAggName(name_);
        var func = if (state) .avg else parser.resolveAggFunc(name_) orelse return error.UnknownAggFunc;
        if (f.distinct != 0) func = .count_distinct;
        const second = parser.aggSecond(name_);
        const wanted: usize = if (second == .required or (second == .optional and f.args.count == 2)) 2 else 1;
        if (f.args.count != wanted) return if (f.args.count == 0) error.UnexpectedChar else error.ExpectedRParen;
        if (f.order_by.count != 0) return error.ExpectedRParen;
        const arg: *const c.LpNode = f.args.items[0] orelse return error.BadAggArg;
        if (f.distinct != 0 and arg.kind == c.LP_EXPR_STAR) return error.UnexpectedChar;
        return .{
            .func = func,
            .arg = if (arg.kind == c.LP_EXPR_STAR) null else try self.expression(arg, 0),
            .arg2 = if (wanted == 2) try self.expression(f.args.items[1] orelse return error.BadAggArg, 0) else null,
            .where = if (f.filter) |pred| try self.predicate(pred, 0) else null,
            .alias = if (alias) |a| try self.copy(a) else try self.copy(name_),
            .default_name = if (alias != null) null else try self.copy(display),
            .state = state,
            .boolean_only = boolean_only,
        };
    }
};

test "a constant tree deeper than the stack budget is refused by name, and folds within it" {
    const gpa = std.testing.allocator;
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);
    var text: std.ArrayList(u8) = .empty;
    defer text.deinit(gpa);
    // The parser builds `1 + 1 + ...` as one left-deep tree without recursing; the fold recurses once per level.
    try text.appendSlice(gpa, "SELECT count(*) FROM t WHERE i < 1");
    for (0..2000) |_| try text.appendSlice(gpa, " + 1");
    try text.append(gpa, 0);
    var message: [*c]const u8 = null;
    const stmts = c.lp_parse_all(text.items[0 .. text.items.len - 1 :0], lp_arena, &message);
    const where = stmts.*.items[0].*.u.select.where orelse return error.SqlParseError;

    var out: std.heap.ArenaAllocator = .init(gpa);
    defer out.deinit();
    var tight: Lower = .{ .arena = out.allocator(), .lp_arena = lp_arena, .stack_limit = 4096 };
    try std.testing.expectError(error.ConstantTooDeep, tight.predicate(where, 0));
    // The thread's own budget holds two thousand levels, so the same tree folds there.
    var roomy: Lower = .{ .arena = out.allocator(), .lp_arena = lp_arena };
    const folded = try roomy.predicate(where, 0);
    try std.testing.expectEqualStrings("2001", folded.comparison.value);
}
