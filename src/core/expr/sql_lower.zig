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
                    c.LP_OP_MOD => return error.ModuloNotSupported,
                    else => return error.UnexpectedChar,
                },
                .left = try self.ptr(node.u.binary.left orelse return error.SqlParseError, depth + 1),
                .right = try self.ptr(node.u.binary.right orelse return error.SqlParseError, depth + 1),
            } },
            c.LP_EXPR_FUNCTION => blk: {
                const f = node.u.function;
                const name_ = std.mem.span(f.name);
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
            } else .{ .call = .{ .name = "CAST", .args = &.{} } },
            else => error.UnexpectedChar,
        };
    }

    /// DuckDB's names for its one string type that a cast takes; a length (`VARCHAR(10)`) DuckDB ignores, so zpq does.
    fn textType(type_name: ?[*:0]const u8) bool {
        const t = std.mem.span(type_name orelse return false);
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
                    const width: Width = @enumFromInt(@max(@intFromEnum(l.int.width), @intFromEnum(r.int.width)));
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
                break :blk .{ .comparison = .{ .operand = try self.operand(left), .op = op, .value = try self.literal(right) } };
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
        var func = if (state) .avg else parser.resolveAggFunc(name_) orelse return error.UnknownAggFunc;
        if (f.distinct != 0) func = .count_distinct;
        if (f.args.count != 1) return if (f.args.count == 0) error.UnexpectedChar else error.ExpectedRParen;
        if (f.order_by.count != 0) return error.ExpectedRParen;
        const arg: *const c.LpNode = f.args.items[0] orelse return error.BadAggArg;
        if (f.distinct != 0 and arg.kind == c.LP_EXPR_STAR) return error.UnexpectedChar;
        return .{
            .func = func,
            .arg = if (arg.kind == c.LP_EXPR_STAR) null else try self.expression(arg, 0),
            .where = if (f.filter) |pred| try self.predicate(pred, 0) else null,
            .alias = if (alias) |a| try self.copy(a) else try self.copy(name_),
            .default_name = if (alias != null) null else try self.copy(display),
            .state = state,
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
