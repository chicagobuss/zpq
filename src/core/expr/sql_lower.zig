//! Copy the SQL parser's borrowed C nodes into owned, schema-independent syntax.
const std = @import("std");
const c = @import("liteparser_c");
const syntax = @import("syntax.zig");
const parser = @import("parser.zig");
const filters = @import("../filter/parser.zig");
const filter = @import("../filter/ast.zig");
const metadata = @import("../parquet/metadata.zig");
const Error = parser.Error || error{ SqlParseError, UnparseError };

pub const Lower = struct {
    arena: std.mem.Allocator,
    lp_arena: *c.arena_t,
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
                if (cs.operand != null or @mod(cs.when_exprs.count, 2) != 0) return error.UnsupportedCase;
                const whens = try self.arena.alloc(syntax.Expr.When, @intCast(@divExact(cs.when_exprs.count, 2)));
                for (whens, 0..) |*w, i| w.* = .{
                    .cond = try self.predicate(cs.when_exprs.items[i * 2] orelse return error.SqlParseError, 0),
                    .then = try self.nullable(cs.when_exprs.items[i * 2 + 1], depth + 1),
                };
                break :blk .{ .case_ = .{ .whens = whens, .else_ = try self.nullable(cs.else_expr, depth + 1) } };
            },
            c.LP_EXPR_CAST => .{ .call = .{ .name = "CAST", .args = &.{} } },
            else => error.UnexpectedChar,
        };
    }

    fn nullable(self: *Lower, node: ?*c.LpNode, depth: u32) !?*const syntax.Expr {
        const n = node orelse return null;
        return if (n.kind == c.LP_EXPR_LITERAL_NULL) null else try self.ptr(n, depth);
    }

    fn operand(self: *Lower, node: *const c.LpNode) !syntax.Operand {
        if (node.kind == c.LP_EXPR_COLUMN_REF) return .{ .column = try self.name(node) };
        if (node.kind == c.LP_EXPR_FUNCTION) {
            const f = node.u.function;
            if (std.ascii.eqlIgnoreCase(std.mem.span(f.name), "make_date") and f.args.count == 1)
                return .{ .column = try self.name(f.args.items[0] orelse return error.BadOperator), .make_date = true };
        }
        return error.ExpressionInPredicate;
    }

    /// Only a standalone literal is spelled for the shared exact/temporal value converters; no query is rebuilt.
    fn literal(self: *Lower, node: *const c.LpNode) ![]const u8 {
        switch (node.kind) {
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT, c.LP_EXPR_LITERAL_NULL, c.LP_EXPR_LITERAL_STRING, c.LP_EXPR_LITERAL_BOOL => {},
            c.LP_EXPR_UNARY_OP => {
                if (node.u.unary.op != c.LP_UOP_MINUS) return error.BadValue;
                const value: *const c.LpNode = node.u.unary.operand orelse return error.BadValue;
                if (value.kind != c.LP_EXPR_LITERAL_INT and value.kind != c.LP_EXPR_LITERAL_FLOAT) return error.BadValue;
            },
            else => return error.BadValue,
        }
        const value = c.lp_ast_to_sql(@constCast(node), self.lp_arena);
        if (value == null) return error.UnparseError;
        return self.copy(std.mem.span(value));
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
                    if (b.escape != null) return error.BadValue;
                    break :blk .{ .like = .{ .column = try self.name(left), .pattern = try self.literal(right), .negate = false } };
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
