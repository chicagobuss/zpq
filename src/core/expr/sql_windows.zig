//! A query with window functions in its SELECT list, planned as stages (`window_plan.zig`): the query without its window
//! calls, into a file whose columns are named `c0`, `c1`, ...; a
//! pass per distinct `(PARTITION BY, ORDER BY)` that adds the calls' `w0`, `w1`, ... columns; and the original SELECT list
//! (each window call its column, each other expression its `c`) and ORDER BY / LIMIT / OFFSET over the last file.
//!
//! The text is read as the parser reads it (comments gone, the text rewrites done). A window call is a whole item or an
//! operand of arithmetic, a function call or a cast. What is not followed is refused by name.
const std = @import("std");
const c = @import("liteparser_c");
const wp = @import("window_plan.zig");
const sql_setops = @import("sql_setops.zig");
const sql_names = @import("sql_names.zig");
const sql_literals = @import("sql_literals.zig");

const interval_marker = sql_literals.frame_interval;

pub const Error = error{ OutOfMemory, SqlParseError, WindowNotSupported, SelectAliasRequired };

/// Why a window query is refused, for the message.
pub const Refusal = struct {
    buf: [400]u8 = undefined,
    len: usize = 0,

    pub fn text(self: *const Refusal) []const u8 {
        return self.buf[0..self.len];
    }

    fn set(self: *Refusal, comptime fmt: []const u8, args: anytype) Error {
        const written: []const u8 = std.fmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..];
        self.len = written.len;
        return error.WindowNotSupported;
    }
};

/// Whether `sql` has the word `over` followed by an open parenthesis (or a window name), outside quoted text.
pub fn mentions(sql: []const u8) bool {
    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '\'' or sql[i] == '"') {
            i = sql_setops.quotedEnd(sql, i);
            continue;
        }
        if (sql_setops.startsWord(sql, i)) {
            const word = sql_setops.wordAt(sql, i);
            if (sql_setops.eql(word, "over")) return true;
            i += word.len;
            continue;
        }
        i += 1;
    }
    return false;
}

const Win = struct { name: []const u8, pass: usize, call: wp.Call };

const PassB = struct {
    sort: []const wp.SortKey,
    partition: []const []const u8,
    order: []const []const u8,
    calls: std.ArrayList(usize) = .empty,
};

const Builder = struct {
    arena: std.mem.Allocator,
    lp: *c.arena_t,
    why: *Refusal,
    inputs: std.ArrayList([]const u8) = .empty,
    /// The select items that have an alias, in order: QUALIFY may name them.
    aliases: std.ArrayList(struct { name: []const u8, expr: *c.LpNode }) = .empty,
    /// The window calls planned so far, by their tree: one written twice (a select item, then QUALIFY by its alias) is one column.
    planned: std.ArrayList(struct { node: *c.LpNode, name: []const u8 }) = .empty,
    /// The table's column names, when they are known: in QUALIFY a column of the table is read before a select alias of that name.
    columns: ?[]const []const u8 = null,
    wins: std.ArrayList(Win) = .empty,
    passes: std.ArrayList(PassB) = .empty,

    fn unparse(self: *Builder, node: *c.LpNode) Error![]const u8 {
        const text_c = c.lp_ast_to_sql(node, self.lp) orelse return error.SqlParseError;
        return self.arena.dupe(u8, std.mem.span(text_c));
    }

    /// The input column that holds the expression `text`.
    fn input(self: *Builder, text: []const u8) Error![]const u8 {
        for (self.inputs.items, 0..) |t, i| if (std.mem.eql(u8, t, text)) return std.fmt.allocPrint(self.arena, "c{d}", .{i});
        try self.inputs.append(self.arena, text);
        return std.fmt.allocPrint(self.arena, "c{d}", .{self.inputs.items.len - 1});
    }

    /// A window-free expression as an input column's name.
    fn column(self: *Builder, node: *c.LpNode, what: []const u8) Error![]const u8 {
        if (hasWindow(node)) return self.why.set("a window function inside the {s} of a window function is not supported", .{what});
        return self.input(try self.unparse(node));
    }

    /// `column` for a key the window partitions or orders by, which compares its value: a bare AVG is staged as the
    /// DOUBLE an AVG in an expression reads (`avg(x) + 0`), not as the rounded DECIMAL mean it prints for a DECIMAL.
    fn key(self: *Builder, node: *c.LpNode, what: []const u8) Error![]const u8 {
        const bare = node.kind == c.LP_EXPR_FUNCTION and node.u.function.over == null and node.u.function.name != null;
        const avg = bare and std.ascii.eqlIgnoreCase(std.mem.span(node.u.function.name), "avg");
        if (!avg) return self.column(node, what);
        return self.input(try std.fmt.allocPrint(self.arena, "{s} + 0", .{try self.unparse(node)}));
    }

    /// `node` for the final query: a window call its column, a window-free expression its input column, and an
    /// operation on those rebuilt around them.
    fn lower(self: *Builder, node: *c.LpNode) Error![]const u8 {
        if (node.kind == c.LP_RESULT_COLUMN) return self.lower(node.u.result_column.expr orelse return error.SqlParseError);
        if (node.kind == c.LP_EXPR_FUNCTION and node.u.function.over != null) {
            for (self.planned.items) |p| if (c.lp_node_equal(p.node, node) != 0) return std.fmt.allocPrint(self.arena, "\"{s}\"", .{p.name});
            const name = try self.window(node);
            try self.planned.append(self.arena, .{ .node = node, .name = name });
            return std.fmt.allocPrint(self.arena, "\"{s}\"", .{name});
        }
        if (!hasWindow(node)) return std.fmt.allocPrint(self.arena, "\"{s}\"", .{try self.input(try self.unparse(node))});
        switch (node.kind) {
            c.LP_EXPR_BINARY_OP => {
                const b = node.u.binary;
                const op: []const u8 = switch (b.op) {
                    c.LP_OP_ADD => "+",
                    c.LP_OP_SUB => "-",
                    c.LP_OP_MUL => "*",
                    c.LP_OP_DIV => "/",
                    c.LP_OP_MOD => "%",
                    c.LP_OP_CONCAT => "||",
                    else => return self.why.set("a window function inside this operation is not supported", .{}),
                };
                const left = try self.operand(b.left orelse return error.SqlParseError);
                const right = try self.operand(b.right orelse return error.SqlParseError);
                return std.fmt.allocPrint(self.arena, "({s} {s} {s})", .{ left, op, right });
            },
            c.LP_EXPR_UNARY_OP => {
                if (node.u.unary.op != c.LP_UOP_MINUS) return self.why.set("a window function inside this operation is not supported", .{});
                return std.fmt.allocPrint(self.arena, "(-{s})", .{try self.operand(node.u.unary.operand orelse return error.SqlParseError)});
            },
            c.LP_EXPR_FUNCTION => {
                const f = node.u.function;
                if (f.distinct != 0 or f.filter != null or f.order_by.count != 0) return self.why.set("a window function inside this call is not supported", .{});
                var args: std.ArrayList(u8) = .empty;
                for (0..@intCast(f.args.count)) |i| {
                    if (i > 0) try args.appendSlice(self.arena, ", ");
                    try args.appendSlice(self.arena, try self.operand(f.args.items[i] orelse return error.SqlParseError));
                }
                return std.fmt.allocPrint(self.arena, "{s}({s})", .{ std.mem.span(f.name), args.items });
            },
            c.LP_EXPR_CAST => return std.fmt.allocPrint(self.arena, "CAST({s} AS {s})", .{
                try self.operand(node.u.cast.expr orelse return error.SqlParseError),
                std.mem.span(node.u.cast.type_name),
            }),
            else => return self.why.set("a window function inside this expression is not supported", .{}),
        }
    }

    /// An operand of an operation: a literal stays in place, anything else is lowered.
    fn operand(self: *Builder, node: *c.LpNode) Error![]const u8 {
        switch (node.kind) {
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT, c.LP_EXPR_LITERAL_STRING, c.LP_EXPR_LITERAL_NULL => return self.unparse(node),
            else => return self.lower(node),
        }
    }

    /// QUALIFY reads a name as DuckDB does: a column of the table first, else the last select item with that alias, whatever it is
    /// and wherever the name stands (an argument, a window's PARTITION BY). Each such name is replaced by that item's expression.
    fn substituteAliases(self: *Builder, root: *c.LpNode) Error!void {
        const Found = struct {
            arena: std.mem.Allocator,
            nodes: std.ArrayList(*c.LpNode) = .empty,
            failed: bool = false,

            fn enter(data: [*c]c.LpVisitor, n: [*c]c.LpNode) callconv(.c) c_int {
                const found: *@This() = @ptrCast(@alignCast(data.*.user_data));
                // A subquery reads its own names.
                if (n.*.kind == c.LP_STMT_SELECT) return 1;
                if (n.*.kind == c.LP_EXPR_COLUMN_REF and n.*.u.column_ref.table == null and n.*.u.column_ref.schema == null)
                    found.nodes.append(found.arena, n) catch {
                        found.failed = true;
                    };
                return 0;
            }
        };
        var found: Found = .{ .arena = self.arena };
        var visitor: c.LpVisitor = .{ .user_data = &found, .enter = Found.enter, .leave = null };
        c.lp_ast_walk(root, &visitor);
        if (found.failed) return error.OutOfMemory;
        for (found.nodes.items) |ref| {
            const name = std.mem.span(ref.u.column_ref.column);
            var alias: ?*c.LpNode = null;
            var at = self.aliases.items.len;
            while (at > 0) {
                at -= 1;
                if (std.ascii.eqlIgnoreCase(self.aliases.items[at].name, name)) {
                    alias = self.aliases.items[at].expr;
                    break;
                }
            }
            const expr = alias orelse continue;
            // Without the table's columns it cannot be told whether this is a column or the alias.
            const cols = self.columns orelse return self.why.set("QUALIFY names the output column {s}, and the table's columns are not known to tell it from one", .{name});
            var is_column = false;
            for (cols) |col| is_column = is_column or std.ascii.eqlIgnoreCase(col, name);
            if (!is_column) ref.* = expr.*;
        }
    }

    /// A QUALIFY predicate for the final query's WHERE: comparisons, AND / OR / NOT, IS NULL, BETWEEN and IN over lowered
    /// operands. A window call, or a column the query reads, is a column of the last file.
    fn predicate(self: *Builder, node: *c.LpNode) Error![]const u8 {
        switch (node.kind) {
            c.LP_EXPR_BINARY_OP => {
                const b = node.u.binary;
                const op: []const u8 = switch (b.op) {
                    c.LP_OP_EQ => "=",
                    c.LP_OP_NE => "<>",
                    c.LP_OP_LT => "<",
                    c.LP_OP_LE => "<=",
                    c.LP_OP_GT => ">",
                    c.LP_OP_GE => ">=",
                    c.LP_OP_IS => "IS",
                    c.LP_OP_ISNOT => "IS NOT",
                    c.LP_OP_AND, c.LP_OP_OR => {
                        const l = try self.predicate(b.left orelse return error.SqlParseError);
                        const r = try self.predicate(b.right orelse return error.SqlParseError);
                        return std.fmt.allocPrint(self.arena, "({s} {s} {s})", .{ l, if (b.op == c.LP_OP_AND) "AND" else "OR", r });
                    },
                    else => return self.operand(node),
                };
                const l = try self.operand(b.left orelse return error.SqlParseError);
                const r = try self.operand(b.right orelse return error.SqlParseError);
                return std.fmt.allocPrint(self.arena, "({s} {s} {s})", .{ l, op, r });
            },
            c.LP_EXPR_UNARY_OP => {
                if (node.u.unary.op != c.LP_UOP_NOT) return self.operand(node);
                return std.fmt.allocPrint(self.arena, "(NOT {s})", .{try self.predicate(node.u.unary.operand orelse return error.SqlParseError)});
            },
            c.LP_EXPR_BETWEEN => {
                const t = node.u.between;
                return std.fmt.allocPrint(self.arena, "({s} {s}BETWEEN {s} AND {s})", .{
                    try self.operand(t.expr orelse return error.SqlParseError),
                    if (t.is_not != 0) "NOT " else "",
                    try self.operand(t.low orelse return error.SqlParseError),
                    try self.operand(t.high orelse return error.SqlParseError),
                });
            },
            c.LP_EXPR_IN => {
                const t = node.u.in;
                if (t.select != null) return self.why.set("a subquery in QUALIFY is not supported", .{});
                var list: std.ArrayList(u8) = .empty;
                for (0..@intCast(t.values.count)) |i| {
                    if (i > 0) try list.appendSlice(self.arena, ", ");
                    try list.appendSlice(self.arena, try self.operand(t.values.items[i] orelse return error.SqlParseError));
                }
                return std.fmt.allocPrint(self.arena, "({s} {s}IN ({s}))", .{
                    try self.operand(t.expr orelse return error.SqlParseError),
                    if (t.is_not != 0) "NOT " else "",
                    list.items,
                });
            },
            else => return self.operand(node),
        }
    }

    fn literalInt(self: *Builder, node: ?*c.LpNode, what: []const u8) Error!u32 {
        const n = node orelse return self.why.set("{s} needs a number", .{what});
        if (n.kind != c.LP_EXPR_LITERAL_INT) return self.why.set("{s} must be a whole number written in the query", .{what});
        return std.fmt.parseInt(u32, std.mem.span(n.u.literal.value), 10) catch return self.why.set("{s} is out of range", .{what});
    }

    /// How far a RANGE bound reaches: a whole number or a decimal written in the query, or an INTERVAL (which `frameIntervals`
    /// has made a string literal). A negative one is an error in DuckDB, whenever a row has a key.
    fn reach(self: *Builder, node: ?*c.LpNode) Error!wp.Reach {
        const n = node orelse return self.why.set("a RANGE offset needs a number", .{});
        switch (n.kind) {
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => {
                const text = std.mem.span(n.u.literal.value);
                const dot = std.mem.indexOfScalar(u8, text, '.');
                const whole = if (dot) |d| text[0..d] else text;
                const frac = if (dot) |d| text[d + 1 ..] else "";
                if (std.mem.indexOfAny(u8, text, "eE") != null)
                    return self.why.set("a RANGE offset written as a floating-point number is not supported", .{});
                if (whole.len + frac.len > 38 or whole.len + frac.len == 0 or frac.len > 38)
                    return self.why.set("this RANGE offset is out of range", .{});
                var digits: [40]u8 = undefined;
                @memcpy(digits[0..whole.len], whole);
                @memcpy(digits[whole.len..][0..frac.len], frac);
                const mant = std.fmt.parseInt(i128, digits[0 .. whole.len + frac.len], 10) catch return self.why.set("this RANGE offset is out of range", .{});
                const bits: u8 = if (mant <= std.math.maxInt(i32)) 32 else if (mant <= std.math.maxInt(i64)) 64 else 128;
                return .{ .number = .{ .mant = mant, .scale = @intCast(frac.len), .bits = bits } };
            },
            c.LP_EXPR_LITERAL_STRING => {
                const text = std.mem.span(n.u.literal.value);
                if (std.mem.startsWith(u8, text, interval_marker)) {
                    var it = std.mem.splitScalar(u8, text[interval_marker.len..], ':');
                    const months = std.fmt.parseInt(i32, it.next() orelse return error.SqlParseError, 10) catch return error.SqlParseError;
                    const days = std.fmt.parseInt(i32, it.next() orelse return error.SqlParseError, 10) catch return error.SqlParseError;
                    const micros = std.fmt.parseInt(i64, it.next() orelse return error.SqlParseError, 10) catch return error.SqlParseError;
                    return .{ .interval = .{ .months = months, .days = days, .micros = micros } };
                }
            },
            c.LP_EXPR_UNARY_OP => return self.why.set("a negative RANGE offset is an error in DuckDB, and is not supported", .{}),
            else => {},
        }
        return self.why.set("a RANGE offset must be a number or an INTERVAL written in the query", .{});
    }

    fn bound(self: *Builder, node: ?*c.LpNode, rows: bool) Error!wp.Bound {
        const n = node orelse return .current_row;
        const b = n.u.frame_bound;
        return switch (b.type) {
            c.LP_BOUND_CURRENT_ROW => .current_row,
            c.LP_BOUND_UNBOUNDED_PRECEDING => .unbounded_preceding,
            c.LP_BOUND_UNBOUNDED_FOLLOWING => .unbounded_following,
            c.LP_BOUND_PRECEDING => if (rows) .{ .preceding = try self.literalInt(b.expr, "a frame offset") } else .{ .reach_preceding = try self.reach(b.expr) },
            c.LP_BOUND_FOLLOWING => if (rows) .{ .following = try self.literalInt(b.expr, "a frame offset") } else .{ .reach_following = try self.reach(b.expr) },
            else => return self.why.set("this frame bound is not supported", .{}),
        };
    }

    fn frame(self: *Builder, node: ?*c.LpNode) Error!?wp.Frame {
        const n = node orelse return null;
        const f = n.u.window_frame;
        if (f.exclude != c.LP_EXCLUDE_NONE and f.exclude != c.LP_EXCLUDE_NO_OTHERS)
            return self.why.set("EXCLUDE in a window frame is not supported", .{});
        if (f.type != c.LP_FRAME_ROWS and f.type != c.LP_FRAME_RANGE) return self.why.set("GROUPS frames are not supported", .{});
        const rows = f.type == c.LP_FRAME_ROWS;
        const start = try self.bound(f.start, rows);
        const end = try self.bound(f.end, rows);
        if (start == .unbounded_following or end == .unbounded_preceding)
            return self.why.set("a window frame cannot start at UNBOUNDED FOLLOWING or end at UNBOUNDED PRECEDING", .{});
        // DuckDB's parser refuses a frame that starts after it ends in kind (`1 FOLLOWING AND 1 PRECEDING`).
        if (boundClass(start) > boundClass(end)) return self.why.set("this window frame starts after it ends", .{});
        return .{ .rows = rows, .start = start, .end = end };
    }

    /// Register the window call `node`; its result column's name.
    fn window(self: *Builder, node: *c.LpNode) Error![]const u8 {
        const f = node.u.function;
        const raw = std.mem.span(f.name orelse return error.SqlParseError);
        var lower_buf: [32]u8 = undefined;
        const name = if (raw.len <= lower_buf.len) std.ascii.lowerString(&lower_buf, raw) else raw;
        if (f.distinct != 0) return self.why.set("DISTINCT in a window function is not supported", .{});
        if (f.filter != null) return self.why.set("FILTER on a window function is not supported", .{});
        if (f.order_by.count != 0) return self.why.set("ORDER BY inside a window function's arguments is not supported", .{});
        const over: *c.LpNode = f.over orelse return error.SqlParseError;
        if (over.kind != c.LP_WINDOW_DEF) return self.why.set("a named window is not supported", .{});
        const wd = over.u.window_def;
        if (wd.name != null or wd.base_name != null) return self.why.set("a named window is not supported", .{});

        const nargs: usize = @intCast(f.args.count);
        var call: wp.Call = .{ .func = undefined };
        const Kind = enum { ranking, offset, aggregate, value, tile };
        const kind: Kind = blk: {
            if (std.mem.eql(u8, name, "row_number")) call.func = .row_number else if (std.mem.eql(u8, name, "rank")) call.func = .rank else if (std.mem.eql(u8, name, "dense_rank")) call.func = .dense_rank else if (std.mem.eql(u8, name, "percent_rank")) call.func = .percent_rank else if (std.mem.eql(u8, name, "cume_dist")) call.func = .cume_dist else if (std.mem.eql(u8, name, "lag")) {
                call.func = .lag;
                break :blk .offset;
            } else if (std.mem.eql(u8, name, "lead")) {
                call.func = .lead;
                break :blk .offset;
            } else if (std.mem.eql(u8, name, "sum")) {
                call.func = .sum;
                break :blk .aggregate;
            } else if (std.mem.eql(u8, name, "count")) {
                call.func = .count;
                break :blk .aggregate;
            } else if (std.mem.eql(u8, name, "avg")) {
                call.func = .avg;
                break :blk .aggregate;
            } else if (std.mem.eql(u8, name, "min")) {
                call.func = .min;
                break :blk .aggregate;
            } else if (std.mem.eql(u8, name, "max")) {
                call.func = .max;
                break :blk .aggregate;
            } else if (std.mem.eql(u8, name, "first_value")) {
                call.func = .first_value;
                break :blk .value;
            } else if (std.mem.eql(u8, name, "last_value")) {
                call.func = .last_value;
                break :blk .value;
            } else if (std.mem.eql(u8, name, "nth_value")) {
                call.func = .nth_value;
                break :blk .value;
            } else if (std.mem.eql(u8, name, "ntile")) {
                call.func = .ntile;
                break :blk .tile;
            } else return self.why.set("the window function {s} is not supported (row_number, rank, dense_rank, percent_rank, cume_dist, ntile, lag, lead, first_value, last_value, nth_value, sum, count, avg, min and max are)", .{raw});
            break :blk .ranking;
        };
        switch (kind) {
            .ranking => if (nargs != 0) return self.why.set("{s} takes no arguments", .{raw}),
            .offset => {
                if (nargs < 1 or nargs > 3) return self.why.set("{s} takes a value, an offset and a default", .{raw});
                call.arg = try self.column(f.args.items[0] orelse return error.SqlParseError, "argument");
                if (nargs >= 2) call.offset = try self.literalInt(f.args.items[1], "the offset of lag / lead");
                if (nargs == 3) call.default = try self.default(f.args.items[2] orelse return error.SqlParseError);
            },
            .value => {
                const want: usize = if (call.func == .nth_value) 2 else 1;
                if (nargs != want) return self.why.set("{s} takes {d} argument(s)", .{ raw, want });
                call.arg = try self.column(f.args.items[0] orelse return error.SqlParseError, "argument");
                if (call.func == .nth_value) {
                    call.offset = try self.literalInt(f.args.items[1], "the n of nth_value");
                    if (call.offset < 1) return self.why.set("the n of nth_value must be at least 1", .{});
                }
            },
            .tile => {
                if (nargs != 1) return self.why.set("ntile takes the number of buckets", .{});
                call.offset = try self.literalInt(f.args.items[0], "the number of buckets of ntile");
                if (call.offset < 1) return self.why.set("the number of buckets of ntile must be at least 1", .{});
            },
            .aggregate => {
                const first_arg: ?*c.LpNode = if (nargs > 0) f.args.items[0] else null;
                const star = nargs == 0 or (nargs == 1 and (first_arg orelse return error.SqlParseError).kind == c.LP_EXPR_STAR);
                if (star) {
                    if (call.func != .count) return self.why.set("{s}(*) is not supported", .{raw});
                    call.func = .count_star;
                } else if (nargs != 1) return self.why.set("{s} takes one argument", .{raw}) else {
                    call.arg = try self.column(f.args.items[0] orelse return error.SqlParseError, "argument");
                }
            },
        }

        // PARTITION BY and ORDER BY.
        var partition: std.ArrayList([]const u8) = .empty;
        var sort: std.ArrayList(wp.SortKey) = .empty;
        var order: std.ArrayList([]const u8) = .empty;
        for (0..@intCast(wd.partition_by.count)) |i| {
            const col = try self.key(wd.partition_by.items[i] orelse return error.SqlParseError, "PARTITION BY");
            var seen = false;
            for (partition.items) |p| seen = seen or std.mem.eql(u8, p, col);
            if (seen) continue;
            try partition.append(self.arena, col);
            try sort.append(self.arena, .{ .column = col });
        }
        for (0..@intCast(wd.order_by.count)) |i| {
            const term: *c.LpNode = wd.order_by.items[i] orelse return error.SqlParseError;
            const t = term.u.order_term;
            const col = try self.key(t.expr orelse return error.SqlParseError, "ORDER BY");
            try order.append(self.arena, col);
            try sort.append(self.arena, .{
                .column = col,
                .descending = t.direction == c.LP_SORT_DESC,
                .nulls_first = if (t.nulls == c.LP_SORT_ASC) true else if (t.nulls == c.LP_SORT_DESC) false else null,
            });
        }
        if (try self.frame(wd.frame)) |fr| {
            // The grammar hands every window a frame; the default one is no frame at all.
            const is_default = !fr.rows and fr.start == .unbounded_preceding and fr.end == .current_row;
            const reaches = (fr.start == .reach_preceding or fr.start == .reach_following or fr.end == .reach_preceding or fr.end == .reach_following);
            if (reaches and order.items.len != 1) return self.why.set("a RANGE frame with an offset needs exactly one ORDER BY key", .{});
            if (kind != .aggregate and kind != .value and !is_default) return self.why.set("a frame on {s} is not supported", .{raw});
            call.frame = fr;
        }

        // The pass of this PARTITION BY and ORDER BY.
        var pass_index: ?usize = null;
        for (self.passes.items, 0..) |p, pi| {
            if (samePass(p, sort.items, partition.items.len)) {
                pass_index = pi;
                break;
            }
        }
        if (pass_index == null) {
            try self.passes.append(self.arena, .{ .sort = sort.items, .partition = partition.items, .order = order.items });
            pass_index = self.passes.items.len - 1;
        }
        const result = try std.fmt.allocPrint(self.arena, "w{d}", .{self.wins.items.len});
        try self.wins.append(self.arena, .{ .name = result, .pass = pass_index.?, .call = call });
        try self.passes.items[pass_index.?].calls.append(self.arena, self.wins.items.len - 1);
        return result;
    }

    fn default(self: *Builder, node: *c.LpNode) Error!wp.Default {
        switch (node.kind) {
            c.LP_EXPR_LITERAL_NULL => return .null,
            c.LP_EXPR_LITERAL_INT => return .{ .int = std.fmt.parseInt(i64, std.mem.span(node.u.literal.value), 10) catch
                return self.why.set("the default of lag / lead is out of range", .{}) },
            c.LP_EXPR_LITERAL_FLOAT => return .{ .float = std.fmt.parseFloat(f64, std.mem.span(node.u.literal.value)) catch
                return self.why.set("the default of lag / lead is not a number", .{}) },
            c.LP_EXPR_LITERAL_STRING => return .{ .string = try self.arena.dupe(u8, std.mem.span(node.u.literal.value)) },
            c.LP_EXPR_UNARY_OP => {
                const inner: *c.LpNode = node.u.unary.operand orelse return error.SqlParseError;
                if (node.u.unary.op == c.LP_UOP_MINUS) switch (inner.kind) {
                    c.LP_EXPR_LITERAL_INT => return .{ .int = -(std.fmt.parseInt(i64, std.mem.span(inner.u.literal.value), 10) catch
                        return self.why.set("the default of lag / lead is out of range", .{})) },
                    c.LP_EXPR_LITERAL_FLOAT => return .{ .float = -(std.fmt.parseFloat(f64, std.mem.span(inner.u.literal.value)) catch
                        return self.why.set("the default of lag / lead is not a number", .{})) },
                    else => {},
                };
                return self.why.set("the default of lag / lead must be a literal", .{});
            },
            else => return self.why.set("the default of lag / lead must be a literal", .{}),
        }
    }
};

fn boundClass(b: wp.Bound) u8 {
    return switch (b) {
        .unbounded_preceding => 0,
        .preceding, .reach_preceding => 1,
        .current_row => 2,
        .following, .reach_following => 3,
        .unbounded_following => 4,
    };
}

/// A pass is one split of the rows into partitions and peers: the same sort keys and the same count of them that are PARTITION BY
/// (`ORDER BY a, b`, `PARTITION BY a ORDER BY b` and `PARTITION BY a, b` sort alike and split differently).
fn samePass(p: PassB, sort: []const wp.SortKey, partitions: usize) bool {
    return p.partition.len == partitions and sameSort(p.sort, sort);
}

fn sameSort(a: []const wp.SortKey, b: []const wp.SortKey) bool {
    if (a.len != b.len) return false;
    for (a, b) |x, y| {
        if (!std.mem.eql(u8, x.column, y.column) or x.descending != y.descending) return false;
        if ((x.nulls_first orelse false) != (y.nulls_first orelse false)) return false;
    }
    return true;
}

fn hasWindow(node: *c.LpNode) bool {
    var found = false;
    const Visit = struct {
        fn enter(data: [*c]c.LpVisitor, n: [*c]c.LpNode) callconv(.c) c_int {
            const flag: *bool = @ptrCast(@alignCast(data.*.user_data));
            if (n.*.kind == c.LP_EXPR_FUNCTION and n.*.u.function.over != null) flag.* = true;
            return 0;
        }
    };
    var visitor: c.LpVisitor = .{ .user_data = &found, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(node, &visitor);
    return found;
}

/// Where the main clauses start in `text`, at the top level (outside parentheses and quoted text): the FROM, the
/// ORDER BY and the LIMIT / OFFSET after it.
const Clauses = struct { from: ?usize = null, order: ?usize = null, tail: ?usize = null, qualify: ?usize = null };

fn clauses(text: []const u8) Clauses {
    var out: Clauses = .{};
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = sql_setops.quotedEnd(text, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')' and depth > 0) depth -= 1;
        if (depth == 0 and sql_setops.startsWord(text, i)) {
            const word = sql_setops.wordAt(text, i);
            if (sql_setops.eql(word, "from") and out.from == null) out.from = i;
            if (sql_setops.eql(word, "qualify") and out.from != null and out.qualify == null) out.qualify = i;
            if (sql_setops.eql(word, "order") and out.from != null) {
                const next = sql_setops.nextWord(text, i + word.len);
                if (sql_setops.eql(next.word, "by")) out.order = i;
            }
            if ((sql_setops.eql(word, "limit") or sql_setops.eql(word, "offset")) and out.from != null and out.tail == null) out.tail = i;
            i += word.len;
            continue;
        }
        i += 1;
    }
    return out;
}

fn quoteName(arena: std.mem.Allocator, name: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.append(arena, '"');
    for (name) |ch| {
        if (ch == '"') try out.append(arena, '"');
        try out.append(arena, ch);
    }
    try out.append(arena, '"');
    return out.items;
}

/// Plan the query `text` (a NUL-terminated SELECT as the parser reads it). Strings of the plan are allocated from `arena`.
pub fn build(arena: std.mem.Allocator, full_text: [:0]const u8, why: *Refusal, columns: ?[]const []const u8) Error!wp.Plan {
    const lp = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp);
    var error_msg: [*c]const u8 = null;
    // QUALIFY is no clause of the grammar: its predicate is read apart, and the query without it is the query.
    var text: [:0]const u8 = full_text;
    var qualify_text: ?[]const u8 = null;
    const whole = clauses(full_text);
    if (whole.qualify) |at| {
        // DuckDB's grammar puts QUALIFY before ORDER BY and LIMIT; after them it is a syntax error.
        if ((whole.order orelse at) < at or (whole.tail orelse at) < at)
            return why.set("QUALIFY must come before ORDER BY, LIMIT and OFFSET", .{});
        const end = whole.order orelse whole.tail orelse full_text.len;
        qualify_text = std.mem.trim(u8, full_text[at + "qualify".len .. end], " \t\r\n;");
        text = try std.fmt.allocPrintSentinel(arena, "{s} {s}", .{ full_text[0..at], full_text[end..] }, 0);
    }
    const stmts = c.lp_parse_all(text.ptr, lp, &error_msg);
    if (stmts == null or stmts.*.count != 1) return error.SqlParseError;
    const root: *c.LpNode = stmts.*.items[0] orelse return error.SqlParseError;
    if (root.*.kind != c.LP_STMT_SELECT) return why.set("window functions are supported in a SELECT", .{});
    const sel = root.*.u.select;
    if (sel.with != null) return why.set("a window function in a query with WITH is not supported", .{});
    if (sel.distinct != 0) return why.set("SELECT DISTINCT with a window function is not supported", .{});
    if (sel.window_defs.count != 0) return why.set("a WINDOW clause is not supported", .{});
    if (sel.where) |w| if (hasWindow(w)) return why.set("a window function in WHERE is not supported (QUALIFY is not supported yet)", .{});
    if (sel.having) |h| if (hasWindow(h)) return why.set("a window function in HAVING is not supported", .{});
    for (0..@intCast(sel.group_by.count)) |i| if (sel.group_by.items[i]) |g| if (hasWindow(g))
        return why.set("a window function in GROUP BY is not supported", .{});

    var b: Builder = .{ .arena = arena, .lp = lp, .why = why, .columns = columns };
    const cl = clauses(text);
    const from_at = cl.from orelse return why.set("a window query needs a FROM clause", .{});
    const order_at = cl.order;
    const tail_at = cl.tail;
    const from_end = order_at orelse tail_at orelse text.len;
    const from_text = std.mem.trim(u8, text[from_at..from_end], " \t\r\n");

    // The SELECT list.
    var items: std.ArrayList(u8) = .empty;
    var aliases: std.ArrayList([]const u8) = .empty;
    var any_window = false;
    for (0..@intCast(sel.result_columns.count)) |i| {
        const rc: *c.LpNode = sel.result_columns.items[i] orelse return error.SqlParseError;
        const expr: *c.LpNode = if (rc.kind == c.LP_RESULT_COLUMN) rc.u.result_column.expr orelse return error.SqlParseError else rc;
        if (expr.kind == c.LP_EXPR_STAR) return why.set("SELECT * with a window function is not supported", .{});
        const alias: ?[]const u8 = if (rc.kind == c.LP_RESULT_COLUMN and rc.u.result_column.alias != null)
            std.mem.span(rc.u.result_column.alias)
        else
            null;
        const windowed = hasWindow(expr);
        any_window = any_window or windowed;
        const lowered = try b.lower(expr);
        const shown: []const u8 = if (alias) |al| al else if (windowed) return error.SelectAliasRequired else if (expr.kind == c.LP_EXPR_COLUMN_REF and
            expr.u.column_ref.table == null and expr.u.column_ref.schema == null)
            try quoteName(arena, std.mem.span(expr.u.column_ref.column))
        else blk: {
            const name = sql_names.of(arena, expr) catch |err| switch (err) {
                error.Unnameable => return error.SelectAliasRequired,
                error.OutOfMemory => return error.OutOfMemory,
            };
            break :blk try quoteName(arena, name);
        };
        try aliases.append(arena, if (alias) |al| al else shown);
        if (alias) |al| try b.aliases.append(arena, .{ .name = if (al.len >= 2 and al[0] == '"') al[1 .. al.len - 1] else al, .expr = expr });
        if (i > 0) try items.appendSlice(arena, ", ");
        try items.print(arena, "{s} AS {s}", .{ lowered, shown });
    }
    if (!any_window and qualify_text == null) return why.set("no window function found", .{});

    // QUALIFY: the predicate over the window columns, which filters the last file's rows.
    var where_text: []const u8 = "";
    if (qualify_text) |predicate_text| {
        if (predicate_text.len == 0) return error.SqlParseError;
        const wrapped = try std.fmt.allocPrintSentinel(arena, "SELECT {s} FROM t", .{predicate_text}, 0);
        const pstmts = c.lp_parse_all(wrapped.ptr, lp, &error_msg);
        if (pstmts == null or pstmts.*.count != 1) return error.SqlParseError;
        const proot: *c.LpNode = pstmts.*.items[0] orelse return error.SqlParseError;
        if (proot.*.kind != c.LP_STMT_SELECT or proot.*.u.select.result_columns.count != 1) return error.SqlParseError;
        const prc: *c.LpNode = proot.*.u.select.result_columns.items[0] orelse return error.SqlParseError;
        const pexpr: *c.LpNode = if (prc.kind == c.LP_RESULT_COLUMN) prc.u.result_column.expr orelse return error.SqlParseError else prc;
        try b.substituteAliases(pexpr);
        // DuckDB asks for a window function in the select list or in QUALIFY, one of them.
        if (!any_window and !hasWindow(pexpr)) return why.set("a window function must appear in the select list or in QUALIFY", .{});
        where_text = try b.predicate(pexpr);
    }

    // The outer ORDER BY: a position or an output name stays, any other expression is read like a select item.
    var tail: std.ArrayList(u8) = .empty;
    if (sel.order_by.count > 0) {
        try tail.appendSlice(arena, "ORDER BY ");
        for (0..@intCast(sel.order_by.count)) |i| {
            const term: *c.LpNode = sel.order_by.items[i] orelse return error.SqlParseError;
            const t = term.u.order_term;
            const expr: *c.LpNode = t.expr orelse return error.SqlParseError;
            if (i > 0) try tail.appendSlice(arena, ", ");
            var named = false;
            if (expr.kind == c.LP_EXPR_LITERAL_INT) {
                try tail.appendSlice(arena, std.mem.span(expr.u.literal.value));
                named = true;
            } else if (expr.kind == c.LP_EXPR_COLUMN_REF and expr.u.column_ref.table == null) {
                const col = std.mem.span(expr.u.column_ref.column);
                for (aliases.items) |al| {
                    const bare = if (al.len >= 2 and al[0] == '"') al[1 .. al.len - 1] else al;
                    if (std.ascii.eqlIgnoreCase(bare, col)) {
                        try tail.appendSlice(arena, al);
                        named = true;
                        break;
                    }
                }
            }
            if (!named) try tail.appendSlice(arena, try b.lower(expr));
            if (t.direction == c.LP_SORT_DESC) try tail.appendSlice(arena, " DESC");
            if (t.nulls == c.LP_SORT_ASC) try tail.appendSlice(arena, " NULLS FIRST") else if (t.nulls == c.LP_SORT_DESC) try tail.appendSlice(arena, " NULLS LAST");
        }
    }
    if (tail_at) |at| {
        if (tail.items.len > 0) try tail.append(arena, ' ');
        try tail.appendSlice(arena, std.mem.trim(u8, text[at..], " \t\r\n;"));
    }

    // The input stage.
    var input: std.ArrayList(u8) = .empty;
    try input.appendSlice(arena, "SELECT ");
    for (b.inputs.items, 0..) |expr_text, i| {
        if (i > 0) try input.appendSlice(arena, ", ");
        try input.print(arena, "{s} AS \"c{d}\"", .{ expr_text, i });
    }
    try input.append(arena, ' ');
    try input.appendSlice(arena, from_text);

    const passes = try arena.alloc(wp.Pass, b.passes.items.len);
    for (b.passes.items, passes, 0..) |pb, *out, pi| {
        const calls = try arena.alloc(wp.Call, pb.calls.items.len);
        const results = try arena.alloc([]const u8, pb.calls.items.len);
        for (pb.calls.items, calls, results) |wi, *call, *result| {
            call.* = b.wins.items[wi].call;
            result.* = b.wins.items[wi].name;
        }
        _ = pi;
        out.* = .{ .sort = pb.sort, .partition = pb.partition, .order = pb.order, .calls = calls, .results = results };
    }
    return .{ .input_sql = input.items, .passes = passes, .final_items = items.items, .final_where = where_text, .final_tail = tail.items };
}

test "windows with the same sort and a different split are different passes" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const text: [:0]const u8 = "SELECT sum(v) OVER (ORDER BY a, b) AS s1, sum(v) OVER (PARTITION BY a ORDER BY b) AS s2, " ++
        "sum(v) OVER (PARTITION BY a, b) AS s3, rank() OVER (PARTITION BY a ORDER BY b) AS r FROM t";
    const plan = try build(a, text, &why, null);
    try std.testing.expectEqual(@as(usize, 3), plan.passes.len);
    try std.testing.expectEqual(@as(usize, 0), plan.passes[0].partition.len);
    try std.testing.expectEqual(@as(usize, 1), plan.passes[1].partition.len);
    try std.testing.expectEqual(@as(usize, 2), plan.passes[2].partition.len);
    // The rank joins the pass of the same split.
    try std.testing.expectEqual(@as(usize, 2), plan.passes[1].calls.len);
    // The input query has no ORDER BY: the rows leave in the order they came.
    try std.testing.expect(std.mem.indexOf(u8, plan.input_sql, "ORDER BY") == null);
}

test "QUALIFY is the final query's WHERE over the window columns, and a column of the table wins over an output name" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const text: [:0]const u8 = "SELECT id, rank() OVER (PARTITION BY g ORDER BY v) AS r FROM t QUALIFY r <= 2 AND row_number() OVER (ORDER BY id) BETWEEN 1 AND 9 ORDER BY id LIMIT 3";
    const plan = try build(a, text, &why, &.{ "id", "g", "v" });
    try std.testing.expectEqualStrings("((\"w0\" <= 2) AND (\"w1\" BETWEEN 1 AND 9))", plan.final_where);
    try std.testing.expectEqual(@as(usize, 2), plan.passes.len);
    try std.testing.expect(std.mem.indexOf(u8, plan.final_tail, "LIMIT 3") != null);
    // `r` is a column of this table: QUALIFY reads the column, not the window call that is called `r`.
    const column_first = try build(a, text, &why, &.{ "id", "g", "v", "r" });
    try std.testing.expect(std.mem.indexOf(u8, column_first.final_where, "\"w0\" <= 2") == null);
    try std.testing.expect(std.mem.indexOf(u8, column_first.final_where, "<= 2") != null);
    // And with nothing but that column QUALIFY holds no window call, which DuckDB takes when the select list has one.
    const only_column: [:0]const u8 = "SELECT id, rank() OVER (ORDER BY v) AS r FROM t QUALIFY r <= 2";
    const filtered = try build(a, only_column, &why, &.{ "id", "v", "r" });
    try std.testing.expect(std.mem.indexOf(u8, filtered.final_where, "\"w0\"") == null);
    // Without the table's columns the name cannot be told, and is refused rather than guessed.
    try std.testing.expectError(error.WindowNotSupported, build(a, text, &why, null));
}

test "a window partitions and orders by a bare AVG as the DOUBLE, and its argument reads the column as printed" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const plan = try build(a, "SELECT g, avg(d) AS m, rank() OVER (PARTITION BY avg(e) ORDER BY avg(d)) AS r, lag(avg(d)) OVER (ORDER BY g) AS p FROM t GROUP BY g", &why, &.{ "g", "d", "e" });
    try std.testing.expect(std.mem.indexOf(u8, plan.input_sql, "avg(d) AS \"c1\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.input_sql, "avg(e) + 0 AS") != null);
    try std.testing.expect(std.mem.indexOf(u8, plan.input_sql, "avg(d) + 0 AS") != null);
}

test "QUALIFY binds a name to the last select item of that alias, in any select item and anywhere in the condition" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const columns: []const []const u8 = &.{ "id", "v" };
    // The last `r` is the constant, not the rank.
    const dup = try build(a, "SELECT rank() OVER (ORDER BY id) AS r, 100 AS r FROM t QUALIFY r = 100", &why, columns);
    try std.testing.expectEqualStrings("(100 = 100)", dup.final_where);
    // An alias of an expression with no window call.
    const plain = try build(a, "SELECT id, v + 1 AS x, row_number() OVER (ORDER BY id) AS rn FROM t QUALIFY x > 15 AND rn <= 4 ORDER BY id", &why, columns);
    try std.testing.expectEqualStrings("((\"c1\" > 15) AND (\"w0\" <= 4))", plain.final_where);
    try std.testing.expect(std.mem.indexOf(u8, plain.input_sql, "v + 1 AS \"c1\"") != null);
    // An alias inside a call.
    const nested = try build(a, "SELECT id, rank() OVER (ORDER BY v) AS r FROM t QUALIFY abs(r) > 2 ORDER BY id", &why, columns);
    try std.testing.expectEqualStrings("(abs(\"w0\") > 2)", nested.final_where);
    // The window written in the select list and again through its alias is one column.
    const once = try build(a, "SELECT rank() OVER (ORDER BY v) AS r FROM t QUALIFY r <= 2 AND rank() OVER (ORDER BY v) < 9", &why, columns);
    try std.testing.expectEqual(@as(usize, 1), once.passes[0].calls.len);
    // A window argument that is an alias is that expression.
    const arg = try build(a, "SELECT id, v + 1 AS x FROM t QUALIFY sum(x) OVER (ORDER BY id) > 40", &why, columns);
    try std.testing.expect(std.mem.indexOf(u8, arg.input_sql, "v + 1") != null);
}

test "QUALIFY comes before ORDER BY and LIMIT, and a window function must be somewhere in the query" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const columns: []const []const u8 = &.{ "id", "v" };
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT id, rank() OVER (ORDER BY id) AS r FROM t ORDER BY id QUALIFY r > 2", &why, columns));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT id, rank() OVER (ORDER BY id) AS r FROM t LIMIT 3 QUALIFY r > 2", &why, columns));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT id, v + 1 AS x FROM t QUALIFY x > 20", &why, columns));
    _ = try build(a, "SELECT id, rank() OVER (ORDER BY id) AS r FROM t QUALIFY id > 2", &why, columns);
}

test "windows: a RANGE frame's offset is a reach from the ORDER BY key, in the width DuckDB gives the literal" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    const text: [:0]const u8 = "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN 3 PRECEDING AND 2 FOLLOWING) AS a, " ++
        "count(*) OVER (ORDER BY i RANGE BETWEEN 1000000000000 PRECEDING AND 1.5 FOLLOWING) AS b, " ++
        "sum(v) OVER (ORDER BY i ROWS BETWEEN 1 PRECEDING AND 1 FOLLOWING) AS c, " ++
        "sum(v) OVER (ORDER BY i DESC RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS d FROM t";
    const plan = try build(a, text, &why, null);
    // Three ORDER BY i calls share one pass; the DESC one sorts its own way.
    try std.testing.expectEqual(@as(usize, 2), plan.passes.len);
    const calls = plan.passes[0].calls;
    try std.testing.expectEqual(@as(usize, 3), calls.len);
    try std.testing.expectEqual(wp.Bound{ .reach_preceding = .{ .number = .{ .mant = 3, .scale = 0, .bits = 32 } } }, calls[0].frame.start);
    try std.testing.expectEqual(wp.Bound{ .reach_following = .{ .number = .{ .mant = 2, .scale = 0, .bits = 32 } } }, calls[0].frame.end);
    // A literal past INT32 is added in BIGINT; a decimal keeps its scale.
    try std.testing.expectEqual(wp.Bound{ .reach_preceding = .{ .number = .{ .mant = 1000000000000, .scale = 0, .bits = 64 } } }, calls[1].frame.start);
    try std.testing.expectEqual(wp.Bound{ .reach_following = .{ .number = .{ .mant = 15, .scale = 1, .bits = 32 } } }, calls[1].frame.end);
    // ROWS keeps counting rows.
    try std.testing.expect(calls[2].frame.rows);
    try std.testing.expectEqual(wp.Bound{ .preceding = 1 }, calls[2].frame.start);
    try std.testing.expectEqual(wp.Bound{ .following = 1 }, calls[2].frame.end);
    // Under DESC a reach is still the ORDER BY key's own reach: the pass says which way it sorts.
    try std.testing.expect(plan.passes[1].sort[0].descending);
    try std.testing.expectEqual(wp.Bound{ .reach_preceding = .{ .number = .{ .mant = 1, .scale = 0, .bits = 32 } } }, plan.passes[1].calls[0].frame.start);
}

test "windows: an INTERVAL offset reaches through the frame marker string the parser leaves" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    var report: sql_literals.Report = .{};
    const sql = "SELECT count(*) OVER (ORDER BY ts RANGE BETWEEN INTERVAL '2 days 3 hours' PRECEDING AND INTERVAL 1 MONTH FOLLOWING) AS n FROM t";
    const framed = try sql_literals.frameIntervals(a, sql, &report);
    const plan = try build(a, try std.fmt.allocPrintSentinel(a, "{s}", .{framed}, 0), &why, null);
    const call = plan.passes[0].calls[0];
    try std.testing.expectEqual(wp.Bound{ .reach_preceding = .{ .interval = .{ .months = 0, .days = 2, .micros = 10_800_000_000 } } }, call.frame.start);
    try std.testing.expectEqual(wp.Bound{ .reach_following = .{ .interval = .{ .months = 1, .days = 0, .micros = 0 } } }, call.frame.end);
}

test "windows: a RANGE offset is refused where no key or offset of the query reaches" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var why: Refusal = .{};
    // DuckDB binds these as errors too: an offset needs exactly one ORDER BY key, and a value a key can be moved by.
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (ORDER BY i, v RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (RANGE BETWEEN 1 PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN -1 PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN 1e5 PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN v PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    try std.testing.expectError(error.WindowNotSupported, build(a, "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN NULL PRECEDING AND CURRENT ROW) AS s FROM t", &why, null));
    // An offset that reaches the other way from the key is what the engine refuses, not the planner.
    _ = try build(a, "SELECT sum(v) OVER (ORDER BY i RANGE BETWEEN 0 PRECEDING AND 0 FOLLOWING) AS s FROM t", &why, null);
}
