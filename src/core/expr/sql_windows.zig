//! A query with window functions in its SELECT list, planned as stages (`window_plan.zig`): the query without its window
//! calls, ordered by the first call's PARTITION BY / ORDER BY, into a file whose columns are named `c0`, `c1`, ...; a
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

    /// `node` for the final query: a window call its column, a window-free expression its input column, and an
    /// operation on those rebuilt around them.
    fn lower(self: *Builder, node: *c.LpNode) Error![]const u8 {
        if (node.kind == c.LP_RESULT_COLUMN) return self.lower(node.u.result_column.expr orelse return error.SqlParseError);
        if (node.kind == c.LP_EXPR_FUNCTION and node.u.function.over != null) {
            const name = try self.window(node);
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
                const left = try self.lower(b.left orelse return error.SqlParseError);
                const right = try self.lower(b.right orelse return error.SqlParseError);
                return std.fmt.allocPrint(self.arena, "({s} {s} {s})", .{ left, op, right });
            },
            c.LP_EXPR_UNARY_OP => {
                if (node.u.unary.op != c.LP_UOP_MINUS) return self.why.set("a window function inside this operation is not supported", .{});
                return std.fmt.allocPrint(self.arena, "(-{s})", .{try self.lower(node.u.unary.operand orelse return error.SqlParseError)});
            },
            c.LP_EXPR_FUNCTION => {
                const f = node.u.function;
                if (f.distinct != 0 or f.filter != null or f.order_by.count != 0) return self.why.set("a window function inside this call is not supported", .{});
                var args: std.ArrayList(u8) = .empty;
                for (0..@intCast(f.args.count)) |i| {
                    if (i > 0) try args.appendSlice(self.arena, ", ");
                    try args.appendSlice(self.arena, try self.lower(f.args.items[i] orelse return error.SqlParseError));
                }
                return std.fmt.allocPrint(self.arena, "{s}({s})", .{ std.mem.span(f.name), args.items });
            },
            c.LP_EXPR_CAST => return std.fmt.allocPrint(self.arena, "CAST({s} AS {s})", .{
                try self.lower(node.u.cast.expr orelse return error.SqlParseError),
                std.mem.span(node.u.cast.type_name),
            }),
            else => return self.why.set("a window function inside this expression is not supported", .{}),
        }
    }

    fn literalInt(self: *Builder, node: ?*c.LpNode, what: []const u8) Error!u32 {
        const n = node orelse return self.why.set("{s} needs a number", .{what});
        if (n.kind != c.LP_EXPR_LITERAL_INT) return self.why.set("{s} must be a whole number written in the query", .{what});
        return std.fmt.parseInt(u32, std.mem.span(n.u.literal.value), 10) catch return self.why.set("{s} is out of range", .{what});
    }

    fn bound(self: *Builder, node: ?*c.LpNode) Error!wp.Bound {
        const n = node orelse return .current_row;
        const b = n.u.frame_bound;
        return switch (b.type) {
            c.LP_BOUND_CURRENT_ROW => .current_row,
            c.LP_BOUND_UNBOUNDED_PRECEDING => .unbounded_preceding,
            c.LP_BOUND_UNBOUNDED_FOLLOWING => .unbounded_following,
            c.LP_BOUND_PRECEDING => .{ .preceding = try self.literalInt(b.expr, "a frame offset") },
            c.LP_BOUND_FOLLOWING => .{ .following = try self.literalInt(b.expr, "a frame offset") },
            else => return self.why.set("this frame bound is not supported", .{}),
        };
    }

    fn frame(self: *Builder, node: ?*c.LpNode) Error!?wp.Frame {
        const n = node orelse return null;
        const f = n.u.window_frame;
        if (f.exclude != c.LP_EXCLUDE_NONE and f.exclude != c.LP_EXCLUDE_NO_OTHERS)
            return self.why.set("EXCLUDE in a window frame is not supported", .{});
        const start = try self.bound(f.start);
        const end = try self.bound(f.end);
        if (start == .unbounded_following or end == .unbounded_preceding)
            return self.why.set("a window frame cannot start at UNBOUNDED FOLLOWING or end at UNBOUNDED PRECEDING", .{});
        // DuckDB's parser refuses a frame that starts after it ends in kind (`1 FOLLOWING AND 1 PRECEDING`).
        if (boundClass(start) > boundClass(end)) return self.why.set("this window frame starts after it ends", .{});
        switch (f.type) {
            c.LP_FRAME_ROWS => return .{ .rows = true, .start = start, .end = end },
            c.LP_FRAME_RANGE => {
                if (start != .unbounded_preceding or (end != .current_row and end != .unbounded_following))
                    return self.why.set("RANGE frames are supported only as UNBOUNDED PRECEDING to CURRENT ROW or to UNBOUNDED FOLLOWING", .{});
                return .{ .rows = false, .start = start, .end = end };
            },
            else => return self.why.set("GROUPS frames are not supported", .{}),
        }
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
        const Kind = enum { ranking, offset, aggregate };
        const kind: Kind = blk: {
            if (std.mem.eql(u8, name, "row_number")) call.func = .row_number else if (std.mem.eql(u8, name, "rank")) call.func = .rank else if (std.mem.eql(u8, name, "dense_rank")) call.func = .dense_rank else if (std.mem.eql(u8, name, "lag")) {
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
            } else return self.why.set("the window function {s} is not supported (row_number, rank, dense_rank, lag, lead, sum, count, avg, min and max are)", .{raw});
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
            const col = try self.column(wd.partition_by.items[i] orelse return error.SqlParseError, "PARTITION BY");
            var seen = false;
            for (partition.items) |p| seen = seen or std.mem.eql(u8, p, col);
            if (seen) continue;
            try partition.append(self.arena, col);
            try sort.append(self.arena, .{ .column = col });
        }
        for (0..@intCast(wd.order_by.count)) |i| {
            const term: *c.LpNode = wd.order_by.items[i] orelse return error.SqlParseError;
            const t = term.u.order_term;
            const col = try self.column(t.expr orelse return error.SqlParseError, "ORDER BY");
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
            if (kind != .aggregate and !is_default) return self.why.set("a frame on {s} is not supported", .{raw});
            call.frame = fr;
        }

        // The pass of this PARTITION BY and ORDER BY.
        var pass_index: ?usize = null;
        for (self.passes.items, 0..) |p, pi| {
            if (sameSort(p.sort, sort.items)) {
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
                const operand: *c.LpNode = node.u.unary.operand orelse return error.SqlParseError;
                if (node.u.unary.op == c.LP_UOP_MINUS) switch (operand.kind) {
                    c.LP_EXPR_LITERAL_INT => return .{ .int = -(std.fmt.parseInt(i64, std.mem.span(operand.u.literal.value), 10) catch
                        return self.why.set("the default of lag / lead is out of range", .{})) },
                    c.LP_EXPR_LITERAL_FLOAT => return .{ .float = -(std.fmt.parseFloat(f64, std.mem.span(operand.u.literal.value)) catch
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
        .preceding => 1,
        .current_row => 2,
        .following => 3,
        .unbounded_following => 4,
    };
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
const Clauses = struct { from: ?usize = null, order: ?usize = null, tail: ?usize = null };

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

/// `keys` as ORDER BY terms: by column name, or, for the input stage whose columns are `c0`, `c1`, ... in order, by position
/// (a quoted name of a key is not found by every aggregate path, a position is).
fn sortClause(arena: std.mem.Allocator, out: *std.ArrayList(u8), keys: []const wp.SortKey, positions: bool) !void {
    for (keys, 0..) |k, i| {
        if (i > 0) try out.appendSlice(arena, ", ");
        if (positions) {
            try out.print(arena, "{d}", .{(std.fmt.parseInt(usize, k.column[1..], 10) catch 0) + 1});
        } else try out.print(arena, "\"{s}\"", .{k.column});
        if (k.descending) try out.appendSlice(arena, " DESC");
        if (k.nulls_first) |first| try out.appendSlice(arena, if (first) " NULLS FIRST" else " NULLS LAST");
    }
}

/// The sort of a pass, as the text of an ORDER BY's terms.
pub fn sortText(arena: std.mem.Allocator, keys: []const wp.SortKey) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try sortClause(arena, &out, keys, false);
    return out.items;
}

/// Plan the query `text` (a NUL-terminated SELECT as the parser reads it). Strings of the plan are allocated from `arena`.
pub fn build(arena: std.mem.Allocator, text: [:0]const u8, why: *Refusal) Error!wp.Plan {
    const lp = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp);
    var error_msg: [*c]const u8 = null;
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

    var b: Builder = .{ .arena = arena, .lp = lp, .why = why };
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
        if (i > 0) try items.appendSlice(arena, ", ");
        try items.print(arena, "{s} AS {s}", .{ lowered, shown });
    }
    if (!any_window) return why.set("no window function found", .{});

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
    const first = b.passes.items[0];
    if (first.sort.len > 0) {
        try input.appendSlice(arena, " ORDER BY ");
        try sortClause(arena, &input, first.sort, true);
    }

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
    return .{ .input_sql = input.items, .passes = passes, .final_items = items.items, .final_tail = tail.items };
}
