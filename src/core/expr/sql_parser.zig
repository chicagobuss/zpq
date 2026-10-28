const std = @import("std");
const c = @import("liteparser_c");
const ast = @import("ast.zig");

// CLI machine diagnostics use the same parser messages without mixing prose into JSON stderr.
pub threadlocal var diagnostic_fn: ?*const fn ([]const u8) void = null;
fn diagnosticPrint(comptime fmt: []const u8, args: anytype) void {
    if (diagnostic_fn) |report| {
        var buffer: [2048]u8 = undefined;
        report(std.fmt.bufPrint(&buffer, fmt, args) catch return);
    } else std.debug.print(fmt, args);
}

pub const ParsedQuery = struct {
    table_name: []const u8,
    is_aggregate: bool,
    select_str: ?[]const u8,
    aggregate_str: ?[]const u8,
    filter_str: ?[]const u8,
    group_by_str: ?[]const u8 = null,
    /// The result columns, expression and alias apart, for GROUP BY output binding.
    select_cols: []const ast.SelectColumn = &.{},
    /// ORDER BY / LIMIT / OFFSET of an aggregate query, applied to its result rows.
    result_order: ast.ResultOrder = .{},
    /// Holds `result_order.having`.
    having_arena: ?*std.heap.ArenaAllocator = null,

    pub fn deinit(self: ParsedQuery, allocator: std.mem.Allocator) void {
        freeResultOrder(allocator, self.result_order);
        if (self.having_arena) |ha| {
            ha.deinit();
            allocator.destroy(ha);
        }
        allocator.free(self.table_name);
        if (self.select_str) |s| allocator.free(s);
        if (self.aggregate_str) |a| allocator.free(a);
        if (self.filter_str) |f| allocator.free(f);
        if (self.group_by_str) |g| allocator.free(g);
        freeSelectCols(allocator, self.select_cols);
    }
};

fn freeResultOrder(allocator: std.mem.Allocator, order: ast.ResultOrder) void {
    allocator.free(order.keys);
    for (order.rows) |k| allocator.free(k.expr);
    allocator.free(order.rows);
    freeSelectCols(allocator, order.hidden);
}

fn freeSelectCols(allocator: std.mem.Allocator, cols: []const ast.SelectColumn) void {
    for (cols) |col| {
        allocator.free(col.expr);
        if (col.alias) |a| allocator.free(a);
    }
    allocator.free(cols);
}

fn stripQuotes(name: []const u8) []const u8 {
    if (name.len >= 2 and ((name[0] == '\'' and name[name.len - 1] == '\'') or (name[0] == '"' and name[name.len - 1] == '"'))) {
        return name[1 .. name.len - 1];
    }
    return name;
}

/// What an expression subtree contains. Drives both routing (aggregate →
/// `--aggregate`) and rejection (window → not supported). Collected in a
/// single walk so we never traverse twice.
const ExprInfo = struct {
    has_agg: bool = false,
    has_window: bool = false,
    /// A column reference that is NOT inside an aggregate call. Mixing one of
    /// these with an aggregate in the SELECT list is implicit GROUP BY.
    has_bare_col: bool = false,
    /// `count(DISTINCT x)` — distinct on the function node itself.
    has_func_distinct: bool = false,
    /// EXISTS / scalar subquery / IN (SELECT ...).
    has_subquery: bool = false,
    fn merge(self: *ExprInfo, o: ExprInfo) void {
        self.has_agg = self.has_agg or o.has_agg;
        self.has_window = self.has_window or o.has_window;
        self.has_bare_col = self.has_bare_col or o.has_bare_col;
        self.has_func_distinct = self.has_func_distinct or o.has_func_distinct;
        self.has_subquery = self.has_subquery or o.has_subquery;
    }
};

fn analyzeExpr(node: *const c.LpNode) ExprInfo {
    var info: ExprInfo = .{};
    switch (node.kind) {
        c.LP_EXPR_FUNCTION => {
            var is_agg = false;
            var is_count = false;
            if (node.u.function.name) |nm| {
                const name = std.mem.span(nm);
                if (std.ascii.eqlIgnoreCase(name, "count")) {
                    is_agg = true;
                    is_count = true;
                } else if (std.ascii.eqlIgnoreCase(name, "sum") or
                    std.ascii.eqlIgnoreCase(name, "avg") or
                    std.ascii.eqlIgnoreCase(name, "avg_state") or
                    std.ascii.eqlIgnoreCase(name, "min") or
                    std.ascii.eqlIgnoreCase(name, "max")) is_agg = true;
            }
            if (is_agg) info.has_agg = true;
            if (node.u.function.over != null) info.has_window = true; // `f() OVER (...)`
            if (node.u.function.distinct != 0) {
                if (!is_count) info.has_func_distinct = true; // `sum(DISTINCT x)`
            }
            var child: ExprInfo = .{};
            var i: usize = 0;
            while (i < @as(usize, @intCast(node.u.function.args.count))) : (i += 1) {
                if (node.u.function.args.items[i]) |arg| child.merge(analyzeExpr(arg));
            }
            if (node.u.function.filter) |flt| child.merge(analyzeExpr(flt));
            // Column refs inside an aggregate are aggregated, not "bare" —
            // so `sum(a)` is pure-agg, while `sum(a) + b` keeps b's bare flag.
            if (is_agg) child.has_bare_col = false;
            info.merge(child);
        },
        c.LP_EXPR_BINARY_OP => {
            if (node.u.binary.left) |l| info.merge(analyzeExpr(l));
            if (node.u.binary.right) |r| info.merge(analyzeExpr(r));
        },
        c.LP_EXPR_UNARY_OP => {
            if (node.u.unary.operand) |o| info.merge(analyzeExpr(o));
        },
        c.LP_EXPR_CAST => {
            if (node.u.cast.expr) |e| info.merge(analyzeExpr(e));
        },
        c.LP_EXPR_COLLATE => {
            if (node.u.collate.expr) |e| info.merge(analyzeExpr(e));
        },
        c.LP_EXPR_BETWEEN => {
            if (node.u.between.expr) |e| info.merge(analyzeExpr(e));
            if (node.u.between.low) |lo| info.merge(analyzeExpr(lo));
            if (node.u.between.high) |hi| info.merge(analyzeExpr(hi));
        },
        c.LP_EXPR_IN => {
            if (node.u.in.expr) |e| info.merge(analyzeExpr(e));
            if (node.u.in.select != null) info.has_subquery = true; // IN (SELECT ...)
            var i: usize = 0;
            while (i < @as(usize, @intCast(node.u.in.values.count))) : (i += 1) {
                if (node.u.in.values.items[i]) |v| info.merge(analyzeExpr(v));
            }
        },
        c.LP_EXPR_COLUMN_REF => info.has_bare_col = true,
        c.LP_EXPR_EXISTS, c.LP_EXPR_SUBQUERY => info.has_subquery = true,
        c.LP_EXPR_CASE => {
            if (node.u.case_.operand) |op| info.merge(analyzeExpr(op));
            var i: usize = 0;
            while (i < @as(usize, @intCast(node.u.case_.when_exprs.count))) : (i += 1) {
                if (node.u.case_.when_exprs.items[i]) |w| info.merge(analyzeExpr(w));
            }
            if (node.u.case_.else_expr) |e| info.merge(analyzeExpr(e));
        },
        c.LP_RESULT_COLUMN => {
            if (node.u.result_column.expr) |e| info.merge(analyzeExpr(e));
        },
        else => {},
    }
    return info;
}

/// Reject SELECT clauses zpq doesn't implement with a clear, named error
/// instead of silently ignoring them and returning a confidently wrong
/// answer. These are outside the bounded streaming SQL surface, not
/// forbidden forever, so the messages say "not yet".
fn rejectUnsupportedClauses(sel: anytype) !void {
    if (sel.distinct != 0) {
        diagnosticPrint("sql: DISTINCT is not supported yet\n", .{});
        return error.DistinctNotSupported;
    }
    // GROUP BY and HAVING are supported.
    // ORDER BY / LIMIT / OFFSET are resolved against the result columns in `resultOrder`.
    if (sel.window_defs.count > 0) {
        diagnosticPrint("sql: window functions are not supported yet\n", .{});
        return error.WindowNotSupported;
    }
    if (sel.with != null) {
        diagnosticPrint("sql: WITH / CTE is not supported yet\n", .{});
        return error.CteNotSupported;
    }
}

/// Column references reach the flag parsers (and `metadata.resolveColumn`) spelled as the flag forms would spell
/// them: bare `a.b`, which binds a top-level column so named before the path, or the quoted path `"a"."b"`, which
/// binds the path only. A path with any quoted segment is quoted whole, as the flag forms have no mixed spelling and
/// SQL means the same path either way.
fn quoteWholePaths(root: *c.LpNode) void {
    const Visit = struct {
        fn enter(_: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            if (node.*.kind != c.LP_EXPR_COLUMN_REF) return 0;
            const ref = &node.*.u.column_ref;
            if (ref.quoted == 0) return 0;
            if (ref.schema != null) ref.quoted |= c.LP_QUOTED_SCHEMA;
            if (ref.table != null) ref.quoted |= c.LP_QUOTED_TABLE;
            ref.quoted |= c.LP_QUOTED_COLUMN;
            return 0;
        }
    };
    var visitor: c.LpVisitor = .{ .user_data = null, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(root, &visitor);
}

/// The grammar reads an unquoted `true` / `false` as a column name, which the unparser then quotes (`"true"`), and
/// no column of that name exists: make each such reference the boolean literal SQL means. A quoted `"true"` stays a
/// column.
fn boolLiterals(root: *c.LpNode) void {
    const Visit = struct {
        fn enter(_: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            if (node.*.kind != c.LP_EXPR_COLUMN_REF) return 0;
            const ref = &node.*.u.column_ref;
            if (ref.quoted != 0 or ref.schema != null or ref.table != null or ref.column == null) return 0;
            const name = std.mem.span(ref.column);
            const value: [*c]const u8 = if (std.ascii.eqlIgnoreCase(name, "true"))
                "TRUE"
            else if (std.ascii.eqlIgnoreCase(name, "false"))
                "FALSE"
            else
                return 0;
            node.*.kind = c.LP_EXPR_LITERAL_BOOL;
            node.*.u = .{ .literal = .{ .value = @constCast(value) } };
            return 0;
        }
    };
    var visitor: c.LpVisitor = .{ .user_data = null, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(root, &visitor);
}

/// Refuse a predicate in WHERE, a FILTER or a CASE condition whose value side reads a column (`s = t`,
/// `s IN ('a', t)`, `x BETWEEN 1 AND y`): the filter grammar compares a column with literals only, and before this a
/// value-side identifier, a column or a misspelt one, could bind as literal text and match incorrect rows.
fn rejectColumnComparisons(root: *c.LpNode) error{BadValue}!void {
    const Visit = struct {
        found: ?*c.LpNode = null,

        fn readsColumn(node: ?*c.LpNode) bool {
            const n = node orelse return false;
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
            c.lp_ast_walk(n, &visitor);
            return find.hit;
        }

        fn enter(v: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(v.*.user_data));
            const bad = switch (node.*.kind) {
                c.LP_EXPR_BINARY_OP => switch (node.*.u.binary.op) {
                    c.LP_OP_EQ,
                    c.LP_OP_NE,
                    c.LP_OP_LT,
                    c.LP_OP_LE,
                    c.LP_OP_GT,
                    c.LP_OP_GE,
                    c.LP_OP_LIKE,
                    c.LP_OP_GLOB,
                    c.LP_OP_IS,
                    c.LP_OP_ISNOT,
                    => readsColumn(node.*.u.binary.right) or readsColumn(node.*.u.binary.escape),
                    else => false,
                },
                c.LP_EXPR_BETWEEN => readsColumn(node.*.u.between.low) or readsColumn(node.*.u.between.high),
                c.LP_EXPR_IN => for (0..@intCast(node.*.u.in.values.count)) |i| {
                    if (readsColumn(node.*.u.in.values.items[i])) break true;
                } else false,
                else => false,
            };
            if (!bad) return 0;
            self.found = node;
            return 2;
        }
    };
    var state: Visit = .{};
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    if (root.u.select.where) |w| c.lp_ast_walk(w, &visitor);
    var i: usize = 0;
    while (state.found == null and i < @as(usize, @intCast(root.u.select.result_columns.count))) : (i += 1) {
        if (root.u.select.result_columns.items[i]) |rc| c.lp_ast_walk(rc, &visitor);
    }
    if (state.found == null) return;
    diagnosticPrint("sql: a comparison takes a literal on its right, not a column or an expression of one\n", .{});
    return error.BadValue;
}

/// Push every NOT down to the leaves (De Morgan; `NOT NOT p` is `p`), in place. liteparser prints a NOT's operand
/// without parentheses, so `NOT (a AND b)` unparsed as `NOT a AND b`, which the filter grammar (as SQL) reads as
/// `(NOT a) AND b`: a different predicate. With NOT only above leaves, every unparsed predicate (WHERE, aggregate
/// FILTER) means what it was written as. Rewriting at enter, before the walker reads the node's children, means the
/// NOTs it creates are visited and pushed further in turn.
fn pushNotsDown(root: *c.LpNode, lp_arena: *c.arena_t) error{OutOfMemory}!void {
    const Visit = struct {
        failed: bool = false,
        arena: *anyopaque,

        fn isNot(n: *const c.LpNode) bool {
            return n.kind == c.LP_EXPR_UNARY_OP and n.u.unary.op == c.LP_UOP_NOT and n.u.unary.operand != null;
        }
        fn enter(visitor: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(visitor.*.user_data));
            const arena: *c.arena_t = @ptrCast(@alignCast(self.arena));
            while (isNot(node)) {
                const operand: *c.LpNode = node.*.u.unary.operand;
                if (isNot(operand)) {
                    node.* = operand.u.unary.operand.*;
                    continue;
                }
                if (operand.kind != c.LP_EXPR_BINARY_OP) break;
                const op = operand.u.binary.op;
                if (op != c.LP_OP_AND and op != c.LP_OP_OR) break;
                const left = c.lp_node_alloc(arena, c.LP_EXPR_UNARY_OP) orelse {
                    self.failed = true;
                    return 2;
                };
                const right = c.lp_node_alloc(arena, c.LP_EXPR_UNARY_OP) orelse {
                    self.failed = true;
                    return 2;
                };
                left.*.u.unary = .{ .op = c.LP_UOP_NOT, .operand = operand.u.binary.left };
                right.*.u.unary = .{ .op = c.LP_UOP_NOT, .operand = operand.u.binary.right };
                node.* = operand.*;
                node.*.u.binary.op = if (op == c.LP_OP_AND) c.LP_OP_OR else c.LP_OP_AND;
                node.*.u.binary.left = left;
                node.*.u.binary.right = right;
            }
            return 0;
        }
    };
    var state: Visit = .{ .arena = lp_arena };
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(root, &visitor);
    if (state.failed) return error.OutOfMemory;
}

fn unparseResultColumn(rc: *c.LpNode, lp_arena: *c.arena_t, allocator: std.mem.Allocator) ![]const u8 {
    // NOTE: lp_ast_to_sql aborts if handed a result-column wrapper — it must
    // get the inner expression. So unparse the expr, then append the alias.
    // (Residual minor limitation: an alias needing quotes — e.g. `AS "My Space"`
    // — isn't re-quoted; such an alias would fail downstream, not silently.)
    if (rc.kind == c.LP_RESULT_COLUMN) {
        const expr_c = c.lp_ast_to_sql(rc.u.result_column.expr, lp_arena);
        if (expr_c == null) return error.UnparseError;
        const expr_slice = std.mem.span(expr_c);
        if (rc.u.result_column.alias != null) {
            const alias_slice = std.mem.span(rc.u.result_column.alias);
            return std.fmt.allocPrint(allocator, "{s} AS {s}", .{ expr_slice, alias_slice });
        }
        return allocator.dupe(u8, expr_slice);
    }
    const expr_c = c.lp_ast_to_sql(rc, lp_arena);
    if (expr_c == null) return error.UnparseError;
    return allocator.dupe(u8, std.mem.span(expr_c));
}

/// `rc` as a `SelectColumn`: its expression unparsed, its alias as written. Fills `out` field by field, so a caller
/// freeing `out` on failure frees only what was allocated.
fn selectColumn(rc: *c.LpNode, lp_arena: *c.arena_t, allocator: std.mem.Allocator, out: *ast.SelectColumn) !void {
    const expr_node = if (rc.kind == c.LP_RESULT_COLUMN) rc.u.result_column.expr else rc;
    const expr_c = c.lp_ast_to_sql(expr_node, lp_arena);
    if (expr_c == null) return error.UnparseError;
    out.expr = try allocator.dupe(u8, std.mem.span(expr_c));
    if (rc.kind == c.LP_RESULT_COLUMN and rc.u.result_column.alias != null) {
        out.alias = try aliasText(allocator, std.mem.span(rc.u.result_column.alias));
    }
}

/// The output name an alias spells: the parser keeps a quoted one quoted (`"n m"`, `"a""b"`); the name is its text,
/// a doubled quote one quote.
fn aliasText(allocator: std.mem.Allocator, raw: []const u8) ![]const u8 {
    if (raw.len < 2 or raw[0] != '"' or raw[raw.len - 1] != '"') return allocator.dupe(u8, raw);
    return std.mem.replaceOwned(u8, allocator, raw[1 .. raw.len - 1], "\"\"", "\"");
}

/// `sql` with every `EXTRACT(part FROM x)` written `date_part('part', x)`, which is what it means (DuckDB's own
/// rewrite) and the call form liteparser's grammar has; quoted text is left as it is.
fn extractAsDatePart(allocator: std.mem.Allocator, sql: []const u8) ![:0]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    const word = struct {
        fn isByte(ch: u8) bool {
            return std.ascii.isAlphanumeric(ch) or ch == '_';
        }
        fn end(src: []const u8, from: usize) usize {
            var j = from;
            while (j < src.len and isByte(src[j])) j += 1;
            return j;
        }
        fn skipSpace(src: []const u8, from: usize) usize {
            var j = from;
            while (j < src.len and std.ascii.isWhitespace(src[j])) j += 1;
            return j;
        }
    };
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            // A quoted span, its doubled quotes included, copies through whole.
            var j = i + 1;
            while (j < sql.len) : (j += 1) if (sql[j] == ch) {
                if (j + 1 < sql.len and sql[j + 1] == ch) j += 1 else break;
            };
            const stop = @min(j + 1, sql.len);
            try out.appendSlice(allocator, sql[i..stop]);
            i = stop;
            continue;
        }
        if (!word.isByte(ch)) {
            try out.append(allocator, ch);
            i += 1;
            continue;
        }
        const w_end = word.end(sql, i);
        if (i == 0 or !word.isByte(sql[i - 1])) if (std.ascii.eqlIgnoreCase(sql[i..w_end], "extract")) {
            const open = word.skipSpace(sql, w_end);
            if (open < sql.len and sql[open] == '(') {
                // The part is a word, or one quoted as a string or a name (`EXTRACT('year' FROM d)`).
                var p_start = word.skipSpace(sql, open + 1);
                const quote: ?u8 = if (p_start < sql.len and (sql[p_start] == '\'' or sql[p_start] == '"'))
                    sql[p_start]
                else
                    null;
                if (quote != null) p_start += 1;
                var p_end = word.end(sql, p_start);
                if (quote) |q| p_end = if (p_end < sql.len and sql[p_end] == q) p_end + 1 else p_start;
                const from = word.skipSpace(sql, p_end);
                const from_end = word.end(sql, from);
                const part = sql[p_start .. p_end - @intFromBool(quote != null and p_end > p_start)];
                if (part.len > 0 and from > p_end and std.ascii.eqlIgnoreCase(sql[from..from_end], "from") and
                    from_end < sql.len and !word.isByte(sql[from_end]))
                {
                    try out.print(allocator, "date_part('{s}', ", .{part});
                    i = word.skipSpace(sql, from_end);
                    continue;
                }
            }
        };
        try out.appendSlice(allocator, sql[i..w_end]);
        i = w_end;
    }
    return out.toOwnedSliceSentinel(allocator, 0);
}

pub fn parseSqlQuery(allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);

    var error_msg: [*c]const u8 = null;
    const sql_c = try extractAsDatePart(allocator, sql);
    defer allocator.free(sql_c);

    // lp_parse() silently stops at the first ';', so trailing statements/clauses
    // would vanish unnoticed. lp_parse_all() returns every statement → we reject
    // anything but exactly one.
    const stmts = c.lp_parse_all(sql_c, lp_arena, &error_msg);
    if (stmts == null or stmts.*.count == 0) {
        if (error_msg) |msg| diagnosticPrint("sql parse error: {s}\n", .{std.mem.span(msg)});
        return error.SqlParseError;
    }
    if (stmts.*.count > 1) {
        diagnosticPrint("sql: only a single statement is supported (found {d})\n", .{stmts.*.count});
        return error.MultipleStatements;
    }
    const root = stmts.*.items[0] orelse return error.SqlParseError;

    // Only single-table SELECT. UNION/INSERT/etc. arrive as a non-SELECT
    // root; joins and subqueries arrive as a non-LP_FROM_TABLE `from`.
    if (root.*.kind != c.LP_STMT_SELECT) {
        diagnosticPrint("sql: only SELECT is supported (no UNION/INSERT/DDL)\n", .{});
        return error.UnsupportedStatement;
    }
    // Reject clauses we'd otherwise silently drop → wrong answers.
    try rejectUnsupportedClauses(root.*.u.select);
    boolLiterals(root);
    quoteWholePaths(root);
    try pushNotsDown(root, lp_arena);
    try rejectColumnComparisons(root);
    if (root.*.u.select.where) |w| {
        const wi = analyzeExpr(w);
        if (wi.has_window) {
            diagnosticPrint("sql: window functions are not supported yet\n", .{});
            return error.WindowNotSupported;
        }
        if (wi.has_subquery) {
            diagnosticPrint("sql: subqueries are not supported yet\n", .{});
            return error.SubqueryNotSupported;
        }
        if (wi.has_func_distinct) {
            diagnosticPrint("sql: DISTINCT inside a function is not supported yet\n", .{});
            return error.DistinctNotSupported;
        }
        if (wi.has_agg) {
            diagnosticPrint("sql: aggregates are not allowed in WHERE\n", .{});
            return error.AggregateInWhere;
        }
    }

    // Extract table name from FROM clause
    const from_node = root.*.u.select.from orelse return error.MissingFromClause;
    if (from_node.*.kind != c.LP_FROM_TABLE) {
        return error.UnsupportedFromClause;
    }
    if (from_node.*.u.from_table.name == null) {
        return error.MissingTableName;
    }
    const table_name_raw = std.mem.span(from_node.*.u.from_table.name);
    const table_name = try allocator.dupe(u8, stripQuotes(table_name_raw));
    errdefer allocator.free(table_name);

    // Classify the result columns. An aggregate query routes to
    // `--aggregate`; a plain one to `--select`. Mixing the two (e.g.
    // `SELECT flag, sum(id)`) is implicit GROUP BY — unsupported, and
    // rejected here instead of misrouting into the agg parser. Inline
    // window functions are rejected too.
    var has_agg = false;
    var has_bare = false;
    var i: usize = 0;
    while (i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
        const rc = root.*.u.select.result_columns.items[i] orelse continue;
        const info = analyzeExpr(rc);
        if (info.has_window) {
            diagnosticPrint("sql: window functions (OVER) are not supported yet\n", .{});
            return error.WindowNotSupported;
        }
        if (info.has_subquery) {
            diagnosticPrint("sql: subqueries are not supported yet\n", .{});
            return error.SubqueryNotSupported;
        }
        if (info.has_func_distinct) {
            diagnosticPrint("sql: DISTINCT inside a function is not supported yet\n", .{});
            return error.DistinctNotSupported;
        }
        if (info.has_agg) has_agg = true;
        if (info.has_bare_col) has_bare = true;
    }
    const has_group_by = root.*.u.select.group_by.count > 0;
    const is_aggregate = has_agg or has_group_by;

    if (is_aggregate) {
        if (has_bare and !has_group_by) {
            diagnosticPrint("sql: mixing aggregates with plain columns requires GROUP BY\n", .{});
            return error.MixedSelectNotSupported;
        }
        if (has_group_by) {
            try substituteGroupRefs(root.*.u.select);
            try validateGroupBy(allocator, root.*.u.select, lp_arena);
        }
    }

    // Unparse result columns
    var columns_list: std.ArrayList([]const u8) = .empty;
    defer {
        for (columns_list.items) |col| allocator.free(col);
        columns_list.deinit(allocator);
    }
    i = 0;
    while (i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
        const rc = root.*.u.select.result_columns.items[i] orelse continue;
        const col_sql = try unparseResultColumn(rc, lp_arena, allocator);
        try columns_list.append(allocator, col_sql);
    }

    const select_cols = try allocator.alloc(ast.SelectColumn, columns_list.items.len);
    @memset(select_cols, .{ .expr = "" });
    errdefer freeSelectCols(allocator, select_cols);
    i = 0;
    var col_idx: usize = 0;
    while (i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
        const rc = root.*.u.select.result_columns.items[i] orelse continue;
        try selectColumn(rc, lp_arena, allocator, &select_cols[col_idx]);
        if (has_group_by) select_cols[col_idx].role = if (analyzeExpr(rc).has_agg) .agg else .key;
        col_idx += 1;
    }
    var hidden: std.ArrayList(ast.SelectColumn) = .empty;
    defer {
        freeSelectCols(allocator, hidden.items);
        hidden.deinit(allocator);
    }
    const sel = root.*.u.select;
    var result_order = try resultOrder(allocator, sel, lp_arena, select_cols, is_aggregate, has_group_by, &hidden);
    errdefer allocator.free(result_order.keys);
    var having_arena: ?*std.heap.ArenaAllocator = null;
    errdefer if (having_arena) |ha| {
        ha.deinit();
        allocator.destroy(ha);
    };
    if (sel.having) |having| {
        if (!is_aggregate) {
            diagnosticPrint("sql: HAVING needs an aggregate or GROUP BY\n", .{});
            return error.HavingNotSupported;
        }
        const ha = try allocator.create(std.heap.ArenaAllocator);
        ha.* = .init(allocator);
        having_arena = ha;
        var ctx: HavingBuilder(@TypeOf(sel)) = .{
            .allocator = allocator,
            .arena = ha.allocator(),
            .sel = sel,
            .lp_arena = lp_arena,
            .select_cols = select_cols,
            .has_group_by = has_group_by,
            .hidden = &hidden,
        };
        const tree = try ha.allocator().create(ast.Having);
        tree.* = try ctx.build(having, 0);
        result_order.having = tree;
    }
    result_order.hidden = try hidden.toOwnedSlice(allocator);
    errdefer freeSelectCols(allocator, result_order.hidden);

    // Join result columns with ", "
    var joined_cols: std.ArrayList(u8) = .empty;
    errdefer joined_cols.deinit(allocator);
    for (columns_list.items, 0..) |col, idx| {
        if (idx > 0) try joined_cols.appendSlice(allocator, ", ");
        try joined_cols.appendSlice(allocator, col);
    }
    const columns_str = try joined_cols.toOwnedSlice(allocator);
    errdefer allocator.free(columns_str);

    // Extract WHERE filter clause if present
    var filter_str: ?[]const u8 = null;
    if (root.*.u.select.where) |where_node| {
        // A present WHERE must round-trip — never proceed filter-less (that
        // would scan everything and return confidently wrong data).
        const where_c = c.lp_ast_to_sql(where_node, lp_arena);
        if (where_c == null) return error.UnparseError;
        filter_str = try allocator.dupe(u8, std.mem.span(where_c));
    }
    errdefer if (filter_str) |fs| allocator.free(fs);

    // Extract GROUP BY clause if present
    var group_by_str: ?[]const u8 = null;
    if (has_group_by) {
        var group_by_list: std.ArrayList([]const u8) = .empty;
        defer {
            for (group_by_list.items) |col| allocator.free(col);
            group_by_list.deinit(allocator);
        }
        var j: usize = 0;
        while (j < @as(usize, @intCast(root.*.u.select.group_by.count))) : (j += 1) {
            const gb_node = root.*.u.select.group_by.items[j] orelse continue;
            const gb_c = c.lp_ast_to_sql(gb_node, lp_arena);
            if (gb_c == null) return error.UnparseError;
            const gb_sql = try allocator.dupe(u8, std.mem.span(gb_c));
            try group_by_list.append(allocator, gb_sql);
        }

        var joined_gb: std.ArrayList(u8) = .empty;
        errdefer joined_gb.deinit(allocator);
        for (group_by_list.items, 0..) |col, idx| {
            if (idx > 0) try joined_gb.appendSlice(allocator, ", ");
            try joined_gb.appendSlice(allocator, col);
        }
        group_by_str = try joined_gb.toOwnedSlice(allocator);
    }
    errdefer if (group_by_str) |gs| allocator.free(gs);

    var aggregate_str: []const u8 = columns_str;
    if (is_aggregate and has_group_by) {
        var agg_list: std.ArrayList([]const u8) = .empty;
        defer {
            for (agg_list.items) |col| allocator.free(col);
            agg_list.deinit(allocator);
        }
        i = 0;
        while (i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
            const rc = root.*.u.select.result_columns.items[i] orelse continue;
            const info = analyzeExpr(rc);
            if (!info.has_agg) continue;
            const col_sql = try unparseResultColumn(rc, lp_arena, allocator);
            try agg_list.append(allocator, col_sql);
        }
        // Hidden ORDER BY aggregates follow, in order, so they are the calls after the SELECT list's.
        for (result_order.hidden, 0..) |h, n| if (h.role == .agg) {
            try agg_list.ensureUnusedCapacity(allocator, 1);
            agg_list.appendAssumeCapacity(try std.fmt.allocPrint(allocator, "{s} AS {s}{d}", .{ h.expr, hidden_alias, n }));
        };

        allocator.free(columns_str);
        if (agg_list.items.len == 0) {
            aggregate_str = try allocator.dupe(u8, "");
        } else {
            var joined_agg: std.ArrayList(u8) = .empty;
            errdefer joined_agg.deinit(allocator);
            for (agg_list.items, 0..) |col, idx| {
                if (idx > 0) try joined_agg.appendSlice(allocator, ", ");
                try joined_agg.appendSlice(allocator, col);
            }
            aggregate_str = try joined_agg.toOwnedSlice(allocator);
        }
    } else if (is_aggregate) {
        // Without GROUP BY every SELECT item is an aggregate; hidden HAVING aggregates follow them.
        var joined: std.ArrayList(u8) = .empty;
        errdefer joined.deinit(allocator);
        try joined.appendSlice(allocator, columns_str);
        for (result_order.hidden, 0..) |h, n| if (h.role == .agg) {
            try joined.print(allocator, ", {s} AS {s}{d}", .{ h.expr, hidden_alias, n });
        };
        if (joined.items.len != columns_str.len) {
            aggregate_str = try joined.toOwnedSlice(allocator);
            allocator.free(columns_str);
        } else joined.deinit(allocator);
    }

    if (is_aggregate) {
        return .{
            .table_name = table_name,
            .is_aggregate = true,
            .select_str = null,
            .aggregate_str = aggregate_str,
            .filter_str = filter_str,
            .group_by_str = group_by_str,
            .select_cols = select_cols,
            .result_order = result_order,
            .having_arena = having_arena,
        };
    }
    // `SELECT *` → no projection list → the engine's zero-copy passthrough
    // (byte-copy fast path), which is exactly zpq's surgical sweet spot.
    // A bare star can't be an aggregate, so this only reaches the plain path.
    if (std.mem.eql(u8, std.mem.trim(u8, columns_str, " "), "*")) {
        allocator.free(columns_str);
        freeSelectCols(allocator, select_cols);
        return .{
            .table_name = table_name,
            .is_aggregate = false,
            .select_str = null,
            .aggregate_str = null,
            .filter_str = filter_str,
            .result_order = result_order,
            .having_arena = having_arena,
        };
    }
    return .{
        .table_name = table_name,
        .is_aggregate = false,
        .select_str = columns_str,
        .aggregate_str = null,
        .filter_str = filter_str,
        .select_cols = select_cols,
        .result_order = result_order,
        .having_arena = having_arena,
    };
}

/// The alias a hidden ORDER BY aggregate's call carries (followed by its position among the hidden terms): never
/// printed, as its result column is dropped once the rows are ranked.
pub const hidden_alias = "__zpq_order_";

/// ORDER BY / LIMIT / OFFSET as positions in the SELECT list. Only an aggregate's result rows are ordered: plain rows
/// stream out as they are read, and an aggregate without GROUP BY is one row, which a LIMIT may keep but not drop.
/// Under GROUP BY a term no SELECT column gives becomes a hidden result column (`ResultOrder.hidden`): an aggregate,
/// computed as a call of its own, or an expression of the GROUP BY keys.
fn resultOrder(
    allocator: std.mem.Allocator,
    sel: anytype,
    lp_arena: *c.arena_t,
    select_cols: []const ast.SelectColumn,
    is_aggregate: bool,
    has_group_by: bool,
    hidden: *std.ArrayList(ast.SelectColumn),
) !ast.ResultOrder {
    const n_terms: usize = @intCast(sel.order_by.count);
    var order: ast.ResultOrder = .{};
    if (sel.limit) |lim| {
        order.limit = try nonNegativeInt(lim.*.u.limit.count orelse return error.LimitNotSupported);
        if (lim.*.u.limit.offset) |off| order.offset = try nonNegativeInt(off);
    }
    if (!is_aggregate) {
        if (n_terms > 0) order.rows = try rowOrder(allocator, sel, lp_arena, select_cols);
        return order;
    }

    const keys = try allocator.alloc(ast.OrderKey, n_terms);
    errdefer allocator.free(keys);
    for (keys, 0..) |*key, t| {
        const term = sel.order_by.items[t] orelse return error.SqlParseError;
        // The grammar spells NULLS FIRST as LP_SORT_ASC, NULLS LAST as LP_SORT_DESC, neither as LP_SORT_UNDEFINED.
        if (term.*.u.order_term.nulls == c.LP_SORT_ASC) {
            diagnosticPrint("sql: NULLS FIRST is not supported yet (NULLs sort last)\n", .{});
            return error.OrderByNotSupported;
        }
        const expr = term.*.u.order_term.expr orelse return error.SqlParseError;
        const column = try orderColumn(sel, lp_arena, select_cols, expr) orelse
            select_cols.len + try hiddenTerm(allocator, sel, lp_arena, expr, has_group_by, .order_by, hidden);
        key.* = .{ .column = column, .descending = term.*.u.order_term.direction == c.LP_SORT_DESC };
    }
    order.keys = keys;
    return order;
}

const Clause = enum { order_by, having };

/// ORDER BY over plain rows: each term an expression of the input row (a column, arithmetic, a function, CASE),
/// written directly, through a SELECT alias or by position. No aggregate: there are no groups to rank.
fn rowOrder(
    allocator: std.mem.Allocator,
    sel: anytype,
    lp_arena: *c.arena_t,
    select_cols: []const ast.SelectColumn,
) ![]const ast.RowKey {
    const n_terms: usize = @intCast(sel.order_by.count);
    const keys = try allocator.alloc(ast.RowKey, n_terms);
    var made: usize = 0;
    errdefer {
        for (keys[0..made]) |k| allocator.free(k.expr);
        allocator.free(keys);
    }
    for (keys) |*key| {
        const term = sel.order_by.items[made] orelse return error.SqlParseError;
        if (term.*.u.order_term.nulls == c.LP_SORT_ASC) {
            diagnosticPrint("sql: NULLS FIRST is not supported yet (NULLs sort last)\n", .{});
            return error.OrderByNotSupported;
        }
        var expr: *c.LpNode = term.*.u.order_term.expr orelse return error.SqlParseError;
        // A position or a SELECT alias stands for that item's expression.
        if (expr.kind == c.LP_EXPR_LITERAL_INT) {
            const pos = try nonNegativeInt(expr);
            if (pos < 1 or pos > select_cols.len) {
                diagnosticPrint("sql: ORDER BY position {d} is not in the SELECT list\n", .{pos});
                return error.OrderByNotSupported;
            }
            expr = selectExpr(sel, @intCast(pos - 1)) orelse return error.SqlParseError;
        } else if (expr.kind == c.LP_EXPR_COLUMN_REF and expr.u.column_ref.table == null) {
            const name = std.mem.span(expr.u.column_ref.column);
            for (select_cols, 0..) |col, idx| if (col.alias) |alias| if (std.ascii.eqlIgnoreCase(alias, name)) {
                expr = selectExpr(sel, idx) orelse return error.SqlParseError;
                break;
            };
        }
        const info = analyzeExpr(expr);
        if (info.has_agg or info.has_window or info.has_subquery) {
            const text = c.lp_ast_to_sql(expr, lp_arena);
            diagnosticPrint("sql: ORDER BY {s} over plain rows is not supported\n", .{std.mem.span(text)});
            return error.OrderByNotSupported;
        }
        const text_c = c.lp_ast_to_sql(expr, lp_arena);
        if (text_c == null) return error.UnparseError;
        key.* = .{
            .expr = try allocator.dupe(u8, std.mem.span(text_c)),
            .descending = term.*.u.order_term.direction == c.LP_SORT_DESC,
        };
        made += 1;
    }
    return keys;
}

/// The expression of SELECT item `i`, `*` excepted.
fn selectExpr(sel: anytype, i: usize) ?*c.LpNode {
    if (i >= @as(usize, @intCast(sel.result_columns.count))) return null;
    const rc = sel.result_columns.items[i] orelse return null;
    return if (rc.*.kind == c.LP_RESULT_COLUMN) rc.*.u.result_column.expr else rc;
}

/// Add ORDER BY or HAVING term `expr`, which names no SELECT column, to `hidden` (once: an equal term already there is
/// reused); its index there. Under GROUP BY such a term is an aggregate or an expression whose columns are all
/// grouped; without GROUP BY, HAVING may read an aggregate the SELECT list lacks, and ORDER BY nothing.
fn hiddenTerm(
    allocator: std.mem.Allocator,
    sel: anytype,
    lp_arena: *c.arena_t,
    expr: *c.LpNode,
    has_group_by: bool,
    clause: Clause,
    hidden: *std.ArrayList(ast.SelectColumn),
) !usize {
    const text_c = c.lp_ast_to_sql(expr, lp_arena);
    if (text_c == null) return error.UnparseError;
    const text = std.mem.span(text_c);
    const name = if (clause == .order_by) "ORDER BY" else "HAVING";
    const info = analyzeExpr(expr);
    if (!has_group_by and (clause == .order_by or !info.has_agg)) {
        diagnosticPrint("sql: {s} {s} must name a result column (by position, alias or expression)\n", .{ name, text });
        return if (clause == .order_by) error.OrderByNotSupported else error.ColumnMustBeGrouped;
    }
    if (info.has_window or info.has_subquery or info.has_func_distinct) {
        diagnosticPrint("sql: {s} {s} is not supported yet\n", .{ name, text });
        return if (clause == .order_by) error.OrderByNotSupported else error.HavingNotSupported;
    }
    if (!info.has_agg and info.has_bare_col and !try groupedExpr(allocator, sel, lp_arena, expr)) {
        diagnosticPrint("sql: {s} {s} must be in the GROUP BY list or aggregated\n", .{ name, text });
        return error.ColumnMustBeGrouped;
    }
    // Written alike, one slot: `ORDER BY count(*) DESC, count(*)` computes one aggregate.
    for (hidden.items, 0..) |h, i| if (std.mem.eql(u8, h.expr, text)) return i;
    try hidden.ensureUnusedCapacity(allocator, 1);
    hidden.appendAssumeCapacity(.{ .expr = try allocator.dupe(u8, text), .role = if (info.has_agg) .agg else .key });
    return hidden.items.len - 1;
}

/// Lowers a HAVING clause, its NOTs already at the leaves (`pushNotsDown`), to an `ast.Having` over result row
/// columns: each leaf compares one result (a SELECT column by alias or expression, else a hidden term) with a literal.
/// Chains build in a loop; only grouping recurses, bounded as the filter grammar bounds it.
fn HavingBuilder(comptime Sel: type) type {
    return struct {
        allocator: std.mem.Allocator,
        arena: std.mem.Allocator,
        sel: Sel,
        lp_arena: *c.arena_t,
        select_cols: []const ast.SelectColumn,
        has_group_by: bool,
        hidden: *std.ArrayList(ast.SelectColumn),

        const Self = @This();
        const max_depth = @import("../filter/parser.zig").MAX_GROUP_DEPTH;

        fn unsupported(self: *Self, node: *c.LpNode) anyerror {
            const text = c.lp_ast_to_sql(node, self.lp_arena);
            if (text == null) return error.UnparseError;
            diagnosticPrint("sql: HAVING {s} is not supported yet (compare an aggregate or key with a literal)\n", .{
                std.mem.span(text),
            });
            return error.HavingNotSupported;
        }

        fn build(self: *Self, node: *c.LpNode, depth: u32) anyerror!ast.Having {
            if (depth > max_depth) return error.FilterTooDeep;
            switch (node.kind) {
                c.LP_EXPR_BINARY_OP => {
                    const op = node.u.binary.op;
                    if (op == c.LP_OP_AND or op == c.LP_OP_OR) return self.chain(node, op, depth);
                    const left = node.u.binary.left orelse return self.unsupported(node);
                    const right = node.u.binary.right orelse return self.unsupported(node);
                    if (op == c.LP_OP_IS or op == c.LP_OP_ISNOT) {
                        if (right.*.kind != c.LP_EXPR_LITERAL_NULL) return self.unsupported(node);
                        return .{ .null_check = .{ .column = try self.term(left), .is_not = op == c.LP_OP_ISNOT } };
                    }
                    const cmp: ast.CmpOp = switch (op) {
                        c.LP_OP_EQ => .eq,
                        c.LP_OP_NE => .ne,
                        c.LP_OP_LT => .lt,
                        c.LP_OP_LE => .le,
                        c.LP_OP_GT => .gt,
                        c.LP_OP_GE => .ge,
                        else => return self.unsupported(node),
                    };
                    if (try self.literal(right)) |lit| {
                        if (try self.literal(left) != null) return self.unsupported(node);
                        return .{ .cmp = .{ .column = try self.term(left), .op = cmp, .literal = lit } };
                    }
                    const lit = try self.literal(left) orelse return self.unsupported(node);
                    return .{ .cmp = .{ .column = try self.term(right), .op = cmp.flip(), .literal = lit } };
                },
                c.LP_EXPR_UNARY_OP => {
                    if (node.u.unary.op != c.LP_UOP_NOT) return self.unsupported(node);
                    const inner = try self.build(node.u.unary.operand orelse return self.unsupported(node), depth + 1);
                    return inner.negated(self.arena);
                },
                c.LP_EXPR_BETWEEN => {
                    const b = node.u.between;
                    const column = try self.term(b.expr orelse return self.unsupported(node));
                    const lo = try self.literal(b.low orelse return self.unsupported(node)) orelse return self.unsupported(node);
                    const hi = try self.literal(b.high orelse return self.unsupported(node)) orelse return self.unsupported(node);
                    const terms = try self.arena.alloc(ast.Having, 2);
                    terms[0] = .{ .cmp = .{ .column = column, .op = .ge, .literal = lo } };
                    terms[1] = .{ .cmp = .{ .column = column, .op = .le, .literal = hi } };
                    const range: ast.Having = .{ .all = terms };
                    return if (b.is_not != 0) range.negated(self.arena) else range;
                },
                c.LP_EXPR_IN => {
                    const in = node.u.in;
                    if (in.select != null or in.values.count == 0) return self.unsupported(node);
                    const column = try self.term(in.expr orelse return self.unsupported(node));
                    const terms = try self.arena.alloc(ast.Having, @intCast(in.values.count));
                    for (terms, 0..) |*t, i| {
                        const v = in.values.items[i] orelse return self.unsupported(node);
                        const lit = try self.literal(v) orelse return self.unsupported(node);
                        t.* = .{ .cmp = .{ .column = column, .op = .eq, .literal = lit } };
                    }
                    const list: ast.Having = .{ .any = terms };
                    return if (in.is_not != 0) list.negated(self.arena) else list;
                },
                else => return self.unsupported(node),
            }
        }

        /// A chain of one operator, its operands gathered with a stack rather than recursion: left-deep chains of any
        /// length cost no parse depth.
        fn chain(self: *Self, node: *c.LpNode, op: anytype, depth: u32) anyerror!ast.Having {
            var stack: std.ArrayList(*c.LpNode) = .empty;
            defer stack.deinit(self.allocator);
            var operands: std.ArrayList(ast.Having) = .empty;
            try stack.append(self.allocator, node);
            while (stack.pop()) |n| {
                if (n.kind == c.LP_EXPR_BINARY_OP and n.u.binary.op == op) {
                    try stack.append(self.allocator, n.u.binary.right orelse return self.unsupported(n));
                    try stack.append(self.allocator, n.u.binary.left orelse return self.unsupported(n));
                    continue;
                }
                try operands.append(self.arena, try self.build(n, depth + 1));
            }
            return if (op == c.LP_OP_AND) .{ .all = operands.items } else .{ .any = operands.items };
        }

        /// `node` as a HAVING literal, or null when it is none: a number (a minus sign folded in), a string or NULL.
        fn literal(self: *Self, node: *c.LpNode) !?ast.HavingLiteral {
            switch (node.kind) {
                c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => {
                    return .{ .number = try self.arena.dupe(u8, std.mem.span(node.u.literal.value)) };
                },
                c.LP_EXPR_LITERAL_STRING => return .{ .string = try self.arena.dupe(u8, std.mem.span(node.u.literal.value)) },
                c.LP_EXPR_LITERAL_NULL => return .null,
                c.LP_EXPR_UNARY_OP => {
                    if (node.u.unary.op != c.LP_UOP_MINUS) return null;
                    const operand = node.u.unary.operand orelse return null;
                    if (operand.*.kind != c.LP_EXPR_LITERAL_INT and operand.*.kind != c.LP_EXPR_LITERAL_FLOAT) return null;
                    const text = std.mem.span(operand.*.u.literal.value);
                    return .{ .number = try std.fmt.allocPrint(self.arena, "-{s}", .{text}) };
                },
                else => return null,
            }
        }

        /// The result row column a HAVING operand reads: a SELECT column it names, else a hidden term.
        fn term(self: *Self, node: *c.LpNode) !usize {
            // The tested operand names a result; an integer here is a value, never an ORDER BY-style position
            // (`HAVING 1 IN (2, 3)` kept the groups whose first column was 2 or 3).
            switch (node.kind) {
                c.LP_EXPR_LITERAL_INT,
                c.LP_EXPR_LITERAL_FLOAT,
                c.LP_EXPR_LITERAL_STRING,
                c.LP_EXPR_LITERAL_NULL,
                c.LP_EXPR_LITERAL_BOOL,
                c.LP_EXPR_LITERAL_BLOB,
                => {
                    diagnosticPrint("sql: HAVING tests a literal; it compares a result or key with literals\n", .{});
                    return error.HavingNotSupported;
                },
                else => {},
            }
            if (try orderColumn(self.sel, self.lp_arena, self.select_cols, node)) |i| return i;
            return self.select_cols.len +
                try hiddenTerm(self.allocator, self.sel, self.lp_arena, node, self.has_group_by, .having, self.hidden);
        }
    };
}

/// The SELECT-list position an ORDER BY term names: a 1-based position, an alias, or an expression written as in the
/// SELECT list (a column reference matching as GROUP BY matches it). An alias wins over a column of the same name.
/// Null for an expression no SELECT column gives.
fn orderColumn(sel: anytype, lp_arena: *c.arena_t, select_cols: []const ast.SelectColumn, expr: *c.LpNode) !?usize {
    if (expr.kind == c.LP_EXPR_LITERAL_INT) {
        const pos = try nonNegativeInt(expr);
        if (pos >= 1 and pos <= select_cols.len) return @intCast(pos - 1);
        diagnosticPrint("sql: ORDER BY position {d} is not in the SELECT list\n", .{pos});
        return error.OrderByNotSupported;
    }
    if (expr.kind == c.LP_EXPR_COLUMN_REF and expr.u.column_ref.schema == null and expr.u.column_ref.table == null) {
        const name = std.mem.span(expr.u.column_ref.column);
        for (select_cols, 0..) |col, idx| {
            if (col.alias) |alias| if (std.ascii.eqlIgnoreCase(alias, name)) return idx;
        }
    }
    const expr_c = c.lp_ast_to_sql(expr, lp_arena);
    if (expr_c == null) return error.UnparseError;
    const text = std.mem.span(expr_c);
    var idx: usize = 0;
    for (0..@intCast(sel.result_columns.count)) |i| {
        const rc = sel.result_columns.items[i] orelse continue;
        const rc_expr = if (rc.*.kind == c.LP_RESULT_COLUMN) rc.*.u.result_column.expr else rc;
        if (sameColumnRef(expr, rc_expr) orelse std.mem.eql(u8, text, select_cols[idx].expr)) return idx;
        idx += 1;
    }
    // Spelled with other letter case (`count(*)` for `COUNT(*)`): the one item that matches so, if only one does.
    var found: ?usize = null;
    for (select_cols, 0..) |col, i| if (std.ascii.eqlIgnoreCase(text, col.expr)) {
        if (found != null) {
            found = null;
            break;
        }
        found = i;
    };
    return found;
}

/// A LIMIT, OFFSET or ORDER BY position, which must be a plain non-negative integer literal.
fn nonNegativeInt(node: *const c.LpNode) !u64 {
    if (node.kind == c.LP_EXPR_LITERAL_INT) {
        if (std.fmt.parseInt(u64, std.mem.span(node.u.literal.value), 10)) |v| return v else |_| {}
    }
    diagnosticPrint("sql: LIMIT, OFFSET and ORDER BY positions must be non-negative integer literals\n", .{});
    return error.LimitNotSupported;
}

/// Whether two column references name the same segments, ignoring case and quoting (`"r"."v"` is `r.v`): which
/// column each binds is the engine's to decide, as the text both unparsed to once was. Null unless both are column
/// references.
fn sameColumnRef(a: *const c.LpNode, other: *const c.LpNode) ?bool {
    if (a.kind != c.LP_EXPR_COLUMN_REF or other.kind != c.LP_EXPR_COLUMN_REF) return null;
    const x = &a.u.column_ref;
    const y = &other.u.column_ref;
    return partEql(x.schema, y.schema) and partEql(x.table, y.table) and partEql(x.column, y.column);
}

fn partEql(a: [*c]const u8, b: [*c]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.ascii.eqlIgnoreCase(std.mem.span(a), std.mem.span(b));
}

/// Rewrite each GROUP BY integer constant into the SELECT item at that 1-based position, as DuckDB binds `GROUP BY 1`.
/// A name a SELECT alias gives binds in the engine, which knows whether a column of the name wins
/// (`expr_parser.parseGroupByOf`).
fn substituteGroupRefs(sel: anytype) !void {
    const n_cols: usize = @intCast(sel.result_columns.count);
    for (0..@intCast(sel.group_by.count)) |g| {
        const term = sel.group_by.items[g] orelse continue;
        if (term.*.kind != c.LP_EXPR_LITERAL_INT) continue;
        const pos = std.fmt.parseInt(u64, std.mem.span(term.*.u.literal.value), 10) catch 0;
        if (pos < 1 or pos > n_cols) {
            diagnosticPrint("sql: GROUP BY position {s} is not in the SELECT list\n", .{std.mem.span(term.*.u.literal.value)});
            return error.ColumnMustBeGrouped;
        }
        const rc = sel.result_columns.items[@intCast(pos - 1)] orelse return error.SqlParseError;
        const expr = if (rc.*.kind == c.LP_RESULT_COLUMN) rc.*.u.result_column.expr orelse return error.SqlParseError else rc;
        if (analyzeExpr(expr).has_agg) {
            diagnosticPrint("sql: GROUP BY cannot refer to an aggregate\n", .{});
            return error.AggregateInGroupBy;
        }
        sel.group_by.items[g] = expr;
    }
}

/// Whether `expr` is one of the GROUP BY terms: the same column reference (case and quoting aside, as the engine
/// binds names) or the same expression text.
fn groupedExpr(allocator: std.mem.Allocator, sel: anytype, lp_arena: *c.arena_t, expr: *c.LpNode) !bool {
    _ = allocator;
    const expr_c = c.lp_ast_to_sql(expr, lp_arena);
    if (expr_c == null) return error.UnparseError;
    const a = stripQuotes(std.mem.trim(u8, std.mem.span(expr_c), " "));
    for (0..@intCast(sel.group_by.count)) |g| {
        const gb_node = sel.group_by.items[g] orelse continue;
        if (try sameTerm(lp_arena, expr, a, gb_node)) return true;
        // A term naming a SELECT alias may stand for that item's expression (the engine decides; see
        // `substituteGroupRefs`).
        if (gb_node.*.kind != c.LP_EXPR_COLUMN_REF or gb_node.*.u.column_ref.table != null) continue;
        const name = std.mem.span(gb_node.*.u.column_ref.column);
        for (0..@intCast(sel.result_columns.count)) |i| {
            const rc = sel.result_columns.items[i] orelse continue;
            if (rc.*.kind != c.LP_RESULT_COLUMN or rc.*.u.result_column.alias == null) continue;
            if (!std.ascii.eqlIgnoreCase(std.mem.span(rc.*.u.result_column.alias), name)) continue;
            if (try sameTerm(lp_arena, expr, a, rc.*.u.result_column.expr orelse continue)) return true;
        }
    }
    return false;
}

/// Whether `expr` (unparsed as `text`, quotes stripped) is the term `other`: the same column reference, case and
/// quoting aside, or the same expression text.
fn sameTerm(lp_arena: *c.arena_t, expr: *c.LpNode, text: []const u8, other: *c.LpNode) !bool {
    if (sameColumnRef(expr, other)) |same| return same;
    const other_c = c.lp_ast_to_sql(other, lp_arena);
    if (other_c == null) return error.UnparseError;
    return std.ascii.eqlIgnoreCase(text, stripQuotes(std.mem.trim(u8, std.mem.span(other_c), " ")));
}

/// Reject a SELECT item that reads a column outside an aggregate unless it is a GROUP BY term. The engine binds the
/// items again, structurally against the bound keys; this catches what plainly cannot bind before any file opens.
fn validateGroupBy(allocator: std.mem.Allocator, sel: anytype, lp_arena: *c.arena_t) !void {
    for (0..@intCast(sel.result_columns.count)) |i| {
        const rc = sel.result_columns.items[i] orelse continue;
        if (!analyzeExpr(rc).has_bare_col) continue;
        const expr_node = rc.*.u.result_column.expr orelse continue;
        if (!try groupedExpr(allocator, sel, lp_arena, expr_node)) {
            const expr_c = c.lp_ast_to_sql(expr_node, lp_arena);
            diagnosticPrint("sql: result column '{s}' must be in the GROUP BY list\n", .{std.mem.span(expr_c)});
            return error.ColumnMustBeGrouped;
        }
    }
}

test "sql_parser basic select" {
    const allocator = std.testing.allocator;
    const q = try parseSqlQuery(allocator, "SELECT a, b + c AS sum_val FROM 'test.parquet' WHERE a > 10");
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("test.parquet", q.table_name);
    try std.testing.expect(!q.is_aggregate);
    try std.testing.expectEqualStrings("a, b + c AS sum_val", q.select_str.?);
    try std.testing.expectEqualStrings("a > 10", q.filter_str.?);
    try std.testing.expect(q.aggregate_str == null);
}

test "sql_parser rejects unsupported clauses instead of silently dropping them" {
    const a = std.testing.allocator;
    // Each of these previously parsed and SILENTLY ignored the clause →
    // a confidently wrong answer. They must error with a named clause now.
    // GROUP BY y: x is aggregated, y is not but y is in GROUP BY, so it's valid:
    const q_gb = try parseSqlQuery(a, "SELECT sum(x) AS s FROM 't' GROUP BY y");
    defer q_gb.deinit(a);
    try std.testing.expectEqualStrings("y", q_gb.group_by_str.?);
    try std.testing.expectEqualStrings("sum(x) AS s", q_gb.aggregate_str.?);

    const q_gb_mixed = try parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't' GROUP BY y");
    defer q_gb_mixed.deinit(a);
    try std.testing.expectEqualStrings("sum(x) AS s", q_gb_mixed.aggregate_str.?);
    try std.testing.expectEqual(@as(usize, 2), q_gb_mixed.select_cols.len);
    try std.testing.expectEqualStrings("y", q_gb_mixed.select_cols[0].expr);
    try std.testing.expect(q_gb_mixed.select_cols[0].alias == null);
    try std.testing.expectEqualStrings("sum(x)", q_gb_mixed.select_cols[1].expr);
    try std.testing.expectEqualStrings("s", q_gb_mixed.select_cols[1].alias.?);

    const sorted = try parseSqlQuery(a, "SELECT x FROM 't' ORDER BY x");
    defer sorted.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), sorted.result_order.rows.len);
    const limited = try parseSqlQuery(a, "SELECT x FROM 't' LIMIT 5");
    defer limited.deinit(a);
    try std.testing.expectEqual(@as(?u64, 5), limited.result_order.limit);
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT x FROM 't'"));
    try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, "SELECT x FROM 't' HAVING x > 1"));
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't'"));
    try std.testing.expectError(error.UnsupportedStatement, parseSqlQuery(a, "SELECT x FROM 't' UNION SELECT x FROM 'u'"));
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't' GROUP BY z"));
}

test "sql_parser resolves ORDER BY / LIMIT / OFFSET against the result columns" {
    const a = std.testing.allocator;
    const Key = ast.OrderKey;
    const Case = struct { sql: []const u8, keys: []const Key, limit: ?u64 = null, offset: u64 = 0 };
    const cases = [_]Case{
        // By alias, DESC; by position; by the expression as written; a column of the same name loses to the alias.
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY c DESC LIMIT 10", .keys = &.{.{ .column = 1, .descending = true }}, .limit = 10 },
        .{ .sql = "SELECT y, count(*) FROM 't' GROUP BY y ORDER BY 2 DESC, 1", .keys = &.{ .{ .column = 1, .descending = true }, .{ .column = 0, .descending = false } } },
        .{ .sql = "SELECT y, count(*) FROM 't' GROUP BY y ORDER BY count(*) DESC", .keys = &.{.{ .column = 1, .descending = true }} },
        // Other letter case still names the one item it can.
        .{ .sql = "SELECT y, COUNT(*) FROM 't' GROUP BY y ORDER BY count(*) DESC", .keys = &.{.{ .column = 1, .descending = true }} },
        .{ .sql = "SELECT Y, sum(x) AS y FROM 't' GROUP BY Y ORDER BY y", .keys = &.{.{ .column = 1, .descending = false }} },
        // A column reference matches as GROUP BY matches it: case and quoting aside.
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY \"Y\" ASC NULLS LAST", .keys = &.{.{ .column = 0, .descending = false }} },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY c LIMIT 10 OFFSET 1000", .keys = &.{.{ .column = 1, .descending = false }}, .limit = 10, .offset = 1000 },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y LIMIT 5", .keys = &.{}, .limit = 5 },
        // One aggregate row: ordering it is a no-op, and a LIMIT may keep it.
        .{ .sql = "SELECT count(*) AS c FROM 't' ORDER BY c LIMIT 1", .keys = &.{.{ .column = 0, .descending = false }}, .limit = 1 },
    };
    for (cases) |case| {
        const q = try parseSqlQuery(a, case.sql);
        defer q.deinit(a);
        try std.testing.expectEqualSlices(Key, case.keys, q.result_order.keys);
        try std.testing.expectEqual(case.limit, q.result_order.limit);
        try std.testing.expectEqual(case.offset, q.result_order.offset);
    }

    const rejected = [_]struct { sql: []const u8, err: anyerror }{
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY z", .err = error.ColumnMustBeGrouped },
        .{ .sql = "SELECT count(*) AS c FROM 't' ORDER BY sum(x)", .err = error.OrderByNotSupported },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY 3", .err = error.OrderByNotSupported },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY 0", .err = error.OrderByNotSupported },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY c NULLS FIRST", .err = error.OrderByNotSupported },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y LIMIT -1", .err = error.LimitNotSupported },
        .{ .sql = "SELECT y, count(*) AS c FROM 't' GROUP BY y LIMIT c", .err = error.LimitNotSupported },
    };
    for (rejected) |case| try std.testing.expectError(case.err, parseSqlQuery(a, case.sql));
}

test "regression: sql_parser refuses a comparison that reads a column on its value side" {
    const a = std.testing.allocator;
    const refused = [_][]const u8{
        "SELECT id FROM 't' WHERE s = t",
        "SELECT id FROM 't' WHERE s = x",
        "SELECT id FROM 't' WHERE s <> \"t\"",
        "SELECT id FROM 't' WHERE s = -t",
        "SELECT id FROM 't' WHERE s = upper(t)",
        "SELECT id FROM 't' WHERE s IN (x, 'z')",
        "SELECT id FROM 't' WHERE n BETWEEN 1 AND m",
        "SELECT id FROM 't' WHERE s LIKE t",
        "SELECT id FROM 't' WHERE NOT (s = t AND n > 1)",
        "SELECT count(*) FILTER (WHERE s <> t) AS n FROM 't'",
        "SELECT CASE WHEN s = t THEN 1 ELSE 0 END AS k FROM 't'",
    };
    for (refused) |sql| try std.testing.expectError(error.BadValue, parseSqlQuery(a, sql));
    const kept = [_][]const u8{
        "SELECT id FROM 't' WHERE s = 'x' AND n > -5 AND flag = true AND d IS NULL",
        "SELECT id FROM 't' WHERE s IN ('x', 'y') AND n BETWEEN 1 AND 2 AND s LIKE 'a%'",
    };
    for (kept) |sql| {
        var q = try parseSqlQuery(a, sql);
        q.deinit(a);
    }
}

test "regression: a HAVING literal operand is a value, not a SELECT position" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT g, count(*) AS c FROM 't' GROUP BY g HAVING 1 BETWEEN 2 AND 3",
        "SELECT g, count(*) AS c FROM 't' GROUP BY g HAVING 1 IN (2, 3)",
        "SELECT g, count(*) AS c FROM 't' GROUP BY g HAVING 9 IS NULL",
    }) |sql| try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, sql));
    var q = try parseSqlQuery(a, "SELECT g, count(*) AS c FROM 't' GROUP BY g HAVING c BETWEEN 2 AND 3");
    q.deinit(a);
}

test "sql_parser rejects unsupported aggregate shapes" {
    const a = std.testing.allocator;
    // sum(DISTINCT x) — distinct on non-count functions is not supported.
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT sum(DISTINCT x) AS c FROM 't'"));
    // Bare column inside an aggregate column: `sum(a) + b` is implicit GROUP BY.
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT sum(a) + b AS x FROM 't'"));
    // Aggregate in WHERE is illegal.
    try std.testing.expectError(error.AggregateInWhere, parseSqlQuery(a, "SELECT x FROM 't' WHERE sum(a) > 1"));
    // Trailing statement after ';' must not be silently dropped.
    try std.testing.expectError(error.MultipleStatements, parseSqlQuery(a, "SELECT x FROM 't'; SELECT y FROM 'u'"));
    // Subquery in WHERE.
    try std.testing.expectError(error.SubqueryNotSupported, parseSqlQuery(a, "SELECT x FROM 't' WHERE x IN (SELECT y FROM 'u')"));
}

test "sql_parser accepts count(DISTINCT x)" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT count(DISTINCT x) AS c FROM 't'");
    defer q.deinit(a);
    try std.testing.expect(q.is_aggregate);
    try std.testing.expectEqualStrings("count(DISTINCT x) AS c", q.aggregate_str.?);
}

test "sql_parser aggregate select" {
    const allocator = std.testing.allocator;
    const q = try parseSqlQuery(allocator, "SELECT sum(a) AS total, count(*) AS count_val FROM \"data.parquet\"");
    defer q.deinit(allocator);

    try std.testing.expectEqualStrings("data.parquet", q.table_name);
    try std.testing.expect(q.is_aggregate);
    try std.testing.expectEqualStrings("sum(a) AS total, count(*) AS count_val", q.aggregate_str.?);
    try std.testing.expect(q.select_str == null);
    try std.testing.expect(q.filter_str == null);
}

test "sql_parser hands identifiers on as the flag forms spell them" {
    // The flag parsers bind them: `a.b` is a top-level column so named when there is one, else the path; `"a"."b"`
    // is the path. Re-quoting only where SQL needs it turned `"a"."b"` into `a.b`.
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, select: []const u8, filter: []const u8, group_by: []const u8 };
    const cases = [_]Case{
        .{ .sql = "b", .select = "b", .filter = "b = 1", .group_by = "b" },
        .{ .sql = "a.b", .select = "a.b", .filter = "a.b = 1", .group_by = "a.b" },
        .{ .sql = "\"a.b\"", .select = "\"a.b\"", .filter = "\"a.b\" = 1", .group_by = "\"a.b\"" },
        .{ .sql = "\"a\".\"b\"", .select = "\"a\".\"b\"", .filter = "\"a\".\"b\" = 1", .group_by = "\"a\".\"b\"" },
        // A quoted segment quotes the whole path: the flag forms have no mixed spelling.
        .{ .sql = "a.\"b\"", .select = "\"a\".\"b\"", .filter = "\"a\".\"b\" = 1", .group_by = "\"a\".\"b\"" },
        .{ .sql = "`a`.[b]", .select = "\"a\".\"b\"", .filter = "\"a\".\"b\" = 1", .group_by = "\"a\".\"b\"" },
        .{
            .sql = "\"q\"\"x\".b",
            .select = "\"q\"\"x\".\"b\"",
            .filter = "\"q\"\"x\".\"b\" = 1",
            .group_by = "\"q\"\"x\".\"b\"",
        },
    };
    for (cases) |case| {
        const plain = try std.fmt.allocPrint(a, "SELECT {s} FROM 't' WHERE {s} = 1", .{ case.sql, case.sql });
        defer a.free(plain);
        const q = try parseSqlQuery(a, plain);
        defer q.deinit(a);
        try std.testing.expectEqualStrings(case.select, q.select_str.?);
        try std.testing.expectEqualStrings(case.filter, q.filter_str.?);

        const sql = "SELECT {s}, count(*) AS n FROM 't' GROUP BY {s}";
        const grouped = try std.fmt.allocPrint(a, sql, .{ case.sql, case.sql });
        defer a.free(grouped);
        const g = try parseSqlQuery(a, grouped);
        defer g.deinit(a);
        try std.testing.expectEqualStrings(case.group_by, g.group_by_str.?);
        try std.testing.expectEqualStrings(case.select, g.select_cols[0].expr);
    }
}

test "sql_parser: a grouped column may be spelled with or without quotes in SELECT and GROUP BY" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT \"r\".\"v\", count(*) AS n FROM 't' GROUP BY r.v",
        "SELECT r.v, count(*) AS n FROM 't' GROUP BY \"r\".\"v\"",
        "SELECT \"k\", count(*) AS n FROM 't' GROUP BY K",
    }) |sql| {
        const q = parseSqlQuery(a, sql) catch |err| {
            diagnosticPrint("{s}: {s}\n", .{ sql, @errorName(err) });
            return err;
        };
        q.deinit(a);
    }
    const other = "SELECT \"r\".\"w\", count(*) FROM 't' GROUP BY r.v";
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, other));
}

test "sql_parser binds GROUP BY aliases and positions to SELECT items, as DuckDB does" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, group_by: []const u8 };
    const cases = [_]Case{
        .{ .sql = "SELECT 1, URL, count(*) AS c FROM 't' GROUP BY 1, URL", .group_by = "1, URL" },
        // An alias binds in the engine, where a column of its name wins.
        .{ .sql = "SELECT URL AS Dst, count(*) AS c FROM 't' GROUP BY Dst", .group_by = "Dst" },
        .{ .sql = "SELECT a + 1 AS k, count(*) FROM 't' GROUP BY 1", .group_by = "a + 1" },
        .{ .sql = "SELECT a - 1, count(*) FROM 't' GROUP BY a - 1", .group_by = "a - 1" },
    };
    for (cases) |case| {
        const q = try parseSqlQuery(a, case.sql);
        defer q.deinit(a);
        try std.testing.expectEqualStrings(case.group_by, q.group_by_str.?);
        for (q.select_cols) |col| try std.testing.expect(col.role != .named);
    }
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT URL, count(*) FROM 't' GROUP BY 3"));
    try std.testing.expectError(error.AggregateInGroupBy, parseSqlQuery(a, "SELECT URL, count(*) FROM 't' GROUP BY 2"));
}

test "sql_parser makes ORDER BY terms the SELECT list lacks hidden result columns" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT k FROM 't' GROUP BY k, g ORDER BY count(*) DESC, g, k LIMIT 3");
    defer q.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), q.result_order.hidden.len);
    try std.testing.expectEqualStrings("count(*)", q.result_order.hidden[0].expr);
    try std.testing.expectEqual(ast.SelectColumn.Role.agg, q.result_order.hidden[0].role);
    try std.testing.expectEqual(ast.SelectColumn.Role.key, q.result_order.hidden[1].role);
    try std.testing.expectEqualSlices(ast.OrderKey, &.{
        .{ .column = 1, .descending = true },
        .{ .column = 2, .descending = false },
        .{ .column = 0, .descending = false },
    }, q.result_order.keys);
    // The hidden aggregate is a call of its own, after the SELECT list's.
    try std.testing.expectEqualStrings("count(*) AS " ++ hidden_alias ++ "0", q.aggregate_str.?);

    // A term written twice is one hidden column and one call.
    const twice = try parseSqlQuery(a, "SELECT b FROM 't' GROUP BY b ORDER BY count(*) DESC, count(*) ASC");
    defer twice.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), twice.result_order.hidden.len);
    try std.testing.expectEqualSlices(ast.OrderKey, &.{
        .{ .column = 1, .descending = true },
        .{ .column = 1, .descending = false },
    }, twice.result_order.keys);
    try std.testing.expectEqualStrings("count(*) AS " ++ hidden_alias ++ "0", twice.aggregate_str.?);
}

test "sql_parser hands NOT over groups on as the predicate written" {
    const a = std.testing.allocator;
    const Case = struct { sql: []const u8, filter: []const u8 };
    const cases = [_]Case{
        .{ .sql = "SELECT x FROM 't' WHERE NOT (a = 1 AND b = 2)", .filter = "NOT a = 1 OR NOT b = 2" },
        .{ .sql = "SELECT x FROM 't' WHERE NOT ((a = 1 OR b = 2) AND c = 3)", .filter = "NOT a = 1 AND NOT b = 2 OR NOT c = 3" },
        .{ .sql = "SELECT x FROM 't' WHERE NOT NOT (a = 1 OR b = 2) AND c = 3", .filter = "(a = 1 OR b = 2) AND c = 3" },
        .{ .sql = "SELECT x FROM 't' WHERE (a = 1 OR b = 2) AND NOT c IN (1, 2)", .filter = "(a = 1 OR b = 2) AND NOT c IN (1, 2)" },
    };
    for (cases) |case| {
        const q = try parseSqlQuery(a, case.sql);
        defer q.deinit(a);
        try std.testing.expectEqualStrings(case.filter, q.filter_str.?);
    }
}

test "sql_parser lowers HAVING to result columns, hidden terms and literals" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT k, count(*) AS c FROM 't' GROUP BY k HAVING c > 100000 AND NOT (sum(x) BETWEEN -1 AND 2.5 OR k IS NULL) ORDER BY sum(x)");
    defer q.deinit(a);
    const h = q.result_order.having.?.*;
    // NOT (range OR null) pushes down to (outside the range AND not null), which joins the top chain.
    try std.testing.expectEqual(@as(usize, 3), h.all.len);
    try std.testing.expectEqual(@as(usize, 1), h.all[0].cmp.column);
    try std.testing.expectEqual(ast.CmpOp.gt, h.all[0].cmp.op);
    try std.testing.expectEqualStrings("100000", h.all[0].cmp.literal.number);
    // The hidden sum(x) is shared with ORDER BY.
    try std.testing.expectEqual(ast.CmpOp.lt, h.all[1].any[0].cmp.op);
    try std.testing.expectEqualStrings("-1", h.all[1].any[0].cmp.literal.number);
    try std.testing.expectEqual(@as(usize, 2), h.all[1].any[0].cmp.column);
    try std.testing.expect(h.all[2].null_check.is_not);
    try std.testing.expectEqual(@as(usize, 1), q.result_order.hidden.len);
    try std.testing.expectEqual(@as(usize, 2), q.result_order.keys[0].column);

    // Without GROUP BY a HAVING aggregate may be hidden; a literal on the left flips.
    const g = try parseSqlQuery(a, "SELECT count(*) FROM 't' HAVING 5 < sum(x)");
    defer g.deinit(a);
    try std.testing.expectEqual(ast.CmpOp.gt, g.result_order.having.?.cmp.op);
    try std.testing.expectEqual(@as(usize, 1), g.result_order.having.?.cmp.column);
    try std.testing.expectEqualStrings("count(*), sum(x) AS " ++ hidden_alias ++ "0", g.aggregate_str.?);

    try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, "SELECT k, count(*) FROM 't' GROUP BY k HAVING count(*) > sum(x)"));
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT k, count(*) FROM 't' GROUP BY k HAVING z > 1"));
}

test "sql_parser reads unquoted true and false as boolean literals" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT x FROM 't' WHERE flag = true AND NOT b = False AND \"true\" = 1");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("flag = TRUE AND NOT b = FALSE AND \"true\" = 1", q.filter_str.?);
}

test "sql_parser binds ORDER BY over plain rows to row expressions" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT SearchPhrase AS p FROM 't' WHERE SearchPhrase <> '' ORDER BY EventTime, p DESC, 1 LIMIT 10 OFFSET 2");
    defer q.deinit(a);
    const rows = q.result_order.rows;
    try std.testing.expectEqual(@as(usize, 3), rows.len);
    try std.testing.expectEqualStrings("EventTime", rows[0].expr);
    try std.testing.expectEqualStrings("SearchPhrase", rows[1].expr);
    try std.testing.expect(rows[1].descending);
    try std.testing.expectEqualStrings("SearchPhrase", rows[2].expr);
    try std.testing.expectEqual(@as(?u64, 10), q.result_order.limit);
    try std.testing.expectEqual(@as(u64, 2), q.result_order.offset);
    // Without a LIMIT every row is sorted; an expression of the row is a key, an aggregate is not.
    const all = try parseSqlQuery(a, "SELECT x FROM 't' ORDER BY x + 1, strlen(s) DESC");
    defer all.deinit(a);
    try std.testing.expectEqualStrings("x + 1", all.result_order.rows[0].expr);
    try std.testing.expectEqual(@as(?u64, null), all.result_order.limit);
    try std.testing.expectError(error.OrderByNotSupported, parseSqlQuery(a, "SELECT x FROM 't' ORDER BY count(*) LIMIT 3"));
}

test "regression: a quoted alias names its text, a doubled quote one quote" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT k AS \"a\"\"b\", count(*) AS \"n m\", sum(x) AS s FROM 't' GROUP BY k");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("a\"b", q.select_cols[0].alias.?);
    try std.testing.expectEqualStrings("n m", q.select_cols[1].alias.?);
    try std.testing.expectEqualStrings("s", q.select_cols[2].alias.?);
}

test "sql_parser reads EXTRACT(part FROM x) as date_part('part', x), quoted text left alone" {
    const a = std.testing.allocator;
    const got = try extractAsDatePart(a, "SELECT extract(minute FROM epoch_ms(t)) AS m, 'extract(x FROM y)', " ++
        "\"extract\" FROM f WHERE EXTRACT ( year\nfrom d) = 2013");
    defer a.free(got);
    try std.testing.expectEqualStrings("SELECT date_part('minute', epoch_ms(t)) AS m, 'extract(x FROM y)', " ++
        "\"extract\" FROM f WHERE date_part('year', d) = 2013", got);
    const quoted = try extractAsDatePart(a, "SELECT EXTRACT('year' FROM d), extract(\"month\" from d), " ++
        "extract('' FROM d)");
    defer a.free(quoted);
    try std.testing.expectEqualStrings("SELECT date_part('year', d), date_part('month', d), " ++
        "extract('' FROM d)", quoted);
    const q = try parseSqlQuery(a, "SELECT extract(minute FROM t) AS m, count(*) FROM f GROUP BY m");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("date_part('minute', t)", q.select_cols[0].expr);
}
