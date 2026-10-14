const std = @import("std");
const c = @import("liteparser_c");
const ast = @import("ast.zig");

pub const ParsedQuery = struct {
    table_name: []const u8,
    is_aggregate: bool,
    select_str: ?[]const u8,
    aggregate_str: ?[]const u8,
    filter_str: ?[]const u8,
    group_by_str: ?[]const u8 = null,
    /// The result columns, expression and alias apart, for GROUP BY output binding.
    select_cols: []const ast.SelectColumn = &.{},

    pub fn deinit(self: ParsedQuery, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        if (self.select_str) |s| allocator.free(s);
        if (self.aggregate_str) |a| allocator.free(a);
        if (self.filter_str) |f| allocator.free(f);
        if (self.group_by_str) |g| allocator.free(g);
        freeSelectCols(allocator, self.select_cols);
    }
};

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
            if (node.u.function.name) |nm| {
                const name = std.mem.span(nm);
                if (std.ascii.eqlIgnoreCase(name, "sum") or
                    std.ascii.eqlIgnoreCase(name, "count") or
                    std.ascii.eqlIgnoreCase(name, "avg") or
                    std.ascii.eqlIgnoreCase(name, "min") or
                    std.ascii.eqlIgnoreCase(name, "max")) is_agg = true;
            }
            if (is_agg) info.has_agg = true;
            if (node.u.function.over != null) info.has_window = true; // `f() OVER (...)`
            if (node.u.function.distinct != 0) info.has_func_distinct = true; // `count(DISTINCT x)`
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

/// Reject SELECT clauses ZPQ doesn't implement with a clear, named error
/// instead of silently ignoring them and returning a confidently wrong
/// answer. These are outside the bounded streaming SQL surface, not
/// forbidden forever, so the messages say "not yet".
fn rejectUnsupportedClauses(sel: anytype) !void {
    if (sel.distinct != 0) {
        std.debug.print("sql: DISTINCT is not supported yet\n", .{});
        return error.DistinctNotSupported;
    }
    // GROUP BY is supported
    if (sel.having != null) {
        std.debug.print("sql: HAVING is not supported yet\n", .{});
        return error.HavingNotSupported;
    }
    if (sel.order_by.count > 0) {
        std.debug.print("sql: ORDER BY is not supported yet\n", .{});
        return error.OrderByNotSupported;
    }
    if (sel.limit != null) {
        std.debug.print("sql: LIMIT / OFFSET is not supported yet\n", .{});
        return error.LimitNotSupported;
    }
    if (sel.window_defs.count > 0) {
        std.debug.print("sql: window functions are not supported yet\n", .{});
        return error.WindowNotSupported;
    }
    if (sel.with != null) {
        std.debug.print("sql: WITH / CTE is not supported yet\n", .{});
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

fn isNot(n: *const c.LpNode) bool {
    return n.kind == c.LP_EXPR_UNARY_OP and n.u.unary.op == c.LP_UOP_NOT and n.u.unary.operand != null;
}

fn isBool(n: *const c.LpNode, op: c.LpBinOp) bool {
    return n.kind == c.LP_EXPR_BINARY_OP and n.u.binary.op == op and n.u.binary.left != null and
        n.u.binary.right != null;
}

/// Rewrite predicates, in place, into the shape the flat filter grammar reads back exactly. liteparser prints a NOT's
/// operand without parentheses, so `NOT (a AND b)` unparsed as `NOT a AND b`, which the grammar reads as
/// `(NOT a) AND b`: a different predicate. So every NOT is pushed to the leaves (De Morgan; `NOT NOT p` is `p`;
/// `NOT x NOT IN/BETWEEN` drops both NOTs), which keeps three-valued logic: a NULL operand leaves the leaf UNKNOWN
/// either way. Right-nested `a OR (b OR c)` unparses with parentheses the grammar rejects, so same-operator chains are
/// re-associated to the left (AND and OR are associative in three-valued logic too) once the children are final.
/// Pushing NOTs at enter, before the walker reads the node's children, means the NOTs it creates are pushed in turn.
fn normalizePredicates(root: *c.LpNode, lp_arena: *c.arena_t) error{OutOfMemory}!void {
    const Visit = struct {
        failed: bool = false,
        arena: *c.arena_t,

        fn enter(visitor: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(visitor.*.user_data));
            // The grammar reads `NOT x BETWEEN` but not `x NOT BETWEEN`.
            if (node.*.kind == c.LP_EXPR_BETWEEN and node.*.u.between.is_not != 0) {
                const between = c.lp_node_alloc(self.arena, c.LP_EXPR_BETWEEN) orelse {
                    self.failed = true;
                    return 2;
                };
                between.* = node.*;
                between.*.u.between.is_not = 0;
                node.*.kind = c.LP_EXPR_UNARY_OP;
                node.*.u = .{ .unary = .{ .op = c.LP_UOP_NOT, .operand = between } };
                return 0;
            }
            while (true) {
                if (isNot(node)) {
                    const operand: *c.LpNode = node.*.u.unary.operand;
                    if (isNot(operand)) {
                        node.* = operand.u.unary.operand.*;
                        continue;
                    }
                    if ((operand.kind == c.LP_EXPR_IN and operand.u.in.is_not != 0) or
                        (operand.kind == c.LP_EXPR_BETWEEN and operand.u.between.is_not != 0))
                    {
                        node.* = operand.*;
                        if (node.*.kind == c.LP_EXPR_IN) node.*.u.in.is_not = 0 else node.*.u.between.is_not = 0;
                        continue;
                    }
                    if (!isBool(operand, c.LP_OP_AND) and !isBool(operand, c.LP_OP_OR)) break;
                    const op = operand.u.binary.op;
                    const left = c.lp_node_alloc(self.arena, c.LP_EXPR_UNARY_OP) orelse {
                        self.failed = true;
                        return 2;
                    };
                    const right = c.lp_node_alloc(self.arena, c.LP_EXPR_UNARY_OP) orelse {
                        self.failed = true;
                        return 2;
                    };
                    left.*.u.unary = .{ .op = c.LP_UOP_NOT, .operand = operand.u.binary.left };
                    right.*.u.unary = .{ .op = c.LP_UOP_NOT, .operand = operand.u.binary.right };
                    node.* = operand.*;
                    node.*.u.binary.op = if (op == c.LP_OP_AND) c.LP_OP_OR else c.LP_OP_AND;
                    node.*.u.binary.left = left;
                    node.*.u.binary.right = right;
                    continue;
                }
                break;
            }
            return 0;
        }

        /// Children are final here. Rotating `a op (b op c)` to `(a op b) op c` reuses the inner node for
        /// `(a op b)`, whose `b` may itself be an `op` chain, so walk down the left spine.
        fn leave(_: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            inline for (.{ c.LP_OP_AND, c.LP_OP_OR }) |op| {
                var n: *c.LpNode = node;
                while (isBool(n, op)) : (n = n.u.binary.left) {
                    while (isBool(n.u.binary.right, op)) {
                        const inner: *c.LpNode = n.u.binary.right;
                        n.u.binary.right = inner.u.binary.right;
                        inner.u.binary.right = inner.u.binary.left;
                        inner.u.binary.left = n.u.binary.left;
                        n.u.binary.left = inner;
                    }
                }
            }
            return 0;
        }
    };
    var state: Visit = .{ .arena = lp_arena };
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = Visit.leave };
    c.lp_ast_walk(root, &visitor);
    if (state.failed) return error.OutOfMemory;
}

/// Whether the filter grammar's raw-substring splitting would cut `s`: it splits on ` OR ` (case-sensitive) and on
/// ` AND ` / ` BETWEEN ` (case-insensitive) without regard to quoting.
fn splitsFlatGrammar(s: []const u8) bool {
    return std.mem.indexOf(u8, s, " OR ") != null or std.ascii.findIgnoreCase(s, " AND ") != null or
        std.ascii.findIgnoreCase(s, " BETWEEN ") != null;
}

fn isColumn(n: ?*const c.LpNode) bool {
    const node = n orelse return false;
    if (node.kind != c.LP_EXPR_COLUMN_REF) return false;
    const ref = node.u.column_ref;
    for ([_][*c]u8{ ref.schema, ref.table, ref.column }) |part| {
        if (part) |p| if (splitsFlatGrammar(std.mem.span(p))) return false;
    }
    return true;
}

/// A literal the grammar reads back as the same value: a number, optionally signed, or a string it will not cut.
/// NULL is not one (the grammar reads it as the string `NULL`), nor is anything it would take as a bare string: a
/// column, a cast, a COLLATE, an expression or a blob.
fn isLiteral(n: ?*const c.LpNode, in_list: bool) bool {
    const node = n orelse return false;
    switch (node.kind) {
        c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => return true,
        c.LP_EXPR_UNARY_OP => {
            if (node.u.unary.op != c.LP_UOP_MINUS and node.u.unary.op != c.LP_UOP_PLUS) return false;
            const operand: *const c.LpNode = node.u.unary.operand orelse return false;
            return operand.kind == c.LP_EXPR_LITERAL_INT or operand.kind == c.LP_EXPR_LITERAL_FLOAT;
        },
        c.LP_EXPR_LITERAL_STRING => {
            const s = std.mem.span(node.u.literal.value orelse return false);
            // The grammar reads an escaped quote (`''`) as two quotes of data, and splits an IN list on every comma.
            return !splitsFlatGrammar(s) and std.mem.indexOfScalar(u8, s, '\'') == null and
                !(in_list and std.mem.indexOfScalar(u8, s, ',') != null);
        },
        else => return false,
    }
}

/// Whether a normalized leaf is one of the shapes the filter grammar reads back as written: `col op literal`,
/// `col [NOT] LIKE 'pattern'` (no ESCAPE, which it would read as pattern), `col IS [NOT] NULL`, `col [NOT] IN
/// (literals)` or `col [NOT] BETWEEN literal AND literal`, under at most the grammar's own leading NOT.
fn flatLeaf(leaf: *c.LpNode) bool {
    const n: *c.LpNode = if (isNot(leaf)) leaf.u.unary.operand else leaf;
    switch (n.kind) {
        c.LP_EXPR_BINARY_OP => {
            const b = n.u.binary;
            if (!isColumn(b.left)) return false;
            return switch (b.op) {
                c.LP_OP_EQ, c.LP_OP_NE, c.LP_OP_LT, c.LP_OP_LE, c.LP_OP_GT, c.LP_OP_GE => isLiteral(b.right, false),
                c.LP_OP_LIKE => b.escape == null and b.right != null and b.right.*.kind == c.LP_EXPR_LITERAL_STRING and
                    isLiteral(b.right, false),
                c.LP_OP_IS, c.LP_OP_ISNOT => b.right != null and b.right.*.kind == c.LP_EXPR_LITERAL_NULL,
                else => false,
            };
        },
        c.LP_EXPR_IN => {
            const in = n.u.in;
            if (in.select != null or !isColumn(in.expr) or in.values.count == 0) return false;
            for (0..@intCast(in.values.count)) |i| if (!isLiteral(in.values.items[i], true)) return false;
            return true;
        },
        c.LP_EXPR_BETWEEN => {
            const b = n.u.between;
            return isColumn(b.expr) and isLiteral(b.low, false) and isLiteral(b.high, false);
        },
        else => return false,
    }
}

/// Refuse a normalized predicate the flat AND/OR grammar cannot hold as written: an OR under an AND (it needs
/// grouping), or a leaf that is not a shape `flatLeaf` knows reads back as itself. Evaluating either anyway would
/// answer another query.
fn checkFlatPredicate(pred: *c.LpNode) error{ GroupingNotSupported, UnsupportedPredicate }!void {
    var disj: *c.LpNode = pred;
    while (true) {
        const term: *c.LpNode = if (isBool(disj, c.LP_OP_OR)) disj.u.binary.right else disj;
        var conj: *c.LpNode = term;
        while (true) {
            const leaf: *c.LpNode = if (isBool(conj, c.LP_OP_AND)) conj.u.binary.right else conj;
            if (isBool(leaf, c.LP_OP_OR)) {
                std.debug.print("sql: the filter grammar is a flat OR of ANDs; this predicate needs an OR\n" ++
                    "  inside an AND (after pushing NOT down). Rewrite it as an OR of ANDs\n", .{});
                return error.GroupingNotSupported;
            }
            if (!flatLeaf(leaf)) {
                std.debug.print("sql: a WHERE/FILTER condition must be `column op literal`, LIKE (no ESCAPE),\n" ++
                    "  IS [NOT] NULL, or IN/BETWEEN over literals. NULL, TRUE/FALSE, casts,\n" ++
                    "  expressions and other columns are not supported as values, nor are\n" ++
                    "  strings holding a quote, ' AND ', ' OR ' or ' BETWEEN '\n", .{});
                return error.UnsupportedPredicate;
            }
            if (conj == leaf) break;
            conj = conj.u.binary.left;
        }
        if (disj == term) break;
        disj = disj.u.binary.left;
    }
}

/// Check every aggregate FILTER body under `node`: like WHERE, it reaches the flat grammar as unparsed text.
fn checkAggFilters(node: *c.LpNode) error{ GroupingNotSupported, UnsupportedPredicate }!void {
    const Visit = struct {
        err: ?error{ GroupingNotSupported, UnsupportedPredicate } = null,
        fn enter(visitor: [*c]c.LpVisitor, n: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(visitor.*.user_data));
            if (n.*.kind == c.LP_EXPR_FUNCTION) if (n.*.u.function.filter) |f| {
                checkFlatPredicate(f) catch |e| {
                    self.err = e;
                    return 2;
                };
            };
            return 0;
        }
    };
    var state: Visit = .{};
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(node, &visitor);
    if (state.err) |e| return e;
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
        out.alias = try allocator.dupe(u8, std.mem.span(rc.u.result_column.alias));
    }
}

pub fn parseSqlQuery(allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);

    var error_msg: [*c]const u8 = null;
    const sql_c = try allocator.dupeSentinel(u8, sql, 0);
    defer allocator.free(sql_c);

    // lp_parse() silently stops at the first ';', so trailing statements/clauses
    // would vanish unnoticed. lp_parse_all() returns every statement → we reject
    // anything but exactly one.
    const stmts = c.lp_parse_all(sql_c, lp_arena, &error_msg);
    if (stmts == null or stmts.*.count == 0) {
        if (error_msg) |msg| std.debug.print("sql parse error: {s}\n", .{std.mem.span(msg)});
        return error.SqlParseError;
    }
    if (stmts.*.count > 1) {
        std.debug.print("sql: only a single statement is supported (found {d})\n", .{stmts.*.count});
        return error.MultipleStatements;
    }
    const root = stmts.*.items[0] orelse return error.SqlParseError;

    // Only single-table SELECT. UNION/INSERT/etc. arrive as a non-SELECT
    // root; joins and subqueries arrive as a non-LP_FROM_TABLE `from`.
    if (root.*.kind != c.LP_STMT_SELECT) {
        std.debug.print("sql: only SELECT is supported (no UNION/INSERT/DDL)\n", .{});
        return error.UnsupportedStatement;
    }
    // Reject clauses we'd otherwise silently drop → wrong answers.
    try rejectUnsupportedClauses(root.*.u.select);
    quoteWholePaths(root);
    try normalizePredicates(root, lp_arena);
    if (root.*.u.select.where) |w| {
        const wi = analyzeExpr(w);
        if (wi.has_window) {
            std.debug.print("sql: window functions are not supported yet\n", .{});
            return error.WindowNotSupported;
        }
        if (wi.has_subquery) {
            std.debug.print("sql: subqueries are not supported yet\n", .{});
            return error.SubqueryNotSupported;
        }
        if (wi.has_func_distinct) {
            std.debug.print("sql: DISTINCT inside a function is not supported yet\n", .{});
            return error.DistinctNotSupported;
        }
        if (wi.has_agg) {
            std.debug.print("sql: aggregates are not allowed in WHERE\n", .{});
            return error.AggregateInWhere;
        }
        try checkFlatPredicate(w);
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
            std.debug.print("sql: window functions (OVER) are not supported yet\n", .{});
            return error.WindowNotSupported;
        }
        if (info.has_subquery) {
            std.debug.print("sql: subqueries are not supported yet\n", .{});
            return error.SubqueryNotSupported;
        }
        if (info.has_func_distinct) {
            std.debug.print("sql: DISTINCT inside a function is not supported yet\n", .{});
            return error.DistinctNotSupported;
        }
        try checkAggFilters(rc);
        if (info.has_agg) has_agg = true;
        if (info.has_bare_col) has_bare = true;
    }
    const has_group_by = root.*.u.select.group_by.count > 0;
    const is_aggregate = has_agg or has_group_by;

    if (is_aggregate) {
        if (has_bare and !has_group_by) {
            std.debug.print("sql: mixing aggregates with plain columns requires GROUP BY\n", .{});
            return error.MixedSelectNotSupported;
        }
        if (has_group_by) {
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
        col_idx += 1;
    }

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
        };
    }
    // `SELECT *` → no projection list → the engine's zero-copy passthrough
    // (byte-copy fast path), which is exactly ZPQ's surgical sweet spot.
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
        };
    }
    return .{
        .table_name = table_name,
        .is_aggregate = false,
        .select_str = columns_str,
        .aggregate_str = null,
        .filter_str = filter_str,
        .select_cols = select_cols,
    };
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

fn validateGroupBy(allocator: std.mem.Allocator, sel: anytype, lp_arena: *c.arena_t) !void {
    // 1. Unparse and collect all GROUP BY expressions
    var group_by_list: std.ArrayList([]const u8) = .empty;
    var group_by_nodes: std.ArrayList(*const c.LpNode) = .empty;
    defer {
        for (group_by_list.items) |col| allocator.free(col);
        group_by_list.deinit(allocator);
        group_by_nodes.deinit(allocator);
    }
    var i: usize = 0;
    while (i < @as(usize, @intCast(sel.group_by.count))) : (i += 1) {
        const gb_node = sel.group_by.items[i] orelse continue;
        const gb_c = c.lp_ast_to_sql(gb_node, lp_arena);
        if (gb_c == null) return error.UnparseError;
        const gb_sql = try allocator.dupe(u8, std.mem.span(gb_c));
        try group_by_list.append(allocator, gb_sql);
        try group_by_nodes.append(allocator, gb_node);
    }

    // 2. Validate each SELECT result column
    i = 0;
    while (i < @as(usize, @intCast(sel.result_columns.count))) : (i += 1) {
        const rc = sel.result_columns.items[i] orelse continue;
        const info = analyzeExpr(rc);
        if (info.has_bare_col) {
            // Result column contains a bare column. We must unparse the expression
            // itself (not including the AS alias, which is at the result_column level).
            const expr_node = rc.*.u.result_column.expr orelse continue;
            const expr_c = c.lp_ast_to_sql(expr_node, lp_arena);
            if (expr_c == null) return error.UnparseError;
            const expr_sql = std.mem.span(expr_c);

            // Check if this raw expression SQL exists in our group_by list (case-insensitively).
            var found = false;
            for (group_by_list.items, group_by_nodes.items) |gb, gb_node| {
                if (sameColumnRef(expr_node, gb_node)) |same| {
                    if (same) {
                        found = true;
                        break;
                    }
                    continue;
                }
                const a = stripQuotes(std.mem.trim(u8, expr_sql, " "));
                const b = stripQuotes(std.mem.trim(u8, gb, " "));
                if (std.ascii.eqlIgnoreCase(a, b)) {
                    found = true;
                    break;
                }
            }

            if (!found) {
                std.debug.print("sql: result column '{s}' must be in the GROUP BY list\n", .{expr_sql});
                return error.ColumnMustBeGrouped;
            }
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

    try std.testing.expectError(error.OrderByNotSupported, parseSqlQuery(a, "SELECT x FROM 't' ORDER BY x"));
    try std.testing.expectError(error.LimitNotSupported, parseSqlQuery(a, "SELECT x FROM 't' LIMIT 5"));
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT x FROM 't'"));
    try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, "SELECT sum(x) AS s FROM 't' HAVING sum(x) > 1"));
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't'"));
    try std.testing.expectError(error.UnsupportedStatement, parseSqlQuery(a, "SELECT x FROM 't' UNION SELECT x FROM 'u'"));
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't' GROUP BY z"));
}

test "sql_parser rejects unsupported aggregate shapes" {
    const a = std.testing.allocator;
    // count(DISTINCT x) — distinct on the function node (was silently dropped).
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT count(DISTINCT x) AS c FROM 't'"));
    // Bare column inside an aggregate column: `sum(a) + b` is implicit GROUP BY.
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT sum(a) + b AS x FROM 't'"));
    // Aggregate in WHERE is illegal.
    try std.testing.expectError(error.AggregateInWhere, parseSqlQuery(a, "SELECT x FROM 't' WHERE sum(a) > 1"));
    // Trailing statement after ';' must not be silently dropped.
    try std.testing.expectError(error.MultipleStatements, parseSqlQuery(a, "SELECT x FROM 't'; SELECT y FROM 'u'"));
    // Subquery in WHERE.
    try std.testing.expectError(error.SubqueryNotSupported, parseSqlQuery(a, "SELECT x FROM 't' WHERE x IN (SELECT y FROM 'u')"));
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
            std.debug.print("{s}: {s}\n", .{ sql, @errorName(err) });
            return err;
        };
        q.deinit(a);
    }
    const other = "SELECT \"r\".\"w\", count(*) FROM 't' GROUP BY r.v";
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, other));
}

test "sql_parser pushes NOT to the leaves so the flat filter grammar reads the predicate as written" {
    // liteparser unparses `NOT (a AND b)` as `NOT a AND b`, which the grammar reads as `(NOT a) AND b`.
    const a = std.testing.allocator;
    const Case = struct { where: []const u8, filter: []const u8 };
    const cases = [_]Case{
        .{ .where = "NOT (x > 5 AND y < 3)", .filter = "NOT x > 5 OR NOT y < 3" },
        .{ .where = "NOT (x > 5 OR y < 3)", .filter = "NOT x > 5 AND NOT y < 3" },
        .{ .where = "NOT NOT (x > 5 OR y < 3)", .filter = "x > 5 OR y < 3" },
        .{ .where = "NOT NOT NOT x > 5", .filter = "NOT x > 5" },
        .{ .where = "z = 1 AND NOT (x > 5 OR y < 3)", .filter = "z = 1 AND NOT x > 5 AND NOT y < 3" },
        .{ .where = "NOT (x > 5 OR y < 3) AND z = 1", .filter = "NOT x > 5 AND NOT y < 3 AND z = 1" },
        .{ .where = "NOT (x > 5 AND y < 3) OR z = 1", .filter = "NOT x > 5 OR NOT y < 3 OR z = 1" },
        .{ .where = "z = 1 OR NOT (x > 5 AND y < 3)", .filter = "z = 1 OR NOT x > 5 OR NOT y < 3" },
        // De Morgan into an OR of ANDs.
        .{ .where = "NOT ((x > 5 OR y < 3) AND (z = 1 OR x = 2))", .filter = "NOT x > 5 AND NOT y < 3 OR NOT z = 1 AND NOT x = 2" },
        .{ .where = "NOT (x IN (1, 2) AND y = 1)", .filter = "NOT x IN (1, 2) OR NOT y = 1" },
        .{ .where = "NOT (x NOT IN (1, 2) OR y = 1)", .filter = "x IN (1, 2) AND NOT y = 1" },
        .{ .where = "NOT (x NOT BETWEEN 1 AND 3)", .filter = "x BETWEEN 1 AND 3" },
        .{ .where = "x NOT BETWEEN 1 AND 3 OR y = 1", .filter = "NOT x BETWEEN 1 AND 3 OR y = 1" },
        .{ .where = "NOT (x BETWEEN 1 AND 3 OR y = 1)", .filter = "NOT x BETWEEN 1 AND 3 AND NOT y = 1" },
        .{ .where = "NOT (s LIKE 'a%' AND s NOT LIKE '%b')", .filter = "NOT s LIKE 'a%' OR s LIKE '%b'" },
        .{ .where = "NOT (s IS NULL OR s IS NOT NULL)", .filter = "NOT s IS NULL AND NOT s IS NOT NULL" },
        // Right-nested chains unparse with parentheses the grammar rejects; they are re-associated instead.
        .{ .where = "x = 1 OR (y = 2 OR z = 3)", .filter = "x = 1 OR y = 2 OR z = 3" },
        .{ .where = "x = 1 AND (y = 2 AND (z = 3 AND x = 4))", .filter = "x = 1 AND y = 2 AND z = 3 AND x = 4" },
        .{ .where = "(x = 1 AND y = 2) OR z = 3", .filter = "x = 1 AND y = 2 OR z = 3" },
        .{ .where = "x BETWEEN 1 AND 2 OR s = 'a or b'", .filter = "x BETWEEN 1 AND 2 OR s = 'a or b'" },
        // Literal spellings the grammar reads back as the same value.
        .{ .where = "x > -5 AND y < +2.5e3 AND z IN (-1, 2)", .filter = "x > -5 AND y < +2.5e3 AND z IN (-1, 2)" },
        .{ .where = "x BETWEEN -1.5 AND 1e2 OR s = ''", .filter = "x BETWEEN -1.5 AND 1e2 OR s = ''" },
        .{ .where = "d >= '2020-06-01' AND s IS NOT NULL", .filter = "d >= '2020-06-01' AND s IS NOT NULL" },
    };
    for (cases) |case| {
        errdefer std.debug.print("WHERE {s}\n", .{case.where});
        const sql = try std.fmt.allocPrint(a, "SELECT count(*) AS c FROM 't' WHERE {s}", .{case.where});
        defer a.free(sql);
        const q = try parseSqlQuery(a, sql);
        defer q.deinit(a);
        try std.testing.expectEqualStrings(case.filter, q.filter_str.?);
    }
    // An aggregate FILTER body reaches the same grammar as text.
    const q = try parseSqlQuery(a, "SELECT count(*) FILTER (WHERE NOT (x > 5 AND y < 3)) AS c FROM 't'");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("count(*) FILTER (WHERE NOT x > 5 OR NOT y < 3) AS c", q.aggregate_str.?);
}

test "sql_parser refuses predicates the flat filter grammar cannot hold as written" {
    const a = std.testing.allocator;
    const Case = struct { where: []const u8, err: anyerror };
    const g = error.GroupingNotSupported;
    const u = error.UnsupportedPredicate;
    const cases = [_]Case{
        .{ .where = "NOT ((x > 5 AND y < 3) OR z = 1)", .err = g }, // (NOT x > 5 OR NOT y < 3) AND NOT z = 1
        .{ .where = "x = 1 AND (y = 2 OR z = 3)", .err = g },
        .{ .where = "(x = 1 OR y = 2) AND z = 3", .err = g },
        .{ .where = "NOT (x = 1 OR y = 2) OR NOT (z = 3 AND x = 4) AND y = 5", .err = g },
        .{ .where = "NOT (x = 1 AND y = 2) AND z = 3", .err = g },
        // Literals and names the grammar's raw-substring splitting would cut.
        .{ .where = "s = 'a OR b = 1'", .err = u },
        .{ .where = "s = 'rock AND roll'", .err = u },
        .{ .where = "s = 'x BETWEEN y'", .err = u },
        .{ .where = "s = 'O''Brien'", .err = u },
        .{ .where = "s IN ('a,b', 'c')", .err = u },
        .{ .where = "\"a OR b\" = 1", .err = u },
        .{ .where = "s LIKE 'a#%' ESCAPE '#'", .err = u },
        // Values the grammar would read as something else: NULL as the string `NULL`, the rest as bare strings.
        .{ .where = "s NOT IN ('a', NULL)", .err = u },
        .{ .where = "NOT (s IN ('a', NULL))", .err = u },
        .{ .where = "s IN (NULL)", .err = u },
        .{ .where = "s != NULL", .err = u },
        .{ .where = "s > NULL", .err = u },
        .{ .where = "s LIKE NULL", .err = u },
        .{ .where = "x BETWEEN NULL AND 5", .err = u },
        .{ .where = "s IS 'a'", .err = u },
        .{ .where = "b = TRUE", .err = u },
        .{ .where = "s = t", .err = u },
        .{ .where = "s = \"t\"", .err = u },
        .{ .where = "s = 'a' || 'b'", .err = u },
        .{ .where = "s = 'a' COLLATE NOCASE", .err = u },
        .{ .where = "s = upper('a')", .err = u },
        .{ .where = "s = CAST('a' AS TEXT)", .err = u },
        .{ .where = "s = x'61'", .err = u },
        .{ .where = "x > 2 + 3", .err = u },
        .{ .where = "x > - -5", .err = u },
        .{ .where = "5 < x", .err = u },
        .{ .where = "x + 1 > 5", .err = u },
        .{ .where = "s GLOB 'a*'", .err = u },
    };
    for (cases) |case| {
        errdefer std.debug.print("WHERE {s}\n", .{case.where});
        const sql = try std.fmt.allocPrint(a, "SELECT count(*) AS c FROM 't' WHERE {s}", .{case.where});
        defer a.free(sql);
        try std.testing.expectError(case.err, parseSqlQuery(a, sql));
    }
    try std.testing.expectError(error.GroupingNotSupported, parseSqlQuery(a,
        \\SELECT count(*) FILTER (WHERE NOT ((x > 5 AND y < 3) OR z = 1)) AS c FROM 't'
    ));
    try std.testing.expectError(error.UnsupportedPredicate, parseSqlQuery(a,
        \\SELECT count(*) FILTER (WHERE s NOT IN ('a', NULL)) AS c FROM 't'
    ));
}
