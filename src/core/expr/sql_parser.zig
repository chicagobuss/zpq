const std = @import("std");
const c = @cImport({
    @cInclude("liteparser.h");
    @cInclude("arena.h");
});

pub const ParsedQuery = struct {
    table_name: []const u8,
    is_aggregate: bool,
    select_str: ?[]const u8,
    aggregate_str: ?[]const u8,
    filter_str: ?[]const u8,
    group_by_str: ?[]const u8 = null,
    select_cols: []const []const u8 = &[_][]const u8{},

    pub fn deinit(self: ParsedQuery, allocator: std.mem.Allocator) void {
        allocator.free(self.table_name);
        if (self.select_str) |s| allocator.free(s);
        if (self.aggregate_str) |a| allocator.free(a);
        if (self.filter_str) |f| allocator.free(f);
        if (self.group_by_str) |g| allocator.free(g);
        for (self.select_cols) |col| allocator.free(col);
        allocator.free(self.select_cols);
    }
};

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

pub fn parseSqlQuery(allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);

    var error_msg: [*c]const u8 = null;
    const sql_c = try allocator.dupeZ(u8, sql);
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

    const select_cols = try allocator.alloc([]const u8, columns_list.items.len);
    errdefer {
        for (select_cols) |col| allocator.free(col);
        allocator.free(select_cols);
    }
    for (columns_list.items, 0..) |col, idx| {
        select_cols[idx] = try allocator.dupe(u8, col);
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
        for (select_cols) |col| allocator.free(col);
        allocator.free(select_cols);
        return .{
            .table_name = table_name,
            .is_aggregate = false,
            .select_str = null,
            .aggregate_str = null,
            .filter_str = filter_str,
            .select_cols = &[_][]const u8{},
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

fn validateGroupBy(allocator: std.mem.Allocator, sel: anytype, lp_arena: *c.arena_t) !void {
    // 1. Unparse and collect all GROUP BY expressions
    var group_by_list: std.ArrayList([]const u8) = .empty;
    defer {
        for (group_by_list.items) |col| allocator.free(col);
        group_by_list.deinit(allocator);
    }
    var i: usize = 0;
    while (i < @as(usize, @intCast(sel.group_by.count))) : (i += 1) {
        const gb_node = sel.group_by.items[i] orelse continue;
        const gb_c = c.lp_ast_to_sql(gb_node, lp_arena);
        if (gb_c == null) return error.UnparseError;
        const gb_sql = try allocator.dupe(u8, std.mem.span(gb_c));
        try group_by_list.append(allocator, gb_sql);
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
            for (group_by_list.items) |gb| {
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
    try std.testing.expectEqualStrings("y", q_gb_mixed.select_cols[0]);
    try std.testing.expectEqualStrings("sum(x) AS s", q_gb_mixed.select_cols[1]);

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
