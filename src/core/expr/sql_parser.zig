const std = @import("std");
const c = @import("liteparser_c");
const ast = @import("ast.zig");
const syntax = @import("syntax.zig");
const Lower = @import("sql_lower.zig").Lower;
const parser = @import("parser.zig");
const literals = @import("sql_literals.zig");
const sql_join = @import("sql_join.zig");
const schema = @import("../schema.zig");
const metadata = @import("../parquet/metadata.zig");

// CLI machine diagnostics use the same parser messages without mixing prose into JSON stderr.
pub threadlocal var diagnostic_fn: ?*const fn ([]const u8) void = null;
fn diagnosticPrint(comptime fmt: []const u8, args: anytype) void {
    if (diagnostic_fn) |report| {
        var buffer: [2048]u8 = undefined;
        report(std.fmt.bufPrint(&buffer, fmt, args) catch return);
    } else std.debug.print(fmt, args);
}

pub const ParsedQuery = struct {
    syntax: ?*const syntax.Query = null,
    syntax_arena: ?*std.heap.ArenaAllocator = null,
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
        if (self.syntax_arena) |owner| {
            owner.deinit();
            allocator.destroy(owner);
        }
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

fn freeSelectItems(allocator: std.mem.Allocator, cols: []const ast.SelectColumn) void {
    for (cols) |col| {
        allocator.free(col.expr);
        if (col.alias) |a| allocator.free(a);
    }
}

fn freeSelectCols(allocator: std.mem.Allocator, cols: []const ast.SelectColumn) void {
    freeSelectItems(allocator, cols);
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
                } else if (parser.isSqlAggName(name)) is_agg = true;
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

/// `SELECT DISTINCT a, b` is `SELECT a, b ... GROUP BY a, b` with no aggregate: the groups are the distinct rows, and
/// NULLs fall in one group as DISTINCT puts them. With an aggregate, a star, a GROUP BY or a HAVING the rewrite would
/// be another query, so those stay refused by name.
fn distinctAsGroupBy(root: *c.LpNode, lp_arena: *c.arena_t) !void {
    const sel = &root.*.u.select;
    if (sel.distinct == 0) return;
    if (sel.group_by.count != 0 or sel.having != null) {
        diagnosticPrint("sql: DISTINCT with GROUP BY or HAVING is not supported yet\n", .{});
        return error.DistinctNotSupported;
    }
    var i: usize = 0;
    while (i < @as(usize, @intCast(sel.result_columns.count))) : (i += 1) {
        const expr = selectExpr(sel.*, i) orelse return error.SqlParseError;
        if (expr.kind == c.LP_EXPR_STAR) {
            diagnosticPrint("sql: SELECT DISTINCT * is not supported yet (name the columns)\n", .{});
            return error.DistinctNotSupported;
        }
        const info = analyzeExpr(expr);
        if (info.has_agg or info.has_window or info.has_subquery) {
            diagnosticPrint("sql: DISTINCT over an aggregate, window or subquery is not supported yet\n", .{});
            return error.DistinctNotSupported;
        }
        try pushGroupTerm(sel, lp_arena, expr);
    }
    sel.distinct = 0;
}

/// Whether two unparsed expressions are written alike: names and keywords without regard to case, but the contents
/// of a string literal byte for byte (`'a'` and `'A'` are two values).
fn sameExprText(a: []const u8, b: []const u8) bool {
    if (a.len != b.len) return false;
    var in_string = false;
    for (a, b) |x, y| {
        if (x == '\'') in_string = !in_string;
        if (if (in_string) x != y else std.ascii.toLower(x) != std.ascii.toLower(y)) return false;
    }
    return true;
}

/// Add `expr` to GROUP BY once: a column or expression written alike is the same term.
fn pushGroupTerm(sel: anytype, lp_arena: *c.arena_t, expr: *c.LpNode) !void {
    const text_c = c.lp_ast_to_sql(expr, lp_arena);
    if (text_c == null) return error.UnparseError;
    const text = std.mem.span(text_c);
    for (0..@intCast(sel.group_by.count)) |g| {
        const term = sel.group_by.items[g] orelse continue;
        if (sameColumnRef(expr, term) orelse blk: {
            const other = c.lp_ast_to_sql(term, lp_arena);
            break :blk other != null and sameExprText(text, std.mem.span(other));
        }) return;
    }
    c.lp_list_push(lp_arena, &sel.group_by, expr);
}

/// An expression of literals alone (`1`, `-5`, `2 * 3`, `CAST('x' AS date)`): one value, whatever the rows.
fn constantOnly(node: *const c.LpNode) bool {
    return switch (node.kind) {
        c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT, c.LP_EXPR_LITERAL_STRING, c.LP_EXPR_LITERAL_NULL, c.LP_EXPR_LITERAL_BOOL => true,
        c.LP_EXPR_UNARY_OP => if (node.u.unary.operand) |o| constantOnly(o) else false,
        c.LP_EXPR_BINARY_OP => (if (node.u.binary.left) |l| constantOnly(l) else false) and
            (if (node.u.binary.right) |r| constantOnly(r) else false),
        c.LP_EXPR_CAST => if (node.u.cast.expr) |e| constantOnly(e) else false,
        else => false,
    };
}

/// `GROUP BY ALL` groups by every SELECT item that is no aggregate (DuckDB). `sql_literals` spelled it as the one name
/// `marker`, chosen not to occur in the query, which this swaps for those items. A constant is no grouping key of its
/// own: with other keys it joins them (it cannot split a group), and with none the query has no grouping at all, so
/// the aggregates form one group and a query of constants alone keeps one row per input row, as in DuckDB.
fn groupByAll(root: *c.LpNode, lp_arena: *c.arena_t, marker: []const u8) !void {
    const sel = &root.*.u.select;
    if (sel.group_by.count != 1) return;
    const term = sel.group_by.items[0] orelse return;
    if (term.*.kind != c.LP_EXPR_COLUMN_REF or term.*.u.column_ref.table != null or term.*.u.column_ref.column == null) return;
    if (term.*.u.column_ref.quoted != 0 or !std.mem.eql(u8, std.mem.span(term.*.u.column_ref.column), marker)) return;
    sel.group_by.count = 0;
    var any_key = false;
    var any_agg = false;
    var i: usize = 0;
    while (i < @as(usize, @intCast(sel.result_columns.count))) : (i += 1) {
        const expr = selectExpr(sel.*, i) orelse return error.SqlParseError;
        if (expr.kind == c.LP_EXPR_STAR) {
            diagnosticPrint("sql: GROUP BY ALL with SELECT * is not supported yet (name the columns)\n", .{});
            return error.ColumnMustBeGrouped;
        }
        const info = analyzeExpr(expr);
        if (info.has_agg or info.has_window or info.has_subquery) {
            any_agg = any_agg or info.has_agg;
            continue;
        }
        any_key = any_key or !constantOnly(expr);
        try pushGroupTerm(sel, lp_arena, expr);
    }
    if (any_key or sel.group_by.count == 0) return;
    // Only constants: no grouping at all.
    sel.group_by.count = 0;
    if (any_agg) {
        diagnosticPrint("sql: GROUP BY ALL with a constant beside an aggregate is not supported yet\n", .{});
        return error.MixedSelectNotSupported;
    }
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

/// Refuse a predicate in a FILTER or a CASE condition whose value side reads a column (`s = t`, `s IN ('a', t)`,
/// `x BETWEEN 1 AND y`), and in WHERE such a LIKE, IN or BETWEEN: the filter grammar compares a column with literals
/// only, and before this a value-side identifier, a column or a misspelt one, could bind as literal text and match
/// incorrect rows. WHERE's own comparisons of two expressions lower as such (`syntax.Predicate.expr_compare`).
fn rejectColumnComparisons(root: *c.LpNode, where_expressions: bool) error{BadValue}!void {
    const Visit = struct {
        found: ?*c.LpNode = null,
        /// Walking WHERE: a comparison (=, <>, <, <=, >, >=) may read columns on both sides.
        where: bool = false,

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
                    => !self.where and readsColumn(node.*.u.binary.right),
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
    var state: Visit = .{ .where = where_expressions };
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    if (root.u.select.where) |w| c.lp_ast_walk(w, &visitor);
    state.where = false;
    var i: usize = 0;
    while (state.found == null and i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
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

/// Words that end an operand of `//` rather than being one.
fn endsOperand(word: []const u8) bool {
    const words = [_][]const u8{
        "and", "or", "not", "in", "is", "like", "ilike", "between", "case", "when", "then", "else", "end", "select",
        "from", "where", "group", "by", "having", "order", "limit", "offset", "on", "as", "distinct", "union",
    };
    for (words) |w| if (std.ascii.eqlIgnoreCase(word, w)) return true;
    return false;
}

fn isNameByte(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '$';
}

fn skipSpacesBack(buf: []const u8, end: usize) usize {
    var k = end;
    while (k > 0 and std.ascii.isWhitespace(buf[k - 1])) k -= 1;
    return k;
}

/// The start of the quoted text that ends at `buf[end - 1]` (a quote), a doubled quote inside it being one character.
fn quotedBack(buf: []const u8, end: usize) ?usize {
    const q = buf[end - 1];
    var k = end - 1;
    while (k > 0) {
        k -= 1;
        if (buf[k] != q) continue;
        if (k > 0 and buf[k - 1] == q) {
            k -= 1;
            continue;
        }
        return k;
    }
    return null;
}

/// The name (a word, or quoted segments joined by dots) that ends at `end`, as its start; null if none or a keyword.
fn nameBack(buf: []const u8, end: usize) ?usize {
    var k = end;
    while (true) {
        if (k == 0) break;
        if (buf[k - 1] == '"') {
            k = quotedBack(buf, k) orelse return null;
        } else if (isNameByte(buf[k - 1])) {
            while (k > 0 and isNameByte(buf[k - 1])) k -= 1;
        } else break;
        if (k > 0 and buf[k - 1] == '.') k -= 1 else break;
    }
    if (k == end) return null;
    if (endsOperand(buf[k..end])) return null;
    return k;
}

/// The start of the operand that ends at `end`: a name or number, a quoted text, or a group or call, whose
/// parentheses are matched backward.
fn atomBack(buf: []const u8, end: usize) ?usize {
    if (end == 0) return null;
    switch (buf[end - 1]) {
        ')' => {
            var depth: usize = 0;
            var quote: u8 = 0;
            var k = end;
            while (k > 0) {
                k -= 1;
                const ch = buf[k];
                if (quote != 0) {
                    if (ch == quote) quote = 0;
                    continue;
                }
                switch (ch) {
                    '\'', '"' => quote = ch,
                    ')' => depth += 1,
                    '(' => {
                        depth -= 1;
                        if (depth == 0) return nameBack(buf, k) orelse k;
                    },
                    else => {},
                }
            }
            return null;
        },
        '\'' => return quotedBack(buf, end),
        else => return nameBack(buf, end),
    }
}

/// Whether the sign just before `at` is a unary one: nothing, an operator or a keyword comes before it.
fn signIsUnary(buf: []const u8, at: usize) bool {
    const k = skipSpacesBack(buf, at);
    if (k == 0) return true;
    if (std.mem.indexOfScalar(u8, "(,*/%+-=<>|&^!", buf[k - 1]) != null) return true;
    var w = k;
    while (w > 0 and isNameByte(buf[w - 1])) w -= 1;
    return w < k and endsOperand(buf[w..k]);
}

/// Where the left operand of a `//` that follows `buf` starts: the chain of operands joined by `*`, `/` and `%` (and
/// the sign of its first), which `//` shares its level and left-to-right reading with. Null if there is none.
fn leftOperandStart(buf: []const u8) ?usize {
    var p = skipSpacesBack(buf, buf.len);
    while (true) {
        p = atomBack(buf, p) orelse return null;
        const k = skipSpacesBack(buf, p);
        if (k > 0 and (buf[k - 1] == '-' or buf[k - 1] == '+') and signIsUnary(buf, k - 1)) p = k - 1;
        const o = skipSpacesBack(buf, p);
        if (o == 0 or std.mem.indexOfScalar(u8, "*/%", buf[o - 1]) == null) return p;
        // An operator of the chain; a second slash before it is a `//` that was not rewritten.
        if (buf[o - 1] == '/' and o > 1 and buf[o - 2] == '/') return null;
        p = skipSpacesBack(buf, o - 1);
    }
}

/// The end of the quoted text that starts at `sql[at]`.
fn quotedForward(sql: []const u8, at: usize) ?usize {
    const q = sql[at];
    var k = at + 1;
    while (k < sql.len) : (k += 1) {
        if (sql[k] != q) continue;
        if (k + 1 < sql.len and sql[k + 1] == q) {
            k += 1;
            continue;
        }
        return k + 1;
    }
    return null;
}

/// The end of the group whose `(` is `sql[at]`.
fn groupForward(sql: []const u8, at: usize) ?usize {
    var depth: usize = 0;
    var k = at;
    while (k < sql.len) {
        switch (sql[k]) {
            '\'', '"' => k = quotedForward(sql, k) orelse return null,
            '(' => {
                depth += 1;
                k += 1;
            },
            ')' => {
                depth -= 1;
                k += 1;
                if (depth == 0) return k;
            },
            else => k += 1,
        }
    }
    return null;
}

/// The end of the right operand of a `//` whose operator ends at `from`: one optionally signed operand.
fn rightOperandEnd(sql: []const u8, from: usize) ?usize {
    var k = from;
    while (k < sql.len and std.ascii.isWhitespace(sql[k])) k += 1;
    if (k < sql.len and (sql[k] == '-' or sql[k] == '+')) {
        k += 1;
        while (k < sql.len and std.ascii.isWhitespace(sql[k])) k += 1;
    }
    if (k >= sql.len) return null;
    switch (sql[k]) {
        '(' => return groupForward(sql, k),
        '\'', '"' => {
            var e = quotedForward(sql, k) orelse return null;
            // Quoted segments joined by dots, then a call's arguments.
            while (e < sql.len and sql[e] == '.' and e + 1 < sql.len and sql[e + 1] == '"') e = quotedForward(sql, e + 1) orelse return null;
            return if (e < sql.len and sql[e] == '(') groupForward(sql, e) else e;
        },
        else => {
            const start = k;
            while (k < sql.len and (isNameByte(sql[k]) or sql[k] == '.')) k += 1;
            if (k == start or endsOperand(sql[start..k])) return null;
            // A signed exponent: `1e-3`.
            if (std.ascii.isDigit(sql[start]) and k + 1 < sql.len and (sql[k - 1] == 'e' or sql[k - 1] == 'E') and
                (sql[k] == '-' or sql[k] == '+') and std.ascii.isDigit(sql[k + 1]))
            {
                k += 1;
                while (k < sql.len and std.ascii.isDigit(sql[k])) k += 1;
            }
            return if (k < sql.len and sql[k] == '(') groupForward(sql, k) else k;
        },
    }
}

/// `sql` with `TRY_CAST(x AS type)` written as `CAST(x AS zpq_try_type)`: the grammar has no `TRY_CAST`, and the marker on the
/// type name is how the lowering knows the cast is lenient. Quoted text is untouched; a `TRY_CAST` with no `AS` of its own
/// stays in the text for the grammar to refuse.
fn tryCasts(allocator: std.mem.Allocator, sql: []const u8) ![:0]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            const end = quotedForward(sql, i) orelse sql.len;
            try out.appendSlice(allocator, sql[i..end]);
            i = end;
            continue;
        }
        const word = "try_cast";
        const boundary_before = i == 0 or !(std.ascii.isAlphanumeric(sql[i - 1]) or sql[i - 1] == '_' or sql[i - 1] == '$');
        if (boundary_before and i + word.len <= sql.len and std.ascii.eqlIgnoreCase(sql[i .. i + word.len], word)) {
            var open = i + word.len;
            while (open < sql.len and std.ascii.isWhitespace(sql[open])) open += 1;
            if (open < sql.len and sql[open] == '(') {
                if (castAs(sql, open)) |as_end| {
                    try out.appendSlice(allocator, "CAST");
                    // The operand is rewritten too: it may hold a `TRY_CAST` of its own.
                    const operand = try tryCasts(allocator, sql[i + word.len .. as_end]);
                    defer allocator.free(operand);
                    try out.appendSlice(allocator, operand);
                    try out.appendSlice(allocator, " zpq_try_");
                    i = as_end;
                    while (i < sql.len and std.ascii.isWhitespace(sql[i])) i += 1;
                    continue;
                }
            }
        }
        try out.append(allocator, ch);
        i += 1;
    }
    return out.toOwnedSliceSentinel(allocator, 0);
}

/// Where the last top-level `AS` of the call whose `(` is `sql[open]` ends, or null if the call has none or never closes.
fn castAs(sql: []const u8, open: usize) ?usize {
    var depth: usize = 0;
    var k = open;
    var found: ?usize = null;
    while (k < sql.len) {
        switch (sql[k]) {
            '\'', '"' => {
                k = quotedForward(sql, k) orelse return null;
                continue;
            },
            '(' => depth += 1,
            ')' => {
                depth -= 1;
                if (depth == 0) return found;
            },
            'a', 'A' => if (depth == 1 and k + 2 <= sql.len and std.ascii.eqlIgnoreCase(sql[k .. k + 2], "as") and
                k > open and !(std.ascii.isAlphanumeric(sql[k - 1]) or sql[k - 1] == '_') and
                (k + 2 == sql.len or !(std.ascii.isAlphanumeric(sql[k + 2]) or sql[k + 2] == '_')))
            {
                found = k + 2;
            },
            else => {},
        }
        k += 1;
    }
    return null;
}

test "TRY_CAST is written as a CAST to a marked type" {
    const T = std.testing;
    const cases = [_][2][]const u8{
        .{ "SELECT try_cast(x AS INT) FROM t", "SELECT CAST(x AS zpq_try_INT) FROM t" },
        .{ "SELECT TRY_CAST ( a + 1 AS DOUBLE PRECISION ) AS y", "SELECT CAST ( a + 1 AS zpq_try_DOUBLE PRECISION ) AS y" },
        .{ "SELECT try_cast(CAST(x AS INT) AS DECIMAL(10, 2))", "SELECT CAST(CAST(x AS INT) AS zpq_try_DECIMAL(10, 2))" },
        .{ "SELECT try_cast(try_cast(x AS INT) AS DOUBLE)", "SELECT CAST(CAST(x AS zpq_try_INT) AS zpq_try_DOUBLE)" },
        .{ "SELECT 'try_cast(x AS INT)', \"try_cast\" FROM t", "SELECT 'try_cast(x AS INT)', \"try_cast\" FROM t" },
        .{ "SELECT try_cast(x) FROM t", "SELECT try_cast(x) FROM t" },
        .{ "SELECT my_try_cast(x AS INT)", "SELECT my_try_cast(x AS INT)" },
    };
    for (cases) |pair| {
        const got = try tryCasts(T.allocator, pair[0]);
        defer T.allocator.free(got);
        try T.expectEqualStrings(pair[1], got);
    }
}

/// `sql` with DuckDB's integer division `a // b` written as the call `"//"(a, b)`, DuckDB's own name for it and a form
/// the grammar reads. `//` has the level of `*`, `/` and `%`, read left to right: its left operand is the chain of
/// operands before it back to a looser operator, its right one the single operand after it (a signed number, a name, a
/// string, a call or a group). A `//` whose operands are none of those stays in the text, and the grammar refuses it.
/// Quoted text is untouched; comments in a query that has a `//` become a space. Nesting is bounded.
fn integerDivision(allocator: std.mem.Allocator, sql: []const u8, depth: u32) ![:0]u8 {
    if (depth > 16) return error.SqlParseError;
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var found = false;
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            const end = quotedForward(sql, i) orelse sql.len;
            try out.appendSlice(allocator, sql[i..end]);
            i = end;
        } else if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') i += 1;
            try out.append(allocator, ' ');
        } else if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            const end = std.mem.indexOfPos(u8, sql, i + 2, "*/") orelse sql.len - 2;
            i = end + 2;
            try out.append(allocator, ' ');
        } else if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '/') {
            const left = leftOperandStart(out.items);
            const right = rightOperandEnd(sql, i + 2);
            if (left == null or right == null) {
                try out.appendSlice(allocator, "//");
                i += 2;
                continue;
            }
            found = true;
            const lhs = try allocator.dupe(u8, std.mem.trim(u8, out.items[left.?..], " \t\r\n"));
            defer allocator.free(lhs);
            out.shrinkRetainingCapacity(left.?);
            const rhs = try integerDivision(allocator, std.mem.trim(u8, sql[i + 2 .. right.?], " \t\r\n"), depth + 1);
            defer allocator.free(rhs);
            try out.appendSlice(allocator, "\"//\"(");
            try out.appendSlice(allocator, lhs);
            try out.appendSlice(allocator, ", ");
            try out.appendSlice(allocator, rhs);
            try out.append(allocator, ')');
            i = right.?;
        } else {
            try out.append(allocator, ch);
            i += 1;
        }
    }
    // Only a query with a `//` is rewritten: any other keeps its text, so its diagnostics keep their positions.
    if (!found) {
        out.clearRetainingCapacity();
        try out.appendSlice(allocator, sql);
    }
    return out.toOwnedSliceSentinel(allocator, 0);
}

test "integer division is rewritten to the call DuckDB names it by, operands and levels as read" {
    const T = std.testing;
    const cases = [_][2][]const u8{
        .{ "SELECT sum(a // 7) FROM t", "SELECT sum(\"//\"(a, 7)) FROM t" },
        .{ "SELECT a // b // c FROM t", "SELECT \"//\"(\"//\"(a, b), c) FROM t" },
        .{ "SELECT x * y // 3 FROM t", "SELECT \"//\"(x * y, 3) FROM t" },
        .{ "SELECT 1 + x // 2 FROM t", "SELECT 1 + \"//\"(x, 2) FROM t" },
        .{ "SELECT a - 7 // 2 FROM t", "SELECT a - \"//\"(7, 2) FROM t" },
        .{ "SELECT -7 // 2 FROM t", "SELECT \"//\"(-7, 2) FROM t" },
        .{ "SELECT x // -2 FROM t", "SELECT \"//\"(x, -2) FROM t" },
        .{ "SELECT f(x, 1) // g(2) FROM t", "SELECT \"//\"(f(x, 1), g(2)) FROM t" },
        .{ "SELECT (a + b) // (c - d) FROM t", "SELECT \"//\"((a + b), (c - d)) FROM t" },
        .{ "SELECT a // (b // c) FROM t", "SELECT \"//\"(a, (\"//\"(b, c))) FROM t" },
        .{ "SELECT a%3 //2 FROM t", "SELECT \"//\"(a%3, 2) FROM t" },
        .{ "SELECT a // 2 * 3 FROM t", "SELECT \"//\"(a, 2) * 3 FROM t" },
        .{ "SELECT \"my col\" // 2, t.x // 1e-3 FROM t", "SELECT \"//\"(\"my col\", 2), \"//\"(t.x, 1e-3) FROM t" },
        .{ "SELECT a // 2 AS q -- c // d\nFROM t", "SELECT \"//\"(a, 2) AS q  \nFROM t" },
    };
    for (cases) |pair| {
        const got = try integerDivision(T.allocator, pair[0], 0);
        defer T.allocator.free(got);
        try T.expectEqualStrings(pair[1], got);
    }
    // Untouched: no operator outside quotes or comments, and operands this cannot delimit.
    for ([_][]const u8{
        "SELECT '// not' FROM 's3://b/k.parquet'",
        "SELECT a /* x // y */ FROM t",
        "SELECT a // FROM t",
        "SELECT // 2 FROM t",
        "SELECT CASE WHEN a THEN 1 END // 2 FROM t",
        "SELECT a // CASE WHEN a THEN 1 END FROM t",
    }) |sql| {
        const got = try integerDivision(T.allocator, sql, 0);
        defer T.allocator.free(got);
        try T.expectEqualStrings(sql, got);
    }
}

/// `sql` with the keyword form of `substring` written as the call DuckDB reads it as: `substring(x FROM a FOR b)` is
/// `substring(x, a, b)`, `substring(x FROM a)` is `substring(x, a)` and `substring(x FOR b)` is `substring(x, 1, b)`.
/// Only a keyword that sits directly in the call's own parentheses counts (not one in a nested call or subquery) and
/// quoted text is left as it is. One pass with its own stack, so a deeply nested query costs no recursion.
fn substringKeywords(allocator: std.mem.Allocator, sql: []const u8) ![:0]u8 {
    const Frame = struct {
        /// This paren opens a `substring(` call.
        call: bool,
        /// A comma or a keyword was already written in it: the call is a plain argument list, or has its FROM.
        comma: bool = false,
        from: bool = false,
        /// A keyword was rewritten, so a comma after it makes a form DuckDB refuses.
        rewrote: bool = false,
    };
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var frames: std.ArrayList(Frame) = .empty;
    defer frames.deinit(allocator);
    const isWord = struct {
        fn at(ch: u8) bool {
            return std.ascii.isAlphanumeric(ch) or ch == '_';
        }
    }.at;
    var i: usize = 0;
    var last_word: []const u8 = "";
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            var j = i + 1;
            while (j < sql.len) : (j += 1) if (sql[j] == ch) {
                if (j + 1 < sql.len and sql[j + 1] == ch) j += 1 else break;
            };
            const stop = @min(j + 1, sql.len);
            try out.appendSlice(allocator, sql[i..stop]);
            i = stop;
            last_word = "";
            continue;
        }
        if (isWord(ch)) {
            var j = i;
            while (j < sql.len and isWord(sql[j])) j += 1;
            const text = sql[i..j];
            const top: ?*Frame = if (frames.items.len == 0) null else blk: {
                const innermost = &frames.items[frames.items.len - 1];
                break :blk if (innermost.call) innermost else null;
            };
            if (top) |f| if (!f.comma and std.ascii.eqlIgnoreCase(text, "from") and !f.from) {
                f.from = true;
                f.rewrote = true;
                try out.appendSlice(allocator, ",");
                i = j;
                last_word = "";
                continue;
            } else if (!f.comma and std.ascii.eqlIgnoreCase(text, "for") and !f.rewrote) {
                // No FROM before it: the start is 1.
                f.rewrote = true;
                try out.appendSlice(allocator, ", 1,");
                i = j;
                last_word = "";
                continue;
            } else if (!f.comma and std.ascii.eqlIgnoreCase(text, "for") and f.from) {
                try out.appendSlice(allocator, ",");
                f.rewrote = true;
                i = j;
                last_word = "";
                continue;
            };
            try out.appendSlice(allocator, text);
            last_word = text;
            i = j;
            continue;
        }
        switch (ch) {
            '(' => {
                try frames.append(allocator, .{ .call = std.ascii.eqlIgnoreCase(last_word, "substring") });
                last_word = "";
            },
            ')' => {
                _ = frames.pop();
                last_word = "";
            },
            ',' => {
                if (frames.items.len > 0 and frames.items[frames.items.len - 1].call) {
                    const f = &frames.items[frames.items.len - 1];
                    // A comma after a rewritten keyword is a form DuckDB refuses: leave a syntax error behind.
                    if (f.rewrote) try out.append(allocator, ',');
                    f.comma = true;
                }
                last_word = "";
            },
            ' ', '\t', '\n', '\r' => {},
            else => last_word = "",
        }
        try out.append(allocator, ch);
        i += 1;
    }
    return out.toOwnedSliceSentinel(allocator, 0);
}

const QueryShape = struct { is_aggregate: bool, has_group_by: bool };

fn classifyQuery(
    allocator: std.mem.Allocator,
    root: *c.LpNode,
    lp_arena: *c.arena_t,
) !QueryShape {
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

    return .{ .is_aggregate = is_aggregate, .has_group_by = has_group_by };
}

pub fn parseSqlQuery(allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    return parseSql(false, allocator, sql);
}

/// Production SQL carries owned syntax to the schema binder; legacy fragment strings remain a compatibility API.
/// What the frontend asks of the files a join names: their footers, read however the caller reads files.
pub const Schemas = struct {
    ctx: *const anyopaque,
    footer: *const fn (ctx: *const anyopaque, path: []const u8) anyerror!*const schema.FileMetaData,
};

/// A two-table N:1 join (`sql_join.zig`): the probe side's single-table query, with the resolved join on its syntax
/// (`syntax.Query.join`). Null when `sql` is no join. `allocator` must be an arena: the probe's and every build's
/// syntax outlive the call. The footers decide only which table owns a column and which side probes.
pub fn parseSqlJoinPlan(allocator: std.mem.Allocator, sql: []const u8, schemas: Schemas) !?ParsedQuery {
    // The binder reads the query as the single-table parser does, once folded; its pieces parse with the same markers.
    var report: literals.Report = .{};
    const folded = try fold(allocator, sql, &report);
    const markers = report.markers;
    const z = try allocator.dupeSentinel(u8, folded, 0);
    const paths = (try sql_join.tables(allocator, z)) orelse return null;
    const first = try resolveJoin(allocator, z, paths, schemas, markers, false);
    // A probe query that still holds `IN (SELECT ...)` semi-joins the probe itself.
    const peeled = try peelSemis(allocator, first.probe_sql, schemas, markers, first.reduced);
    var pq = try parseFolded(true, allocator, peeled.rest, markers);
    const q = try allocator.create(syntax.Query);
    q.* = pq.syntax.?.*;
    q.join = .{ .kind = first.kind, .fk = first.fk, .build = first.build, .semis = peeled.semis, .dims = first.dims };
    pq.syntax = q;
    return pq;
}

const Resolved = struct {
    kind: syntax.JoinKind,
    fk: []const u8,
    build: syntax.Build,
    dims: []const syntax.Dim,
    probe_sql: []const u8,
    /// Its INs are reductions' (`sql_join.Plan.reduced`): their builds' keys must be unique.
    reduced: bool,
};

const Owners = struct {
    metas: []const *const schema.FileMetaData,
    fn has(raw: *const anyopaque, table: usize, name: []const u8) bool {
        const self: *const Owners = @ptrCast(@alignCast(raw));
        _ = metadata.resolveColumn(self.metas[table], name) catch return false;
        return true;
    }
};

/// `unique_ins`: the query this one came out of reduced, so every IN in it is a reduction's too.
fn resolveJoin(a: std.mem.Allocator, sql: [:0]const u8, paths: []const []const u8, schemas: Schemas, markers: literals.Markers, unique_ins: bool) anyerror!Resolved {
    const metas = try a.alloc(*const schema.FileMetaData, paths.len);
    const rows = try a.alloc(u64, paths.len);
    for (paths, metas, rows) |path, *m, *r| {
        m.* = try schemas.footer(schemas.ctx, path);
        r.* = @intCast(m.*.num_rows);
    }
    const owners: Owners = .{ .metas = metas };
    const p = try sql_join.plan(a, sql, .{ .paths = paths, .has = Owners.has, .rows = rows, .ctx = &owners });
    const unique = unique_ins or p.reduced;
    const dims = try a.alloc(syntax.Dim, p.dims.len);
    for (p.dims, dims) |d, *out| out.* = .{
        .fk = d.fk,
        .build = try resolveBuild(a, d.path, d.key, d.columns, d.where, schemas, markers, unique),
        .left = d.left,
    };
    return .{
        .kind = switch (p.kind) {
            inline else => |k| @field(syntax.JoinKind, @tagName(k)),
        },
        .fk = p.fk,
        .build = try resolveBuild(a, p.build_path, p.build_key, p.build_columns, p.build_where, schemas, markers, unique),
        .dims = dims,
        .probe_sql = p.probe_sql,
        .reduced = unique,
    };
}

/// One build: its own conjuncts, and the SEMI joins the binder reduced onto it (tables of a chain).
fn resolveBuild(a: std.mem.Allocator, path: []const u8, key: []const u8, columns: []const []const u8, where: ?[]const u8, schemas: Schemas, markers: literals.Markers, unique: bool) !syntax.Build {
    var build: syntax.Build = .{ .path = path, .key = key, .columns = columns };
    const w = where orelse return build;
    const text = try std.fmt.allocPrint(a, "SELECT count(*) FROM '{s}' WHERE {s}", .{ path, w });
    const peeled = try peelSemis(a, text, schemas, markers, unique);
    build.semis = peeled.semis;
    build.filter = (try parseFolded(true, a, peeled.rest, markers)).syntax.?.filter;
    return build;
}

/// Every `IN (SELECT ...)` and `NOT IN (SELECT ...)` of `sql`, as SEMI and ANTI joins of its table, and the query left
/// once they are out.
fn peelSemis(a: std.mem.Allocator, sql: []const u8, schemas: Schemas, markers: literals.Markers, unique: bool) !struct { semis: []const syntax.Semi, rest: []const u8 } {
    var rest = sql;
    var semis: std.ArrayList(syntax.Semi) = .empty;
    while (true) {
        const z = try a.dupeSentinel(u8, rest, 0);
        const inner = (try sql_join.tables(a, z)) orelse break;
        const r = try resolveJoin(a, z, inner, schemas, markers, unique);
        const anti = r.kind == .anti;
        try semis.append(a, .{ .fk = r.fk, .build = r.build, .unique = unique and !anti, .anti = anti });
        rest = r.probe_sql;
    }
    return .{ .semis = semis.items, .rest = rest };
}

/// `UPDATE 'file' SET col = literal [, ...] WHERE predicate`, read into what the engine's UPDATE binds: the WHERE
/// and each assignment as filter text, lowered the way a SELECT's WHERE is (`col = literal` binds the literal to the
/// column's type exactly as a comparison does). Null for any other statement, which the SELECT parser then reads.
pub const ParsedUpdate = struct {
    table: []const u8,
    where: syntax.Predicate,
    sets: []const SetClause,
};

pub const SetClause = struct {
    column: []const u8,
    /// `column = literal`; null for NULL.
    assignment: ?syntax.Predicate,
};

/// `allocator` must be an arena: everything returned lives on it.
pub fn parseSqlUpdate(allocator: std.mem.Allocator, sql: []const u8) !?ParsedUpdate {
    var report: literals.Report = .{};
    const folded = try fold(allocator, sql, &report);
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);
    var error_msg: [*c]const u8 = null;
    const z = try allocator.dupeSentinel(u8, folded, 0);
    const stmts = c.lp_parse_all(z, lp_arena, &error_msg);
    if (stmts == null or stmts.*.count != 1) return null;
    const root = stmts.*.items[0] orelse return null;
    if (root.*.kind != c.LP_STMT_UPDATE) return null;
    const u = root.*.u.update;
    if (u.from != null or u.order_by.count > 0 or u.limit != null or u.returning.count > 0 or u.or_conflict != 0) {
        diagnosticPrint("sql: UPDATE takes SET and WHERE only (no FROM, ORDER BY, LIMIT, RETURNING or OR ...)\n", .{});
        return error.UnsupportedStatement;
    }
    if (u.table == null) return error.MissingTableName;
    boolLiterals(root);
    const where = u.where orelse {
        diagnosticPrint("sql: UPDATE needs a WHERE clause; to rewrite a whole column, use a SELECT written with -o\n", .{});
        return error.UpdateWithoutWhere;
    };
    const where_c = c.lp_ast_to_sql(where, lp_arena);
    if (where_c == null) return error.UnparseError;
    const sets = try allocator.alloc(SetClause, @intCast(u.set_clauses.count));
    for (sets, 0..) |*set, i| {
        const node = u.set_clauses.items[i] orelse return error.SqlParseError;
        const sc = node.*.u.set_clause;
        if (sc.column == null) {
            diagnosticPrint("sql: UPDATE sets one column per assignment\n", .{});
            return error.UnsupportedStatement;
        }
        const expr = sc.expr orelse return error.SqlParseError;
        const column = try allocator.dupe(u8, std.mem.span(sc.column));
        if (expr.*.kind == c.LP_EXPR_LITERAL_NULL) {
            set.* = .{ .column = column, .assignment = null };
            continue;
        }
        const expr_c = c.lp_ast_to_sql(expr, lp_arena);
        if (expr_c == null) return error.UnparseError;
        const text = try std.fmt.allocPrint(allocator, "{s} = {s}", .{ column, std.mem.span(expr_c) });
        set.* = .{ .column = column, .assignment = try updatePredicate(allocator, text, report.markers) };
    }
    return .{
        .table = try allocator.dupe(u8, stripQuotes(std.mem.span(u.table))),
        .where = try updatePredicate(allocator, std.mem.span(where_c), report.markers),
        .sets = sets,
    };
}

/// A predicate of an UPDATE as filter text: read as the WHERE of a SELECT, under that SELECT's rules.
fn updatePredicate(allocator: std.mem.Allocator, predicate: []const u8, markers: literals.Markers) !syntax.Predicate {
    const select = try std.fmt.allocPrint(allocator, "SELECT * FROM 'update_target' WHERE {s}", .{predicate});
    const pq = try parseFolded(true, allocator, select, markers);
    return pq.syntax.?.filter orelse error.SqlParseError;
}

pub fn parseSqlPlan(allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    return parseSql(true, allocator, sql);
}

fn parseSql(comptime structured: bool, allocator: std.mem.Allocator, sql: []const u8) !ParsedQuery {
    var literal_report: literals.Report = .{};
    const folded_sql = try fold(allocator, sql, &literal_report);
    defer allocator.free(folded_sql);
    return parseFolded(structured, allocator, folded_sql, literal_report.markers);
}

/// `sql` with each comment written over by spaces (newlines kept, so every position stays where it was). DuckDB's block
/// comments nest (`/* a /* b */ c */` is one), which the grammar's own lexer and the rewrites before it do not read:
/// each would end the comment at the first `*/` and run what is left of it as SQL. Quoted text is untouched, and a block
/// comment that is never closed is an error, as DuckDB's.
fn stripComments(allocator: std.mem.Allocator, sql: []const u8) ![]u8 {
    var out = try allocator.dupe(u8, sql);
    errdefer allocator.free(out);
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            i = quotedForward(sql, i) orelse sql.len;
        } else if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            while (i < sql.len and sql[i] != '\n') : (i += 1) out[i] = ' ';
        } else if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            var depth: usize = 1;
            var k = i + 2;
            while (k < sql.len and depth > 0) {
                if (sql[k] == '/' and k + 1 < sql.len and sql[k + 1] == '*') {
                    depth += 1;
                    k += 2;
                } else if (sql[k] == '*' and k + 1 < sql.len and sql[k + 1] == '/') {
                    depth -= 1;
                    k += 2;
                } else k += 1;
            }
            if (depth > 0) {
                diagnosticPrint("sql parse error: unterminated /* comment\n", .{});
                return error.SqlParseError;
            }
            for (out[i..k]) |*b| {
                if (b.* != '\n') b.* = ' ';
            }
            i = k;
        } else i += 1;
    }
    return out;
}

test "comments are written over by spaces, nested block comments whole, and an open one is an error" {
    const T = std.testing;
    // Each comment is as many spaces as it was long: the query with `blank` spaces where the comment was.
    const spaces: [64]u8 = @splat(' ');
    const cases = [_]struct { sql: []const u8, head: []const u8, blank: usize, tail: []const u8 }{
        .{ .sql = "SELECT a /* x */ FROM t", .head = "SELECT a ", .blank = 7, .tail = " FROM t" },
        .{ .sql = "SELECT a /* o /* i */ + 999 -- */ AS r FROM t", .head = "SELECT a ", .blank = 24, .tail = " AS r FROM t" },
        .{ .sql = "SELECT a -- c /* d\nFROM t", .head = "SELECT a ", .blank = 9, .tail = "\nFROM t" },
        .{ .sql = "SELECT '/* x */', \"--y\" FROM t", .head = "SELECT '/* x */', \"--y\" FROM t", .blank = 0, .tail = "" },
        .{ .sql = "SELECT a /*/ b */ + 1 FROM t", .head = "SELECT a ", .blank = 8, .tail = " + 1 FROM t" },
    };
    for (cases) |case| {
        const got = try stripComments(T.allocator, case.sql);
        defer T.allocator.free(got);
        const want = try std.fmt.allocPrint(T.allocator, "{s}{s}{s}", .{ case.head, spaces[0..case.blank], case.tail });
        defer T.allocator.free(want);
        try T.expectEqualStrings(want, got);
    }
    for ([_][]const u8{ "SELECT a /* x FROM t", "SELECT a /* o /* i */ FROM t" }) |sql| {
        try T.expectError(error.SqlParseError, stripComments(T.allocator, sql));
    }
}

/// `sql` with its typed literals and sugar folded (`sql_literals.rewrite`); `report.markers` say how to read it back.
fn fold(allocator: std.mem.Allocator, raw: []const u8, report: *literals.Report) ![]u8 {
    const sql = try stripComments(allocator, raw);
    defer allocator.free(sql);
    return literals.rewrite(allocator, sql, report) catch |err| {
        if (report.len > 0) diagnosticPrint("sql: {s}\n", .{report.text()});
        return err;
    };
}

/// A query `fold` rewrote, read with the markers it chose: a join's probe and build queries are its pieces.
fn parseFolded(comptime structured: bool, allocator: std.mem.Allocator, folded_sql: []const u8, markers: literals.Markers) !ParsedQuery {
    const lp_arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    defer c.arena_destroy(lp_arena);

    var error_msg: [*c]const u8 = null;
    const extracted = try extractAsDatePart(allocator, folded_sql);
    defer allocator.free(extracted);
    const keyworded = try substringKeywords(allocator, extracted);
    defer allocator.free(keyworded);
    const casted = try tryCasts(allocator, keyworded);
    defer allocator.free(casted);
    const sql_c = try integerDivision(allocator, casted, 0);
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
    try distinctAsGroupBy(root, lp_arena);
    try groupByAll(root, lp_arena, markers.groupAll());
    // Reject clauses we'd otherwise silently drop → wrong answers.
    try rejectUnsupportedClauses(root.*.u.select);
    boolLiterals(root);
    quoteWholePaths(root);
    try pushNotsDown(root, lp_arena);
    // Only the structured plan carries WHERE's comparisons of two expressions; the fragment strings would read a
    // value-side column as literal text.
    try rejectColumnComparisons(root, structured);
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

    const syntax_owner: ?*std.heap.ArenaAllocator = if (structured) try allocator.create(std.heap.ArenaAllocator) else null;
    if (syntax_owner) |owner| owner.* = .init(allocator);
    errdefer if (syntax_owner) |owner| {
        owner.deinit();
        allocator.destroy(owner);
    };
    var lower: Lower = .{
        .arena = if (syntax_owner) |owner| owner.allocator() else undefined,
        .lp_arena = lp_arena,
        .ilike_escape = markers.ilikeEscape(),
    };
    const lowering = if (structured) &lower else null;
    const shape = try classifyQuery(allocator, root, lp_arena);
    if (shape.has_group_by) try orderTermsAsKeys(allocator, root, lp_arena);
    const is_aggregate = shape.is_aggregate;
    const has_group_by = shape.has_group_by;
    var i: usize = 0;

    // Unparse result columns
    var columns_list: std.ArrayList([]const u8) = .empty;
    defer {
        for (columns_list.items) |col| allocator.free(col);
        columns_list.deinit(allocator);
    }
    i = 0;
    while (!structured and i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
        const rc = root.*.u.select.result_columns.items[i] orelse continue;
        const col_sql = try unparseResultColumn(rc, lp_arena, allocator);
        try columns_list.append(allocator, col_sql);
    }

    const select_cols = try allocator.alloc(ast.SelectColumn, @intCast(root.*.u.select.result_columns.count));
    @memset(select_cols, .{ .expr = "" });
    errdefer freeSelectCols(allocator, select_cols);
    i = 0;
    var col_idx: usize = 0;
    while (i < @as(usize, @intCast(root.*.u.select.result_columns.count))) : (i += 1) {
        const rc = root.*.u.select.result_columns.items[i] orelse continue;
        try selectColumn(rc, lp_arena, allocator, &select_cols[col_idx]);
        const has_agg = analyzeExpr(rc).has_agg;
        // An expression over aggregates is computed after the groups are complete, not by the scan.
        const derived = has_agg and !isAggregateCall(selectExpr(root.*.u.select, i) orelse return error.SqlParseError);
        if (derived) {
            select_cols[col_idx].role = .derived;
        } else if (has_group_by) select_cols[col_idx].role = if (has_agg) .agg else .key;
        col_idx += 1;
    }
    var hidden: std.ArrayList(ast.SelectColumn) = .empty;
    defer {
        freeSelectItems(allocator, hidden.items);
        hidden.deinit(allocator);
    }
    const sel = root.*.u.select;
    var post: PostArena = .{ .allocator = allocator };
    errdefer post.deinit();
    // The aggregates a derived column reads come first among the hidden columns.
    var any_derived = false;
    for (select_cols, 0..) |*col, idx| if (col.role == .derived) {
        any_derived = true;
        var builder: PostBuilder = .{
            .allocator = allocator,
            .arena = try post.get(),
            .lp_arena = lp_arena,
            .n_select = select_cols.len,
            .hidden = &hidden,
            .lower = lowering,
        };
        col.post = try builder.build(selectExpr(sel, idx) orelse return error.SqlParseError, 0);
    };
    var result_order = try resultOrder(allocator, sel, lp_arena, select_cols, is_aggregate, has_group_by, &hidden, lowering, &post);
    errdefer freeResultOrder(allocator, result_order);
    if (sel.having) |having| {
        if (!is_aggregate) {
            diagnosticPrint("sql: HAVING needs an aggregate or GROUP BY\n", .{});
            return error.HavingNotSupported;
        }
        const arena = try post.get();
        var ctx: HavingBuilder(@TypeOf(sel)) = .{
            .allocator = allocator,
            .arena = arena,
            .sel = sel,
            .lp_arena = lp_arena,
            .select_cols = select_cols,
            .has_group_by = has_group_by,
            .hidden = &hidden,
            .lower = lowering,
            .post = &post,
        };
        const tree = try arena.create(ast.Having);
        tree.* = try ctx.build(having, 0);
        result_order.having = tree;
    }
    for (hidden.items) |h| any_derived = any_derived or h.role == .derived;
    result_order.post = any_derived;
    result_order.hidden = try hidden.toOwnedSlice(allocator);

    if (structured) {
        const owned = try lower.arena.create(syntax.Query);
        owned.* = try lowerQuery(&lower, root, select_cols, result_order, is_aggregate, has_group_by);
        return .{
            .syntax = owned,
            .syntax_arena = syntax_owner,
            .table_name = table_name,
            .is_aggregate = is_aggregate,
            .select_str = null,
            .aggregate_str = if (is_aggregate) "" else null,
            .filter_str = null,
            .group_by_str = if (has_group_by) "" else null,
            .select_cols = select_cols,
            .result_order = result_order,
            .having_arena = post.owner,
        };
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
            if (!info.has_agg or select_cols[i].role == .derived) continue;
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
        // Without GROUP BY every SELECT item is an aggregate or derived from them; the derived ones are the result
        // stage's, so the scan gets the aggregates, then the hidden ones that HAVING and derived columns read.
        var joined: std.ArrayList(u8) = .empty;
        errdefer joined.deinit(allocator);
        for (columns_list.items, 0..) |col, idx| {
            if (select_cols[idx].role == .derived) continue;
            if (joined.items.len > 0) try joined.appendSlice(allocator, ", ");
            try joined.appendSlice(allocator, col);
        }
        for (result_order.hidden, 0..) |h, n| if (h.role == .agg) {
            if (joined.items.len > 0) try joined.appendSlice(allocator, ", ");
            try joined.print(allocator, "{s} AS {s}{d}", .{ h.expr, hidden_alias, n });
        };
        aggregate_str = try joined.toOwnedSlice(allocator);
        allocator.free(columns_str);
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
            .having_arena = post.owner,
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
            .having_arena = post.owner,
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
        .having_arena = post.owner,
    };
}

fn lowerQuery(lower: *Lower, root: *const c.LpNode, cols: []const ast.SelectColumn, order: ast.ResultOrder, aggregate: bool, grouped: bool) !syntax.Query {
    const a = lower.arena;
    const sel = root.u.select;
    const columns = try a.alloc(syntax.Column, cols.len);
    var calls: std.ArrayList(syntax.Aggregate) = .empty;
    var selected: std.ArrayList(syntax.SelectItem) = .empty;
    var star = false;
    for (cols, columns, 0..) |col, *out, i| {
        const node = selectExpr(sel, i) orelse return error.SqlParseError;
        out.* = .{ .display = try a.dupe(u8, col.expr), .alias = if (col.alias) |alias| try a.dupe(u8, alias) else null, .role = col.role, .post = col.post };
        if (col.role == .derived) {
            // Computed from the aggregates it reads, which are hidden columns: the scan has nothing for it.
        } else if (aggregate and (!grouped or col.role == .agg)) {
            try calls.append(a, try lower.aggregate(node, col.alias, col.expr));
        } else if (node.kind == c.LP_EXPR_STAR) {
            star = true;
        } else {
            out.expr = try lower.expression(node, 0);
            if (!aggregate) try selected.append(a, .{ .expr = out.expr.?, .alias = out.alias });
        }
    }
    const hidden = try a.alloc(syntax.Column, order.hidden.len);
    for (order.hidden, hidden, lower.hidden.items, 0..) |col, *out, node, i| {
        out.* = .{ .display = try a.dupe(u8, col.expr), .role = col.role, .post = col.post };
        if (col.role == .agg) {
            const alias = try std.fmt.allocPrint(a, "{s}{d}", .{ hidden_alias, i });
            try calls.append(a, try lower.aggregate(node, alias, col.expr));
        } else if (col.role == .key) out.expr = try lower.expression(node, 0);
    }
    const keys = if (grouped) try a.alloc(syntax.SelectItem, @intCast(sel.group_by.count)) else null;
    if (keys) |items| for (items, 0..) |*out, i| {
        out.* = .{ .expr = try lower.expression(sel.group_by.items[i] orelse return error.SqlParseError, 0) };
    };
    return .{
        .aggregate = aggregate,
        .calls = calls.items,
        .keys = keys,
        .filter = if (sel.where) |node| blk: {
            lower.compare_expressions = true;
            defer lower.compare_expressions = false;
            break :blk try lower.predicate(node, 0);
        } else null,
        .select = if (aggregate or star) null else selected.items,
        .columns = if (star) &.{} else columns,
        .hidden = hidden,
        .row_keys = lower.rows.items,
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
    lower: ?*Lower,
    post: *PostArena,
) !ast.ResultOrder {
    const n_terms: usize = @intCast(sel.order_by.count);
    var order: ast.ResultOrder = .{};
    if (sel.limit) |lim| {
        order.limit = try nonNegativeInt(lim.*.u.limit.count orelse return error.LimitNotSupported);
        if (lim.*.u.limit.offset) |off| order.offset = try nonNegativeInt(off);
    }
    if (!is_aggregate) {
        if (n_terms > 0) order.rows = try rowOrder(allocator, sel, lp_arena, select_cols, lower);
        return order;
    }

    const keys = try allocator.alloc(ast.OrderKey, n_terms);
    errdefer allocator.free(keys);
    for (keys, 0..) |*key, t| {
        const term = sel.order_by.items[t] orelse return error.SqlParseError;
        // The grammar spells NULLS FIRST as LP_SORT_ASC, NULLS LAST as LP_SORT_DESC, neither as LP_SORT_UNDEFINED.
        // NULLs sort last by default, in either direction, so only NULLS FIRST changes the order.
        const expr = term.*.u.order_term.expr orelse return error.SqlParseError;
        const column = try orderColumn(sel, lp_arena, select_cols, expr) orelse
            (select_cols.len + try hiddenTerm(allocator, sel, lp_arena, expr, has_group_by, .order_by, hidden, lower, select_cols.len, post));
        key.* = .{
            .column = column,
            .descending = term.*.u.order_term.direction == c.LP_SORT_DESC,
            .nulls_first = term.*.u.order_term.nulls == c.LP_SORT_ASC,
        };
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
    lower: ?*Lower,
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
        if (lower) |l| try l.rows.append(l.arena, .{
            .expr = try l.expression(expr, 0),
            .display = try l.arena.dupe(u8, std.mem.span(text_c)),
            .descending = term.*.u.order_term.direction == c.LP_SORT_DESC,
            .nulls_first = term.*.u.order_term.nulls == c.LP_SORT_ASC,
        });
        key.* = .{
            .expr = try allocator.dupe(u8, std.mem.span(text_c)),
            .descending = term.*.u.order_term.direction == c.LP_SORT_DESC,
            .nulls_first = term.*.u.order_term.nulls == c.LP_SORT_ASC,
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

/// The arena the post-aggregate expressions (and HAVING) of one query are built in; created when the first is needed.
/// `ParsedQuery.having_arena` takes it over.
const PostArena = struct {
    allocator: std.mem.Allocator,
    owner: ?*std.heap.ArenaAllocator = null,

    fn get(self: *PostArena) !std.mem.Allocator {
        if (self.owner == null) {
            const created = try self.allocator.create(std.heap.ArenaAllocator);
            created.* = .init(self.allocator);
            self.owner = created;
        }
        return self.owner.?.allocator();
    }

    fn deinit(self: *PostArena) void {
        if (self.owner) |owner| {
            owner.deinit();
            self.allocator.destroy(owner);
        }
        self.owner = null;
    }
};

/// Whether `node` is itself one aggregate call (`sum(x)`, `count(*)`), which the scan computes as it is: not an
/// expression over aggregates, which `PostBuilder` turns into a derived column.
fn isAggregateCall(node: *const c.LpNode) bool {
    if (node.kind != c.LP_EXPR_FUNCTION) return false;
    const name = std.mem.span(node.u.function.name orelse return false);
    return parser.isSqlAggName(name);
}

/// The names `unsupported` gives a post-aggregate expression: what stays refused, by name.
fn postUnsupported(lp_arena: *c.arena_t, node: *c.LpNode, what: []const u8) error{ UnparseError, PostAggNotSupported } {
    const text = c.lp_ast_to_sql(node, lp_arena);
    if (text == null) return error.UnparseError;
    diagnosticPrint("sql: {s} {s} over aggregates is not supported yet\n", .{ what, std.mem.span(text) });
    return error.PostAggNotSupported;
}

/// Builds the `ast.PostExpr` of an expression over aggregates, which the result stage evaluates once per group:
/// arithmetic (`+ - * /`) and `round` over aggregate calls and number literals. Each aggregate call it reads is
/// added to `hidden` (once, as written), so the scan computes it; the expression names it by result row column.
const PostBuilder = struct {
    allocator: std.mem.Allocator,
    arena: std.mem.Allocator,
    lp_arena: *c.arena_t,
    n_select: usize,
    hidden: *std.ArrayList(ast.SelectColumn),
    lower: ?*Lower,

    const Error = error{ OutOfMemory, UnparseError, PostAggNotSupported, FilterTooDeep };
    const max_depth = @import("../filter/parser.zig").MAX_GROUP_DEPTH;

    fn build(self: *PostBuilder, node: *c.LpNode, depth: u32) Error!*const ast.PostExpr {
        if (depth > max_depth) return error.FilterTooDeep;
        const out = try self.arena.create(ast.PostExpr);
        out.* = switch (node.kind) {
            c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT => .{
                .number = try self.arena.dupe(u8, std.mem.span(node.u.literal.value)),
            },
            c.LP_EXPR_UNARY_OP => blk: {
                const operand = node.u.unary.operand orelse return postUnsupported(self.lp_arena, node, "the expression");
                switch (node.u.unary.op) {
                    c.LP_UOP_MINUS => break :blk .{ .neg = try self.build(operand, depth + 1) },
                    c.LP_UOP_PLUS => return self.build(operand, depth + 1),
                    else => return postUnsupported(self.lp_arena, node, "the operator in"),
                }
            },
            c.LP_EXPR_BINARY_OP => blk: {
                const op: ast.PostOp = switch (node.u.binary.op) {
                    c.LP_OP_ADD => .add,
                    c.LP_OP_SUB => .sub,
                    c.LP_OP_MUL => .mul,
                    c.LP_OP_DIV => .div,
                    else => return postUnsupported(self.lp_arena, node, "the operator in"),
                };
                const left = node.u.binary.left orelse return postUnsupported(self.lp_arena, node, "the expression");
                const right = node.u.binary.right orelse return postUnsupported(self.lp_arena, node, "the expression");
                break :blk .{ .binary = .{
                    .op = op,
                    .left = try self.build(left, depth + 1),
                    .right = try self.build(right, depth + 1),
                } };
            },
            c.LP_EXPR_FUNCTION => try self.call(node, depth),
            else => return postUnsupported(self.lp_arena, node, "the expression"),
        };
        return out;
    }

    fn call(self: *PostBuilder, node: *c.LpNode, depth: u32) Error!ast.PostExpr {
        const f = node.u.function;
        const name = std.mem.span(f.name orelse return postUnsupported(self.lp_arena, node, "the call"));
        if (isAggregateCall(node)) {
            // An AVG in an expression is DOUBLE, computed from the exact sum and count: the call is read as
            // `avg_state`, whose pair has both, not as `avg`, which answers a DECIMAL column with a rounded DECIMAL mean.
            const mean = std.ascii.eqlIgnoreCase(name, "avg");
            const call_node = if (mean) try avgStateCopy(self.lp_arena, node) else node;
            const at = try hiddenAgg(self.allocator, self.lp_arena, call_node, self.hidden, self.lower);
            return .{ .col = .{ .column = self.n_select + at, .mean = mean } };
        }
        const is_abs = std.ascii.eqlIgnoreCase(name, "abs");
        const is_nullif = std.ascii.eqlIgnoreCase(name, "nullif");
        if (f.over != null or f.distinct != 0 or f.filter != null or f.order_by.count != 0 or
            !(std.ascii.eqlIgnoreCase(name, "round") or is_abs or is_nullif) or f.args.count < 1 or f.args.count > 2 or
            (is_abs and f.args.count != 1) or (is_nullif and f.args.count != 2))
            return postUnsupported(self.lp_arena, node, "the function in");
        const value = try self.build(f.args.items[0] orelse return postUnsupported(self.lp_arena, node, "the call"), depth + 1);
        if (is_abs) return .{ .abs = value };
        if (is_nullif) return .{ .nullif = .{
            .value = value,
            .other = try self.build(f.args.items[1] orelse return postUnsupported(self.lp_arena, node, "the call"), depth + 1),
        } };
        var digits: i32 = 0;
        if (f.args.count == 2) {
            digits = integerLiteral(f.args.items[1] orelse return postUnsupported(self.lp_arena, node, "the call")) orelse
                return postUnsupported(self.lp_arena, node, "a digit count that is no integer literal in");
        }
        return .{ .round = .{ .value = value, .digits = digits } };
    }

    /// `node` as a small integer literal (a minus sign folded in), or null.
    fn integerLiteral(node: *c.LpNode) ?i32 {
        if (node.kind == c.LP_EXPR_UNARY_OP and node.u.unary.op == c.LP_UOP_MINUS) {
            const operand = node.u.unary.operand orelse return null;
            const v = integerLiteral(operand) orelse return null;
            return std.math.negate(v) catch null;
        }
        if (node.kind != c.LP_EXPR_LITERAL_INT) return null;
        const v = std.fmt.parseInt(i32, std.mem.span(node.u.literal.value), 10) catch return null;
        // DuckDB's own bound on a digit count's useful range; past it a result is the input or zero anyway.
        return if (v > 1000) null else v;
    }
};

const avg_state_name = "avg_state\x00".*;

/// `avg(...)` as `avg_state(...)`: a copy of the call node, so the user's own is untouched.
fn avgStateCopy(lp_arena: *c.arena_t, node: *c.LpNode) error{OutOfMemory}!*c.LpNode {
    const copy: *c.LpNode = c.lp_node_alloc(lp_arena, c.LP_EXPR_FUNCTION) orelse return error.OutOfMemory;
    copy.* = node.*;
    copy.u.function.name = @constCast(&avg_state_name);
    return copy;
}

/// Add the aggregate call `node` to `hidden` (once: the same call written alike is reused); its index there.
fn hiddenAgg(
    allocator: std.mem.Allocator,
    lp_arena: *c.arena_t,
    node: *c.LpNode,
    hidden: *std.ArrayList(ast.SelectColumn),
    lower: ?*Lower,
) !usize {
    const text_c = c.lp_ast_to_sql(node, lp_arena);
    if (text_c == null) return error.UnparseError;
    const text = std.mem.span(text_c);
    for (hidden.items, 0..) |h, i| if (h.role == .agg and std.mem.eql(u8, h.expr, text)) return i;
    try hidden.ensureUnusedCapacity(allocator, 1);
    hidden.appendAssumeCapacity(.{ .expr = try allocator.dupe(u8, text), .role = .agg });
    if (lower) |l| try l.hidden.append(l.arena, node);
    return hidden.items.len - 1;
}

/// Add ORDER BY or HAVING term `expr`, which names no SELECT column, to `hidden` (once: an equal term already there is
/// reused); its index there. Under GROUP BY such a term is an aggregate, an expression over aggregates (a derived
/// column, computed after the groups are complete) or an expression whose columns are all grouped; without GROUP BY,
/// HAVING may read an aggregate the SELECT list lacks, and ORDER BY nothing.
fn hiddenTerm(
    allocator: std.mem.Allocator,
    sel: anytype,
    lp_arena: *c.arena_t,
    expr: *c.LpNode,
    has_group_by: bool,
    clause: Clause,
    hidden: *std.ArrayList(ast.SelectColumn),
    lower: ?*Lower,
    n_select: usize,
    post: *PostArena,
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
    if (info.has_agg and !isAggregateCall(expr)) {
        // The aggregates it reads are hidden columns of their own, added first.
        var builder: PostBuilder = .{
            .allocator = allocator,
            .arena = try post.get(),
            .lp_arena = lp_arena,
            .n_select = n_select,
            .hidden = hidden,
            .lower = lower,
        };
        const tree = try builder.build(expr, 0);
        try hidden.ensureUnusedCapacity(allocator, 1);
        hidden.appendAssumeCapacity(.{ .expr = try allocator.dupe(u8, text), .role = .derived, .post = tree });
        if (lower) |l| try l.hidden.append(l.arena, expr);
        return hidden.items.len - 1;
    }
    try hidden.ensureUnusedCapacity(allocator, 1);
    hidden.appendAssumeCapacity(.{ .expr = try allocator.dupe(u8, text), .role = if (info.has_agg) .agg else .key });
    if (lower) |l| try l.hidden.append(l.arena, expr);
    return hidden.items.len - 1;
}

/// Whether `node` reads a result: an aggregate or a column, not a constant.
fn isResult(node: *const c.LpNode) bool {
    const info = analyzeExpr(node);
    return info.has_agg or info.has_bare_col;
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
        lower: ?*Lower,
        post: *PostArena,

        const Self = @This();
        const max_depth = @import("../filter/parser.zig").MAX_GROUP_DEPTH;
        const Error = error{
            FilterTooDeep,
            HavingNotSupported,
            UnparseError,
            LimitNotSupported,
            OrderByNotSupported,
            ColumnMustBeGrouped,
            PostAggNotSupported,
        } || std.mem.Allocator.Error;

        fn unsupported(self: *Self, node: *c.LpNode) error{ UnparseError, HavingNotSupported } {
            const text = c.lp_ast_to_sql(node, self.lp_arena);
            if (text == null) return error.UnparseError;
            diagnosticPrint("sql: HAVING {s} is not supported yet (compare an aggregate or key with a literal or another)\n", .{
                std.mem.span(text),
            });
            return error.HavingNotSupported;
        }

        fn build(self: *Self, node: *c.LpNode, depth: u32) Error!ast.Having {
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
                    if (try self.literal(left)) |lit| {
                        return .{ .cmp = .{ .column = try self.term(right), .op = cmp.flip(), .literal = lit } };
                    }
                    // Two results compared: each an aggregate or key, or an expression over them.
                    if (!isResult(left) or !isResult(right)) return self.unsupported(node);
                    return .{ .cmp_col = .{ .left = try self.term(left), .op = cmp, .right = try self.term(right) } };
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
        fn chain(self: *Self, node: *c.LpNode, op: anytype, depth: u32) Error!ast.Having {
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
            return self.select_cols.len + try hiddenTerm(
                self.allocator,
                self.sel,
                self.lp_arena,
                node,
                self.has_group_by,
                .having,
                self.hidden,
                self.lower,
                self.select_cols.len,
                self.post,
            );
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
    for (select_cols, 0..) |col, i| if (sameExprText(text, col.expr)) {
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

/// `GROUP BY k ORDER BY k * 2`: the term reads only grouped columns, so it has one value per group, but the engine binds
/// an ORDER BY term to a result column or a GROUP BY key. Adding the expression to GROUP BY as one more key makes it
/// the latter and cannot change the groups, as it is a function of the keys already there.
fn orderTermsAsKeys(allocator: std.mem.Allocator, root: *c.LpNode, lp_arena: *c.arena_t) !void {
    const sel = &root.*.u.select;
    for (0..@intCast(sel.order_by.count)) |t| {
        const term = sel.order_by.items[t] orelse continue;
        const expr: *c.LpNode = term.*.u.order_term.expr orelse continue;
        if (expr.kind == c.LP_EXPR_LITERAL_INT or expr.kind == c.LP_EXPR_COLUMN_REF) continue;
        const info = analyzeExpr(expr);
        if (info.has_agg or info.has_window or info.has_subquery or !info.has_bare_col) continue;
        if (try groupedExpr(allocator, sel.*, lp_arena, expr)) continue;
        // A term a SELECT item already spells is that column, not a new key.
        const text_c = c.lp_ast_to_sql(expr, lp_arena);
        if (text_c == null) return error.UnparseError;
        var selected = false;
        for (0..@intCast(sel.result_columns.count)) |i| {
            const item = selectExpr(sel.*, i) orelse continue;
            const item_c = c.lp_ast_to_sql(item, lp_arena);
            if (item_c != null and sameExprText(std.mem.span(text_c), std.mem.span(item_c))) selected = true;
        }
        if (selected or !try columnsGrouped(allocator, sel.*, lp_arena, expr)) continue;
        try pushGroupTerm(sel, lp_arena, expr);
    }
}

/// Whether every column `expr` reads is a GROUP BY term, or sits in a subexpression that is one.
fn columnsGrouped(allocator: std.mem.Allocator, sel: anytype, lp_arena: *c.arena_t, expr: *c.LpNode) !bool {
    const Visit = struct {
        allocator: std.mem.Allocator,
        sel: @TypeOf(sel),
        arena: *c.arena_t,
        all: bool = true,
        failed: bool = false,

        fn enter(v: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(v.*.user_data));
            const is_column = node.*.kind == c.LP_EXPR_COLUMN_REF;
            // A subexpression that is a GROUP BY term (`n - 10` under GROUP BY n - 10) is one value per group, whatever
            // columns it reads; the columns of anything else must each be a term.
            const grouped = groupedExpr(self.allocator, self.sel, self.arena, node) catch {
                self.failed = true;
                return 2;
            };
            if (grouped) return 1;
            if (!is_column) return 0;
            self.all = false;
            return 2;
        }
    };
    var state: Visit = .{ .allocator = allocator, .sel = sel, .arena = lp_arena };
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(expr, &visitor);
    if (state.failed) return error.UnparseError;
    return state.all;
}

/// Whether `expr` (unparsed as `text`, quotes stripped) is the term `other`: the same column reference, case and
/// quoting aside, or the same expression text.
fn sameTerm(lp_arena: *c.arena_t, expr: *c.LpNode, text: []const u8, other: *c.LpNode) !bool {
    if (sameColumnRef(expr, other)) |same| return same;
    const other_c = c.lp_ast_to_sql(other, lp_arena);
    if (other_c == null) return error.UnparseError;
    return sameExprText(text, stripQuotes(std.mem.trim(u8, std.mem.span(other_c), " ")));
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
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT * FROM 't'"));
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT count(*) AS c FROM 't'"));
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT y, sum(x) AS s FROM 't' GROUP BY y"));
    try std.testing.expectError(error.DistinctNotSupported, parseSqlQuery(a, "SELECT DISTINCT ON (y) y, x FROM 't'"));
    try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, "SELECT x FROM 't' HAVING x > 1"));
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't'"));
    try std.testing.expectError(error.UnsupportedStatement, parseSqlQuery(a, "SELECT x FROM 't' UNION SELECT x FROM 'u'"));
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT y, sum(x) AS s FROM 't' GROUP BY z"));
}

test "sql_parser: DISTINCT, GROUP BY ALL and an ORDER BY expression of the keys are GROUP BY" {
    const a = std.testing.allocator;
    const distinct = try parseSqlQuery(a, "SELECT DISTINCT x, y + 1 AS z, x FROM 't' ORDER BY z");
    defer distinct.deinit(a);
    try std.testing.expect(distinct.is_aggregate);
    try std.testing.expectEqualStrings("x, y + 1", distinct.group_by_str.?);
    try std.testing.expectEqual(@as(usize, 3), distinct.select_cols.len);
    try std.testing.expectEqual(ast.SelectColumn.Role.key, distinct.select_cols[1].role);

    const all = try parseSqlQuery(a, "SELECT g, n - 1 AS m, count(*) AS c FROM 't' GROUP BY ALL");
    defer all.deinit(a);
    try std.testing.expectEqualStrings("g, n - 1", all.group_by_str.?);
    try std.testing.expectEqualStrings("count(*) AS c", all.aggregate_str.?);

    // Only aggregates: one group, as with no GROUP BY at all.
    const none = try parseSqlQuery(a, "SELECT count(*) AS c FROM 't' GROUP BY ALL");
    defer none.deinit(a);
    try std.testing.expect(none.group_by_str == null);

    // `k * 2` reads only a grouped column: one value per group, so it joins the keys.
    const ordered = try parseSqlQuery(a, "SELECT k, count(*) AS c FROM 't' GROUP BY k ORDER BY k * 2 DESC");
    defer ordered.deinit(a);
    try std.testing.expectEqualStrings("k, k * 2", ordered.group_by_str.?);
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT k, count(*) AS c FROM 't' GROUP BY k ORDER BY j * 2"));
}

test "sql_parser: NULLS FIRST reaches the order keys and NULLS LAST is the default" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT y, count(*) AS c FROM 't' GROUP BY y ORDER BY y NULLS FIRST, c DESC NULLS LAST");
    defer q.deinit(a);
    try std.testing.expectEqualSlices(ast.OrderKey, &.{
        .{ .column = 0, .descending = false, .nulls_first = true },
        .{ .column = 1, .descending = true, .nulls_first = false },
    }, q.result_order.keys);
    const rows = try parseSqlQuery(a, "SELECT x FROM 't' ORDER BY x DESC NULLS FIRST");
    defer rows.deinit(a);
    try std.testing.expect(rows.result_order.rows[0].nulls_first and rows.result_order.rows[0].descending);
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

test "sql_parser: WHERE compares two expressions in the plan; FILTER and CASE conditions keep the literal grammar" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT count(*) AS n FROM 't' WHERE s = t",
        "SELECT count(*) AS n FROM 't' WHERE n + 1 > m * 2 AND s = 'x'",
        "SELECT count(*) AS n FROM 't' WHERE 3 < n",
    }) |sql| {
        var q = try parseSqlPlan(a, sql);
        defer q.deinit(a);
        const f = q.syntax.?.filter.?;
        const compare = switch (f) {
            .expr_compare => f,
            .and_ => |terms| terms[0],
            else => return error.TestUnexpectedResult,
        };
        try std.testing.expect(compare == .expr_compare);
    }
    // A column against a literal stays the leaf statistics prune, a constant side folded first.
    var plain = try parseSqlPlan(a, "SELECT count(*) AS n FROM 't' WHERE n > 1 + 1");
    defer plain.deinit(a);
    try std.testing.expectEqualStrings("2", plain.syntax.?.filter.?.comparison.value);
    for ([_][]const u8{
        "SELECT count(*) FILTER (WHERE s <> t) AS n FROM 't'",
        "SELECT CASE WHEN s = t THEN 1 ELSE 0 END AS k FROM 't'",
        "SELECT id FROM 't' WHERE s LIKE t",
    }) |sql| try std.testing.expectError(error.BadValue, parseSqlPlan(a, sql));
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

test "sql_parser: constant integers keep DuckDB's width, a signed one after its sign" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT count(*) AS n FROM 't' WHERE i > -2147483648 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > 2147483647 + 1",
        "SELECT count(*) AS n FROM 't' WHERE i > -9223372036854775808 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > 9223372036854775807 + 1",
        "SELECT count(*) AS n FROM 't' WHERE i > 3037000500 * 3037000500",
        "SELECT count(*) AS n FROM 't' WHERE i > 170141183460469231731687303715884105727 + 1",
    }) |sql| try std.testing.expectError(error.BadValue, parseSqlPlan(a, sql));
    for ([_][]const u8{
        "SELECT count(*) AS n FROM 't' WHERE i > -2147483647 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > 2147483648 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > -9223372036854775807 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > 9223372036854775808 - 1",
        "SELECT count(*) AS n FROM 't' WHERE i > -2147483648",
    }) |sql| {
        const q = try parseSqlPlan(a, sql);
        q.deinit(a);
    }
}

test "sql_parser: a column spelled like GROUP BY ALL's marker is a column" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT count(*) AS n FROM 't' GROUP BY __zpq_group_all ORDER BY n");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("__zpq_group_all", q.group_by_str.?);
    // ... in any case, and beside a real GROUP BY ALL.
    const upper = try parseSqlQuery(a, "SELECT __ZPQ_GROUP_ALL, count(*) AS n FROM 't' GROUP BY ALL");
    defer upper.deinit(a);
    try std.testing.expectEqualStrings("__ZPQ_GROUP_ALL", upper.group_by_str.?);
}

test "sql_parser: GROUP BY ALL keeps constants out of the keys" {
    const a = std.testing.allocator;
    // Constants alone: no grouping, so one row per input row, as plain rows are.
    const one = try parseSqlQuery(a, "SELECT 1 AS one FROM 't' GROUP BY ALL");
    defer one.deinit(a);
    try std.testing.expect(!one.is_aggregate and one.group_by_str == null);
    try std.testing.expectEqualStrings("1 AS one", one.select_str.?);
    // With a key they join it, binding each SELECT item to a key.
    const keyed = try parseSqlQuery(a, "SELECT 1 AS one, g, count(*) AS n FROM 't' GROUP BY ALL");
    defer keyed.deinit(a);
    try std.testing.expectEqualStrings("1, g", keyed.group_by_str.?);
    // With only aggregates there is no key, and the constant has no place in the one group: named.
    try std.testing.expectError(error.MixedSelectNotSupported, parseSqlQuery(a, "SELECT 1 AS one, count(*) AS n FROM 't' GROUP BY ALL"));
    // DISTINCT of a constant is one row, and stays one grouping.
    const distinct = try parseSqlQuery(a, "SELECT DISTINCT 1 AS one FROM 't'");
    defer distinct.deinit(a);
    try std.testing.expectEqualStrings("1", distinct.group_by_str.?);
}

test "sql_parser: an ESCAPE the user writes is refused, whatever it spells" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT count(*) AS n FROM 't' WHERE s LIKE 'a%' ESCAPE 'zpq:ilike'",
        "SELECT count(*) AS n FROM 't' WHERE s LIKE 'a%' ESCAPE '\\'",
        "SELECT count(*) AS n FROM 't' WHERE s NOT LIKE 'a%' ESCAPE 'zpq:ilike_'",
    }) |sql| try std.testing.expectError(error.BadValue, parseSqlPlan(a, sql));
    const q = try parseSqlPlan(a, "SELECT count(*) AS n FROM 't' WHERE s ILIKE 'a%' AND t LIKE 'zpq:ilike'");
    defer q.deinit(a);
    try std.testing.expect(q.syntax.?.filter.?.and_[0].like.fold);
    try std.testing.expect(!q.syntax.?.filter.?.and_[1].like.fold);
}

test "sql_parser: expressions that differ only in a string literal's case are two group keys" {
    const a = std.testing.allocator;
    try std.testing.expect(sameExprText("CASE WHEN i = 1 THEN 'a' END", "case when I = 1 then 'a' end"));
    try std.testing.expect(!sameExprText("CASE WHEN i = 1 THEN 'a' END", "CASE WHEN i = 1 THEN 'A' END"));
    try std.testing.expect(!sameExprText("x = 'it''s'", "x = 'IT''S'"));
    const q = try parseSqlQuery(a, "SELECT DISTINCT CASE WHEN i = 1 THEN 'a' ELSE 'a' END AS a, CASE WHEN i = 1 THEN 'a' ELSE 'A' END AS b FROM 't'");
    defer q.deinit(a);
    try std.testing.expectEqual(@as(usize, 2), std.mem.count(u8, q.group_by_str.?, "CASE"));
    const same = try parseSqlQuery(a, "SELECT DISTINCT CASE WHEN i = 1 THEN 'a' END AS a, case when I = 1 then 'a' end AS b FROM 't'");
    defer same.deinit(a);
    try std.testing.expectEqual(@as(usize, 1), std.mem.count(u8, same.group_by_str.?, "CASE"));
}

test "sql_parser makes an expression over aggregates a derived column that reads hidden aggregates" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT k, sum(a) / sum(b) AS r, count(*) AS c FROM 't' GROUP BY k");
    defer q.deinit(a);
    try std.testing.expect(q.result_order.post);
    try std.testing.expectEqual(ast.SelectColumn.Role.key, q.select_cols[0].role);
    try std.testing.expectEqual(ast.SelectColumn.Role.derived, q.select_cols[1].role);
    try std.testing.expectEqual(ast.SelectColumn.Role.agg, q.select_cols[2].role);
    // The scan computes count(*), then the hidden sum(a) and sum(b); `r` reads them by result column.
    try std.testing.expectEqualStrings("count(*) AS c, sum(a) AS " ++ hidden_alias ++ "0, sum(b) AS " ++ hidden_alias ++ "1", q.aggregate_str.?);
    const post = q.select_cols[1].post.?.binary;
    try std.testing.expectEqual(ast.PostOp.div, post.op);
    try std.testing.expectEqual(@as(usize, 3), post.left.col.column);
    try std.testing.expectEqual(@as(usize, 4), post.right.col.column);

    // Structured: the derived column has no call of its own, and the hidden ones are the calls after the SELECT list's.
    const s = try parseSqlPlan(a, "SELECT k, sum(a) / sum(b) AS r, count(*) AS c FROM 't' GROUP BY k");
    defer s.deinit(a);
    try std.testing.expectEqual(@as(usize, 3), s.syntax.?.calls.len);
    try std.testing.expectEqual(ast.SelectColumn.Role.derived, s.syntax.?.columns[1].role);
    try std.testing.expect(s.syntax.?.columns[1].post != null);
}

test "sql_parser reads a derived ORDER BY or HAVING term and an AVG as a mean" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT k, sum(a) AS s FROM 't' GROUP BY k HAVING sum(a) > avg(a) * 1000 ORDER BY sum(a) / count(*) DESC, s");
    defer q.deinit(a);
    try std.testing.expect(q.result_order.post);
    // ORDER BY's term first: its aggregates, then it; HAVING's right side after them. HAVING's `sum(a)` is the
    // SELECT list's own column.
    try std.testing.expectEqual(@as(usize, 5), q.result_order.hidden.len);
    try std.testing.expectEqualStrings("sum(a)", q.result_order.hidden[0].expr);
    try std.testing.expectEqualStrings("count(*)", q.result_order.hidden[1].expr);
    try std.testing.expectEqual(ast.SelectColumn.Role.derived, q.result_order.hidden[2].role);
    // An AVG in an expression is read as `avg_state`, for its exact sum and count.
    try std.testing.expectEqualStrings("avg_state(a)", q.result_order.hidden[3].expr);
    try std.testing.expectEqual(ast.SelectColumn.Role.derived, q.result_order.hidden[4].role);
    try std.testing.expect(q.result_order.hidden[3].role == .agg);
    // Result columns: k, s, then the hidden ones from 2.
    try std.testing.expectEqual(@as(usize, 4), q.result_order.keys[0].column);
    try std.testing.expectEqual(@as(usize, 1), q.result_order.keys[1].column);
    const cmp = q.result_order.having.?.cmp_col;
    try std.testing.expectEqual(@as(usize, 1), cmp.left);
    try std.testing.expectEqual(@as(usize, 6), cmp.right);
    try std.testing.expect(q.result_order.hidden[4].post.?.binary.left.col.mean);

    // Without GROUP BY: the derived column reads hidden aggregates, whose calls follow the plain ones.
    const scalar = try parseSqlQuery(a, "SELECT count(*) AS c, 100 * sum(a) / sum(b) AS pct FROM 't'");
    defer scalar.deinit(a);
    try std.testing.expect(scalar.result_order.post);
    try std.testing.expectEqualStrings("count(*) AS c, sum(a) AS " ++ hidden_alias ++ "0, sum(b) AS " ++ hidden_alias ++ "1", scalar.aggregate_str.?);
}

test "sql_parser: abs and nullif over aggregates are post-aggregate expressions; over rows abs is a call, nullif a CASE" {
    const a = std.testing.allocator;
    const q = try parseSqlQuery(a, "SELECT g, sum(a) / nullif(sum(b), 0) AS r, abs(sum(a)) AS m FROM 't' GROUP BY g");
    defer q.deinit(a);
    try std.testing.expectEqual(ast.SelectColumn.Role.derived, q.select_cols[1].role);
    try std.testing.expect(q.select_cols[1].post.?.binary.right.* == .nullif);
    try std.testing.expect(q.select_cols[2].post.?.* == .abs);
    // Over plain rows `nullif` is the CASE the batch evaluator already runs, `abs` a function.
    const rows = try parseSqlPlan(a, "SELECT nullif(n, 0) AS z, abs(n - 1) AS m, nullif(s, 'x') AS t FROM 't'");
    defer rows.deinit(a);
    const items = rows.syntax.?.select.?;
    try std.testing.expect(items[0].expr == .case_);
    try std.testing.expect(items[0].expr.case_.whens[0].then == null);
    try std.testing.expectEqualStrings("abs", items[1].expr.call.name);
    try std.testing.expect(items[2].expr == .case_);
    try std.testing.expectError(error.BadValue, parseSqlPlan(a, "SELECT nullif(n, m) AS z FROM 't'"));
    try std.testing.expectError(error.ExpressionInPredicate, parseSqlPlan(a, "SELECT nullif(n - 1, 0) AS z FROM 't'"));
    try std.testing.expectError(error.WrongArity, parseSqlPlan(a, "SELECT nullif(n) AS z FROM 't'"));
}

test "sql_parser names what stays refused in an expression over aggregates" {
    const a = std.testing.allocator;
    for ([_][]const u8{
        "SELECT sum(a) % 2 AS r FROM 't'",
        "SELECT sqrt(sum(a)) AS r FROM 't'",
        "SELECT sum(a) || 'x' AS r FROM 't'",
        "SELECT round(sum(a), count(*)) AS r FROM 't'",
        "SELECT CASE WHEN sum(a) > 1 THEN 1 ELSE 0 END AS r FROM 't'",
        "SELECT sum(a) + 'x' AS r FROM 't'",
    }) |sql| try std.testing.expectError(error.PostAggNotSupported, parseSqlQuery(a, sql));
    // A column beside an aggregate must be a GROUP BY term as a whole, as before.
    try std.testing.expectError(error.ColumnMustBeGrouped, parseSqlQuery(a, "SELECT k, sum(a) / k AS r FROM 't' GROUP BY k"));
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

    // Two results compare as columns; a constant on one side is neither a literal nor a result.
    const cols = try parseSqlQuery(a, "SELECT k, count(*) FROM 't' GROUP BY k HAVING count(*) > sum(x)");
    defer cols.deinit(a);
    try std.testing.expectEqual(ast.CmpOp.gt, cols.result_order.having.?.cmp_col.op);
    try std.testing.expectEqual(@as(usize, 1), cols.result_order.having.?.cmp_col.left);
    try std.testing.expectEqual(@as(usize, 2), cols.result_order.having.?.cmp_col.right);
    try std.testing.expectError(error.HavingNotSupported, parseSqlQuery(a, "SELECT k, count(*) FROM 't' GROUP BY k HAVING count(*) > 1 + 1"));
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

test "sql_parser reads substring(x FROM a FOR b) as substring(x, a, b), and only that" {
    const a = std.testing.allocator;
    const cases = [_][2][]const u8{
        .{ "SELECT substring(s FROM 2 FOR 3), SUBSTRING (s From 2), substring(s FOR 4) FROM t", "SELECT substring(s , 2 , 3), SUBSTRING (s , 2), substring(s , 1, 4) FROM t" },
        .{ "substring(substring(s FROM 1 FOR 5) FROM 2)", "substring(substring(s , 1 , 5) , 2)" },
        // A keyword in a nested call or subquery belongs to that one.
        .{ "substring(f(x FROM y) FROM 1)", "substring(f(x FROM y) , 1)" },
        .{ "substring((SELECT a FROM t) FROM 1)", "substring((SELECT a FROM t) , 1)" },
        // Plain argument lists, quoted text and other calls are not touched.
        .{ "substring(s, 2, 3) FROM t", "substring(s, 2, 3) FROM t" },
        .{ "'substring(a FROM 1)' \"substring\"(a FROM 1)", "'substring(a FROM 1)' \"substring\"(a FROM 1)" },
        .{ "foo(a FROM b) substringx(a FROM 1)", "foo(a FROM b) substringx(a FROM 1)" },
        .{ "substring(x, 1 FROM 2)", "substring(x, 1 FROM 2)" },
        // A comma after a keyword makes no call DuckDB reads: it is left as a syntax error.
        .{ "substring(x FROM 1, 2)", "substring(x , 1,, 2)" },
        .{ "substring(s FROM 'a''FOR' FOR 2)", "substring(s , 'a''FOR' , 2)" },
    };
    for (cases) |case| {
        const got = try substringKeywords(a, case[0]);
        defer a.free(got);
        try std.testing.expectEqualStrings(case[1], got);
    }
    const q = try parseSqlQuery(a, "SELECT substring(s FROM 2 FOR 3) AS p FROM f GROUP BY p");
    defer q.deinit(a);
    try std.testing.expectEqualStrings("substring(s, 2, 3)", q.select_cols[0].expr);
    try std.testing.expectError(error.SqlParseError, parseSqlQuery(a, "SELECT substring(s FROM 1, 2) AS p FROM f"));
}

test "syntax: owned SQL releases partial nodes and result-order storage on allocation failure" {
    const Check = struct {
        fn run(a: std.mem.Allocator) !void {
            const parsed = try parseSqlPlan(a, "SELECT b AS k, count(*) AS n FROM 't.parquet' WHERE b > 0 GROUP BY b HAVING sum(n) > 1 ORDER BY n DESC LIMIT 2");
            defer parsed.deinit(a);
            try std.testing.expect(parsed.syntax != null);
            try std.testing.expectEqual(@as(usize, 2), parsed.syntax.?.calls.len);
        }
    };
    var baseline = std.testing.FailingAllocator.init(std.testing.allocator, .{ .resize_fail_index = 0 });
    try Check.run(baseline.allocator());
    try std.testing.expectEqual(baseline.allocated_bytes, baseline.freed_bytes);
    for (0..baseline.alloc_index) |fail_index| {
        var failing = std.testing.FailingAllocator.init(std.testing.allocator, .{
            .fail_index = fail_index,
            .resize_fail_index = 0,
        });
        try std.testing.expectError(error.OutOfMemory, Check.run(failing.allocator()));
        try std.testing.expect(failing.has_induced_failure);
        try std.testing.expectEqual(failing.allocated_bytes, failing.freed_bytes);
    }
}
