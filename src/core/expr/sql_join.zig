//! The join binder: an N:1 star of equi-joins written as `FROM a, b, ... WHERE a.k = b.k ...` or `FROM a JOIN b ON
//! a.k = b.k ...` (INNER) or `FROM a LEFT JOIN b ON a.k = b.k ...` (LEFT), or as `WHERE a.k [NOT] IN (SELECT b.k FROM b
//! WHERE ...)` (SEMI, ANTI), becomes one single-table query over the probe (the largest table a LEFT JOIN keeps) and a
//! build spec per other table, each joined to the probe by one key. A table only its own conjuncts read reduces to an
//! IN first (`reduce`), onto the table it joins. The probe query reads build columns by name; the frontend binds them as
//! virtual leaves (`join/lookup.zig`), a LEFT JOIN's NULL on a miss.
//!
//! Every WHERE/ON conjunct must read one side only, apart from the one key equality: build-side conjuncts become the
//! build's filter, the rest stay in the probe query. A LEFT JOIN's table is read by its own ON alone (`NullSide`).
//! Anything else (residuals across sides, OR across sides, a second equality, RIGHT and FULL joins) refuses rather
//! than guess.
const std = @import("std");
const c = @import("liteparser_c");
const spells = @import("syntax.zig").spells;

pub const Kind = enum { inner, semi, anti, left };

pub const Error = error{
    SqlParseError,
    UnsupportedJoin,
    SubqueryLimitNotSupported,
    CorrelatedSubqueryNotSupported,
    JoinKeyNotFound,
    JoinResidualNotSupported,
    AmbiguousJoinColumn,
    JoinColumnNeedsAlias,
    UsingJoinNotSupported,
    ExistsSubquery,
    LeftJoinOn,
    LeftJoinWhere,
    OutOfMemory,
};

pub const Plan = struct {
    kind: Kind,
    probe_path: []const u8,
    build_path: []const u8,
    /// The probe's foreign key and the build's key column, and a composite key's further columns of each, paired.
    fk: []const u8,
    build_key: []const u8,
    fk_more: []const []const u8 = &.{},
    build_key_more: []const []const u8 = &.{},
    /// The query with the join removed: a single-table SELECT over `probe_path`.
    probe_sql: []const u8,
    /// The build side's own conjuncts, as one SQL expression, or null.
    build_where: ?[]const u8,
    /// Build columns the probe query reads, in first-reference order (never the key: it reads as the FK).
    build_columns: []const []const u8,
    /// The names those go by in the probe query, when any is not its own (`publishedName`); else empty.
    build_columns_as: []const []const u8 = &.{},
    /// Every IN of the probe's and the builds' queries is one a reduction wrote (`reduce`): an INNER edge whose keys
    /// must be unique, as any INNER join's are, though no column of its table is read. A query that reduced holds no IN
    /// of its own (its table count would not match).
    reduced: bool = false,
    /// The join itself is a NOT EXISTS (`Exists`), not a NOT IN.
    not_exists: bool = false,
    /// The query's (other) NOT EXISTS conditions on the probe, and on the main build.
    semis: []const Exists = &.{},
    build_semis: []const Exists = &.{},
    /// The star's other dimensions: each joined to the probe by one key equality and publishing its own columns.
    dims: []const Dim = &.{},
    /// A snowflake's tables joined to the main build by an FK of its own (`Dim.dims`).
    build_dims: []const Dim = &.{},
};

/// One dimension of a star beside the main build (`Plan`'s own build fields).
pub const Dim = struct {
    path: []const u8,
    fk: []const u8,
    key: []const u8,
    /// A composite key's further columns: the parent's and this table's, paired.
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    where: ?[]const u8,
    columns: []const []const u8,
    /// The names `columns` go by in the probe query, when any is not its own (`publishedName`); else empty.
    columns_as: []const []const u8 = &.{},
    /// A LEFT JOIN's: every probe row stays, a miss reading the dimension's columns as NULL.
    left: bool = false,
    /// A snowflake's tables joined to this one by an FK of its own (`fk` a column of this table), INNER, each
    /// publishing its columns through this one's build.
    dims: []const Dim = &.{},
    /// The query's NOT EXISTS conditions on this table.
    semis: []const Exists = &.{},
};

/// An EXISTS of the WHERE that no IN can say: a NOT EXISTS (an ANTI join of the table whose column `fk` is, where a
/// NULL on either side matches nothing), or one that reads the outer query by more than one key. It leaves the WHERE
/// and travels here, beside the query, never inside its SQL: written as SQL a NOT EXISTS would be a NOT IN, whose
/// answer is UNKNOWN against a NULL, and nothing a query can spell may stand for the difference.
pub const Exists = struct {
    path: []const u8,
    fk: []const u8,
    key: []const u8,
    /// A composite key's further columns: the outer table's and the subquery's, paired.
    fk_more: []const []const u8 = &.{},
    key_more: []const []const u8 = &.{},
    /// The subquery's conditions on its own table, as one SQL expression, or null.
    where: ?[]const u8,
    /// NOT EXISTS; else EXISTS, a SEMI join.
    anti: bool = true,
};

/// What the caller knows of the tables (from their footers): whether a column name is theirs, and their rows.
pub const Tables = struct {
    paths: []const []const u8,
    has: *const fn (ctx: *const anyopaque, table: usize, name: []const u8) bool,
    rows: []const u64,
    ctx: *const anyopaque,
    root: ?usize = null,
    /// Whether two columns can be parts of one composite key: both of a type a key takes, and the same one. Null
    /// takes every pair.
    same_key: ?*const fn (ctx: *const anyopaque, table: usize, name: []const u8, other: usize, other_name: []const u8) bool = null,
    /// The FROM is as the query wrote it, so a table without an alias goes by its file's name. False for the binder's
    /// own text (a probe query, a build's filter), whose FROM has lost the alias the query gave it.
    user_from: bool = true,
};

fn stripQuotes(s: []const u8) []const u8 {
    if (s.len >= 2 and (s[0] == '\'' or s[0] == '"') and s[s.len - 1] == s[0]) return s[1 .. s.len - 1];
    return s;
}

fn span(p: [*c]const u8) []const u8 {
    return if (p == null) "" else std.mem.span(p);
}

const Parsed = struct {
    arena: *c.arena_t,
    root: *c.LpNode,
    fn deinit(self: Parsed) void {
        c.arena_destroy(self.arena);
    }
};

fn parse(sql: [:0]const u8) Error!Parsed {
    const arena = c.arena_create(64 * 1024) orelse return error.OutOfMemory;
    errdefer c.arena_destroy(arena);
    var msg: [*c]const u8 = null;
    const stmts = c.lp_parse_all(sql.ptr, arena, &msg);
    if (stmts == null or stmts.*.count != 1) return error.SqlParseError;
    const root = stmts.*.items[0] orelse return error.SqlParseError;
    if (root.*.kind != c.LP_STMT_SELECT) return error.SqlParseError;
    // The plan's pieces are unparsed and read back (`toSql`): with NOT only above leaves they mean what was written,
    // as the single-table parser's do, however the planner regroups the conditions (`hoistOrs`).
    // `TRUE` / `FALSE` read as column names until made literals; left so, `d.flag = TRUE` reads a column no table has.
    @import("sql_parser.zig").boolLiterals(root);
    @import("sql_parser.zig").pushNotsDown(root, arena) catch return error.OutOfMemory;
    try unquoteAliases(root, arena);
    return .{ .arena = arena, .root = root };
}

/// Every alias as the name it spells. The grammar keeps a quoted alias in its quotes (`AS "dup"` is held as `"dup"`),
/// where it holds a column or a qualifier as its text. Left so, the alias would name nothing a reference could match,
/// and unparsed (quoted as its text needs) it would come back with its quotes as part of the name.
fn unquoteAliases(root: *c.LpNode, arena: *c.arena_t) Error!void {
    const Visit = struct {
        arena: *c.arena_t,
        failed: bool = false,

        fn enter(v: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const self: *@This() = @ptrCast(@alignCast(v.*.user_data));
            const alias: *[*c]u8 = switch (node.*.kind) {
                c.LP_RESULT_COLUMN => &node.*.u.result_column.alias,
                c.LP_FROM_TABLE => &node.*.u.from_table.alias,
                c.LP_FROM_SUBQUERY => &node.*.u.from_subquery.alias,
                else => return 0,
            };
            if (alias.* == null) return 0;
            const raw = std.mem.span(alias.*);
            if (raw.len < 2 or raw[0] != '"' or raw[raw.len - 1] != '"') return 0;
            const out: [*]u8 = @ptrCast(c.arena_alloc(self.arena, raw.len - 1) orelse {
                self.failed = true;
                return 2;
            });
            var n: usize = 0;
            var i: usize = 1;
            while (i < raw.len - 1) : (i += 1) {
                // A doubled quote is one quote.
                if (raw[i] == '"' and i + 2 < raw.len and raw[i + 1] == '"') i += 1;
                out[n] = raw[i];
                n += 1;
            }
            out[n] = 0;
            alias.* = out;
            return 0;
        }
    };
    var state: Visit = .{ .arena = arena };
    var visitor: c.LpVisitor = .{ .user_data = &state, .enter = Visit.enter, .leave = null };
    c.lp_ast_walk(root, &visitor);
    if (state.failed) return error.OutOfMemory;
}

const FromTable = struct {
    path: []const u8,
    alias: ?[]const u8,

    /// What the table's columns may be qualified with where the query wrote this FROM: its alias, else the file's own
    /// name (`t.k` for `FROM 'dir/t.parquet'`), as DuckDB names a file it reads and the single-table parser does.
    fn name(t: FromTable) []const u8 {
        if (t.alias) |al| return al;
        const base = std.fs.path.basename(t.path);
        return if (std.mem.lastIndexOfScalar(u8, base, '.')) |dot| base[0..dot] else base;
    }
};

fn fromTable(n: *c.LpNode) ?FromTable {
    if (n.kind != c.LP_FROM_TABLE or n.u.from_table.name == null) return null;
    return .{ .path = stripQuotes(span(n.u.from_table.name)), .alias = if (n.u.from_table.alias) |a| span(a) else null };
}

/// The two tables' paths when `sql` is a join this binder may take, so the caller can read their footers; else null
/// (an ordinary single-table query, left to the established parser).
pub fn tables(a: std.mem.Allocator, sql: [:0]const u8) Error!?[]const []const u8 {
    const p = parse(sql) catch return null;
    defer p.deinit();
    const lowered = try lowerExists(a, p.root);
    const from = p.root.u.select.from orelse return null;
    var list: std.ArrayList([]const u8) = .empty;
    defer list.deinit(a);
    if (from.*.kind == c.LP_JOIN_CLAUSE) {
        var nodes: std.ArrayList(*c.LpNode) = .empty;
        defer nodes.deinit(a);
        try flattenFrom(from, &nodes, null, null, a);
        for (nodes.items) |n| try list.append(a, fromTable(n).?.path);
    } else {
        const t0 = fromTable(from) orelse return null;
        // The join `planSemi` makes of a single table: its first IN, else its first NOT EXISTS.
        const sub = if (semiConjunct(p.root.u.select.where)) |in| blk: {
            const sub_from = in.node.u.in.select.*.u.select.from orelse return error.UnsupportedJoin;
            break :blk (fromTable(sub_from) orelse return error.UnsupportedJoin).path;
        } else if (lowered.len != 0) lowered[0].path else return null;
        try list.appendSlice(a, &.{ t0.path, sub });
    }
    if (list.items.len < 2 or list.items.len > 64) return error.UnsupportedJoin;
    // Paths outlive the C arena.
    const out = try a.alloc([]const u8, list.items.len);
    for (list.items, out) |path, *o| o.* = try a.dupe(u8, path);
    return out;
}

/// A LEFT JOIN's null-supplying table, by its place in the FROM list, and its ON conjuncts: they decide which of its
/// rows match a probe row, never which probe rows exist, so they stay apart from the WHERE.
const NullSide = struct { table: usize, on: std.ArrayList(*c.LpNode) = .empty };

/// The FROM tables of a join tree in order, the INNER ON conditions into `on` and each LEFT JOIN's into `nulls`.
/// Comma, INNER and LEFT joins, a LEFT JOIN's right side one table: NATURAL and USING name no key, and RIGHT and FULL
/// joins keep the rows of a side this binder does not probe.
fn flattenFrom(n: *c.LpNode, out: *std.ArrayList(*c.LpNode), on: ?*std.ArrayList(*c.LpNode), nulls: ?*std.ArrayList(NullSide), a: std.mem.Allocator) Error!void {
    if (n.kind == c.LP_JOIN_CLAUSE) {
        const j = n.u.join;
        const left = j.join_type & c.LP_JOIN_LEFT != 0;
        const allowed: c_int = if (left) c.LP_JOIN_LEFT | c.LP_JOIN_OUTER else c.LP_JOIN_INNER | c.LP_JOIN_CROSS;
        if (j.join_type & ~allowed != 0) return error.UnsupportedJoin;
        if (j.using_columns.count != 0 and on != null) return error.UsingJoinNotSupported;
        try flattenFrom(j.left orelse return error.UnsupportedJoin, out, on, nulls, a);
        if (left) {
            const right = j.right orelse return error.UnsupportedJoin;
            _ = fromTable(right) orelse return error.UnsupportedJoin;
            try out.append(a, right);
            if (nulls) |list| {
                var side: NullSide = .{ .table = out.items.len - 1 };
                try flatten(j.on_expr, &side.on, a);
                try list.append(a, side);
            }
            return;
        }
        try flattenFrom(j.right orelse return error.UnsupportedJoin, out, on, nulls, a);
        if (on) |list| try flatten(j.on_expr, list, a);
        return;
    }
    _ = fromTable(n) orelse return error.UnsupportedJoin;
    try out.append(a, n);
}

const Conj = struct { node: *c.LpNode, anti: bool };

fn flatten(n: ?*c.LpNode, list: *std.ArrayList(*c.LpNode), a: std.mem.Allocator) !void {
    const node = n orelse return;
    if (node.kind == c.LP_EXPR_BINARY_OP and node.u.binary.op == c.LP_OP_AND) {
        try flatten(node.u.binary.left, list, a);
        try flatten(node.u.binary.right, list, a);
        return;
    }
    try list.append(a, node);
}

/// The first top-level `col IN (SELECT ...)` or `col NOT IN (SELECT ...)` conjunct of `where`.
fn semiConjunct(where: ?*c.LpNode) ?Conj {
    var stack: [64]*c.LpNode = undefined;
    var len: usize = 0;
    if (where) |w| {
        stack[0] = w;
        len = 1;
    }
    while (len > 0) {
        len -= 1;
        const n = stack[len];
        if (n.kind == c.LP_EXPR_BINARY_OP and n.u.binary.op == c.LP_OP_AND and len + 2 <= stack.len) {
            if (n.u.binary.right) |r| {
                stack[len] = r;
                len += 1;
            }
            if (n.u.binary.left) |l| {
                stack[len] = l;
                len += 1;
            }
            continue;
        }
        if (n.kind == c.LP_EXPR_IN and n.u.in.select != null and n.u.in.expr != null and
            n.u.in.expr.*.kind == c.LP_EXPR_COLUMN_REF) return .{ .node = n, .anti = n.u.in.is_not != 0 };
    }
    return null;
}

/// A NOT EXISTS `lowerExists` took out of the WHERE, as the parse holds it (`Exists` once planned).
const Lowered = struct {
    path: []const u8,
    /// The key equalities, the outer query's column and the subquery's: at least one.
    fks: []const *c.LpNode,
    keys: []const *c.LpNode,
    own: []const *c.LpNode,
    anti: bool,

    fn planned(x: Lowered, a: std.mem.Allocator, p: Parsed) Error!Exists {
        return .{
            .path = try a.dupe(u8, x.path),
            .fk = try columnOf(a, x.fks[0]),
            .key = try columnOf(a, x.keys[0]),
            .fk_more = try columnsOf(a, x.fks[1..]),
            .key_more = try columnsOf(a, x.keys[1..]),
            .where = try whereSql(a, p, x.own),
            .anti = x.anti,
        };
    }
};

fn columnOf(a: std.mem.Allocator, ref: *c.LpNode) ![]const u8 {
    return a.dupe(u8, span(ref.u.column_ref.column));
}

fn columnsOf(a: std.mem.Allocator, refs: []const *c.LpNode) ![]const []const u8 {
    const out = try a.alloc([]const u8, refs.len);
    for (refs, out) |ref, *name| name.* = try columnOf(a, ref);
    return out;
}

/// Each top-level `[NOT] EXISTS (SELECT ... FROM b WHERE b.k = a.fk AND ...)` of the WHERE, lowered. An EXISTS by one
/// key becomes the `a.fk IN (SELECT b.k FROM b WHERE ...)` the SEMI path takes: the same rows. A NOT EXISTS is no NOT
/// IN (a NULL matches nothing, where NOT IN's answer is UNKNOWN), and no IN joins by two keys, so those leave the
/// WHERE and are returned, for the plan to carry. Inside the subquery a name qualified by anything but its own table's
/// name is the outer query's, and every other name its own table's (the innermost scope; one it lacks then refuses
/// when its filter binds). The outer query is read by key equalities alone, each of two columns, and the subquery is
/// one table's rows, nothing that makes a row of none (an aggregate) or drops one (LIMIT).
fn lowerExists(a: std.mem.Allocator, root: *c.LpNode) Error![]const Lowered {
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    try flatten(root.u.select.where, &conjuncts, a);
    var kept: std.ArrayList(*c.LpNode) = .empty;
    var out: std.ArrayList(Lowered) = .empty;
    for (conjuncts.items) |n| {
        const not = n.kind == c.LP_EXPR_UNARY_OP and n.u.unary.op == c.LP_UOP_NOT and n.u.unary.operand != null and
            n.u.unary.operand.*.kind == c.LP_EXPR_EXISTS;
        const exists: *c.LpNode = if (not) n.u.unary.operand else n;
        if (exists.kind != c.LP_EXPR_EXISTS) {
            try kept.append(a, n);
            continue;
        }
        const sub_node: *c.LpNode = exists.u.exists.select orelse return error.ExistsSubquery;
        if (sub_node.kind != c.LP_STMT_SELECT) return error.ExistsSubquery;
        const sub = &sub_node.u.select;
        const table = fromTable(sub.from orelse return error.ExistsSubquery) orelse return error.ExistsSubquery;
        if (sub.group_by.count != 0 or sub.having != null or sub.limit != null or sub.window_defs.count != 0 or
            sub.with != null) return error.ExistsSubquery;
        for (0..@intCast(sub.result_columns.count)) |i| {
            const e = sub.result_columns.items[i].*.u.result_column.expr orelse continue;
            switch (e.*.kind) {
                c.LP_EXPR_LITERAL_INT, c.LP_EXPR_LITERAL_FLOAT, c.LP_EXPR_LITERAL_STRING, c.LP_EXPR_LITERAL_NULL => {},
                c.LP_EXPR_LITERAL_BOOL, c.LP_EXPR_COLUMN_REF, c.LP_EXPR_STAR => {},
                else => return error.ExistsSubquery,
            }
        }
        var terms: std.ArrayList(*c.LpNode) = .empty;
        try flatten(sub.where, &terms, a);
        var own: std.ArrayList(*c.LpNode) = .empty;
        var keys: std.ArrayList(*c.LpNode) = .empty;
        var fks: std.ArrayList(*c.LpNode) = .empty;
        for (terms.items) |term| {
            const outer = for (try columnRefs(term, a)) |ref| {
                if (outerRef(ref, table)) break true;
            } else false;
            if (!outer) {
                try own.append(a, term);
                continue;
            }
            if (term.kind != c.LP_EXPR_BINARY_OP or term.u.binary.op != c.LP_OP_EQ) return error.ExistsSubquery;
            const l: *c.LpNode = term.u.binary.left orelse return error.ExistsSubquery;
            const r: *c.LpNode = term.u.binary.right orelse return error.ExistsSubquery;
            if (l.kind != c.LP_EXPR_COLUMN_REF or r.kind != c.LP_EXPR_COLUMN_REF) return error.ExistsSubquery;
            const l_outer = outerRef(l, table);
            if (l_outer == outerRef(r, table)) return error.ExistsSubquery;
            try fks.append(a, if (l_outer) l else r);
            try keys.append(a, if (l_outer) r else l);
        }
        if (keys.items.len == 0) return error.ExistsSubquery;
        // An IN says one key and no NULL rule of NOT EXISTS's: the rest the plan carries.
        if (not or keys.items.len > 1) {
            try out.append(a, .{ .path = table.path, .fks = fks.items, .keys = keys.items, .own = own.items, .anti = not });
            continue;
        }
        const rc = try create(a, c.LP_RESULT_COLUMN);
        rc.u.result_column.expr = keys.items[0];
        const items = try a.alloc([*c]c.LpNode, 1);
        items[0] = rc;
        sub.result_columns = .{ .items = items.ptr, .count = 1, .capacity = 1 };
        sub.where = try andOf(a, own.items);
        n.* = std.mem.zeroes(c.LpNode);
        n.kind = c.LP_EXPR_IN;
        n.u.in.expr = fks.items[0];
        n.u.in.select = sub_node;
        try kept.append(a, n);
    }
    if (out.items.len != 0) root.u.select.where = try andOf(a, kept.items);
    return out.items;
}

/// Whether `ref`, read inside a subquery over `table`, names the outer query: by a qualifier that is not the table's.
fn outerRef(ref: *c.LpNode, table: FromTable) bool {
    const q = ref.u.column_ref.table orelse return false;
    return !std.ascii.eqlIgnoreCase(span(q), table.name());
}

const Refs = struct {
    list: std.ArrayList(*c.LpNode) = .empty,
    a: std.mem.Allocator,
    failed: bool = false,
};

/// Every column reference under `n`, subqueries excluded.
fn columnRefs(n: *c.LpNode, a: std.mem.Allocator) ![]*c.LpNode {
    const Visit = struct {
        fn enter(v: [*c]c.LpVisitor, node: [*c]c.LpNode) callconv(.c) c_int {
            const refs: *Refs = @ptrCast(@alignCast(v.*.user_data));
            if (node.*.kind == c.LP_STMT_SELECT) return 1;
            if (node.*.kind != c.LP_EXPR_COLUMN_REF) return 0;
            refs.list.append(refs.a, node) catch {
                refs.failed = true;
            };
            return 0;
        }
    };
    var refs: Refs = .{ .a = a };
    var visitor: c.LpVisitor = .{ .user_data = &refs, .enter = Visit.enter, .leave = null };
    if (n.kind == c.LP_STMT_SELECT) {
        // The statement itself: walk its parts, not its FROM.
        const s = n.u.select;
        for (0..@intCast(s.result_columns.count)) |i| if (s.result_columns.items[i]) |rc| c.lp_ast_walk(rc, &visitor);
        for (0..@intCast(s.group_by.count)) |i| if (s.group_by.items[i]) |g| c.lp_ast_walk(g, &visitor);
        for (0..@intCast(s.order_by.count)) |i| if (s.order_by.items[i]) |o| {
            if (!orderByAlias(n, o)) c.lp_ast_walk(o, &visitor);
        };
        if (s.having) |h| c.lp_ast_walk(h, &visitor);
    } else c.lp_ast_walk(n, &visitor);
    if (refs.failed) return error.OutOfMemory;
    return refs.list.items;
}

/// Whether ORDER BY term `term` is a select item by its alias: a bare name an item is aliased, which ORDER BY reads
/// as that item whatever columns the tables have of that name. It is no reference to a table, so the binder leaves
/// it as written, for the probe query to resolve as the same alias.
fn orderByAlias(select: *c.LpNode, term: *c.LpNode) bool {
    const e: *c.LpNode = if (term.kind == c.LP_ORDER_TERM) term.u.order_term.expr orelse return false else term;
    if (e.kind != c.LP_EXPR_COLUMN_REF or e.u.column_ref.table != null or e.u.column_ref.column == null) return false;
    const name = span(e.u.column_ref.column);
    const s = select.u.select;
    for (0..@intCast(s.result_columns.count)) |i| {
        const rc = s.result_columns.items[i] orelse continue;
        if (rc.*.kind != c.LP_RESULT_COLUMN) continue;
        // Aliases are their text here (`unquoteAliases`).
        if (std.ascii.eqlIgnoreCase(span(rc.*.u.result_column.alias), name)) return true;
    }
    return false;
}

/// The ORDER BY terms that name a column: a bare reference that is no alias (`orderByAlias`), qualified or not.
fn orderColumns(a: std.mem.Allocator, select: *c.LpNode) ![]*c.LpNode {
    var out: std.ArrayList(*c.LpNode) = .empty;
    const s = select.u.select;
    for (0..@intCast(s.order_by.count)) |i| {
        const term: *c.LpNode = s.order_by.items[i] orelse continue;
        const e: *c.LpNode = term.u.order_term.expr orelse continue;
        if (e.kind == c.LP_EXPR_COLUMN_REF and !orderByAlias(select, term)) try out.append(a, e);
    }
    return out.items;
}

/// The probe query resolves a bare ORDER BY name as a select alias first, and the binder leaves every reference
/// bare. A column an ORDER BY term names (`columns`, taken before any was rewritten) whose name is now an alias too
/// would so rank by another item: qualified by the probe's table (`probe`, its path), it is the column again.
fn keepOrderColumns(a: std.mem.Allocator, select: *c.LpNode, columns: []const *c.LpNode, probe: []const u8) !void {
    const s = select.u.select;
    for (columns) |e| {
        const name = span(e.u.column_ref.column);
        const shadowed = for (0..@intCast(s.result_columns.count)) |i| {
            const rc = s.result_columns.items[i] orelse continue;
            if (rc.*.kind == c.LP_RESULT_COLUMN and std.ascii.eqlIgnoreCase(span(rc.*.u.result_column.alias), name)) break true;
        } else false;
        if (!shadowed) continue;
        const table: FromTable = .{ .path = probe, .alias = null };
        e.u.column_ref.table = (try a.dupeSentinel(u8, table.name(), 0)).ptr;
        e.u.column_ref.quoted |= c.LP_QUOTED_TABLE;
    }
}

const Sides = struct {
    t: Tables,
    /// Original table indices of the tables in play (all of them, or two after a reduction).
    tables: []const usize,
    /// Each table's name for a qualifier (`FromTable.name`), by original index.
    aliases: []const []const u8,

    /// Which table (position in `tables`) a reference names: by its qualifier when it has one, else by which table
    /// has the column. Null for a name no table has (a SELECT alias).
    fn of(s: Sides, ref: *c.LpNode) Error!?usize {
        return s.at(ref);
    }

    fn at(s: Sides, ref: *c.LpNode) Error!?usize {
        const col = span(ref.u.column_ref.column);
        if (ref.u.column_ref.table) |q| {
            // A reference the binder wrote (`lowerUsing`) holds its table's own name string: that table, whatever
            // another is called. Any other qualifier is a name, and two tables of one name (a file read twice without
            // aliases) leave it naming neither.
            for (s.tables, 0..) |ti, i| if (s.aliases[ti].ptr == q) return i;
            var named: ?usize = null;
            for (s.tables, 0..) |ti, i| if (std.ascii.eqlIgnoreCase(s.aliases[ti], span(q))) {
                if (named != null) return error.AmbiguousJoinColumn;
                named = i;
            };
            return named orelse error.UnsupportedJoin;
        }
        var found: ?usize = null;
        for (s.tables, 0..) |ti, i| if (s.t.has(s.t.ctx, ti, col)) {
            if (found != null) return error.AmbiguousJoinColumn;
            found = i;
        };
        return found;
    }

    /// The one side every column of `n` reads, or null when it reads none.
    fn only(s: Sides, n: *c.LpNode, a: std.mem.Allocator) Error!?usize {
        var side: ?usize = null;
        for (try columnRefs(n, a)) |ref| if (try s.of(ref)) |x| {
            if (side != null and side.? != x) return error.JoinResidualNotSupported;
            side = x;
        };
        return side;
    }

    /// Whether `l = x` (positions in `tables`) can be one more part of a key (`Tables.same_key`).
    fn keyable(s: Sides, l: usize, l_ref: *c.LpNode, x: usize, x_ref: *c.LpNode) bool {
        const same = s.t.same_key orelse return true;
        return same(s.t.ctx, s.tables[l], span(l_ref.u.column_ref.column), s.tables[x], span(x_ref.u.column_ref.column));
    }

    /// The tables `n` reads, as a bit set over positions in `tables`.
    fn reads(s: Sides, n: *c.LpNode, a: std.mem.Allocator) Error!u64 {
        var set: u64 = 0;
        for (try columnRefs(n, a)) |ref| if (try s.at(ref)) |i| {
            set |= @as(u64, 1) << @intCast(i);
        };
        return set;
    }
};

/// `L.k = X.m` between two column references of different tables: (L's position, its ref, X's ref).
fn keyEquality(s: Sides, n: *c.LpNode) Error!?struct { l: usize, l_ref: *c.LpNode, x: usize, x_ref: *c.LpNode } {
    if (n.kind != c.LP_EXPR_BINARY_OP or n.u.binary.op != c.LP_OP_EQ) return null;
    const l = n.u.binary.left orelse return null;
    const r = n.u.binary.right orelse return null;
    if (l.*.kind != c.LP_EXPR_COLUMN_REF or r.*.kind != c.LP_EXPR_COLUMN_REF) return null;
    const ls = try s.at(l) orelse return null;
    const rs = try s.at(r) orelse return null;
    if (ls == rs) return null;
    return .{ .l = ls, .l_ref = l, .x = rs, .x_ref = r };
}

fn create(a: std.mem.Allocator, kind: c_uint) !*c.LpNode {
    const n = try a.create(c.LpNode);
    n.* = std.mem.zeroes(c.LpNode);
    n.kind = @intCast(kind);
    return n;
}

/// One table fewer: a table no result, GROUP BY, ORDER BY or HAVING term reads, joined by one key equality to another
/// and otherwise read only by its own conjuncts, is a SEMI join of that other table, `x.m IN (SELECT l.k FROM l WHERE
/// ...)`. Rewrites `sel.where` in place (the other conjuncts and the IN) and returns the position in `s.tables` of the
/// table it took out, or null when no table reduces. `keep`, the probe-to-be, never reduces.
fn reduce(a: std.mem.Allocator, sel: anytype, tables_in: []*c.LpNode, conjuncts: []*c.LpNode, s: Sides, root: *c.LpNode, keep: usize, lowered: []const Lowered) Error!?usize {
    var outside: u64 = 0;
    for (try columnRefs(root, a)) |ref| if (try s.at(ref)) |i| {
        outside |= @as(u64, 1) << @intCast(i);
    };
    // An EXISTS the plan carries reads its table too, on that table's build, which an IN's text could not.
    for (lowered) |x| if (try s.at(x.fks[0])) |i| {
        outside |= @as(u64, 1) << @intCast(i);
    };
    for (s.tables, 0..) |ti, l| {
        const bit = @as(u64, 1) << @intCast(l);
        if (outside & bit != 0 or l == keep) continue;
        var key: ?usize = null;
        var ok = true;
        var own: std.ArrayList(*c.LpNode) = .empty;
        for (conjuncts, 0..) |n, ci| {
            const set = try s.reads(n, a);
            if (set & bit == 0) continue;
            if (set == bit) {
                try own.append(a, n);
                continue;
            }
            const eq = try keyEquality(s, n);
            if (eq == null or key != null or @popCount(set) != 2) {
                ok = false;
                break;
            }
            key = ci;
        }
        if (!ok or key == null) continue;
        const eq = (try keyEquality(s, conjuncts[key.?])).?;
        const l_ref = if (eq.l == l) eq.l_ref else eq.x_ref;
        const x_ref = if (eq.l == l) eq.x_ref else eq.l_ref;
        // SELECT l.k FROM l WHERE <l's own conjuncts>
        const rc = try create(a, c.LP_RESULT_COLUMN);
        rc.u.result_column.expr = l_ref;
        const items = try a.alloc([*c]c.LpNode, 1);
        items[0] = rc;
        const sub = try create(a, c.LP_STMT_SELECT);
        sub.u.select.result_columns = .{ .items = items.ptr, .count = 1, .capacity = 1 };
        sub.u.select.from = tables_in[ti];
        sub.u.select.where = try andOf(a, own.items);
        const in = try create(a, c.LP_EXPR_IN);
        in.u.in.expr = x_ref;
        in.u.in.select = sub;
        var kept: std.ArrayList(*c.LpNode) = .empty;
        for (conjuncts, 0..) |n, ci| {
            if (ci == key.? or (try s.reads(n, a)) == bit) continue;
            try kept.append(a, n);
        }
        try kept.append(a, in);
        sel.where = try andOf(a, kept.items);
        return l;
    }
    return null;
}

/// `x OR y`'s branches, nested ORs flattened.
fn flattenOr(n: *c.LpNode, list: *std.ArrayList(*c.LpNode), a: std.mem.Allocator) !void {
    if (n.kind == c.LP_EXPR_BINARY_OP and n.u.binary.op == c.LP_OP_OR) {
        try flattenOr(n.u.binary.left, list, a);
        try flattenOr(n.u.binary.right, list, a);
        return;
    }
    try list.append(a, n);
}

fn binaryOf(a: std.mem.Allocator, op: c_uint, l: *c.LpNode, r: *c.LpNode) !*c.LpNode {
    const node = try create(a, c.LP_EXPR_BINARY_OP);
    node.u.binary.op = @intCast(op);
    node.u.binary.left = l;
    node.u.binary.right = r;
    return node;
}

/// Whether two key equalities join the same columns of the same tables, either way round.
fn sameEquality(s: Sides, x: *c.LpNode, y: *c.LpNode) !bool {
    const ex = try keyEquality(s, x) orelse return false;
    const ey = try keyEquality(s, y) orelse return false;
    const lx = span(ex.l_ref.*.u.column_ref.column);
    const rx = span(ex.x_ref.*.u.column_ref.column);
    const ly = span(ey.l_ref.*.u.column_ref.column);
    const ry = span(ey.x_ref.*.u.column_ref.column);
    const same = ex.l == ey.l and ex.x == ey.x and std.mem.eql(u8, lx, ly) and std.mem.eql(u8, rx, ry);
    const crossed = ex.l == ey.x and ex.x == ey.l and std.mem.eql(u8, lx, ry) and std.mem.eql(u8, rx, ly);
    return same or crossed;
}

/// PDS-H q19's shape: `(k AND a1 AND b1) OR (k AND a2 AND b2)`, with `k` a key equality every branch holds, is
/// `k AND (a1 OR a2) AND ((a1 AND b1) OR (a2 AND b2))`, where `a` are one table's conditions. Pulled out, `k` joins.
/// Each table every branch holds a condition of gets their OR as a condition of its own (`(a1 OR a2)`, a build's
/// filter or the probe's), which the OR implies, so its rows narrow before the join. What is left of the OR reads
/// both tables and narrows after the lookups (a residual). Null when no OR has such a key. The implied conditions
/// are copies: the planner rewrites a condition's column references in place, as the table it lands on reads them.
fn hoistOrs(a: std.mem.Allocator, p: Parsed, s: Sides, conjuncts: []*c.LpNode) Error!?std.ArrayList(*c.LpNode) {
    var out: std.ArrayList(*c.LpNode) = .empty;
    var changed = false;
    for (conjuncts) |node| {
        if (node.kind != c.LP_EXPR_BINARY_OP or node.u.binary.op != c.LP_OP_OR) {
            try out.append(a, node);
            continue;
        }
        var branches: std.ArrayList(*c.LpNode) = .empty;
        try flattenOr(node, &branches, a);
        const terms = try a.alloc(std.ArrayList(*c.LpNode), branches.items.len);
        for (branches.items, terms) |b, *ts| {
            ts.* = .empty;
            try flatten(b, ts, a);
        }
        // The key equalities of the first branch that every other branch holds too.
        var keys: std.ArrayList(*c.LpNode) = .empty;
        for (terms[0].items) |eq| {
            if (try keyEquality(s, eq) == null) continue;
            const everywhere = for (terms[1..]) |ts| {
                const found = for (ts.items) |other| {
                    if (try sameEquality(s, eq, other)) break true;
                } else false;
                if (!found) break false;
            } else true;
            if (everywhere) try keys.append(a, eq);
        }
        if (keys.items.len == 0) {
            try out.append(a, node);
            continue;
        }
        changed = true;
        try out.appendSlice(a, keys.items);
        // Each branch without them; a branch they were all of is TRUE, and the OR with it.
        var rest: ?*c.LpNode = null;
        var always = false;
        for (terms) |*ts| {
            var kept: std.ArrayList(*c.LpNode) = .empty;
            for (ts.items) |term| {
                const is_key = for (keys.items) |k| {
                    if (try sameEquality(s, k, term)) break true;
                } else false;
                if (!is_key) try kept.append(a, term);
            }
            ts.* = kept;
            const branch = try andOf(a, kept.items) orelse {
                always = true;
                break;
            };
            rest = if (rest) |r| try binaryOf(a, c.LP_OP_OR, r, branch) else branch;
        }
        if (always) continue;
        // Each table's own conditions, ORed across the branches when every branch has some.
        for (0..s.tables.len) |ti| {
            const bit = @as(u64, 1) << @intCast(ti);
            var implied: ?*c.LpNode = null;
            const all = for (terms) |ts| {
                var mine: std.ArrayList(*c.LpNode) = .empty;
                for (ts.items) |term| if ((try s.reads(term, a)) == bit) {
                    try mine.append(a, c.lp_node_clone(p.arena, term) orelse return error.OutOfMemory);
                };
                const branch = try andOf(a, mine.items) orelse break false;
                implied = if (implied) |acc| try binaryOf(a, c.LP_OP_OR, acc, branch) else branch;
            } else true;
            if (all) try out.append(a, implied.?);
        }
        try out.append(a, rest.?);
    }
    return if (changed) out else null;
}

fn andOf(a: std.mem.Allocator, nodes: []const *c.LpNode) !?*c.LpNode {
    if (nodes.len == 0) return null;
    var acc = nodes[0];
    for (nodes[1..]) |n| {
        const and_node = try a.create(c.LpNode);
        and_node.* = std.mem.zeroes(c.LpNode);
        and_node.kind = c.LP_EXPR_BINARY_OP;
        and_node.u.binary.op = c.LP_OP_AND;
        and_node.u.binary.left = acc;
        and_node.u.binary.right = n;
        acc = and_node;
    }
    return acc;
}

fn toSql(a: std.mem.Allocator, p: Parsed, n: *c.LpNode) ![]const u8 {
    const text = c.lp_ast_to_sql(n, p.arena) orelse return error.SqlParseError;
    return a.dupe(u8, std.mem.span(text));
}

/// USING publishes the left key unqualified. Lower the single-key two-table form to the existing ON machinery.
fn lowerUsing(a: std.mem.Allocator, root: *c.LpNode, t: Tables) Error!void {
    const sel = &root.u.select;
    const from = sel.from orelse return;
    if (from.*.kind != c.LP_JOIN_CLAUSE) return;
    const j = &from.*.u.join;
    if (j.using_columns.count == 0) return;
    if (j.using_columns.count != 1 or t.paths.len != 2) return error.UsingJoinNotSupported;
    const left = j.left orelse return error.UsingJoinNotSupported;
    const right = j.right orelse return error.UsingJoinNotSupported;
    if (left.*.kind != c.LP_FROM_TABLE or right.*.kind != c.LP_FROM_TABLE) return error.UsingJoinNotSupported;
    const key = j.using_columns.items[0] orelse return error.UsingJoinNotSupported;
    if (key.*.kind != c.LP_EXPR_COLUMN_REF) return error.UsingJoinNotSupported;
    const name = span(key.*.u.column_ref.column);
    if (!t.has(t.ctx, 0, name) or !t.has(t.ctx, 1, name)) return error.JoinKeyNotFound;
    // Each side goes by its table's name, made its alias here: the key equality names each table by that very string,
    // which tells even two tables of one name apart (`Sides.at`).
    if (left.*.u.from_table.alias == null) left.*.u.from_table.alias = (try a.dupeSentinel(u8, fromTable(left).?.name(), 0)).ptr;
    if (right.*.u.from_table.alias == null) right.*.u.from_table.alias = (try a.dupeSentinel(u8, fromTable(right).?.name(), 0)).ptr;
    const l = try create(a, c.LP_EXPR_COLUMN_REF);
    l.u.column_ref.column = key.*.u.column_ref.column;
    l.u.column_ref.table = left.*.u.from_table.alias;
    const r = try create(a, c.LP_EXPR_COLUMN_REF);
    r.u.column_ref.column = key.*.u.column_ref.column;
    r.u.column_ref.table = right.*.u.from_table.alias;
    j.on_expr = try binaryOf(a, c.LP_OP_EQ, l, r);
    j.using_columns.count = 0;
    var refs = try columnRefs(root, a);
    if (sel.where) |w| refs = try std.mem.concat(a, *c.LpNode, &.{ refs, try columnRefs(w, a) });
    for (refs) |ref| if (ref.u.column_ref.table == null and std.ascii.eqlIgnoreCase(span(ref.u.column_ref.column), name)) {
        ref.u.column_ref.table = left.*.u.from_table.alias;
    };
}

/// Split `sql` into the probe query and the build specs: one build for a two-table join or an IN, one per dimension for
/// a star (each joined to the probe by one key equality), after every filter-only table is reduced to an IN.
pub fn plan(a: std.mem.Allocator, sql: [:0]const u8, t: Tables) Error!Plan {
    const p = try parse(sql);
    defer p.deinit();
    const lowered = try lowerExists(a, p.root);
    try lowerUsing(a, p.root, t);
    const sel = &p.root.u.select;
    const from = sel.from orelse return error.UnsupportedJoin;
    if (from.*.kind != c.LP_JOIN_CLAUSE) return planSemi(a, p, sel, from, t, lowered);
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    var list: std.ArrayList(*c.LpNode) = .empty;
    var nulls: std.ArrayList(NullSide) = .empty;
    try flattenFrom(from, &list, &conjuncts, &nulls, a);
    try flatten(sel.where, &conjuncts, a);
    if (list.items.len != t.paths.len or list.items.len > 64) return error.UnsupportedJoin;
    const aliases = try a.alloc([]const u8, list.items.len);
    for (list.items, aliases) |n, *al| al.* = fromTable(n).?.name();
    // The tables still in play, by original index; filter-only ones reduce to INs one at a time.
    var live: std.ArrayList(usize) = .empty;
    for (0..list.items.len) |i| try live.append(a, i);
    var reduced = false;
    // An OR whose every branch holds one key equality joins by it (`hoistOrs`): the WHERE then holds the equality,
    // each table's own conditions the branches imply, and the rest of the OR as a condition across tables.
    {
        const all: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
        if (try hoistOrs(a, p, all, conjuncts.items)) |rewritten| {
            conjuncts = rewritten;
            sel.where = try andOf(a, conjuncts.items);
        }
    }
    // A LEFT JOIN's null-supplying table is read by its ON alone: a WHERE or INNER ON condition on it would need its
    // NULL rows, and one joining it to a third table would stack a join on the join. Having no conjunct, it never reduces.
    var null_set: u64 = 0;
    for (nulls.items) |ns| null_set |= @as(u64, 1) << @intCast(ns.table);
    if (null_set != 0) {
        const all: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
        for (conjuncts.items) |node| if ((try all.reads(node, a)) & null_set != 0) return error.LeftJoinWhere;
        for (lowered) |x| if (try all.at(x.fks[0])) |i| {
            if (null_set & (@as(u64, 1) << @intCast(i)) != 0) return error.LeftJoinWhere;
        };
    }
    // The probe is chosen once, over every table a LEFT JOIN keeps: the reductions that follow take tables out around it.
    const center = if (t.root) |root| blk: {
        if (null_set != 0 or std.mem.indexOfScalar(usize, live.items, root) == null) return error.UnsupportedJoin;
        break :blk root;
    } else live.items[largest(t, live.items, null_set)];
    while (live.items.len > 2) {
        const sides: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
        const keep = std.mem.indexOfScalar(usize, live.items, center).?;
        const out = try reduce(a, sel, list.items, conjuncts.items, sides, p.root, keep, lowered) orelse break;
        _ = live.orderedRemove(out);
        reduced = true;
        conjuncts.clearRetainingCapacity();
        try flatten(sel.where, &conjuncts, a);
    }
    const sides: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
    const n = live.items.len;
    const probe = std.mem.indexOfScalar(usize, live.items, center).?;
    // The join tree: each other table's parent (the table holding its FK), its FK there and its key. Breadth first
    // from the probe, each table takes the first key equality joining it to the level before: a star's dimensions,
    // then theirs (a snowflake). Every other conjunct reads one table; a second edge (a cycle) refuses as a residual.
    const fks = try a.alloc(?[]const u8, n);
    const keys = try a.alloc(?[]const u8, n);
    const parent = try a.alloc(usize, n);
    @memset(fks, null);
    @memset(keys, null);
    @memset(parent, probe);
    const wheres = try a.alloc(std.ArrayList(*c.LpNode), n);
    @memset(wheres, .empty);
    const used = try a.alloc(bool, conjuncts.items.len);
    @memset(used, false);
    // A LEFT JOIN's null-supplying table joins the probe by its ON alone.
    var null_live: u64 = 0;
    for (nulls.items) |ns| null_live |= @as(u64, 1) << @intCast(std.mem.indexOfScalar(usize, live.items, ns.table).?);
    var in_tree: u64 = @as(u64, 1) << @intCast(probe);
    var level = in_tree;
    while (level != 0) {
        var next: u64 = 0;
        for (conjuncts.items, used) |node, *u| {
            const eq = try keyEquality(sides, node) orelse continue;
            const l_up = level & (@as(u64, 1) << @intCast(eq.l)) != 0;
            if (l_up == (level & (@as(u64, 1) << @intCast(eq.x)) != 0)) continue;
            const up = if (l_up) eq.l else eq.x;
            const dn = if (l_up) eq.x else eq.l;
            const bit = @as(u64, 1) << @intCast(dn);
            if ((in_tree | null_live) & bit != 0) continue;
            parent[dn] = up;
            fks[dn] = try a.dupe(u8, span((if (l_up) eq.l_ref else eq.x_ref).*.u.column_ref.column));
            keys[dn] = try a.dupe(u8, span((if (l_up) eq.x_ref else eq.l_ref).*.u.column_ref.column));
            in_tree |= bit;
            next |= bit;
            u.* = true;
        }
        level = next;
    }
    // Every further key equality between a table and its parent is part of that edge's key: a composite key. One of
    // columns no key takes together (a DOUBLE, two types) stays what it was, a condition across the two tables.
    const fks_more = try a.alloc(std.ArrayList([]const u8), n);
    const keys_more = try a.alloc(std.ArrayList([]const u8), n);
    @memset(fks_more, .empty);
    @memset(keys_more, .empty);
    for (conjuncts.items, used) |node, *u| {
        if (u.*) continue;
        const eq = try keyEquality(sides, node) orelse continue;
        const dn = if (eq.l != probe and keys[eq.l] != null and parent[eq.l] == eq.x)
            eq.l
        else if (eq.x != probe and keys[eq.x] != null and parent[eq.x] == eq.l)
            eq.x
        else
            continue;
        if (!sides.keyable(eq.l, eq.l_ref, eq.x, eq.x_ref)) continue;
        try fks_more[dn].append(a, try columnOf(a, if (dn == eq.l) eq.x_ref else eq.l_ref));
        try keys_more[dn].append(a, try columnOf(a, if (dn == eq.l) eq.l_ref else eq.x_ref));
        u.* = true;
    }
    // A condition across tables (a cycle of the tree, or a build column against the probe's) stays the probe's: it
    // narrows after the lookups, over the columns they publish (`filter_ast.Residual`).
    var residuals: std.ArrayList(*c.LpNode) = .empty;
    for (conjuncts.items, used) |node, u| {
        if (u) continue;
        if (@popCount(try sides.reads(node, a)) > 1) {
            try residuals.append(a, node);
            try wheres[probe].append(a, node);
            continue;
        }
        const side = try sides.only(node, a) orelse probe;
        try wheres[side].append(a, node);
    }
    // Each LEFT JOIN's ON: its key equalities with the probe, and conditions on its own table (its build's filter).
    const left = try a.alloc(bool, n);
    @memset(left, false);
    for (nulls.items) |ns| {
        const d = std.mem.indexOfScalar(usize, live.items, ns.table).?;
        left[d] = true;
        const bit = @as(u64, 1) << @intCast(d);
        for (ns.on.items) |node| {
            if (try keyEquality(sides, node)) |eq| if ((eq.l == probe and eq.x == d) or (eq.x == probe and eq.l == d)) {
                if (keys[d] == null) {
                    const fk_ref = if (eq.l == probe) eq.l_ref else eq.x_ref;
                    const key_ref = if (eq.l == probe) eq.x_ref else eq.l_ref;
                    fks[d] = try a.dupe(u8, span(fk_ref.*.u.column_ref.column));
                    keys[d] = try a.dupe(u8, span(key_ref.*.u.column_ref.column));
                    continue;
                }
                if (sides.keyable(eq.l, eq.l_ref, eq.x, eq.x_ref)) {
                    try fks_more[d].append(a, try columnOf(a, if (eq.l == probe) eq.l_ref else eq.x_ref));
                    try keys_more[d].append(a, try columnOf(a, if (eq.l == probe) eq.x_ref else eq.l_ref));
                    continue;
                }
            };
            if ((try sides.reads(node, a)) & ~bit != 0) return error.LeftJoinOn;
            try wheres[d].append(a, node);
        }
    }
    for (0..n) |i| if (i != probe and keys[i] == null) return error.JoinKeyNotFound;
    // The probe query: its own FROM, its own conjuncts, unqualified names. A table's key reads as its FK, up the tree
    // to the probe's own column or a table's that publishes it; a LEFT JOIN's key is NULL on a miss, where the FK is
    // not, so it reads as a build column.
    const columns = try a.alloc(std.ArrayList([]const u8), n);
    @memset(columns, .empty);
    // A selected key keeps its own name, as DuckDB names it, though it reads as the FK: an unaliased result column
    // that is one column reference, with the name it had.
    const named = try a.alloc(?[]const u8, @intCast(sel.result_columns.count));
    for (named, 0..) |*nm, i| {
        const rc = sel.result_columns.items[i];
        const e = rc.*.u.result_column.expr;
        nm.* = if (rc.*.u.result_column.alias == null and e != null and e.*.kind == c.LP_EXPR_COLUMN_REF) span(e.*.u.column_ref.column) else null;
    }
    const order_columns = try orderColumns(a, p.root);
    var refs: std.ArrayList(*c.LpNode) = .empty;
    try refs.appendSlice(a, try columnRefs(p.root, a));
    for (residuals.items) |node| try refs.appendSlice(a, try columnRefs(node, a));
    // Each reference to a build column: its build and its place among that build's columns.
    const Read = struct { ref: *c.LpNode, d: usize, at: usize };
    var reads: std.ArrayList(Read) = .empty;
    for (refs.items) |ref| {
        const side = try sides.of(ref);
        ref.*.u.column_ref.table = null;
        var d = side orelse continue;
        var name = span(ref.*.u.column_ref.column);
        while (d != probe and !left[d]) {
            name = if (std.mem.eql(u8, name, keys[d].?)) fks[d].? else for (keys_more[d].items, fks_more[d].items) |k, f| {
                if (std.mem.eql(u8, name, k)) break f;
            } else break;
            d = parent[d];
        }
        if (name.ptr != span(ref.*.u.column_ref.column).ptr) ref.*.u.column_ref.column = (try a.dupeSentinel(u8, name, 0)).ptr;
        if (d != probe) try reads.append(a, .{ .ref = ref, .d = d, .at = try addName(a, &columns[d], name) });
    }
    // A snowflake's table reads its FK off its parent's build: the parent publishes it.
    for (0..n) |d| if (d != probe and parent[d] != probe) {
        _ = try addName(a, &columns[parent[d]], fks[d].?);
        for (fks_more[d].items) |f| _ = try addName(a, &columns[parent[d]], f);
    };
    // A build column reaches the probe as a column of its own name. One whose name the probe has too, or another
    // build publishes (a role-playing dimension's `d1.d_date` and `d2.d_date`, a self join's `e.name` and `m.name`),
    // would be one ambiguous column there: it goes by a name of its own instead, which its references then read.
    const published = try a.alloc([]const []const u8, n);
    var taken: std.ArrayList([]const u8) = .empty;
    for (0..n) |d| {
        const names = try a.alloc([]const u8, columns[d].items.len);
        published[d] = names;
        for (columns[d].items, names) |name, *as| {
            const shared = t.has(t.ctx, live.items[probe], name) or for (0..n) |e| {
                const theirs = e != d and for (columns[e].items) |y| {
                    if (std.ascii.eqlIgnoreCase(name, y)) break true;
                } else false;
                if (theirs) break true;
            } else false;
            as.* = name;
            if (!shared) continue;
            as.* = try publishedName(a, sql, t, taken.items, aliases[live.items[d]], name);
            try taken.append(a, as.*);
        }
    }
    var moved: std.ArrayList(*c.LpNode) = .empty;
    for (reads.items) |r| {
        const as = published[r.d][r.at];
        if (as.ptr == columns[r.d].items[r.at].ptr) continue;
        const ref = &r.ref.*.u.column_ref;
        ref.column = (try a.dupeSentinel(u8, as, 0)).ptr;
        ref.quoted &= ~@as(@TypeOf(ref.quoted), c.LP_QUOTED_COLUMN);
        try moved.append(a, r.ref);
    }
    for (named, 0..) |nm, i| if (nm) |name| {
        const rc = sel.result_columns.items[i];
        if (span(rc.*.u.result_column.expr.*.u.column_ref.column).ptr != name.ptr)
            rc.*.u.result_column.alias = (try a.dupeSentinel(u8, name, 0)).ptr;
    };
    // An unaliased expression is named by its text, and a column that goes by another name there would name it by
    // one the query never wrote: it needs AS.
    for (0..@intCast(sel.result_columns.count)) |i| {
        const rc = sel.result_columns.items[i];
        const e = rc.*.u.result_column.expr orelse continue;
        if (rc.*.u.result_column.alias != null or e.*.kind == c.LP_EXPR_COLUMN_REF) continue;
        for (try columnRefs(e, a)) |ref| if (std.mem.indexOfScalar(*c.LpNode, moved.items, ref) != null) return error.JoinColumnNeedsAlias;
    }
    try keepOrderColumns(a, p.root, order_columns, t.paths[live.items[probe]]);
    const probe_from = list.items[live.items[probe]];
    probe_from.*.u.from_table.alias = null;
    sel.from = probe_from;
    sel.where = try andOf(a, wheres[probe].items);
    if (sel.where) |w| for (try columnRefs(w, a)) |ref| {
        ref.*.u.column_ref.table = null;
    };
    // The main build: the dimension a GROUP BY key names the FK of (a group-join's), else the first.
    var main: usize = for (0..n) |d| {
        if (d != probe and parent[d] == probe) break d;
    } else unreachable;
    for (0..@intCast(sel.group_by.count)) |gi| if (sel.group_by.items[gi]) |g| if (g.*.kind == c.LP_EXPR_COLUMN_REF) {
        const name = span(g.*.u.column_ref.column);
        for (0..n) |d| if (d != probe and parent[d] == probe and std.mem.eql(u8, name, fks[d].?)) {
            main = d;
        };
    };
    // Each carried EXISTS, on the one table its FKs are columns of: the probe's own beside its lookups, a build's
    // inside it.
    const semis = try a.alloc(std.ArrayList(Exists), n);
    @memset(semis, .empty);
    for (lowered) |x| {
        const d = try sides.at(x.fks[0]) orelse return error.UnsupportedJoin;
        for (x.fks[1..]) |ref| if ((try sides.at(ref)) != d) return error.ExistsSubquery;
        try semis[d].append(a, try x.planned(a, p));
    }
    const tree: Tree = .{ .t = t, .p = p, .live = live.items, .probe = probe, .parent = parent, .fks = fks, .keys = keys, .fks_more = fks_more, .keys_more = keys_more, .wheres = wheres, .columns = columns, .published = published, .left = left, .semis = semis };
    var dims: std.ArrayList(Dim) = .empty;
    for (0..n) |d| if (d != probe and d != main and parent[d] == probe) try dims.append(a, try tree.dim(a, d));
    return .{
        .kind = if (left[main]) .left else .inner,
        .probe_path = t.paths[live.items[probe]],
        .build_path = t.paths[live.items[main]],
        .fk = fks[main].?,
        .build_key = keys[main].?,
        .fk_more = fks_more[main].items,
        .build_key_more = keys_more[main].items,
        .probe_sql = try toSql(a, p, p.root),
        .build_where = try whereSql(a, p, wheres[main].items),
        .build_columns = columns[main].items,
        .build_columns_as = tree.columnsAs(main),
        .build_dims = try tree.children(a, main),
        .reduced = reduced,
        .semis = semis[probe].items,
        .build_semis = semis[main].items,
        .dims = dims.items,
    };
}

/// `name`'s place in `list`, added when new.
fn addName(a: std.mem.Allocator, list: *std.ArrayList([]const u8), name: []const u8) !usize {
    for (list.items, 0..) |b, i| if (std.mem.eql(u8, b, name)) return i;
    try list.append(a, try a.dupe(u8, name));
    return list.items.len - 1;
}

/// The name a build column goes by in the probe query when its own would be ambiguous there: its table's name and its
/// own, as one identifier. No table of the join has a column of that name, no other build column goes by it, and the
/// query spells it nowhere (underscores are added until all three hold), so nothing the query wrote can mean it.
fn publishedName(a: std.mem.Allocator, sql: []const u8, t: Tables, taken: []const []const u8, table: []const u8, column: []const u8) ![]const u8 {
    var name: std.ArrayList(u8) = .empty;
    for ([_][]const u8{ table, "_", column }) |part| for (part) |ch| {
        try name.append(a, if (std.ascii.isAlphanumeric(ch) or ch == '_') ch else '_');
    };
    if (std.ascii.isDigit(name.items[0])) try name.insert(a, 0, '_');
    while (true) : (try name.append(a, '_')) {
        if (spells(sql, name.items)) continue;
        const used = for (taken) |other| {
            if (std.ascii.eqlIgnoreCase(other, name.items)) break true;
        } else for (0..t.paths.len) |ti| {
            if (t.has(t.ctx, ti, name.items)) break true;
        } else false;
        if (!used) return name.items;
    }
}

/// The join tree `plan` resolved, by position in `live`: each table's build, with its own snowflake under it.
const Tree = struct {
    t: Tables,
    p: Parsed,
    live: []const usize,
    probe: usize,
    parent: []const usize,
    fks: []const ?[]const u8,
    keys: []const ?[]const u8,
    fks_more: []const std.ArrayList([]const u8),
    keys_more: []const std.ArrayList([]const u8),
    wheres: []const std.ArrayList(*c.LpNode),
    columns: []const std.ArrayList([]const u8),
    published: []const []const []const u8,
    left: []const bool,
    semis: []const std.ArrayList(Exists),

    /// `d`'s columns' names in the probe query when any is not its own, else none: a plan that renames nothing is
    /// the plan it was.
    fn columnsAs(tr: Tree, d: usize) []const []const u8 {
        for (tr.columns[d].items, tr.published[d]) |name, as| if (name.ptr != as.ptr) return tr.published[d];
        return &.{};
    }

    fn dim(tr: Tree, a: std.mem.Allocator, d: usize) Error!Dim {
        return .{
            .path = tr.t.paths[tr.live[d]],
            .fk = tr.fks[d].?,
            .key = tr.keys[d].?,
            .fk_more = tr.fks_more[d].items,
            .key_more = tr.keys_more[d].items,
            .where = try whereSql(a, tr.p, tr.wheres[d].items),
            .columns = tr.columns[d].items,
            .columns_as = tr.columnsAs(d),
            .left = tr.left[d],
            .dims = try tr.children(a, d),
            .semis = tr.semis[d].items,
        };
    }

    /// The tables joined to `d` by an FK of its own.
    fn children(tr: Tree, a: std.mem.Allocator, d: usize) Error![]const Dim {
        var out: std.ArrayList(Dim) = .empty;
        for (0..tr.parent.len) |x| if (x != tr.probe and x != d and tr.parent[x] == d) try out.append(a, try tr.dim(a, x));
        return out.items;
    }
};

/// The probe, as a position in `live`: the largest table by footer rows (a star's fact), the first of equals, of the
/// tables outside `skip` (original indices: LEFT JOINs' null-supplying ones). The FROM's first table is never skipped.
fn largest(t: Tables, live: []const usize, skip: u64) usize {
    var at: ?usize = null;
    for (live, 0..) |ti, i| {
        if (skip & (@as(u64, 1) << @intCast(ti)) != 0) continue;
        if (at == null or t.rows[ti] > t.rows[live[at.?]]) at = i;
    }
    return at.?;
}

/// A build's own conjuncts as one SQL expression with unqualified names, or null.
fn whereSql(a: std.mem.Allocator, p: Parsed, nodes: []const *c.LpNode) Error!?[]const u8 {
    const w = try andOf(a, nodes) orelse return null;
    for (try columnRefs(w, a)) |ref| ref.*.u.column_ref.table = null;
    return try toSql(a, p, w);
}

/// `FROM t WHERE ... t.k IN (SELECT b.k FROM b WHERE ...)`: a SEMI join of `t` and the subquery's table. The join is
/// the first IN, the others staying in the probe query; without an IN it is the first NOT EXISTS (`lowered`), and
/// every other NOT EXISTS is an ANTI join of the probe beside it.
fn planSemi(a: std.mem.Allocator, p: Parsed, sel: anytype, from: *c.LpNode, t: Tables, lowered: []const Lowered) Error!Plan {
    const semi = semiConjunct(sel.where);
    if (semi == null and lowered.len == 0) return error.UnsupportedJoin;
    const t0 = fromTable(from) orelse return error.UnsupportedJoin;
    // What qualifies the outer table's columns. The binder's own text has no alias left on its FROM, and a qualifier
    // there is a stale one inside a subquery: it names another scope's table, whatever this file is called.
    const outer_name: ?[]const u8 = if (t.user_from) t0.name() else t0.alias;
    for (lowered) |x| for (x.fks) |ref| if (!qualifies(outer_name, ref)) return error.UnsupportedJoin;
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    try flatten(sel.where, &conjuncts, a);
    var probe_conj: std.ArrayList(*c.LpNode) = .empty;
    var build_conj: std.ArrayList(*c.LpNode) = .empty;
    var fk: []const u8 = "";
    var build_key: []const u8 = "";
    var fk_more: []const []const u8 = &.{};
    var build_key_more: []const []const u8 = &.{};
    var rest = lowered;
    if (semi == null) {
        fk = try columnOf(a, lowered[0].fks[0]);
        build_key = try columnOf(a, lowered[0].keys[0]);
        fk_more = try columnsOf(a, lowered[0].fks[1..]);
        build_key_more = try columnsOf(a, lowered[0].keys[1..]);
        try build_conj.appendSlice(a, lowered[0].own);
        rest = lowered[1..];
    }
    for (conjuncts.items) |n| {
        if (semi != null and n == semi.?.node) {
            const sub = &n.u.in.select.*.u.select;
            if (sub.result_columns.count != 1 or sub.group_by.count != 0 or sub.having != null) return error.UnsupportedJoin;
            // What else a subquery says changes its rows, so it refuses rather than drop: LIMIT and OFFSET (ORDER BY
            // alone orders a set IN does not, and DISTINCT is what IN does).
            if (sub.limit != null) return error.SubqueryLimitNotSupported;
            if (sub.window_defs.count != 0 or sub.with != null) return error.UnsupportedJoin;
            const sub_table = fromTable(sub.from orelse return error.UnsupportedJoin) orelse return error.UnsupportedJoin;
            const rc = sub.result_columns.items[0] orelse return error.UnsupportedJoin;
            const e = rc.*.u.result_column.expr orelse return error.UnsupportedJoin;
            if (e.*.kind != c.LP_EXPR_COLUMN_REF) return error.JoinKeyNotFound;
            // The FK is the outer table's, by its qualifier when it has one.
            if (!qualifies(outer_name, n.u.in.expr)) return error.UnsupportedJoin;
            // Every name the subquery reads is its own table's: a qualifier naming another table, or a name only the
            // outer table has, is a correlated reference, which this release does not join.
            var own = try columnRefs(n.u.in.select, a);
            if (sub.where) |w| own = try std.mem.concat(a, *c.LpNode, &.{ own, try columnRefs(w, a) });
            for (own) |ref| {
                if (outerRef(ref, sub_table)) return error.CorrelatedSubqueryNotSupported;
                const col = span(ref.*.u.column_ref.column);
                if (!t.has(t.ctx, 1, col))
                    return if (t.has(t.ctx, 0, col)) error.CorrelatedSubqueryNotSupported else error.JoinKeyNotFound;
            }
            fk = try a.dupe(u8, span(n.u.in.expr.*.u.column_ref.column));
            build_key = try a.dupe(u8, span(e.*.u.column_ref.column));
            try flatten(sub.where, &build_conj, a);
            continue;
        }
        try probe_conj.append(a, n);
    }
    if (fk.len == 0) return error.JoinKeyNotFound;
    from.*.u.from_table.alias = null;
    sel.where = try andOf(a, probe_conj.items);
    // The subquery's table is not in scope outside it: every other name is the probe's, or a build column an earlier
    // join published under its own name (which the subquery's table may share), and a qualifier names the probe.
    const order_columns = try orderColumns(a, p.root);
    var outer = try columnRefs(p.root, a);
    if (sel.where) |w| outer = try std.mem.concat(a, *c.LpNode, &.{ outer, try columnRefs(w, a) });
    for (outer) |ref| {
        if (!qualifies(outer_name, ref)) return error.UnsupportedJoin;
        ref.*.u.column_ref.table = null;
    }
    try keepOrderColumns(a, p.root, order_columns, t.paths[0]);
    const semis = try a.alloc(Exists, rest.len);
    for (rest, semis) |x, *out| out.* = try x.planned(a, p);
    return .{
        .kind = if (semi) |in| (if (in.anti) .anti else .semi) else if (lowered[0].anti) .anti else .semi,
        .probe_path = t.paths[0],
        .build_path = t.paths[1],
        .fk = fk,
        .build_key = build_key,
        .probe_sql = try toSql(a, p, p.root),
        .build_where = try whereSql(a, p, build_conj.items),
        .build_columns = &.{},
        .fk_more = fk_more,
        .build_key_more = build_key_more,
        .not_exists = semi == null and lowered[0].anti,
        .semis = semis,
    };
}

/// Whether `ref` may read a table named `name`: unqualified, or qualified by that name.
fn qualifies(name: ?[]const u8, ref: *c.LpNode) bool {
    const q = ref.u.column_ref.table orelse return true;
    return if (name) |n| std.ascii.eqlIgnoreCase(span(q), n) else false;
}

test "a snowflake plans as a tree: a dimension's dimension under its build, its key read as the FK" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "amt" }, &.{ "pk", "grp", "w" }, &.{ "gk", "gname" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet", "g.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100, 4 }, .ctx = undefined };
    const p = try plan(a, "SELECT gname, g.gk, sum(amt) AS s FROM 'f.parquet' f, 'd.parquet' d, 'g.parquet' g WHERE f.fk = d.pk AND d.grp = g.gk AND d.w > 2 GROUP BY gname, g.gk", t);
    try std.testing.expectEqualStrings("d.parquet", p.build_path);
    try std.testing.expectEqual(@as(usize, 0), p.dims.len);
    // The chained table's key reads as the FK its parent publishes; its own column publishes through the parent.
    try std.testing.expectEqual(@as(usize, 1), p.build_columns.len);
    try std.testing.expectEqualStrings("grp", p.build_columns[0]);
    try std.testing.expectEqualStrings("w > 2", p.build_where.?);
    try std.testing.expectEqual(@as(usize, 1), p.build_dims.len);
    const g = p.build_dims[0];
    try std.testing.expectEqualStrings("grp", g.fk);
    try std.testing.expectEqualStrings("gk", g.key);
    try std.testing.expectEqual(@as(usize, 1), g.columns.len);
    try std.testing.expectEqualStrings("gname", g.columns[0]);
    try std.testing.expect(std.mem.indexOf(u8, p.probe_sql, "grp") != null);
    // A second edge to a table already in the tree is a cycle: a residual of the probe, over the published key.
    const cyc = try plan(a, "SELECT gname, count(*) AS n FROM 'f.parquet' f, 'd.parquet' d, 'g.parquet' g WHERE f.fk = d.pk AND d.grp = g.gk AND f.amt = g.gk GROUP BY gname", t);
    try std.testing.expect(std.mem.indexOf(u8, cyc.probe_sql, "WHERE grp = amt") != null);
}

test "an OR whose every branch holds a key equality joins by it, with each table's implied conditions its own" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "amt", "q" }, &.{ "pk", "grp", "w" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100 }, .ctx = undefined };
    const p = try plan(a, "SELECT sum(amt) AS s FROM 'f.parquet' f, 'd.parquet' d WHERE (f.fk = d.pk AND d.grp = 1 AND f.q < 3) OR (d.pk = f.fk AND d.w > 7)", t);
    try std.testing.expectEqualStrings("fk", p.fk);
    try std.testing.expectEqualStrings("pk", p.build_key);
    // The build narrows by the OR of its own conditions; the probe keeps the whole OR (a residual) and no FK equality.
    try std.testing.expect(std.mem.indexOf(u8, p.build_where.?, "OR") != null);
    try std.testing.expect(std.mem.indexOf(u8, p.probe_sql, "OR") != null);
    try std.testing.expect(std.mem.indexOf(u8, p.probe_sql, "= pk") == null);
    // Without the key in every branch there is no join to find.
    try std.testing.expectError(error.JoinKeyNotFound, plan(a, "SELECT sum(amt) AS s FROM 'f.parquet' f, 'd.parquet' d WHERE (f.fk = d.pk AND d.grp = 1) OR d.grp = 2", t));
}

test "EXISTS lowers to the IN a SEMI join takes; NOT EXISTS leaves the SQL and rides the plan" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "amt" }, &.{ "pk", "grp", "w" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100 }, .ctx = undefined };
    // Alone, a NOT EXISTS is the join.
    const p = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f WHERE f.amt > 1 AND NOT EXISTS (SELECT 1 FROM 'd.parquet' d WHERE d.pk = f.fk AND d.w > 2)", t);
    try std.testing.expectEqual(Kind.anti, p.kind);
    try std.testing.expect(p.not_exists);
    try std.testing.expectEqualStrings("fk", p.fk);
    try std.testing.expectEqualStrings("pk", p.build_key);
    try std.testing.expectEqualStrings("w > 2", p.build_where.?);
    try std.testing.expect(std.mem.endsWith(u8, p.probe_sql, "WHERE amt > 1"));
    // Beside an IN (here an EXISTS's), it is an ANTI join of the probe the plan carries: the probe query holds neither.
    const two = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f WHERE EXISTS (SELECT 1 FROM 'd.parquet' d WHERE f.fk = d.pk) AND NOT EXISTS (SELECT * FROM 'd.parquet' x WHERE x.grp = f.amt AND x.w < 9)", t);
    try std.testing.expectEqual(Kind.semi, two.kind);
    try std.testing.expect(!two.not_exists);
    try std.testing.expectEqual(@as(usize, 1), two.semis.len);
    try std.testing.expectEqualStrings("amt", two.semis[0].fk);
    try std.testing.expectEqualStrings("grp", two.semis[0].key);
    try std.testing.expectEqualStrings("d.parquet", two.semis[0].path);
    try std.testing.expectEqualStrings("w < 9", two.semis[0].where.?);
    try std.testing.expect(std.mem.indexOf(u8, two.probe_sql, "WHERE") == null);
    // Nothing a query can spell makes a NOT IN a NOT EXISTS: whatever the subquery calls its column, the plan is one.
    const plain = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f WHERE f.fk NOT IN (SELECT pk FROM 'd.parquet')", t);
    try std.testing.expectEqual(Kind.anti, plain.kind);
    try std.testing.expect(!plain.not_exists);
    for ([_][:0]const u8{ "__zpq_not_exists", "__zpq_using_left", "__zpq_using_right", "__zpq_group_all", "__zpq_order_0", "not_exists" }) |alias| {
        const sql = try std.fmt.allocPrintSentinel(a, "SELECT count(*) AS {s} FROM 'f.parquet' {s} WHERE {s}.fk NOT IN (SELECT pk AS {s} FROM 'd.parquet' {s})", .{ alias, alias, alias, alias, alias }, 0);
        const named = try plan(a, sql, t);
        try std.testing.expectEqual(Kind.anti, named.kind);
        try std.testing.expect(!named.not_exists);
        try std.testing.expectEqual(@as(usize, 0), named.semis.len);
        try std.testing.expectEqualStrings(plain.fk, named.fk);
        try std.testing.expectEqualStrings(plain.build_key, named.build_key);
    }
    // In a join it goes to the table its FK is a column of, which then stays a build.
    const j = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f, 'd.parquet' d WHERE f.fk = d.pk AND NOT EXISTS (SELECT 1 FROM 'd.parquet' x WHERE x.pk = d.grp) AND NOT EXISTS (SELECT 1 FROM 'd.parquet' y WHERE y.w = f.amt)", t);
    try std.testing.expectEqual(Kind.inner, j.kind);
    try std.testing.expectEqual(@as(usize, 1), j.semis.len);
    try std.testing.expectEqualStrings("amt", j.semis[0].fk);
    try std.testing.expectEqual(@as(usize, 1), j.build_semis.len);
    try std.testing.expectEqualStrings("grp", j.build_semis[0].fk);
    // An unaliased file goes by its own name where the query wrote the FROM, and by none in the binder's own text.
    const implicit = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' WHERE f.fk IN (SELECT d.pk FROM 'd.parquet' WHERE d.w > 2)", t);
    try std.testing.expectEqualStrings("fk", implicit.fk);
    var inner = t;
    inner.user_from = false;
    try std.testing.expectError(error.UnsupportedJoin, plan(a, "SELECT count(*) AS n FROM 'f.parquet' WHERE f.fk IN (SELECT pk FROM 'd.parquet')", inner));
    try std.testing.expectError(error.UnsupportedJoin, plan(a, "SELECT count(*) AS n FROM 'f.parquet' g WHERE f.fk IN (SELECT pk FROM 'd.parquet')", t));
    // The outer query is read by one key equality, its column qualified; the subquery is one table's rows.
    const q = "SELECT count(*) AS n FROM 'f.parquet' f WHERE EXISTS ";
    try std.testing.expectError(error.ExistsSubquery, plan(a, q ++ "(SELECT 1 FROM 'd.parquet' d WHERE d.pk = fk)", t));
    try std.testing.expectError(error.ExistsSubquery, plan(a, q ++ "(SELECT 1 FROM 'd.parquet' d WHERE d.pk < f.fk)", t));
    try std.testing.expectError(error.ExistsSubquery, plan(a, q ++ "(SELECT 1 FROM 'd.parquet' d WHERE d.pk = f.fk AND d.w > f.amt)", t));
    try std.testing.expectError(error.ExistsSubquery, plan(a, q ++ "(SELECT count(*) FROM 'd.parquet' d WHERE d.pk = f.fk)", t));
    try std.testing.expectError(error.ExistsSubquery, plan(a, q ++ "(SELECT 1 FROM 'd.parquet' d WHERE d.pk = f.fk LIMIT 1)", t));
}

test "every key equality of an edge is its key, in a join's ON, its WHERE and an EXISTS" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "cy", "ct", "cl", "amt" }, &.{ "dy", "dt", "dl", "name" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t2: Tables = .{ .paths = &.{ "f.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100 }, .ctx = undefined };
    // ON and WHERE alike; a key column of the build reads as its FK, whichever part of the key it is.
    const p = try plan(a, "SELECT d.dt, d.name, count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' d ON f.cy = d.dy AND d.dt = f.ct WHERE f.cl = d.dl AND d.name <> 'x' GROUP BY d.dt, d.name", t2);
    try std.testing.expectEqualStrings("cy", p.fk);
    try std.testing.expectEqualStrings("dy", p.build_key);
    try std.testing.expectEqual(@as(usize, 2), p.fk_more.len);
    for (p.fk_more, p.build_key_more, [_][]const u8{ "ct", "cl" }, [_][]const u8{ "dt", "dl" }) |fk, key, want_fk, want_key| {
        try std.testing.expectEqualStrings(want_fk, fk);
        try std.testing.expectEqualStrings(want_key, key);
    }
    try std.testing.expectEqualStrings("name != 'x'", p.build_where.?);
    try std.testing.expect(std.mem.startsWith(u8, p.probe_sql, "SELECT ct AS dt, name, count(*) AS n FROM "));
    try std.testing.expect(std.mem.endsWith(u8, p.probe_sql, " GROUP BY ct, name"));
    // A LEFT JOIN's ON.
    const left = try plan(a, "SELECT count(d.name) AS n FROM 'f.parquet' f LEFT JOIN 'd.parquet' d ON f.cy = d.dy AND f.ct = d.dt", t2);
    try std.testing.expectEqual(Kind.left, left.kind);
    try std.testing.expectEqualStrings("ct", left.fk_more[0]);
    // An EXISTS by two keys is no IN: the plan carries it, a SEMI join; a NOT EXISTS an ANTI one.
    const ex = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f WHERE EXISTS (SELECT 1 FROM 'd.parquet' d WHERE d.dy = f.cy AND f.ct = d.dt AND d.name > 'm') AND NOT EXISTS (SELECT 1 FROM 'd.parquet' e WHERE e.dy = f.cy AND e.dl = f.cl)", t2);
    try std.testing.expectEqual(Kind.semi, ex.kind);
    try std.testing.expect(!ex.not_exists);
    try std.testing.expectEqualStrings("cy", ex.fk);
    try std.testing.expectEqualStrings("ct", ex.fk_more[0]);
    try std.testing.expectEqualStrings("dt", ex.build_key_more[0]);
    try std.testing.expectEqualStrings("name > 'm'", ex.build_where.?);
    try std.testing.expectEqual(@as(usize, 1), ex.semis.len);
    try std.testing.expect(ex.semis[0].anti);
    try std.testing.expectEqualStrings("cl", ex.semis[0].fk_more[0]);
    try std.testing.expectEqualStrings("dl", ex.semis[0].key_more[0]);
    // In a join the keys of one EXISTS are one table's.
    try std.testing.expectError(error.ExistsSubquery, plan(a, "SELECT count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' d ON f.cy = d.dy WHERE EXISTS (SELECT 1 FROM 'g.parquet' g WHERE g.gk = f.cy AND g.gname = d.name)", t2));
}

test "a column two tables of a join have goes by a name of its own, which the query nowhere spells" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "q", "amt", "w" }, &.{ "pk", "grp", "w", "name" }, &.{ "pk", "grp", "w", "name" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100, 100 }, .ctx = undefined };
    // A dimension read twice, and a column (`w`) the probe has too: each build column of a shared name is renamed, an
    // unaliased item keeps the name it was written with, and one name nothing shares stays as it is.
    const p = try plan(a, "SELECT a.name, b.name AS other, f.w, a.w AS aw, f.amt, count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' a ON f.fk = a.pk JOIN 'd.parquet' b ON f.q = b.pk WHERE a.grp < b.grp GROUP BY a.name, b.name, f.w, a.w, f.amt", t);
    try std.testing.expectEqual(@as(usize, 3), p.build_columns.len);
    for (p.build_columns, p.build_columns_as, [_][]const u8{ "name", "w", "grp" }, [_][]const u8{ "a_name", "a_w", "a_grp" }) |own, as, want_own, want_as| {
        try std.testing.expectEqualStrings(want_own, own);
        try std.testing.expectEqualStrings(want_as, as);
    }
    try std.testing.expectEqual(@as(usize, 1), p.dims.len);
    try std.testing.expectEqualStrings("b_name", p.dims[0].columns_as[0]);
    try std.testing.expectEqualStrings("b_grp", p.dims[0].columns_as[1]);
    try std.testing.expect(std.mem.startsWith(u8, p.probe_sql, "SELECT a_name AS name, b_name AS other, w, a_w AS aw, amt, count(*) AS n FROM"));
    try std.testing.expect(std.mem.indexOf(u8, p.probe_sql, "WHERE a_grp < b_grp GROUP BY a_name, b_name, w, a_w, amt") != null);
    // A plan that renames nothing is the plan it was.
    const plain = try plan(a, "SELECT a.name, count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' a ON f.fk = a.pk GROUP BY a.name", .{ .paths = t.paths[0..2], .has = Cols.has, .rows = t.rows[0..2], .ctx = undefined });
    try std.testing.expectEqual(@as(usize, 0), plain.build_columns_as.len);
    try std.testing.expect(std.mem.indexOf(u8, plain.probe_sql, "SELECT name, count(*)") != null);
    // The query spells the obvious names itself: the columns go by ones it does not.
    const spelled = try plan(a, "SELECT a.w AS a_w, b.w AS b_w_, count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' a ON f.fk = a.pk JOIN 'd.parquet' b ON f.q = b.pk GROUP BY a.w, b.w", t);
    try std.testing.expectEqualStrings("a_w_", spelled.build_columns_as[0]);
    try std.testing.expectEqualStrings("b_w__", spelled.dims[0].columns_as[0]);
    // An unaliased expression would be named by the name its column goes by; an unqualified shared name is nobody's.
    try std.testing.expectError(error.JoinColumnNeedsAlias, plan(a, "SELECT sum(a.w), sum(b.w) AS sb FROM 'f.parquet' f JOIN 'd.parquet' a ON f.fk = a.pk JOIN 'd.parquet' b ON f.q = b.pk", t));
    try std.testing.expectError(error.AmbiguousJoinColumn, plan(a, "SELECT sum(name) AS s FROM 'f.parquet' f JOIN 'd.parquet' a ON f.fk = a.pk JOIN 'd.parquet' b ON f.q = b.pk", t));
}

test "an alias is its text: a quoted table alias qualifies, and an ORDER BY alias is no table's column" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "amt", "dup" }, &.{ "pk", "grp", "w" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100 }, .ctx = undefined };
    // The alias shadows the build's key, which the binder reads as the FK: ORDER BY means the item (REVIEW-WP4b).
    const key = try plan(a, "SELECT f.amt AS \"pk\" FROM 'f.parquet' \"F\" JOIN 'd.parquet' \"my d\" ON \"F\".fk = \"my d\".pk WHERE \"my d\".w > 1 ORDER BY \"pk\" DESC", t);
    try std.testing.expectEqualStrings("fk", key.fk);
    try std.testing.expectEqualStrings("w > 1", key.build_where.?);
    try std.testing.expectEqual(@as(usize, 0), key.build_columns.len);
    try std.testing.expect(std.mem.startsWith(u8, key.probe_sql, "SELECT amt AS pk FROM "));
    try std.testing.expect(std.mem.endsWith(u8, key.probe_sql, " ORDER BY \"pk\" DESC"));
    // It shadows a build column: not published for it, and not renamed.
    const build = try plan(a, "SELECT f.amt AS \"grp\" FROM 'f.parquet' f JOIN 'd.parquet' d ON f.fk = d.pk ORDER BY grp", t);
    try std.testing.expectEqual(@as(usize, 0), build.build_columns.len);
    try std.testing.expect(std.mem.endsWith(u8, build.probe_sql, " ORDER BY grp"));
    // Inside an expression the name is the column, and a qualified name always is.
    const expr = try plan(a, "SELECT f.amt AS grp FROM 'f.parquet' f JOIN 'd.parquet' d ON f.fk = d.pk ORDER BY grp + 1, d.grp", t);
    try std.testing.expectEqual(@as(usize, 1), expr.build_columns.len);
    // A qualified name is the column. Bare, the probe query would read it as the alias: it stays qualified, by the
    // probe's own name, whichever table it came from (a build's column, a key read as the FK).
    const column = try plan(a, "SELECT f.amt AS grp, f.dup AS fk FROM 'f.parquet' f JOIN 'd.parquet' d ON f.fk = d.pk ORDER BY d.grp, d.pk, f.dup", t);
    try std.testing.expect(std.mem.endsWith(u8, column.probe_sql, " ORDER BY \"f\".grp, \"f\".fk, dup"));
    const semi = try plan(a, "SELECT f.amt AS dup FROM 'f.parquet' f WHERE f.fk IN (SELECT pk FROM 'd.parquet') ORDER BY f.dup", t);
    try std.testing.expect(std.mem.endsWith(u8, semi.probe_sql, " ORDER BY \"f\".dup"));
    // An alias that needs its quotes is written back in them, once.
    const spaced = try plan(a, "SELECT f.amt AS \"my n\", d.w AS \"Select\" FROM 'f.parquet' f JOIN 'd.parquet' d ON f.fk = d.pk ORDER BY \"my n\"", t);
    try std.testing.expect(std.mem.startsWith(u8, spaced.probe_sql, "SELECT amt AS \"my n\", w AS \"Select\" FROM "));
    try std.testing.expect(std.mem.endsWith(u8, spaced.probe_sql, " ORDER BY \"my n\""));
}

test "a boolean literal in a build column's condition keeps it the build's" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const Cols = struct {
        const of = [_][]const []const u8{ &.{ "fk", "amt" }, &.{ "pk", "flag" } };
        fn has(_: *const anyopaque, table: usize, name: []const u8) bool {
            for (of[table]) |col| if (std.mem.eql(u8, col, name)) return true;
            return false;
        }
    };
    const t: Tables = .{ .paths = &.{ "f.parquet", "d.parquet" }, .has = Cols.has, .rows = &.{ 1000, 100 }, .ctx = undefined };
    // `TRUE` parses as a column until made a literal: it must not read as a column of the probe.
    const p = try plan(a, "SELECT count(*) AS n FROM 'f.parquet' f JOIN 'd.parquet' d ON f.fk = d.pk WHERE d.flag = TRUE", t);
    try std.testing.expectEqualStrings("flag = TRUE", p.build_where.?);
    try std.testing.expect(std.mem.indexOf(u8, p.probe_sql, "WHERE") == null);
}
