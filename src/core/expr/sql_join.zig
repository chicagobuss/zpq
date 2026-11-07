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

pub const Kind = enum { inner, semi, anti, left };

pub const Error = error{
    SqlParseError,
    UnsupportedJoin,
    SubqueryLimitNotSupported,
    CorrelatedSubqueryNotSupported,
    JoinKeyNotFound,
    JoinResidualNotSupported,
    AmbiguousJoinColumn,
    LeftJoinOn,
    LeftJoinWhere,
    OutOfMemory,
};

pub const Plan = struct {
    kind: Kind,
    probe_path: []const u8,
    build_path: []const u8,
    /// The probe's foreign key and the build's key column.
    fk: []const u8,
    build_key: []const u8,
    /// The query with the join removed: a single-table SELECT over `probe_path`.
    probe_sql: []const u8,
    /// The build side's own conjuncts, as one SQL expression, or null.
    build_where: ?[]const u8,
    /// Build columns the probe query reads, in first-reference order (never the key: it reads as the FK).
    build_columns: []const []const u8,
    /// Every IN of the probe's and the builds' queries is one a reduction wrote (`reduce`): an INNER edge whose keys
    /// must be unique, as any INNER join's are, though no column of its table is read. A query that reduced holds no IN
    /// of its own (its table count would not match).
    reduced: bool = false,
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
    where: ?[]const u8,
    columns: []const []const u8,
    /// A LEFT JOIN's: every probe row stays, a miss reading the dimension's columns as NULL.
    left: bool = false,
    /// A snowflake's tables joined to this one by an FK of its own (`fk` a column of this table), INNER, each
    /// publishing its columns through this one's build.
    dims: []const Dim = &.{},
};

/// What the caller knows of the tables (from their footers): whether a column name is theirs, and their rows.
pub const Tables = struct {
    paths: []const []const u8,
    has: *const fn (ctx: *const anyopaque, table: usize, name: []const u8) bool,
    rows: []const u64,
    ctx: *const anyopaque,
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
    return .{ .arena = arena, .root = root };
}

const FromTable = struct { path: []const u8, alias: ?[]const u8 };

fn fromTable(n: *c.LpNode) ?FromTable {
    if (n.kind != c.LP_FROM_TABLE or n.u.from_table.name == null) return null;
    return .{ .path = stripQuotes(span(n.u.from_table.name)), .alias = if (n.u.from_table.alias) |a| span(a) else null };
}

/// The two tables' paths when `sql` is a join this binder may take, so the caller can read their footers; else null
/// (an ordinary single-table query, left to the established parser).
pub fn tables(a: std.mem.Allocator, sql: [:0]const u8) Error!?[]const []const u8 {
    const p = parse(sql) catch return null;
    defer p.deinit();
    const from = p.root.u.select.from orelse return null;
    var list: std.ArrayList(*c.LpNode) = .empty;
    defer list.deinit(a);
    if (from.*.kind == c.LP_JOIN_CLAUSE) {
        try flattenFrom(from, &list, null, null, a);
    } else {
        _ = fromTable(from) orelse return null;
        const in = semiConjunct(p.root.u.select.where) orelse return null;
        const sub = in.node.u.in.select.*.u.select.from orelse return error.UnsupportedJoin;
        _ = fromTable(sub) orelse return error.UnsupportedJoin;
        try list.append(a, from);
        try list.append(a, sub);
    }
    if (list.items.len < 2 or list.items.len > 64) return error.UnsupportedJoin;
    // Paths outlive the C arena.
    const out = try a.alloc([]const u8, list.items.len);
    for (list.items, out) |n, *o| o.* = try a.dupe(u8, fromTable(n).?.path);
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
        if (j.join_type & ~allowed != 0 or j.using_columns.count != 0) return error.UnsupportedJoin;
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
        for (0..@intCast(s.order_by.count)) |i| if (s.order_by.items[i]) |o| c.lp_ast_walk(o, &visitor);
        if (s.having) |h| c.lp_ast_walk(h, &visitor);
    } else c.lp_ast_walk(n, &visitor);
    if (refs.failed) return error.OutOfMemory;
    return refs.list.items;
}

const Sides = struct {
    t: Tables,
    /// Original table indices of the tables in play (all of them, or two after a reduction).
    tables: []const usize,
    aliases: []const ?[]const u8,

    /// Which table (position in `tables`) a reference names: by its qualifier when it has one, else by which table
    /// has the column. Null for a name no table has (a SELECT alias).
    fn of(s: Sides, ref: *c.LpNode) Error!?usize {
        return s.at(ref);
    }

    fn at(s: Sides, ref: *c.LpNode) Error!?usize {
        const col = span(ref.u.column_ref.column);
        if (ref.u.column_ref.table) |q| {
            const qual = span(q);
            for (s.tables, 0..) |ti, i| {
                const alias = s.aliases[ti] orelse continue;
                if (std.mem.eql(u8, alias, qual)) return i;
            }
            return error.UnsupportedJoin;
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
fn reduce(a: std.mem.Allocator, sel: anytype, tables_in: []*c.LpNode, conjuncts: []*c.LpNode, s: Sides, root: *c.LpNode, keep: usize) Error!?usize {
    var outside: u64 = 0;
    for (try columnRefs(root, a)) |ref| if (try s.at(ref)) |i| {
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

/// Split `sql` into the probe query and the build specs: one build for a two-table join or an IN, one per dimension for
/// a star (each joined to the probe by one key equality), after every filter-only table is reduced to an IN.
pub fn plan(a: std.mem.Allocator, sql: [:0]const u8, t: Tables) Error!Plan {
    const p = try parse(sql);
    defer p.deinit();
    const sel = &p.root.u.select;
    const from = sel.from orelse return error.UnsupportedJoin;
    if (from.*.kind != c.LP_JOIN_CLAUSE) return planSemi(a, p, sel, from, t);
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    var list: std.ArrayList(*c.LpNode) = .empty;
    var nulls: std.ArrayList(NullSide) = .empty;
    try flattenFrom(from, &list, &conjuncts, &nulls, a);
    try flatten(sel.where, &conjuncts, a);
    if (list.items.len != t.paths.len or list.items.len > 64) return error.UnsupportedJoin;
    const aliases = try a.alloc(?[]const u8, list.items.len);
    for (list.items, aliases) |n, *al| al.* = fromTable(n).?.alias;
    // The tables still in play, by original index; filter-only ones reduce to INs one at a time.
    var live: std.ArrayList(usize) = .empty;
    for (0..list.items.len) |i| try live.append(a, i);
    var reduced = false;
    // A LEFT JOIN's null-supplying table is read by its ON alone: a WHERE or INNER ON condition on it would need its
    // NULL rows, and one joining it to a third table would stack a join on the join. Having no conjunct, it never reduces.
    var null_set: u64 = 0;
    for (nulls.items) |ns| null_set |= @as(u64, 1) << @intCast(ns.table);
    if (null_set != 0) {
        const all: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
        for (conjuncts.items) |node| if ((try all.reads(node, a)) & null_set != 0) return error.LeftJoinWhere;
    }
    // The probe is chosen once, over every table a LEFT JOIN keeps: the reductions that follow take tables out around it.
    const center = live.items[largest(t, live.items, null_set)];
    while (live.items.len > 2) {
        const sides: Sides = .{ .t = t, .tables = live.items, .aliases = aliases };
        const keep = std.mem.indexOfScalar(usize, live.items, center).?;
        const out = try reduce(a, sel, list.items, conjuncts.items, sides, p.root, keep) orelse break;
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
    for (conjuncts.items, used) |node, u| {
        if (u) continue;
        const side = try sides.only(node, a) orelse probe;
        try wheres[side].append(a, node);
    }
    // Each LEFT JOIN's ON: its one key equality with the probe, and conditions on its own table (its build's filter).
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
    for (try columnRefs(p.root, a)) |ref| {
        const side = try sides.of(ref);
        ref.*.u.column_ref.table = null;
        var d = side orelse continue;
        var name = span(ref.*.u.column_ref.column);
        while (d != probe and !left[d] and std.mem.eql(u8, name, keys[d].?)) {
            name = fks[d].?;
            d = parent[d];
        }
        if (name.ptr != span(ref.*.u.column_ref.column).ptr) ref.*.u.column_ref.column = (try a.dupeSentinel(u8, name, 0)).ptr;
        if (d != probe) try addName(a, &columns[d], name);
    }
    // A snowflake's table reads its FK off its parent's build: the parent publishes it.
    for (0..n) |d| if (d != probe and parent[d] != probe) try addName(a, &columns[parent[d]], fks[d].?);
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
    const tree: Tree = .{ .t = t, .p = p, .live = live.items, .probe = probe, .parent = parent, .fks = fks, .keys = keys, .wheres = wheres, .columns = columns, .left = left };
    var dims: std.ArrayList(Dim) = .empty;
    for (0..n) |d| if (d != probe and d != main and parent[d] == probe) try dims.append(a, try tree.dim(a, d));
    return .{
        .kind = if (left[main]) .left else .inner,
        .probe_path = t.paths[live.items[probe]],
        .build_path = t.paths[live.items[main]],
        .fk = fks[main].?,
        .build_key = keys[main].?,
        .probe_sql = try toSql(a, p, p.root),
        .build_where = try whereSql(a, p, wheres[main].items),
        .build_columns = columns[main].items,
        .build_dims = try tree.children(a, main),
        .reduced = reduced,
        .dims = dims.items,
    };
}

fn addName(a: std.mem.Allocator, list: *std.ArrayList([]const u8), name: []const u8) !void {
    for (list.items) |b| if (std.mem.eql(u8, b, name)) return;
    try list.append(a, try a.dupe(u8, name));
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
    wheres: []const std.ArrayList(*c.LpNode),
    columns: []const std.ArrayList([]const u8),
    left: []const bool,

    fn dim(tr: Tree, a: std.mem.Allocator, d: usize) Error!Dim {
        return .{
            .path = tr.t.paths[tr.live[d]],
            .fk = tr.fks[d].?,
            .key = tr.keys[d].?,
            .where = try whereSql(a, tr.p, tr.wheres[d].items),
            .columns = tr.columns[d].items,
            .left = tr.left[d],
            .dims = try tr.children(a, d),
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

/// `FROM t WHERE ... t.k IN (SELECT b.k FROM b WHERE ...)`: a SEMI join of `t` and the subquery's table.
fn planSemi(a: std.mem.Allocator, p: Parsed, sel: anytype, from: *c.LpNode, t: Tables) Error!Plan {
    const semi = semiConjunct(sel.where) orelse return error.UnsupportedJoin;
    const t0 = fromTable(from) orelse return error.UnsupportedJoin;
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    try flatten(sel.where, &conjuncts, a);
    var probe_conj: std.ArrayList(*c.LpNode) = .empty;
    var build_conj: std.ArrayList(*c.LpNode) = .empty;
    var fk: []const u8 = "";
    var build_key: []const u8 = "";
    for (conjuncts.items) |n| {
        if (n == semi.node) {
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
            // Every name the subquery reads is its own table's: a qualifier naming another table, or a name only the
            // outer table has, is a correlated reference, which this release does not join.
            var own = try columnRefs(n.u.in.select, a);
            if (sub.where) |w| own = try std.mem.concat(a, *c.LpNode, &.{ own, try columnRefs(w, a) });
            for (own) |ref| {
                const r = ref.*.u.column_ref;
                if (r.table != null and (sub_table.alias == null or !std.mem.eql(u8, span(r.table), sub_table.alias.?)))
                    return error.CorrelatedSubqueryNotSupported;
                if (!t.has(t.ctx, 1, span(r.column)))
                    return if (t.has(t.ctx, 0, span(r.column))) error.CorrelatedSubqueryNotSupported else error.JoinKeyNotFound;
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
    var outer = try columnRefs(p.root, a);
    if (sel.where) |w| outer = try std.mem.concat(a, *c.LpNode, &.{ outer, try columnRefs(w, a) });
    for (outer) |ref| {
        if (ref.*.u.column_ref.table) |q| if (t0.alias == null or !std.mem.eql(u8, span(q), t0.alias.?))
            return error.UnsupportedJoin;
        ref.*.u.column_ref.table = null;
    }
    return .{
        .kind = if (semi.anti) .anti else .semi,
        .probe_path = t.paths[0],
        .build_path = t.paths[1],
        .fk = fk,
        .build_key = build_key,
        .probe_sql = try toSql(a, p, p.root),
        .build_where = try whereSql(a, p, build_conj.items),
        .build_columns = &.{},
    };
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
    // A second edge to a table already in the tree is a cycle: a residual, refused.
    try std.testing.expectError(error.JoinResidualNotSupported, plan(a, "SELECT gname, count(*) AS n FROM 'f.parquet' f, 'd.parquet' d, 'g.parquet' g WHERE f.fk = d.pk AND d.grp = g.gk AND f.amt = g.gk GROUP BY gname", t));
}
