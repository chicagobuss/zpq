//! A minimal join binder (0.6.0 prototype): a two-table N:1 equi-join written as `FROM a, b WHERE a.k = b.k ...`
//! or `FROM a JOIN b ON a.k = b.k ...` (INNER), or as `WHERE a.k IN (SELECT b.k FROM b WHERE ...)` (SEMI), becomes
//! one single-table query over the probe side and a build spec. The probe query reads build columns by name; the
//! frontend binds them as virtual leaves (`join/lookup.zig`).
//!
//! Every WHERE/ON conjunct must read one side only, apart from the one key equality: build-side conjuncts become the
//! build's filter, the rest stay in the probe query. Anything else (residuals across sides, OR across sides, a second
//! equality, outer joins) refuses rather than guess.
const std = @import("std");
const c = @import("liteparser_c");

pub const Kind = enum { inner, semi };

/// Which side's query holds the `IN (SELECT ...)` a three-table reduction wrote (`reduce`).
pub const Side = enum { probe, build };

pub const Error = error{
    SqlParseError,
    UnsupportedJoin,
    SubqueryLimitNotSupported,
    CorrelatedSubqueryNotSupported,
    JoinKeyNotFound,
    JoinResidualNotSupported,
    AmbiguousJoinColumn,
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
    /// The side whose query holds the IN a three-table reduction wrote. That IN is an INNER edge: its keys must be
    /// unique, as any INNER join's are, though no column of its table is read.
    reduced: ?Side = null,
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
        try flattenFrom(from, &list, null, a);
    } else {
        _ = fromTable(from) orelse return null;
        const in = semiConjunct(p.root.u.select.where) orelse return null;
        const sub = in.node.u.in.select.*.u.select.from orelse return error.UnsupportedJoin;
        _ = fromTable(sub) orelse return error.UnsupportedJoin;
        try list.append(a, from);
        try list.append(a, sub);
    }
    if (list.items.len < 2 or list.items.len > 3) return error.UnsupportedJoin;
    // Paths outlive the C arena.
    const out = try a.alloc([]const u8, list.items.len);
    for (list.items, out) |n, *o| o.* = try a.dupe(u8, fromTable(n).?.path);
    return out;
}

/// The FROM tables of a join tree in order, its ON conditions into `on`. Comma and INNER joins only: NATURAL, USING
/// and outer joins change which rows exist.
fn flattenFrom(n: *c.LpNode, out: *std.ArrayList(*c.LpNode), on: ?*std.ArrayList(*c.LpNode), a: std.mem.Allocator) Error!void {
    if (n.kind == c.LP_JOIN_CLAUSE) {
        const j = n.u.join;
        if (j.join_type & ~@as(c_int, c.LP_JOIN_INNER | c.LP_JOIN_CROSS) != 0 or j.using_columns.count != 0)
            return error.UnsupportedJoin;
        try flattenFrom(j.left orelse return error.UnsupportedJoin, out, on, a);
        try flattenFrom(j.right orelse return error.UnsupportedJoin, out, on, a);
        if (on) |list| try flatten(j.on_expr, list, a);
        return;
    }
    _ = fromTable(n) orelse return error.UnsupportedJoin;
    try out.append(a, n);
}

const Conj = struct { node: *c.LpNode };

fn flatten(n: ?*c.LpNode, list: *std.ArrayList(*c.LpNode), a: std.mem.Allocator) !void {
    const node = n orelse return;
    if (node.kind == c.LP_EXPR_BINARY_OP and node.u.binary.op == c.LP_OP_AND) {
        try flatten(node.u.binary.left, list, a);
        try flatten(node.u.binary.right, list, a);
        return;
    }
    try list.append(a, node);
}

/// The first top-level `col IN (SELECT ...)` conjunct of `where`.
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
        if (n.kind == c.LP_EXPR_IN and n.u.in.select != null and n.u.in.is_not == 0 and n.u.in.expr != null and
            n.u.in.expr.*.kind == c.LP_EXPR_COLUMN_REF) return .{ .node = n };
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
    fn of(s: Sides, ref: *c.LpNode) Error!?u1 {
        const i = try s.at(ref) orelse return null;
        return @intCast(i);
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
    fn only(s: Sides, n: *c.LpNode, a: std.mem.Allocator) Error!?u1 {
        var side: ?u1 = null;
        for (try columnRefs(n, a)) |ref| if (try s.of(ref)) |x| {
            if (side != null and side.? != x) return error.JoinResidualNotSupported;
            side = x;
        };
        return side;
    }

    /// The tables `n` reads, as a bit set over positions in `tables`.
    fn reads(s: Sides, n: *c.LpNode, a: std.mem.Allocator) Error!u8 {
        var set: u8 = 0;
        for (try columnRefs(n, a)) |ref| if (try s.at(ref)) |i| {
            set |= @as(u8, 1) << @intCast(i);
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

/// Three tables become two: a table no result, GROUP BY, ORDER BY or HAVING term reads, joined by one key equality
/// to another and otherwise read only by its own conjuncts, is a SEMI join of that other table, `x.m IN (SELECT l.k
/// FROM l WHERE ...)`. Rewrites `sel` in place (its FROM to the two tables left, its WHERE to their conjuncts and the
/// IN) and returns the two tables' original indices and the IN.
fn reduce(a: std.mem.Allocator, sel: anytype, tables_in: []*c.LpNode, conjuncts: []*c.LpNode, s: Sides, root: *c.LpNode) Error!struct { left: [2]usize, in: *c.LpNode } {
    var outside: u8 = 0;
    for (try columnRefs(root, a)) |ref| if (try s.at(ref)) |i| {
        outside |= @as(u8, 1) << @intCast(i);
    };
    for (0..3) |l| {
        if (outside & (@as(u8, 1) << @intCast(l)) != 0) continue;
        var key: ?usize = null;
        var ok = true;
        var own: std.ArrayList(*c.LpNode) = .empty;
        for (conjuncts, 0..) |n, ci| {
            const set = try s.reads(n, a);
            if (set & (@as(u8, 1) << @intCast(l)) == 0) continue;
            if (set == @as(u8, 1) << @intCast(l)) {
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
        sub.u.select.from = tables_in[l];
        sub.u.select.where = try andOf(a, own.items);
        const in = try create(a, c.LP_EXPR_IN);
        in.u.in.expr = x_ref;
        in.u.in.select = sub;
        var kept: std.ArrayList(*c.LpNode) = .empty;
        for (conjuncts, 0..) |n, ci| {
            if (ci == key.? or (try s.reads(n, a)) == @as(u8, 1) << @intCast(l)) continue;
            try kept.append(a, n);
        }
        try kept.append(a, in);
        var left: [2]usize = undefined;
        var at: usize = 0;
        for (0..3) |i| if (i != l) {
            left[at] = i;
            at += 1;
        };
        const join = try create(a, c.LP_JOIN_CLAUSE);
        join.u.join.left = tables_in[left[0]];
        join.u.join.right = tables_in[left[1]];
        sel.from = join;
        sel.where = try andOf(a, kept.items);
        return .{ .left = left, .in = in };
    }
    return error.UnsupportedJoin;
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

/// Split `sql` into the probe query and the build spec.
pub fn plan(a: std.mem.Allocator, sql: [:0]const u8, t: Tables) Error!Plan {
    const p = try parse(sql);
    defer p.deinit();
    const sel = &p.root.u.select;
    const from = sel.from orelse return error.UnsupportedJoin;
    var conjuncts: std.ArrayList(*c.LpNode) = .empty;
    var kind: Kind = .inner;
    var aliases: [2]?[]const u8 = .{ null, null };
    var from_tables: [2]*c.LpNode = undefined;
    var key_pair: ?[2]*c.LpNode = null;
    var semi: ?Conj = null;
    var probe_from: *c.LpNode = from;
    var paths: [2][]const u8 = undefined;
    var rows: [2]u64 = undefined;
    var map: [2]usize = .{ 0, 1 };
    var reduced_in: ?*c.LpNode = null;
    var reduced: ?Side = null;
    if (from.*.kind == c.LP_JOIN_CLAUSE) {
        var list: std.ArrayList(*c.LpNode) = .empty;
        try flattenFrom(from, &list, &conjuncts, a);
        try flatten(sel.where, &conjuncts, a);
        if (list.items.len != t.paths.len) return error.UnsupportedJoin;
        const all_aliases = try a.alloc(?[]const u8, list.items.len);
        for (list.items, all_aliases) |n, *al| al.* = fromTable(n).?.alias;
        if (list.items.len == 3) {
            const all = [_]usize{ 0, 1, 2 };
            const r = try reduce(a, sel, list.items, conjuncts.items, .{ .t = t, .tables = &all, .aliases = all_aliases }, p.root);
            map = r.left;
            reduced_in = r.in;
            conjuncts.clearRetainingCapacity();
            try flatten(sel.where, &conjuncts, a);
        } else if (list.items.len != 2) return error.UnsupportedJoin;
        for (map, 0..) |m, i| aliases[i] = all_aliases[m];
        from_tables = .{ list.items[map[0]], list.items[map[1]] };
    } else {
        kind = .semi;
        semi = semiConjunct(sel.where) orelse return error.UnsupportedJoin;
        const t0 = fromTable(from) orelse return error.UnsupportedJoin;
        aliases[0] = t0.alias;
        try flatten(sel.where, &conjuncts, a);
    }
    for (map, 0..) |m, i| {
        paths[i] = t.paths[m];
        rows[i] = t.rows[m];
    }
    const sides: Sides = .{ .t = t, .tables = &map, .aliases = blk: {
        const al = try a.alloc(?[]const u8, t.paths.len);
        @memset(al, null);
        for (map, aliases) |m, x| al[m] = x;
        break :blk al;
    } };
    // The probe: the outer table of a SEMI join, else the larger by footer rows.
    const probe: u1 = if (kind == .semi) 0 else if (rows[1] > rows[0]) 1 else 0;
    const build: u1 = 1 - probe;
    var probe_conj: std.ArrayList(*c.LpNode) = .empty;
    var build_conj: std.ArrayList(*c.LpNode) = .empty;
    var fk: []const u8 = "";
    var build_key: []const u8 = "";
    for (conjuncts.items) |n| {
        if (semi) |s| if (n == s.node) {
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
                if (!t.has(t.ctx, map[build], span(r.column)))
                    return if (t.has(t.ctx, map[probe], span(r.column))) error.CorrelatedSubqueryNotSupported else error.JoinKeyNotFound;
            }
            fk = try a.dupe(u8, span(n.u.in.expr.*.u.column_ref.column));
            build_key = try a.dupe(u8, span(e.*.u.column_ref.column));
            try flatten(sub.where, &build_conj, a);
            continue;
        };
        if (n.kind == c.LP_EXPR_BINARY_OP and n.u.binary.op == c.LP_OP_EQ and key_pair == null) {
            const l = n.u.binary.left;
            const r = n.u.binary.right;
            if (l != null and r != null and l.*.kind == c.LP_EXPR_COLUMN_REF and r.*.kind == c.LP_EXPR_COLUMN_REF) {
                const ls = try sides.of(l);
                const rs = try sides.of(r);
                if (ls != null and rs != null and ls.? != rs.?) {
                    key_pair = if (ls.? == probe) .{ l, r } else .{ r, l };
                    fk = try a.dupe(u8, span(key_pair.?[0].*.u.column_ref.column));
                    build_key = try a.dupe(u8, span(key_pair.?[1].*.u.column_ref.column));
                    continue;
                }
            }
        }
        const side = try sides.only(n, a);
        const on_build = side != null and side.? == build;
        if (n == reduced_in) reduced = if (on_build) .build else .probe;
        if (on_build) try build_conj.append(a, n) else try probe_conj.append(a, n);
    }
    if (fk.len == 0) return error.JoinKeyNotFound;
    if (kind == .inner) probe_from = from_tables[probe];
    // The probe query: its own FROM, its own conjuncts, unqualified names; the build key reads as the FK.
    probe_from.*.u.from_table.alias = null;
    sel.from = probe_from;
    sel.where = try andOf(a, probe_conj.items);
    var build_cols: std.ArrayList([]const u8) = .empty;
    for (try columnRefs(p.root, a)) |ref| {
        const side = try sides.of(ref);
        ref.*.u.column_ref.table = null;
        if (side == null or side.? != build) continue;
        const name = span(ref.*.u.column_ref.column);
        if (std.mem.eql(u8, name, build_key)) {
            ref.*.u.column_ref.column = (try a.dupeSentinel(u8, fk, 0)).ptr;
            continue;
        }
        if (kind == .semi) return error.UnsupportedJoin; // a SEMI join's build columns are not in its result
        for (build_cols.items) |b| {
            if (std.mem.eql(u8, b, name)) break;
        } else try build_cols.append(a, try a.dupe(u8, name));
    }
    if (sel.where) |w| for (try columnRefs(w, a)) |ref| {
        ref.*.u.column_ref.table = null;
    };
    const build_where: ?[]const u8 = if (try andOf(a, build_conj.items)) |w| blk: {
        for (try columnRefs(w, a)) |ref| ref.*.u.column_ref.table = null;
        break :blk try toSql(a, p, w);
    } else null;
    return .{
        .kind = kind,
        .probe_path = paths[probe],
        .build_path = paths[build],
        .fk = fk,
        .build_key = build_key,
        .probe_sql = try toSql(a, p, p.root),
        .build_where = build_where,
        .build_columns = build_cols.items,
        .reduced = reduced,
    };
}
