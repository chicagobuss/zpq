//! Non-recursive `WITH` CTEs and subqueries in FROM (derived tables), as text. Each CTE a query reads and each derived
//! table becomes a stage: a query of its own, which names neither, whose rows the CLI writes to a scratch Parquet file
//! (`cli/cte.zig`); the query itself then reads those files where it named them. Every stage and the final query go
//! through the ordinary frontend, so a CTE's body may be anything a query may be (a join, a set operation, windows),
//! and nothing downstream knows a CTE was there.
//!
//! A CTE is computed once, the first time something reads it, however many times it is read: its file is read again.
//! The engine has no plan to inline a body into (it runs one SELECT over files), and inlining a body read twice would
//! scan its inputs twice. A CTE nothing reads is not computed, as in DuckDB.
//!
//! The text carries no comments (`stripComments` ran first), so a keyword is a word outside quotes. Names compare
//! without case, quoted or not, as DuckDB's do; a quoted string in FROM is a file, never a CTE.
const std = @import("std");
const setops = @import("sql_setops.zig");

pub const Stage = struct {
    /// A query that names no CTE and no derived table.
    sql: []const u8,
    /// The Parquet file its rows go to, as `sql` texts after it name it.
    out: []const u8,
    /// New names for its first columns (`WITH t(a, b)`, `(...) AS t(a, b)`); empty for none. Extra names are ignored, as
    /// DuckDB ignores them.
    columns: []const []const u8,
};

pub const Plan = struct {
    /// In the order they must run: a stage reads only the files of stages before it.
    stages: []const Stage,
    /// The query, reading the stages' files.
    sql: []const u8,
};

pub const Error = error{
    OutOfMemory,
    /// `WITH RECURSIVE`.
    CteRecursive,
    /// Two CTEs of one WITH share a name.
    CteDuplicate,
    /// A CTE's body reads the CTE itself.
    CteCircular,
    /// `LATERAL`: a subquery in FROM that reads the tables beside it.
    LateralNotSupported,
    /// A WITH list or a derived table's alias that does not parse.
    SqlParseError,
};

/// The stages `sql` needs, with their files in `dir`, or null when it names no CTE and no derived table.
pub fn plan(arena: std.mem.Allocator, sql: []const u8, dir: []const u8) Error!?Plan {
    var p: Planner = .{ .arena = arena, .dir = dir };
    const text = try p.query(null, sql);
    if (!p.changed) return null;
    return .{ .stages = p.stages.items, .sql = text };
}

const Cte = struct {
    name: []const u8,
    columns: []const []const u8,
    body: []const u8,
    /// Its file once staged.
    out: ?[]const u8 = null,
    /// Its body is being planned: a reference to it now is circular.
    defining: bool = false,
};

/// The CTEs a piece of text sees: the first `len` of one WITH list, then the ones the enclosing query saw.
const View = struct {
    ctes: []Cte,
    len: usize,
    parent: ?*const View,
};

const Planner = struct {
    arena: std.mem.Allocator,
    dir: []const u8,
    stages: std.ArrayList(Stage) = .empty,
    changed: bool = false,

    /// `text`, a whole query, with its WITH list taken off and every CTE and derived table it reads replaced by a file.
    fn query(self: *Planner, view: ?*const View, text_in: []const u8) Error![]const u8 {
        const text = std.mem.trim(u8, text_in, " \t\r\n");
        const head = setops.nextWord(text, 0);
        if (!setops.eql(head.word, "with")) return self.body(view, text, false);
        self.changed = true;
        var i = head.end;
        if (setops.eql(setops.nextWord(text, i).word, "recursive")) return error.CteRecursive;
        var ctes: std.ArrayList(Cte) = .empty;
        while (true) {
            const name = nameAt(text, i) orelse return error.SqlParseError;
            i = name.end;
            for (ctes.items) |c| if (sameName(c.name, name.text)) return error.CteDuplicate;
            var columns: []const []const u8 = &.{};
            i = skipSpace(text, i);
            if (i < text.len and text[i] == '(') {
                const close = closing(text, i) orelse return error.SqlParseError;
                columns = try self.names(text[i + 1 .. close - 1]);
                i = close;
            }
            var w = setops.nextWord(text, i);
            if (!setops.eql(w.word, "as")) return error.SqlParseError;
            w = setops.nextWord(text, w.end);
            if (setops.eql(w.word, "not")) w = setops.nextWord(text, w.end);
            i = if (setops.eql(w.word, "materialized")) w.end else w.end - w.word.len;
            i = skipSpace(text, i);
            if (i >= text.len or text[i] != '(') return error.SqlParseError;
            const close = closing(text, i) orelse return error.SqlParseError;
            try ctes.append(self.arena, .{ .name = name.text, .columns = columns, .body = text[i + 1 .. close - 1] });
            i = skipSpace(text, close);
            if (i < text.len and text[i] == ',') {
                i += 1;
                continue;
            }
            break;
        }
        const all = try self.arena.create(View);
        all.* = .{ .ctes = ctes.items, .len = ctes.items.len, .parent = view };
        return self.body(all, text[i..], false);
    }

    /// The file the CTE `name` is in, staging it first; null when `view` has no CTE of that name.
    fn lookup(self: *Planner, view: ?*const View, name: []const u8) Error!?[]const u8 {
        var v = view;
        while (v) |at| : (v = at.parent) {
            var k = at.len;
            while (k > 0) {
                k -= 1;
                const c = &at.ctes[k];
                if (!sameName(c.name, name)) continue;
                if (c.out) |out| return out;
                if (c.defining) return error.CteCircular;
                // The body sees the CTEs before it in its list, and itself (to name a circular reference).
                c.defining = true;
                const inner: View = .{ .ctes = at.ctes, .len = k + 1, .parent = at.parent };
                const sql = try self.query(&inner, c.body);
                c.defining = false;
                c.out = try self.stage(sql, c.columns);
                return c.out;
            }
        }
        return null;
    }

    fn stage(self: *Planner, sql: []const u8, columns: []const []const u8) Error![]const u8 {
        const file = try std.fmt.allocPrint(self.arena, "cte{d}.parquet", .{self.stages.items.len});
        const out = try std.fs.path.join(self.arena, &.{ self.dir, file });
        try self.stages.append(self.arena, .{ .sql = sql, .out = out, .columns = columns });
        self.changed = true;
        return out;
    }

    /// `text` with its table references resolved. A FROM counts only at a level that is a query (it has a SELECT): the
    /// FROM of `EXTRACT(year FROM d)` or `substring(s FROM 2)` reads no table. `in_from` starts a parenthesized join.
    fn body(self: *Planner, view: ?*const View, text: []const u8, in_from_start: bool) Error![]const u8 {
        var out: std.ArrayList(u8) = .empty;
        var is_query = in_from_start;
        var in_from = in_from_start;
        var expect_table = in_from_start;
        // The names of the tables before a derived table in its FROM, which it may not read (an implicit LATERAL).
        var left: std.ArrayList([]const u8) = .empty;
        var i: usize = 0;
        while (i < text.len) {
            const ch = text[i];
            if (ch == '\'') {
                const end = setops.quotedEnd(text, i);
                try out.appendSlice(self.arena, text[i..end]);
                if (expect_table) {
                    const base = std.fs.path.basename(text[i + 1 .. end - 1]);
                    try left.append(self.arena, aliasAt(text, end) orelse base[0 .. std.mem.indexOfScalar(u8, base, '.') orelse base.len]);
                }
                expect_table = false;
                i = end;
                continue;
            }
            if (ch == '"' or setops.startsWord(text, i)) {
                const end = if (ch == '"') setops.quotedEnd(text, i) else i + setops.wordAt(text, i).len;
                const token = text[i..end];
                if (expect_table) {
                    expect_table = false;
                    if (ch != '"' and setops.eql(token, "lateral")) return error.LateralNotSupported;
                    try left.append(self.arena, aliasAt(text, end) orelse token);
                    const next = skipSpace(text, end);
                    // A table function (`read_parquet(...)`) or a qualified name is no CTE.
                    const plain = next >= text.len or (text[next] != '(' and text[next] != '.');
                    if (plain) if (try self.lookup(view, token)) |file| {
                        try out.print(self.arena, "'{s}'", .{file});
                        if (!aliasFollows(text, end)) try out.print(self.arena, " AS {s}", .{token});
                        i = end;
                        continue;
                    };
                    try out.appendSlice(self.arena, token);
                    i = end;
                    continue;
                }
                if (ch != '"') {
                    if (setops.eql(token, "select")) {
                        is_query = true;
                        in_from = false;
                    } else if (is_query and setops.eql(token, "from")) {
                        in_from = true;
                        expect_table = true;
                        left.clearRetainingCapacity();
                    } else if (in_from and setops.eql(token, "join")) {
                        expect_table = true;
                    } else if (in_from and endsFrom(token)) {
                        in_from = false;
                    }
                }
                try out.appendSlice(self.arena, token);
                i = end;
                continue;
            }
            if (ch == ',' and in_from) expect_table = true;
            if (ch == '(') {
                const close = closing(text, i) orelse {
                    try out.appendSlice(self.arena, text[i..]);
                    break;
                };
                const inner = text[i + 1 .. close - 1];
                if (expect_table) {
                    expect_table = false;
                    if (isQuery(inner)) {
                        if (reads(inner, left.items)) return error.LateralNotSupported;
                        if (aliasAt(text, close)) |name| try left.append(self.arena, name);
                        const file = try self.stage(try self.query(view, inner), &.{});
                        i = try self.derivedAlias(text, close, &out, file);
                        continue;
                    }
                    try out.print(self.arena, "({s})", .{try self.body(view, inner, true)});
                } else if (isQuery(inner)) {
                    try out.print(self.arena, "({s})", .{try self.query(view, inner)});
                } else {
                    try out.print(self.arena, "({s})", .{try self.body(view, inner, false)});
                }
                i = close;
                continue;
            }
            try out.append(self.arena, ch);
            i += 1;
        }
        return out.items;
    }

    /// Writes the derived table's file and its alias, which may name its columns (`AS t(a, b)`): those become the stage's
    /// `columns`, as the grammar after it has no column list. Returns where the text goes on.
    fn derivedAlias(self: *Planner, text: []const u8, from: usize, out: *std.ArrayList(u8), file: []const u8) Error!usize {
        try out.print(self.arena, "'{s}'", .{file});
        if (!aliasFollows(text, from)) return from;
        var i = from;
        const w = setops.nextWord(text, i);
        if (setops.eql(w.word, "as")) i = w.end;
        const name = nameAt(text, i) orelse return error.SqlParseError;
        try out.print(self.arena, " AS {s}", .{name.text});
        i = name.end;
        const next = skipSpace(text, i);
        if (next < text.len and text[next] == '(') {
            const close = closing(text, next) orelse return error.SqlParseError;
            self.stages.items[self.stages.items.len - 1].columns = try self.names(text[next + 1 .. close - 1]);
            return close;
        }
        return i;
    }

    /// `a, "b c", d` as names.
    fn names(self: *Planner, list: []const u8) Error![]const []const u8 {
        var out: std.ArrayList([]const u8) = .empty;
        var i: usize = 0;
        while (true) {
            const name = nameAt(list, i) orelse return error.SqlParseError;
            try out.append(self.arena, unquote(name.text));
            i = skipSpace(list, name.end);
            if (i >= list.len) break;
            if (list[i] != ',') return error.SqlParseError;
            i += 1;
        }
        return out.items;
    }
};

/// The alias after a table reference that ends at `from`.
fn aliasAt(text: []const u8, from: usize) ?[]const u8 {
    if (!aliasFollows(text, from)) return null;
    const w = setops.nextWord(text, from);
    const name = nameAt(text, if (setops.eql(w.word, "as")) w.end else from) orelse return null;
    return name.text;
}

/// Whether `inner` qualifies a column with one of `names` (`x.g`) that it does not name a table of its own.
fn reads(inner: []const u8, names: []const []const u8) bool {
    var qualifies = false;
    var i: usize = 0;
    while (i < inner.len) {
        const ch = inner[i];
        if (ch == '\'') {
            i = setops.quotedEnd(inner, i);
            continue;
        }
        if (ch != '"' and !setops.startsWord(inner, i)) {
            i += 1;
            continue;
        }
        const end = if (ch == '"') setops.quotedEnd(inner, i) else i + setops.wordAt(inner, i).len;
        const dotted = end < inner.len and inner[end] == '.';
        // A dotted word right after a dot is a column or a nested field, not a qualifier.
        const after_dot = i > 0 and inner[i - 1] == '.';
        if (!after_dot) for (names) |n| if (sameName(n, inner[i..end])) {
            // Undotted, it names a table of the subquery's own.
            if (!dotted) return false;
            qualifies = true;
        };
        i = end;
    }
    return qualifies;
}

/// A word or a double-quoted name after `from`, spaces skipped.
fn nameAt(text: []const u8, from: usize) ?struct { text: []const u8, end: usize } {
    const s = skipSpace(text, from);
    if (s >= text.len) return null;
    if (text[s] == '"') {
        const e = setops.quotedEnd(text, s);
        if (e - s < 3 or text[e - 1] != '"') return null;
        return .{ .text = text[s..e], .end = e };
    }
    const w = setops.wordAt(text, s);
    if (w.len == 0) return null;
    return .{ .text = w, .end = s + w.len };
}

fn unquote(name: []const u8) []const u8 {
    return if (name.len >= 2 and name[0] == '"') name[1 .. name.len - 1] else name;
}

fn sameName(a: []const u8, b: []const u8) bool {
    return std.ascii.eqlIgnoreCase(unquote(a), unquote(b));
}

fn skipSpace(text: []const u8, from: usize) usize {
    var i = from;
    while (i < text.len and std.ascii.isWhitespace(text[i])) i += 1;
    return i;
}

/// Just past the `)` that closes the `(` at `open`, or null when none does.
fn closing(text: []const u8, open: usize) ?usize {
    var depth: usize = 0;
    var i = open;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(text, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') {
            depth -= 1;
            if (depth == 0) return i + 1;
        }
        i += 1;
    }
    return null;
}

/// A parenthesized query: SELECT or WITH first, under any parentheses of its own.
fn isQuery(inner: []const u8) bool {
    var i = skipSpace(inner, 0);
    while (i < inner.len and inner[i] == '(') i = skipSpace(inner, i + 1);
    const w = setops.wordAt(inner, i);
    return setops.eql(w, "select") or setops.eql(w, "with");
}

/// The words that end a FROM clause at its level.
fn endsFrom(word: []const u8) bool {
    const enders = [_][]const u8{ "where", "group", "having", "order", "limit", "offset", "qualify", "window", "union", "intersect", "except" };
    for (enders) |e| if (std.ascii.eqlIgnoreCase(word, e)) return true;
    return false;
}

/// Whether an alias follows a table reference that ends at `from`: AS, a quoted name, or a word that is not a keyword the
/// grammar can put after a table.
fn aliasFollows(text: []const u8, from: usize) bool {
    const s = skipSpace(text, from);
    if (s >= text.len) return false;
    if (text[s] == '"') return true;
    const w = setops.wordAt(text, s);
    if (w.len == 0) return false;
    const after = [_][]const u8{
        "where",   "group",  "having",    "order", "limit",  "offset",   "qualify", "window", "union",
        "intersect", "except", "join",    "inner", "left",   "right",    "full",    "cross",  "natural",
        "on",      "using",  "semi",      "anti",  "positional", "asof", "tablesample", "using_sample",
    };
    for (after) |k| if (std.ascii.eqlIgnoreCase(w, k)) return false;
    return true;
}

const testing = std.testing;

fn expectPlan(sql: []const u8, stages: []const []const u8, final: []const u8) !void {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const p = (try plan(arena_state.allocator(), sql, "d")).?;
    try testing.expectEqual(stages.len, p.stages.len);
    for (stages, p.stages) |want, got| try testing.expectEqualStrings(want, got.sql);
    try testing.expectEqualStrings(final, p.sql);
}

test "a CTE is staged once, where it is first read, and every read names its file" {
    try expectPlan(
        "WITH c AS (SELECT g, sum(v) AS s FROM 't.parquet' GROUP BY g) SELECT * FROM c WHERE s > (SELECT avg(s) FROM c)",
        &.{"SELECT g, sum(v) AS s FROM 't.parquet' GROUP BY g"},
        "SELECT * FROM 'd/cte0.parquet' AS c WHERE s > (SELECT avg(s) FROM 'd/cte0.parquet' AS c)",
    );
    // A later CTE reads an earlier one; the earlier one is staged first. An unread CTE is never staged.
    try expectPlan(
        "with a as (select x from 't'), unused as (select 1/0 from 't'), b(y) as (select x + 1 AS z from a) select y from b x",
        &.{ "select x from 't'", "select x + 1 AS z from 'd/cte0.parquet' AS a" },
        "select y from 'd/cte1.parquet' x",
    );
    // A join of two CTEs, an alias with AS, and a name in another case or quoted.
    try expectPlan(
        "WITH \"Big\" AS (SELECT 1 AS k FROM 't'), small AS NOT MATERIALIZED (SELECT 2 AS k FROM 'u') SELECT count(*) AS n FROM big AS l JOIN \"SMALL\" ON l.k = \"SMALL\".k, big",
        &.{ "SELECT 1 AS k FROM 't'", "SELECT 2 AS k FROM 'u'" },
        "SELECT count(*) AS n FROM 'd/cte0.parquet' AS l JOIN 'd/cte1.parquet' AS \"SMALL\" ON l.k = \"SMALL\".k, 'd/cte0.parquet' AS big",
    );
}

test "a derived table is a stage, nested ones first, and its column list names the stage's columns" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const p = (try plan(a, "SELECT t.a FROM (SELECT x, y FROM (SELECT x, y FROM 'f' WHERE x > 1) AS i) AS t(a, b) JOIN 'g' ON t.a = k", "d")).?;
    try testing.expectEqual(@as(usize, 2), p.stages.len);
    try testing.expectEqualStrings("SELECT x, y FROM 'f' WHERE x > 1", p.stages[0].sql);
    try testing.expectEqualStrings("SELECT x, y FROM 'd/cte0.parquet' AS i", p.stages[1].sql);
    try testing.expectEqual(@as(usize, 2), p.stages[1].columns.len);
    try testing.expectEqualStrings("b", p.stages[1].columns[1]);
    try testing.expectEqualStrings("SELECT t.a FROM 'd/cte1.parquet' AS t JOIN 'g' ON t.a = k", p.sql);
    // No alias, a comma join, and a set operation inside.
    try expectPlan(
        "SELECT * FROM 'f', ((SELECT 1 AS x FROM 'g') UNION ALL (SELECT 2 FROM 'g')) WHERE 1 = 1",
        &.{"(SELECT 1 AS x FROM 'g') UNION ALL (SELECT 2 FROM 'g')"},
        "SELECT * FROM 'f', 'd/cte0.parquet' WHERE 1 = 1",
    );
}

test "a FROM inside a function, a quoted file and a table function are no table names" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expect((try plan(a, "SELECT EXTRACT(year FROM d) AS y FROM 'c' WHERE x IN (SELECT x FROM 'u')", "d")) == null);
    try expectPlan(
        "WITH d AS (SELECT 1 AS d FROM 't') SELECT EXTRACT(year FROM d) AS y, substring(s FROM 2) AS z FROM read_parquet('d'), 'd', d",
        &.{"SELECT 1 AS d FROM 't'"},
        "SELECT EXTRACT(year FROM d) AS y, substring(s FROM 2) AS z FROM read_parquet('d'), 'd', 'd/cte0.parquet' AS d",
    );
    // An inner WITH shadows an outer CTE of the same name, inside its own query only.
    try expectPlan(
        "WITH c AS (SELECT 1 AS x FROM 't') SELECT * FROM (WITH c AS (SELECT 2 AS x FROM 't') SELECT * FROM c) s, c",
        &.{ "SELECT 2 AS x FROM 't'", "SELECT * FROM 'd/cte0.parquet' AS c", "SELECT 1 AS x FROM 't'" },
        "SELECT * FROM 'd/cte1.parquet' AS s, 'd/cte2.parquet' AS c",
    );
}

test "recursive, circular and duplicate CTEs and LATERAL are refused by name" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expectError(error.CteRecursive, plan(a, "WITH RECURSIVE c AS (SELECT 1) SELECT * FROM c", "d"));
    try testing.expectError(error.CteCircular, plan(a, "WITH c AS (SELECT * FROM c) SELECT * FROM c", "d"));
    try testing.expectError(error.CteCircular, plan(a, "WITH b AS (SELECT 1 AS x FROM 't'), c AS (SELECT * FROM b, (SELECT * FROM c) z) SELECT * FROM c", "d"));
    try testing.expectError(error.CteDuplicate, plan(a, "WITH c AS (SELECT 1), C AS (SELECT 2) SELECT * FROM c", "d"));
    try testing.expectError(error.LateralNotSupported, plan(a, "SELECT * FROM 't' x, LATERAL (SELECT x.a) y", "d"));
    // DuckDB reads a subquery in FROM that names a table to its left as a LATERAL one.
    try testing.expectError(error.LateralNotSupported, plan(a, "SELECT * FROM 'dir/t.parquet', (SELECT max(v) AS m FROM 'u' y WHERE y.g = t.g) z", "d"));
    try testing.expectError(error.LateralNotSupported, plan(a, "WITH c AS (SELECT 1 AS g FROM 't') SELECT * FROM c JOIN (SELECT * FROM 'u' WHERE u.g = c.g) z ON z.g = c.g", "d"));
    // Its own table of the same name, or a column of that name, is no outer reference.
    try testing.expect((try plan(a, "SELECT * FROM 't' x, (SELECT x.a FROM 'u' x) z", "d")) != null);
    try testing.expect((try plan(a, "SELECT * FROM 't' x, (SELECT s.x FROM 'u' s) z", "d")) != null);
    try testing.expectError(error.SqlParseError, plan(a, "WITH c (SELECT 1) SELECT 1", "d"));
    try testing.expectError(error.SqlParseError, plan(a, "WITH c AS SELECT 1", "d"));
    try testing.expectError(error.SqlParseError, plan(a, "SELECT * FROM (SELECT 1) t(a,)", "d"));
    // A CTE body that is not read is not even planned, but its list still parses.
    try testing.expect((try plan(a, "WITH c AS (SELECT * FROM c) SELECT 1 AS one FROM 't'", "d")) != null);
}
