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

/// A CTE's or a derived table's query, as the text that reads it will: inlined into it, or a stage's file.
const Source = struct {
    /// Its query, naming no CTE and no derived table.
    sql: []const u8,
    columns: []const []const u8,
    /// Its stage's file, once staged.
    out: ?[]const u8 = null,

    fn file(self: *Source, p: *Planner) Error![]const u8 {
        if (self.out == null) self.out = try p.stage(self.sql, self.columns);
        return self.out.?;
    }
};

const Cte = struct {
    name: []const u8,
    columns: []const []const u8,
    body: []const u8,
    /// Planned the first time it is read.
    source: ?*Source = null,
    /// Its body is being planned: a reference to it now is circular.
    defining: bool = false,
};

/// The CTEs a piece of text sees: the first `len` of one WITH list, then the ones the enclosing query saw.
const View = struct {
    ctes: []Cte,
    len: usize,
    parent: ?*const View,
};

/// A filter of one file that a reference reads in place of a stage: `'file'`, its WHERE, and whether it keeps every
/// column (a `*` over it reads the same columns either way).
const Inline = struct { file: []const u8, where: ?[]const u8, star: bool };

/// A reference this level wrote as an inline source's file, at `out[start..end]`.
const Pending = struct { source: *Source, inline_: Inline, start: usize, end: usize };

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

    /// The CTE `name`, planned the first time; null when `view` has no CTE of that name.
    fn lookup(self: *Planner, view: ?*const View, name: []const u8) Error!?*Source {
        var v = view;
        while (v) |at| : (v = at.parent) {
            var k = at.len;
            while (k > 0) {
                k -= 1;
                const c = &at.ctes[k];
                if (!sameName(c.name, name)) continue;
                if (c.source) |s| return s;
                if (c.defining) return error.CteCircular;
                // The body sees the CTEs before it in its list, and itself (to name a circular reference).
                c.defining = true;
                const inner: View = .{ .ctes = at.ctes, .len = k + 1, .parent = at.parent };
                const sql = try self.query(&inner, c.body);
                c.defining = false;
                c.source = try self.source(sql, c.columns);
                return c.source;
            }
        }
        return null;
    }

    fn source(self: *Planner, sql: []const u8, columns: []const []const u8) Error!*Source {
        const s = try self.arena.create(Source);
        s.* = .{ .sql = sql, .columns = columns };
        self.changed = true;
        return s;
    }

    fn stage(self: *Planner, sql: []const u8, columns: []const []const u8) Error![]const u8 {
        const file = try std.fmt.allocPrint(self.arena, "cte{d}.parquet", .{self.stages.items.len});
        const out = try std.fs.path.join(self.arena, &.{ self.dir, file });
        try self.stages.append(self.arena, .{ .sql = sql, .out = out, .columns = columns });
        return out;
    }

    /// Writes the reference to `s` called `name`: the filtered file itself when it may be inlined (`pending`
    /// decides at the level's end), else its stage's file.
    fn reference(self: *Planner, out: *std.ArrayList(u8), pending: *std.ArrayList(Pending), s: *Source, name: ?[]const u8) Error!void {
        const start = out.items.len;
        const in_place = if (s.columns.len == 0 and s.out == null) inlineable(s.sql) else null;
        if (in_place) |inl| try out.appendSlice(self.arena, inl.file) else try out.print(self.arena, "'{s}'", .{try s.file(self)});
        if (in_place) |inl| try pending.append(self.arena, .{ .source = s, .inline_ = inl, .start = start, .end = out.items.len });
        if (name) |n| try out.print(self.arena, " AS {s}", .{n});
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
        // What inlining a filter needs of this level: one table, one SELECT, its WHERE's span in `out`, and whether a
        // `*` or COLUMNS(...) would see the file's columns where the filter kept fewer.
        var pending: std.ArrayList(Pending) = .empty;
        var tables: usize = 0;
        var selects: usize = 0;
        var stars = false;
        var where: ?struct { start: usize, end: ?usize } = null;
        var from_end: ?usize = null;
        var i: usize = 0;
        while (i < text.len) {
            const ch = text[i];
            if (ch == '\'') {
                const end = setops.quotedEnd(text, i);
                try out.appendSlice(self.arena, text[i..end]);
                if (expect_table) {
                    const base = std.fs.path.basename(text[i + 1 .. end - 1]);
                    try left.append(self.arena, aliasAt(text, end) orelse base[0 .. std.mem.indexOfScalar(u8, base, '.') orelse base.len]);
                    tables += 1;
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
                    tables += 1;
                    if (ch != '"' and setops.eql(token, "lateral")) return error.LateralNotSupported;
                    try left.append(self.arena, aliasAt(text, end) orelse token);
                    const next = skipSpace(text, end);
                    // A table function (`read_parquet(...)`) or a qualified name is no CTE.
                    const plain = next >= text.len or (text[next] != '(' and text[next] != '.');
                    if (plain) if (try self.lookup(view, token)) |s| {
                        try self.reference(&out, &pending, s, if (aliasFollows(text, end)) null else token);
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
                        selects += 1;
                    } else if (is_query and setops.eql(token, "from")) {
                        in_from = true;
                        expect_table = true;
                        left.clearRetainingCapacity();
                    } else if (in_from and setops.eql(token, "join")) {
                        expect_table = true;
                    } else if (in_from and endsFrom(token)) {
                        in_from = false;
                        from_end = std.mem.trimEnd(u8, out.items, " \t\r\n").len;
                    } else if (setops.eql(token, "columns") and skipSpace(text, end) < text.len and text[skipSpace(text, end)] == '(') {
                        stars = true;
                    }
                    if (where) |*w| if (w.end == null and endsFrom(token)) {
                        w.end = std.mem.trimEnd(u8, out.items, " \t\r\n").len;
                    };
                    if (is_query and !in_from_start and setops.eql(token, "where") and where == null) {
                        try out.appendSlice(self.arena, token);
                        where = .{ .start = out.items.len, .end = null };
                        i = end;
                        continue;
                    }
                }
                try out.appendSlice(self.arena, token);
                i = end;
                continue;
            }
            if (ch == ',' and in_from) expect_table = true;
            if (ch == '*' and starItem(text, i)) stars = true;
            if (ch == '(') {
                const close = closing(text, i) orelse {
                    try out.appendSlice(self.arena, text[i..]);
                    break;
                };
                const inner = text[i + 1 .. close - 1];
                if (expect_table) {
                    expect_table = false;
                    tables += 1;
                    if (isQuery(inner)) {
                        if (reads(inner, left.items)) return error.LateralNotSupported;
                        const alias = try self.derivedAlias(text, close);
                        if (alias.name) |name| try left.append(self.arena, name);
                        try self.reference(&out, &pending, try self.source(try self.query(view, inner), alias.columns), alias.name);
                        i = alias.end;
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
        if (pending.items.len == 0) return out.items;
        // A filter is inlined only into a SELECT whose one table it is: its WHERE joins the query's own.
        const p = pending.items[0];
        if (pending.items.len == 1 and tables == 1 and selects == 1 and !in_from_start and (p.inline_.star or !stars)) {
            const cond = p.inline_.where orelse return out.items;
            const last = std.mem.trimEnd(u8, out.items, " \t\r\n;").len;
            if (where) |w| {
                try out.insertSlice(self.arena, w.end orelse last, ")");
                try out.insertSlice(self.arena, w.start, try std.fmt.allocPrint(self.arena, " ({s}) AND (", .{cond}));
            } else try out.insertSlice(self.arena, from_end orelse last, try std.fmt.allocPrint(self.arena, " WHERE {s}", .{cond}));
            return out.items;
        }
        // Otherwise each is staged: its file stands where the filtered file was written.
        var k = pending.items.len;
        while (k > 0) {
            k -= 1;
            const q = pending.items[k];
            try out.replaceRange(self.arena, q.start, q.end - q.start, try std.fmt.allocPrint(self.arena, "'{s}'", .{try q.source.file(self)}));
        }
        return out.items;
    }

    /// The alias after a derived table, which may name its columns (`AS t(a, b)`).
    fn derivedAlias(self: *Planner, text: []const u8, from: usize) Error!struct { name: ?[]const u8, columns: []const []const u8, end: usize } {
        if (!aliasFollows(text, from)) return .{ .name = null, .columns = &.{}, .end = from };
        const w = setops.nextWord(text, from);
        const name = nameAt(text, if (setops.eql(w.word, "as")) w.end else from) orelse return error.SqlParseError;
        const next = skipSpace(text, name.end);
        if (next < text.len and text[next] == '(') {
            const close = closing(text, next) orelse return error.SqlParseError;
            return .{ .name = name.text, .columns = try self.names(text[next + 1 .. close - 1]), .end = close };
        }
        return .{ .name = name.text, .columns = &.{}, .end = name.end };
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

/// `sql` as a filter of one file, when it is one: `SELECT * | col, ... FROM 'file' [[AS] alias] [WHERE cond]`, with
/// plain columns and a condition that qualifies nothing (the alias is gone once inlined) and holds no query.
fn inlineable(sql: []const u8) ?Inline {
    const t = std.mem.trim(u8, sql, " \t\r\n");
    var w = setops.nextWord(t, 0);
    if (!setops.eql(w.word, "select")) return null;
    var i = skipSpace(t, w.end);
    var star = false;
    if (i < t.len and t[i] == '*') {
        star = true;
        i += 1;
    } else while (true) {
        const name = nameAt(t, i) orelse return null;
        if (setops.eql(name.text, "distinct") or setops.eql(name.text, "all") or std.ascii.isDigit(name.text[0])) return null;
        i = skipSpace(t, name.end);
        if (i >= t.len or t[i] != ',') break;
        i += 1;
    }
    w = setops.nextWord(t, i);
    if (!setops.eql(w.word, "from")) return null;
    i = skipSpace(t, w.end);
    if (i >= t.len or t[i] != '\'') return null;
    const file_end = setops.quotedEnd(t, i);
    const file = t[i..file_end];
    i = file_end;
    if (aliasAt(t, i)) |_| {
        w = setops.nextWord(t, i);
        i = (nameAt(t, if (setops.eql(w.word, "as")) w.end else i) orelse return null).end;
    }
    w = setops.nextWord(t, i);
    if (w.word.len == 0) return if (skipSpace(t, i) == t.len) .{ .file = file, .where = null, .star = star } else null;
    if (!setops.eql(w.word, "where")) return null;
    const cond = std.mem.trim(u8, t[w.end..], " \t\r\n");
    var depth: usize = 0;
    var k: usize = 0;
    while (k < cond.len) {
        const ch = cond[k];
        if (ch == '\'') {
            k = setops.quotedEnd(cond, k);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') depth -|= 1;
        if (ch == '"' or setops.startsWord(cond, k)) {
            const end = if (ch == '"') setops.quotedEnd(cond, k) else k + setops.wordAt(cond, k).len;
            const token = cond[k..end];
            if (ch != '"' and setops.eql(token, "select")) return null;
            if (depth == 0 and ch != '"' and endsFrom(token)) return null;
            if (end < cond.len and cond[end] == '.' and !std.ascii.isDigit(token[0])) return null;
            k = end;
            continue;
        }
        k += 1;
    }
    return .{ .file = file, .where = cond, .star = star };
}

/// Whether the `*` at `i` is a select item (`SELECT *`, `t.*`, `a, *`), not a product or `count(*)`.
fn starItem(text: []const u8, i: usize) bool {
    var k = i;
    while (k > 0 and std.ascii.isWhitespace(text[k - 1])) k -= 1;
    if (k == 0) return false;
    if (text[k - 1] == ',' or text[k - 1] == '.') return true;
    var s = k;
    while (s > 0 and setops.isWord(text[s - 1])) s -= 1;
    const w = text[s..k];
    return setops.eql(w, "select") or setops.eql(w, "distinct") or setops.eql(w, "all");
}

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
    // A later CTE reads an earlier one, here inlined as the filter it is. An unread CTE is never planned.
    try expectPlan(
        "with a as (select x from 't'), unused as (select 1/0 from 't'), b(y) as (select x + 1 AS z from a) select y from b x",
        &.{"select x + 1 AS z from 't' AS a"},
        "select y from 'd/cte0.parquet' x",
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
    // The innermost is a filter, inlined into the one that reads it; that one names its columns, so it is staged.
    try testing.expectEqual(@as(usize, 1), p.stages.len);
    try testing.expectEqualStrings("SELECT x, y FROM 'f' AS i WHERE x > 1", p.stages[0].sql);
    try testing.expectEqual(@as(usize, 2), p.stages[0].columns.len);
    try testing.expectEqualStrings("b", p.stages[0].columns[1]);
    try testing.expectEqualStrings("SELECT t.a FROM 'd/cte0.parquet' AS t JOIN 'g' ON t.a = k", p.sql);
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
        &.{ "SELECT 2 AS x FROM 't'", "SELECT 1 AS x FROM 't'", "SELECT * FROM 'd/cte0.parquet' AS c" },
        "SELECT * FROM 'd/cte2.parquet' AS s, 'd/cte1.parquet' AS c",
    );
}

test "a filter of one file is inlined into a query whose one table it is, and staged anywhere else" {
    try expectPlan(
        "WITH l AS (SELECT a, b FROM 'f' WHERE a > 1 OR a IS NULL) SELECT b, count(*) AS n FROM l WHERE b < 3 OR b > 7 GROUP BY b",
        &.{},
        "SELECT b, count(*) AS n FROM 'f' AS l WHERE (a > 1 OR a IS NULL) AND ( b < 3 OR b > 7) GROUP BY b",
    );
    try expectPlan("SELECT x.b FROM (SELECT b FROM 'f' WHERE b IS NULL) x ORDER BY 1;", &.{}, "SELECT x.b FROM 'f' AS x WHERE b IS NULL ORDER BY 1;");
    try expectPlan("WITH c AS (SELECT * FROM 'f' WHERE g = 1) SELECT count(*) AS n FROM c t", &.{}, "SELECT count(*) AS n FROM 'f' t WHERE g = 1");
    try expectPlan("WITH c AS (SELECT * FROM 'f' AS z) SELECT *, 2 * g AS h FROM c", &.{}, "SELECT *, 2 * g AS h FROM 'f' AS c");
    // Read in a subquery and in the query, each inlined where it is read.
    try expectPlan(
        "WITH c AS (SELECT a FROM 'f' WHERE a > 1) SELECT sum(a) AS s FROM c WHERE a IN (SELECT a FROM c WHERE a < 9)",
        &.{},
        "SELECT sum(a) AS s FROM 'f' AS c WHERE (a > 1) AND ( a IN (SELECT a FROM 'f' AS c WHERE (a > 1) AND ( a < 9)))",
    );
    // A `*` would read the columns the filter dropped; a join's other table could own a column of the condition; a
    // qualified condition names an alias that is gone; a set operation has two SELECTs: all staged, once.
    try expectPlan("WITH c AS (SELECT a FROM 'f' WHERE a > 1) SELECT * FROM c", &.{"SELECT a FROM 'f' WHERE a > 1"}, "SELECT * FROM 'd/cte0.parquet' AS c");
    try expectPlan("WITH c AS (SELECT a FROM 'f' WHERE a > 1) SELECT COLUMNS('a') FROM c", &.{"SELECT a FROM 'f' WHERE a > 1"}, "SELECT COLUMNS('a') FROM 'd/cte0.parquet' AS c");
    try expectPlan(
        "WITH c AS (SELECT a FROM 'f' WHERE a > 1) SELECT count(*) AS n FROM c, 'g' WHERE a = k",
        &.{"SELECT a FROM 'f' WHERE a > 1"},
        "SELECT count(*) AS n FROM 'd/cte0.parquet' AS c, 'g' WHERE a = k",
    );
    try expectPlan("WITH c AS (SELECT a FROM 'f' x WHERE x.a > 1.5) SELECT sum(a) AS s FROM c", &.{"SELECT a FROM 'f' x WHERE x.a > 1.5"}, "SELECT sum(a) AS s FROM 'd/cte0.parquet' AS c");
    try expectPlan(
        "WITH c AS (SELECT a FROM 'f' WHERE a > 1) SELECT a FROM c UNION ALL SELECT a FROM c",
        &.{"SELECT a FROM 'f' WHERE a > 1"},
        "SELECT a FROM 'd/cte0.parquet' AS c UNION ALL SELECT a FROM 'd/cte0.parquet' AS c",
    );
    // Not filters: an expression, DISTINCT, a GROUP BY, an ORDER BY, a subquery in the condition.
    for ([_][]const u8{ "SELECT a + 1 AS b FROM 'f'", "SELECT DISTINCT a FROM 'f'", "SELECT a FROM 'f' WHERE a > 1 GROUP BY a", "SELECT a FROM 'f' ORDER BY a", "SELECT a FROM 'f' WHERE a IN (SELECT k FROM 'g')", "SELECT a FROM 'f', 'g'" }) |body|
        try testing.expect(inlineable(body) == null);
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
