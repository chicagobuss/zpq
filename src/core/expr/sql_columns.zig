//! `COLUMNS(...)` in a SELECT list. DuckDB writes the select item that holds one once for each column it names, with the
//! `COLUMNS(...)` replaced by that column and the column's name as the item's name (`max(COLUMNS('^t')) + 1` is
//! `max(tu) + 1 AS tu, max(tm) + 1 AS tm`). The names come from the table's footer, so this runs on the query text before
//! it is parsed, given a way to ask for them.
const std = @import("std");
const regex = @import("regex.zig");
const sql_setops = @import("sql_setops.zig");

pub const Error = error{ OutOfMemory, ColumnsNotSupported, ColumnsNoMatch, InvalidRegex };

/// How the expansion learns the top-level column names of the table a query reads, in schema order.
pub const Schema = struct {
    ctx: *anyopaque,
    names: *const fn (ctx: *anyopaque, arena: std.mem.Allocator, table: []const u8) anyerror![]const []const u8,
};

/// `sql` with each select item that holds `COLUMNS(...)` written once per column it names; `sql` itself where it holds none.
/// What is not exact is refused: `COLUMNS` outside the SELECT list, two in one item, a lambda or a list of names, a table
/// that is not one file or glob.
pub fn expand(arena: std.mem.Allocator, sql: []const u8, schema: Schema) anyerror![]const u8 {
    if (!mentions(sql)) return sql;
    const head = selectStart(sql) orelse return error.ColumnsNotSupported;
    var items: std.ArrayList([]const u8) = .empty;
    var depth: usize = 0;
    var from: ?usize = null;
    var item_start = head;
    var i = head;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            i = sql_setops.quotedEnd(sql, i);
            continue;
        }
        switch (ch) {
            '(', '[' => depth += 1,
            ')', ']' => {
                if (depth == 0) break;
                depth -= 1;
            },
            ',' => if (depth == 0) {
                try items.append(arena, sql[item_start..i]);
                item_start = i + 1;
            },
            else => {},
        }
        if (depth == 0 and sql_setops.startsWord(sql, i)) {
            const word = sql_setops.wordAt(sql, i);
            if (sql_setops.eql(word, "from")) {
                from = i;
                break;
            }
            i += word.len;
            continue;
        }
        i += 1;
    }
    const from_at = from orelse return error.ColumnsNotSupported;
    try items.append(arena, sql[item_start..from_at]);
    // A set operation or a grouping-set query is run as the queries it is made of, each of which comes back here.
    if (splits(sql[from_at..])) return sql;
    // The rest of the query reads the columns by name; COLUMNS there is a different feature.
    if (mentions(sql[from_at..])) return error.ColumnsNotSupported;

    const table = try tableOf(arena, sql[from_at + 4 ..]);
    var names: ?[]const []const u8 = null;
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, sql[0..head]);
    try out.append(arena, ' ');
    var first = true;
    for (items.items) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        const call = find(item) orelse {
            if (!first) try out.appendSlice(arena, ", ");
            first = false;
            try out.appendSlice(arena, item);
            continue;
        };
        if (find(item[call.end..]) != null) return error.ColumnsNotSupported;
        if (names == null) names = try schema.names(schema.ctx, arena, table);
        const matched = try select(arena, item[call.args_start..call.args_end], names.?);
        if (matched.len == 0) return error.ColumnsNoMatch;
        const before = item[0..call.start];
        const after = item[call.end..];
        const bare = std.mem.trim(u8, before, " \t\r\n").len == 0 and std.mem.trim(u8, after, " \t\r\n").len == 0;
        const aliased = hasAlias(item);
        for (matched) |name| {
            if (!first) try out.appendSlice(arena, ", ");
            first = false;
            try out.appendSlice(arena, before);
            try quoted(arena, &out, name);
            try out.appendSlice(arena, after);
            if (!bare and !aliased) {
                try out.appendSlice(arena, " AS ");
                try quoted(arena, &out, name);
            }
        }
    }
    try out.append(arena, ' ');
    try out.appendSlice(arena, sql[from_at..]);
    return out.items;
}

/// Whether `sql` has the word `columns` followed by an open parenthesis, outside quoted text.
fn mentions(sql: []const u8) bool {
    return find(sql) != null;
}

/// Whether `sql` has a set operator or a grouping-set clause outside quoted text.
fn splits(sql: []const u8) bool {
    var i: usize = 0;
    while (i < sql.len) {
        if (sql[i] == '\'' or sql[i] == '"') {
            i = sql_setops.quotedEnd(sql, i);
            continue;
        }
        if (sql_setops.startsWord(sql, i)) {
            const word = sql_setops.wordAt(sql, i);
            for ([_][]const u8{ "union", "intersect", "except", "rollup", "cube", "grouping" }) |w| {
                if (std.ascii.eqlIgnoreCase(word, w)) return true;
            }
            i += word.len;
            continue;
        }
        i += 1;
    }
    return false;
}

const Call = struct { start: usize, end: usize, args_start: usize, args_end: usize };

/// The first `COLUMNS( ... )` in `sql`, outside quoted text.
fn find(sql: []const u8) ?Call {
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            i = sql_setops.quotedEnd(sql, i);
            continue;
        }
        if (sql_setops.startsWord(sql, i)) {
            const word = sql_setops.wordAt(sql, i);
            if (sql_setops.eql(word, "columns")) {
                var open = i + word.len;
                while (open < sql.len and std.ascii.isWhitespace(sql[open])) open += 1;
                if (open < sql.len and sql[open] == '(') {
                    var depth: usize = 0;
                    var k = open;
                    while (k < sql.len) {
                        const at = sql[k];
                        if (at == '\'' or at == '"') {
                            k = sql_setops.quotedEnd(sql, k);
                            continue;
                        }
                        if (at == '(') depth += 1;
                        if (at == ')') {
                            depth -= 1;
                            if (depth == 0) return .{ .start = i, .end = k + 1, .args_start = open + 1, .args_end = k };
                        }
                        k += 1;
                    }
                    return null;
                }
            }
            i += word.len;
            continue;
        }
        i += 1;
    }
    return null;
}

/// Where the select list starts: after `SELECT` and an optional `DISTINCT`.
fn selectStart(sql: []const u8) ?usize {
    var i: usize = 0;
    while (i < sql.len and std.ascii.isWhitespace(sql[i])) i += 1;
    const first = sql_setops.nextWord(sql, i);
    if (!sql_setops.eql(first.word, "select")) return null;
    const second = sql_setops.nextWord(sql, first.end);
    if (sql_setops.eql(second.word, "distinct")) return second.end;
    return first.end;
}

/// The table a FROM clause reads: one quoted path or bare name, and nothing joined to it.
fn tableOf(arena: std.mem.Allocator, rest: []const u8) Error![]const u8 {
    var i: usize = 0;
    while (i < rest.len and std.ascii.isWhitespace(rest[i])) i += 1;
    if (i >= rest.len) return error.ColumnsNotSupported;
    var table: []const u8 = undefined;
    var end: usize = undefined;
    if (rest[i] == '\'' or rest[i] == '"') {
        end = sql_setops.quotedEnd(rest, i);
        const doubled = [_]u8{ rest[i], rest[i] };
        table = try std.mem.replaceOwned(u8, arena, rest[i + 1 .. end - 1], &doubled, rest[i .. i + 1]);
    } else {
        end = i;
        while (end < rest.len and !std.ascii.isWhitespace(rest[end]) and rest[end] != ';' and rest[end] != ')' and rest[end] != ',') end += 1;
        table = rest[i..end];
    }
    // A join, a comma or a subquery brings other columns in; the alias and the clauses after it do not.
    var tail = end;
    while (tail < rest.len and std.ascii.isWhitespace(rest[tail])) tail += 1;
    if (tail < rest.len and rest[tail] == ',') return error.ColumnsNotSupported;
    var j = tail;
    while (j < rest.len) {
        if (rest[j] == '\'' or rest[j] == '"') {
            j = sql_setops.quotedEnd(rest, j);
            continue;
        }
        if (sql_setops.startsWord(rest, j)) {
            const word = sql_setops.wordAt(rest, j);
            if (sql_setops.eql(word, "join") or sql_setops.eql(word, "cross") or sql_setops.eql(word, "natural") or
                sql_setops.eql(word, "using") or sql_setops.eql(word, "select"))
                return error.ColumnsNotSupported;
            j += word.len;
            continue;
        }
        j += 1;
    }
    if (table.len == 0) return error.ColumnsNotSupported;
    return table;
}

/// The names `args` picks from `names`: a regular expression matched anywhere in a name, or `*` with an optional
/// `EXCLUDE (names)`.
fn select(arena: std.mem.Allocator, args: []const u8, names: []const []const u8) Error![]const []const u8 {
    const text = std.mem.trim(u8, args, " \t\r\n");
    var picked: std.ArrayList([]const u8) = .empty;
    if (text.len > 0 and text[0] == '\'') {
        if (sql_setops.quotedEnd(text, 0) != text.len) return error.ColumnsNotSupported;
        const pattern = try std.mem.replaceOwned(u8, arena, text[1 .. text.len - 1], "''", "'");
        const m = regex.Match.compile(arena, pattern, "") catch return error.InvalidRegex;
        var scratch: regex.Scratch = .init(arena);
        for (names) |name| if (m.apply(&scratch, name) catch return error.InvalidRegex) try picked.append(arena, name);
        return picked.items;
    }
    if (text.len > 0 and text[0] == '*') {
        const rest = std.mem.trim(u8, text[1..], " \t\r\n");
        var excluded: std.ArrayList([]const u8) = .empty;
        if (rest.len > 0) {
            const word = sql_setops.nextWord(rest, 0);
            if (!sql_setops.eql(word.word, "exclude")) return error.ColumnsNotSupported;
            var list = std.mem.trim(u8, rest[word.end..], " \t\r\n");
            if (list.len > 1 and list[0] == '(' and list[list.len - 1] == ')') list = list[1 .. list.len - 1];
            var parts = std.mem.splitScalar(u8, list, ',');
            while (parts.next()) |part| {
                var id = std.mem.trim(u8, part, " \t\r\n");
                if (id.len >= 2 and id[0] == '"' and id[id.len - 1] == '"') id = id[1 .. id.len - 1];
                if (id.len == 0) return error.ColumnsNotSupported;
                try excluded.append(arena, id);
            }
        }
        for (names) |name| {
            var skip = false;
            for (excluded.items) |e| if (std.ascii.eqlIgnoreCase(e, name)) {
                skip = true;
            };
            if (!skip) try picked.append(arena, name);
        }
        return picked.items;
    }
    return error.ColumnsNotSupported;
}

/// Whether the item ends in `AS name`: the last `AS` outside parentheses and quotes, with one name after it.
fn hasAlias(item: []const u8) bool {
    var depth: usize = 0;
    var i: usize = 0;
    var last_as: ?usize = null;
    while (i < item.len) {
        const ch = item[i];
        if (ch == '\'' or ch == '"') {
            i = sql_setops.quotedEnd(item, i);
            continue;
        }
        if (ch == '(' or ch == '[') depth += 1;
        if (ch == ')' or ch == ']') depth -|= 1;
        if (depth == 0 and sql_setops.startsWord(item, i)) {
            const word = sql_setops.wordAt(item, i);
            if (sql_setops.eql(word, "as")) last_as = i + word.len;
            i += word.len;
            continue;
        }
        i += 1;
    }
    const at = last_as orelse return false;
    const name = std.mem.trim(u8, item[at..], " \t\r\n");
    return name.len > 0 and std.mem.indexOfAny(u8, name, " \t\r\n,()") == null;
}

fn quoted(arena: std.mem.Allocator, out: *std.ArrayList(u8), name: []const u8) !void {
    try out.append(arena, '"');
    for (name) |ch| {
        if (ch == '"') try out.append(arena, '"');
        try out.append(arena, ch);
    }
    try out.append(arena, '"');
}

const test_names = [_][]const u8{ "id", "d", "dn", "tu", "tm", "tz" };

fn testNames(_: *anyopaque, _: std.mem.Allocator, _: []const u8) anyerror![]const []const u8 {
    return &test_names;
}

test "COLUMNS(regex) expands to the columns it matches anywhere in their names, named as they are" {
    var arena_state: std.heap.ArenaAllocator = .init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var dummy: u8 = 0;
    const schema: Schema = .{ .ctx = &dummy, .names = testNames };
    const cases = [_][2][]const u8{
        .{ "SELECT max(COLUMNS('^t')) FROM 'f.parquet'", "SELECT max(\"tu\") AS \"tu\", max(\"tm\") AS \"tm\", max(\"tz\") AS \"tz\" FROM 'f.parquet'" },
        .{ "select COLUMNS('^d') from f", "select \"d\", \"dn\" from f" },
        .{ "SELECT id, min(COLUMNS('d')) + 1 AS v FROM f", "SELECT id, min(\"id\") + 1 AS v, min(\"d\") + 1 AS v, min(\"dn\") + 1 AS v FROM f" },
        .{ "SELECT count(*), max(COLUMNS(* EXCLUDE (id, d, dn))) FROM f", "SELECT count(*), max(\"tu\") AS \"tu\", max(\"tm\") AS \"tm\", max(\"tz\") AS \"tz\" FROM f" },
        .{ "SELECT sum(CAST(COLUMNS('^d$') AS BIGINT)) FROM f", "SELECT sum(CAST(\"d\" AS BIGINT)) AS \"d\" FROM f" },
        .{ "SELECT 'COLUMNS(x)', a FROM f", "SELECT 'COLUMNS(x)', a FROM f" },
    };
    for (cases) |case| {
        const got = try expand(a, case[0], schema);
        try std.testing.expectEqualStrings(case[1], got);
    }
    // A set operation is expanded branch by branch, later.
    const compound = "SELECT max(COLUMNS('^t')) FROM f UNION ALL SELECT max(COLUMNS('^t')) FROM g";
    try std.testing.expectEqualStrings(compound, try expand(a, compound, schema));
    try std.testing.expectError(error.ColumnsNoMatch, expand(a, "SELECT max(COLUMNS('zzz')) FROM f", schema));
    try std.testing.expectError(error.ColumnsNotSupported, expand(a, "SELECT a FROM f WHERE COLUMNS('^t') > 0", schema));
    try std.testing.expectError(error.ColumnsNotSupported, expand(a, "SELECT max(COLUMNS('^t')) + max(COLUMNS('^d')) FROM f", schema));
    try std.testing.expectError(error.ColumnsNotSupported, expand(a, "SELECT max(COLUMNS(['d'])) FROM f", schema));
    try std.testing.expectError(error.ColumnsNotSupported, expand(a, "SELECT max(COLUMNS('^t')) FROM f JOIN g ON f.a = g.a", schema));
    try std.testing.expectError(error.InvalidRegex, expand(a, "SELECT max(COLUMNS('(')) FROM f", schema));
}
