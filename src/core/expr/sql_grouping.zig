//! `GROUP BY ROLLUP (...)`, `CUBE (...)` and `GROUPING SETS (...)` as the UNION ALL of one ordinary GROUP BY query per
//! grouping set, which the set-operation driver (`cli/setops.zig`) already runs. Text in, text out: the query is cut at
//! its top-level clauses, the sets are worked out as DuckDB works them out (the elements of a GROUP BY multiply), and
//! each set gets its own SELECT:
//!
//!  - a select item that is a grouping key the set lacks becomes `min(key) FILTER (WHERE key IS NULL AND key IS NOT NULL)`,
//!    an aggregate over no rows, so it is NULL in the key's own type;
//!  - `GROUPING(k, ...)` and `GROUPING_ID(k, ...)` become the set's constant, added to an empty `count(*)` (a BIGINT);
//!  - every other item, the FROM and WHERE, and the HAVING are the query's own, run per set;
//!  - the ORDER BY / LIMIT / OFFSET move to the whole result.
//!
//! A set that is empty is a scalar aggregate, which gives its one row over no input as DuckDB's grand total does.
const std = @import("std");
const setops = @import("sql_setops.zig");

pub const Error = error{
    OutOfMemory,
    SqlParseError,
    /// A key that some set leaves out is not a column, so there is no column to make the NULL from (or an ordinal).
    GroupingSetsKey,
    /// `SELECT DISTINCT` over grouping sets is a DISTINCT of the union, not of each set.
    GroupingSetsDistinct,
    /// ROLLUP or CUBE inside GROUPING SETS.
    GroupingSetsNesting,
    /// `GROUPING` of something that is not a grouping key.
    GroupingSetsGrouping,
    /// More sets than a query is allowed (each one is a scan).
    GroupingSetsTooMany,
    /// A QUALIFY clause beside the grouping sets.
    GroupingSetsQualify,
};

pub const max_sets = 64;

const Key = struct {
    /// As written.
    text: []const u8,
    /// Lower case, no spaces and no quotes: what two spellings of one column share.
    norm: []const u8,
    /// The column it is, when the text is a column (a qualified or quoted name); null for an expression.
    column: ?[]const u8,
    /// The name an unaliased select item of it has.
    name: []const u8,
};

/// The rewrite of `sql`, or null when it has no ROLLUP, CUBE or GROUPING SETS to rewrite.
pub fn expand(arena: std.mem.Allocator, sql: []const u8) Error!?setops.Compound {
    const text = std.mem.trim(u8, sql, " \t\r\n;");
    const c = (try clauses(arena, text)) orelse return null;
    var keys: std.ArrayList(Key) = .empty;
    var sets: std.ArrayList(std.ArrayList(usize)) = .empty;
    try sets.append(arena, .empty);
    var grouped = false;
    for (try commaList(arena, text[c.group_start..c.group_end])) |item_text| {
        const item = std.mem.trim(u8, item_text, " \t\r\n");
        if (item.len == 0) return error.SqlParseError;
        const alts = (try element(arena, item, &keys)) orelse blk: {
            // A plain key is in every set.
            const only = try arena.alloc(std.ArrayList(usize), 1);
            only[0] = .empty;
            try only[0].append(arena, try keyIndex(arena, &keys, item));
            break :blk only;
        };
        if (isGroupingElement(item)) grouped = true;
        if (sets.items.len * alts.len > max_sets) return error.GroupingSetsTooMany;
        var next: std.ArrayList(std.ArrayList(usize)) = .empty;
        for (sets.items) |base| for (alts) |alt| {
            var merged: std.ArrayList(usize) = .empty;
            try merged.appendSlice(arena, base.items);
            for (alt.items) |k| if (std.mem.indexOfScalar(usize, merged.items, k) == null) try merged.append(arena, k);
            try next.append(arena, merged);
        };
        sets = next;
    }
    if (!grouped) return null;
    if (c.trailing) return if (c.trailing_qualify) error.GroupingSetsQualify else error.SqlParseError;

    const select_text = std.mem.trim(u8, text[c.select_start..c.from_start], " \t\r\n");
    if (setops.eql(setops.wordAt(select_text, 0), "distinct")) return error.GroupingSetsDistinct;
    const items = try selectItems(arena, select_text, keys.items);

    if (callsGrouping(c.having) or callsGrouping(c.tail)) return error.GroupingSetsGrouping;
    var leaves: std.ArrayList([]const u8) = .empty;
    const qualifier = fromQualifier(text[c.from_start..c.where_end]);
    for (sets.items) |set| try leaves.append(arena, try leaf(arena, text, c, items, keys.items, set.items, qualifier));

    // UNION ALL, left to right.
    var root: *const setops.Node = blk: {
        const n = try arena.create(setops.Node);
        n.* = .{ .leaf = 0 };
        break :blk n;
    };
    for (1..leaves.items.len) |i| {
        const right = try arena.create(setops.Node);
        right.* = .{ .leaf = i };
        const bin = try arena.create(setops.Node);
        bin.* = .{ .bin = .{ .op = .union_all, .left = root, .right = right } };
        root = bin;
    }
    return .{ .leaves = leaves.items, .root = root, .tail = try setops.normalizeTail(arena, try withoutQualifier(arena, c.tail, qualifier)) };
}

/// What the FROM clause lets a column be qualified with: the alias of the one table, else the file's own name.
fn fromQualifier(from: []const u8) ?[]const u8 {
    var i: usize = 0;
    const first = setops.wordAt(from, 0);
    i = first.len;
    while (i < from.len and std.ascii.isWhitespace(from[i])) i += 1;
    if (i >= from.len) return null;
    var path: []const u8 = undefined;
    if (from[i] == '\'' or from[i] == '"') {
        const end = setops.quotedEnd(from, i);
        path = from[i + 1 .. end - 1];
        i = end;
    } else {
        path = setops.wordAt(from, i);
        i += path.len;
    }
    const next = setops.nextWord(from, i);
    var word = next.word;
    if (setops.eql(word, "as")) word = setops.nextWord(from, next.end).word;
    const keywords = [_][]const u8{ "where", "group", "having", "order", "limit", "offset", "join", "inner", "left", "cross", "window", "qualify", "union" };
    if (word.len > 0) for (keywords) |k| if (std.ascii.eqlIgnoreCase(word, k)) {
        word = "";
        break;
    };
    if (word.len > 0) return word;
    const base = std.fs.path.basename(path);
    return if (std.mem.lastIndexOfScalar(u8, base, '.')) |dot| base[0..dot] else base;
}

/// `text` without `qualifier.` before a name: the final query reads scratch files, where the table has no name.
fn withoutQualifier(arena: std.mem.Allocator, text: []const u8, qualifier: ?[]const u8) ![]const u8 {
    const q = qualifier orelse return text;
    if (q.len == 0 or text.len == 0) return text;
    var out: std.ArrayList(u8) = .empty;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            const end = setops.quotedEnd(text, i);
            try out.appendSlice(arena, text[i..end]);
            i = end;
            continue;
        }
        if (setops.startsWord(text, i)) {
            const w = setops.wordAt(text, i);
            if (std.ascii.eqlIgnoreCase(w, q) and i + w.len < text.len and text[i + w.len] == '.') {
                i += w.len + 1;
                continue;
            }
            try out.appendSlice(arena, w);
            i += w.len;
            continue;
        }
        try out.append(arena, ch);
        i += 1;
    }
    return out.items;
}

const Clauses = struct {
    select_start: usize,
    from_start: usize,
    where_end: usize,
    group_start: usize,
    group_end: usize,
    having: []const u8,
    tail: []const u8,
    /// A QUALIFY or WINDOW clause follows the GROUP BY: the grouping sets cannot be rewritten around it.
    trailing: bool = false,
    /// The trailing clause is a QUALIFY (else a WINDOW clause).
    trailing_qualify: bool = false,
};

/// The query's top-level clauses, or null when it is not a plain SELECT with a GROUP BY.
fn clauses(arena: std.mem.Allocator, text: []const u8) Error!?Clauses {
    _ = arena;
    var depth: usize = 0;
    var i: usize = 0;
    var select_end: ?usize = null;
    var from_start: ?usize = null;
    var group_at: ?usize = null;
    var group_start: usize = 0;
    var having_at: ?usize = null;
    var tail_at: ?usize = null;
    var trailing_at: ?usize = null;
    var trailing_qualify = false;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(text, i);
            continue;
        }
        if (ch == '(') {
            depth += 1;
        } else if (ch == ')') {
            depth -|= 1;
        } else if (depth == 0 and setops.startsWord(text, i)) {
            const w = setops.wordAt(text, i);
            if (i == 0 and !setops.eql(w, "select")) return null;
            if (i == 0) select_end = w.len;
            if (setops.eql(w, "union") or setops.eql(w, "intersect") or setops.eql(w, "except")) return null;
            if (setops.eql(w, "from") and from_start == null and tail_at == null) from_start = i;
            if (setops.eql(w, "group") and group_at == null and setops.eql(setops.nextWord(text, i + w.len).word, "by")) {
                group_at = i;
                group_start = setops.nextWord(text, i + w.len).end;
            }
            if (group_at != null and having_at == null and tail_at == null and setops.eql(w, "having")) having_at = i;
            if (group_at != null and tail_at == null and (setops.eql(w, "limit") or setops.eql(w, "offset") or
                (setops.eql(w, "order") and setops.eql(setops.nextWord(text, i + w.len).word, "by")))) tail_at = i;
            if (group_at != null and trailing_at == null and (setops.eql(w, "window") or setops.eql(w, "qualify"))) {
                trailing_at = i;
                // A QUALIFY clause follows an expression's end, not an ORDER BY or HAVING that names a column `qualify`.
                trailing_qualify = setops.eql(w, "qualify") and tail_at == null and followsExpression(text, i);
            }
            i += w.len;
            continue;
        }
        i += 1;
    }
    const group = group_at orelse return null;
    const from = from_start orelse return null;
    const tail_start = tail_at orelse text.len;
    const group_end = @min(having_at orelse tail_start, trailing_at orelse text.len);
    return .{
        .select_start = select_end orelse return null,
        .from_start = from,
        .where_end = group,
        .group_start = group_start,
        .group_end = group_end,
        .having = if (having_at) |h| std.mem.trim(u8, text[h..tail_start], " \t\r\n") else "",
        .tail = std.mem.trim(u8, text[tail_start..], " \t\r\n"),
        .trailing = trailing_at != null,
        .trailing_qualify = trailing_qualify,
    };
}

/// Whether the word at `at` follows the end of an expression (a closing parenthesis or quote, a name or a number), not an
/// operator, a comma or a keyword that wants one (`HAVING qualify > 0`): where a clause of its own can start.
fn followsExpression(text: []const u8, at: usize) bool {
    var end = at;
    while (end > 0 and std.ascii.isWhitespace(text[end - 1])) end -= 1;
    if (end == 0) return false;
    const last = text[end - 1];
    if (last == ')' or last == '\'' or last == '"') return true;
    if (!std.ascii.isAlphanumeric(last) and last != '_') return false;
    var start = end;
    while (start > 0 and (std.ascii.isAlphanumeric(text[start - 1]) or text[start - 1] == '_')) start -= 1;
    const word = text[start..end];
    inline for (.{ "having", "by", "and", "or", "not", "is", "in", "like", "between", "as", "on", "where", "case", "when", "then", "else" }) |kw|
        if (setops.eql(word, kw)) return false;
    return true;
}

/// A comma-separated list as DuckDB reads one in a grouping clause: one trailing comma is ignored, and an empty entry
/// anywhere else (a leading or doubled comma, or nothing at all) is a syntax error.
fn commaList(arena: std.mem.Allocator, text: []const u8) Error![]const []const u8 {
    var parts = try split(arena, text);
    if (parts.len > 1 and std.mem.trim(u8, parts[parts.len - 1], " \t\r\n").len == 0) parts = parts[0 .. parts.len - 1];
    for (parts) |p| if (std.mem.trim(u8, p, " \t\r\n").len == 0) return error.SqlParseError;
    return parts;
}

/// `text` cut at the commas outside parentheses and quotes.
fn split(arena: std.mem.Allocator, text: []const u8) ![]const []const u8 {
    var out: std.ArrayList([]const u8) = .empty;
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(text, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') depth -|= 1;
        if (ch == ',' and depth == 0) {
            try out.append(arena, text[start..i]);
            start = i + 1;
        }
        i += 1;
    }
    try out.append(arena, text[start..]);
    return out.items;
}

/// The text inside the parentheses that open `text[0]`, when they close at its end.
fn parenthesized(text: []const u8) ?[]const u8 {
    if (text.len < 2 or text[0] != '(') return null;
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(text, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') {
            depth -= 1;
            if (depth == 0) return if (i == text.len - 1) text[1..i] else null;
        }
        i += 1;
    }
    return null;
}

/// Whether `text` calls GROUPING or GROUPING_ID anywhere in it.
fn callsGrouping(text: []const u8) bool {
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(text, i);
            continue;
        }
        if (setops.startsWord(text, i)) {
            const w = setops.wordAt(text, i);
            if (setops.eql(w, "grouping") or setops.eql(w, "grouping_id")) {
                const after = std.mem.trimStart(u8, text[i + w.len ..], " \t\r\n");
                if (after.len > 0 and after[0] == '(') return true;
            }
            i += w.len;
            continue;
        }
        i += 1;
    }
    return false;
}

/// The HAVING of one set. A grouping key the set leaves out is NULL in its groups, so each mention of it outside an
/// aggregate's arguments is the aggregate over no rows (`min(k) FILTER (WHERE k IS NULL AND k IS NOT NULL)`), NULL in the
/// key's type; `HAVING a IS NOT NULL` then drops the grand-total row, as DuckDB's does. A mention inside a call
/// (`count(a)`, `sum(a)`) is the column itself.
fn havingForSet(arena: std.mem.Allocator, having: []const u8, keys: []const Key, set: []const usize, qualifier: ?[]const u8) Error![]const u8 {
    var absent = false;
    for (keys, 0..) |k, i| if (k.column != null and std.mem.indexOfScalar(usize, set, i) == null) {
        absent = true;
    };
    if (!absent) return having;
    var out: std.ArrayList(u8) = .empty;
    // Per open parenthesis: whether it opens a function call's arguments (or a FILTER's), where nothing is replaced.
    var in_call: std.ArrayList(bool) = .empty;
    var i: usize = 0;
    var prev_word: []const u8 = "";
    while (i < having.len) {
        const ch = having[i];
        if (ch == '\'' or ch == '"') {
            const end = setops.quotedEnd(having, i);
            try out.appendSlice(arena, having[i..end]);
            i = end;
            prev_word = "";
            continue;
        }
        if (ch == '(') {
            const parent = in_call.items.len > 0 and in_call.items[in_call.items.len - 1];
            const keywords = [_][]const u8{ "in", "and", "or", "not", "between", "like", "case", "when", "then", "else", "end", "is", "exists", "any", "all", "having", "on", "where" };
            var is_call = prev_word.len > 0;
            if (is_call) for (keywords) |k| if (std.ascii.eqlIgnoreCase(prev_word, k)) {
                is_call = false;
            };
            try in_call.append(arena, parent or is_call);
            try out.append(arena, ch);
            i += 1;
            prev_word = "";
            continue;
        }
        if (ch == ')') {
            _ = in_call.pop();
            try out.append(arena, ch);
            i += 1;
            prev_word = "";
            continue;
        }
        if (setops.startsWord(having, i)) {
            const w = setops.wordAt(having, i);
            const end = i + w.len;
            const skip = in_call.items.len > 0 and in_call.items[in_call.items.len - 1];
            const preceded_by_dot = out.items.len > 0 and out.items[out.items.len - 1] == '.';
            const followed_by_call = blk: {
                const after = std.mem.trimStart(u8, having[end..], " \t\r\n");
                break :blk after.len > 0 and after[0] == '(';
            };
            var replaced = false;
            if (!skip and !followed_by_call) for (keys, 0..) |k, ki| {
                const col = k.column orelse continue;
                if (std.mem.indexOfScalar(usize, set, ki) != null) continue;
                if (!std.ascii.eqlIgnoreCase(w, k.name)) continue;
                if (preceded_by_dot) {
                    // The table's own qualifier before the dot is part of the mention.
                    const q = qualifier orelse continue;
                    if (out.items.len < q.len + 1 or !std.ascii.eqlIgnoreCase(out.items[out.items.len - 1 - q.len .. out.items.len - 1], q)) continue;
                    out.shrinkRetainingCapacity(out.items.len - 1 - q.len);
                }
                try out.print(arena, "min({s}) FILTER (WHERE {s} IS NULL AND {s} IS NOT NULL)", .{ col, col, col });
                replaced = true;
                break;
            };
            if (!replaced) try out.appendSlice(arena, w);
            i = end;
            prev_word = w;
            continue;
        }
        if (!std.ascii.isWhitespace(ch)) prev_word = "";
        try out.append(arena, ch);
        i += 1;
    }
    return out.items;
}

fn isGroupingElement(item: []const u8) bool {
    const w = setops.wordAt(item, 0);
    const rest = std.mem.trimStart(u8, item[w.len..], " \t\r\n");
    if (setops.eql(w, "rollup") or setops.eql(w, "cube")) return rest.len > 0 and rest[0] == '(';
    if (setops.eql(w, "grouping")) return setops.eql(setops.wordAt(rest, 0), "sets");
    return false;
}

/// The alternatives a ROLLUP, CUBE or GROUPING SETS item stands for (each a list of key indices), or null for a plain key.
fn element(arena: std.mem.Allocator, item: []const u8, keys: *std.ArrayList(Key)) Error!?[]std.ArrayList(usize) {
    if (!isGroupingElement(item)) return null;
    const w = setops.wordAt(item, 0);
    var rest = std.mem.trimStart(u8, item[w.len..], " \t\r\n");
    const is_sets = setops.eql(w, "grouping");
    if (is_sets) rest = std.mem.trimStart(u8, rest[setops.wordAt(rest, 0).len..], " \t\r\n");
    const inner = parenthesized(rest) orelse return error.SqlParseError;
    const parts = try commaList(arena, inner);

    if (is_sets) {
        var out: std.ArrayList(std.ArrayList(usize)) = .empty;
        for (parts) |raw| {
            const part = std.mem.trim(u8, raw, " \t\r\n");
            if (isGroupingElement(part)) return error.GroupingSetsNesting;
            var set: std.ArrayList(usize) = .empty;
            // `()` alone is the empty set; a set of keys may end in a comma.
            const members = parenthesized(part) orelse part;
            if (std.mem.trim(u8, members, " \t\r\n").len > 0 or parenthesized(part) == null) for (try commaList(arena, members)) |m| {
                const k = try keyIndex(arena, keys, std.mem.trim(u8, m, " \t\r\n"));
                if (std.mem.indexOfScalar(usize, set.items, k) == null) try set.append(arena, k);
            };
            try out.append(arena, set);
        }
        return out.items;
    }

    // ROLLUP and CUBE: each part is a key, or a parenthesized group of keys that goes in or out together.
    const units = try arena.alloc(std.ArrayList(usize), parts.len);
    for (parts, units) |raw, *unit| {
        const part = std.mem.trim(u8, raw, " \t\r\n");
        if (isGroupingElement(part)) return error.GroupingSetsNesting;
        unit.* = .empty;
        const members = parenthesized(part) orelse part;
        for (try commaList(arena, members)) |m| try unit.append(arena, try keyIndex(arena, keys, std.mem.trim(u8, m, " \t\r\n")));
    }
    var out: std.ArrayList(std.ArrayList(usize)) = .empty;
    if (setops.eql(w, "rollup")) {
        var n = units.len;
        while (true) : (n -= 1) {
            var set: std.ArrayList(usize) = .empty;
            for (units[0..n]) |u| for (u.items) |k| if (std.mem.indexOfScalar(usize, set.items, k) == null) try set.append(arena, k);
            try out.append(arena, set);
            if (n == 0) break;
        }
    } else {
        if (units.len > 6) return error.GroupingSetsTooMany;
        const total: usize = @as(usize, 1) << @intCast(units.len);
        var mask = total;
        while (mask > 0) {
            mask -= 1;
            var set: std.ArrayList(usize) = .empty;
            for (units, 0..) |u, bit| if (mask & (@as(usize, 1) << @intCast(bit)) != 0) for (u.items) |k| {
                if (std.mem.indexOfScalar(usize, set.items, k) == null) try set.append(arena, k);
            };
            try out.append(arena, set);
        }
    }
    return out.items;
}

/// The index of the key `text` writes, adding it if it is new.
fn keyIndex(arena: std.mem.Allocator, keys: *std.ArrayList(Key), text: []const u8) Error!usize {
    if (text.len == 0) return error.SqlParseError;
    const norm = try normalize(arena, text);
    for (keys.items, 0..) |k, i| if (std.mem.eql(u8, k.norm, norm)) return i;
    const column = columnName(text);
    try keys.append(arena, .{ .text = text, .norm = norm, .column = if (column) |_| text else null, .name = column orelse text });
    return keys.items.len - 1;
}

/// Lower case without spaces or quotes.
fn normalize(arena: std.mem.Allocator, text: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (text) |ch| {
        if (std.ascii.isWhitespace(ch) or ch == '"') continue;
        try out.append(arena, std.ascii.toLower(ch));
    }
    return out.items;
}

/// The last name of `text` when it is a column (`a`, `t.a`, `"a b"`), null for anything else (a number, a call, an operator).
fn columnName(text: []const u8) ?[]const u8 {
    var start: usize = 0;
    var i: usize = 0;
    var last: []const u8 = "";
    while (i < text.len) {
        if (text[i] == '"') {
            const end = setops.quotedEnd(text, i);
            last = text[i + 1 .. end - 1];
            i = end;
        } else if (setops.isWord(text[i]) and !std.ascii.isDigit(text[i])) {
            start = i;
            while (i < text.len and setops.isWord(text[i])) i += 1;
            last = text[start..i];
        } else return null;
        if (i < text.len) {
            if (text[i] != '.') return null;
            i += 1;
            if (i >= text.len) return null;
        }
    }
    return if (last.len > 0) last else null;
}

const Item = union(enum) {
    /// Not about the keys: kept as written.
    plain: []const u8,
    /// A select item that is key `key`; `alias` is the name it is output under.
    key: struct { text: []const u8, key: usize, alias: []const u8, aliased: bool },
    /// `GROUPING(...)` / `GROUPING_ID(...)` of these keys.
    grouping: struct { keys: []const usize, alias: []const u8 },
};

fn selectItems(arena: std.mem.Allocator, text: []const u8, keys: []Key) Error![]const Item {
    var out: std.ArrayList(Item) = .empty;
    for (try commaList(arena, text)) |raw| {
        const item = std.mem.trim(u8, raw, " \t\r\n");
        if (item.len == 0) return error.SqlParseError;
        var expr = item;
        var alias: ?[]const u8 = null;
        if (lastAs(item)) |at| {
            expr = std.mem.trim(u8, item[0..at], " \t\r\n");
            alias = std.mem.trim(u8, item[at + 2 ..], " \t\r\n");
        }
        const w = setops.wordAt(expr, 0);
        const after = std.mem.trimStart(u8, expr[w.len..], " \t\r\n");
        if ((setops.eql(w, "grouping") or setops.eql(w, "grouping_id")) and after.len > 0 and after[0] == '(' and parenthesized(after) != null) {
            const inner = parenthesized(after).?;
            var members: std.ArrayList(usize) = .empty;
            for (try split(arena, inner)) |m| {
                const norm = try normalize(arena, std.mem.trim(u8, m, " \t\r\n"));
                const k = for (keys, 0..) |key, i| {
                    if (std.mem.eql(u8, key.norm, norm)) break i;
                } else return error.GroupingSetsGrouping;
                try members.append(arena, k);
            }
            try out.append(arena, .{ .grouping = .{ .keys = members.items, .alias = alias orelse try quote(arena, expr) } });
            continue;
        }
        const norm = try normalize(arena, expr);
        var k = for (keys, 0..) |key, i| {
            if (std.mem.eql(u8, key.norm, norm)) break i;
        } else null;
        // A key may be written as the alias of the item it names (`SELECT g AS k ... ROLLUP (k)`): it is that item's expression.
        if (k == null) if (alias) |al| {
            const al_norm = try normalize(arena, al);
            for (keys, 0..) |*key, i| if (std.mem.eql(u8, key.norm, al_norm)) {
                key.text = expr;
                key.column = if (columnName(expr) != null) expr else null;
                key.name = al;
                k = i;
                break;
            };
        };
        if (k) |idx| {
            try out.append(arena, .{ .key = .{ .text = expr, .key = idx, .alias = alias orelse keys[idx].name, .aliased = alias != null } });
        } else {
            // GROUPING is computed per set, so only as a select item of its own, not inside an expression.
            if (callsGrouping(item)) return error.GroupingSetsGrouping;
            try out.append(arena, .{ .plain = item });
        }
    }
    return out.items;
}

/// Where the last top-level `AS` of a select item starts, or null.
fn lastAs(item: []const u8) ?usize {
    var depth: usize = 0;
    var found: ?usize = null;
    var i: usize = 0;
    while (i < item.len) {
        const ch = item[i];
        if (ch == '\'' or ch == '"') {
            i = setops.quotedEnd(item, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') depth -|= 1;
        if (depth == 0 and setops.startsWord(item, i)) {
            const w = setops.wordAt(item, i);
            if (setops.eql(w, "as") and i > 0) found = i;
            i += w.len;
            continue;
        }
        i += 1;
    }
    return found;
}

fn quote(arena: std.mem.Allocator, name: []const u8) ![]const u8 {
    return std.fmt.allocPrint(arena, "\"{s}\"", .{name});
}

/// The SELECT for `set`.
fn leaf(arena: std.mem.Allocator, text: []const u8, c: Clauses, items: []const Item, keys: []const Key, set: []const usize, qualifier: ?[]const u8) Error![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    try out.appendSlice(arena, "SELECT ");
    for (items, 0..) |item, n| {
        if (n > 0) try out.appendSlice(arena, ", ");
        switch (item) {
            .plain => |t| try out.appendSlice(arena, t),
            .key => |k| if (std.mem.indexOfScalar(usize, set, k.key) != null) {
                try out.appendSlice(arena, k.text);
                if (k.aliased) try out.print(arena, " AS {s}", .{k.alias});
            } else {
                const col = keys[k.key].column orelse return error.GroupingSetsKey;
                try out.print(arena, "min({s}) FILTER (WHERE {s} IS NULL AND {s} IS NOT NULL) AS {s}", .{ col, col, col, k.alias });
            },
            .grouping => |g| {
                var value: usize = 0;
                for (g.keys) |k| value = value * 2 + @intFromBool(std.mem.indexOfScalar(usize, set, k) == null);
                const col = keys[g.keys[0]].column orelse return error.GroupingSetsKey;
                try out.print(arena, "count(*) FILTER (WHERE {s} IS NULL AND {s} IS NOT NULL) + {d} AS {s}", .{ col, col, value, g.alias });
            },
        }
    }
    try out.append(arena, ' ');
    try out.appendSlice(arena, std.mem.trim(u8, text[c.from_start..c.where_end], " \t\r\n"));
    if (set.len > 0) {
        try out.appendSlice(arena, " GROUP BY ");
        for (set, 0..) |k, n| {
            if (n > 0) try out.appendSlice(arena, ", ");
            try out.appendSlice(arena, keys[k].text);
        }
    }
    if (c.having.len > 0) {
        try out.append(arena, ' ');
        try out.appendSlice(arena, try havingForSet(arena, c.having, keys, set, qualifier));
    }
    return out.items;
}

const testing = std.testing;

fn leavesOf(arena: std.mem.Allocator, sql: []const u8) ![]const []const u8 {
    return (try expand(arena, sql)).?.leaves;
}

test "ROLLUP, CUBE and GROUPING SETS become one GROUP BY query per set" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expect((try expand(a, "SELECT a, count(*) FROM t GROUP BY a")) == null);
    try testing.expect((try expand(a, "SELECT a FROM t")) == null);
    const r = (try expand(a, "SELECT a, b, sum(x) AS s FROM t WHERE x > 1 GROUP BY ROLLUP (a, b) HAVING sum(x) > 2 ORDER BY a NULLS FIRST LIMIT 5;")).?;
    try testing.expectEqual(@as(usize, 3), r.leaves.len);
    try testing.expectEqualStrings("SELECT a, b, sum(x) AS s FROM t WHERE x > 1 GROUP BY a, b HAVING sum(x) > 2", r.leaves[0]);
    try testing.expectEqualStrings("SELECT a, min(b) FILTER (WHERE b IS NULL AND b IS NOT NULL) AS b, sum(x) AS s FROM t WHERE x > 1 GROUP BY a HAVING sum(x) > 2", r.leaves[1]);
    try testing.expectEqualStrings("SELECT min(a) FILTER (WHERE a IS NULL AND a IS NOT NULL) AS a, min(b) FILTER (WHERE b IS NULL AND b IS NOT NULL) AS b, sum(x) AS s FROM t WHERE x > 1 HAVING sum(x) > 2", r.leaves[2]);
    try testing.expectEqualStrings("ORDER BY a NULLS FIRST LIMIT 5", r.tail);
    try testing.expectEqual(@as(usize, 4), (try leavesOf(a, "SELECT a, b, count(*) FROM t GROUP BY CUBE (a, b)")).len);
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a, b, count(*) FROM t GROUP BY GROUPING SETS ((a), (b))")).len);
    // The elements of a GROUP BY multiply.
    try testing.expectEqual(@as(usize, 3), (try leavesOf(a, "SELECT a, b, c, count(*) FROM t GROUP BY a, ROLLUP (b, c)")).len);
    try testing.expectEqual(@as(usize, 6), (try leavesOf(a, "SELECT a, b, c, count(*) FROM t GROUP BY ROLLUP (a, b), ROLLUP (c)")).len);
    // GROUPING gives the set's bits, the first argument the highest.
    const g = try leavesOf(a, "SELECT a, b, GROUPING(a, b) AS gab, GROUPING_ID(b, a) AS gid FROM t GROUP BY CUBE (a, b)");
    try testing.expectEqualStrings("SELECT a, b, count(*) FILTER (WHERE a IS NULL AND a IS NOT NULL) + 0 AS gab, count(*) FILTER (WHERE b IS NULL AND b IS NOT NULL) + 0 AS gid FROM t GROUP BY a, b", g[0]);
    try testing.expect(std.mem.indexOf(u8, g[3], "+ 3 AS gab") != null and std.mem.indexOf(u8, g[3], "+ 3 AS gid") != null);
    try testing.expect(std.mem.indexOf(u8, g[1], "+ 2 AS gab") != null and std.mem.indexOf(u8, g[1], "+ 1 AS gid") != null);
}

test "what cannot be rewritten is refused by name" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expectError(error.GroupingSetsDistinct, expand(a, "SELECT DISTINCT a, count(*) FROM t GROUP BY ROLLUP (a)"));
    for ([_][]const u8{ "CUBE (a, b)", "GROUPING SETS ((a), ())" }) |grouping| {
        const sql = try std.fmt.allocPrint(a, "SELECT a, count(*) AS n, rank() OVER (ORDER BY a) AS r FROM t GROUP BY {s} QUALIFY r = 1", .{grouping});
        try testing.expectError(error.GroupingSetsQualify, expand(a, sql));
    }
    try testing.expectError(error.GroupingSetsNesting, expand(a, "SELECT a, count(*) FROM t GROUP BY GROUPING SETS (ROLLUP (a), (b))"));
    try testing.expectError(error.GroupingSetsGrouping, expand(a, "SELECT a, GROUPING(z) FROM t GROUP BY ROLLUP (a)"));
    try testing.expectError(error.GroupingSetsGrouping, expand(a, "SELECT a, GROUPING(a) + 1 AS x FROM t GROUP BY ROLLUP (a)"));
    try testing.expectError(error.GroupingSetsGrouping, expand(a, "SELECT a, count(*) FROM t GROUP BY ROLLUP (a) ORDER BY GROUPING(a)"));
    try testing.expectError(error.GroupingSetsKey, expand(a, "SELECT a % 2, count(*) FROM t GROUP BY ROLLUP (a % 2)"));
    try testing.expectError(error.GroupingSetsTooMany, expand(a, "SELECT a FROM t GROUP BY CUBE (a, b, c, d, e, f, g)"));
    // A key may be written as an alias of a select item.
    const al = try leavesOf(a, "SELECT g AS k, count(*) AS n FROM t GROUP BY ROLLUP (k)");
    try testing.expectEqualStrings("SELECT g AS k, count(*) AS n FROM t GROUP BY g", al[0]);
    try testing.expectEqualStrings("SELECT min(g) FILTER (WHERE g IS NULL AND g IS NOT NULL) AS k, count(*) AS n FROM t", al[1]);
    // A key every set has may be an expression.
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a % 2, b, count(*) FROM t GROUP BY a % 2, ROLLUP (b)")).len);
}

test "lists are read as DuckDB reads them: one trailing comma is ignored, any other empty entry is an error" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expectEqual(@as(usize, 1), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY GROUPING SETS ((a),)")).len);
    try testing.expectEqual(@as(usize, 1), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY GROUPING SETS ((a,))")).len);
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY GROUPING SETS (a, b,)")).len);
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY ROLLUP (a,)")).len);
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY GROUPING SETS ((a), ())")).len);
    try testing.expectEqual(@as(usize, 2), (try leavesOf(a, "SELECT a, count(*) FROM t GROUP BY ROLLUP (a),")).len);
    for ([_][]const u8{
        "GROUPING SETS ()",
        "GROUPING SETS ((a),,(b))",
        "GROUPING SETS (,(a))",
        "GROUPING SETS ((,a))",
        "ROLLUP ()",
        "CUBE ()",
        "ROLLUP (a,,b)",
        "ROLLUP (,a)",
    }) |group| {
        const sql = try std.fmt.allocPrint(a, "SELECT a, count(*) FROM t GROUP BY {s}", .{group});
        try testing.expectError(error.SqlParseError, expand(a, sql));
    }
}

test "the tail of a grouping query is the SELECT grammar's, and a key a set leaves out is NULL in its HAVING" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const t = (try expand(a, "SELECT a, count(*) AS n FROM t GROUP BY ROLLUP (a) ORDER BY a OFFSET 2")).?;
    try testing.expectEqualStrings("ORDER BY a LIMIT 9223372036854775807 OFFSET 2", t.tail);
    const h = (try expand(a, "SELECT count(*) AS n FROM t GROUP BY ROLLUP (a) HAVING a IS NOT NULL AND count(a) > 1 AND sum(a) > 3 OR (a + 1) > 2")).?;
    try testing.expectEqualStrings("SELECT count(*) AS n FROM t GROUP BY a HAVING a IS NOT NULL AND count(a) > 1 AND sum(a) > 3 OR (a + 1) > 2", h.leaves[0]);
    const nul = "min(a) FILTER (WHERE a IS NULL AND a IS NOT NULL)";
    try testing.expectEqualStrings("SELECT count(*) AS n FROM t HAVING " ++ nul ++ " IS NOT NULL AND count(a) > 1 AND sum(a) > 3 OR (" ++ nul ++ " + 1) > 2", h.leaves[1]);
}

test "the query's own table name is dropped from the tail that runs over scratch files" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const r = (try expand(a, "SELECT t.g, count(*) AS c FROM 'dir/in.parquet' t WHERE t.id > 1 GROUP BY ROLLUP (t.g) ORDER BY t.g NULLS FIRST, c")).?;
    try testing.expectEqualStrings("ORDER BY g NULLS FIRST, c", r.tail);
    try testing.expectEqualStrings("SELECT t.g, count(*) AS c FROM 'dir/in.parquet' t WHERE t.id > 1 GROUP BY t.g", r.leaves[0]);
    const f = (try expand(a, "SELECT in.g, count(*) AS c FROM 'dir/in.parquet' GROUP BY ROLLUP (in.g) ORDER BY in.g")).?;
    try testing.expectEqualStrings("ORDER BY g", f.tail);
    const q = (try expand(a, "SELECT g, count(*) AS c FROM 'x.parquet' AS tt GROUP BY ROLLUP (g) ORDER BY 'tt.g', tt.c")).?;
    try testing.expectEqualStrings("ORDER BY 'tt.g', c", q.tail);
}

test "a QUALIFY after GROUP BY is no grouping-set query, and with ROLLUP it is refused" {
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try std.testing.expect((try expand(a, "SELECT g, sum(v) FROM t GROUP BY g QUALIFY rank() OVER (ORDER BY sum(v)) <= 3 ORDER BY g")) == null);
    try std.testing.expectError(error.GroupingSetsQualify, expand(a, "SELECT g, sum(v) FROM t GROUP BY ROLLUP (g) QUALIFY rank() OVER (ORDER BY sum(v)) <= 3"));
    // A column named `qualify` in ORDER BY or HAVING is no QUALIFY clause: it stays a bare parse error, as in DuckDB.
    try std.testing.expectError(error.SqlParseError, expand(a, "SELECT g, sum(v) FROM t GROUP BY ROLLUP (g) ORDER BY qualify"));
    try std.testing.expectError(error.SqlParseError, expand(a, "SELECT g, sum(v) FROM t GROUP BY ROLLUP (g) HAVING qualify > 0"));
    try std.testing.expectError(error.GroupingSetsQualify, expand(a, "SELECT g, sum(v) FROM t GROUP BY ROLLUP (g) HAVING sum(v) > 0 QUALIFY rank() OVER (ORDER BY g) <= 3"));
    // A WINDOW clause there is still a bare parse error.
    try std.testing.expectError(error.SqlParseError, expand(a, "SELECT g, sum(v) FROM t GROUP BY ROLLUP (g) WINDOW w AS (ORDER BY g)"));
}
