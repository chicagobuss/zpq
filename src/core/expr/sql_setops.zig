//! Splits `SELECT ... UNION [ALL] | INTERSECT | EXCEPT SELECT ...` into its SELECTs and the tree DuckDB combines them
//! with, as text: each branch then goes through the ordinary one-SELECT frontend, and the CLI combines their results
//! (`cli/setops.zig`). The text carries no comments (`stripComments` ran first), so a keyword is a word outside quotes
//! and parentheses.
//!
//! DuckDB's grammar: INTERSECT binds tighter than UNION and EXCEPT, which associate to the left; a trailing
//! ORDER BY / LIMIT / OFFSET belongs to the whole result, and an earlier branch must be parenthesized to carry its own.
const std = @import("std");

pub const Op = enum { union_all, union_distinct, intersect, except };

pub const Node = union(enum) {
    /// An index into `Compound.leaves`.
    leaf: usize,
    bin: struct { op: Op, left: *const Node, right: *const Node },
};

pub const Compound = struct {
    /// One SELECT each, in the order they are written, outer parentheses removed.
    leaves: []const []const u8,
    root: *const Node,
    /// `ORDER BY ...`, `LIMIT ...` and `OFFSET ...` after the last branch, as written; empty when there are none.
    tail: []const u8,
};

pub const Error = error{
    OutOfMemory,
    /// `INTERSECT ALL` and `EXCEPT ALL` count duplicates, which zpq does not.
    SetOperationAll,
    /// `UNION [ALL] BY NAME` matches columns by name.
    SetOperationByName,
    /// An ORDER BY, LIMIT or OFFSET on a branch that is not the last one and not in parentheses.
    SetOperationOrder,
    /// A parenthesized combination that carries an ORDER BY, LIMIT or OFFSET of its own.
    SetOperationNesting,
    /// An operator with no SELECT on one side.
    SqlParseError,
};

/// The combination `sql` writes, or null when it has no set operator outside parentheses.
pub fn split(arena: std.mem.Allocator, sql: []const u8) Error!?Compound {
    var leaves: std.ArrayList([]const u8) = .empty;
    const parsed = try parseLevel(arena, sql, &leaves, true) orelse return null;
    return .{ .leaves = leaves.items, .root = parsed.root, .tail = parsed.tail };
}

const Level = struct { root: *const Node, tail: []const u8 };

const Piece = struct { text: []const u8, op_after: ?Op };

fn parseLevel(arena: std.mem.Allocator, sql: []const u8, leaves: *std.ArrayList([]const u8), outermost: bool) Error!?Level {
    var pieces: std.ArrayList(Piece) = .empty;
    var depth: usize = 0;
    var start: usize = 0;
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            i = quotedEnd(sql, i);
            continue;
        }
        if (ch == '(') {
            depth += 1;
        } else if (ch == ')') {
            depth -|= 1;
        } else if (depth == 0 and startsWord(sql, i)) {
            if (try operatorAt(sql, i)) |found| {
                try pieces.append(arena, .{ .text = sql[start..i], .op_after = found.op });
                i = found.end;
                start = i;
                continue;
            }
        }
        i += 1;
    }
    if (pieces.items.len == 0) return null;
    try pieces.append(arena, .{ .text = sql[start..], .op_after = null });

    // The last branch's trailing ORDER BY / LIMIT / OFFSET is the whole result's.
    var tail: []const u8 = "";
    const last = &pieces.items[pieces.items.len - 1];
    if (tailStart(last.text)) |at| {
        tail = std.mem.trim(u8, last.text[at..], " \t\r\n;");
        last.text = last.text[0..at];
    } else last.text = std.mem.trimEnd(u8, last.text, " \t\r\n;");
    if (tail.len > 0 and !outermost) return error.SetOperationNesting;
    tail = try normalizeTail(arena, tail);
    for (pieces.items[0 .. pieces.items.len - 1]) |p| if (tailStart(p.text) != null) return error.SetOperationOrder;

    // One tree node per branch: a SELECT is a leaf, a parenthesized combination is its own tree.
    var nodes: std.ArrayList(*const Node) = .empty;
    for (pieces.items) |p| {
        const text = std.mem.trim(u8, p.text, " \t\r\n");
        if (text.len == 0) return error.SqlParseError;
        const node = try arena.create(Node);
        if (wrapped(text)) |inner| {
            if (try parseLevel(arena, inner, leaves, false)) |sub| {
                node.* = sub.root.*;
            } else {
                try leaves.append(arena, std.mem.trim(u8, inner, " \t\r\n"));
                node.* = .{ .leaf = leaves.items.len - 1 };
            }
        } else {
            try leaves.append(arena, text);
            node.* = .{ .leaf = leaves.items.len - 1 };
        }
        try nodes.append(arena, node);
    }

    // INTERSECT first, then UNION and EXCEPT left to right.
    var ops: std.ArrayList(Op) = .empty;
    for (pieces.items[0 .. pieces.items.len - 1]) |p| try ops.append(arena, p.op_after.?);
    var k: usize = 0;
    while (k < ops.items.len) {
        if (ops.items[k] == .intersect) {
            nodes.items[k] = try join(arena, .intersect, nodes.items[k], nodes.items[k + 1]);
            _ = nodes.orderedRemove(k + 1);
            _ = ops.orderedRemove(k);
        } else k += 1;
    }
    var root = nodes.items[0];
    for (ops.items, nodes.items[1..]) |op, right| root = try join(arena, op, root, right);
    return .{ .root = root, .tail = tail };
}

fn join(arena: std.mem.Allocator, op: Op, left: *const Node, right: *const Node) !*const Node {
    const node = try arena.create(Node);
    node.* = .{ .bin = .{ .op = op, .left = left, .right = right } };
    return node;
}

const Found = struct { op: Op, end: usize };

/// The set operator, with its ALL / DISTINCT, that starts at `sql[i]`; null if the word there is not one. A spelling
/// zpq does not carry is an error, not a miss: it is an operator that must not be skipped over.
fn operatorAt(sql: []const u8, i: usize) Error!?Found {
    const w = wordAt(sql, i);
    var op: Op = undefined;
    if (eql(w, "union")) {
        op = .union_distinct;
    } else if (eql(w, "intersect")) {
        op = .intersect;
    } else if (eql(w, "except")) {
        op = .except;
    } else return null;
    var end = i + w.len;
    var next = nextWord(sql, end);
    if (eql(next.word, "all")) {
        if (op != .union_distinct) return error.SetOperationAll;
        op = .union_all;
        end = next.end;
        next = nextWord(sql, end);
    } else if (eql(next.word, "distinct")) {
        end = next.end;
        next = nextWord(sql, end);
    }
    if (eql(next.word, "by") and eql(nextWord(sql, next.end).word, "name")) return error.SetOperationByName;
    return .{ .op = op, .end = end };
}

pub fn eql(a: []const u8, comptime lower: []const u8) bool {
    return std.ascii.eqlIgnoreCase(a, lower);
}

pub fn isWord(ch: u8) bool {
    return std.ascii.isAlphanumeric(ch) or ch == '_' or ch == '$';
}

pub fn startsWord(sql: []const u8, i: usize) bool {
    return isWord(sql[i]) and (i == 0 or !isWord(sql[i - 1]));
}

pub fn wordAt(sql: []const u8, i: usize) []const u8 {
    var e = i;
    while (e < sql.len and isWord(sql[e])) e += 1;
    return sql[i..e];
}

pub fn nextWord(sql: []const u8, from: usize) struct { word: []const u8, end: usize } {
    var s = from;
    while (s < sql.len and std.ascii.isWhitespace(sql[s])) s += 1;
    const w = wordAt(sql, s);
    return .{ .word = w, .end = s + w.len };
}

/// Past the quoted text that starts at `sql[at]` (a doubled quote is one inside it).
pub fn quotedEnd(sql: []const u8, at: usize) usize {
    const q = sql[at];
    var i = at + 1;
    while (i < sql.len) : (i += 1) {
        if (sql[i] == q) {
            if (i + 1 < sql.len and sql[i + 1] == q) {
                i += 1;
                continue;
            }
            return i + 1;
        }
    }
    return sql.len;
}

/// `tail` as the SELECT grammar takes it: ORDER BY, then LIMIT, then OFFSET. DuckDB takes LIMIT and OFFSET in either order,
/// and an OFFSET alone (the rows after it all); the grammar needs a LIMIT, so the largest one stands in.
pub fn normalizeTail(arena: std.mem.Allocator, tail: []const u8) ![]const u8 {
    if (tail.len == 0) return tail;
    var order: ?[]const u8 = null;
    var limit: ?[]const u8 = null;
    var offset: ?[]const u8 = null;
    var kind: enum { none, order, limit, offset } = .none;
    var start: usize = 0;
    var depth: usize = 0;
    var i: usize = 0;
    while (i <= tail.len) {
        var boundary: ?@TypeOf(kind) = null;
        var word_len: usize = 0;
        if (i < tail.len) {
            const ch = tail[i];
            if (ch == '\'' or ch == '"') {
                i = quotedEnd(tail, i);
                continue;
            }
            if (ch == '(') depth += 1;
            if (ch == ')') depth -|= 1;
            if (depth == 0 and startsWord(tail, i)) {
                const w = wordAt(tail, i);
                word_len = w.len;
                if (eql(w, "limit")) boundary = .limit;
                if (eql(w, "offset")) boundary = .offset;
                if (eql(w, "order") and eql(nextWord(tail, i + w.len).word, "by")) {
                    boundary = .order;
                    word_len = nextWord(tail, i + w.len).end - i;
                }
                if (boundary == null) {
                    i += w.len;
                    continue;
                }
            } else {
                i += 1;
                continue;
            }
        }
        // `i` is the start of the next clause, or the end: the clause begun at `start` ends here.
        const body = std.mem.trim(u8, tail[start..i], " \t\r\n");
        switch (kind) {
            .none => {},
            .order => if (order != null) return tail else {
                order = body;
            },
            .limit => if (limit != null) return tail else {
                limit = body;
            },
            .offset => if (offset != null) return tail else {
                offset = body;
            },
        }
        if (boundary) |b| {
            kind = b;
            start = i + word_len;
            i += word_len;
        } else break;
    }
    if (offset != null and limit == null) limit = "9223372036854775807";
    var out: std.ArrayList(u8) = .empty;
    if (order) |o| try out.print(arena, "ORDER BY {s}", .{o});
    if (limit) |l| try out.print(arena, "{s}LIMIT {s}", .{ if (out.items.len > 0) " " else "", l });
    if (offset) |o| try out.print(arena, "{s}OFFSET {s}", .{ if (out.items.len > 0) " " else "", o });
    return out.items;
}

/// Where a top-level `ORDER BY`, `LIMIT` or `OFFSET` starts in `text`, or null.
fn tailStart(text: []const u8) ?usize {
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = quotedEnd(text, i);
            continue;
        }
        if (ch == '(') {
            depth += 1;
        } else if (ch == ')') {
            depth -|= 1;
        } else if (depth == 0 and startsWord(text, i)) {
            const w = wordAt(text, i);
            if (eql(w, "limit") or eql(w, "offset")) return i;
            if (eql(w, "order") and eql(nextWord(text, i + w.len).word, "by")) return i;
            i += w.len;
            continue;
        }
        i += 1;
    }
    return null;
}

/// The text inside `text` when one pair of parentheses encloses all of it.
fn wrapped(text: []const u8) ?[]const u8 {
    if (text.len < 2 or text[0] != '(' or text[text.len - 1] != ')') return null;
    var depth: usize = 0;
    var i: usize = 0;
    while (i < text.len) {
        const ch = text[i];
        if (ch == '\'' or ch == '"') {
            i = quotedEnd(text, i);
            continue;
        }
        if (ch == '(') depth += 1;
        if (ch == ')') {
            depth -= 1;
            if (depth == 0 and i != text.len - 1) return null;
        }
        i += 1;
    }
    return text[1 .. text.len - 1];
}

const testing = std.testing;

fn expectShape(node: *const Node, out: *std.ArrayList(u8), gpa: std.mem.Allocator) !void {
    switch (node.*) {
        .leaf => |n| try out.print(gpa, "{d}", .{n}),
        .bin => |b| {
            try out.append(gpa, '(');
            try expectShape(b.left, out, gpa);
            try out.print(gpa, " {s} ", .{switch (b.op) {
                .union_all => "UA",
                .union_distinct => "U",
                .intersect => "I",
                .except => "E",
            }});
            try expectShape(b.right, out, gpa);
            try out.append(gpa, ')');
        },
    }
}

fn shape(arena: std.mem.Allocator, sql: []const u8) ![]const u8 {
    const c = (try split(arena, sql)) orelse return "none";
    var out: std.ArrayList(u8) = .empty;
    try expectShape(c.root, &out, arena);
    return out.items;
}

test "set operators split the SELECTs and group as DuckDB does" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expectEqualStrings("none", try shape(a, "SELECT x FROM t WHERE y IN (SELECT 1 UNION SELECT 2)"));
    try testing.expectEqualStrings("(0 UA 1)", try shape(a, "SELECT 1 FROM t UNION ALL SELECT 2 FROM t"));
    try testing.expectEqualStrings("(0 U 1)", try shape(a, "SELECT a FROM t union SELECT b FROM u"));
    try testing.expectEqualStrings("(0 U 1)", try shape(a, "SELECT a FROM t UNION DISTINCT SELECT b FROM u"));
    // INTERSECT binds tighter; UNION and EXCEPT go left to right.
    try testing.expectEqualStrings("(0 U (1 I 2))", try shape(a, "SELECT 0 FROM t UNION SELECT 1 FROM t INTERSECT SELECT 2 FROM t"));
    try testing.expectEqualStrings("((0 E 1) U 2)", try shape(a, "SELECT 0 FROM t EXCEPT SELECT 1 FROM t UNION SELECT 2 FROM t"));
    try testing.expectEqualStrings("(((0 UA 1) UA 2) UA 3)", try shape(a, "SELECT 0 FROM t UNION ALL SELECT 1 FROM t UNION ALL SELECT 2 FROM t UNION ALL SELECT 3 FROM t"));
    // A parenthesized combination is a node of its own.
    try testing.expectEqualStrings("((0 U 1) I 2)", try shape(a, "(SELECT 0 FROM t UNION SELECT 1 FROM t) INTERSECT SELECT 2 FROM t"));
    // Keywords inside quotes or identifiers are not operators.
    try testing.expectEqualStrings("none", try shape(a, "SELECT 'a UNION b' AS \"x union y\", union_all FROM t"));
}

test "the trailing ORDER BY, LIMIT and OFFSET are the whole result's" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const c = (try split(a, "SELECT a FROM t UNION ALL SELECT b FROM u ORDER BY 1 DESC LIMIT 5 OFFSET 2;")).?;
    try testing.expectEqual(@as(usize, 2), c.leaves.len);
    try testing.expectEqualStrings("SELECT a FROM t", c.leaves[0]);
    try testing.expectEqualStrings("SELECT b FROM u", c.leaves[1]);
    try testing.expectEqualStrings("ORDER BY 1 DESC LIMIT 5 OFFSET 2", c.tail);
    // A parenthesized branch keeps its own.
    const d = (try split(a, "(SELECT a FROM t ORDER BY a LIMIT 1) UNION ALL (SELECT b FROM u LIMIT 2)")).?;
    try testing.expectEqualStrings("SELECT a FROM t ORDER BY a LIMIT 1", d.leaves[0]);
    try testing.expectEqualStrings("SELECT b FROM u LIMIT 2", d.leaves[1]);
    try testing.expectEqualStrings("", d.tail);
    try testing.expectError(error.SetOperationOrder, split(a, "SELECT a FROM t ORDER BY a UNION ALL SELECT b FROM u"));
    try testing.expectError(error.SetOperationNesting, split(a, "(SELECT 1 FROM t UNION SELECT 2 FROM t LIMIT 1) UNION ALL SELECT 3 FROM t"));
}

test "operators zpq does not carry are refused by name" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expectError(error.SetOperationAll, split(a, "SELECT a FROM t INTERSECT ALL SELECT b FROM u"));
    try testing.expectError(error.SetOperationAll, split(a, "SELECT a FROM t EXCEPT ALL SELECT b FROM u"));
    try testing.expectError(error.SetOperationByName, split(a, "SELECT a FROM t UNION BY NAME SELECT b FROM u"));
    try testing.expectError(error.SetOperationByName, split(a, "SELECT a FROM t UNION ALL BY NAME SELECT b FROM u"));
    try testing.expectError(error.SqlParseError, split(a, "SELECT a FROM t UNION"));
}

test "the tail takes LIMIT and OFFSET in either order, and an OFFSET alone" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const cases = [_][2][]const u8{
        .{ "ORDER BY 1 LIMIT 5 OFFSET 2", "ORDER BY 1 LIMIT 5 OFFSET 2" },
        .{ "LIMIT 5", "LIMIT 5" },
        .{ "OFFSET 3", "LIMIT 9223372036854775807 OFFSET 3" },
        .{ "ORDER BY x DESC OFFSET 3", "ORDER BY x DESC LIMIT 9223372036854775807 OFFSET 3" },
        .{ "OFFSET 3 LIMIT 9", "LIMIT 9 OFFSET 3" },
        .{ "ORDER BY f(a, 'limit 1') OFFSET 1 LIMIT 2", "ORDER BY f(a, 'limit 1') LIMIT 2 OFFSET 1" },
    };
    for (cases) |c| {
        const got = (try split(a, std.fmt.allocPrint(a, "SELECT a FROM t UNION ALL SELECT b FROM u {s}", .{c[0]}) catch unreachable)).?;
        try testing.expectEqualStrings(c[1], got.tail);
    }
}
