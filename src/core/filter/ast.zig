//! Filter AST + Operator enum.
//!
//! Leaf nodes carry a typed value plus the column index they reference;
//! composites carry their operands, two or more, flat (see `Composite`).
//!
//! Surface: =, !=, <, <=, >, >= with AND/OR.

const std = @import("std");
const keyEql = @import("../bytes.zig").keyEql;

pub const Operator = enum {
    Eq,
    NotEq,
    Lt,
    LtEq,
    Gt,
    GtEq,

    pub fn negate(self: Operator) Operator {
        return switch (self) {
            .Eq => .NotEq,
            .NotEq => .Eq,
            .Lt => .GtEq,
            .LtEq => .Gt,
            .Gt => .LtEq,
            .GtEq => .Lt,
        };
    }
};

/// Test a comparison `a op b` returning whether the predicate holds.
/// `T` must support all the comparison operators. Floats compare in total order (see `floatOp`).
pub fn applyOp(comptime T: type, a: T, op: Operator, b: T) bool {
    if (comptime @typeInfo(T) == .float) return floatOp(op, T, a, b);
    return switch (op) {
        .Eq => a == b,
        .NotEq => a != b,
        .Lt => a < b,
        .LtEq => a <= b,
        .Gt => a > b,
        .GtEq => a >= b,
    };
}

/// `a op b` for floats, or vectors of them, in the total order every other path already sorts, groups and takes
/// min/max in: NaN equals NaN and lies above every number (-0 still equals +0). Each operator is then the exact
/// complement of its `negate`, so NOT stays a flip of the operator. `a > b` alone kept no NaN row, and so neither did
/// `NOT (a < b)`.
pub inline fn floatOp(op: Operator, comptime T: type, a: T, b: T) @TypeOf(a == b) {
    const a_nan = a != a;
    const b_nan = b != b;
    return switch (op) {
        .Eq => (a == b) | (a_nan & b_nan),
        .NotEq => (a != b) & !(a_nan & b_nan),
        .Lt => (a < b) | (b_nan & !a_nan),
        .LtEq => (a <= b) | b_nan,
        .Gt => (a > b) | (a_nan & !b_nan),
        .GtEq => (a >= b) | a_nan,
    };
}

/// String comparison via lexicographic byte order.
pub fn applyOpStr(a: []const u8, op: Operator, b: []const u8) bool {
    const cmp = std.mem.order(u8, a, b);
    return switch (op) {
        .Eq => cmp == .eq,
        .NotEq => cmp != .eq,
        .Lt => cmp == .lt,
        .LtEq => cmp != .gt,
        .Gt => cmp == .gt,
        .GtEq => cmp != .lt,
    };
}

pub const Composite = struct {
    /// The operands, two or more. Chains are flat: no operand is a composite of the same kind, except an IN list's,
    /// which keeps its `in_set`. Only grouping parentheses deepen a tree, and the parser bounds their nesting
    /// (`parser.MAX_GROUP_DEPTH`), so every recursive pass over it is bounded however long the expression.
    children: []const Filter,
    /// Set on the root of an IN / NOT IN list the parser built: the list's literals bound for evaluation in one pass
    /// (see `in_set.zig`). The leaves below stay for every other reader (pruning, membership).
    in_set: ?*const in_set_mod.InSet = null,
    /// A NULL member of an IN list (or a comparison with NULL): UNKNOWN for every row no other member matches. The OR
    /// form (`x IN (1, NULL)`) is then exactly the OR of its children, which may be none (`x = NULL`); the AND form
    /// (`x NOT IN (1, NULL)`, or a negated IN) is never TRUE. Negation flips the form and keeps the flag, and such a
    /// node is never flattened into a chain around it, whose negation would lose it.
    null_member: bool = false,

    /// Whether this node, an AND, can pass no row whatever its children say.
    pub fn neverTrue(self: Composite, conjunction: bool) bool {
        return self.null_member and (conjunction or self.children.len == 0);
    }
};

const in_set_mod = @import("in_set.zig");

pub fn Leaf(comptime T: type) type {
    return struct {
        col_idx: usize,
        op: Operator,
        value: T,
        /// Set by the binder alone, when `value` is exactly a value the column stores in its physical type, so its
        /// PLAIN bytes are what a Bloom filter hashed and a dictionary holds. Lanes that convert on the way in (DECIMAL
        /// at the column's scale, INT96 to nanoseconds, FLOAT16 to f64) leave it unset, and so does any leaf built
        /// elsewhere.
        stored: bool = false,
        /// `--trust-stats` on a FLOAT/DOUBLE leaf: a chunk or page whose statistics carry no NaN count is pruned as
        /// if they said zero, so `>`/`>=` skip on the bounds as they did before NaN counted. Without it a missing count
        /// means NaN may be present, and such a chunk is kept (see `encoded.rangeIntersects`).
        trust_missing_nan_count: bool = false,
        /// `--trust-stats`: statistics that are plausible but contradict themselves or go uncorroborated are pruned on
        /// as written (`statistics.Trust.plausible`).
        trust_stats: bool = false,
    };
}

/// `col IS NULL` / `col IS NOT NULL`. Evaluated against the column's
/// definition levels, not its values — so it's type-agnostic (one node
/// covers every physical type). `is_not == false` → IS NULL.
pub const NullCheck = struct {
    col_idx: usize,
    is_not: bool,
    /// As `Leaf.trust_stats`.
    trust_stats: bool = false,
};

/// SQL `LIKE` (`%` = any run, `_` = one char). The pattern is classified
/// once at parse time so most patterns avoid the general matcher — the
/// trick DuckDB's optimizer uses. `operand` is the literal substring for
/// the fast kinds (prefix/suffix/contains/exact) or the raw pattern for
/// `.general`. No ESCAPE clause: `%`/`_` are always wildcards.
pub const LikeKind = enum { exact, prefix, suffix, contains, general };
pub const LikeMatch = struct {
    col_idx: usize,
    kind: LikeKind,
    operand: []const u8,
    negate: bool, // true → NOT LIKE
};

/// Backtracking `%`/`_` matcher for the general case. `%` matches any run
/// (incl. empty); `_` matches exactly one UTF-8 character, as SQL means it
/// (a byte at a time, `_` missed `é` and `a_` missed `aé`). A pattern's `%`
/// and `_` are wildcards before anything else: tested for equality first, a
/// `%` facing a `%` in the value was taken as a literal and left nothing to
/// backtrack to (`'a%b' LIKE '_%'` missed). O(n·m) worst case, linear in
/// practice — the fast kinds skip this entirely.
pub fn likeGlob(pattern: []const u8, str: []const u8) bool {
    var p: usize = 0;
    var s: usize = 0;
    var star_p: ?usize = null;
    var star_s: usize = 0;
    while (s < str.len) {
        if (p < pattern.len and pattern[p] == '%') {
            star_p = p;
            star_s = s;
            p += 1;
        } else if (p < pattern.len and pattern[p] == '_') {
            p += 1;
            s += charLen(str, s);
        } else if (p < pattern.len and pattern[p] == str[s]) {
            p += 1;
            s += 1;
        } else if (star_p) |sp| {
            // Restart on character boundaries, so a later `_` never starts inside a character.
            p = sp + 1;
            star_s += charLen(str, star_s);
            s = star_s;
        } else return false;
    }
    while (p < pattern.len and pattern[p] == '%') p += 1;
    return p == pattern.len;
}

/// Bytes of the UTF-8 character starting at `str[i]`: its lead byte's sequence length when every continuation byte
/// follows as 10xxxxxx, else one, so a binary value (no UTF-8) still matches byte by byte.
fn charLen(str: []const u8, i: usize) usize {
    const n: usize = std.unicode.utf8ByteSequenceLength(str[i]) catch return 1;
    if (n > str.len - i) return 1;
    for (str[i + 1 ..][0 .. n - 1]) |c| if (c & 0xc0 != 0x80) return 1;
    return n;
}

/// Whether `needle` occurs in `hay`. The needle's first and last bytes are compared against 32 starting positions at
/// once, and only positions where both match are checked in full, instead of a byte-by-byte search from every position.
pub fn containsBytes(hay: []const u8, needle: []const u8) bool {
    const n = needle.len;
    if (n == 0) return true;
    if (hay.len < n) return false;
    if (n == 1) return std.mem.indexOfScalar(u8, hay, needle[0]) != null;
    const lanes = 32;
    const V = @Vector(lanes, u8);
    const first: V = @splat(needle[0]);
    const last: V = @splat(needle[n - 1]);
    const middle = needle[1 .. n - 1];
    // Starting positions run over [0, end); a block of them reads the last byte at `i + n - 1 + lanes - 1 < hay.len`.
    const end = hay.len - n + 1;
    var i: usize = 0;
    while (i + lanes <= end) : (i += lanes) {
        const a: V = hay[i..][0..lanes].*;
        const b: V = hay[i + n - 1 ..][0..lanes].*;
        var hits = @as(u32, @bitCast(a == first)) & @as(u32, @bitCast(b == last));
        while (hits != 0) : (hits &= hits - 1) {
            if (keyEql(hay[i + @ctz(hits) + 1 ..][0..middle.len], middle)) return true;
        }
    }
    while (i < end) : (i += 1) {
        if (hay[i] == needle[0] and hay[i + n - 1] == needle[n - 1] and keyEql(hay[i + 1 ..][0..middle.len], middle))
            return true;
    }
    return false;
}

test "containsBytes finds exactly what a byte-by-byte search finds" {
    var prng = std.Random.DefaultPrng.init(0x11ce);
    const rand = prng.random();
    var hay: [300]u8 = undefined;
    var needle: [12]u8 = undefined;
    for (0..20_000) |_| {
        const hn = rand.uintAtMost(usize, hay.len);
        const nn = rand.uintAtMost(usize, needle.len);
        // A three-letter alphabet makes partial and overlapping matches common.
        for (hay[0..hn]) |*c| c.* = "abc"[rand.uintLessThan(usize, 3)];
        for (needle[0..nn]) |*c| c.* = "abc"[rand.uintLessThan(usize, 3)];
        // Sometimes plant the needle, at any offset including the very end.
        if (nn > 0 and nn <= hn and rand.boolean()) @memcpy(hay[rand.uintAtMost(usize, hn - nn)..][0..nn], needle[0..nn]);
        const want = std.mem.indexOf(u8, hay[0..hn], needle[0..nn]) != null;
        try std.testing.expectEqual(want, containsBytes(hay[0..hn], needle[0..nn]));
    }
}

/// Apply a classified LIKE to one value (before the NOT flip).
pub fn likeMatches(kind: LikeKind, operand: []const u8, value: []const u8) bool {
    return switch (kind) {
        .exact => keyEql(value, operand),
        .prefix => std.mem.startsWith(u8, value, operand),
        .suffix => std.mem.endsWith(u8, value, operand),
        .contains => containsBytes(value, operand),
        .general => likeGlob(operand, value),
    };
}

pub const Filter = union(enum) {
    int32: Leaf(i32),
    int64: Leaf(i64),
    /// Any unsigned integer column (UINT_8..UINT_64, INTEGER{isSigned=false}), compared as u64. Decoded lanes hold
    /// such a column as raw i32 bits, zero-extended i64, or raw u64 bits in an i64; all three bitcast to the value.
    uint64: Leaf(u64),
    float: Leaf(f32),
    double: Leaf(f64),
    /// A DECIMAL column, compared exactly: `value` is the literal's unscaled integer at the column's scale (see
    /// `exactLeaf` for a literal that scale cannot hold), tested against the decoded `unscaled` integers.
    decimal: Leaf(i128),
    string: Leaf([]const u8),
    boolean: Leaf(bool),
    null_check: NullCheck,
    like: LikeMatch,
    and_filter: Composite,
    or_filter: Composite,

    /// Walk the AST collecting every leaf's column index.
    /// Useful for the "fetch only filter columns first" pattern.
    pub fn collectColumns(self: Filter, list: *std.ArrayList(usize), allocator: std.mem.Allocator) std.mem.Allocator.Error!void {
        switch (self) {
            .int32 => |f| try list.append(allocator, f.col_idx),
            .int64 => |f| try list.append(allocator, f.col_idx),
            .uint64 => |f| try list.append(allocator, f.col_idx),
            .float => |f| try list.append(allocator, f.col_idx),
            .double => |f| try list.append(allocator, f.col_idx),
            .decimal => |f| try list.append(allocator, f.col_idx),
            .string => |f| try list.append(allocator, f.col_idx),
            .boolean => |f| try list.append(allocator, f.col_idx),
            .null_check => |f| try list.append(allocator, f.col_idx),
            .like => |f| try list.append(allocator, f.col_idx),
            .and_filter, .or_filter => |c| for (c.children) |child| try child.collectColumns(list, allocator),
        }
    }
};

/// The column a leaf reads; null for a composite.
pub fn leafColumn(f: Filter) ?usize {
    return switch (f) {
        .and_filter, .or_filter => null,
        inline else => |leaf| leaf.col_idx,
    };
}

/// Mark every column some leaf of `f` reads outside its mandatory conjuncts: beneath an OR, where a row may pass
/// although the leaf fails. What a page proves of a mandatory conjunct holds for every row that passes; a leaf under
/// an OR proves nothing of its siblings, so a reader that stands proven values in for a column's real ones
/// (consumer's page min-fill) must decode every column marked here.
pub fn markNonMandatoryColumns(f: Filter, cols: []bool) void {
    switch (f) {
        .and_filter => |c| for (c.children) |child| markNonMandatoryColumns(child, cols),
        .or_filter => |c| for (c.children) |child| markAllColumns(child, cols),
        else => {},
    }
}

/// Mark every column a leaf of `f` reads.
pub fn markColumns(f: Filter, cols: []bool) void {
    markAllColumns(f, cols);
}

/// Whether two trees are the same predicate as bound: the same shape, columns, operators and literal values. An IN
/// list's set is derived from its leaves, so it is not compared.
pub fn eql(a: Filter, b: Filter) bool {
    if (std.meta.activeTag(a) != std.meta.activeTag(b)) return false;
    return switch (a) {
        .and_filter, .or_filter => |ca| blk: {
            const cb = if (a == .and_filter) b.and_filter else b.or_filter;
            if (ca.children.len != cb.children.len or ca.null_member != cb.null_member) break :blk false;
            for (ca.children, cb.children) |x, y| if (!eql(x, y)) break :blk false;
            break :blk true;
        },
        .string => |l| l.col_idx == b.string.col_idx and l.op == b.string.op and std.mem.eql(u8, l.value, b.string.value) and
            l.trust_stats == b.string.trust_stats,
        .float => |l| l.col_idx == b.float.col_idx and l.op == b.float.op and
            @as(u32, @bitCast(l.value)) == @as(u32, @bitCast(b.float.value)) and
            l.trust_missing_nan_count == b.float.trust_missing_nan_count and l.trust_stats == b.float.trust_stats,
        .double => |l| l.col_idx == b.double.col_idx and l.op == b.double.op and
            @as(u64, @bitCast(l.value)) == @as(u64, @bitCast(b.double.value)) and
            l.trust_missing_nan_count == b.double.trust_missing_nan_count and l.trust_stats == b.double.trust_stats,
        .null_check => |n| n.col_idx == b.null_check.col_idx and n.is_not == b.null_check.is_not and
            n.trust_stats == b.null_check.trust_stats,
        .like => |m| m.col_idx == b.like.col_idx and m.kind == b.like.kind and m.negate == b.like.negate and
            std.mem.eql(u8, m.operand, b.like.operand),
        inline .int32, .int64, .uint64, .decimal, .boolean => |l, tag| blk: {
            const o = @field(b, @tagName(tag));
            break :blk l.col_idx == o.col_idx and l.op == o.op and l.value == o.value and l.trust_stats == o.trust_stats;
        },
    };
}

/// `col op v` as an equivalent comparison against a value of type `T`, where `v` is `floor` when exact and otherwise
/// lies strictly between `floor` and `floor + 1` (or beyond `T`'s range). Rounding a literal to the column's unit
/// changed answers (`>= 0.0995` s on a millisecond column matched 0.099 s), and because the rewrite is equivalent on
/// every non-null value, eval, page/row-group pruning and the full-match proof stay exact unchanged.
pub fn exactLeaf(comptime T: type, col_idx: usize, op: Operator, floor: i256, inexact: bool) Leaf(T) {
    const lo = std.math.minInt(T);
    const hi = std.math.maxInt(T);
    // No stored value equals `v`: `=` matches nothing and `!=` every non-null row. `col > maxInt` and
    // `col <= maxInt` say exactly that while still rejecting nulls, and negate into each other for NOT.
    const never: Leaf(T) = .{ .col_idx = col_idx, .op = .Gt, .value = hi };
    const always: Leaf(T) = .{ .col_idx = col_idx, .op = .LtEq, .value = hi };
    const o: Operator = if (!inexact) op else switch (op) {
        .Eq => return never,
        .NotEq => return always,
        .Gt, .GtEq => .Gt,
        .Lt, .LtEq => .LtEq,
    };
    if (floor > hi) return switch (o) {
        .Gt, .GtEq, .Eq => never,
        .Lt, .LtEq, .NotEq => always,
    };
    if (floor < lo) return switch (o) {
        .Lt, .LtEq, .Eq => never,
        .Gt, .GtEq, .NotEq => always,
    };
    return .{ .col_idx = col_idx, .op = o, .value = @intCast(floor) };
}

/// `f` with every FLOAT/DOUBLE leaf trusting a missing NaN count as zero, composites rebuilt around them; the input is
/// not changed. The NaN probe asks this alone of a filter; `--trust-stats` asks `trustStats`.
pub fn trustMissingNanCounts(arena: std.mem.Allocator, f: Filter) std.mem.Allocator.Error!Filter {
    return withTrust(arena, f, false);
}

/// `f` as `--trust-stats` reads it: every leaf trusts plausible statistics, and FLOAT/DOUBLE leaves a missing NaN count
/// as zero.
pub fn trustStats(arena: std.mem.Allocator, f: Filter) std.mem.Allocator.Error!Filter {
    return withTrust(arena, f, true);
}

fn withTrust(arena: std.mem.Allocator, f: Filter, plausible: bool) std.mem.Allocator.Error!Filter {
    return switch (f) {
        inline .int32, .int64, .uint64, .float, .double, .decimal, .string, .boolean => |l, tag| blk: {
            var out = l;
            if (tag == .float or tag == .double) out.trust_missing_nan_count = true;
            if (plausible) out.trust_stats = true;
            break :blk @unionInit(Filter, @tagName(tag), out);
        },
        .null_check => |n| blk: {
            var out = n;
            if (plausible) out.trust_stats = true;
            break :blk .{ .null_check = out };
        },
        .and_filter, .or_filter => |c| blk: {
            const children = try arena.alloc(Filter, c.children.len);
            for (c.children, children) |child, *n| n.* = try withTrust(arena, child, plausible);
            var out = c;
            out.children = children;
            break :blk if (f == .and_filter) .{ .and_filter = out } else .{ .or_filter = out };
        },
        .like => f,
    };
}

fn markAllColumns(f: Filter, cols: []bool) void {
    switch (f) {
        .and_filter, .or_filter => |c| for (c.children) |child| markAllColumns(child, cols),
        else => if (leafColumn(f)) |ci| if (ci < cols.len) {
            cols[ci] = true;
        },
    }
}

/// `operands` joined by AND (OR when `!conjunction`) into one flat node: an operand that is itself a node of the same
/// kind, without an IN list's set, contributes its operands instead. One operand is returned as it is.
pub fn join(arena: std.mem.Allocator, conjunction: bool, operands: []const Filter) std.mem.Allocator.Error!Filter {
    if (operands.len == 1) return operands[0];
    var flat: std.ArrayList(Filter) = .empty;
    for (operands) |o| {
        const same: ?Composite = switch (o) {
            .and_filter => |c| if (conjunction and c.in_set == null and !c.null_member) c else null,
            .or_filter => |c| if (!conjunction and c.in_set == null and !c.null_member) c else null,
            else => null,
        };
        if (same) |c| try flat.appendSlice(arena, c.children) else try flat.append(arena, o);
    }
    const c: Composite = .{ .children = try flat.toOwnedSlice(arena) };
    return if (conjunction) .{ .and_filter = c } else .{ .or_filter = c };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "Operator.negate" {
    try testing.expectEqual(Operator.NotEq, Operator.Eq.negate());
    try testing.expectEqual(Operator.Eq, Operator.NotEq.negate());
    try testing.expectEqual(Operator.GtEq, Operator.Lt.negate());
    try testing.expectEqual(Operator.Lt, Operator.GtEq.negate());
    try testing.expectEqual(Operator.Gt, Operator.LtEq.negate());
    try testing.expectEqual(Operator.LtEq, Operator.Gt.negate());
}

test "float comparisons follow the total order (NaN = NaN, above every number), and negate is their complement" {
    const ops = [_]Operator{ .Eq, .NotEq, .Lt, .LtEq, .Gt, .GtEq };
    inline for (.{ f32, f64 }) |T| {
        const nan = std.math.nan(T);
        const inf = std.math.inf(T);
        const vals = [_]T{ -inf, -1, -0.0, 0.0, 0.5, inf, nan, -nan };
        // The order by rank: NaN of any sign above +inf, -0 equal to +0.
        const rank = struct {
            fn of(v: T) f64 {
                return if (std.math.isNan(v)) std.math.floatMax(f64) else if (std.math.isInf(v)) std.math.copysign(std.math.floatMax(f64) / 2, v) else v;
            }
        }.of;
        for (vals) |a| for (vals) |b| for (ops) |op| {
            const want = applyOp(f64, rank(a), op, rank(b));
            try testing.expectEqual(want, applyOp(T, a, op, b));
            try testing.expectEqual(!want, applyOp(T, a, op.negate(), b));
            const V = @Vector(4, T);
            const got: [4]bool = floatOp(op, V, @splat(a), @splat(b));
            try testing.expectEqual(want, got[3]);
        };
    }
}

test "applyOp on integers" {
    try testing.expect(applyOp(i32, 5, .Eq, 5));
    try testing.expect(!applyOp(i32, 5, .Eq, 6));
    try testing.expect(applyOp(i32, 5, .Lt, 6));
    try testing.expect(applyOp(i32, 5, .LtEq, 5));
    try testing.expect(applyOp(i32, 5, .GtEq, 5));
    try testing.expect(applyOp(i32, 5, .Gt, 4));
    try testing.expect(applyOp(i32, -3, .Lt, 0));
}

test "applyOpStr lexicographic" {
    try testing.expect(applyOpStr("abc", .Eq, "abc"));
    try testing.expect(applyOpStr("abc", .Lt, "abd"));
    try testing.expect(applyOpStr("abc", .LtEq, "abc"));
    try testing.expect(!applyOpStr("abc", .Gt, "abc"));
    try testing.expect(applyOpStr("alpha", .Lt, "beta"));
}

test "Filter.collectColumns" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();

    const composite = try join(a, true, &.{
        .{ .int32 = .{ .col_idx = 1, .op = .Eq, .value = 7 } },
        .{ .string = .{ .col_idx = 5, .op = .Lt, .value = "hello" } },
    });

    var cols: std.ArrayList(usize) = .empty;
    defer cols.deinit(a);
    try composite.collectColumns(&cols, a);
    try testing.expectEqualSlices(usize, &.{ 1, 5 }, cols.items);
}

test "join flattens chains of its own kind and keeps an IN list's node whole" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const x: Filter = .{ .int32 = .{ .col_idx = 0, .op = .Eq, .value = 1 } };
    const inner = try join(a, true, &.{ x, x });
    const outer = try join(a, true, &.{ inner, x, inner });
    try testing.expectEqual(@as(usize, 5), outer.and_filter.children.len);
    // OR under AND stays a child; a lone operand comes back as it is.
    const either = try join(a, false, &.{ x, x });
    try testing.expectEqual(@as(usize, 2), (try join(a, true, &.{ either, x })).and_filter.children.len);
    try testing.expect((try join(a, true, &.{x})) == .int32);
}
