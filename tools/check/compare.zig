//! How zpq's answers are compared with an oracle's. One policy for every check and the scorecard:
//!
//! - NULL equals only NULL. sum/min/max/avg over no values is NULL on every engine; count is never NULL.
//! - Integers compare exactly, including integral results an oracle widens to DECIMAL or HUGEINT, so two large
//!   sums one apart never pass.
//! - DECIMAL compares exactly against DECIMAL (lane `exact`). Elsewhere a fractional zpq number is a float.
//! - Floats compare with a relative tolerance (`rel_tol`, absolute below 1.0): engines sum in different orders.
//!   NaN equals NaN and each infinity itself; they are the tokens "NaN", "Infinity" and "-Infinity".
//! - Booleans keep their type: true never equals 1.
//! - Temporals compare exactly, as the text the engines print.
//! - A decimal average is the exact mean rounded half away from zero at the result's scale (`decimalMean`).
//!
//! Multiset comparisons pair rows by `canon` keys (floats rounded to `float_digits` significant digits), then
//! compare each pair with `same`. Parse every input with `parse` so decimal digits survive.

const std = @import("std");

pub const rel_tol = 1e-4;
/// Significant digits a float keeps in `canon`: ample for f32 (about 7), coarse enough to absorb summation order.
pub const float_digits = 6;

/// i512 holds a 40-digit DECIMAL sum scaled by 10^38 with room to spare; wider inputs fail rather than wrap.
pub const Big = i512;

pub const Lane = enum { plain, exact };

pub const Dec = struct {
    /// Never divisible by 10 when scale > 0: equal decimals have equal fields.
    unscaled: Big,
    scale: u16,
};

pub const Val = union(enum) {
    null,
    boolean: bool,
    int: Big,
    /// Exact and non-integral.
    dec: Dec,
    /// Finite; non-integral or beyond 2^53.
    float: f64,
    /// Strings, temporals as printed, and the non-finite float tokens.
    text: []const u8,
    nested: std.json.Value,
};

/// Bare NaN/Infinity tokens (which DuckDB prints, and std.json rejects) become the strings the policy compares.
pub fn quoteTokens(gpa: std.mem.Allocator, text: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(gpa);
    var in_string = false;
    var i: usize = 0;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (in_string) {
            try out.append(gpa, c);
            if (c == '\\' and i + 1 < text.len) {
                i += 1;
                try out.append(gpa, text[i]);
            } else if (c == '"') in_string = false;
            continue;
        }
        if (c == '"') {
            in_string = true;
            try out.append(gpa, c);
            continue;
        }
        inline for (.{ "-Infinity", "Infinity", "NaN" }) |token| {
            if (std.mem.startsWith(u8, text[i..], token)) {
                try out.print(gpa, "\"{s}\"", .{token});
                i += token.len - 1;
                break;
            }
        } else try out.append(gpa, c);
    }
    return out.toOwnedSlice(gpa);
}

/// JSON with every number kept as its source text. A repeated key keeps its last value, as the CLIs print it.
pub fn parse(arena: std.mem.Allocator, text: []const u8) !std.json.Value {
    const quoted = try quoteTokens(arena, text);
    return std.json.parseFromSliceLeaky(std.json.Value, arena, quoted, .{ .parse_numbers = false, .duplicate_field_behavior = .use_last });
}

fn isFractional(text: []const u8) bool {
    return std.mem.indexOfAny(u8, text, ".eE") != null;
}

/// An exact decimal from number text, normalized; integral values become `.int`.
pub fn exactNumber(text: []const u8) !Val {
    var digits: Big = 0;
    var scale: i32 = 0;
    var negative = false;
    var seen_point = false;
    var i: usize = 0;
    if (i < text.len and (text[i] == '-' or text[i] == '+')) {
        negative = text[i] == '-';
        i += 1;
    }
    var any = false;
    while (i < text.len) : (i += 1) {
        const c = text[i];
        if (c == '.') {
            if (seen_point) return error.BadNumber;
            seen_point = true;
        } else if (c >= '0' and c <= '9') {
            digits = try std.math.add(Big, try std.math.mul(Big, digits, 10), c - '0');
            if (seen_point) scale += 1;
            any = true;
        } else if (c == 'e' or c == 'E') {
            const exp = try std.fmt.parseInt(i32, text[i + 1 ..], 10);
            scale -= exp;
            break;
        } else return error.BadNumber;
    }
    if (!any) return error.BadNumber;
    if (negative) digits = -digits;
    while (scale < 0) : (scale += 1) digits = try std.math.mul(Big, digits, 10);
    return normalizeDec(digits, @intCast(scale));
}

fn normalizeDec(unscaled: Big, scale_in: u16) Val {
    var u = unscaled;
    var scale = scale_in;
    while (scale > 0 and @rem(u, 10) == 0) {
        u = @divTrunc(u, 10);
        scale -= 1;
    }
    if (scale == 0) return .{ .int = u };
    return .{ .dec = .{ .unscaled = u, .scale = scale } };
}

fn isToken(text: []const u8) bool {
    return std.mem.eql(u8, text, "NaN") or std.mem.eql(u8, text, "Infinity") or std.mem.eql(u8, text, "-Infinity");
}

/// A float under the policy: non-finite values are their tokens, integral ones below 2^53 are integers.
pub fn fromFloat(f: f64) Val {
    if (std.math.isNan(f)) return .{ .text = "NaN" };
    if (std.math.isInf(f)) return .{ .text = if (f > 0) "Infinity" else "-Infinity" };
    if (@floor(f) == f and @abs(f) < 9007199254740992.0) return .{ .int = @intFromFloat(f) };
    return .{ .float = f };
}

fn floatOfText(text: []const u8) !Val {
    if (isToken(text)) return .{ .text = text };
    return fromFloat(try std.fmt.parseFloat(f64, text));
}

/// A value zpq printed. Fractional numbers are floats unless their column is a DECIMAL (`exact`).
pub fn fromZpq(raw: std.json.Value, lane: Lane) !Val {
    return switch (raw) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .number_string => |s| if (!isFractional(s)) .{ .int = try std.fmt.parseInt(Big, s, 10) } else switch (lane) {
            .exact => exactNumber(s),
            .plain => floatOfText(s),
        },
        .integer => |n| .{ .int = n },
        .float => |f| fromFloat(f),
        .string => |s| .{ .text = s },
        .array, .object => .{ .nested = raw },
    };
}

/// A value the oracle printed, coerced by its declared type.
pub fn fromOracle(raw: std.json.Value, type_name: []const u8) !Val {
    if (raw == .null) return .null;
    const t = type_name;
    const text: ?[]const u8 = switch (raw) {
        .number_string => |s| s,
        .string => |s| s,
        else => null,
    };
    if (startsWithIgnoreCase(t, "DECIMAL")) return exactNumber(text orelse return error.BadOracleValue);
    if (startsWithAny(t, &.{ "DOUBLE", "FLOAT", "REAL" })) return floatOfText(text orelse return error.BadOracleValue);
    if (startsWithIgnoreCase(t, "BOOL")) return switch (raw) {
        .bool => |b| .{ .boolean = b },
        else => error.BadOracleValue,
    };
    if (startsWithAny(t, &.{ "HUGEINT", "BIGINT", "INTEGER", "SMALLINT", "TINYINT", "INT", "UBIGINT", "UINTEGER", "USMALLINT", "UTINYINT", "UHUGEINT" })) {
        return .{ .int = try std.fmt.parseInt(Big, text orelse return error.BadOracleValue, 10) };
    }
    return switch (raw) {
        .number_string => |s| if (isFractional(s)) exactNumber(s) else .{ .int = try std.fmt.parseInt(Big, s, 10) },
        .bool => |b| .{ .boolean = b },
        .string => |s| .{ .text = s },
        else => .{ .nested = raw },
    };
}

fn startsWithIgnoreCase(s: []const u8, prefix: []const u8) bool {
    return s.len >= prefix.len and std.ascii.eqlIgnoreCase(s[0..prefix.len], prefix);
}

fn startsWithAny(s: []const u8, prefixes: []const []const u8) bool {
    for (prefixes) |p| if (startsWithIgnoreCase(s, p)) return true;
    return false;
}

pub fn laneOf(type_name: []const u8) Lane {
    return if (startsWithIgnoreCase(type_name, "DECIMAL")) .exact else .plain;
}

fn toFloat(v: Val) ?f64 {
    return switch (v) {
        .int => |n| @floatFromInt(n),
        .dec => |d| decToFloat(d),
        .float => |f| f,
        else => null,
    };
}

fn decToFloat(d: Dec) f64 {
    var buf: [200]u8 = undefined;
    const text = decText(&buf, d);
    return std.fmt.parseFloat(f64, text) catch unreachable;
}

/// `unscaled` with the point `scale` digits from the right.
pub fn decText(buf: []u8, d: Dec) []const u8 {
    var digits_buf: [180]u8 = undefined;
    const digits = std.fmt.bufPrint(&digits_buf, "{d}", .{@abs(d.unscaled)}) catch unreachable;
    var n: usize = 0;
    if (d.unscaled < 0) {
        buf[0] = '-';
        n = 1;
    }
    if (digits.len <= d.scale) {
        @memcpy(buf[n..][0..2], "0.");
        n += 2;
        @memset(buf[n..][0 .. d.scale - digits.len], '0');
        n += d.scale - digits.len;
        @memcpy(buf[n..][0..digits.len], digits);
        return buf[0 .. n + digits.len];
    }
    const whole = digits.len - d.scale;
    @memcpy(buf[n..][0..whole], digits[0..whole]);
    n += whole;
    buf[n] = '.';
    n += 1;
    @memcpy(buf[n..][0..d.scale], digits[whole..]);
    return buf[0 .. n + d.scale];
}

/// Whether zpq's `got` equals the oracle's `want` under the policy.
pub fn same(got: Val, want: Val) bool {
    if (got == .boolean or want == .boolean) {
        return got == .boolean and want == .boolean and got.boolean == want.boolean;
    }
    if (got == .float or want == .float) {
        if (toFloat(got)) |g| if (toFloat(want)) |w| return @abs(g - w) <= rel_tol * @max(1.0, @abs(w));
    }
    return exactEqual(got, want);
}

fn exactEqual(a: Val, b: Val) bool {
    return switch (a) {
        .null => b == .null,
        .boolean => |x| b == .boolean and b.boolean == x,
        .int => |x| b == .int and b.int == x,
        .dec => |x| b == .dec and b.dec.unscaled == x.unscaled and b.dec.scale == x.scale,
        .float => |x| b == .float and b.float == x,
        .text => |x| b == .text and std.mem.eql(u8, x, b.text),
        .nested => |x| b == .nested and jsonEqual(x, b.nested),
    };
}

/// Structural equality; numbers compare by exact value, object keys in any order.
pub fn jsonEqual(a: std.json.Value, b: std.json.Value) bool {
    switch (a) {
        .array => |xs| {
            if (b != .array or b.array.items.len != xs.items.len) return false;
            for (xs.items, b.array.items) |x, y| if (!jsonEqual(x, y)) return false;
            return true;
        },
        .object => |xs| {
            if (b != .object or b.object.count() != xs.count()) return false;
            var it = xs.iterator();
            while (it.next()) |e| {
                const other = b.object.get(e.key_ptr.*) orelse return false;
                if (!jsonEqual(e.value_ptr.*, other)) return false;
            }
            return true;
        },
        else => {},
    }
    const x = scalarOf(a) catch return false;
    const y = scalarOf(b) catch return false;
    if (x == .float or y == .float) {
        const fx = toFloat(x) orelse return false;
        const fy = toFloat(y) orelse return false;
        return fx == fy;
    }
    return exactEqual(x, y);
}

fn scalarOf(v: std.json.Value) !Val {
    return switch (v) {
        .null => .null,
        .bool => |b| .{ .boolean = b },
        .number_string => |s| if (isFractional(s)) exactNumber(s) else .{ .int = try std.fmt.parseInt(Big, s, 10) },
        .integer => |n| .{ .int = n },
        .float => |f| fromFloat(f),
        .string => |s| .{ .text = s },
        else => error.NotScalar,
    };
}

/// The float rounded to `float_digits` significant digits, as canon() sees it.
pub fn roundFloat(f: f64) Val {
    var buf: [64]u8 = undefined;
    const text = std.fmt.bufPrint(&buf, "{e:.5}", .{f}) catch unreachable;
    return fromFloat(std.fmt.parseFloat(f64, text) catch unreachable);
}

/// A key under which equal values compare equal and sort together.
pub fn canon(arena: std.mem.Allocator, v: Val) ![]const u8 {
    return switch (v) {
        .null => "n",
        .boolean => |b| if (b) "b1" else "b0",
        .int => |n| std.fmt.allocPrint(arena, "i{d}", .{n}),
        .dec => |d| blk: {
            var buf: [200]u8 = undefined;
            break :blk std.fmt.allocPrint(arena, "d{s}", .{decText(&buf, d)});
        },
        .float => |f| switch (roundFloat(f)) {
            .int => |n| std.fmt.allocPrint(arena, "i{d}", .{n}),
            .float => |r| std.fmt.allocPrint(arena, "f{e}", .{r}),
            else => unreachable,
        },
        .text => |s| std.fmt.allocPrint(arena, "s{s}", .{s}),
        .nested => |j| blk: {
            var out: std.Io.Writer.Allocating = .init(arena);
            try canonJson(&out.writer, j);
            break :blk std.fmt.allocPrint(arena, "j{s}", .{out.written()});
        },
    };
}

fn canonJson(w: *std.Io.Writer, v: std.json.Value) !void {
    switch (v) {
        .array => |xs| {
            try w.writeByte('[');
            for (xs.items, 0..) |x, i| {
                if (i != 0) try w.writeByte(',');
                try canonJson(w, x);
            }
            try w.writeByte(']');
        },
        .object => |xs| {
            var keys: std.ArrayList([]const u8) = .empty;
            defer keys.deinit(std.heap.page_allocator);
            var it = xs.iterator();
            while (it.next()) |e| try keys.append(std.heap.page_allocator, e.key_ptr.*);
            std.mem.sort([]const u8, keys.items, {}, lessText);
            try w.writeByte('{');
            for (keys.items, 0..) |k, i| {
                if (i != 0) try w.writeByte(',');
                try w.print("{s}:", .{k});
                try canonJson(w, xs.get(k).?);
            }
            try w.writeByte('}');
        },
        else => {
            const s = scalarOf(v) catch return error.WriteFailed;
            switch (s) {
                .null => try w.writeAll("null"),
                .boolean => |b| try w.writeAll(if (b) "true" else "false"),
                .int => |n| try w.print("{d}", .{n}),
                .dec => |d| {
                    var buf: [200]u8 = undefined;
                    try w.writeAll(decText(&buf, d));
                },
                .float => |f| try w.print("{e}", .{f}),
                .text => |t| try w.print("\"{s}\"", .{t}),
                .nested => unreachable,
            }
        },
    }
}

pub fn lessText(_: void, a: []const u8, b: []const u8) bool {
    return std.mem.order(u8, a, b) == .lt;
}

/// The exact mean of `total` over `n`, rounded half away from zero at `scale`; null over no values.
pub fn decimalMean(total: Val, n: Val, scale: u16) !Val {
    const count: Big = switch (n) {
        .int => |c| c,
        .null => return .null,
        else => return error.BadMean,
    };
    if (count == 0) return .null;
    const exact = if (total == .text) try exactNumber(total.text) else total;
    const sum: Dec = switch (exact) {
        .int => |x| .{ .unscaled = x, .scale = 0 },
        .dec => |d| d,
        .null => return .null,
        else => return error.BadMean,
    };
    const num = try std.math.mul(Big, @intCast(@abs(sum.unscaled)), try pow10(scale));
    const den = try std.math.mul(Big, try pow10(sum.scale), @intCast(@abs(count)));
    const m = @divFloor(try std.math.add(Big, try std.math.mul(Big, 2, num), den), try std.math.mul(Big, 2, den));
    const negative = (sum.unscaled < 0) != (count < 0);
    return normalizeDec(if (negative) -m else m, scale);
}

fn pow10(n: u16) !Big {
    var x: Big = 1;
    for (0..n) |_| x = try std.math.mul(Big, x, 10);
    return x;
}

/// A row of keys, compared and sorted as one.
pub fn rowKey(arena: std.mem.Allocator, row: []const Val) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (row) |v| {
        const k = try canon(arena, v);
        try out.print(arena, "{d}:{s}|", .{ k.len, k });
    }
    return out.items;
}

/// Equal multisets of rows: rows pair up by their `canon` keys, then each value pair must be `same`.
pub fn sameMultiset(arena: std.mem.Allocator, got: []const []const Val, want: []const []const Val) !?[]const u8 {
    if (got.len != want.len) return try std.fmt.allocPrint(arena, "zpq={d} rows oracle={d}", .{ got.len, want.len });
    return sameRows(arena, try sortedRows(arena, got), try sortedRows(arena, want));
}

/// Equal rows in order: each value pair must be `same`.
pub fn sameOrdered(arena: std.mem.Allocator, got: []const []const Val, want: []const []const Val) !?[]const u8 {
    if (got.len != want.len) return try std.fmt.allocPrint(arena, "zpq={d} rows oracle={d}", .{ got.len, want.len });
    return sameRows(arena, got, want);
}

fn sameRows(arena: std.mem.Allocator, got: []const []const Val, want: []const []const Val) !?[]const u8 {
    for (got, want) |g, w| {
        const equal = g.len == w.len and for (g, w) |a, b| {
            if (!same(a, b)) break false;
        } else true;
        if (!equal) return try std.fmt.allocPrint(arena, "row zpq={s} oracle={s}", .{ clip(try rowKey(arena, g)), clip(try rowKey(arena, w)) });
    }
    return null;
}

fn sortedRows(arena: std.mem.Allocator, rows: []const []const Val) ![]const []const Val {
    const Keyed = struct { key: []const u8, row: []const Val };
    const keyed = try arena.alloc(Keyed, rows.len);
    for (rows, 0..) |r, i| keyed[i] = .{ .key = try rowKey(arena, r), .row = r };
    std.mem.sort(Keyed, keyed, {}, struct {
        fn lt(_: void, a: Keyed, b: Keyed) bool {
            return std.mem.order(u8, a.key, b.key) == .lt;
        }
    }.lt);
    const out = try arena.alloc([]const Val, rows.len);
    for (keyed, 0..) |k, i| out[i] = k.row;
    return out;
}

fn clip(s: []const u8) []const u8 {
    return s[0..@min(s.len, 300)];
}

/// A value for messages.
pub fn show(arena: std.mem.Allocator, v: Val) []const u8 {
    return canon(arena, v) catch "?";
}

const testing = std.testing;

fn zv(text: []const u8, lane: Lane) !Val {
    const v = try parse(testing.allocator, text);
    return fromZpq(v, lane);
}

test "the comparator cases each rule exists for" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const P = struct {
        fn z(arena: std.mem.Allocator, text: []const u8, lane: Lane) !Val {
            return fromZpq(try parse(arena, text), lane);
        }
        fn o(arena: std.mem.Allocator, text: []const u8, t: []const u8) !Val {
            return fromOracle(try parse(arena, text), t);
        }
    };
    const big = "9223372036854775809";
    const cases = [_]struct { got: Val, want: Val, expect: bool }{
        .{ .got = .null, .want = .null, .expect = true },
        .{ .got = .{ .int = 0 }, .want = .null, .expect = false }, // an empty SUM is NULL, not 0
        .{ .got = .null, .want = .{ .int = 0 }, .expect = false },
        .{ .got = .{ .text = "" }, .want = .null, .expect = false },
        .{ .got = try P.z(a, big, .plain), .want = try P.o(a, "\"9223372036854775808\"", "DECIMAL(38,0)"), .expect = false },
        .{ .got = try P.z(a, big, .plain), .want = try P.o(a, "\"" ++ big ++ "\"", "HUGEINT"), .expect = true },
        .{ .got = try P.z(a, "6", .plain), .want = try P.o(a, "\"6\"", "DECIMAL(10,0)"), .expect = true },
        .{ .got = try P.z(a, "1.10", .exact), .want = try P.o(a, "\"1.1\"", "DECIMAL(5,2)"), .expect = true },
        .{ .got = try P.z(a, "12345678901234567.01", .exact), .want = try P.o(a, "\"12345678901234567.02\"", "DECIMAL(19,2)"), .expect = false },
        .{ .got = try P.z(a, "12345678901234567890123456789.01", .exact), .want = try P.o(a, "\"12345678901234567890123456789.02\"", "DECIMAL(38,2)"), .expect = false },
        .{ .got = try P.z(a, "1.1", .plain), .want = try P.o(a, "1.10", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "100.00001", .plain), .want = try P.o(a, "100.0", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "100.1", .plain), .want = try P.o(a, "100.0", "DOUBLE"), .expect = false },
        .{ .got = try P.z(a, "NaN", .plain), .want = try P.o(a, "NaN", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "Infinity", .plain), .want = try P.o(a, "Infinity", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "Infinity", .plain), .want = try P.o(a, "-Infinity", "DOUBLE"), .expect = false },
        .{ .got = try P.z(a, "true", .plain), .want = try P.o(a, "1", "INTEGER"), .expect = false },
        .{ .got = try P.z(a, "false", .plain), .want = try P.o(a, "0", "INTEGER"), .expect = false },
        .{ .got = try P.z(a, "true", .plain), .want = try P.o(a, "true", "BOOLEAN"), .expect = true },
        .{ .got = try P.z(a, "\"2020-01-01 00:00:00.0995\"", .plain), .want = try P.o(a, "\"2020-01-01 00:00:00.099\"", "TIMESTAMP"), .expect = false },
    };
    for (cases, 0..) |c, i| {
        errdefer std.debug.print("case {d}\n", .{i});
        try testing.expectEqual(c.expect, same(c.got, c.want));
        // canon agrees with same() wherever no tolerance is involved
        if (c.got != .float and c.want != .float) {
            try testing.expectEqual(c.expect, std.mem.eql(u8, try canon(a, c.got), try canon(a, c.want)));
        }
    }
    // canon rounds floats to six significant digits, so summation-order noise collapses
    try testing.expectEqualStrings(try canon(a, try P.z(a, "0.30000000000000004", .plain)), try canon(a, try P.z(a, "0.3", .plain)));
    try testing.expect(!std.mem.eql(u8, try canon(a, try P.z(a, "0.300001", .plain)), try canon(a, try P.z(a, "0.3", .plain))));
    try testing.expectEqualStrings(try canon(a, try P.z(a, "1234567.4", .plain)), try canon(a, .{ .int = 1234570 }));
}

test "decimal means round half away from zero at the result's scale" {
    var buf: [200]u8 = undefined;
    const cases = [_]struct { sum: Val, n: i64, scale: u16, want: []const u8 }{
        .{ .sum = .{ .int = 5 }, .n = 2, .scale = 0, .want = "3" },
        .{ .sum = .{ .int = -5 }, .n = 2, .scale = 0, .want = "-3" },
        .{ .sum = .{ .dec = .{ .unscaled = 1, .scale = 2 } }, .n = 3, .scale = 4, .want = "0.0033" },
        .{ .sum = .{ .dec = .{ .unscaled = 2, .scale = 2 } }, .n = 3, .scale = 4, .want = "0.0067" },
        .{ .sum = .{ .dec = .{ .unscaled = 5, .scale = 1 } }, .n = 1, .scale = 3, .want = "0.5" },
    };
    for (cases) |c| {
        const got = try decimalMean(c.sum, .{ .int = c.n }, c.scale);
        const text = switch (got) {
            .int => |n| try std.fmt.bufPrint(&buf, "{d}", .{n}),
            .dec => |d| decText(&buf, d),
            else => unreachable,
        };
        try testing.expectEqualStrings(c.want, text);
    }
    try testing.expect(try decimalMean(.null, .{ .int = 0 }, 2) == .null);
    // 38 nines over 1 at scale 4 still fits
    const nines = try exactNumber("99999999999999999999999999999999999999");
    _ = try decimalMean(nines, .{ .int = 1 }, 38);
}

test "bare non-finite tokens become strings, string contents stay" {
    const out = try quoteTokens(testing.allocator, "[NaN,\"NaN x\",-Infinity,Infinity,\"a\\\"NaN\"]");
    defer testing.allocator.free(out);
    try testing.expectEqualStrings("[\"NaN\",\"NaN x\",\"-Infinity\",\"Infinity\",\"a\\\"NaN\"]", out);
}

test "nested values compare structurally and by exact number" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expect(jsonEqual(try parse(a, "{\"x\":1.50,\"y\":[1,null]}"), try parse(a, "{\"y\":[1,null],\"x\":1.5}")));
    try testing.expect(!jsonEqual(try parse(a, "[1,2]"), try parse(a, "[2,1]")));
    try testing.expect(!jsonEqual(try parse(a, "{\"x\":1}"), try parse(a, "{\"x\":\"1\"}")));
    _ = zv;
}
