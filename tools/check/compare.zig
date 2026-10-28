//! How zpq's answers are compared with an oracle's. One policy for every check and the scorecard:
//!
//! - NULL equals only NULL. sum/min/max/avg over no values is NULL on every engine; count is never NULL.
//! - Integers compare exactly, including integral results an oracle widens to DECIMAL or HUGEINT, so two large
//!   sums one apart never pass.
//! - DECIMAL compares exactly against DECIMAL (lane `exact`). Elsewhere a fractional zpq number is a float.
//! - Floats compare with a relative tolerance (`rel_tol`, absolute below 1.0): engines sum in different orders.
//!   NaN equals NaN and each infinity itself, and each equals its token text ("NaN", "Infinity", "-Infinity").
//! - Booleans keep their type: true never equals 1.
//! - Temporals compare exactly, as the text the engines print.
//! - A decimal average is the exact mean rounded half away from zero at the result's scale (`decimalMean`).
//!
//! Row comparisons (`sameMultiset`, `sameOrdered`) require equal `canon` keys, where floats round to `float_digits`
//! significant digits. `exactKey` is raw value equality: a float is its exact binary value, so DECIMAL 0.1 never
//! equals DOUBLE 0.1, and NaN equals nothing. Parse every input with `parse` so decimal digits survive.

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
    /// Strings and temporals as printed.
    text: []const u8,
    nested: std.json.Value,
    /// A non-finite float.
    special: Special,
};

pub const Special = enum {
    nan,
    pos_inf,
    neg_inf,

    pub fn token(self: Special) []const u8 {
        return switch (self) {
            .nan => "NaN",
            .pos_inf => "Infinity",
            .neg_inf => "-Infinity",
        };
    }
};

/// The string a bare NaN/Infinity token becomes: no JSON text holds a NUL byte, so it never collides with data.
const sentinel = "\x00";

/// Bare NaN/Infinity tokens (which DuckDB prints, and std.json rejects) become sentinel strings that `fromZpq` and
/// `fromOracle` read back as floats, distinct from the strings "NaN" and "Infinity".
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
                try out.print(gpa, "\"\\u0000{s}\"", .{token});
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
    return std.json.parseFromSliceLeaky(
        std.json.Value,
        arena,
        quoted,
        .{
            .parse_numbers = false,
            .duplicate_field_behavior = .use_last,
        },
    );
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

/// The non-finite float a token spells, with or without the sentinel.
fn specialOf(text: []const u8) ?Special {
    const t = if (std.mem.startsWith(u8, text, sentinel)) text[sentinel.len..] else text;
    if (std.mem.eql(u8, t, "NaN")) return .nan;
    if (std.mem.eql(u8, t, "Infinity")) return .pos_inf;
    if (std.mem.eql(u8, t, "-Infinity")) return .neg_inf;
    return null;
}

/// The f64 nearest `n`, rounded as an exact conversion rounds. LLVM lowers no conversion of an integer wider than 128
/// bits to a float on aarch64, so a wide value is shifted into 120 bits first, with a sticky low bit standing for the
/// bits shifted out: an f64 keeps 53, so the rounding is the exact value's.
pub fn bigToFloat(n: Big) f64 {
    if (std.math.cast(i128, n)) |x| return @floatFromInt(x);
    const shift: u16 = @intCast(@bitSizeOf(Big) - @clz(@abs(n)) - 120);
    var top: i128 = @intCast(n >> @intCast(shift));
    if ((n & ((@as(Big, 1) << @intCast(shift)) - 1)) != 0) top |= 1;
    return std.math.ldexp(@as(f64, @floatFromInt(top)), shift);
}

/// A float under the policy: non-finite values are their tokens, integral ones below 2^53 are integers.
pub fn fromFloat(f: f64) Val {
    if (std.math.isNan(f)) return .{ .special = .nan };
    if (std.math.isInf(f)) return .{ .special = if (f > 0) .pos_inf else .neg_inf };
    if (@floor(f) == f and @abs(f) < 9007199254740992.0) return .{ .int = @as(i64, @intFromFloat(f)) };
    return .{ .float = f };
}

fn floatOfText(text: []const u8) !Val {
    if (specialOf(text)) |sp| return .{ .special = sp };
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
        // A NUL-led string that is not exactly a token is data (corpus strings start with NUL too).
        .string => |s| if (!std.mem.startsWith(u8, s, sentinel))
            .{ .text = s }
        else if (specialOf(s)) |sp| .{ .special = sp } else .{ .text = s },
        .array, .object => .{ .nested = raw },
    };
}

/// A value the oracle printed, coerced by its declared type.
pub fn fromOracle(raw: std.json.Value, type_name: []const u8) !Val {
    if (raw == .null) return .null;
    // A LIST or STRUCT type's name starts with its element's (BIGINT[]): its value is nested whatever that is.
    if (raw == .array or raw == .object) return .{ .nested = raw };
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
    if (startsWithAny(t, &.{
        "HUGEINT",
        "BIGINT",
        "INTEGER",
        "SMALLINT",
        "TINYINT",
        "INT",
        "UBIGINT",
        "UINTEGER",
        "USMALLINT",
        "UTINYINT",
        "UHUGEINT",
    })) {
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

pub fn isFloatType(type_name: []const u8) bool {
    return startsWithAny(type_name, &.{ "DOUBLE", "FLOAT", "REAL" }) and
        std.mem.indexOfScalar(u8, type_name, '[') == null;
}

pub fn laneOf(type_name: []const u8) Lane {
    return if (startsWithIgnoreCase(type_name, "DECIMAL")) .exact else .plain;
}

fn toFloat(v: Val) ?f64 {
    return switch (v) {
        .int => |n| bigToFloat(n),
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
pub fn same(got_in: Val, want_in: Val) bool {
    // As the old policy did, a non-finite float compares as its token text.
    const got: Val = if (got_in == .special) .{ .text = got_in.special.token() } else got_in;
    const want: Val = if (want_in == .special) .{ .text = want_in.special.token() } else want_in;
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
        .special => |x| b == .special and b.special == x,
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
        // A NUL-led string that is not exactly a token is data (corpus strings start with NUL too).
        .string => |s| if (!std.mem.startsWith(u8, s, sentinel))
            .{ .text = s }
        else if (specialOf(s)) |sp| .{ .special = sp } else .{ .text = s },
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
        .special => |x| std.fmt.allocPrint(arena, "s{s}", .{x.token()}),
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
                .special => |x| try w.print("\"{s}\"", .{x.token()}),
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

/// Equal multisets of rows: the sorted `canon` keys must be identical.
pub fn sameMultiset(arena: std.mem.Allocator, got: []const []const Val, want: []const []const Val) !?[]const u8 {
    if (got.len != want.len) return try std.fmt.allocPrint(arena, "zpq={d} rows oracle={d}", .{ got.len, want.len });
    const a = try sortedKeys(arena, got, rowKey);
    const b = try sortedKeys(arena, want, rowKey);
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return try std.fmt.allocPrint(arena, "row zpq={s} oracle={s}", .{
        clip(x),
        clip(y),
    });
    return null;
}

/// Equal rows in order: each pair's `canon` keys must be identical.
pub fn sameOrdered(arena: std.mem.Allocator, got: []const []const Val, want: []const []const Val) !?[]const u8 {
    if (got.len != want.len) return try std.fmt.allocPrint(arena, "zpq={d} rows oracle={d}", .{ got.len, want.len });
    for (got, want) |g, w| {
        const x = try rowKey(arena, g);
        const y = try rowKey(arena, w);
        if (!std.mem.eql(u8, x, y)) return try std.fmt.allocPrint(
            arena,
            "row zpq={s} oracle={s}",
            .{ clip(x), clip(y) },
        );
    }
    return null;
}

/// Raw value equality, as Python's `==` on the values the old comparer held: numbers by exact value across
/// integer, DECIMAL and binary float; NaN equal to nothing (`nan` numbers each NaN apart); other values by type.
pub fn exactKey(arena: std.mem.Allocator, v: Val, nan: *usize) ![]const u8 {
    return switch (v) {
        .int => |n| std.fmt.allocPrint(arena, "x{d}", .{n}),
        .dec => |d| blk: {
            var buf: [200]u8 = undefined;
            break :blk std.fmt.allocPrint(arena, "x{s}", .{decText(&buf, d)});
        },
        .float => |f| std.fmt.allocPrint(arena, "x{s}", .{try exactFloatText(arena, f)}),
        .special => |x| switch (x) {
            .nan => blk: {
                nan.* += 1;
                break :blk std.fmt.allocPrint(arena, "nan#{d}", .{nan.*});
            },
            else => std.fmt.allocPrint(arena, "f{s}", .{x.token()}),
        },
        else => canon(arena, v),
    };
}

/// Equal multisets (or, `ordered`, sequences) of rows under `exactKey`.
pub fn exactRows(
    arena: std.mem.Allocator,
    got: []const []const Val,
    want: []const []const Val,
    ordered: bool,
) !?[]const u8 {
    if (got.len != want.len) return try std.fmt.allocPrint(arena, "zpq={d} rows truth={d}", .{ got.len, want.len });
    var nan: usize = 0;
    const a = try arena.alloc([]const u8, got.len);
    const b = try arena.alloc([]const u8, want.len);
    for (got, 0..) |r, i| a[i] = try exactRowKey(arena, r, &nan);
    for (want, 0..) |r, i| b[i] = try exactRowKey(arena, r, &nan);
    if (!ordered) {
        std.mem.sort([]const u8, a, {}, lessText);
        std.mem.sort([]const u8, b, {}, lessText);
    }
    for (a, b) |x, y| if (!std.mem.eql(u8, x, y)) return try std.fmt.allocPrint(arena, "row zpq={s} truth={s}", .{
        clip(x),
        clip(y),
    });
    return null;
}

fn exactRowKey(arena: std.mem.Allocator, row: []const Val, nan: *usize) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (row) |v| {
        const k = try exactKey(arena, v, nan);
        try out.print(arena, "{d}:{s}|", .{ k.len, k });
    }
    return out.items;
}

/// The exact decimal value of a binary float: m * 2^e is m * 5^-e / 10^-e when e < 0, which is finite.
pub fn exactFloatText(arena: std.mem.Allocator, f: f64) ![]const u8 {
    const bits: u64 = @bitCast(f);
    const negative = bits >> 63 == 1;
    const biased: i32 = @intCast((bits >> 52) & 0x7ff);
    var mantissa: u64 = bits & ((1 << 52) - 1);
    var exp: i32 = undefined;
    if (biased == 0) {
        exp = -1074;
    } else {
        mantissa |= 1 << 52;
        exp = biased - 1075;
    }
    if (mantissa == 0) return "0";
    while (mantissa & 1 == 0) {
        mantissa >>= 1;
        exp += 1;
    }
    var n = try std.math.big.int.Managed.initSet(arena, mantissa);
    var scale: usize = 0;
    if (exp >= 0) {
        try n.shiftLeft(&n, @intCast(exp));
    } else {
        scale = @intCast(-exp);
        var five = try std.math.big.int.Managed.initSet(arena, 5);
        try five.pow(&five, @intCast(scale));
        try n.mul(&n, &five);
    }
    const digits = try n.toString(arena, 10, .lower);
    return pointText(arena, negative, digits, scale);
}

/// `digits` with the point `scale` places from the right, in `decText`'s form.
fn pointText(arena: std.mem.Allocator, negative: bool, digits: []const u8, scale: usize) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    if (negative) try out.append(arena, '-');
    if (scale == 0) {
        try out.appendSlice(arena, digits);
    } else if (digits.len <= scale) {
        try out.appendSlice(arena, "0.");
        try out.appendNTimes(arena, '0', scale - digits.len);
        try out.appendSlice(arena, digits);
    } else {
        try out.appendSlice(arena, digits[0 .. digits.len - scale]);
        try out.append(arena, '.');
        try out.appendSlice(arena, digits[digits.len - scale ..]);
    }
    return out.items;
}

fn sortedKeys(arena: std.mem.Allocator, rows: []const []const Val, comptime key: anytype) ![]const []const u8 {
    const keys = try arena.alloc([]const u8, rows.len);
    for (rows, 0..) |r, i| keys[i] = try key(arena, r);
    std.mem.sort([]const u8, keys, {}, lessText);
    return keys;
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
        .{
            .got = try P.z(a, big, .plain),
            .want = try P.o(a, "\"9223372036854775808\"", "DECIMAL(38,0)"),
            .expect = false,
        },
        .{ .got = try P.z(a, big, .plain), .want = try P.o(a, "\"" ++ big ++ "\"", "HUGEINT"), .expect = true },
        .{ .got = try P.z(a, "6", .plain), .want = try P.o(a, "\"6\"", "DECIMAL(10,0)"), .expect = true },
        .{ .got = try P.z(a, "1.10", .exact), .want = try P.o(a, "\"1.1\"", "DECIMAL(5,2)"), .expect = true },
        .{
            .got = try P.z(a, "12345678901234567.01", .exact),
            .want = try P.o(
                a,
                "\"12345678901234567.02\"",
                "DECIMAL(19,2)",
            ),
            .expect = false,
        },
        .{
            .got = try P.z(a, "12345678901234567890123456789.01", .exact),
            .want = try P.o(
                a,
                "\"12345678901234567890123456789.02\"",
                "DECIMAL(38,2)",
            ),
            .expect = false,
        },
        .{ .got = try P.z(a, "1.1", .plain), .want = try P.o(a, "1.10", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "100.00001", .plain), .want = try P.o(a, "100.0", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "100.1", .plain), .want = try P.o(a, "100.0", "DOUBLE"), .expect = false },
        .{ .got = try P.z(a, "NaN", .plain), .want = try P.o(a, "NaN", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "Infinity", .plain), .want = try P.o(a, "Infinity", "DOUBLE"), .expect = true },
        .{ .got = try P.z(a, "Infinity", .plain), .want = try P.o(a, "-Infinity", "DOUBLE"), .expect = false },
        .{ .got = try P.z(a, "true", .plain), .want = try P.o(a, "1", "INTEGER"), .expect = false },
        .{ .got = try P.z(a, "false", .plain), .want = try P.o(a, "0", "INTEGER"), .expect = false },
        .{ .got = try P.z(a, "true", .plain), .want = try P.o(a, "true", "BOOLEAN"), .expect = true },
        .{
            .got = try P.z(a, "\"2020-01-01 00:00:00.0995\"", .plain),
            .want = try P.o(
                a,
                "\"2020-01-01 00:00:00.099\"",
                "TIMESTAMP",
            ),
            .expect = false,
        },
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
    try testing.expectEqualStrings(try canon(a, try P.z(a, "0.30000000000000004", .plain)), try canon(a, try P.z(
        a,
        "0.3",
        .plain,
    )));
    try testing.expect(!std.mem.eql(u8, try canon(a, try P.z(a, "0.300001", .plain)), try canon(a, try P.z(
        a,
        "0.3",
        .plain,
    ))));
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

test "bare non-finite tokens become floats, distinct from the strings that spell them" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const v = try parse(a, "[NaN,\"NaN\",-Infinity,Infinity,\"a\\\"NaN\"]");
    const items = v.array.items;
    try testing.expect((try fromZpq(items[0], .plain)) == .special);
    try testing.expectEqualStrings("NaN", (try fromZpq(items[1], .plain)).text);
    try testing.expect((try fromZpq(items[2], .plain)).special == .neg_inf);
    try testing.expect((try fromZpq(items[3], .plain)).special == .pos_inf);
    try testing.expectEqualStrings("a\"NaN", (try fromZpq(items[4], .plain)).text);
    // The policy still lets a float NaN equal the token text, as the old comparer did.
    try testing.expect(same(try fromZpq(items[0], .plain), try fromZpq(items[1], .plain)));
}

test "row comparisons need equal canonical values, not values within the scalar tolerance" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const got = [_][]const Val{&.{ .{ .int = 1 }, .{ .float = 0.300001 } }};
    const want = [_][]const Val{&.{ .{ .int = 1 }, .{ .float = 0.3 } }};
    try testing.expect(same(got[0][1], want[0][1])); // within 1e-4: a scalar answer passes
    try testing.expect((try sameMultiset(a, &got, &want)) != null);
    try testing.expect((try sameOrdered(a, &got, &want)) != null);
    const near = [_][]const Val{&.{ .{ .int = 1 }, .{ .float = 0.30000000000000004 } }};
    try testing.expect((try sameOrdered(a, &near, &want)) == null);
}

test "raw equality is exact: a binary 0.1 is not the DECIMAL 0.1, and NaN equals nothing" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const dec01 = try exactNumber("0.1");
    try testing.expect((try exactRows(a, &.{&.{dec01}}, &.{&.{Val{ .float = 0.1 }}}, true)) != null);
    try testing.expect((try exactRows(a, &.{&.{try exactNumber("1.5")}}, &.{&.{Val{ .float = 1.5 }}}, true)) == null);
    try testing.expect((try exactRows(
        a,
        &.{&.{try exactNumber("0.1000000000000000055511151231257827021181583404541015625")}},
        &.{&.{Val{ .float = 0.1 }}},
        true,
    )) == null);
    try testing.expect((try exactRows(
        a,
        &.{&.{Val{ .special = .nan }}},
        &.{&.{Val{ .special = .nan }}},
        true,
    )) != null);
    try testing.expect((try exactRows(
        a,
        &.{&.{Val{ .special = .pos_inf }}},
        &.{&.{Val{ .special = .pos_inf }}},
        true,
    )) == null);
    try testing.expectEqualStrings("-0.000244140625", try exactFloatText(a, -0.000244140625));
}

test "nested values compare structurally and by exact number" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    try testing.expect(jsonEqual(
        try parse(a, "{\"x\":1.50,\"y\":[1,null]}"),
        try parse(a, "{\"y\":[1,null],\"x\":1.5}"),
    ));
    try testing.expect(!jsonEqual(try parse(a, "[1,2]"), try parse(a, "[2,1]")));
    try testing.expect(!jsonEqual(try parse(a, "{\"x\":1}"), try parse(a, "{\"x\":\"1\"}")));
    // A list's type name begins with its element type's: the value decides, so BIGINT[] is not an integer.
    const list = try fromOracle(try parse(a, "[0,null,3]"), "BIGINT[]");
    try testing.expect(list == .nested and same(list, try fromOracle(try parse(a, "[0,null,3]"), "BIGINT[]")));
    try testing.expect(!same(list, try fromOracle(try parse(a, "[0,null,4]"), "BIGINT[]")));
    _ = zv;
}

test "a wide integer converts to the f64 an exact conversion gives, ties and sticky bits included" {
    const two = @as(Big, 1);
    const cases = [_]Big{
        0,                -7,
        (two << 127) - 1, -(two << 127),
        (two << 200) + 1, (two << 200) + (two << 147), // a tie, rounded to even
        (two << 200) + (two << 147) + 1, // above the tie: up
        -((two << 300) + (two << 247) + 3),
        (two << 510) - 1,
    };
    for (cases) |n| {
        if (@import("builtin").cpu.arch == .x86_64) {
            try testing.expectEqual(@as(f64, @floatFromInt(n)), bigToFloat(n));
        }
    }
    try testing.expectEqual(std.math.ldexp(@as(f64, 1), 200), bigToFloat((two << 200) + (two << 147)));
    const up = std.math.ldexp(@as(f64, 1), 200) + std.math.ldexp(@as(f64, 1), 148);
    try testing.expectEqual(up, bigToFloat((two << 200) + (two << 147) + 1));
}
