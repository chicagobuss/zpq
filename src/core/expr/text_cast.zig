//! Text read as a number, as DuckDB 2.0's `CAST(varchar AS ...)` reads it (its `strict = false` cast, the one `CAST` and
//! `TRY_CAST` use). Ported from `integer_cast_operator.hpp` and `double_cast_operator.hpp` with its patched
//! `fast_float`, so the same strings parse and the same ones do not: surrounding whitespace, one `+`, hexadecimal and
//! binary integers, a `_` between digits, a decimal point and an exponent in an integer (rounded half away from zero),
//! `inf`, `infinity` and `nan` in any case.
const std = @import("std");
const testing = std.testing;

/// DuckDB's `StringUtil::CharacterIsSpace`.
fn isSpace(c: u8) bool {
    return c == ' ' or (c >= 9 and c <= 13);
}

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

fn hexValue(c: u8) ?u8 {
    return switch (c) {
        '0'...'9' => c - '0',
        'a'...'f' => c - 'a' + 10,
        'A'...'F' => c - 'A' + 10,
        else => null,
    };
}

/// The running value of `IntegerDecimalCastData<int64_t>`: the digits so far, the first fractional digits, and how many
/// there were.
const IntState = struct {
    result: i64 = 0,
    decimal: i64 = 0,
    decimal_digits: u64 = 0,
};

fn handleDigit(s: *IntState, negative: bool, digit: u8) bool {
    if (negative) {
        if (s.result < @divTrunc(std.math.minInt(i64) + @as(i64, digit), 10)) return false;
        s.result = s.result * 10 - digit;
    } else {
        if (s.result > @divTrunc(std.math.maxInt(i64) - @as(i64, digit), 10)) return false;
        s.result = s.result * 10 + digit;
    }
    return true;
}

/// Digits past the 32768th cannot change the result for any exponent; ones that overflow the store are dropped.
fn handleDecimal(s: *IntState, digit: u8) void {
    if (s.decimal_digits == 32768 or s.decimal > @divTrunc(std.math.maxInt(i64) - @as(i64, digit), 10)) return;
    s.decimal_digits += 1;
    s.decimal = s.decimal * 10 + digit;
}

/// `IntegerDecimalCastOperation::Finalize`: round on the first fractional digit, half away from zero.
fn finalize(s: *IntState, negative: bool) bool {
    var tmp = s.result;
    while (s.decimal > 10) {
        s.decimal = @divTrunc(s.decimal, 10);
        s.decimal_digits -%= 1;
    }
    if (s.decimal_digits == 1 and s.decimal >= 5) {
        const r = if (negative) @subWithOverflow(tmp, 1) else @addWithOverflow(tmp, 1);
        if (r[1] != 0) return false;
        tmp = r[0];
    }
    s.result = tmp;
    return true;
}

fn handleExponent(s: *IntState, negative: bool, exponent: i16) bool {
    var e = exponent;
    if (e < 0) {
        while (e < 0) {
            e += 1;
            s.decimal = @rem(s.result, 10);
            s.result = @divTrunc(s.result, 10);
            if (s.result == 0 and s.decimal == 0) break;
        }
        if (s.decimal < 0) s.decimal = -s.decimal;
        s.decimal_digits = 1;
        return finalize(s, negative);
    }
    while (s.result != 0 and e > 0) {
        e -= 1;
        const r = @mulWithOverflow(s.result, 10);
        if (r[1] != 0) return false;
        s.result = r[0];
    }
    if (s.decimal == 0) return finalize(s, negative);
    var shift: i64 = @as(i64, exponent) -% @as(i64, @bitCast(s.decimal_digits));
    var remainder: i64 = 0;
    if (shift < 0) {
        if (@as(u64, @intCast(-shift)) < 19) {
            var power: i64 = 1;
            while (shift < 0) {
                shift += 1;
                power *= 10;
            }
            remainder = @rem(s.decimal, power);
            s.decimal = @divTrunc(s.decimal, power);
        } else {
            remainder = s.decimal;
            s.decimal = 0;
        }
    } else {
        while (shift > 0) {
            shift -= 1;
            const r = @mulWithOverflow(s.decimal, 10);
            if (r[1] != 0) return false;
            s.decimal = r[0];
        }
    }
    s.decimal_digits -%= @as(u64, @bitCast(@as(i64, exponent)));
    const joined = if (negative) @subWithOverflow(s.result, s.decimal) else @addWithOverflow(s.result, s.decimal);
    if (joined[1] != 0) return false;
    s.result = joined[0];
    s.decimal = remainder;
    return finalize(s, negative);
}

/// The exponent of a number: `IntegerCastLoop` over 16 bits with no exponent and no decimal digits of its own. It is a plain
/// integer, optionally signed, digits with a `_` between them, trailing whitespace allowed, and a `.` is allowed if no digit
/// follows it (`6E3.` is 6000), which is how that loop reads it.
fn parseExponent(buf: []const u8) ?i16 {
    const negative = buf[0] == '-';
    const start_pos: usize = if (negative or buf[0] == '+') 1 else 0;
    var pos = start_pos;
    var v: i32 = 0;
    while (pos < buf.len) {
        if (!isDigit(buf[pos])) {
            if (buf[pos] == '.') {
                const number_before_period = pos > start_pos;
                pos += 1;
                const start_digit = pos;
                // A digit after the point is a decimal, which a plain integer does not take.
                if (pos < buf.len and isDigit(buf[pos])) return null;
                if (!(number_before_period or pos > start_digit)) return null;
                if (pos >= buf.len) break;
            }
            if (isSpace(buf[pos])) {
                if (pos == start_pos) return null;
                pos += 1;
                while (pos < buf.len) : (pos += 1) if (!isSpace(buf[pos])) return null;
                break;
            }
            return null;
        }
        const digit: i32 = buf[pos] - '0';
        pos += 1;
        v = if (negative) v * 10 - digit else v * 10 + digit;
        if (v > std.math.maxInt(i16) or v < std.math.minInt(i16)) return null;
        if (pos != buf.len and buf[pos] == '_') {
            pos += 1;
            if (pos == buf.len or !isDigit(buf[pos])) return null;
        }
    }
    if (pos <= start_pos) return null;
    return @intCast(v);
}

/// `IntegerCastLoop` for a decimal integer with the decimal and exponent operations, non-strict.
fn decimalLoop(buf: []const u8, comptime negative: bool, s: *IntState) bool {
    var start_pos: usize = 0;
    if (negative) {
        start_pos = 1;
    } else if (buf[0] == '+') {
        start_pos = 1;
    }
    var pos = start_pos;
    while (pos < buf.len) {
        if (!isDigit(buf[pos])) {
            if (buf[pos] == '.') {
                const number_before_period = pos > start_pos;
                pos += 1;
                const start_digit = pos;
                while (pos < buf.len) {
                    if (!isDigit(buf[pos])) break;
                    handleDecimal(s, buf[pos] - '0');
                    pos += 1;
                    if (pos != buf.len and buf[pos] == '_') {
                        pos += 1;
                        if (pos == buf.len or !isDigit(buf[pos])) return false;
                    }
                }
                if (!(number_before_period or pos > start_digit)) return false;
                if (pos >= buf.len) break;
            }
            if (isSpace(buf[pos])) {
                if (pos == start_pos) return false;
                pos += 1;
                while (pos < buf.len) : (pos += 1) if (!isSpace(buf[pos])) return false;
                break;
            }
            if (buf[pos] == 'e' or buf[pos] == 'E') {
                if (pos == start_pos) return false;
                pos += 1;
                if (pos >= buf.len) return false;
                const exponent = parseExponent(buf[pos..]) orelse return false;
                return handleExponent(s, negative, exponent);
            }
            return false;
        }
        const digit = buf[pos] - '0';
        pos += 1;
        if (!handleDigit(s, negative, digit)) return false;
        if (pos != buf.len and buf[pos] == '_') {
            pos += 1;
            if (pos == buf.len or !isDigit(buf[pos])) return false;
        }
    }
    if (!finalize(s, negative)) return false;
    return pos > start_pos;
}

/// `IntegerHexCastLoop` / `IntegerBinaryCastLoop`: `buf` starts at the `x` or `b`.
fn radixLoop(buf: []const u8, comptime base: u8, s: *IntState) bool {
    var pos: usize = 1;
    while (pos < buf.len) {
        const digit: u8 = if (base == 16) (hexValue(buf[pos]) orelse return false) else switch (buf[pos]) {
            '0' => 0,
            '1' => 1,
            else => return false,
        };
        pos += 1;
        if (pos != buf.len and buf[pos] == '_') {
            pos += 1;
            if (pos == buf.len) return false;
            if (base == 16) {
                if (hexValue(buf[pos]) == null) return false;
            } else if (buf[pos] != '0' and buf[pos] != '1') return false;
        }
        if (s.result > @divTrunc(std.math.maxInt(i64) - @as(i64, digit), base)) return false;
        s.result = s.result * base + digit;
    }
    if (!finalize(s, false)) return false;
    return pos > 1;
}

/// DuckDB's non-strict text to BIGINT, null where the cast fails. A narrower integer type reads the same text and
/// takes the value if it fits (the casts to TINYINT, SMALLINT and INTEGER range-check the same BIGINT-wide parse).
pub fn parseInt(text: []const u8) ?i64 {
    var buf = text;
    while (buf.len > 0 and isSpace(buf[0])) buf = buf[1..];
    if (buf.len == 0) return null;
    var s: IntState = .{};
    if (buf[0] == '-') {
        if (!decimalLoop(buf, true, &s)) return null;
        return s.result;
    }
    if (buf.len > 1 and buf[0] == '0') {
        if (buf[1] == 'x' or buf[1] == 'X') {
            if (!radixLoop(buf[1..], 16, &s)) return null;
            return s.result;
        }
        if (buf[1] == 'b' or buf[1] == 'B') {
            if (!radixLoop(buf[1..], 2, &s)) return null;
            return s.result;
        }
    }
    if (!decimalLoop(buf, false, &s)) return null;
    return s.result;
}

/// Case-insensitive prefix test for the three-letter specials.
fn startsWithNoCase(buf: []const u8, word: []const u8) bool {
    if (buf.len < word.len) return false;
    for (word, buf[0..word.len]) |w, b| if (std.ascii.toLower(b) != w) return false;
    return true;
}

/// How far `fast_float` gets into `buf` (its `lastmatch`) and whether it read a number at all; the number goes into `out`
/// in a form `std.fmt.parseFloat` reads: `[-]int[.frac][e[-]exp]`. The grammar of `parse_number_string`, non-strict: a `-`
/// then digits with one `_` allowed between them, a `.` and more digits, an `e` with a signed exponent (an `e` that no digit
/// follows is left unread).
const Scan = struct { end: usize, ok: bool };

fn scanNumber(buf: []const u8, out: *std.ArrayList(u8), gpa: std.mem.Allocator) !Scan {
    const fail: Scan = .{ .end = 0, .ok = false };
    var p: usize = 0;
    if (buf[p] == '-') {
        try out.append(gpa, '-');
        p += 1;
        if (p == buf.len) return fail;
        if (!isDigit(buf[p]) and buf[p] != '.') return fail;
    }
    var int_digits: usize = 0;
    while (p < buf.len and isDigit(buf[p])) {
        try out.append(gpa, buf[p]);
        int_digits += 1;
        p += 1;
        if (p < buf.len and buf[p] == '_') {
            p += 1;
            if (p == buf.len or !isDigit(buf[p])) return fail;
        }
    }
    if (int_digits == 0) try out.append(gpa, '0');
    var frac_digits: usize = 0;
    if (p < buf.len and buf[p] == '.') {
        p += 1;
        var wrote_point = false;
        while (p < buf.len and isDigit(buf[p])) {
            if (!wrote_point) {
                try out.append(gpa, '.');
                wrote_point = true;
            }
            try out.append(gpa, buf[p]);
            frac_digits += 1;
            p += 1;
            if (p < buf.len and buf[p] == '_') {
                p += 1;
                if (p == buf.len or !isDigit(buf[p])) return fail;
            }
        }
    }
    if (int_digits + frac_digits == 0) return fail;
    if (p < buf.len and (buf[p] == 'e' or buf[p] == 'E')) {
        var q = p + 1;
        var negative = false;
        if (q < buf.len and (buf[q] == '-' or buf[q] == '+')) {
            negative = buf[q] == '-';
            q += 1;
        }
        if (q < buf.len and isDigit(buf[q])) {
            // The exponent stops growing at 0x10000, as fast_float's does: past that it is infinity or zero anyway.
            var exp_number: u32 = 0;
            while (q < buf.len and isDigit(buf[q])) {
                if (exp_number < 0x10000) exp_number = 10 * exp_number + (buf[q] - '0');
                q += 1;
                if (q < buf.len and buf[q] == '_') {
                    q += 1;
                    if (q == buf.len or !isDigit(buf[q])) return fail;
                }
            }
            try out.append(gpa, 'e');
            if (negative) try out.append(gpa, '-');
            var digits: [12]u8 = undefined;
            try out.appendSlice(gpa, std.fmt.bufPrint(&digits, "{d}", .{exp_number}) catch unreachable);
            p = q;
        }
    }
    return .{ .end = p, .ok = true };
}

/// DuckDB's non-strict text to DOUBLE, null where the cast fails. A number too large reads as infinity, one too small
/// as zero.
pub fn parseDouble(digits: *std.ArrayList(u8), gpa: std.mem.Allocator, text: []const u8) ?f64 {
    var buf = text;
    while (buf.len > 0 and isSpace(buf[0])) buf = buf[1..];
    if (buf.len == 0) return null;
    if (buf[0] == '+') buf = buf[1..];
    if (buf.len == 0) return null;
    digits.clearRetainingCapacity();
    const scan = scanNumber(buf, digits, gpa) catch return null;
    var end: usize = undefined;
    var value: f64 = undefined;
    if (scan.ok) {
        end = scan.end;
        value = std.fmt.parseFloat(f64, digits.items) catch return null;
    } else {
        // `parse_infnan`: an optional minus, then nan (with an optional `(name)`), inf or infinity.
        var rest = buf;
        var negative = false;
        if (rest[0] == '-') {
            negative = true;
            rest = rest[1..];
        }
        if (rest.len >= 3 and startsWithNoCase(rest, "nan")) {
            value = if (negative) -std.math.nan(f64) else std.math.nan(f64);
            end = buf.len - rest.len + 3;
            if (rest.len > 3 and rest[3] == '(') {
                var k: usize = 4;
                while (k < rest.len) : (k += 1) {
                    if (rest[k] == ')') {
                        end = buf.len - rest.len + k + 1;
                        break;
                    }
                    const ch = rest[k];
                    if (!(std.ascii.isAlphanumeric(ch) or ch == '_')) break;
                }
            }
        } else if (rest.len >= 3 and startsWithNoCase(rest, "inf")) {
            value = if (negative) -std.math.inf(f64) else std.math.inf(f64);
            end = buf.len - rest.len + (if (rest.len >= 8 and startsWithNoCase(rest[3..], "inity")) @as(usize, 8) else 3);
        } else return null;
    }
    while (end < buf.len and isSpace(buf[end])) end += 1;
    return if (end == buf.len) value else null;
}

test "text to integer is DuckDB 2.0.0's" {
    const T = testing;
    const cases = [_]struct { s: []const u8, want: ?i64 }{
        .{ .s = "12", .want = 12 },
        .{ .s = "-12", .want = -12 },
        .{ .s = "+12", .want = 12 },
        .{ .s = " 12 ", .want = 12 },
        .{ .s = "\t12\n", .want = 12 },
        .{ .s = "1 2", .want = null },
        .{ .s = "1.5", .want = 2 },
        .{ .s = "1.4", .want = 1 },
        .{ .s = "-1.5", .want = -2 },
        .{ .s = "2.5", .want = 3 },
        .{ .s = "1e3", .want = 1000 },
        .{ .s = "1E3", .want = 1000 },
        .{ .s = "0x1F", .want = 31 },
        .{ .s = "1_000", .want = 1000 },
        .{ .s = "", .want = null },
        .{ .s = "  ", .want = null },
        .{ .s = "+", .want = null },
        .{ .s = "-", .want = null },
        .{ .s = "007", .want = 7 },
        .{ .s = "-0", .want = 0 },
        .{ .s = "12abc", .want = null },
        .{ .s = "abc", .want = null },
        .{ .s = "1.", .want = 1 },
        .{ .s = "  .5", .want = 1 },
        .{ .s = ".5", .want = 1 },
        .{ .s = "1.9999", .want = 2 },
        .{ .s = "99999999999999999999", .want = null },
        .{ .s = "9223372036854775807", .want = 9223372036854775807 },
        .{ .s = "9223372036854775808", .want = null },
        .{ .s = "-9223372036854775808", .want = -9223372036854775808 },
        .{ .s = "1e2", .want = 100 },
        .{ .s = " +12 ", .want = 12 },
        .{ .s = "+-1", .want = null },
        .{ .s = "--1", .want = null },
        .{ .s = "1,000", .want = null },
        .{ .s = "6E3.", .want = 6000 },
        .{ .s = "3e9.", .want = 3000000000 },
        .{ .s = "6e3.5", .want = null },
    };
    for (cases) |c| try T.expectEqual(c.want, parseInt(c.s));
}

test "text to double is DuckDB 2.0.0's" {
    const T = testing;
    const a = T.allocator;
    var scratch: std.ArrayList(u8) = .empty;
    defer scratch.deinit(a);
    const cases = [_]struct { s: []const u8, want: ?f64 }{
        .{ .s = "1", .want = 1.0 },
        .{ .s = "-1.5", .want = -1.5 },
        .{ .s = "+1.5", .want = 1.5 },
        .{ .s = " 1.5 ", .want = 1.5 },
        .{ .s = "1e3", .want = 1000.0 },
        .{ .s = "1E-3", .want = 0.001 },
        .{ .s = "1e", .want = null },
        .{ .s = "e3", .want = null },
        .{ .s = ".5", .want = 0.5 },
        .{ .s = "5.", .want = 5.0 },
        .{ .s = "1_000", .want = 1000.0 },
        .{ .s = "1_0.5", .want = 10.5 },
        .{ .s = "1.5_5", .want = 1.55 },
        .{ .s = "inf", .want = std.math.inf(f64) },
        .{ .s = "-Infinity", .want = -std.math.inf(f64) },
        .{ .s = "0x10", .want = null },
        .{ .s = "1.5.5", .want = null },
        .{ .s = "", .want = null },
        .{ .s = "+", .want = null },
        .{ .s = ".", .want = null },
        .{ .s = "1e5e5", .want = null },
        .{ .s = "+-1", .want = -1.0 },
        .{ .s = "++1", .want = null },
        .{ .s = "1e309", .want = std.math.inf(f64) },
        .{ .s = "1e-400", .want = 0.0 },
        .{ .s = "1__0", .want = null },
        .{ .s = "_1", .want = null },
        .{ .s = "1_", .want = null },
        .{ .s = "in", .want = null },
        .{ .s = "nanx", .want = null },
    };
    for (cases) |c| {
        const got = parseDouble(&scratch, a, c.s);
        if (c.want) |w| {
            try T.expect(got != null);
            try T.expectEqual(w, got.?);
        } else try T.expectEqual(@as(?f64, null), got);
    }
    try T.expect(std.math.isNan(parseDouble(&scratch, a, "NaN").?));
    try T.expect(std.math.isNan(parseDouble(&scratch, a, "  nan  ").?));
    // DuckDB keeps the sign of a NaN it reads.
    try T.expect(std.math.signbit(parseDouble(&scratch, a, "-nan").?));
    try T.expect(!std.math.signbit(parseDouble(&scratch, a, "+nan").?));
    try T.expect(std.math.signbit(parseDouble(&scratch, a, "-0").?));
}
