//! CSV value types: an exact lattice and the field parsers behind it.
//!
//! The contract: a column gets a type only when every one of its values' text maps to that type exactly, so no
//! value is ever rounded, truncated, wrapped or reinterpreted. Anything else is STRING, or BYTE_ARRAY without the
//! STRING annotation when its bytes are not UTF-8. Concretely:
//!   - INT64: canonical integer text only (`-?(0|[1-9][0-9]*)`, in range). `007`, `+5`, ` 5` and `-0` are not
//!     integers, so writing the integer back reproduces the text.
//!   - DECIMAL(p,s): canonical decimal text, at most 38 digits; integers too wide for INT64 are DECIMAL(p,0). A
//!     value with a shorter fraction than the column's scale is padded (`1.1` in a scale-2 column is 1.10): the
//!     same number.
//!   - DOUBLE: exponent, `inf` or `nan` text, and only while every value has at most 15 significant digits (the
//!     most that survive text -> double -> text); the parse is correctly rounded. Overflow (`1e400`) is not a
//!     double.
//!   - DATE `YYYY-MM-DD`; TIMESTAMP `YYYY-MM-DD[ T]HH:MM:SS[.f{1,9}][Z|±HH:MM]`, microseconds unless a fraction
//!     needs nanoseconds, UTC-adjusted when offsets are given (all values or none); a date in a timestamp column
//!     is its midnight.
//!   - BOOLEAN `true`/`false`, any case.
//! NULL is decided before typing (an unquoted empty field); a quoted empty field is the empty string, a STRING.
//!
//! Types join upward (`Type.join`) and a column's type is the join of every value it holds. A reader samples, then
//! widens and restarts when a later value does not fit (`fits`). DECIMAL precision is not part of the join's
//! decision: the schema is in the footer, so the writer sets it from the data.

const std = @import("std");
const schema = @import("../schema.zig");

pub const Kind = enum(u8) { null_only, boolean, int64, decimal, float64, date, timestamp, string, binary };

pub const max_decimal_digits = 38;
/// Significant digits a DOUBLE carries from text back to the same text.
pub const max_double_digits = 15;

pub const Type = struct {
    kind: Kind = .null_only,
    /// DECIMAL: most integer digits seen (0 for `0.5`).
    int_digits: u8 = 0,
    /// DECIMAL: most fraction digits seen.
    scale: u8 = 0,
    /// Most significant digits of any numeric value: whether a DOUBLE still holds them all.
    sig: u8 = 0,
    /// TIMESTAMP: some fraction needs nanoseconds.
    nanos: bool = false,
    /// TIMESTAMP: values carry offsets and are instants.
    tz: bool = false,
    /// DATE or TIMESTAMP: some value lies outside what int64 nanoseconds since the epoch hold (1677-09-21 to
    /// 2262-04-11), so the column can never be TIMESTAMP_NS: one needing nanoseconds is STRING.
    out_of_ns: bool = false,

    pub fn precision(t: Type) u32 {
        return @max(1, @as(u32, t.int_digits) + t.scale);
    }

    /// DECIMAL stored as INT64 (to 18 digits) or as 16-byte FIXED_LEN_BYTE_ARRAY (to 38).
    pub fn decimalInt64(t: Type) bool {
        return t.precision() <= 18;
    }

    /// The least type holding every value of both.
    pub fn join(a: Type, b: Type) Type {
        if (a.kind == .null_only) return b;
        if (b.kind == .null_only) return a;
        var o = a;
        o.int_digits = @max(a.int_digits, b.int_digits);
        o.scale = @max(a.scale, b.scale);
        o.sig = @max(a.sig, b.sig);
        o.nanos = a.nanos or b.nanos;
        o.tz = a.tz or b.tz;
        o.out_of_ns = a.out_of_ns or b.out_of_ns;
        if (a.kind == b.kind) {
            if (a.kind == .timestamp and a.tz != b.tz) o.kind = .string;
            return o.bounded();
        }
        const lo: Kind = @fromBackingInt(@intCast(@min(@backingInt(a.kind), @backingInt(b.kind))));
        const hi: Kind = @fromBackingInt(@intCast(@max(@backingInt(a.kind), @backingInt(b.kind))));
        o.kind = switch (hi) {
            .binary => .binary,
            .decimal => if (lo == .int64) .decimal else .string,
            .float64 => if ((lo == .int64 or lo == .decimal) and o.sig <= max_double_digits) .float64 else .string,
            // A bare date has no offset to pair with an instant.
            .timestamp => if (lo == .date and !o.tz) .timestamp else .string,
            else => .string,
        };
        return o.bounded();
    }

    fn bounded(t: Type) Type {
        var o = t;
        // Nanoseconds for one value and a range past them for another: no timestamp unit holds both.
        if (o.kind == .timestamp and o.nanos and o.out_of_ns) o.kind = .string;
        if (o.kind == .decimal and o.precision() > max_decimal_digits)
            o.kind = if (o.sig <= max_double_digits) .float64 else .string;
        return o;
    }

    pub fn name(t: Type, buf: []u8) []const u8 {
        return switch (t.kind) {
            .decimal => std.fmt.bufPrint(buf, "DECIMAL({d},{d})", .{ t.precision(), t.scale }) catch "DECIMAL",
            .timestamp => if (t.tz)
                (if (t.nanos) "TIMESTAMP_NS WITH TIME ZONE" else "TIMESTAMP WITH TIME ZONE")
            else if (t.nanos) "TIMESTAMP_NS" else "TIMESTAMP",
            .int64 => "BIGINT",
            .float64 => "DOUBLE",
            .boolean => "BOOLEAN",
            .date => "DATE",
            .string, .null_only => "VARCHAR",
            .binary => "BLOB",
        };
    }
};

// ---------------------------------------------------------------- parsers

fn isDigit(c: u8) bool {
    return c >= '0' and c <= '9';
}

/// Canonical integer text in INT64 range.
pub fn parseInt(s: []const u8) ?i64 {
    var i: usize = 0;
    const neg = s.len > 0 and s[0] == '-';
    if (neg) i = 1;
    const n = s.len - i;
    if (n == 0 or n > 19) return null;
    if (s[i] == '0' and (n > 1 or neg)) return null;
    var v: u64 = 0;
    while (i < s.len) : (i += 1) {
        const d = s[i] -% '0';
        if (d > 9) return null;
        v = v * 10 + d; // 19 digits fit u64
    }
    if (neg) {
        if (v > @as(u64, 1) << 63) return null;
        return @intCast(-@as(i128, v));
    }
    return std.math.cast(i64, v);
}

pub const DecimalShape = struct { int_digits: u8, frac_digits: u8, sig: u8 };

/// Canonical decimal or integer text: `-?(0|[1-9][0-9]*)(\.[0-9]+)?`, at most 38 digits. No `-0`.
pub fn decimalShape(s: []const u8) ?DecimalShape {
    var i: usize = 0;
    const neg = s.len > 0 and s[0] == '-';
    if (neg) i = 1;
    const int_start = i;
    while (i < s.len and isDigit(s[i])) i += 1;
    const int_len = i - int_start;
    if (int_len == 0 or (int_len > 1 and s[int_start] == '0')) return null;
    var frac_len: usize = 0;
    if (i < s.len) {
        if (s[i] != '.') return null;
        i += 1;
        const frac_start = i;
        while (i < s.len and isDigit(s[i])) i += 1;
        frac_len = i - frac_start;
        if (frac_len == 0 or i != s.len) return null;
    }
    const zero_int = int_len == 1 and s[int_start] == '0';
    // Precision counts the digits of the unscaled value: `0.5` has one, not two.
    if (@intFromBool(!zero_int) * int_len + frac_len > max_decimal_digits) return null;
    // Significant digits: the digit string without its leading zeros.
    var lead: usize = 0;
    for (s[int_start..]) |c| {
        if (c == '.') continue;
        if (c != '0') break;
        lead += 1;
    }
    const digits = int_len + frac_len;
    // Negative zero is no decimal (the sign would be lost); it is the double -0.
    if (neg and lead == digits) return null;
    return .{
        .int_digits = @intCast(if (zero_int) 0 else int_len),
        .frac_digits = @intCast(frac_len),
        .sig = @intCast(@max(1, digits -| lead)),
    };
}

/// A DECIMAL value: canonical text with at most `max_int` integer digits and at most `scale` fraction digits,
/// unscaled at `scale`. Null for anything else (the column must widen).
pub fn parseDecimal(s: []const u8, scale: u8, max_int: u8) ?i128 {
    var i: usize = 0;
    const neg = s.len > 0 and s[0] == '-';
    if (neg) i = 1;
    const int_start = i;
    var v: u64 = 0;
    while (i < s.len) : (i += 1) {
        const d = s[i] -% '0';
        if (d > 9) break;
        v = v *% 10 +% d;
    }
    const int_len = i - int_start;
    if (int_len == 0 or (int_len > 1 and s[int_start] == '0')) return null;
    const zero_int = int_len == 1 and s[int_start] == '0';
    if ((if (zero_int) 0 else int_len) > max_int) return null;
    var frac: usize = 0;
    if (i < s.len) {
        if (s[i] != '.') return null;
        i += 1;
        const frac_start = i;
        while (i < s.len) : (i += 1) {
            const d = s[i] -% '0';
            if (d > 9) return null;
            v = v *% 10 +% d;
        }
        frac = i - frac_start;
        if (frac == 0 or frac > scale) return null;
    }
    var r: i128 = undefined;
    if (int_len + frac <= 18) {
        r = v;
    } else {
        if (@intFromBool(!zero_int) * int_len + frac > max_decimal_digits) return null;
        r = 0;
        for (s[int_start..]) |c| {
            if (c == '.') continue;
            r = std.math.add(i128, std.math.mul(i128, r, 10) catch return null, c - '0') catch return null;
        }
    }
    if (neg and r == 0) return null; // negative zero: a double, not a decimal
    // Padding to the scale is checked too: a column never asks for more than 38 digits, a caller might.
    var k = frac;
    while (k < scale) : (k += 1) r = std.math.mul(i128, r, 10) catch return null;
    if (@abs(r) >= std.math.pow(u128, 10, max_decimal_digits)) return null;
    return if (neg) -r else r;
}

/// Exponent, inf or nan text a DOUBLE holds exactly (significant digits <= 15, finite unless spelled inf/nan).
/// Returns the significant digit count.
pub fn floatShape(s: []const u8) ?u8 {
    // A quoted empty field reaches here as a value (it is the empty string, not NULL): no number.
    if (s.len == 0) return null;
    if (special(s) != null) return 1;
    var i: usize = 0;
    if (s.len > 0 and s[0] == '-') i = 1;
    var mant: usize = 0;
    var sig: usize = 0;
    var leading = true;
    var dot = false;
    while (i < s.len and (isDigit(s[i]) or (s[i] == '.' and !dot))) : (i += 1) {
        if (s[i] == '.') {
            dot = true;
            continue;
        }
        mant += 1;
        if (s[i] != '0') leading = false;
        if (!leading) sig += 1;
    }
    if (mant == 0 or i >= s.len or (s[i] != 'e' and s[i] != 'E')) {
        // `-0` and `-0.0` are no decimal (the sign would be lost) but are the double -0.
        if (i == s.len and s[0] == '-' and sig == 0 and mant > 0) return 1;
        return null;
    }
    i += 1;
    if (i < s.len and (s[i] == '-' or s[i] == '+')) i += 1;
    const exp_start = i;
    while (i < s.len and isDigit(s[i])) i += 1;
    if (i == exp_start or i != s.len or sig > max_double_digits) return null;
    const v = std.fmt.parseFloat(f64, s) catch return null;
    if (!std.math.isFinite(v)) return null;
    // Past the normal range fewer digits survive (a subnormal keeps as few as one), and underflow keeps none.
    if (!roundTrips(s, v)) return null;
    return @intCast(@max(sig, 1));
}

/// A number's decimal digits without leading or trailing zeros, and the power of ten of the first of them.
const Normal = struct {
    digits: [24]u8 = undefined,
    len: usize = 0,
    exp: i64 = 0,
    /// Too many digits to compare (more than any accepted text has).
    long: bool = false,

    fn of(text: []const u8) ?Normal {
        var n: Normal = .{};
        var i: usize = 0;
        if (i < text.len and (text[i] == '-' or text[i] == '+')) i += 1;
        var point: ?i64 = null;
        var seen: i64 = 0; // mantissa digits read, leading zeros included
        var first: ?i64 = null; // position of the first nonzero digit among them
        var trailing: usize = 0;
        while (i < text.len and text[i] != 'e' and text[i] != 'E') : (i += 1) {
            const c = text[i];
            if (c == '.') {
                point = seen;
                continue;
            }
            if (c < '0' or c > '9') return null;
            if (first == null and c == '0') {
                seen += 1;
                continue;
            }
            if (first == null) first = seen;
            seen += 1;
            if (n.len == n.digits.len) {
                n.long = true;
                continue;
            }
            n.digits[n.len] = c;
            n.len += 1;
            trailing = if (c == '0') trailing + 1 else 0;
        }
        var e: i64 = 0;
        if (i < text.len) e = std.fmt.parseInt(i64, text[i + 1 ..], 10) catch return null;
        const f = first orelse return n; // zero: no digits
        n.len -= trailing;
        // Checked: an exponent at i64's ends plus the digits' shift has no power of ten here; such a number does
        // not round-trip (it is zero or infinity as a double), so the caller takes it as text.
        n.exp = std.math.add(i64, e, (point orelse seen) - f - 1) catch return null;
        return n;
    }

    fn eql(a: Normal, b: Normal) bool {
        return !a.long and !b.long and a.exp == b.exp and std.mem.eql(u8, a.digits[0..a.len], b.digits[0..b.len]);
    }
};

/// Whether `v`, the double `text` parses to, still says what `text` says: its shortest round-trip form has the same
/// digits and power of ten. True for every normal double up to 15 significant digits; false for underflow to zero
/// and for a subnormal whose digits the double cannot keep.
fn roundTrips(text: []const u8, v: f64) bool {
    var buf: [40]u8 = undefined;
    const shortest = std.fmt.bufPrint(&buf, "{e}", .{@abs(v)}) catch return false;
    const want = Normal.of(text) orelse return false;
    const got = Normal.of(shortest) orelse return false;
    return want.eql(got);
}

fn special(s: []const u8) ?f64 {
    const eq = std.ascii.eqlIgnoreCase;
    if (eq(s, "nan")) return std.math.nan(f64);
    if (eq(s, "inf") or eq(s, "infinity")) return std.math.inf(f64);
    if (eq(s, "-inf") or eq(s, "-infinity")) return -std.math.inf(f64);
    return null;
}

/// A DOUBLE value: integer, decimal or exponent text with at most 15 significant digits, or inf/nan. Correctly
/// rounded: Clinger's fast path (a mantissa below 2^53 over an exact power of ten is one rounding), else the
/// standard library's Eisel-Lemire parse.
pub fn parseFloat(s: []const u8) ?f64 {
    if (special(s)) |v| return v;
    if (decimalShape(s)) |d| {
        if (d.sig > max_double_digits) return null;
        var i: usize = 0;
        const neg = s[0] == '-';
        if (neg) i = 1;
        var m: u64 = 0;
        var frac: usize = 0;
        var dot = false;
        while (i < s.len) : (i += 1) {
            if (s[i] == '.') {
                dot = true;
                continue;
            }
            m = m * 10 + (s[i] - '0');
            if (dot) frac += 1;
        }
        if (m < (1 << 53) and frac <= 22) {
            const v = @as(f64, @floatFromInt(m)) / pow10[frac];
            return if (neg) -v else v;
        }
        return std.fmt.parseFloat(f64, s) catch null;
    }
    if (floatShape(s) == null) return null;
    return std.fmt.parseFloat(f64, s) catch null;
}

const pow10 = blk: {
    var t: [23]f64 = undefined;
    var x: f64 = 1;
    for (&t) |*p| {
        p.* = x;
        x *= 10;
    }
    break :blk t;
};

fn two(s: []const u8, i: usize) ?u32 {
    const a = s[i] -% '0';
    const b = s[i + 1] -% '0';
    if (a > 9 or b > 9) return null;
    return @as(u32, a) * 10 + b;
}

/// Days since 1970-01-01 of a valid proleptic Gregorian `YYYY-MM-DD`.
pub fn parseDate(s: []const u8) ?i32 {
    if (s.len != 10 or s[4] != '-' or s[7] != '-') return null;
    const y: i32 = @intCast((two(s, 0) orelse return null) * 100 + (two(s, 2) orelse return null));
    const m = two(s, 5) orelse return null;
    const d = two(s, 8) orelse return null;
    if (m < 1 or m > 12 or d < 1) return null;
    const leap = (@mod(y, 4) == 0 and @mod(y, 100) != 0) or @mod(y, 400) == 0;
    const mdays = [_]u32{ 31, if (leap) 29 else 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31 };
    if (d > mdays[m - 1]) return null;
    // days_from_civil (Howard Hinnant).
    const yy = if (m <= 2) y - 1 else y;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const mp: i32 = @intCast((m + 9) % 12);
    const doy = @divFloor(153 * mp + 2, 5) + @as(i32, @intCast(d)) - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

pub const Instant = struct {
    /// Seconds since the epoch (UTC when `tz`).
    secs: i64,
    nanos: u32,
    /// The fraction has non-zero digits past microseconds.
    sub_micro: bool,
    tz: bool,

    pub fn micros(t: Instant) ?i64 {
        const s = std.math.mul(i64, t.secs, 1_000_000) catch return null;
        return std.math.add(i64, s, t.nanos / 1000) catch null;
    }

    /// In a wider intermediate, as the floor second of the earliest representable instant overflows alone.
    pub fn nanosSinceEpoch(t: Instant) ?i64 {
        return std.math.cast(i64, @as(i128, t.secs) * 1_000_000_000 + t.nanos);
    }
};

/// `YYYY-MM-DD[ T]HH:MM:SS[.f{1,9}][Z|±HH:MM]`.
pub fn parseTimestamp(s: []const u8) ?Instant {
    if (s.len < 19 or (s[10] != ' ' and s[10] != 'T') or s[13] != ':' or s[16] != ':') return null;
    const days = parseDate(s[0..10]) orelse return null;
    const hh = two(s, 11) orelse return null;
    const mi = two(s, 14) orelse return null;
    const ss = two(s, 17) orelse return null;
    if (hh > 23 or mi > 59 or ss > 59) return null;
    var i: usize = 19;
    var nanos: u32 = 0;
    var sub_micro = false;
    if (i < s.len and s[i] == '.') {
        i += 1;
        const start = i;
        var unit: u32 = 100_000_000;
        while (i < s.len and isDigit(s[i])) : (i += 1) {
            if (i - start >= 9) return null;
            nanos += (s[i] - '0') * unit;
            if (i - start >= 6 and s[i] != '0') sub_micro = true;
            unit /= 10;
        }
        if (i == start) return null;
    }
    var offset: i64 = 0;
    var tz = false;
    if (i < s.len) {
        if (s[i] == 'Z' and i + 1 == s.len) {
            tz = true;
        } else if ((s[i] == '+' or s[i] == '-') and i + 6 == s.len and s[i + 3] == ':') {
            const oh = two(s, i + 1) orelse return null;
            const om = two(s, i + 4) orelse return null;
            if (oh > 23 or om > 59) return null;
            offset = @as(i64, oh) * 3600 + @as(i64, om) * 60;
            if (s[i] == '-') offset = -offset;
            tz = true;
        } else return null;
    }
    const secs = @as(i64, days) * 86400 + @as(i64, hh) * 3600 + @as(i64, mi) * 60 + ss - offset;
    return .{ .secs = secs, .nanos = nanos, .sub_micro = sub_micro, .tz = tz };
}

/// Whether midnight of `days` is a nanosecond timestamp.
fn dateInNanos(days: i32) bool {
    const inst: Instant = .{ .secs = @as(i64, days) * 86400, .nanos = 0, .sub_micro = false, .tz = false };
    return inst.nanosSinceEpoch() != null;
}

pub fn parseBool(s: []const u8) ?bool {
    if (std.ascii.eqlIgnoreCase(s, "true")) return true;
    if (std.ascii.eqlIgnoreCase(s, "false")) return false;
    return null;
}

/// The most specific type holding this one non-NULL value.
pub fn classify(s: []const u8, utf8: bool) Type {
    if (!utf8) return .{ .kind = .binary };
    if (s.len == 0) return .{ .kind = .string };
    const c = s[0];
    if (isDigit(c) or c == '-') {
        if (parseInt(s)) |_| {
            const digits: u8 = @intCast(s.len - @intFromBool(c == '-'));
            return .{ .kind = .int64, .int_digits = digits, .sig = digits };
        }
        if (decimalShape(s)) |d|
            return .{ .kind = .decimal, .int_digits = d.int_digits, .scale = d.frac_digits, .sig = d.sig };
        if (floatShape(s)) |sig| return .{ .kind = .float64, .sig = sig };
        if (parseDate(s)) |days| return .{ .kind = .date, .out_of_ns = !dateInNanos(days) };
        if (parseTimestamp(s)) |t| {
            const in_ns = t.nanosSinceEpoch() != null;
            // Its own digits need nanoseconds it cannot have: no timestamp type holds it.
            if (t.sub_micro and !in_ns) return .{ .kind = .string };
            return .{ .kind = .timestamp, .nanos = t.sub_micro, .tz = t.tz, .out_of_ns = !in_ns };
        }
        return .{ .kind = .string };
    }
    if (floatShape(s)) |sig| return .{ .kind = .float64, .sig = sig };
    if (parseBool(s)) |_| return .{ .kind = .boolean };
    return .{ .kind = .string };
}

/// Whether `s` is a value of `t` (the converters accept exactly this).
pub fn fits(t: Type, s: []const u8, utf8: bool) bool {
    return switch (t.kind) {
        .null_only => false,
        .boolean => parseBool(s) != null,
        .int64 => parseInt(s) != null,
        .decimal => parseDecimal(s, t.scale, decimalIntCapacity(t)) != null,
        .float64 => parseFloat(s) != null,
        .date => parseDate(s) != null,
        .timestamp => timestampValue(t, s) != null,
        .string => utf8,
        .binary => true,
    };
}

/// The integer digits a DECIMAL column's physical type holds at its scale: the precision is set from the data at
/// the end, so a wider value is no reason to restart while it still fits.
pub fn decimalIntCapacity(t: Type) u8 {
    return @intCast((if (t.decimalInt64()) @as(u32, 18) else max_decimal_digits) - t.scale);
}

/// A TIMESTAMP column's value in its unit (microseconds, or nanoseconds when `t.nanos`), or null when the text is
/// not a timestamp of this column (offset or no offset, sub-microsecond digits, range).
pub fn timestampValue(t: Type, s: []const u8) ?i64 {
    if (parseTimestamp(s)) |ts| {
        if (ts.tz != t.tz or (ts.sub_micro and !t.nanos)) return null;
        return if (t.nanos) ts.nanosSinceEpoch() else ts.micros();
    }
    if (t.tz) return null;
    const days = parseDate(s) orelse return null;
    const inst: Instant = .{ .secs = @as(i64, days) * 86400, .nanos = 0, .sub_micro = false, .tz = false };
    return if (t.nanos) inst.nanosSinceEpoch() else inst.micros();
}

/// The type a `--csv-types` spec names: BIGINT, DOUBLE, DECIMAL(p,s), DATE, TIMESTAMP, TIMESTAMP_NS, TIMESTAMPTZ,
/// TIMESTAMPTZ_NS, BOOLEAN, VARCHAR or BLOB (and the usual aliases), any case. Null for anything else.
pub fn parseName(s: []const u8) ?Type {
    const eq = std.ascii.eqlIgnoreCase;
    if (eq(s, "BIGINT") or eq(s, "INT64") or eq(s, "INT8") or eq(s, "LONG")) return .{ .kind = .int64, .int_digits = 19 };
    if (eq(s, "DOUBLE") or eq(s, "FLOAT8") or eq(s, "FLOAT64")) return .{ .kind = .float64, .sig = 1 };
    if (eq(s, "DATE")) return .{ .kind = .date };
    if (eq(s, "TIMESTAMP")) return .{ .kind = .timestamp };
    if (eq(s, "TIMESTAMP_NS")) return .{ .kind = .timestamp, .nanos = true };
    if (eq(s, "TIMESTAMPTZ") or eq(s, "TIMESTAMP WITH TIME ZONE")) return .{ .kind = .timestamp, .tz = true };
    if (eq(s, "TIMESTAMPTZ_NS")) return .{ .kind = .timestamp, .tz = true, .nanos = true };
    if (eq(s, "BOOLEAN") or eq(s, "BOOL")) return .{ .kind = .boolean };
    if (eq(s, "VARCHAR") or eq(s, "STRING") or eq(s, "TEXT")) return .{ .kind = .string };
    if (eq(s, "BLOB") or eq(s, "BINARY") or eq(s, "BYTES")) return .{ .kind = .binary };
    if (s.len > 9 and eq(s[0..8], "DECIMAL(") and s[s.len - 1] == ')') {
        var it = std.mem.splitScalar(u8, s[8 .. s.len - 1], ',');
        const p = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse return null, " "), 10) catch return null;
        const sc = std.fmt.parseInt(u8, std.mem.trim(u8, it.next() orelse return null, " "), 10) catch return null;
        if (it.next() != null or p == 0 or p > max_decimal_digits or sc > p) return null;
        return .{ .kind = .decimal, .int_digits = p - sc, .scale = sc };
    }
    return null;
}

// ---------------------------------------------------------------- header and names

/// Whether the first record is a header, given the types sampled from the records after it: yes unless every one
/// of its fields fits its column's sampled type and some column is not text (an all-text file keeps a header).
pub fn firstRecordIsHeader(first: []const []const u8, sampled: []const Type) bool {
    var all_fit = true;
    var any_typed = false;
    for (first, sampled) |s, t| {
        if (t.kind != .string and t.kind != .binary and t.kind != .null_only) any_typed = true;
        if (s.len == 0) continue;
        if (t.join(classify(s, std.unicode.utf8ValidateSlice(s))).kind != t.kind) all_fit = false;
    }
    return !(all_fit and any_typed);
}

/// Column names from a header's fields: an empty name is `columnN` (N from 0); a repeated name gets `_1`, `_2`, ...
/// until unique. Nothing is dropped.
pub fn columnNames(a: std.mem.Allocator, header: ?[]const []const u8, ncols: usize) ![]const []const u8 {
    const names = try a.alloc([]const u8, ncols);
    for (names, 0..) |*nm, i| {
        const base = if (header) |h| h[i] else "";
        var candidate = if (base.len == 0) try std.fmt.allocPrint(a, "column{d}", .{i}) else base;
        var suffix: usize = 1;
        while (contains(names[0..i], candidate)) : (suffix += 1)
            candidate = try std.fmt.allocPrint(a, "{s}_{d}", .{ if (base.len == 0) candidate else base, suffix });
        nm.* = candidate;
    }
    return names;
}

fn contains(list: []const []const u8, s: []const u8) bool {
    for (list) |x| if (std.mem.eql(u8, x, s)) return true;
    return false;
}

// ---------------------------------------------------------------- Parquet

/// The Parquet leaf for a column of type `t`. Every CSV column is OPTIONAL (any field may be empty). A DECIMAL's
/// precision here is its physical capacity (18 or 38); the writer sets the data's precision in the footer.
pub fn schemaElement(name: []const u8, t: Type) schema.SchemaElement {
    var e: schema.SchemaElement = .{
        .name = name,
        .type = .BYTE_ARRAY,
        .repetition_type = .OPTIONAL,
        .type_length = null,
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    switch (t.kind) {
        .int64 => e.type = .INT64,
        .decimal => {
            const p: i32 = if (t.decimalInt64()) 18 else max_decimal_digits;
            e.type = if (t.decimalInt64()) .INT64 else .FIXED_LEN_BYTE_ARRAY;
            if (!t.decimalInt64()) e.type_length = 16;
            e.scale = t.scale;
            e.precision = p;
            e.converted_type = .DECIMAL;
            e.logical_type = .{ .DECIMAL = .{ .scale = t.scale, .precision = p } };
        },
        .float64 => e.type = .DOUBLE,
        .date => {
            e.type = .INT32;
            e.converted_type = .DATE;
            e.logical_type = .{ .DATE = .{} };
        },
        .timestamp => {
            e.type = .INT64;
            e.logical_type = .{ .TIMESTAMP = .{
                .isAdjustedToUTC = t.tz,
                .unit = if (t.nanos) .{ .NANOS = .{} } else .{ .MICROS = .{} },
            } };
            // Parquet's compatibility table annotates microseconds with the legacy TIMESTAMP_MICROS, local or UTC alike
            // (readers of only the legacy annotation read local timestamps that way); nanoseconds have none.
            if (!t.nanos) e.converted_type = .TIMESTAMP_MICROS;
        },
        .boolean => e.type = .BOOLEAN,
        .string, .null_only => {
            e.converted_type = .UTF8;
            e.logical_type = .{ .STRING = .{} };
        },
        .binary => {},
    }
    return e;
}

/// The data's DECIMAL precision: the digit count of the largest unscaled magnitude, at least the scale and 1.
pub fn decimalPrecision(t: Type, max_abs: u128) u8 {
    var digits: u8 = 1;
    var x = max_abs;
    while (x >= 10) : (x /= 10) digits += 1;
    return @max(digits, t.scale, 1);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "csv integers are canonical text only" {
    try testing.expectEqual(@as(?i64, 0), parseInt("0"));
    try testing.expectEqual(@as(?i64, -42), parseInt("-42"));
    try testing.expectEqual(@as(?i64, std.math.maxInt(i64)), parseInt("9223372036854775807"));
    try testing.expectEqual(@as(?i64, std.math.minInt(i64)), parseInt("-9223372036854775808"));
    for ([_][]const u8{ "", "-", "007", "+5", " 5", "5 ", "-0", "9223372036854775808", "1e3", "1.0", "0x10" }) |s|
        try testing.expectEqual(@as(?i64, null), parseInt(s));
}

test "csv decimals are exact at the column's scale; anything else widens" {
    try testing.expectEqual(@as(?i128, 1710), parseDecimal("17.1", 2, 18));
    try testing.expectEqual(@as(?i128, -5), parseDecimal("-0.05", 2, 18));
    try testing.expectEqual(@as(?i128, 1700), parseDecimal("17", 2, 18));
    try testing.expectEqual(@as(?i128, 1234567890123456789), parseDecimal("12345678901234567.89", 2, 36));
    try testing.expectEqual(@as(?i128, null), parseDecimal("1.234", 2, 18)); // longer fraction
    try testing.expectEqual(@as(?i128, null), parseDecimal("123.4", 2, 2)); // more integer digits than fit
    try testing.expectEqual(@as(?i128, null), parseDecimal("07.5", 2, 18));
    try testing.expectEqual(@as(?i128, null), parseDecimal("-0", 2, 18));
    try testing.expectEqual(@as(?i128, null), parseDecimal("1.", 2, 18));
    try testing.expectEqual(@as(?i128, null), parseDecimal(".5", 2, 18));
    const t = classify("99999999999999999999", true);
    try testing.expectEqual(Kind.decimal, t.kind);
    try testing.expectEqual(@as(u32, 20), t.precision());
}

test "csv doubles: only text a double round-trips, correctly rounded" {
    try testing.expectEqual(@as(?f64, 1e5), parseFloat("1e5"));
    try testing.expectEqual(@as(?f64, 0.1), parseFloat("0.1"));
    try testing.expectEqual(@as(?f64, 4810.18), parseFloat("4810.18"));
    try testing.expect(std.math.isNegativeZero(parseFloat("-0").?));
    try testing.expect(std.math.isNan(parseFloat("NaN").?));
    try testing.expectEqual(@as(?f64, null), parseFloat("1e400"));
    try testing.expectEqual(@as(?f64, null), parseFloat("12345678901234567.89"));
    try testing.expectEqual(Kind.string, classify("1e400", true).kind);
    // The fast path agrees with the full parse on every short decimal.
    var prng = std.Random.DefaultPrng.init(7);
    var buf: [32]u8 = undefined;
    for (0..20000) |_| {
        const r = prng.random();
        const ip = r.uintLessThan(u64, 1_000_000_000);
        const s = std.fmt.bufPrint(&buf, "{d}.{d}", .{ ip, r.uintLessThan(u32, 100_000) }) catch unreachable;
        if (decimalShape(s) == null) continue;
        try testing.expectEqual(std.fmt.parseFloat(f64, s) catch unreachable, parseFloat(s).?);
    }
}

test "csv review P1-1: a late quoted-empty value does not fit a DOUBLE column; it widens to STRING" {
    const double: Type = .{ .kind = .float64, .sig = 1 };
    try testing.expect(!fits(double, "", true));
    try testing.expectEqual(@as(?f64, null), parseFloat(""));
    try testing.expectEqual(@as(?u8, null), floatShape(""));
    try testing.expectEqual(Kind.string, classify("1e5", true).join(classify("", true)).kind);
}

test "csv review P1-3: a DOUBLE holds its text's number, or the column is STRING" {
    // Underflow and a subnormal that cannot keep the text's digits are no doubles.
    for ([_][]const u8{ "1e-400", "123456789012345e-334", "4.9e-324", "2.225073858507201e-308", "1e309" }) |s| {
        try testing.expectEqual(Kind.string, classify(s, true).kind);
        try testing.expect(!fits(.{ .kind = .float64, .sig = 1 }, s, true));
    }
    // A subnormal or extreme normal that round-trips stays a double, with its exact value.
    for ([_][]const u8{ "5e-324", "1e-320", "2.2250738585072e-308", "1e308", "1.79769313486231e308", "-1e-310" }) |s| {
        try testing.expectEqual(Kind.float64, classify(s, true).kind);
        try testing.expectEqual(std.fmt.parseFloat(f64, s) catch unreachable, parseFloat(s).?);
    }
    try testing.expectEqual(Kind.float64, classify("0e0", true).kind);
    try testing.expectEqual(Kind.float64, classify("0.000e-5", true).kind);
}

test "csv review P2-2 and P2-3: DECIMAL(38,38) is a decimal; microsecond timestamps carry TIMESTAMP_MICROS" {
    const s = "0.12345678901234567890123456789012345678";
    const t = classify(s, true);
    try testing.expectEqual(Kind.decimal, t.kind);
    try testing.expectEqual(@as(u32, 38), t.precision());
    const unscaled: i128 = 12345678901234567890123456789012345678;
    try testing.expectEqual(@as(?i128, unscaled), parseDecimal(s, 38, decimalIntCapacity(t)));
    const micros: ?schema.ConvertedType = .TIMESTAMP_MICROS;
    try testing.expectEqual(micros, schemaElement("t", .{ .kind = .timestamp }).converted_type);
    try testing.expectEqual(micros, schemaElement("t", .{ .kind = .timestamp, .tz = true }).converted_type);
    const nanos = schemaElement("t", .{ .kind = .timestamp, .nanos = true });
    try testing.expectEqual(@as(?schema.ConvertedType, null), nanos.converted_type);
}

test "csv review P1-4: an exponent at the ends of i64 is text, never an overflow" {
    const double: Type = .{ .kind = .float64, .sig = 1 };
    for ([_][]const u8{
        "0.1e-9223372036854775808",
        "0.0001e-9223372036854775805",
        "1e-9223372036854775808",
        "1e-9223372036854775809",
        "1e9223372036854775807",
        "1e+9223372036854775807",
        "10e9223372036854775807",
        "1e99999999999999999999",
    }) |s| {
        try testing.expectEqual(Kind.string, classify(s, true).kind);
        try testing.expectEqual(@as(?f64, null), parseFloat(s));
        try testing.expect(!fits(double, s, true));
    }
}

fn fuzzValue(_: void, smith: *std.testing.Smith) anyerror!void {
    var buf: [64]u8 = undefined;
    const s = buf[0..smith.slice(&buf)];
    const t = classify(s, std.unicode.utf8ValidateSlice(s));
    // Whatever a value is classified as, it is a value of that type; and no text makes a parser trap.
    if (!fits(t, s, std.unicode.utf8ValidateSlice(s))) return error.ClassifiedValueDoesNotFit;
    _ = parseFloat(s);
    _ = parseDecimal(s, 38, 38);
    _ = parseTimestamp(s);
    _ = parseInt(s);
}

test "csv review P1-4: a decimal parse past 38 digits is refused, never an overflow" {
    const forty = "1234567890123456789012345678901234567890";
    try testing.expectEqual(@as(?i128, null), parseDecimal(forty, 0, 40));
    try testing.expectEqual(@as(?i128, null), parseDecimal("123456789012345678901234567890", 38, 38));
    try testing.expectEqual(@as(?i128, null), parseDecimal("1.5", 38, 38));
    try testing.expect(parseDecimal("0.5", 37, 1) != null);
}

test "fuzz: csv value text classifies to a type it fits, and no parser traps" {
    try std.testing.fuzz({}, fuzzValue, .{ .corpus = &.{
        "0.1e-9223372036854775808",
        "1e9223372036854775807",
        "-0.000e-9223372036854775807",
        "9.99999999999999e308",
        "4.9e-324",
        "12345678901234567890123456789012345678",
        "-0.5",
        "9999-12-31 23:59:59.999999999+23:59",
        "0001-01-01T00:00:00-23:59",
        "nan",
    } });
}

test "csv dates and timestamps: valid calendar, exact fractions, offsets as instants" {
    try testing.expectEqual(@as(?i32, 0), parseDate("1970-01-01"));
    try testing.expectEqual(@as(?i32, 19723), parseDate("2024-01-01"));
    try testing.expectEqual(@as(?i32, -25567), parseDate("1900-01-01"));
    try testing.expectEqual(@as(?i32, null), parseDate("2024-02-30"));
    try testing.expectEqual(@as(?i32, null), parseDate("2023-02-29"));
    try testing.expect(parseDate("2024-02-29") != null);
    try testing.expectEqual(@as(?i32, null), parseDate("01/02/2024"));
    const t = parseTimestamp("2024-12-31T23:59:59+02:00").?;
    try testing.expect(t.tz);
    try testing.expectEqual(@as(i64, 1735682399), t.secs);
    const ns = parseTimestamp("2024-12-31 23:59:59.123456789").?;
    try testing.expect(ns.sub_micro and !ns.tz);
    const local: Type = .{ .kind = .timestamp };
    try testing.expectEqual(@as(?i64, null), timestampValue(local, "2024-12-31 23:59:59.123456789"));
    const ns_col: Type = .{ .kind = .timestamp, .nanos = true };
    try testing.expectEqual(@as(?i64, 1735689599123456789), timestampValue(ns_col, "2024-12-31 23:59:59.123456789"));
    // Microseconds reach past 2262, where nanoseconds stop.
    try testing.expect(timestampValue(local, "9999-12-31 23:59:59") != null);
    try testing.expectEqual(@as(?i64, null), timestampValue(ns_col, "9999-12-31 23:59:59"));
    try testing.expectEqual(@as(?i64, 1704067200000000), timestampValue(local, "2024-01-01"));
}

test "csv review P1-2: a timestamp type holds every value it was built from, in either order" {
    // Its own value: nanoseconds past 2262 are no timestamp.
    const own = "9999-12-31 23:59:59.000000001";
    try testing.expectEqual(Kind.string, classify(own, true).kind);
    try testing.expect(fits(classify(own, true), own, true));
    // Microseconds in 9999 and nanoseconds in 2024: no unit holds both, so STRING, whichever comes first.
    const old = "9999-12-31 23:59:59";
    const next = "2024-01-01 00:00:00.000000001";
    for ([2][2][]const u8{ .{ old, next }, .{ next, old } }) |pair| {
        const joined = classify(pair[0], true).join(classify(pair[1], true));
        try testing.expectEqual(Kind.string, joined.kind);
        try testing.expect(fits(joined, pair[0], true) and fits(joined, pair[1], true));
    }
    // A date past the range promotes a nanosecond column to STRING too; inside it, to TIMESTAMP_NS.
    try testing.expectEqual(Kind.string, classify("0001-01-01", true).join(classify(next, true)).kind);
    try testing.expectEqual(Kind.timestamp, classify("2000-01-01", true).join(classify(next, true)).kind);
    // An offset normalizes to UTC before the range is judged: 2262-04-11 23:47:16 at +01:00 is in range.
    try testing.expect(classify("2262-04-12T00:47:16.854775807+01:00", true).kind == .timestamp);
    try testing.expect(classify("2262-04-11T23:47:16.854775807-00:01", true).kind == .string);
}

test "csv review P2-1: the earliest nanosecond timestamp is representable" {
    const ns_col: Type = .{ .kind = .timestamp, .nanos = true };
    try testing.expectEqual(@as(?i64, std.math.minInt(i64)), timestampValue(ns_col, "1677-09-21 00:12:43.145224192"));
    try testing.expectEqual(@as(?i64, std.math.maxInt(i64)), timestampValue(ns_col, "2262-04-11 23:47:16.854775807"));
    try testing.expectEqual(@as(?i64, null), timestampValue(ns_col, "2262-04-11 23:47:16.854775808"));
    try testing.expectEqual(@as(?i64, null), timestampValue(ns_col, "1677-09-21 00:12:43.145224191"));
}

test "csv property: temporal joins in any order hold every value" {
    const pool = [_][]const u8{
        "0001-01-01",
        "9999-12-31",
        "1677-09-21",
        "2262-04-11",
        "2024-02-29",
        "9999-12-31 23:59:59",
        "0001-01-01T00:00:00",
        "2024-01-01 00:00:00.000000001",
        "2024-01-01 00:00:00.123456",
        "1677-09-21 00:12:43.145224192",
        "1677-09-21 00:12:43.145224191",
        "2262-04-11 23:47:16.854775807",
        "2262-04-11 23:47:16.854775808",
        "9999-12-31T23:59:59Z",
        "2024-01-01T00:00:00.000000001Z",
        "2262-04-12T00:47:16.854775807+01:00",
        "1677-09-20T23:12:43.145224192-01:00",
    };
    var prng = std.Random.DefaultPrng.init(0x7173);
    const r = prng.random();
    for (0..20_000) |_| {
        var picked: [4][]const u8 = undefined;
        var t: Type = .{};
        for (&picked) |*p| {
            p.* = pool[r.uintLessThan(usize, pool.len)];
            t = t.join(classify(p.*, true));
        }
        for (picked) |p| if (!fits(t, p, true)) {
            std.debug.print("{s} does not fit {s} built from {any}\n", .{ p, @tagName(t.kind), picked });
            return error.TestUnexpectedResult;
        };
    }
}

test "csv the lattice joins to the least exact type" {
    const c = struct {
        fn k(s: []const u8) Type {
            return classify(s, std.unicode.utf8ValidateSlice(s));
        }
    }.k;
    try testing.expectEqual(Kind.decimal, c("3").join(c("3.5")).kind);
    try testing.expectEqual(@as(u8, 1), c("3").join(c("3.5")).scale);
    try testing.expectEqual(Kind.string, c("02134").kind);
    try testing.expectEqual(Kind.string, c("3").join(c("abc")).kind);
    try testing.expectEqual(Kind.string, c("2024-01-01").join(c("2024-02-30")).kind);
    try testing.expectEqual(Kind.timestamp, c("2024-01-01").join(c("2024-01-01 10:00:00")).kind);
    try testing.expectEqual(Kind.string, c("2024-01-01").join(c("2024-01-01T10:00:00Z")).kind);
    try testing.expectEqual(Kind.string, c("2024-01-01 10:00:00").join(c("2024-01-01T10:00:00Z")).kind);
    try testing.expectEqual(Kind.float64, c("1.5").join(c("1e5")).kind);
    try testing.expectEqual(Kind.string, c("12345678901234567.89").join(c("1e5")).kind);
    try testing.expectEqual(Kind.string, c("true").join(c("1")).kind);
    try testing.expectEqual(Kind.binary, c("x").join(classify("caf\xe9", false)).kind);
    try testing.expectEqual(Kind.string, c(" 42 ").kind);
    try testing.expectEqual(Kind.int64, (Type{}).join(c("5")).kind);
}

test "csv every value a joined type was built from fits it" {
    var prng = std.Random.DefaultPrng.init(42);
    const r = prng.random();
    const pool = [_][]const u8{
        "0",
        "-1",
        "17",
        "007",
        "3.5",
        "-0.25",
        "1e5",
        "NaN",
        "inf",
        "1e400",
        "true",
        "False",
        "",
        "-0",
        "abc",
        "12345678901234567.89",
        "2024-01-01",
        "1.10",
        "2024-02-30",
        "99999999999999999999",
        "2024-01-01 10:00:00",
        "9223372036854775807",
        "2024-01-01T10:00:00.123456789",
        "2024-01-01T10:00:00+02:00",
    };
    for (0..5000) |_| {
        var picked: [6][]const u8 = undefined;
        var t: Type = .{};
        for (&picked) |*p| {
            p.* = pool[r.uintLessThan(usize, pool.len)];
            t = t.join(classify(p.*, true));
        }
        for (picked) |p| if (!fits(t, p, true)) {
            std.debug.print("{s} does not fit {s} built from {any}\n", .{ p, @tagName(t.kind), picked });
            return error.TestUnexpectedResult;
        };
    }
}

test "csv --csv-types names" {
    try testing.expectEqual(Kind.int64, parseName("bigint").?.kind);
    const d = parseName("DECIMAL(15, 2)").?;
    try testing.expectEqual(Kind.decimal, d.kind);
    try testing.expectEqual(@as(u32, 15), d.precision());
    try testing.expect(parseName("timestamptz").?.tz);
    try testing.expectEqual(@as(?Type, null), parseName("DECIMAL(40,2)"));
    try testing.expectEqual(@as(?Type, null), parseName("INT128"));
}

test "csv header detection and column names" {
    const ints: [2]Type = .{ .{ .kind = .int64 }, .{ .kind = .int64 } };
    try testing.expect(!firstRecordIsHeader(&.{ "1", "2" }, &ints));
    try testing.expect(firstRecordIsHeader(&.{ "a", "b" }, &ints));
    const texts: [1]Type = .{.{ .kind = .string }};
    try testing.expect(firstRecordIsHeader(&.{"name"}, &texts));
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const names = try columnNames(arena.allocator(), &.{ "a", "a", "", "b", "a_1" }, 5);
    try testing.expectEqualStrings("a", names[0]);
    try testing.expectEqualStrings("a_1", names[1]);
    try testing.expectEqualStrings("column2", names[2]);
    try testing.expectEqualStrings("b", names[3]);
    try testing.expectEqualStrings("a_1_1", names[4]);
}

test "csv property: zpq's typed reading agrees with DuckDB 2.0's on every value both accept" {
    // Generated by tools/csv_type_oracle.py: text, the type zpq chooses for it alone, and DuckDB's reading of the
    // text as that type. zpq is stricter (it refuses what DuckDB rounds); where it accepts, the values must match.
    const oracle = @embedFile("testdata/duckdb_types.tsv");
    var lines = std.mem.splitScalar(u8, oracle, '\n');
    var checked: usize = 0;
    while (lines.next()) |line| {
        if (line.len == 0 or line[0] == '#') continue;
        var cols = std.mem.splitScalar(u8, line, '\t');
        const text = unescapeTsv(cols.next().?);
        const kind = cols.next().?;
        const duck = cols.next().?;
        var buf: [64]u8 = undefined;
        const t = classify(text, true);
        try testing.expectEqualStrings(kind, t.name(&buf));
        const ok = switch (t.kind) {
            .int64 => std.mem.eql(u8, duck, try std.fmt.bufPrint(&buf, "{d}", .{parseInt(text).?})),
            // DuckDB drops the integer zero (`-.5`).
            .decimal => blk: {
                const v = parseDecimal(text, t.scale, decimalIntCapacity(t)).?;
                break :blk std.mem.eql(u8, duck, try decimalText(&buf, v, t.scale)) or
                    std.mem.eql(u8, duck, try dropIntegerZero(&buf, v, t.scale));
            },
            .float64 => blk: {
                const got = parseFloat(text).?;
                const want = std.fmt.parseFloat(f64, duck) catch
                    break :blk std.ascii.eqlIgnoreCase(duck, "nan") and std.math.isNan(got);
                const same_bits = @as(u64, @bitCast(want)) == @as(u64, @bitCast(got));
                break :blk same_bits or (std.math.isNan(want) and std.math.isNan(got));
            },
            .date => std.mem.eql(u8, duck, try std.fmt.bufPrint(&buf, "{d}", .{parseDate(text).?})),
            .timestamp => std.mem.eql(u8, duck, try std.fmt.bufPrint(&buf, "{d}", .{timestampValue(t, text).?})),
            .boolean => std.mem.eql(u8, duck, if (parseBool(text).?) "true" else "false"),
            else => true,
        };
        if (!ok) {
            std.debug.print("{s} as {s}: duckdb {s}\n", .{ text, kind, duck });
            return error.TestUnexpectedResult;
        }
        checked += 1;
    }
    try testing.expect(checked > 1000);
}

fn unescapeTsv(s: []const u8) []const u8 {
    return if (std.mem.eql(u8, s, "\\e")) "" else s;
}

fn dropIntegerZero(buf: []u8, v: i128, scale: u8) ![]const u8 {
    const t = try decimalText(buf, v, scale);
    const at = std.mem.indexOf(u8, t, "0.") orelse return t;
    if (at > 1 or (at == 1 and t[0] != '-')) return t;
    std.mem.copyForwards(u8, buf[at..], t[at + 1 ..]);
    return buf[0 .. t.len - 1];
}

fn decimalText(buf: []u8, v: i128, scale: u8) ![]const u8 {
    const neg = v < 0;
    const mag: u128 = @abs(v);
    var digits: [48]u8 = undefined;
    const raw = try std.fmt.bufPrint(&digits, "{d}", .{mag});
    var n: usize = 0;
    if (neg) {
        buf[0] = '-';
        n = 1;
    }
    if (raw.len <= scale) {
        buf[n] = '0';
        n += 1;
        if (scale > 0) {
            buf[n] = '.';
            n += 1;
            for (0..scale - raw.len) |_| {
                buf[n] = '0';
                n += 1;
            }
            @memcpy(buf[n..][0..raw.len], raw);
            n += raw.len;
        }
    } else {
        const int_len = raw.len - scale;
        @memcpy(buf[n..][0..int_len], raw[0..int_len]);
        n += int_len;
        if (scale > 0) {
            buf[n] = '.';
            n += 1;
            @memcpy(buf[n..][0..scale], raw[int_len..]);
            n += scale;
        }
    }
    return buf[0..n];
}
