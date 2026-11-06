//! Literal spellings liteparser's grammar lacks, folded before it parses: `DATE '..'` and `TIMESTAMP '..'`, `date +/-
//! INTERVAL '..'` on constants, and `x::TYPE`. Every one becomes a form the grammar has (`CAST('..' AS date)`), so the
//! lowering sees a typed constant and the filter binder compares it with the column's own kind: the fold costs nothing
//! at run time and row-group pruning sees the same literal an untyped one would.
//!
//! The rest of the constant folding (CAST of a literal, `0.06 - 0.01`) works on liteparser's tree, in `sql_lower.zig`,
//! with the exact decimal arithmetic here.
const std = @import("std");
const filters = @import("../filter/parser.zig");

pub const Error = error{ BadValue, BadInterval, IntervalNotSupported, DistinctNotSupported, OutOfMemory };

/// The two names the rewrite writes for what the grammar has no spelling for: `GROUP BY ALL` becomes a grouping by one
/// name, and `x ILIKE 'p'` becomes `x LIKE 'p' ESCAPE '<one string>'`. Neither is a fixed spelling: each is chosen not
/// to occur anywhere in the query's text, so a name the parser finds is the rewrite's, never the user's (a column may
/// be called `__zpq_group_all`, and an ESCAPE clause the user writes is refused whatever it spells).
pub const Markers = struct {
    group: [48]u8 = undefined,
    group_len: usize = 0,
    ilike: [48]u8 = undefined,
    ilike_len: usize = 0,

    pub fn choose(sql: []const u8) error{BadValue}!Markers {
        var m: Markers = .{};
        m.group_len = try pick(&m.group, "__zpq_group_all", sql);
        m.ilike_len = try pick(&m.ilike, "zpq:ilike", sql);
        return m;
    }

    /// `base`, with underscores added until the text holds no such name (identifiers match without regard to case).
    fn pick(buf: []u8, base: []const u8, sql: []const u8) error{BadValue}!usize {
        @memcpy(buf[0..base.len], base);
        var n = base.len;
        while (std.ascii.findIgnoreCase(sql, buf[0..n]) != null) {
            if (n == buf.len) return error.BadValue;
            buf[n] = '_';
            n += 1;
        }
        return n;
    }

    pub fn groupAll(self: *const Markers) []const u8 {
        return self.group[0..self.group_len];
    }

    pub fn ilikeEscape(self: *const Markers) []const u8 {
        return self.ilike[0..self.ilike_len];
    }
};

/// What went wrong, for the message the parser prints; the error value carries the kind.
pub const Report = struct {
    buf: [200]u8 = undefined,
    len: usize = 0,
    /// The names `rewrite` chose for what it rewrote, which the parser reads the query's markers by.
    markers: Markers = .{},

    fn set(self: *Report, comptime fmt: []const u8, args: anytype) void {
        const written: []const u8 = std.fmt.bufPrint(&self.buf, fmt, args) catch self.buf[0..];
        self.len = written.len;
    }

    pub fn text(self: *const Report) []const u8 {
        return self.buf[0..self.len];
    }
};

const us_per_day: i64 = 86_400_000_000;

pub const Civil = struct { y: i64, m: i64, d: i64 };

/// The inverse of `daysFromCivil` (Hinnant's civil_from_days): proleptic Gregorian, days since 1970-01-01.
pub fn civilFromDays(days: i64) Civil {
    const z = days + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const d = doy - @divFloor(153 * mp + 2, 5) + 1;
    const m = if (mp < 10) mp + 3 else mp - 9;
    return .{ .y = yoe + era * 400 + @as(i64, @intFromBool(m <= 2)), .m = m, .d = d };
}

fn monthLength(y: i64, m: i64) i64 {
    const leap = @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
    return switch (m) {
        2 => if (leap) 29 else 28,
        4, 6, 9, 11 => 30,
        else => 31,
    };
}

/// A moment: whole days since the epoch and microseconds into the day, DuckDB's TIMESTAMP resolution.
pub const Moment = struct { days: i64, micros: i64 };

/// DuckDB's `interval`: months, days and microseconds kept apart, since a month is not a fixed number of days. The
/// fields have DuckDB's widths: a component past them is out of range there, and so here, even if another would
/// cancel it.
pub const Interval = struct {
    months: i32 = 0,
    days: i32 = 0,
    micros: i64 = 0,

    fn plus(self: Interval, o: Interval) ?Interval {
        return .{
            .months = std.math.add(i32, self.months, o.months) catch return null,
            .days = std.math.add(i32, self.days, o.days) catch return null,
            .micros = std.math.add(i64, self.micros, o.micros) catch return null,
        };
    }
};

/// DuckDB's TIMESTAMP is microseconds in an i64 without the two values that stand for infinity.
pub fn inTimestampRange(m: Moment) bool {
    const us = @as(i128, m.days) * us_per_day + m.micros;
    return us > -@as(i128, std.math.maxInt(i64)) and us < std.math.maxInt(i64);
}

/// `m + sign * iv` as DuckDB adds an interval to a TIMESTAMP: the months first, the day of the month clamped to the
/// month's length (2000-01-31 + 1 month is 2000-02-29), then the days, then the microseconds, carried into days.
/// Null when an operation overflows or the result leaves the range the filter's date parser takes (years 0..999999)
/// or TIMESTAMP's.
pub fn addInterval(m: Moment, iv: Interval, sign: i64) ?Moment {
    const c = civilFromDays(m.days);
    const months = c.y * 12 + (c.m - 1) + sign * @as(i64, iv.months);
    const y = @divFloor(months, 12);
    const mo = @mod(months, 12) + 1;
    if (y < 0 or y > 999_999) return null;
    const d = @min(c.d, monthLength(y, mo));
    var days = filters.daysFromCivil(y, mo, d) + sign * @as(i64, iv.days);
    var micros = std.math.add(i64, m.micros, std.math.mul(i64, sign, iv.micros) catch return null) catch return null;
    days += @divFloor(micros, us_per_day);
    micros = @mod(micros, us_per_day);
    const out = civilFromDays(days);
    if (out.y < 0 or out.y > 999_999) return null;
    const moved: Moment = .{ .days = days, .micros = micros };
    return if (inTimestampRange(moved)) moved else null;
}

// Unsigned fields: a signed `{d}` with a width prints a `+`.
fn unsigned(v: i64) u64 {
    return @intCast(v);
}

pub fn formatDate(allocator: std.mem.Allocator, days: i64) ![]u8 {
    const c = civilFromDays(days);
    return std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2}", .{ unsigned(c.y), unsigned(c.m), unsigned(c.d) });
}

pub fn formatTimestamp(allocator: std.mem.Allocator, m: Moment) ![]u8 {
    const c = civilFromDays(m.days);
    const secs = @divFloor(m.micros, 1_000_000);
    const frac = @mod(m.micros, 1_000_000);
    const head = try std.fmt.allocPrint(allocator, "{d:0>4}-{d:0>2}-{d:0>2} {d:0>2}:{d:0>2}:{d:0>2}", .{
        unsigned(c.y), unsigned(c.m), unsigned(c.d), unsigned(@divFloor(secs, 3600)), unsigned(@mod(@divFloor(secs, 60), 60)), unsigned(@mod(secs, 60)),
    });
    if (frac == 0) return head;
    defer allocator.free(head);
    return std.fmt.allocPrint(allocator, "{s}.{d:0>6}", .{ head, unsigned(frac) });
}

/// A DATE or TIMESTAMP literal's text as a moment. A timestamp's digits past the microsecond are dropped, as DuckDB's
/// TIMESTAMP keeps no finer; a DATE takes only the date part, as `CAST('2024-01-01 10:00' AS DATE)` does.
pub fn parseMoment(text: []const u8, is_date: bool) Error!Moment {
    const lit = filters.parseDateTimeNs(std.mem.trim(u8, text, " \t")) catch return error.BadValue;
    const days = @divFloor(lit.ns, 86_400 * 1_000_000_000);
    if (is_date) return .{ .days = @intCast(days), .micros = 0 };
    const tod: i128 = lit.ns - days * 86_400 * 1_000_000_000;
    const out: Moment = .{ .days = @intCast(days), .micros = @intCast(@divFloor(tod, 1000)) };
    // A TIMESTAMP literal past TIMESTAMP's range is a conversion error in DuckDB, not a threshold nothing reaches.
    return if (inTimestampRange(out)) out else error.BadValue;
}

// ---- exact decimals -----------------------------------------------------------------------------------------------

/// A decimal literal held exactly: `mant / 10^scale`. DuckDB reads a literal of 38 digits or fewer as DECIMAL (and
/// `+`, `-` and `*` of two stay exact), so the fold agrees with it; anything wider is DOUBLE there, and refused here.
pub const Dec = struct {
    mant: i256,
    scale: u32,

    pub const max_digits = 38;

    pub fn digits(self: Dec) u32 {
        var n: u32 = 0;
        var v = if (self.mant < 0) -self.mant else self.mant;
        while (v > 0) : (v = @divTrunc(v, 10)) n += 1;
        return n;
    }

    /// 10^n, for n up to 76 (the most an i256 holds).
    fn pow10(n: u32) i256 {
        std.debug.assert(n <= 76);
        var p: i256 = 1;
        for (0..n) |_| p *= 10;
        return p;
    }

    fn scaled(mant: i256, by: u32) ?i256 {
        if (by > 76) return null;
        return std.math.mul(i256, mant, pow10(by)) catch null;
    }

    /// `[+-]digits[.digits]`, with an exponent only if `allow_exp` (a string cast takes one; a bare literal with one
    /// is a DOUBLE in DuckDB and not exact).
    pub fn parse(text: []const u8, allow_exp: bool) ?Dec {
        var s = std.mem.trim(u8, text, " \t");
        var neg = false;
        if (s.len > 0 and (s[0] == '-' or s[0] == '+')) {
            neg = s[0] == '-';
            s = s[1..];
        }
        var exp: i32 = 0;
        if (std.mem.indexOfAny(u8, s, "eE")) |e| {
            if (!allow_exp) return null;
            exp = std.fmt.parseInt(i32, s[e + 1 ..], 10) catch return null;
            s = s[0..e];
        }
        const dot = std.mem.indexOfScalar(u8, s, '.');
        const int_part = if (dot) |d| s[0..d] else s;
        const frac_part = if (dot) |d| s[d + 1 ..] else "";
        if (int_part.len + frac_part.len == 0) return null;
        var mant: i256 = 0;
        for ([_][]const u8{ int_part, frac_part }) |part| for (part) |c| {
            if (c < '0' or c > '9') return null;
            mant = mant * 10 + (c - '0');
            if (mant >= pow10(Dec.max_digits + 2)) return null;
        };
        var scale: i64 = @intCast(frac_part.len);
        scale -= exp;
        if (scale < 0) {
            if (-scale > Dec.max_digits) return null;
            // Prove the product fits 38 digits before forming it: the digits already read times a large exponent
            // passes an i256, and counting the digits afterwards is too late.
            const have: Dec = .{ .mant = mant, .scale = 0 };
            if (mant != 0 and have.digits() + @as(u32, @intCast(-scale)) > Dec.max_digits) return null;
            mant = scaled(mant, @intCast(-scale)) orelse return null;
            scale = 0;
        }
        if (scale > Dec.max_digits + 2) return null;
        var out: Dec = .{ .mant = if (neg) -mant else mant, .scale = @intCast(scale) };
        if (out.digits() > Dec.max_digits) {
            // Trailing zeros of a long fraction do not count: `0.1000000000000000000000000000000000000000`.
            while (out.scale > 0 and @rem(out.mant, 10) == 0) {
                out.mant = @divTrunc(out.mant, 10);
                out.scale -= 1;
            }
            if (out.digits() > Dec.max_digits) return null;
        }
        return out;
    }

    fn align_(a: Dec, b: Dec) ?struct { a: i256, b: i256, scale: u32 } {
        const scale = @max(a.scale, b.scale);
        return .{
            .a = scaled(a.mant, scale - a.scale) orelse return null,
            .b = scaled(b.mant, scale - b.scale) orelse return null,
            .scale = scale,
        };
    }

    fn fit(self: Dec) ?Dec {
        return if (self.digits() > Dec.max_digits or self.scale > Dec.max_digits) null else self;
    }

    pub fn add(a: Dec, b: Dec) ?Dec {
        const x = align_(a, b) orelse return null;
        return (Dec{ .mant = std.math.add(i256, x.a, x.b) catch return null, .scale = x.scale }).fit();
    }

    pub fn sub(a: Dec, b: Dec) ?Dec {
        const x = align_(a, b) orelse return null;
        return (Dec{ .mant = std.math.sub(i256, x.a, x.b) catch return null, .scale = x.scale }).fit();
    }

    pub fn mul(a: Dec, b: Dec) ?Dec {
        return (Dec{ .mant = std.math.mul(i256, a.mant, b.mant) catch return null, .scale = a.scale + b.scale }).fit();
    }

    /// The value as an integer when it has no fractional part.
    pub fn toInt(self: Dec) ?i64 {
        if (self.scale > 76) return null;
        const p = pow10(self.scale);
        if (@rem(self.mant, p) != 0) return null;
        return std.math.cast(i64, @divTrunc(self.mant, p));
    }

    /// At `scale` digits after the point, ties away from zero (DuckDB's rounding of a decimal or string cast).
    pub fn rescale(self: Dec, scale: u32) ?Dec {
        if (scale >= self.scale) return (Dec{ .mant = scaled(self.mant, scale - self.scale) orelse return null, .scale = scale }).fit();
        if (self.scale - scale > 76) return null;
        const p = pow10(self.scale - scale);
        const q = @divTrunc(self.mant, p);
        const r = @rem(self.mant, p);
        const twice = (if (r < 0) -r else r) * 2;
        const bump: i256 = if (twice >= p) (if (self.mant < 0) -1 else 1) else 0;
        return (Dec{ .mant = q + bump, .scale = scale }).fit();
    }

    pub fn format(self: Dec, allocator: std.mem.Allocator) ![]u8 {
        const neg = self.mant < 0;
        var digits_buf: [80]u8 = undefined;
        var v = if (neg) -self.mant else self.mant;
        var n: usize = 0;
        while (v > 0 or n <= self.scale) : (v = @divTrunc(v, 10)) {
            digits_buf[digits_buf.len - 1 - n] = '0' + @as(u8, @intCast(@rem(v, 10)));
            n += 1;
        }
        const all = digits_buf[digits_buf.len - n ..];
        const split = all.len - self.scale;
        return std.fmt.allocPrint(allocator, "{s}{s}{s}{s}", .{
            if (neg) "-" else "",
            all[0..split],
            if (self.scale > 0) "." else "",
            all[split..],
        });
    }
};

// ---- the text rewrite ---------------------------------------------------------------------------------------------

fn isWord(c: u8) bool {
    return std.ascii.isAlphanumeric(c) or c == '_';
}

fn isSpace(c: u8) bool {
    return c == ' ' or c == '\t' or c == '\r' or c == '\n';
}

fn skipSpace(s: []const u8, from: usize) usize {
    var i = from;
    while (i < s.len and isSpace(s[i])) i += 1;
    return i;
}

fn wordEnd(s: []const u8, from: usize) usize {
    var i = from;
    while (i < s.len and isWord(s[i])) i += 1;
    return i;
}

/// The index past the quoted span opening at `s[i]` (a doubled quote inside is escaped), or null if it never closes.
fn quotedEnd(s: []const u8, i: usize) ?usize {
    const q = s[i];
    var j = i + 1;
    while (j < s.len) : (j += 1) if (s[j] == q) {
        if (j + 1 < s.len and s[j + 1] == q) {
            j += 1;
            continue;
        }
        return j + 1;
    };
    return null;
}

fn wordIs(s: []const u8, at: usize, name: []const u8) bool {
    if (at > 0 and (isWord(s[at - 1]) or s[at - 1] == '.')) return false;
    const end = wordEnd(s, at);
    return end > at and std.ascii.eqlIgnoreCase(s[at..end], name);
}

const Unit = enum { months, days, micros };

/// An interval unit's size in `Unit`s; null for a word that is no unit.
fn unitSize(word: []const u8) ?struct { unit: Unit, size: i64 } {
    const names = [_]struct { []const u8, Unit, i64 }{
        .{ "millennium", .months, 12_000 },    .{ "millennia", .months, 12_000 },     .{ "century", .months, 1_200 },
        .{ "centuries", .months, 1_200 },      .{ "decade", .months, 120 },           .{ "decades", .months, 120 },
        .{ "year", .months, 12 },              .{ "years", .months, 12 },             .{ "quarter", .months, 3 },
        .{ "quarters", .months, 3 },           .{ "month", .months, 1 },              .{ "months", .months, 1 },
        .{ "week", .days, 7 },                 .{ "weeks", .days, 7 },                .{ "day", .days, 1 },
        .{ "days", .days, 1 },                 .{ "hour", .micros, 3_600_000_000 },   .{ "hours", .micros, 3_600_000_000 },
        .{ "minute", .micros, 60_000_000 },    .{ "minutes", .micros, 60_000_000 },   .{ "second", .micros, 1_000_000 },
        .{ "seconds", .micros, 1_000_000 },    .{ "millisecond", .micros, 1_000 },    .{ "milliseconds", .micros, 1_000 },
        .{ "microsecond", .micros, 1 },        .{ "microseconds", .micros, 1 },
    };
    for (names) |n| if (std.ascii.eqlIgnoreCase(word, n[0])) return .{ .unit = n[1], .size = n[2] };
    return null;
}

/// `n` of a unit: a whole number of any unit, or a decimal number of seconds (to the microsecond). `INTERVAL '1.5' DAY`
/// is 1 day 12 hours in DuckDB, which no fixed unit holds here, so a fraction elsewhere is refused.
fn intervalOf(number: []const u8, unit_word: []const u8, report: *Report) Error!Interval {
    const u = unitSize(unit_word) orelse {
        report.set("unknown INTERVAL unit '{s}'", .{unit_word});
        return error.BadInterval;
    };
    const dec = Dec.parse(number, false) orelse {
        report.set("INTERVAL takes a number, not '{s}'", .{number});
        return error.BadInterval;
    };
    const per_second = u.unit == .micros and u.size == 1_000_000;
    const total: i64 = if (per_second) blk: {
        if (dec.scale > 6) {
            report.set("INTERVAL '{s}' {s} has digits past the microsecond", .{ number, unit_word });
            return error.BadInterval;
        }
        const micros = dec.rescale(6) orelse return intervalRange(report, number, unit_word);
        break :blk std.math.cast(i64, micros.mant) orelse return intervalRange(report, number, unit_word);
    } else blk: {
        const whole = dec.toInt() orelse {
            report.set("INTERVAL '{s}' {s} is not a whole number of {s}; only seconds take a fraction", .{ number, unit_word, unit_word });
            return error.BadInterval;
        };
        break :blk std.math.mul(i64, whole, u.size) catch return intervalRange(report, number, unit_word);
    };
    // Each component has its field's width, checked as it is read: DuckDB refuses `2147483648 days` whatever follows
    // it, and `INTERVAL '2147483648 days -2147483648 days'` is no zero.
    return switch (u.unit) {
        .months => .{ .months = std.math.cast(i32, total) orelse return intervalRange(report, number, unit_word) },
        .days => .{ .days = std.math.cast(i32, total) orelse return intervalRange(report, number, unit_word) },
        .micros => .{ .micros = total },
    };
}

fn intervalRange(report: *Report, number: []const u8, unit_word: []const u8) error{BadInterval} {
    report.set("INTERVAL '{s}' {s} is out of range", .{ number, unit_word });
    return error.BadInterval;
}

const Parsed = struct { iv: Interval, end: usize };

/// `INTERVAL '90' DAY`, `INTERVAL 90 DAY`, `INTERVAL '1 year 2 months'` (and `DAY (3)`) at `at`, or null when the
/// word `interval` there is no interval literal (a column named `interval`).
fn intervalAt(s: []const u8, at: usize, report: *Report) Error!?Parsed {
    if (!wordIs(s, at, "interval")) return null;
    var j = skipSpace(s, at + "interval".len);
    if (j >= s.len) return null;
    var iv: Interval = .{};
    if (s[j] == '\'') {
        const e = quotedEnd(s, j) orelse {
            report.set("INTERVAL's quoted amount is not closed", .{});
            return error.BadInterval;
        };
        const body = std.mem.trim(u8, s[j + 1 .. e - 1], " \t");
        j = e;
        if (std.mem.indexOfAny(u8, body, " \t") == null) {
            const u = skipSpace(s, j);
            const ue = wordEnd(s, u);
            if (ue == u) {
                report.set("INTERVAL '{s}' needs a unit (DAY, MONTH, ...)", .{body});
                return error.BadInterval;
            }
            iv = try intervalOf(body, s[u..ue], report);
            j = ue;
        } else {
            var it = std.mem.tokenizeAny(u8, body, " \t");
            while (it.next()) |number| {
                const unit_word = it.next() orelse {
                    report.set("INTERVAL '{s}' has a number with no unit", .{body});
                    return error.BadInterval;
                };
                iv = iv.plus(try intervalOf(number, unit_word, report)) orelse return intervalRange(report, body, "");
            }
        }
    } else if (std.ascii.isDigit(s[j]) or s[j] == '-' or s[j] == '+' or s[j] == '.') {
        var e = j + 1;
        while (e < s.len and (std.ascii.isDigit(s[e]) or s[e] == '.')) e += 1;
        const u = skipSpace(s, e);
        const ue = wordEnd(s, u);
        if (ue == u or unitSize(s[u..ue]) == null) return null;
        iv = try intervalOf(s[j..e], s[u..ue], report);
        j = ue;
    } else if (s[j] == '(') {
        report.set("INTERVAL takes a literal amount; an expression is not folded", .{});
        return error.IntervalNotSupported;
    } else return null;
    // `DAY (3)`: Oracle-style leading precision, which TPC-H's text carries and which changes nothing.
    const p = skipSpace(s, j);
    if (p < s.len and s[p] == '(') {
        var q = p + 1;
        while (q < s.len and std.ascii.isDigit(s[q])) q += 1;
        if (q > p + 1 and q < s.len and s[q] == ')') j = q + 1;
    }
    return .{ .iv = iv, .end = j };
}

const Kind = enum { date, timestamp };

const Typed = struct { kind: Kind, at: Moment, end: usize };

/// `DATE '..'`, `TIMESTAMP '..'` or `CAST('..' AS DATE | TIMESTAMP)` at `at`.
fn typedAt(s: []const u8, at: usize, report: *Report) Error!?Typed {
    const end = wordEnd(s, at);
    if (end == at or (at > 0 and (isWord(s[at - 1]) or s[at - 1] == '.'))) return null;
    const word = s[at..end];
    const j = skipSpace(s, end);
    if (std.ascii.eqlIgnoreCase(word, "date") or std.ascii.eqlIgnoreCase(word, "timestamp")) {
        if (j >= s.len or s[j] != '\'') return null;
        const e = quotedEnd(s, j) orelse return null;
        const kind: Kind = if (std.ascii.eqlIgnoreCase(word, "date")) .date else .timestamp;
        return .{ .kind = kind, .at = try moment(s[j + 1 .. e - 1], kind, report), .end = e };
    }
    if (!std.ascii.eqlIgnoreCase(word, "cast") or j >= s.len or s[j] != '(') return null;
    const q = skipSpace(s, j + 1);
    if (q >= s.len or s[q] != '\'') return null;
    const qe = quotedEnd(s, q) orelse return null;
    const a = skipSpace(s, qe);
    if (!wordIs(s, a, "as")) return null;
    const t = skipSpace(s, a + 2);
    const te = wordEnd(s, t);
    const kind: Kind = if (std.ascii.eqlIgnoreCase(s[t..te], "date")) .date else if (std.ascii.eqlIgnoreCase(s[t..te], "timestamp")) .timestamp else return null;
    const close = skipSpace(s, te);
    if (close >= s.len or s[close] != ')') return null;
    return .{ .kind = kind, .at = try moment(s[q + 1 .. qe - 1], kind, report), .end = close + 1 };
}

fn moment(body: []const u8, kind: Kind, report: *Report) Error!Moment {
    return parseMoment(body, kind == .date) catch {
        report.set("invalid {s} literal '{s}'", .{ if (kind == .date) "DATE" else "TIMESTAMP", body });
        return error.BadValue;
    };
}

/// The typed literal, then every `+/- INTERVAL ..` after it, folded: DuckDB adds an interval to a DATE as a TIMESTAMP,
/// so a chain ends as one. Appends the folded `CAST('..' AS ..)` to `out` and returns where the input resumes.
fn foldTyped(allocator: std.mem.Allocator, out: *std.ArrayList(u8), s: []const u8, first: Typed, report: *Report) Error!usize {
    var cur = first;
    while (true) {
        const op = skipSpace(s, cur.end);
        if (op >= s.len or (s[op] != '+' and s[op] != '-')) break;
        const parsed = (try intervalAt(s, skipSpace(s, op + 1), report)) orelse break;
        const sign: i64 = if (s[op] == '+') 1 else -1;
        cur.at = addInterval(cur.at, parsed.iv, sign) orelse {
            report.set("the date arithmetic leaves TIMESTAMP's supported range", .{});
            return error.BadValue;
        };
        cur.kind = .timestamp;
        cur.end = parsed.end;
    }
    const text = if (cur.kind == .date) try formatDate(allocator, cur.at.days) else try formatTimestamp(allocator, cur.at);
    defer allocator.free(text);
    try out.print(allocator, "CAST('{s}' AS {s})", .{ text, if (cur.kind == .date) "DATE" else "TIMESTAMP" });
    return cur.end;
}

/// Where the operand of a `::` starts in the output so far: the last atom (a name path, a literal, a parenthesized
/// group with the function name before it), tracked as the rewrite goes.
const Atom = struct {
    have: bool = false,
    start: usize = 0,
    end: usize = 0,

    fn continues(self: Atom, out: []const u8) bool {
        return self.have and out.len > 0 and out[out.len - 1] == '.' and self.end == out.len;
    }
};

/// The type after `::`: a name, optional `(n[, m])`, and `PRECISION` after DOUBLE. Returns its end, or null.
fn typeAt(s: []const u8, from: usize) ?usize {
    const start = skipSpace(s, from);
    var e = wordEnd(s, start);
    if (e == start) return null;
    if (std.ascii.eqlIgnoreCase(s[start..e], "double")) {
        const n = skipSpace(s, e);
        if (wordIs(s, n, "precision")) e = wordEnd(s, n);
    }
    if (e < s.len and s[e] == '(') {
        var q = e + 1;
        while (q < s.len and (std.ascii.isDigit(s[q]) or s[q] == ',' or s[q] == ' ')) q += 1;
        if (q < s.len and s[q] == ')') e = q + 1;
    }
    return e;
}

/// The index past `GROUP BY ALL` when it starts at `at`.
fn groupByAllAt(s: []const u8, at: usize) ?usize {
    const by = skipSpace(s, wordEnd(s, at));
    if (!wordIs(s, by, "by")) return null;
    const all = skipSpace(s, wordEnd(s, by));
    if (!wordIs(s, all, "all")) return null;
    return wordEnd(s, all);
}

/// `sql` with the spellings above folded. Quoted text and comments pass through. A `DATE '..'` or `INTERVAL` the fold
/// cannot take is a named error, never a syntax error from the parser that follows.
pub fn rewrite(allocator: std.mem.Allocator, sql: []const u8, report: *Report) Error![]u8 {
    report.markers = Markers.choose(sql) catch {
        report.set("the query spells every name zpq's rewrites use", .{});
        return error.BadValue;
    };
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(allocator);
    var open: std.ArrayList(struct { at: usize, name: ?usize }) = .empty;
    defer open.deinit(allocator);
    var atom: Atom = .{};
    var i: usize = 0;
    while (i < sql.len) {
        const ch = sql[i];
        if (ch == '\'' or ch == '"') {
            const e = quotedEnd(sql, i) orelse sql.len;
            const cont = atom.continues(out.items);
            const at = out.items.len;
            try out.appendSlice(allocator, sql[i..e]);
            atom = .{ .have = true, .start = if (cont) atom.start else at, .end = out.items.len };
            i = e;
            continue;
        }
        if (ch == '-' and i + 1 < sql.len and sql[i + 1] == '-') {
            const e = std.mem.indexOfScalarPos(u8, sql, i, '\n') orelse sql.len;
            try out.appendSlice(allocator, sql[i..e]);
            i = e;
            continue;
        }
        if (ch == '/' and i + 1 < sql.len and sql[i + 1] == '*') {
            const e = if (std.mem.indexOfPos(u8, sql, i + 2, "*/")) |x| x + 2 else sql.len;
            try out.appendSlice(allocator, sql[i..e]);
            i = e;
            continue;
        }
        if (ch == ':' and i + 1 < sql.len and sql[i + 1] == ':') {
            const only_space = atom.have and (for (out.items[atom.end..]) |b| {
                if (!isSpace(b)) break false;
            } else true);
            const type_end = if (only_space) typeAt(sql, i + 2) else null;
            if (type_end) |te| {
                const operand = try allocator.dupe(u8, out.items[atom.start..atom.end]);
                defer allocator.free(operand);
                const type_text = std.mem.trim(u8, sql[i + 2 .. te], " \t");
                out.shrinkRetainingCapacity(atom.start);
                const is_text = operand[0] == '\'' and operand[operand.len - 1] == '\'';
                const kind: ?Kind = if (std.ascii.eqlIgnoreCase(type_text, "date")) .date else if (std.ascii.eqlIgnoreCase(type_text, "timestamp")) .timestamp else null;
                var next = te;
                if (is_text and kind != null) {
                    const body = operand[1 .. operand.len - 1];
                    const typed: Typed = .{ .kind = kind.?, .at = try moment(body, kind.?, report), .end = te };
                    next = try foldTyped(allocator, &out, sql, typed, report);
                } else {
                    try out.print(allocator, "CAST({s} AS {s})", .{ operand, type_text });
                }
                atom.end = out.items.len;
                i = next;
                continue;
            }
        }
        if (ch == '(') {
            const adjacent = atom.have and atom.end == out.items.len;
            try open.append(allocator, .{ .at = out.items.len, .name = if (adjacent) atom.start else null });
            try out.append(allocator, '(');
            atom.have = false;
            i += 1;
            continue;
        }
        if (ch == ')') {
            try out.append(allocator, ')');
            if (open.pop()) |o| atom = .{ .have = true, .start = o.name orelse o.at, .end = out.items.len } else atom.have = false;
            i += 1;
            continue;
        }
        if (ch == '.' and atom.have and atom.end == out.items.len) {
            try out.append(allocator, '.');
            atom.end = out.items.len;
            i += 1;
            continue;
        }
        if (isSpace(ch)) {
            try out.append(allocator, ch);
            i += 1;
            continue;
        }
        if (!isWord(ch)) {
            try out.append(allocator, ch);
            atom.have = false;
            i += 1;
            continue;
        }
        const end = wordEnd(sql, i);
        if (try typedAt(sql, i, report)) |typed| {
            // CAST('..' AS DATE) alone is left to the parser; only a chain after it (or DATE '..') is rewritten here.
            const cast_form = std.ascii.eqlIgnoreCase(sql[i..end], "cast");
            const op = skipSpace(sql, typed.end);
            const chained = op < sql.len and (sql[op] == '+' or sql[op] == '-') and
                (try intervalAt(sql, skipSpace(sql, op + 1), report)) != null;
            if (!cast_form or chained) {
                const start = out.items.len;
                i = try foldTyped(allocator, &out, sql, typed, report);
                atom = .{ .have = true, .start = start, .end = out.items.len };
                continue;
            }
        }
        if (wordIs(sql, i, "ilike")) {
            const lit = skipSpace(sql, end);
            const lit_end = if (lit < sql.len and sql[lit] == '\'') quotedEnd(sql, lit) else null;
            const stop = lit_end orelse {
                report.set("ILIKE takes a string literal pattern", .{});
                return error.BadValue;
            };
            try out.appendSlice(allocator, "like ");
            try out.appendSlice(allocator, sql[lit..stop]);
            try out.appendSlice(allocator, " escape '");
            try out.appendSlice(allocator, report.markers.ilikeEscape());
            try out.append(allocator, '\'');
            atom.have = false;
            i = stop;
            continue;
        }
        if (wordIs(sql, i, "group")) if (groupByAllAt(sql, i)) |stop| {
            try out.appendSlice(allocator, "group by ");
            const name = report.markers.groupAll();
            try out.appendSlice(allocator, name);
            atom = .{ .have = true, .start = out.items.len - name.len, .end = out.items.len };
            i = stop;
            continue;
        };
        if (wordIs(sql, i, "distinct")) {
            const next = skipSpace(sql, end);
            if (wordIs(sql, next, "on") and skipSpace(sql, wordEnd(sql, next)) < sql.len and sql[skipSpace(sql, wordEnd(sql, next))] == '(') {
                report.set("DISTINCT ON is not supported yet", .{});
                return error.DistinctNotSupported;
            }
        }
        if (wordIs(sql, i, "interval") and (try intervalAt(sql, i, report)) != null) {
            report.set("INTERVAL is supported only added to or subtracted from a DATE or TIMESTAMP literal", .{});
            return error.IntervalNotSupported;
        }
        const cont = atom.continues(out.items);
        const at = out.items.len;
        try out.appendSlice(allocator, sql[i..end]);
        atom = .{ .have = true, .start = if (cont) atom.start else at, .end = out.items.len };
        i = end;
    }
    return out.toOwnedSlice(allocator);
}

/// `{G}` and `{E}` in `want` stand for the GROUP BY ALL name and the ILIKE escape string the rewrite chose.
fn expectRewrite(sql: []const u8, want: []const u8) !void {
    var report: Report = .{};
    const got = try rewrite(std.testing.allocator, sql, &report);
    defer std.testing.allocator.free(got);
    const with_group = try std.mem.replaceOwned(u8, std.testing.allocator, want, "{G}", report.markers.groupAll());
    defer std.testing.allocator.free(with_group);
    const expected = try std.mem.replaceOwned(u8, std.testing.allocator, with_group, "{E}", report.markers.ilikeEscape());
    defer std.testing.allocator.free(expected);
    try std.testing.expectEqualStrings(expected, got);
}

fn expectRewriteError(sql: []const u8, err: anyerror) !void {
    var report: Report = .{};
    try std.testing.expectError(err, rewrite(std.testing.allocator, sql, &report));
}

test "sql_literals: typed DATE and TIMESTAMP literals become CASTs the grammar has" {
    try expectRewrite("SELECT 1 WHERE d <= DATE '1998-09-02'", "SELECT 1 WHERE d <= CAST('1998-09-02' AS DATE)");
    try expectRewrite("x >= date'1994-1-1'", "x >= CAST('1994-01-01' AS DATE)");
    try expectRewrite("t > TIMESTAMP '2024-02-29 10:30:00.123456789'", "t > CAST('2024-02-29 10:30:00.123456' AS TIMESTAMP)");
    // A column or function named date, a string, a comment and a qualified name are left alone.
    try expectRewrite("SELECT date, t.date, date('x'), 'DATE ''x''' -- DATE '2020-01-01'\n", "SELECT date, t.date, date('x'), 'DATE ''x''' -- DATE '2020-01-01'\n");
    try expectRewriteError("d <= DATE '1998-13-02'", error.BadValue);
}

test "sql_literals: constant date arithmetic folds as DuckDB adds an interval" {
    // DuckDB: date +/- interval is a TIMESTAMP; months first with the day clamped, then days, then the rest.
    try expectRewrite("d < DATE '1994-01-01' + INTERVAL '1' YEAR", "d < CAST('1995-01-01 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d <= date '1998-12-01' - interval '90' day (3)", "d <= CAST('1998-09-02 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = DATE '2000-01-31' + INTERVAL 1 MONTH", "d = CAST('2000-02-29 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = DATE '2000-03-31' - INTERVAL '1' MONTH", "d = CAST('2000-02-29 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = DATE '2000-02-29' + INTERVAL '1' YEAR", "d = CAST('2001-02-28 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = DATE '1999-12-31' + INTERVAL '1 year 2 months 3 days'", "d = CAST('2001-03-03 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = DATE '2020-01-01' + INTERVAL '36' HOUR", "d = CAST('2020-01-02 12:00:00' AS TIMESTAMP)");
    try expectRewrite("d = TIMESTAMP '2020-01-01 00:00:00' - INTERVAL '1.5' SECOND", "d = CAST('2019-12-31 23:59:58.500000' AS TIMESTAMP)");
    try expectRewrite("d = DATE '1994-01-01' + INTERVAL '1' YEAR - INTERVAL '1' DAY", "d = CAST('1994-12-31 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = CAST('1994-01-01' AS date) + INTERVAL '2' WEEK", "d = CAST('1994-01-15 00:00:00' AS TIMESTAMP)");
    try expectRewrite("d = '1994-01-01'::DATE + INTERVAL '3' MONTH", "d = CAST('1994-04-01 00:00:00' AS TIMESTAMP)");
    // A bare CAST of a literal, with no interval, is left to the parser; so is a minus that is no interval.
    try expectRewrite("d = CAST('1994-01-01' AS date) - 5", "d = CAST('1994-01-01' AS date) - 5");
    try expectRewrite("SELECT interval FROM t WHERE interval - 5 > 0", "SELECT interval FROM t WHERE interval - 5 > 0");
}

test "sql_literals: an interval the fold cannot take is a named error" {
    try expectRewriteError("d < now() - INTERVAL '1' DAY", error.IntervalNotSupported);
    try expectRewriteError("d < DATE '2020-01-01' + INTERVAL '1.5' DAY", error.BadInterval);
    try expectRewriteError("d < DATE '2020-01-01' + INTERVAL '5'", error.BadInterval);
    try expectRewriteError("d < DATE '2020-01-01' + INTERVAL (1 + 1) DAY", error.IntervalNotSupported);
}

test "sql_literals: oversized constants and out-of-range components are named errors, never an abort" {
    // Each component has DuckDB's field width, checked as it is read: no later component can cancel it.
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '9223372036854775807' DAY", error.BadInterval);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '9223372036854775807 days 1 day'", error.BadInterval);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '2147483648 days -2147483648 days'", error.BadInterval);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '2147483647 days 1 day'", error.BadInterval);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '178956971 years'", error.BadInterval);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '2562047789 hours'", error.BadInterval);
    // In range as an interval, past TIMESTAMP's range as a date.
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '2147483647 days'", error.BadValue);
    try expectRewriteError("d < DATE '2024-01-01' + INTERVAL '178956970 years'", error.BadValue);
    try expectRewriteError("d < DATE '294247-01-01' + INTERVAL '1' YEAR", error.BadValue);
    try expectRewriteError("d < TIMESTAMP '294247-01-10 04:00:54.775806' + INTERVAL '1' MICROSECOND", error.BadValue);
    // A TIMESTAMP literal has TIMESTAMP's range, to the last microsecond below the infinity value.
    try expectRewriteError("d < TIMESTAMP '999999-01-01 00:00:00'", error.BadValue);
    try expectRewriteError("d < TIMESTAMP '294247-01-10 04:00:54.775807'", error.BadValue);
    try expectRewrite("d < TIMESTAMP '294247-01-10 04:00:54.775806'", "d < CAST('294247-01-10 04:00:54.775806' AS TIMESTAMP)");
    // A DATE has the filter's date range, which a TIMESTAMP does not.
    try expectRewrite("d < DATE '999999-01-01'", "d < CAST('999999-01-01' AS DATE)");
}

test "sql_literals: decimal arithmetic proves a product fits before it forms it" {
    // Forty nines times 10^38 passes an i256; the digits are counted before the multiplication.
    try std.testing.expect(Dec.parse("9999999999999999999999999999999999999999e38", true) == null);
    try std.testing.expect(Dec.parse("1e38", true) == null);
    try std.testing.expect(Dec.parse("9e37", true) != null);
    // Operands whose common scale passes an i256, and a product of 76 digits, are refused, not wrapped.
    const tiny = Dec.parse("0.0000000000000000000000000000000000000001", false).?;
    const big = Dec.parse("99999999999999999999999999999999999999", false).?;
    try std.testing.expect(tiny.add(big) == null);
    try std.testing.expect(tiny.sub(big) == null);
    try std.testing.expect(big.mul(big) == null);
    try std.testing.expect(big.rescale(38) == null);
}

test "sql_literals: the rewrite's marker names occur nowhere in the query text" {
    var plain: Report = .{};
    const a = try rewrite(std.testing.allocator, "SELECT g FROM t GROUP BY ALL", &plain);
    defer std.testing.allocator.free(a);
    try std.testing.expectEqualStrings("__zpq_group_all", plain.markers.groupAll());
    try std.testing.expectEqualStrings("zpq:ilike", plain.markers.ilikeEscape());
    // A column of that name, in any case, and an ESCAPE string of that spelling, are the user's: the marker moves.
    var taken: Report = .{};
    const sql = "SELECT __ZPQ_GROUP_ALL FROM t WHERE s LIKE 'a%' ESCAPE 'zpq:ilike' GROUP BY __zpq_group_all_";
    const b = try rewrite(std.testing.allocator, sql, &taken);
    defer std.testing.allocator.free(b);
    try std.testing.expectEqualStrings(sql, b);
    try std.testing.expectEqualStrings("__zpq_group_all__", taken.markers.groupAll());
    try std.testing.expectEqualStrings("zpq:ilike_", taken.markers.ilikeEscape());
    try expectRewrite("SELECT __zpq_group_all FROM t GROUP BY ALL", "SELECT __zpq_group_all FROM t group by {G}");
}

test "sql_literals: x::TYPE becomes CAST for the operand just before it" {
    try expectRewrite("SELECT a::INT, t.b::DECIMAL(15, 2) AS c", "SELECT CAST(a AS INT), CAST(t.b AS DECIMAL(15, 2)) AS c");
    try expectRewrite("SELECT abs(a - 1)::BIGINT, (a + 1)::DOUBLE PRECISION", "SELECT CAST(abs(a - 1) AS BIGINT), CAST((a + 1) AS DOUBLE PRECISION)");
    try expectRewrite("SELECT 0.5::DECIMAL(3,2), 'x'::VARCHAR, a::INT::BIGINT", "SELECT CAST(0.5 AS DECIMAL(3,2)), CAST('x' AS VARCHAR), CAST(CAST(a AS INT) AS BIGINT)");
    try expectRewrite("SELECT \"a b\"::INT, s.\"c\"::TEXT", "SELECT CAST(\"a b\" AS INT), CAST(s.\"c\" AS TEXT)");
    try expectRewrite("SELECT 1 + a::INT", "SELECT 1 + CAST(a AS INT)");
}

test "sql_literals: GROUP BY ALL, ILIKE and DISTINCT ON" {
    try expectRewrite("SELECT g, count(*) FROM t GROUP BY ALL", "SELECT g, count(*) FROM t group by {G}");
    try expectRewrite("select g from t group   by\n all order by g", "select g from t group by {G} order by g");
    try expectRewrite("SELECT g FROM t GROUP BY all_col", "SELECT g FROM t GROUP BY all_col");
    try expectRewrite("WHERE s ILIKE 'A%' AND t NOT ILIKE 'b_' OR u = 1", "WHERE s like 'A%' escape '{E}' AND t NOT like 'b_' escape '{E}' OR u = 1");
    try expectRewrite("WHERE ilike_flag = 1", "WHERE ilike_flag = 1");
    try expectRewriteError("WHERE s ILIKE t", error.BadValue);
    try expectRewriteError("SELECT DISTINCT ON (g) g FROM t", error.DistinctNotSupported);
    try expectRewrite("SELECT DISTINCT onion FROM t", "SELECT DISTINCT onion FROM t");
}

test "sql_literals: exact decimals add, subtract, multiply and round half away from zero" {
    const a = std.testing.allocator;
    const check = struct {
        fn go(alloc: std.mem.Allocator, d: ?Dec, want: []const u8) !void {
            const text = try d.?.format(alloc);
            defer alloc.free(text);
            try std.testing.expectEqualStrings(want, text);
        }
    }.go;
    try check(a, Dec.parse(".06", false).?.sub(Dec.parse("0.01", false).?), "0.05");
    try check(a, Dec.parse("0.06", false).?.add(Dec.parse("1", false).?), "1.06");
    try check(a, Dec.parse("1.5", false).?.mul(Dec.parse("2", false).?), "3.0");
    try check(a, Dec.parse("0.055", false).?.rescale(2), "0.06");
    try check(a, Dec.parse("-0.055", false).?.rescale(2), "-0.06");
    try check(a, Dec.parse("0.045", false).?.rescale(2), "0.05");
    try check(a, Dec.parse("1e2", true).?.rescale(2), "100.00");
    try check(a, Dec.parse("-12.5", false).?.rescale(0), "-13");
    try std.testing.expect(Dec.parse("1e2", false) == null);
    try std.testing.expect(Dec.parse("abc", false) == null);
    try std.testing.expect(Dec.parse("123456789012345678901234567890123456789", false) == null);
}

test "sql_literals: civil dates round-trip through days" {
    var days: i64 = -719_468; // 0000-03-01: the filter's date parser takes no year before 0
    while (days < 800_000) : (days += 997) {
        const c = civilFromDays(days);
        try std.testing.expectEqual(days, filters.daysFromCivil(c.y, c.m, c.d));
    }
}
