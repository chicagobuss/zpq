//! The proleptic Gregorian calendar over integer time, as DuckDB's `date_trunc` and `date_part` read it: DATE days
//! and TIMESTAMP ticks since 1970-01-01 (UTC, no time zone), floored toward the past so times before the epoch land
//! in the day, minute or month they fall in.

const std = @import("std");
const ast = @import("ast.zig");

pub const Error = error{
    /// A `make_date` argument outside INTEGER's range, as DuckDB's INTEGER cast refuses it; or year, month and day that
    /// are no date DuckDB's DATE holds.
    DateOutOfRange,
    /// A time whose microseconds since the epoch do not fit 64 bits, which no DuckDB TIMESTAMP holds.
    TimestampOutOfRange,
    /// A calendar field or truncation of DuckDB's infinite DATE, which has neither.
    InfiniteDate,
};

const us_per_s: i64 = std.time.us_per_s;
const us_per_min: i64 = 60 * us_per_s;
const us_per_hour: i64 = 60 * us_per_min;
const us_per_day: i64 = 24 * us_per_hour;

/// Days since 1970-01-01 of a civil date (Hinnant's `days_from_civil`). `m` is 1..12; `d` may run past the month.
pub fn daysFromCivil(y_in: i64, m: i64, d: i64) i64 {
    const y = if (m <= 2) y_in - 1 else y_in;
    const era = @divFloor(y, 400);
    const yoe = y - era * 400;
    const mp = if (m > 2) m - 3 else m + 9;
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return era * 146097 + doe - 719468;
}

pub const Civil = struct { y: i64, m: i64, d: i64 };

/// The civil date `days` after 1970-01-01 (Hinnant's `civil_from_days`).
pub fn civilFromDays(days: i64) Civil {
    const z = days + 719468;
    const era = @divFloor(z, 146097);
    const doe = z - era * 146097;
    const yoe = @divFloor(doe - @divFloor(doe, 1460) + @divFloor(doe, 36524) - @divFloor(doe, 146096), 365);
    const doy = doe - (365 * yoe + @divFloor(yoe, 4) - @divFloor(yoe, 100));
    const mp = @divFloor(5 * doy + 2, 153);
    const m = if (mp < 10) mp + 3 else mp - 9;
    return .{ .y = yoe + era * 400 + @intFromBool(m <= 2), .m = m, .d = doy - @divFloor(153 * mp + 2, 5) + 1 };
}

fn isLeap(y: i64) bool {
    return @mod(y, 4) == 0 and (@mod(y, 100) != 0 or @mod(y, 400) == 0);
}

fn daysInMonth(y: i64, m: i64) i64 {
    return switch (m) {
        2 => if (isLeap(y)) 29 else 28,
        4, 6, 9, 11 => 30,
        else => 31,
    };
}

/// The most days from the epoch DuckDB's DATE holds either way (5881580-07-10 and -5877641-06-25); one more is infinity.
const max_date_days: i64 = std.math.maxInt(i32) - 1;

/// `make_date(y, m, d)`: days since 1970-01-01, or `DateOutOfRange` for a month or day the calendar has not, or a date
/// DuckDB's DATE cannot hold.
pub fn makeDate(y: i64, m: i64, d: i64) Error!i64 {
    if (m < 1 or m > 12 or d < 1 or y < -5877641 or y > 5881580 or d > daysInMonth(y, m)) return error.DateOutOfRange;
    const days = daysFromCivil(y, m, d);
    if (days > max_date_days or days < -max_date_days) return error.DateOutOfRange;
    return days;
}

/// `v` over `per`, to the nearest, a tie away from zero: how DuckDB takes TIMESTAMP_NS to microseconds.
pub fn roundDivAway(v: i64, per: i64) i64 {
    const q = @divTrunc(v, per);
    const r = @rem(v, per);
    return if (@as(i128, @abs(r)) * 2 >= per) q + (if (v < 0) @as(i64, -1) else 1) else q;
}

/// `date_diff(unit, a, b)`: how many boundaries of `unit` lie between two times, `b` minus `a`, each counted in its own
/// unit; a boundary is crossed, not a whole period elapsed (2020-01-31 to 2020-02-01 is a month). A DATE is its midnight,
/// a TIMESTAMP_NS its microsecond to the nearest, as DuckDB casts it. A week is not a Monday's boundary: it is the day
/// count over 7, truncated toward zero (1473 days are 210 weeks). Null where either time is infinite.
pub fn diff(unit: ast.DiffPart, ta_in: ast.Temporal, a_in: i64, tb_in: ast.Temporal, b_in: i64) Error!?i64 {
    const ts_infinity = std.math.maxInt(i64);
    if ((ta_in != .date and (a_in == ts_infinity or a_in == -ts_infinity)) or
        (tb_in != .date and (b_in == ts_infinity or b_in == -ts_infinity))) return null;
    const ta: ast.Temporal = if (ta_in == .ts_ns) .ts_us else ta_in;
    const tb: ast.Temporal = if (tb_in == .ts_ns) .ts_us else tb_in;
    const a = if (ta_in == .ts_ns) roundDivAway(a_in, std.time.ns_per_us) else a_in;
    const b = if (tb_in == .ts_ns) roundDivAway(b_in, std.time.ns_per_us) else b_in;
    const sa = split(ta, a) catch |e| return if (e == error.InfiniteDate) null else e;
    const sb = split(tb, b) catch |e| return if (e == error.InfiniteDate) null else e;
    const ca = civilFromDays(sa.days);
    const cb = civilFromDays(sb.days);
    const da: i128 = sa.days;
    const db: i128 = sb.days;
    const r: i128 = switch (unit) {
        .year => cb.y - ca.y,
        .decade => @divTrunc(cb.y, 10) - @divTrunc(ca.y, 10),
        .century => @divTrunc(cb.y, 100) - @divTrunc(ca.y, 100),
        .millennium => @divTrunc(cb.y, 1000) - @divTrunc(ca.y, 1000),
        .quarter => @divFloor(cb.y * 12 + cb.m - 1, 3) - @divFloor(ca.y * 12 + ca.m - 1, 3),
        .month => (cb.y * 12 + cb.m) - (ca.y * 12 + ca.m),
        .week => @divTrunc(db - da, 7),
        .day => db - da,
        .hour => (db * 24 + @divFloor(sb.of_day, us_per_hour)) - (da * 24 + @divFloor(sa.of_day, us_per_hour)),
        .minute => (db * 1440 + @divFloor(sb.of_day, us_per_min)) - (da * 1440 + @divFloor(sa.of_day, us_per_min)),
        .second => (db * 86400 + @divFloor(sb.of_day, us_per_s)) - (da * 86400 + @divFloor(sa.of_day, us_per_s)),
        .millisecond => (db * 86_400_000 + @divFloor(sb.of_day, std.time.us_per_ms)) -
            (da * 86_400_000 + @divFloor(sa.of_day, std.time.us_per_ms)),
        .microsecond => (db * us_per_day + sb.of_day) - (da * us_per_day + sa.of_day),
    };
    return std.math.cast(i64, r) orelse error.TimestampOutOfRange;
}

/// A time split into its day and the microseconds into it; a DATE has none. DuckDB's infinite DATEs (`make_date` of
/// ±maxInt(i32)) have no calendar fields, so they are refused rather than answered as a finite day.
const Split = struct { days: i64, of_day: i64 };

fn split(t: ast.Temporal, v: i64) Error!Split {
    const us: i64 = switch (t) {
        .date => {
            if (v == std.math.maxInt(i32) or v == -std.math.maxInt(i32)) return error.InfiniteDate;
            return .{ .days = v, .of_day = 0 };
        },
        .ts_ms => std.math.mul(i64, v, std.time.us_per_ms) catch return error.TimestampOutOfRange,
        .ts_us => v,
        .ts_ns => @divFloor(v, std.time.ns_per_us),
    };
    return .{ .days = @divFloor(us, us_per_day), .of_day = @mod(us, us_per_day) };
}

/// Days since the epoch of the Monday starting `days`'s ISO week (1970-01-01 was a Thursday).
fn mondayOf(days: i64) i64 {
    return days - @mod(days + 3, 7);
}

/// `date_trunc(part, v)`: `v` floored to the part, as microseconds since the epoch (a TIMESTAMP, for a DATE too).
pub inline fn trunc(comptime p: ast.DatePart, t: ast.Temporal, v: i64) Error!i64 {
    const x = try split(t, v);
    const step: ?i64 = switch (p) {
        .microsecond => 1,
        .millisecond => std.time.us_per_ms,
        .second => us_per_s,
        .minute => us_per_min,
        .hour => us_per_hour,
        .day => us_per_day,
        else => null,
    };
    const first_day, const of_day = if (step) |s| .{ x.days, x.of_day - @mod(x.of_day, s) } else .{ switch (p) {
        .week => mondayOf(x.days),
        .month, .quarter, .year => blk: {
            const c = civilFromDays(x.days);
            const m = switch (p) {
                .month => c.m,
                .quarter => c.m - @mod(c.m - 1, 3),
                else => 1,
            };
            break :blk daysFromCivil(c.y, m, 1);
        },
        else => comptime unreachable,
    }, 0 };
    const day_us = std.math.mul(i64, first_day, us_per_day) catch return error.TimestampOutOfRange;
    return std.math.add(i64, day_us, of_day) catch error.TimestampOutOfRange;
}

/// `date_part(part, v)`. `second`, `millisecond` and `microsecond` count within the minute, as DuckDB's do, and a
/// DATE's are 0; `week` is the ISO week number, `dow` 0 for Sunday, `isodow` 7 for it.
pub inline fn part(comptime p: ast.DatePart, t: ast.Temporal, v: i64) Error!i64 {
    const x = try split(t, v);
    const of_minute = @mod(x.of_day, us_per_min);
    return switch (p) {
        .microsecond => of_minute,
        .millisecond => @divFloor(of_minute, std.time.us_per_ms),
        .second => @divFloor(of_minute, us_per_s),
        .minute => @mod(@divFloor(x.of_day, us_per_min), 60),
        .hour => @divFloor(x.of_day, us_per_hour),
        .dow => @mod(x.days + 4, 7),
        .isodow => @mod(x.days + 3, 7) + 1,
        .day => civilFromDays(x.days).d,
        .month => civilFromDays(x.days).m,
        .quarter => @divFloor(civilFromDays(x.days).m - 1, 3) + 1,
        .year => civilFromDays(x.days).y,
        .doy => x.days - daysFromCivil(civilFromDays(x.days).y, 1, 1) + 1,
        .week => blk: {
            // The ISO week year is the one holding this week's Thursday; its week 1 holds January 4.
            const thursday = mondayOf(x.days) + 3;
            const week1 = mondayOf(daysFromCivil(civilFromDays(thursday).y, 1, 4));
            break :blk @divFloor(thursday - 3 - week1, 7) + 1;
        },
    };
}

const testing = std.testing;

test "civil dates round-trip around the epoch and across leap centuries" {
    var d: i64 = -800_000;
    while (d < 800_000) : (d += 997) {
        const c = civilFromDays(d);
        try testing.expectEqual(d, daysFromCivil(c.y, c.m, c.d));
    }
    try testing.expectEqual(@as(i64, 15887), daysFromCivil(2013, 7, 1));
    try testing.expectEqual(Civil{ .y = 2000, .m = 2, .d = 29 }, civilFromDays(daysFromCivil(2000, 2, 29)));
    try testing.expectEqual(Civil{ .y = 1969, .m = 12, .d = 31 }, civilFromDays(-1));
}

test "trunc and part match DuckDB on 2013-07-31 19:59:59.123, before the epoch and on DATEs" {
    const us: i64 = 1375300799123 * 1000;
    try testing.expectEqual(@as(i64, 1375300740) * us_per_s, try trunc(.minute, .ts_us, us));
    try testing.expectEqual(daysFromCivil(2013, 7, 29) * us_per_day, try trunc(.week, .ts_us, us));
    try testing.expectEqual(daysFromCivil(2013, 7, 1) * us_per_day, try trunc(.quarter, .ts_us, us));
    try testing.expectEqual(daysFromCivil(2013, 1, 1) * us_per_day, try trunc(.year, .ts_ms, us / 1000));
    try testing.expectEqual(@as(i64, 59), try part(.minute, .ts_us, us));
    try testing.expectEqual(@as(i64, 59), try part(.second, .ts_ns, us * 1000));
    try testing.expectEqual(@as(i64, 59123), try part(.millisecond, .ts_us, us));
    try testing.expectEqual(@as(i64, 19), try part(.hour, .ts_us, us));
    try testing.expectEqual(@as(i64, 3), try part(.dow, .ts_us, us));
    try testing.expectEqual(@as(i64, 212), try part(.doy, .ts_us, us));
    try testing.expectEqual(@as(i64, 31), try part(.week, .ts_us, us));
    // -1 ms is 1969-12-31 23:59:59.999.
    try testing.expectEqual(@as(i64, 59), try part(.minute, .ts_ms, -1));
    try testing.expectEqual(@as(i64, 1969), try part(.year, .ts_ms, -1));
    try testing.expectEqual(@as(i64, -60) * us_per_s, try trunc(.minute, .ts_ms, -1));
    // ISO weeks at year ends: 2021-01-03 is in 2020's week 53, 2024-12-30 in 2025's week 1.
    try testing.expectEqual(@as(i64, 53), try part(.week, .date, daysFromCivil(2021, 1, 3)));
    try testing.expectEqual(@as(i64, 1), try part(.week, .date, daysFromCivil(2024, 12, 30)));
    // A DATE's fields come from its days, past TIMESTAMP's range too; truncating one gives a TIMESTAMP, which must fit.
    try testing.expectEqual(@as(i64, 5881580), try part(.year, .date, 2147483646));
    try testing.expectEqual(@as(i64, 0), try part(.minute, .date, 2147483646));
    try testing.expectEqual(@as(i64, 4), try part(.dow, .date, -2147483646));
    try testing.expectError(error.TimestampOutOfRange, trunc(.month, .date, 2147483646));
    try testing.expectError(error.InfiniteDate, part(.year, .date, std.math.maxInt(i32)));
    try testing.expectError(error.TimestampOutOfRange, trunc(.day, .ts_ms, -9223372036854775));
}

test "make_date(y, m, d) and its edges match DuckDB 2.0.0" {
    const T = std.testing;
    try T.expectEqual(@as(i64, 0), try makeDate(1970, 1, 1));
    try T.expectEqual(@as(i64, 18321), try makeDate(2020, 2, 29));
    try T.expectEqual(@as(i64, -719528), try makeDate(0, 1, 1));
    try T.expectEqual(@as(i64, 2147483646), try makeDate(5881580, 7, 10));
    try T.expectEqual(@as(i64, -2147483646), try makeDate(-5877641, 6, 25));
    for ([_][3]i64{ .{ 5881580, 7, 11 }, .{ -5877641, 6, 24 }, .{ 2021, 2, 29 }, .{ 2020, 13, 1 }, .{ 2020, 0, 1 }, .{ 2020, 1, 32 }, .{ 2020, 4, 31 }, .{ 2020, 1, 0 }, .{ 1900, 2, 29 }, .{ std.math.maxInt(i64), 1, 1 } }) |bad|
        try T.expectError(error.DateOutOfRange, makeDate(bad[0], bad[1], bad[2]));
    try T.expectEqual(@as(i64, 11016), try makeDate(2000, 2, 29));
}

test "date_diff counts boundaries crossed, not periods elapsed, and is NULL for infinity" {
    const T = std.testing;
    const d = struct {
        fn at(y: i64, m: i64, day: i64) i64 {
            return makeDate(y, m, day) catch unreachable;
        }
    }.at;
    try T.expectEqual(@as(?i64, 1), try diff(.month, .date, d(2020, 1, 31), .date, d(2020, 2, 1)));
    try T.expectEqual(@as(?i64, -1), try diff(.month, .date, d(2020, 3, 1), .date, d(2020, 2, 29)));
    try T.expectEqual(@as(?i64, 1), try diff(.year, .date, d(2019, 12, 31), .date, d(2020, 1, 1)));
    try T.expectEqual(@as(?i64, 1), try diff(.quarter, .date, d(2020, 3, 31), .date, d(2020, 4, 1)));
    try T.expectEqual(@as(?i64, 0), try diff(.quarter, .date, d(2020, 4, 1), .date, d(2020, 6, 30)));
    // A week is the day count over 7, truncated: 6 days are none, 7 are one, -6 none, -8 minus one.
    try T.expectEqual(@as(?i64, 0), try diff(.week, .date, d(2020, 1, 1), .date, d(2020, 1, 7)));
    try T.expectEqual(@as(?i64, 1), try diff(.week, .date, d(2020, 1, 1), .date, d(2020, 1, 8)));
    try T.expectEqual(@as(?i64, 0), try diff(.week, .date, d(2020, 1, 7), .date, d(2020, 1, 1)));
    try T.expectEqual(@as(?i64, -1), try diff(.week, .date, d(2020, 1, 9), .date, d(2020, 1, 1)));
    try T.expectEqual(@as(?i64, 0), try diff(.week, .date, d(2020, 1, 5), .date, d(2020, 1, 6)));
    // A TIMESTAMP_NS reaches microseconds to the nearest first: 499 ns short of midnight is midnight.
    try T.expectEqual(@as(?i64, 0), try diff(.second, .ts_us, 0, .ts_ns, -499));
    try T.expectEqual(@as(?i64, 1), try diff(.second, .ts_ns, -500, .ts_us, 0));
    // 23:59:59.5 on 2019-12-31 to 00:00:00.25 the next day.
    const a: i64 = d(2019, 12, 31) * 86_400_000_000 + 86_399_500_000;
    const b: i64 = d(2020, 1, 1) * 86_400_000_000 + 250_000;
    for ([_]struct { p: ast.DiffPart, want: i64 }{
        .{ .p = .day, .want = 1 },         .{ .p = .hour, .want = 1 },        .{ .p = .minute, .want = 1 },
        .{ .p = .second, .want = 1 },      .{ .p = .millisecond, .want = 750 }, .{ .p = .microsecond, .want = 750_000 },
        .{ .p = .year, .want = 1 },        .{ .p = .decade, .want = 1 },      .{ .p = .century, .want = 0 },
        .{ .p = .millennium, .want = 0 },  .{ .p = .week, .want = 0 },
    }) |c| try T.expectEqual(@as(?i64, c.want), try diff(c.p, .ts_us, a, .ts_us, b));
    // A DATE is its midnight and a mix of units compares on the same clock.
    try T.expectEqual(@as(?i64, 24), try diff(.hour, .date, d(2020, 1, 1), .date, d(2020, 1, 2)));
    try T.expectEqual(@as(?i64, 1), try diff(.day, .date, d(2020, 1, 1), .ts_ms, (d(2020, 1, 2) * 86_400_000) + 43_200_000));
    // Infinity, either side and either kind of time.
    try T.expectEqual(@as(?i64, null), try diff(.day, .date, std.math.maxInt(i32), .date, d(2020, 1, 1)));
    try T.expectEqual(@as(?i64, null), try diff(.day, .ts_us, 0, .ts_us, std.math.maxInt(i64)));
}

