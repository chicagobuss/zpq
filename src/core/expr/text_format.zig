//! A value as text, the way DuckDB 2.0 prints it for `CAST(x AS VARCHAR)`: integers in full, BOOLEAN as `true` and `false`,
//! DECIMAL at its scale, DOUBLE and FLOAT by their shortest round-trip digits in DuckDB's layout, DATE as `YYYY-MM-DD` (years
//! before 1 as `(BC)`), TIMESTAMP with its fraction cut of trailing zeros, UTC-adjusted ones with `+00`.
const std = @import("std");
const testing = std.testing;
const calendar = @import("calendar.zig");
const ast = @import("ast.zig");
const grisu = @import("grisu.zig");

pub fn writeInt(out: *std.ArrayList(u8), gpa: std.mem.Allocator, v: i128) std.mem.Allocator.Error!void {
    var buf: [48]u8 = undefined;
    try out.appendSlice(gpa, std.fmt.bufPrint(&buf, "{d}", .{v}) catch unreachable);
}

pub fn writeBool(out: *std.ArrayList(u8), gpa: std.mem.Allocator, v: bool) std.mem.Allocator.Error!void {
    try out.appendSlice(gpa, if (v) "true" else "false");
}

/// A DECIMAL's unscaled value at `scale` digits after the point (none, and no point, at scale 0): `0.05`, `-12345.60`, `0.00`.
pub fn writeDecimal(out: *std.ArrayList(u8), gpa: std.mem.Allocator, unscaled: i128, scale: u8) std.mem.Allocator.Error!void {
    var buf: [48]u8 = undefined;
    const digits = std.fmt.bufPrint(&buf, "{d}", .{@abs(unscaled)}) catch unreachable;
    if (unscaled < 0) try out.append(gpa, '-');
    if (scale == 0) return out.appendSlice(gpa, digits);
    if (digits.len <= scale) {
        try out.appendSlice(gpa, "0.");
        try out.appendNTimes(gpa, '0', scale - digits.len);
        return out.appendSlice(gpa, digits);
    }
    try out.appendSlice(gpa, digits[0 .. digits.len - scale]);
    try out.append(gpa, '.');
    try out.appendSlice(gpa, digits[digits.len - scale ..]);
}

/// DOUBLE (`T = f64`) or FLOAT (`T = f32`): the shortest digits that read back the same, fixed notation for a decimal exponent from -4
/// to 15 (`1e15` is `1000000000000000.0`, `0.0001` is `0.0001`), else `d.ddde+XX` with an exponent of at least two digits.
pub fn writeFloat(out: *std.ArrayList(u8), gpa: std.mem.Allocator, comptime T: type, x: T) std.mem.Allocator.Error!void {
    // fmt prints the sign of a NaN too: `-nan` for `inf - inf`, `sqrt(-1)` and the text `-nan`.
    if (std.math.isNan(x)) return out.appendSlice(gpa, if (std.math.signbit(x)) "-nan" else "nan");
    if (std.math.isInf(x)) return out.appendSlice(gpa, if (x < 0) "-inf" else "inf");
    if (std.math.signbit(x)) try out.append(gpa, '-');
    var digits_buf: [32]u8 = undefined;
    var n: usize = 0;
    var exp: i32 = 0;
    // DuckDB's fmt runs Grisu3 on a FLOAT's own interval; where it declines, the digits are the widened double's.
    if (T == f32 and x != 0) if (grisu.shortest32(@abs(x))) |g| {
        n = g.len;
        @memcpy(digits_buf[0..n], g.slice());
        exp = g.exp10 + @as(i32, @intCast(n)) - 1;
    };
    if (n == 0) {
        var buf: [128]u8 = undefined;
        const wide: f64 = @abs(x);
        const sci = std.fmt.float.render(&buf, wide, .{ .mode = .scientific }) catch unreachable;
        // `d[.ddd]e[-]x`
        const e_at = std.mem.indexOfScalar(u8, sci, 'e').?;
        exp = std.fmt.parseInt(i32, sci[e_at + 1 ..], 10) catch unreachable;
        for (sci[0..e_at]) |ch| {
            if (ch == '.') continue;
            digits_buf[n] = ch;
            n += 1;
        }
    }
    const digits = digits_buf[0..n];
    if (exp >= -4 and exp <= 15) {
        if (exp >= 0) {
            const int_len: usize = @intCast(exp + 1);
            if (n <= int_len) {
                try out.appendSlice(gpa, digits);
                try out.appendNTimes(gpa, '0', int_len - n);
                try out.appendSlice(gpa, ".0");
            } else {
                try out.appendSlice(gpa, digits[0..int_len]);
                try out.append(gpa, '.');
                try out.appendSlice(gpa, digits[int_len..]);
            }
        } else {
            try out.appendSlice(gpa, "0.");
            try out.appendNTimes(gpa, '0', @intCast(-exp - 1));
            try out.appendSlice(gpa, digits);
        }
        return;
    }
    try out.append(gpa, digits[0]);
    if (n > 1) {
        try out.append(gpa, '.');
        try out.appendSlice(gpa, digits[1..]);
    }
    var eb: [8]u8 = undefined;
    const mag: u32 = @abs(exp);
    try out.appendSlice(gpa, std.fmt.bufPrint(&eb, "e{c}{d:0>2}", .{ @as(u8, if (exp < 0) '-' else '+'), mag }) catch unreachable);
}

/// `YYYY-MM-DD`: the year at least four digits, a year before 1 as its BC year (`1 - y`) with ` (BC)` after the date.
fn writeCivil(out: *std.ArrayList(u8), gpa: std.mem.Allocator, days: i64) std.mem.Allocator.Error!bool {
    const c = calendar.civilFromDays(days);
    const bc = c.y < 1;
    const year: u64 = @intCast(if (bc) 1 - c.y else c.y);
    var buf: [40]u8 = undefined;
    try out.appendSlice(gpa, std.fmt.bufPrint(&buf, "{d:0>4}-{d:0>2}-{d:0>2}", .{ year, @as(u64, @intCast(c.m)), @as(u64, @intCast(c.d)) }) catch unreachable);
    return bc;
}

/// A DATE as its days since the epoch; the infinite ones (`maxInt(i32)`) as `infinity` and `-infinity`.
pub fn writeDate(out: *std.ArrayList(u8), gpa: std.mem.Allocator, days: i64) std.mem.Allocator.Error!void {
    if (days == std.math.maxInt(i32)) return out.appendSlice(gpa, "infinity");
    if (days == -std.math.maxInt(i32)) return out.appendSlice(gpa, "-infinity");
    if (try writeCivil(out, gpa, days)) try out.appendSlice(gpa, " (BC)");
}

/// A TIMESTAMP counted in `unit` (and whether it is UTC-adjusted): the date, the time, and the fraction in the unit's
/// own digits (3, 6 or 9) without trailing zeros, `+00` after a UTC-adjusted one; infinite ones as `infinity`.
pub fn writeTimestamp(out: *std.ArrayList(u8), gpa: std.mem.Allocator, unit: ast.Temporal, utc: bool, v: i64) error{ OutOfMemory, ConversionOutOfRange }!void {
    if (v == std.math.maxInt(i64)) return out.appendSlice(gpa, "infinity");
    if (v == -std.math.maxInt(i64)) return out.appendSlice(gpa, "-infinity");
    // DuckDB reads Parquet milliseconds as microseconds, an error where they do not fit. Nothing else is refused: a
    // time below its documented minimum (290309-12-22 BC) still prints, as a UTC-adjusted one always did.
    if (unit == .ts_ms) _ = std.math.mul(i64, v, 1000) catch return error.ConversionOutOfRange;
    const per_sec: i64 = switch (unit) {
        .ts_ms => 1_000,
        .ts_us => 1_000_000,
        .ts_ns => 1_000_000_000,
        .date => unreachable,
    };
    const secs = @divFloor(v, per_sec);
    const frac: u64 = @intCast(@mod(v, per_sec));
    const days = @divFloor(secs, 86_400);
    const of_day: u64 = @intCast(@mod(secs, 86_400));
    const bc = try writeCivil(out, gpa, days);
    if (bc) try out.appendSlice(gpa, " (BC)");
    var buf: [16]u8 = undefined;
    try out.appendSlice(gpa, std.fmt.bufPrint(&buf, " {d:0>2}:{d:0>2}:{d:0>2}", .{ of_day / 3600, of_day / 60 % 60, of_day % 60 }) catch unreachable);
    if (frac != 0) {
        const width: usize = switch (unit) {
            .ts_ms => 3,
            .ts_us => 6,
            .ts_ns => 9,
            .date => unreachable,
        };
        var fb: [9]u8 = undefined;
        const text = std.fmt.bufPrint(&fb, "{d:0>[1]}", .{ frac, width }) catch unreachable;
        try out.append(gpa, '.');
        try out.appendSlice(gpa, std.mem.trimEnd(u8, text, "0"));
    }
    if (utc) try out.appendSlice(gpa, "+00");
}

fn expectFloat(comptime T: type, x: T, want: []const u8) !void {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(testing.allocator);
    try writeFloat(&out, testing.allocator, T, x);
    try testing.expectEqualStrings(want, out.items);
}

test "DOUBLE and FLOAT text is DuckDB 2.0.0's" {
    // `SELECT x::VARCHAR`.
    try expectFloat(f64, 1.0, "1.0");
    try expectFloat(f64, 100.0, "100.0");
    try expectFloat(f64, 1e15, "1000000000000000.0");
    try expectFloat(f64, 1e16, "1e+16");
    try expectFloat(f64, 1e17, "1e+17");
    try expectFloat(f64, 123456789012345678.0, "1.2345678901234568e+17");
    try expectFloat(f64, 1.5e15, "1500000000000000.0");
    try expectFloat(f64, 999999999999999.9, "999999999999999.9");
    try expectFloat(f64, 0.0001, "0.0001");
    try expectFloat(f64, 0.00001, "1e-05");
    try expectFloat(f64, 1.5e-7, "1.5e-07");
    try expectFloat(f64, 100000.0, "100000.0");
    try expectFloat(f64, 1e22, "1e+22");
    try expectFloat(f64, 1e100, "1e+100");
    try expectFloat(f64, 5e-324, "5e-324");
    try expectFloat(f64, 1.7976931348623157e308, "1.7976931348623157e+308");
    try expectFloat(f64, 12345678.9, "12345678.9");
    try expectFloat(f64, 9.999e-5, "9.999e-05");
    try expectFloat(f64, 1234567890123456.0, "1234567890123456.0");
    try expectFloat(f64, 12345678901234567.0, "1.2345678901234568e+16");
    try expectFloat(f64, 0.000123, "0.000123");
    try expectFloat(f64, -1.5, "-1.5");
    try expectFloat(f64, 0.0, "0.0");
    try expectFloat(f64, std.math.inf(f64), "inf");
    try expectFloat(f64, -std.math.inf(f64), "-inf");
    try expectFloat(f64, std.math.nan(f64), "nan");
    try expectFloat(f64, -std.math.nan(f64), "-nan");
    try expectFloat(f32, -std.math.nan(f32), "-nan");
    try expectFloat(f32, 1.5, "1.5");
    try expectFloat(f32, 1e10, "10000000000.0");
    try expectFloat(f32, 1e16, "1e+16");
    try expectFloat(f32, 0.1, "0.1");
    try expectFloat(f32, 3.4028235e38, "3.4028235e+38");
    try expectFloat(f32, 1e-5, "1e-05");
    try expectFloat(f32, 16777216.0, "16777216.0");
    try expectFloat(f32, -0.0, "-0.0");
    // Where Grisu3 declines (a tie between two shortest candidates) DuckDB prints the widened double's digits.
    try expectFloat(f32, -521345.625, "-521345.625");
    try expectFloat(f32, 2215130.75, "2215130.75");
    try expectFloat(f32, -1564322.25, "-1564322.25");
    try expectFloat(f32, 1234567.9, "1234567.9");
}

test "DECIMAL, DATE and TIMESTAMP text is DuckDB 2.0.0's" {
    const a = testing.allocator;
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(a);
    const dec = [_]struct { v: i128, s: u8, want: []const u8 }{
        .{ .v = 150, .s = 2, .want = "1.50" },
        .{ .v = 5, .s = 2, .want = "0.05" },
        .{ .v = -5, .s = 2, .want = "-0.05" },
        .{ .v = 0, .s = 2, .want = "0.00" },
        .{ .v = -1234560, .s = 2, .want = "-12345.60" },
        .{ .v = 12, .s = 0, .want = "12" },
        .{ .v = -3, .s = 0, .want = "-3" },
        .{ .v = 1, .s = 7, .want = "0.0000001" },
        .{ .v = -123456789012345678901234567890123456, .s = 37, .want = "-0.0123456789012345678901234567890123456" },
    };
    for (dec) |c| {
        out.clearRetainingCapacity();
        try writeDecimal(&out, a, c.v, c.s);
        try testing.expectEqualStrings(c.want, out.items);
    }
    const dates = [_]struct { d: i64, want: []const u8 }{
        .{ .d = 18325, .want = "2020-03-04" },
        .{ .d = -719162, .want = "0001-01-01" },
        .{ .d = -719163, .want = "0001-12-31 (BC)" },
        .{ .d = 0, .want = "1970-01-01" },
        .{ .d = std.math.maxInt(i32), .want = "infinity" },
        .{ .d = -std.math.maxInt(i32), .want = "-infinity" },
    };
    for (dates) |c| {
        out.clearRetainingCapacity();
        try writeDate(&out, a, c.d);
        try testing.expectEqualStrings(c.want, out.items);
    }
    const stamps = [_]struct { u: ast.Temporal, z: bool, v: i64, want: []const u8 }{
        .{ .u = .ts_us, .z = false, .v = 1583316672000000, .want = "2020-03-04 10:11:12" },
        .{ .u = .ts_us, .z = false, .v = 1583316672500000, .want = "2020-03-04 10:11:12.5" },
        .{ .u = .ts_us, .z = false, .v = 1583316672000001, .want = "2020-03-04 10:11:12.000001" },
        .{ .u = .ts_us, .z = false, .v = 1583316672123456, .want = "2020-03-04 10:11:12.123456" },
        .{ .u = .ts_us, .z = false, .v = 1583316672120000, .want = "2020-03-04 10:11:12.12" },
        .{ .u = .ts_us, .z = false, .v = -1, .want = "1969-12-31 23:59:59.999999" },
        .{ .u = .ts_us, .z = false, .v = -62135596800000000, .want = "0001-01-01 00:00:00" },
        .{ .u = .ts_us, .z = false, .v = -62167219200000000, .want = "0001-01-01 (BC) 00:00:00" },
        .{ .u = .ts_us, .z = false, .v = std.math.maxInt(i64), .want = "infinity" },
        .{ .u = .ts_ms, .z = false, .v = 1583316672500, .want = "2020-03-04 10:11:12.5" },
        .{ .u = .ts_ms, .z = false, .v = 1583316672120, .want = "2020-03-04 10:11:12.12" },
        .{ .u = .ts_ns, .z = false, .v = 1583316672123456789, .want = "2020-03-04 10:11:12.123456789" },
        .{ .u = .ts_ns, .z = false, .v = 1583316672500000000, .want = "2020-03-04 10:11:12.5" },
        .{ .u = .ts_ns, .z = false, .v = -1, .want = "1969-12-31 23:59:59.999999999" },
        .{ .u = .ts_us, .z = true, .v = 1583316672500000, .want = "2020-03-04 10:11:12.5+00" },
        .{ .u = .ts_us, .z = true, .v = 1583316672000000, .want = "2020-03-04 10:11:12+00" },
    };
    for (stamps) |c| {
        out.clearRetainingCapacity();
        try writeTimestamp(&out, a, c.u, c.z, c.v);
        try testing.expectEqualStrings(c.want, out.items);
    }
}

test "a millisecond count that overflows microseconds is a conversion error in text; a time below the TIMESTAMP minimum prints" {
    var out: std.ArrayList(u8) = .empty;
    defer out.deinit(testing.allocator);
    const a = testing.allocator;
    const min_us: i64 = -9_223_372_022_400_000_000; // DuckDB's documented earliest TIMESTAMP, 290309-12-22 (BC)
    // Milliseconds are read as microseconds first: a count that overflows is an error, the two infinite ones are not.
    try testing.expectError(error.ConversionOutOfRange, writeTimestamp(&out, a, .ts_ms, false, 9223372036854776));
    try testing.expectError(error.ConversionOutOfRange, writeTimestamp(&out, a, .ts_ms, true, 9223372036854776));
    try testing.expectError(error.ConversionOutOfRange, writeTimestamp(&out, a, .ts_ms, false, std.math.minInt(i64)));
    try writeTimestamp(&out, a, .ts_ms, false, std.math.maxInt(i64));
    try testing.expectEqualStrings("infinity", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_ms, false, 9223372036854775);
    try testing.expectEqualStrings("294247-01-10 04:00:54.775", out.items);
    out.clearRetainingCapacity();
    // The minimum and everything below it print, down to the smallest count; the same text, UTC-adjusted or not.
    try writeTimestamp(&out, a, .ts_us, false, min_us);
    try testing.expectEqualStrings("290309-12-22 (BC) 00:00:00", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_us, false, min_us - 1);
    try testing.expectEqualStrings("290309-12-21 (BC) 23:59:59.999999", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_us, false, std.math.minInt(i64));
    try testing.expectEqualStrings("290309-12-21 (BC) 19:59:05.224192", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_ms, false, @divExact(min_us, 1000) - 1);
    try testing.expectEqualStrings("290309-12-21 (BC) 23:59:59.999", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_us, true, min_us - 1);
    try testing.expectEqualStrings("290309-12-21 (BC) 23:59:59.999999+00", out.items);
    out.clearRetainingCapacity();
    try writeTimestamp(&out, a, .ts_ms, true, @divExact(min_us, 1000) - 1);
    try testing.expectEqualStrings("290309-12-21 (BC) 23:59:59.999+00", out.items);
    out.clearRetainingCapacity();
    // Nanoseconds have no floor of their own: the earliest one is 1677.
    try writeTimestamp(&out, a, .ts_ns, false, std.math.minInt(i64));
    try testing.expectEqualStrings("1677-09-21 00:12:43.145224192", out.items);
}
