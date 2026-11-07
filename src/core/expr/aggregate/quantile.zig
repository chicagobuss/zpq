//! `median` and `quantile_cont`: DuckDB 2.0's continuous quantile of a list of doubles. The values are sorted (NaN last,
//! as DuckDB orders doubles), the position is `(n - 1) * q`, and a position between two values interpolates them as
//! its compiled `lo * (1 - d) + hi * d` does, with the fused multiply-add the x86-64 build contracts it to.
const std = @import("std");
const testing = std.testing;

fn lessNanLast(_: void, a: f64, b: f64) bool {
    if (std.math.isNan(a)) return false;
    if (std.math.isNan(b)) return true;
    return a < b;
}

/// The quantile `q` (0 to 1) of `values`, sorted in place; null over none.
pub fn cont(values: []f64, q: f64) ?f64 {
    if (values.len == 0) return null;
    std.sort.pdq(f64, values, {}, lessNanLast);
    const rn = @as(f64, @floatFromInt(values.len - 1)) * q;
    const frn: usize = @intFromFloat(@floor(rn));
    const crn: usize = @intFromFloat(@ceil(rn));
    if (frn == crn) return values[frn];
    const d = rn - @as(f64, @floatFromInt(frn));
    return @mulAdd(f64, values[frn], 1.0 - d, values[crn] * d);
}

test "quantile_cont is DuckDB 2.0.0's" {
    const T = testing;
    var odd = [_]f64{ 7, 1, 3, 9, 5 };
    try T.expectEqual(@as(?f64, 5), cont(&odd, 0.5));
    try T.expectEqual(@as(?f64, 1), cont(&odd, 0));
    try T.expectEqual(@as(?f64, 9), cont(&odd, 1));
    var even = [_]f64{ 1.1, 2.2, 7.3, 9.9 };
    try T.expectEqual(@as(?f64, 4.75), cont(&even, 0.5));
    // A quantile between two values, and NaN and infinity: NaN sorts last, infinity interpolates to itself.
    var spread = [_]f64{ 1.5, std.math.nan(f64), 2.5, std.math.inf(f64) };
    try T.expectEqual(@as(?f64, std.math.inf(f64)), cont(&spread, 0.5));
    try T.expect(std.math.isNan(cont(&spread, 1).?));
    var none = [_]f64{};
    try T.expectEqual(@as(?f64, null), cont(&none, 0.5));
}
