const std = @import("std");
const expr_ast = @import("../expr/ast.zig");

pub const AggValue = union(enum) {
    /// Single integer result (count, integer sum/min/max). i128 so a sum that
    /// overflows i64 — or an unsigned-64 value/extremum up to 2^64-1 — is
    /// representable (DuckDB likewise widens int SUM to HUGEINT). Counts and
    /// signed i64 values widen in losslessly.
    i: i128,
    /// Single f64 result (float sum/min/max, avg's mean).
    f: f64,
    /// avg_state's split-output: ship sum + count, caller divides at end. The sum
    /// is NULL when count is 0, as sum() over no values is.
    avg: struct { sum: ?f64, count: i64 },
    /// String (bytewise/unsigned) min/max result or string group key. Owned by the `AggOutput` holding it.
    s: []const u8,
    /// Exact DECIMAL: unscaled value and its type. Printed as exact text, written as DECIMAL. Also avg's mean.
    dec: DecValue,
    /// avg_state over a DECIMAL: the exact sum and count, which re-aggregate, and the mean rounded to the argument's
    /// type, which only ranks and filters (HAVING) the rows, never printed.
    /// The sum may pass 38 digits while the mean fits (two values near 10^38); JSON prints it whole, and only `-o`,
    /// whose `__sum` column is DECIMAL(38, s), refuses it. `sum` and `mean` are NULL when count is 0.
    avg_dec: struct { sum: ?i192, count: i64, mean: ?i128, spec: expr_ast.Dec },
    null_val: void,
};

pub const DecValue = struct { v: i128, spec: expr_ast.Dec };

pub const AggOutputItem = struct {
    alias: []const u8,
    value: AggValue,
};

/// How two result values of one column sort. NULL last in either direction; NaN above every number, as zpq's float
/// min/max order it; strings bytewise; DECIMAL exactly.
pub fn orderValues(a: AggValue, b: AggValue, descending: bool) std.math.Order {
    return orderSortKeys(sortKey(a), sortKey(b), descending);
}

pub fn orderSortKeys(a: SortKey, b: SortKey, descending: bool) std.math.Order {
    const a_null = a == .none;
    const b_null = b == .none;
    if (a_null or b_null) return if (a_null == b_null) .eq else if (a_null) .gt else .lt;
    const ord = compareKeys(a, b);
    return if (descending) ord.invert() else ord;
}

pub const SortKey = union(enum) { none, int: i128, float: f64, dec: DecValue, bytes: []const u8 };

pub fn sortKey(v: AggValue) SortKey {
    return switch (v) {
        .i => |i| .{ .int = i },
        .f => |f| .{ .float = f },
        .avg => |a| if (a.sum) |s| .{ .float = s / @as(f64, @floatFromInt(a.count)) } else .none,
        .dec => |d| .{ .dec = d },
        .avg_dec => |a| if (a.mean) |m| .{ .dec = .{ .v = m, .spec = a.spec } } else .none,
        .s => |s| .{ .bytes = s },
        .null_val => .none,
    };
}

fn compareKeys(a: SortKey, b: SortKey) std.math.Order {
    return switch (a) {
        .bytes => |x| if (b == .bytes) std.mem.order(u8, x, b.bytes) else .gt,
        .dec => |x| if (b == .dec and b.dec.spec.scale == x.spec.scale)
            std.math.order(x.v, b.dec.v)
        else if (b == .bytes)
            .lt
        else
            compareFloats(asFloat(a), asFloat(b)),
        .int => |x| if (b == .int) std.math.order(x, b.int) else if (b == .bytes) .lt else compareFloats(asFloat(a), asFloat(b)),
        .float => if (b == .bytes) .lt else compareFloats(asFloat(a), asFloat(b)),
        .none => unreachable,
    };
}

fn asFloat(k: SortKey) f64 {
    return switch (k) {
        .int => |i| @floatFromInt(i),
        .float => |f| f,
        .dec => |d| @as(f64, @floatFromInt(d.v)) / std.math.pow(f64, 10, @floatFromInt(d.spec.scale)),
        .none, .bytes => unreachable,
    };
}

fn compareFloats(a: f64, b: f64) std.math.Order {
    if (std.math.isNan(a) or std.math.isNan(b)) {
        return if (std.math.isNan(a) == std.math.isNan(b)) .eq else if (std.math.isNan(a)) .gt else .lt;
    }
    return std.math.order(a, b);
}

