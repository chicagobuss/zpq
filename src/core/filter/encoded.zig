//! Range predicates over Parquet statistics: whether a chunk or page whose bounds are `[min, max]` could hold, or
//! must hold only, values satisfying `value op needle`.
//!
//! Bounds arrive as Parquet bytes; the needle arrives typed, as the filter parser built it.
//!   - INT32/INT64/FLOAT/DOUBLE: decode the bounds with `readFixedLE` and compare numerically. Raw little-endian
//!     byte order does not match numeric order for multi-byte values.
//!   - BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY: compare lexicographically.
//!   - FLOAT/DOUBLE bounds containing NaN are unusable. Ordered comparisons with NaN are false, which callers would
//!     otherwise interpret as permission to prune the page.

const std = @import("std");
const ast = @import("ast.zig");

/// True iff a chunk bounded by `[min, max]` could hold a value satisfying `value op needle`. False negatives would
/// lose rows; false positives only cost a decode, so bounds that do not read as `T` intersect everything.
pub fn rangeIntersects(comptime T: type, op: ast.Operator, min: []const u8, max: []const u8, needle: T) bool {
    const min_v = readFixedLE(T, min) orelse return true;
    const max_v = readFixedLE(T, max) orelse return true;
    if (comptime @typeInfo(T) == .float) {
        // Real files sometimes contain NaN statistics despite the spec.
        // Unordered bounds cannot safely prove that a page misses the filter.
        if (std.math.isNan(min_v) or std.math.isNan(max_v)) return true;
        // Bounds leave NaN out, and NaN != x holds, so `lo == hi == x` does not rule out a match. Only a nan_count
        // of zero does; callers that have one decide that case themselves.
        if (op == .NotEq) return true;
    }
    return rangeOverlapsValue(T, op, min_v, max_v, needle);
}

/// True iff every value in `[min, max]` satisfies `value op needle`: the positive assertion behind `.always_match`.
pub fn rangeAlwaysMatches(comptime T: type, op: ast.Operator, min: []const u8, max: []const u8, needle: T) bool {
    // FLOAT/DOUBLE: a positive assertion from min/max is UNSAFE. NaNs may be excluded from (legacy) bounds, so
    // [min,max] can look like it covers every value while NaN rows silently fail the predicate.
    if (comptime @typeInfo(T) == .float) return false;
    const min_v = readFixedLE(T, min) orelse return false;
    const max_v = readFixedLE(T, max) orelse return false;
    if (min_v > max_v) return false; // corrupt stats
    return switch (op) {
        .Eq => min_v == max_v and min_v == needle,
        .NotEq => needle < min_v or needle > max_v,
        .Lt => max_v < needle,
        .LtEq => max_v <= needle,
        .Gt => min_v > needle,
        .GtEq => min_v >= needle,
    };
}

pub fn readFixedLE(comptime T: type, bytes: []const u8) ?T {
    const sz = @sizeOf(T);
    if (bytes.len != sz) return null;
    return switch (T) {
        i32, i64 => std.mem.readInt(T, bytes[0..sz], .little),
        f32 => @bitCast(std.mem.readInt(u32, bytes[0..4], .little)),
        f64 => @bitCast(std.mem.readInt(u64, bytes[0..8], .little)),
        else => @compileError("readFixedLE: unsupported type"),
    };
}

pub fn rangeOverlapsValue(comptime T: type, op: ast.Operator, lo: T, hi: T, needle: T) bool {
    return switch (op) {
        // [lo, hi] intersects {x : x == needle} iff lo <= needle <= hi
        .Eq => !(needle < lo or needle > hi),
        // {x : x != needle} is everything except {needle}; range
        // intersects unless lo == hi == needle.
        .NotEq => !(lo == hi and lo == needle),
        // [lo, hi] intersects {x : x < needle} iff lo < needle.
        .Lt => lo < needle,
        .LtEq => lo <= needle,
        .Gt => hi > needle,
        .GtEq => hi >= needle,
    };
}

/// True iff every value of a chunk bounded by `[min, max]` satisfies `value op needle` in unsigned bytewise order.
/// A truncated `min` is a prefix of the true minimum, so still a lower bound, and proofs needing only `min` always
/// hold. A truncated `max` is an upper bound only if the writer rounded it up, so proofs needing `max` require the
/// writer's `is_max_value_exact`.
pub fn rangeAlwaysMatchesBytes(
    op: ast.Operator,
    min: []const u8,
    max: []const u8,
    max_exact: bool,
    needle: []const u8,
) bool {
    if (std.mem.lessThan(u8, max, min)) return false; // corrupt stats
    return switch (op) {
        .Gt => std.mem.lessThan(u8, needle, min),
        .GtEq => !std.mem.lessThan(u8, min, needle),
        .Lt => max_exact and std.mem.lessThan(u8, max, needle),
        .LtEq => max_exact and !std.mem.lessThan(u8, needle, max),
        .Eq => max_exact and std.mem.eql(u8, min, needle) and std.mem.eql(u8, max, needle),
        .NotEq => std.mem.lessThan(u8, needle, min) or (max_exact and std.mem.lessThan(u8, max, needle)),
    };
}

pub fn rangeIntersectsBytes(op: ast.Operator, min: []const u8, max: []const u8, needle: []const u8) bool {
    return switch (op) {
        .Eq => !(std.mem.lessThan(u8, needle, min) or std.mem.lessThan(u8, max, needle)),
        .NotEq => !(std.mem.eql(u8, min, max) and std.mem.eql(u8, min, needle)),
        .Lt => std.mem.lessThan(u8, min, needle),
        .LtEq => !std.mem.lessThan(u8, needle, min),
        .Gt => std.mem.lessThan(u8, needle, max),
        .GtEq => !std.mem.lessThan(u8, max, needle),
    };
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

fn le(comptime T: type, v: T) [@sizeOf(T)]u8 {
    var b: [@sizeOf(T)]u8 = undefined;
    const U = @Int(.unsigned, @bitSizeOf(T));
    std.mem.writeInt(U, &b, @bitCast(v), .little);
    return b;
}

test "rangeIntersects INT64: Eq inside and outside the range" {
    const min = le(i64, 50);
    const max = le(i64, 200);
    try testing.expect(rangeIntersects(i64, .Eq, &min, &max, 100));
    try testing.expect(!rangeIntersects(i64, .Eq, &min, &max, 300));
}

test "rangeIntersects negative INT32 (sign-bit edge)" {
    // Range [-100, 100], filter Eq -50 → should match.
    const min = le(i32, -100);
    const max = le(i32, 100);
    try testing.expect(rangeIntersects(i32, .Eq, &min, &max, -50));
    try testing.expect(rangeIntersects(i32, .Lt, &min, &max, -50)); // -100 < -50
    try testing.expect(rangeIntersects(i32, .Gt, &min, &max, -50)); // 100 > -50
}

test "rangeIntersects keeps bounds that do not read as the column type" {
    const short = [_]u8{ 1, 2, 3 };
    const max = le(i64, 0);
    try testing.expect(rangeIntersects(i64, .Eq, &short, &max, 1_000));
    try testing.expect(!rangeAlwaysMatches(i64, .GtEq, &short, &max, -1_000));
}

test "rangeIntersects DOUBLE keeps pages with NaN bounds" {
    const nan = le(f64, std.math.nan(f64));
    const real = le(f64, 100.0);
    const ops = [_]ast.Operator{ .Eq, .NotEq, .Lt, .LtEq, .Gt, .GtEq };
    for (ops) |op| {
        try testing.expect(rangeIntersects(f64, op, &nan, &real, 5.0));
        try testing.expect(rangeIntersects(f64, op, &real, &nan, 5.0));
        try testing.expect(rangeIntersects(f64, op, &nan, &nan, 5.0));
    }
}

test "rangeIntersects FLOAT keeps pages with NaN bounds" {
    const nan = le(f32, std.math.nan(f32));
    const real = le(f32, 100.0);
    try testing.expect(rangeIntersects(f32, .Lt, &nan, &real, 5.0));
    try testing.expect(rangeIntersects(f32, .Gt, &real, &nan, 5.0));
}

test "rangeIntersects float != keeps a constant range equal to the literal: NaN rows sit outside the bounds" {
    const zero = le(f64, 0.0);
    try testing.expect(rangeIntersects(f64, .NotEq, &zero, &zero, 0.0));
    try testing.expect(!rangeIntersects(f64, .Lt, &zero, &zero, 0.0));
    // Integers have no NaN: a constant chunk equal to the literal still fails `!=` everywhere.
    const izero = le(i64, 0);
    try testing.expect(!rangeIntersects(i64, .NotEq, &izero, &izero, 0));
}

test "rangeIntersects DOUBLE still prunes real disjoint bounds" {
    const min = le(f64, 10.0);
    const max = le(f64, 100.0);
    try testing.expect(!rangeIntersects(f64, .Lt, &min, &max, 5.0));
    try testing.expect(!rangeIntersects(f64, .Eq, &min, &max, 5.0));
    try testing.expect(rangeIntersects(f64, .Gt, &min, &max, 5.0));
}

test "rangeAlwaysMatches: integers prove a full match, floats never do" {
    const min = le(i32, 10);
    const max = le(i32, 20);
    try testing.expect(rangeAlwaysMatches(i32, .GtEq, &min, &max, 10));
    try testing.expect(!rangeAlwaysMatches(i32, .Gt, &min, &max, 10));
    try testing.expect(rangeAlwaysMatches(i32, .NotEq, &min, &max, 21));
    try testing.expect(!rangeAlwaysMatches(i32, .Lt, &max, &min, 30)); // corrupt: min > max
    const fmin = le(f64, 10.0);
    const fmax = le(f64, 20.0);
    try testing.expect(!rangeAlwaysMatches(f64, .GtEq, &fmin, &fmax, 0.0));
}

test "rangeIntersectsBytes — Eq" {
    try testing.expect(rangeIntersectsBytes(.Eq, "category_0", "category_9", "category_5"));
    try testing.expect(!rangeIntersectsBytes(.Eq, "category_a", "category_z", "category_5"));
    try testing.expect(!rangeIntersectsBytes(.Eq, "alpha", "beta", "category_5"));
}
