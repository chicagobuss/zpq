//! `arg_max(arg, val)` and `arg_min(arg, val)` (also `max_by`, `min_by`): the `arg` of the row with the greatest or
//! least `val`, as DuckDB 2.0's default null handling does it. A row with a NULL `arg` or a NULL `val` is ignored; the
//! first row with the best `val` keeps it (only a strictly better one replaces it), and partials merge in input order
//! with the same rule, so the answer is the first such row of the scan, the same at every `-j`. The state is one record
//! in a blob: the ordering and direction, the best `val`, and its `arg`.
const std = @import("std");
const testing = std.testing;

/// How `val`s compare.
pub const Order = enum(u8) { signed, unsigned, double, text, wide };

/// Bytes before the value: the order, the direction (1 for `arg_min`), and the length of the value.
pub const header = 6;

/// The record for `arg` at `val`, written into `out`, which holds `size(val.len, arg.len)` bytes.
pub fn write(out: []u8, order: Order, is_min: bool, val: []const u8, arg: []const u8) void {
    out[0] = @intFromEnum(order);
    out[1] = @intFromBool(is_min);
    std.mem.writeInt(u32, out[2..6], @intCast(val.len), .little);
    @memcpy(out[header..][0..val.len], val);
    @memcpy(out[header + val.len ..][0..arg.len], arg);
}

pub fn size(val_len: usize, arg_len: usize) usize {
    return header + val_len + arg_len;
}

pub fn orderOf(rec: []const u8) Order {
    return @enumFromInt(rec[0]);
}

pub fn isMin(rec: []const u8) bool {
    return rec[1] != 0;
}

pub fn valOf(rec: []const u8) []const u8 {
    const n = std.mem.readInt(u32, rec[2..6], .little);
    return rec[header..][0..n];
}

pub fn argOf(rec: []const u8) []const u8 {
    return rec[header + valOf(rec).len ..];
}

/// A DOUBLE in DuckDB's order: NaN is greater than every number, and equal to itself.
fn doubleOrder(a: f64, b: f64) std.math.Order {
    const an = std.math.isNan(a);
    const bn = std.math.isNan(b);
    if (an or bn) return if (an and bn) .eq else if (an) .gt else .lt;
    return std.math.order(a, b);
}

/// Whether value `a` beats `b` under `order`: strictly greater for `arg_max`, strictly less for `arg_min`.
pub fn beats(order: Order, is_min_dir: bool, a: []const u8, b: []const u8) bool {
    const ord: std.math.Order = switch (order) {
        .signed => std.math.order(std.mem.readInt(i64, a[0..8], .little), std.mem.readInt(i64, b[0..8], .little)),
        .unsigned => std.math.order(std.mem.readInt(u64, a[0..8], .little), std.mem.readInt(u64, b[0..8], .little)),
        .double => doubleOrder(@bitCast(std.mem.readInt(u64, a[0..8], .little)), @bitCast(std.mem.readInt(u64, b[0..8], .little))),
        .text => std.mem.order(u8, a, b),
        .wide => std.math.order(std.mem.readInt(i128, a[0..16], .little), std.mem.readInt(i128, b[0..16], .little)),
    };
    return if (is_min_dir) ord == .lt else ord == .gt;
}

/// The sixteen bytes an exact DECIMAL's unscaled value is held as.
pub fn bytes16(x: i128) [16]u8 {
    var out: [16]u8 = undefined;
    std.mem.writeInt(i128, &out, x, .little);
    return out;
}

/// The eight bytes an integer or a double is held as.
pub fn bytes8(x: anytype) [8]u8 {
    var out: [8]u8 = undefined;
    switch (@TypeOf(x)) {
        i64 => std.mem.writeInt(i64, &out, x, .little),
        f64 => std.mem.writeInt(u64, &out, @bitCast(x), .little),
        else => comptime unreachable,
    }
    return out;
}

test "arg_max and arg_min order values as DuckDB does" {
    const T = testing;
    const one = bytes8(@as(i64, 1));
    const three = bytes8(@as(i64, 3));
    const minus = bytes8(@as(i64, -1));
    try T.expect(beats(.signed, false, &three, &one));
    try T.expect(!beats(.signed, false, &one, &one));
    try T.expect(beats(.signed, true, &minus, &one));
    // As unsigned, -1 is the greatest.
    try T.expect(beats(.unsigned, false, &minus, &three));
    const nan = bytes8(std.math.nan(f64));
    const inf = bytes8(std.math.inf(f64));
    try T.expect(beats(.double, false, &nan, &inf));
    try T.expect(!beats(.double, false, &nan, &nan));
    try T.expect(beats(.double, true, &inf, &nan));
    try T.expect(!beats(.double, false, &bytes8(@as(f64, 0.0)), &bytes8(@as(f64, -0.0))));
    const wide_lo = bytes16(-5);
    const wide_hi = bytes16(@as(i128, 1) << 100);
    try T.expect(beats(.wide, false, &wide_hi, &wide_lo));
    try T.expect(beats(.wide, true, &wide_lo, &wide_hi));
    try T.expect(beats(.text, false, "c", "b"));
    try T.expect(beats(.text, false, "ba", "b"));
    try T.expect(beats(.text, true, "a", "b"));
    var rec: [size(3, 2)]u8 = undefined;
    write(&rec, .text, true, "abc", "xy");
    try T.expectEqual(Order.text, orderOf(&rec));
    try T.expect(isMin(&rec));
    try T.expectEqualStrings("abc", valOf(&rec));
    try T.expectEqualStrings("xy", argOf(&rec));
}
