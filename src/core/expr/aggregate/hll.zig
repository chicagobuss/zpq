//! `approx_count_distinct`: DuckDB 2.0's HyperLogLog, register for register, so the estimate is the one it gives.
//! 2^10 one-byte registers; a row's hash picks a register with its low 10 bits and the register keeps the largest
//! position of the first set bit in the rest (Ertl's "New cardinality estimation algorithms for HyperLogLog sketches",
//! algorithm 1, estimator algorithm 6). Merging two sketches is the per-register maximum, so the estimate is the same
//! for every order of rows, row groups and workers.
const std = @import("std");
const testing = std.testing;
const filter_eval = @import("../../filter/eval.zig");
const expr_ast = @import("../ast.zig");
const schema = @import("../../schema.zig");

pub const precision = 10;
pub const registers = 1 << precision;
/// Hash bits left for the position of the first set bit, and the largest position a register can hold plus none.
const q = 64 - precision;
const alpha = 0.721347520444481703680;

/// DuckDB's `MurmurHash64` (hash.hpp), the finalizer of every value hash below.
pub inline fn murmur64(v: u64) u64 {
    var x = v;
    x ^= x >> 32;
    x *%= 0xd6e8feb86659fd93;
    x ^= x >> 32;
    x *%= 0xd6e8feb86659fd93;
    x ^= x >> 32;
    return x;
}

/// DuckDB's string hash (`Hash(string_t)`, which for an inlined string equals `HashBytes`): eight-byte little-endian
/// blocks folded in, then the rest zero-extended.
pub fn hashBytes(bytes: []const u8) u64 {
    var h: u64 = 0xe17a1465 ^ (@as(u64, bytes.len) *% 0xc6a4a7935bd1e995);
    var i: usize = 0;
    while (i + 8 <= bytes.len) : (i += 8) {
        h ^= std.mem.readInt(u64, bytes[i..][0..8], .little);
        h *%= 0xd6e8feb86659fd93;
    }
    if (i < bytes.len) {
        var rest: [8]u8 = @splat(0);
        @memcpy(rest[0 .. bytes.len - i], bytes[i..]);
        h ^= std.mem.readInt(u64, &rest, .little);
        h *%= 0xd6e8feb86659fd93;
    }
    return murmur64(h);
}

/// How DuckDB hashes a column's values, by the physical width its type has there (not the Parquet one): every integer
/// up to 32 bits, a DATE and a DECIMAL of up to 9 digits as the 32 bits of the value; BIGINT, the timestamps (in their
/// own unit), and a DECIMAL of up to 18 digits as 64; a wider DECIMAL as a HUGEINT. A millisecond timestamp (plain or
/// UTC-adjusted) is read by DuckDB as its microsecond TIMESTAMP, so it hashes its count times 1000.
pub const Kind = enum { w32, w64, ts_ms, w128, f32, f64, boolean, string };

/// The `Kind` of `arg`'s values, or null for a column whose DuckDB type this does not reproduce (a TIME, a UUID, an
/// INTERVAL, an INT96 timestamp, a half float) or an argument that is no column.
pub fn kindOf(file: *const schema.FileMetaData, arg: expr_ast.Expr) ?Kind {
    if (arg != .col_ref) return null;
    const c = arg.col_ref;
    var scratch: schema.LeafIterator = undefined;
    const leaf = file.leafAt(c.col_idx, &scratch) orelse return null;
    if (leaf.max_rep > 0) return null;
    if (leaf.element.converted_type) |ct| if (ct == .INTERVAL) return null;
    if (leaf.element.logical_type == null and leaf.element.logical_type_raw.len != 0) return null;
    if (leaf.element.effectiveLogicalType()) |lt| switch (lt) {
        .TIME, .UUID, .FLOAT16, .UNKNOWN => return null,
        else => {},
    };
    return kindOfColRef(c);
}

/// The `Kind` from a bound column alone (the schema checks of `kindOf` having passed at bind time).
pub fn kindOfColRef(c: expr_ast.ColRef) ?Kind {
    return switch (c.expr_type) {
        .str => .string,
        .f64 => switch (c.physical_type) {
            .FLOAT => .f32,
            .DOUBLE => .f64,
            else => null,
        },
        .dec => blk: {
            const p = (c.dec orelse return null).precision;
            break :blk if (p <= 9) .w32 else if (p <= 18) .w64 else .w128;
        },
        .i64 => switch (c.physical_type) {
            .BOOLEAN => .boolean,
            .INT32 => .w32,
            .INT64 => if (c.temporal == .ts_ms) .ts_ms else .w64,
            else => null,
        },
    };
}

/// The hash of row `r` of `col`, which is not NULL. Floats hash their bits after DuckDB's normalization (every zero
/// positive, every NaN the quiet one).
pub inline fn hashRow(kind: Kind, col: filter_eval.Batch.Column, r: usize) error{ConversionOutOfRange}!u64 {
    switch (kind) {
        .ts_ms => {
            // DuckDB stops at a count its microseconds do not hold ("Could not convert Timestamp(MS) to Timestamp(US)").
            const us = std.math.mul(i64, @intCast(wide(col, r)), 1000) catch return error.ConversionOutOfRange;
            return murmur64(@bitCast(us));
        },
        .w32 => return murmur64(@as(u32, @truncate(@as(u128, @bitCast(wide(col, r)))))),
        .w64 => return murmur64(@as(u64, @truncate(@as(u128, @bitCast(wide(col, r)))))),
        .w128 => {
            const v: u128 = @bitCast(wide(col, r));
            return murmur64(@truncate(v)) ^ murmur64(@truncate(v >> 64));
        },
        .f32 => {
            var v: f32 = switch (col) {
                .f32 => |c| c.values[r],
                .f64 => |c| @floatCast(c.values[r]),
                else => unreachable,
            };
            if (v == 0) v = 0 else if (std.math.isNan(v)) v = std.math.nan(f32);
            return murmur64(@as(u32, @bitCast(v)));
        },
        .f64 => {
            var v: f64 = switch (col) {
                .f32 => |c| c.values[r],
                .f64 => |c| c.values[r],
                else => unreachable,
            };
            if (v == 0) v = 0 else if (std.math.isNan(v)) v = std.math.nan(f64);
            return murmur64(@bitCast(v));
        },
        // A bare BOOLEAN column reaches the fold widened to the integer lane.
        .boolean => return murmur64(@intFromBool(switch (col) {
            .boolean => |c| c.values[r],
            else => wide(col, r) != 0,
        })),
        .string => {
            const c = col.string;
            return hashBytes(if (c.codes) |codes| c.dict[codes[r]] else c.values[r]);
        },
    }
}

/// Row `r` of any integer lane (an exact DECIMAL's included) as an i128; the hash reads the width it needs.
inline fn wide(col: filter_eval.Batch.Column, r: usize) i128 {
    return switch (col) {
        .i32 => |c| c.values[r],
        .i64 => |c| c.values[r],
        .i128 => |c| c.values[r],
        else => unreachable,
    };
}

/// Algorithm 1 on a sketch of `registers` bytes.
pub inline fn insert(sketch: []u8, hash: u64) void {
    const i: usize = @intCast(hash & (registers - 1));
    const h = (hash >> precision) | (@as(u64, 1) << q);
    const z: u8 = @intCast(@ctz(h) + 1);
    sketch[i] = @max(sketch[i], z);
}

/// Algorithm 2: the per-register maximum.
pub fn merge(dst: []u8, src: []const u8) void {
    for (dst, src) |*d, s| d.* = @max(d.*, s);
}

fn sigma(x_in: f64) f64 {
    var x = x_in;
    if (x == 1.0) return std.math.inf(f64);
    var y: f64 = 1;
    var z = x;
    while (true) {
        x *= x;
        const prev = z;
        z += x * y;
        y += y;
        if (prev == z) return z;
    }
}

fn tau(x_in: f64) f64 {
    var x = x_in;
    if (x == 0.0 or x == 1.0) return 0.0;
    var y: f64 = 1.0;
    var z = 1 - x;
    while (true) {
        x = @sqrt(x);
        const prev = z;
        y *= 0.5;
        const d = 1 - x;
        z -= d * d * y;
        if (prev == z) return z / 3;
    }
}

/// Algorithms 4 and 6: the estimate DuckDB's `Count` gives for a sketch (0 for one never touched).
pub fn count(sketch: []const u8) i64 {
    if (sketch.len == 0) return 0;
    var c: [q + 2]u32 = @splat(0);
    for (sketch) |k| c[k] += 1;
    const m: f64 = registers;
    var z = m * tau((m - @as(f64, @floatFromInt(c[q]))) / m);
    var k: usize = q;
    while (k >= 1) : (k -= 1) {
        z += @floatFromInt(c[k]);
        z *= 0.5;
    }
    z += m * sigma(@as(f64, @floatFromInt(c[0])) / m);
    return @intFromFloat(@round(alpha * m * m / z));
}

test "hashes are DuckDB 2.0.0's `hash()`" {
    const T = testing;
    // Integers: up to 32 bits the 32 bits of the value (sign-extended -1 is 0xFFFFFFFF), BIGINT its 64.
    try T.expectEqual(@as(u64, 4717996019076358352), murmur64(1));
    try T.expectEqual(@as(u64, 4739667815145166545), murmur64(0xFFFFFFFF));
    try T.expectEqual(@as(u64, 13381090676719377747), murmur64(255));
    try T.expectEqual(@as(u64, 6774767090898797280), murmur64(@bitCast(@as(i64, -5))));
    const ints = [_]i64{ 1, -1, 255, -5 };
    const col: filter_eval.Batch.Column = .{ .i64 = .{ .values = &ints } };
    try T.expectEqual(@as(u64, 4717996019076358352), try hashRow(.w32, col, 0));
    try T.expectEqual(@as(u64, 4739667815145166545), try hashRow(.w32, col, 1));
    try T.expectEqual(@as(u64, 13381090676719377747), try hashRow(.w32, col, 2));
    try T.expectEqual(@as(u64, 6774767090898797280), try hashRow(.w64, col, 3));
    // A millisecond timestamp hashes as the microseconds DuckDB reads it as; one its microseconds cannot hold is an error.
    const ms = [_]i64{ 5, -3, std.math.maxInt(i64) / 1000 + 1 };
    const ms_col: filter_eval.Batch.Column = .{ .i64 = .{ .values = &ms } };
    try T.expectEqual(murmur64(5000), try hashRow(.ts_ms, ms_col, 0));
    try T.expectEqual(murmur64(@bitCast(@as(i64, -3000))), try hashRow(.ts_ms, ms_col, 1));
    try T.expectError(error.ConversionOutOfRange, hashRow(.ts_ms, ms_col, 2));
    // A HUGEINT (a DECIMAL past 18 digits): the hash of the low half xor the hash of the high.
    const wide_vals = [_]i128{ 123456789012345678901234567890, -3, -12345 };
    const wide_col: filter_eval.Batch.Column = .{ .i128 = .{ .values = &wide_vals } };
    try T.expectEqual(@as(u64, 14671742139295629465), try hashRow(.w128, wide_col, 0));
    try T.expectEqual(@as(u64, 7369585739780452761), try hashRow(.w128, wide_col, 1));
    try T.expectEqual(@as(u64, 8365155709346026511), try hashRow(.w128, wide_col, 2));
    // Floats: bits after the zero and NaN normalization.
    const dbl = [_]f64{ 1.5, 0.0, -0.0, std.math.nan(f64) };
    const dcol: filter_eval.Batch.Column = .{ .f64 = .{ .values = &dbl } };
    try T.expectEqual(@as(u64, 1706605666616485939), try hashRow(.f64, dcol, 0));
    try T.expectEqual(@as(u64, 0), try hashRow(.f64, dcol, 1));
    try T.expectEqual(@as(u64, 0), try hashRow(.f64, dcol, 2));
    try T.expectEqual(@as(u64, 9170934016072976158), try hashRow(.f64, dcol, 3));
    const fl = [_]f32{1.5};
    try T.expectEqual(@as(u64, 877323241837685928), try hashRow(.f32, .{ .f32 = .{ .values = &fl } }, 0));
    const bools = [_]bool{ true, false };
    try T.expectEqual(@as(u64, 4717996019076358352), try hashRow(.boolean, .{ .boolean = .{ .values = &bools } }, 0));
    try T.expectEqual(@as(u64, 0), try hashRow(.boolean, .{ .boolean = .{ .values = &bools } }, 1));
    // Strings: empty, under, at, over eight bytes, an inlined twelve and a longer one.
    try T.expectEqual(@as(u64, 5104928228550385088), hashBytes(""));
    try T.expectEqual(@as(u64, 12561829011207016135), hashBytes("a"));
    try T.expectEqual(@as(u64, 8628213525773890682), hashBytes("hello"));
    try T.expectEqual(@as(u64, 10586210142991718607), hashBytes("abcdefgh"));
    try T.expectEqual(@as(u64, 5942002808662391075), hashBytes("abcdefghijkl"));
    try T.expectEqual(@as(u64, 282108676135603331), hashBytes("abcdefghijklmnop"));
    try T.expectEqual(@as(u64, 245041878005366112), hashBytes("a long string of more than twelve bytes"));
}

test "the sketch: DuckDB's registers and estimate" {
    const T = testing;
    var sketch: [registers]u8 = @splat(0);
    try T.expectEqual(@as(i64, 0), count(&sketch));
    // `approx_count_distinct(i)` over range(n) of DuckDB 2.0.0 (hashes of BIGINT).
    const want = [_]struct { n: u64, est: i64 }{ .{ .n = 1, .est = 1 }, .{ .n = 10, .est = 10 }, .{ .n = 1000, .est = 978 }, .{ .n = 100000, .est = 99467 } };
    var n: u64 = 0;
    for (want) |w| {
        while (n < w.n) : (n += 1) insert(&sketch, murmur64(n));
        try T.expectEqual(w.est, count(&sketch));
    }
    // Merging is the per-register maximum: halves of the range give the whole's sketch.
    var lo: [registers]u8 = @splat(0);
    var hi: [registers]u8 = @splat(0);
    var whole: [registers]u8 = @splat(0);
    for (0..5000) |i| insert(&lo, murmur64(i));
    for (5000..10000) |i| insert(&hi, murmur64(i));
    for (0..10000) |i| insert(&whole, murmur64(i));
    merge(&lo, &hi);
    try T.expectEqualSlices(u8, &whole, &lo);
}
