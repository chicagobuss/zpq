//! EncodedFilter — pre-encode the filter value once into Parquet's
//! binary representation so row-group / page stats (also stored as
//! Parquet bytes in metadata) become byte-comparable.
//!
//! The trick:
//!   - Equality: `min <= needle <= max` works as raw byte comparison
//!     for *all* fixed-width Parquet types and BYTE_ARRAY. The Parquet
//!     spec defines its statistics ordering as the natural type order,
//!     and for unsigned widths + lexicographic byte order, that
//!     coincides with little-endian byte comparison.
//!   - Range: signed integers (INT32, INT64) need type-aware compare
//!     because LE byte order disagrees with numeric order across the
//!     sign bit. We branch on physical type for range checks.
//!
//! We encode once per filter rather than per row group; a typical
//! query touches dozens of row groups so the savings compound.

const std = @import("std");
const schema = @import("../schema.zig");
const ast = @import("ast.zig");

pub const Error = error{
    UnsupportedType,
    BadValue,
} || std.mem.Allocator.Error;

pub const EncodedValue = struct {
    bytes: []const u8,
    parquet_type: schema.Type,

    /// True iff a row whose stats range is `[min, max]` could possibly
    /// satisfy `column op self.bytes`. Returns conservatively false-
    /// negatives are not allowed (would lose data); false-positives
    /// are fine (we'll re-evaluate value-level).
    pub fn rangeIntersects(self: EncodedValue, op: ast.Operator, min: []const u8, max: []const u8) bool {
        switch (self.parquet_type) {
            // Signed types: compare numerically because byte ordering
            // disagrees across the sign bit.
            .INT32 => return rangeIntersectsSigned(i32, op, min, max, self.bytes),
            .INT64 => return rangeIntersectsSigned(i64, op, min, max, self.bytes),
            .FLOAT => return rangeIntersectsSigned(f32, op, min, max, self.bytes),
            .DOUBLE => return rangeIntersectsSigned(f64, op, min, max, self.bytes),
            // BYTE_ARRAY + FIXED_LEN_BYTE_ARRAY use lexicographic byte
            // comparison natively.
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => return rangeIntersectsBytes(op, min, max, self.bytes),
            .BOOLEAN => {
                // Booleans only really do equality. Parquet stats for
                // a bool column with mixed values have min=0 max=1.
                if (op == .Eq) return rangeIntersectsBytes(.Eq, min, max, self.bytes);
                if (op == .NotEq) return true; // can't prune
                return true;
            },
            .INT96 => return true, // legacy / deprecated; conservative
        }
    }

    /// True iff ALL values in `[min, max]` satisfy `column op self.bytes`.
    /// Used for `.always_match` positive assertion page pruning.
    pub fn rangeAlwaysMatches(self: EncodedValue, op: ast.Operator, min: []const u8, max: []const u8) bool {
        switch (self.parquet_type) {
            .INT32 => return rangeAlwaysMatchesSigned(i32, op, min, max, self.bytes),
            .INT64 => return rangeAlwaysMatchesSigned(i64, op, min, max, self.bytes),
            // FLOAT/DOUBLE: a positive assertion from min/max is UNSAFE. NaNs
            // may be excluded from (legacy) bounds, so [min,max] can look like
            // it covers every value while NaN rows silently fail the predicate.
            // Proving NaN absence needs `nan_count`, which zpq does not parse;
            // per Apache Parquet's IEEE-754 total-order guidance a missing
            // nan_count must be treated as unknown. Never claim always_match.
            .FLOAT, .DOUBLE => return false,
            // Truncated bounds makes BYTE_ARRAY/FIXED_LEN_BYTE_ARRAY always_match unsafe,
            // so we return false for these in v1.
            .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => return false,
            .BOOLEAN => return false,
            .INT96 => return false,
        }
    }
};

fn rangeAlwaysMatchesSigned(comptime T: type, op: ast.Operator, min: []const u8, max: []const u8, needle: []const u8) bool {
    const min_v = readFixedLE(T, min) orelse return false;
    const max_v = readFixedLE(T, max) orelse return false;
    const needle_v = readFixedLE(T, needle) orelse return false;
    if (min_v > max_v) return false; // corrupt stats
    return switch (op) {
        .Eq => min_v == max_v and min_v == needle_v,
        .NotEq => needle_v < min_v or needle_v > max_v,
        .Lt => max_v < needle_v,
        .LtEq => max_v <= needle_v,
        .Gt => min_v > needle_v,
        .GtEq => min_v >= needle_v,
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

fn rangeIntersectsSigned(comptime T: type, op: ast.Operator, min: []const u8, max: []const u8, needle: []const u8) bool {
    const min_v = readFixedLE(T, min) orelse return true;
    const max_v = readFixedLE(T, max) orelse return true;
    const needle_v = readFixedLE(T, needle) orelse return true;
    return rangeOverlapsValue(T, op, min_v, max_v, needle_v);
}

fn rangeOverlapsValue(comptime T: type, op: ast.Operator, lo: T, hi: T, needle: T) bool {
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

/// Parse a string filter value into Parquet's on-the-wire byte
/// representation for the column's type.
pub fn encode(allocator: std.mem.Allocator, value_str: []const u8, parquet_type: schema.Type) Error!EncodedValue {
    switch (parquet_type) {
        .INT32 => {
            const v = std.fmt.parseInt(i32, value_str, 10) catch return error.BadValue;
            const bytes = try allocator.alloc(u8, 4);
            std.mem.writeInt(i32, bytes[0..4], v, .little);
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .INT64 => {
            const v = std.fmt.parseInt(i64, value_str, 10) catch return error.BadValue;
            const bytes = try allocator.alloc(u8, 8);
            std.mem.writeInt(i64, bytes[0..8], v, .little);
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .FLOAT => {
            const v = std.fmt.parseFloat(f32, value_str) catch return error.BadValue;
            const bytes = try allocator.alloc(u8, 4);
            std.mem.writeInt(u32, bytes[0..4], @bitCast(v), .little);
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .DOUBLE => {
            const v = std.fmt.parseFloat(f64, value_str) catch return error.BadValue;
            const bytes = try allocator.alloc(u8, 8);
            std.mem.writeInt(u64, bytes[0..8], @bitCast(v), .little);
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .BYTE_ARRAY, .FIXED_LEN_BYTE_ARRAY => {
            const bytes = try allocator.dupe(u8, value_str);
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .BOOLEAN => {
            const truthy = std.mem.eql(u8, value_str, "true") or std.mem.eql(u8, value_str, "1");
            const falsy = std.mem.eql(u8, value_str, "false") or std.mem.eql(u8, value_str, "0");
            if (!truthy and !falsy) return error.BadValue;
            const bytes = try allocator.alloc(u8, 1);
            bytes[0] = if (truthy) 1 else 0;
            return .{ .bytes = bytes, .parquet_type = parquet_type };
        },
        .INT96 => return error.UnsupportedType,
    }
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "encode INT64 then byte-compare" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const ev = try encode(arena.allocator(), "12345", .INT64);
    try testing.expectEqual(@as(usize, 8), ev.bytes.len);
    const decoded = std.mem.readInt(i64, ev.bytes[0..8], .little);
    try testing.expectEqual(@as(i64, 12345), decoded);
}

test "encode BYTE_ARRAY copies the bytes" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const ev = try encode(arena.allocator(), "active", .BYTE_ARRAY);
    try testing.expectEqualStrings("active", ev.bytes);
}

test "encode BOOLEAN accepts true/false/1/0" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const t1 = try encode(arena.allocator(), "true", .BOOLEAN);
    try testing.expectEqual(@as(u8, 1), t1.bytes[0]);
    const t0 = try encode(arena.allocator(), "false", .BOOLEAN);
    try testing.expectEqual(@as(u8, 0), t0.bytes[0]);
    try testing.expectError(error.BadValue, encode(arena.allocator(), "yes", .BOOLEAN));
}

test "rangeIntersects with INT64 stats — Eq inside range" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const ev = try encode(arena.allocator(), "100", .INT64);

    var min_buf: [8]u8 = undefined;
    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &min_buf, 50, .little);
    std.mem.writeInt(i64, &max_buf, 200, .little);

    try testing.expect(ev.rangeIntersects(.Eq, &min_buf, &max_buf));
}

test "rangeIntersects with INT64 stats — Eq outside range" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const ev = try encode(arena.allocator(), "300", .INT64);

    var min_buf: [8]u8 = undefined;
    var max_buf: [8]u8 = undefined;
    std.mem.writeInt(i64, &min_buf, 50, .little);
    std.mem.writeInt(i64, &max_buf, 200, .little);

    try testing.expect(!ev.rangeIntersects(.Eq, &min_buf, &max_buf));
}

test "rangeIntersects negative INT32 (sign-bit edge)" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // Range [-100, 100], filter Eq -50 → should match.
    const ev = try encode(arena.allocator(), "-50", .INT32);

    var min_buf: [4]u8 = undefined;
    var max_buf: [4]u8 = undefined;
    std.mem.writeInt(i32, &min_buf, -100, .little);
    std.mem.writeInt(i32, &max_buf, 100, .little);

    try testing.expect(ev.rangeIntersects(.Eq, &min_buf, &max_buf));
    try testing.expect(ev.rangeIntersects(.Lt, &min_buf, &max_buf)); // -100 < -50
    try testing.expect(ev.rangeIntersects(.Gt, &min_buf, &max_buf)); // 100 > -50
}

test "rangeIntersects BYTE_ARRAY — Eq" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    const ev = try encode(arena.allocator(), "category_5", .BYTE_ARRAY);

    try testing.expect(ev.rangeIntersects(.Eq, "category_0", "category_9"));
    try testing.expect(!ev.rangeIntersects(.Eq, "category_a", "category_z"));
    try testing.expect(!ev.rangeIntersects(.Eq, "alpha", "beta"));
}
