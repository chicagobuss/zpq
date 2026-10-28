const std = @import("std");
const testing = std.testing;
const expr_ast = @import("../expr/ast.zig");
const AggValue = @import("output_values.zig").AggValue;

/// How one group-key column is framed in a serialized composite key.
///
/// These are the only three shapes `expr_agg.serializeRowKey` can write.
/// `evalGroupKeyExpr` builds key columns from the EXPRESSION type (three
/// lanes), and `serializeRowKey` widens BOOLEAN into the i64 lane so a
/// caller that skips eval still matches `deserializeKeyColumn`. Deriving
/// framing from the physical Parquet type instead let the two disagree: a
/// BOOLEAN column evaluates to i64 (8 bytes) while the physical type said
/// `boolean` (1 byte) — silently truncating every later column in a
/// composite key.
/// `u64` frames exactly like `i64`; only the reported value differs, read as the unsigned column it came from.
/// `dec` frames 16 bytes, an exact DECIMAL key's unscaled value, reported at its type.
pub const KeyType = union(enum) { i64, u64, f64, string, dec: expr_ast.Dec };

/// Framing for a group-key column, taken from the expression's own type.
///
/// Must agree with the lane `evalGroupKeyExpr` produces and `expr_agg.serializeRowKey` then writes: i32 and i64 both
/// serialize as an 8-byte i64, f32 and f64 as an 8-byte f64, so these three cover every column variant.
/// Deliberately does NOT consult the Parquet schema — a leaf index does not address it (see `leafLabel`).
/// An unsigned 64-bit column rides the i64 lane as raw bits, so a bare one is read back as `u64`: as `i64`, values of
/// 2^63 and up came out negative.
pub fn keyTypeFromExpr(expr: expr_ast.Expr) KeyType {
    if (expr == .col_ref and expr.col_ref.unsigned_64) return .u64;
    return switch (expr.typeOf()) {
        .i64 => .i64,
        .f64 => .f64,
        .str => .string,
        .dec => .{ .dec = expr.decSpec().? },
    };
}

/// Pull one column out of a serialized composite group key: a 1-byte present flag, then what
/// `expr_agg.serializeRowKey` wrote for that lane. Bounds-checked rather than trusting the framing: a writer/reader
/// disagreement turns a length field into the previous column's payload bytes, panicking or allocating wildly.
pub fn deserializeKeyColumn(
    allocator: std.mem.Allocator,
    key_bytes: []const u8,
    target_idx: usize,
    key_types: []const KeyType,
) !AggValue {
    return deserializeKeyColumnMode(allocator, key_bytes, target_idx, key_types, false);
}

pub fn deserializeKeyColumnMode(
    allocator: std.mem.Allocator,
    key_bytes: []const u8,
    target_idx: usize,
    key_types: []const KeyType,
    borrow: bool,
) !AggValue {
    var cursor: usize = 0;
    var current_idx: usize = 0;
    while (current_idx <= target_idx) : (current_idx += 1) {
        if (current_idx >= key_types.len) return error.BadGroupKey;
        if (cursor >= key_bytes.len) return error.BadGroupKey;
        const is_present = key_bytes[cursor];
        cursor += 1;

        if (is_present == 0) {
            if (current_idx == target_idx) return .null_val;
            continue;
        }

        switch (key_types[current_idx]) {
            .i64, .u64, .f64 => {
                if (cursor + 8 > key_bytes.len) return error.BadGroupKey;
                const raw = std.mem.readInt(u64, key_bytes[cursor..][0..8], .little);
                cursor += 8;
                if (current_idx == target_idx) {
                    return switch (key_types[current_idx]) {
                        .i64 => .{ .i = @as(i64, @bitCast(raw)) },
                        .u64 => .{ .i = raw },
                        .f64 => .{ .f = @bitCast(raw) },
                        .string, .dec => unreachable,
                    };
                }
            },
            .dec => |spec| {
                if (cursor + 16 > key_bytes.len) return error.BadGroupKey;
                const v = std.mem.readInt(i128, key_bytes[cursor..][0..16], .little);
                cursor += 16;
                if (current_idx == target_idx) return .{ .dec = .{ .v = v, .spec = spec } };
            },
            .string => {
                if (cursor + 4 > key_bytes.len) return error.BadGroupKey;
                const len = std.mem.readInt(u32, key_bytes[cursor..][0..4], .little);
                cursor += 4;
                if (cursor + len > key_bytes.len) return error.BadGroupKey;
                if (current_idx == target_idx) {
                    if (borrow) return .{ .s = key_bytes[cursor .. cursor + len] };
                    const buf = try allocator.alloc(u8, len);
                    @memcpy(buf, key_bytes[cursor .. cursor + len]);
                    return .{ .s = buf };
                }
                cursor += len;
            },
        }
    }
    return error.BadGroupKey;
}

test "deserializeKeyColumn: an unsigned 64-bit key reads back unsigned" {
    var key: [9]u8 = undefined;
    key[0] = 1;
    std.mem.writeInt(u64, key[1..9], std.math.maxInt(u64), .little);
    const v = try deserializeKeyColumn(testing.allocator, &key, 0, &.{.u64});
    try testing.expectEqual(@as(i128, std.math.maxInt(u64)), v.i);
    const signed = try deserializeKeyColumn(testing.allocator, &key, 0, &.{.i64});
    try testing.expectEqual(@as(i128, -1), signed.i);
}

test "deserializeKeyColumn: corrupt framing errors instead of panicking" {
    const a = testing.allocator;

    // A length field claiming far more than the buffer holds — previously a panic or a huge alloc.
    var runaway: [5]u8 = .{ 1, 0xFF, 0xFF, 0xFF, 0xFF };
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &runaway, 0, &.{.string}),
    );

    // Truncated mid-payload.
    var short_i64: [4]u8 = .{ 1, 0, 0, 0 };
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &short_i64, 0, &.{.i64}),
    );

    // Asking for a column beyond what the key holds.
    var one_null: [1]u8 = .{0};
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &one_null, 1, &.{ .i64, .i64 }),
    );

    // Empty input.
    try testing.expectError(
        error.BadGroupKey,
        deserializeKeyColumn(a, &.{}, 0, &.{.i64}),
    );

    // A well-formed key still round-trips: null, then i64, then string.
    var ok_key: std.ArrayList(u8) = .empty;
    defer ok_key.deinit(a);
    try ok_key.append(a, 0); // null
    try ok_key.append(a, 1);
    try ok_key.appendSlice(a, &std.mem.toBytes(@as(i64, -7)));
    try ok_key.append(a, 1);
    try ok_key.appendSlice(a, &std.mem.toBytes(@as(u32, 3)));
    try ok_key.appendSlice(a, "abc");
    const types = [_]KeyType{ .i64, .i64, .string };
    try testing.expect((try deserializeKeyColumn(a, ok_key.items, 0, &types)) == .null_val);
    try testing.expectEqual(@as(i128, -7), (try deserializeKeyColumn(a, ok_key.items, 1, &types)).i);
    const s = try deserializeKeyColumn(a, ok_key.items, 2, &types);
    defer a.free(s.s);
    try testing.expectEqualStrings("abc", s.s);
}

