//! Little-endian integer loads: fixed-width, and the bounded ULEB128 varint shared by the page encodings and the
//! snappy preamble.
//!
//! `std.mem.readInt` is `@bitCast(array.*)`. Under Zig 0.17 with
//! ReleaseSmall, LLVM no longer folds that array-to-int bitcast into a
//! single load; it emits a per-byte shift/or loop. A plain unaligned
//! pointer load compiles to one instruction in every mode.

const std = @import("std");

pub inline fn readLe(comptime T: type, bytes: *const [@divExact(@typeInfo(T).int.bits, 8)]u8) T {
    return std.mem.littleToNative(T, @as(*align(1) const T, @ptrCast(bytes)).*);
}

test readLe {
    const buf = [_]u8{ 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11 };
    try std.testing.expectEqual(std.mem.readInt(u32, buf[1..5], .little), readLe(u32, buf[1..5]));
    try std.testing.expectEqual(std.mem.readInt(u64, buf[1..9], .little), readLe(u64, buf[1..9]));
    try std.testing.expectEqual(std.mem.readInt(u128, buf[1..17], .little), readLe(u128, buf[1..17]));
}

pub const Uleb128 = struct { value: u64, len: usize };

/// Reads one ULEB128 varint from the front of `bytes`. `Overflow` covers both a varint longer than ten bytes and a
/// tenth byte carrying bits past 2^64: an unchecked shift would silently drop them (nine 0x80 then 0x02 is 2^64, not
/// 0). Callers map the errors onto their own format's and apply any narrower limit to `value` themselves.
pub inline fn readUleb128(bytes: []const u8) error{ Truncated, Overflow }!Uleb128 {
    var value: u64 = 0;
    var shift: u6 = 0;
    for (bytes, 1..) |b, len| {
        const payload: u64 = b & 0x7f;
        if (shift == 63 and payload > 1) return error.Overflow;
        value |= payload << shift;
        if (b & 0x80 == 0) return .{ .value = value, .len = len };
        if (shift == 63) return error.Overflow;
        shift += 7;
    }
    return error.Truncated;
}

test readUleb128 {
    const nine_cont = @as([9]u8, @splat(0x80));
    try std.testing.expectEqual(Uleb128{ .value = 0, .len = 1 }, try readUleb128(&.{ 0x00, 0xff }));
    try std.testing.expectEqual(Uleb128{ .value = 300, .len = 2 }, try readUleb128(&.{ 0xac, 0x02 }));
    // Maximum valid: 2^64 - 1 in ten bytes, and 2^63 as a tenth byte of exactly 1.
    try std.testing.expectEqual(
        Uleb128{ .value = std.math.maxInt(u64), .len = 10 },
        try readUleb128(&(@as([9]u8, @splat(0xff)) ++ [_]u8{0x01})),
    );
    try std.testing.expectEqual(Uleb128{ .value = 1 << 63, .len = 10 }, try readUleb128(&(nine_cont ++ [_]u8{0x01})));
    // Overflowing terminal byte: 2^64 must not wrap to 0.
    try std.testing.expectError(error.Overflow, readUleb128(&(nine_cont ++ [_]u8{0x02})));
    try std.testing.expectError(error.Overflow, readUleb128(&(@as([10]u8, @splat(0xff)) ++ [_]u8{0x01})));
    // Unterminated.
    try std.testing.expectError(error.Truncated, readUleb128(&.{}));
    try std.testing.expectError(error.Truncated, readUleb128(&nine_cont));
}
