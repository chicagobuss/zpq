//! Little-endian integer loads.
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
