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

/// Fast equality comparison for byte slices / string keys.
///
/// Under native compilation (znver5 / AVX-512 / AVX2), Zig 0.17's `std.mem.eql` emits a byte-at-a-time
/// vector-insert loop for slices longer than 64 bytes. `keyEql` compares in 32-byte `@Vector` chunks with
/// overlapping tails for lengths >= 32, and unaligned 64-bit integer loads for lengths >= 8.
pub inline fn keyEql(a: []const u8, b: []const u8) bool {
    if (a.ptr == b.ptr and a.len == b.len) return true;
    if (a.len != b.len) return false;
    const n = a.len;
    if (n >= 32) {
        const V = @Vector(32, u8);
        var i: usize = 0;
        while (i + 32 <= n) : (i += 32) {
            const x: V = a[i..][0..32].*;
            const y: V = b[i..][0..32].*;
            if (@reduce(.Or, x != y)) return false;
        }
        if (i < n) {
            const x: V = a[n - 32 ..][0..32].*;
            const y: V = b[n - 32 ..][0..32].*;
            if (@reduce(.Or, x != y)) return false;
        }
        return true;
    }
    if (n >= 8) {
        var i: usize = 0;
        while (i + 8 <= n) : (i += 8) {
            if (readLe(u64, a[i..][0..8]) != readLe(u64, b[i..][0..8])) return false;
        }
        return readLe(u64, a[n - 8 ..][0..8]) == readLe(u64, b[n - 8 ..][0..8]);
    }
    for (a, b) |x, y| if (x != y) return false;
    return true;
}

pub const KeyContext = struct {
    pub inline fn hash(_: @This(), s: []const u8) u64 {
        return std.hash.Wyhash.hash(0, s);
    }
    pub inline fn eql(_: @This(), a: []const u8, b: []const u8) bool {
        return keyEql(a, b);
    }
};

pub fn KeyHashMap(comptime V: type) type {
    return std.HashMap([]const u8, V, KeyContext, std.hash_map.default_max_load_percentage);
}

pub fn KeyHashMapUnmanaged(comptime V: type) type {
    return std.HashMapUnmanaged([]const u8, V, KeyContext, std.hash_map.default_max_load_percentage);
}

test "keyEql: exhaustive equality vs std.mem.eql over lengths 0..200" {
    var prng = std.Random.DefaultPrng.init(0xdeadbeef);
    var buf1: [256]u8 = undefined;
    var buf2: [256]u8 = undefined;
    prng.random().bytes(&buf1);

    for (0..201) |len| {
        @memcpy(buf2[0..len], buf1[0..len]);
        const s1 = buf1[0..len];
        const s2 = buf2[0..len];

        // Same contents, different addresses
        try std.testing.expect(keyEql(s1, s2));
        try std.testing.expect(keyEql(s2, s1));
        // Same address
        try std.testing.expect(keyEql(s1, s1));

        // Compare against std.mem.eql
        try std.testing.expectEqual(std.mem.eql(u8, s1, s2), keyEql(s1, s2));

        // Different lengths
        if (len > 0) {
            try std.testing.expect(!keyEql(buf1[0..len], buf2[0 .. len - 1]));
            try std.testing.expect(!keyEql(buf1[0 .. len - 1], buf2[0..len]));
        }

        // Single byte mismatch at every position
        for (0..len) |pos| {
            buf2[pos] ^= 0x5a;
            try std.testing.expect(!keyEql(buf1[0..len], buf2[0..len]));
            try std.testing.expect(!keyEql(buf2[0..len], buf1[0..len]));
            try std.testing.expectEqual(std.mem.eql(u8, buf1[0..len], buf2[0..len]), keyEql(buf1[0..len], buf2[0..len]));
            buf2[pos] ^= 0x5a; // restore
        }
    }
}

test "KeyHashMap: allocation failure leaves no leaks" {
    try std.testing.checkAllAllocationFailures(std.testing.allocator, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var map = KeyHashMap(u32).init(allocator);
            defer map.deinit();

            try map.put("short", 1);
            try map.put("medium_length_string_key_12345", 2);
            try map.put("very_long_string_key_exceeding_vector_width_32_bytes_with_some_suffix_data", 3);

            try std.testing.expectEqual(@as(?u32, 1), map.get("short"));
            try std.testing.expectEqual(@as(?u32, 2), map.get("medium_length_string_key_12345"));
            try std.testing.expectEqual(@as(?u32, 3), map.get("very_long_string_key_exceeding_vector_width_32_bytes_with_some_suffix_data"));
            try std.testing.expectEqual(@as(?u32, null), map.get("nonexistent_key"));
        }
    }.run, .{});
}
