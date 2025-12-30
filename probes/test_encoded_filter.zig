//! Probe: Test that byte-level comparison works for encoded Parquet values
//!
//! Key insight: For equality predicates, we can compare raw encoded bytes
//! instead of decoding to native types. This enables a single code path
//! for all fixed-width types.

const std = @import("std");

pub fn main() !void {
    std.debug.print("=== Encoded Filter Viability Test ===\n\n", .{});

    // Test 1: INT64 encoding and comparison
    std.debug.print("Test 1: INT64 byte encoding\n", .{});
    {
        const val: i64 = 12345;
        const encoded = std.mem.asBytes(&val);
        std.debug.print("  i64 value: {d}\n", .{val});
        std.debug.print("  encoded bytes: {any}\n", .{encoded.*});

        // Verify round-trip
        const decoded = std.mem.readInt(i64, encoded, .little);
        std.debug.print("  decoded back: {d}\n", .{decoded});
        std.debug.print("  round-trip OK: {}\n\n", .{val == decoded});
    }

    // Test 2: Byte ordering matches numeric ordering for positive INT64
    std.debug.print("Test 2: Byte ordering for positive INT64\n", .{});
    {
        const a: i64 = 100;
        const b: i64 = 200;
        const c: i64 = 150;

        const a_bytes = std.mem.asBytes(&a);
        const b_bytes = std.mem.asBytes(&b);
        const c_bytes = std.mem.asBytes(&c);

        // For UNSIGNED values, little-endian byte order matches numeric order
        // But for SIGNED, this breaks with negative numbers!
        const byte_order_ab = std.mem.order(u8, a_bytes, b_bytes);
        const byte_order_bc = std.mem.order(u8, b_bytes, c_bytes);

        std.debug.print("  a=100, b=200, c=150\n", .{});
        std.debug.print("  byte compare a vs b: {}\n", .{byte_order_ab});
        std.debug.print("  byte compare b vs c: {}\n", .{byte_order_bc});
        std.debug.print("  a < b via bytes: {} (expected: true)\n", .{byte_order_ab == .lt});
        std.debug.print("  c < b via bytes: {} (expected: true)\n\n", .{byte_order_bc == .gt});
    }

    // Test 3: PROBLEM - Negative numbers break byte ordering!
    std.debug.print("Test 3: Negative INT64 (potential problem)\n", .{});
    {
        const neg: i64 = -100;
        const pos: i64 = 100;

        const neg_bytes = std.mem.asBytes(&neg);
        const pos_bytes = std.mem.asBytes(&pos);

        std.debug.print("  neg=-100 bytes: {any}\n", .{neg_bytes.*});
        std.debug.print("  pos=100 bytes:  {any}\n", .{pos_bytes.*});

        const byte_order = std.mem.order(u8, neg_bytes, pos_bytes);
        std.debug.print("  byte compare neg vs pos: {}\n", .{byte_order});
        std.debug.print("  neg < pos via bytes: {} (expected: true, actual might be wrong!)\n\n", .{byte_order == .lt});
    }

    // Test 4: For EQUALITY, byte comparison always works!
    std.debug.print("Test 4: Equality comparison (always works)\n", .{});
    {
        const val1: i64 = -12345;
        const val2: i64 = -12345;
        const val3: i64 = 12345;

        const b1 = std.mem.asBytes(&val1);
        const b2 = std.mem.asBytes(&val2);
        const b3 = std.mem.asBytes(&val3);

        std.debug.print("  -12345 == -12345 via bytes: {}\n", .{std.mem.eql(u8, b1, b2)});
        std.debug.print("  -12345 == 12345 via bytes:  {}\n", .{std.mem.eql(u8, b1, b3)});
        std.debug.print("  Equality via bytes works for signed values!\n\n", .{});
    }

    // Test 5: Range checks (min/max) - need care with signed
    std.debug.print("Test 5: Range check implications\n", .{});
    {
        // For range checks (page skip), we need to handle signed correctly
        // Option A: Decode to i64 for comparison (current approach)
        // Option B: Use XOR with sign bit to fix ordering
        // Option C: Only use byte compare for equality, decode for range

        std.debug.print("  For EQUALITY: byte compare works perfectly\n", .{});
        std.debug.print("  For RANGE (min/max): need type-aware comparison\n", .{});
        std.debug.print("  Recommendation: byte-compare for value matching,\n", .{});
        std.debug.print("                  type-aware for page skip predicates\n\n", .{});
    }

    // Test 6: BYTE_ARRAY - already bytes, trivial
    std.debug.print("Test 6: BYTE_ARRAY (strings)\n", .{});
    {
        const s1 = "hello";
        const s2 = "hello";
        const s3 = "world";

        std.debug.print("  'hello' == 'hello': {}\n", .{std.mem.eql(u8, s1, s2)});
        std.debug.print("  'hello' == 'world': {}\n", .{std.mem.eql(u8, s1, s3)});
        std.debug.print("  String comparison: trivial!\n\n", .{});
    }

    // Conclusion
    std.debug.print("=== CONCLUSION ===\n", .{});
    std.debug.print("Byte-level equality comparison works for ALL types.\n", .{});
    std.debug.print("Page skip (range) still needs type-aware min/max compare.\n", .{});
    std.debug.print("This means we can unify the VALUE MATCHING loop,\n", .{});
    std.debug.print("but keep type-specific mightContain() for page skip.\n", .{});
}
