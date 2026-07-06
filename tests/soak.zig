const std = @import("std");
const testing = std.testing;
const zpq = @import("zpq");

const dbp = zpq.core.parquet.encoding.delta_binary_packed;

test "soak: malformed delta-binary-packed bit width returns an error" {
    // Header:
    // - block_size = 128
    // - mini_blocks = 4 (32 values each)
    // - total_value_count = 33 (header first value + one full mini-block)
    // - first_value = 0
    //
    // The first mini-block advertises bit width 65, which is invalid for i64.
    // The payload is intentionally long enough that the 32-value fast path would
    // otherwise enter its dispatcher; the decoder should reject the block header
    // before that point instead of reaching unreachable code.
    var bytes: [310]u8 = undefined;
    @memset(&bytes, 0);
    bytes[0] = 0x80;
    bytes[1] = 0x01;
    bytes[2] = 0x04;
    bytes[3] = 0x21;
    bytes[4] = 0x00;
    bytes[5] = 0x00;
    bytes[6] = 65;

    var dec = try dbp.Decoder(i64).init(&bytes);
    var out: [33]i64 = undefined;
    try testing.expectError(error.InvalidHeader, dec.decode(&out));
}
