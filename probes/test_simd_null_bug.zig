const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    const compact = [_]u64{ 1, 2, 3, 4, 5, 6, 7, 8 };
    var output = [_]?u64{null} ** 8;
    const mask: u8 = 0b10101010; // Bits 1, 3, 5, 7 set. Should take values 1, 2, 3, 4.

    const consumed = zpq.core.simd.expandNullsBatch8(u64, &compact, mask, &output);
    
    std.debug.print("Mask: {b:0>8}\n", .{mask});
    std.debug.print("Consumed: {d} (Expected 4)\n", .{consumed});
    std.debug.print("Output: {any}\n", .{output});
    std.debug.print("Expected: [null, 1, null, 2, null, 3, null, 4]\n", .{});

    var match = true;
    const expected = [_]?u64{ null, 1, null, 2, null, 3, null, 4 };
    for (output, expected) |o, e| {
        if (o != e) match = false;
    }

    if (match) {
        std.debug.print("SUCCESS: expandNullsBatch8 is correct!\n", .{});
    } else {
        std.debug.print("FAILURE: expandNullsBatch8 is WRONG!\n", .{});
    }
}

