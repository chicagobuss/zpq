const std = @import("std");
const sigv4 = @import("src/zpq/io/s3/sigv4.zig");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const cases = [_]struct { input: []const u8, expected: []const u8 }{
        .{ .input = "/bucket/file.txt", .expected = "/bucket/file.txt" },
        .{ .input = "/bucket/space file.txt", .expected = "/bucket/space%20file.txt" },
        .{ .input = "/bucket/path/to/file.txt", .expected = "/bucket/path/to/file.txt" },
        .{ .input = "/bucket/with+plus.txt", .expected = "/bucket/with%2Bplus.txt" },
    };

    for (cases) |case| {
        const encoded = try sigv4.encodeS3Path(allocator, case.input);
        defer if (encoded.ptr != case.input.ptr) allocator.free(encoded);
        
        if (!std.mem.eql(u8, encoded, case.expected)) {
            std.debug.print("FAIL: input='{s}', expected='{s}', got='{s}'\n", .{ case.input, case.expected, encoded });
            std.process.exit(1);
        }
    }
    std.debug.print("encodeS3Path tests PASSED\n", .{});
}

