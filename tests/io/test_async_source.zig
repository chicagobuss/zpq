const std = @import("std");
const zpq = @import("zpq");
const AsyncS3Source = zpq.s3.AsyncS3Source;
const ConnectionPool = zpq.s3.ConnectionPool;
const scheduler = zpq.s3.scheduler;
const Range = scheduler.Range;

const HOST = "127.0.0.1";
const PORT = 9000;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("\n--- Testing AsyncS3Source (Full Stack) ---\n", .{});

    var pool = ConnectionPool.init(allocator);
    defer pool.deinit();

    // Init with TLS=false, Certs=null
    var source = try AsyncS3Source.init(allocator, &pool, HOST, PORT, "bucket", "key", false, null);
    defer source.deinit();

    // Request: 0-5 and 15-20 (Gap 5-15)
    // Mock server data: offset % 256
    var buf1: [5]u8 = undefined;
    var buf2: [5]u8 = undefined;

    const ranges = &[_]Range{
        .{ .start = 0, .end = 5 },
        .{ .start = 15, .end = 20 },
    };
    const buffers = &[_][]u8{ &buf1, &buf2 };

    // Execute!
    try source.readRanges(ranges, buffers);

    // Verify
    std.debug.print("Read Complete. Verifying...\n", .{});

    // Buf1: 0, 1, 2, 3, 4
    for (buf1, 0..) |b, i| {
        if (b != i % 256) {
            std.debug.print("Buf1 Mismatch at {d}: Expected {d}, Got {d}\n", .{i, i % 256, b});
            return error.DataMismatch;
        }
    }

    // Buf2: 15, 16, 17, 18, 19
    for (buf2, 0..) |b, i| {
        const expected = (15 + i) % 256;
        if (b != expected) {
            std.debug.print("Buf2 Mismatch at {d}: Expected {d}, Got {d}\n", .{i, expected, b});
            return error.DataMismatch;
        }
    }

    std.debug.print("SUCCESS: AsyncS3Source coalesced and read correctly!\n", .{});
}
