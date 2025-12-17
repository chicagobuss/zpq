const std = @import("std");
const RawS3Source = @import("raw_s3_source").RawS3Source;

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const uri = try std.Uri.parse("http://127.0.0.1:9000/bucket/key");
    var s3 = try RawS3Source.init(allocator, uri);
    defer s3.source.close();

    std.debug.print("Connecting to {s}\n", .{s3.host});

    var buf: [100]u8 = undefined;

    // First Read
    std.debug.print("\n--- Read 1 ---\n", .{});
    var timer = try std.time.Timer.start();
    var n = try s3.source.readAt(0, &buf);
    var elapsed = timer.read();
    std.debug.print("Read 1: {d} bytes in {d}ms\n", .{n, elapsed / std.time.ns_per_ms});
    
    // Second Read (Should reuse connection)
    std.debug.print("\n--- Read 2 (Should Reuse) ---\n", .{});
    timer.reset();
    n = try s3.source.readAt(100, &buf);
    elapsed = timer.read();
    std.debug.print("Read 2: {d} bytes in {d}ms\n", .{n, elapsed / std.time.ns_per_ms});

    // Third Read
    std.debug.print("\n--- Read 3 (Should Reuse) ---\n", .{});
    timer.reset();
    n = try s3.source.readAt(200, &buf);
    elapsed = timer.read();
    std.debug.print("Read 3: {d} bytes in {d}ms\n", .{n, elapsed / std.time.ns_per_ms});
}
