/// Minimal probe to test XevS3Source HEAD + readAt
/// Usage: S3_HOST=... S3_BUCKET=... S3_KEY=... zig build probe-xev-s3-head
const std = @import("std");
const zpq = @import("zpq");

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = std.posix.getenv("S3_HOST") orelse {
        std.debug.print("ERROR: S3_HOST not set\n", .{});
        return;
    };
    const bucket = std.posix.getenv("S3_BUCKET") orelse {
        std.debug.print("ERROR: S3_BUCKET not set\n", .{});
        return;
    };
    const key = std.posix.getenv("S3_KEY") orelse {
        std.debug.print("ERROR: S3_KEY not set\n", .{});
        return;
    };
    const region = std.posix.getenv("S3_REGION") orelse "auto";
    const access_key = std.posix.getenv("S3_ACCESS_KEY");
    const secret_key = std.posix.getenv("S3_SECRET_KEY");

    std.debug.print("=== XevS3Source Probe ===\n", .{});
    std.debug.print("Host: {s}\n", .{host});
    std.debug.print("Bucket: {s}\n", .{bucket});
    std.debug.print("Key: {s}\n", .{key});
    std.debug.print("Credentials: {s}\n\n", .{if (access_key != null) "provided" else "none"});

    std.debug.print("[1] Creating XevS3Source...\n", .{});
    const source = try zpq.s3.XevS3Source.init(allocator, host, bucket, key, region, true, 443);

    if (access_key) |ak| {
        if (secret_key) |sk| {
            std.debug.print("[2] Setting credentials...\n", .{});
            try source.setCredentials(ak, sk, null);
        }
    }

    std.debug.print("[3] Calling fetchSize (HEAD request)...\n", .{});
    source.fetchSize() catch |err| {
        std.debug.print("ERROR: fetchSize failed: {}\n", .{err});
        source.deinit();
        allocator.destroy(source);
        return;
    };
    std.debug.print("    File size: {d} bytes\n", .{source.file_size});

    // Test readAt - read last 8 bytes (PAR1 magic + footer length)
    std.debug.print("[4] Testing readAt (last 8 bytes)...\n", .{});
    var buf: [8]u8 = undefined;
    const offset = source.file_size - 8;
    const n = source.readAt(offset, &buf) catch |err| {
        std.debug.print("ERROR: readAt failed: {}\n", .{err});
        source.deinit();
        allocator.destroy(source);
        return;
    };
    std.debug.print("    Read {d} bytes at offset {d}\n", .{n, offset});
    std.debug.print("    Magic: {s} (expect PAR1)\n", .{buf[4..8]});

    std.debug.print("[5] Cleaning up...\n", .{});
    source.deinit();
    allocator.destroy(source);

    std.debug.print("=== Probe Complete ===\n", .{});
}
