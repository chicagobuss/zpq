const std = @import("std");
const zpq = @import("zpq");
const XevS3Source = zpq.s3.XevS3Source;
const fixtures = @import("minio_fixtures");

pub const std_options = std.Options{
    .log_level = .info,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .s3_source, .level = .debug },
        .{ .scope = .tls, .level = .debug },
    },
};

const payload = fixtures.range_payload;

pub fn main() !void {
    // Ignore SIGPIPE
    if (@import("builtin").os.tag != .windows) {
        std.posix.sigaction(std.posix.SIG.PIPE, &.{
            .handler = .{ .handler = std.posix.SIG.IGN },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        }, null);
    }

    if (std.posix.getenv("ZPQ_TEST_MINIO") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_MINIO=1 to run\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const host = "127.0.0.1";
    const bucket = "zpq-ci";
    const key = "range_payload.bin";
    const region = "us-east-1";
    const port = 9000;

    var source = try XevS3Source.init(allocator, host, bucket, key, region, true, port);
    defer source.deinit();

    // The logic in XevS3Source defaults to 443 for TLS.
    // MinIO local is running on 9000.
    // We need to support custom port in XevS3Source or resolver.
    // Hack: XevS3Source currently hardcodes 443 in resolve().
    // I need to update XevS3Source to accept a port.
    
    // Test logic:
    const start: usize = 123;
    const len: usize = 321;
    const expected = payload[start .. start + len];
    
    const buf = try allocator.alloc(u8, len);
    defer allocator.free(buf);
    
    std.debug.print("Reading from MinIO via XevS3Source...\n", .{});
    const n = try source.readAt(start, buf);
    
    if (n != len) return error.ShortRead;
    if (!std.mem.eql(u8, buf, expected)) return error.ContentMismatch;
    
    std.debug.print("Success! Read {d} bytes.\n", .{n});
}

