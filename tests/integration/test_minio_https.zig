const std = @import("std");
const xev = @import("xev");
const zpq = @import("zpq");

pub const std_options = std.Options{
    .log_level = .debug,
    .log_scope_levels = &[_]std.log.ScopeLevel{
        .{ .scope = .tls, .level = .debug },
    },
};

pub fn main() !void {
    // Skip if ZPQ_TEST_MINIO not set (requires local minio docker)
    if (std.posix.getenv("ZPQ_TEST_MINIO") == null) {
        std.debug.print("SKIP: set ZPQ_TEST_MINIO=1 to run (requires local minio)\n", .{});
        return;
    }

    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    var loop = try xev.Loop.init(.{});
    defer loop.deinit();

    // MinIO local default credentials
    // User: minioadmin
    // Pass: minioadmin
    // Host: localhost:9000 (HTTPS)

    // We need to use the IP address of localhost because DNS resolution isn't hooked up in libxev yet?
    // Or we can just use 127.0.0.1.
    const host = "localhost";
    const ip = "127.0.0.1";
    const port = 9000;

    std.debug.print("\nTesting HTTP Client against MinIO ({s}:{d})...\n", .{host, port});

    var client = zpq.io.http.Client.init(&loop, allocator);
    var result = zpq.io.http.Client.FetchResult{};
    defer client.cleanupFetchResult(&result);
    // Client deinit is manual in this simple impl?
    // It doesn't have deinit, but it allocates ReqContexts.

    // Attempt a HEAD request to root
    // MinIO root usually returns 403 Forbidden (signature check) or 200 OK (if public)
    // or 400 Bad Request if Host header is missing/wrong.
    try client.fetchWithResult(host, ip, port, "/", &result);

    try loop.run(.until_done);

    if (result.err) |err| {
        // Connection-close after we got a response is expected for `Connection: close`.
        if (!(result.got_any_data and (err == error.EOF or err == error.TlsConnectionClosed))) {
            return err;
        }
    }
    if (!result.got_any_data) return error.NoResponse;

    std.debug.print("\nMinIO Test Complete.\n", .{});
}
