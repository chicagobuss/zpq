//! Lambda integration tests — in-process fake runtime API.
//!
//! Spawns ./zig-out/bin/zpq-lambda as a subprocess with
//! `AWS_LAMBDA_RUNTIME_API=127.0.0.1:<port>`, where <port> is a
//! kernel-assigned localhost port we hold open in the parent process.
//! The parent serves AWS Lambda's runtime API HTTP/1.1 contract,
//! sends synthetic invocation events, captures responses, and
//! asserts on them.
//!
//! This is the same kind of testing loop AWS's RIE provides, but
//! with no external binary, no Docker, no network setup. Iteration
//! latency is single-digit milliseconds — TDD-grade fast.
//!
//! Build target: `zig build test-integration` (depends on the lambda
//! binary being built first).

const std = @import("std");
const builtin = @import("builtin");
const linux = std.os.linux;
const integration_opts = @import("integration_opts");

const LAMBDA_BIN = integration_opts.lambda_bin;

// ============================================================
// Fake runtime API server
// ============================================================

const FakeServer = struct {
    listen_fd: linux.fd_t,
    port: u16,

    fn start() !FakeServer {
        const fd = try sysSocket();
        errdefer sysClose(fd);

        // SO_REUSEADDR so re-running tests doesn't hit TIME_WAIT.
        const yes: i32 = 1;
        _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, std.mem.asBytes(&yes).ptr, @sizeOf(i32));

        var addr = std.mem.zeroes(linux.sockaddr.in);
        addr.family = linux.AF.INET;
        addr.port = 0; // kernel picks
        addr.addr = std.mem.nativeToBig(u32, 0x7f000001); // 127.0.0.1
        if (errIs(linux.bind(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)))) return error.BindFailed;
        if (errIs(linux.listen(fd, 8))) return error.ListenFailed;

        // Read back the assigned port.
        var bound = std.mem.zeroes(linux.sockaddr.in);
        var len: linux.socklen_t = @sizeOf(linux.sockaddr.in);
        if (errIs(linux.getsockname(fd, @ptrCast(&bound), &len))) return error.GetsocknameFailed;
        const port = std.mem.bigToNative(u16, bound.port);

        return .{ .listen_fd = fd, .port = port };
    }

    fn deinit(self: *FakeServer) void {
        sysClose(self.listen_fd);
    }

    /// Accept one connection, read the request, return the parsed
    /// method+path. Caller closes the conn via `replyAndClose`.
    fn acceptRequest(self: *FakeServer, allocator: std.mem.Allocator) !Request {
        const conn = try sysAccept(self.listen_fd);
        errdefer sysClose(conn);

        const raw = try readUntilHeadersOrBody(allocator, conn);
        const parsed = try parseRequest(allocator, raw);
        return .{
            .conn = conn,
            .method = parsed.method,
            .path = parsed.path,
            .body = parsed.body,
            .raw = raw,
            .allocator = allocator,
        };
    }
};

const Request = struct {
    conn: linux.fd_t,
    method: []const u8,
    path: []const u8,
    body: []const u8,
    raw: []u8,
    allocator: std.mem.Allocator,

    fn deinit(self: *Request) void {
        sysClose(self.conn);
        self.allocator.free(self.raw);
    }

    fn replyAndClose(
        self: *Request,
        allocator: std.mem.Allocator,
        status: u16,
        extra_headers: []const u8,
        body: []const u8,
    ) !void {
        const status_text = switch (status) {
            200 => "OK",
            202 => "Accepted",
            else => "Unknown",
        };
        const resp = try std.fmt.allocPrint(
            allocator,
            "HTTP/1.1 {d} {s}\r\nContent-Length: {d}\r\nConnection: close\r\n{s}\r\n{s}",
            .{ status, status_text, body.len, extra_headers, body },
        );
        defer allocator.free(resp);
        var off: usize = 0;
        while (off < resp.len) {
            const r = linux.write(self.conn, resp[off..].ptr, resp.len - off);
            if (errIs(r)) return error.WriteFailed;
            const n: usize = @intCast(r);
            if (n == 0) break;
            off += n;
        }
        sysClose(self.conn);
        self.allocator.free(self.raw);
        self.* = undefined;
    }
};

const ParsedRequest = struct {
    method: []const u8,
    path: []const u8,
    body: []const u8,
};

fn parseRequest(allocator: std.mem.Allocator, raw: []const u8) !ParsedRequest {
    _ = allocator;
    const header_end = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse return error.BadRequest;
    const start_line_end = std.mem.indexOf(u8, raw, "\r\n") orelse return error.BadRequest;
    const start_line = raw[0..start_line_end];
    const sp1 = std.mem.indexOfScalar(u8, start_line, ' ') orelse return error.BadRequest;
    const sp2 = std.mem.indexOfScalarPos(u8, start_line, sp1 + 1, ' ') orelse return error.BadRequest;
    const method = start_line[0..sp1];
    const path = start_line[sp1 + 1 .. sp2];

    // Honor Content-Length if present.
    const headers = raw[start_line_end + 2 .. header_end];
    var content_length: ?usize = null;
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch null;
        }
    }
    const body_start = header_end + 4;
    const body_end = if (content_length) |n| @min(body_start + n, raw.len) else raw.len;
    return .{ .method = method, .path = path, .body = raw[body_start..body_end] };
}

/// Read until we have at least the headers + Content-Length-many body bytes.
/// HTTP/1.1 with Connection: close usually lets the kernel deliver everything
/// before the recv loop ends, but we don't rely on that.
fn readUntilHeadersOrBody(allocator: std.mem.Allocator, fd: linux.fd_t) ![]u8 {
    var buf: std.ArrayList(u8) = .empty;
    errdefer buf.deinit(allocator);
    var tmp: [4096]u8 = undefined;
    var content_length: ?usize = null;
    var headers_end: ?usize = null;

    while (true) {
        const r = linux.read(fd, &tmp, tmp.len);
        if (errIs(r)) return error.ReadFailed;
        const n: usize = @intCast(r);
        if (n == 0) break;
        try buf.appendSlice(allocator, tmp[0..n]);

        if (headers_end == null) {
            if (std.mem.indexOf(u8, buf.items, "\r\n\r\n")) |i| {
                headers_end = i;
                // Look for Content-Length in the headers.
                const start_line_end = std.mem.indexOf(u8, buf.items, "\r\n") orelse continue;
                const headers = buf.items[start_line_end + 2 .. i];
                var lines = std.mem.splitSequence(u8, headers, "\r\n");
                while (lines.next()) |line| {
                    const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
                    const name = line[0..colon];
                    const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
                    if (std.ascii.eqlIgnoreCase(name, "content-length")) {
                        content_length = std.fmt.parseInt(usize, value, 10) catch null;
                    }
                }
            }
        }
        if (headers_end) |he| {
            const want = he + 4 + (content_length orelse 0);
            if (buf.items.len >= want) break;
        }
    }
    return buf.toOwnedSlice(allocator);
}

// ============================================================
// Process spawning
// ============================================================

fn spawnLambda(allocator: std.mem.Allocator, runtime_endpoint: []const u8) !std.process.Child {
    var env_map: std.process.Environ.Map = .{ .allocator = allocator, .array_hash_map = .empty };
    defer env_map.deinit();
    try env_map.put("AWS_LAMBDA_RUNTIME_API", runtime_endpoint);
    try env_map.put("AWS_LAMBDA_FUNCTION_NAME", "test-fn");
    try env_map.put("AWS_LAMBDA_FUNCTION_VERSION", "$LATEST");
    try env_map.put("AWS_LAMBDA_FUNCTION_MEMORY_SIZE", "1024");
    try env_map.put("AWS_REGION", "us-west-2");
    try env_map.put("PATH", "/usr/bin:/bin");

    return try std.process.spawn(std.testing.io, .{
        .argv = &.{LAMBDA_BIN},
        .environ_map = &env_map,
        .stdout = .pipe,
        .stderr = .pipe,
    });
}

fn killChild(child: *std.process.Child) void {
    // kill() in 0.16 internally reaps and clears child.id; no wait() after.
    child.kill(std.testing.io);
}

// ============================================================
// Test scenarios
// ============================================================

test "lambda echoes one invocation" {
    var server = try FakeServer.start();
    defer server.deinit();

    const endpoint = try std.fmt.allocPrint(
        std.testing.allocator,
        "127.0.0.1:{d}",
        .{server.port},
    );
    defer std.testing.allocator.free(endpoint);

    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    // 1. The bootstrap should poll /next first.
    var poll = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("GET", poll.method);
    try std.testing.expectEqualStrings("/2018-06-01/runtime/invocation/next", poll.path);
    try poll.replyAndClose(
        std.testing.allocator,
        200,
        "Lambda-Runtime-Aws-Request-Id: req-001\r\n" ++
            "Lambda-Runtime-Deadline-Ms: 1700000000000\r\n" ++
            "Lambda-Runtime-Invoked-Function-Arn: arn:aws:lambda:us-west-2:0:function:test-fn\r\n" ++
            "Content-Type: application/json\r\n",
        "{\"hello\":\"world\"}",
    );

    // 2. The bootstrap should post the response.
    var resp = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("POST", resp.method);
    try std.testing.expectEqualStrings(
        "/2018-06-01/runtime/invocation/req-001/response",
        resp.path,
    );
    // Expect the handler stub's JSON shape.
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"loop\":\"epoll\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"request_id\":\"req-001\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"echo_bytes\":17") != null); // {"hello":"world"}
    try resp.replyAndClose(std.testing.allocator, 202, "", "");
}

test "lambda handles three back-to-back invocations" {
    var server = try FakeServer.start();
    defer server.deinit();

    const endpoint = try std.fmt.allocPrint(
        std.testing.allocator,
        "127.0.0.1:{d}",
        .{server.port},
    );
    defer std.testing.allocator.free(endpoint);

    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    const ids = [_][]const u8{ "req-A", "req-B", "req-C" };
    for (ids) |id| {
        var poll = try server.acceptRequest(std.testing.allocator);
        try std.testing.expectEqualStrings("GET", poll.method);
        try std.testing.expectEqualStrings("/2018-06-01/runtime/invocation/next", poll.path);

        const headers = try std.fmt.allocPrint(
            std.testing.allocator,
            "Lambda-Runtime-Aws-Request-Id: {s}\r\n" ++
                "Lambda-Runtime-Deadline-Ms: 1700000000000\r\n" ++
                "Content-Type: application/json\r\n",
            .{id},
        );
        defer std.testing.allocator.free(headers);
        try poll.replyAndClose(std.testing.allocator, 200, headers, "{\"n\":1}");

        var resp = try server.acceptRequest(std.testing.allocator);
        try std.testing.expectEqualStrings("POST", resp.method);
        const expected_path = try std.fmt.allocPrint(
            std.testing.allocator,
            "/2018-06-01/runtime/invocation/{s}/response",
            .{id},
        );
        defer std.testing.allocator.free(expected_path);
        try std.testing.expectEqualStrings(expected_path, resp.path);

        const expected_id_field = try std.fmt.allocPrint(
            std.testing.allocator,
            "\"request_id\":\"{s}\"",
            .{id},
        );
        defer std.testing.allocator.free(expected_id_field);
        try std.testing.expect(std.mem.indexOf(u8, resp.body, expected_id_field) != null);
        try resp.replyAndClose(std.testing.allocator, 202, "", "");
    }
}

// ============================================================
// Raw syscall helpers
// ============================================================

fn sysSocket() !linux.fd_t {
    const r = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP);
    if (errIs(r)) return error.SocketFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn sysAccept(listen_fd: linux.fd_t) !linux.fd_t {
    const r = linux.accept4(listen_fd, null, null, linux.SOCK.CLOEXEC);
    if (errIs(r)) return error.AcceptFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

fn sysClose(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn errIs(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}
