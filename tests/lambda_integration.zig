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

/// Answer a next-invocation poll with `body` framed `Transfer-Encoding: chunked`, as the real runtime API
/// sends events above a few KiB: 4000-byte chunks, the first carrying a chunk extension.
fn replyChunkedAndClose(req: *Request, allocator: std.mem.Allocator, headers: []const u8, body: []const u8) !void {
    var resp: std.ArrayList(u8) = .empty;
    defer resp.deinit(allocator);
    const head = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n{s}\r\n";
    try resp.print(allocator, head, .{headers});
    var off: usize = 0;
    while (off < body.len) {
        const n = @min(4000, body.len - off);
        try resp.print(allocator, "{x}{s}\r\n", .{ n, if (off == 0) ";ext=1" else "" });
        try resp.appendSlice(allocator, body[off..][0..n]);
        try resp.appendSlice(allocator, "\r\n");
        off += n;
    }
    try resp.appendSlice(allocator, "0\r\n\r\n");
    try writeAllFd(req.conn, resp.items);
    req.deinit();
}

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
// Fake S3 endpoint
// ============================================================

/// Plain-HTTP, path-style S3 stand-in that serves one object under every key: ranged and conditional GETs on
/// keep-alive connections, one thread per connection. With `idle_close_ms` > 0 it drops a connection left idle that
/// long, the way S3 and the network path drop a pooled keep-alive socket while a Lambda sandbox is frozen.
const FakeS3 = struct {
    const MAX_CONNS = 256;
    const ETAG = "fake-etag";

    listen_fd: linux.fd_t,
    port: u16,
    object: []const u8,
    idle_close_ms: i32,
    accept_thread: std.Thread = undefined,
    conn_fds: [MAX_CONNS]linux.fd_t = @splat(-1),
    conn_threads: [MAX_CONNS]?std.Thread = @splat(null),
    /// Written only by the accept thread; read by `deinit` after joining it.
    conns: usize = 0,
    requests: std.atomic.Value(u32) = .init(0),
    idle_closes: std.atomic.Value(u32) = .init(0),

    fn start(self: *FakeS3, object: []const u8, idle_close_ms: i32) !void {
        const fd = try sysSocket();
        errdefer sysClose(fd);
        const yes: i32 = 1;
        _ = linux.setsockopt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, std.mem.asBytes(&yes).ptr, @sizeOf(i32));
        var addr = std.mem.zeroes(linux.sockaddr.in);
        addr.family = linux.AF.INET;
        addr.addr = std.mem.nativeToBig(u32, 0x7f000001);
        if (errIs(linux.bind(fd, @ptrCast(&addr), @sizeOf(linux.sockaddr.in)))) return error.BindFailed;
        if (errIs(linux.listen(fd, 64))) return error.ListenFailed;
        var bound = std.mem.zeroes(linux.sockaddr.in);
        var len: linux.socklen_t = @sizeOf(linux.sockaddr.in);
        if (errIs(linux.getsockname(fd, @ptrCast(&bound), &len))) return error.GetsocknameFailed;

        self.* = .{
            .listen_fd = fd,
            .port = std.mem.bigToNative(u16, bound.port),
            .object = object,
            .idle_close_ms = idle_close_ms,
        };
        self.accept_thread = try std.Thread.spawn(.{}, acceptLoop, .{self});
    }

    /// Call after the Lambda child is gone, so every connection thread has seen its peer close.
    fn deinit(self: *FakeS3) void {
        _ = linux.shutdown(self.listen_fd, linux.SHUT.RDWR);
        self.accept_thread.join();
        for (self.conn_fds[0..self.conns], self.conn_threads[0..self.conns]) |fd, thread| {
            _ = linux.shutdown(fd, linux.SHUT.RDWR);
            if (thread) |t| t.join();
            sysClose(fd);
        }
        sysClose(self.listen_fd);
    }

    fn acceptLoop(self: *FakeS3) void {
        while (self.conns < MAX_CONNS) {
            const fd = sysAccept(self.listen_fd) catch return;
            self.conn_fds[self.conns] = fd;
            self.conn_threads[self.conns] = std.Thread.spawn(.{}, serveConn, .{ self, fd }) catch null;
            self.conns += 1;
        }
    }

    fn serveConn(self: *FakeS3, fd: linux.fd_t) void {
        var buf: [16 * 1024]u8 = undefined;
        var len: usize = 0;
        while (true) {
            if (std.mem.indexOf(u8, buf[0..len], "\r\n\r\n")) |head_end| {
                self.respond(fd, buf[0..head_end]) catch return;
                const used = head_end + 4;
                std.mem.copyForwards(u8, buf[0 .. len - used], buf[used..len]);
                len -= used;
                continue;
            }
            if (len == buf.len) return;
            if (self.idle_close_ms > 0 and len == 0) {
                var pfd = [_]linux.pollfd{.{ .fd = fd, .events = linux.POLL.IN, .revents = 0 }};
                const r = linux.poll(&pfd, 1, self.idle_close_ms);
                if (errIs(r)) return;
                if (r == 0) {
                    // FIN now; `deinit` closes the descriptor.
                    _ = self.idle_closes.fetchAdd(1, .monotonic);
                    _ = linux.shutdown(fd, linux.SHUT.RDWR);
                    return;
                }
            }
            const r = linux.read(fd, buf[len..].ptr, buf.len - len);
            if (errIs(r) or r == 0) return;
            len += r;
        }
    }

    fn respond(self: *FakeS3, fd: linux.fd_t, head: []const u8) !void {
        _ = self.requests.fetchAdd(1, .monotonic);
        const obj = self.object;
        var range: ?[]const u8 = null;
        var if_none_match: ?[]const u8 = null;
        var lines = std.mem.splitSequence(u8, head, "\r\n");
        _ = lines.next();
        while (lines.next()) |line| {
            const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
            const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
            if (std.ascii.eqlIgnoreCase(line[0..colon], "range")) range = value;
            if (std.ascii.eqlIgnoreCase(line[0..colon], "if-none-match")) if_none_match = value;
        }
        var hdr_buf: [256]u8 = undefined;
        if (if_none_match) |v| if (std.mem.eql(u8, std.mem.trim(u8, v, "\""), ETAG)) {
            return writeAllFd(fd, "HTTP/1.1 304 Not Modified\r\nETag: \"" ++ ETAG ++ "\"\r\n\r\n");
        };
        var first: usize = 0;
        var end: usize = obj.len; // exclusive
        if (range) |r| {
            const spec = if (std.mem.startsWith(u8, r, "bytes=")) r["bytes=".len..] else return error.BadRange;
            const dash = std.mem.indexOfScalar(u8, spec, '-') orelse return error.BadRange;
            if (dash == 0) {
                first = obj.len - @min(obj.len, try std.fmt.parseInt(usize, spec[1..], 10));
            } else {
                first = @min(obj.len, try std.fmt.parseInt(usize, spec[0..dash], 10));
                if (dash + 1 < spec.len) end = @min(obj.len, try std.fmt.parseInt(usize, spec[dash + 1 ..], 10) + 1);
            }
            const hdr = try std.fmt.bufPrint(&hdr_buf, "HTTP/1.1 206 Partial Content\r\nContent-Length: {d}\r\n" ++
                "Content-Range: bytes {d}-{d}/{d}\r\nETag: \"" ++ ETAG ++ "\"\r\n\r\n", .{ end - first, first, end - 1, obj.len });
            try writeAllFd(fd, hdr);
        } else {
            const hdr = try std.fmt.bufPrint(&hdr_buf, "HTTP/1.1 200 OK\r\nContent-Length: {d}\r\n" ++
                "ETag: \"" ++ ETAG ++ "\"\r\n\r\n", .{obj.len});
            try writeAllFd(fd, hdr);
        }
        try writeAllFd(fd, obj[first..end]);
    }
};

fn writeAllFd(fd: linux.fd_t, bytes: []const u8) !void {
    var off: usize = 0;
    while (off < bytes.len) {
        const r = linux.write(fd, bytes[off..].ptr, bytes.len - off);
        if (errIs(r) or r == 0) return error.WriteFailed;
        off += r;
    }
}

/// `path` with `pad` zero bytes spliced in before its footer. Column-chunk offsets are absolute, so they still point at
/// the unmoved chunks and the file stays valid Parquet; the padding pushes every chunk out of the 64 KiB tail zpq reads
/// first, so a query also runs the parallel range-fetch stage.
fn paddedParquet(allocator: std.mem.Allocator, path: []const u8, pad: usize) ![]u8 {
    const raw = try readFileSlice(allocator, path);
    defer allocator.free(raw);
    const footer_len = std.mem.readInt(u32, raw[raw.len - 8 ..][0..4], .little);
    const footer_start = raw.len - 8 - footer_len;
    const out = try allocator.alloc(u8, raw.len + pad);
    @memcpy(out[0..footer_start], raw[0..footer_start]);
    @memset(out[footer_start..][0..pad], 0);
    @memcpy(out[footer_start + pad ..], raw[footer_start..]);
    return out;
}

/// A Lambda child whose `s3://` inputs resolve to `s3`.
fn spawnLambdaForS3(allocator: std.mem.Allocator, runtime_endpoint: []const u8, s3: *const FakeS3) !std.process.Child {
    var url_buf: [64]u8 = undefined;
    const url = try std.fmt.bufPrint(&url_buf, "http://127.0.0.1:{d}", .{s3.port});
    return spawnLambdaWithEnv(allocator, runtime_endpoint, &.{
        .{ "S3_ENDPOINT_URL", url },
        .{ "S3_ACCESS_KEY_ID", "fake" },
        .{ "S3_SECRET_ACCESS_KEY", "fake" },
        .{ "S3_REGION", "us-east-1" },
    });
}

/// Pin `pid` (and the threads it starts afterwards) to one CPU. The read stage's worker hand-off races are easiest to
/// lose on a single CPU, which is also what a small Lambda gets.
fn pinToOneCpu(pid: linux.pid_t) void {
    var set: linux.cpu_set_t = @splat(0);
    if (errIs(linux.sched_getaffinity(0, @sizeOf(linux.cpu_set_t), &set))) return;
    for (set, 0..) |word, i| if (word != 0) {
        var one: linux.cpu_set_t = @splat(0);
        one[i] = @as(usize, 1) << @intCast(@ctz(word));
        linux.sched_setaffinity(pid, &one) catch {};
        return;
    };
}

/// Serve one invocation of `body` and return the response body (caller frees).
fn invoke(server: *FakeServer, allocator: std.mem.Allocator, body: []const u8) ![]u8 {
    var poll = try server.acceptRequest(allocator);
    try poll.replyAndClose(allocator, 200, "Lambda-Runtime-Aws-Request-Id: req-s3\r\nContent-Type: application/json\r\n", body);
    var resp = try server.acceptRequest(allocator);
    const out = try allocator.dupe(u8, resp.body);
    errdefer allocator.free(out);
    if (!std.mem.endsWith(u8, resp.path, "/response")) return error.TestUnexpectedResult;
    try resp.replyAndClose(allocator, 202, "", "");
    return out;
}

/// The `"agg":{...}` member of an aggregate response.
fn aggMember(resp: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, resp, "\"agg\":{") orelse return null;
    const end = std.mem.indexOfScalarPos(u8, resp, start, '}') orelse return null;
    return resp[start .. end + 1];
}

// ============================================================
// Process spawning
// ============================================================

fn spawnLambda(allocator: std.mem.Allocator, runtime_endpoint: []const u8) !std.process.Child {
    return spawnLambdaWithEnv(allocator, runtime_endpoint, &.{});
}

fn spawnLambdaWithEnv(
    allocator: std.mem.Allocator,
    runtime_endpoint: []const u8,
    extra_env: []const [2][]const u8,
) !std.process.Child {
    var env_map: std.process.Environ.Map = .{ .allocator = allocator, .array_hash_map = .empty };
    defer env_map.deinit();
    for (extra_env) |kv| try env_map.put(kv[0], kv[1]);
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

test "lambda rejects empty body cleanly" {
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

    var poll = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("GET", poll.method);
    try std.testing.expectEqualStrings("/2018-06-01/runtime/invocation/next", poll.path);
    try poll.replyAndClose(
        std.testing.allocator,
        200,
        "Lambda-Runtime-Aws-Request-Id: req-empty\r\n" ++
            "Content-Type: application/octet-stream\r\n",
        "",
    );

    var resp = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("POST", resp.method);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"error\":\"empty_body\"") != null);
    try resp.replyAndClose(std.testing.allocator, 202, "", "");
}

test "lambda decodes int8 column from a real Parquet file" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(std.testing.allocator, fixture_path) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("skipping: {s} not present\n", .{fixture_path});
            return error.SkipZigTest;
        }
        return err;
    };
    defer std.testing.allocator.free(file_bytes);

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

    var poll = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("/2018-06-01/runtime/invocation/next", poll.path);
    try poll.replyAndClose(
        std.testing.allocator,
        200,
        "Lambda-Runtime-Aws-Request-Id: req-decode\r\n" ++
            "Content-Type: application/octet-stream\r\n",
        file_bytes,
    );

    var resp = try server.acceptRequest(std.testing.allocator);
    try std.testing.expectEqualStrings("POST", resp.method);
    try std.testing.expectEqualStrings(
        "/2018-06-01/runtime/invocation/req-decode/response",
        resp.path,
    );

    // Expect the success envelope with all 524288 rows decoded.
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"ok\":true") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"column\":\"int8\"") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"rows\":524288") != null);
    try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"row_groups\":4") != null);
    std.debug.print("[lambda decode] response: {s}\n", .{resp.body});
    try resp.replyAndClose(std.testing.allocator, 202, "", "");
}

test "lambda handles back-to-back invocations" {
    const fixture_path = "data/benchmark_100mb.parquet";
    const file_bytes = readFileSlice(std.testing.allocator, fixture_path) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer std.testing.allocator.free(file_bytes);

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

    const ids = [_][]const u8{ "req-A", "req-B" };
    for (ids) |id| {
        var poll = try server.acceptRequest(std.testing.allocator);
        try std.testing.expectEqualStrings("GET", poll.method);

        const headers = try std.fmt.allocPrint(
            std.testing.allocator,
            "Lambda-Runtime-Aws-Request-Id: {s}\r\n" ++
                "Content-Type: application/octet-stream\r\n",
            .{id},
        );
        defer std.testing.allocator.free(headers);
        try poll.replyAndClose(std.testing.allocator, 200, headers, file_bytes);

        var resp = try server.acceptRequest(std.testing.allocator);
        try std.testing.expectEqualStrings("POST", resp.method);
        const expected_path = try std.fmt.allocPrint(
            std.testing.allocator,
            "/2018-06-01/runtime/invocation/{s}/response",
            .{id},
        );
        defer std.testing.allocator.free(expected_path);
        try std.testing.expectEqualStrings(expected_path, resp.path);

        // Each invocation should yield the success envelope.
        try std.testing.expect(std.mem.indexOf(u8, resp.body, "\"rows\":524288") != null);
        try resp.replyAndClose(std.testing.allocator, 202, "", "");
    }
}

test "lambda aggregate over no rows answers NULL for sum/avg/min/max, 0 for count" {
    const fixture_path = "data/parquet-testing/data/alltypes_plain.parquet";
    const probe = readFileSlice(std.testing.allocator, fixture_path) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    std.testing.allocator.free(probe);

    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(std.testing.allocator, "127.0.0.1:{d}", .{server.port});
    defer std.testing.allocator.free(endpoint);
    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    var poll = try server.acceptRequest(std.testing.allocator);
    try poll.replyAndClose(
        std.testing.allocator,
        200,
        "Lambda-Runtime-Aws-Request-Id: req-empty-agg\r\n" ++
            "Content-Type: application/json\r\n",
        "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"filter\":\"id < 0\"," ++
            "\"aggregate\":\"sum(id) AS s, avg(id) AS a, min(string_col) AS m, max(double_col) AS x, " ++
            "count(*) AS c, count(id) AS ci\"}",
    );

    var resp = try server.acceptRequest(std.testing.allocator);
    const want = "\"agg\":{\"s\":null,\"a\":{\"sum\":null,\"count\":0},\"m\":null,\"x\":null,\"c\":0,\"ci\":0}";
    if (std.mem.indexOf(u8, resp.body, want) == null) {
        std.debug.print("[lambda empty agg] response: {s}\n", .{resp.body});
        return error.TestUnexpectedResult;
    }
    try resp.replyAndClose(std.testing.allocator, 202, "", "");
}

test "lambda aggregate JSON escapes strings and spells NaN/Infinity like the CLI" {
    // A string min/max and a group key carrying `"`, `\`, a newline and 0x01; a NaN sum and an +inf max.
    const fixture_path = "ci/fixtures/parquet/json_escape.parquet";
    try expectResponses("lambda json escape", &.{
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"]," ++
                "\"aggregate\":\"min(s) AS m, sum(x) AS n, max(y) AS i, avg(x) AS a\"}",
            .want = "\"agg\":{\"m\":\"a \\\"q\\\" c:\\\\d\\nnext\\u0001end\",\"n\":\"NaN\",\"i\":\"Infinity\"," ++
                "\"a\":{\"sum\":\"NaN\",\"count\":3}}",
        },
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"group_by\":\"s\",\"aggregate\":\"sum(x) AS n\"}",
            .want = "{\"s\":\"b\",\"n\":\"NaN\"},{\"s\":\"c\",\"n\":2},{\"s\":\"a \\\"q\\\" c:\\\\d\\nnext\\u0001end\",\"n\":1}",
        },
        .{
            // Invalid UTF-8 becomes one U+FFFD per offending byte; U+2028 is escaped.
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"group_by\":\"b\",\"aggregate\":\"count(*) AS n\"}",
            .want = "[{\"b\":\"\\u2028\",\"n\":1},{\"b\":\"\u{FFFD}\u{FFFD}\u{FFFD}\",\"n\":1}," ++
                "{\"b\":\"\u{FFFD}\u{FFFD}A\u{FFFD}\u{FFFD}\",\"n\":1}]",
        },
    });
}

test "lambda labels a nested GROUP BY key by its quoted path when a top-level column takes its dotted name" {
    // Top-level `a.b` (1, null, 3) beside the group `a`'s field `b` (10, 20, null): two key names, not one. Named
    // unquoted here (`b` binds the nested field, `a.b` the top-level column); quoted in the escapes test.
    const fixture_path = "ci/fixtures/parquet/dotted_twin.parquet";
    const q = "\"\\\"a\\\".\\\"b\\\"\""; // the JSON key "\"a\".\"b\""
    try expectResponses("lambda dotted twin", &.{
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"group_by\":\"b, a.b\",\"aggregate\":\"count(*) AS n\"}",
            .want = "\"agg\":[{" ++ q ++ ":null,\"a.b\":3,\"n\":1},{" ++ q ++ ":10,\"a.b\":1,\"n\":1}," ++
                "{" ++ q ++ ":20,\"a.b\":null,\"n\":1}]",
        },
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"group_by\":\"b\",\"aggregate\":\"max(a.b) AS m\"}",
            .want = "\"agg\":[{" ++ q ++ ":null,\"m\":3},{" ++ q ++ ":10,\"m\":1},{" ++ q ++ ":20,\"m\":null}]",
        },
    });
}

/// Send each event in turn to one lambda process and require every response to be valid JSON containing `want`.
fn expectResponses(tag: []const u8, events: []const struct { body: []const u8, want: []const u8 }) !void {
    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(std.testing.allocator, "127.0.0.1:{d}", .{server.port});
    defer std.testing.allocator.free(endpoint);
    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    for (events) |ev| {
        var poll = try server.acceptRequest(std.testing.allocator);
        try poll.replyAndClose(
            std.testing.allocator,
            200,
            "Lambda-Runtime-Aws-Request-Id: req-event\r\nContent-Type: application/json\r\n",
            ev.body,
        );
        var resp = try server.acceptRequest(std.testing.allocator);
        const parsed = std.json.parseFromSlice(std.json.Value, std.testing.allocator, resp.body, .{}) catch |err| {
            std.debug.print("[{s}] invalid JSON ({s}) for {s}: {s}\n", .{ tag, @errorName(err), ev.body, resp.body });
            return err;
        };
        parsed.deinit();
        if (!std.mem.endsWith(u8, resp.path, "/response") or std.mem.indexOf(u8, resp.body, ev.want) == null) {
            const fmt = "[{s}] request {s}\n  response {s} {s}\n  want {s}\n";
            std.debug.print(fmt, .{ tag, ev.body, resp.path, resp.body, ev.want });
            return error.TestUnexpectedResult;
        }
        try resp.replyAndClose(std.testing.allocator, 202, "", "");
    }
}

test "lambda write response escapes an output name holding a quote and a backslash" {
    const esc = "ci/fixtures/parquet/json_escape.parquet";
    const out = "zig-out/lambda \"q\" \\ out.parquet";
    defer _ = linux.unlink(out);
    try expectResponses("lambda write escape", &.{.{
        .body = "{\"inputs\":[\"" ++ esc ++ "\"],\"columns\":[\"s\"]," ++
            "\"output_url\":\"zig-out/lambda \\\"q\\\" \\\\ out.parquet\"}",
        .want = "{\"ok\":true,\"output\":\"zig-out/lambda \\\"q\\\" \\\\ out.parquet\",\"files_in\":1,",
    }});
}

test "lambda decodes JSON escapes in request strings" {
    const twin = "ci/fixtures/parquet/dotted_twin.parquet";
    const esc = "ci/fixtures/parquet/json_escape.parquet";
    const q = "\"\\\"a\\\".\\\"b\\\"\""; // the response key "\"a\".\"b\""
    try expectResponses("lambda escapes", &.{
        .{
            // Quoted column names: the nested field b of group a, then the top-level column `a.b`.
            .body = "{\"inputs\":[\"" ++ twin ++ "\"],\"group_by\":\"\\\"a\\\".\\\"b\\\", \\\"a.b\\\"\"," ++
                "\"aggregate\":\"count(*) AS n\"}",
            .want = "\"agg\":[{" ++ q ++ ":null,\"a.b\":3,\"n\":1},{" ++ q ++ ":10,\"a.b\":1,\"n\":1}," ++
                "{" ++ q ++ ":20,\"a.b\":null,\"n\":1}]",
        },
        .{
            // \u escapes, and a surrogate pair decoded into one UTF-8 sequence (no row holds it).
            .body = "{\"inputs\":[\"" ++ twin ++ "\"],\"group_by\":\"\\u0062\"," ++
                "\"aggregate\":\"count(*) AS n\",\"filter\":\"\\\"a.b\\\" IS NOT NULL\"}",
            .want = "\"agg\":[{" ++ q ++ ":null,\"n\":1},{" ++ q ++ ":10,\"n\":1}]",
        },
        .{
            // A string literal holding an escaped quote, a backslash, a newline and a control byte.
            .body = "{\"inputs\":[\"" ++ esc ++ "\"],\"filter\":\"s = 'a \\\"q\\\" c:\\\\d\\nnext\\u0001end'\"," ++
                "\"aggregate\":\"count(*) AS n\"}",
            .want = "\"agg\":{\"n\":1}",
        },
        .{
            .body = "{\"inputs\":[\"" ++ esc ++ "\"]," ++
                "\"filter\":\"s != '\\ud83d\\ude00' AND s != '\\/\\b\\f\\r\\t'\"," ++
                "\"aggregate\":\"count(*) AS n\",\"mode\":\"ignored\"}",
            .want = "\"agg\":{\"n\":3}",
        },
    });
}

test "lambda rejects malformed requests with a clean error and keeps serving" {
    const esc = "ci/fixtures/parquet/json_escape.parquet";
    const head = "{\"inputs\":[\"" ++ esc ++ "\"]";
    const bad = "{\"error\":\"bad_json\",\"reason\":\"";
    try expectResponses("lambda malformed", &.{
        .{ .body = "{\"inputs\":[", .want = bad },
        .{ .body = head ++ ",\"filter\":\"x}", .want = bad },
        .{ .body = head ++ ",\"filter\":\"\\q\"}", .want = bad },
        .{ .body = head ++ ",\"filter\":\"\\ud800\"}", .want = bad },
        .{ .body = head ++ ",\"filter\":\"\xff\"}", .want = bad },
        .{ .body = head ++ "} trailing", .want = bad },
        .{
            .body = head ++ ",\"aggregate\":\"count(*) AS n\",\"aggregate\":\"sum(x) AS n\"}",
            .want = "{\"error\":\"bad_json\",\"reason\":\"DuplicateField\"}",
        },
        .{
            .body = head ++ ",\"aggregate\":\"count(*) AS n\",\"filter\":5}",
            .want = "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"filter\"}",
        },
        .{
            .body = "{\"inputs\":\"" ++ esc ++ "\",\"aggregate\":\"count(*) AS n\"}",
            .want = "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"inputs\"}",
        },
        .{
            .body = "{\"inputs\":[\"" ++ esc ++ "\", 7],\"aggregate\":\"count(*) AS n\"}",
            .want = "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"inputs\"}",
        },
        .{
            .body = head ++ ",\"aggregate\":\"count(*) AS n\",\"scan_all\":\"yes\"}",
            .want = "{\"error\":\"bad_json\",\"reason\":\"BadFieldType\",\"field\":\"scan_all\"}",
        },
        .{ .body = "{\"aggregate\":\"count(*) AS n\"}", .want = "{\"error\":\"bad_json\",\"reason\":\"MissingField\"" },
        // Still serving: a well-formed request after all of the above.
        .{ .body = head ++ ",\"aggregate\":\"count(*) AS n\"}", .want = "\"agg\":{\"n\":3}" },
    });
}

test "lambda answers a ~50 KB event the runtime API sends chunked" {
    // Events of about 5 KB and up arrive chunked; they used to reach the JSON parser still framed (UnexpectedToken).
    const a = std.testing.allocator;
    const fixture_path = "ci/fixtures/parquet/json_escape.parquet";
    const pad = try a.alloc(u8, 50 * 1024);
    defer a.free(pad);
    for (pad, 0..) |*b, i| b.* = 'a' + @as(u8, @intCast(i % 26));
    const body = try std.mem.concat(a, u8, &.{
        "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"aggregate\":\"count(*) AS n\",\"filter\":\"s != '",
        pad,
        "'\"}",
    });
    defer a.free(body);

    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{server.port});
    defer a.free(endpoint);
    var child = try spawnLambda(a, endpoint);
    defer killChild(&child);

    var poll = try server.acceptRequest(a);
    const headers = "Lambda-Runtime-Aws-Request-Id: req-chunked\r\nContent-Type: application/json\r\n";
    try replyChunkedAndClose(&poll, a, headers, body);
    var resp = try server.acceptRequest(a);
    const ok = std.mem.eql(u8, resp.path, "/2018-06-01/runtime/invocation/req-chunked/response") and
        std.mem.indexOf(u8, resp.body, "\"agg\":{\"n\":3}") != null;
    if (!ok) {
        std.debug.print("[lambda chunked event] {s}: {s}\n", .{ resp.path, resp.body });
        return error.TestUnexpectedResult;
    }
    try resp.replyAndClose(a, 202, "", "");
}

test "lambda rejects clashing or unknown column names and names the column" {
    const fixture_path = "ci/fixtures/parquet/json_escape.parquet";
    const events = [_]struct { body: []const u8, want: []const u8 }{
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"group_by\":\"s AS k\",\"aggregate\":\"sum(x) AS s\"}",
            .want = "{\"error\":\"engine\",\"reason\":\"AmbiguousOutputColumn\",\"column\":\"s\"}",
        },
        .{
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"aggregate\":\"sum(x) AS t, max(y) AS t\"}",
            .want = "{\"error\":\"engine\",\"reason\":\"DuplicateOutputColumn\",\"column\":\"t\"}",
        },
        .{
            // An unknown projection column is named too, and nothing is written.
            .body = "{\"inputs\":[\"" ++ fixture_path ++ "\"],\"columns\":[\"s\",\"nope\"]," ++
                "\"output_url\":\"zig-out/lambda_unknown_columns.parquet\"}",
            .want = "{\"error\":\"engine\",\"reason\":\"UnknownColumn\",\"column\":\"nope\"}",
        },
    };

    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(std.testing.allocator, "127.0.0.1:{d}", .{server.port});
    defer std.testing.allocator.free(endpoint);
    var child = try spawnLambda(std.testing.allocator, endpoint);
    defer killChild(&child);

    for (events) |ev| {
        var poll = try server.acceptRequest(std.testing.allocator);
        try poll.replyAndClose(
            std.testing.allocator,
            200,
            "Lambda-Runtime-Aws-Request-Id: req-name-clash\r\nContent-Type: application/json\r\n",
            ev.body,
        );
        var resp = try server.acceptRequest(std.testing.allocator);
        if (!std.mem.eql(u8, resp.body, ev.want)) {
            std.debug.print("[lambda name clash] response: {s}\n", .{resp.body});
            return error.TestUnexpectedResult;
        }
        if (readFileSlice(std.testing.allocator, "zig-out/lambda_unknown_columns.parquet")) |bytes| {
            std.testing.allocator.free(bytes);
            return error.TestUnexpectedResult;
        } else |_| {}
        try resp.replyAndClose(std.testing.allocator, 202, "", "");
    }
}

test "lambda names the input file a query could not read, and why" {
    try expectResponses("lambda bad input", &.{
        .{
            .body = "{\"inputs\":[\"zig-out/no such \\\"file\\\".parquet\"],\"aggregate\":\"count(*) AS n\"}",
            .want = "{\"error\":\"engine\",\"reason\":\"OpenFailed\"," ++
                "\"input\":\"zig-out/no such \\\"file\\\".parquet\",\"cause\":\"FileNotFound\"}",
        },
        .{
            .body = "{\"inputs\":[\"build.zig\"],\"aggregate\":\"count(*) AS n\"}",
            .want = "{\"error\":\"engine\",\"reason\":\"NotParquet\",\"input\":\"build.zig\",\"cause\":",
        },
    });
}

const S3_AGG = "\"aggregate\":\"count(*) AS c, sum(x) AS sx, min(s) AS ms, max(n) AS mn, sum(u) AS su\"";
const S3_FIXTURE = "ci/fixtures/parquet/full_match.parquet";

test "lambda answers back-to-back S3 queries without running out of read workers" {
    // Eight inputs fill both read stages to the worker limit: eight metadata fetches, then eight range fetches, on one
    // executor. The fetch stage used to be rejected with ConcurrencyUnavailable whenever a metadata worker had finished
    // but was still counted busy.
    const a = std.testing.allocator;
    const object = try paddedParquet(a, S3_FIXTURE, 128 * 1024);
    defer a.free(object);
    var s3: FakeS3 = undefined;
    try s3.start(object, 0);
    defer s3.deinit();

    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{server.port});
    defer a.free(endpoint);
    var child = try spawnLambdaForS3(a, endpoint, &s3);
    defer killChild(&child);
    if (child.id) |pid| pinToOneCpu(pid);

    const f = "\"" ++ S3_FIXTURE ++ "\"";
    const local = try invoke(&server, a, "{\"inputs\":[" ++ f ++ "," ++ f ++ "," ++ f ++ "," ++ f ++ "," ++ f ++ "," ++
        f ++ "," ++ f ++ "," ++ f ++ "]," ++ S3_AGG ++ "}");
    defer a.free(local);
    const want = aggMember(local) orelse {
        std.debug.print("[s3 back-to-back] local reference failed: {s}\n", .{local});
        return error.TestUnexpectedResult;
    };

    const body = "{\"inputs\":[\"s3://bkt/f0.parquet\",\"s3://bkt/f1.parquet\",\"s3://bkt/f2.parquet\"," ++
        "\"s3://bkt/f3.parquet\",\"s3://bkt/f4.parquet\",\"s3://bkt/f5.parquet\",\"s3://bkt/f6.parquet\"," ++
        "\"s3://bkt/f7.parquet\"]," ++ S3_AGG ++ "}";
    for (0..60) |i| {
        const resp = try invoke(&server, a, body);
        defer a.free(resp);
        const got = aggMember(resp) orelse "";
        if (!std.mem.eql(u8, got, want)) {
            std.debug.print("[s3 back-to-back] invocation {d}: {s}\n  want {s}\n", .{ i, resp, want });
            return error.TestUnexpectedResult;
        }
    }
}

test "lambda reopens S3 connections the server dropped while it sat idle, without backing off" {
    // The server drops a connection after 150 ms idle. A second query 400 ms later finds every pooled connection dead
    // (younger than the pool's idle timeout, so still offered for reuse) and must retry each at once on a new
    // connection. A third query after more than the idle timeout must not try them at all.
    const a = std.testing.allocator;
    const object = try paddedParquet(a, S3_FIXTURE, 128 * 1024);
    defer a.free(object);
    var s3: FakeS3 = undefined;
    try s3.start(object, 150);
    defer s3.deinit();

    var server = try FakeServer.start();
    defer server.deinit();
    const endpoint = try std.fmt.allocPrint(a, "127.0.0.1:{d}", .{server.port});
    defer a.free(endpoint);
    var child = try spawnLambdaForS3(a, endpoint, &s3);
    defer killChild(&child);

    const body = "{\"inputs\":[\"s3://bkt/f0.parquet\",\"s3://bkt/f1.parquet\",\"s3://bkt/f2.parquet\"," ++
        "\"s3://bkt/f3.parquet\"]," ++ S3_AGG ++ "}";
    const first = try invoke(&server, a, body);
    defer a.free(first);
    const want = aggMember(first) orelse {
        std.debug.print("[s3 idle] first query failed: {s}\n", .{first});
        return error.TestUnexpectedResult;
    };

    const Step = struct { idle_ms: u64, stale: bool };
    for ([_]Step{ .{ .idle_ms = 400, .stale = true }, .{ .idle_ms = 4300, .stale = false } }) |step| {
        const ts: linux.timespec = .{ .sec = @intCast(step.idle_ms / 1000), .nsec = @intCast((step.idle_ms % 1000) * std.time.ns_per_ms) };
        _ = linux.nanosleep(&ts, null);
        const resp = try invoke(&server, a, body);
        defer a.free(resp);
        const parsed = try std.json.parseFromSlice(std.json.Value, a, resp, .{});
        defer parsed.deinit();
        const pool = parsed.value.object.get("pool") orelse return error.TestUnexpectedResult;
        const stale = pool.object.get("stale_retries").?.integer;
        const evicted = pool.object.get("idle_evictions").?.integer;
        const backoff = pool.object.get("backoff_retries").?.integer;
        const ok = std.mem.eql(u8, aggMember(resp) orelse "", want) and backoff == 0 and
            if (step.stale) stale > 0 and evicted == 0 else stale == 0 and evicted > 0;
        if (!ok) {
            std.debug.print("[s3 idle] after {d} ms idle: {s}\n  want {s}\n", .{ step.idle_ms, resp, want });
            return error.TestUnexpectedResult;
        }
    }
    try std.testing.expect(s3.idle_closes.load(.monotonic) > 0);
}

fn readFileSlice(allocator: std.mem.Allocator, path: []const u8) ![]u8 {
    var path_z: [256]u8 = undefined;
    if (path.len + 1 > path_z.len) return error.PathTooLong;
    @memcpy(path_z[0..path.len], path);
    path_z[path.len] = 0;

    const r_open = linux.openat(linux.AT.FDCWD, @ptrCast(&path_z[0]), .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    const fd: linux.fd_t = blk: {
        if (errIs(r_open)) return error.FileNotFound;
        break :blk @intCast(@as(isize, @bitCast(r_open)));
    };
    defer _ = linux.close(fd);

    const SEEK_END: usize = 2;
    const SEEK_SET: usize = 0;
    const end_pos = linux.lseek(fd, 0, SEEK_END);
    if (errIs(end_pos)) return error.SeekFailed;
    _ = linux.lseek(fd, 0, SEEK_SET);
    const size: usize = @intCast(end_pos);

    const buf = try allocator.alloc(u8, size);
    errdefer allocator.free(buf);
    var off: usize = 0;
    while (off < size) {
        const n = linux.read(fd, buf[off..].ptr, size - off);
        if (errIs(n)) return error.ReadFailed;
        const bytes: usize = @intCast(n);
        if (bytes == 0) break;
        off += bytes;
    }
    return buf;
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
