//! AWS Lambda Runtime API client.
//!
//! Speaks plain HTTP/1.1 to the runtime API endpoint provided in
//! `AWS_LAMBDA_RUNTIME_API`. Blocking-socket implementation — the
//! runtime API is a low-frequency control plane (one round-trip per
//! invocation), so non-blocking I/O would add complexity without moving
//! the data-path latency.
//! The data plane (S3, decode, sink) goes through the epoll Loop.
//!
//! Endpoints used (per AWS Lambda runtime API spec, version 2018-06-01):
//!
//!   GET  /2018-06-01/runtime/invocation/next
//!     Long-polls until an invocation arrives. Response body is the
//!     event payload; headers carry the request ID, invoked function
//!     ARN, and a deadline (epoch ms).
//!
//!   POST /2018-06-01/runtime/invocation/{request_id}/response
//!     Posts the successful response body. Body is opaque to Lambda;
//!     the convention is JSON.
//!
//!   POST /2018-06-01/runtime/invocation/{request_id}/error
//!     Posts an error. Body is JSON {"errorType": "...", "errorMessage": "..."}.
//!
//!   POST /2018-06-01/runtime/init/error
//!     Posts an init failure. This client currently exits on fatal init
//!     errors before entering the invocation loop.

const std = @import("std");
const linux = std.os.linux;

pub const Error = error{
    NoRuntimeApi,
    BadAddress,
    SocketFailed,
    ConnectFailed,
    WriteFailed,
    ReadFailed,
    BadResponse,
    OutOfMemory,
};

pub const Invocation = struct {
    request_id: []u8,
    body: []u8,
    /// Epoch milliseconds. Null if header was absent or unparseable.
    deadline_ms: ?i64,
    invoked_function_arn: ?[]u8,

    pub fn deinit(self: *Invocation, allocator: std.mem.Allocator) void {
        allocator.free(self.request_id);
        allocator.free(self.body);
        if (self.invoked_function_arn) |s| allocator.free(s);
    }
};

pub const Client = struct {
    /// Dotted-quad IPv4 address. Lambda always provides numeric host.
    addr: linux.sockaddr.in,
    /// Original "host:port" string — kept for diagnostics and the
    /// `Host:` HTTP header.
    host_header: []u8,
    allocator: std.mem.Allocator,

    pub fn fromEnv(allocator: std.mem.Allocator, env: std.process.Environ) Error!Client {
        const api = env.getPosix("AWS_LAMBDA_RUNTIME_API") orelse return error.NoRuntimeApi;
        return fromHostPort(allocator, api);
    }

    pub fn fromHostPort(allocator: std.mem.Allocator, host_port: []const u8) Error!Client {
        const colon = std.mem.indexOfScalar(u8, host_port, ':') orelse host_port.len;
        const host = host_port[0..colon];
        const port: u16 = if (colon < host_port.len)
            std.fmt.parseInt(u16, host_port[colon + 1 ..], 10) catch 80
        else
            80;

        var parts: [4]u8 = undefined;
        var i: usize = 0;
        var iter = std.mem.splitScalar(u8, host, '.');
        while (iter.next()) |p| : (i += 1) {
            if (i >= 4) return error.BadAddress;
            parts[i] = std.fmt.parseInt(u8, p, 10) catch return error.BadAddress;
        }
        if (i != 4) return error.BadAddress;

        var addr = std.mem.zeroes(linux.sockaddr.in);
        addr.family = linux.AF.INET;
        addr.port = std.mem.nativeToBig(u16, port);
        const ip: u32 = (@as(u32, parts[0]) << 24) | (@as(u32, parts[1]) << 16) | (@as(u32, parts[2]) << 8) | parts[3];
        addr.addr = std.mem.nativeToBig(u32, ip);

        const host_header = try allocator.dupe(u8, host_port);
        return .{ .addr = addr, .host_header = host_header, .allocator = allocator };
    }

    pub fn deinit(self: *Client) void {
        self.allocator.free(self.host_header);
        self.* = undefined;
    }

    /// Long-polls /runtime/invocation/next. Caller owns the returned
    /// Invocation and must call its deinit when done.
    pub fn nextInvocation(self: *Client) Error!Invocation {
        const sock = try self.connect();
        defer close(sock);

        const req = "GET /2018-06-01/runtime/invocation/next HTTP/1.1\r\n" ++
            "Host: lambda-runtime\r\n" ++
            "Connection: close\r\n\r\n";
        try writeAll(sock, req);

        const raw = try readAll(self.allocator, sock);
        defer self.allocator.free(raw);

        return try parseInvocation(self.allocator, raw);
    }

    /// Posts the success response. Body is sent as-is.
    pub fn postResponse(self: *Client, request_id: []const u8, body: []const u8) Error!void {
        try self.postTo(request_id, "response", "application/json", body);
    }

    /// Posts an error. Builds a minimal JSON envelope around `message`.
    pub fn postError(self: *Client, request_id: []const u8, error_type: []const u8, message: []const u8) Error!void {
        const json = std.fmt.allocPrint(
            self.allocator,
            "{{\"errorType\":\"{s}\",\"errorMessage\":\"{s}\"}}",
            .{ error_type, message },
        ) catch return error.OutOfMemory;
        defer self.allocator.free(json);
        try self.postTo(request_id, "error", "application/json", json);
    }

    fn postTo(
        self: *Client,
        request_id: []const u8,
        suffix: []const u8,
        content_type: []const u8,
        body: []const u8,
    ) Error!void {
        const sock = try self.connect();
        defer close(sock);

        const req = std.fmt.allocPrint(
            self.allocator,
            "POST /2018-06-01/runtime/invocation/{s}/{s} HTTP/1.1\r\n" ++
                "Host: lambda-runtime\r\n" ++
                "Content-Type: {s}\r\n" ++
                "Content-Length: {d}\r\n" ++
                "Connection: close\r\n\r\n" ++
                "{s}",
            .{ request_id, suffix, content_type, body.len, body },
        ) catch return error.OutOfMemory;
        defer self.allocator.free(req);
        try writeAll(sock, req);

        // Drain the response so the kernel queues it before we close.
        var drain: [1024]u8 = undefined;
        _ = linux.read(sock, &drain, drain.len);
    }

    fn connect(self: *const Client) Error!linux.fd_t {
        const r = linux.socket(linux.AF.INET, linux.SOCK.STREAM | linux.SOCK.CLOEXEC, linux.IPPROTO.TCP);
        const sock = fdOrErr(r) catch return error.SocketFailed;
        errdefer close(sock);

        const cr = linux.connect(sock, @ptrCast(&self.addr), @sizeOf(linux.sockaddr.in));
        if (isErr(cr)) return error.ConnectFailed;
        return sock;
    }
};

// ============================================================
// HTTP/1.1 response parsing — minimal, just enough for runtime API.
// ============================================================

fn parseInvocation(allocator: std.mem.Allocator, raw: []const u8) Error!Invocation {
    const header_end = std.mem.indexOf(u8, raw, "\r\n\r\n") orelse return error.BadResponse;
    const status_and_headers = raw[0..header_end];
    const body = raw[header_end + 4 ..];

    // Parse status line: "HTTP/1.1 200 OK"
    const first_crlf = std.mem.indexOf(u8, status_and_headers, "\r\n") orelse return error.BadResponse;
    const status_line = status_and_headers[0..first_crlf];
    if (status_line.len < 12 or !std.mem.startsWith(u8, status_line, "HTTP/1.1 ")) return error.BadResponse;
    const status_code = std.fmt.parseInt(u16, status_line[9..12], 10) catch return error.BadResponse;
    if (status_code != 200) return error.BadResponse;

    // Walk headers; capture the ones we care about.
    var request_id: ?[]const u8 = null;
    var deadline_ms: ?i64 = null;
    var invoked_function_arn: ?[]const u8 = null;
    var content_length: ?usize = null;

    const headers = status_and_headers[first_crlf + 2 ..];
    var lines = std.mem.splitSequence(u8, headers, "\r\n");
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "lambda-runtime-aws-request-id")) {
            request_id = value;
        } else if (std.ascii.eqlIgnoreCase(name, "lambda-runtime-deadline-ms")) {
            deadline_ms = std.fmt.parseInt(i64, value, 10) catch null;
        } else if (std.ascii.eqlIgnoreCase(name, "lambda-runtime-invoked-function-arn")) {
            invoked_function_arn = value;
        } else if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            content_length = std.fmt.parseInt(usize, value, 10) catch null;
        }
    }

    const id = request_id orelse return error.BadResponse;

    // Use Content-Length if present; otherwise trust the body slice.
    const body_slice = if (content_length) |n|
        body[0..@min(n, body.len)]
    else
        body;

    return .{
        .request_id = try allocator.dupe(u8, id),
        .body = try allocator.dupe(u8, body_slice),
        .deadline_ms = deadline_ms,
        .invoked_function_arn = if (invoked_function_arn) |s| try allocator.dupe(u8, s) else null,
    };
}

// ============================================================
// Raw socket helpers — same shape as the probe binary.
// ============================================================

fn close(fd: linux.fd_t) void {
    _ = linux.close(fd);
}

fn writeAll(fd: linux.fd_t, buf: []const u8) Error!void {
    var off: usize = 0;
    while (off < buf.len) {
        const r = linux.write(fd, buf[off..].ptr, buf.len - off);
        if (isErr(r)) return error.WriteFailed;
        const n: usize = @intCast(r);
        if (n == 0) return error.WriteFailed;
        off += n;
    }
}

fn readAll(allocator: std.mem.Allocator, fd: linux.fd_t) Error![]u8 {
    var list: std.ArrayList(u8) = .empty;
    errdefer list.deinit(allocator);
    var tmp: [8192]u8 = undefined;
    while (true) {
        const r = linux.read(fd, &tmp, tmp.len);
        if (isErr(r)) return error.ReadFailed;
        const n: usize = @intCast(r);
        if (n == 0) break;
        list.appendSlice(allocator, tmp[0..n]) catch return error.OutOfMemory;
    }
    return list.toOwnedSlice(allocator) catch return error.OutOfMemory;
}

fn isErr(r: usize) bool {
    const signed: isize = @bitCast(r);
    return signed >= -4095 and signed < 0;
}

fn fdOrErr(r: usize) error{SyscallFailed}!linux.fd_t {
    if (isErr(r)) return error.SyscallFailed;
    return @intCast(@as(isize, @bitCast(r)));
}

// ============================================================
// Tests
// ============================================================

test "fromHostPort parses valid address" {
    var c = try Client.fromHostPort(std.testing.allocator, "127.0.0.1:9001");
    defer c.deinit();
    try std.testing.expectEqualStrings("127.0.0.1:9001", c.host_header);
    try std.testing.expectEqual(@as(u16, std.mem.nativeToBig(u16, 9001)), c.addr.port);
}

test "fromHostPort defaults port to 80" {
    var c = try Client.fromHostPort(std.testing.allocator, "169.254.100.1");
    defer c.deinit();
    try std.testing.expectEqual(@as(u16, std.mem.nativeToBig(u16, 80)), c.addr.port);
}

test "fromHostPort rejects bad address" {
    try std.testing.expectError(error.BadAddress, Client.fromHostPort(std.testing.allocator, "not.an.ip"));
}

test "parseInvocation extracts request id and body" {
    const raw =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: application/json\r\n" ++
        "Content-Length: 13\r\n" ++
        "Lambda-Runtime-Aws-Request-Id: abc-123\r\n" ++
        "Lambda-Runtime-Deadline-Ms: 1700000000000\r\n" ++
        "\r\n" ++
        "{\"hello\":1}\r\n";
    var inv = try parseInvocation(std.testing.allocator, raw);
    defer inv.deinit(std.testing.allocator);
    try std.testing.expectEqualStrings("abc-123", inv.request_id);
    try std.testing.expectEqualStrings("{\"hello\":1}\r\n", inv.body);
    try std.testing.expectEqual(@as(?i64, 1700000000000), inv.deadline_ms);
}
