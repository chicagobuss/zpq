//! Synchronous HTTP/1.1 client over TLS.
//!
//! Single request per call over a caller-owned `tls.Connection`. Callers
//! may hand in one-shot connections or pooled persistent connections;
//! this layer only serializes one HTTP exchange and drains the response.
//!
//! The surface is intentionally narrow:
//!   - GET / HEAD / PUT / POST methods
//!   - `Content-Length`-bounded or chunked response bodies; a body
//!     that ends early is always an error
//!   - Response headers as a name/value list (case-insensitive lookups)
//!   - Status code + raw body bytes
//!
//! Lifetime: the returned `Response.body` and `Response.headers` are
//! arena-allocated. Caller's arena owns them.

const std = @import("std");
const tls = @import("tls.zig");

pub const Error = error{
    SendFailed,
    /// The peer closed or reset the connection before sending any of the response: on a pooled keep-alive connection,
    /// the mark of one the server dropped while it sat idle. Distinct from `RecvFailed`/`BodyTruncated`, which mean the
    /// response had started.
    ClosedBeforeResponse,
    RecvFailed,
    BadStatusLine,
    BadHeader,
    BodyTruncated,
    BodyTooLarge,
    HeadersTooLarge,
} || std.mem.Allocator.Error || tls.Error;

pub const Header = struct {
    name: []const u8,
    value: []const u8,
};

pub const Method = enum {
    GET,
    HEAD,
    PUT,
    POST,
    DELETE,

    fn str(self: Method) []const u8 {
        return @tagName(self);
    }
};

pub const Request = struct {
    method: Method,
    /// "Host:" header content. The path starts at `path` (e.g. `/key`).
    host: []const u8,
    path: []const u8,
    headers: []const Header,
    body: []const u8 = "",
};

pub const Response = struct {
    status: u16,
    headers: []const Header,
    body: []const u8,

    pub fn header(self: *const Response, name: []const u8) ?[]const u8 {
        for (self.headers) |h| {
            if (std.ascii.eqlIgnoreCase(h.name, name)) return h.value;
        }
        return null;
    }
};

/// Send a request on an already-connected TLS conn. Drains the
/// response. The conn is left in whatever state the server left it
/// in; callers decide whether to return it to a pool or close it.
pub fn sendRequest(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    req: Request,
) Error!Response {
    try sendRequestBytes(arena, conn, req);

    return try drainResponse(arena, conn, req.method == .HEAD);
}

/// Send a request and stream the response body directly into `target`.
/// Used by ranged S3 GETs where the expected body size is known from
/// parquet metadata. `Response.body` aliases `target[0..content_length]`.
///
/// On `error.BodyTooLarge`, unread response bytes remain on `conn`; the
/// caller must discard the connection rather than returning it to a pool.
pub fn sendRequestInto(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    req: Request,
    target: []u8,
) Error!Response {
    try sendRequestBytes(arena, conn, req);
    return try drainResponseInto(arena, conn, target);
}

fn sendRequestBytes(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    req: Request,
) Error!void {
    var head: std.ArrayList(u8) = .empty;
    defer head.deinit(arena);
    try buildRequestHead(arena, req, &head);
    try conn.send(head.items);
    if (req.body.len > 0) try conn.send(req.body);
}

fn buildRequestHead(arena: std.mem.Allocator, req: Request, buf: *std.ArrayList(u8)) Error!void {
    try buf.appendSlice(arena, req.method.str());
    try buf.append(arena, ' ');
    try buf.appendSlice(arena, req.path);
    try buf.appendSlice(arena, " HTTP/1.1\r\n");

    // Emit Host: only if the caller didn't already include it (e.g.
    // SigV4-signed requests carry it in their signed-headers list).
    // HTTP/1.1's default is persistent connections — we don't auto-emit
    // Connection: close. Callers that want one-shot semantics pass it
    // explicitly via req.headers.
    var caller_has_host = false;
    for (req.headers) |h| {
        if (std.ascii.eqlIgnoreCase(h.name, "Host")) caller_has_host = true;
    }
    if (!caller_has_host) {
        try buf.appendSlice(arena, "Host: ");
        try buf.appendSlice(arena, req.host);
        try buf.appendSlice(arena, "\r\n");
    }
    for (req.headers) |h| {
        try buf.appendSlice(arena, h.name);
        try buf.appendSlice(arena, ": ");
        try buf.appendSlice(arena, h.value);
        try buf.appendSlice(arena, "\r\n");
    }
    if (req.body.len > 0 or req.method == .PUT or req.method == .POST) {
        var clen_buf: [32]u8 = undefined;
        const clen = std.fmt.bufPrint(&clen_buf, "Content-Length: {d}\r\n", .{req.body.len}) catch unreachable;
        try buf.appendSlice(arena, clen);
    }
    try buf.appendSlice(arena, "\r\n");
}

// ============================================================
// Response parsing
// ============================================================

const MAX_HEADERS_BYTES: usize = 64 * 1024;

/// `head_only`: the response to a HEAD, whose Content-Length describes a body that is never sent.
fn drainResponse(arena: std.mem.Allocator, conn: *tls.Connection, head_only: bool) Error!Response {
    // Read until \r\n\r\n appears, then keep reading the body.
    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(arena);

    var headers_end: ?usize = null;
    var content_length: ?usize = null;
    var chunked: bool = false;
    var status: ?u16 = null;

    while (true) {
        var tmp: [16 * 1024]u8 = undefined;
        const n = try recvResponseBytes(conn, &tmp, stream.items.len);
        if (n == 0) break;
        try stream.appendSlice(arena, tmp[0..n]);
        if (headers_end == null) {
            if (std.mem.indexOf(u8, stream.items, "\r\n\r\n")) |i| {
                if (i > MAX_HEADERS_BYTES) return error.HeadersTooLarge;
                headers_end = i;
                status = try parseStatus(stream.items[0..i]);
                content_length = parseContentLength(stream.items[0..i]);
                chunked = parseTransferEncodingChunked(stream.items[0..i]);
                // RFC 7230 §3.3.3 rule 1: 1xx, 204, and 304 responses
                // MUST NOT include a message body — regardless of any
                // Content-Length the server may have sent (some
                // implementations send it; some don't). Without this
                // check the no-CL/no-chunked path falls through to
                // "read until conn closes," which on a keep-alive
                // connection hangs until the TCP idle timeout fires.
                if (status) |s| if (head_only or statusIsBodyless(s)) {
                    return .{
                        .status = s,
                        .headers = try parseHeaders(arena, stream.items[0..i]),
                        .body = &.{},
                    };
                };
            }
        }
        if (headers_end) |he| {
            if (content_length) |clen| {
                if (stream.items.len >= he + 4 + clen) break;
            } else if (chunked) {
                if (chunkedBodyComplete(stream.items, he + 4)) break;
            }
            // No Content-Length, no chunked: read until conn closes.
        }
    }

    const he = headers_end orelse return error.BodyTruncated;
    if (chunked and !chunkedBodyComplete(stream.items, he + 4)) return error.BodyTruncated;
    const final_status = status orelse try parseStatus(stream.items[0..he]);
    const headers = try parseHeaders(arena, stream.items[0..he]);

    const body_start = he + 4;
    const raw_body_end = if (content_length) |clen|
        @min(body_start + clen, stream.items.len)
    else
        stream.items.len;

    if (content_length) |clen| {
        if (raw_body_end < body_start + clen) return error.BodyTruncated;
    }

    const raw_body = stream.items[body_start..raw_body_end];
    const body = if (chunked)
        try decodeChunked(arena, raw_body)
    else
        try arena.dupe(u8, raw_body);

    return .{ .status = final_status, .headers = headers, .body = body };
}

/// Per RFC 7230 §3.3.3, these status codes MUST NOT carry a message
/// body. The drain loop must short-circuit on them rather than wait
/// for body bytes that will never arrive.
fn statusIsBodyless(status: u16) bool {
    return switch (status) {
        100...199, 204, 304 => true,
        else => false,
    };
}

fn drainResponseInto(arena: std.mem.Allocator, conn: *tls.Connection, target: []u8) Error!Response {
    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(arena);

    var headers_end: ?usize = null;
    while (headers_end == null) {
        var tmp: [16 * 1024]u8 = undefined;
        const n = try recvResponseBytes(conn, &tmp, stream.items.len);
        if (n == 0) return error.BodyTruncated;
        try stream.appendSlice(arena, tmp[0..n]);
        if (std.mem.indexOf(u8, stream.items, "\r\n\r\n")) |i| {
            if (i > MAX_HEADERS_BYTES) return error.HeadersTooLarge;
            headers_end = i;
        } else if (stream.items.len > MAX_HEADERS_BYTES) {
            return error.HeadersTooLarge;
        }
    }

    const he = headers_end.?;
    const status = try parseStatus(stream.items[0..he]);
    const headers = try parseHeaders(arena, stream.items[0..he]);
    const content_length = parseContentLength(stream.items[0..he]) orelse return error.BodyTruncated;
    if (parseTransferEncodingChunked(stream.items[0..he])) return error.BodyTruncated;
    if (content_length > target.len) return error.BodyTooLarge;

    const body_start = he + 4;
    const initial_body = stream.items[body_start..];
    var written: usize = @min(initial_body.len, content_length);
    if (written > 0) @memcpy(target[0..written], initial_body[0..written]);

    while (written < content_length) {
        var tmp: [16 * 1024]u8 = undefined;
        const n = try recvResponseBytes(conn, &tmp, he + 4 + written);
        if (n == 0) return error.BodyTruncated;
        const take = @min(n, content_length - written);
        @memcpy(target[written..][0..take], tmp[0..take]);
        written += take;
    }

    return .{ .status = status, .headers = headers, .body = target[0..written] };
}

/// `conn.recv` for a response of which `received` bytes have arrived. The peer closing (EOF, which is also how a TLS
/// close_notify ends) or resetting the connection before the first byte is `error.ClosedBeforeResponse`; a reset later
/// is `error.RecvFailed`, and a TLS stream ending without close_notify later is `error.BodyTruncated`: callers only
/// read again while the response is incomplete. Every other error, local ones such as running out of memory
/// included, passes through.
fn recvResponseBytes(conn: *tls.Connection, dest: []u8, received: usize) Error!usize {
    const n = conn.recv(dest) catch |err| switch (err) {
        error.ConnectionReset => return if (received == 0) error.ClosedBeforeResponse else error.RecvFailed,
        error.UncleanClose => return if (received == 0) error.ClosedBeforeResponse else error.BodyTruncated,
        else => return err,
    };
    if (n == 0 and received == 0) return error.ClosedBeforeResponse;
    return n;
}

fn parseTransferEncodingChunked(headers_bytes: []const u8) bool {
    var lines = std.mem.splitSequence(u8, headers_bytes, "\r\n");
    _ = lines.next();
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "transfer-encoding")) {
            // Chunked is the de-facto value S3 sends; tolerate "chunked" mixed
            // with other tokens.
            return std.ascii.findIgnoreCase(value, "chunked") != null;
        }
    }
    return false;
}

/// Returns true once `bytes[body_start..]` contains a complete chunked
/// body — i.e. the terminating zero-length chunk has been received.
fn chunkedBodyComplete(bytes: []const u8, body_start: usize) bool {
    if (body_start > bytes.len) return false;
    var i = body_start;
    while (i < bytes.len) {
        // Each chunk: <hex-len>[;ext]\r\n<data>\r\n
        const line_end = std.mem.indexOfPos(u8, bytes, i, "\r\n") orelse return false;
        const size_str_end = std.mem.indexOfScalarPos(u8, bytes, i, ';') orelse line_end;
        const size_str = std.mem.trim(u8, bytes[i..@min(size_str_end, line_end)], " \t");
        const chunk_len = std.fmt.parseInt(usize, size_str, 16) catch return false;
        const data_start = line_end + 2;
        if (chunk_len == 0) {
            // Trailers (optional headers) ending in CRLF, then final CRLF.
            // Find the final \r\n\r\n at or after data_start.
            return std.mem.indexOfPos(u8, bytes, data_start - 2, "\r\n\r\n") != null;
        }
        const data_end = data_start + chunk_len;
        if (data_end + 2 > bytes.len) return false; // need data + trailing CRLF
        i = data_end + 2;
    }
    return false;
}

fn decodeChunked(arena: std.mem.Allocator, raw: []const u8) Error![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(arena);
    var i: usize = 0;
    while (i < raw.len) {
        const line_end = std.mem.indexOfPos(u8, raw, i, "\r\n") orelse return error.BodyTruncated;
        const size_str_end = std.mem.indexOfScalarPos(u8, raw, i, ';') orelse line_end;
        const size_str = std.mem.trim(u8, raw[i..@min(size_str_end, line_end)], " \t");
        const chunk_len = std.fmt.parseInt(usize, size_str, 16) catch return error.BodyTruncated;
        const data_start = line_end + 2;
        if (chunk_len == 0) return out.toOwnedSlice(arena);
        if (chunk_len > raw.len - data_start or raw.len - data_start - chunk_len < 2) return error.BodyTruncated;
        const data_end = data_start + chunk_len;
        if (!std.mem.eql(u8, raw[data_end..][0..2], "\r\n")) return error.BodyTruncated;
        try out.appendSlice(arena, raw[data_start..data_end]);
        i = data_end + 2;
    }
    // No last chunk: the body was cut short.
    return error.BodyTruncated;
}

fn parseStatus(headers_bytes: []const u8) Error!u16 {
    const first_crlf = std.mem.indexOf(u8, headers_bytes, "\r\n") orelse headers_bytes.len;
    const status_line = headers_bytes[0..first_crlf];
    if (status_line.len < 12) return error.BadStatusLine;
    if (!std.mem.startsWith(u8, status_line, "HTTP/1.")) return error.BadStatusLine;
    // Status code is the 3 digits after "HTTP/1.x ".
    if (status_line.len < 12 or status_line[8] != ' ') return error.BadStatusLine;
    return std.fmt.parseInt(u16, status_line[9..12], 10) catch error.BadStatusLine;
}

fn parseContentLength(headers_bytes: []const u8) ?usize {
    var lines = std.mem.splitSequence(u8, headers_bytes, "\r\n");
    _ = lines.next(); // skip status line
    while (lines.next()) |line| {
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse continue;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        if (std.ascii.eqlIgnoreCase(name, "content-length")) {
            return std.fmt.parseInt(usize, value, 10) catch null;
        }
    }
    return null;
}

fn parseHeaders(arena: std.mem.Allocator, headers_bytes: []const u8) Error![]const Header {
    var list: std.ArrayList(Header) = .empty;
    errdefer list.deinit(arena);
    var lines = std.mem.splitSequence(u8, headers_bytes, "\r\n");
    _ = lines.next(); // skip status line
    while (lines.next()) |line| {
        if (line.len == 0) continue;
        const colon = std.mem.indexOfScalar(u8, line, ':') orelse return error.BadHeader;
        const name = line[0..colon];
        const value = std.mem.trim(u8, line[colon + 1 ..], " \t");
        try list.append(arena, .{
            .name = try arena.dupe(u8, name),
            .value = try arena.dupe(u8, value),
        });
    }
    return list.toOwnedSlice(arena);
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "parseStatus extracts 200" {
    try testing.expectEqual(@as(u16, 200), try parseStatus("HTTP/1.1 200 OK"));
    try testing.expectEqual(@as(u16, 206), try parseStatus("HTTP/1.1 206 Partial Content"));
    try testing.expectEqual(@as(u16, 404), try parseStatus("HTTP/1.1 404 Not Found"));
}

test "parseStatus rejects garbage" {
    try testing.expectError(error.BadStatusLine, parseStatus(""));
    try testing.expectError(error.BadStatusLine, parseStatus("hello"));
    try testing.expectError(error.BadStatusLine, parseStatus("HTTP/2 200 OK"));
}

test "parseContentLength finds the header" {
    const raw =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: text/plain\r\n" ++
        "Content-Length: 42\r\n" ++
        "X-Other: foo";
    try testing.expectEqual(@as(?usize, 42), parseContentLength(raw));
}

test "parseHeaders extracts name/value pairs" {
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();

    const raw =
        "HTTP/1.1 200 OK\r\n" ++
        "Content-Type: application/json\r\n" ++
        "ETag: \"abc\"\r\n";
    const hs = try parseHeaders(arena.allocator(), raw);
    try testing.expectEqual(@as(usize, 2), hs.len);
    try testing.expectEqualStrings("Content-Type", hs[0].name);
    try testing.expectEqualStrings("application/json", hs[0].value);
    try testing.expectEqualStrings("ETag", hs[1].name);
    try testing.expectEqualStrings("\"abc\"", hs[1].value);
}

test "Response.header is case-insensitive" {
    const headers = [_]Header{
        .{ .name = "Content-Type", .value = "application/json" },
        .{ .name = "Content-Length", .value = "42" },
    };
    const resp: Response = .{ .status = 200, .headers = &headers, .body = "" };
    try testing.expectEqualStrings("application/json", resp.header("CONTENT-TYPE").?);
    try testing.expectEqualStrings("42", resp.header("content-length").?);
    try testing.expect(resp.header("missing") == null);
}

test "buildRequestHead emits headers without copying body" {
    var head: std.ArrayList(u8) = .empty;
    defer head.deinit(testing.allocator);

    const headers = [_]Header{
        .{ .name = "X-Test", .value = "1" },
    };
    try buildRequestHead(testing.allocator, .{
        .method = .PUT,
        .host = "example.com",
        .path = "/object",
        .headers = &headers,
        .body = "payload",
    }, &head);

    try testing.expect(std.mem.indexOf(u8, head.items, "Content-Length: 7\r\n") != null);
    try testing.expect(std.mem.endsWith(u8, head.items, "\r\n\r\n"));
    try testing.expect(std.mem.indexOf(u8, head.items, "payload") == null);
}

test "a connection closed before any response byte is told apart from a truncated response" {
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds)) != .SUCCESS)
        return error.SkipZigTest;
    defer _ = std.posix.system.close(fds[1]);
    var conn: tls.Connection = .{ .fd = fds[0], .tls = undefined, .allocator = testing.allocator, .plain = true };
    defer conn.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var target: [16]u8 = undefined;

    // A response that starts and stops short is truncated...
    const partial = "HTTP/1.1 206 Partial Content\r\nContent-Length: 16\r\n\r\nabc";
    _ = std.posix.system.write(fds[1], partial.ptr, partial.len);
    _ = std.posix.system.shutdown(fds[1], std.posix.SHUT.WR);
    try testing.expectError(error.BodyTruncated, drainResponseInto(arena.allocator(), &conn, &target));
    // ...while EOF with nothing received is a connection the peer had already closed.
    try testing.expectError(error.ClosedBeforeResponse, drainResponseInto(arena.allocator(), &conn, &target));
    try testing.expectError(error.ClosedBeforeResponse, drainResponse(arena.allocator(), &conn, false));
}

test "only a closed or reset connection counts as closed before the response; local read errors propagate" {
    const posix = std.posix;
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var target: [16]u8 = undefined;

    // A peer that resets the connection before answering.
    {
        const ls = posix.system.socket(posix.AF.INET, posix.SOCK.STREAM, posix.IPPROTO.TCP);
        if (posix.errno(ls) != .SUCCESS) return error.SkipZigTest;
        const lfd: posix.fd_t = @intCast(ls);
        defer _ = posix.system.close(lfd);
        var addr: posix.sockaddr.in = .{ .port = 0, .addr = std.mem.nativeToBig(u32, 0x7f000001) };
        var len: posix.socklen_t = @sizeOf(posix.sockaddr.in);
        if (posix.errno(posix.system.bind(lfd, @ptrCast(&addr), len)) != .SUCCESS) return error.SkipZigTest;
        _ = posix.system.listen(lfd, 1);
        _ = posix.system.getsockname(lfd, @ptrCast(&addr), &len);
        var conn = try tls.Connection.connectPlain(testing.allocator, .loopback(std.mem.bigToNative(u16, addr.port)));
        defer conn.deinit();
        const peer: posix.fd_t = @intCast(posix.system.accept(lfd, null, null));
        const linger: extern struct { on: c_int, secs: c_int } = .{ .on = 1, .secs = 0 };
        _ = posix.system.setsockopt(peer, posix.SOL.SOCKET, posix.SO.LINGER, @ptrCast(&linger), @sizeOf(@TypeOf(linger)));
        _ = posix.system.close(peer);
        try testing.expectError(error.ClosedBeforeResponse, drainResponseInto(arena.allocator(), &conn, &target));
    }

    // A read that fails for a local reason (here: the descriptor is a directory) is not a dropped connection.
    {
        const rc = posix.system.open(".", .{ .ACCMODE = .RDONLY, .DIRECTORY = true }, @as(posix.mode_t, 0));
        if (posix.errno(rc) != .SUCCESS) return error.SkipZigTest;
        var conn: tls.Connection = .{ .fd = @intCast(rc), .tls = undefined, .allocator = testing.allocator, .plain = true };
        defer conn.deinit();
        try testing.expectError(error.RecvFailed, drainResponseInto(arena.allocator(), &conn, &target));
        try testing.expectError(error.RecvFailed, drainResponse(arena.allocator(), &conn, false));
    }
}

test "a HEAD response ends at its headers whatever Content-Length says" {
    var fds: [2]std.posix.fd_t = undefined;
    if (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds)) != .SUCCESS)
        return error.SkipZigTest;
    defer _ = std.posix.system.close(fds[1]);
    var conn: tls.Connection = .{ .fd = fds[0], .tls = undefined, .allocator = testing.allocator, .plain = true };
    defer conn.deinit();
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    // The peer stays open: reading for the advertised body would block forever.
    const resp = "HTTP/1.1 200 OK\r\nContent-Length: 1048576\r\nETag: \"abc-2\"\r\n\r\n";
    _ = std.posix.system.write(fds[1], resp.ptr, resp.len);
    const r = try sendRequest(arena.allocator(), &conn, .{ .method = .HEAD, .host = "h", .path = "/k", .headers = &.{} });
    try testing.expectEqual(@as(u16, 200), r.status);
    try testing.expectEqualStrings("\"abc-2\"", r.header("etag").?);
    try testing.expectEqual(@as(usize, 0), r.body.len);
}

const test_server = @import("tls_test_server.zig");

test "a TLS response cut short is an error whether or not the server sent close_notify" {
    var pki = try test_server.Pki.init();
    defer pki.deinit();
    var leaf = try pki.issue("leaf", "DNS:good.test");
    defer leaf.deinit();
    var context = try tls.ClientContext.init(.{ .ca_cert = pki.ca });
    defer context.deinit();

    const End = test_server.Behavior.End;
    const short = "HTTP/1.1 206 Partial Content\r\nContent-Length: 16\r\n\r\nabc";
    const unframed = "HTTP/1.1 200 OK\r\nConnection: close\r\n\r\nabc";
    const Case = struct { response: []const u8, end: End, into: bool = false, want: anyerror![]const u8 };
    for ([_]Case{
        .{ .response = short, .end = .close_notify, .want = error.BodyTruncated },
        .{ .response = short, .end = .tcp_close, .want = error.BodyTruncated },
        .{ .response = short, .end = .close_notify, .into = true, .want = error.BodyTruncated },
        .{ .response = short, .end = .tcp_close, .into = true, .want = error.BodyTruncated },
        // A chunked body that stops at a chunk boundary, before the last chunk.
        .{
            .response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n",
            .end = .close_notify,
            .want = error.BodyTruncated,
        },
        .{
            .response = "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\n3\r\nabc\r\n",
            .end = .tcp_close,
            .want = error.BodyTruncated,
        },
        // A body delimited by the end of the stream is whole only if the stream ended with close_notify.
        .{ .response = unframed, .end = .close_notify, .want = "abc" },
        .{ .response = unframed, .end = .tcp_close, .want = error.BodyTruncated },
        // Bytes that are not TLS are a read error, not the end of the body.
        .{ .response = short, .end = .garbage, .want = error.RecvFailed },
        .{ .response = short, .end = .garbage, .into = true, .want = error.RecvFailed },
        // A complete response is complete however the connection ends afterwards.
        .{ .response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", .end = .tcp_close, .want = "abc" },
        .{ .response = "HTTP/1.1 200 OK\r\nContent-Length: 3\r\n\r\nabc", .end = .garbage, .want = "abc" },
    }) |case| {
        var server: test_server.Server = undefined;
        try server.startWith(&leaf, 1, .{ .respond = .{ .response = case.response, .end = case.end } });
        defer server.finish();
        const peer: tls.Peer = .loopback(server.port);
        var conn = try tls.Connection.connectWithContext(testing.allocator, &context, peer, "good.test");
        defer conn.deinit();
        var arena = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena.deinit();
        const req: Request = .{ .method = .GET, .host = "good.test", .path = "/k", .headers = &.{} };
        var target: [16]u8 = undefined;
        const a = arena.allocator();
        const got = if (case.into) sendRequestInto(a, &conn, req, &target) else sendRequest(a, &conn, req);
        if (case.want) |body| {
            const resp = got catch |err| {
                std.debug.print("{s} / {t}: {t}\n", .{ case.response, case.end, err });
                return err;
            };
            try testing.expectEqualStrings(body, resp.body);
        } else |want_err| {
            if (got) |resp| {
                std.debug.print("{s} / {t}: accepted body {s}\n", .{ case.response, case.end, resp.body });
                return error.TestUnexpectedResult;
            } else |err| if (err != want_err) {
                std.debug.print("{s} / {t}: {t}, want {t}\n", .{ case.response, case.end, err, want_err });
                return error.TestUnexpectedError;
            }
        }
    }
}
