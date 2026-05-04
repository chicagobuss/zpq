//! Synchronous HTTP/1.1 client over TLS.
//!
//! Single request per call; no connection pool, no keep-alive. The
//! caller hands us a connected `tls.Connection`, we send the request
//! line + headers + body, then drain the response into an
//! arena-allocated body slice.
//!
//! Phase 3 surface is intentionally narrow:
//!   - GET / HEAD / PUT / POST methods
//!   - `Content-Length`-bounded response bodies (no chunked transfer
//!     yet — S3 GETs always return Content-Length)
//!   - Response headers as a name/value list (case-insensitive lookups)
//!   - Status code + raw body bytes
//!
//! Lifetime: the returned `Response.body` and `Response.headers` are
//! arena-allocated. Caller's arena owns them.

const std = @import("std");
const tls = @import("tls.zig");

pub const Error = error{
    SendFailed,
    RecvFailed,
    BadStatusLine,
    BadHeader,
    BodyTruncated,
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
/// in (the caller closes it via conn.deinit() — Phase 3 does one
/// request per connection so we don't reuse).
pub fn sendRequest(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    req: Request,
) Error!Response {
    // Build the request line + headers in one go.
    var buf: std.ArrayList(u8) = .empty;
    defer buf.deinit(arena);
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
    if (req.body.len > 0) try buf.appendSlice(arena, req.body);

    try conn.send(buf.items);

    return try drainResponse(arena, conn);
}

// ============================================================
// Response parsing
// ============================================================

const MAX_HEADERS_BYTES: usize = 64 * 1024;

fn drainResponse(arena: std.mem.Allocator, conn: *tls.Connection) Error!Response {
    // Read until \r\n\r\n appears, then keep reading the body.
    var stream: std.ArrayList(u8) = .empty;
    defer stream.deinit(arena);

    var headers_end: ?usize = null;
    var content_length: ?usize = null;
    var chunked: bool = false;

    while (true) {
        var tmp: [16 * 1024]u8 = undefined;
        const n = try conn.recv(&tmp);
        if (n == 0) break;
        try stream.appendSlice(arena, tmp[0..n]);
        if (headers_end == null) {
            if (std.mem.indexOf(u8, stream.items, "\r\n\r\n")) |i| {
                if (i > MAX_HEADERS_BYTES) return error.HeadersTooLarge;
                headers_end = i;
                content_length = parseContentLength(stream.items[0..i]);
                chunked = parseTransferEncodingChunked(stream.items[0..i]);
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
    const status = try parseStatus(stream.items[0..he]);
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

    return .{ .status = status, .headers = headers, .body = body };
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
            return std.ascii.indexOfIgnoreCase(value, "chunked") != null;
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
        if (chunk_len == 0) break;
        const data_end = data_start + chunk_len;
        if (data_end > raw.len) return error.BodyTruncated;
        try out.appendSlice(arena, raw[data_start..data_end]);
        i = data_end + 2; // skip trailing CRLF
    }
    return out.toOwnedSlice(arena);
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
