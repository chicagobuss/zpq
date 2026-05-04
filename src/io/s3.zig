//! Synchronous S3 GET client.
//!
//! Composes io.tls.Connection (HTTPS) + io.http (HTTP/1.1) +
//! io.sigv4 (SigV4 signing) into a one-call S3 GET. Range requests
//! are first-class for the footer-then-column-fetch pattern that
//! Parquet readers rely on.
//!
//! Phase 3 surface:
//!   pub fn open(env, bucket, key, region) !Client
//!   pub fn get(arena, range_or_null) !Response
//!   pub fn close()
//!
//! No connection pool, no concurrent requests, no PUT. Phase 3.B+
//! adds those.
//!
//! Credentials: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
//! optional `AWS_SESSION_TOKEN` are read from the environment.
//! Lambda always sets these from the execution role.
//!
//! DNS: getaddrinfo via libc. Linking libc adds ~150 KB to the
//! ReleaseSmall musl binary; rolling our own resolver is a future
//! follow-up.

const std = @import("std");
const tls = @import("tls.zig");
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");

pub const Error = error{
    NoCredentials,
    NoRegion,
    BadS3Url,
    DnsFailed,
    BadResponse,
    SignFailed,
} || tls.Error || http.Error || std.mem.Allocator.Error;

pub const Range = struct {
    start: u64,
    /// Inclusive end. Use the helper constructors below.
    end: u64,

    /// Bytes [start, end] inclusive, as the HTTP Range header expects.
    pub fn span(start: u64, end_inclusive: u64) Range {
        return .{ .start = start, .end = end_inclusive };
    }

    /// Last `n` bytes of the resource. Translated to a "bytes=-N"
    /// suffix request by the formatter.
    pub fn suffix(n: u64) Range {
        // Encoded by setting start = 0xFFFF_FFFF_FFFF_FFFF as a
        // sentinel — `format` checks this.
        return .{ .start = std.math.maxInt(u64), .end = n };
    }

    fn writeHeader(self: Range, buf: []u8) ![]const u8 {
        if (self.start == std.math.maxInt(u64)) {
            return std.fmt.bufPrint(buf, "bytes=-{d}", .{self.end});
        }
        return std.fmt.bufPrint(buf, "bytes={d}-{d}", .{ self.start, self.end });
    }
};

pub const Credentials = struct {
    access_key: []const u8,
    secret_key: []const u8,
    session_token: ?[]const u8 = null,
    region: []const u8,

    pub fn fromEnv(env: std.process.Environ) Error!Credentials {
        const ak = env.getPosix("AWS_ACCESS_KEY_ID") orelse return error.NoCredentials;
        const sk = env.getPosix("AWS_SECRET_ACCESS_KEY") orelse return error.NoCredentials;
        const region = env.getPosix("AWS_REGION") orelse return error.NoRegion;
        return .{
            .access_key = ak,
            .secret_key = sk,
            .session_token = env.getPosix("AWS_SESSION_TOKEN"),
            .region = region,
        };
    }
};

pub const Url = struct {
    bucket: []const u8,
    key: []const u8,

    /// Parse an `s3://bucket/key` URL.
    pub fn parse(s: []const u8) Error!Url {
        const prefix = "s3://";
        if (!std.mem.startsWith(u8, s, prefix)) return error.BadS3Url;
        const rest = s[prefix.len..];
        const slash = std.mem.indexOfScalar(u8, rest, '/') orelse return error.BadS3Url;
        if (slash == 0) return error.BadS3Url;
        if (slash + 1 >= rest.len) return error.BadS3Url;
        return .{ .bucket = rest[0..slash], .key = rest[slash + 1 ..] };
    }
};

/// Single-shot S3 GET. Resolves the host, opens TLS, signs the
/// request, sends it, drains the response.
///
/// Returns the Response (status + headers + body), all in `arena`.
pub fn get(
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    range: ?Range,
) Error!http.Response {
    // Construct virtual-hosted endpoint:
    //   <bucket>.s3.<region>.amazonaws.com
    const host = try std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ url.bucket, creds.region },
    );

    // Resolve to an IPv4 dotted-quad.
    const addr_v4 = try resolveIpv4(arena, host);

    // Build the request path (key prepended with /). We don't url-
    // encode here; AWS accepts most key bytes as-is. If the caller has
    // a key with funky characters, they pass an already-encoded one.
    const path = try std.fmt.allocPrint(arena, "/{s}", .{url.key});

    // Range header (if any).
    var range_buf: [64]u8 = undefined;
    const range_header_value: ?[]const u8 = if (range) |r|
        r.writeHeader(&range_buf) catch return error.BadResponse
    else
        null;

    // Sign the request. Always UNSIGNED-PAYLOAD for GET (we're on HTTPS).
    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };

    var hdr_in: std.ArrayList(sigv4.SigV4.Header) = .empty;
    defer hdr_in.deinit(arena);
    if (range_header_value) |rv| {
        try hdr_in.append(arena, .{ .name = "Range", .value = rv });
    }

    const signed_headers = signer.sign(
        arena,
        "GET",
        host,
        path,
        null,
        hdr_in.items,
        "",
        .{ .use_unsigned_payload = true },
    ) catch return error.SignFailed;

    // Translate sigv4.Header → http.Header (same shape but different
    // type to keep the modules independent).
    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(arena);
    for (signed_headers) |h| {
        try req_headers.append(arena, .{ .name = h.name, .value = h.value });
    }

    // Connect, send, drain.
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    return try http.sendRequest(arena, &conn, .{
        .method = .GET,
        .host = host,
        .path = path,
        .headers = req_headers.items,
    });
}

// ============================================================
// DNS via libc getaddrinfo
// ============================================================

const c = struct {
    extern fn getaddrinfo(
        node: [*:0]const u8,
        service: ?[*:0]const u8,
        hints: ?*const addrinfo,
        res: *?*addrinfo,
    ) c_int;
    extern fn freeaddrinfo(res: *addrinfo) void;

    const addrinfo = extern struct {
        flags: c_int,
        family: c_int,
        socktype: c_int,
        protocol: c_int,
        addrlen: u32,
        addr: ?*sockaddr,
        canonname: ?[*:0]u8,
        next: ?*addrinfo,
    };

    const sockaddr = extern struct {
        family: u16,
        port: u16,
        addr: u32, // for IPv4 only
        zero: [8]u8,
    };

    const AF_INET: c_int = 2;
};

fn resolveIpv4(arena: std.mem.Allocator, host: []const u8) Error![]const u8 {
    const host_z = try arena.dupeZ(u8, host);

    var hints = std.mem.zeroes(c.addrinfo);
    hints.family = c.AF_INET;
    var result: ?*c.addrinfo = null;
    const rc = c.getaddrinfo(host_z, null, &hints, &result);
    if (rc != 0 or result == null) return error.DnsFailed;
    defer c.freeaddrinfo(result.?);

    const sa = result.?.addr orelse return error.DnsFailed;
    const ip = std.mem.bigToNative(u32, sa.addr);
    return std.fmt.allocPrint(arena, "{d}.{d}.{d}.{d}", .{
        (ip >> 24) & 0xff,
        (ip >> 16) & 0xff,
        (ip >> 8) & 0xff,
        ip & 0xff,
    });
}

// ============================================================
// Tests
// ============================================================

const testing = std.testing;

test "parse s3 url" {
    const u = try Url.parse("s3://my-bucket/path/to/key.parquet");
    try testing.expectEqualStrings("my-bucket", u.bucket);
    try testing.expectEqualStrings("path/to/key.parquet", u.key);
}

test "parse s3 url rejects malformed" {
    try testing.expectError(error.BadS3Url, Url.parse("https://foo/bar"));
    try testing.expectError(error.BadS3Url, Url.parse("s3://bucket"));
    try testing.expectError(error.BadS3Url, Url.parse("s3:///key"));
}

test "Range.span formats inclusive byte range" {
    var buf: [64]u8 = undefined;
    const got = try Range.span(0, 1023).writeHeader(&buf);
    try testing.expectEqualStrings("bytes=0-1023", got);
}

test "Range.suffix formats negative-N suffix" {
    var buf: [64]u8 = undefined;
    const got = try Range.suffix(65536).writeHeader(&buf);
    try testing.expectEqualStrings("bytes=-65536", got);
}
