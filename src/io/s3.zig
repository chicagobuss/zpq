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

/// Multi-fetch S3 client over a single keep-alive TLS connection.
///
/// Lifecycle:
///   var client = try Client.init(allocator, creds, bucket);
///   defer client.deinit();
///   const r1 = try client.get(req_arena, key, range1);
///   const r2 = try client.get(req_arena, key, range2);  // reuses TLS
///
/// One DNS lookup + one TLS handshake amortized across N requests.
/// On a connection error during a request (e.g. S3 closed the idle
/// socket past its server-side keep-alive timeout), we close and
/// retry once with a fresh connection.
pub const Client = struct {
    /// Long-lived storage: the resolved IPv4 string, the host string,
    /// and the TLS connection itself.
    client_arena: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    host: []u8, // owned, in client_arena
    addr_v4: []const u8, // owned, in client_arena
    conn: ?tls.Connection,

    pub fn init(
        client_arena: std.mem.Allocator,
        creds: Credentials,
        bucket: []const u8,
    ) Error!Client {
        const host = try std.fmt.allocPrint(
            client_arena,
            "{s}.s3.{s}.amazonaws.com",
            .{ bucket, creds.region },
        );
        const addr_v4 = try resolveIpv4(client_arena, host);
        return .{
            .client_arena = client_arena,
            .creds = creds,
            .bucket = bucket,
            .host = host,
            .addr_v4 = addr_v4,
            .conn = null,
        };
    }

    pub fn deinit(self: *Client) void {
        if (self.conn) |*conn| {
            conn.deinit();
            self.conn = null;
        }
    }

    /// Send a GET. Reuses the existing TLS connection when possible;
    /// reconnects on a stale-connection error and retries once.
    /// Body / headers are arena-allocated in `req_arena`.
    pub fn get(
        self: *Client,
        req_arena: std.mem.Allocator,
        key: []const u8,
        range: ?Range,
    ) Error!http.Response {
        return self.sendOnce(req_arena, key, range) catch |err| switch (err) {
            // Stale-connection signals: server closed our idle socket
            // since the last request. Retry once with a fresh conn.
            error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => blk: {
                if (self.conn) |*conn| {
                    conn.deinit();
                    self.conn = null;
                }
                break :blk try self.sendOnce(req_arena, key, range);
            },
            else => return err,
        };
    }

    fn sendOnce(
        self: *Client,
        req_arena: std.mem.Allocator,
        key: []const u8,
        range: ?Range,
    ) Error!http.Response {
        if (self.conn == null) {
            self.conn = try tls.Connection.connect(
                self.client_arena,
                self.addr_v4,
                443,
                self.host,
            );
        }
        return try buildAndSend(
            req_arena,
            &self.conn.?,
            self.creds,
            self.host,
            key,
            range,
        );
    }
};

/// Single-shot S3 GET. Convenience wrapper for one-off fetches.
/// For multiple GETs against the same bucket, use Client which keeps
/// the TLS connection warm.
pub fn get(
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    range: ?Range,
) Error!http.Response {
    var client = try Client.init(arena, creds, url.bucket);
    defer client.deinit();
    return try client.get(arena, url.key, range);
}

fn buildAndSend(
    req_arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: Credentials,
    host: []const u8,
    key: []const u8,
    range: ?Range,
) Error!http.Response {
    const path = try std.fmt.allocPrint(req_arena, "/{s}", .{key});

    var range_buf: [64]u8 = undefined;
    const range_header_value: ?[]const u8 = if (range) |r|
        r.writeHeader(&range_buf) catch return error.BadResponse
    else
        null;

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };

    var hdr_in: std.ArrayList(sigv4.SigV4.Header) = .empty;
    defer hdr_in.deinit(req_arena);
    if (range_header_value) |rv| {
        try hdr_in.append(req_arena, .{ .name = "Range", .value = rv });
    }

    const signed_headers = signer.sign(
        req_arena,
        "GET",
        host,
        path,
        null,
        hdr_in.items,
        "",
        .{ .use_unsigned_payload = true },
    ) catch return error.SignFailed;

    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(req_arena);
    for (signed_headers) |h| {
        try req_headers.append(req_arena, .{ .name = h.name, .value = h.value });
    }

    return try http.sendRequest(req_arena, conn, .{
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
