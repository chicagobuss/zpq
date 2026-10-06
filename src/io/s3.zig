//! Synchronous S3 GET client.
//!
//! Composes io.tls.Connection (HTTPS) + io.http (HTTP/1.1) +
//! io.sigv4 (SigV4 signing) into a one-call S3 GET. Range requests
//! are first-class for the footer-then-column-fetch pattern that
//! Parquet readers rely on.
//!
//! Public surface:
//!   pub fn open(env, bucket, key, region) !Client
//!   pub fn get(arena, range_or_null) !Response
//!   pub fn close()
//!
//! Connection-pool helpers, concurrent range fetches, and multipart
//! uploads are layered below the single-call GET/PUT helpers.
//!
//! Credentials: `AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and
//! optional `AWS_SESSION_TOKEN` are read from the environment.
//! Lambda always sets these from the execution role.
//! `S3_NO_SIGN_REQUEST=1` sends requests unsigned instead (public buckets).
//!
//! DNS: getaddrinfo via libc. Linking libc adds ~150 KB to the
//! ReleaseSmall musl binary; rolling our own resolver is a future
//! follow-up.

const std = @import("std");
const Io = std.Io;
const tls = @import("tls.zig");
const work_cursor = @import("work_cursor.zig");
const AtomicWorkCursor = work_cursor.AtomicWorkCursor;
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");
const pool_mod = @import("pool.zig");
const multipart_sink = @import("multipart_sink.zig");
const retry = @import("retry.zig");

pub const Pool = pool_mod.Pool;
pub const PoolCriteria = pool_mod.Criteria;
pub const AnyPool = pool_mod.AnyPool;

/// Maximum concurrent in-flight requests against S3 (multipart parts
/// or split sub-range fetches). Larger pools increase memory and TLS
/// pressure before they improve throughput for the target workloads.
pub const MAX_PARTS: usize = 8;
/// S3's hard minimum for a non-final multipart part.
pub const MIN_PART_SIZE: usize = 5 * 1024 * 1024;
/// Below this size, single-PUT is faster than multipart — the
/// Create+Complete round-trips dominate.
pub const MULTIPART_THRESHOLD: usize = 32 * 1024 * 1024;
/// Above this size, fetchManyRanges splits a single range into
/// sub-ranges so we can fan out across the pool. Below it the
/// per-request TLS handshake amortizes badly across tiny fetches.
pub const SPLIT_THRESHOLD: usize = 24 * 1024 * 1024;
/// Target sub-range size when splitting. Kept separate from multipart
/// write part size; reads and writes have different round-trip and
/// buffering tradeoffs.
pub const TARGET_SUB_SIZE: usize = 19 * 1024 * 1024;

pub const Error = error{
    NoCredentials,
    NoRegion,
    BadS3Url,
    DnsFailed,
    /// The endpoint host is an IPv4 address in a spelling other than a canonical dotted quad (`127.1`, `0x7f.0.0.1`,
    /// `010.0.0.1`), which the resolver and the certificate check would read differently.
    NonCanonicalIpEndpoint,
    /// The endpoint host is an IPv6 address; connections are IPv4-only.
    Ipv6EndpointUnsupported,
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
    /// Optional S3-compatible endpoint override (e.g. R2:
    /// `https://<accountid>.r2.cloudflarestorage.com`). When set,
    /// requests target this host with **path-style** URLs (i.e.
    /// `/bucket/key` rather than `bucket.host/key`) and SigV4 signs
    /// against the endpoint's hostname. AWS S3 stays virtual-hosted-
    /// style. Read from `S3_ENDPOINT_URL` env var by `fromEnv`.
    /// The host may be a DNS name or an IPv4 address written as a dotted
    /// quad; IPv6 endpoints (`https://[::1]:9000`) are not supported.
    endpoint: ?[]const u8 = null,

    pub fn fromEnv(env: std.process.Environ) Error!Credentials {
        // `S3_NO_SIGN_REQUEST` (any value but empty or `0`) asks for anonymous
        // requests: no keys are read, and the empty access key tells the
        // signer to leave the Authorization header off. Only buckets that
        // allow public access answer these.
        if (env.getPosix("S3_NO_SIGN_REQUEST")) |v| {
            if (v.len > 0 and !std.mem.eql(u8, v, "0")) {
                return .{
                    .access_key = "",
                    .secret_key = "",
                    .region = env.getPosix("S3_REGION") orelse env.getPosix("AWS_REGION") orelse return error.NoRegion,
                    .endpoint = env.getPosix("S3_ENDPOINT_URL"),
                };
            }
        }
        // `S3_*`-prefixed vars override `AWS_*`. This lets a Lambda
        // function target a non-AWS S3-compatible endpoint (R2, MinIO,
        // GCS in compat mode) without colliding with Lambda's
        // reserved `AWS_*` env vars (which the runtime injects from
        // the execution role and refuses to let us override at deploy
        // time).
        //
        // Critically: session_token is paired with the access_key it was
        // issued for — never mix S3_* credentials with AWS_SESSION_TOKEN.
        // R2 (and most S3-compat servers) reject `X-Amz-Security-Token`
        // entirely, returning HTTP 400 InvalidArgument.
        const s3_ak = env.getPosix("S3_ACCESS_KEY_ID");
        const ak = s3_ak orelse env.getPosix("AWS_ACCESS_KEY_ID") orelse return error.NoCredentials;
        const sk = if (s3_ak != null)
            env.getPosix("S3_SECRET_ACCESS_KEY") orelse return error.NoCredentials
        else
            env.getPosix("AWS_SECRET_ACCESS_KEY") orelse return error.NoCredentials;
        const region = if (s3_ak != null)
            env.getPosix("S3_REGION") orelse env.getPosix("AWS_REGION") orelse return error.NoRegion
        else
            env.getPosix("AWS_REGION") orelse env.getPosix("S3_REGION") orelse return error.NoRegion;
        const session_token = if (s3_ak != null)
            env.getPosix("S3_SESSION_TOKEN")
        else
            env.getPosix("AWS_SESSION_TOKEN");
        return .{
            .access_key = ak,
            .secret_key = sk,
            .session_token = session_token,
            .region = region,
            .endpoint = env.getPosix("S3_ENDPOINT_URL"),
        };
    }

    /// Strip protocol, optional port, and trailing slash from an endpoint URL,
    /// returning just the DNS hostname. Doesn't allocate; result is a slice into
    /// the input. `http://minio:9000/` -> `minio`.
    pub fn endpointHost(self: Credentials) ?[]const u8 {
        var h = self.endpointAuthority() orelse return null;
        if (h.len > 0 and h[0] == '[') {
            const close = std.mem.indexOfScalar(u8, h, ']') orelse return h;
            return h[1..close];
        }
        if (std.mem.lastIndexOfScalar(u8, h, ':')) |colon| {
            h = h[0..colon];
        }
        return h;
    }

    /// Host header authority: hostname plus explicit port, when supplied.
    pub fn endpointAuthority(self: Credentials) ?[]const u8 {
        const ep = self.endpoint orelse return null;
        var h = ep;
        if (std.mem.startsWith(u8, h, "https://")) h = h["https://".len..];
        if (std.mem.startsWith(u8, h, "http://")) h = h["http://".len..];
        if (std.mem.endsWith(u8, h, "/")) h = h[0 .. h.len - 1];
        return h;
    }

    pub fn endpointUseTls(self: Credentials) bool {
        const ep = self.endpoint orelse return true;
        return !std.mem.startsWith(u8, ep, "http://");
    }

    pub fn endpointPort(self: Credentials) u16 {
        var authority = self.endpointAuthority() orelse return 443;
        if (std.mem.lastIndexOfScalar(u8, authority, ']')) |close| authority = authority[close + 1 ..];
        if (std.mem.lastIndexOfScalar(u8, authority, ':')) |colon| {
            return std.fmt.parseInt(u16, authority[colon + 1 ..], 10) catch defaultPort(self.endpointUseTls());
        }
        return defaultPort(self.endpointUseTls());
    }
};

fn defaultPort(use_tls: bool) u16 {
    return if (use_tls) 443 else 80;
}

/// Whether requests name `bucket` in the host (virtual-hosted) rather than in the path. Only on AWS, and only for a
/// bucket without dots: AWS's certificate covers `*.s3.<region>.amazonaws.com`, one label, so `my.bucket.s3...` fails
/// hostname verification and dotted buckets go path-style, as the AWS SDKs send them.
pub fn virtualHosted(creds: Credentials, bucket: []const u8) bool {
    return creds.endpoint == null and std.mem.indexOfScalar(u8, bucket, '.') == null;
}

/// Construct the request hostname for `bucket` under these creds.
/// AWS S3: `<bucket>.s3.<region>.amazonaws.com` (virtual-hosted), or
/// `s3.<region>.amazonaws.com` for a dotted bucket (path-style).
/// Custom endpoint: the endpoint host (path-style; bucket is in the path).
pub fn hostFor(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) ![]u8 {
    if (creds.endpointAuthority()) |h| {
        return arena.dupe(u8, h);
    }
    return awsHost(arena, creds, bucket);
}

pub fn connectHostFor(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) ![]u8 {
    if (creds.endpointHost()) |h| {
        return arena.dupe(u8, h);
    }
    return awsHost(arena, creds, bucket);
}

fn awsHost(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) ![]u8 {
    if (!virtualHosted(creds, bucket)) return std.fmt.allocPrint(arena, "s3.{s}.amazonaws.com", .{creds.region});
    return std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
}

pub fn portFor(creds: Credentials) u16 {
    return creds.endpointPort();
}

pub fn useTls(creds: Credentials) bool {
    return creds.endpointUseTls();
}

pub fn poolCriteria(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) !PoolCriteria {
    const host = try hostFor(arena, creds, bucket);
    const connect_host = try connectHostFor(arena, creds, bucket);
    const addrs = try resolveIpv4(arena, connect_host);
    return .{ .host = host, .addrs = addrs, .port = portFor(creds), .use_tls = useTls(creds) };
}

/// A one-off connection to `host`, at the first of `addrs` that accepts.
pub fn connect(
    arena: std.mem.Allocator,
    creds: Credentials,
    addrs: []const tls.Ipv4,
    host: []const u8,
) Error!tls.Connection {
    const peer: tls.Peer = .{ .addrs = addrs, .port = portFor(creds) };
    if (useTls(creds)) return try tls.Connection.connect(arena, peer, host);
    return try tls.Connection.connectPlain(arena, peer);
}

/// Construct the HTTP request path. Virtual-hosted: `/<key>`.
/// Path-style (custom endpoint, dotted bucket): `/<bucket>/<key>`. Both URI-encode the
/// key per RFC 3986 — both the path component and the SigV4 canonical
/// URI MUST use this same encoding.
pub fn pathFor(
    arena: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
) ![]u8 {
    const encoded_key = try buildEncodedPath(arena, key);
    if (virtualHosted(creds, bucket)) return encoded_key;
    // Path-style: prefix with /<bucket>. encoded_key already starts with `/`.
    return std.fmt.allocPrint(arena, "/{s}{s}", .{ bucket, encoded_key });
}

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
    addrs: []const tls.Ipv4, // owned, in client_arena
    conn: ?tls.Connection,

    pub fn init(
        client_arena: std.mem.Allocator,
        creds: Credentials,
        bucket: []const u8,
    ) Error!Client {
        const host = try hostFor(client_arena, creds, bucket);
        const connect_host = try connectHostFor(client_arena, creds, bucket);
        const addrs = try resolveIpv4(client_arena, connect_host);
        return .{
            .client_arena = client_arena,
            .creds = creds,
            .bucket = bucket,
            .host = host,
            .addrs = addrs,
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
            error.ClosedBeforeResponse, error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => blk: {
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
            self.conn = try connect(self.client_arena, self.creds, self.addrs, self.host);
        }
        return try buildAndSend(
            req_arena,
            &self.conn.?,
            self.creds,
            self.host,
            self.bucket,
            key,
            .{ .range = range },
        );
    }

    /// One ListObjectsV2 page. Reuses the cached TLS connection.
    /// Caller drives pagination — pass the previous response's
    /// `next_token` back in to fetch the next page; null = first page.
    /// `prefix` is sent verbatim (URL-encoded by us); empty string lists
    /// the whole bucket (bounded by max_keys).
    pub fn list(
        self: *Client,
        req_arena: std.mem.Allocator,
        prefix: []const u8,
        max_keys: u32,
        continuation_token: ?[]const u8,
    ) Error!ListPage {
        if (self.conn == null) {
            self.conn = try connect(self.client_arena, self.creds, self.addrs, self.host);
        }
        return try sendListV2(
            req_arena,
            &self.conn.?,
            self.creds,
            self.host,
            self.bucket,
            prefix,
            max_keys,
            continuation_token,
        );
    }
};

pub const ListEntry = struct {
    key: []const u8, // arena-owned
    size: u64,
};

pub const ListPage = struct {
    entries: []const ListEntry,
    /// Set when the response had `<IsTruncated>true</IsTruncated>` —
    /// pass back to `Client.list` for the next page.
    next_token: ?[]const u8,
};

/// List every key under `prefix`. Drives pagination internally; for
/// prefixes with > 1000 keys this issues multiple round-trips.
pub fn listAll(
    req_arena: std.mem.Allocator,
    client: *Client,
    prefix: []const u8,
) Error![]const ListEntry {
    var all: std.ArrayList(ListEntry) = .empty;
    var token: ?[]const u8 = null;
    while (true) {
        const page = try client.list(req_arena, prefix, 1000, token);
        for (page.entries) |e| try all.append(req_arena, e);
        token = page.next_token;
        if (token == null) break;
    }
    return all.items;
}

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

/// Single-shot S3 PUT. Body must fit in one HTTP request — S3's per-PUT
/// limit is 5 GB. Use uploadMultipart for larger or parallelism.
///
/// Fresh TLS handshake per call. For warm Lambda invocations where a
/// pool already exists for the same host, prefer `putViaPool` —
/// reusing a pooled connection saves ~50 ms of handshake.
pub fn put(
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) Error!http.Response {
    const host = try hostFor(arena, creds, url.bucket);
    const connect_host = try connectHostFor(arena, creds, url.bucket);
    const addrs = try resolveIpv4(arena, connect_host);
    var conn = try connect(arena, creds, addrs, host);
    defer conn.deinit();
    return try sendPut(arena, &conn, creds, host, url.bucket, url.key, body);
}

/// Same as `get`, but acquires a connection from the supplied pool
/// instead of opening a fresh one. Caller is responsible for the
/// pool's host matching `url.bucket`. A pooled connection found dead
/// is retried at once on a new one; server errors back off (`retry`).
pub fn getViaPool(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    range: ?Range,
) !http.Response {
    return getViaPoolWithOpts(io, p, arena, creds, url, .{ .range = range });
}

/// Extra request options for ranged/conditional GET. Each field is
/// optional; default-init reproduces a plain Range-only GET.
pub const GetOpts = struct {
    range: ?Range = null,
    /// `If-None-Match: <etag>`. Server replies 304 (Not Modified) if
    /// the object's current ETag matches; the response body is empty
    /// in that case. Used by the metadata cache to skip the
    /// data-page-bearing rounds when an object is unchanged.
    if_none_match: ?[]const u8 = null,
};

pub fn getViaPoolWithOpts(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    opts: GetOpts,
) !http.Response {
    const host = try hostFor(arena, creds, url.bucket);
    const criteria = try poolCriteria(arena, creds, url.bucket);
    const Get = struct {
        creds: Credentials,
        host: []const u8,
        url: Url,
        opts: GetOpts,
        fn send(self: @This(), a: std.mem.Allocator, conn: *tls.Connection) !http.Response {
            return buildAndSend(a, conn, self.creds, self.host, self.url.bucket, self.url.key, self.opts);
        }
    };
    return requestViaPool(io, p, arena, criteria, .{ .idempotent = true }, Get{ .creds = creds, .host = host, .url = url, .opts = opts }, Get.send);
}

pub const RequestOpts = struct {
    /// See `retry.Request.idempotent`; decides whether a dead pooled connection is retried at once or fails the request.
    idempotent: bool,
};

/// One request over pooled connections under the retry policy: `send(context, arena, conn)` performs one exchange and
/// returns the response. Failing to get a connection (connect, TLS handshake) counts as a failed attempt on a fresh
/// connection and backs off like one; a reused connection found dead retries at once on a new one; a throttle or
/// server-error status backs off, and is returned as is once the budget runs out. Every other status is the answer.
pub fn requestViaPool(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    criteria: PoolCriteria,
    opts: RequestOpts,
    context: anytype,
    comptime send: fn (@TypeOf(context), std.mem.Allocator, *tls.Connection) anyerror!http.Response,
) !http.Response {
    var throttled: ?http.Response = null;
    var attempts: retry.Attempts = .{};
    while (true) {
        var last_err: anyerror = error.RetryableStatus;
        const failure: retry.Failure = blk: {
            const handle = p.acquire(io, criteria, .{ .fresh = attempts.fresh }) catch |err| {
                last_err = err;
                break :blk retry.classifyConnect(err);
            };
            if (send(context, arena, handle.conn)) |resp| {
                p.release(io, handle);
                if (!retry.retryableStatus(resp.status)) return resp;
                throttled = resp;
                break :blk .transient;
            } else |err| {
                p.discard(io, handle);
                last_err = err;
                break :blk retry.classify(err, .{ .idempotent = opts.idempotent, .reused = handle.reused });
            }
        };
        if (failure == .fatal) return last_err;
        if (!try attempts.retryAfter(io, failure)) {
            // Out of budget: a throttled answer tells the caller more than the connection error does.
            if (throttled) |r| return r;
            return last_err;
        }
        p.noteRetry(io, failure);
    }
}

/// Same as `put`, but acquires a connection from the supplied pool
/// instead of opening a fresh one, retrying as `getViaPool` does.
pub fn putViaPool(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) !http.Response {
    const host = try hostFor(arena, creds, url.bucket);
    const criteria = try poolCriteria(arena, creds, url.bucket);
    const Put = struct {
        creds: Credentials,
        host: []const u8,
        url: Url,
        body: []const u8,
        fn send(self: @This(), a: std.mem.Allocator, conn: *tls.Connection) !http.Response {
            return sendPut(a, conn, self.creds, self.host, self.url.bucket, self.url.key, self.body);
        }
    };
    return requestViaPool(io, p, arena, criteria, .{ .idempotent = true }, Put{ .creds = creds, .host = host, .url = url, .body = body }, Put.send);
}

fn sendPut(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: Credentials,
    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    body: []const u8,
) Error!http.Response {
    const path = try pathFor(arena, creds, bucket, key);

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };

    const signed_headers = signer.sign(
        arena,
        "PUT",
        host,
        path,
        null,
        &.{},
        body,
        .{ .use_unsigned_payload = true },
    ) catch return error.SignFailed;

    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(arena);
    for (signed_headers) |h| {
        try req_headers.append(arena, .{ .name = h.name, .value = h.value });
    }

    return try http.sendRequest(arena, conn, .{
        .method = .PUT,
        .host = host,
        .path = path,
        .headers = req_headers.items,
        .body = body,
    });
}

/// One unit of work for `fetchJobs` — fetch the bytes of `range` from
/// (bucket, key) and write them into `target`. `target.len` must equal
/// `range.end - range.start`. Multiple jobs from different files can
/// be submitted to one `fetchJobs` call so the pool's parallelism is
/// shared across all of them.
pub const FetchJob = struct {
    bucket: []const u8,
    key: []const u8,
    range: Range, // exclusive end (coalescer semantics)
    target: []u8,
};

/// Parallel multi-file range fetch using a shared `Pool`. Splits any
/// input range larger than SPLIT_THRESHOLD into sub-ranges, then runs them through `workers` worker loops. Returns
/// total bytes fetched.
///
/// Dispatches ranges from different `(bucket, key)` inputs
/// interchangeably through the same bounded pool, so multi-file scans
/// share one global connection budget.
///
/// `workers` is clamped to pool capacity and to the job count. Workers start through `startWorkers`, so an `Io`
/// whose concurrency limit is reached delays the batch instead of failing it.
pub fn fetchJobs(
    io: Io,
    p: anytype, // *Pool(N) for some comptime N
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    creds: Credentials,
    jobs: []const FetchJob,
    workers: usize,
) !u64 {
    // 1. Pre-process: split big ranges into sub-jobs.
    var split: std.ArrayList(FetchJob) = .empty;
    defer split.deinit(arena);
    for (jobs) |j| {
        std.debug.assert(j.target.len == j.range.end - j.range.start);
        const len = j.range.end - j.range.start;
        if (len > SPLIT_THRESHOLD) {
            const k = @max(@as(u64, 2), len / TARGET_SUB_SIZE);
            const sub = (len + k - 1) / k; // ceil
            var i: u64 = 0;
            while (i < k) : (i += 1) {
                const sub_off_start: usize = @intCast(i * sub);
                const sub_off_end: usize = @intCast(@min((i + 1) * sub, len));
                if (sub_off_start >= sub_off_end) break;
                try split.append(arena, .{
                    .bucket = j.bucket,
                    .key = j.key,
                    .range = .{
                        .start = j.range.start + @as(u64, sub_off_start),
                        .end = j.range.start + @as(u64, sub_off_end),
                    },
                    .target = j.target[sub_off_start..sub_off_end],
                });
            }
        } else {
            try split.append(arena, j);
        }
    }
    if (split.items.len == 0) return 0;

    // 2. Resolve host+addr once per bucket. `Pool(N)` gates in-flight
    // requests globally, but idle TLS sessions are keyed by this criteria.
    const HostInfo = struct { host: []const u8, addrs: []const tls.Ipv4, port: u16, use_tls: bool };
    var hosts: std.StringHashMapUnmanaged(HostInfo) = .empty;
    defer hosts.deinit(arena);
    for (split.items) |j| {
        if (hosts.get(j.bucket) != null) continue;
        const h = try hostFor(arena, creds, j.bucket);
        const connect_host = try connectHostFor(arena, creds, j.bucket);
        const a = try resolveIpv4(arena, connect_host);
        try hosts.put(arena, j.bucket, .{
            .host = h,
            .addrs = a,
            .port = portFor(creds),
            .use_tls = useTls(creds),
        });
    }

    // 3. Per-sub-job context.
    var ctxs = try arena.alloc(FetchCtx, split.items.len);
    for (split.items, 0..) |j, i| {
        const hi = hosts.get(j.bucket).?;
        ctxs[i] = .{
            .pool = AnyPool.of(p),
            .gpa = gpa,
            .creds = creds,
            .bucket = j.bucket,
            .key = j.key,
            .host = hi.host,
            .addrs = hi.addrs,
            .port = hi.port,
            .use_tls = hi.use_tls,
            .range = j.range,
            .target = j.target,
            .ok = false,
        };
    }

    // 4. Pool permits gate *sockets*, not threads: `Io.Group.concurrent` spawns an OS thread whenever all workers are
    //    busy, and a worker parked on a blocking socket read is busy, so submitting every sub-job at once created far
    //    more threads than permits. Completion order is arbitrary either way — nothing may depend on it.
    var shared: AtomicWorkCursor(FetchCtx) = .{ .items = ctxs };
    var group: Io.Group = .init;
    defer group.cancel(io);
    // Floor of 1: zero workers with jobs pending would leave every `ctx.ok` false and surface as `RangeFetchFailed`
    // rather than as the bad argument it is.
    const n_workers = @max(1, @min(@min(workers, @TypeOf(p.*).capacity), ctxs.len));
    try work_cursor.startWorkers(FetchCtx, &group, io, &shared, n_workers, io, fetchOneTask);
    try group.await(io);

    // 5. Tally + check for failures.
    var fetched: u64 = 0;
    for (ctxs) |ctx| {
        if (!ctx.ok) return error.RangeFetchFailed;
        fetched += ctx.target.len;
    }
    return fetched;
}

/// Backward-compat wrapper: single-file ranges into a sparse `into`
/// buffer (each range is written at its absolute file offset). Built
/// on top of fetchJobs.
pub fn fetchManyRanges(
    io: Io,
    p: anytype,
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
    ranges: []const Range,
    into: []u8,
) !u64 {
    var jobs: std.ArrayList(FetchJob) = .empty;
    defer jobs.deinit(arena);
    try jobs.ensureTotalCapacity(arena, ranges.len);
    for (ranges) |r| {
        try jobs.append(arena, .{
            .bucket = bucket,
            .key = key,
            .range = r,
            .target = into[@intCast(r.start)..@intCast(r.end)],
        });
    }
    return fetchJobs(io, p, gpa, arena, creds, jobs.items, @TypeOf(p.*).capacity);
}

const FetchCtx = struct {
    pool: AnyPool,
    gpa: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
    host: []const u8,
    addrs: []const tls.Ipv4,
    port: u16,
    use_tls: bool,
    range: Range,
    /// Pre-sized target buffer; `target.len == range.end - range.start`.
    target: []u8,
    ok: bool,
};

/// Fetch `ctx.range` into `ctx.target`, setting `ctx.ok` on success, retrying as `requestViaPool` does.
fn fetchOneTask(io: Io, ctx: *FetchCtx) Io.Cancelable!void {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const criteria: PoolCriteria = .{
        .host = ctx.host,
        .addrs = ctx.addrs,
        .port = ctx.port,
        .use_tls = ctx.use_tls,
    };
    const resp = requestViaPool(io, ctx.pool, arena_state.allocator(), criteria, .{ .idempotent = true }, ctx, doFetch) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => return,
    };
    // Any status but a full 200/206 (including a throttle that outlasted the budget) fails this fetch.
    ctx.ok = (resp.status == 206 or resp.status == 200) and resp.body.len == ctx.target.len;
}

/// Issue one ranged GET on `conn`, streaming the body into `ctx.target`.
fn doFetch(ctx: *FetchCtx, arena: std.mem.Allocator, conn: *tls.Connection) !http.Response {
    const path = try pathFor(arena, ctx.creds, ctx.bucket, ctx.key);

    var range_buf: [64]u8 = undefined;
    const inclusive: Range = .{ .start = ctx.range.start, .end = ctx.range.end - 1 };
    const range_header = try inclusive.writeHeader(&range_buf);

    const signer: sigv4.SigV4 = .{
        .region = ctx.creds.region,
        .access_key = ctx.creds.access_key,
        .secret_key = ctx.creds.secret_key,
        .session_token = ctx.creds.session_token,
    };

    var hdr_in: std.ArrayList(sigv4.SigV4.Header) = .empty;
    try hdr_in.append(arena, .{ .name = "Range", .value = range_header });

    const signed = try signer.sign(arena, "GET", ctx.host, path, null, hdr_in.items, "", .{ .use_unsigned_payload = true });

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    return http.sendRequestInto(arena, conn, .{
        .method = .GET,
        .host = ctx.host,
        .path = path,
        .headers = headers.items,
    }, ctx.target);
}

/// Parallel multipart upload of `body` to `url`. Thin wrapper around
/// `multipart_sink.MultipartSink`: build a sink, push the entire body,
/// close. The sink internally chunks at multipart_sink.TARGET_PART_SIZE,
/// dispatches `UploadPart` workers on a long-lived `Io.Group`, and
/// either calls `CompleteMultipartUpload` or falls back to a single
/// `PutObject` if the body never exceeded one part.
///
/// Kept for backward compatibility while step 4 of B3 migrates the
/// lambda call sites to construct the sink directly. Once migrated,
/// this function can be deleted along with the `MIN_PART_SIZE` guard.
pub fn uploadMultipart(
    io: Io,
    p: anytype, // *Pool(N) shared with the caller when buckets match
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) !void {
    if (body.len < MIN_PART_SIZE) return error.BodyTooSmallForMultipart;

    const criteria = try poolCriteria(arena, creds, url.bucket);
    var sink = multipart_sink.MultipartSink.init(
        io,
        gpa,
        arena,
        creds,
        url,
        p,
        criteria,
        .{},
    );
    defer {
        if (!sink.isClosed()) sink.abort();
        sink.deinit();
    }

    try sink.push(body);
    try sink.close();
}

pub fn extractXml(arena: std.mem.Allocator, xml: []const u8, tag: []const u8) ![]const u8 {
    const open = try std.fmt.allocPrint(arena, "<{s}>", .{tag});
    const close = try std.fmt.allocPrint(arena, "</{s}>", .{tag});
    const start = std.mem.indexOf(u8, xml, open) orelse return error.XmlTagMissing;
    const after_open = start + open.len;
    const end = std.mem.indexOfPos(u8, xml, after_open, close) orelse return error.XmlTagMissing;
    return try arena.dupe(u8, xml[after_open..end]);
}

fn buildAndSend(
    req_arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: Credentials,
    host: []const u8,
    bucket: []const u8,
    key: []const u8,
    opts: GetOpts,
) Error!http.Response {
    const path = try pathFor(req_arena, creds, bucket, key);

    var range_buf: [64]u8 = undefined;
    const range_header_value: ?[]const u8 = if (opts.range) |r|
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
    if (opts.if_none_match) |etag| {
        // Sign with the conditional header so AWS doesn't reject the
        // request as malformed signed-headers. SigV4 requires every
        // header sent to be either signed or x-ignore-* style; safest
        // is to include it.
        try hdr_in.append(req_arena, .{ .name = "If-None-Match", .value = etag });
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

/// Issue a ListObjectsV2 request and parse one page of `<Contents>`
/// entries plus an optional `<NextContinuationToken>`. Pagination
/// driver lives in `listAll`; this is the single-RT primitive.
fn sendListV2(
    req_arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: Credentials,
    host: []const u8,
    bucket: []const u8,
    prefix: []const u8,
    max_keys: u32,
    continuation_token: ?[]const u8,
) Error!ListPage {
    // Path: virtual-hosted is "/"; path-style (R2 / endpoint override /
    // dotted bucket) is "/<bucket>". Same logic as `pathFor` for an empty key.
    const list_path: []const u8 = if (virtualHosted(creds, bucket))
        "/"
    else
        try std.fmt.allocPrint(req_arena, "/{s}", .{bucket});

    // SigV4 canonical query string: params URL-encoded, sorted by key.
    // Three params (continuation-token, list-type, max-keys, prefix —
    // sorted: c, l, m, p). Prefix encoded; the rest are tokens.
    const encoded_prefix = try urlEncodeForQuery(req_arena, prefix);
    const max_keys_str = try std.fmt.allocPrint(req_arena, "{d}", .{max_keys});
    const query = if (continuation_token) |tok| blk: {
        const enc = try urlEncodeForQuery(req_arena, tok);
        break :blk try std.fmt.allocPrint(req_arena, "continuation-token={s}&list-type=2&max-keys={s}&prefix={s}", .{ enc, max_keys_str, encoded_prefix });
    } else try std.fmt.allocPrint(req_arena, "list-type=2&max-keys={s}&prefix={s}", .{ max_keys_str, encoded_prefix });

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = signer.sign(
        req_arena,
        "GET",
        host,
        list_path,
        query,
        &.{},
        "",
        .{ .use_unsigned_payload = true },
    ) catch return error.SignFailed;

    var req_headers: std.ArrayList(http.Header) = .empty;
    defer req_headers.deinit(req_arena);
    for (signed) |h| try req_headers.append(req_arena, .{ .name = h.name, .value = h.value });

    const path_with_query = try std.fmt.allocPrint(req_arena, "{s}?{s}", .{ list_path, query });

    const resp = try http.sendRequest(req_arena, conn, .{
        .method = .GET,
        .host = host,
        .path = path_with_query,
        .headers = req_headers.items,
    });
    if (resp.status != 200) return error.BadResponse;

    return parseListV2Body(req_arena, resp.body);
}

/// SigV4 canonical-query URL encoding: RFC 3986 unreserved chars
/// pass through; everything else becomes `%HH`.
fn urlEncodeForQuery(arena: std.mem.Allocator, s: []const u8) ![]const u8 {
    var out: std.ArrayList(u8) = .empty;
    for (s) |b| {
        const ok = (b >= 'A' and b <= 'Z') or
            (b >= 'a' and b <= 'z') or
            (b >= '0' and b <= '9') or
            b == '-' or b == '_' or b == '.' or b == '~';
        if (ok) {
            try out.append(arena, b);
        } else {
            try out.print(arena, "%{X:0>2}", .{b});
        }
    }
    return out.items;
}

/// Tiny XML scraper for the ListObjectsV2 response. Pulls every
/// `<Key>` + `<Size>` from `<Contents>` blocks plus the optional
/// `<NextContinuationToken>`. Doesn't do a real XML parse — the
/// response shape is fixed and AWS/R2 don't put surprising entities
/// in keys we care about. If a real parser becomes necessary it
/// goes here.
fn parseListV2Body(arena: std.mem.Allocator, body: []const u8) Error!ListPage {
    var entries: std.ArrayList(ListEntry) = .empty;
    var pos: usize = 0;
    while (std.mem.indexOfPos(u8, body, pos, "<Contents>")) |start| {
        const block_end = std.mem.indexOfPos(u8, body, start, "</Contents>") orelse break;
        const key = extractTag(body[start..block_end], "<Key>", "</Key>") orelse {
            pos = block_end + "</Contents>".len;
            continue;
        };
        const size_s = extractTag(body[start..block_end], "<Size>", "</Size>") orelse "0";
        const size = std.fmt.parseInt(u64, size_s, 10) catch 0;
        try entries.append(arena, .{
            .key = try arena.dupe(u8, key),
            .size = size,
        });
        pos = block_end + "</Contents>".len;
    }

    var next_token: ?[]const u8 = null;
    if (extractTag(body, "<NextContinuationToken>", "</NextContinuationToken>")) |tok| {
        next_token = try arena.dupe(u8, tok);
    }
    return .{ .entries = entries.items, .next_token = next_token };
}

fn extractTag(haystack: []const u8, open_tag: []const u8, close_tag: []const u8) ?[]const u8 {
    const start = std.mem.indexOf(u8, haystack, open_tag) orelse return null;
    const after_open = start + open_tag.len;
    const end = std.mem.indexOfPos(u8, haystack, after_open, close_tag) orelse return null;
    return haystack[after_open..end];
}

// ============================================================
// DNS via libc getaddrinfo
// ============================================================

// `std.c.addrinfo` follows each OS's layout: Darwin orders `canonname` before `addr`, the reverse of Linux.
const c = std.c;

/// Refuse a host the resolver would take as an address but the TLS layer would not: an IPv6 literal (connections are
/// IPv4-only), or IPv4 in a legacy spelling getaddrinfo still accepts (`127.1`, `0x7f.0.0.1`, `2130706433`, `010.0.0.1`
/// in octal). Such a host would be dialled as one address and verified as a DNS name, so it is an error, not something
/// to normalise behind the user's back.
fn checkNumericHost(host_z: [:0]const u8) Error!void {
    if (std.mem.indexOfScalar(u8, host_z, ':') != null) {
        std.log.warn("S3 endpoint host '{s}' is an IPv6 address; IPv6 endpoints are not supported", .{host_z});
        return error.Ipv6EndpointUnsupported;
    }
    var hints = std.mem.zeroes(c.addrinfo);
    hints.family = c.AF.INET;
    hints.socktype = c.SOCK.STREAM;
    hints.flags = .{ .NUMERICHOST = true };
    var result: ?*c.addrinfo = null;
    if (@backingInt(c.getaddrinfo(host_z, null, &hints, &result)) != 0) return; // not numeric: a DNS name
    if (result) |r| c.freeaddrinfo(r);
    _ = std.Io.net.Ip4Address.parse(host_z, 0) catch {
        std.log.warn("S3 endpoint host '{s}' is a non-canonical IPv4 address; write it as a dotted quad", .{host_z});
        return error.NonCanonicalIpEndpoint;
    };
}

/// URI-encode an S3 key for use in path + SigV4 canonical URI. Both
/// the HTTP request line and the SigV4 signature canonical-URI must
/// use the SAME encoding, otherwise S3 returns 403 SignatureDoesNotMatch.
/// Per RFC 3986: keep A-Z, a-z, 0-9, '-', '.', '_', '~', '/'; %-encode
/// everything else. (Hive-style partitions like `year=2026` use '=',
/// which must be encoded as %3D.)
pub fn buildEncodedPath(arena: std.mem.Allocator, key: []const u8) ![]u8 {
    var out: std.ArrayList(u8) = .empty;
    errdefer out.deinit(arena);
    try out.ensureTotalCapacity(arena, key.len + 16);
    try out.append(arena, '/');
    for (key) |b| {
        const safe = (b >= 'A' and b <= 'Z') or
            (b >= 'a' and b <= 'z') or
            (b >= '0' and b <= '9') or
            b == '-' or b == '.' or b == '_' or b == '~' or b == '/';
        if (safe) {
            try out.append(arena, b);
        } else {
            const hex = "0123456789ABCDEF";
            try out.append(arena, '%');
            try out.append(arena, hex[(b >> 4) & 0xF]);
            try out.append(arena, hex[b & 0xF]);
        }
    }
    return out.toOwnedSlice(arena);
}

/// Every IPv4 address of `host`, in the resolver's order, without duplicates.
pub fn resolveIpv4(arena: std.mem.Allocator, host: []const u8) Error![]const tls.Ipv4 {
    const host_z = try arena.dupeSentinel(u8, host, 0);
    try checkNumericHost(host_z);

    var hints = std.mem.zeroes(c.addrinfo);
    hints.family = c.AF.INET;
    hints.socktype = c.SOCK.STREAM;
    var result: ?*c.addrinfo = null;
    const rc = c.getaddrinfo(host_z, null, &hints, &result);
    if (@backingInt(rc) != 0 or result == null) return error.DnsFailed;
    defer c.freeaddrinfo(result.?);

    var addrs: std.ArrayList(tls.Ipv4) = .empty;
    var it = result;
    while (it) |ai| : (it = ai.next) {
        const sa = ai.addr orelse continue;
        const sin: *align(1) const c.sockaddr.in = @ptrCast(sa);
        const ip: tls.Ipv4 = @bitCast(sin.addr);
        for (addrs.items) |seen| {
            if (std.mem.eql(u8, &seen, &ip)) break;
        } else try addrs.append(arena, ip);
    }
    if (addrs.items.len == 0) return error.DnsFailed;
    return addrs.items;
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

test "a dotted AWS bucket is addressed path-style, so its host stays inside AWS's wildcard certificate" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const aws: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-west-2" };

    try testing.expectEqualStrings("s3.us-west-2.amazonaws.com", try hostFor(a, aws, "logs.example"));
    try testing.expectEqualStrings("s3.us-west-2.amazonaws.com", try connectHostFor(a, aws, "logs.example"));
    try testing.expectEqualStrings("/logs.example/k%3D1.parquet", try pathFor(a, aws, "logs.example", "k=1.parquet"));

    try testing.expectEqualStrings("logs.s3.us-west-2.amazonaws.com", try hostFor(a, aws, "logs"));
    try testing.expectEqualStrings("/k.parquet", try pathFor(a, aws, "logs", "k.parquet"));

    const r2: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "auto",
        .endpoint = "https://acct.r2.example:8443",
    };
    try testing.expectEqualStrings("acct.r2.example:8443", try hostFor(a, r2, "logs"));
    try testing.expectEqualStrings("/logs/k.parquet", try pathFor(a, r2, "logs", "k.parquet"));
}

fn testEnviron(comptime entries: []const [*:0]const u8) std.process.Environ {
    const slice = comptime blk: {
        var a: [entries.len:null]?[*:0]const u8 = undefined;
        for (entries, 0..) |e, i| a[i] = e;
        const final = a;
        break :blk &final;
    };
    return .{ .block = .{ .slice = slice } };
}

test "S3_NO_SIGN_REQUEST yields anonymous credentials instead of NoCredentials" {
    const anon = try Credentials.fromEnv(testEnviron(&.{ "AWS_REGION=us-east-1", "S3_NO_SIGN_REQUEST=1" }));
    try testing.expectEqualStrings("", anon.access_key);
    try testing.expectEqualStrings("", anon.secret_key);
    try testing.expectEqual(@as(?[]const u8, null), anon.session_token);
    try testing.expectEqualStrings("us-east-1", anon.region);

    const r2 = try Credentials.fromEnv(testEnviron(&.{
        "S3_REGION=auto", "S3_ENDPOINT_URL=https://acct.r2.example", "S3_NO_SIGN_REQUEST=yes",
    }));
    try testing.expectEqualStrings("auto", r2.region);
    try testing.expectEqualStrings("https://acct.r2.example", r2.endpoint.?);

    try testing.expectError(error.NoCredentials, Credentials.fromEnv(testEnviron(&.{"AWS_REGION=us-east-1"})));
    try testing.expectError(error.NoCredentials, Credentials.fromEnv(testEnviron(&.{ "AWS_REGION=us-east-1", "S3_NO_SIGN_REQUEST=0" })));
    try testing.expectError(error.NoCredentials, Credentials.fromEnv(testEnviron(&.{ "AWS_REGION=us-east-1", "S3_NO_SIGN_REQUEST=" })));
    try testing.expectError(error.NoRegion, Credentials.fromEnv(testEnviron(&.{"S3_NO_SIGN_REQUEST=1"})));
}

fn endpointCreds(endpoint: []const u8) Credentials {
    return .{ .access_key = "ak", .secret_key = "sk", .region = "auto", .endpoint = endpoint };
}

test "an endpoint written as a non-canonical IPv4 address is refused, not quietly reinterpreted" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    // The resolver reads each of these as a number (`010.0.0.1` in octal, as 8.0.0.1), while the certificate check
    // would take it for a DNS name.
    for ([_][]const u8{
        "https://127.1:9000",
        "https://0x7f.0.0.1:9000",
        "https://2130706433",
        "https://010.0.0.1",
        "https://127.0.0.01:9000",
    }) |ep| {
        if (poolCriteria(a, endpointCreds(ep), "bkt")) |_| {
            std.debug.print("accepted endpoint {s}\n", .{ep});
            return error.TestUnexpectedResult;
        } else |err| try testing.expectEqual(error.NonCanonicalIpEndpoint, err);
    }
    const crit = try poolCriteria(a, endpointCreds("https://127.0.0.1:9"), "bkt");
    try testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &crit.addrs[0]);
    try testing.expectEqual(@as(u16, 9), crit.port);
}

test "an IPv6 endpoint is refused as unsupported" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    for ([_][]const u8{ "https://[::1]:9000", "https://[::1]", "http://[2001:db8::1]:9000/" }) |ep| {
        try testing.expectError(error.Ipv6EndpointUnsupported, poolCriteria(a, endpointCreds(ep), "bkt"));
    }
    const v6 = endpointCreds("https://[::1]:9");
    try testing.expectEqualStrings("::1", v6.endpointHost().?);
    try testing.expectEqual(@as(u16, 9), v6.endpointPort());
}

test "resolveIpv4 returns every address once" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const one = try resolveIpv4(arena_state.allocator(), "127.0.0.1");
    try testing.expectEqual(@as(usize, 1), one.len);
    try testing.expectEqualSlices(u8, &.{ 127, 0, 0, 1 }, &one[0]);
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

test "fetchManyRanges instantiates and short-circuits on an empty range list" {
    // Signature-break guard, not a behaviour test: `fetchManyRanges` has no in-repo caller, so Zig's lazy analysis
    // never type-checks its body unless something instantiates it — a `fetchJobs` arity change once slipped past the
    // whole build. Zero ranges opens no socket.
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var p: pool_mod.Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();

    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
    };

    var io_threaded: std.Io.Threaded = .init(testing.allocator, .{});
    defer io_threaded.deinit();

    const n = try fetchManyRanges(
        io_threaded.io(),
        &p,
        testing.allocator,
        arena,
        creds,
        "bucket",
        "key",
        &.{},
        &.{},
    );
    try testing.expectEqual(@as(u64, 0), n);
}

/// A stand-in for `Pool(N)` that scripts what each `acquire` yields, for driving the retry loops without a network.
/// A live connection is a socketpair whose peer has the response already queued; a dead one's peer has shut down
/// writing, so the request goes out and EOF comes back before any response byte.
const ScriptedPool = struct {
    pub const capacity: usize = 1;
    pub const Step = union(enum) {
        fail: anyerror,
        live: struct { reused: bool, response: []const u8 },
        dead: struct { reused: bool },
    };
    pub const Slot = struct { conn: tls.Connection, peer: std.posix.fd_t };
    pub const Handle = struct { conn: *tls.Connection, node: *Slot, permit: usize, reused: bool };

    steps: []const Step,
    next: usize = 0,
    fresh_requests: usize = 0,
    stale_retries: usize = 0,
    backoff_retries: usize = 0,
    slots: [8]Slot = undefined,

    fn deinit(self: *ScriptedPool) void {
        for (self.slots[0..self.next], self.steps[0..self.next]) |*slot, step| switch (step) {
            .fail => {},
            else => {
                slot.conn.deinit();
                _ = std.posix.system.close(slot.peer);
            },
        };
    }

    pub fn acquire(self: *ScriptedPool, io: Io, criteria: PoolCriteria, opts: pool_mod.AcquireOptions) !Handle {
        _ = io;
        _ = criteria;
        if (opts.fresh) self.fresh_requests += 1;
        const i = self.next;
        self.next += 1;
        const slot = &self.slots[i];
        const reused, const response: ?[]const u8 = switch (self.steps[i]) {
            .fail => |err| return err,
            .live => |l| .{ l.reused, l.response },
            .dead => |d| .{ d.reused, null },
        };
        var fds: [2]std.posix.fd_t = undefined;
        if (std.posix.errno(std.posix.system.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &fds)) != .SUCCESS)
            return error.SkipZigTest;
        slot.* = .{ .conn = .{ .fd = fds[0], .tls = undefined, .allocator = testing.allocator, .plain = true }, .peer = fds[1] };
        if (response) |r| {
            _ = std.posix.system.write(fds[1], r.ptr, r.len);
        } else {
            _ = std.posix.system.shutdown(fds[1], std.posix.SHUT.WR);
        }
        return .{ .conn = &slot.conn, .node = slot, .permit = 0, .reused = reused };
    }
    pub fn release(_: *ScriptedPool, _: Io, _: Handle) void {}
    pub fn discard(_: *ScriptedPool, _: Io, _: Handle) void {}
    pub fn noteRetry(self: *ScriptedPool, _: Io, failure: retry.Failure) void {
        switch (failure) {
            .stale => self.stale_retries += 1,
            .transient => self.backoff_retries += 1,
            .fatal => {},
        }
    }
};

const test_creds: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-east-1", .endpoint = "http://127.0.0.1:9" };
const ok_206 = "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\n\r\nok";

test "getViaPool retries a connection that fails its handshake, including the fresh one after a stale failure" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const url: Url = .{ .bucket = "b", .key = "k" };

    // Thawed sandbox: the pooled connection is dead, and the fresh one forced after it fails its TLS handshake.
    var p: ScriptedPool = .{ .steps = &.{
        .{ .dead = .{ .reused = true } },
        .{ .fail = error.HandshakeFailed },
        .{ .live = .{ .reused = false, .response = ok_206 } },
    } };
    defer p.deinit();
    const resp = try getViaPool(testing.io, &p, arena_state.allocator(), test_creds, url, Range.span(0, 1));
    try testing.expectEqual(@as(u16, 206), resp.status);
    try testing.expectEqualStrings("ok", resp.body);
    try testing.expectEqual(@as(usize, 1), p.stale_retries);
    try testing.expectEqual(@as(usize, 1), p.backoff_retries);
    try testing.expectEqual(@as(usize, 1), p.fresh_requests);

    // A ClientHello that cannot be written surfaces from acquire as SendFailed; it is still only a connect failure.
    var q: ScriptedPool = .{ .steps = &.{
        .{ .fail = error.SendFailed },
        .{ .fail = error.ConnectFailed },
        .{ .live = .{ .reused = false, .response = ok_206 } },
    } };
    defer q.deinit();
    const put_resp = try putViaPool(testing.io, &q, arena_state.allocator(), test_creds, url, "ok");
    try testing.expectEqual(@as(u16, 206), put_resp.status);
    try testing.expectEqual(@as(usize, 2), q.backoff_retries);
}

test "requestViaPool retries a dead reused connection at once only for an idempotent request" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const criteria: PoolCriteria = .{ .host = "h", .addrs = &.{.{ 127, 0, 0, 1 }}, .port = 9, .use_tls = false };
    const Post = struct {
        fn send(_: void, a: std.mem.Allocator, conn: *tls.Connection) !http.Response {
            return http.sendRequest(a, conn, .{ .method = .POST, .host = "h", .path = "/k?uploads=", .headers = &.{} });
        }
    };
    const steps: []const ScriptedPool.Step = &.{
        .{ .dead = .{ .reused = true } },
        .{ .live = .{ .reused = false, .response = "HTTP/1.1 200 OK\r\nContent-Length: 0\r\n\r\n" } },
    };

    // POST-style: the dead connection fails the request; nothing is sent again.
    var post_pool: ScriptedPool = .{ .steps = steps };
    defer post_pool.deinit();
    try testing.expectError(error.ClosedBeforeResponse, requestViaPool(testing.io, &post_pool, arena_state.allocator(), criteria, .{ .idempotent = false }, {}, Post.send));
    try testing.expectEqual(@as(usize, 1), post_pool.next);
    try testing.expectEqual(@as(usize, 0), post_pool.stale_retries);

    // The same exchange declared idempotent goes again at once on a fresh connection.
    var put_pool: ScriptedPool = .{ .steps = steps };
    defer put_pool.deinit();
    const resp = try requestViaPool(testing.io, &put_pool, arena_state.allocator(), criteria, .{ .idempotent = true }, {}, Post.send);
    try testing.expectEqual(@as(u16, 200), resp.status);
    try testing.expectEqual(@as(usize, 1), put_pool.stale_retries);
    try testing.expectEqual(@as(usize, 1), put_pool.fresh_requests);
}
