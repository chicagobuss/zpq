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
//!
//! DNS: getaddrinfo via libc. Linking libc adds ~150 KB to the
//! ReleaseSmall musl binary; rolling our own resolver is a future
//! follow-up.

const std = @import("std");
const Io = std.Io;
const tls = @import("tls.zig");
const AtomicWorkCursor = @import("work_cursor.zig").AtomicWorkCursor;
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");
const pool_mod = @import("pool.zig");
const multipart_sink = @import("multipart_sink.zig");
const retry = @import("retry.zig");

pub const Pool = pool_mod.Pool;
pub const PoolCriteria = pool_mod.Criteria;

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
    endpoint: ?[]const u8 = null,

    pub fn fromEnv(env: std.process.Environ) Error!Credentials {
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
        const authority = self.endpointAuthority() orelse return 443;
        if (std.mem.lastIndexOfScalar(u8, authority, ':')) |colon| {
            return std.fmt.parseInt(u16, authority[colon + 1 ..], 10) catch defaultPort(self.endpointUseTls());
        }
        return defaultPort(self.endpointUseTls());
    }
};

fn defaultPort(use_tls: bool) u16 {
    return if (use_tls) 443 else 80;
}

/// Construct the request hostname for `bucket` under these creds.
/// AWS S3: `<bucket>.s3.<region>.amazonaws.com` (virtual-hosted).
/// Custom endpoint: the endpoint host (path-style; bucket is in the path).
pub fn hostFor(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) ![]u8 {
    if (creds.endpointAuthority()) |h| {
        return arena.dupe(u8, h);
    }
    return std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
}

pub fn connectHostFor(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) ![]u8 {
    if (creds.endpointHost()) |h| {
        return arena.dupe(u8, h);
    }
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
    const addr = try resolveIpv4(arena, connect_host);
    return .{ .host = host, .addr_v4 = addr, .port = portFor(creds), .use_tls = useTls(creds) };
}

pub fn connect(arena: std.mem.Allocator, creds: Credentials, addr_v4: []const u8, host: []const u8) Error!tls.Connection {
    const port = portFor(creds);
    if (useTls(creds)) return try tls.Connection.connect(arena, addr_v4, port, host);
    return try tls.Connection.connectPlain(arena, addr_v4, port);
}

/// Construct the HTTP request path. AWS S3 (virtual-hosted): `/<key>`.
/// Custom endpoint (path-style): `/<bucket>/<key>`. Both URI-encode the
/// key per RFC 3986 — both the path component and the SigV4 canonical
/// URI MUST use this same encoding.
pub fn pathFor(
    arena: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
) ![]u8 {
    const encoded_key = try buildEncodedPath(arena, key);
    if (creds.endpoint == null) return encoded_key;
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
    addr_v4: []const u8, // owned, in client_arena
    conn: ?tls.Connection,

    pub fn init(
        client_arena: std.mem.Allocator,
        creds: Credentials,
        bucket: []const u8,
    ) Error!Client {
        const host = try hostFor(client_arena, creds, bucket);
        const connect_host = try connectHostFor(client_arena, creds, bucket);
        const addr_v4 = try resolveIpv4(client_arena, connect_host);
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
            self.conn = try connect(self.client_arena, self.creds, self.addr_v4, self.host);
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
            self.conn = try connect(self.client_arena, self.creds, self.addr_v4, self.host);
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
    const addr_v4 = try resolveIpv4(arena, connect_host);
    var conn = try connect(arena, creds, addr_v4, host);
    defer conn.deinit();
    return try sendPut(arena, &conn, creds, host, url.bucket, url.key, body);
}

/// Same as `get`, but acquires a connection from the supplied pool
/// instead of opening a fresh one. Caller is responsible for the
/// pool's host matching `url.bucket`. Retries once on stale-connection
/// errors (S3 closes idle connections after ~30s; the pool can hand
/// out a closed slot when warm-container reuse spans that gap).
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
    var throttled: ?http.Response = null;
    var attempt: u8 = 0;
    while (attempt < MAX_PARTS + 1) : (attempt += 1) {
        if (attempt > 0) try retry.sleepBackoff(io, retry.default_policy, attempt - 1);
        const resp = getViaPoolOnce(io, p, arena, creds, url, opts) catch |err| switch (err) {
            error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => continue,
            else => return err,
        };
        // Throttle/transient statuses (429/5xx) retry with backoff; any
        // other status — including 304/403/404 — is a real answer the
        // caller must interpret.
        if (retry.retryableStatus(resp.status)) {
            throttled = resp;
            continue;
        }
        return resp;
    }
    // Budget exhausted on throttle: surface the last response so the
    // caller sees the actual status instead of a connection error.
    if (throttled) |r| return r;
    return error.RecvFailed;
}

fn getViaPoolOnce(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    opts: GetOpts,
) !http.Response {
    const host = try hostFor(arena, creds, url.bucket);
    const criteria = try poolCriteria(arena, creds, url.bucket);
    const handle = try p.acquire(io, criteria);
    var released = false;
    errdefer if (!released) p.discard(io, handle);

    const resp = try buildAndSend(arena, handle.conn, creds, host, url.bucket, url.key, opts);

    p.release(io, handle);
    released = true;
    return resp;
}

/// Same as `put`, but acquires a connection from the supplied pool
/// instead of opening a fresh one. Retries once on stale-connection
/// errors (see getViaPool).
pub fn putViaPool(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) !http.Response {
    var throttled: ?http.Response = null;
    var attempt: u8 = 0;
    while (attempt < MAX_PARTS + 1) : (attempt += 1) {
        if (attempt > 0) try retry.sleepBackoff(io, retry.default_policy, attempt - 1);
        const resp = putViaPoolOnce(io, p, arena, creds, url, body) catch |err| switch (err) {
            error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => continue,
            else => return err,
        };
        if (retry.retryableStatus(resp.status)) {
            throttled = resp;
            continue;
        }
        return resp;
    }
    if (throttled) |r| return r;
    return error.SendFailed;
}

fn putViaPoolOnce(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) !http.Response {
    const host = try hostFor(arena, creds, url.bucket);
    const criteria = try poolCriteria(arena, creds, url.bucket);
    const handle = try p.acquire(io, criteria);
    var released = false;
    errdefer if (!released) p.discard(io, handle);

    const resp = try sendPut(arena, handle.conn, creds, host, url.bucket, url.key, body);

    p.release(io, handle);
    released = true;
    return resp;
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
/// `workers` is clamped to the pool's capacity and to the job count, but must not exceed the concurrency limit of the
/// `Io` passed in: `Io.Group.concurrent` *rejects* submissions past its limit rather than queueing them, and dispatch
/// runs under `try` inside a `defer group.cancel`, so an over-large count cancels the batch.
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
    var hosts: std.StringHashMapUnmanaged(struct { host: []const u8, addr: []const u8, port: u16, use_tls: bool }) = .empty;
    defer hosts.deinit(arena);
    for (split.items) |j| {
        if (hosts.get(j.bucket) != null) continue;
        const h = try hostFor(arena, creds, j.bucket);
        const connect_host = try connectHostFor(arena, creds, j.bucket);
        const a = try resolveIpv4(arena, connect_host);
        try hosts.put(arena, j.bucket, .{
            .host = h,
            .addr = a,
            .port = portFor(creds),
            .use_tls = useTls(creds),
        });
    }

    // 3. Per-sub-job context.
    var ctxs = try arena.alloc(FetchCtx, split.items.len);
    for (split.items, 0..) |j, i| {
        const hi = hosts.get(j.bucket).?;
        ctxs[i] = .{
            .pool_ptr = @ptrCast(p),
            .pool_acquire_fn = poolAcquireFn(@TypeOf(p.*)),
            .pool_release_fn = poolReleaseFn(@TypeOf(p.*)),
            .pool_discard_fn = poolDiscardFn(@TypeOf(p.*)),
            .gpa = gpa,
            .creds = creds,
            .bucket = j.bucket,
            .key = j.key,
            .host = hi.host,
            .addr_v4 = hi.addr,
            .port = hi.port,
            .use_tls = hi.use_tls,
            .range = j.range,
            .target = j.target,
            .ok = false,
        };
    }

    // 4. Run the sub-jobs through a fixed set of worker loops. Pool permits
    //    gate *sockets*, not threads: `Io.Group.concurrent` spawns an OS
    //    thread whenever all workers are busy, and a worker parked on a
    //    blocking socket read is busy, so submitting every sub-job at once
    //    created far more threads than permits. Completion order stays
    //    arbitrary either way — nothing may depend on it.
    var shared: AtomicWorkCursor(FetchCtx) = .{ .items = ctxs };
    const Worker = struct {
        fn run(loop_io: Io, sh: *AtomicWorkCursor(FetchCtx)) Io.Cancelable!void {
            while (sh.next()) |ctx_ptr| try fetchOneTask(loop_io, ctx_ptr);
        }
    };
    var group: Io.Group = .init;
    defer group.cancel(io);
    // Zero workers with work pending would leave every `ctx.ok` false and surface as `RangeFetchFailed` rather than as
    // the bad argument it is.
    const n_workers = if (ctxs.len == 0)
        0
    else
        @max(1, @min(@min(workers, @TypeOf(p.*).capacity), ctxs.len));
    for (0..n_workers) |_| {
        try group.concurrent(io, Worker.run, .{ io, &shared });
    }
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
    // Preserves this wrapper's arity: pool capacity is what its callers implicitly got before `fetchJobs` took an
    // explicit worker count.
    return fetchJobs(io, p, gpa, arena, creds, jobs.items, @TypeOf(p.*).capacity);
}

/// Type-erased view of `Pool(N).Handle` so the dispatch glue between
/// `fetchManyRanges` / `uploadMultipart` and any `Pool(N)` instance
/// can speak a common shape regardless of the comptime size.
pub const PoolHandle = struct {
    conn: *tls.Connection,
    node: *anyopaque,
    permit: usize,
};

const FetchCtx = struct {
    /// Type-erased pool handle so this works for any Pool(N).
    pool_ptr: *anyopaque,
    pool_acquire_fn: *const fn (*anyopaque, Io, PoolCriteria) anyerror!PoolHandle,
    pool_release_fn: *const fn (*anyopaque, Io, PoolHandle) void,
    pool_discard_fn: *const fn (*anyopaque, Io, PoolHandle) void,

    gpa: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
    host: []const u8,
    addr_v4: []const u8,
    port: u16,
    use_tls: bool,
    range: Range,
    /// Pre-sized target buffer; `target.len == range.end - range.start`.
    target: []u8,
    ok: bool,
};

fn poolAcquireFn(comptime P: type) *const fn (*anyopaque, Io, PoolCriteria) anyerror!PoolHandle {
    return struct {
        fn f(p: *anyopaque, io: Io, criteria: PoolCriteria) anyerror!PoolHandle {
            const typed: *P = @ptrCast(@alignCast(p));
            const h = try typed.acquire(io, criteria);
            return .{ .conn = h.conn, .node = @ptrCast(h.node), .permit = h.permit };
        }
    }.f;
}

fn poolReleaseFn(comptime P: type) *const fn (*anyopaque, Io, PoolHandle) void {
    return struct {
        fn f(p: *anyopaque, io: Io, h: PoolHandle) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.release(io, .{
                .conn = h.conn,
                .node = @ptrCast(@alignCast(h.node)),
                .permit = h.permit,
            });
        }
    }.f;
}

fn poolDiscardFn(comptime P: type) *const fn (*anyopaque, Io, PoolHandle) void {
    return struct {
        fn f(p: *anyopaque, io: Io, h: PoolHandle) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.discard(io, .{
                .conn = h.conn,
                .node = @ptrCast(@alignCast(h.node)),
                .permit = h.permit,
            });
        }
    }.f;
}

fn fetchOneTask(io: Io, ctx: *FetchCtx) Io.Cancelable!void {
    // Retry up to POOL_SIZE+1 times on stale-connection errors. After
    // idle, S3 may have closed every pooled connection; each failed
    // acquire discards-and-recycles its slot to the queue tail, so
    // worst case we burn through all 8 stale slots before getting a
    // freshly-init'd one.
    var attempts: u8 = 0;
    while (attempts < MAX_PARTS + 1) : (attempts += 1) {
        if (attempts > 0) try retry.sleepBackoff(io, retry.default_policy, attempts - 1);
        var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const criteria: PoolCriteria = .{
            .host = ctx.host,
            .addr_v4 = ctx.addr_v4,
            .port = ctx.port,
            .use_tls = ctx.use_tls,
        };
        const handle = ctx.pool_acquire_fn(ctx.pool_ptr, io, criteria) catch return;
        const ok = doFetch(arena, ctx, handle.conn) catch |err| {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle);
            switch (err) {
                error.RecvFailed,
                error.SendFailed,
                error.BodyTruncated,
                error.BadStatusLine,
                error.RetryableStatus,
                => continue,
                else => return,
            }
        };
        if (!ok) {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle);
            return;
        }
        ctx.ok = true;
        ctx.pool_release_fn(ctx.pool_ptr, io, handle);
        return;
    }
}

/// Issue one ranged GET on the given connection and copy the bytes
/// into ctx.into. Returns true on success, false on a non-error
/// failure (e.g. wrong status, length mismatch). Errors propagate so
/// the caller can decide whether to retry.
fn doFetch(arena: std.mem.Allocator, ctx: *FetchCtx, conn: *tls.Connection) !bool {
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

    const resp = try http.sendRequestInto(arena, conn, .{
        .method = .GET,
        .host = ctx.host,
        .path = path,
        .headers = headers.items,
    }, ctx.target);

    // Throttle/transient statuses retry with backoff via fetchOneTask's
    // loop; any other unexpected status (or a short body on a good
    // status) is a hard failure for this fetch.
    if (retry.retryableStatus(resp.status)) return error.RetryableStatus;
    if (resp.status != 206 and resp.status != 200) return false;

    if (resp.body.len != ctx.target.len) return false;
    return true;
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
    // Path: virtual-hosted (AWS) is "/"; path-style (R2 / endpoint
    // override) is "/<bucket>". Same logic as `pathFor` for an empty key.
    const list_path: []const u8 = if (creds.endpoint == null)
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

pub fn resolveIpv4(arena: std.mem.Allocator, host: []const u8) Error![]const u8 {
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
