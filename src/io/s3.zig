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
const Io = std.Io;
const tls = @import("tls.zig");
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");
const pool_mod = @import("pool.zig");

pub const Pool = pool_mod.Pool;

/// Maximum concurrent in-flight requests against S3 (multipart parts
/// or split sub-range fetches). Sized for the bakeoff sweet spot —
/// see journal 2026-05-04.
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
/// Target sub-range size when splitting. ~19 MB matches the per-part
/// size we use for multipart writes.
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
    const host = try std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ url.bucket, creds.region },
    );
    const addr_v4 = try resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();
    return try sendPut(arena, &conn, creds, host, url.key, body);
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
    var attempt: u8 = 0;
    while (attempt < MAX_PARTS + 1) : (attempt += 1) {
        return getViaPoolOnce(io, p, arena, creds, url, range) catch |err| switch (err) {
            error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => continue,
            else => return err,
        };
    }
    return error.RecvFailed;
}

fn getViaPoolOnce(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    range: ?Range,
) !http.Response {
    const handle = try p.acquire(io);
    var released = false;
    errdefer if (!released) p.discard(io, handle);

    const host = try std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ url.bucket, creds.region },
    );
    const resp = try buildAndSend(arena, handle.conn, creds, host, url.key, range);

    try p.release(io, handle);
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
    var attempt: u8 = 0;
    while (attempt < MAX_PARTS + 1) : (attempt += 1) {
        return putViaPoolOnce(io, p, arena, creds, url, body) catch |err| switch (err) {
            error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => continue,
            else => return err,
        };
    }
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
    const handle = try p.acquire(io);
    var released = false;
    errdefer if (!released) p.discard(io, handle);

    const host = try std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ url.bucket, creds.region },
    );
    const resp = try sendPut(arena, handle.conn, creds, host, url.key, body);

    try p.release(io, handle);
    released = true;
    return resp;
}

fn sendPut(
    arena: std.mem.Allocator,
    conn: *tls.Connection,
    creds: Credentials,
    host: []const u8,
    key: []const u8,
    body: []const u8,
) Error!http.Response {
    const path = try buildEncodedPath(arena, key);

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
/// input range larger than SPLIT_THRESHOLD into sub-ranges; dispatches
/// all sub-jobs via Io.Group.concurrent, bounded by the pool's
/// connection budget. Returns total bytes fetched.
///
/// Generalizes the old single-file fetchManyRanges shape to work
/// across N input files in one call — the pool's 8 in-flight slots
/// dispatch ranges from different (bucket, key) interchangeably.
/// Underpins multi-file scan (Phase 6.1).
pub fn fetchJobs(
    io: Io,
    p: anytype, // *Pool(N) for some comptime N
    gpa: std.mem.Allocator,
    arena: std.mem.Allocator,
    creds: Credentials,
    jobs: []const FetchJob,
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

    // 2. Per-sub-job context.
    var ctxs = try arena.alloc(FetchCtx, split.items.len);
    for (split.items, 0..) |j, i| {
        ctxs[i] = .{
            .pool_ptr = @ptrCast(p),
            .pool_acquire_fn = poolAcquireFn(@TypeOf(p.*)),
            .pool_release_fn = poolReleaseFn(@TypeOf(p.*)),
            .pool_discard_fn = poolDiscardFn(@TypeOf(p.*)),
            .gpa = gpa,
            .creds = creds,
            .bucket = j.bucket,
            .key = j.key,
            .range = j.range,
            .target = j.target,
            .ok = false,
        };
    }

    // 3. Dispatch via Io.Group.concurrent — pool gates parallelism.
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (ctxs) |*ctx_ptr| try group.concurrent(io, fetchOneTask, .{ io, ctx_ptr });
    try group.await(io);

    // 4. Tally + check for failures.
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
    return fetchJobs(io, p, gpa, arena, creds, jobs.items);
}

/// Type-erased view of `Pool(N).Handle` so the dispatch glue between
/// `fetchManyRanges` / `uploadMultipart` and any `Pool(N)` instance
/// can speak a common shape regardless of the comptime size.
pub const PoolHandle = struct {
    conn: *tls.Connection,
    idx: usize,
};

const FetchCtx = struct {
    /// Type-erased pool handle so this works for any Pool(N).
    pool_ptr: *anyopaque,
    pool_acquire_fn: *const fn (*anyopaque, Io) anyerror!PoolHandle,
    pool_release_fn: *const fn (*anyopaque, Io, usize) anyerror!void,
    pool_discard_fn: *const fn (*anyopaque, Io, usize) void,

    gpa: std.mem.Allocator,
    creds: Credentials,
    bucket: []const u8,
    key: []const u8,
    range: Range,
    /// Pre-sized target buffer; `target.len == range.end - range.start`.
    target: []u8,
    ok: bool,
};

fn poolAcquireFn(comptime P: type) *const fn (*anyopaque, Io) anyerror!PoolHandle {
    return struct {
        fn f(p: *anyopaque, io: Io) anyerror!PoolHandle {
            const typed: *P = @ptrCast(@alignCast(p));
            const h = try typed.acquire(io);
            return .{ .conn = h.conn, .idx = h.idx };
        }
    }.f;
}

fn poolReleaseFn(comptime P: type) *const fn (*anyopaque, Io, usize) anyerror!void {
    return struct {
        fn f(p: *anyopaque, io: Io, idx: usize) anyerror!void {
            const typed: *P = @ptrCast(@alignCast(p));
            try typed.release(io, .{ .conn = undefined, .idx = idx });
        }
    }.f;
}

fn poolDiscardFn(comptime P: type) *const fn (*anyopaque, Io, usize) void {
    return struct {
        fn f(p: *anyopaque, io: Io, idx: usize) void {
            const typed: *P = @ptrCast(@alignCast(p));
            typed.discard(io, .{ .conn = undefined, .idx = idx });
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
        var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const handle = ctx.pool_acquire_fn(ctx.pool_ptr, io) catch return;
        const ok = doFetch(arena, ctx, handle.conn) catch |err| {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle.idx);
            switch (err) {
                error.RecvFailed,
                error.SendFailed,
                error.BodyTruncated,
                error.BadStatusLine,
                => continue,
                else => return,
            }
        };
        if (!ok) {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle.idx);
            return;
        }
        ctx.ok = true;
        ctx.pool_release_fn(ctx.pool_ptr, io, handle.idx) catch return;
        return;
    }
}

/// Issue one ranged GET on the given connection and copy the bytes
/// into ctx.into. Returns true on success, false on a non-error
/// failure (e.g. wrong status, length mismatch). Errors propagate so
/// the caller can decide whether to retry.
fn doFetch(arena: std.mem.Allocator, ctx: *FetchCtx, conn: *tls.Connection) !bool {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ ctx.bucket, ctx.creds.region });
    const path = try buildEncodedPath(arena, ctx.key);

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

    const signed = try signer.sign(arena, "GET", host, path, null, hdr_in.items, "", .{ .use_unsigned_payload = true });

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, conn, .{
        .method = .GET,
        .host = host,
        .path = path,
        .headers = headers.items,
    });

    if (resp.status != 206 and resp.status != 200) return false;

    if (resp.body.len != ctx.target.len) return false;
    @memcpy(ctx.target, resp.body);
    return true;
}

/// Parallel multipart upload of `body` to `url`. Uses `Io.Group.concurrent`
/// to dispatch each part on its own thread; each task owns its own TLS
/// connection for its lifetime (no pooling — see writer_design.md).
///
/// Number of parts is auto-computed: aim for ~MULTIPART_THRESHOLD-sized
/// parts up to MAX_PARTS, with a 5 MB floor (S3's hard minimum). For
/// bodies smaller than MULTIPART_THRESHOLD the caller should use `put`
/// instead — single-PUT wins at that size.
pub fn uploadMultipart(
    io: Io,
    p: anytype, // *Pool(N) shared with the read phase when buckets match
    arena: std.mem.Allocator,
    gpa: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    body: []const u8,
) !void {
    if (body.len < MIN_PART_SIZE) return error.BodyTooSmallForMultipart;

    const num_parts = @min(MAX_PARTS, @max(@as(usize, 2), body.len / MULTIPART_THRESHOLD));
    const part_size = (body.len + num_parts - 1) / num_parts;
    std.debug.assert(part_size >= MIN_PART_SIZE or num_parts == 1);

    const upload_id = try createMultipart(arena, creds, url);

    // Pre-allocate per-part state. Stack arrays sized to MAX_PARTS;
    // we only use the first `num_parts` slots.
    var etag_storage: [MAX_PARTS][512]u8 = undefined;
    var etag_lens: [MAX_PARTS]usize = .{0} ** MAX_PARTS;
    var ctxs: [MAX_PARTS]PartCtx = undefined;

    for (0..num_parts) |i| {
        const start = i * part_size;
        const end = @min(start + part_size, body.len);
        ctxs[i] = .{
            .pool_ptr = @ptrCast(p),
            .pool_acquire_fn = poolAcquireFn(@TypeOf(p.*)),
            .pool_release_fn = poolReleaseFn(@TypeOf(p.*)),
            .pool_discard_fn = poolDiscardFn(@TypeOf(p.*)),
            .gpa = gpa,
            .creds = creds,
            .url = url,
            .upload_id = upload_id,
            .part_number = @intCast(i + 1),
            .body = body[start..end],
            .etag_buf = &etag_storage[i],
            .etag_len = &etag_lens[i],
        };
    }

    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..num_parts) |i| {
        try group.concurrent(io, uploadPartTask, .{ io, &ctxs[i] });
    }
    try group.await(io);

    // Detect any failures by checking ETags landed.
    for (0..num_parts) |i| {
        if (etag_lens[i] == 0) return error.PartUploadFailed;
    }

    try completeMultipart(arena, creds, url, upload_id, &etag_storage, &etag_lens, num_parts);
}

const PartCtx = struct {
    pool_ptr: *anyopaque,
    pool_acquire_fn: *const fn (*anyopaque, Io) anyerror!PoolHandle,
    pool_release_fn: *const fn (*anyopaque, Io, usize) anyerror!void,
    pool_discard_fn: *const fn (*anyopaque, Io, usize) void,

    gpa: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    upload_id: []const u8,
    part_number: u32,
    body: []const u8,
    etag_buf: *[512]u8,
    etag_len: *usize,
};

fn uploadPartTask(io: Io, ctx: *PartCtx) Io.Cancelable!void {
    // Retry up to POOL_SIZE+1 times: after idle, every slot may be
    // stale; each failed acquire discards-and-recycles its slot to
    // the queue tail, so worst case we burn through all 8 stale slots
    // before getting a freshly-init'd one.
    var attempts: u8 = 0;
    while (attempts < MAX_PARTS + 1) : (attempts += 1) {
        var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
        defer arena_state.deinit();
        const arena = arena_state.allocator();

        const handle = ctx.pool_acquire_fn(ctx.pool_ptr, io) catch return;
        const ok = doUploadPart(arena, ctx, handle.conn) catch |err| {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle.idx);
            switch (err) {
                error.RecvFailed,
                error.SendFailed,
                error.BodyTruncated,
                error.BadStatusLine,
                => continue,
                else => return,
            }
        };
        if (!ok) {
            ctx.pool_discard_fn(ctx.pool_ptr, io, handle.idx);
            return;
        }
        ctx.pool_release_fn(ctx.pool_ptr, io, handle.idx) catch return;
        return;
    }
}

fn doUploadPart(arena: std.mem.Allocator, ctx: *PartCtx, conn: *tls.Connection) !bool {
    const host = try std.fmt.allocPrint(
        arena,
        "{s}.s3.{s}.amazonaws.com",
        .{ ctx.url.bucket, ctx.creds.region },
    );
    const path = try buildEncodedPath(arena, ctx.url.key);
    const query = try std.fmt.allocPrint(
        arena,
        "partNumber={d}&uploadId={s}",
        .{ ctx.part_number, ctx.upload_id },
    );
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const signer: sigv4.SigV4 = .{
        .region = ctx.creds.region,
        .access_key = ctx.creds.access_key,
        .secret_key = ctx.creds.secret_key,
        .session_token = ctx.creds.session_token,
    };
    const signed = try signer.sign(
        arena,
        "PUT",
        host,
        path,
        query,
        &.{},
        ctx.body,
        .{ .use_unsigned_payload = true },
    );

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, conn, .{
        .method = .PUT,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = ctx.body,
    });
    if (resp.status != 200) return false;

    const etag = resp.header("ETag") orelse return false;
    if (etag.len > ctx.etag_buf.len) return false;
    @memcpy(ctx.etag_buf[0..etag.len], etag);
    ctx.etag_len.* = etag.len;
    return true;
}

fn createMultipart(
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
) ![]const u8 {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try buildEncodedPath(arena, url.key);
    const query = "uploads=";
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    const addr_v4 = try resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, "", .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = "",
    });
    if (resp.status != 200) return error.CreateMultipartFailed;

    return try extractXml(arena, resp.body, "UploadId");
}

fn completeMultipart(
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    upload_id: []const u8,
    etag_storage: *const [MAX_PARTS][512]u8,
    etag_lens: *const [MAX_PARTS]usize,
    num_parts: usize,
) !void {
    const host = try std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ url.bucket, creds.region });
    const path = try buildEncodedPath(arena, url.key);
    const query = try std.fmt.allocPrint(arena, "uploadId={s}", .{upload_id});
    const path_with_query = try std.fmt.allocPrint(arena, "{s}?{s}", .{ path, query });

    var body: std.ArrayList(u8) = .empty;
    try body.appendSlice(arena, "<CompleteMultipartUpload>");
    for (0..num_parts) |i| {
        const etag = etag_storage[i][0..etag_lens[i]];
        const piece = try std.fmt.allocPrint(
            arena,
            "<Part><PartNumber>{d}</PartNumber><ETag>{s}</ETag></Part>",
            .{ i + 1, etag },
        );
        try body.appendSlice(arena, piece);
    }
    try body.appendSlice(arena, "</CompleteMultipartUpload>");

    const addr_v4 = try resolveIpv4(arena, host);
    var conn = try tls.Connection.connect(arena, addr_v4, 443, host);
    defer conn.deinit();

    const signer: sigv4.SigV4 = .{
        .region = creds.region,
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    const signed = try signer.sign(arena, "POST", host, path, query, &.{}, body.items, .{});

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    const resp = try http.sendRequest(arena, &conn, .{
        .method = .POST,
        .host = host,
        .path = path_with_query,
        .headers = headers.items,
        .body = body.items,
    });
    if (resp.status != 200) return error.CompleteMultipartFailed;
    if (std.mem.indexOf(u8, resp.body, "<Error>") != null) return error.CompleteMultipartFailed;
}

fn extractXml(arena: std.mem.Allocator, xml: []const u8, tag: []const u8) ![]const u8 {
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
    key: []const u8,
    range: ?Range,
) Error!http.Response {
    const path = try buildEncodedPath(req_arena, key);

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

/// URI-encode an S3 key for use in path + SigV4 canonical URI. Both
/// the HTTP request line and the SigV4 signature canonical-URI must
/// use the SAME encoding, otherwise S3 returns 403 SignatureDoesNotMatch.
/// Per RFC 3986: keep A-Z, a-z, 0-9, '-', '.', '_', '~', '/'; %-encode
/// everything else. (Hive-style partitions like `year=2026` use '=',
/// which must be encoded as %3D.)
fn buildEncodedPath(arena: std.mem.Allocator, key: []const u8) ![]u8 {
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
