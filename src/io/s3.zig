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
const winEnvValue = @import("../core/system.zig").winEnvValue;
// The native environment map and POSIX environment provide the same lookup at this seam.
const WindowsEnvironment = struct {
    environ: std.process.Environ,
    inline fn getPosix(self: @This(), name: []const u8) ?[]const u8 {
        return winEnvValue(self.environ, name);
    }
};
const builtin = @import("builtin");
const Io = std.Io;
const tls = @import("tls.zig");
const work_cursor = @import("work_cursor.zig");
const AtomicWorkCursor = work_cursor.AtomicWorkCursor;
const http = @import("http.zig");
const sigv4 = @import("sigv4.zig");
const pool_mod = @import("pool.zig");
const retry = @import("retry.zig");
const clock = @import("../clock.zig");
const profile = @import("../profile.zig");
const diag_mod = @import("../core/diag.zig");

pub const Pool = pool_mod.Pool;
pub const PoolCriteria = pool_mod.Criteria;
pub const AnyPool = pool_mod.AnyPool;

/// Maximum concurrent in-flight requests against S3 (multipart parts
/// or split sub-range fetches). Larger pools increase memory and TLS
/// pressure before they improve throughput for the target workloads.
pub const MAX_PARTS: usize = 8;
/// Above this size, fetchManyRanges splits a single range into
/// sub-ranges so we can fan out across the pool. Below it the
/// per-request TLS handshake amortizes badly across tiny fetches.
pub const SPLIT_THRESHOLD: usize = 24 * 1024 * 1024;

/// Most data ranges in flight on Lambda, where more made its network policer slower: 16 on x86_64, 8 on aarch64.
/// Every connection's TLS stream costs CPU, and on a 3008 MB (2-vCPU) Graviton sandbox 16 at once took several times
/// the user CPU of 8 for the same bytes and finished cold scans later; x86_64 finished them sooner at 16. Only that
/// memory size was measured, so the cap does not follow the vCPU count.
pub const S3_IN_FLIGHT: usize = if (builtin.target.cpu.arch == .aarch64) 8 else 16;
/// Most data ranges in flight anywhere else, and the size of the connection pool. Over a link with a long round trip
/// (a laptop or a server far from the bucket), each request's first byte is the cost, so throughput follows how many
/// are in flight: 16 left most of a home link idle.
pub const S3_IN_FLIGHT_MAX: usize = 64;

/// Data ranges this process keeps in flight: `ZPQ_S3_IN_FLIGHT` when it is a positive integer (at most
/// `S3_IN_FLIGHT_MAX`), else `S3_IN_FLIGHT` on Lambda and `S3_IN_FLIGHT_MAX` elsewhere.
pub fn inFlight(env: ?std.process.Environ) usize {
    const e = env orelse return S3_IN_FLIGHT;
    if (winEnvValue(e, "ZPQ_S3_IN_FLIGHT")) |v| if (std.fmt.parseInt(usize, v, 10)) |n| {
        if (n > 0) return @min(n, S3_IN_FLIGHT_MAX);
    } else |_| {};
    return if (winEnvValue(e, "AWS_LAMBDA_FUNCTION_NAME") != null) S3_IN_FLIGHT else S3_IN_FLIGHT_MAX;
}
/// Data-range requests in flight until this query's first data-range response header, unless `ZPQ_S3_FIRST_WAVE`
/// says otherwise. Footer, metadata and Bloom requests are not part of that wave and do not raise the cap. After
/// the header, the cap is `inFlight`. Setting the first cap to that is no slow start.
pub const S3_FIRST_WAVE: usize = 8;

/// The first-wave cap for this process: `ZPQ_S3_FIRST_WAVE` when it is a positive integer, otherwise
/// `S3_FIRST_WAVE`. A value above `S3_IN_FLIGHT` is clamped to it, so 16 starts every data request at the full cap.
/// On aarch64 the first wave is already the full cap.
pub fn firstWaveCap(env: ?std.process.Environ) usize {
    const text = if (env) |e| winEnvValue(e, "ZPQ_S3_FIRST_WAVE") else null;
    return parseFirstWave(text);
}

fn parseFirstWave(text: ?[]const u8) usize {
    const raw = text orelse return S3_FIRST_WAVE;
    const n = std.fmt.parseInt(usize, raw, 10) catch return S3_FIRST_WAVE;
    if (n == 0) return S3_FIRST_WAVE;
    return @min(n, S3_IN_FLIGHT);
}
/// A data GET longer than this is split.
pub const S3_RANGE_SPLIT: u64 = 8 << 20;
/// Assumed bytes per second of one connection, for a coalesce gap once latency has been measured.
pub const S3_BYTES_PER_S: u64 = 80_000_000;
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
    /// S3 answered with an error code this client names. The status, code and message are on the query's
    /// `Diag.detail.s3_request`.
    NoSuchKey,
    NoSuchBucket,
    NoSuchUpload,
    AccessDenied,
    InvalidAccessKeyId,
    SignatureDoesNotMatch,
    WrongRegion,
    SlowDown,
    S3ServerError,
    S3RequestFailed,
    DirectoryBucketInvalidPrefix,
    DirectoryBucketOperationUnsupported,
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
    /// Connection deadlines (`tls.Timeouts`): the defaults, or `ZPQ_S3_*_TIMEOUT_MS` from the environment.
    timeouts: tls.Timeouts = .{},
    /// Address AWS path-style on `s3.<region>.amazonaws.com`. Lambda sets this so the connections opened during
    /// init, before any bucket is known, are the ones the query reuses. The CLI leaves it unset and stays
    /// virtual-hosted.
    path_style: bool = false,
    /// Set only on credentials returned by CreateSession.
    express_session: bool = false,

    pub fn signer(self: Credentials) sigv4.SigV4 {
        return .{
            .region = self.region,
            .service = if (self.express_session) "s3express" else "s3",
            .access_key = self.access_key,
            .secret_key = self.secret_key,
            .session_token = self.session_token,
            .s3express_session = self.express_session,
        };
    }

    pub fn fromEnv(env: std.process.Environ) Error!Credentials {
        const vars = if (builtin.os.tag == .windows) WindowsEnvironment{ .environ = env } else env;
        // `S3_NO_SIGN_REQUEST` (any value but empty or `0`) asks for anonymous
        // requests: no keys are read, and the empty access key tells the
        // signer to leave the Authorization header off. Only buckets that
        // allow public access answer these.
        if (vars.getPosix("S3_NO_SIGN_REQUEST")) |v| {
            if (v.len > 0 and !std.mem.eql(u8, v, "0")) {
                return .{
                    .access_key = "",
                    .secret_key = "",
                    .region = vars.getPosix("S3_REGION") orelse vars.getPosix("AWS_REGION") orelse return error.NoRegion,
                    .endpoint = vars.getPosix("S3_ENDPOINT_URL"),
                    .timeouts = .fromEnv(env),
                };
            }
        }
        // `S3_*`-prefixed vars override `AWS_*`. This lets a Lambda
        // function target a non-AWS S3-compatible endpoint (R2, RustFS,
        // GCS in compat mode) without colliding with Lambda's
        // reserved `AWS_*` env vars (which the runtime injects from
        // the execution role and refuses to let us override at deploy
        // time).
        //
        // Critically: session_token is paired with the access_key it was
        // issued for — never mix S3_* credentials with AWS_SESSION_TOKEN.
        // R2 (and most S3-compat servers) reject `X-Amz-Security-Token`
        // entirely, returning HTTP 400 InvalidArgument.
        const s3_ak = vars.getPosix("S3_ACCESS_KEY_ID");
        const ak = s3_ak orelse vars.getPosix("AWS_ACCESS_KEY_ID") orelse return error.NoCredentials;
        const sk = if (s3_ak != null)
            vars.getPosix("S3_SECRET_ACCESS_KEY") orelse return error.NoCredentials
        else
            vars.getPosix("AWS_SECRET_ACCESS_KEY") orelse return error.NoCredentials;
        const region = if (s3_ak != null)
            vars.getPosix("S3_REGION") orelse vars.getPosix("AWS_REGION") orelse return error.NoRegion
        else
            vars.getPosix("AWS_REGION") orelse vars.getPosix("S3_REGION") orelse return error.NoRegion;
        const session_token = if (s3_ak != null)
            vars.getPosix("S3_SESSION_TOKEN")
        else
            vars.getPosix("AWS_SESSION_TOKEN");
        return .{
            .access_key = ak,
            .secret_key = sk,
            .session_token = session_token,
            .region = region,
            .endpoint = vars.getPosix("S3_ENDPOINT_URL"),
            .timeouts = .fromEnv(env),
        };
    }

    /// Strip protocol, optional port, and trailing slash from an endpoint URL,
    /// returning just the DNS hostname. Doesn't allocate; result is a slice into
    /// the input. `http://rustfs:9000/` -> `rustfs`.
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
    if (creds.endpoint == null and directoryBucketZone(bucket) != null) return true;
    if (creds.path_style) return false;
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
    if (directoryBucketZone(bucket)) |zone| {
        return std.fmt.allocPrint(arena, "{s}.s3express-{s}.{s}.amazonaws.com", .{ bucket, zone, creds.region });
    }
    if (!virtualHosted(creds, bucket)) return std.fmt.allocPrint(arena, "s3.{s}.amazonaws.com", .{creds.region});
    return std.fmt.allocPrint(arena, "{s}.s3.{s}.amazonaws.com", .{ bucket, creds.region });
}

/// The zone is the component immediately before the directory-bucket suffix.
pub fn directoryBucketZone(bucket: []const u8) ?[]const u8 {
    const suffix = "--x-s3";
    if (!std.mem.endsWith(u8, bucket, suffix)) return null;
    const stem = bucket[0 .. bucket.len - suffix.len];
    const separator = std.mem.lastIndexOf(u8, stem, "--") orelse return null;
    if (separator == 0 or separator + 2 == stem.len) return null;
    const zone = stem[separator + 2 ..];
    for (zone) |ch| if (!(std.ascii.isLower(ch) or std.ascii.isDigit(ch) or ch == '-')) return null;
    return zone;
}

test "directory bucket zonal host and virtual path" {
    var arena = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena.deinit();
    const a = arena.allocator();
    const bucket = "example--usw2-az1--x-s3";
    const creds: Credentials = .{ .access_key = "a", .secret_key = "s", .region = "us-west-2", .path_style = true };
    try std.testing.expectEqualStrings("usw2-az1", directoryBucketZone(bucket).?);
    try std.testing.expectEqualStrings("example--usw2-az1--x-s3.s3express-usw2-az1.us-west-2.amazonaws.com", try hostFor(a, creds, bucket));
    try std.testing.expectEqualStrings("/object", try pathFor(a, creds, bucket, "object"));
    try std.testing.expect(directoryBucketZone("example--x-s3") == null);
    try std.testing.expect(directoryBucketZone("example----x-s3") == null);
    try std.testing.expect(directoryBucketZone("example--zone--x-s3.extra") == null);
}

pub fn portFor(creds: Credentials) u16 {
    return creds.endpointPort();
}

pub fn useTls(creds: Credentials) bool {
    return creds.endpointUseTls();
}

/// How many connections to open before the first request. `ZPQ_S3_PRECONNECT`, default 4; 0 opens none.
/// A value that does not parse is the default.
pub fn preconnectCount(env: std.process.Environ) usize {
    const v = winEnvValue(env, "ZPQ_S3_PRECONNECT") orelse return 4;
    return std.fmt.parseInt(usize, v, 10) catch 4;
}

pub fn poolCriteria(arena: std.mem.Allocator, creds: Credentials, bucket: []const u8) !PoolCriteria {
    const host = try hostFor(arena, creds, bucket);
    const connect_host = try connectHostFor(arena, creds, bucket);
    const addrs = try resolveIpv4(arena, connect_host);
    return .{
        .host = host,
        .addrs = addrs,
        .port = portFor(creds),
        .use_tls = useTls(creds),
        .timeouts = creds.timeouts,
    };
}

/// A one-off connection to `host`, at the first of `addrs` that accepts.
pub fn connect(
    arena: std.mem.Allocator,
    creds: Credentials,
    addrs: []const tls.Ipv4,
    host: []const u8,
) Error!tls.Connection {
    const peer: tls.Peer = .{ .addrs = addrs, .port = portFor(creds), .timeouts = creds.timeouts };
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
        const bucket = rest[0..slash];
        const key = rest[slash + 1 ..];
        // The bucket goes into the Host header and, path-style, the request line as it is: only the characters a
        // bucket name may hold (AWS's, plus the uppercase and `_` of legacy and S3-compatible stores) get that far.
        // The key is percent-encoded on the wire, but a control byte in it is refused all the same.
        for (bucket) |ch| {
            if (!(std.ascii.isAlphanumeric(ch) or ch == '.' or ch == '-' or ch == '_')) return error.BadS3Url;
        }
        for (key) |ch| if (ch < 0x20 or ch == 0x7f) return error.BadS3Url;
        return .{ .bucket = bucket, .key = key };
    }
};

pub const SessionMode = enum { read_only, read_write };

// Entries live for the process lifetime so a detached refresh never borrows a query's arena. The mutex protects both
// the list and each entry; request credentials are copied into the caller's arena before releasing it.
const SessionEntry = struct {
    next: ?*SessionEntry = null,
    base: Credentials,
    bucket: []const u8,
    mode: SessionMode,
    refreshing: bool = false,
    done: Io.Condition = .init,
    session_arena: ?*std.heap.ArenaAllocator = null,
    session: ?Credentials = null,
    expires: i64 = 0,
    failure: Error = error.S3RequestFailed,
};
var session_mutex: Io.Mutex = .init;
var sessions: ?*SessionEntry = null;
const session_io = @import("../core/blocking.zig").io;
const session_alloc = std.heap.page_allocator;

fn sameOptional(a: ?[]const u8, b: ?[]const u8) bool {
    if (a == null or b == null) return a == null and b == null;
    return std.mem.eql(u8, a.?, b.?);
}

fn copyCredentials(a: std.mem.Allocator, creds: Credentials) !Credentials {
    var out = creds;
    out.access_key = try a.dupe(u8, creds.access_key);
    out.secret_key = try a.dupe(u8, creds.secret_key);
    out.region = try a.dupe(u8, creds.region);
    out.session_token = if (creds.session_token) |v| try a.dupe(u8, v) else null;
    out.endpoint = if (creds.endpoint) |v| try a.dupe(u8, v) else null;
    return out;
}

/// Acquire a bucket-scoped session. The refresh thread is independent of the caller's cancellation and arena.
pub fn sessionCredentials(arena: std.mem.Allocator, base: Credentials, bucket: []const u8, mode: SessionMode) Error!Credentials {
    return sessionCredentialsWait(null, arena, base, bucket, mode) catch |err| switch (err) {
        error.Canceled => unreachable,
        else => |e| return e,
    };
}

pub fn sessionCredentialsWithIo(io: Io, arena: std.mem.Allocator, base: Credentials, bucket: []const u8, mode: SessionMode) (Error || Io.Cancelable)!Credentials {
    return sessionCredentialsWait(io, arena, base, bucket, mode);
}

fn sessionCredentialsWait(io: ?Io, arena: std.mem.Allocator, base: Credentials, bucket: []const u8, mode: SessionMode) (Error || Io.Cancelable)!Credentials {
    if (directoryBucketZone(bucket) == null or base.express_session) return base;
    if (base.access_key.len == 0) return error.NoCredentials;
    session_mutex.lockUncancelable(session_io);
    defer session_mutex.unlock(session_io);
    var entry = sessions;
    while (entry) |e| : (entry = e.next) {
        if (e.mode == mode and std.mem.eql(u8, e.bucket, bucket) and
            std.mem.eql(u8, e.base.region, base.region) and std.mem.eql(u8, e.base.access_key, base.access_key) and
            std.mem.eql(u8, e.base.secret_key, base.secret_key) and sameOptional(e.base.session_token, base.session_token) and
            sameOptional(e.base.endpoint, base.endpoint)) break;
    }
    if (entry == null) {
        const storage = try session_alloc.create(std.heap.ArenaAllocator);
        storage.* = std.heap.ArenaAllocator.init(session_alloc);
        errdefer {
            storage.deinit();
            session_alloc.destroy(storage);
        }
        const a = storage.allocator();
        const e = try a.create(SessionEntry);
        e.* = .{ .base = try copyCredentials(a, base), .bucket = try a.dupe(u8, bucket), .mode = mode, .next = sessions };
        sessions = e;
        entry = e;
    }
    const e = entry.?;
    const now = clock.realtimeS();
    if (e.session != null and e.expires > now + 60) return copyCredentials(arena, e.session.?);
    if (!e.refreshing) {
        e.refreshing = true;
        const t = std.Thread.spawn(.{}, refreshSession, .{e}) catch {
            e.refreshing = false;
            return error.S3RequestFailed;
        };
        t.detach();
    }
    if (e.session != null and e.expires > now + 5) return copyCredentials(arena, e.session.?);
    while (e.refreshing) {
        if (io) |wait_io| try e.done.wait(wait_io, &session_mutex) else e.done.waitUncancelable(session_io, &session_mutex);
    }
    if (e.session) |s| if (e.expires > clock.realtimeS() + 5) return copyCredentials(arena, s);
    return e.failure;
}

const SessionResult = struct { creds: Credentials, expires: i64 };

fn refreshSession(e: *SessionEntry) void {
    const storage = session_alloc.create(std.heap.ArenaAllocator) catch {
        session_mutex.lockUncancelable(session_io);
        e.failure = error.OutOfMemory;
        e.refreshing = false;
        e.done.broadcast(session_io);
        session_mutex.unlock(session_io);
        return;
    };
    storage.* = std.heap.ArenaAllocator.init(session_alloc);
    var scratch = std.heap.ArenaAllocator.init(session_alloc);
    defer scratch.deinit();
    const result: Error!SessionResult = blk: {
        const fresh = createSession(scratch.allocator(), e.base, e.bucket, e.mode) catch |err| break :blk err;
        const held = copyCredentials(storage.allocator(), fresh.creds) catch |err| break :blk err;
        break :blk .{ .creds = held, .expires = fresh.expires };
    };
    session_mutex.lockUncancelable(session_io);
    if (result) |s| {
        if (e.session_arena) |old| {
            old.deinit();
            session_alloc.destroy(old);
        }
        e.session_arena = storage;
        e.session = s.creds;
        e.expires = s.expires;
    } else |err| {
        storage.deinit();
        session_alloc.destroy(storage);
        e.failure = err;
    }
    e.refreshing = false;
    e.done.broadcast(session_io);
    session_mutex.unlock(session_io);
}

fn createSession(arena: std.mem.Allocator, base: Credentials, bucket: []const u8, mode: SessionMode) Error!SessionResult {
    const host = try hostFor(arena, base, bucket);
    const connect_host = try connectHostFor(arena, base, bucket);
    const addrs = try resolveIpv4(arena, connect_host);
    var conn = try connect(arena, base, addrs, host);
    defer conn.deinit();
    const path = try pathFor(arena, base, bucket, "");
    var signer = base.signer();
    signer.service = "s3express";
    const mode_header = if (mode == .read_only) "ReadOnly" else "ReadWrite";
    const signed = signer.sign(arena, "GET", host, path, "session=", &.{.{ .name = "x-amz-create-session-mode", .value = mode_header }}, "", .{}) catch return error.SignFailed;
    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });
    const target = try std.fmt.allocPrint(arena, "{s}?session", .{path});
    const resp = try http.sendRequest(arena, &conn, .{ .method = .GET, .host = host, .path = target, .headers = headers.items });
    if (resp.status != 200) return refusal(.GetObject, bucket, "", resp, base).err;
    var creds = base;
    creds.access_key = extractXml(arena, resp.body, "AccessKeyId") catch return error.BadResponse;
    creds.secret_key = extractXml(arena, resp.body, "SecretAccessKey") catch return error.BadResponse;
    creds.session_token = extractXml(arena, resp.body, "SessionToken") catch return error.BadResponse;
    creds.express_session = true;
    const exp = extractXml(arena, resp.body, "Expiration") catch return error.BadResponse;
    const expires = parseSessionExpiration(exp) orelse return error.BadResponse;
    if (expires <= clock.realtimeS() + 5) return error.BadResponse;
    return .{ .creds = creds, .expires = expires };
}

fn parseSessionExpiration(s: []const u8) ?i64 {
    if (s.len < 20 or s[4] != '-' or s[7] != '-' or s[10] != 'T' or s[13] != ':' or s[16] != ':' or s[s.len - 1] != 'Z') return null;
    const num = struct {
        fn f(t: []const u8) ?i64 {
            return std.fmt.parseInt(i64, t, 10) catch null;
        }
    }.f;
    const y = num(s[0..4]) orelse return null;
    const m = num(s[5..7]) orelse return null;
    const d = num(s[8..10]) orelse return null;
    const hh = num(s[11..13]) orelse return null;
    const mm = num(s[14..16]) orelse return null;
    const ss = num(s[17..19]) orelse return null;
    if (m < 1 or m > 12 or d < 1 or d > 31 or hh > 23 or mm > 59 or ss > 59) return null;
    const yy = if (m <= 2) y - 1 else y;
    const era = @divFloor(yy, 400);
    const yoe = yy - era * 400;
    const mp = @mod(m + 9, 12);
    const doy = @divFloor(153 * mp + 2, 5) + d - 1;
    const doe = yoe * 365 + @divFloor(yoe, 4) - @divFloor(yoe, 100) + doy;
    return (era * 146097 + doe - 719468) * 86400 + hh * 3600 + mm * 60 + ss;
}

test "S3 Express session expiration and standard credentials" {
    try std.testing.expectEqual(@as(?i64, 1_791_066_063), parseSessionExpiration("2026-10-03T22:21:03.000Z"));
    try std.testing.expectEqual(@as(?i64, null), parseSessionExpiration("bad"));
    const base: Credentials = .{ .access_key = "key", .secret_key = "secret", .region = "us-west-2" };
    const got = try sessionCredentials(std.testing.allocator, base, "standard-bucket", .read_only);
    try std.testing.expectEqualStrings(base.access_key, got.access_key);
}

test "an S3 URL with a bucket that could break a header, or a control byte in its key, is refused" {
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt\r\nX-Injected: 1/key"));
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt evil/key"));
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt:443/key"));
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt/a\nb"));
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt/a\x00b"));
    try std.testing.expectError(error.BadS3Url, Url.parse("s3://bkt/a\x7fb"));
    const u = try Url.parse("s3://My_legacy.bucket-1/dir/a b+c%.parquet");
    try std.testing.expectEqualStrings("My_legacy.bucket-1", u.bucket);
    try std.testing.expectEqualStrings("dir/a b+c%.parquet", u.key);
}

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
    /// Receives a failed LIST's S3 answer; null when the caller has no diagnostic.
    diag: ?*diag_mod.Diag = null,

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
        const creds = try sessionCredentials(req_arena, self.creds, self.bucket, .read_only);
        if (self.conn == null) {
            self.conn = try connect(self.client_arena, self.creds, self.addrs, self.host);
        }
        return try buildAndSend(
            req_arena,
            &self.conn.?,
            creds,
            self.host,
            self.bucket,
            key,
            .{ .range = range },
        );
    }

    pub fn head(self: *Client, arena: std.mem.Allocator, key: []const u8) Error!http.Response {
        return self.headOnce(arena, key) catch |err| switch (err) {
            error.ClosedBeforeResponse, error.RecvFailed, error.SendFailed, error.BodyTruncated, error.BadStatusLine => blk: {
                if (self.conn) |*conn| conn.deinit();
                self.conn = null;
                break :blk try self.headOnce(arena, key);
            },
            else => return err,
        };
    }

    fn headOnce(self: *Client, arena: std.mem.Allocator, key: []const u8) Error!http.Response {
        const creds = try sessionCredentials(arena, self.creds, self.bucket, .read_only);
        if (self.conn == null) self.conn = try connect(self.client_arena, creds, self.addrs, self.host);
        const path = try pathFor(arena, creds, self.bucket, key);
        const signed = creds.signer().sign(arena, "HEAD", self.host, path, null, &.{}, "", .{ .use_unsigned_payload = true }) catch return error.SignFailed;
        var headers: std.ArrayList(http.Header) = .empty;
        for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });
        return withRequestSecrets(try http.sendRequest(arena, &self.conn.?, .{ .method = .HEAD, .host = self.host, .path = path, .headers = headers.items }), creds);
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
        if (directoryBucketZone(self.bucket) != null and prefix.len != 0 and !std.mem.endsWith(u8, prefix, "/")) {
            diag_mod.setS3Failure(self.diag, transportFailure(.ListObjectsV2, self.bucket, prefix, error.DirectoryBucketInvalidPrefix));
            return error.DirectoryBucketInvalidPrefix;
        }
        if (self.conn == null) {
            self.conn = try connect(self.client_arena, self.creds, self.addrs, self.host);
        }
        return try sendListV2(req_arena, self, prefix, max_keys, continuation_token);
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
    if (directoryBucketZone(client.bucket) != null) {
        std.sort.pdq(ListEntry, all.items, {}, struct {
            fn less(_: void, a: ListEntry, b: ListEntry) bool {
                return std.mem.lessThan(u8, a.key, b.key);
            }
        }.less);
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
/// limit is 5 GB. Larger bodies go through `multipart_sink.MultipartSink`.
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
    const request_creds = try sessionCredentials(arena, creds, url.bucket, .read_write);
    const host = try hostFor(arena, creds, url.bucket);
    const connect_host = try connectHostFor(arena, creds, url.bucket);
    const addrs = try resolveIpv4(arena, connect_host);
    var conn = try connect(arena, creds, addrs, host);
    defer conn.deinit();
    return try sendPut(arena, &conn, request_creds, host, url.bucket, url.key, body);
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
    if_match: ?[]const u8 = null,
    /// Records the request as an `http_get` span when set.
    prof: ?*profile.Thread = null,
};

pub fn getViaPoolWithOpts(
    io: Io,
    p: anytype,
    arena: std.mem.Allocator,
    creds: Credentials,
    url: Url,
    opts: GetOpts,
) !http.Response {
    const request_creds = try sessionCredentialsWithIo(io, arena, creds, url.bucket, .read_only);
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
    // A suffix range has no start until the object's size is known: its span shows [0, n).
    const r = opts.range;
    const is_suffix = r != null and r.?.start == std.math.maxInt(u64);
    const bytes: u64 = if (r) |rr| (if (is_suffix) rr.end else rr.end - rr.start + 1) else 0;
    const tag: [2]u64 = if (r) |rr| (if (is_suffix) .{ 0, rr.end } else .{ rr.start, rr.end + 1 }) else .{ 0, 0 };
    const response = try requestViaPool(io, p, arena, criteria, .{
        .idempotent = true,
        .method = .GET,
        .prof = opts.prof,
        .tag = tag,
        .bytes = bytes,
    }, Get{ .creds = request_creds, .host = host, .url = url, .opts = opts }, Get.send);
    return withRequestSecrets(response, request_creds);
}

pub const RequestOpts = struct {
    /// HTTP attempts counted after acquiring a connection; null for unclassified control operations.
    method: ?http.Method = null,
    /// See `retry.Request.idempotent`; decides whether a dead pooled connection is retried at once or fails the
    /// request.
    idempotent: bool,
    /// Records the request, every attempt included, as one span of `kind` when set.
    prof: ?*profile.Thread = null,
    kind: profile.Kind = .http_get,
    /// The span's first two arguments: a GET's byte range, a part upload's number.
    tag: [2]u64 = .{ 0, 0 },
    /// Bytes the request is expected to move, for the in-flight gauge; 0 when unknown.
    bytes: u64 = 0,
};

/// One request's timing for its profile span: the final attempt's pool wait and connect, time to first byte, and
/// body transfer.
const RequestTrace = struct {
    opts: RequestOpts,
    start: profile.Span,
    attempts: u8 = 0,
    status: u16 = 0,
    reused: bool = false,
    body_len: u64 = 0,
    acquire_ns: i64 = 0,
    ttfb_ns: i64 = 0,
    body_ns: i64 = 0,

    fn begin(opts: RequestOpts) RequestTrace {
        profile.gauge(opts.prof, .requests_in_flight, 1);
        profile.gauge(opts.prof, .bytes_in_flight, @intCast(opts.bytes));
        return .{ .opts = opts, .start = profile.Span.begin(opts.prof) };
    }

    fn end(self: *const RequestTrace) void {
        const p = self.opts.prof orelse return;
        p.gauge(.requests_in_flight, -1);
        p.gauge(.bytes_in_flight, -@as(i64, @intCast(self.opts.bytes)));
        const clamp = struct {
            fn f(ns: i64) u64 {
                return @intCast(std.math.clamp(ns, 0, std.math.maxInt(u32)));
            }
        }.f;
        self.start.end(self.opts.kind, profile.args(.{
            self.opts.tag[0],
            self.opts.tag[1],
            self.body_len,
            profile.httpFlags(self.status, self.attempts, self.reused),
            clamp(self.ttfb_ns) | (clamp(self.acquire_ns) << 32),
            @as(u64, @intCast(@max(0, self.body_ns))),
        }));
    }
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
    var trace: RequestTrace = .begin(opts);
    defer trace.end();
    while (true) {
        var last_err: anyerror = error.RetryableStatus;
        var retry_status: u16 = 0;
        var slow_down = false;
        const failure: retry.Failure = blk: {
            const t_acquire = clock.monoNs();
            trace.attempts +|= 1;
            const handle = p.acquire(io, criteria, .{ .fresh = attempts.fresh }) catch |err| {
                last_err = err;
                break :blk retry.classifyConnect(err);
            };
            const t_send = clock.monoNs();
            trace.reused = handle.reused;
            if (send(context, arena, handle.conn)) |resp| {
                p.release(io, handle);
                var out = resp;
                if (out.first_byte_ns != 0) {
                    const d = out.first_byte_ns - t_send;
                    if (d > 0) out.ttfb_ns = d;
                }
                p.noteS3(io, .{
                    .get = opts.method == .GET,
                    .head = opts.method == .HEAD,
                    .bytes = if (opts.method == .HEAD) 0 else out.body.len,
                    .ttfb_ns = @intCast(@max(0, out.ttfb_ns)),
                });
                if (opts.prof != null) {
                    const t_done = clock.monoNs();
                    const first = if (out.first_byte_ns != 0) out.first_byte_ns else t_done;
                    trace.status = out.status;
                    trace.body_len = out.body.len;
                    trace.acquire_ns = t_send - t_acquire;
                    trace.ttfb_ns = first - t_send;
                    trace.body_ns = t_done - first;
                }
                if (!retry.retryableStatus(out.status)) return out;
                retry_status = out.status;
                const code = out.header("x-amz-error-code") orelse extractTag(out.body, "<Code>", "</Code>") orelse "";
                slow_down = std.mem.eql(u8, std.mem.trim(u8, code, " \t\r\n"), "SlowDown");
                throttled = out;
                break :blk .transient;
            } else |err| {
                p.noteS3(io, .{ .get = opts.method == .GET, .head = opts.method == .HEAD });
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
        if (retry_status != 0) p.noteS3(io, .{ .retry_status = retry_status, .slow_down = slow_down });
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
    const request_creds = try sessionCredentialsWithIo(io, arena, creds, url.bucket, .read_write);
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
    return requestViaPool(io, p, arena, criteria, .{ .idempotent = true }, Put{ .creds = request_creds, .host = host, .url = url, .body = body }, Put.send);
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

    const signer = creds.signer();

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

    return withRequestSecrets(try http.sendRequest(arena, conn, .{
        .method = .PUT,
        .host = host,
        .path = path,
        .headers = req_headers.items,
        .body = body,
    }), creds);
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
    /// Told, on the fetching thread, each time one of this job's requests has landed its bytes in `target` (a job
    /// split into several requests reports each), so a reader can start on them before the whole batch is in.
    landed: ?Landed = null,
    /// This request overlaps a filter column, so a dispatch sends it before the others.
    urgent: bool = false,
    /// A footer-derived selection must read only the generation the planner proved.
    if_match: ?[]const u8 = null,
};

pub const Landed = struct {
    ctx: *anyopaque,
    f: *const fn (ctx: *anyopaque, bytes: usize) void,
};

/// The query's first data wave. At most `firstWaveCap` data-range requests are in flight until the first
/// data-range response header of this query arrives, then at most `DataWave.full` (`inFlight`). The default cap is
/// `S3_FIRST_WAVE`; `ZPQ_S3_FIRST_WAVE=16` starts at the full cap. Footer, metadata and Bloom requests do not use
/// a `DataWave`, so they neither take a slot nor open the gate. One query shares one value across its data fetches;
/// a later batch in that query already sees the raised cap.
pub const DataWave = struct {
    cap: std.atomic.Value(usize) = .init(S3_FIRST_WAVE),
    in_flight: std.atomic.Value(usize) = .init(0),
    /// The cap once the first header is in (`inFlight`).
    full: usize = S3_IN_FLIGHT,
    /// Bumped whenever a slot may have opened (a `release`, the first `noteHeader`). A worker without a slot sleeps
    /// on it instead of polling: until the first header, the workers past the first wave would otherwise wake every
    /// 50 µs, and on a 2-vCPU Lambda that CPU was taken from the TLS handshakes whose header they were waiting for.
    seq: std.atomic.Value(u32) = .init(0),

    /// Wait until fewer than `cap` data-range requests are in flight, then count this one.
    pub fn acquire(self: *DataWave, io: Io) Io.Cancelable!void {
        while (true) {
            // Read `seq` before the slots: a release between the two moves it, and the wait returns at once.
            const seen = self.seq.load(.acquire);
            const cap = self.cap.load(.acquire);
            var cur = self.in_flight.load(.acquire);
            while (cur < cap) {
                if (self.in_flight.cmpxchgWeak(cur, cur + 1, .acq_rel, .acquire)) |observed| {
                    cur = observed;
                    continue;
                }
                return;
            }
            try io.futexWait(u32, &self.seq.raw, seen);
        }
    }

    pub fn release(self: *DataWave, io: Io) void {
        _ = self.in_flight.fetchSub(1, .release);
        _ = self.seq.fetchAdd(1, .release);
        io.futexWake(u32, &self.seq.raw, 1);
    }

    /// A data-range response header has arrived. Later acquires may use the full cap. Idempotent: only the first wakes
    /// the waiting workers.
    pub fn noteHeader(self: *DataWave, io: Io) void {
        if (self.cap.swap(self.full, .acq_rel) == self.full) return;
        _ = self.seq.fetchAdd(1, .release);
        io.futexWake(u32, &self.seq.raw, std.math.maxInt(u32));
    }
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
    /// Records each request as a span on the fetching thread when set.
    profiler: ?*profile.Profiler,
    /// The query's data wave, or null for a fetch that is not a data range (Bloom, tests, `fetchManyRanges`).
    wave: ?*DataWave,
    /// Receives the first failed request's named error and S3 answer.
    diag: ?*diag_mod.Diag,
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
                    .landed = j.landed,
                    .urgent = j.urgent,
                    .if_match = j.if_match,
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
            .if_match = j.if_match,
            .target = j.target,
            .ok = false,
            .profiler = profiler,
            .landed = j.landed,
            .wave = wave,
            .io = io,
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
    // Every worker starts now. A data wave holds each one at `DataWave` until a slot is free, so the first
    // response header, not a claim count, is what lets the rest go out.
    try work_cursor.startWorkers(FetchCtx, &group, io, &shared, n_workers, io, fetchOneTask);
    try group.await(io);

    // 5. Tally + check for failures. A later named refusal is more useful than an earlier short body.
    var fetched: u64 = 0;
    var failed: ?anyerror = null;
    var failed_failure: ?S3Failure = null;
    for (ctxs) |ctx| {
        if (ctx.ok) {
            fetched += ctx.target.len;
            continue;
        }
        if (failed == null or (failed_failure == null and ctx.failure != null)) {
            failed = ctx.err orelse error.RangeFetchFailed;
            failed_failure = ctx.failure;
        }
    }
    if (failed) |err| {
        if (failed_failure) |f| diag_mod.setS3Failure(diag, f);
        return err;
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
    return fetchJobs(io, p, gpa, arena, creds, jobs.items, @TypeOf(p.*).capacity, null, null, null);
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
    if_match: ?[]const u8 = null,
    /// Pre-sized target buffer; `target.len == range.end - range.start`.
    target: []u8,
    ok: bool,
    /// The named error and S3 answer when this request failed; read after the workers join.
    err: ?anyerror = null,
    failure: ?diag_mod.S3Failure = null,
    profiler: ?*profile.Profiler = null,
    landed: ?Landed = null,
    /// Set for a query's data-range fetches. Null for Bloom and other callers.
    wave: ?*DataWave = null,
    /// The fetch's executor, for the wave's wakeups from the header callback.
    io: Io,
};

/// Fetch `ctx.range` into `ctx.target`, setting `ctx.ok` on success, retrying as `requestViaPool` does.
fn fetchOneTask(io: Io, ctx: *FetchCtx) Io.Cancelable!void {
    if (ctx.wave) |w| try w.acquire(io);
    defer if (ctx.wave) |w| w.release(io);
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const criteria: PoolCriteria = .{
        .host = ctx.host,
        .addrs = ctx.addrs,
        .port = ctx.port,
        .use_tls = ctx.use_tls,
        .timeouts = ctx.creds.timeouts,
    };
    const opts: RequestOpts = .{
        .idempotent = true,
        .method = .GET,
        .prof = profile.threadOf(ctx.profiler, .fetch),
        .tag = .{ ctx.range.start, ctx.range.end },
        .bytes = ctx.target.len,
    };
    const arena = arena_state.allocator();
    const resp = requestViaPool(io, ctx.pool, arena, criteria, opts, ctx, doFetch) catch |err| switch (err) {
        error.Canceled => return error.Canceled,
        else => {
            ctx.err = err;
            ctx.failure = transportFailure(.GetObject, ctx.bucket, ctx.key, err);
            return;
        },
    };
    // Any status but a full 200/206 (including a throttle that outlasted the budget) fails this fetch.
    if (resp.status == 206 or resp.status == 200) {
        ctx.ok = resp.body.len == ctx.target.len;
    } else if (resp.status == 412 and ctx.if_match != null) {
        ctx.err = error.AlignedJoinSnapshotChanged;
    } else {
        const r = refusal(.GetObject, ctx.bucket, ctx.key, resp, ctx.creds);
        ctx.failure = r.failure;
        ctx.err = r.err;
    }
    if (ctx.ok) if (ctx.landed) |l| l.f(l.ctx, ctx.target.len);
}

/// Issue one ranged GET on `conn`, streaming the body into `ctx.target`.
fn doFetch(ctx: *FetchCtx, arena: std.mem.Allocator, conn: *tls.Connection) !http.Response {
    const creds = try sessionCredentialsWithIo(ctx.io, arena, ctx.creds, ctx.bucket, .read_only);
    const path = try pathFor(arena, creds, ctx.bucket, ctx.key);

    var range_buf: [64]u8 = undefined;
    const inclusive: Range = .{ .start = ctx.range.start, .end = ctx.range.end - 1 };
    const range_header = try inclusive.writeHeader(&range_buf);

    const signer = creds.signer();

    var hdr_in: std.ArrayList(sigv4.SigV4.Header) = .empty;
    try hdr_in.append(arena, .{ .name = "Range", .value = range_header });
    if (ctx.if_match) |etag| try hdr_in.append(arena, .{ .name = "If-Match", .value = etag });

    const signed = try signer.sign(arena, "GET", ctx.host, path, null, hdr_in.items, "", .{ .use_unsigned_payload = true });

    var headers: std.ArrayList(http.Header) = .empty;
    for (signed) |h| try headers.append(arena, .{ .name = h.name, .value = h.value });

    return withRequestSecrets(try http.sendRequestInto(arena, conn, .{
        .method = .GET,
        .host = ctx.host,
        .path = path,
        .headers = headers.items,
    }, ctx.target, if (ctx.wave != null) .{ .ctx = ctx, .f = noteDataHeader } else null), creds);
}

fn noteDataHeader(ctx: *anyopaque) void {
    const f: *FetchCtx = @ptrCast(@alignCast(ctx));
    if (f.wave) |w| w.noteHeader(f.io);
}

pub fn extractXml(arena: std.mem.Allocator, xml: []const u8, tag: []const u8) ![]const u8 {
    const open = try std.fmt.allocPrint(arena, "<{s}>", .{tag});
    const close = try std.fmt.allocPrint(arena, "</{s}>", .{tag});
    const start = std.mem.indexOf(u8, xml, open) orelse return error.XmlTagMissing;
    const after_open = start + open.len;
    const end = std.mem.indexOfPos(u8, xml, after_open, close) orelse return error.XmlTagMissing;
    return try arena.dupe(u8, xml[after_open..end]);
}

pub const S3Failure = diag_mod.S3Failure;

/// One failed S3 response and the named error it maps to.
pub const Refusal = struct {
    failure: S3Failure,
    err: Error,
};

/// Keep the credentials used for this request until its error is mapped; a refresh may replace the cached session.
pub fn withRequestSecrets(response: http.Response, creds: Credentials) http.Response {
    var result = response;
    if (creds.express_session) result.request_secrets = .{
        .access_key = creds.access_key,
        .secret_key = creds.secret_key,
        .session_token = creds.session_token,
    };
    return result;
}

fn secretAt(text: []const u8, secret: []const u8) ?usize {
    if (secret.len == 0 or !std.mem.startsWith(u8, text, secret)) return null;
    return secret.len;
}

fn redactDelimiter(ch: u8) bool {
    return switch (ch) {
        ' ', '\t', '\r', '\n', '"', '\'', '&', ';', ',', ')', ']', '}', '>' => true,
        else => false,
    };
}

const RedactField = struct { marker: usize, value: usize };

fn redactFieldAt(text: []const u8) ?RedactField {
    const markers = [_][]const u8{
        "x-amz-credential=",     "credential=",            "x-amz-signature=", "signature=",
        "x-amz-security-token=", "x-amz-s3session-token=", "security-token=",  "awsaccesskeyid=",
        "canonicalrequest=",     "authorization:",
    };
    for (markers) |marker| {
        if (!std.ascii.startsWithIgnoreCase(text, marker)) continue;
        var end = marker.len;
        while (end < text.len and !redactDelimiter(text[end])) end += 1;
        return .{ .marker = marker.len, .value = end - marker.len };
    }
    return null;
}

fn awsKeyIdAt(text: []const u8) ?usize {
    const prefixes = [_][]const u8{
        "AKIA", "ASIA", "AIDA", "AROA", "AIPA", "ANPA", "ANVA", "APKA", "AGPA", "ACCA", "ABIA",
    };
    for (prefixes) |prefix| {
        if (!std.mem.startsWith(u8, text, prefix) or text.len < 20) continue;
        for (text[prefix.len..20]) |ch| if (!std.ascii.isAlphanumeric(ch)) return null;
        return 20;
    }
    return null;
}

fn copyRedacted(out: []u8, at: *usize, bytes: []const u8) void {
    const len = @min(out.len - at.*, bytes.len);
    @memcpy(out[at.*..][0..len], bytes[0..len]);
    at.* += len;
}

/// Copy S3's text with the configured credentials, authorization fields, signatures and AWS key IDs replaced.
fn redactInto(dst: *diag_mod.Text, text: []const u8, creds: Credentials, secrets: ?http.Response.RequestSecrets) void {
    var out: [256]u8 = undefined;
    var at: usize = 0;
    var i: usize = 0;
    while (i < text.len and at < out.len) {
        if (secretAt(text[i..], creds.access_key)) |len| {
            copyRedacted(&out, &at, "[redacted]");
            i += len;
        } else if (secretAt(text[i..], creds.secret_key)) |len| {
            copyRedacted(&out, &at, "[redacted]");
            i += len;
        } else if (if (creds.session_token) |token| secretAt(text[i..], token) else null) |len| {
            copyRedacted(&out, &at, "[redacted]");
            i += len;
        } else if (if (secrets) |s| secretAt(text[i..], s.access_key) orelse secretAt(text[i..], s.secret_key) orelse (if (s.session_token) |token| secretAt(text[i..], token) else null) else null) |len| {
            copyRedacted(&out, &at, "[redacted]");
            i += len;
        } else if (redactFieldAt(text[i..])) |field| {
            copyRedacted(&out, &at, text[i..][0..field.marker]);
            copyRedacted(&out, &at, "[redacted]");
            i += field.marker + field.value;
        } else if (awsKeyIdAt(text[i..])) |len| {
            copyRedacted(&out, &at, "[redacted]");
            i += len;
        } else {
            out[at] = text[i];
            at += 1;
            i += 1;
        }
    }
    dst.set(out[0..at]);
}

/// Parse S3's error answer once: status, `<Code>` and `<Message>`, plus `x-amz-bucket-region`, then map the code to a
/// named error. A HEAD body is empty, so its status and headers are all there is. A redirect or a region complaint is
/// `WrongRegion`; a 5xx is `SlowDown` when S3 says so and `S3ServerError` otherwise. The code and message are copied
/// with credentials and signatures redacted.
pub fn refusal(op: diag_mod.S3Failure.Op, bucket: []const u8, key: []const u8, resp: http.Response, creds: Credentials) Refusal {
    var failure: S3Failure = .{
        .op = op,
        .phase = .response,
        .status = resp.status,
        .bucket = .of(bucket),
        .key = .of(key),
    };
    const code_raw = if (extractTag(resp.body, "<Code>", "</Code>")) |code| std.mem.trim(u8, code, " \t\r\n") else "";
    const message_raw = if (extractTag(resp.body, "<Message>", "</Message>")) |message| std.mem.trim(u8, message, " \t\r\n") else "";
    if (resp.header("x-amz-bucket-region")) |region| failure.region.set(std.mem.trim(u8, region, " \t\r\n"));

    const redirect = resp.status == 301 or resp.status == 307 or resp.status == 308;
    const unsupported = directoryBucketZone(bucket) != null and
        (std.mem.eql(u8, code_raw, "NotImplemented") or std.mem.eql(u8, code_raw, "UnsupportedOperation") or
            (std.mem.eql(u8, code_raw, "InvalidRequest") and
                (std.mem.indexOf(u8, message_raw, "not supported") != null or std.mem.indexOf(u8, message_raw, "Not supported") != null)));
    const err: Error = if (unsupported)
        error.DirectoryBucketOperationUnsupported
    else if (redirect or std.mem.eql(u8, code_raw, "AuthorizationHeaderMalformed") or
        std.mem.eql(u8, code_raw, "PermanentRedirect") or std.mem.eql(u8, code_raw, "TemporaryRedirect"))
        error.WrongRegion
    else if (std.mem.eql(u8, code_raw, "NoSuchKey"))
        error.NoSuchKey
    else if (std.mem.eql(u8, code_raw, "NoSuchBucket"))
        error.NoSuchBucket
    else if (std.mem.eql(u8, code_raw, "NoSuchUpload"))
        error.NoSuchUpload
    else if (std.mem.eql(u8, code_raw, "AccessDenied"))
        error.AccessDenied
    else if (std.mem.eql(u8, code_raw, "InvalidAccessKeyId"))
        error.InvalidAccessKeyId
    else if (std.mem.eql(u8, code_raw, "SignatureDoesNotMatch"))
        error.SignatureDoesNotMatch
    else if (std.mem.eql(u8, code_raw, "InvalidToken") or std.mem.eql(u8, code_raw, "ExpiredToken"))
        error.InvalidAccessKeyId
    else if (std.mem.eql(u8, code_raw, "InvalidRequest") and std.mem.indexOf(u8, message_raw, "region") != null)
        error.WrongRegion
    else if (std.mem.eql(u8, code_raw, "SlowDown") or resp.status == 429)
        error.SlowDown
    else if (resp.status == 404)
        switch (op) {
            .GetObject, .HeadObject => error.NoSuchKey,
            .UploadPart, .CompleteMultipartUpload, .AbortMultipartUpload => error.NoSuchUpload,
            else => error.NoSuchBucket,
        }
    else if (resp.status == 403)
        error.AccessDenied
    else if (resp.status >= 500)
        error.S3ServerError
    else
        error.S3RequestFailed;

    redactInto(&failure.code, code_raw, creds, resp.request_secrets);
    redactInto(&failure.message, message_raw, creds, resp.request_secrets);
    if (failure.message.len == 0 and err == error.NoSuchKey) failure.message.set("the object does not exist");
    return .{ .failure = failure, .err = err };
}

/// A request that never got an answer: its operation, bucket and key, and what failed.
pub fn transportFailure(op: diag_mod.S3Failure.Op, bucket: []const u8, key: []const u8, err: anyerror) S3Failure {
    const before_send = switch (err) {
        error.DnsFailed, error.ConnectFailed, error.ConnectTimeout, error.HandshakeFailed, error.HandshakeTimeout, error.CertificateRejected, error.SocketFailed, error.NonCanonicalIpEndpoint, error.Ipv6EndpointUnsupported => true,
        else => false,
    };
    return .{
        .op = op,
        .phase = if (before_send) .connect else .request,
        .bucket = .of(bucket),
        .key = .of(key),
        .err = err,
    };
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

    const signer = creds.signer();

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
    if (opts.if_match) |etag| try hdr_in.append(req_arena, .{ .name = "If-Match", .value = etag });

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

    return withRequestSecrets(try http.sendRequest(req_arena, conn, .{
        .method = .GET,
        .host = host,
        .path = path,
        .headers = req_headers.items,
    }), creds);
}

/// Issue a ListObjectsV2 request and parse one page of `<Contents>`
/// entries plus an optional `<NextContinuationToken>`. Pagination
/// driver lives in `listAll`; this is the single-RT primitive.
fn sendListV2(
    req_arena: std.mem.Allocator,
    client: *Client,
    prefix: []const u8,
    max_keys: u32,
    continuation_token: ?[]const u8,
) Error!ListPage {
    const creds = try sessionCredentials(req_arena, client.creds, client.bucket, .read_only);
    const host = client.host;
    const bucket = client.bucket;
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

    const signer = creds.signer();
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

    const resp = try http.sendRequest(req_arena, &client.conn.?, .{
        .method = .GET,
        .host = host,
        .path = path_with_query,
        .headers = req_headers.items,
    });
    if (resp.status != 200) {
        const r = refusal(.ListObjectsV2, bucket, prefix, resp, creds);
        diag_mod.setS3Failure(client.diag, r.failure);
        return r.err;
    }

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
const WinDns = struct {
    const AddrInfo = extern struct {
        flags: c_int = 0,
        family: c_int = 0,
        socktype: c_int = 0,
        protocol: c_int = 0,
        addrlen: usize = 0,
        canonname: ?[*:0]u8 = null,
        addr: ?*std.os.windows.ws2_32.sockaddr = null,
        next: ?*AddrInfo = null,
    };
    extern "ws2_32" fn getaddrinfo([*:0]const u8, ?[*:0]const u8, *const AddrInfo, *?*AddrInfo) callconv(.winapi) c_int;
    extern "ws2_32" fn freeaddrinfo(*AddrInfo) callconv(.winapi) void;
    fn numeric(host: [:0]const u8) bool {
        var hints: AddrInfo = .{ .flags = 4, .family = 2, .socktype = 1 };
        var result: ?*AddrInfo = null;
        if (getaddrinfo(host, null, &hints, &result) != 0) return false;
        if (result) |r| freeaddrinfo(r);
        return true;
    }
    fn resolve(arena: std.mem.Allocator, host: [:0]const u8) Error![]const tls.Ipv4 {
        var hints: AddrInfo = .{ .family = 2, .socktype = 1 };
        var result: ?*AddrInfo = null;
        if (getaddrinfo(host, null, &hints, &result) != 0 or result == null) return error.DnsFailed;
        defer freeaddrinfo(result.?);
        var addrs: std.ArrayList(tls.Ipv4) = .empty;
        var it = result;
        while (it) |ai| : (it = ai.next) {
            const sa = ai.addr orelse continue;
            const sin: *align(1) const std.os.windows.ws2_32.sockaddr.in = @ptrCast(sa);
            const ip: tls.Ipv4 = @bitCast(sin.addr);
            for (addrs.items) |seen| {
                if (std.mem.eql(u8, &seen, &ip)) break;
            } else try addrs.append(arena, ip);
        }
        if (addrs.items.len == 0) return error.DnsFailed;
        return addrs.items;
    }
};

/// Refuse a host the resolver would take as an address but the TLS layer would not: an IPv6 literal (connections are
/// IPv4-only), or IPv4 in a legacy spelling getaddrinfo still accepts (`127.1`, `0x7f.0.0.1`, `2130706433`, `010.0.0.1`
/// in octal). Such a host would be dialled as one address and verified as a DNS name, so it is an error, not something
/// to normalise behind the user's back.
fn checkNumericHost(host_z: [:0]const u8) Error!void {
    if (std.mem.indexOfScalar(u8, host_z, ':') != null) {
        std.log.warn("S3 endpoint host '{s}' is an IPv6 address; IPv6 endpoints are not supported", .{host_z});
        return error.Ipv6EndpointUnsupported;
    }
    if (@import("builtin").os.tag == .windows) {
        if (!tls.ensureWindowsSockets()) return error.DnsFailed;
        if (!WinDns.numeric(host_z)) return;
        _ = std.Io.net.Ip4Address.parse(host_z, 0) catch return error.NonCanonicalIpEndpoint;
        return;
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
    if (@import("builtin").os.tag == .windows) return WinDns.resolve(arena, host_z);

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

    var path_style = aws;
    path_style.path_style = true;
    try testing.expectEqualStrings("s3.us-west-2.amazonaws.com", try hostFor(a, path_style, "logs"));
    try testing.expectEqualStrings("/logs/k.parquet", try pathFor(a, path_style, "logs", "k.parquet"));

    const r2: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "auto",
        .endpoint = "https://acct.r2.example:8443",
    };
    try testing.expectEqualStrings("acct.r2.example:8443", try hostFor(a, r2, "logs"));
    try testing.expectEqualStrings("/logs/k.parquet", try pathFor(a, r2, "logs", "k.parquet"));
}

test "S3 refusals name the request, status, code and message, and map the code to a named error" {
    const wrong_region = [_]http.Header{.{ .name = "x-amz-bucket-region", .value = "eu-west-1" }};
    const Case = struct {
        status: u16,
        body: []const u8,
        headers: []const http.Header,
        op: diag_mod.S3Failure.Op,
        key: []const u8,
        want: anyerror,
        text: []const u8,
    };
    const cases = [_]Case{
        .{ .status = 404, .body = "<Error><Code>NoSuchKey</Code><Message>The specified key does not exist.</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.NoSuchKey, .text = "GET s3://bkt/key: 404 NoSuchKey: The specified key does not exist." },
        .{ .status = 404, .body = "<Error><Code>NoSuchBucket</Code><Message>The specified bucket does not exist</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.NoSuchBucket, .text = "GET s3://bkt/key: 404 NoSuchBucket: The specified bucket does not exist" },
        .{ .status = 403, .body = "<Error><Code>AccessDenied</Code><Message>Access Denied</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.AccessDenied, .text = "GET s3://bkt/key: 403 AccessDenied: Access Denied" },
        .{ .status = 403, .body = "<Error><Code>InvalidAccessKeyId</Code><Message>The AWS Access Key Id you provided does not exist.</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.InvalidAccessKeyId, .text = "GET s3://bkt/key: 403 InvalidAccessKeyId: The AWS Access Key Id you provided does not exist." },
        .{ .status = 403, .body = "<Error><Code>SignatureDoesNotMatch</Code><Message>The request signature we calculated does not match.</Message></Error>", .headers = &.{}, .op = .PutObject, .key = "key", .want = error.SignatureDoesNotMatch, .text = "PUT s3://bkt/key: 403 SignatureDoesNotMatch: The request signature we calculated does not match." },
        .{ .status = 301, .body = "", .headers = &wrong_region, .op = .GetObject, .key = "key", .want = error.WrongRegion, .text = "GET s3://bkt/key: 301 (no S3 error code); the bucket is in region eu-west-1" },
        .{ .status = 400, .body = "<Error><Code>AuthorizationHeaderMalformed</Code><Message>The authorization header is malformed; expecting 'eu-west-1'.</Message></Error>", .headers = &wrong_region, .op = .GetObject, .key = "key", .want = error.WrongRegion, .text = "GET s3://bkt/key: 400 AuthorizationHeaderMalformed: The authorization header is malformed; expecting 'eu-west-1'.; the bucket is in region eu-west-1" },
        .{ .status = 400, .body = "<Error><Code>InvalidRequest</Code><Message>invalid credential region</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.WrongRegion, .text = "GET s3://bkt/key: 400 InvalidRequest: invalid credential region" },
        .{ .status = 503, .body = "<Error><Code>SlowDown</Code><Message>Please reduce your request rate.</Message></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.SlowDown, .text = "GET s3://bkt/key: 503 SlowDown: Please reduce your request rate." },
        .{ .status = 500, .body = "<Error><Code>InternalError</Code></Error>", .headers = &.{}, .op = .GetObject, .key = "key", .want = error.S3ServerError, .text = "GET s3://bkt/key: 500 InternalError" },
        .{ .status = 404, .body = "", .headers = &.{}, .op = .HeadObject, .key = "key", .want = error.NoSuchKey, .text = "HEAD s3://bkt/key: 404 (no S3 error code): the object does not exist" },
        .{ .status = 404, .body = "", .headers = &.{}, .op = .ListObjectsV2, .key = "prefix", .want = error.NoSuchBucket, .text = "LIST s3://bkt/prefix: 404 (no S3 error code)" },
        .{ .status = 404, .body = "<Error><Code>NoSuchUpload</Code></Error>", .headers = &.{}, .op = .UploadPart, .key = "key", .want = error.NoSuchUpload, .text = "PUT part s3://bkt/key: 404 NoSuchUpload" },
        .{ .status = 404, .body = "", .headers = &.{}, .op = .UploadPart, .key = "key", .want = error.NoSuchUpload, .text = "PUT part s3://bkt/key: 404 (no S3 error code)" },
        .{ .status = 403, .body = "<Error><Code>AccessDenied</Code></Error>", .headers = &.{}, .op = .UploadPart, .key = "key", .want = error.AccessDenied, .text = "PUT part s3://bkt/key: 403 AccessDenied" },
    };
    for (cases) |case| {
        const r = refusal(case.op, "bkt", case.key, .{ .status = case.status, .headers = case.headers, .body = case.body }, .{
            .access_key = "AKIAIOSFODNN7EXAMPLE",
            .secret_key = "secretKeyValue1234567890",
            .region = "us-east-1",
        });
        try testing.expectEqual(case.want, r.err);
        var buf: [512]u8 = undefined;
        try testing.expectEqualStrings(case.text, try std.fmt.bufPrint(&buf, "{f}", .{r.failure}));
    }
}

test "S3 refusals redact credentials and key IDs from S3's message" {
    const creds: Credentials = .{
        .access_key = "AKIAIOSFODNN7EXAMPLE",
        .secret_key = "secretKeyValue1234567890",
        .session_token = "session-token-value",
        .region = "us-east-1",
    };
    const body = try std.fmt.allocPrint(std.testing.allocator, "<Error><Code>AuthorizationHeaderMalformed</Code><Message>The authorization header is mal-formed; " ++
        "expecting \"{s}/20261114/us-east-1/s3/aws4_request\"; {s}; " ++
        "X-Amz-Credential={s}%2F20261114%2Fus-east-1%2Fs3%2Faws4_request; " ++
        "X-Amz-Signature=deadbeef; X-Amz-Security-Token={s}</Message></Error>", .{ creds.access_key, creds.secret_key, creds.access_key, creds.session_token.? });
    defer std.testing.allocator.free(body);
    const r = refusal(.GetObject, "bkt", "key", .{ .status = 403, .headers = &.{}, .body = body }, creds);
    const message = r.failure.message.get();
    try testing.expect(std.mem.indexOf(u8, message, "AKIAIOSFODNN7EXAMPLE") == null);
    try testing.expect(std.mem.indexOf(u8, message, "secretKeyValue1234567890") == null);
    try testing.expect(std.mem.indexOf(u8, message, "session-token-value") == null);
    try testing.expect(std.mem.indexOf(u8, message, "deadbeef") == null);
    try testing.expect(std.mem.indexOf(u8, message, "[redacted]") != null);
}

test "a GET that never got an answer still names its bucket and key" {
    var buf: [256]u8 = undefined;
    const f = transportFailure(.GetObject, "bkt", "key", error.ConnectTimeout);
    try testing.expectEqualStrings("GET s3://bkt/key: failed while connecting: ConnectTimeout", try std.fmt.bufPrint(&buf, "{f}", .{f}));
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

test "ZPQ_S3_PRECONNECT defaults to 4 and 0 disables it" {
    if (comptime @import("../test_skip.zig").lacks(.posix)) return @import("../test_skip.zig").skipNeeding(.posix, "POSIX environment fixture");
    try testing.expectEqual(@as(usize, 4), preconnectCount(testEnviron(&.{})));
    try testing.expectEqual(@as(usize, 0), preconnectCount(testEnviron(&.{"ZPQ_S3_PRECONNECT=0"})));
    try testing.expectEqual(@as(usize, 2), preconnectCount(testEnviron(&.{"ZPQ_S3_PRECONNECT=2"})));
    try testing.expectEqual(@as(usize, 4), preconnectCount(testEnviron(&.{"ZPQ_S3_PRECONNECT=no"})));
}

test "S3_NO_SIGN_REQUEST yields anonymous credentials instead of NoCredentials" {
    if (comptime @import("../test_skip.zig").lacks(.posix)) return @import("../test_skip.zig").skipNeeding(.posix, "POSIX environment fixture");
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
const test_posix = if (builtin.os.tag == .windows) @import("win_test_socket.zig") else std.posix;
const ScriptedPool = struct {
    pub const capacity: usize = 1;
    pub const Step = union(enum) {
        fail: anyerror,
        live: struct { reused: bool, response: []const u8 },
        dead: struct { reused: bool },
    };
    pub const Slot = struct { conn: tls.Connection, peer: test_posix.fd_t };
    pub const Handle = struct { conn: *tls.Connection, node: *Slot, permit: usize, reused: bool };

    steps: []const Step,
    next: usize = 0,
    fresh_requests: usize = 0,
    stale_retries: usize = 0,
    backoff_retries: usize = 0,
    telemetry: Pool(1).Stats = .{},
    slots: [8]Slot = undefined,

    fn deinit(self: *ScriptedPool) void {
        for (self.slots[0..self.next], self.steps[0..self.next]) |*slot, step| switch (step) {
            .fail => {},
            else => {
                slot.conn.deinit();
                _ = test_posix.system.close(slot.peer);
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
        var fds: [2]test_posix.fd_t = undefined;
        if (test_posix.errno(test_posix.system.socketpair(test_posix.AF.UNIX, test_posix.SOCK.STREAM, 0, &fds)) != .SUCCESS)
            return error.SkipZigTest;
        slot.* = .{ .conn = .{ .fd = fds[0], .tls = undefined, .allocator = testing.allocator, .plain = true }, .peer = fds[1] };
        if (response) |r| {
            _ = test_posix.system.write(fds[1], r.ptr, r.len);
        } else {
            _ = test_posix.system.shutdown(fds[1], test_posix.SHUT.WR);
        }
        return .{ .conn = &slot.conn, .node = slot, .permit = 0, .reused = reused };
    }
    pub fn release(_: *ScriptedPool, _: Io, _: Handle) void {}
    pub fn discard(_: *ScriptedPool, _: Io, _: Handle) void {}
    pub fn noteRetry(self: *ScriptedPool, _: Io, failure: retry.Failure) void {
        switch (failure) {
            .stale => self.stale_retries += 1,
            .transient, .stalled => self.backoff_retries += 1,
            .fatal => {},
        }
    }
    pub fn noteS3(self: *ScriptedPool, _: Io, sample: pool_mod.S3Observation) void {
        self.telemetry.observeS3(sample);
    }
};

const test_creds: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-east-1", .endpoint = "http://127.0.0.1:9" };
const ok_206 = "HTTP/1.1 206 Partial Content\r\nContent-Length: 2\r\nContent-Range: bytes 0-1/2\r\n\r\nok";

test "s3 telemetry counts GET attempts, SlowDown, HEAD bodies and completed-response first bytes" {
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    const body = "<Error><Code>SlowDown</Code></Error>";
    const slow = try std.fmt.allocPrint(a, "HTTP/1.1 503 Service Unavailable\r\nContent-Length: {d}\r\n\r\n{s}", .{ body.len, body });
    var p: ScriptedPool = .{ .steps = &.{
        .{ .live = .{ .reused = true, .response = slow } },
        .{ .live = .{ .reused = true, .response = ok_206 } },
        .{ .live = .{ .reused = true, .response = "HTTP/1.1 200 OK\r\nContent-Length: 999\r\n\r\n" } },
    } };
    defer p.deinit();
    _ = try getViaPool(testing.io, &p, a, test_creds, .{ .bucket = "b", .key = "k" }, Range.span(0, 1));
    const Head = struct {
        fn send(_: void, alloc: std.mem.Allocator, conn: *tls.Connection) !http.Response {
            return http.sendRequest(alloc, conn, .{ .method = .HEAD, .host = "b", .path = "/k", .headers = &.{} });
        }
    };
    _ = try requestViaPool(testing.io, &p, a, .{ .host = "b", .addrs = &.{}, .port = 9 }, .{ .idempotent = true, .method = .HEAD }, {}, Head.send);
    try testing.expectEqual(@as(u64, 2), p.telemetry.get_requests);
    try testing.expectEqual(@as(u64, 1), p.telemetry.head_requests);
    try testing.expectEqual(@as(u64, body.len + 2), p.telemetry.bytes_fetched);
    try testing.expectEqual(@as(u64, 1), p.telemetry.retry_http_503);
    try testing.expectEqual(@as(u64, 1), p.telemetry.slow_down_retries);
    try testing.expect(p.telemetry.ttfb_ns_max > 0 and p.telemetry.ttfb_ns_sum >= p.telemetry.ttfb_ns_max);
}

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
    try testing.expectEqual(@as(u64, 2), p.telemetry.get_requests); // The failed handshake sent no GET.

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

// ------------------------------------------------------------------ stalled connections (tls.Timeouts)

const fake_s3 = @import("fake_s3.zig");

test "S3 Express session shared by concurrent callers and survives cancellation" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    fake.hold_session = true;
    fake.require_session = true;
    defer fake.release_session.store(true, .release);
    try fake.serveBytes("dir/file", "abcdefgh");
    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "base-key",
        .secret_key = "base-secret",
        .region = "us-west-2",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
    };
    const bucket = "test--usw2-az1--x-s3";
    var threaded = Io.Threaded.init(testing.allocator, .{});
    defer threaded.deinit();
    const io = threaded.io();
    const Call = struct {
        fn run(task_io: Io, base: Credentials, b: []const u8) anyerror!void {
            var arena = std.heap.ArenaAllocator.init(testing.allocator);
            defer arena.deinit();
            const s = try sessionCredentialsWithIo(task_io, arena.allocator(), base, b, .read_only);
            try testing.expectEqualStrings("session-key", s.access_key);
        }
    };
    var canceled = try io.concurrent(Call.run, .{ io, creds, bucket });
    while (!fake.session_arrived.load(.acquire)) clock.sleepNs(std.time.ns_per_ms);
    var others: [8]Io.Future(anyerror!void) = undefined;
    for (&others) |*f| f.* = try io.concurrent(Call.run, .{ io, creds, bucket });
    try testing.expectError(error.Canceled, canceled.cancel(io));
    fake.release_session.store(true, .release);
    for (&others) |*f| try f.await(io);

    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();
    var client = try Client.init(a, creds, bucket);
    defer client.deinit();
    const whole = try client.get(a, "dir/file", null);
    try testing.expectEqualStrings("abcdefgh", whole.body);
    const part = try client.get(a, "dir/file", Range.span(2, 4));
    try testing.expectEqualStrings("cde", part.body);
    const head = try client.head(a, "dir/file");
    try testing.expectEqual(@as(u16, 200), head.status);
    try testing.expectEqualStrings("8", head.header("Content-Length").?);
    const entries = try listAll(a, &client, "dir/");
    try testing.expectEqual(@as(usize, 1), entries.len);
    try testing.expectEqualStrings("dir/file", entries[0].key);
    session_mutex.lockUncancelable(session_io);
    var current = sessions.?;
    while (!std.mem.eql(u8, current.bucket, bucket) or current.mode != .read_only) current = current.next.?;
    current.expires = clock.realtimeS() + 30;
    session_mutex.unlock(session_io);
    _ = try sessionCredentials(a, creds, bucket, .read_only);
    session_mutex.lockUncancelable(session_io);
    while (current.refreshing) current.done.waitUncancelable(session_io, &session_mutex);
    session_mutex.unlock(session_io);
    _ = try sessionCredentials(a, creds, bucket, .read_write);
    const response = try put(a, creds, .{ .bucket = bucket, .key = "dir/new" }, "data");
    try testing.expectEqual(@as(u16, 200), response.status);
    fake.lock();
    defer fake.unlock();
    try testing.expectEqual(@as(u32, 3), fake.sessions);
    try testing.expect(!fake.session_bad_signature);
}

test "S3 Express listing sorts keys and rejects a non-directory prefix" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    fake.require_session = true;
    try fake.serveBytes("dir/z", "z");
    try fake.serveBytes("dir/a", "a");
    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-west-2", .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}) };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    var client = try Client.init(arena.allocator(), creds, "test--usw2-az1--x-s3");
    defer client.deinit();
    try testing.expectError(error.DirectoryBucketInvalidPrefix, client.list(arena.allocator(), "dir/prefix", 100, null));
    const entries = try listAll(arena.allocator(), &client, "dir/");
    try testing.expectEqualStrings("dir/a", entries[0].key);
    try testing.expectEqualStrings("dir/z", entries[1].key);
}

test "S3 Express errors redact the session used by the request" {
    const base: Credentials = .{ .access_key = "base-key", .secret_key = "base-secret", .region = "us-west-2" };
    const session: Credentials = .{ .access_key = "session-key", .secret_key = "session-secret", .session_token = "session-token", .region = "us-west-2", .express_session = true };
    const response = withRequestSecrets(.{ .status = 403, .headers = &.{}, .body = "<Error><Code>AccessDenied</Code><Message>session-key session-secret session-token</Message></Error>" }, session);
    const result = refusal(.GetObject, "test--usw2-az1--x-s3", "file", response, base);
    try testing.expectEqual(error.AccessDenied, result.err);
    try testing.expectEqualStrings("[redacted] [redacted] [redacted]", result.failure.message.get());
    try testing.expectEqualStrings("AccessDenied", result.failure.code.get());
}

test "S3 Express unsupported operations retain the AWS error type" {
    const creds: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-west-2" };
    const response: http.Response = .{ .status = 501, .headers = &.{}, .body = "<Error><Code>NotImplemented</Code><Message>not supported</Message></Error>" };
    const result = refusal(.GetObject, "test--usw2-az1--x-s3", "file", response, creds);
    try testing.expectEqual(error.DirectoryBucketOperationUnsupported, result.err);
    try testing.expectEqualStrings("NotImplemented", result.failure.code.get());
    try testing.expectEqualStrings("test--usw2-az1--x-s3", result.failure.bucket.get());
    try testing.expectEqualStrings("file", result.failure.key.get());
}

test "S3 redirects never forward credentials to another host or scheme" {
    var origin: fake_s3.FakeS3 = undefined;
    try origin.start(testing.allocator);
    defer origin.deinit();
    var destination: fake_s3.FakeS3 = undefined;
    try destination.start(testing.allocator);
    defer destination.deinit();
    var endpoint_buf: [64]u8 = undefined;
    var redirect_buf: [128]u8 = undefined;
    const creds: Credentials = .{ .access_key = "base-key", .secret_key = "base-secret", .session_token = "base-token", .region = "us-west-2", .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{origin.port}) };
    var arena = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena.deinit();
    for ([_][]const u8{ "ordinary", "test--usw2-az1--x-s3" }) |bucket| {
        origin.lock();
        origin.require_session = directoryBucketZone(bucket) != null;
        origin.unlock();
        var client = try Client.init(arena.allocator(), creds, bucket);
        defer client.deinit();
        for ([_]u16{ 301, 302, 303, 307, 308 }) |status| {
            origin.lock();
            origin.redirect_status = status;
            origin.redirect_location = try std.fmt.bufPrint(&redirect_buf, "http://localhost:{d}/key", .{destination.port});
            origin.unlock();
            const response = try client.get(arena.allocator(), "key", null);
            try testing.expectEqual(status, response.status);
        }
        origin.lock();
        origin.redirect_status = 307;
        origin.redirect_location = try std.fmt.bufPrint(&redirect_buf, "https://127.0.0.1:{d}/key", .{destination.port});
        origin.unlock();
        const response = try client.get(arena.allocator(), "key", null);
        try testing.expectEqual(@as(u16, 307), response.status);
    }
    try testing.expectEqual(@as(u32, 0), destination.requests_seen.load(.monotonic));
}

/// Per-connection deadlines short enough for tests; production uses `tls.Timeouts` defaults or the environment.
const test_timeouts: tls.Timeouts = .{ .connect_ms = 300, .handshake_ms = 300, .first_byte_ms = 200, .idle_ms = 200 };

const stall_payload: [96 * 1024]u8 = blk: {
    @setEvalBranchQuota(400_000);
    var b: [96 * 1024]u8 = undefined;
    for (&b, 0..) |*x, i| x.* = @truncate(i *% 131 +% 17);
    break :blk b;
};

/// A ranged GET of `stall_payload` from `fake` through a pool of its own; returns the body (on `arena`).
fn fakeGet(fake: *fake_s3.FakeS3, arena: std.mem.Allocator, stats: *Pool(4).Stats, timeouts: tls.Timeouts) ![]const u8 {
    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
        .timeouts = timeouts,
    };
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    defer stats.* = p.snapshotStats();
    const url: Url = .{ .bucket = "bkt", .key = "obj" };
    const resp = try getViaPool(testing.io, &p, arena, creds, url, Range.span(1000, 1000 + 64 * 1024 - 1));
    if (resp.status != 206) return error.TestUnexpectedResult;
    return try arena.dupe(u8, resp.body);
}

test "fake S3 serves a Parquet fixture to the Windows CLI" {
    const a = testing.allocator;
    const windows = @import("builtin").os.tag == .windows;
    const fixture = "ci/fixtures/parquet/nullable_mix.parquet";
    const data = try std.Io.Dir.cwd().readFileAlloc(testing.io, fixture, a, .limited(8 << 20));
    defer a.free(data);
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(a);
    defer fake.deinit();
    try fake.serveBytes("obj", data);
    var endpoint_buf: [64]u8 = undefined;
    const endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port});
    var arena_state = std.heap.ArenaAllocator.init(a);
    defer arena_state.deinit();
    var pool: Pool(4) = undefined;
    try pool.init(a);
    defer pool.deinit();
    const creds: Credentials = .{ .access_key = "ak", .secret_key = "sk", .region = "us-east-1", .endpoint = endpoint };
    const response = try getViaPool(testing.io, &pool, arena_state.allocator(), creds, .{ .bucket = "bkt", .key = "obj" }, Range.span(0, data.len - 1));
    try testing.expectEqual(@as(u16, 206), response.status);
    try testing.expectEqualSlices(u8, data, response.body);

    if (windows) {
        var env: std.process.Environ.Map = .{ .allocator = a, .array_hash_map = .empty };
        defer env.deinit();
        try env.put("SystemRoot", "C:\\Windows");
        try env.put("S3_ENDPOINT_URL", endpoint);
        try env.put("S3_ACCESS_KEY_ID", "ak");
        try env.put("S3_SECRET_ACCESS_KEY", "sk");
        try env.put("S3_REGION", "us-east-1");
        const executable = ".\\zpq.exe";
        const local = try std.process.run(a, testing.io, .{ .argv = &.{ executable, "query", fixture, "-j", "1" }, .environ_map = &env });
        defer a.free(local.stdout);
        defer a.free(local.stderr);
        try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, local.term);
        const remote = try std.process.run(a, testing.io, .{ .argv = &.{ executable, "query", "s3://bkt/obj", "-j", "1" }, .environ_map = &env });
        defer a.free(remote.stdout);
        defer a.free(remote.stderr);
        if (remote.term != .exited or remote.term.exited != 0) std.debug.print("S3 CLI failed: {any} {s}\n", .{ remote.term, remote.stderr });
        try testing.expectEqual(std.process.Child.Term{ .exited = 0 }, remote.term);
        try testing.expectEqualSlices(u8, local.stdout, remote.stdout);
    }
}

test "a ranged GET that stalls before, inside or after its response headers is sent again and gets the exact bytes" {
    for ([_]fake_s3.FakeS3.StallAt{ .before_headers, .mid_headers, .mid_body }) |at| {
        var fake: fake_s3.FakeS3 = undefined;
        try fake.start(testing.allocator);
        defer fake.deinit();
        try fake.serveBytes("obj", &stall_payload);
        fake.stall = "GET /bkt/obj";
        fake.stall_at = at;
        var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
        defer arena_state.deinit();
        var stats: Pool(4).Stats = .{};
        const body = try fakeGet(&fake, arena_state.allocator(), &stats, test_timeouts);
        try testing.expectEqualSlices(u8, stall_payload[1000..][0 .. 64 * 1024], body);
        try testing.expectEqual(@as(u32, 1), fake.stalled);
        // One retry, charged, on a connection opened for it.
        try testing.expectEqual(@as(u64, 1), stats.timeout_retries);
        try testing.expectEqual(@as(u64, 0), stats.stale_retries);
        try testing.expectEqual(@as(u64, 2), stats.opens);
    }
}

test "a response that keeps arriving is not cut off by the idle timeout, however slow in all" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    try fake.serveBytes("obj", &stall_payload);
    fake.stall = "GET /bkt/obj";
    fake.stall_at = .trickle;
    // CI VMs stretch short sleeps; keep gaps far below the idle timeout.
    fake.trickle_ms = 60;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var stats: Pool(4).Stats = .{};
    const t0 = clock.monoNs();
    var timeouts = test_timeouts;
    timeouts.idle_ms = 400;
    const body = try fakeGet(&fake, arena_state.allocator(), &stats, timeouts);
    try testing.expectEqualSlices(u8, stall_payload[1000..][0 .. 64 * 1024], body);
    try testing.expect(clock.monoNs() - t0 >= 440 * std.time.ns_per_ms);
    try testing.expectEqual(@as(u64, 0), stats.timeout_retries);
    try testing.expectEqual(@as(u64, 0), stats.backoff_retries);
}

test "a later named refusal is not hidden by an earlier short body" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    try fake.serveBytes("small", &.{});
    fake.fail = "GET /bkt/bad";
    fake.fails_left = std.math.maxInt(u32);
    fake.fail_status = 403;
    fake.fail_code = "AccessDenied";

    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
        .timeouts = test_timeouts,
    };
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var short: [16]u8 = undefined;
    var refused: [512]u8 = undefined;
    const jobs = [_]FetchJob{
        .{ .bucket = "bkt", .key = "small", .range = .{ .start = 0, .end = 16 }, .target = &short },
        .{ .bucket = "bkt", .key = "bad", .range = .{ .start = 0, .end = 512 }, .target = &refused },
    };
    var diag: diag_mod.Diag = .{};
    try testing.expectError(error.AccessDenied, fetchJobs(testing.io, &p, testing.allocator, arena_state.allocator(), creds, &jobs, 2, null, null, &diag));
    try testing.expect(diag.detail == .s3_request);
    try testing.expectEqualStrings("AccessDenied", diag.detail.s3_request.code.get());
}

test "a data GET that never gets a status names its bucket and key" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    fake.stall = "GET /bkt/obj";
    fake.stall_at = .before_headers;
    fake.stalls_left = std.math.maxInt(u32);

    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
        .timeouts = .{ .connect_ms = 300, .handshake_ms = 300, .first_byte_ms = 50, .idle_ms = 50, .request_ms = 300 },
    };
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var target: [16]u8 = undefined;
    const jobs = [_]FetchJob{.{
        .bucket = "bkt",
        .key = "obj",
        .range = .{ .start = 0, .end = 16 },
        .target = &target,
    }};
    var diag: diag_mod.Diag = .{};
    try testing.expectError(error.FirstByteTimeout, fetchJobs(testing.io, &p, testing.allocator, arena_state.allocator(), creds, &jobs, 1, null, null, &diag));
    try testing.expect(diag.detail == .s3_request);
    try testing.expectEqual(diag_mod.S3Failure.Op.GetObject, diag.detail.s3_request.op);
    try testing.expectEqualStrings("bkt", diag.detail.s3_request.bucket.get());
    try testing.expectEqualStrings("obj", diag.detail.s3_request.key.get());
    try testing.expectEqual(@as(?anyerror, error.FirstByteTimeout), diag.detail.s3_request.err);
}

test "regression: a response that trickles in past its attempt's deadline is cut off and fetched again" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    try fake.serveBytes("obj", &stall_payload);
    fake.stall = "GET /bkt/obj";
    fake.stall_at = .trickle;
    fake.trickle_ms = 60;
    var timeouts = test_timeouts;
    timeouts.idle_ms = 400;
    timeouts.request_ms = 300;
    timeouts.min_rate = 1 << 30;
    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
        .timeouts = timeouts,
    };
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var p: Pool(4) = undefined;
    try p.init(testing.allocator);
    defer p.deinit();
    const url: Url = .{ .bucket = "bkt", .key = "obj" };
    const range = Range.span(1000, 1000 + 64 * 1024 - 1);
    const resp = try getViaPool(testing.io, &p, arena_state.allocator(), creds, url, range);
    try testing.expectEqual(@as(u16, 206), resp.status);
    try testing.expectEqualSlices(u8, stall_payload[1000..][0 .. 64 * 1024], resp.body);
    try testing.expectEqual(@as(u64, 1), p.snapshotStats().timeout_retries);
}

fn sleepMs(ms: u64) void {
    clock.sleepNs(ms * std.time.ns_per_ms);
}

test "ZPQ_S3_FIRST_WAVE selects the cap before the first data header, and 16 is no slow start" {
    try testing.expectEqual(S3_FIRST_WAVE, parseFirstWave(null));
    try testing.expectEqual(@as(usize, 8), parseFirstWave("8"));
    try testing.expectEqual(@as(usize, 4), parseFirstWave("4"));
    try testing.expectEqual(S3_IN_FLIGHT, parseFirstWave("16"));
    try testing.expectEqual(S3_IN_FLIGHT, parseFirstWave("32"));
    try testing.expectEqual(S3_FIRST_WAVE, parseFirstWave("0"));
    try testing.expectEqual(S3_FIRST_WAVE, parseFirstWave("nope"));

    var open: DataWave = .{ .cap = .init(parseFirstWave("16")) };
    try testing.expectEqual(S3_IN_FLIGHT, open.cap.load(.acquire));
    open.noteHeader(testing.io);
    try testing.expectEqual(S3_IN_FLIGHT, open.cap.load(.acquire));

    var slow: DataWave = .{ .cap = .init(parseFirstWave(null)) };
    try testing.expectEqual(S3_FIRST_WAVE, slow.cap.load(.acquire));
    slow.noteHeader(testing.io);
    try testing.expectEqual(S3_IN_FLIGHT, slow.cap.load(.acquire));
}

test "a worker without a data-wave slot sleeps until a release or the first header frees one" {
    const io = testing.io;
    var wave: DataWave = .{ .cap = .init(1), .full = 3 };
    try wave.acquire(io); // the first wave's one slot
    const Waiter = struct {
        fn run(w: *DataWave, got: *std.atomic.Value(u32)) void {
            w.acquire(testing.io) catch return;
            _ = got.fetchAdd(1, .acq_rel);
        }
        fn waitFor(got: *std.atomic.Value(u32), n: u32) !void {
            var spins: usize = 0;
            while (got.load(.acquire) < n) : (spins += 1) {
                if (spins > 5_000) return error.TestTimedOut;
                try testing.io.sleep(.fromMilliseconds(1), .awake);
            }
        }
    };
    var got: std.atomic.Value(u32) = .init(0);
    var threads: [3]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Waiter.run, .{ &wave, &got });
    defer for (threads) |t| t.join();

    try io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectEqual(@as(u32, 0), got.load(.acquire));
    // Waiting does not move the count a release or a header moves: the waiters are asleep, not polling.
    try testing.expectEqual(@as(u32, 0), wave.seq.load(.acquire));

    wave.noteHeader(io); // full cap 3, one held: two of the three get in
    try Waiter.waitFor(&got, 2);
    try io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectEqual(@as(u32, 2), got.load(.acquire));
    wave.noteHeader(io); // a later header changes nothing and wakes no one
    try testing.expectEqual(@as(u32, 1), wave.seq.load(.acquire));

    wave.release(io); // the first slot comes back: the last waiter takes it
    try Waiter.waitFor(&got, 3);
    try testing.expectEqual(@as(u32, 2), wave.seq.load(.acquire));
    try testing.expectEqual(@as(usize, 3), wave.in_flight.load(.acquire));
}

test "the first data header wakes every worker waiting for a data-wave slot" {
    const io = testing.io;
    var wave: DataWave = .{ .cap = .init(1), .full = 7 };
    try wave.acquire(io);
    const Waiter = struct {
        fn run(w: *DataWave, got: *std.atomic.Value(u32)) void {
            w.acquire(testing.io) catch return;
            _ = got.fetchAdd(1, .acq_rel);
        }
    };
    var got: std.atomic.Value(u32) = .init(0);
    var threads: [6]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Waiter.run, .{ &wave, &got });
    defer for (threads) |t| t.join();
    try io.sleep(.fromMilliseconds(20), .awake);
    try testing.expectEqual(@as(u32, 0), got.load(.acquire));
    wave.noteHeader(io);
    var spins: usize = 0;
    while (got.load(.acquire) < threads.len) : (spins += 1) {
        if (spins > 5_000) return error.TestTimedOut;
        try io.sleep(.fromMilliseconds(1), .awake);
    }
    try testing.expectEqual(@as(usize, 7), wave.in_flight.load(.acquire));
}

test "data-wave slots lose no wakeup when releases race acquires" {
    const io = testing.io;
    const cap = 2;
    var wave: DataWave = .{ .cap = .init(cap), .full = cap };
    const Worker = struct {
        fn run(w: *DataWave, held: *std.atomic.Value(usize), peak: *std.atomic.Value(usize), done: *std.atomic.Value(u32)) void {
            for (0..2_000) |_| {
                w.acquire(testing.io) catch return;
                const now = held.fetchAdd(1, .acq_rel) + 1;
                _ = peak.fetchMax(now, .acq_rel);
                _ = held.fetchSub(1, .acq_rel);
                w.release(testing.io);
            }
            _ = done.fetchAdd(1, .acq_rel);
        }
    };
    var held: std.atomic.Value(usize) = .init(0);
    var peak: std.atomic.Value(usize) = .init(0);
    var done: std.atomic.Value(u32) = .init(0);
    var threads: [8]std.Thread = undefined;
    for (&threads) |*t| t.* = try std.Thread.spawn(.{}, Worker.run, .{ &wave, &held, &peak, &done });
    // A lost wakeup leaves a worker asleep with a slot free: the workers would never all finish.
    var spins: usize = 0;
    const finished = while (done.load(.acquire) < threads.len) : (spins += 1) {
        if (spins > 30_000) break false;
        try io.sleep(.fromMilliseconds(1), .awake);
    } else true;
    if (!finished) {
        // Let any stranded worker out before failing, so the threads can be joined.
        wave.full = 1 << 20;
        wave.noteHeader(io);
    }
    for (threads) |t| t.join();
    try testing.expect(finished);
    try testing.expect(peak.load(.acquire) <= cap);
    try testing.expectEqual(@as(usize, 0), wave.in_flight.load(.acquire));
}

test "the first data wave keeps at most 8 data-range requests in flight until the first data-range response header" {
    // Footer, metadata and Bloom fetches pass a null DataWave. They do not take a slot and do not open this gate.
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    const blob = "0123456789abcdefghij";
    try fake.serveBytes("obj", blob);
    fake.hold_gets = true;

    var endpoint_buf: [64]u8 = undefined;
    const creds: Credentials = .{
        .access_key = "ak",
        .secret_key = "sk",
        .region = "us-east-1",
        .endpoint = try std.fmt.bufPrint(&endpoint_buf, "http://127.0.0.1:{d}", .{fake.port}),
        .timeouts = .{ .connect_ms = 2_000, .handshake_ms = 2_000, .first_byte_ms = 5_000, .idle_ms = 5_000 },
    };
    var pool: Pool(16) = undefined;
    try pool.init(testing.allocator);
    defer pool.deinit();
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var targets: [20][1]u8 = undefined;
    var jobs: [20]FetchJob = undefined;
    for (&jobs, 0..) |*j, i| {
        const n: u64 = @intCast(i);
        j.* = .{
            .bucket = "bkt",
            .key = "obj",
            .range = .{ .start = n, .end = n + 1 },
            .target = &targets[i],
        };
    }
    // A full cap past the first wave, whatever this target's default, so the gate can be seen to open.
    var wave: DataWave = .{ .full = 16 };
    var threaded = std.Io.Threaded.init(testing.allocator, .{ .concurrent_limit = .limited(16) });
    defer threaded.deinit();

    const Box = struct {
        io: Io,
        pool: *Pool(16),
        arena: std.mem.Allocator,
        creds: Credentials,
        jobs: []FetchJob,
        wave: *DataWave,
        err: ?anyerror = null,

        fn run(self: *@This()) void {
            _ = fetchJobs(self.io, self.pool, testing.allocator, self.arena, self.creds, self.jobs, 16, null, self.wave, null) catch |e| {
                self.err = e;
            };
        }
    };
    var box: Box = .{
        .io = threaded.io(),
        .pool = &pool,
        .arena = arena_state.allocator(),
        .creds = creds,
        .jobs = &jobs,
        .wave = &wave,
    };
    const worker = try std.Thread.spawn(.{}, Box.run, .{&box});

    var spins: usize = 0;
    while (fake.gets_waiting.load(.acquire) < 8) {
        sleepMs(5);
        spins += 1;
        if (spins > 400) return error.TestUnexpectedResult;
    }
    sleepMs(50);
    const before = fake.gets_waiting.load(.acquire);
    const before_max = fake.max_waiting.load(.acquire);
    try testing.expectEqual(@as(u32, 8), before);
    try testing.expectEqual(@as(u32, 8), before_max);

    // Forget the pre-header peak so the post-header peak is only what the open gate allowed.
    fake.max_waiting.store(0, .release);
    fake.release_one_get.store(true, .release);
    spins = 0;
    while (fake.max_waiting.load(.acquire) <= 8) {
        sleepMs(5);
        spins += 1;
        if (spins > 400) return error.TestUnexpectedResult;
    }
    sleepMs(50);
    const after = fake.gets_waiting.load(.acquire);
    const after_max = fake.max_waiting.load(.acquire);
    try testing.expect(after > 8);
    try testing.expect(after <= 16);
    try testing.expect(after_max <= 16);
    try testing.expect(after_max > 8);

    fake.release_gets.store(true, .release);
    worker.join();
    if (box.err) |e| return e;
}

test "a server that always stalls fails the request with the timeout once the retry budget is spent" {
    var fake: fake_s3.FakeS3 = undefined;
    try fake.start(testing.allocator);
    defer fake.deinit();
    try fake.serveBytes("obj", &stall_payload);
    fake.stall = "GET /bkt/obj";
    fake.stall_at = .mid_body;
    fake.stalls_left = 1000;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    var stats: Pool(4).Stats = .{};
    try testing.expectError(error.ReadTimeout, fakeGet(&fake, arena_state.allocator(), &stats, test_timeouts));
    try testing.expectEqual(@as(u32, retry.max_attempts), fake.stalled);
    try testing.expectEqual(@as(u64, retry.max_attempts - 1), stats.timeout_retries);
}

test "S3_IN_FLIGHT is 8 on aarch64 and 16 on x86_64" {
    try testing.expectEqual(@as(usize, if (builtin.target.cpu.arch == .aarch64) 8 else 16), S3_IN_FLIGHT);
    try testing.expect(S3_FIRST_WAVE <= S3_IN_FLIGHT);
}

test "inFlight: S3_IN_FLIGHT on Lambda, the pool's size elsewhere, ZPQ_S3_IN_FLIGHT within it" {
    if (comptime @import("../test_skip.zig").lacks(.posix)) return @import("../test_skip.zig").skipNeeding(.posix, "POSIX environment fixture");
    try testing.expectEqual(S3_IN_FLIGHT, inFlight(testEnviron(&.{"AWS_LAMBDA_FUNCTION_NAME=f"})));
    try testing.expectEqual(S3_IN_FLIGHT_MAX, inFlight(testEnviron(&.{"AWS_REGION=us-east-1"})));
    try testing.expectEqual(@as(usize, 32), inFlight(testEnviron(&.{ "AWS_LAMBDA_FUNCTION_NAME=f", "ZPQ_S3_IN_FLIGHT=32" })));
    try testing.expectEqual(S3_IN_FLIGHT_MAX, inFlight(testEnviron(&.{"ZPQ_S3_IN_FLIGHT=1000"})));
    try testing.expectEqual(S3_IN_FLIGHT_MAX, inFlight(testEnviron(&.{"ZPQ_S3_IN_FLIGHT=0"})));
    try testing.expectEqual(S3_IN_FLIGHT, inFlight(null));
}
