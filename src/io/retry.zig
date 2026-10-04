//! Retry policy for S3-class object stores.
//!
//! Two things live here, both pure:
//!   * which HTTP status codes are worth retrying (throttle / transient
//!     server classes — S3 says try again, so we do);
//!   * how long to wait before the next attempt: exponential backoff
//!     with *full jitter* (sleep = random(0, min(cap, base << attempt))),
//!     the AWS-recommended variant — it decorrelates a fleet of workers
//!     hammering the same prefix far better than equal or no jitter.
//!
//! It also classifies a failed attempt (`classify`) and keeps one
//! request's retry state (`Attempts`). A pooled keep-alive connection
//! the server dropped while it sat idle is not a server problem: the
//! request is retried at once on a new connection, off the budget, and
//! only server-side errors and failures on fresh connections back off.

const std = @import("std");
const clock = @import("../clock.zig");
const Io = std.Io;

/// Attempts one request gets, counting only those charged to the budget (see `Failure`). With the default policy the
/// backoff before the last one totals at most about 7 s, enough to ride out S3's 503 SlowDown.
pub const max_attempts: u8 = 9;

/// Pacing: how long attempt N waits.
pub const Policy = struct {
    base_ms: u64 = 50,
    cap_ms: u64 = 2_000,
};

pub const default_policy: Policy = .{};

/// Transient/throttle statuses where S3-compatible stores want a retry:
/// 408 (request timeout), 429 (rate limit — R2/GCS dialect), and the
/// 5xx server class (S3 SlowDown arrives as 503; internal errors as
/// 500; bad gateways from fronting proxies as 502/504). Everything
/// else — including 3xx and 4xx like 304/403/404 — is a real answer
/// the caller must see.
pub fn retryableStatus(status: u16) bool {
    return status == 408 or status == 429 or (status >= 500 and status <= 599);
}

/// What a failed attempt says about the next one.
pub const Failure = enum {
    /// A connection reused from the pool turned out dead before the request reached a live server: the send failed,
    /// or the peer closed or reset it before the first response byte. S3 and the network path drop idle keep-alive
    /// connections (every one of them, after a Lambda sandbox sits frozen for a few seconds), so this says nothing
    /// about the server. Only for an idempotent request (a GET, a HEAD, a PUT of a whole object or part), which the
    /// server may already have applied: retry at once on a new connection, without backoff and without charging the
    /// budget.
    stale,
    /// A throttle or server error (408, 429, 5xx), a failure on a fresh connection, or one after the response had
    /// started: back off and charge the budget.
    transient,
    /// Anything else; not worth repeating.
    fatal,
};

pub const Request = struct {
    /// Sending the request twice has the same effect as once. A request that is not idempotent is never re-sent once
    /// it may have reached the server: only a failure to connect, or a status asking for a retry, repeats it.
    idempotent: bool,
    /// Its connection came from the pool's idle list rather than being opened for it.
    reused: bool,
};

/// Classify a failed attempt by its error and its request. `error.RetryableStatus` stands for a response with a
/// `retryableStatus` code: S3's contract is that the request was not applied and may be repeated.
pub fn classify(err: anyerror, req: Request) Failure {
    return switch (err) {
        error.RetryableStatus => .transient,
        error.SendFailed, error.ClosedBeforeResponse => if (!req.idempotent) .fatal else if (req.reused) .stale else .transient,
        error.RecvFailed, error.BodyTruncated, error.BadStatusLine => if (req.idempotent) .transient else .fatal,
        else => .fatal,
    };
}

/// Classify a failure to obtain a connection: resolving, connecting, or the TLS handshake (a ClientHello that cannot be
/// written surfaces as `error.SendFailed`). No request has been sent yet, so these back off like any failure on a fresh
/// connection; only local trouble (cancelation, memory, a closed pool) is fatal.
pub fn classifyConnect(err: anyerror) Failure {
    return switch (err) {
        error.DnsFailed, error.SocketFailed, error.ConnectFailed, error.HandshakeFailed, error.SendFailed, error.RecvFailed => .transient,
        else => .fatal,
    };
}

/// Retry state for one request over pooled connections.
pub const Attempts = struct {
    policy: Policy = default_policy,
    /// Failed attempts charged to the budget so far.
    charged: u8 = 0,
    /// The next attempt must open a new connection: the last one failed on a stale pooled connection, and the others
    /// in the pool have probably been idle as long.
    fresh: bool = false,

    /// Account for a failed attempt, sleeping its backoff when it is charged. False when the request should give up.
    /// A stale failure forces a fresh connection next, so it is always followed by a chargeable attempt.
    pub fn retryAfter(self: *Attempts, io: Io, failure: Failure) Io.Cancelable!bool {
        switch (failure) {
            .fatal => return false,
            .stale => {
                self.fresh = true;
                return true;
            },
            .transient => {
                self.fresh = false;
                self.charged += 1;
                if (self.charged >= max_attempts) return false;
                try sleepBackoff(io, self.policy, self.charged - 1);
                return true;
            },
        }
    }
};

/// Backoff for the attempt that just failed (0-based): a uniformly
/// random duration in [0, min(cap, base << attempt)]. Seeded from the
/// monotonic clock per call — workers landing in the same millisecond
/// still diverge by ns-resolution seed, which is all the decorrelation
/// full jitter needs. No shared PRNG, no locks.
pub fn backoffMs(policy: Policy, attempt: u8) u64 {
    const shift: u6 = @intCast(@min(attempt, 16));
    const ceiling = @min(policy.cap_ms, policy.base_ms << shift);
    if (ceiling == 0) return 0;
    var prng = std.Random.DefaultPrng.init(@bitCast(clock.monoNs()));
    return prng.random().uintAtMost(u64, ceiling);
}

/// Sleep the full-jitter backoff for `attempt` on the supplied event
/// loop. Cancelable: a group cancellation during the sleep propagates
/// instead of stalling shutdown.
pub fn sleepBackoff(io: Io, policy: Policy, attempt: u8) Io.Cancelable!void {
    const ms = backoffMs(policy, attempt);
    if (ms == 0) return;
    try io.sleep(.fromMilliseconds(@intCast(ms)), .awake);
}

test "retryableStatus classifies the S3 retry classes" {
    const t = std.testing;
    for ([_]u16{ 408, 429, 500, 502, 503, 504, 599 }) |s| try t.expect(retryableStatus(s));
    for ([_]u16{ 200, 206, 301, 304, 400, 403, 404, 412 }) |s| try t.expect(!retryableStatus(s));
}

test "backoffMs stays within the jitter ceiling and respects the cap" {
    const t = std.testing;
    const p: Policy = .{ .base_ms = 50, .cap_ms = 2_000 };
    var attempt: u8 = 0;
    while (attempt < 12) : (attempt += 1) {
        const ceiling = @min(p.cap_ms, p.base_ms << @as(u6, @intCast(@min(attempt, 16))));
        var i: usize = 0;
        while (i < 32) : (i += 1) {
            try t.expect(backoffMs(p, attempt) <= ceiling);
        }
    }
    // Large attempt counts must not overflow the shift.
    try t.expect(backoffMs(p, 255) <= p.cap_ms);
}

test "backoffMs ceiling growth is monotone up to the cap" {
    const t = std.testing;
    const p: Policy = .{ .base_ms = 50, .cap_ms = 2_000 };
    // ceilings: 50, 100, 200, 400, 800, 1600, 2000, 2000, ...
    try t.expectEqual(@as(u64, 50), @min(p.cap_ms, p.base_ms << 0));
    try t.expectEqual(@as(u64, 1600), @min(p.cap_ms, p.base_ms << 5));
    try t.expectEqual(@as(u64, 2000), @min(p.cap_ms, p.base_ms << 6));
}

test "classify retries a dead pooled connection for free and backs off for everything server-side" {
    const t = std.testing;
    const reused: Request = .{ .idempotent = true, .reused = true };
    const fresh: Request = .{ .idempotent = true, .reused = false };
    // Nothing reached a live server on a reused connection: stale.
    try t.expectEqual(Failure.stale, classify(error.SendFailed, reused));
    try t.expectEqual(Failure.stale, classify(error.ClosedBeforeResponse, reused));
    // The same on a connection just opened is a real failure.
    try t.expectEqual(Failure.transient, classify(error.SendFailed, fresh));
    try t.expectEqual(Failure.transient, classify(error.ClosedBeforeResponse, fresh));
    // Once the response has started, or the server answered 408/429/5xx, it is never stale.
    for ([_]Request{ reused, fresh }) |req| {
        try t.expectEqual(Failure.transient, classify(error.RecvFailed, req));
        try t.expectEqual(Failure.transient, classify(error.BodyTruncated, req));
        try t.expectEqual(Failure.transient, classify(error.BadStatusLine, req));
        try t.expectEqual(Failure.transient, classify(error.RetryableStatus, req));
        try t.expectEqual(Failure.fatal, classify(error.BadResponse, req));
        try t.expectEqual(Failure.fatal, classify(error.OutOfMemory, req));
    }
    // Failing to get a connection at all is an ordinary backoff, never stale, and local trouble is fatal.
    for ([_]anyerror{ error.ConnectFailed, error.HandshakeFailed, error.SendFailed, error.DnsFailed }) |err|
        try t.expectEqual(Failure.transient, classifyConnect(err));
    for ([_]anyerror{ error.OutOfMemory, error.Canceled, error.QueueClosed }) |err|
        try t.expectEqual(Failure.fatal, classifyConnect(err));
}

test "classify never repeats a request that is not idempotent once it may have reached the server" {
    const t = std.testing;
    for ([_]bool{ true, false }) |reused| {
        const post: Request = .{ .idempotent = false, .reused = reused };
        for ([_]anyerror{ error.SendFailed, error.ClosedBeforeResponse, error.RecvFailed, error.BodyTruncated, error.BadStatusLine }) |err|
            try t.expectEqual(Failure.fatal, classify(err, post));
        // The server said it did not apply it.
        try t.expectEqual(Failure.transient, classify(error.RetryableStatus, post));
    }
}

test "Attempts charges only transient failures and forces a fresh connection after a stale one" {
    const t = std.testing;
    const io = t.io;
    var a: Attempts = .{ .policy = .{ .base_ms = 0, .cap_ms = 0 } };

    // Every pooled connection dead: each stale failure is free and asks for a new connection.
    for (0..3 * max_attempts) |_| {
        try t.expect(try a.retryAfter(io, .stale));
        try t.expect(a.fresh);
        try t.expectEqual(@as(u8, 0), a.charged);
    }
    // A failure on that new connection is charged and lets the next attempt reuse again.
    try t.expect(try a.retryAfter(io, .transient));
    try t.expect(!a.fresh);
    try t.expectEqual(@as(u8, 1), a.charged);
    // The budget runs out after `max_attempts` charged failures.
    var retried: u8 = 1;
    while (try a.retryAfter(io, .transient)) retried += 1;
    try t.expectEqual(max_attempts, a.charged);
    try t.expectEqual(max_attempts - 1, retried);
    var fresh_start: Attempts = .{};
    try t.expect(!try fresh_start.retryAfter(io, .fatal));
}
