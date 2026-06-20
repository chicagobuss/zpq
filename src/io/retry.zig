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
//! Connection-level transient errors (RecvFailed et al.) keep their
//! existing retry-on-fresh-connection semantics in the callers; this
//! module adds the *pacing* and the *status* classification.

const std = @import("std");
const Io = std.Io;

/// Pacing only — attempt *budgets* stay with the call sites, whose
/// loop bounds encode a different rationale (burn through every stale
/// pool slot once). This just decides how long attempt N waits.
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

/// Backoff for the attempt that just failed (0-based): a uniformly
/// random duration in [0, min(cap, base << attempt)]. Seeded from the
/// monotonic clock per call — workers landing in the same millisecond
/// still diverge by ns-resolution seed, which is all the decorrelation
/// full jitter needs. No shared PRNG, no locks.
pub fn backoffMs(policy: Policy, attempt: u8) u64 {
    const shift: u6 = @intCast(@min(attempt, 16));
    const ceiling = @min(policy.cap_ms, policy.base_ms << shift);
    if (ceiling == 0) return 0;
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    var prng = std.Random.DefaultPrng.init(@bitCast(@as(i64, ts.nsec) ^ (@as(i64, ts.sec) << 20)));
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
