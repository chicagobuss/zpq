//! Portable clock reads for leaf code that has no `Io` handle.
//!
//! Goes through `std.posix.system.clock_gettime` (libc when linked,
//! the vDSO-backed Linux syscall otherwise) with std's own
//! `Io.Clock` -> clockid mapping, so `.awake` is CLOCK_MONOTONIC on
//! Linux and CLOCK_UPTIME_RAW on macOS.

const std = @import("std");
const posix = std.posix;

fn read(clock: std.Io.Clock) posix.timespec {
    var ts: posix.timespec = .{ .sec = 0, .nsec = 0 };
    _ = posix.system.clock_gettime(std.Io.Threaded.clockToPosix(clock), &ts);
    return ts;
}

/// Monotonic nanoseconds since an unspecified point. Elapsed-time
/// measurement only.
pub fn monoNs() i64 {
    const ts = read(.awake);
    return @as(i64, @intCast(ts.sec)) * std.time.ns_per_s + @as(i64, @intCast(ts.nsec));
}

/// Wall-clock seconds since the Unix epoch.
pub fn realtimeS() i64 {
    return @intCast(read(.real).sec);
}

test "monoNs is non-decreasing and realtimeS is past 2020" {
    const a = monoNs();
    const b = monoNs();
    try std.testing.expect(a > 0);
    try std.testing.expect(b >= a);
    try std.testing.expect(realtimeS() > 1_577_836_800);
}
