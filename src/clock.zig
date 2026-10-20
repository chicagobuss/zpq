//! Portable clock reads for leaf code that has no `Io` handle.
//!
//! Goes through `std.posix.system.clock_gettime` (libc when linked,
//! the vDSO-backed Linux syscall otherwise) with std's own
//! `Io.Clock` -> clockid mapping, so `.awake` is CLOCK_MONOTONIC on
//! Linux and CLOCK_UPTIME_RAW on macOS, and `.boot` is CLOCK_BOOTTIME on
//! Linux.

const std = @import("std");
const builtin = @import("builtin");
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

/// Nanoseconds since boot, counting time the system spends suspended: CLOCK_BOOTTIME on Linux. For ages that must
/// include time a frozen Lambda sandbox spends between invocations, which CLOCK_MONOTONIC is not specified to count.
pub fn bootNs() i64 {
    const ts = read(.boot);
    return @as(i64, @intCast(ts.sec)) * std.time.ns_per_s + @as(i64, @intCast(ts.nsec));
}

/// Wall-clock milliseconds since the Unix epoch.
pub fn realtimeMs() i64 {
    const ts = read(.real);
    return @as(i64, @intCast(ts.sec)) * std.time.ms_per_s + @divTrunc(@as(i64, @intCast(ts.nsec)), std.time.ns_per_ms);
}

/// Wall-clock seconds since the Unix epoch.
pub fn realtimeS() i64 {
    return @intCast(read(.real).sec);
}

/// Sleep for `ns` nanoseconds, or until a signal cuts it short. The raw syscall on Linux; libc elsewhere, where a
/// raw Linux syscall is not one.
pub fn sleepNs(ns: u64) void {
    if (builtin.os.tag == .linux) {
        var ts: std.os.linux.timespec = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
        _ = std.os.linux.nanosleep(&ts, null);
    } else {
        var ts: std.c.timespec = .{ .sec = @intCast(ns / std.time.ns_per_s), .nsec = @intCast(ns % std.time.ns_per_s) };
        _ = std.c.nanosleep(&ts, null);
    }
}

test "monoNs and bootNs are non-decreasing and realtimeS is past 2020" {
    const a = monoNs();
    const b = monoNs();
    try std.testing.expect(a > 0);
    try std.testing.expect(b >= a);
    const c = bootNs();
    const d = bootNs();
    try std.testing.expect(c > 0);
    try std.testing.expect(d >= c);
    try std.testing.expect(realtimeS() > 1_577_836_800);
}

test "sleepNs sleeps at least as long as asked" {
    const a = monoNs();
    sleepNs(2 * std.time.ns_per_ms);
    try std.testing.expect(monoNs() - a >= 2 * std.time.ns_per_ms);
}
