const std = @import("std");
const s3_window = @import("../io/s3_window.zig");
const system = @import("../core/system.zig");

// Large Lambda scans should overlap fetching and decoding instead of retaining a serial decode tail.
pub const lambda_window_cap: u64 = 128 << 20;

/// A query's S3 fetch window: `ZPQ_S3_WINDOW_KIB` when set, else budget-derived and capped on Lambda.
pub fn s3FetchWindow(env: ?std.process.Environ, max_memory: usize) u64 {
    if (env) |e| if (e.getPosix("ZPQ_S3_WINDOW_KIB")) |v| if (std.fmt.parseInt(u64, v, 10)) |k| {
        return @max(1, k) << 10;
    } else |_| {};
    const derived = s3_window.windowFor(max_memory);
    return if (system.onLambda(env)) @min(derived, lambda_window_cap) else derived;
}

test "S3 fetch window caps only the Lambda default and preserves explicit overrides" {
    try std.testing.expectEqual(@as(u64, 128 << 20), lambda_window_cap);
    const lambda = [_:null]?[*:0]const u8{"AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008"};
    const override = [_:null]?[*:0]const u8{
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008", "ZPQ_S3_WINDOW_KIB=262144",
    };
    const small = [_:null]?[*:0]const u8{
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008", "ZPQ_S3_WINDOW_KIB=0",
    };
    const invalid = [_:null]?[*:0]const u8{
        "AWS_LAMBDA_FUNCTION_MEMORY_SIZE=3008", "ZPQ_S3_WINDOW_KIB=invalid",
    };
    try std.testing.expectEqual(lambda_window_cap, s3FetchWindow(.{ .block = .{ .slice = &lambda } }, 2 << 30));
    try std.testing.expectEqual(@as(u64, 64 << 20), s3FetchWindow(.{ .block = .{ .slice = &lambda } }, 256 << 20));
    try std.testing.expectEqual(@as(u64, 256 << 20), s3FetchWindow(.{ .block = .{ .slice = &override } }, 2 << 30));
    try std.testing.expectEqual(@as(u64, 1024), s3FetchWindow(.{ .block = .{ .slice = &small } }, 2 << 30));
    try std.testing.expectEqual(lambda_window_cap, s3FetchWindow(.{ .block = .{ .slice = &invalid } }, 2 << 30));
    try std.testing.expectEqual(@as(u64, 512 << 20), s3FetchWindow(.empty, 2 << 30));
    try std.testing.expectEqual(@as(u64, 512 << 20), s3FetchWindow(null, 2 << 30));
}


/// Bytes of the object tail one suffix GET asks for. The footer, a page index and a Bloom run written at the end of
/// the file usually fit, so cold open is one round trip. `ZPQ_S3_FOOTER_PROBE` overrides it, in bytes, but never
/// above the budget-derived maximum from `footerProbeFor`: `--max-memory` wins, and a larger override is clamped
/// with a one-line warning. The trailing PAR1 is always checked. The leading PAR1 is not a request of its own: it is
/// checked when a fetched range covers offset 0, including a footer-prefix GET that starts there. A footer that does
/// not fit is one more GET for the missing prefix, and never a third.
pub const FOOTER_PROBE_DEFAULT: u64 = 1 << 20;
const FOOTER_PROBE_MIN: u64 = 64 << 10;
const FOOTER_PROBE_MAX: u64 = 16 << 20;

pub fn footerProbeBytes(env: std.process.Environ) u64 {
    const v = env.getPosix("ZPQ_S3_FOOTER_PROBE") orelse return FOOTER_PROBE_DEFAULT;
    const n = std.fmt.parseInt(u64, v, 10) catch return FOOTER_PROBE_DEFAULT;
    if (n < 8) return 8;
    return @min(n, FOOTER_PROBE_MAX);
}

/// Probe for this budget: `clamp(budget / (2 × concurrent footer fetches), 64 KiB, 1 MiB)`. The 2 counts the response
/// buffer and the copy that may be retained. An explicit `ZPQ_S3_FOOTER_PROBE` replaces the default only when it is
/// within that maximum. A larger override is clamped to it, and a warning names both sizes.
pub fn footerProbeFor(env: std.process.Environ, budget: u64, concurrent: usize) u64 {
    const n: u64 = @max(concurrent, 1);
    const derived = std.math.clamp(budget / (2 * n), FOOTER_PROBE_MIN, FOOTER_PROBE_DEFAULT);
    if (env.getPosix("ZPQ_S3_FOOTER_PROBE") == null) return derived;
    const requested = footerProbeBytes(env);
    if (requested > derived) {
        std.log.warn("ZPQ_S3_FOOTER_PROBE={d} exceeds the memory budget; using {d} bytes", .{ requested, derived });
        return derived;
    }
    return requested;
}


/// In-flight footer responses and retained suffixes, against one resident budget. A charge that does not fit is
/// refused: the caller drops the suffix and fetches those ranges later.
pub const ByteAccount = struct {
    limit: u64,
    used: std.atomic.Value(u64) = .init(0),
    /// Highest value `used` reached. In-flight charges are unconditional, so this can sit above `limit` by the
    /// responses already in hand when a retain is refused.
    peak: std.atomic.Value(u64) = .init(0),

    fn note(self: *ByteAccount, now: u64) void {
        var seen = self.peak.load(.monotonic);
        while (now > seen) {
            if (self.peak.cmpxchgWeak(seen, now, .monotonic, .monotonic) == null) break;
            seen = self.peak.load(.monotonic);
        }
    }

    pub fn add(self: *ByteAccount, n: u64) void {
        if (n == 0) return;
        const now = self.used.fetchAdd(n, .monotonic) + n;
        self.note(now);
    }

    pub fn sub(self: *ByteAccount, n: u64) void {
        if (n == 0) return;
        _ = self.used.fetchSub(n, .monotonic);
    }

    pub fn tryAdd(self: *ByteAccount, n: u64) bool {
        if (n == 0) return true;
        while (true) {
            const cur = self.used.load(.monotonic);
            const sum = std.math.add(u64, cur, n) catch return false;
            if (sum > self.limit) return false;
            if (self.used.cmpxchgWeak(cur, sum, .acq_rel, .monotonic) == null) {
                self.note(sum);
                return true;
            }
        }
    }
};


/// Where a footer suffix of `body_len` bytes starts in an object of `total` bytes. The total is the server's word
/// (Content-Range); one smaller than the suffix it came with is a bad response.
pub fn suffixStart(total: u64, body_len: usize) error{BadResponse}!u64 {
    if (body_len > total) return error.BadResponse;
    return total - body_len;
}

pub fn parseTotalFromContentRange(hdr: ?[]const u8) !u64 {
    const v = hdr orelse return error.BadResponse;
    // Format: "bytes start-end/total"
    const slash = std.mem.indexOfScalar(u8, v, '/') orelse return error.BadResponse;
    return std.fmt.parseInt(u64, std.mem.trim(u8, v[slash + 1 ..], " \t"), 10) catch error.BadResponse;
}

test "a footer suffix whose Content-Range total is smaller than its body is a bad response, not an underflow" {
    try std.testing.expectError(error.BadResponse, suffixStart(50, 100));
    try std.testing.expectEqual(@as(u64, 0), try suffixStart(100, 100));
    try std.testing.expectEqual(@as(u64, 900), try suffixStart(1000, 100));
}


test "footer probe defaults to 1 MiB and ZPQ_S3_FOOTER_PROBE overrides it" {
    try std.testing.expectEqual(FOOTER_PROBE_DEFAULT, footerProbeBytes(.empty));
    const set = [_:null]?[*:0]const u8{"ZPQ_S3_FOOTER_PROBE=65536"};
    try std.testing.expectEqual(@as(u64, 65536), footerProbeBytes(.{ .block = .{ .slice = &set } }));
    const bad = [_:null]?[*:0]const u8{"ZPQ_S3_FOOTER_PROBE=nope"};
    try std.testing.expectEqual(FOOTER_PROBE_DEFAULT, footerProbeBytes(.{ .block = .{ .slice = &bad } }));
    const tiny = [_:null]?[*:0]const u8{"ZPQ_S3_FOOTER_PROBE=1"};
    try std.testing.expectEqual(@as(u64, 8), footerProbeBytes(.{ .block = .{ .slice = &tiny } }));
    const huge = [_:null]?[*:0]const u8{"ZPQ_S3_FOOTER_PROBE=999999999999"};
    try std.testing.expectEqual(FOOTER_PROBE_MAX, footerProbeBytes(.{ .block = .{ .slice = &huge } }));
    // No override: clamp(budget / (2 × concurrent), 64 KiB, 1 MiB).
    try std.testing.expectEqual(FOOTER_PROBE_DEFAULT, footerProbeFor(.empty, 16 << 20, 8));
    try std.testing.expectEqual(@as(u64, 512 << 10), footerProbeFor(.empty, 8 << 20, 8));
    try std.testing.expectEqual(FOOTER_PROBE_MIN, footerProbeFor(.empty, 1 << 20, 8));
    // Under the budget-derived maximum, the override is kept. A smaller pin (8 bytes) is kept too.
    try std.testing.expectEqual(@as(u64, 65536), footerProbeFor(.{ .block = .{ .slice = &set } }, 16 << 20, 8));
    try std.testing.expectEqual(@as(u64, 8), footerProbeFor(.{ .block = .{ .slice = &tiny } }, 16 << 20, 8));
    // 16 MiB explicit, 8 MiB budget, 8 fetches: derived is 512 KiB. The override cannot exceed it.
    const over = [_:null]?[*:0]const u8{"ZPQ_S3_FOOTER_PROBE=16777216"};
    try std.testing.expectEqual(@as(u64, 512 << 10), footerProbeFor(.{ .block = .{ .slice = &over } }, 8 << 20, 8));
}

