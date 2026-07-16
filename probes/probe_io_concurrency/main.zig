//! Spike: how should zpq's CPU fan-out (row-group re-encode) be structured on
//! Zig 0.16's std.Io, and does the parallelism actually stay bounded?
//!
//! Simulates N independent CPU-bound "encode a row group" tasks and runs them
//! under four styles, measuring for each: wall time, PEAK observed concurrency
//! (the real question — is oversubscription avoided?), and a checksum (must be
//! identical everywhere = correctness).
//!
//!   1. raw std.Thread.spawn   — the current experiment style (one OS thread
//!                               per job → peak concurrency ≈ N).
//!   2. Io.Group.async         — queued onto Io.Threaded's pool, self-bounded
//!                               by `async_limit` (no manual semaphore).
//!   3. Io.Group.concurrent    — forced parallelism; UNBOUNDED unless you set
//!                               `concurrent_limit` (demonstrates issue #25748).
//!   4. Io.Group.concurrent    — same, but with `concurrent_limit` set.
//!
//! build: ~/.local/zig-stable/zig build-exe -O ReleaseFast probes/probe_io_concurrency/main.zig
const std = @import("std");
const Io = std.Io;

const Shared = struct {
    active: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    sum: std.atomic.Value(u64) = .init(0),
    iters: u64,

    fn enter(s: *Shared) void {
        const now = s.active.fetchAdd(1, .acq_rel) + 1;
        var p = s.peak.load(.monotonic);
        while (now > p) p = s.peak.cmpxchgWeak(p, now, .monotonic, .monotonic) orelse break;
    }
    fn leave(s: *Shared) void {
        _ = s.active.fetchSub(1, .acq_rel);
    }
};

/// Deterministic CPU work — stands in for decode+re-encode of one row group.
fn cpuWork(iters: u64, salt: u64) u64 {
    var x: u64 = 1469598103934665603 ^ salt;
    var i: u64 = 0;
    while (i < iters) : (i += 1) x = (x ^ (i +% salt)) *% 1099511628211;
    return x;
}

fn task(s: *Shared, id: usize) void {
    s.enter();
    const r = cpuWork(s.iters, id);
    _ = s.sum.fetchAdd(r, .monotonic);
    s.leave();
}

const Result = struct { ms: f64, peak: usize, sum: u64 };

fn nowNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

fn runRawSpawn(gpa: std.mem.Allocator, n: usize, iters: u64) !Result {
    var s = Shared{ .iters = iters };
    const threads = try gpa.alloc(std.Thread, n);
    defer gpa.free(threads);
    const t0 = nowNs();
    for (threads, 0..) |*th, i| th.* = try std.Thread.spawn(.{}, task, .{ &s, i });
    for (threads) |th| th.join();
    const t1 = nowNs();
    return .{ .ms = @as(f64, @floatFromInt(t1 - t0)) / 1e6, .peak = s.peak.load(.monotonic), .sum = s.sum.load(.monotonic) };
}

fn runIoAsync(gpa: std.mem.Allocator, n: usize, iters: u64, limit: usize) !Result {
    var s = Shared{ .iters = iters };
    var threaded = std.Io.Threaded.init(gpa, .{ .async_limit = .limited(limit) });
    defer threaded.deinit();
    const io = threaded.io();
    const t0 = nowNs();
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..n) |i| group.async(io, task, .{ &s, i });
    try group.await(io);
    const t1 = nowNs();
    return .{ .ms = @as(f64, @floatFromInt(t1 - t0)) / 1e6, .peak = s.peak.load(.monotonic), .sum = s.sum.load(.monotonic) };
}

fn runIoConcurrent(gpa: std.mem.Allocator, n: usize, iters: u64, limit: ?usize) !Result {
    var s = Shared{ .iters = iters };
    var threaded = std.Io.Threaded.init(gpa, .{
        .concurrent_limit = if (limit) |l| .limited(l) else .unlimited,
    });
    defer threaded.deinit();
    const io = threaded.io();
    const t0 = nowNs();
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..n) |i| group.concurrent(io, task, .{ &s, i }) catch |e| {
        std.debug.print("  concurrent() failed at task {d}: {s}\n", .{ i, @errorName(e) });
        break;
    };
    try group.await(io);
    const t1 = nowNs();
    return .{ .ms = @as(f64, @floatFromInt(t1 - t0)) / 1e6, .peak = s.peak.load(.monotonic), .sum = s.sum.load(.monotonic) };
}

pub fn main() !void {
    const gpa = std.heap.smp_allocator;

    const n: usize = 200; // ~row-group count for a ~1GB file
    const iters: u64 = 3_000_000; // ~a few ms of CPU per "row group"
    const cores = std.Thread.getCpuCount() catch 1;
    std.debug.print("host cores={d}  jobs={d}  work/job={d} iters\n", .{ cores, n, iters });
    std.debug.print("{s:<34} {s:>9} {s:>6}  {s}\n", .{ "style", "wall_ms", "peak", "checksum" });

    const line = struct {
        fn p(name: []const u8, r: Result) void {
            std.debug.print("{s:<34} {d:>9.1} {d:>6}  {x}\n", .{ name, r.ms, r.peak, r.sum });
        }
    }.p;

    line("raw spawn (all N)", try runRawSpawn(gpa, n, iters));
    line("Io.async  limit=1", try runIoAsync(gpa, n, iters, 1));
    line("Io.async  limit=2", try runIoAsync(gpa, n, iters, 2));
    line("Io.async  limit=cores", try runIoAsync(gpa, n, iters, cores));
    line("Io.concurrent  (unbounded)", try runIoConcurrent(gpa, n, iters, null));
    line("Io.concurrent  limit=cores", try runIoConcurrent(gpa, n, iters, cores));
}
