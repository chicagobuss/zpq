//! Spike: bounded-memory re-encode via back-pressure, grounded in the two
//! realities we actually ship to.
//!
//! The re-encode fan-out produces encoded row-group buffers out of order but
//! must WRITE them in order. Buffering them all is the +600MB we measured. The
//! fix is a sliding WINDOW: at most W row groups may be in flight; a producer
//! for slot i can't start until the in-order writer has drained to within W of
//! it. Peak memory = W × rg_bytes, regardless of total output size.
//!
//! The window size is the whole story, and it's set differently per env:
//!
//!   * LAMBDA  — you own the container and know its memory tier up front
//!     (AWS_LAMBDA_FUNCTION_MEMORY_SIZE). The budget is simply "what the env
//!     gave me": W large. You effectively pre-allocate the tier and almost
//!     never stall — there is no other tenant to be polite to.
//!   * NORMAL  — you do NOT own the machine; grabbing all RAM is antisocial.
//!     The budget is a conservative cap and back-pressure actually bites:
//!     W small, peak stays flat, the box stays usable.
//!
//! A sliding window (not an arbitrary byte-credit semaphore) is deliberate:
//! in-order draining + arbitrary credits can DEADLOCK (a late RG grabs the
//! credit the writer needs for an early one). Admitting strictly in submission
//! order guarantees the writer's next slot is always eventually fillable.
//!
//! Idiom note: 0.16 puts everything blocking on `Io` — the window is a
//! `std.Io.Semaphore`, producers are `Io.Group.async` (bounded by async_limit),
//! matching how zpq's S3 paths already work.
//!
//! build: ~/.local/zig-stable/zig build-exe -O ReleaseFast probes/probe_backpressure/main.zig
const std = @import("std");
const Io = std.Io;

const Slot = struct {
    buf: []u8 = &.{},
    done: std.atomic.Value(bool) = .init(false),
};

const Ctx = struct {
    gpa: std.mem.Allocator,
    slots: []Slot,
    rg_bytes: usize,
    window: *Io.Semaphore, // W permits = max row groups in flight
    inflight: std.atomic.Value(usize) = .init(0),
    peak: std.atomic.Value(usize) = .init(0),
    checksum: u64 = 0, // writer-only, touched in order
};

fn trackPeak(c: *Ctx, now: usize) void {
    var p = c.peak.load(.monotonic);
    while (now > p) p = c.peak.cmpxchgWeak(p, now, .monotonic, .monotonic) orelse break;
}

/// Produce one encoded row group (allocate + fill deterministically per i).
fn produce(c: *Ctx, i: usize) void {
    const buf = c.gpa.alloc(u8, c.rg_bytes) catch @panic("oom");
    for (buf, 0..) |*b, j| b.* = @truncate((i *% 31) +% j);
    c.slots[i].buf = buf;
    const now = c.inflight.fetchAdd(c.rg_bytes, .acq_rel) + c.rg_bytes;
    trackPeak(c, now);
    c.slots[i].done.store(true, .release);
}

/// In-order writer (own thread): drains slots 0..N, freeing each and posting a
/// window permit so the next producer may start. Runs concurrently.
fn writer(c: *Ctx, io: Io) void {
    for (c.slots) |*slot| {
        while (!slot.done.load(.acquire)) std.Thread.yield() catch {};
        for (slot.buf) |b| c.checksum = (c.checksum ^ b) *% 1099511628211;
        _ = c.inflight.fetchSub(slot.buf.len, .acq_rel);
        c.gpa.free(slot.buf);
        c.window.post(io); // slide window forward
    }
}

const Result = struct { ms: f64, peak_mb: f64, checksum: u64 };

fn nowNs() i64 {
    var ts: std.os.linux.timespec = .{ .sec = 0, .nsec = 0 };
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    return @as(i64, ts.sec) * std.time.ns_per_s + @as(i64, ts.nsec);
}

fn run(gpa: std.mem.Allocator, n: usize, rg_bytes: usize, window: usize, async_limit: usize) !Result {
    const slots = try gpa.alloc(Slot, n);
    defer gpa.free(slots);
    for (slots) |*s| s.* = .{};

    var sem: Io.Semaphore = .{ .permits = window };
    var c = Ctx{ .gpa = gpa, .slots = slots, .rg_bytes = rg_bytes, .window = &sem };

    var threaded = std.Io.Threaded.init(gpa, .{ .async_limit = .limited(async_limit) });
    defer threaded.deinit();
    const io = threaded.io();

    const t0 = nowNs();
    const wt = try std.Thread.spawn(.{}, writer, .{ &c, io });

    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..n) |i| {
        sem.waitUncancelable(io); // back-pressure: block until window has room
        group.async(io, produce, .{ &c, i });
    }
    try group.await(io);
    wt.join();
    const t1 = nowNs();

    return .{
        .ms = @as(f64, @floatFromInt(t1 - t0)) / 1e6,
        .peak_mb = @as(f64, @floatFromInt(c.peak.load(.monotonic))) / (1024 * 1024),
        .checksum = c.checksum,
    };
}

pub fn main() !void {
    const gpa = std.heap.smp_allocator;
    const cores = std.Thread.getCpuCount() catch 1;

    const n: usize = 200;
    const rg_bytes: usize = 2 * 1024 * 1024; // ~2MB encoded per row group
    const total_mb = @as(f64, @floatFromInt(n * rg_bytes)) / (1024 * 1024);
    std.debug.print("cores={d}  row_groups={d}  {d:.0}MB each  total_out={d:.0}MB\n\n", .{ cores, n, @as(f64, @floatFromInt(rg_bytes)) / (1024 * 1024), total_mb });
    std.debug.print("{s:<40} {s:>8} {s:>10}  {s}\n", .{ "strategy", "wall_ms", "peak_MB", "checksum" });

    const p = struct {
        fn line(name: []const u8, r: Result) void {
            std.debug.print("{s:<40} {d:>8.1} {d:>10.1}  {x}\n", .{ name, r.ms, r.peak_mb, r.checksum });
        }
    }.line;

    // Buffer-all (today's experiment): window = N → peak = full output.
    p("buffer-all (W=N)  [the +600MB]", try run(gpa, n, rg_bytes, n, cores));
    // Lambda: own the tier — big window, pre-allocate the env, rarely stall.
    p("lambda (W=128, own the container)", try run(gpa, n, rg_bytes, 128, cores));
    // Normal: don't own the box — tight window, flat memory.
    p("normal (W=2*cores, be polite)", try run(gpa, n, rg_bytes, 2 * cores, cores));
    p("normal tight (W=4)", try run(gpa, n, rg_bytes, 4, cores));
}
