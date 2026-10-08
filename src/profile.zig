//! Query profiling: spans, counters and run metadata recorded into per-thread buffers, exported as a Chrome Trace
//! Event JSON trace (open it at https://ui.perfetto.dev) and condensed by `profile_summary.zig`.
//!
//! Cost. A span site holds an optional `*Thread`. With profiling off it is null and the site costs one predictable
//! branch; `-Dprofile=false` (`build_options.enable_profile`) compiles every site out. With profiling on, a span is two
//! `clock.monoNs` reads and a store into the calling thread's own buffer: no lock, no atomic, and one allocation per
//! `block_events` events. Several spans reuse the timestamps the engine already takes for its phase timings, so those
//! sums reconcile with `Timings` exactly rather than approximately.
//!
//! Threading. A `Thread` buffer is written only by the OS thread `Profiler.thread` returned it to, and read by the
//! exporter only after that thread's work is joined (the engine joins every worker before `runQuery` returns). Buffers
//! hang off a lock-free, append-only list keyed by OS thread id, so a thread that profiles several stages keeps one
//! buffer and its spans nest by construction.

const std = @import("std");
const builtin = @import("builtin");
const build_options = @import("build_options");
const clock = @import("clock.zig");
const engine = @import("engine.zig");
const schema = @import("core/schema.zig");
const system = @import("core/system.zig");

/// Comptime switch: false compiles every span site to nothing.
pub const enabled: bool = build_options.enable_profile;

/// Where a span's exclusive time is charged in the summary.
pub const Phase = enum(u8) {
    open,
    footer,
    plan,
    prune,
    io,
    decompress,
    decode,
    filter,
    project,
    aggregate,
    encode,
    write,
    merge,
    other,
    /// A thread parked on other threads' work (a join, a semaphore). Not work: the summary leaves it out of busy time
    /// and the wall shares, so a waiting coordinator does not compete with the workers it waits on.
    wait,

    pub const count = @typeInfo(Phase).@"enum".field_names.len;
};

/// What a span measures. Each kind has a fixed phase and fixed argument names (`argNames`).
pub const Kind = enum(u8) {
    query,
    open_inputs,
    open_file,
    /// The explicit local reader over a query's scan: the mode asked for (1 pread, 2 io_uring), bytes read, reader
    /// threads, worker stalls waiting on reads and their total ns, waits for the window.
    local_read,
    /// What that reader actually read with: threads on io_uring, threads on pread (a pread mode, or a ring that failed
    /// to set up), files opened O_DIRECT, files read through the page cache.
    local_backend,
    /// How a query's local inputs are read: the ZPQ_IO_PROFILE asked for (0 auto, 1 mmap, 2 pread, 3 uring), the mode
    /// chosen (0 mmap, 1 pread, 2 uring), and the share of the inputs cached and of them the query plans to read (per
    /// mille; coverage 2^32-1 when not weighed).
    io_choice,
    /// How a zstd write compressed its pages: the `CompressPriority` (0 size, 1 balanced, 2 speed), the pages each
    /// `ZstdMode` took (raw blocks, literals only, level 1, level 3), and the pages whose cheaper form failed its
    /// full-page checks and were compressed at level 3 too.
    zstd_plan,
    footer_batch,
    footer_fetch,
    footer_parse,
    parse_query,
    plan,
    plan_ranges,
    fetch,
    http_get,
    http_put,
    scan,
    scan_worker,
    worker_release,
    row_group,
    prune_pages,
    decode,
    column_chunk,
    decompress,
    filter,
    project,
    stats_fold,
    aggregate,
    merge,
    finalize,
    reencode,
    encode_rg,
    encode,
    encode_chunk,
    copy_rg,
    sink_write,
    upload_create,
    upload_wait,
    upload_complete,
    footer_write,
    counter,

    pub fn phase(k: Kind) Phase {
        return switch (k) {
            .query, .scan_worker, .worker_release, .row_group, .encode_rg, .counter => .other,
            .open_inputs, .open_file => .open,
            .footer_fetch, .footer_parse => .footer,
            .parse_query, .plan, .plan_ranges => .plan,
            .prune_pages => .prune,
            .http_get, .http_put, .upload_create, .upload_complete => .io,
            .decode, .column_chunk => .decode,
            .decompress => .decompress,
            .filter => .filter,
            .project => .project,
            .stats_fold, .aggregate => .aggregate,
            .encode, .encode_chunk => .encode,
            .copy_rg, .sink_write, .footer_write => .write,
            .merge, .finalize => .merge,
            .footer_batch, .fetch, .scan, .reencode, .upload_wait => .wait,
            .local_read, .local_backend, .io_choice, .zstd_plan => .wait,
        };
    }

    /// Names of `Event.args[0..]`; arguments past the list's end are unused. `http_*.flags` packs the status (bits
    /// 0-15), attempts (16-23) and whether the final attempt reused a pooled connection (bit 24); `http_*.timing` the
    /// final attempt's time to first byte (low 32 bits, ns) and its pool wait plus connect (high 32);
    /// `column_chunk.types` packs codec (bits 0-7), encoding (8-15) and physical type (16-23); `decompress.types`
    /// codec, page type and page encoding the same way. `row_group.decision` is 0 skip, 1 full match, 2 filtered;
    /// `reason` the leaf index + 1 of the filter column whose statistics skipped it (0 none).
    pub fn argNames(k: Kind) []const []const u8 {
        return switch (k) {
            .query, .parse_query, .merge, .finalize => &.{},
            .open_inputs, .footer_batch => &.{"files"},
            .open_file, .footer_parse => &.{ "file", "bytes" },
            .local_read => &.{ "mode", "bytes", "threads", "stalls", "stall_ns", "window_waits" },
            .local_backend => &.{ "uring_threads", "pread_threads", "direct_files", "buffered_files" },
            .io_choice => &.{ "profile", "mode", "resident_permille", "coverage_permille" },
            .zstd_plan => &.{ "priority", "raw_pages", "literals_pages", "level1_pages", "level3_pages", "rechecked" },
            .footer_fetch => &.{ "file", "cache" },
            .plan => &.{ "work_items", "workers", "row_groups" },
            .plan_ranges => &.{ "ranges", "merged", "fetch_jobs", "bytes", "row_groups", "row_groups_pruned" },
            .fetch => &.{ "jobs", "bytes" },
            .http_get => &.{ "range_start", "range_end", "bytes", "flags", "timing", "body_ns" },
            .http_put => &.{ "part", "unused", "bytes", "flags", "timing", "body_ns" },
            .scan => &.{"workers"},
            .worker_release => &.{"worker"},
            .scan_worker => &.{ "worker", "items", "user_ns", "sys_ns", "minor_faults", "major_faults" },
            .row_group => &.{ "file", "rg", "decision", "rows", "reason", "columns" },
            .prune_pages => &.{ "pages", "pages_skipped" },
            .decode => &.{ "file", "rg", "columns" },
            .column_chunk => &.{ "file", "rg", "col", "types", "bytes_in", "bytes_out" },
            .decompress => &.{ "types", "bytes_in", "bytes_out" },
            .filter => &.{ "rows", "selected" },
            .project => &.{ "rows", "columns" },
            .stats_fold => &.{"answered"},
            .aggregate => &.{ "rows", "calls" },
            .reencode => &.{ "row_groups", "workers", "window" },
            .encode_rg => &.{ "file", "rg", "passthrough" },
            .encode => &.{ "columns", "rows" },
            .encode_chunk => &.{ "col", "codec", "bytes_in", "bytes_out", "values" },
            .copy_rg, .sink_write, .footer_write => &.{"bytes"},
            .upload_create, .upload_wait, .upload_complete => &.{"parts"},
            .counter => &.{ "gauge", "value" },
        };
    }
};

pub const Args = [6]u64;

/// `xs` (a tuple of integers, at most six) as `Args`, zero-padded.
pub fn args(xs: anytype) Args {
    var a: Args = @splat(0);
    inline for (xs, 0..) |x, i| a[i] = toU64(x);
    return a;
}

fn toU64(x: anytype) u64 {
    return switch (@typeInfo(@TypeOf(x))) {
        .bool => @intFromBool(x),
        .@"enum" => @intCast(@backingInt(x)),
        .comptime_int => x,
        .int => |info| if (info.signedness == .signed) @bitCast(@as(i64, @intCast(x))) else @intCast(x),
        else => @compileError("profile.args takes integers, bools and enums"),
    };
}

/// One recorded span or counter sample. `start` is a `clock.monoNs` reading; a counter has `dur == 0` and its gauge
/// and value in `args`.
pub const Event = struct {
    start: i64,
    dur: i64,
    args: Args,
    kind: Kind,
};

/// Values tracked across threads. Updated where they change, so the trace has exact steps rather than samples, except
/// `rss_bytes`, which the sampler thread reads.
pub const Gauge = enum(u8) {
    requests_in_flight,
    bytes_in_flight,
    buffered_bytes,
    rss_bytes,

    pub const count = @typeInfo(Gauge).@"enum".field_names.len;
};

/// The role a thread's buffer was created for; names the thread in the trace.
pub const Label = enum(u8) { main, parse, scan, fetch, encode, writer, upload, sampler };

/// One OS thread's event buffer. Written only by that thread; see the file comment.
pub const Thread = struct {
    profiler: *Profiler,
    tid: u64,
    label: Label,
    /// Creation order, for stable thread naming.
    seq: u32,
    next: ?*Thread = null,
    blocks: std.ArrayList(Block) = .empty,
    /// Fill of the last block; earlier blocks record their own in `Block.len`.
    len: usize = 0,
    dropped: u64 = 0,
    /// Sampler only: how many times its ring was halved, each doubling the sampling period.
    decimations: u6 = 0,

    const Block = struct { buf: []Event, len: usize = 0 };

    /// Out of line so a span site stays a branch and a call: the sites are many and the Lambda ships them all.
    pub noinline fn record(self: *Thread, kind: Kind, start_ns: i64, end_ns: i64, a: Args) void {
        self.push(.{ .kind = kind, .start = start_ns, .dur = @max(0, end_ns - start_ns), .args = a });
    }

    /// Add `delta` to `g` and record the new value.
    pub noinline fn gauge(self: *Thread, g: Gauge, delta: i64) void {
        const p = self.profiler;
        const now = p.gauges[@backingInt(g)].fetchAdd(delta, .monotonic) + delta;
        _ = p.gauge_peaks[@backingInt(g)].fetchMax(now, .monotonic);
        self.push(.{ .kind = .counter, .start = clock.monoNs(), .dur = 0, .args = args(.{ g, now }) });
    }

    /// Set `g` to `value` and record it.
    pub fn gaugeSet(self: *Thread, g: Gauge, value: i64) void {
        const p = self.profiler;
        p.gauges[@backingInt(g)].store(value, .monotonic);
        _ = p.gauge_peaks[@backingInt(g)].fetchMax(value, .monotonic);
        self.push(.{ .kind = .counter, .start = clock.monoNs(), .dur = 0, .args = args(.{ g, value }) });
    }

    fn push(self: *Thread, ev: Event) void {
        const items = self.blocks.items;
        if (items.len == 0 or self.len == items[items.len - 1].buf.len) {
            if (!self.grow()) {
                self.dropped += 1;
                return;
            }
        }
        self.blocks.items[self.blocks.items.len - 1].buf[self.len] = ev;
        self.len += 1;
    }

    /// A fresh block, drawn from the profiler's event budget. False once the budget or memory runs out: profiling
    /// degrades to counting drops and never fails the query.
    fn grow(self: *Thread) bool {
        @branchHint(.cold);
        const p = self.profiler;
        if (self.label == .sampler) return self.decimate();
        const n: usize = if (self.blocks.items.len == 0) first_block_events else block_events;
        if (p.events_left.fetchSub(@intCast(n), .monotonic) < n) return false;
        const buf = p.gpa.alloc(Event, n) catch return false;
        self.blocks.append(p.gpa, .{ .buf = buf }) catch {
            p.gpa.free(buf);
            return false;
        };
        const items = self.blocks.items;
        if (items.len > 1) items[items.len - 2].len = self.len;
        self.len = 0;
        return true;
    }

    /// The sampler's one block, `sampler_events` long and outside the event budget, so a long query's RSS samples
    /// cannot crowd out its spans. When the block fills, every other sample goes and the period doubles: the series
    /// stays evenly spaced from start to end in constant memory, and `gauge_peaks` still holds the exact peak.
    fn decimate(self: *Thread) bool {
        const p = self.profiler;
        if (self.blocks.items.len == 0) {
            const buf = p.gpa.alloc(Event, sampler_events) catch return false;
            self.blocks.append(p.gpa, .{ .buf = buf }) catch {
                p.gpa.free(buf);
                return false;
            };
            self.len = 0;
            return true;
        }
        const buf = self.blocks.items[0].buf;
        var kept: usize = 0;
        var i: usize = 0;
        while (i < self.len) : (i += 2) {
            buf[kept] = buf[i];
            kept += 1;
        }
        self.len = kept;
        self.decimations +|= 1;
        return true;
    }

    /// Every recorded event, block by block. Valid once the thread's work is joined.
    pub fn eventBlocks(self: *const Thread) EventIterator {
        return .{ .t = self };
    }

    fn deinit(self: *Thread, gpa: std.mem.Allocator) void {
        for (self.blocks.items) |b| gpa.free(b.buf);
        self.blocks.deinit(gpa);
    }
};

pub const EventIterator = struct {
    t: *const Thread,
    i: usize = 0,

    pub fn next(self: *EventIterator) ?[]const Event {
        const blocks = self.t.blocks.items;
        if (self.i >= blocks.len) return null;
        defer self.i += 1;
        const b = blocks[self.i];
        return b.buf[0..if (self.i + 1 == blocks.len) self.t.len else b.len];
    }
};

/// Small first block: most threads (a fetch worker, a parse worker) record a handful of spans.
const first_block_events: usize = 64;
const block_events: usize = 4096;
/// The sampler's ring: 8 s of samples at the default period before the first halving.
const sampler_events: usize = 4096;

pub const Options = struct {
    /// Events kept across all threads; later ones are counted as dropped.
    max_events: usize = max_events_cap,
    /// RSS sampling period; 0 disables the sampler thread (RSS is still read at start and finish).
    sample_interval_ns: u64 = 2 * std.time.ns_per_ms,

    /// The most events any run keeps, whatever its budget: 72 MB.
    pub const max_events_cap: usize = 1 << 20;
    /// The share of the memory budget the event buffers may take.
    const budget_divisor = 32;

    /// Options for a query whose memory budget (`--max-memory`, the Lambda tier, the cgroup limit) is
    /// `budget_bytes`: the event buffers take at most 1/32 of it, so profiling cannot push a query that fits its
    /// budget out of memory. Give the engine `budget_bytes - reservedBytes()`.
    pub fn forBudget(budget_bytes: usize) Options {
        return .{ .max_events = @min(max_events_cap, budget_bytes / budget_divisor / @sizeOf(Event)) };
    }

    /// The most memory the profiler's buffers hold while the query runs: the event cap and the sampler's ring.
    pub fn reservedBytes(self: Options) usize {
        const ring: usize = if (self.sample_interval_ns > 0) sampler_events else 0;
        return (self.max_events + ring) * @sizeOf(Event);
    }
};

/// The largest trace to build in memory for a query with this memory budget: 1/16 of it, between 1 and 256 MiB. The
/// trace is built after the query has released its own memory; past the cap, events are left out and counted
/// (`TraceStats.omitted`) rather than risk the process.
pub fn traceBytesFor(budget_bytes: usize) usize {
    return std.math.clamp(budget_bytes / 16, 1 << 20, 256 << 20);
}

/// What `Profiler.writeTraceCapped` wrote.
pub const TraceStats = struct {
    bytes: usize = 0,
    events: u64 = 0,
    /// Events left out because the trace reached its byte cap.
    omitted: u64 = 0,
};

/// Forwards to `inner`, counting bytes, so a trace can stop at a size cap whatever it is written to.
const CountingWriter = struct {
    inner: *std.Io.Writer,
    forwarded: usize = 0,
    writer: std.Io.Writer,

    fn init(inner: *std.Io.Writer, buffer: []u8) CountingWriter {
        return .{ .inner = inner, .writer = .{ .vtable = &.{ .drain = drain }, .buffer = buffer } };
    }

    fn count(self: *const CountingWriter) usize {
        return self.forwarded + self.writer.end;
    }

    fn drain(w: *std.Io.Writer, data: []const []const u8, splat: usize) std.Io.Writer.Error!usize {
        const self: *CountingWriter = @alignCast(@fieldParentPtr("writer", w));
        try self.inner.writeAll(w.buffered());
        self.forwarded += w.end;
        w.end = 0;
        const n = try self.inner.writeSplat(data, splat);
        self.forwarded += n;
        return n;
    }
};

/// Run metadata written into the trace and the summary.
pub const Meta = struct {
    version: []const u8 = build_options.version,
    build_mode: []const u8 = @tagName(builtin.mode),
    target: []const u8 = @tagName(builtin.cpu.arch) ++ "-" ++ @tagName(builtin.os.tag) ++ "-" ++ @tagName(builtin.abi),
    cpu_count: usize = 0,
    /// `system.discoverAvailableParallelism`: what the engine sizes its workers from.
    parallelism: usize = 0,
    /// `system.discoverAvailableMemory`: the memory tier the engine budgets against.
    memory_bytes: usize = 0,
    on_lambda: bool = false,
    lambda_memory_mb: ?usize = null,
    /// The query as the caller expressed it (argv or the request), for the reader.
    query: []const u8 = "",

    pub fn discover(env: ?std.process.Environ) Meta {
        const mb: ?usize = if (env) |e|
            (if (e.getPosix("AWS_LAMBDA_FUNCTION_MEMORY_SIZE")) |s| std.fmt.parseInt(usize, s, 10) catch null else null)
        else
            null;
        return .{
            .cpu_count = std.Thread.getCpuCount() catch 1,
            .parallelism = system.discoverAvailableParallelism(env),
            .memory_bytes = system.discoverAvailableMemory(env),
            .on_lambda = system.onLambda(env),
            .lambda_memory_mb = mb,
        };
    }
};

/// Process resource usage at one instant.
pub const Usage = struct {
    user_ns: u64 = 0,
    sys_ns: u64 = 0,
    minor_faults: u64 = 0,
    major_faults: u64 = 0,

    pub fn sub(a: Usage, b: Usage) Usage {
        return .{
            .user_ns = a.user_ns -| b.user_ns,
            .sys_ns = a.sys_ns -| b.sys_ns,
            .minor_faults = a.minor_faults -| b.minor_faults,
            .major_faults = a.major_faults -| b.major_faults,
        };
    }
};

/// What a run's `peak_rss` measures. A process's own high-water mark (`getrusage`, `VmHWM`) spans its whole life, so
/// on a warm Lambda it would report an earlier invocation's peak; only a reset makes it this run's.
pub const RssScope = enum {
    /// `VmHWM` after resetting it at the start of the run (`/proc/self/clear_refs`): this run's exact peak.
    run,
    /// The highest RSS sample of this run: a lower bound, which can miss a spike shorter than the sampling period.
    sampled,
    /// The process's lifetime peak (`getrusage`), where neither of the above is available.
    process,
};

/// Reset this process's RSS high-water mark (`VmHWM`) to its current RSS. False where the kernel refuses or has no
/// such file.
fn resetPeakRss() bool {
    if (builtin.os.tag != .linux) return false;
    const linux = std.os.linux;
    const rc = linux.open("/proc/self/clear_refs", .{ .ACCMODE = .WRONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return false;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    const n = linux.write(fd, "5", 1);
    return linux.errno(n) == .SUCCESS and n == 1;
}

/// `VmHWM` from /proc/self/status, bytes; 0 where unavailable.
fn peakRssSinceReset() u64 {
    if (builtin.os.tag != .linux) return 0;
    const linux = std.os.linux;
    const rc = linux.open("/proc/self/status", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return 0;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    var buf: [4096]u8 = undefined;
    var len: usize = 0;
    while (len < buf.len) {
        const n = linux.read(fd, buf[len..].ptr, buf.len - len);
        if (linux.errno(n) != .SUCCESS or n == 0) break;
        len += n;
    }
    const at = std.mem.indexOf(u8, buf[0..len], "VmHWM:") orelse return 0;
    var fields = std.mem.tokenizeAny(u8, buf[at + "VmHWM:".len .. len], " \t\n");
    const kb = std.fmt.parseInt(u64, fields.next() orelse return 0, 10) catch return 0;
    return kb * 1024;
}

/// This process's usage, or (Linux only) the calling thread's.
pub fn usage(comptime who: enum { process, thread }) Usage {
    if (builtin.os.tag == .windows) return .{};
    if (who == .thread and builtin.os.tag != .linux) return .{};
    const ru = std.posix.getrusage(if (who == .thread) 1 else 0);
    const tvNs = struct {
        fn f(tv: anytype) u64 {
            const sec: u64 = @intCast(@max(0, tv.sec));
            const usec: u64 = @intCast(@max(0, tv.usec));
            return sec * std.time.ns_per_s + usec * std.time.ns_per_us;
        }
    }.f;
    return .{
        .user_ns = tvNs(ru.utime),
        .sys_ns = tvNs(ru.stime),
        .minor_faults = @intCast(@max(0, ru.minflt)),
        .major_faults = @intCast(@max(0, ru.majflt)),
    };
}

/// The process's lifetime peak RSS from `getrusage`, bytes.
fn lifetimePeakRss() u64 {
    if (builtin.os.tag == .windows) return 0;
    const ru = std.posix.getrusage(0);
    // Linux reports maxrss in KiB, macOS in bytes.
    const rss_unit: u64 = if (builtin.os.tag == .linux) 1024 else 1;
    return @as(u64, @intCast(@max(0, ru.maxrss))) * rss_unit;
}

/// Current resident set from /proc/self/statm; 0 where that does not exist.
pub fn currentRss() u64 {
    if (builtin.os.tag != .linux) return 0;
    const linux = std.os.linux;
    const rc = linux.open("/proc/self/statm", .{ .ACCMODE = .RDONLY, .CLOEXEC = true }, 0);
    if (linux.errno(rc) != .SUCCESS) return 0;
    const fd: i32 = @intCast(rc);
    defer _ = linux.close(fd);
    var buf: [128]u8 = undefined;
    const n = linux.read(fd, &buf, buf.len);
    if (linux.errno(n) != .SUCCESS) return 0;
    var fields = std.mem.tokenizeScalar(u8, buf[0..n], ' ');
    _ = fields.next() orelse return 0;
    const pages = std.fmt.parseInt(u64, fields.next() orelse return 0, 10) catch return 0;
    return pages * std.heap.pageSize();
}

pub const Profiler = struct {
    gpa: std.mem.Allocator,
    options: Options,
    epoch_ns: i64,
    end_ns: i64 = 0,
    /// Wall-clock start, seconds since the Unix epoch.
    epoch_real_s: i64,
    head: std.atomic.Value(?*Thread) = .init(null),
    threads_created: std.atomic.Value(u32) = .init(0),
    events_left: std.atomic.Value(i64),
    gauges: [Gauge.count]std.atomic.Value(i64) = @splat(.init(0)),
    gauge_peaks: [Gauge.count]std.atomic.Value(i64) = @splat(.init(0)),
    /// Owner-thread only: names and metadata are set before workers start and read after they finish.
    arena: std.heap.ArenaAllocator,
    files: []const []const u8 = &.{},
    columns: []const []const u8 = &.{},
    meta: Meta = .{},
    usage_start: Usage = .{},
    usage_end: Usage = .{},
    /// Set by `finish`; see `RssScope`.
    peak_rss: u64 = 0,
    peak_rss_scope: RssScope = .process,
    rss_reset: bool = false,
    /// RSS when the run started: a warm process holds memory from earlier runs, which the peak includes.
    start_rss: u64 = 0,
    sampler: ?std.Thread = null,
    /// Nonzero once `finish` runs; the sampler sleeps on it as a futex so finishing never waits out a period.
    stop: std.atomic.Value(u32) = .init(0),

    /// A started profiler: the epoch is now, and the sampler (when enabled) is running. `finish` before reading.
    pub fn create(gpa: std.mem.Allocator, options: Options, meta: Meta) !*Profiler {
        const p = try gpa.create(Profiler);
        p.* = .{
            .gpa = gpa,
            .options = options,
            .epoch_ns = clock.monoNs(),
            .epoch_real_s = clock.realtimeS(),
            .events_left = .init(@intCast(options.max_events)),
            .arena = .init(gpa),
        };
        p.meta = meta;
        p.meta.query = p.arena.allocator().dupe(u8, meta.query) catch "";
        p.usage_start = usage(.process);
        p.rss_reset = resetPeakRss();
        p.start_rss = currentRss();
        if (p.thread(.main)) |t| t.gaugeSet(.rss_bytes, @intCast(p.start_rss));
        if (options.sample_interval_ns > 0) {
            // Not smaller: glibc carves static TLS out of the stack and rejects a stack too small to hold it.
            p.sampler = std.Thread.spawn(.{ .stack_size = 1 << 20 }, samplerRun, .{p}) catch null;
        }
        return p;
    }

    /// Stop the sampler and close the run. Idempotent.
    pub fn finish(self: *Profiler) void {
        if (self.end_ns != 0) return;
        self.stop.store(1, .release);
        if (builtin.os.tag == .linux) _ = std.os.linux.futex_3arg(&self.stop.raw, .{ .cmd = .WAKE, .private = true }, 1);
        if (self.sampler) |t| t.join();
        self.sampler = null;
        self.end_ns = clock.monoNs();
        self.usage_end = usage(.process);
        if (self.thread(.main)) |t| t.gaugeSet(.rss_bytes, @intCast(currentRss()));
        const sampled: u64 = @intCast(@max(0, self.peak(.rss_bytes)));
        const hwm = if (self.rss_reset) peakRssSinceReset() else 0;
        if (hwm > 0) {
            self.peak_rss = @max(hwm, sampled);
            self.peak_rss_scope = .run;
        } else if (sampled > 0) {
            self.peak_rss = sampled;
            self.peak_rss_scope = .sampled;
        } else {
            self.peak_rss = lifetimePeakRss();
            self.peak_rss_scope = .process;
        }
    }

    pub fn destroy(self: *Profiler) void {
        self.finish();
        var it = self.head.load(.acquire);
        while (it) |t| {
            it = t.next;
            t.deinit(self.gpa);
            self.gpa.destroy(t);
        }
        self.arena.deinit();
        self.gpa.destroy(self);
    }

    /// The calling OS thread's buffer, created on first use. Null when out of memory.
    pub fn thread(self: *Profiler, label: Label) ?*Thread {
        const tid: u64 = @intCast(std.Thread.getCurrentId());
        var it = self.head.load(.acquire);
        while (it) |t| : (it = t.next) {
            if (t.tid == tid) return t;
        }
        const t = self.gpa.create(Thread) catch return null;
        t.* = .{
            .profiler = self,
            .tid = tid,
            .label = label,
            .seq = self.threads_created.fetchAdd(1, .monotonic),
        };
        var old = self.head.load(.monotonic);
        while (true) {
            t.next = old;
            old = self.head.cmpxchgWeak(old, t, .release, .monotonic) orelse return t;
        }
    }

    /// Display names for input file indexes. Owner thread only, before workers start.
    pub fn setFiles(self: *Profiler, names: []const []const u8) void {
        self.files = self.dupeNames(names) catch &.{};
    }

    /// Display names for leaf column indexes. Owner thread only, before workers start.
    pub fn setColumns(self: *Profiler, names: []const []const u8) void {
        self.columns = self.dupeNames(names) catch &.{};
    }

    /// Leaf names from a footer's first row group (`a.b.c` paths), when no names are set yet.
    pub fn setColumnsFromMeta(self: *Profiler, meta: *const schema.FileMetaData) void {
        if (self.columns.len > 0 or meta.row_groups.items.len == 0) return;
        const a = self.arena.allocator();
        const chunks = meta.row_groups.items[0].columns.items;
        const names = a.alloc([]const u8, chunks.len) catch return;
        for (chunks, names) |c, *n| {
            const md = c.meta_data orelse {
                n.* = "?";
                continue;
            };
            n.* = std.mem.join(a, ".", md.path_in_schema.items) catch "?";
        }
        self.columns = names;
    }

    fn dupeNames(self: *Profiler, names: []const []const u8) ![]const []const u8 {
        const a = self.arena.allocator();
        const out = try a.alloc([]const u8, names.len);
        for (names, out) |n, *o| o.* = try a.dupe(u8, n);
        return out;
    }

    pub fn fileName(self: *const Profiler, i: u64) []const u8 {
        return if (i < self.files.len) self.files[@intCast(i)] else "?";
    }

    pub fn columnName(self: *const Profiler, i: u64) []const u8 {
        return if (i < self.columns.len) self.columns[@intCast(i)] else "?";
    }

    pub fn peak(self: *const Profiler, g: Gauge) i64 {
        return self.gauge_peaks[@backingInt(g)].load(.monotonic);
    }

    /// Events dropped once the budget ran out, over every thread.
    pub fn dropped(self: *const Profiler) u64 {
        var n: u64 = 0;
        var it = self.head.load(.acquire);
        while (it) |t| : (it = t.next) n += t.dropped;
        return n;
    }

    /// Every thread buffer, in creation order. Caller frees the slice.
    pub fn threads(self: *const Profiler, gpa: std.mem.Allocator) ![]*const Thread {
        var list: std.ArrayList(*const Thread) = .empty;
        errdefer list.deinit(gpa);
        var it = self.head.load(.acquire);
        while (it) |t| : (it = t.next) try list.append(gpa, t);
        std.mem.sortUnstable(*const Thread, list.items, {}, struct {
            fn lt(_: void, a: *const Thread, b: *const Thread) bool {
                return a.seq < b.seq;
            }
        }.lt);
        return list.toOwnedSlice(gpa);
    }

    fn samplerRun(self: *Profiler) void {
        const t = self.thread(.sampler) orelse return;
        while (self.stop.load(.acquire) == 0) {
            const interval: u64 = self.options.sample_interval_ns << @min(t.decimations, 20);
            const ts: std.posix.timespec = .{
                .sec = @intCast(interval / std.time.ns_per_s),
                .nsec = @intCast(interval % std.time.ns_per_s),
            };
            if (builtin.os.tag == .linux) {
                // Returns early on the wake from `finish` (or spuriously); either way the loop re-checks.
                _ = std.os.linux.futex_4arg(&self.stop.raw, .{ .cmd = .WAIT, .private = true }, 0, &ts);
            } else {
                _ = std.posix.system.nanosleep(&ts, null);
            }
            if (self.stop.load(.acquire) != 0) break;
            t.gaugeSet(.rss_bytes, @intCast(currentRss()));
        }
    }

    /// Chrome Trace Event JSON (object form). Call after `finish`.
    pub fn writeTrace(self: *Profiler, gpa: std.mem.Allocator, w: *std.Io.Writer) !void {
        _ = try self.writeTraceCapped(gpa, w, std.math.maxInt(usize));
    }

    /// `writeTrace`, stopping before the event that would take it past about `max_bytes` (one event may overshoot).
    /// The trace stays valid JSON either way, and its trailing `zpq` member counts what was dropped and left out.
    pub fn writeTraceCapped(self: *Profiler, gpa: std.mem.Allocator, out: *std.Io.Writer, max_bytes: usize) !TraceStats {
        const ts = try self.threads(gpa);
        defer gpa.free(ts);
        var buf: [4096]u8 = undefined;
        var cw: CountingWriter = .init(out, &buf);
        const w = &cw.writer;
        var stats: TraceStats = .{};

        try w.writeAll("{\"displayTimeUnit\":\"ns\",\"otherData\":");
        try self.writeMetaJson(w, std.math.maxInt(usize));
        try w.writeAll(",\"traceEvents\":[\n");
        try w.writeAll("{\"name\":\"process_name\",\"ph\":\"M\",\"pid\":1,\"tid\":0,\"args\":{\"name\":\"zpq\"}}");

        // One name per OS thread: its first buffer's label, numbered within the label.
        var label_counts: [@typeInfo(Label).@"enum".field_names.len]u32 = @splat(0);
        for (ts) |t| {
            const n = label_counts[@backingInt(t.label)];
            label_counts[@backingInt(t.label)] += 1;
            try w.print(
                ",\n{{\"name\":\"thread_name\",\"ph\":\"M\",\"pid\":1,\"tid\":{d},\"args\":{{\"name\":\"{s} {d}\"}}}}",
                .{ t.tid, @tagName(t.label), n },
            );
            try w.print(
                ",\n{{\"name\":\"thread_sort_index\",\"ph\":\"M\",\"pid\":1,\"tid\":{d}," ++
                    "\"args\":{{\"sort_index\":{d}}}}}",
                .{ t.tid, t.seq },
            );
        }
        for (ts) |t| {
            var blocks = t.eventBlocks();
            while (blocks.next()) |evs| for (evs) |ev| {
                if (cw.count() >= max_bytes) {
                    stats.omitted += 1;
                    continue;
                }
                try self.writeEvent(w, t.tid, ev);
                stats.events += 1;
            };
        }
        try w.print("\n],\"zpq\":{{\"event_cap\":{d},\"dropped_events\":{d},\"omitted_events\":{d}}}}}\n", .{
            self.options.max_events, self.dropped(), stats.omitted,
        });
        try w.flush();
        stats.bytes = cw.count();
        return stats;
    }

    fn writeEvent(self: *const Profiler, w: *std.Io.Writer, tid: u64, ev: Event) !void {
        const ts_us = @as(f64, @floatFromInt(ev.start - self.epoch_ns)) / 1000.0;
        if (ev.kind == .counter) {
            const g = std.enums.fromInt(Gauge, ev.args[0]) orelse return;
            // Perfetto names a counter track "<name> <arg>".
            const name, const arg = switch (g) {
                .requests_in_flight => .{ "requests", "in_flight" },
                .bytes_in_flight => .{ "bytes", "in_flight" },
                .buffered_bytes => .{ "buffered", "bytes" },
                .rss_bytes => .{ "rss", "bytes" },
            };
            try w.print(",\n{{\"name\":\"{s}\",\"ph\":\"C\",\"ts\":{d:.3},\"pid\":1,\"args\":{{\"{s}\":{d}}}}}", .{
                name, ts_us, arg, @as(i64, @bitCast(ev.args[1])),
            });
            return;
        }
        try w.print(",\n{{\"name\":\"{s}\",\"cat\":\"{s}\",\"ph\":\"X\",\"ts\":{d:.3},\"dur\":{d:.3}," ++
            "\"pid\":1,\"tid\":{d},\"args\":{{", .{
            @tagName(ev.kind),
            @tagName(ev.kind.phase()),
            ts_us,
            @as(f64, @floatFromInt(ev.dur)) / 1000.0,
            tid,
        });
        try self.writeArgs(w, ev);
        try w.writeAll("}}");
    }

    fn writeArgs(self: *const Profiler, w: *std.Io.Writer, ev: Event) !void {
        const names = ev.kind.argNames();
        var first = true;
        for (names, 0..) |name, i| {
            if (std.mem.eql(u8, name, "unused")) continue;
            const v = ev.args[i];
            if (!first) try w.writeByte(',');
            first = false;
            try w.print("\"{s}\":", .{if (std.mem.eql(u8, name, "timing")) "ttfb_ns" else name});
            if (std.mem.eql(u8, name, "types")) {
                const page = ev.kind == .decompress;
                const b1 = (v >> 8) & 0xff;
                const b2 = (v >> 16) & 0xff;
                try w.print("\"{s}/{s}/{s}\"", .{
                    enumName(schema.CompressionCodec, v & 0xff),
                    if (page) enumName(schema.PageType, b1) else enumName(schema.Encoding, b1),
                    if (page) enumName(schema.Encoding, b2) else enumName(schema.Type, b2),
                });
            } else if (std.mem.eql(u8, name, "timing")) {
                try w.print("{d},\"acquire_ns\":{d}", .{ v & 0xffff_ffff, v >> 32 });
            } else if (std.mem.eql(u8, name, "flags")) {
                const reused = (v >> 24) & 1 == 1;
                try w.print("{d},\"attempts\":{d},\"reused\":{}", .{ v & 0xffff, (v >> 16) & 0xff, reused });
            } else if (std.mem.eql(u8, name, "codec")) {
                try w.print("\"{s}\"", .{enumName(schema.CompressionCodec, v)});
            } else if (std.mem.eql(u8, name, "file")) {
                try w.print("{d},\"file_name\":", .{v});
                try writeJsonString(w, self.fileName(v));
            } else if (std.mem.eql(u8, name, "col")) {
                try w.print("{d},\"column\":", .{v});
                try writeJsonString(w, self.columnName(v));
            } else if (std.mem.eql(u8, name, "reason")) {
                if (v == 0) try w.writeAll("null") else try writeJsonString(w, self.columnName(v - 1));
            } else if (std.mem.eql(u8, name, "decision")) {
                try w.print("\"{s}\"", .{switch (v) {
                    0 => "skip",
                    1 => "full_match",
                    else => "filter",
                }});
            } else {
                try w.print("{d}", .{v});
            }
        }
    }

    /// The run metadata as a JSON object, the query cut to at most `max_query` bytes on a character boundary.
    pub fn writeMetaJson(self: *const Profiler, w: *std.Io.Writer, max_query: usize) !void {
        const m = self.meta;
        try w.writeAll("{\"zpq_version\":");
        try writeJsonString(w, m.version);
        try w.print(",\"build_mode\":\"{s}\",\"target\":\"{s}\",\"cpu_count\":{d},\"parallelism\":{d}," ++
            "\"memory_bytes\":{d},\"on_lambda\":{},\"lambda_memory_mb\":", .{
            m.build_mode, m.target, m.cpu_count, m.parallelism, m.memory_bytes, m.on_lambda,
        });
        if (m.lambda_memory_mb) |mb| try w.print("{d}", .{mb}) else try w.writeAll("null");
        try w.print(",\"start_unix_s\":{d},\"query\":", .{self.epoch_real_s});
        try writeJsonString(w, utf8Prefix(m.query, max_query));
        try w.writeByte('}');
    }
};

pub fn enumName(comptime E: type, v: u64) []const u8 {
    const e = std.enums.fromInt(E, v) orelse return "?";
    return @tagName(e);
}

/// `s` as a quoted JSON string under the Lambda's JSON policy (`engine.writeJsonString`): escapes, and every byte
/// that does not start valid UTF-8 written as U+FFFD. Names and the request echo are arbitrary bytes; the output
/// must still parse.
pub fn writeJsonString(w: *std.Io.Writer, s: []const u8) !void {
    try w.writeByte('"');
    try engine.writeJsonString(w, s);
    try w.writeByte('"');
}

fn isContinuation(c: u8) bool {
    return c & 0xc0 == 0x80;
}

/// The longest prefix of `s` of at most `n` bytes that does not end inside a UTF-8 sequence. Backs off at most three
/// bytes: a longer run of continuation bytes is not UTF-8, and `writeJsonString` replaces it anyway.
pub fn utf8Prefix(s: []const u8, n: usize) []const u8 {
    if (s.len <= n) return s;
    var end = n;
    var k: usize = 0;
    while (end > 0 and k < 3 and isContinuation(s[end])) : (k += 1) end -= 1;
    return s[0..end];
}

/// The longest suffix of `s` of at most `n` bytes that does not start inside a UTF-8 sequence.
pub fn utf8Suffix(s: []const u8, n: usize) []const u8 {
    if (s.len <= n) return s;
    var start = s.len - n;
    var k: usize = 0;
    while (start < s.len and k < 3 and isContinuation(s[start])) : (k += 1) start += 1;
    return s[start..];
}

// ---------------------------------------------------------------- span sites

/// The calling thread's buffer under `p`, or null when not profiling.
pub inline fn threadOf(p: ?*Profiler, label: Label) ?*Thread {
    if (comptime !enabled) return null;
    const pp = p orelse return null;
    return pp.thread(label);
}

/// A span in progress. `begin` reads the clock only when `t` is set.
pub const Span = struct {
    t: ?*Thread,
    start: i64,

    pub inline fn begin(t: ?*Thread) Span {
        if (comptime !enabled) return .{ .t = null, .start = 0 };
        const tt = t orelse return .{ .t = null, .start = 0 };
        return .{ .t = tt, .start = clock.monoNs() };
    }

    pub inline fn end(self: Span, kind: Kind, a: Args) void {
        if (comptime !enabled) return;
        if (self.t) |t| t.record(kind, self.start, clock.monoNs(), a);
    }
};

/// A span from timestamps the caller already took.
pub inline fn record(t: ?*Thread, kind: Kind, start_ns: i64, end_ns: i64, a: Args) void {
    if (comptime !enabled) return;
    if (t) |tt| tt.record(kind, start_ns, end_ns, a);
}

pub inline fn gauge(t: ?*Thread, g: Gauge, delta: i64) void {
    if (comptime !enabled) return;
    if (t) |tt| tt.gauge(g, delta);
}

/// The encoding a column chunk's values mostly use: its dictionary encoding when it has one, else the first that is
/// not a level encoding.
pub fn chunkEncoding(encodings: []const schema.Encoding) schema.Encoding {
    for (encodings) |e| if (e == .RLE_DICTIONARY or e == .PLAIN_DICTIONARY) return e;
    for (encodings) |e| if (e != .RLE and e != .BIT_PACKED) return e;
    return if (encodings.len > 0) encodings[0] else .PLAIN;
}

pub fn chunkTypes(codec: schema.CompressionCodec, encoding: schema.Encoding, physical: schema.Type) u64 {
    return @as(u64, @intCast(@backingInt(codec))) |
        (@as(u64, @intCast(@backingInt(encoding))) << 8) |
        (@as(u64, @intCast(@backingInt(physical))) << 16);
}

pub fn pageTypes(codec: schema.CompressionCodec, page_type: schema.PageType, encoding: ?schema.Encoding) u64 {
    return @as(u64, @intCast(@backingInt(codec))) |
        (@as(u64, @intCast(@backingInt(page_type))) << 8) |
        (@as(u64, if (encoding) |e| @intCast(@backingInt(e)) else 0xff) << 16);
}

pub fn httpFlags(status: u16, attempts: u8, reused: bool) u64 {
    return @as(u64, status) | (@as(u64, attempts) << 16) | (@as(u64, @intFromBool(reused)) << 24);
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

test "spans from many threads land in per-thread buffers and nest" {
    if (!enabled) return error.SkipZigTest;
    const p = try Profiler.create(testing.allocator, .{ .sample_interval_ns = 0 }, .{ .query = "test" });
    defer p.destroy();

    const Worker = struct {
        fn run(prof: *Profiler, n: usize) void {
            const t = prof.thread(.scan).?;
            for (0..n) |i| {
                const outer = Span.begin(t);
                const inner = Span.begin(t);
                inner.end(.decompress, args(.{ 1, i, i * 2 }));
                outer.end(.column_chunk, args(.{ 0, 0, i }));
            }
        }
    };
    var threads: [8]std.Thread = undefined;
    for (&threads) |*th| th.* = try std.Thread.spawn(.{}, Worker.run, .{ p, 5000 });
    for (threads) |th| th.join();
    p.finish();

    const ts = try p.threads(testing.allocator);
    defer testing.allocator.free(ts);
    var total: usize = 0;
    var seen_tids: std.AutoHashMapUnmanaged(u64, void) = .empty;
    defer seen_tids.deinit(testing.allocator);
    for (ts) |t| {
        try testing.expect(!seen_tids.contains(t.tid));
        try seen_tids.put(testing.allocator, t.tid, {});
        var blocks = t.eventBlocks();
        var prev_outer_end: i64 = 0;
        var pending_inner: ?Event = null;
        while (blocks.next()) |evs| for (evs) |ev| {
            total += 1;
            switch (ev.kind) {
                .decompress => pending_inner = ev,
                .column_chunk => {
                    const inner = pending_inner.?;
                    try testing.expect(inner.start >= ev.start and inner.start + inner.dur <= ev.start + ev.dur);
                    try testing.expect(ev.start >= prev_outer_end);
                    prev_outer_end = ev.start + ev.dur;
                    pending_inner = null;
                },
                else => {},
            }
        };
    }
    // Each worker recorded 10000 spans; the main thread one RSS counter.
    try testing.expect(total >= 8 * 10_000);
    try testing.expectEqual(@as(u64, 0), p.dropped());
}

test "the event budget caps memory and counts drops" {
    if (!enabled) return error.SkipZigTest;
    const p = try Profiler.create(testing.allocator, .{ .sample_interval_ns = 0, .max_events = 100 }, .{});
    defer p.destroy();
    const t = p.thread(.main).?;
    for (0..10_000) |_| record(t, .filter, 1, 2, args(.{}));
    try testing.expect(p.dropped() > 9_000);
    var kept: usize = 0;
    var blocks = t.eventBlocks();
    while (blocks.next()) |evs| kept += evs.len;
    try testing.expect(kept <= 100);
}

test "args packs integers, bools and enums" {
    const a = args(.{ @as(u32, 7), true, schema.CompressionCodec.ZSTD, @as(i64, -1) });
    try testing.expectEqual(@as(u64, 7), a[0]);
    try testing.expectEqual(@as(u64, 1), a[1]);
    try testing.expectEqual(@as(u64, 6), a[2]);
    try testing.expectEqual(@as(i64, -1), @as(i64, @bitCast(a[3])));
    try testing.expectEqual(@as(u64, 0), a[5]);
}

test "JSON output stays valid UTF-8: cuts land on character boundaries, bad bytes become U+FFFD" {
    // `é` (C3 A9) straddles byte 512, the response's cut.
    const query = "{\"note\":\"" ++ @as([502]u8, @splat('a')) ++ "\u{e9}" ++ @as([20]u8, @splat('b')) ++ "\xff\"}";
    try testing.expectEqual(@as(u8, 0xc3), query[511]);
    const cut = utf8Prefix(query, 512);
    try testing.expectEqual(@as(usize, 511), cut.len);
    try testing.expectEqualStrings("\u{e9}b", utf8Prefix(query[511..], 3));
    try testing.expectEqualStrings("", utf8Prefix(query[511..], 1));
    try testing.expectEqualStrings("b", utf8Suffix("\u{e9}b", 2));
    try testing.expectEqualStrings("\u{e9}b", utf8Suffix("\u{e9}b", 3));
    try testing.expectEqualStrings("\u{20ac}", utf8Suffix("x\u{20ac}", 3));
    try testing.expectEqualStrings("", utf8Suffix("x\u{20ac}", 2));

    var p: Profiler = undefined;
    p.meta = .{ .query = query };
    p.epoch_real_s = 0;
    for ([_]usize{ 512, std.math.maxInt(usize) }) |max| {
        var aw: std.Io.Writer.Allocating = .init(testing.allocator);
        defer aw.deinit();
        try p.writeMetaJson(&aw.writer, max);
        try testing.expect(std.unicode.utf8ValidateSlice(aw.written()));
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, aw.written(), .{});
        defer parsed.deinit();
        const q = parsed.value.object.get("query").?.string;
        if (max == 512) {
            try testing.expectEqualStrings(query[0..511], q);
        } else {
            // The stray 0xff is replaced, not passed through.
            try testing.expect(std.mem.endsWith(u8, q, "b\u{fffd}\"}"));
        }
    }
}

test "the RSS sampler keeps its own ring: it never spends the event budget and thins out instead of dropping" {
    if (!enabled) return error.SkipZigTest;
    const p = try Profiler.create(testing.allocator, .{ .sample_interval_ns = 0, .max_events = 64 }, .{});
    defer p.destroy();
    const budget = p.events_left.load(.monotonic);
    // A thread of its own stands in for the sampler (this one already has a buffer, labelled `main`).
    const Fake = struct {
        t: *Thread = undefined,
        last: i64 = 0,
        fn run(self: *@This(), prof: *Profiler) void {
            self.t = prof.thread(.sampler).?;
            // Timestamps step by the period the sampler would be using, which doubles at each halving.
            var now: i64 = 0;
            for (0..5 * sampler_events) |i| {
                self.last = now;
                self.t.push(.{ .kind = .counter, .start = now, .dur = 0, .args = args(.{ Gauge.rss_bytes, i }) });
                now += @as(i64, 1) << self.t.decimations;
            }
        }
    };
    var fake: Fake = .{};
    const th = try std.Thread.spawn(.{}, Fake.run, .{ &fake, p });
    th.join();
    const t = fake.t;
    const last = fake.last;
    try testing.expectEqual(budget, p.events_left.load(.monotonic));
    try testing.expectEqual(@as(u64, 0), p.dropped());
    try testing.expectEqual(@as(usize, 1), t.blocks.items.len);
    try testing.expect(t.decimations >= 2);
    // Still evenly spaced, oldest first, spanning the whole run.
    var blocks = t.eventBlocks();
    const evs = blocks.next().?;
    try testing.expect(evs.len > sampler_events / 4 and evs.len <= sampler_events);
    try testing.expectEqual(@as(i64, 0), evs[0].start);
    const step = evs[1].start - evs[0].start;
    for (evs[1..], evs[0 .. evs.len - 1]) |b, a| try testing.expectEqual(step, b.start - a.start);
    try testing.expectEqual(last, evs[evs.len - 1].start);
}

test "peak RSS is this run's, not the process's: a later run does not inherit an earlier peak" {
    if (!enabled or builtin.os.tag != .linux) return error.SkipZigTest;
    const big = 96 << 20;
    var peaks: [2]u64 = undefined;
    for (&peaks, 0..) |*peak, run| {
        const p = try Profiler.create(testing.allocator, .{ .sample_interval_ns = 0 }, .{});
        defer p.destroy();
        if (run == 0) {
            // Touch every page so it is resident, then give it back before the run ends.
            const buf = try std.heap.page_allocator.alloc(u8, big);
            @memset(buf, 1);
            std.mem.doNotOptimizeAway(buf.ptr);
            std.heap.page_allocator.free(buf);
        }
        p.finish();
        try testing.expect(p.peak_rss > 0);
        if (p.peak_rss_scope == .process) return error.SkipZigTest;
        if (run == 0 and p.peak_rss_scope == .run) try testing.expect(p.peak_rss >= big);
        try testing.expect(p.start_rss > 0 and p.start_rss <= p.peak_rss);
        peak.* = p.peak_rss;
    }
    // The second, warm run must not report the first one's peak; the first saw the buffer when the reset works.
    try testing.expect(peaks[1] < currentRss() + big / 2);
    if (peaks[1] < peaks[0]) try testing.expect(peaks[0] - peaks[1] > big / 2);
}

test "the event cap scales with the memory budget and is reserved out of it" {
    const small = Options.forBudget(128 << 20);
    try testing.expectEqual(@as(usize, (128 << 20) / 32 / @sizeOf(Event)), small.max_events);
    try testing.expect(small.reservedBytes() <= (128 << 20) / 32 + sampler_events * @sizeOf(Event));
    try testing.expectEqual(Options.max_events_cap, Options.forBudget(64 << 30).max_events);
    try testing.expectEqual(@as(usize, 0), Options.forBudget(800).max_events);
    try testing.expectEqual(@as(usize, 1 << 20), traceBytesFor(1 << 20));
    try testing.expectEqual(@as(usize, 16 << 20), traceBytesFor(256 << 20));
    try testing.expectEqual(@as(usize, 256 << 20), traceBytesFor(64 << 30));
}

test "a capped trace stops near its cap, stays valid JSON and counts what it left out" {
    if (!enabled) return error.SkipZigTest;
    const p = try Profiler.create(testing.allocator, .{ .sample_interval_ns = 0, .max_events = 5000 }, .{});
    defer p.destroy();
    p.setFiles(&.{"s3://bucket/a/long/key/name.parquet"});
    const t = p.thread(.main).?;
    for (0..6000) |i| record(t, .open_file, @intCast(i), @intCast(i + 1), args(.{ 0, i }));
    p.finish();

    for ([_]usize{ 64 << 10, std.math.maxInt(usize) }) |cap| {
        var aw: std.Io.Writer.Allocating = .init(testing.allocator);
        defer aw.deinit();
        const st = try p.writeTraceCapped(testing.allocator, &aw.writer, cap);
        try testing.expectEqual(aw.written().len, st.bytes);
        const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, aw.written(), .{});
        defer parsed.deinit();
        const zpq = parsed.value.object.get("zpq").?.object;
        try testing.expectEqual(@as(i64, 5000), zpq.get("event_cap").?.integer);
        try testing.expect(zpq.get("dropped_events").?.integer >= 1000);
        try testing.expectEqual(@as(i64, @intCast(st.omitted)), zpq.get("omitted_events").?.integer);
        if (cap == std.math.maxInt(usize)) {
            try testing.expectEqual(@as(u64, 0), st.omitted);
        } else {
            try testing.expect(st.omitted > 0 and st.events > 0);
            try testing.expect(st.bytes < cap + 1024);
        }
    }
}
