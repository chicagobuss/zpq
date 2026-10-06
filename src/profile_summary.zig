//! The terminal-sized digest of a profile: wall and busy time per phase, each phase's share of the wall time where
//! phases overlap, the costliest column chunks, I/O and pruning counts, and the engine's own phase sums for
//! cross-checking.
//!
//! Exclusive time. A span's own time is its duration minus its children's, so nested spans (a column chunk around
//! its page decompressions) are charged once. Each thread's spans nest (they come from stack-disciplined code on
//! that thread; see `profile.zig`), which lets one sort and a stack recover the exclusive segments.
//!
//! Wall share. Wall time is swept once over every thread's exclusive segments. Each instant is split across the
//! phases running then, in proportion to how many threads run each, so `share` sums to the query's wall time and says
//! where it went; `alone` is the time a phase ran with no other, the serial part no extra parallelism can hide. Time
//! a thread spends parked on others (`Phase.wait`) counts for nothing; an instant with no phase running is `idle`.

const std = @import("std");
const profile = @import("profile.zig");
const schema = @import("core/schema.zig");

const Phase = profile.Phase;
const Kind = profile.Kind;
const Event = profile.Event;

pub const PhaseStat = struct {
    /// Time at least one thread ran this phase.
    wall_ns: u64 = 0,
    /// Exclusive span time summed over threads: time a thread spent inside the phase's spans, not CPU time. It
    /// includes page-fault stalls and time descheduled, and for `io`, time blocked on the network.
    busy_ns: u64 = 0,
    /// The phase's share of the wall time: see the file comment. Not a critical path.
    share_ns: u64 = 0,
    alone_ns: u64 = 0,
    spans: u64 = 0,
};

pub const ChunkStat = struct {
    file: u64 = 0,
    rg: u64 = 0,
    col: u64 = 0,
    types: u64 = 0,
    bytes_in: u64 = 0,
    bytes_out: u64 = 0,
    dur_ns: u64 = 0,
    decompress_ns: u64 = 0,
    pages: u32 = 0,
};

pub const IoStat = struct {
    gets: u64 = 0,
    puts: u64 = 0,
    bytes: u64 = 0,
    ttfb_ns: u64 = 0,
    body_ns: u64 = 0,
    acquire_ns: u64 = 0,
    /// Requests whose final attempt went out on a pooled connection; the rest opened one.
    reused: u64 = 0,
    retries: u64 = 0,
    errors: u64 = 0,
    /// The S3 range planner: chunk ranges wanted, after coalescing, and the requests that fetched them.
    ranges: u64 = 0,
    merged: u64 = 0,
    fetch_jobs: u64 = 0,
    mapped_files: u64 = 0,
    mapped_bytes: u64 = 0,

    pub fn requests(self: IoStat) u64 {
        return self.gets + self.puts;
    }
};

pub const Reason = struct { col: u64, count: u64 };

pub const PruneStat = struct {
    row_groups: u64 = 0,
    skipped: u64 = 0,
    full_match: u64 = 0,
    filtered: u64 = 0,
    pages: u64 = 0,
    pages_skipped: u64 = 0,
    /// Row-group aggregates answered from statistics instead of decoding.
    stats_answered: u64 = 0,
    reasons: [max_reasons]Reason = undefined,
    n_reasons: usize = 0,

    const max_reasons = 5;
};

/// Span sums taken from the same timestamps as the engine's `Timings`, so they equal the response's phase fields.
pub const Reconcile = struct {
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    decode_ns: u64 = 0,
    decode_wall_ns: u64 = 0,
    /// Filter evaluation plus statistics folds (the aggregate path's `eval_ns`).
    eval_ns: u64 = 0,
    /// Aggregate folds: what the aggregate path reports as `encode_ns`.
    agg_ns: u64 = 0,
    /// Column encoding on the write path (`encode_ns` there).
    encode_ns: u64 = 0,
};

pub const top_n = 10;

pub const Summary = struct {
    total_ns: u64 = 0,
    idle_ns: u64 = 0,
    threads: usize = 0,
    events: u64 = 0,
    /// Events lost to the event cap (`profile.Options.max_events`): the later spans of a run that hit it.
    dropped: u64 = 0,
    event_cap: u64 = 0,
    phases: [Phase.count]PhaseStat = @splat(.{}),
    top: [top_n]ChunkStat = undefined,
    n_top: usize = 0,
    chunks: u64 = 0,
    chunk_ns: u64 = 0,
    io: IoStat = .{},
    prune: PruneStat = .{},
    reconcile: Reconcile = .{},
    usage: profile.Usage = .{},
    workers: u64 = 0,
    worker_user_ns: u64 = 0,
    worker_sys_ns: u64 = 0,
    worker_minor_faults: u64 = 0,
    peak_rss: u64 = 0,
    peak_requests_in_flight: u64 = 0,
    peak_bytes_in_flight: u64 = 0,
    peak_buffered_bytes: u64 = 0,
};

const Segment = struct { start: i64, end: i64, phase: Phase };

const Frame = struct {
    end: i64,
    cursor: i64,
    phase: Phase,
    /// Index into the chunk list when this frame is a column chunk.
    chunk: ?usize,
    /// The row group a `row_group` or `encode_rg` frame covers, inherited by the frames inside it.
    file: u64 = 0,
    rg: u64 = 0,
};

const Indexed = struct { ev: Event, idx: u32 };

/// Summarize `p`, which must be finished.
pub fn compute(gpa: std.mem.Allocator, p: *const profile.Profiler) !Summary {
    var s: Summary = .{};
    s.usage = p.usage_end.sub(p.usage_start);
    s.dropped = p.dropped();
    s.event_cap = p.options.max_events;
    s.peak_rss = p.peak_rss;
    s.peak_requests_in_flight = @intCast(@max(0, p.peak(.requests_in_flight)));
    s.peak_bytes_in_flight = @intCast(@max(0, p.peak(.bytes_in_flight)));
    s.peak_buffered_bytes = @intCast(@max(0, p.peak(.buffered_bytes)));

    const threads = try p.threads(gpa);
    defer gpa.free(threads);
    s.threads = threads.len;

    var segments: std.ArrayList(Segment) = .empty;
    defer segments.deinit(gpa);
    var chunks: std.ArrayList(ChunkStat) = .empty;
    defer chunks.deinit(gpa);
    var reasons: std.AutoArrayHashMapUnmanaged(u64, u64) = .empty;
    defer reasons.deinit(gpa);
    // A row group split into several work items (aggregates divided across workers) is one row group.
    var seen_rgs: std.AutoHashMapUnmanaged([2]u64, void) = .empty;
    defer seen_rgs.deinit(gpa);
    var evs: std.ArrayList(Indexed) = .empty;
    defer evs.deinit(gpa);
    var stack: std.ArrayList(Frame) = .empty;
    defer stack.deinit(gpa);

    var window_start: i64 = p.epoch_ns;
    var window_end: i64 = if (p.end_ns != 0) p.end_ns else p.epoch_ns;

    for (threads) |t| {
        evs.clearRetainingCapacity();
        var blocks = t.eventBlocks();
        while (blocks.next()) |block| for (block) |ev| {
            s.events += 1;
            if (ev.kind == .counter) continue;
            try evs.append(gpa, .{ .ev = ev, .idx = @intCast(evs.items.len) });
            try tally(gpa, &s, &reasons, &seen_rgs, ev);
            if (ev.kind == .query) {
                window_start = ev.start;
                window_end = ev.start + ev.dur;
            }
        };
        // Parents are recorded when they end, after their children: on a tie, the later record encloses the earlier.
        std.mem.sortUnstable(Indexed, evs.items, {}, struct {
            fn lt(_: void, a: Indexed, b: Indexed) bool {
                if (a.ev.start != b.ev.start) return a.ev.start < b.ev.start;
                const ae = a.ev.start + a.ev.dur;
                const be = b.ev.start + b.ev.dur;
                if (ae != be) return ae > be;
                return a.idx > b.idx;
            }
        }.lt);

        stack.clearRetainingCapacity();
        for (evs.items) |item| {
            const ev = item.ev;
            while (stack.items.len > 0 and stack.items[stack.items.len - 1].end <= ev.start) {
                try popFrame(gpa, &stack, &segments);
            }
            var end = ev.start + ev.dur;
            if (stack.items.len > 0) {
                const top = &stack.items[stack.items.len - 1];
                if (ev.start > top.cursor) {
                    try segments.append(gpa, .{ .start = top.cursor, .end = ev.start, .phase = top.phase });
                }
                top.cursor = @max(top.cursor, ev.start);
                // Out-of-spec overlap: clip to the parent so exclusive time never double counts.
                end = @min(end, top.end);
            }
            var chunk: ?usize = null;
            var file: u64 = if (stack.items.len > 0) stack.items[stack.items.len - 1].file else 0;
            var rg: u64 = if (stack.items.len > 0) stack.items[stack.items.len - 1].rg else 0;
            switch (ev.kind) {
                .row_group, .encode_rg => {
                    file = ev.args[0];
                    rg = ev.args[1];
                },
                .column_chunk => {
                    chunk = chunks.items.len;
                    // The write path's decoder does not know its row group; the enclosing span does.
                    const own = ev.args[0] != 0 or ev.args[1] != 0;
                    try chunks.append(gpa, .{
                        .file = if (own) ev.args[0] else file,
                        .rg = if (own) ev.args[1] else rg,
                        .col = ev.args[2],
                        .types = ev.args[3],
                        .bytes_in = ev.args[4],
                        .bytes_out = ev.args[5],
                        .dur_ns = @intCast(ev.dur),
                    });
                },
                .decompress => {
                    var i = stack.items.len;
                    while (i > 0) : (i -= 1) if (stack.items[i - 1].chunk) |ci| {
                        chunks.items[ci].decompress_ns += @intCast(ev.dur);
                        chunks.items[ci].pages += 1;
                        break;
                    };
                },
                else => {},
            }
            try stack.append(gpa, .{
                .end = end,
                .cursor = ev.start,
                .phase = ev.kind.phase(),
                .chunk = chunk,
                .file = file,
                .rg = rg,
            });
        }
        while (stack.items.len > 0) try popFrame(gpa, &stack, &segments);
    }

    if (window_end < window_start) window_end = window_start;
    s.total_ns = @intCast(window_end - window_start);
    try sweep(gpa, &s, segments.items, window_start, window_end);

    s.chunks = chunks.items.len;
    for (chunks.items) |c| s.chunk_ns += c.dur_ns;
    std.mem.sortUnstable(ChunkStat, chunks.items, {}, struct {
        fn lt(_: void, a: ChunkStat, b: ChunkStat) bool {
            return a.dur_ns > b.dur_ns;
        }
    }.lt);
    s.n_top = @min(top_n, chunks.items.len);
    @memcpy(s.top[0..s.n_top], chunks.items[0..s.n_top]);

    const Entry = struct { col: u64, count: u64 };
    var by_count: std.ArrayList(Entry) = .empty;
    defer by_count.deinit(gpa);
    var it = reasons.iterator();
    while (it.next()) |e| try by_count.append(gpa, .{ .col = e.key_ptr.*, .count = e.value_ptr.* });
    std.mem.sortUnstable(Entry, by_count.items, {}, struct {
        fn lt(_: void, a: Entry, b: Entry) bool {
            return a.count > b.count or (a.count == b.count and a.col < b.col);
        }
    }.lt);
    s.prune.n_reasons = @min(PruneStat.max_reasons, by_count.items.len);
    for (by_count.items[0..s.prune.n_reasons], s.prune.reasons[0..s.prune.n_reasons]) |e, *r| {
        r.* = .{ .col = e.col, .count = e.count };
    }
    return s;
}

fn popFrame(gpa: std.mem.Allocator, stack: *std.ArrayList(Frame), segments: *std.ArrayList(Segment)) !void {
    const f = stack.pop().?;
    if (f.end > f.cursor) try segments.append(gpa, .{ .start = f.cursor, .end = f.end, .phase = f.phase });
    if (stack.items.len > 0) {
        const parent = &stack.items[stack.items.len - 1];
        parent.cursor = @max(parent.cursor, f.end);
    }
}

/// Per-kind counts that need no nesting.
fn tally(
    gpa: std.mem.Allocator,
    s: *Summary,
    reasons: *std.AutoArrayHashMapUnmanaged(u64, u64),
    seen_rgs: *std.AutoHashMapUnmanaged([2]u64, void),
    ev: Event,
) !void {
    const a = ev.args;
    const dur: u64 = @intCast(ev.dur);
    s.phases[@backingInt(ev.kind.phase())].spans += 1;
    switch (ev.kind) {
        .open_inputs => s.reconcile.read_ns += dur,
        .open_file => {
            s.io.mapped_files += 1;
            s.io.mapped_bytes += a[1];
        },
        .parse_query => s.reconcile.parse_ns += dur,
        .scan => s.reconcile.decode_wall_ns += dur,
        .decode => s.reconcile.decode_ns += dur,
        .filter, .stats_fold => {
            s.reconcile.eval_ns += dur;
            if (ev.kind == .stats_fold) s.prune.stats_answered += a[0];
        },
        .aggregate => s.reconcile.agg_ns += dur,
        .encode => s.reconcile.encode_ns += dur,
        .plan_ranges => {
            s.io.ranges += a[0];
            s.io.merged += a[1];
            s.io.fetch_jobs += a[2];
        },
        .http_get, .http_put => {
            if (ev.kind == .http_get) s.io.gets += 1 else s.io.puts += 1;
            s.io.bytes += a[2];
            const status = a[3] & 0xffff;
            const attempts = (a[3] >> 16) & 0xff;
            if (status == 0 or status >= 400) s.io.errors += 1;
            if ((a[3] >> 24) & 1 == 1) s.io.reused += 1;
            s.io.retries += attempts -| 1;
            s.io.ttfb_ns += a[4] & 0xffff_ffff;
            s.io.acquire_ns += a[4] >> 32;
            s.io.body_ns += a[5];
        },
        .scan_worker => {
            s.workers += 1;
            s.worker_user_ns += a[2];
            s.worker_sys_ns += a[3];
            s.worker_minor_faults += a[4];
        },
        .row_group => {
            if ((try seen_rgs.getOrPut(gpa, .{ a[0], a[1] })).found_existing) return;
            s.prune.row_groups += 1;
            switch (a[2]) {
                0 => {
                    s.prune.skipped += 1;
                    if (a[4] != 0) {
                        const gop = try reasons.getOrPut(gpa, a[4] - 1);
                        if (!gop.found_existing) gop.value_ptr.* = 0;
                        gop.value_ptr.* += 1;
                    }
                },
                1 => s.prune.full_match += 1,
                else => s.prune.filtered += 1,
            }
        },
        .prune_pages => {
            s.prune.pages += a[0];
            s.prune.pages_skipped += a[1];
        },
        else => {},
    }
}

const Boundary = struct { t: i64, phase: Phase, delta: i32 };

fn sweep(gpa: std.mem.Allocator, s: *Summary, segments: []const Segment, w0: i64, w1: i64) !void {
    var bounds: std.ArrayList(Boundary) = .empty;
    defer bounds.deinit(gpa);
    for (segments) |seg| {
        if (seg.phase == .wait) continue;
        s.phases[@backingInt(seg.phase)].busy_ns += @intCast(seg.end - seg.start);
        const a = std.math.clamp(seg.start, w0, w1);
        const b = std.math.clamp(seg.end, w0, w1);
        if (b <= a) continue;
        try bounds.append(gpa, .{ .t = a, .phase = seg.phase, .delta = 1 });
        try bounds.append(gpa, .{ .t = b, .phase = seg.phase, .delta = -1 });
    }
    std.mem.sortUnstable(Boundary, bounds.items, {}, struct {
        fn lt(_: void, a: Boundary, b: Boundary) bool {
            return a.t < b.t;
        }
    }.lt);

    var active: [Phase.count]i32 = @splat(0);
    var share: [Phase.count]f64 = @splat(0);
    var prev = w0;
    var i: usize = 0;
    while (true) {
        const now = if (i < bounds.items.len) bounds.items[i].t else w1;
        if (now > prev) {
            const dt: u64 = @intCast(now - prev);
            var total: i32 = 0;
            var distinct: usize = 0;
            var only: usize = 0;
            for (active, 0..) |c, ph| if (c > 0) {
                total += c;
                distinct += 1;
                only = ph;
            };
            if (total == 0) {
                s.idle_ns += dt;
            } else {
                for (active, 0..) |c, ph| if (c > 0) {
                    s.phases[ph].wall_ns += dt;
                    share[ph] += @as(f64, @floatFromInt(dt)) * ratio(@intCast(c), @intCast(total));
                };
                if (distinct == 1) s.phases[only].alone_ns += dt;
            }
            prev = now;
        }
        if (i >= bounds.items.len) break;
        active[@backingInt(bounds.items[i].phase)] += bounds.items[i].delta;
        i += 1;
    }
    for (share, &s.phases) |c, *ph| ph.share_ns = @intFromFloat(@round(c));
}

// ---------------------------------------------------------------- rendering

fn ratio(a: u64, b: u64) f64 {
    return if (b == 0) 0 else @as(f64, @floatFromInt(a)) / @as(f64, @floatFromInt(b));
}

fn ms(ns: u64) f64 {
    return @as(f64, @floatFromInt(ns)) / 1e6;
}

fn mb(bytes: u64) f64 {
    return @as(f64, @floatFromInt(bytes)) / (1024.0 * 1024.0);
}

/// At most `n` bytes of `s`, keeping the tail (the distinctive end of a path) and whole characters.
fn tail(s: []const u8, n: usize) []const u8 {
    return profile.utf8Suffix(s, n);
}

fn rssLabel(scope: profile.RssScope) []const u8 {
    return switch (scope) {
        .run => "this run",
        .sampled => "sampled",
        .process => "process lifetime",
    };
}

/// An encoding's name short enough for the chunk table.
fn shortEncoding(v: u64) []const u8 {
    const e = std.enums.fromInt(schema.Encoding, v) orelse return "?";
    return switch (e) {
        .PLAIN => "PLAIN",
        .PLAIN_DICTIONARY, .RLE_DICTIONARY => "DICT",
        .RLE => "RLE",
        .BIT_PACKED => "BITPACK",
        .DELTA_BINARY_PACKED => "DELTA",
        .DELTA_LENGTH_BYTE_ARRAY => "DELTA_LEN",
        .DELTA_BYTE_ARRAY => "DELTA_BA",
        .BYTE_STREAM_SPLIT => "BSS",
    };
}

/// The summary for a terminal: 80 columns.
pub fn writeText(s: Summary, p: *const profile.Profiler, w: *std.Io.Writer) !void {
    const cores = ratio(s.usage.user_ns + s.usage.sys_ns, s.total_ns);
    try w.print("zpq profile: {d:.2} ms wall, {d} threads, {d} events", .{ ms(s.total_ns), s.threads, s.events });
    if (s.dropped > 0) {
        try w.print("\n  {d} EVENTS DROPPED at the cap of {d}, which scales with the memory budget;\n" ++
            "  the figures below miss the later spans", .{ s.dropped, s.event_cap });
    }
    try w.print("\n  cpu user {d:.1} ms, sys {d:.1} ms ({d:.1} cores); {d} minor faults\n", .{
        ms(s.usage.user_ns), ms(s.usage.sys_ns), cores, s.usage.minor_faults,
    });
    try w.print("  rss peak {d:.0} MB ({s})", .{ mb(s.peak_rss), rssLabel(p.peak_rss_scope) });
    if (p.start_rss > 0) try w.print(", {d:.0} MB at start", .{mb(p.start_rss)});
    try w.writeByte('\n');
    const m = p.meta;
    const lambda = if (m.on_lambda) ", on Lambda" else "";
    try w.print("  {s} {s} {s}, {d} cpus, parallelism {d}{s}\n", .{
        m.version, m.build_mode, m.target, m.cpu_count, m.parallelism, lambda,
    });

    try w.writeAll("\nphase          wall ms   busy ms  share ms  alone ms    spans\n");
    for (s.phases, 0..) |ph, i| {
        const phase: Phase = @fromBackingInt(@intCast(i));
        if (phase == .wait or (ph.spans == 0 and ph.busy_ns == 0)) continue;
        try w.print("  {s:<11} {d:>8.2}  {d:>8.2}  {d:>8.2}  {d:>8.2}  {d:>7}\n", .{
            @tagName(phase), ms(ph.wall_ns), ms(ph.busy_ns), ms(ph.share_ns), ms(ph.alone_ns), ph.spans,
        });
    }
    if (s.idle_ns > 0) {
        try w.print("  {s:<11} {d:>8.2}  {s:>8}  {d:>8.2}\n", .{ "(idle)", ms(s.idle_ns), "", ms(s.idle_ns) });
    }
    try w.writeAll("  busy: time in the phase summed over threads (page-fault stalls included);\n" ++
        "  share: its share of wall time while phases overlap (sums to wall);\n" ++
        "  alone: wall time it ran with no other phase (the serial part)\n");

    if (s.n_top > 0) {
        try w.print("\ntop column chunks by time ({d} chunks, {d:.2} ms in all)\n", .{ s.chunks, ms(s.chunk_ns) });
        try w.writeAll("       ms  decomp   in MB  out MB    MB/s  codec/encoding     rg  column\n");
        for (s.top[0..s.n_top]) |c| {
            const rate = if (c.dur_ns > 0) mb(c.bytes_out) / (@as(f64, @floatFromInt(c.dur_ns)) / 1e9) else 0;
            const pct = 100 * ratio(c.decompress_ns, c.dur_ns);
            var types_buf: [40]u8 = undefined;
            const types = std.fmt.bufPrint(&types_buf, "{s}/{s}", .{
                profile.enumName(schema.CompressionCodec, c.types & 0xff),
                shortEncoding((c.types >> 8) & 0xff),
            }) catch "?";
            try w.print("  {d:>7.2}  {d:>5.0}%  {d:>6.2}  {d:>6.2}  {d:>6.0}  {s:<17} {d:>3}  {s}\n", .{
                ms(c.dur_ns), pct, mb(c.bytes_in), mb(c.bytes_out), rate, types, c.rg, tail(p.columnName(c.col), 16),
            });
        }
    }

    try w.writeAll("\n");
    const io = s.io;
    if (io.mapped_files > 0) {
        try w.print("io     local mmap: {d} file(s), {d:.1} MB mapped\n", .{ io.mapped_files, mb(io.mapped_bytes) });
    }
    if (io.requests() > 0) {
        const n: f64 = @floatFromInt(io.requests());
        try w.print("io     {d} GET + {d} PUT, {d:.1} MB; avg acquire {d:.1} ms, ttfb {d:.1} ms, body {d:.1} ms\n", .{
            io.gets, io.puts, mb(io.bytes), ms(io.acquire_ns) / n, ms(io.ttfb_ns) / n, ms(io.body_ns) / n,
        });
        try w.print("       {d} reused / {d} new connections, {d} retries, {d} errors, peak {d} in flight\n", .{
            io.reused, io.requests() - io.reused, io.retries, io.errors, s.peak_requests_in_flight,
        });
    }
    if (io.ranges > 0) {
        try w.print("       coalescing: {d} chunk ranges -> {d} merged ({d:.1}x) -> {d} requests\n", .{
            io.ranges, io.merged, ratio(io.ranges, io.merged), io.fetch_jobs,
        });
    }
    if (io.mapped_files == 0 and io.requests() == 0) try w.writeAll("io     none recorded\n");

    // Only scans record row-group decisions; a write has none to report rather than zero pruned.
    const pr = s.prune;
    if (pr.row_groups > 0) {
        try w.print("prune  row groups: {d} total, {d} skipped, {d} full match, {d} filtered\n", .{
            pr.row_groups, pr.skipped, pr.full_match, pr.filtered,
        });
        try w.print("       pages: {d} of {d} skipped by page index; {d} aggregates from stats\n", .{
            pr.pages_skipped, pr.pages, pr.stats_answered,
        });
    }
    if (pr.n_reasons > 0) {
        try w.writeAll("       skipped by statistics of:");
        for (pr.reasons[0..pr.n_reasons]) |r| try w.print(" {s} ({d})", .{ tail(p.columnName(r.col), 24), r.count });
        try w.writeAll("\n");
    }
    if (s.workers > 0) {
        try w.print("scan   {d} workers: user {d:.1} ms, sys {d:.1} ms, {d} minor faults\n", .{
            s.workers, ms(s.worker_user_ns), ms(s.worker_sys_ns), s.worker_minor_faults,
        });
    }
    const r = s.reconcile;
    try w.print("engine read {d:.2} parse {d:.2} decode {d:.2} (wall {d:.2}) eval {d:.2} ms\n", .{
        ms(r.read_ns), ms(r.parse_ns), ms(r.decode_ns), ms(r.decode_wall_ns), ms(r.eval_ns),
    });
    try w.print("       agg {d:.2} encode {d:.2} ms (the sources of the JSON phase fields)\n", .{
        ms(r.agg_ns), ms(r.encode_ns),
    });
}

/// A JSON object written member by member. Keys and values go through a handful of runtime calls rather than a
/// format string per member, which keeps this out of the binary's size budget (it ships in the Lambda).
const JsonObject = struct {
    w: *std.Io.Writer,
    first: bool = true,

    fn begin(w: *std.Io.Writer) !JsonObject {
        try w.writeByte('{');
        return .{ .w = w };
    }

    fn end(self: *JsonObject) !void {
        try self.w.writeByte('}');
    }

    fn key(self: *JsonObject, name: []const u8) !void {
        if (!self.first) try self.w.writeByte(',');
        self.first = false;
        try profile.writeJsonString(self.w, name);
        try self.w.writeByte(':');
    }

    fn int(self: *JsonObject, name: []const u8, v: u64) !void {
        try self.key(name);
        try self.w.printInt(v, 10, .lower, .{});
    }

    fn str(self: *JsonObject, name: []const u8, v: []const u8) !void {
        try self.key(name);
        try profile.writeJsonString(self.w, v);
    }

    fn object(self: *JsonObject, name: []const u8) !JsonObject {
        try self.key(name);
        return begin(self.w);
    }

    /// Every integer field of `v` under its own name, bar `n_*` lengths of arrays written separately.
    fn ints(self: *JsonObject, v: anytype) !void {
        const info = @typeInfo(@TypeOf(v)).@"struct";
        inline for (info.field_names, info.field_types) |name, T| {
            if (comptime @typeInfo(T) == .int and !std.mem.startsWith(u8, name, "n_")) {
                try self.int(name, @field(v, name));
            }
        }
    }
};

/// The summary as one JSON object, bounded in size: names are cut to 128 bytes and only `top_n` chunks are listed.
pub fn writeJson(s: Summary, p: *const profile.Profiler, w: *std.Io.Writer) !void {
    var o: JsonObject = try .begin(w);
    try o.int("total_ns", s.total_ns);
    try o.int("idle_ns", s.idle_ns);
    try o.int("threads", s.threads);
    try o.int("events", s.events);
    try o.int("dropped", s.dropped);
    try o.int("event_cap", s.event_cap);
    {
        var cpu = try o.object("cpu");
        try cpu.ints(s.usage);
        try cpu.int("peak_rss", s.peak_rss);
        try cpu.int("start_rss", p.start_rss);
        try cpu.str("peak_rss_scope", @tagName(p.peak_rss_scope));
        try cpu.end();
    }
    try o.key("meta");
    try p.writeMetaJson(w, 512);
    {
        var phases = try o.object("phases");
        for (s.phases, 0..) |ph, i| {
            const phase: Phase = @fromBackingInt(@intCast(i));
            if (phase == .wait or (ph.spans == 0 and ph.busy_ns == 0)) continue;
            var one = try phases.object(@tagName(phase));
            try one.ints(ph);
            try one.end();
        }
        try phases.end();
    }
    try o.int("chunks", s.chunks);
    try o.int("chunk_ns", s.chunk_ns);
    try o.key("top_chunks");
    try w.writeByte('[');
    for (s.top[0..s.n_top], 0..) |c, i| {
        if (i > 0) try w.writeByte(',');
        var one: JsonObject = try .begin(w);
        try one.int("file", c.file);
        try one.int("rg", c.rg);
        try one.str("column", tail(p.columnName(c.col), 128));
        try one.str("codec", profile.enumName(schema.CompressionCodec, c.types & 0xff));
        try one.str("encoding", profile.enumName(schema.Encoding, (c.types >> 8) & 0xff));
        try one.int("bytes_in", c.bytes_in);
        try one.int("bytes_out", c.bytes_out);
        try one.int("ns", c.dur_ns);
        try one.int("decompress_ns", c.decompress_ns);
        try one.int("pages", c.pages);
        try one.end();
    }
    try w.writeByte(']');
    {
        var io = try o.object("io");
        try io.ints(s.io);
        try io.int("peak_in_flight", s.peak_requests_in_flight);
        try io.int("peak_bytes_in_flight", s.peak_bytes_in_flight);
        try io.end();
    }
    {
        const pr = s.prune;
        var prune = try o.object("prune");
        try prune.ints(pr);
        var by = try prune.object("skipped_by");
        for (pr.reasons[0..pr.n_reasons]) |r| try by.int(tail(p.columnName(r.col), 128), r.count);
        try by.end();
        try prune.end();
    }
    {
        var workers = try o.object("workers");
        try workers.int("count", s.workers);
        try workers.int("user_ns", s.worker_user_ns);
        try workers.int("sys_ns", s.worker_sys_ns);
        try workers.int("minor_faults", s.worker_minor_faults);
        try workers.end();
    }
    {
        var engine = try o.object("engine");
        try engine.ints(s.reconcile);
        try engine.end();
    }
    try o.int("peak_buffered_bytes", s.peak_buffered_bytes);
    try o.end();
}

// ---------------------------------------------------------------- tests

const testing = std.testing;

fn fakeProfiler() !*profile.Profiler {
    const p = try profile.Profiler.create(testing.allocator, .{ .sample_interval_ns = 0 }, .{ .query = "q" });
    p.finish();
    return p;
}

test "exclusive time and the wall shares add up to the wall time" {
    if (!profile.enabled) return error.SkipZigTest;
    const p = try fakeProfiler();
    defer p.destroy();
    const t0 = p.epoch_ns;
    const main = p.thread(.main).?;
    // main: query [0,100) with a scan wait [10,90); worker: decode [10,60) holding a chunk [20,50) holding a
    // decompress [30,40); filter [60,80). Times in ns past the epoch.
    main.record(.scan, t0 + 10, t0 + 90, profile.args(.{}));
    main.record(.query, t0, t0 + 100, profile.args(.{}));
    const Worker = struct {
        fn run(prof: *profile.Profiler, base: i64) void {
            const w = prof.thread(.scan).?;
            w.record(.decompress, base + 30, base + 40, profile.args(.{ 1, 10, 20 }));
            w.record(.column_chunk, base + 20, base + 50, profile.args(.{ 0, 0, 3, 1, 10, 20 }));
            w.record(.decode, base + 10, base + 60, profile.args(.{}));
            w.record(.filter, base + 60, base + 80, profile.args(.{}));
        }
    };
    const th = try std.Thread.spawn(.{}, Worker.run, .{ p, t0 });
    th.join();

    const s = try compute(testing.allocator, p);
    try testing.expectEqual(@as(u64, 100), s.total_ns);
    const ph = s.phases;
    try testing.expectEqual(@as(u64, 40), ph[@backingInt(Phase.decode)].busy_ns);
    try testing.expectEqual(@as(u64, 10), ph[@backingInt(Phase.decompress)].busy_ns);
    try testing.expectEqual(@as(u64, 20), ph[@backingInt(Phase.filter)].busy_ns);
    // query's own time: [0,10) and [90,100); the scan wait counts for nothing.
    try testing.expectEqual(@as(u64, 20), ph[@backingInt(Phase.other)].busy_ns);
    try testing.expectEqual(@as(u64, 0), ph[@backingInt(Phase.wait)].busy_ns);
    var share_sum: u64 = s.idle_ns;
    for (ph) |x| share_sum += x.share_ns;
    try testing.expectEqual(s.total_ns, share_sum);
    // [80,90) nobody works.
    try testing.expectEqual(@as(u64, 10), s.idle_ns);
    try testing.expectEqual(@as(usize, 1), s.n_top);
    try testing.expectEqual(@as(u64, 10), s.top[0].decompress_ns);
    try testing.expectEqual(@as(u32, 1), s.top[0].pages);
    try testing.expectEqual(@as(u64, 50), s.reconcile.decode_ns);
}

test "overlapping phases on two threads split the wall by thread count" {
    if (!profile.enabled) return error.SkipZigTest;
    const p = try fakeProfiler();
    defer p.destroy();
    const t0 = p.epoch_ns;
    const main = p.thread(.main).?;
    // The coordinator waits throughout, so only the workers' phases compete.
    main.record(.scan, t0, t0 + 100, profile.args(.{}));
    main.record(.query, t0, t0 + 100, profile.args(.{}));
    const Worker = struct {
        fn run(prof: *profile.Profiler, base: i64, kind: Kind) void {
            prof.thread(.scan).?.record(kind, base, base + 100, profile.args(.{}));
        }
    };
    for ([_]Kind{ .decode, .decode, .http_get }) |k| {
        const th = try std.Thread.spawn(.{}, Worker.run, .{ p, t0, k });
        th.join();
    }
    const s = try compute(testing.allocator, p);
    // The threads ran one after another, but their spans overlap in time: two decoding, one fetching.
    try testing.expectEqual(@as(u64, 67), s.phases[@backingInt(Phase.decode)].share_ns);
    try testing.expectEqual(@as(u64, 33), s.phases[@backingInt(Phase.io)].share_ns);
    try testing.expectEqual(@as(u64, 100), s.phases[@backingInt(Phase.io)].wall_ns);
    try testing.expectEqual(@as(u64, 0), s.phases[@backingInt(Phase.io)].alone_ns);
    try testing.expectEqual(@as(u64, 200), s.phases[@backingInt(Phase.decode)].busy_ns);
}

test "the summary reports events dropped at the cap" {
    if (!profile.enabled) return error.SkipZigTest;
    const p = try profile.Profiler.create(testing.allocator, .{ .sample_interval_ns = 0, .max_events = 100 }, .{});
    defer p.destroy();
    const t = p.thread(.main).?;
    for (0..1000) |i| t.record(.filter, @intCast(i), @intCast(i + 1), profile.args(.{}));
    p.finish();
    const s = try compute(testing.allocator, p);
    try testing.expect(s.dropped >= 900);
    try testing.expectEqual(@as(u64, 100), s.event_cap);

    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try writeText(s, p, &aw.writer);
    try testing.expect(std.mem.indexOf(u8, aw.written(), "EVENTS DROPPED at the cap of 100") != null);
    aw.clearRetainingCapacity();
    try writeJson(s, p, &aw.writer);
    const parsed = try std.json.parseFromSlice(std.json.Value, testing.allocator, aw.written(), .{});
    defer parsed.deinit();
    try testing.expectEqual(@as(i64, @intCast(s.dropped)), parsed.value.object.get("dropped").?.integer);
    try testing.expectEqual(@as(i64, 100), parsed.value.object.get("event_cap").?.integer);
}

test "the text summary says busy, not CPU, and leaves out pruning a write does not record" {
    if (!profile.enabled) return error.SkipZigTest;
    const p = try fakeProfiler();
    defer p.destroy();
    const t0 = p.epoch_ns;
    const main = p.thread(.main).?;
    main.record(.encode, t0 + 10, t0 + 50, profile.args(.{}));
    main.record(.encode_rg, t0 + 5, t0 + 60, profile.args(.{ 0, 0, 0 }));
    main.record(.query, t0, t0 + 100, profile.args(.{}));
    const s = try compute(testing.allocator, p);
    var aw: std.Io.Writer.Allocating = .init(testing.allocator);
    defer aw.deinit();
    try writeText(s, p, &aw.writer);
    const text = aw.written();
    try testing.expect(std.mem.indexOf(u8, text, "prune") == null);
    try testing.expect(std.mem.indexOf(u8, text, "crit") == null);
    try testing.expect(std.mem.indexOf(u8, text, "share ms") != null);
    var lines = std.mem.splitScalar(u8, text, '\n');
    while (lines.next()) |line| try testing.expect(line.len <= 80);
}
