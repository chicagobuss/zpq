const sbbf = @import("../core/parquet/bloom.zig");
const AtomicWorkCursor = @import("../io/work_cursor.zig").AtomicWorkCursor;
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


const nowMonoNs = @import("../clock.zig").monoNs;
const s3 = @import("../io/s3.zig");
const http = @import("../io/http.zig");
const coalescer = @import("../io/coalescer.zig");
const work_cursor = @import("../io/work_cursor.zig");
const profile = @import("../profile.zig");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const membership = @import("../core/filter/membership.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_prune = @import("../core/filter/prune.zig");
const expr_ast = @import("../core/expr/ast.zig");
const expr_parser = @import("../core/expr/parser.zig");
const expr_agg = @import("../core/expr/agg.zig");
const agg_plan = @import("../core/agg_plan.zig");
const types = @import("types.zig");
const Context = types.Context;
const QueryArgs = types.QueryArgs;
const POOL_SIZE = types.POOL_SIZE;
const CompactRange = @import("s3_rebase.zig").CompactRange;
const projection = @import("projection.zig");
const planProjection = projection.planProjection;
const bindFilter = projection.bindFilter;

pub const S3Fetcher = s3_window.Fetcher(s3.Pool(POOL_SIZE));

pub const FetchWindow = struct {
    limit: u64,
    always: bool = false,
    /// Explain fetches the trailer and exact footer only, and never starts a data fetcher.
    footer_only: bool = false,
};


/// Footer and metadata worker loops. One worker holds one permit. A worker sleeping in retry backoff still holds
/// its slot, so live concurrency can dip below this while a host throttles. Data fetches pass `s3.inFlight`;
/// `s3.DataWave` holds the first of those at `s3.firstWaveCap` (`ZPQ_S3_FIRST_WAVE`, default `S3_FIRST_WAVE`) until
/// a data-range response header arrives.
pub const READ_CONCURRENCY: usize = s3.S3_FIRST_WAVE;


pub const PAR1: [4]u8 = .{ 'P', 'A', 'R', '1' };


/// What the S3 planner fetches from each row group.
const RangePlan = union(enum) {
    /// Aggregates and GROUP BY: the very plan the scan workers follow, made from the same footers, so a chunk left
    /// unfetched is one no worker reads.
    aggregate: agg_plan.Plan,
    /// Write and print evaluate the filter in every row group they keep, so a kept row group needs all of `columns`.
    /// `carry`: the columns whose Bloom filters a write's byte copy keeps; null when no row group is copied.
    projection: struct {
        filter: ?filter_ast.Filter,
        columns: []const bool,
        carry: ?[]const bool = null,
        /// Leaves the filter reads. Empty when there is no filter.
        filter_cols: []const bool = &.{},
    },

    /// Every column some row group may fetch.
    pub fn columns(self: *const RangePlan) []const bool {
        return switch (self.*) {
            .aggregate => |*p| p.columns.read,
            .projection => |p| p.columns,
        };
    }

    /// The columns to fetch from `rg`, or null when statistics prune it.
    pub fn rowGroupColumns(self: *const RangePlan, rg: *const schema.RowGroup, meta: *const schema.FileMetaData) ?[]const bool {
        switch (self.*) {
            .aggregate => |*p| {
                const decision = p.decide(rg, meta);
                return if (decision == .skip) null else p.columns.forRowGroup(decision);
            },
            .projection => |p| {
                if (p.filter) |f| if (filter_prune.pruneRowGroup(rg, f, meta) == .skip) return null;
                return p.columns;
            },
        }
    }

    /// The columns of `rg` whose Bloom filters the output will carry, or null: a write copies a row group only when
    /// there is no filter or statistics prove every row passes it (the same rule `runWrite` applies).
    pub fn carriedFilters(
        self: *const RangePlan,
        rg: *const schema.RowGroup,
        meta: *const schema.FileMetaData,
    ) ?[]const bool {
        const p = switch (self.*) {
            .aggregate => return null,
            .projection => |p| p,
        };
        const cols = p.carry orelse return null;
        if (p.filter) |f| if (filter_prune.pruneRowGroup(rg, f, meta) != .always_match) return null;
        return cols;
    }

    pub fn isFilterCol(self: *const RangePlan, ci: usize) bool {
        const mask = switch (self.*) {
            .aggregate => |*p| p.filter_cols,
            .projection => |p| p.filter_cols,
        };
        return ci < mask.len and mask[ci];
    }
};

/// Plan the S3 fetch from whatever args are set. `metas` are the S3 inputs' footers, parsed against the same schema.

/// Plan the S3 fetch from whatever args are set. `metas` are the S3 inputs' footers, parsed against the same schema.
pub fn planRanges(arena: std.mem.Allocator, args: QueryArgs, metas: []const schema.FileMetaData) !RangePlan {
    const meta0 = &metas[0];
    if (args.aggregate != null or args.group_by != null) {
        const filter: ?filter_ast.Filter = if (args.filter) |fs|
            (if (fs.len > 0) try bindFilter(arena, fs, meta0, args) else null)
        else
            null;
        const group_keys: ?[]const expr_ast.Expr = if (args.group_by) |gb| blk: {
            const items = try expr_parser.parseGroupByOf(arena, gb, meta0, args.diag, args.select_cols orelse &.{});
            const keys = try arena.alloc(expr_ast.Expr, items.len);
            for (items, keys) |item, *key| key.* = item.expr;
            break :blk keys;
        } else null;
        const agg_str = args.aggregate orelse "";
        const calls: []const expr_agg.AggCall = if (args.group_by != null and agg_str.len == 0)
            &.{}
        else
            try expr_parser.parseAggList(arena, agg_str, meta0, args.diag);
        return .{ .aggregate = try .init(arena, .{
            .filter = filter,
            .calls = calls,
            .group_keys = group_keys,
        }, metas, .fromFlags(args.scan_all, args.trust_stats)) };
    }

    // The write and print paths decode exactly these leaves.
    const proj = try planProjection(arena, args, meta0);
    const copies = args.output != null and proj.select_items == null and !proj.any_computed and
        (proj.filter == null or !args.scan_all);
    const filter = if (args.scan_all) null else proj.filter;
    return .{ .projection = .{
        .filter = filter,
        .columns = proj.fetch,
        .carry = if (copies) proj.kept else null,
        .filter_cols = try filterColumnMask(arena, filter, proj.fetch.len),
    } };
}

fn filterColumnMask(arena: std.mem.Allocator, filter: ?filter_ast.Filter, n: usize) ![]const bool {
    const mask = try arena.alloc(bool, n);
    @memset(mask, false);
    const f = filter orelse return mask;
    var idxs: std.ArrayList(usize) = .empty;
    try f.collectColumns(&idxs, arena);
    for (idxs.items) |i| {
        if (i < n) mask[i] = true;
    }
    return mask;
}

/// One fetch job per `S3_RANGE_SPLIT` bytes. `target.len` is `end - start`.
pub fn appendSplitFetch(
    list: *std.ArrayList(s3.FetchJob),
    arena: std.mem.Allocator,
    bucket: []const u8,
    key: []const u8,
    start: u64,
    end: u64,
    target: []u8,
) !void {
    const len = end - start;
    std.debug.assert(target.len == len);
    var off: u64 = 0;
    while (off < len) {
        const n = @min(s3.S3_RANGE_SPLIT, len - off);
        const o: usize = @intCast(off);
        const nsz: usize = @intCast(n);
        try list.append(arena, .{
            .bucket = bucket,
            .key = key,
            .range = .{ .start = start + off, .end = start + off + n },
            .target = target[o .. o + nsz],
        });
        off += n;
    }
}

/// Stable partition: jobs that overlap a filter column's bytes come first, so those GETs start while output columns wait.
pub fn orderFilterFirst(arena: std.mem.Allocator, jobs: []s3.FetchJob, filters: []const coalescer.Range) !void {
    if (filters.len == 0 or jobs.len < 2) return;
    var front: std.ArrayList(s3.FetchJob) = .empty;
    var back: std.ArrayList(s3.FetchJob) = .empty;
    for (jobs) |j| {
        const r: coalescer.Range = .{ .start = j.range.start, .end = j.range.end };
        if (coalescer.overlapsAny(r, filters)) {
            var u = j;
            u.urgent = true;
            try front.append(arena, u);
        } else try back.append(arena, j);
    }
    var n: usize = 0;
    for (front.items) |j| {
        jobs[n] = j;
        n += 1;
    }
    for (back.items) |j| {
        jobs[n] = j;
        n += 1;
    }
}


/// Per-s3-file state during the meta-fetch + range-fetch dance.
/// `file_buf` is a compact packed buffer containing only fetched ranges.
/// Parsed metadata offsets are rebased from source-file offsets into this
/// buffer before consumers see the input.
pub const FetchSpec = struct {
    url: s3.Url,
    /// Index into the query's inputs, for profile spans.
    file_index: usize = 0,
    footer_only: bool = false,
    meta: schema.FileMetaData = undefined,
    file_buf: []u8 = &.{},
    compact_ranges: []CompactRange = &.{},
    tail: []const u8 = &.{},
    footer_region: []u8 = &.{},
    footer_offset: u64 = 0,
    total_size: u64 = 0,
    tail_start: u64 = 0,
    /// Per row group, the columns fetched from it; null where statistics prune it.
    rg_columns: []?[]const bool = &.{},
    /// The object's ETag (quotes stripped), when S3 sent one that fits: what cached Bloom filters are keyed by.
    etag_buf: [96]u8 = undefined,
    etag_len: usize = 0,
    /// Time to the first byte of a successful footer request: a cold suffix, a stale 200/206, or a 304. Zero until
    /// one has. That is the latency the coalesce gap uses. A 304 used to record the whole exchange; the body after
    /// the first byte is not latency.
    rtt_ns: u64 = 0,
    err: ?anyerror = null,

    /// Free the fetched tail, unless it is `footer_region` itself (the warm meta-cache path), which is owned apart.
    pub fn releaseTail(self: *FetchSpec, gpa: std.mem.Allocator) void {
        if (self.tail.len > 0 and self.tail.ptr != self.footer_region.ptr) gpa.free(self.tail);
        self.tail = &.{};
    }

    fn etag(self: *const FetchSpec) []const u8 {
        return self.etag_buf[0..self.etag_len];
    }

    fn noteEtag(self: *FetchSpec, raw: ?[]const u8) void {
        var e = raw orelse return;
        if (e.len >= 2 and e[0] == '"' and e[e.len - 1] == '"') e = e[1 .. e.len - 1];
        if (e.len > self.etag_buf.len) return;
        @memcpy(self.etag_buf[0..e.len], e);
        self.etag_len = e.len;
    }
};

/// Bloom filter reads for S3 inputs, the range fetcher behind `membership.Source`. Ranges inside the prefetched tail
/// are copied from it; the rest go out at once as ranged GETs through the query's pool. Whole filters are kept in the
/// warm metadata cache under the object's ETag.
pub const S3Membership = struct {
    ctx: Context,
    creds: s3.Credentials,
    pool: *s3.Pool(POOL_SIZE),
    specs: []const FetchSpec,
    /// Records each filter fetch as a span when set.
    profiler: ?*profile.Profiler = null,
    /// What this query has measured: a request's fixed cost, and one connection's bandwidth. Null until measured.
    rtt_ns: ?u64 = null,
    bytes_per_s: ?f64 = null,
    /// The query's fetch window. Bloom gaps are charged against it.
    window: u64 = 0,
    /// Requests kept in flight (`s3.inFlight`).
    in_flight: usize = s3.S3_IN_FLIGHT,

    /// Until measured: a GET's first byte after ~25 ms at ~80 MB/s a connection, as on Lambda against S3 in-region.
    const default_rtt_ns: u64 = 25 * std.time.ns_per_ms;
    const default_bytes_per_s: f64 = 80e6;

    pub fn init(
        ctx: Context,
        creds: s3.Credentials,
        pool: *s3.Pool(POOL_SIZE),
        specs: []const FetchSpec,
        window: u64,
    ) S3Membership {
        var self: S3Membership = .{
            .ctx = ctx,
            .creds = creds,
            .pool = pool,
            .specs = specs,
            .window = window,
            .in_flight = s3.inFlight(ctx.env),
        };
        // The fastest footer round trip: slower ones waited on a connection being opened.
        for (specs) |sp| if (sp.rtt_ns > 0) {
            self.rtt_ns = @min(self.rtt_ns orelse sp.rtt_ns, sp.rtt_ns);
        };
        return self;
    }

    /// Bloom uses the same latency cap, in-flight cap and 8 MiB split as a data dispatch. `request_bytes` stays the
    /// calibration until a distant link has been measured; it only chooses blocks against whole filters.
    fn bloomCost(self: *const S3Membership, request_bytes: u64) membership.Cost {
        const rtt = self.rtt_ns orelse 0;
        const bps: u64 = if (self.bytes_per_s) |b| @intFromFloat(b) else s3.S3_BYTES_PER_S;
        return .{
            .request_bytes = request_bytes,
            .concurrency = READ_CONCURRENCY,
            .max_request = s3.SPLIT_THRESHOLD,
            .gap_cap = coalescer.s3CoalesceGap(rtt, if (rtt == 0) 0 else bps),
            .in_flight = self.in_flight,
            .split = s3.S3_RANGE_SPLIT,
            .window = if (self.window == 0) null else self.window,
        };
    }

    pub fn source(self: *S3Membership) membership.Source {
        return .{ .ctx = self, .vtable = &vtable, .cost = self.bloomCost(2 << 20) };
    }

    const vtable: membership.Source.VTable = .{
        .read = read,
        .read_into = readInto,
        .cached = cached,
        .copy_cached = copyCached,
        .has_cached = hasCached,
        .retain = retain,
        .measured = measured,
        .suffix = suffix,
    };

    /// A request's fixed cost as the bytes a connection moves meanwhile, from what this query has timed. A round trip
    /// no slower than the calibration's means an in-region client, and the calibration stands: there the first reads
    /// also pay for new connections, which a few hundred kilobytes cannot tell apart from slow bandwidth. A slower one
    /// means a distant link, where what was measured beats any constant.
    fn measured(ctx: *anyopaque) ?membership.Cost {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const rtt = self.rtt_ns orelse return null;
        if (rtt <= default_rtt_ns) return null;
        const rtt_s = @as(f64, @floatFromInt(rtt)) / std.time.ns_per_s;
        const request_bytes: u64 = @intFromFloat(@max(4096.0, rtt_s * (self.bytes_per_s orelse default_bytes_per_s)));
        return self.bloomCost(request_bytes);
    }

    fn suffix(ctx: *anyopaque, file: u32) ?membership.Span {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        if (file >= self.specs.len) return null;
        const sp = &self.specs[file];
        if (sp.tail.len == 0) return null;
        return .{ .file = file, .start = sp.tail_start, .end = sp.tail_start + sp.tail.len };
    }

    /// Learn one connection's bandwidth from a batch of `n` requests moving `bytes` in `ns`, once the round trips are
    /// paid; a batch of small requests shows none.
    fn observe(self: *S3Membership, n: usize, bytes: u64, ns: u64) void {
        if (n == 0) return;
        const lanes = @min(n, self.in_flight);
        const rounds = std.math.divCeil(usize, n, self.in_flight) catch unreachable;
        const per_lane = bytes / lanes;
        if (per_lane < 128 << 10) return;
        const transfer = ns -| rounds * @max(self.rtt_ns orelse default_rtt_ns, default_rtt_ns);
        if (transfer < ns / 4) return;
        const bw = @as(f64, @floatFromInt(per_lane * rounds)) / (@as(f64, @floatFromInt(transfer)) / std.time.ns_per_s);
        self.bytes_per_s = std.math.clamp(bw, 1e6, 2e9);
    }

    fn read(
        ctx: *anyopaque,
        arena: std.mem.Allocator,
        spans: []const membership.Span,
        out: [][]const u8,
    ) anyerror!void {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        var jobs: std.ArrayList(s3.FetchJob) = .empty;
        for (spans, out) |s, *o| {
            const sp = &self.specs[s.file];
            const end = @min(s.end, sp.total_size);
            if (s.start >= end) {
                o.* = &.{};
                continue;
            }
            // A span the tail already holds is that slice. Copying it would double the suffix in the wave's arena.
            const tail_end = sp.tail_start + sp.tail.len;
            if (sp.tail.len > 0 and s.start >= sp.tail_start and end <= tail_end) {
                const off: usize = @intCast(s.start - sp.tail_start);
                const n: usize = @intCast(end - s.start);
                o.* = sp.tail[off..][0..n];
                continue;
            }
            // A caller that has not placed its own buffer gets one of the span. Bloom bodies use `readInto` instead,
            // so this allocation is the dictionary and header path.
            const buf = try arena.alloc(u8, @intCast(end - s.start));
            const fetch_end = @min(end, sp.tail_start);
            if (s.start < fetch_end) try jobs.append(arena, .{
                .bucket = sp.url.bucket,
                .key = sp.url.key,
                .range = .{ .start = s.start, .end = fetch_end },
                .target = buf[0..@intCast(fetch_end - s.start)],
            });
            if (end > sp.tail_start) {
                const from = @max(s.start, sp.tail_start);
                const tail_off: usize = @intCast(from - sp.tail_start);
                const n: usize = @intCast(end - from);
                if (tail_off + n > sp.tail.len) return error.BadResponse;
                @memcpy(buf[@intCast(from - s.start)..][0..n], sp.tail[tail_off..][0..n]);
            }
            o.* = buf;
        }
        if (jobs.items.len > 0) {
            const t0 = nowMonoNs();
            const got = try s3.fetchJobs(
                self.ctx.io,
                self.pool,
                self.ctx.gpa,
                arena,
                self.creds,
                jobs.items,
                self.in_flight,
                self.profiler,
                null,
            );
            self.observe(jobs.items.len, got, @intCast(nowMonoNs() - t0));
        }
        for (spans, out) |s, o| try ensureHeadMagic(s.start, o);
    }

    fn readInto(
        ctx: *anyopaque,
        arena: std.mem.Allocator,
        spans: []const membership.Span,
        into: [][]u8,
    ) anyerror!void {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        var jobs: std.ArrayList(s3.FetchJob) = .empty;
        for (spans, into) |s, dest| {
            const sp = &self.specs[s.file];
            const end = @min(s.end, sp.total_size);
            if (s.start >= end) continue;
            const n: usize = @intCast(end - s.start);
            if (dest.len < n) return error.BadResponse;
            const tail_end = sp.tail_start + sp.tail.len;
            if (sp.tail.len > 0 and s.start >= sp.tail_start and end <= tail_end) {
                const off: usize = @intCast(s.start - sp.tail_start);
                @memcpy(dest[0..n], sp.tail[off..][0..n]);
                continue;
            }
            const fetch_end = @min(end, if (sp.tail.len > 0) sp.tail_start else end);
            if (s.start < fetch_end) try jobs.append(arena, .{
                .bucket = sp.url.bucket,
                .key = sp.url.key,
                .range = .{ .start = s.start, .end = fetch_end },
                .target = dest[0..@intCast(fetch_end - s.start)],
            });
            if (sp.tail.len > 0 and end > sp.tail_start) {
                const from = @max(s.start, sp.tail_start);
                const tail_off: usize = @intCast(from - sp.tail_start);
                const tn: usize = @intCast(end - from);
                if (tail_off + tn > sp.tail.len) return error.BadResponse;
                @memcpy(dest[@intCast(from - s.start)..][0..tn], sp.tail[tail_off..][0..tn]);
            }
        }
        if (jobs.items.len > 0) {
            const t0 = nowMonoNs();
            const got = try s3.fetchJobs(
                self.ctx.io,
                self.pool,
                self.ctx.gpa,
                arena,
                self.creds,
                jobs.items,
                self.in_flight,
                self.profiler,
                null,
            );
            self.observe(jobs.items.len, got, @intCast(nowMonoNs() - t0));
        }
        for (spans, into) |s, dest| try ensureHeadMagic(s.start, dest);
    }

    fn cached(ctx: *anyopaque, arena: std.mem.Allocator, s: membership.Span) ?[]const u8 {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return null;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0) return null;
        const len: usize = @intCast(s.end - s.start);
        return cache.getBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, len, arena);
    }

    fn hasCached(ctx: *anyopaque, s: membership.Span) bool {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return false;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0 or s.end < s.start) return false;
        return cache.hasBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, @intCast(s.end - s.start));
    }

    fn copyCached(ctx: *anyopaque, s: membership.Span, dest: []u8) bool {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return false;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0) return false;
        return cache.copyBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, dest);
    }

    fn retain(ctx: *anyopaque, s: membership.Span, bytes: []const u8) void {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0) return;
        cache.putBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, bytes);
    }
};

pub fn releaseChargedTail(sp: *FetchSpec, gpa: std.mem.Allocator, account: *ByteAccount) void {
    if (sp.tail.len > 0 and sp.tail.ptr != sp.footer_region.ptr) account.sub(sp.tail.len);
    sp.releaseTail(gpa);
}

/// Drop a retained suffix down to the span planned ranges still overlap. No overlap frees it: a count over many
/// files keeps the footer, not a probe per file.
pub fn trimRetainedSuffix(gpa: std.mem.Allocator, sp: *FetchSpec, plan: RangePlan, account: *ByteAccount) void {
    if (sp.tail.len == 0 or sp.tail.ptr == sp.footer_region.ptr) return;
    const tail_lo = sp.tail_start;
    const tail_hi = tail_lo + sp.tail.len;
    var lo: u64 = tail_hi;
    var hi: u64 = tail_lo;
    const meta = &sp.meta;
    for (meta.row_groups.items) |*rg| {
        if (plan.rowGroupColumns(rg, meta)) |cols| {
            for (cols, 0..) |needed, ci| {
                if (!needed or ci >= rg.columns.items.len) continue;
                const cm = rg.columns.items[ci].meta_data orelse continue;
                const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
                noteSuffixOverlap(&lo, &hi, tail_lo, tail_hi, start, start + @as(u64, @intCast(cm.total_compressed_size)));
            }
        }
        if (plan.carriedFilters(rg, meta)) |carry| for (carry, 0..) |c, ci| {
            if (!c or ci >= rg.columns.items.len) continue;
            const cm = rg.columns.items[ci].meta_data orelse continue;
            const off = cm.bloom_filter_offset orelse continue;
            const len = cm.bloom_filter_length orelse continue;
            if (off < 4 or len <= 0) continue;
            const start: u64 = @intCast(off);
            noteSuffixOverlap(&lo, &hi, tail_lo, tail_hi, start, start + @as(u64, @intCast(len)));
        };
    }
    if (hi <= lo) {
        releaseChargedTail(sp, gpa, account);
        sp.tail_start = sp.total_size;
        return;
    }
    if (lo == tail_lo and hi == tail_hi) return;
    const off: usize = @intCast(lo - tail_lo);
    const new_len: usize = @intCast(hi - lo);
    const neu = gpa.dupe(u8, sp.tail[off .. off + new_len]) catch return;
    account.sub(sp.tail.len - new_len);
    gpa.free(@constCast(sp.tail));
    sp.tail = neu;
    sp.tail_start = lo;
}

/// Column chunks and carried Bloom filters this row group will fetch.
pub fn appendRgFetchRanges(
    arena: std.mem.Allocator,
    ranges: *std.ArrayList(coalescer.Range),
    rg: *schema.RowGroup,
    cols: []const bool,
    carry: ?[]const bool,
    total_size: u64,
) !void {
    for (cols, 0..) |needed, ci| {
        if (!needed or ci >= rg.columns.items.len) continue;
        const cm = rg.columns.items[ci].meta_data orelse continue;
        const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
        const len: u64 = @intCast(cm.total_compressed_size);
        try ranges.append(arena, .{ .start = start, .end = start + len });
    }
    if (carry) |c| for (c, 0..) |on, ci| {
        if (!on or ci >= rg.columns.items.len) continue;
        const cm = rg.columns.items[ci].meta_data orelse continue;
        const off = cm.bloom_filter_offset orelse continue;
        const len = cm.bloom_filter_length orelse continue;
        if (off < 4 or len <= 0 or len > sbbf.max_filter_bytes) continue;
        const start: u64 = @intCast(off);
        const end = start + @as(u64, @intCast(len));
        if (end > total_size) continue;
        try ranges.append(arena, .{ .start = start, .end = end });
    };
}

/// Twice the original selected bytes, and no more than `budget`, unless the selected bytes themselves already
/// exceed it: those are never dropped.
pub fn ampCap(selected: u64, budget: u64) u64 {
    const amp = selected *| 2;
    const room = if (selected > budget) selected else budget;
    return @min(amp, room);
}

fn noteSuffixOverlap(lo: *u64, hi: *u64, tail_lo: u64, tail_hi: u64, start: u64, end: u64) void {
    const a = @max(start, tail_lo);
    const b = @min(end, tail_hi);
    if (a >= b) return;
    lo.* = @min(lo.*, a);
    hi.* = @max(hi.*, b);
}

pub fn fetchMetaBatch(
    ctx: Context,
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    pool: *s3.Pool(POOL_SIZE),
    specs: []FetchSpec,
    profiler: ?*profile.Profiler,
    account: *ByteAccount,
) !void {
    const permits = @min(READ_CONCURRENCY, @max(specs.len, 1));
    const probe = if (specs.len > 0 and specs[0].footer_only) 8 else footerProbeFor(ctx.env, account.limit, permits);
    const TaskCtx = struct {
        ctx: Context,
        creds: s3.Credentials,
        pool: *s3.Pool(POOL_SIZE),
        sp: *FetchSpec,
        profiler: ?*profile.Profiler,
        account: *ByteAccount,
        probe: u64,
        /// The fetching thread's profile buffer, set when the task runs.
        prof: ?*profile.Thread = null,
        /// 0 cold, 1 cached footer revalidated, 2 cached footer stale.
        cache: u8 = 0,
    };
    const Task = struct {
        fn run(t: *TaskCtx) std.Io.Cancelable!void {
            var scratch_state = std.heap.ArenaAllocator.init(t.ctx.gpa);
            defer scratch_state.deinit();
            t.prof = profile.threadOf(t.profiler, .fetch);
            const span = profile.Span.begin(t.prof);
            defer span.end(.footer_fetch, profile.args(.{ t.sp.file_index, t.cache }));
            doFetchMeta(t, scratch_state.allocator()) catch |err| {
                t.sp.err = err;
            };
        }
    };
    var task_ctxs = try arena.alloc(TaskCtx, specs.len);
    for (specs, 0..) |*sp, i| task_ctxs[i] = .{
        .ctx = ctx,
        .creds = creds,
        .pool = pool,
        .sp = sp,
        .profiler = profiler,
        .account = account,
        .probe = probe,
    };
    // One worker loop per permit rather than one task per file: `Io.Group.concurrent` grows the thread pool on every
    // submission that finds all workers busy, so a thousand-file glob spawned a thousand.
    {
        var shared: AtomicWorkCursor(TaskCtx) = .{ .items = task_ctxs };
        const Item = struct {
            fn run(_: void, tc: *TaskCtx) std.Io.Cancelable!void {
                return Task.run(tc);
            }
        };
        var group: std.Io.Group = .init;
        defer group.cancel(ctx.io);
        try work_cursor.startWorkers(TaskCtx, &group, ctx.io, &shared, @min(READ_CONCURRENCY, task_ctxs.len), {}, Item.run);
        try group.await(ctx.io);
    }
    for (specs) |sp| if (sp.err) |err| return err;
    const prof = profile.threadOf(profiler, .main);
    for (specs) |*sp| {
        if (sp.footer_region.len < 8) return error.BadResponse;
        const footer_len = sp.footer_region.len - 8;
        const span = profile.Span.begin(prof);
        defer span.end(.footer_parse, profile.args(.{ sp.file_index, footer_len }));
        sp.meta = try metadata.openFooter(arena, sp.footer_region[0..footer_len]);
    }
}

fn doFetchMeta(t: anytype, scratch: std.mem.Allocator) !void {
    const sp = t.sp;

    // Cache fast path: if we have a previously-cached entry, issue the
    // suffix GET with `If-None-Match`. A 304 means the object is
    // unchanged → reuse the cached footer bytes. Column ranges still
    // fetch later into compact buffers.
    //
    // On 200/206 the object has changed — the cached entry is stale,
    // we proceed exactly as the cold path. The fresh entry is
    // installed at the end of the cold path.
    if (!sp.footer_only) if (t.ctx.meta_cache) |cache| {
        if (cache.get(t.ctx.io, sp.url.bucket, sp.url.key)) |entry| {
            // Charged before it is asked for: the suffix is at most the probe.
            t.account.add(t.probe);
            defer t.account.sub(t.probe);
            const cond = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
                .range = s3.Range.suffix(t.probe),
                .if_none_match = entry.etag,
                .prof = t.prof,
            });
            t.cache = if (cond.status == 304) 1 else 2;
            noteFooterRtt(sp, cond);
            if (cond.status == 304) {
                sp.noteEtag(entry.etag);
                sp.total_size = entry.total_size;
                sp.footer_offset = entry.footer_offset;
                sp.tail_start = entry.footer_offset;
                sp.footer_region = try t.ctx.gpa.dupe(u8, entry.footer_bytes);
                sp.tail = sp.footer_region;
                cache.noteRevalidated(t.ctx.io);
                return;
            }
            // Cached entry is stale — drop it. We'll repopulate after
            // the cold-path fetch succeeds. Note: AWS returns 200 (not
            // 206) on If-None-Match mismatch when the request would
            // otherwise have been a Range GET, so we accept 200 here
            // and continue with the response we already have.
            cache.invalidate(t.ctx.io, sp.url.bucket, sp.url.key);
            if (cond.status == 206 or cond.status == 200) {
                sp.noteEtag(cond.header("ETag"));
                try populateFromTailResponse(t, scratch, cond);
                try maybeCacheMeta(t.ctx, sp, cond.header("ETag"));
                return;
            }
            return error.BadResponse;
        }
    };

    // Cold path: one suffix, charged before it is asked for. A footer that does not fit costs one more GET, inside
    // populateFromTailResponse.
    t.account.add(t.probe);
    defer t.account.sub(t.probe);
    const tail_resp = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
        .range = s3.Range.suffix(t.probe),
        .prof = t.prof,
    });
    if (tail_resp.status != 206 and tail_resp.status != 200) return error.BadResponse;
    noteFooterRtt(sp, tail_resp);
    sp.noteEtag(tail_resp.header("ETag"));
    try populateFromTailResponse(t, scratch, tail_resp);
    try maybeCacheMeta(t.ctx, sp, tail_resp.header("ETag"));
}

/// Shared "we have the suffix response, finish the metadata fetch" path
/// used by both the cold and (cache-stale → 200) paths. On entry,
/// `sp` is empty; on success, `total_size`/`tail_start` are populated
/// and `footer_region` contains `[footer_offset, total_size)`. Bytes of the
/// suffix past the footer stay in `sp.tail` when the resident budget can hold
/// them, and satisfy later ranges with no extra GET. The leading PAR1 is
/// checked whenever a fetched range covers offset 0, including this suffix
/// and a footer-prefix GET that itself starts at 0.
/// The footer's time to first byte, when the exchange recorded one. Cold, stale and 304 responses all count.
fn noteFooterRtt(sp: *FetchSpec, resp: http.Response) void {
    if (resp.ttfb_ns > 0) sp.rtt_ns = @intCast(resp.ttfb_ns);
}

fn populateFromTailResponse(t: anytype, scratch: std.mem.Allocator, tail_resp: http.Response) !void {
    const sp = t.sp;
    sp.total_size = try parseTotalFromContentRange(tail_resp.header("Content-Range"));
    sp.tail_start = try suffixStart(sp.total_size, tail_resp.body.len);

    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (metadata.hasEncryptedFooter(tail)) return error.EncryptedFile;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    if (footer_len + 8 > sp.total_size) return error.NotParquet;
    const footer_actual_start = sp.total_size - 8 - footer_len;
    // The suffix included offset 0, so the leading magic is in hand.
    if (sp.tail_start == 0) try ensureHeadMagic(0, tail);

    const footer_region_len: usize = @intCast(footer_len + 8);
    // The footer's own copy, charged while it is assembled beside the suffix it comes from.
    t.account.add(footer_region_len);
    defer t.account.sub(footer_region_len);
    const footer_region = try t.ctx.gpa.alloc(u8, footer_region_len);
    sp.footer_region = footer_region;
    sp.footer_offset = footer_actual_start;

    // One more GET, and only this one, when the footer did not fit in the suffix; charged before it is asked for.
    if (footer_actual_start < sp.tail_start) {
        const need_len: usize = @intCast(sp.tail_start - footer_actual_start);
        t.account.add(need_len);
        defer t.account.sub(need_len);
        const need = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
            .range = s3.Range.span(footer_actual_start, sp.tail_start - 1),
            .prof = t.prof,
        });
        if (need.status != 206) return error.BadResponse;
        const missing_len: usize = @intCast(sp.tail_start - footer_actual_start);
        if (need.body.len != missing_len) return error.BadResponse;
        // The prefix itself starts at offset 0. Check it here: nothing later re-reads these bytes.
        try ensureHeadMagic(footer_actual_start, need.body);
        @memcpy(footer_region[0..missing_len], need.body);
        @memcpy(footer_region[missing_len..], tail[0 .. footer_region_len - missing_len]);
    } else {
        const tail_off: usize = @intCast(footer_actual_start - sp.tail_start);
        @memcpy(footer_region, tail[tail_off .. tail_off + footer_region_len]);
    }

    const parsed_footer_len = std.mem.readInt(u32, footer_region[footer_region.len - 8 ..][0..4], .little);
    if (parsed_footer_len != footer_len) return error.BadResponse;
    if (!std.mem.eql(u8, footer_region[footer_region.len - 4 ..], &PAR1)) return error.NotParquet;

    // The response buffer is already charged. Keep a second copy only when the budget still has room; otherwise
    // later ranges are fetched on their own. Correctness does not depend on the copy.
    if (t.account.tryAdd(tail.len)) {
        sp.tail = t.ctx.gpa.dupe(u8, tail) catch |err| {
            t.account.sub(tail.len);
            return err;
        };
    } else {
        sp.tail = &.{};
        sp.tail_start = sp.total_size;
    }
}

/// The leading PAR1, when `bytes` are the file bytes at `start` and that range covers offset 0.
pub fn ensureHeadMagic(start: u64, bytes: []const u8) !void {
    if (start != 0 or bytes.len < PAR1.len) return;
    if (!metadata.hasLeadingMagic(bytes)) return error.NotParquet;
}

/// Insert the just-fetched object into the cache (if cache is wired
/// and we got an ETag back from S3). Bytes captured cover
/// `[footer_offset, total_size)` — the thrift footer plus the trailer
/// (4-byte length + PAR1).
fn maybeCacheMeta(ctx: Context, sp: *FetchSpec, etag_opt: ?[]const u8) !void {
    const cache = ctx.meta_cache orelse return;
    const etag_raw = etag_opt orelse return;

    // S3 ETags arrive double-quoted in the response header
    // (`ETag: "abc123"`). Strip them so cache keys are stable.
    var etag = etag_raw;
    if (etag.len >= 2 and etag[0] == '"' and etag[etag.len - 1] == '"') {
        etag = etag[1 .. etag.len - 1];
    }

    if (sp.footer_region.len < 8) return;

    cache.put(
        ctx.io,
        sp.url.bucket,
        sp.url.key,
        etag,
        sp.total_size,
        sp.footer_offset,
        sp.footer_region,
    ) catch {
        // Cache insert failures are non-fatal — proceed without
        // caching this entry.
    };
}


test "S3 range plan: GROUP BY with no aggregates plans like the scan" {
    // The CLI passes `--group-by` alone as an empty aggregate list. The scan accepts that; the S3 planner used to
    // parse the empty list itself and fail with EmptyAggregate, so the same query answered locally and errored on S3.
    const bytes = metadata.readFileSlice("ci/fixtures/parquet/full_match.parquet", std.testing.allocator) catch |err| {
        if (err == error.FileNotFound) return error.SkipZigTest;
        return err;
    };
    defer std.testing.allocator.free(bytes);
    var arena_state = std.heap.ArenaAllocator.init(std.testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const metas = [_]schema.FileMetaData{try metadata.open(arena, bytes)};

    const plan = try planRanges(arena, .{ .inputs = &.{}, .aggregate = "", .group_by = "b", .filter = "ts >= 500" }, &metas);
    // ts (0) is filter-only; b (1) is the key.
    try std.testing.expectEqualSlices(bool, &.{ true, true, false, false, false, false }, plan.columns());
    const rgs = metas[0].row_groups.items;
    try std.testing.expectEqual(@as(?[]const bool, null), plan.rowGroupColumns(&rgs[0], &metas[0]));
    try std.testing.expectEqualSlices(bool, &.{ false, true, false, false, false, false }, plan.rowGroupColumns(&rgs[7], &metas[0]).?);
}


test "S3 Bloom reads keep the Lambda calibration in-region and measure a distant link" {
    var m: S3Membership = .{ .ctx = undefined, .creds = undefined, .pool = undefined, .specs = &.{} };
    try std.testing.expect(S3Membership.measured(&m) == null);
    // An in-region round trip: the calibration stands whatever the first reads suggest.
    m.rtt_ns = 12 * std.time.ns_per_ms;
    m.observe(8, 8 * 260_000, 40 * std.time.ns_per_ms);
    try std.testing.expect(S3Membership.measured(&m) == null);
    // A 50 ms round trip: eight 256 KiB filters in 275 ms is ~1.2 MB/s a connection, so a request costs what a
    // connection moves in 50 ms, about 60 KB.
    m = .{ .ctx = undefined, .creds = undefined, .pool = undefined, .specs = &.{} };
    m.rtt_ns = 50 * std.time.ns_per_ms;
    m.observe(8, 8 * 262_144, 275 * std.time.ns_per_ms);
    try std.testing.expectApproxEqRel(@as(f64, 262_144.0 / 0.225), m.bytes_per_s.?, 0.01);
    const c = S3Membership.measured(&m).?;
    try std.testing.expect(c.request_bytes > 50_000 and c.request_bytes < 70_000);
    // Small requests show no bandwidth.
    m.observe(115, 115 * 32, 900 * std.time.ns_per_ms);
    try std.testing.expectApproxEqRel(@as(f64, 262_144.0 / 0.225), m.bytes_per_s.?, 0.01);
}

test "regression: releaseTail keeps aliased warm footer ownership and clears cold tails" {
    const a = std.testing.allocator;
    const footer = try a.alloc(u8, 128);
    defer a.free(footer);
    @memset(footer, 42);
    var warm: FetchSpec = .{ .url = try s3.Url.parse("s3://b/k"), .footer_region = footer, .tail = footer };
    warm.releaseTail(a);
    try std.testing.expectEqual(@as(usize, 0), warm.tail.len);
    try std.testing.expectEqual(@as(u8, 42), footer[0]);
    warm.releaseTail(a);
    var cold: FetchSpec = .{
        .url = try s3.Url.parse("s3://b/k"),
        .footer_region = footer,
        .tail = try a.alloc(u8, 65536),
    };
    cold.releaseTail(a);
    try std.testing.expectEqual(@as(usize, 0), cold.tail.len);
    try std.testing.expectEqual(@as(u8, 42), footer[0]);
    cold.releaseTail(a);
}

