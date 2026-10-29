const std = @import("std");
const nowMonoNs = @import("../clock.zig").monoNs;
const profile = @import("../profile.zig");
const local_fs = @import("../local_fs.zig");
const local_reader = @import("../io/local_reader.zig");
const s3 = @import("../io/s3.zig");
const s3_window = @import("../io/s3_window.zig");
const tls = @import("../io/tls.zig");
const http = @import("../io/http.zig");
const coalescer = @import("../io/coalescer.zig");
const meta_cache_mod = @import("../io/meta_cache.zig");
const schema = @import("../core/schema.zig");
const metadata = @import("../core/parquet/metadata.zig");
const scan = @import("../core/scan.zig");
const system = @import("../core/system.zig");
const huge_pages = @import("../core/huge_pages.zig");
const core_memory = @import("../core/memory.zig");
const filter_ast = @import("../core/filter/ast.zig");
const filter_prune = @import("../core/filter/prune.zig");
const membership = @import("../core/filter/membership.zig");
const nanprobe = @import("../core/filter/nanprobe.zig");
const expr_ast = @import("../core/expr/ast.zig");
const Diag = @import("../core/diag.zig").Diag;
const types = @import("types.zig");
const Context = types.Context;
const QueryArgs = types.QueryArgs;
const POOL_SIZE = types.POOL_SIZE;
const fetch = @import("s3_fetch.zig");
const S3Fetcher = fetch.S3Fetcher;
const FetchWindow = fetch.FetchWindow;
const FetchSpec = fetch.FetchSpec;
const S3Membership = fetch.S3Membership;
const ByteAccount = fetch.ByteAccount;
const PAR1 = fetch.PAR1;
const planRanges = fetch.planRanges;
const appendSplitFetch = fetch.appendSplitFetch;
const orderFilterFirst = fetch.orderFilterFirst;
const releaseChargedTail = fetch.releaseChargedTail;
const trimRetainedSuffix = fetch.trimRetainedSuffix;
const appendRgFetchRanges = fetch.appendRgFetchRanges;
const ampCap = fetch.ampCap;
const fetchMetaBatch = fetch.fetchMetaBatch;
const ensureHeadMagic = fetch.ensureHeadMagic;
const CompactRange = @import("s3_rebase.zig").CompactRange;
const rebaseFetchedOffsets = @import("s3_rebase.zig").rebaseFetchedOffsets;
const bindFilter = @import("projection.zig").bindFilter;

/// Local coalesce gap. There is no first-byte latency to trade for extra bytes, so only the few bytes of metadata
/// between adjacent column chunks are merged. S3 uses `coalescer.s3CoalesceGap` instead.
const COALESCE_GAP: u64 = 64 * 1024;

/// Zig's 16 MiB default makes short-lived worker creation expensive, but this is deliberately not tuned to the minimum:
/// a real fetch descends into BoringSSL and libc `getaddrinfo`/NSS, whose stack use nothing here bounds.
const READ_WORKER_STACK_SIZE: usize = 4 << 20;

/// Bounds threads, not just sockets: the process-wide `std.Io.Threaded` has `concurrent_limit = .unlimited`, and
/// `Io.Group.concurrent` spawns a fresh OS thread whenever every worker is busy — including one parked on a blocking
/// socket read — so pool permits alone bound sockets only. Both fetch sites start workers through `startWorkers`, so a
/// submission at the limit waits rather than failing the query; that matters because the metadata stage's workers can
/// still count as busy when the column stage starts. `deinit` joins every worker before the caller returns.
fn readExecutor(gpa: std.mem.Allocator) std.Io.Threaded {
    return std.Io.Threaded.init(gpa, .{
        .concurrent_limit = .limited(s3.S3_IN_FLIGHT_MAX),
        .stack_size = READ_WORKER_STACK_SIZE,
    });
}

/// S3 credentials from the environment, telling a missing region apart from missing keys.
pub fn credentialsFromEnv(env: std.process.Environ) error{ NoCredentials, NoRegion }!s3.Credentials {
    var creds = s3.Credentials.fromEnv(env) catch |err| {
        return if (err == error.NoRegion) error.NoRegion else error.NoCredentials;
    };
    // The Lambda binary pre-connects the regional endpoint before it knows the bucket. Path-style uses that
    // same host, so those sockets are the ones the query acquires. The CLI is not on Lambda and stays virtual-hosted.
    if (system.onLambda(env)) creds.path_style = true;
    return creds;
}

// ============================================================
// Input opening — mmap for local, parallel range-fetch for s3
// ============================================================

pub const OpenedInputs = struct {
    gpa: std.mem.Allocator,
    inputs: []const scan.Input,
    metas: []schema.FileMetaData,
    meta_ready: []bool,
    /// `QueryArgs.row_groups`, applied to each footer as it is parsed (`restrictRowGroups`).
    row_groups: ?[]const ?[2]u32 = null,
    has_preparsed_meta: bool = false,
    /// Per-input mmap state — set for local files so we can munmap on
    /// deinit. Null for s3 inputs (their bytes live in the arena).
    mmaps: []const ?local_fs.Mapped,
    /// One s3 pool per bucket touched. Owned here when the caller's
    /// `Context.pool_registry` is null; borrowed otherwise.
    owned_pools: []*s3.Pool(POOL_SIZE),
    /// S3 sparse file buffers are gpa-owned because they must outlive the
    /// per-task scratch arenas used during parallel metadata fetch.
    s3_buffers: []const []u8,
    /// Bloom filter probing done while planning the S3 fetch.
    membership: membership.Stats = .{},
    /// `Context.exit_after`: the mappings go with the process (`local_fs.leaveForExit`).
    leave_mappings: bool = false,
    release_io: ?std.Io = null,
    /// Fetching S3 bytes into `s3_mapped` behind the scan's gates, when the query reads more than its window.
    s3_fetcher: ?*S3Fetcher = null,
    s3_mapped: []const []align(s3_window.page) u8 = &.{},
    /// High-water of retained footer suffixes plus in-flight footer responses, in bytes.
    footer_resident_peak: u64 = 0,

    /// `error.InputChanged` when a local input was truncated while it was mapped (`local_fs.Mapped.intact`): checked
    /// before an answer is returned or published.
    pub fn intact(self: *const OpenedInputs) error{InputChanged}!void {
        for (self.inputs, self.mmaps) |in, m_opt| if (m_opt) |m| try m.intact(in.name);
    }

    /// Every local input kept its descriptor (`local_fs.Mapped.fd`).
    pub fn descriptorsHeld(self: *const OpenedInputs) bool {
        for (self.mmaps) |m_opt| if (m_opt) |m| if (m.fd == null) return false;
        return true;
    }

    /// For a query's error path: zeros read past a cut parse as a corrupt page, which is the cut, not the file.
    pub fn whole(self: *const OpenedInputs) bool {
        self.intact() catch return false;
        return true;
    }

    pub fn deinit(self: *OpenedInputs) void {
        for (self.mmaps) |m_opt| if (m_opt) |m| {
            if (self.leave_mappings) {
                local_fs.leaveForExit(m);
            } else {
                if (self.release_io) |io| local_fs.releaseMappingPages(m.bytes, io);
                m.unmap();
            }
        };
        // Joined before its buffers go: a batch may still be landing in them.
        if (self.s3_fetcher) |f| f.stop();
        for (self.s3_mapped) |buf| s3_window.unmapBuffer(buf);
        for (self.s3_buffers) |buf| self.gpa.free(buf);
        for (self.owned_pools) |p| {
            p.deinit();
        }
    }
};

/// Probe the Bloom filters of the inputs whose footers were parsed from their mapped bytes (`materializeMetas`); S3
/// inputs were probed through the range fetcher while their fetch was planned.
pub fn loadLocalMembership(
    arena: std.mem.Allocator,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    scan_all: bool,
    /// `nanprobe.Options.compressed`: false for the aggregate scan, which evaluates its filter column first.
    probe_compressed: bool,
) !membership.Stats {
    const f = filter orelse return .{};
    if (scan_all) return .{};
    var local_metas: std.ArrayList(schema.FileMetaData) = .empty;
    var local_bytes: std.ArrayList([]const u8) = .empty;
    for (opened.inputs, opened.meta_ready, metas) |in, ready, m| {
        if (ready) continue;
        // A copy of the footer shares its row groups, so what is recorded on its chunks lands in `metas`.
        try local_metas.append(arena, m);
        try local_bytes.append(arena, in.bytes);
    }
    if (local_metas.items.len == 0) return .{};
    const mapped: membership.MappedSource = .{ .files = local_bytes.items };
    const stats = try membership.load(arena, opened.gpa, f, local_metas.items, mapped.source(), .{});
    // Float chunks the bounds would skip but for an unknown NaN count: proven free of NaN from their values.
    _ = try nanprobe.load(opened.gpa, f, local_metas.items, mapped.source(), .{ .compressed = probe_compressed });
    return stats;
}

pub fn materializeMetas(
    arena: std.mem.Allocator,
    opened: OpenedInputs,
    diag: ?*Diag,
    prof: ?*profile.Thread,
) ![]const schema.FileMetaData {
    var metas = try arena.alloc(schema.FileMetaData, opened.inputs.len);
    for (opened.inputs, 0..) |in, i| {
        if (opened.meta_ready[i]) {
            metas[i] = opened.metas[i];
            continue;
        }
        const span = profile.Span.begin(prof);
        defer span.end(.footer_parse, profile.args(.{ i, in.bytes.len }));
        metas[i] = metadata.open(arena, in.bytes) catch |err| {
            Diag.setInput(diag, in.name, err);
            return switch (err) {
                error.EncryptedFile, error.ExternalColumnChunk, error.InvalidDecimalType => err,
                else => error.NotParquet,
            };
        };
        if (opened.row_groups) |ranges| try restrictRowGroups(arena, &metas[i], ranges[i], diag, in.name);
    }
    return metas;
}

/// Keep row groups `range[0]` up to but not including `range[1]` of a footer, and the row count they hold. A range
/// past the footer's row groups is refused, naming the input, rather than clamped.
fn restrictRowGroups(
    arena: std.mem.Allocator,
    meta: *schema.FileMetaData,
    range: ?[2]u32,
    diag: ?*Diag,
    name: []const u8,
) !void {
    const r = range orelse return;
    const all = meta.row_groups.items;
    if (r[0] > r[1] or r[1] > all.len) {
        Diag.setInput(diag, name, error.RowGroupOutOfRange);
        return error.RowGroupOutOfRange;
    }
    const kept = try arena.dupe(schema.RowGroup, all[r[0]..r[1]]);
    var rows: i64 = 0;
    for (kept) |rg| rows +|= rg.num_rows;
    meta.row_groups = .fromOwnedSlice(kept);
    meta.num_rows = rows;
}

pub fn fetchS3Schema(
    ctx: Context,
    path: []const u8,
    arena: std.mem.Allocator,
) !schema.FileMetaData {
    const creds = try credentialsFromEnv(ctx.env);
    const url = s3.Url.parse(path) catch return error.BadInputUrl;

    const pool = try arena.create(s3.Pool(POOL_SIZE));
    try pool.init(ctx.gpa);
    defer pool.deinit();

    var read_exec = readExecutor(ctx.gpa);
    defer read_exec.deinit();
    var read_ctx = ctx;
    read_ctx.io = read_exec.io();

    var specs = [_]FetchSpec{.{ .url = url }};
    var account: ByteAccount = .{ .limit = std.math.maxInt(u64) };
    try fetchMetaBatch(read_ctx, arena, creds, pool, &specs, null, &account);

    // fetchMetaBatch's parsed metadata borrows string bytes from the
    // gpa-owned footer_region (thrift readString borrows, never copies).
    // Re-parse from an arena-owned copy so the gpa buffers can be freed
    // here instead of leaking out of this one-shot helper.
    const sp = &specs[0];
    sp.releaseTail(ctx.gpa);
    const footer_len = sp.footer_region.len - 8;
    const footer_copy = try arena.dupe(u8, sp.footer_region[0..footer_len]);
    ctx.gpa.free(sp.footer_region);
    return metadata.openFooter(arena, footer_copy);
}

pub fn openInputs(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    /// The S3 bytes the query may hold fetched at once (`s3FetchWindow`). Past it — or always, with `always` — S3
    /// inputs are fetched behind the consumer's row group gates instead of all up front.
    fetch_window: FetchWindow,
) !OpenedInputs {
    const paths = args.inputs;
    const prof = profile.threadOf(args.profiler, .main);
    if (args.profiler) |p| p.setFiles(paths);
    if (args.row_groups) |ranges| if (ranges.len != paths.len) return error.RowGroupOutOfRange;
    var inputs = try arena.alloc(scan.Input, paths.len);
    const metas = try arena.alloc(schema.FileMetaData, paths.len);
    const meta_ready = try arena.alloc(bool, paths.len);
    @memset(meta_ready, false);
    var mmaps = try arena.alloc(?local_fs.Mapped, paths.len);
    @memset(mmaps, null);

    // First pass: classify and group s3 URLs by bucket so we can build
    // one pool per bucket (lambda's PersistentPool already does this;
    // CLI does it inline here).
    var s3_indices: std.ArrayList(usize) = .empty;
    var local_indices: std.ArrayList(usize) = .empty;
    for (paths, 0..) |p, i| {
        if (std.mem.startsWith(u8, p, "s3://")) {
            try s3_indices.append(arena, i);
        } else {
            try local_indices.append(arena, i);
        }
    }

    // Local: mmap each, slice into bytes.
    for (local_indices.items) |i| {
        const span = profile.Span.begin(prof);
        const m = local_fs.mapFile(paths[i]) catch |err| {
            Diag.setInput(args.diag, paths[i], err);
            return switch (err) {
                error.EmptyFile => error.EmptyFile,
                error.NameTooLong => error.PathTooLong,
                else => error.OpenFailed,
            };
        };
        mmaps[i] = m;
        inputs[i] = .{ .name = paths[i], .bytes = m.bytes, .logical_size = m.bytes.len };
        span.end(.open_file, profile.args(.{ i, m.bytes.len }));
    }

    // S3: gather per-bucket batches, one pool per bucket, fetch each
    // batch in parallel via Io.Group.
    var owned_pools: std.ArrayList(*s3.Pool(POOL_SIZE)) = .empty;
    // On success the returned `OpenedInputs` owns them; until then, every way out frees them.
    errdefer for (owned_pools.items) |p| p.deinit();
    var s3_buffers: std.ArrayList([]u8) = .empty;
    // Windowed fetches: the mapped file buffers, and the fetcher filling them (`io/s3_window.zig`).
    var s3_mapped: std.ArrayList([]align(s3_window.page) u8) = .empty;
    errdefer for (s3_mapped.items) |b| s3_window.unmapBuffer(b);
    var s3_fetcher: ?*S3Fetcher = null;
    var membership_stats: membership.Stats = .{};
    var footer_resident_peak: u64 = 0;
    const remote: RemoteInputs = .{
        .ctx = ctx,
        .arena = arena,
        .args = args,
        .fetch_window = fetch_window,
        .paths = paths,
        .prof = prof,
        .inputs = inputs,
        .metas = metas,
        .meta_ready = meta_ready,
        .mmaps = mmaps,
        .s3_indices = s3_indices,
        .owned_pools = &owned_pools,
        .s3_buffers = &s3_buffers,
        .s3_mapped = &s3_mapped,
        .s3_fetcher = &s3_fetcher,
        .membership_stats = &membership_stats,
        .footer_resident_peak = &footer_resident_peak,
    };
    if (s3_indices.items.len > 0) if (try remote.open()) |opened| return opened;

    return .{
        .gpa = ctx.gpa,
        .inputs = inputs,
        .metas = metas,
        .meta_ready = meta_ready,
        .has_preparsed_meta = s3_indices.items.len > 0,
        .row_groups = args.row_groups,
        .mmaps = mmaps,
        .owned_pools = try owned_pools.toOwnedSlice(arena),
        .s3_buffers = try s3_buffers.toOwnedSlice(arena),
        .membership = membership_stats,
        .leave_mappings = ctx.exit_after,
        .release_io = ctx.worker_io,
        .s3_fetcher = s3_fetcher,
        .s3_mapped = try s3_mapped.toOwnedSlice(arena),
        .footer_resident_peak = footer_resident_peak,
    };
}

const RemoteInputs = struct {
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
    fetch_window: FetchWindow,
    paths: []const []const u8,
    prof: ?*profile.Thread,
    inputs: []scan.Input,
    metas: []schema.FileMetaData,
    meta_ready: []bool,
    mmaps: []?local_fs.Mapped,
    s3_indices: std.ArrayList(usize),
    owned_pools: *std.ArrayList(*s3.Pool(POOL_SIZE)),
    s3_buffers: *std.ArrayList([]u8),
    s3_mapped: *std.ArrayList([]align(s3_window.page) u8),
    s3_fetcher: *?*S3Fetcher,
    membership_stats: *membership.Stats,
    footer_resident_peak: *u64,

    fn open(self: *const RemoteInputs) !?OpenedInputs {
        var windowed = false;
        const creds = try credentialsFromEnv(self.ctx.env);

        var read_exec = readExecutor(self.ctx.gpa);
        defer read_exec.deinit();
        var read_ctx = self.ctx;
        read_ctx.io = read_exec.io();

        // Parse and validate s3 URLs, group by bucket. The current rule:
        // all s3 inputs share one bucket per call (matches lambda's
        // existing constraint). Cross-bucket scans are a separate
        // feature.
        var s3_urls = try self.arena.alloc(s3.Url, self.s3_indices.items.len);
        var bucket: []const u8 = undefined;
        for (self.s3_indices.items, 0..) |idx, k| {
            s3_urls[k] = s3.Url.parse(self.paths[idx]) catch return error.BadInputUrl;
            if (k == 0) bucket = s3_urls[k].bucket;
            if (!std.mem.eql(u8, s3_urls[k].bucket, bucket)) return error.CrossBucketNotSupported;
        }

        // Resolve a pool — either persistent (Lambda) or freshly built
        // (CLI). Either way, after this we have a `*s3.Pool(POOL_SIZE)`
        // we can drive.
        const pool: *s3.Pool(POOL_SIZE) = if (self.ctx.pool_registry) |reg|
            try reg.ensure_for_bucket(reg.ctx, self.ctx.gpa, creds, bucket)
        else blk: {
            const p = try self.arena.create(s3.Pool(POOL_SIZE));
            try p.init(self.ctx.gpa);
            errdefer p.deinit();
            try self.owned_pools.append(self.arena, p);
            break :blk p;
        };

        // Fetch metadata (tail + head + footer) for every s3 file in
        // parallel. After this the per-file meta is parsed and we know
        // total file size + tail_start.
        var specs = try self.arena.alloc(FetchSpec, s3_urls.len);
        for (s3_urls, 0..) |u, k| specs[k] = .{
            .url = u,
            .file_index = self.s3_indices.items[k],
            .footer_only = self.fetch_window.footer_only,
        };
        // Retained suffixes and in-flight footer bodies share one budget: the caller's max, and never the fetch
        // window's 64 MiB floor when that floor is higher. A suffix that does not fit is dropped and those ranges
        // are fetched later. The streaming window (including a test's ZPQ_S3_WINDOW_KIB) stays the admission limit;
        // a suffix the budget can hold is still reused.
        var account: ByteAccount = .{ .limit = @min(s3_window.windowFor(self.args.max_memory), @as(u64, self.args.max_memory)) };
        const batch_span = profile.Span.begin(self.prof);
        fetchMetaBatch(read_ctx, self.arena, creds, pool, specs, self.args.profiler, &account) catch |err| {
            for (specs) |*sp| {
                sp.releaseTail(self.ctx.gpa);
                if (sp.file_buf.len > 0) self.ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) self.ctx.gpa.free(sp.footer_region);
            }
            return err;
        };
        batch_span.end(.footer_batch, profile.args(.{specs.len}));
        self.footer_resident_peak.* = account.peak.load(.monotonic);
        var specs_returned = false;
        errdefer if (!specs_returned) {
            for (specs) |*sp| {
                sp.releaseTail(self.ctx.gpa);
                // A windowed buffer is mapped, and unmapped with `s3_mapped`.
                if (sp.file_buf.len > 0 and !windowed) self.ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) self.ctx.gpa.free(sp.footer_region);
            }
            for (self.s3_buffers.items) |b| self.ctx.gpa.free(b);
        };

        // A worker's row groups, before anything plans or fetches from the footers.
        if (self.args.row_groups) |ranges| for (self.s3_indices.items, specs) |idx, *sp| {
            try restrictRowGroups(self.arena, &sp.meta, ranges[idx], self.args.diag, self.paths[idx]);
        };
        const metas_for_plan = try self.arena.alloc(schema.FileMetaData, specs.len);
        for (specs, metas_for_plan) |sp, *m| m.* = sp.meta;

        // Bloom filters first: what they prove lands on the footers' chunks, so the plan below and the scan workers
        // after it prune the same row groups.
        if (!self.fetch_window.footer_only and !self.args.scan_all) {
            const bound = try self.args.bound(self.arena, &metas_for_plan[0]);
            const filter = if (bound) |query| query.filter() else if (self.args.filter) |fs|
                (if (fs.len > 0) try bindFilter(self.arena, fs, &metas_for_plan[0], self.args) else null)
            else
                null;
            if (filter) |f| {
                // Bloom holds its own copies of what it probes. Cap them at the bytes the query can keep, not at the
                // fetch window's 64 MiB floor when that floor is above `--max-memory`.
                var src: S3Membership = .init(read_ctx, creds, pool, specs, @min(self.fetch_window.limit, account.limit));
                src.profiler = self.args.profiler;
                self.membership_stats.* = try membership.load(self.arena, self.ctx.gpa, f, metas_for_plan, src.source(), .{});
                // Reads only the float chunks it probes, never the row group's other columns.
                _ = try nanprobe.load(self.ctx.gpa, f, metas_for_plan, src.source(), .{ .batch_bytes = @min(self.fetch_window.limit, account.limit) });
            }
        }

        // Bound frontend requests share their binding with fetch planning and execution.
        const plan_span = profile.Span.begin(self.prof);
        var n_ranges: usize = 0;
        var n_merged: usize = 0;
        var n_rgs: usize = 0;
        var n_rgs_pruned: usize = 0;
        const plan = try planRanges(self.arena, self.args, metas_for_plan);
        // Bloom has already copied what it needed. Keep only suffix bytes a planned range still overlaps.
        for (specs) |*sp| trimRetainedSuffix(self.ctx.gpa, sp, plan, &account);

        // Per file: stat-prune RGs and collect raw byte ranges for every
        // (surviving RG) × (needed col). One planner then chooses the spans
        // for the whole dispatch. Coalescing matters
        // because column chunks in a parquet RG are written back-to-
        // back; fetching them as separate GETs spends most of the wall
        // in HTTP round-trip overhead even though the bytes are
        // physically adjacent.
        //
        // S3 inputs do NOT materialize a total_size-sized dense buffer.
        // Instead, merged source ranges are packed into a compact buffer
        // and the parsed metadata offsets are rebased to that layout.
        // Over a window's worth of bytes, an aggregate fetches as its workers go (`io/s3_window.zig`): each row group's
        // chunks get their own page-aligned stretch of the buffer, filled ahead of the workers and given back behind
        // them. Under it (and off Linux), everything is fetched before the scan starts.
        // Every file's original chunks, then one plan for the up-front dispatch. The wave target counts ranges in
        // every file. Merges stay inside a file. `need` is the bytes that plan would hold; past the window, each
        // file is planned again under the window cap and admitted a row group at a time.
        const saved = try self.arena.alloc(SavedRanges, specs.len);
        var all_pieces: std.ArrayList(coalescer.Piece) = .empty;
        var selected_all: u64 = 0;
        for (specs, saved, 0..) |*sp, *sv, fi| {
            var ranges: std.ArrayList(coalescer.Range) = .empty;
            const rg_first = try self.arena.alloc(usize, sp.meta.row_groups.items.len + 1);
            for (sp.meta.row_groups.items, 0..) |*rg, ri| {
                rg_first[ri] = ranges.items.len;
                const cols = plan.rowGroupColumns(rg, &sp.meta) orelse continue;
                try appendRgFetchRanges(self.arena, &ranges, rg, cols, plan.carriedFilters(rg, &sp.meta), sp.total_size);
            }
            rg_first[sp.meta.row_groups.items.len] = ranges.items.len;
            const gap_cap = coalescer.s3CoalesceGap(sp.rtt_ns, s3.S3_BYTES_PER_S);
            var selected: u64 = 0;
            var g: usize = 0;
            for (ranges.items, 0..) |r, ri| {
                while (g + 1 < rg_first.len and rg_first[g + 1] <= ri) g += 1;
                selected +|= r.len();
                const group: u32 = @intCast(g);
                try all_pieces.append(self.arena, .{
                    .object = @intCast(fi),
                    .group = group,
                    .start = r.start,
                    .end = r.end,
                    .gap_cap = gap_cap,
                });
            }
            sv.* = .{ .ranges = ranges.items, .rg_first = rg_first, .gap_cap = gap_cap, .selected = selected };
            selected_all +|= selected;
        }
        const upfront = try coalescer.coalesceDispatch(
            self.arena,
            all_pieces.items,
            s3.inFlight(self.ctx.env),
            ampCap(selected_all, self.fetch_window.limit),
            null,
        );
        if (s3_window.supported) {
            var need: u64 = 0;
            for (upfront) |s| need += s.len();
            windowed = need > self.fetch_window.limit or (self.fetch_window.always and need > 0);
        }
        if (self.fetch_window.footer_only) {
            const opened = try self.metadataOnly(specs, windowed, &account);
            specs_returned = true;
            return opened;
        }
        var win_order: std.ArrayList(*s3_window.RowGroup) = .empty;
        const win_files = try self.arena.alloc(s3_window.File, specs.len);

        var fetch_jobs: std.ArrayList(s3.FetchJob) = .empty;
        var span_at: usize = 0;
        var file_plan: RemoteFilePlan = .{
            .builder = self,
            .plan = plan,
            .windowed = windowed,
            .account = &account,
            .fetch_jobs = &fetch_jobs,
            .win_order = &win_order,
            .n_ranges = &n_ranges,
            .n_merged = &n_merged,
            .n_rgs = &n_rgs,
            .n_rgs_pruned = &n_rgs_pruned,
        };
        for (specs, win_files, saved, 0..) |*sp, *wf, sv, fi| {
            const obj: u32 = @intCast(fi);
            const span_lo = span_at;
            while (span_at < upfront.len and upfront[span_at].object == obj) span_at += 1;
            const file_spans = upfront[span_lo..span_at];
            try file_plan.run(sp, wf, sv, file_spans);
        }
        if (windowed) {
            const f = try self.arena.create(S3Fetcher);
            f.* = .{
                .window = .{
                    .gpa = self.ctx.gpa,
                    .order = win_order.items,
                    .limit = self.fetch_window.limit,
                    .batch = @max(1, self.fetch_window.limit / 4),
                    // Only a reader that may stop early (`always`: a print with a limit) fetches its first rows first.
                    .slow_start = self.fetch_window.always,
                },
                .pool = pool,
                .creds = creds,
                .workers = s3.inFlight(self.ctx.env),
                .profiler = self.args.profiler,
                .executor = readExecutor,
                .wave = .{ .cap = .init(s3.firstWaveCap(self.ctx.env)), .full = s3.inFlight(self.ctx.env) },
            };
            self.s3_fetcher.* = f;
        }

        if (plan_span.t != null) {
            var fetch_bytes: u64 = 0;
            for (fetch_jobs.items) |j| fetch_bytes += j.target.len;
            for (win_order.items) |rg| fetch_bytes += rg.bytes;
            plan_span.end(.plan_ranges, profile.args(.{
                n_ranges, n_merged, fetch_jobs.items.len, fetch_bytes, n_rgs, n_rgs_pruned,
            }));
        }

        // Parallel fetch all the column-chunk ranges across all files.
        // s3.fetchJobs handles the Io.Group fan-out + per-job pool
        // acquire/release.
        if (fetch_jobs.items.len > 0) {
            const fetch_span = profile.Span.begin(self.prof);
            // This call is the query's data wave. Footer and Bloom fetches do not share it.
            var data_wave: s3.DataWave = .{ .cap = .init(s3.firstWaveCap(self.ctx.env)), .full = s3.inFlight(self.ctx.env) };
            const fetched = try s3.fetchJobs(
                read_ctx.io,
                pool,
                self.ctx.gpa,
                self.arena,
                creds,
                fetch_jobs.items,
                s3.inFlight(self.ctx.env),
                self.args.profiler,
                &data_wave,
            );
            fetch_span.end(.fetch, profile.args(.{ fetch_jobs.items.len, fetched }));
            for (fetch_jobs.items) |j| try ensureHeadMagic(j.range.start, j.target);
        }

        // Build scan.Inputs. bytes is the compact per-file buffer with
        // all needed chunks materialized and metadata already rebased.
        for (self.s3_indices.items, 0..) |orig_idx, k| {
            self.inputs[orig_idx] = .{
                .name = self.paths[orig_idx],
                .bytes = specs[k].file_buf,
                .logical_size = specs[k].total_size,
                .gate = if (self.s3_fetcher.* != null) win_files[k].gate() else null,
            };
            // Ownership of these gpa buffers moves into `s3_buffers`. Clear the spec so the error path, which frees
            // both lists, cannot free one twice.
            if (@import("builtin").is_test) if (test_arm_s3_buffer_fail) |fa| {
                if (self.s3_buffers.items.len > 0 and self.s3_buffers.items.len == self.s3_buffers.capacity)
                    fa.fail_index = fa.alloc_index;
            };
            if (specs[k].file_buf.len > 0 and self.s3_fetcher.* == null) {
                try self.s3_buffers.append(self.arena, specs[k].file_buf);
                specs[k].file_buf = &.{};
            }
            if (specs[k].footer_region.len > 0) {
                try self.s3_buffers.append(self.arena, specs[k].footer_region);
                specs[k].footer_region = &.{};
            }
            self.metas[orig_idx] = specs[k].meta;
            self.meta_ready[orig_idx] = true;
        }
        specs_returned = true;
        if (self.s3_fetcher.*) |f| try f.start();

        return null;
    }

    fn metadataOnly(
        self: *const RemoteInputs,
        specs: []FetchSpec,
        windowed: bool,
        account: *ByteAccount,
    ) !OpenedInputs {
        for (specs, 0..) |*sp, k| {
            const idx = self.s3_indices.items[k];
            self.inputs[idx] = .{
                .name = self.paths[idx],
                .bytes = &.{},
                .logical_size = sp.total_size,
                .gate = if (windowed) metadataGate() else null,
            };
            // Copy borrowed footer strings into the arena, then release both responses.
            const copy = try self.arena.dupe(u8, sp.footer_region[0 .. sp.footer_region.len - 8]);
            self.metas[idx] = try metadata.openFooter(self.arena, copy);
            if (self.args.row_groups) |ranges|
                try restrictRowGroups(self.arena, &self.metas[idx], ranges[idx], self.args.diag, self.paths[idx]);
            self.meta_ready[idx] = true;
            releaseChargedTail(sp, self.ctx.gpa, account);
            self.ctx.gpa.free(sp.footer_region);
            sp.footer_region = &.{};
        }
        return .{
            .gpa = self.ctx.gpa,
            .inputs = self.inputs,
            .metas = self.metas,
            .meta_ready = self.meta_ready,
            .has_preparsed_meta = true,
            .mmaps = self.mmaps,
            .owned_pools = self.owned_pools.items,
            .s3_buffers = &.{},
            .leave_mappings = self.ctx.exit_after,
            .release_io = self.ctx.worker_io,
            .footer_resident_peak = self.footer_resident_peak.*,
        };
    }
};

const SavedRanges = struct {
    ranges: []const coalescer.Range,
    rg_first: []usize,
    gap_cap: u64,
    selected: u64,
};

const RemoteFilePlan = struct {
    builder: *const RemoteInputs,
    plan: fetch.RangePlan,
    windowed: bool,
    account: *ByteAccount,
    fetch_jobs: *std.ArrayList(s3.FetchJob),
    win_order: *std.ArrayList(*s3_window.RowGroup),
    n_ranges: *usize,
    n_merged: *usize,
    n_rgs: *usize,
    n_rgs_pruned: *usize,

    fn run(
        self: *RemoteFilePlan,
        sp: *FetchSpec,
        wf: *s3_window.File,
        sv: SavedRanges,
        file_spans: []const coalescer.PlannedSpan,
    ) !void {
        // The tail serves the footer parse and the copies below. A windowed copy is applied when the row group
        // is admitted, so that tail stays until the fetcher stops; every other way out frees it here.
        var keep_tail = false;
        defer if (!keep_tail) releaseChargedTail(sp, self.builder.ctx.gpa, self.account);
        const meta = &sp.meta;
        sp.rg_columns = try self.builder.arena.alloc(?[]const bool, meta.row_groups.items.len);
        wf.* = .{ .rgs = try self.builder.arena.alloc(?*s3_window.RowGroup, meta.row_groups.items.len) };
        @memset(wf.rgs, null);

        // Each row group's ranges, `ranges[rg_first[rg]..rg_first[rg + 1]]`.
        var ranges: std.ArrayList(coalescer.Range) = .empty;
        var filter_ranges: std.ArrayList(coalescer.Range) = .empty;
        const rg_first = try self.builder.arena.alloc(usize, meta.row_groups.items.len + 1);
        for (meta.row_groups.items, sp.rg_columns, 0..) |*rg, *rg_cols, ri| {
            rg_first[ri] = ranges.items.len;
            rg_cols.* = self.plan.rowGroupColumns(rg, meta);
            self.n_rgs.* += 1;
            const cols = rg_cols.* orelse {
                self.n_rgs_pruned.* += 1;
                continue;
            };
            // EVERY needed chunk goes into the range list — including ones inside the prefetched tail — so the
            // compact buffer covers it and offset rebasing can find it. Bytes at or past tail_start are
            // satisfied by copying from the tail below (no re-fetch); only the sub-tail_start portion becomes
            // a fetch job.
            try appendRgFetchRanges(self.builder.arena, &ranges, rg, cols, self.plan.carriedFilters(rg, meta), sp.total_size);
            for (cols, 0..) |needed, ci| {
                if (!needed or ci >= rg.columns.items.len) continue;
                if (!self.plan.isFilterCol(ci)) continue;
                const cm = rg.columns.items[ci].meta_data orelse continue;
                const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
                const len: u64 = @intCast(cm.total_compressed_size);
                try filter_ranges.append(self.builder.arena, .{ .start = start, .end = start + len });
            }
        }
        rg_first[meta.row_groups.items.len] = ranges.items.len;

        if (ranges.items.len == 0) {
            sp.file_buf = &.{};
            return;
        }
        // Up front, `upfront` is the one plan for every file: the wave target counted all of them, and each
        // span stays inside its file. A windowed file is planned on its own under the window cap, one row group
        // to a buffer. That charge is what admission adds. Nothing merges again at dispatch.
        var compact: std.ArrayList(CompactRange) = .empty;
        var compact_len: usize = 0;
        const page = std.heap.pageSize();
        // Per row group, `compact.items[rg_compact[rg]..rg_compact[rg + 1]]` (windowed only).
        const rg_compact = try self.builder.arena.alloc(usize, meta.row_groups.items.len + 1);
        var window_plan: []const coalescer.PlannedSpan = &.{};
        if (self.windowed) {
            var pieces: std.ArrayList(coalescer.Piece) = .empty;
            var g: usize = 0;
            for (sv.ranges, 0..) |r, ri| {
                while (g + 1 < sv.rg_first.len and sv.rg_first[g + 1] <= ri) g += 1;
                try pieces.append(self.builder.arena, .{
                    .object = 0,
                    .group = @intCast(g),
                    .start = r.start,
                    .end = r.end,
                    .gap_cap = sv.gap_cap,
                });
            }
            window_plan = try coalescer.coalesceDispatch(
                self.builder.arena,
                pieces.items,
                s3.inFlight(self.builder.ctx.env),
                sv.selected *| 2,
                self.builder.fetch_window.limit,
            );
            self.n_ranges.* += sv.ranges.len;
            self.n_merged.* += window_plan.len;
        } else {
            self.n_ranges.* += sv.ranges.len;
            self.n_merged.* += file_spans.len;
            for (file_spans) |s| {
                if (s.end < s.start or s.end > sp.total_size) return error.BadResponse;
                try compact.append(self.builder.arena, .{ .start = s.start, .end = s.end, .dst_start = compact_len });
                compact_len += @intCast(s.len());
            }
        }
        if (self.windowed) {
            const nrg = meta.row_groups.items.len;
            const bucket_n = try self.builder.arena.alloc(usize, nrg);
            @memset(bucket_n, 0);
            for (window_plan) |s| bucket_n[s.group] += 1;
            const start_ix = try self.builder.arena.alloc(usize, nrg);
            var acc: usize = 0;
            for (bucket_n, start_ix) |bn, *slot| {
                slot.* = acc;
                acc += bn;
            }
            const cursor = try self.builder.arena.alloc(usize, nrg);
            @memcpy(cursor, start_ix);
            const order = try self.builder.arena.alloc(usize, window_plan.len);
            for (window_plan, 0..) |s, i| {
                order[cursor[s.group]] = i;
                cursor[s.group] += 1;
            }
            for (0..nrg + 1) |ri| {
                rg_compact[ri] = compact.items.len;
                if (ri == nrg) break;
                if (bucket_n[ri] == 0) continue;
                compact_len = std.mem.alignForward(usize, compact_len, page);
                for (order[start_ix[ri] .. start_ix[ri] + bucket_n[ri]]) |si| {
                    const s = window_plan[si];
                    if (s.end < s.start or s.end > sp.total_size) return error.BadResponse;
                    try compact.append(self.builder.arena, .{ .start = s.start, .end = s.end, .dst_start = compact_len });
                    compact_len += @intCast(s.len());
                }
            }
        }
        sp.compact_ranges = compact.items;
        if (self.windowed) {
            const buf = try s3_window.mapBuffer(compact_len);
            errdefer s3_window.unmapBuffer(buf);
            try self.builder.s3_mapped.append(self.builder.arena, buf);
            sp.file_buf = buf[0..compact_len];
        } else sp.file_buf = try self.builder.ctx.gpa.alloc(u8, compact_len);
        try rebaseFetchedOffsets(
            &sp.meta,
            self.plan.columns(),
            sp.compact_ranges,
            sp.rg_columns,
            if (self.windowed) rg_compact else null,
        );

        const file_jobs_from = self.fetch_jobs.items.len;
        for (0..meta.row_groups.items.len) |ri| {
            if (!self.windowed and ri > 0) break;
            const span = if (self.windowed) sp.compact_ranges[rg_compact[ri]..rg_compact[ri + 1]] else sp.compact_ranges;
            if (span.len == 0) continue;
            var rg_jobs: std.ArrayList(s3.FetchJob) = .empty;
            var copies: std.ArrayList(s3_window.Copied) = .empty;
            var job_bytes: u64 = 0;
            var copy_bytes: u64 = 0;
            for (span) |r| {
                const source_len: usize = @intCast(r.end - r.start);
                const dst = sp.file_buf[r.dst_start .. r.dst_start + source_len];

                const fetch_end = @min(r.end, sp.tail_start);
                if (r.start < fetch_end) {
                    const list = if (self.windowed) &rg_jobs else self.fetch_jobs;
                    const before = list.items.len;
                    try appendSplitFetch(list, self.builder.arena, sp.url.bucket, sp.url.key, r.start, fetch_end, dst[0..@intCast(fetch_end - r.start)]);
                    for (list.items[before..]) |j| job_bytes += j.target.len;
                }

                if (r.end > sp.tail_start) {
                    const tail_copy_start = @max(r.start, sp.tail_start);
                    const dst_off: usize = @intCast(tail_copy_start - r.start);
                    const tail_off: usize = @intCast(tail_copy_start - sp.tail_start);
                    const copy_len: usize = @intCast(r.end - tail_copy_start);
                    if (tail_off + copy_len > sp.tail.len) return error.BadResponse;
                    const src = sp.tail[tail_off .. tail_off + copy_len];
                    if (self.windowed) {
                        try copies.append(self.builder.arena, .{ .dst = dst[dst_off .. dst_off + copy_len], .src = src });
                        copy_bytes += copy_len;
                    } else @memcpy(dst[dst_off .. dst_off + copy_len], src);
                }
            }
            if (copies.items.len > 0) keep_tail = true;
            if (self.windowed) try orderFilterFirst(self.builder.arena, rg_jobs.items, filter_ranges.items);
            const rg_bytes = job_bytes + copy_bytes;
            if (!self.windowed or rg_bytes == 0) continue;
            // The row group's pages: from its page-aligned start to the next row group's.
            const seg_start = span[0].dst_start;
            const last = span[span.len - 1];
            const seg_end = @min(
                std.mem.alignForward(usize, last.dst_start + @as(usize, @intCast(last.end - last.start)), page),
                self.builder.s3_mapped.items[self.builder.s3_mapped.items.len - 1].len,
            );
            const wrg = try self.builder.arena.create(s3_window.RowGroup);
            wrg.* = .{
                .jobs = rg_jobs.items,
                .copied = copies.items,
                .bytes = rg_bytes,
                .gap = coalescer.s3CoalesceGap(sp.rtt_ns, s3.S3_BYTES_PER_S),
                .seg = @alignCast(self.builder.s3_mapped.items[self.builder.s3_mapped.items.len - 1][seg_start..seg_end]),
            };
            wf.rgs[ri] = wrg;
            try self.win_order.append(self.builder.arena, wrg);
        }
        if (keep_tail) {
            const owned: []u8 = @constCast(sp.tail);
            self.builder.s3_buffers.append(self.builder.arena, owned) catch |err| {
                keep_tail = false;
                return err;
            };
            sp.tail = &.{};
        }
        if (!self.windowed) try orderFilterFirst(self.builder.arena, self.fetch_jobs.items[file_jobs_from..], filter_ranges.items);
    }
};

pub fn hasGatedInputs(inputs: []const scan.Input) bool {
    for (inputs) |in| if (in.gate != null) return true;
    return false;
}

pub fn metadataGate() scan.RowGroupGate {
    const Gate = struct {
        var marker: u8 = 0;
        fn wait(_: *anyopaque, _: usize) anyerror!void {
            return error.MetadataOnly;
        }
        fn done(_: *anyopaque, _: usize, _: usize) void {
            unreachable;
        }
    };
    return .{ .ctx = &Gate.marker, .wait = Gate.wait, .done = Gate.done };
}

/// How the scan reads this query's local files: `policy`'s choice, given how much of them the page cache holds.
/// The mapping when there are none, or when any input is remote (the S3 path plans its own reads).
pub fn allLocal(opened: OpenedInputs) bool {
    if (opened.mmaps.len == 0) return false;
    for (opened.mmaps) |m| if (m == null) return false;
    return true;
}

/// Residency of the bytes the query will read, each file weighted by its planned pages. A failed mincore counts
/// that file's pages as not cached. No plan is share 0, so it does not take the cached mapping.
pub fn plannedResidency(gpa: std.mem.Allocator, opened: OpenedInputs, plan: ?LocalPlan) f64 {
    const lp = plan orelse return 0;
    var weight: local_reader.Weight = .{};
    for (lp.files, opened.mmaps) |per_rg, m_opt| {
        const m = m_opt orelse continue;
        var list: std.ArrayList(local_reader.Range) = .empty;
        defer list.deinit(gpa);
        for (per_rg) |rs_opt| {
            const rs = rs_opt orelse continue;
            list.appendSlice(gpa, rs) catch return 0;
        }
        if (list.items.len == 0) continue;
        weight.add(local_reader.sampleRanges(gpa, m.bytes, list.items));
    }
    return weight.share();
}

/// Per local input, per row group, the byte ranges the scan reads (null when pruned): needed chunks and their page
/// indexes. What the reader reads, and what `auto` weighs against the files' size.
pub const LocalPlan = struct {
    files: []const []const ?[]local_reader.Range,
    planned: u64,
    total: u64,
    /// The most one row group reads: the reader holds a row group's planned bytes whole.
    largest: u64 = 0,

    pub fn coverage(self: LocalPlan) f64 {
        if (self.total == 0) return 0;
        return @min(1.0, @as(f64, @floatFromInt(self.planned)) / @as(f64, @floatFromInt(self.total)));
    }
};

pub fn planLocalReads(
    arena: std.mem.Allocator,
    args: QueryArgs,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
) !LocalPlan {
    const plan = try planRanges(arena, args, metas);
    const files = try arena.alloc([]const ?[]local_reader.Range, metas.len);
    var planned: u64 = 0;
    var total: u64 = 0;
    var largest: u64 = 0;
    for (files, metas, opened.mmaps) |*file, *meta, m_opt| {
        total += if (m_opt) |m| m.bytes.len else 0;
        const per_rg = try arena.alloc(?[]local_reader.Range, meta.row_groups.items.len);
        for (meta.row_groups.items, per_rg) |*rg, *rgs| {
            const cols = plan.rowGroupColumns(rg, meta) orelse {
                rgs.* = null;
                continue;
            };
            var rs: std.ArrayList(local_reader.Range) = .empty;
            var rg_bytes: u64 = 0;
            defer largest = @max(largest, rg_bytes);
            for (cols, 0..) |needed, ci| {
                if (!needed or ci >= rg.columns.items.len) continue;
                const chunk = rg.columns.items[ci];
                const cm = chunk.meta_data orelse continue;
                const start: u64 = @intCast(if (cm.dictionary_page_offset) |dp| @min(dp, cm.data_page_offset) else cm.data_page_offset);
                try rs.append(arena, .{ .start = start, .end = start + @as(u64, @intCast(cm.total_compressed_size)) });
                planned +|= @intCast(cm.total_compressed_size);
                rg_bytes +|= @intCast(cm.total_compressed_size);
                // The scan reads a needed chunk's page index too.
                if (chunk.column_index_offset) |o| if (chunk.column_index_length) |l| {
                    try rs.append(arena, .{ .start = @intCast(o), .end = @as(u64, @intCast(o)) + @as(u64, @intCast(l)) });
                };
                if (chunk.offset_index_offset) |o| if (chunk.offset_index_length) |l| {
                    try rs.append(arena, .{ .start = @intCast(o), .end = @as(u64, @intCast(o)) + @as(u64, @intCast(l)) });
                };
            }
            rgs.* = rs.items;
        }
        file.* = per_rg;
    }
    return .{ .files = files, .planned = planned, .total = total, .largest = largest };
}

/// Start reading every local input's planned ranges, row group by row group, and return the inputs the scan should use:
/// each local one over the reader's buffer, gated per row group.
pub fn planLocalReader(
    ctx: Context,
    arena: std.mem.Allocator,
    opened: OpenedInputs,
    local_plan: LocalPlan,
    cfg: local_reader.Config,
    out: *?*local_reader.Reader,
) ![]const scan.Input {
    const rd = try local_reader.Reader.create(ctx.gpa, cfg);
    out.* = rd;
    const inputs = try arena.dupe(scan.Input, opened.inputs);
    for (inputs, local_plan.files, opened.mmaps) |*in, per_rg, m_opt| {
        const m = m_opt orelse continue;
        // Through the mapping's own descriptor: the footer and plan came from that file, whatever the path names now.
        // One that gave it back is opened again, and read only if the path still names that file as it was.
        const f = if (m.fd) |fd| try rd.borrowFile(fd, m.bytes.len) else blk: {
            const f = try rd.openFile(in.name, m.bytes.len);
            const now = local_fs.fdStamp(f.fd) catch return error.InputChanged;
            if (!std.meta.eql(now, m.stamp)) return error.InputChanged;
            break :blk f;
        };
        try rd.planFile(f, per_rg, COALESCE_GAP);
        in.bytes = f.bytes();
        in.gate = f.gate();
        in.plan_bytes = m.bytes;
        in.dict_wait = .{ .ctx = f, .wait = local_reader.File.probeWait };
    }
    // Started by the scan once it has planned (`scan.ReadStart`).
    return inputs;
}

/// `scan.DictProbe` for local inputs: a reader's input reads the range ahead of its row groups; a mapped one whose
/// first probed range is not all in the page cache (a cut steered to the mapping, no io_uring, a narrow cold read) is
/// told to read its ranges in now, so its dictionaries come in at once instead of fault by fault. A `mincore` of a few
/// ranges per file decides: a whole-file residency pass cost a warm short query more than the advice saves, and one
/// range is not enough, as the first sits in the page the magic read brought in.
pub const DictProbeSink = struct {
    inputs: []const scan.Input,
    mmaps: []const ?local_fs.Mapped,
    /// Per file: ranges found resident so far, until `checks`; `cold` once one is not, `warm` after `checks`.
    cold: []Cold = &.{},

    pub const Cold = enum(u8) { cold = 0xff, warm = 0xfe, _ };
    const checks = 4;

    pub fn add(ctx: *anyopaque, file: usize, rg: usize, start: u64, end: u64) anyerror!void {
        const self: *DictProbeSink = @ptrCast(@alignCast(ctx));
        const in = self.inputs[file];
        if (@import("builtin").os.tag == .linux) if (in.dict_wait) |w| {
            const f: *local_reader.File = @ptrCast(@alignCast(w.ctx));
            return f.reader.probe(f, rg, start, end);
        };
        if (in.gate != null) return;
        const m = self.mmaps[file] orelse return;
        const page = std.heap.pageSize();
        const lo = std.mem.alignBackward(u64, start, page);
        const hi = @min(std.mem.alignForward(u64, end, page), std.mem.alignForward(u64, m.bytes.len, page));
        if (hi <= lo) return;
        const ptr: [*]align(std.heap.page_size_min) u8 = @ptrCast(@alignCast(@constCast(m.bytes.ptr + lo)));
        const c = &self.cold[file];
        if (c.* != .cold and c.* != .warm) {
            const seen = @backingInt(c.*);
            const next: Cold = @fromBackingInt(@intCast(seen + 1));
            c.* = if (!allResident(ptr, hi - lo)) .cold else if (seen + 1 == checks) .warm else next;
        }
        // Advice only: a refusal leaves the pages to fault in as they would have.
        if (c.* == .cold) _ = std.posix.system.madvise(ptr, @intCast(hi - lo), std.posix.MADV.WILLNEED);
    }

    /// Whether the range's first pages (up to 64) are all in the page cache; false where it cannot tell (off Linux).
    fn allResident(ptr: [*]align(std.heap.page_size_min) u8, len: u64) bool {
        if (@import("builtin").os.tag != .linux) return false;
        var vec: [64]u8 = undefined;
        const page = std.heap.pageSize();
        const pages: usize = @intCast(@min((len + page - 1) / page, vec.len));
        if (std.os.linux.errno(std.os.linux.mincore(ptr, pages * page, &vec)) != .SUCCESS) return false;
        for (vec[0..pages]) |v| if (v & 1 == 0) return false;
        return true;
    }
};

/// `scan.ReadStart` for the local reader.
pub const ReadStarter = struct {
    rd: *local_reader.Reader,
    started: bool = false,

    pub fn start(ctx: *anyopaque) anyerror!void {
        const self: *ReadStarter = @ptrCast(@alignCast(ctx));
        if (self.started) return;
        self.started = true;
        // Off Linux the reader never exists (`on_linux`), and its io_uring must not be compiled in.
        if (@import("builtin").os.tag == .linux) try self.rd.start();
    }

    pub fn abandon(ctx: *anyopaque) void {
        const self: *ReadStarter = @ptrCast(@alignCast(ctx));
        if (@import("builtin").os.tag == .linux) self.rd.abandon();
    }

    pub fn planned(ctx: *anyopaque) void {
        const self: *ReadStarter = @ptrCast(@alignCast(ctx));
        if (@import("builtin").os.tag == .linux) self.rd.releasePruned();
    }
};

/// Armed by the footer-ownership test so the next `s3_buffers` growth returns `error.OutOfMemory`.
pub var test_arm_s3_buffer_fail: ?*std.testing.FailingAllocator = null;
