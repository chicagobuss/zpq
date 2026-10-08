//! ZPQ's one query entry point.
//!
//! Both binaries (the CLI and the Lambda) are thin shells over
//! `runQuery(ctx, args) -> QueryResult`. Their job is purely:
//!   - CLI: argv → QueryArgs, format JSON to stdout, exit
//!   - Lambda: Runtime API JSON → QueryArgs, format JSON response, loop
//! Everything in between — input dispatch, parallel fetch, decode,
//! scan, output — lives here.
//!
//! The architecture comes from the comparables synthesis in
//! `docs/measurements/comparables.md`: a per-file `scan.Input`
//! abstraction at the engine level (Polars' `DynByteSource`) and a
//! storage-shaped helper underneath that materializes Inputs from any
//! backend (`local` mmap, `s3` parallel range-fetch). Single-file vs
//! multi-file is just `inputs.len`; no special cases.
//!
//! Sans-IO it's not — engine.zig drives the I/O. But the core of what
//! engine does (parse, plan, decode, fold) lives in `core/`. Engine is
//! the IO orchestration layer that turns "list of paths/URLs and a
//! query" into "the bytes the query needs, fetched optimally, then
//! decoded through `core.scan.runMultiAggregate`."

const std = @import("std");
const nowMonoNs = @import("clock.zig").monoNs;
const profile = @import("profile.zig");
const local_fs = @import("local_fs.zig");
const local_reader = @import("io/local_reader.zig");

const schema = @import("core/schema.zig");
const compression = @import("core/parquet/compression.zig");
const decimal_mod = @import("core/parquet/decimal.zig");
const huge_pages = @import("core/huge_pages.zig");
const consumer = @import("core/consumer.zig");
const invariant = @import("core/invariant.zig");
const scan = @import("core/scan.zig");
const Diag = @import("core/diag.zig").Diag;
const agg_plan = @import("core/agg_plan.zig");
const system = @import("core/system.zig");
const metadata = @import("core/parquet/metadata.zig");
const schema_tree = @import("core/parquet/schema_tree.zig");
const thrift = @import("core/thrift.zig");
const expr_ast = @import("core/expr/ast.zig");
const expr_parser = @import("core/expr/parser.zig");
const expr_agg = @import("core/expr/agg.zig");
const filter_eval = @import("core/filter/eval.zig");
const filter_ast = @import("core/filter/ast.zig");
const filter_parser = @import("core/filter/parser.zig");
const filter_prune = @import("core/filter/prune.zig");
const membership = @import("core/filter/membership.zig");
const sbbf = @import("core/parquet/bloom.zig");
const streaming = @import("core/writer/streaming.zig");
const fastpath = @import("core/writer/fastpath.zig");

const s3 = @import("io/s3.zig");
const tls = @import("io/tls.zig");
const http = @import("io/http.zig");
const sigv4 = @import("io/sigv4.zig");
const multipart_sink = @import("io/multipart_sink.zig");
const pool_mod = @import("io/pool.zig");
const coalescer = @import("io/coalescer.zig");
const work_cursor = @import("io/work_cursor.zig");
const AtomicWorkCursor = work_cursor.AtomicWorkCursor;
const meta_cache_mod = @import("io/meta_cache.zig");

/// Range-coalesce gap. Adjacent column-chunk ranges within a file are
/// physically near-contiguous on disk (parquet writes columns back-to-
/// back in a row group, separated only by a few bytes of internal
/// metadata). Merging anything within 64 KB collapses 27 separate GETs
/// per file into 1 — a 27× cut in HTTP request count for the
/// passthrough write case. The previous lambda code path used the
/// same value (verified 2026-05-07 against the pre-rewrite numbers).
const COALESCE_GAP: u64 = 64 * 1024;

pub const POOL_SIZE: usize = 8;
pub const TAIL_SIZE: u64 = 64 * 1024;

/// Permits for an S3 output's part uploads, apart from the input reads' so neither waits for the other, over the same
/// pool so uploads reuse the connections reads leave idle. As many as uploads had while they shared the read permits.
pub const OUTPUT_POOL_SIZE: usize = s3.MAX_PARTS;
pub const OutputLane = pool_mod.Lane(s3.Pool(POOL_SIZE), OUTPUT_POOL_SIZE);

/// Zig's 16 MiB default makes short-lived worker creation expensive, but this is deliberately not tuned to the minimum:
/// a real fetch descends into BoringSSL and libc `getaddrinfo`/NSS, whose stack use nothing here bounds.
const READ_WORKER_STACK_SIZE: usize = 4 << 20;

/// Fetch worker loops in the read stage. Equal to `POOL_SIZE` so one worker holds one permit and never parks in
/// `acquire`. A worker sleeping in retry backoff still holds its slot, so live concurrency can dip below this while a
/// host throttles.
pub const READ_CONCURRENCY: usize = POOL_SIZE;

/// Bounds threads, not just sockets: the process-wide `std.Io.Threaded` has `concurrent_limit = .unlimited`, and
/// `Io.Group.concurrent` spawns a fresh OS thread whenever every worker is busy — including one parked on a blocking
/// socket read — so pool permits alone bound sockets only. Both fetch sites start workers through `startWorkers`, so a
/// submission at the limit waits rather than failing the query; that matters because the metadata stage's workers can
/// still count as busy when the column stage starts. `deinit` joins every worker before the caller returns.
fn readExecutor(gpa: std.mem.Allocator) std.Io.Threaded {
    return std.Io.Threaded.init(gpa, .{
        .concurrent_limit = .limited(READ_CONCURRENCY),
        .stack_size = READ_WORKER_STACK_SIZE,
    });
}

pub const Error = error{
    NoInputs,
    EmptyAggregate,
    AggregateMutexWithSelect,
    MissingOutputOrAggregate,
    SchemaMismatch,
    NestedReencodeNotSupported,
    INT96ReencodeNotSupported,
    /// A row output column is a nested LIST/MAP leaf, or an expression over one.
    NestedRowOutputNotSupported,
    /// A nested leaf to output under its label has none: a top-level column takes even its quoted path.
    UnlabelledColumn,
    FooterSchemaChunkMismatch,
    CrossBucketNotSupported,
    BadInputUrl,
    BadOutputUrl,
    NoCredentials,
    /// Neither `AWS_REGION` nor `S3_REGION` is set; signed and anonymous S3 requests both need one.
    NoRegion,
    BadResponse,
    TailTooSmall,
    NotParquet,
    AggSumOverflow,
    OpenFailed,
    EmptyFile,
    PathTooLong,
    ExceededMemoryBudget,
    ColumnMustBeGrouped,
    GroupKeyAliasRequired,
    DuplicateOutputColumn,
    AmbiguousOutputColumn,
    /// `--bloom-filter` names a column no reader filters on (BOOLEAN) or zpq cannot hash as stored (a nested column,
    /// a FIXED_LEN_BYTE_ARRAY DECIMAL); `Diag.column` names it.
    BloomFilterUnsupportedColumn,
    /// `--bloom-filter` on a query that writes no rows (an aggregate).
    BloomFilterNeedsRows,
} || std.mem.Allocator.Error;

pub const QueryArgs = struct {
    inputs: []const []const u8, // raw paths/URLs (pre-glob — caller globs)
    output: ?[]const u8 = null,
    filter: ?[]const u8 = null,
    aggregate: ?[]const u8 = null,
    group_by: ?[]const u8 = null,
    columns: ?[]const []const u8 = null,
    select: ?[]const u8 = null,
    /// GROUP BY output columns as a select list writes them (SQL).
    select_cols: ?[]const expr_ast.SelectColumn = null,
    /// Comma-separated output column names for GROUP BY (CLI `--column-order`).
    column_order: ?[]const u8 = null,
    /// Receives what a failure was about; see `core/diag.zig`.
    diag: ?*Diag = null,
    /// Requested output codec. Null when none was asked for: re-encoded
    /// output is then SNAPPY and byte copies keep each chunk's own codec.
    codec: ?schema.CompressionCodec = null,
    /// How hard zstd works on the output (`--compress-priority`, the Lambda's "compress_priority").
    compress_priority: CompressPriority = .size,
    parallelism: usize = 0,
    /// `--scan-all`: disable every stats shortcut (row-group pruning,
    /// stats-driven column drop, aggregate stat short-circuit) and decode
    /// every page/byte. Slower but thorough — the paranoid path, and the
    /// correctness valve for files whose writer emitted untrustworthy stats.
    scan_all: bool = false,
    /// `--trust-stats`: opt in to answering min/max/sum from file statistics
    /// (the fast path) instead of decoding. Off by default — file stats can be
    /// inaccurate (seen from parquet-mr/Spark), so trusting them risks a
    /// silently wrong answer. `count(*)` is always answered from num_rows.
    /// Row-group pruning still uses stats regardless (that path only skips
    /// provably-non-matching groups, so it can't produce a wrong value).
    trust_stats: bool = false,
    /// `--fast-levels`: skip materialising definition levels for data
    /// pages proven to have no nulls. Off by default — it is new, and a
    /// wrong answer here would be silent. See `core/parquet/column.zig`
    /// (`fast_levels`) for what the detection actually proves.
    fast_levels: bool = false,
    /// Encoded execution in aggregate scans (`DecodeOptions.encoded`). `ZPQ_ENCODED=0` in the environment turns it
    /// off too, so one binary can be compared against itself.
    encoded: bool = true,
    /// `--unordered` (the Lambda's `"ordered": false`): GROUP BY rows in no particular order, floating-point
    /// aggregates summed in whatever order workers finish (`MultiAggArgs.unordered`).
    unordered: bool = false,
    max_memory: usize = 512 * 1024 * 1024,
    /// Records the query's spans when set (`--profile`, the Lambda's `"profile"`); see `profile.zig`.
    profiler: ?*profile.Profiler = null,
    /// Output columns to write split-block Bloom filters for (`--bloom-filter`). A copied chunk keeps the filter it
    /// has; one without is decoded to build one.
    bloom_filters: []const BloomSpec = &.{},
};

pub const CompressPriority = compression.CompressPriority;

/// One `--bloom-filter col[:fpp]`: a written column and its filter's false-positive probability.
pub const BloomSpec = struct {
    column: []const u8,
    fpp: f64 = default_fpp,

    /// What parquet-java, Arrow and Hardwood default to.
    pub const default_fpp = 0.01;

    /// `col`, or `col:fpp` with fpp strictly between 0 and 1. A suffix that is no number is part of the name.
    pub fn parse(text: []const u8) error{BadBloomFilter}!BloomSpec {
        const name = std.mem.trim(u8, text, " ");
        if (std.mem.lastIndexOfScalar(u8, name, ':')) |colon| {
            if (std.fmt.parseFloat(f64, name[colon + 1 ..])) |fpp| {
                if (!(fpp > 0 and fpp < 1) or colon == 0) return error.BadBloomFilter;
                return .{ .column = name[0..colon], .fpp = fpp };
            } else |_| {}
        }
        if (name.len == 0) return error.BadBloomFilter;
        return .{ .column = name };
    }
};

pub const Context = struct {
    gpa: std.mem.Allocator,
    env: std.process.Environ,
    io: std.Io,
    /// Optional long-lived pool registry — set by Lambda for warm-
    /// container connection reuse; null in CLI which builds a fresh
    /// pool per invocation.
    pool_registry: ?*PoolRegistry = null,
    /// Optional long-lived metadata cache. Lambda uses this to skip
    /// repeated footer fetches on warm-container repeat invocations
    /// against the same object (validated via If-None-Match
    /// conditional GET on every hit). CLI passes null.
    meta_cache: ?*meta_cache_mod.MetaCache = null,
    /// The process exits once this query's answer is out (the CLI). Input mappings and scan workers' regions are then
    /// left to the exit: unmapping a large populated mapping first holds the answer back by milliseconds, and the
    /// kernel tears it down at exit regardless. Never for a process that runs another query (the Lambda).
    exit_after: bool = false,
};

/// Vtable so both lambda's `PersistentPool` and an inline-CLI
/// implementation satisfy the same shape: get-or-create a pool for a
/// given bucket. Lifetime is owned by whoever provides the registry.
pub const PoolRegistry = struct {
    ctx: *anyopaque,
    ensure_for_bucket: *const fn (
        ctx: *anyopaque,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) anyerror!*s3.Pool(POOL_SIZE),
    /// Upload permits over the pool `ensure_for_bucket` returns for `bucket`.
    ensure_output: *const fn (
        ctx: *anyopaque,
        gpa: std.mem.Allocator,
        creds: s3.Credentials,
        bucket: []const u8,
    ) anyerror!*OutputLane,
};

pub const QueryResult = union(enum) {
    aggregate: AggResult,
    write: WriteResult,

    pub fn deinit(self: *QueryResult, gpa: std.mem.Allocator) void {
        switch (self.*) {
            .aggregate => |*ar| ar.deinit(gpa),
            .write => {},
        }
    }
};

pub const WriteResult = struct {
    /// The codec every written column chunk uses; null when they differ (a
    /// byte copy, with no codec requested, of inputs stored in several).
    codec: ?schema.CompressionCodec,
    files_in: usize,
    rows_in: i64,
    rows_kept: i64,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    row_groups_kept: usize,
    /// Bloom filter probing: what it read and the row groups it ruled out beyond statistics.
    membership: membership.Stats = .{},
    timings: Timings,
};

pub const AggResult = struct {
    files_in: usize,
    rows_in: i64,
    rows_kept: i64,
    bytes_in: u64,
    bytes_out: u64,
    row_groups_in: usize,
    row_groups_pruned: usize,
    row_groups_full_match: usize,
    cols_stat_pruned: usize,
    /// Bloom filter probing: what it read and the row groups it ruled out beyond statistics.
    membership: membership.Stats = .{},
    timings: Timings,
    /// The answer, owned in `Context.gpa`.
    output: scan.AggOutput,

    pub fn deinit(self: *AggResult, gpa: std.mem.Allocator) void {
        self.output.deinit(gpa);
    }
};

pub const Timings = struct {
    read_ns: u64 = 0,
    parse_ns: u64 = 0,
    core: consumer.Timings = .{},
    /// Wall-clock of the parallel scan region (see scan.Timings.decode_wall_ns).
    decode_wall_ns: u64 = 0,
    footer_ns: u64 = 0,
    /// Multipart-upload-only counters; zero for local-fd output.
    /// `complete_ns` is the CompleteMultipartUpload round-trip
    /// (currently a fresh non-pooled TLS connect — opportunity).
    /// `await_ns` is how long the close path blocked draining
    /// in-flight part workers.
    mp_complete_ns: u64 = 0,
    mp_await_ns: u64 = 0,
};

pub fn runQuery(ctx: Context, args: QueryArgs) !QueryResult {
    const span = profile.Span.begin(profile.threadOf(args.profiler, .main));
    defer span.end(.query, profile.args(.{}));
    if (args.inputs.len == 0) return error.NoInputs;
    if ((args.aggregate != null or args.group_by != null) and (args.select != null or args.columns != null)) {
        return error.AggregateMutexWithSelect;
    }
    if (args.aggregate != null or args.group_by != null) {
        if (args.bloom_filters.len > 0) return error.BloomFilterNeedsRows;
        const agg_str = args.aggregate orelse "";
        return .{ .aggregate = try runAggregate(ctx, args, agg_str) };
    }
    if (args.output) |out_path| {
        return .{ .write = try runWrite(ctx, args, out_path) };
    }
    return error.MissingOutputOrAggregate;
}

// ============================================================
// Aggregate path
// ============================================================

fn runAggregate(ctx: Context, args: QueryArgs, agg_str: []const u8) !AggResult {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var t: Timings = .{};

    // 1. Open all inputs — local mmap or s3 parallel range-fetch.
    //    This step already does column pruning for s3 (parses the
    //    aggregate to know which leaves to fetch), so by the time we
    //    return, every Input.bytes contains exactly what the scan
    //    needs.
    const prof = profile.threadOf(args.profiler, .main);
    const t_open = nowMonoNs();
    var opened = try openInputs(ctx, arena, args);
    defer opened.deinit();
    const t_open_end = nowMonoNs();
    t.read_ns = @intCast(t_open_end - t_open);
    profile.record(prof, .open_inputs, t_open, t_open_end, profile.args(.{args.inputs.len}));

    // 2. Run the orchestrator. (Yes, this re-parses the aggregate —
    //    cheap, microseconds. Easier than threading the parsed AST
    //    through the open step + the scan step.) Footers parsed here are probed here; the scan probes the ones it
    //    parses.
    //    Local files may be read explicitly rather than faulted in (`io/local_reader.zig`); their footers are then
    //    parsed here from the mapping, since the scan's copy of the bytes holds only what the reader plans.
    const io_policy = local_reader.Policy.fromEnv(ctx.env);
    // Off Linux everything below is comptime-dead, so the reader (io_uring, futexes) is never compiled in there.
    const on_linux = @import("builtin").os.tag == .linux;
    const all_local = on_linux and allLocal(opened);
    const resident: f64 = if (all_local and io_policy.profile == .auto) localResidency(ctx.gpa, opened) else 0;
    // Anything but the plain mapping needs the footers and the plan here: the explicit modes to read by them, and
    // `auto` over uncached files to weigh how much of them the query reads.
    const plan_io = all_local and (io_policy.profile == .pread or io_policy.profile == .uring or
        (io_policy.profile == .auto and resident < 0.8));
    var probed = opened.membership;
    const metas: ?[]const schema.FileMetaData = if (opened.has_preparsed_meta or plan_io) blk: {
        const m = try materializeMetas(arena, opened, args.diag, prof);
        const filter: ?filter_ast.Filter = if (args.filter) |fs|
            (if (fs.len > 0) try filter_parser.parse(arena, fs, &m[0], args.diag) else null)
        else
            null;
        probed.add(try loadLocalMembership(arena, opened, m, filter, args.scan_all));
        break :blk m;
    } else null;
    const local_plan: ?LocalPlan = if (plan_io) try planLocalReads(arena, args, opened, metas.?) else null;
    const coverage: f64 = if (local_plan) |lp| lp.coverage() else 0;
    const io_mode: local_reader.Mode = if (all_local) io_policy.choose(resident, coverage) else .mmap;
    profile.record(prof, .io_choice, nowMonoNs(), nowMonoNs(), profile.args(.{
        io_policy.profile,
        io_mode,
        @as(u64, @intFromFloat(resident * 1000)),
        @as(u64, if (local_plan != null) @intFromFloat(coverage * 1000) else std.math.maxInt(u32)),
    }));
    // Declared after `opened`, so it is torn down (its threads joined) before the mappings go.
    var reader: ?*local_reader.Reader = null;
    defer if (reader) |rd| rd.destroy();
    const scan_inputs = if (io_mode == .mmap) opened.inputs else try startLocalReader(
        ctx,
        arena,
        opened,
        local_plan.?,
        .{ .mode = io_mode, .threads = io_policy.threads, .window = io_policy.window },
        &reader,
    );
    const t_reader = nowMonoNs();
    defer if (reader) |rd| profile.record(prof, .local_read, t_reader, nowMonoNs(), profile.args(.{
        @as(u64, if (io_mode == .uring) 2 else 1),
        rd.stats.bytes.load(.monotonic),
        rd.threads.items.len,
        rd.stats.stalls.load(.monotonic),
        rd.stats.stall_ns.load(.monotonic),
        rd.stats.window_waits.load(.monotonic),
    }));
    if (reader) |rd| {
        const b = rd.backends();
        profile.record(prof, .local_backend, t_reader, t_reader, profile.args(.{
            b.uring_threads,
            b.pread_threads,
            b.direct_files,
            b.buffered_files,
        }));
    }
    var r = try scan.runMultiAggregate(ctx.gpa, arena, .{
        .inputs = scan_inputs,
        .metas = metas,
        .filter = args.filter,
        .aggregate = agg_str,
        .group_by = args.group_by,
        .parallelism = args.parallelism,
        .scan_all = args.scan_all,
        .trust_stats = args.trust_stats,
        .fast_levels = args.fast_levels,
        .encoded = args.encoded and !envDisables(ctx.env, "ZPQ_ENCODED"),
        .leave_regions = ctx.exit_after,
        .unordered = args.unordered,
        .select_cols = args.select_cols,
        .column_order = args.column_order,
        .max_memory = args.max_memory,
        .diag = args.diag,
        .profiler = args.profiler,
    });
    t.parse_ns = r.timings.parse_ns;
    t.core = r.timings.core;
    t.decode_wall_ns = r.timings.decode_wall_ns;
    probed.add(r.membership);

    defer r.state.deinit(ctx.gpa);
    errdefer r.output.deinit(ctx.gpa);

    // 3. Optional 1-row parquet output. Aggregate output is local-only;
    //    S3 writes use the row-group streaming path.
    var bytes_out: u64 = 0;
    if (args.output) |out_path| {
        if (std.mem.startsWith(u8, out_path, "s3://")) {
            return error.BadOutputUrl;
        }
        const t_footer = nowMonoNs();
        const st = r.state;
        const codec = args.codec orelse .SNAPPY;
        const zstd: compression.ZstdPlan = .{ .lambda = args.compress_priority.lambda() };
        bytes_out = if (r.output.group_rows) |rows|
            try writeGroupedParquet(arena, out_path, r.output.group_cols.?, st.group_col_types.?, rows, codec, zstd)
        else
            try writeOneRowParquet(arena, out_path, st.agg_calls, st.accumulators, st.agg_sources, codec, zstd);
        const t_footer_end = nowMonoNs();
        t.footer_ns += @intCast(t_footer_end - t_footer);
        profile.record(prof, .footer_write, t_footer, t_footer_end, profile.args(.{bytes_out}));
    }

    return .{
        .files_in = r.files_in,
        .rows_in = r.rows_in,
        .rows_kept = r.rows_kept,
        .bytes_in = r.bytes_in,
        .bytes_out = bytes_out,
        .row_groups_in = r.row_groups_in,
        .row_groups_pruned = r.row_groups_pruned,
        .row_groups_full_match = r.row_groups_full_match,
        .cols_stat_pruned = r.cols_stat_pruned,
        .membership = probed,
        .timings = t,
        .output = r.output,
    };
}

// ============================================================
// Write path — projection / select / filter, output to local or s3
// ============================================================

const PAR1: [4]u8 = .{ 'P', 'A', 'R', '1' };

// ── Windowed parallel re-encode (Io.Group.async + sliding window) ─────
// Producers decode+filter+encode whole row groups concurrently, bounded by a
// CPU budget; a single in-order writer drains their buffers to the
// sink, rebasing each row group's file offsets. A sliding window (an
// Io.Semaphore of W permits) caps how many encoded row groups may be in flight
// at once, so peak memory = W × rg_bytes regardless of total output size. W is
// sized from the environment by the caller: generously from the owned memory
// tier on Lambda, a small polite multiple of the worker count elsewhere.
//
// Idiom: this is the same std.Io concurrency the S3 paths already use, applied
// to CPU work — replacing raw std.Thread.spawn + buffer-all. (See the
// probe_io_concurrency / probe_backpressure spikes for why async + async_limit,
// not concurrent, and why a sliding window rather than byte credits.)

/// A Sink that appends into an in-memory buffer (offsets relative to 0).
const BufSink = struct {
    buf: *std.ArrayListUnmanaged(u8),
    gpa: std.mem.Allocator,
    fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *BufSink = @ptrCast(@alignCast(ctx));
        try self.buf.appendSlice(self.gpa, bytes);
    }
};

/// Address space one re-encoded row group reserves for its working memory (committed only as touched; past it, the
/// general allocator).
const ENCODE_REGION_RESERVE: usize = 4 << 30;

/// A re-encode producer's working memory, two huge-page regions kept from one row group to the next so their pages
/// are faulted once per write rather than once per row group: the projected rows in `rows`, and in `work` first the
/// decoded columns, then, once those are dead, the encoded chunks. A row group's working set is tens to hundreds of
/// MiB of short-lived buffers, which on the general allocator cost a page fault per 4 KiB on every thread.
const WorkRegions = struct {
    rows: huge_pages.Region,
    work: huge_pages.Region,
    /// Claimed by one producer at a time; there are as many as producers can run at once.
    busy: std.atomic.Value(bool) = .init(false),

    fn init(gpa: std.mem.Allocator) WorkRegions {
        return .{
            .rows = .init(gpa, ENCODE_REGION_RESERVE),
            .work = .init(gpa, ENCODE_REGION_RESERVE),
        };
    }

    fn deinit(self: *WorkRegions) void {
        self.rows.deinit();
        self.work.deinit();
    }

    fn claim(all: []WorkRegions) ?*WorkRegions {
        for (all) |*r| if (r.busy.cmpxchgStrong(false, true, .acquire, .monotonic) == null) return r;
        return null;
    }
};

const RGJob = struct {
    bytes: []const u8, // source file bytes containing this RG
    meta: *const schema.FileMetaData, // owning file's metadata (shared, read-only)
    rg: *const schema.RowGroup, // the source row group
    /// Stats prove every row passes the filter (Decision.always_match) and the
    /// query is byte-copy-eligible → copy the chunk verbatim instead of
    /// decode→filter→encode. Set only when safe (see bytecopy_ok in runWrite).
    passthrough: bool = false,
    /// Input file and row group (counted across files), for profile spans.
    file_index: usize = 0,
    rg_index: usize = 0,
};

const EncodedRG = struct {
    bytes: []u8 = &.{}, // encoded RG bytes (gpa-owned; writer frees after sink.write)
    rg: ?schema.RowGroup = null, // footer meta, offsets relative to 0 (meta_arena-owned)
    surviving: i64 = 0,
    err: ?anyerror = null,
    /// The producer's decode/eval/encode time for this row group, summed by the writer.
    timings: consumer.Timings = .{},
};

const Slot = struct {
    enc: EncodedRG = .{},
    /// Private per-producer arena for this RG's (small, long-lived) footer
    /// metadata — no cross-thread sharing, so no locking. Freed after the
    /// footer is written. The bulk bytes live on gpa and are freed early.
    meta_arena: std.heap.ArenaAllocator,
    done: std.atomic.Value(bool) = .init(false),
};

/// Encode one row group into a gpa-owned byte buffer with offsets relative to
/// 0; footer metadata is allocated on `meta_alloc` (the slot's private arena).
/// The working memory is `regions`, emptied again before returning.
fn encodeRGToBuffer(
    gpa: std.mem.Allocator,
    meta_alloc: std.mem.Allocator,
    regions: *WorkRegions,
    job: RGJob,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
    decode_options: consumer.DecodeOptions,
    prof: ?*profile.Thread,
    bloom: consumer.BloomWrite,
) !EncodedRG {
    // `rows` holds what lives for the whole row group (the aggregator's frame, the filter's columns and selection);
    // each output column is projected, encoded and written out of `work`, rewound before the next, so the working
    // set is one column rather than the row group (whose first touch, on every producer, is page faults).
    const work_region = &regions.work;
    defer work_region.rewind();
    defer regions.rows.rewind();
    var scratch = std.heap.ArenaAllocator.init(regions.rows.allocator());
    defer scratch.deinit();
    var timings: consumer.Timings = .{ .prof = prof };
    var agg = try consumer.initOutputAggregator(scratch.allocator(), job.meta, output_specs);
    agg.keep_codes = true;
    var projected: consumer.ProjectedRG = undefined;
    try projected.init(
        &agg,
        scratch.allocator(),
        job.rg,
        job.meta,
        .{ .bytes = job.bytes, .byte_origin = 0 },
        filter,
        fetch_arr,
        output_specs,
        &timings,
        decode_options,
    );
    defer projected.deinit();
    // The output waits here for its turn at the sink, so every row group's is fresh memory: in huge pages, a fault per
    // 2 MiB rather than per 4 KiB. Freed by the writer through `gpa`, to which the advisor forwards frees.
    var advisor: huge_pages.HugePageAdvisor = .{ .child = gpa };
    const out_gpa = advisor.allocator();
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    errdefer buf.deinit(out_gpa);
    var bufsink = BufSink{ .buf = &buf, .gpa = out_gpa };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&bufsink), .write_fn = BufSink.writeFn };
    var local_off: u64 = 0;
    var rg_enc: consumer.RGEncoder = .{
        .out_arena = meta_alloc,
        .sink = sink,
        .out_offset = &local_off,
        .codec = codec,
        .zstd = zstd,
        .bloom_write = bloom,
        .timings = &timings,
    };
    if (projected.surviving > 0) {
        // The output is about as big as the compressed chunks it comes from: reserve that once rather than growing
        // the buffer by doubling (each step a copy, and fresh pages to fault).
        var estimate: usize = 0;
        for (job.rg.columns.items, 0..) |cc, ci| {
            const m = cc.meta_data orelse continue;
            if (ci < fetch_arr.len and fetch_arr[ci]) estimate += @intCast(@max(0, m.total_compressed_size));
        }
        try buf.ensureTotalCapacity(out_gpa, estimate);
        agg.num_rows = projected.surviving;
        for (0..output_specs.len) |i| {
            work_region.rewind();
            var col_arena = std.heap.ArenaAllocator.init(work_region.allocator());
            defer col_arena.deinit();
            agg.arena = col_arena.allocator();
            const span = profile.Span.begin(prof);
            try projected.projectColumn(&agg, i, col_arena.allocator(), &timings);
            span.end(.project, profile.args(.{ projected.surviving, 1 }));
            try rg_enc.column(&agg, i, col_arena.allocator());
            // Its rows lived on `col_arena`: nothing may read them after this.
            agg.cols[i] = switch (agg.cols[i]) {
                inline else => |_, tag| @unionInit(consumer.OutputAggregator.ColumnBuf, @tagName(tag), .{}),
            };
        }
    }
    const out = rg_enc.finish(projected.surviving);
    const owned = try buf.toOwnedSlice(out_gpa);
    timings.prof = null;
    return .{ .bytes = owned, .rg = out.rg, .surviving = out.surviving_rows, .timings = timings };
}

/// Byte-copy a fully-passing row group's kept columns into a buffer (no decode
/// or re-encode) — the filter fast path. Same buffer/relative-offset shape as
/// `encodeRGToBuffer` so the windowed writer places it identically.
fn copyRGToBuffer(
    gpa: std.mem.Allocator,
    meta_alloc: std.mem.Allocator,
    job: RGJob,
    kept_set: ?[]const bool,
    prof: ?*profile.Thread,
    bloom_leaves: []const ?f64,
) !EncodedRG {
    var buf: std.ArrayListUnmanaged(u8) = .empty;
    var bufsink = BufSink{ .buf = &buf, .gpa = gpa };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&bufsink), .write_fn = BufSink.writeFn };
    var local_off: u64 = 0;
    var timings: consumer.Timings = .{ .prof = prof };
    const src: consumer.RGSrc = .{ .bytes = job.bytes, .byte_origin = 0 };
    const out = try consumer.copyRG(meta_alloc, job.rg, src, kept_set, sink, &local_off, &timings, false, null, .{
        .fpp = bloom_leaves,
        .meta = job.meta,
        .scratch = gpa,
    });
    const owned = try buf.toOwnedSlice(gpa);
    timings.prof = null;
    return .{ .bytes = owned, .rg = out.rg, .surviving = out.surviving_rows, .timings = timings };
}

const WinCtx = struct {
    const writer = winWriter;
    const producer = winProducer;

    gpa: std.mem.Allocator,
    io: std.Io = undefined, // set in runWindowed once the runtime exists
    jobs: []const RGJob,
    filter: ?filter_ast.Filter,
    fetch_arr: []const bool,
    output_specs: []const consumer.OutputCol,
    kept_set: ?[]const bool, // for byte-copy passthrough jobs
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    decode_options: consumer.DecodeOptions,
    /// The Bloom filters asked for: per output column for re-encoded row groups, per source leaf for copied ones.
    bloom_outputs: []const ?f64 = &.{},
    bloom_leaves: []const ?f64 = &.{},
    slots: []Slot,
    /// One per producer that can run at once.
    regions: []WorkRegions,
    window: *std.Io.Semaphore, // W permits = max row groups in flight
    completions: *std.Io.Semaphore, // "a slot finished, re-check" signal
    cpu: std.Io.Semaphore = .{}, // producers running at once; sized in runWindowed

    // Writer-owned (single writer → no races); read back after the fan-out.
    sink: streaming.Sink,
    out_offset: u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    arena: std.mem.Allocator, // writer-only append target
    rows_kept: i64 = 0,
    rgs_kept: usize = 0,
    sink_ns: u64 = 0,
    /// Producers' decode/eval/encode time, summed as their row groups drain.
    core: consumer.Timings = .{},
    first_err: ?anyerror = null,

    inflight: std.atomic.Value(usize) = .init(0),
    peak_bytes: std.atomic.Value(usize) = .init(0),
    profiler: ?*profile.Profiler = null,
};

fn winProducer(w: *WinCtx, i: usize) void {
    const ma = w.slots[i].meta_arena.allocator();
    const prof = profile.threadOf(w.profiler, .encode);
    const span = profile.Span.begin(prof);
    defer span.end(.encode_rg, profile.args(.{ w.jobs[i].file_index, w.jobs[i].rg_index, w.jobs[i].passthrough }));
    // `cpu` admits no more producers than there are regions, so a claim fails only if that invariant breaks; then
    // the job pays for fresh ones.
    var spare: ?WorkRegions = null;
    defer if (spare) |*r| r.deinit();
    const regions = WorkRegions.claim(w.regions) orelse blk: {
        spare = .init(w.gpa);
        break :blk &spare.?;
    };
    defer regions.busy.store(false, .release);
    const enc = (if (w.jobs[i].passthrough)
        copyRGToBuffer(w.gpa, ma, w.jobs[i], w.kept_set, prof, w.bloom_leaves)
    else
        encodeRGToBuffer(
            w.gpa,
            ma,
            regions,
            w.jobs[i],
            w.filter,
            w.fetch_arr,
            w.output_specs,
            w.codec,
            w.zstd,
            w.decode_options,
            prof,
            .{ .fpp = w.bloom_outputs },
        )) catch |e| {
        w.slots[i].enc = .{ .err = e };
        w.slots[i].done.store(true, .release);
        w.completions.post(w.io);
        return;
    };
    profile.gauge(prof, .buffered_bytes, @intCast(enc.bytes.len));
    const now = w.inflight.fetchAdd(enc.bytes.len, .acq_rel) + enc.bytes.len;
    var p = w.peak_bytes.load(.monotonic);
    while (now > p) p = w.peak_bytes.cmpxchgWeak(p, now, .monotonic, .monotonic) orelse break;
    w.slots[i].enc = enc;
    w.slots[i].done.store(true, .release);
    w.completions.post(w.io);
}

/// In-order writer: drains slots 0..N, rebasing offsets, writing to the sink,
/// then freeing the bulk bytes and posting a window permit (sliding forward).
fn winWriter(w: *WinCtx) void {
    const prof = profile.threadOf(w.profiler, .writer);
    var next: usize = 0;
    while (next < w.slots.len) : (next += 1) {
        while (!w.slots[next].done.load(.acquire)) w.completions.waitUncancelable(w.io);
        const enc = w.slots[next].enc;
        w.core.decode_ns += enc.timings.decode_ns;
        w.core.eval_ns += enc.timings.eval_ns;
        w.core.encode_ns += enc.timings.encode_ns;
        if (enc.err) |e| {
            if (w.first_err == null) w.first_err = e;
        } else if (enc.rg) |rg| {
            if (enc.surviving > 0) {
                var rgm = rg;
                consumer.rebaseRowGroupOffsets(&rgm, @intCast(w.out_offset));
                const t0 = nowMonoNs();
                w.sink.write(enc.bytes) catch |e| {
                    if (w.first_err == null) w.first_err = e;
                };
                const t1 = nowMonoNs();
                w.sink_ns += @intCast(t1 - t0);
                profile.record(prof, .sink_write, t0, t1, profile.args(.{enc.bytes.len}));
                w.out_offset += enc.bytes.len;
                w.rows_kept += enc.surviving;
                w.rgs_kept += 1;
                w.new_row_groups.append(w.arena, rgm) catch |e| {
                    if (w.first_err == null) w.first_err = e;
                };
            }
        }
        if (enc.bytes.len > 0) w.gpa.free(enc.bytes);
        profile.gauge(prof, .buffered_bytes, -@as(i64, @intCast(enc.bytes.len)));
        _ = w.inflight.fetchSub(enc.bytes.len, .acq_rel);
        w.window.post(w.io);
    }
}

fn runWindowedReencode(w: *WinCtx, async_budget: usize) !void {
    return runWindowed(WinCtx, w, w.jobs.len, async_budget);
}

/// Drive the fan-out: `Ctx.writer` on a dedicated thread plus `async_budget` concurrent `Ctx.producer` calls on pool
/// threads, admitting a new producer only once the window has room (memory back-pressure) and a running one has
/// finished (CPU budget).
///
/// The writer's thread counts against the executor's `async_limit`, and a producer submitted at that limit runs inline
/// on this dispatching thread, which then admits nothing until it finishes, idling every other slot. So the CPU budget
/// is the `cpu` semaphore, and the limit leaves room for the writer and for as many producers again that have released
/// their permit but not yet their thread. Exceeding that takes more than `async_budget` producers finishing within the
/// moment a thread takes to return, which only near-empty row groups do, and their inline run is as short.
fn runWindowed(comptime Ctx: type, w: *Ctx, n_jobs: usize, async_budget: usize) !void {
    var threaded = std.Io.Threaded.init(w.gpa, .{ .async_limit = .limited(1 + 2 * async_budget) });
    defer threaded.deinit();
    w.io = threaded.io();
    w.cpu = .{ .permits = async_budget };

    const Run = struct {
        fn producer(ctx: *Ctx, i: usize) void {
            defer ctx.cpu.post(ctx.io);
            Ctx.producer(ctx, i);
        }
    };
    var group: std.Io.Group = .init;
    defer group.cancel(w.io);
    try group.concurrent(w.io, Ctx.writer, .{w});
    for (0..n_jobs) |i| {
        w.window.waitUncancelable(w.io);
        w.cpu.waitUncancelable(w.io);
        group.async(w.io, Run.producer, .{ w, i });
    }
    try group.await(w.io);
    if (w.first_err) |e| return e;
}

/// A byte copy that changes codec: producers recompress whole row groups in parallel, and the in-order writer places
/// them through `consumer.copyRG`, which writes straight to the sink at final offsets, so page indexes carry over
/// exactly as on the serial copy.
const RecompressCtx = struct {
    const writer = recompressWriter;
    const producer = recompressProducer;

    gpa: std.mem.Allocator,
    io: std.Io = undefined, // set in runWindowed once the runtime exists
    jobs: []const RGJob,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan = .{},
    /// Threads each producer recompresses its row group's pages with: the CPU budget shared among the row groups that
    /// run at once, so a file of one row group still uses all of it.
    page_workers: usize = 1,
    /// Bloom filter probability per leaf (`--bloom-filter`), for copied chunks that carry none.
    bloom_leaves: []const ?f64 = &.{},
    slots: []RecompressSlot,
    window: *std.Io.Semaphore,
    completions: *std.Io.Semaphore,
    cpu: std.Io.Semaphore = .{},

    // Writer-owned; read back after the fan-out.
    sink: streaming.Sink,
    out_offset: u64,
    new_row_groups: *std.ArrayListUnmanaged(schema.RowGroup),
    arena: std.mem.Allocator, // writer-only: footer metadata
    rows_kept: i64 = 0,
    rgs_kept: usize = 0,
    core: consumer.Timings = .{},
    first_err: ?anyerror = null,
    profiler: ?*profile.Profiler = null,
};

const RecompressSlot = struct {
    /// Private to the producer until `done`, then to the writer, which frees it once the row group is written.
    arena: std.heap.ArenaAllocator,
    chunks: []?consumer.RecompressedChunk = &.{},
    encode_ns: u64 = 0,
    err: ?anyerror = null,
    done: std.atomic.Value(bool) = .init(false),
};

fn recompressProducer(w: *RecompressCtx, i: usize) void {
    const prof = profile.threadOf(w.profiler, .encode);
    const span = profile.Span.begin(prof);
    defer span.end(.encode_rg, profile.args(.{ w.jobs[i].file_index, w.jobs[i].rg_index, false }));
    const slot = &w.slots[i];
    var timings: consumer.Timings = .{ .prof = prof };
    const job = w.jobs[i];
    if (consumer.recompressRowGroup(
        slot.arena.allocator(),
        w.gpa,
        job.rg,
        .{ .bytes = job.bytes, .byte_origin = 0 },
        w.kept,
        w.codec,
        w.zstd,
        &timings,
        w.page_workers,
    )) |chunks| {
        slot.chunks = chunks;
    } else |e| slot.err = e;
    slot.encode_ns = timings.encode_ns;
    slot.done.store(true, .release);
    w.completions.post(w.io);
}

fn recompressWriter(w: *RecompressCtx) void {
    const prof = profile.threadOf(w.profiler, .writer);
    w.core.prof = prof;
    defer w.core.prof = null;
    for (w.slots, w.jobs) |*slot, job| {
        while (!slot.done.load(.acquire)) w.completions.waitUncancelable(w.io);
        w.core.encode_ns += slot.encode_ns;
        if (slot.err) |e| {
            if (w.first_err == null) w.first_err = e;
        } else if (w.first_err == null) {
            const rg_span = profile.Span.begin(prof);
            defer rg_span.end(.copy_rg, profile.args(.{job.rg.total_byte_size}));
            if (consumer.copyRG(
                w.arena,
                job.rg,
                .{ .bytes = job.bytes, .byte_origin = 0 },
                w.kept,
                w.sink,
                &w.out_offset,
                &w.core,
                true,
                .{ .codec = w.codec, .zstd = w.zstd, .scratch = w.gpa, .done = slot.chunks },
                .{ .fpp = w.bloom_leaves, .meta = job.meta, .scratch = w.gpa },
            )) |out| {
                if (out.rg) |rg| w.new_row_groups.append(w.arena, rg) catch |e| {
                    w.first_err = e;
                };
                w.rows_kept += out.surviving_rows;
                if (out.surviving_rows > 0) w.rgs_kept += 1;
            } else |e| w.first_err = e;
        }
        slot.arena.deinit();
        slot.arena = std.heap.ArenaAllocator.init(w.gpa);
        w.window.post(w.io);
    }
}

const WriteWindow = struct {
    workers: usize,
    window: usize,
    /// The CPU budget `workers` was taken from (they are fewer when the window holds fewer row groups).
    budget: usize,
};

/// The write fan-out's two knobs:
///   - concurrency: explicit -j, else the Lambda- and quota-aware CPU budget;
///   - window: row groups in flight, from a byte budget for buffered output in every environment, so a large -j or
///     large row groups cannot retain many GB. On Lambda we own the container (about half the tier); elsewhere stay
///     polite (a modest cap, a small fraction of what is available).
/// A tight byte budget may cut the workers below the CPU budget: idle workers beat exceeding the memory cap.
fn writeWindow(ctx: Context, args: QueryArgs, jobs: []const RGJob) WriteWindow {
    const async_budget = if (args.parallelism > 0) args.parallelism else system.discoverAvailableParallelism(ctx.env);
    var sum_bytes: u64 = 0;
    for (jobs) |j| sum_bytes += @intCast(@max(@as(i64, 0), j.rg.total_byte_size));
    const avg_rg = @max(@as(u64, 1), sum_bytes / @max(@as(u64, 1), jobs.len));
    const inflight_budget: u64 = if (system.onLambda(ctx.env))
        system.discoverAvailableMemory(ctx.env) / 2
    else
        @min(@as(u64, 256) * 1024 * 1024, system.discoverAvailableMemory(ctx.env) / 8);
    const window = system.windowCapacity(inflight_budget, avg_rg, jobs.len);
    return .{ .workers = @min(async_budget, window), .window = window, .budget = async_budget };
}

/// True when some kept chunk of some row group is stored in another codec than `codec`.
fn anyChunkNeedsRecompress(
    metas: []const schema.FileMetaData,
    kept: ?[]const bool,
    codec: schema.CompressionCodec,
) bool {
    for (metas) |*m| for (m.row_groups.items) |*rg| for (rg.columns.items, 0..) |*col, ci| {
        if (kept) |k| if (ci >= k.len or !k[ci]) continue;
        const md = col.meta_data orelse continue;
        if (md.codec != codec) return true;
    };
    return false;
}

/// True when every kept (output) column chunk of `rg` is already stored in
/// `codec` — the precondition for a byte-copy passthrough to honor a requested
/// --codec without re-encoding.
fn keptColumnsUseCodec(rg: *const schema.RowGroup, kept: []const bool, codec: schema.CompressionCodec) bool {
    for (rg.columns.items, 0..) |*col, ci| {
        if (ci < kept.len and !kept[ci]) continue;
        const md = col.meta_data orelse return false;
        if (md.codec != codec) return false;
    }
    return true;
}

/// Set `set[i]` for each leaf a `--columns` name selects. A name that selects nothing is `UnknownColumn`, named
/// through `diag`, as in --select and --filter: dropping it wrote a file short of that column, or of every column.
/// A list naming no column at all (`--columns ','`) is `EmptyColumnList`, not a file with no columns.
fn markColumns(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    names: []const []const u8,
    set: []bool,
    diag: ?*Diag,
) !void {
    if (names.len == 0) return error.EmptyColumnList;
    for (names) |name| {
        const indices = metadata.resolveProjection(arena, meta, name) catch |err| {
            if (err == error.AmbiguousColumn) Diag.setColumn(diag, name);
            return err;
        };
        if (indices.len == 0) {
            Diag.setColumn(diag, name);
            return error.UnknownColumn;
        }
        for (indices) |idx| if (idx < set.len) {
            set[idx] = true;
        };
    }
}

/// Output names of a file's leaves (`metadata.leafLabels`), computed once per query on first use: labelling a
/// leaf at a time walks the schema per leaf.
const Labels = struct {
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    diag: ?*Diag,
    all: ?metadata.LeafLabels = null,

    fn get(self: *Labels) !metadata.LeafLabels {
        if (self.all == null) self.all = try metadata.leafLabels(self.arena, self.meta);
        return self.all.?;
    }

    /// The label of leaf `leaf_idx`, reporting a leaf that has none: one a top-level column takes even the quoted
    /// path of.
    fn of(self: *Labels, leaf_idx: usize) ![]const u8 {
        const all = try self.get();
        if (leaf_idx >= all.names.len) return error.SchemaMismatch;
        return all.names[leaf_idx] orelse {
            const segments = (try metadata.leafPathSegments(self.arena, self.meta, leaf_idx)).?;
            Diag.setColumn(self.diag, try metadata.quotePath(self.arena, segments));
            return error.UnlabelledColumn;
        };
    }

    /// The label of a nested leaf, or null for a top-level column — whose label is its own name, dots included.
    fn ofNested(self: *Labels, leaf_idx: usize) !?[]const u8 {
        const all = try self.get();
        if (leaf_idx >= all.nested.len or !all.nested[leaf_idx]) return null;
        return try self.of(leaf_idx);
    }
};

/// The name each output column of a flat row result is printed under: an alias, or `metadata.leafLabel` of a
/// passthrough leaf, which no other leaf prints as.
fn outputColumnNames(
    arena: std.mem.Allocator,
    labels: *Labels,
    specs: []const consumer.OutputCol,
) ![]const []const u8 {
    const names = try arena.alloc([]const u8, specs.len);
    for (specs, names) |spec, *name| name.* = switch (spec) {
        .passthrough => |ci| try labels.of(ci),
        .computed => |c| c.alias,
    };
    return names;
}

/// S3 credentials from the environment, telling a missing region apart from missing keys.
/// Whether `name` is set to "0" in `env`.
fn envDisables(env: std.process.Environ, name: []const u8) bool {
    const v = env.getPosix(name) orelse return false;
    return std.mem.eql(u8, v, "0");
}

fn credentialsFromEnv(env: std.process.Environ) error{ NoCredentials, NoRegion }!s3.Credentials {
    return s3.Credentials.fromEnv(env) catch |err| if (err == error.NoRegion) error.NoRegion else error.NoCredentials;
}

fn sameName(name: []const u8) []const u8 {
    return name;
}

/// What a write or print outputs and reads, parsed from --columns / --select / --filter against the first file's
/// footer. Output-format restrictions, nested printing, and sink and codec choice stay with each consumer.
const ProjectionPlan = struct {
    /// Null without --select.
    select_items: ?[]expr_ast.SelectItem,
    filter: ?filter_ast.Filter,
    /// The --columns selection; null when no --columns narrows the leaves.
    kept_set: ?[]const bool,
    /// The leaves kept: `kept_set`, or every leaf.
    kept: []const bool,
    output_specs: []const consumer.OutputCol,
    /// Some output column is an expression or a renamed leaf rather than a leaf copied under its own name.
    any_computed: bool,
    /// Leaves to decode: those the output references plus the filter's.
    fetch: []const bool,
    labels: Labels,

    /// The name each output column is printed under.
    fn names(self: *ProjectionPlan, arena: std.mem.Allocator) ![]const []const u8 {
        return outputColumnNames(arena, &self.labels, self.output_specs);
    }
};

fn planProjection(arena: std.mem.Allocator, args: QueryArgs, meta0: *const schema.FileMetaData) !ProjectionPlan {
    const num_leaves = metadata.leafCount(meta0);

    // --columns resolves to leaf indices; --select is mutually exclusive with it (checked by the callers).
    var kept_set: ?[]bool = null;
    if (args.columns) |cols_list| {
        const set = try arena.alloc(bool, num_leaves);
        @memset(set, false);
        try markColumns(arena, meta0, cols_list, set, args.diag);
        kept_set = set;
    }
    const select_items: ?[]expr_ast.SelectItem = if (args.select) |s|
        try expr_parser.parseSelect(arena, s, meta0, args.diag)
    else
        null;
    var filter: ?filter_ast.Filter = null;
    if (args.filter) |fs| if (fs.len > 0) {
        filter = try filter_parser.parse(arena, fs, meta0, args.diag);
    };

    const kept = try arena.alloc(bool, num_leaves);
    if (kept_set) |s| @memcpy(kept, s) else @memset(kept, true);
    const fetch = try arena.alloc(bool, num_leaves);
    @memset(fetch, false);

    var labels: Labels = .{ .arena = arena, .meta = meta0, .diag = args.diag };
    var output_specs: std.ArrayList(consumer.OutputCol) = .empty;
    var any_computed = false;
    if (select_items) |items| {
        for (items) |item| {
            switch (item.expr) {
                .col_ref => |c| {
                    // The select output is flat, so a nested leaf is written under its label (its dotted path)
                    // rather than its leaf name, which a top-level column may share.
                    const nested_name: ?[]const u8 =
                        if (item.alias == null) try labels.ofNested(c.col_idx) else null;
                    if (item.alias orelse nested_name) |alias| {
                        try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                        any_computed = true;
                    } else {
                        try output_specs.append(arena, .{ .passthrough = c.col_idx });
                    }
                    if (c.col_idx < num_leaves) fetch[c.col_idx] = true;
                },
                else => {
                    const alias = item.alias orelse return error.MissingOutputOrAggregate; // alias required
                    try output_specs.append(arena, .{ .computed = .{ .expr = item.expr, .alias = alias } });
                    any_computed = true;
                    item.expr.collectColumns(fetch);
                },
            }
        }
    } else {
        for (kept, 0..) |b, i| if (b) {
            try output_specs.append(arena, .{ .passthrough = i });
            fetch[i] = true;
        };
    }
    if (filter) |f| {
        var filter_cols: std.ArrayList(usize) = .empty;
        try f.collectColumns(&filter_cols, arena);
        for (filter_cols.items) |ci| {
            if (ci < num_leaves) fetch[ci] = true;
        }
    }
    // A --select names its outputs, so two may clash, as aggregate and GROUP BY outputs may; a plain copy keeps the
    // source's own names and is not checked.
    if (select_items != null) {
        const names = try outputColumnNames(arena, &labels, output_specs.items);
        try scan.checkDistinctOutputNames([]const u8, names, sameName, args.diag);
    }

    return .{
        .select_items = select_items,
        .filter = filter,
        .kept_set = kept_set,
        .kept = kept,
        .output_specs = output_specs.items,
        .any_computed = any_computed,
        .fetch = fetch,
        .labels = labels,
    };
}

/// The `--bloom-filter` columns of a write: false-positive probability by output column (re-encoded row groups) and by
/// source leaf (copied ones), null where none was asked for.
const BloomPlan = struct {
    outputs: []const ?f64 = &.{},
    leaves: []const ?f64 = &.{},
};

fn resolveBlooms(
    arena: std.mem.Allocator,
    args: QueryArgs,
    proj: *ProjectionPlan,
    meta0: *const schema.FileMetaData,
) !BloomPlan {
    if (args.bloom_filters.len == 0) return .{};
    const names = try proj.names(arena);
    const outputs = try arena.alloc(?f64, names.len);
    @memset(outputs, null);
    const leaves = try arena.alloc(?f64, metadata.leafCount(meta0));
    @memset(leaves, null);
    for (args.bloom_filters) |spec| {
        const i = outputIndex(names, spec.column) orelse {
            Diag.setColumn(args.diag, spec.column);
            return error.UnknownColumn;
        };
        switch (proj.output_specs[i]) {
            .passthrough => |ci| {
                var it: schema.LeafIterator = .init(meta0.schema.items);
                const leaf = while (it.next()) |l| {
                    if (l.index == ci) break l;
                } else return error.UnknownColumn;
                const elem = leaf.element.*;
                const flba_decimal = elem.type == .FIXED_LEN_BYTE_ARRAY and decimal_mod.kindFromSchema(&elem) != null;
                if (elem.type == .BOOLEAN or elem.type == .INT96 or leaf.max_rep > 0 or flba_decimal) {
                    Diag.setColumn(args.diag, spec.column);
                    return error.BloomFilterUnsupportedColumn;
                }
                leaves[ci] = spec.fpp;
            },
            .computed => |c| if (c.expr.typeOf().toParquet() == .BOOLEAN) {
                Diag.setColumn(args.diag, spec.column);
                return error.BloomFilterUnsupportedColumn;
            },
        }
        outputs[i] = spec.fpp;
    }
    return .{ .outputs = outputs, .leaves = leaves };
}

/// The output column `name` names: its exact name, else the one name equal to it ignoring ASCII case.
fn outputIndex(names: []const []const u8, name: []const u8) ?usize {
    for (names, 0..) |n, i| if (std.mem.eql(u8, n, name)) return i;
    var found: ?usize = null;
    for (names, 0..) |n, i| if (std.ascii.eqlIgnoreCase(n, name)) {
        if (found != null) return null;
        found = i;
    };
    return found;
}

/// The one codec the written column chunks share, `none_written` when there
/// are none, or null when they differ.
fn writtenCodec(rgs: []const schema.RowGroup, none_written: schema.CompressionCodec) ?schema.CompressionCodec {
    var seen: ?schema.CompressionCodec = null;
    for (rgs) |rg| for (rg.columns.items) |chunk| {
        const md = chunk.meta_data orelse continue;
        if (seen) |c| {
            if (c != md.codec) return null;
        } else seen = md.codec;
    };
    return seen orelse none_written;
}

fn runWrite(ctx: Context, args: QueryArgs, out_path: []const u8) !WriteResult {
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var t: Timings = .{};

    if (args.select != null and args.columns != null) return error.AggregateMutexWithSelect;

    // 1. Open all inputs (mmap or parallel s3 range-fetch w/ column
    //    pruning — same path runAggregate uses).
    const prof = profile.threadOf(args.profiler, .main);
    t.core.prof = prof;
    const t_open = nowMonoNs();
    var opened = try openInputs(ctx, arena, args);
    defer opened.deinit();
    const t_open_end = nowMonoNs();
    t.read_ns = @intCast(t_open_end - t_open);
    profile.record(prof, .open_inputs, t_open, t_open_end, profile.args(.{args.inputs.len}));

    // 2. Parse meta of every file (cheap; serial), validate schemas
    //    against meta[0].
    const t_parse_start = nowMonoNs();
    const metas = try materializeMetas(arena, opened, args.diag, prof);
    const meta0 = &metas[0];
    for (metas[1..], 1..) |*m, i| {
        if (m.schema.items.len != meta0.schema.items.len) return error.SchemaMismatch;
        for (m.schema.items, meta0.schema.items) |a_e, b_e| {
            // Same name is not enough: a byte-copy concatenation (and a shared
            // footer) is only valid when the columns are structurally identical.
            // Differing physical/logical/converted type or repetition produces a
            // footer that lies about the copied bytes, or a wrong union access on
            // re-encode.
            if (!std.ascii.eqlIgnoreCase(a_e.name, b_e.name)) return error.SchemaMismatch;
            if ((a_e.num_children orelse 0) != (b_e.num_children orelse 0)) return error.SchemaMismatch;
            if (a_e.type != b_e.type) return error.SchemaMismatch;
            if (a_e.repetition_type != b_e.repetition_type) return error.SchemaMismatch;
            if (a_e.converted_type != b_e.converted_type) return error.SchemaMismatch;
            if (!std.meta.eql(a_e.logical_type, b_e.logical_type)) return error.SchemaMismatch;
            if (a_e.type_length != b_e.type_length) return error.SchemaMismatch;
        }
        _ = i;
    }
    const tree0 = try schema_tree.SchemaTree.build(arena, meta0.schema.items);

    // 3. Parse --columns / --select / --filter against meta0 into the output columns and the leaves they read.
    var proj = try planProjection(arena, args, meta0);
    const blooms = try resolveBlooms(arena, args, &proj, meta0);
    const kept_set = proj.kept_set;
    const kept_arr = proj.kept;
    const fetch_arr = proj.fetch;
    const select_items = proj.select_items;
    const filter_opt = proj.filter;
    const output_specs = proj.output_specs;
    const any_computed = proj.any_computed;
    const num_leaves = kept_arr.len;
    var probed = opened.membership;
    probed.add(try loadLocalMembership(arena, opened, metas, filter_opt, args.scan_all));
    const t_parse_end = nowMonoNs();
    t.parse_ns = @intCast(t_parse_end - t_parse_start);
    profile.record(prof, .parse_query, t_parse_start, t_parse_end, profile.args(.{}));
    if (args.profiler) |p| p.setColumnsFromMeta(meta0);

    var kept_in_order: std.ArrayList(usize) = .empty;
    for (kept_arr, 0..) |b, i| if (b) try kept_in_order.append(arena, i);

    // zstd pages are compressed per the destination's priority; the modes they take are counted for the profile.
    const priority = args.compress_priority;
    var zstd_counts: compression.ZstdModeCounts = .{};
    const zstd: compression.ZstdPlan = .{ .lambda = priority.lambda(), .counts = &zstd_counts };

    // 4. Open output sink: local fd or s3 multipart.
    const out_is_s3 = std.mem.startsWith(u8, out_path, "s3://");
    var fd_sink: FdSink = .{ .fd = 0 };
    var mp_sink: ?multipart_sink.MultipartSink = null;
    var sink: streaming.Sink = undefined;
    var sink_owns_fd = false;
    defer if (sink_owns_fd) local_fs.close(fd_sink.fd);
    var out_owned_pool: ?*s3.Pool(POOL_SIZE) = null;
    defer if (out_owned_pool) |p| p.deinit();
    defer if (mp_sink) |*ms| {
        if (!ms.isClosed()) ms.abort();
        ms.deinit();
    };

    // A failed write must not leave a truncated local file that looks like output.
    var remove_on_error = false;
    errdefer if (remove_on_error) local_fs.removeFile(out_path);
    if (out_is_s3) {
        const out_url = s3.Url.parse(out_path) catch return error.BadOutputUrl;
        const creds = try credentialsFromEnv(ctx.env);
        const out_criteria = try s3.poolCriteria(arena, creds, out_url.bucket);
        const out_pool: *OutputLane = if (ctx.pool_registry) |reg|
            try reg.ensure_output(reg.ctx, ctx.gpa, creds, out_url.bucket)
        else blk: {
            const p = try arena.create(s3.Pool(POOL_SIZE));
            try p.init(ctx.gpa);
            out_owned_pool = p;
            const lane = try arena.create(OutputLane);
            lane.init(p);
            break :blk lane;
        };
        mp_sink = multipart_sink.MultipartSink.init(
            ctx.io,
            ctx.gpa,
            arena,
            creds,
            out_url,
            out_pool,
            out_criteria,
            .{ .profiler = args.profiler },
        );
        sink = .{
            .ctx = @ptrCast(&mp_sink.?),
            .write_fn = multipart_sink.sinkWriteFn,
        };
    } else {
        fd_sink.fd = try createFile(out_path);
        remove_on_error = true;
        sink_owns_fd = true;
        sink = .{
            .ctx = @ptrCast(&fd_sink),
            .write_fn = FdSink.writeFn,
        };
    }

    // 5. Stream output. PAR1 magic, then per-file × per-RG
    //    encodeRG/copyRG, then footer.
    var out_offset: u64 = 0;
    try sink.write(&PAR1);
    out_offset += PAR1.len;

    var rows_in: i64 = 0;
    var rows_kept: i64 = 0;
    var rgs_in: usize = 0;
    var rg_kept: usize = 0;
    var bytes_in: u64 = 0;
    var new_row_groups: std.ArrayListUnmanaged(schema.RowGroup) = .empty;
    // Footers of inputs with row groups byte-copied into the output: their bounds decide the declared column orders.
    var copied_from: std.ArrayListUnmanaged(*const schema.FileMetaData) = .empty;
    // `--select` always re-encodes: the byte-copy fastpath copies every column
    // chunk in leaf order with `kept_set == null`, but a select builds the
    // footer schema from only the selected/reordered columns — so a pure
    // passthrough `--select "a,b"` would emit a footer that disagrees with the
    // copied row-group data (N chunks, M<N columns). Routing it through the
    // encoder makes the footer match what's written. (`--columns` keeps the
    // fast lossless subset path via `kept_set`.)
    var need_encoder = filter_opt != null or any_computed or select_items != null;
    // Byte-copied bounds keep their source's column order. Inputs that disagree on one leave no footer that is
    // truthful for every row group, so re-encode, which writes every bound in TYPE_DEFINED_ORDER.
    if (!need_encoder) {
        const all = try arena.alloc(*const schema.FileMetaData, metas.len);
        for (metas, all) |*m, *p| p.* = m;
        const kept_map: ?[]const usize = if (kept_set != null) kept_in_order.items else null;
        const n_out = if (kept_map) |k| k.len else invariant.footerLeafCount(meta0.schema.items);
        _ = schema.FileMetaData.outputColumnOrders(arena, n_out, kept_map, all) catch |err| switch (err) {
            error.ColumnOrderMismatch => need_encoder = true,
            else => return err,
        };
    }

    // Re-encoding a nested (repeated) column would drop its rep-levels — the
    // OutputAggregator carries only values/def-levels — yielding pages whose
    // decoded level counts disagree with the header while the footer still
    // describes nested data. Reject loudly instead of writing corrupt output.
    // (Pure SELECT * / `--columns` byte-copy nested columns intact via the
    // fastpath, which never decodes.)
    if (need_encoder) {
        for (output_specs) |spec| switch (spec) {
            .passthrough => |ci| {
                if (ci >= num_leaves or meta0.row_groups.items.len == 0) continue;
                const cm = meta0.row_groups.items[0].columns.items[ci].meta_data orelse continue;
                if (cm.type == .INT96)
                    return error.INT96ReencodeNotSupported;
                if (meta0.getColumnLevels(cm.path_in_schema.items).max_rep > 0)
                    return error.NestedReencodeNotSupported;
            },
            .computed => {},
        };
    }

    // The re-encode path (need_encoder) fans row groups across the Io pool
    // below; the byte-copy fastpath stays serial (memcpy-bound). win_slots hold
    // the small per-row-group footer metadata (on private arenas) that the
    // footer write further down still references, so they are freed at function
    // scope, AFTER the footer is built. (The bulk encoded bytes are freed
    // earlier, by the windowed writer.)
    var win_slots: []Slot = &.{};
    defer for (win_slots) |*s| s.meta_arena.deinit();

    const kept_opt: ?[]const bool = if (kept_set != null) kept_arr else null;
    if (!need_encoder and args.codec != null and anyChunkNeedsRecompress(metas, kept_opt, args.codec.?)) {
        // Byte copy with a codec change: recompression is CPU-bound, so row groups fan out across the CPU budget
        // behind a byte window, as the re-encode path below does.
        var jobs: std.ArrayListUnmanaged(RGJob) = .empty;
        for (opened.inputs, metas, 0..) |in, *meta_p, ii| {
            bytes_in += in.bytes.len;
            for (meta_p.row_groups.items) |*src_rg| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                try jobs.append(arena, .{
                    .bytes = in.bytes,
                    .meta = meta_p,
                    .rg = src_rg,
                    .file_index = ii,
                    .rg_index = rgs_in - 1,
                });
            }
        }
        for (metas) |*m| try copied_from.append(arena, m);
        if (jobs.items.len > 0) {
            const sizing = writeWindow(ctx, args, jobs.items);
            const window = sizing.window;
            const slots = try arena.alloc(RecompressSlot, jobs.items.len);
            for (slots) |*sl| sl.* = .{ .arena = std.heap.ArenaAllocator.init(ctx.gpa) };
            defer for (slots) |*sl| sl.arena.deinit();
            var window_sem: std.Io.Semaphore = .{ .permits = window };
            var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
            var rw = RecompressCtx{
                .gpa = ctx.gpa,
                .jobs = jobs.items,
                .kept = kept_opt,
                .codec = args.codec.?,
                .zstd = zstd,
                .bloom_leaves = blooms.leaves,
                .slots = slots,
                .window = &window_sem,
                .completions = &completions_sem,
                .sink = sink,
                .out_offset = out_offset,
                .new_row_groups = &new_row_groups,
                .arena = arena,
                .profiler = args.profiler,
            };
            const effective = sizing.workers;
            rw.page_workers = @max(1, sizing.budget / @max(1, effective));
            const re_span = profile.Span.begin(prof);
            defer re_span.end(.reencode, profile.args(.{ jobs.items.len, effective, window }));
            try runWindowed(RecompressCtx, &rw, jobs.items.len, effective);
            out_offset = rw.out_offset;
            rows_kept += rw.rows_kept;
            rg_kept += rw.rgs_kept;
            t.core.sink_ns += rw.core.sink_ns;
            t.core.encode_ns += rw.core.encode_ns;
        }
    } else if (!need_encoder) {
        // Byte-copy fastpath: copy kept column chunks verbatim (serial;
        // memcpy-bound and already faster than every other engine). A chunk
        // stored in another codec than the one requested is recompressed.
        const recompress: ?consumer.Recompress = if (args.codec) |c|
            .{ .codec = c, .zstd = zstd, .scratch = ctx.gpa }
        else
            null;
        for (opened.inputs, metas) |in, meta_i| {
            bytes_in += in.bytes.len;
            const rg_src: consumer.RGSrc = .{ .bytes = in.bytes, .byte_origin = 0 };
            const meta_const = meta_i;
            for (meta_const.row_groups.items) |*src_rg| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                const rg_span = profile.Span.begin(prof);
                defer rg_span.end(.copy_rg, profile.args(.{src_rg.total_byte_size}));
                const bloom_write: consumer.BloomWrite = .{ .fpp = blooms.leaves, .meta = &meta_i, .scratch = ctx.gpa };
                const out = try consumer.copyRG(
                    arena,
                    src_rg,
                    rg_src,
                    if (kept_set != null) kept_arr else null,
                    sink,
                    &out_offset,
                    &t.core,
                    true,
                    recompress,
                    bloom_write,
                );
                if (out.rg) |new_rg| try new_row_groups.append(arena, new_rg);
                rows_kept += out.surviving_rows;
                if (out.surviving_rows > 0) rg_kept += 1;
            }
        }
        for (metas) |*m| try copied_from.append(arena, m);
    } else {
        // Windowed re-encode: one path for all environments. Concurrency is
        // bounded by the CPU budget (quota- and Lambda-aware); in-flight
        // memory is bounded by a sliding window (W row groups). No env gate —
        // the two knobs below size themselves from the environment.

        // Byte-copy passthrough eligibility: a fully-passing row group
        // (Decision.always_match) can be copied verbatim instead of
        // decoded/filtered/re-encoded — but only for a plain filter over
        // passthrough columns. A reordering `--select` or a DECIMAL output
        // (which the footer coerces on re-encode) would make a byte-copied RG
        // disagree with the footer, so exclude those.
        var has_widened_output = false;
        if (select_items == null and filter_opt != null) {
            for (0..num_leaves) |ci| {
                if (!kept_arr[ci] or meta0.row_groups.items.len == 0) continue;
                const cm = meta0.row_groups.items[0].columns.items[ci].meta_data orelse continue;
                if (meta0.getColumnSchema(cm.path_in_schema.items)) |elem_val| {
                    var e = elem_val;
                    if (decimal_mod.kindFromSchema(&e) != null or schema.isFloat16(e)) {
                        has_widened_output = true;
                        break;
                    }
                }
            }
        }
        const bytecopy_ok = select_items == null and filter_opt != null and !has_widened_output;

        // 1. Collect surviving row groups (serial, cheap stats-only pruning).
        var jobs: std.ArrayListUnmanaged(RGJob) = .empty;
        for (opened.inputs, 0..) |in, ii| {
            bytes_in += in.bytes.len;
            const meta_p = &metas[ii];
            for (meta_p.row_groups.items) |*src_rg| {
                rgs_in += 1;
                rows_in += src_rg.num_rows;
                var passthrough = false;
                if (filter_opt) |f| if (!args.scan_all) {
                    const d = filter_prune.pruneRowGroup(src_rg, f, meta_p);
                    if (d == .skip) continue;
                    // Byte-copy preserves the source codec, so it may only stand
                    // in for a re-encode when the requested --codec already
                    // matches every kept column (else re-encode to honor it).
                    // ...and only when its bounds are in the TYPE_DEFINED_ORDER the re-encoded row groups declare.
                    if (bytecopy_ok and d == .always_match and
                        keptColumnsUseCodec(src_rg, kept_arr, args.codec orelse .SNAPPY) and
                        meta_p.keptLeavesTypeDefined(kept_arr))
                        passthrough = true;
                };
                if (passthrough and (copied_from.items.len == 0 or copied_from.getLast() != meta_p))
                    try copied_from.append(arena, meta_p);
                try jobs.append(arena, .{
                    .bytes = in.bytes,
                    .meta = meta_p,
                    .rg = src_rg,
                    .passthrough = passthrough,
                    .file_index = ii,
                    .rg_index = rgs_in - 1,
                });
            }
        }

        if (jobs.items.len > 0) {
            // 2. Size the two knobs from the environment.
            const sizing = writeWindow(ctx, args, jobs.items);
            const window = sizing.window;
            const effective_async_budget = sizing.workers;

            // 3. Slots hold per-RG results; their meta arenas live to the footer.
            const slots = try arena.alloc(Slot, jobs.items.len);
            for (slots) |*s| s.* = .{ .meta_arena = std.heap.ArenaAllocator.init(ctx.gpa) };
            win_slots = slots;

            const regions = try arena.alloc(WorkRegions, @max(effective_async_budget, 1));
            for (regions) |*r| r.* = .init(ctx.gpa);
            defer for (regions) |*r| r.deinit();
            var window_sem: std.Io.Semaphore = .{ .permits = window };
            var completions_sem: std.Io.Semaphore = .{ .permits = 0 };
            var w = WinCtx{
                .gpa = ctx.gpa,
                .jobs = jobs.items,
                .filter = filter_opt,
                .fetch_arr = fetch_arr,
                .output_specs = output_specs,
                .kept_set = kept_set,
                .codec = args.codec orelse .SNAPPY,
                .zstd = zstd,
                .decode_options = .{ .fast_levels = args.fast_levels },
                .bloom_outputs = blooms.outputs,
                .bloom_leaves = blooms.leaves,
                .slots = slots,
                .regions = regions,
                .window = &window_sem,
                .completions = &completions_sem,
                .sink = sink,
                .out_offset = out_offset,
                .new_row_groups = &new_row_groups,
                .arena = arena,
                .profiler = args.profiler,
            };
            const re_span = profile.Span.begin(prof);
            defer re_span.end(.reencode, profile.args(.{ jobs.items.len, effective_async_budget, window }));
            try runWindowedReencode(&w, effective_async_budget);
            out_offset = w.out_offset;
            rows_kept += w.rows_kept;
            rg_kept += w.rgs_kept;
            t.core.sink_ns += w.sink_ns;
            t.core.decode_ns += w.core.decode_ns;
            t.core.eval_ns += w.core.eval_ns;
            t.core.encode_ns += w.core.encode_ns;
        }
    }

    if (args.codec == .ZSTD) {
        var pages: [4]u64 = undefined;
        for (&pages, &zstd_counts.pages) |*n, *c| n.* = c.load(.monotonic);
        const now = nowMonoNs();
        profile.record(prof, .zstd_plan, now, now, profile.args(.{
            @intFromEnum(priority), pages[0], pages[1], pages[2], pages[3], zstd_counts.rechecked.load(.monotonic),
        }));
    }

    // 6. Bloom filters after the row groups, then the footer.
    const t_footer = nowMonoNs();
    const footer_span = profile.Span.begin(prof);
    {
        var so: streaming.SinkOut = .{ .sink = sink, .offset = @intCast(out_offset) };
        try fastpath.placeFilters(&so, new_row_groups.items);
        out_offset = so.offset;
    }
    var new_schema_items = meta0.schema;
    if (select_items != null) {
        // Build a flat schema for --select. Each output_spec produces
        // one leaf in the output. (Lifted from cli/query.zig.)
        new_schema_items = try buildSelectSchema(arena, meta0, output_specs);
    } else if (kept_set) |_| {
        const kept_u32 = try arena.alloc(u32, kept_in_order.items.len);
        for (kept_in_order.items, 0..) |idx, i| kept_u32[i] = @intCast(idx);
        const projected = try tree0.projectSubset(arena, kept_u32);
        new_schema_items = try projected.writeFlatThrift(arena);
    }

    // DECIMAL columns whose data went through the decode+re-encode
    // path (any time `need_encoder` is true — i.e. there's a filter,
    // or any column is computed) were written as DOUBLE PLAIN by the
    // encoder, but the footer schema we built above still claims
    // DECIMAL because it was derived from meta0. Rewrite leaves in-
    // place so the file's schema matches the on-disk bytes. Byte-
    // copy passthrough (need_encoder=false) preserves DECIMAL.
    if (need_encoder) {
        try coerceDecimalLeavesToDouble(arena, &new_schema_items);
        coerceFloat16LeavesToDouble(&new_schema_items);
    }

    // Output structural invariant — ALWAYS ON, even under ReleaseFast.
    // Every row group must carry exactly one column chunk per footer
    // leaf. If the footer leaf count and emitted chunks disagree, strict
    // readers reject the file. This guard must survive ReleaseFast, so it
    // is an error instead of a debug assert.
    const footer_leaves = invariant.footerLeafCount(new_schema_items.items);
    const kept_map: ?[]const usize = if (select_items == null and kept_set != null) kept_in_order.items else null;
    const new_meta: schema.FileMetaData = .{
        .version = meta0.version,
        .schema = new_schema_items,
        .num_rows = rows_kept,
        .created_by = meta0.created_by,
        .row_groups = new_row_groups,
        .column_orders = try schema.FileMetaData.outputColumnOrders(arena, footer_leaves, kept_map, copied_from.items),
    };

    for (new_row_groups.items) |rg_chk| {
        if (rg_chk.columns.items.len != footer_leaves) return error.FooterSchemaChunkMismatch;
    }

    var w: thrift.Writer = .init(arena);
    defer w.deinit();
    try new_meta.write(&w);
    const footer_bytes = w.bytes();
    try sink.write(footer_bytes);
    out_offset += footer_bytes.len;
    var len_bytes: [4]u8 = undefined;
    std.mem.writeInt(u32, &len_bytes, @intCast(footer_bytes.len), .little);
    try sink.write(&len_bytes);
    out_offset += len_bytes.len;
    try sink.write(&PAR1);
    out_offset += PAR1.len;
    t.footer_ns += @intCast(nowMonoNs() - t_footer);
    footer_span.end(.footer_write, profile.args(.{footer_bytes.len + 8}));

    // 7. Close s3 multipart upload (commits the parts). Local fd
    //     closes via defer.
    if (mp_sink) |*ms| {
        try ms.close();
        const mp_stats = ms.snapshotStats();
        t.mp_complete_ns = mp_stats.complete_ns;
        t.mp_await_ns = mp_stats.await_ns;
    }

    return .{
        .codec = writtenCodec(new_row_groups.items, args.codec orelse .SNAPPY),
        .files_in = opened.inputs.len,
        .rows_in = rows_in,
        .rows_kept = rows_kept,
        .bytes_in = bytes_in,
        .bytes_out = out_offset,
        .row_groups_in = rgs_in,
        .row_groups_kept = rg_kept,
        .membership = probed,
        .timings = t,
    };
}

/// Build a flat output schema for --select mode. One leaf per
/// output_spec; passthrough columns copy the source leaf (preserving
/// type / type_length / converted_type / etc.), computed columns
/// declare a new leaf typed by the expression's resolved type.
///
/// This is derived from resolved output specs rather than raw query
/// args so passthrough columns preserve their exact leaf metadata and
/// computed columns match the encoder's required-column output shape.
/// The schema leaf shape and encoded data page must agree exactly.
/// Walk a flat thrift schema list and rewrite every DECIMAL leaf to
/// a plain DOUBLE leaf. Used after the encode+re-encode path, where
/// DECIMAL columns get decoded to f64 (see decimal_mod) and emitted
/// as DOUBLE PLAIN. The file's footer must agree.
fn coerceDecimalLeavesToDouble(
    arena: std.mem.Allocator,
    items: *std.ArrayListUnmanaged(schema.SchemaElement),
) !void {
    _ = arena;
    for (items.items) |*elem| {
        // Only leaves (no children) can be DECIMAL.
        const nc = elem.num_children orelse 0;
        if (nc != 0) continue;
        const k = decimal_mod.kindFromSchema(elem) orelse continue;
        // INT32/INT64 (D1) and FIXED_LEN_BYTE_ARRAY (D2) decimals now ride
        // the lossless i128 lane and keep their DECIMAL annotation. Only
        // BYTE_ARRAY-backed decimals (rare) still decode→f64→DOUBLE.
        if (k.physical == .INT32 or k.physical == .INT64 or
            k.physical == .FIXED_LEN_BYTE_ARRAY) continue;

        elem.type = .DOUBLE;
        elem.type_length = null;
        elem.converted_type = null;
        elem.logical_type = null;
        elem.scale = null;
        elem.precision = null;
    }
}

/// FLOAT16 decodes into the f64 lane and the writer has no half-float
/// encoder. Re-encoded output is therefore DOUBLE; keep the footer in
/// lockstep with the column metadata and eight-byte PLAIN values.
fn coerceFloat16LeavesToDouble(items: *std.ArrayListUnmanaged(schema.SchemaElement)) void {
    for (items.items) |*elem| {
        const nc = elem.num_children orelse 0;
        if (nc != 0 or !schema.isFloat16(elem.*)) continue;
        elem.type = .DOUBLE;
        elem.type_length = null;
        elem.converted_type = null;
        elem.logical_type = null;
        elem.scale = null;
        elem.precision = null;
    }
}

fn buildSelectSchema(
    arena: std.mem.Allocator,
    meta: *const schema.FileMetaData,
    specs: []const consumer.OutputCol,
) !std.ArrayListUnmanaged(schema.SchemaElement) {
    var out: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try out.ensureTotalCapacity(arena, specs.len + 1);

    // Root group.
    try out.append(arena, .{
        .type = null,
        .type_length = null,
        .repetition_type = null,
        .name = "schema",
        .num_children = @intCast(specs.len),
        .converted_type = null,
        .logical_type = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    });

    for (specs) |spec| {
        switch (spec) {
            .passthrough => |ci| {
                const elem = metadata.leafSchemaElement(meta, ci) orelse return error.SchemaMismatch;
                var copy = elem;
                copy.num_children = 0;
                try out.append(arena, copy);
            },
            .computed => |c| try out.append(arena, consumer.computedSchemaElem(c)),
        }
    }

    return out;
}

// ============================================================
// Input opening — mmap for local, parallel range-fetch for s3
// ============================================================

const OpenedInputs = struct {
    gpa: std.mem.Allocator,
    inputs: []const scan.Input,
    metas: []schema.FileMetaData,
    meta_ready: []bool,
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
    /// `Context.exit_after`: the mappings go with the process.
    leave_mappings: bool = false,

    pub fn deinit(self: *OpenedInputs) void {
        if (!self.leave_mappings) for (self.mmaps) |m_opt| if (m_opt) |m| m.unmap();
        for (self.s3_buffers) |buf| self.gpa.free(buf);
        for (self.owned_pools) |p| {
            p.deinit();
        }
    }
};

/// Probe the Bloom filters of the inputs whose footers were parsed from their mapped bytes (`materializeMetas`); S3
/// inputs were probed through the range fetcher while their fetch was planned.
fn loadLocalMembership(
    arena: std.mem.Allocator,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
    filter: ?filter_ast.Filter,
    scan_all: bool,
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
    return membership.load(arena, opened.gpa, f, local_metas.items, mapped.source(), .{});
}

fn materializeMetas(
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
            return error.NotParquet;
        };
    }
    return metas;
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
    try fetchMetaBatch(read_ctx, arena, creds, pool, &specs, null);

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

fn openInputs(
    ctx: Context,
    arena: std.mem.Allocator,
    args: QueryArgs,
) !OpenedInputs {
    const paths = args.inputs;
    const prof = profile.threadOf(args.profiler, .main);
    if (args.profiler) |p| p.setFiles(paths);
    var inputs = try arena.alloc(scan.Input, paths.len);
    var metas = try arena.alloc(schema.FileMetaData, paths.len);
    var meta_ready = try arena.alloc(bool, paths.len);
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
    var membership_stats: membership.Stats = .{};
    if (s3_indices.items.len > 0) {
        const creds = try credentialsFromEnv(ctx.env);

        var read_exec = readExecutor(ctx.gpa);
        defer read_exec.deinit();
        var read_ctx = ctx;
        read_ctx.io = read_exec.io();

        // Parse and validate s3 URLs, group by bucket. The current rule:
        // all s3 inputs share one bucket per call (matches lambda's
        // existing constraint). Cross-bucket scans are a separate
        // feature.
        var s3_urls = try arena.alloc(s3.Url, s3_indices.items.len);
        var bucket: []const u8 = undefined;
        for (s3_indices.items, 0..) |idx, k| {
            s3_urls[k] = s3.Url.parse(paths[idx]) catch return error.BadInputUrl;
            if (k == 0) bucket = s3_urls[k].bucket;
            if (!std.mem.eql(u8, s3_urls[k].bucket, bucket)) return error.CrossBucketNotSupported;
        }

        // Resolve a pool — either persistent (Lambda) or freshly built
        // (CLI). Either way, after this we have a `*s3.Pool(POOL_SIZE)`
        // we can drive.
        const pool: *s3.Pool(POOL_SIZE) = if (ctx.pool_registry) |reg|
            try reg.ensure_for_bucket(reg.ctx, ctx.gpa, creds, bucket)
        else blk: {
            const p = try arena.create(s3.Pool(POOL_SIZE));
            try p.init(ctx.gpa);
            errdefer p.deinit();
            try owned_pools.append(arena, p);
            break :blk p;
        };

        // Fetch metadata (tail + head + footer) for every s3 file in
        // parallel. After this the per-file meta is parsed and we know
        // total file size + tail_start.
        var specs = try arena.alloc(FetchSpec, s3_urls.len);
        for (s3_urls, 0..) |u, k| specs[k] = .{ .url = u, .file_index = s3_indices.items[k] };
        const batch_span = profile.Span.begin(prof);
        fetchMetaBatch(read_ctx, arena, creds, pool, specs, args.profiler) catch |err| {
            for (specs) |*sp| {
                sp.releaseTail(ctx.gpa);
                if (sp.file_buf.len > 0) ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) ctx.gpa.free(sp.footer_region);
            }
            return err;
        };
        batch_span.end(.footer_batch, profile.args(.{specs.len}));
        var specs_returned = false;
        errdefer if (!specs_returned) {
            for (specs) |*sp| {
                sp.releaseTail(ctx.gpa);
                if (sp.file_buf.len > 0) ctx.gpa.free(sp.file_buf);
                if (sp.footer_region.len > 0) ctx.gpa.free(sp.footer_region);
            }
        };

        const metas_for_plan = try arena.alloc(schema.FileMetaData, specs.len);
        for (specs, metas_for_plan) |sp, *m| m.* = sp.meta;

        // Bloom filters first: what they prove lands on the footers' chunks, so the plan below and the scan workers
        // after it prune the same row groups.
        if (!args.scan_all) if (args.filter) |fs| if (fs.len > 0) {
            const f = try filter_parser.parse(arena, fs, &metas_for_plan[0], args.diag);
            var src: S3Membership = .init(read_ctx, creds, pool, specs);
            src.profiler = args.profiler;
            membership_stats = try membership.load(arena, ctx.gpa, f, metas_for_plan, src.source(), .{});
        };

        // What each row group needs fetched. (Yes, this re-parses arg strings that scan and runWrite will parse again
        // — microseconds, not worth plumbing the parsed AST through.)
        const plan_span = profile.Span.begin(prof);
        var n_ranges: usize = 0;
        var n_merged: usize = 0;
        var n_rgs: usize = 0;
        var n_rgs_pruned: usize = 0;
        const plan = try planRanges(arena, args, metas_for_plan);

        // Per file: stat-prune RGs, collect raw byte ranges for every
        // (surviving RG) × (needed col), coalesce within the file, and
        // emit one FetchJob per coalesced span. Coalescing matters
        // because column chunks in a parquet RG are written back-to-
        // back; fetching them as separate GETs spends most of the wall
        // in HTTP round-trip overhead even though the bytes are
        // physically adjacent.
        //
        // S3 inputs do NOT materialize a total_size-sized dense buffer.
        // Instead, merged source ranges are packed into a compact buffer
        // and the parsed metadata offsets are rebased to that layout.
        var fetch_jobs: std.ArrayList(s3.FetchJob) = .empty;
        for (specs) |*sp| {
            // The tail serves the footer parse and the copies below; every way out of this iteration frees it, a file
            // whose row groups all prune included.
            defer sp.releaseTail(ctx.gpa);
            const meta = &sp.meta;
            sp.rg_columns = try arena.alloc(?[]const bool, meta.row_groups.items.len);

            var ranges: std.ArrayList(coalescer.Range) = .empty;
            for (meta.row_groups.items, sp.rg_columns) |*rg, *rg_cols| {
                rg_cols.* = plan.rowGroupColumns(rg, meta);
                n_rgs += 1;
                const cols = rg_cols.* orelse {
                    n_rgs_pruned += 1;
                    continue;
                };
                for (cols, 0..) |needed, ci| {
                    if (!needed) continue;
                    if (ci >= rg.columns.items.len) continue;
                    const cm = rg.columns.items[ci].meta_data orelse continue;
                    const start: u64 = if (cm.dictionary_page_offset) |dp| @intCast(dp) else @intCast(cm.data_page_offset);
                    const len: u64 = @intCast(cm.total_compressed_size);
                    // EVERY needed chunk goes into the range list — including
                    // ones inside the prefetched tail — so the compact buffer
                    // covers it and offset rebasing can find it. Bytes at or
                    // past tail_start are satisfied by copying from the tail
                    // below (no re-fetch); only the sub-tail_start portion
                    // becomes a fetch job.
                    try ranges.append(arena, .{ .start = start, .end = start + len });
                }
                // A copied row group keeps its Bloom filters, so they are fetched with it.
                if (plan.carriedFilters(rg, meta)) |carry| for (carry, 0..) |c, ci| {
                    if (!c or ci >= rg.columns.items.len) continue;
                    const cm = rg.columns.items[ci].meta_data orelse continue;
                    const off = cm.bloom_filter_offset orelse continue;
                    const len = cm.bloom_filter_length orelse continue;
                    if (off < 4 or len <= 0 or len > sbbf.max_filter_bytes) continue;
                    const start: u64 = @intCast(off);
                    const end = start + @as(u64, @intCast(len));
                    if (end > sp.total_size) continue;
                    try ranges.append(arena, .{ .start = start, .end = end });
                };
            }

            if (ranges.items.len == 0) {
                sp.file_buf = &.{};
                continue;
            }
            const merged = try coalescer.Coalescer.coalesce(arena, ranges.items, COALESCE_GAP);
            n_ranges += ranges.items.len;
            n_merged += merged.len;
            var compact_len: usize = 0;
            sp.compact_ranges = try arena.alloc(CompactRange, merged.len);
            for (merged, 0..) |r, ri| {
                if (r.end < r.start or r.end > sp.total_size) return error.BadResponse;
                const len: usize = @intCast(r.end - r.start);
                sp.compact_ranges[ri] = .{
                    .start = r.start,
                    .end = r.end,
                    .dst_start = compact_len,
                };
                compact_len += len;
            }
            sp.file_buf = try ctx.gpa.alloc(u8, compact_len);
            try rebaseFetchedOffsets(&sp.meta, plan.columns(), sp.compact_ranges, sp.rg_columns);

            for (sp.compact_ranges) |r| {
                const source_len: usize = @intCast(r.end - r.start);
                const dst = sp.file_buf[r.dst_start .. r.dst_start + source_len];

                const fetch_end = @min(r.end, sp.tail_start);
                if (r.start < fetch_end) {
                    try fetch_jobs.append(arena, .{
                        .bucket = sp.url.bucket,
                        .key = sp.url.key,
                        .range = .{ .start = r.start, .end = fetch_end },
                        .target = dst[0..@intCast(fetch_end - r.start)],
                    });
                }

                if (r.end > sp.tail_start) {
                    const tail_copy_start = @max(r.start, sp.tail_start);
                    const dst_off: usize = @intCast(tail_copy_start - r.start);
                    const tail_off: usize = @intCast(tail_copy_start - sp.tail_start);
                    const copy_len: usize = @intCast(r.end - tail_copy_start);
                    if (tail_off + copy_len > sp.tail.len) return error.BadResponse;
                    @memcpy(dst[dst_off .. dst_off + copy_len], sp.tail[tail_off .. tail_off + copy_len]);
                }
            }
        }

        if (plan_span.t != null) {
            var fetch_bytes: u64 = 0;
            for (fetch_jobs.items) |j| fetch_bytes += j.target.len;
            plan_span.end(.plan_ranges, profile.args(.{
                n_ranges, n_merged, fetch_jobs.items.len, fetch_bytes, n_rgs, n_rgs_pruned,
            }));
        }

        // Parallel fetch all the column-chunk ranges across all files.
        // s3.fetchJobs handles the Io.Group fan-out + per-job pool
        // acquire/release.
        if (fetch_jobs.items.len > 0) {
            const fetch_span = profile.Span.begin(prof);
            const fetched = try s3.fetchJobs(
                read_ctx.io,
                pool,
                ctx.gpa,
                arena,
                creds,
                fetch_jobs.items,
                READ_CONCURRENCY,
                args.profiler,
            );
            fetch_span.end(.fetch, profile.args(.{ fetch_jobs.items.len, fetched }));
        }

        // Build scan.Inputs. bytes is the compact per-file buffer with
        // all needed chunks materialized and metadata already rebased.
        for (s3_indices.items, 0..) |orig_idx, k| {
            inputs[orig_idx] = .{
                .name = paths[orig_idx],
                .bytes = specs[k].file_buf,
                .logical_size = specs[k].total_size,
            };
            if (specs[k].file_buf.len > 0) try s3_buffers.append(arena, specs[k].file_buf);
            if (specs[k].footer_region.len > 0) try s3_buffers.append(arena, specs[k].footer_region);
            metas[orig_idx] = specs[k].meta;
            meta_ready[orig_idx] = true;
        }
        specs_returned = true;
    }

    return .{
        .gpa = ctx.gpa,
        .inputs = inputs,
        .metas = metas,
        .meta_ready = meta_ready,
        .has_preparsed_meta = s3_indices.items.len > 0,
        .mmaps = mmaps,
        .owned_pools = try owned_pools.toOwnedSlice(arena),
        .s3_buffers = try s3_buffers.toOwnedSlice(arena),
        .membership = membership_stats,
        .leave_mappings = ctx.exit_after,
    };
}

/// How the scan reads this query's local files: `policy`'s choice, given how much of them the page cache holds.
/// The mapping when there are none, or when any input is remote (the S3 path plans its own reads).
fn allLocal(opened: OpenedInputs) bool {
    if (opened.mmaps.len == 0) return false;
    for (opened.mmaps) |m| if (m == null) return false;
    return true;
}

/// The share (0..1) of the local inputs' bytes the page cache holds.
fn localResidency(gpa: std.mem.Allocator, opened: OpenedInputs) f64 {
    var total: f64 = 0;
    var resident: f64 = 0;
    for (opened.mmaps) |m_opt| {
        const m = m_opt orelse continue;
        const n: f64 = @floatFromInt(m.bytes.len);
        total += n;
        resident += n * local_reader.residentShare(gpa, m.bytes);
    }
    return if (total == 0) 0 else resident / total;
}

/// Per local input, per row group, the byte ranges the scan reads (null when pruned): needed chunks and their page
/// indexes. What the reader reads, and what `auto` weighs against the files' size.
const LocalPlan = struct {
    files: []const []const ?[]local_reader.Range,
    planned: u64,
    total: u64,

    fn coverage(self: LocalPlan) f64 {
        if (self.total == 0) return 0;
        return @min(1.0, @as(f64, @floatFromInt(self.planned)) / @as(f64, @floatFromInt(self.total)));
    }
};

fn planLocalReads(
    arena: std.mem.Allocator,
    args: QueryArgs,
    opened: OpenedInputs,
    metas: []const schema.FileMetaData,
) !LocalPlan {
    const plan = try planRanges(arena, args, metas);
    const files = try arena.alloc([]const ?[]local_reader.Range, metas.len);
    var planned: u64 = 0;
    var total: u64 = 0;
    for (files, metas, opened.mmaps) |*file, *meta, m_opt| {
        total += if (m_opt) |m| m.bytes.len else 0;
        const per_rg = try arena.alloc(?[]local_reader.Range, meta.row_groups.items.len);
        for (meta.row_groups.items, per_rg) |*rg, *rgs| {
            const cols = plan.rowGroupColumns(rg, meta) orelse {
                rgs.* = null;
                continue;
            };
            var rs: std.ArrayList(local_reader.Range) = .empty;
            for (cols, 0..) |needed, ci| {
                if (!needed or ci >= rg.columns.items.len) continue;
                const chunk = rg.columns.items[ci];
                const cm = chunk.meta_data orelse continue;
                const start: u64 = @intCast(if (cm.dictionary_page_offset) |dp| @min(dp, cm.data_page_offset) else cm.data_page_offset);
                try rs.append(arena, .{ .start = start, .end = start + @as(u64, @intCast(cm.total_compressed_size)) });
                planned += @intCast(cm.total_compressed_size);
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
    return .{ .files = files, .planned = planned, .total = total };
}

/// Start reading every local input's planned ranges, row group by row group, and return the inputs the scan should use:
/// each local one over the reader's buffer, gated per row group.
fn startLocalReader(
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
        const f = try rd.openFile(in.name, m.bytes.len);
        try rd.planFile(f, per_rg, COALESCE_GAP);
        in.bytes = f.bytes();
        in.gate = f.gate();
    }
    try rd.start();
    return inputs;
}

/// What the S3 planner fetches from each row group.
const RangePlan = union(enum) {
    /// Aggregates and GROUP BY: the very plan the scan workers follow, made from the same footers, so a chunk left
    /// unfetched is one no worker reads.
    aggregate: agg_plan.Plan,
    /// Write and print evaluate the filter in every row group they keep, so a kept row group needs all of `columns`.
    /// `carry`: the columns whose Bloom filters a write's byte copy keeps; null when no row group is copied.
    projection: struct { filter: ?filter_ast.Filter, columns: []const bool, carry: ?[]const bool = null },

    /// Every column some row group may fetch.
    fn columns(self: *const RangePlan) []const bool {
        return switch (self.*) {
            .aggregate => |*p| p.columns.read,
            .projection => |p| p.columns,
        };
    }

    /// The columns to fetch from `rg`, or null when statistics prune it.
    fn rowGroupColumns(self: *const RangePlan, rg: *const schema.RowGroup, meta: *const schema.FileMetaData) ?[]const bool {
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
    fn carriedFilters(
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
};

/// Plan the S3 fetch from whatever args are set. `metas` are the S3 inputs' footers, parsed against the same schema.
fn planRanges(arena: std.mem.Allocator, args: QueryArgs, metas: []const schema.FileMetaData) !RangePlan {
    const meta0 = &metas[0];
    if (args.aggregate != null or args.group_by != null) {
        const filter: ?filter_ast.Filter = if (args.filter) |fs|
            (if (fs.len > 0) try filter_parser.parse(arena, fs, meta0, args.diag) else null)
        else
            null;
        const group_keys: ?[]const expr_ast.Expr = if (args.group_by) |gb| blk: {
            const items = try expr_parser.parseGroupBy(arena, gb, meta0, args.diag);
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
    return .{ .projection = .{
        .filter = if (args.scan_all) null else proj.filter,
        .columns = proj.fetch,
        .carry = if (copies) proj.kept else null,
    } };
}

const CompactRange = struct {
    start: u64,
    end: u64,
    dst_start: usize,
};

/// Per-s3-file state during the meta-fetch + range-fetch dance.
/// `file_buf` is a compact packed buffer containing only fetched ranges.
/// Parsed metadata offsets are rebased from source-file offsets into this
/// buffer before consumers see the input.
const FetchSpec = struct {
    url: s3.Url,
    /// Index into the query's inputs, for profile spans.
    file_index: usize = 0,
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
    /// How long a tiny GET took on a connection already open (the 8-byte head read, or the revalidation): this
    /// query's sample of a request's fixed cost.
    rtt_ns: u64 = 0,
    err: ?anyerror = null,

    /// Free the fetched tail, unless it is `footer_region` itself (the warm meta-cache path), which is owned apart.
    fn releaseTail(self: *FetchSpec, gpa: std.mem.Allocator) void {
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
const S3Membership = struct {
    ctx: Context,
    creds: s3.Credentials,
    pool: *s3.Pool(POOL_SIZE),
    specs: []const FetchSpec,
    /// Records each filter fetch as a span when set.
    profiler: ?*profile.Profiler = null,
    /// What this query has measured: a request's fixed cost, and one connection's bandwidth. Null until measured.
    rtt_ns: ?u64 = null,
    bytes_per_s: ?f64 = null,

    /// Until measured: a GET's first byte after ~25 ms at ~80 MB/s a connection, as on Lambda against S3 in-region.
    const default_rtt_ns: u64 = 25 * std.time.ns_per_ms;
    const default_bytes_per_s: f64 = 80e6;

    const cost: membership.Cost = .{
        .request_bytes = 2 << 20,
        .concurrency = READ_CONCURRENCY,
        .max_request = s3.SPLIT_THRESHOLD,
    };

    fn init(ctx: Context, creds: s3.Credentials, pool: *s3.Pool(POOL_SIZE), specs: []const FetchSpec) S3Membership {
        var self: S3Membership = .{ .ctx = ctx, .creds = creds, .pool = pool, .specs = specs };
        // The fastest footer round trip: slower ones waited on a connection being opened.
        for (specs) |sp| if (sp.rtt_ns > 0) {
            self.rtt_ns = @min(self.rtt_ns orelse sp.rtt_ns, sp.rtt_ns);
        };
        return self;
    }

    fn source(self: *S3Membership) membership.Source {
        return .{ .ctx = self, .vtable = &vtable, .cost = cost };
    }

    const vtable: membership.Source.VTable = .{ .read = read, .cached = cached, .retain = retain, .measured = measured };

    /// A request's fixed cost as the bytes a connection moves meanwhile, from what this query has timed. A round trip
    /// no slower than the calibration's means an in-region client, and the calibration stands: there the first reads
    /// also pay for new connections, which a few hundred kilobytes cannot tell apart from slow bandwidth. A slower one
    /// means a distant link, where what was measured beats any constant.
    fn measured(ctx: *anyopaque) ?membership.Cost {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const rtt = self.rtt_ns orelse return null;
        if (rtt <= default_rtt_ns) return null;
        const rtt_s = @as(f64, @floatFromInt(rtt)) / std.time.ns_per_s;
        var c = cost;
        c.request_bytes = @intFromFloat(@max(4096.0, rtt_s * (self.bytes_per_s orelse default_bytes_per_s)));
        return c;
    }

    /// Learn one connection's bandwidth from a batch of `n` requests moving `bytes` in `ns`, once the round trips are
    /// paid; a batch of small requests shows none.
    fn observe(self: *S3Membership, n: usize, bytes: u64, ns: u64) void {
        if (n == 0) return;
        const lanes = @min(n, READ_CONCURRENCY);
        const rounds = std.math.divCeil(usize, n, READ_CONCURRENCY) catch unreachable;
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
                READ_CONCURRENCY,
                self.profiler,
            );
            self.observe(jobs.items.len, got, @intCast(nowMonoNs() - t0));
        }
    }

    fn cached(ctx: *anyopaque, arena: std.mem.Allocator, s: membership.Span) ?[]const u8 {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return null;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0) return null;
        const len: usize = @intCast(s.end - s.start);
        return cache.getBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, len, arena);
    }

    fn retain(ctx: *anyopaque, s: membership.Span, bytes: []const u8) void {
        const self: *S3Membership = @ptrCast(@alignCast(ctx));
        const cache = self.ctx.meta_cache orelse return;
        const sp = &self.specs[s.file];
        if (sp.etag_len == 0) return;
        cache.putBlob(self.ctx.io, sp.url.bucket, sp.url.key, sp.etag(), s.start, bytes);
    }
};

fn fetchMetaBatch(
    ctx: Context,
    arena: std.mem.Allocator,
    creds: s3.Credentials,
    pool: *s3.Pool(POOL_SIZE),
    specs: []FetchSpec,
    profiler: ?*profile.Profiler,
) !void {
    const TaskCtx = struct {
        ctx: Context,
        creds: s3.Credentials,
        pool: *s3.Pool(POOL_SIZE),
        sp: *FetchSpec,
        profiler: ?*profile.Profiler,
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
    // tail GET with `If-None-Match`. A 304 means the object is
    // unchanged → reuse the cached footer bytes. Skips the head GET and
    // the footer-prefix GET entirely; column ranges still fetch later
    // into compact buffers.
    //
    // On 200/206 the object has changed — the cached entry is stale,
    // we proceed exactly as the cold path. The fresh entry is
    // installed at the end of the cold path.
    if (t.ctx.meta_cache) |cache| {
        if (cache.get(t.ctx.io, sp.url.bucket, sp.url.key)) |entry| {
            const t_cond = nowMonoNs();
            const cond = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
                .range = s3.Range.suffix(TAIL_SIZE),
                .if_none_match = entry.etag,
                .prof = t.prof,
            });
            t.cache = if (cond.status == 304) 1 else 2;
            if (cond.status == 304) {
                sp.rtt_ns = @intCast(nowMonoNs() - t_cond);
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
    }

    // Cold path: normal tail/head/footer-prefix fetches.
    const tail_resp = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
        .range = s3.Range.suffix(TAIL_SIZE),
        .prof = t.prof,
    });
    if (tail_resp.status != 206 and tail_resp.status != 200) return error.BadResponse;
    sp.noteEtag(tail_resp.header("ETag"));
    try populateFromTailResponse(t, scratch, tail_resp);
    try maybeCacheMeta(t.ctx, sp, tail_resp.header("ETag"));
}

/// Shared "we have the tail response, finish the metadata fetch" path
/// used by both the cold and (cache-stale → 200) paths. On entry,
/// `sp` is empty; on success, `total_size`/`tail_start` are populated
/// and `footer_region` contains `[footer_offset, total_size)`.
fn populateFromTailResponse(t: anytype, scratch: std.mem.Allocator, tail_resp: http.Response) !void {
    const sp = t.sp;
    sp.total_size = try parseTotalFromContentRange(tail_resp.header("Content-Range"));
    sp.tail_start = sp.total_size - tail_resp.body.len;
    // The response body lives in the per-task scratch arena, which is
    // freed when the fetch task returns — but openInputs reads sp.tail
    // later to materialize tail-resident chunk bytes without re-fetching.
    // Copy it somewhere stable; openInputs frees it after the copies.
    sp.tail = try t.ctx.gpa.dupe(u8, tail_resp.body);
    errdefer {
        t.ctx.gpa.free(sp.tail);
        sp.tail = &.{};
    }

    if (tail_resp.body.len < 8) return error.TailTooSmall;
    const tail = tail_resp.body;
    if (!std.mem.eql(u8, tail[tail.len - 4 ..], "PAR1")) return error.NotParquet;
    const footer_len: u64 = std.mem.readInt(u32, tail[tail.len - 8 ..][0..4], .little);
    const footer_actual_start = sp.total_size - 8 - footer_len;

    // Head: first 8 bytes (PAR1 magic). The Parquet spec requires
    // PAR1 at both offset 0 and at the file end; we already
    // validated the trailing PAR1 above. The previous spike
    // synthesized the leading magic to skip this RT, but that made
    // S3 reads more lenient than the spec — a corrupt object with
    // a forged-looking header could pass our gate. Pay the RT and
    // validate honestly.
    const t_head = nowMonoNs();
    const head = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
        .range = s3.Range.span(0, 7),
        .prof = t.prof,
    });
    sp.rtt_ns = @intCast(nowMonoNs() - t_head);
    if (head.status != 206) return error.BadResponse;
    if (head.body.len < PAR1.len or !std.mem.eql(u8, head.body[0..PAR1.len], &PAR1)) return error.NotParquet;

    const footer_region_len: usize = @intCast(footer_len + 8);
    const footer_region = try t.ctx.gpa.alloc(u8, footer_region_len);
    sp.footer_region = footer_region;
    sp.footer_offset = footer_actual_start;

    // Footer: if it doesn't all fit in the tail we just fetched, fetch
    // the part we're missing.
    if (footer_actual_start < sp.tail_start) {
        const need = try s3.getViaPoolWithOpts(t.ctx.io, t.pool, scratch, t.creds, sp.url, .{
            .range = s3.Range.span(footer_actual_start, sp.tail_start - 1),
            .prof = t.prof,
        });
        if (need.status != 206) return error.BadResponse;
        const missing_len: usize = @intCast(sp.tail_start - footer_actual_start);
        if (need.body.len != missing_len) return error.BadResponse;
        @memcpy(footer_region[0..missing_len], need.body);
        @memcpy(footer_region[missing_len..], tail[0 .. footer_region_len - missing_len]);
    } else {
        const tail_off: usize = @intCast(footer_actual_start - sp.tail_start);
        @memcpy(footer_region, tail[tail_off .. tail_off + footer_region_len]);
    }

    const parsed_footer_len = std.mem.readInt(u32, footer_region[footer_region.len - 8 ..][0..4], .little);
    if (parsed_footer_len != footer_len) return error.BadResponse;
    if (!std.mem.eql(u8, footer_region[footer_region.len - 4 ..], &PAR1)) return error.NotParquet;
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

/// Offset given to a chunk deliberately left unfetched. Far past any compact buffer, so a consumer that reads the chunk
/// anyway fails its bounds check (MissingChunkBytes) instead of decoding whatever bytes a stale offset lands on.
const UNFETCHED_CHUNK_OFFSET: i64 = std.math.maxInt(i64) / 4;

/// Point `meta`'s chunk offsets into the compact buffer `ranges` describe. `fetched` is every column some row group
/// fetched (`RangePlan.columns`) and `rg_columns` what each row group fetched (null where statistics pruned it).
fn rebaseFetchedOffsets(
    meta: *schema.FileMetaData,
    fetched: []const bool,
    ranges: []const CompactRange,
    rg_columns: []const ?[]const bool,
) !void {
    std.debug.assert(rg_columns.len == meta.row_groups.items.len);
    // Page-index blocks are never fetched, so their offsets have nothing to rebase onto — yet consumers bounds-check
    // them against the compact buffer, not the file. In a noncanonical file a stale offset can land inside that buffer,
    // where unrelated bytes may decode as a ColumnIndex whose foreign min/max prune pages on read and get copied into
    // output on write. Pruned row groups are cleared too: the metadata no longer describes the source layout.
    for (meta.row_groups.items) |*rg| {
        for (rg.columns.items) |*chunk| {
            chunk.column_index_offset = null;
            chunk.column_index_length = null;
            chunk.offset_index_offset = null;
            chunk.offset_index_length = null;
            // A Bloom filter fetched for a byte copy moves into the buffer; any other no longer describes it. (Probing
            // is over: it read the filters through their source offsets before this.)
            if (chunk.meta_data) |*cm| {
                cm.bloom_filter_offset = rebaseContained(cm.bloom_filter_offset, cm.bloom_filter_length, ranges);
                if (cm.bloom_filter_offset == null) cm.bloom_filter_length = null;
            }
        }
    }

    for (meta.row_groups.items, rg_columns) |*rg, rg_cols_opt| {
        // Pruned row groups contributed no fetch ranges, so their chunk
        // offsets can't be rebased — and never need to be: the scan
        // workers re-prune with the same stats predicate before touching
        // any offset in these groups.
        const rg_cols = rg_cols_opt orelse continue;
        for (rg.columns.items, 0..) |*chunk, ci| {
            if (ci >= fetched.len or !fetched[ci]) continue;
            // Fetched elsewhere but not in this row group: the filter-only chunk of one proven fully matching.
            if (!rg_cols[ci]) {
                if (chunk.meta_data) |*cm| {
                    cm.data_page_offset = UNFETCHED_CHUNK_OFFSET;
                    cm.dictionary_page_offset = null;
                    cm.index_page_offset = null;
                }
                chunk.file_offset = UNFETCHED_CHUNK_OFFSET;
                continue;
            }
            if (chunk.meta_data) |*cm| {
                cm.data_page_offset = try rebaseOneOffset(cm.data_page_offset, ranges);
                if (cm.dictionary_page_offset) |d| {
                    cm.dictionary_page_offset = try rebaseOneOffset(d, ranges);
                }
                if (cm.index_page_offset) |d| {
                    cm.index_page_offset = rebaseOneOffset(d, ranges) catch null;
                }
            }
            if (chunk.meta_data) |cm| chunk.file_offset = cm.data_page_offset;
        }
    }
}

/// Where `[off, off + len)` of the source lands in the compact buffer, when one range holds all of it.
fn rebaseContained(off_opt: ?i64, len_opt: ?i32, ranges: []const CompactRange) ?i64 {
    const off = std.math.cast(u64, off_opt orelse return null) orelse return null;
    const len = std.math.cast(u64, len_opt orelse return null) orelse return null;
    for (ranges) |r| {
        if (off >= r.start and off + len <= r.end) return @intCast(@as(u64, @intCast(r.dst_start)) + (off - r.start));
    }
    return null;
}

fn rebaseOneOffset(old: i64, ranges: []const CompactRange) !i64 {
    if (old < 0) return error.BadResponse;
    const src: u64 = @intCast(old);
    for (ranges) |r| {
        if (src >= r.start and src < r.end) {
            const dst: u64 = @as(u64, @intCast(r.dst_start)) + (src - r.start);
            return @intCast(dst);
        }
    }
    return error.BadResponse;
}

fn parseTotalFromContentRange(hdr: ?[]const u8) !u64 {
    const v = hdr orelse return error.BadResponse;
    // Format: "bytes start-end/total"
    const slash = std.mem.indexOfScalar(u8, v, '/') orelse return error.BadResponse;
    return std.fmt.parseInt(u64, std.mem.trim(u8, v[slash + 1 ..], " \t"), 10) catch error.BadResponse;
}

// ============================================================
// 1-row aggregate output (local files)
// ============================================================

/// Write a 1-row aggregate parquet to a local path. Kept in the engine so
/// both binaries can call it.
fn writeOneRowParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    agg_calls: []const expr_agg.AggCall,
    accumulators: []const expr_agg.Accumulator,
    sources: []const ?schema.SchemaElement,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) !u64 {
    const fd = try createFile(out_path);
    defer local_fs.close(fd);
    errdefer local_fs.removeFile(out_path);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };

    var all_outs: std.ArrayList(expr_agg.OutputCol) = .empty;
    for (agg_calls, 0..) |call, i| {
        const cols = try expr_agg.finalize(arena, call, accumulators[i]);
        for (cols) |c| try all_outs.append(arena, if (sources[i]) |src| try retypeOutputCol(arena, c, src) else c);
    }
    const written = try consumer.writeAggregateRows(arena, sink, all_outs.items, 1, codec, zstd);
    return written;
}

/// Write GROUP BY results as parquet: one row per group, columns in `cols` order, values exactly as the JSON output
/// reports them. avg splits into `<name>__sum` / `<name>__count` as in the 1-row output.
fn writeGroupedParquet(
    arena: std.mem.Allocator,
    out_path: []const u8,
    cols: []const []const u8,
    types: []const scan.GroupColType,
    rows: []const []const scan.AggValue,
    codec: schema.CompressionCodec,
    zstd: compression.ZstdPlan,
) !u64 {
    var outs: std.ArrayList(expr_agg.OutputCol) = .empty;
    for (cols, types, 0..) |name, ty, ci| switch (ty.lane) {
        inline .i64, .f64, .string => |lane| {
            const T, const phys: schema.Type = switch (lane) {
                .i64 => .{ i64, .INT64 },
                .f64 => .{ f64, .DOUBLE },
                .string => .{ []const u8, .BYTE_ARRAY },
                else => unreachable,
            };
            const raw_u64 = if (ty.source) |src| schema.isUnsignedInt64(src) else false;
            const col = try groupedColumn(T, arena, name, ty.nullable, rows, ci, phys, raw_u64);
            try outs.append(arena, if (ty.source) |src| try retypeOutputCol(arena, col, src) else col);
        },
        .avg => {
            const sum_name = try std.fmt.allocPrint(arena, "{s}__sum", .{name});
            const count_name = try std.fmt.allocPrint(arena, "{s}__count", .{name});
            try outs.append(arena, try groupedColumn(f64, arena, sum_name, true, rows, ci, .DOUBLE, false));
            try outs.append(arena, try groupedColumn(i64, arena, count_name, false, rows, ci, .INT64, false));
        },
    };

    const fd = try createFile(out_path);
    defer local_fs.close(fd);
    var fd_sink: FdSink = .{ .fd = fd };
    const sink: streaming.Sink = .{ .ctx = @ptrCast(&fd_sink), .write_fn = FdSink.writeFn };
    return consumer.writeAggregateRows(arena, sink, outs.items, @intCast(rows.len), codec, zstd);
}

/// Give an aggregate output column the parquet type of the source column its values pass through from unchanged (a
/// bare GROUP BY key, min/max of a column), rather than its evaluation lane's plain INT64 / DOUBLE / BYTE_ARRAY: a
/// DATE comes back a DATE, a string a STRING, an INT8 an INT8. DECIMAL and FLOAT16 stay DOUBLE, the f64 values the
/// lane holds; INT96 becomes INT64 TIMESTAMP(NANOS), the value zpq decodes it to; fixed-length bytes stay plain
/// BYTE_ARRAY.
fn retypeOutputCol(arena: std.mem.Allocator, out: expr_agg.OutputCol, src: schema.SchemaElement) !expr_agg.OutputCol {
    if (decimal_mod.kindFromSchema(&src) != null or schema.isFloat16(src)) return out;
    var res = out;
    res.logical_type = src.logical_type;
    res.converted_type = src.converted_type;
    switch (src.type orelse return out) {
        .INT64, .DOUBLE, .BYTE_ARRAY => {},
        .INT96 => {
            res.logical_type = .{ .TIMESTAMP = .{ .isAdjustedToUTC = false, .unit = .{ .NANOS = .{} } } };
            res.converted_type = null;
        },
        .FIXED_LEN_BYTE_ARRAY => return out,
        .INT32 => {
            const c = out.col.i64;
            const vals = try arena.alloc(i32, c.values.len);
            // Low 32 bits: exact for signed values, and the stored form of UINT_32 values above 2^31.
            for (c.values, vals) |v, *o| o.* = @bitCast(@as(u32, @truncate(@as(u64, @bitCast(v)))));
            res.col = .{ .i32 = sameNulls(i32, c, vals) };
            res.parquet_type = .INT32;
        },
        .FLOAT => {
            const c = out.col.f64;
            const vals = try arena.alloc(f32, c.values.len);
            for (c.values, vals) |v, *o| o.* = @floatCast(v);
            res.col = .{ .f32 = sameNulls(f32, c, vals) };
            res.parquet_type = .FLOAT;
        },
        .BOOLEAN => {
            const c = out.col.i64;
            const vals = try arena.alloc(bool, c.values.len);
            for (c.values, vals) |v, *o| o.* = v != 0;
            res.col = .{ .boolean = sameNulls(bool, c, vals) };
            res.parquet_type = .BOOLEAN;
        },
    }
    return res;
}

/// `values` under `src`'s null layout.
fn sameNulls(comptime T: type, src: anytype, values: []const T) filter_eval.ColumnT(T) {
    return .{ .values = values, .def_levels = src.def_levels, .max_def = src.max_def, .has_nulls = src.has_nulls };
}

/// Column `ci` of every grouped row as a `T` lane. For an avg column, `T == f64` takes the pair's sum and
/// `T == i64` its count.
fn groupedColumn(
    comptime T: type,
    arena: std.mem.Allocator,
    name: []const u8,
    nullable: bool,
    rows: []const []const scan.AggValue,
    ci: usize,
    parquet_type: schema.Type,
    /// Values are unsigned 64-bit: store values of 2^63 and up as their raw bits (the column is annotated unsigned).
    raw_u64: bool,
) !expr_agg.OutputCol {
    const values = try arena.alloc(T, rows.len);
    const defs = try arena.alloc(u32, rows.len);
    var has_nulls = false;
    for (rows, values, defs) |row, *v, *d| {
        const cell: ?T = switch (row[ci]) {
            .null_val => null,
            .i => |x| if (T != i64)
                return error.TypeMismatch
            else if (raw_u64 and x >= 0 and x <= std.math.maxInt(u64))
                @bitCast(@as(u64, @intCast(x)))
            else
                std.math.cast(i64, x) orelse return error.AggIntTooWide,
            .f => |x| if (T == f64) x else return error.TypeMismatch,
            .s => |x| if (T == []const u8) x else return error.TypeMismatch,
            .avg => |p| switch (T) {
                f64 => p.sum,
                i64 => p.count,
                else => return error.TypeMismatch,
            },
        };
        v.* = cell orelse std.mem.zeroes(T);
        d.* = @intFromBool(cell != null);
        if (cell == null) has_nulls = true;
    }
    if (has_nulls and !nullable) return error.UnexpectedNull;
    const col: filter_eval.ColumnT(T) = .{
        .values = values,
        .def_levels = if (has_nulls) defs else null,
        .max_def = @intFromBool(nullable),
        .has_nulls = has_nulls,
    };
    return .{
        .name = name,
        .col = switch (T) {
            i64 => .{ .i64 = col },
            f64 => .{ .f64 = col },
            []const u8 => .{ .string = col },
            else => unreachable,
        },
        .parquet_type = parquet_type,
        .nullable = nullable,
    };
}

const FdSink = struct {
    fd: local_fs.fd_t,
    written: u64 = 0,

    pub fn writeFn(ctx: *anyopaque, bytes: []const u8) anyerror!void {
        const self: *FdSink = @ptrCast(@alignCast(ctx));
        try local_fs.writeAll(self.fd, bytes);
        self.written += bytes.len;
    }
};

/// Keeps the engine's `OpenFailed` / `PathTooLong` contract over `local_fs.createFile`'s detailed errors.
fn createFile(path: []const u8) !local_fs.fd_t {
    return local_fs.createFile(path) catch |err| switch (err) {
        error.NameTooLong => error.PathTooLong,
        else => error.OpenFailed,
    };
}

// ============================================================
// Print path - CSV / JSONL streaming row output
// ============================================================

pub const PrintFormat = enum {
    csv,
    jsonl,
};

pub const StdoutWriter = struct {
    fd: local_fs.fd_t = 1,
    buf: [4096]u8 = undefined,
    pos: usize = 0,

    pub fn writeAll(self: *StdoutWriter, bytes: []const u8) !void {
        var i: usize = 0;
        while (i < bytes.len) {
            const space = self.buf.len - self.pos;
            const n = @min(bytes.len - i, space);
            @memcpy(self.buf[self.pos..][0..n], bytes[i..][0..n]);
            self.pos += n;
            i += n;
            if (self.pos == self.buf.len) try self.flush();
        }
    }

    pub fn writeByte(self: *StdoutWriter, c: u8) !void {
        try self.writeAll(&[_]u8{c});
    }

    pub fn print(self: *StdoutWriter, comptime fmt: []const u8, args: anytype) !void {
        var tmp: [512]u8 = undefined;
        const out = try std.fmt.bufPrint(&tmp, fmt, args);
        try self.writeAll(out);
    }

    pub fn flush(self: *StdoutWriter) !void {
        if (self.pos == 0) return;
        const pending = self.buf[0..self.pos];
        self.pos = 0;
        local_fs.writeAll(self.fd, pending) catch return error.BrokenPipe;
    }
};

fn checkNestedListMap(node: schema_tree.Node, is_nested: bool, list_map_cols: []bool) void {
    switch (node) {
        .primitive => |p| {
            if (is_nested) {
                list_map_cols[p.column_index] = true;
            }
        },
        .group => |g| {
            const nested = is_nested or (g.kind == .list or g.kind == .map);
            for (g.children) |child| {
                checkNestedListMap(child, nested, list_map_cols);
            }
        },
    }
}

fn writeFloat(writer: anytype, f: anytype, is_jsonl: bool) !void {
    if (std.math.isFinite(f)) {
        try writer.print("{d}", .{f});
    } else if (std.math.isNan(f)) {
        if (is_jsonl) {
            try writer.writeAll("\"NaN\"");
        } else {
            try writer.writeAll("NaN");
        }
    } else if (f > 0) {
        if (is_jsonl) {
            try writer.writeAll("\"Infinity\"");
        } else {
            try writer.writeAll("Infinity");
        }
    } else {
        if (is_jsonl) {
            try writer.writeAll("\"-Infinity\"");
        } else {
            try writer.writeAll("-Infinity");
        }
    }
}

fn writeDecimal(writer: anytype, unscaled: i128, scale: i32) !void {
    if (scale <= 0) {
        try writer.print("{d}", .{unscaled});
        return;
    }
    const abs_val = @abs(unscaled);
    const sign = if (unscaled < 0) "-" else "";
    const divisor = std.math.pow(u128, 10, @intCast(scale));
    const integer_part = abs_val / divisor;
    const fractional_part = abs_val % divisor;

    try writer.print("{s}{d}.", .{ sign, integer_part });

    var temp = fractional_part;
    var digits: usize = 0;
    if (temp == 0) {
        digits = 1;
    } else {
        while (temp > 0) {
            digits += 1;
            temp /= 10;
        }
    }

    const num_zeros = if (@as(usize, @intCast(scale)) > digits) @as(usize, @intCast(scale)) - digits else 0;
    var z: usize = 0;
    while (z < num_zeros) : (z += 1) {
        try writer.writeByte('0');
    }
    try writer.print("{d}", .{fractional_part});
}

fn writeCsvString(writer: anytype, s: []const u8) !void {
    var needs_quotes = false;
    for (s) |c| {
        if (c == ',' or c == '"' or c == '\n' or c == '\r') {
            needs_quotes = true;
            break;
        }
    }
    if (needs_quotes) {
        try writer.writeByte('"');
        for (s) |c| {
            if (c == '"') {
                try writer.writeAll("\"\"");
            } else {
                try writer.writeByte(c);
            }
        }
        try writer.writeByte('"');
    } else {
        try writer.writeAll(s);
    }
}

/// A JSON number for `f`. Aggregates can be non-finite (a NaN or ±inf input), and JSON has no literal for that: a bare
/// `nan` is invalid and `null` would read as "no value". Write the strings "NaN", "Infinity" and "-Infinity", as
/// `--format jsonl` does and DuckDB/pyarrow surface them.
pub fn writeJsonFloat(writer: anytype, f: f64) !void {
    try writeFloat(writer, f, true);
}

/// One aggregate result as a JSON value. The CLI and Lambda responses both go through here so they escape strings and
/// spell NULL and NaN the same way.
/// `"membership":{...}`: what Bloom filter and dictionary probing read, and the row groups each ruled out beyond
/// statistics (already counted in `row_groups_pruned`).
pub fn writeMembershipJson(writer: anytype, s: membership.Stats) !void {
    try writer.print(
        "\"membership\":{{\"bloom_pruned\":{d},\"dict_pruned\":{d},\"filters\":{d},\"dictionaries\":{d},\"requests\":{d},\"bytes\":{d},\"bloom_stopped\":{},\"dict_stopped\":{},\"request_kb\":{d},\"us\":{d}}}",
        .{
            s.row_groups_bloom,     s.row_groups_dict,         s.filters,       s.dictionaries,
            s.requests,             s.bytes,                   s.bloom_stopped, s.dict_stopped,
            s.request_bytes / 1024, s.ns / std.time.ns_per_us,
        },
    );
}

pub fn writeAggValueJson(writer: anytype, v: scan.AggValue) !void {
    switch (v) {
        .i => |x| try writer.print("{d}", .{x}),
        .f => |x| try writeJsonFloat(writer, x),
        .s => |x| try writeJsonQuoted(writer, x),
        .avg => |pair| {
            try writer.writeAll("{\"sum\":");
            if (pair.sum) |sum| try writeJsonFloat(writer, sum) else try writer.writeAll("null");
            try writer.print(",\"count\":{d}}}", .{pair.count});
        },
        .null_val => try writer.writeAll("null"),
    }
}

/// `s` as a quoted, escaped JSON string.
pub fn writeJsonQuoted(writer: anytype, s: []const u8) !void {
    try writer.writeByte('"');
    try writeJsonString(writer, s);
    try writer.writeByte('"');
}

/// The body of a JSON string: quotes, backslashes and control characters escaped. JSON text must be UTF-8 and parquet
/// strings need not be, so each byte that does not start a valid UTF-8 sequence (stray continuation bytes, bad
/// leads, truncated, overlong or surrogate encodings) is written as U+FFFD, one per byte, as Python's
/// `errors="replace"` and most decoders do for lone bytes. Valid sequences pass through; U+2028/U+2029 are escaped
/// so the output also stays valid JavaScript.
pub fn writeJsonString(writer: anytype, s: []const u8) !void {
    var i: usize = 0;
    while (i < s.len) {
        const c = s[i];
        if (c < 0x80) {
            switch (c) {
                '"' => try writer.writeAll("\\\""),
                '\\' => try writer.writeAll("\\\\"),
                '\n' => try writer.writeAll("\\n"),
                '\r' => try writer.writeAll("\\r"),
                '\t' => try writer.writeAll("\\t"),
                else => if (c < 0x20) {
                    var buf: [8]u8 = undefined;
                    const n = std.fmt.bufPrint(&buf, "\\u{x:0>4}", .{c}) catch unreachable;
                    try writer.writeAll(n);
                } else {
                    try writer.writeByte(c);
                },
            }
            i += 1;
            continue;
        }
        const len = std.unicode.utf8ByteSequenceLength(c) catch 0;
        const cp: ?u21 = if (len == 0 or i + len > s.len) null else std.unicode.utf8Decode(s[i..][0..len]) catch null;
        if (cp) |code| {
            if (code == 0x2028 or code == 0x2029) {
                try writer.print("\\u{x:0>4}", .{code});
            } else {
                try writer.writeAll(s[i..][0..len]);
            }
            i += len;
        } else {
            try writer.writeAll("\u{FFFD}");
            i += 1;
        }
    }
}

fn printCell(
    writer: anytype,
    col: consumer.OutputAggregator.ColumnBuf,
    row_idx: usize,
    elem: schema.SchemaElement,
    is_jsonl: bool,
) !void {
    const scale = elem.scale orelse 0;
    // An unsigned column's lane holds its bits in a signed integer; print the stored value, not the sign bit.
    const unsigned = schema.isUnsignedInt(elem);
    switch (col) {
        .i32 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const v = tb.values.items[row_idx];
                if (unsigned) try writer.print("{d}", .{@as(u32, @bitCast(v))}) else try writer.print("{d}", .{v});
            }
        },
        .i64 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const v = tb.values.items[row_idx];
                if (unsigned) try writer.print("{d}", .{@as(u64, @bitCast(v))}) else try writer.print("{d}", .{v});
            }
        },
        .f32 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeFloat(writer, tb.values.items[row_idx], is_jsonl);
            }
        },
        .f64 => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeFloat(writer, tb.values.items[row_idx], is_jsonl);
            }
        },
        .string => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                const val = tb.values.items[row_idx];
                if (is_jsonl) {
                    try writer.writeByte('"');
                    try writeJsonString(writer, val);
                    try writer.writeByte('"');
                } else {
                    try writeCsvString(writer, val);
                }
            }
        },
        .boolean => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writer.writeAll(if (tb.values.items[row_idx]) "true" else "false");
            }
        },
        .decimal => |tb| {
            if (tb.max_def > 0 and tb.def_levels.items[row_idx] < tb.max_def) {
                try writer.writeAll(if (is_jsonl) "null" else "");
            } else {
                try writeDecimal(writer, tb.values.items[row_idx], scale);
            }
        },
    }
}

fn flushAggregator(
    agg: *consumer.OutputAggregator,
    names: []const []const u8,
    writer: anytype,
    is_jsonl: bool,
    limit: ?usize,
    total_printed: *usize,
) !void {
    if (agg.num_rows == 0) return;

    var row_idx: usize = 0;
    while (row_idx < agg.num_rows) : (row_idx += 1) {
        if (limit) |lim| {
            if (total_printed.* >= lim) break;
        }

        if (is_jsonl) {
            try writer.writeAll("{");
            for (agg.cols, 0..) |col, col_idx| {
                if (col_idx > 0) try writer.writeAll(",");
                try writeJsonQuoted(writer, names[col_idx]);
                try writer.writeByte(':');
                try printCell(writer, col, row_idx, agg.schema_elems[col_idx], true);
            }
            try writer.writeAll("}\n");
        } else {
            for (agg.cols, 0..) |col, col_idx| {
                if (col_idx > 0) try writer.writeAll(",");
                try printCell(writer, col, row_idx, agg.schema_elems[col_idx], false);
            }
            try writer.writeAll("\n");
        }
        total_printed.* += 1;
    }
}

pub fn runPrint(ctx: Context, args: QueryArgs, format: PrintFormat, limit: ?usize) !void {
    runPrintInternal(ctx, args, format, limit) catch |err| {
        if (err == error.BrokenPipe) {
            return;
        }
        return err;
    };
}

fn runPrintInternal(ctx: Context, args: QueryArgs, format: PrintFormat, limit: ?usize) !void {
    const t_start = nowMonoNs();
    const prof = profile.threadOf(args.profiler, .main);
    const query_span = profile.Span.begin(prof);
    defer query_span.end(.query, profile.args(.{}));
    var arena_state = std.heap.ArenaAllocator.init(ctx.gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    if (args.select != null and args.columns != null) return error.AggregateMutexWithSelect;

    const open_span = profile.Span.begin(prof);
    var opened = try openInputs(ctx, arena, args);
    defer opened.deinit();
    open_span.end(.open_inputs, profile.args(.{args.inputs.len}));

    const parse_span = profile.Span.begin(prof);
    const metas = try materializeMetas(arena, opened, args.diag, prof);
    if (metas.len == 0) return;
    const meta0 = &metas[0];

    for (metas[1..]) |*m| {
        if (m.schema.items.len != meta0.schema.items.len) return error.SchemaMismatch;
        for (m.schema.items, meta0.schema.items) |a_e, b_e| {
            if (!std.ascii.eqlIgnoreCase(a_e.name, b_e.name)) return error.SchemaMismatch;
            if ((a_e.num_children orelse 0) != (b_e.num_children orelse 0)) return error.SchemaMismatch;
            if (a_e.type != b_e.type) return error.SchemaMismatch;
            if (a_e.repetition_type != b_e.repetition_type) return error.SchemaMismatch;
            if (a_e.converted_type != b_e.converted_type) return error.SchemaMismatch;
            if (!std.meta.eql(a_e.logical_type, b_e.logical_type)) return error.SchemaMismatch;
            if (a_e.type_length != b_e.type_length) return error.SchemaMismatch;
        }
    }

    const tree0 = try schema_tree.SchemaTree.build(arena, meta0.schema.items);

    var proj = try planProjection(arena, args, meta0);
    const filter_opt = proj.filter;
    const fetch_arr = proj.fetch;
    const output_specs = proj.output_specs;
    _ = try loadLocalMembership(arena, opened, metas, filter_opt, args.scan_all);
    parse_span.end(.parse_query, profile.args(.{}));
    if (args.profiler) |p| p.setColumnsFromMeta(meta0);

    // Check nested columns
    const list_map_cols = try arena.alloc(bool, tree0.leaves.len);
    @memset(list_map_cols, false);
    checkNestedListMap(.{ .group = tree0.root }, false, list_map_cols);

    // Fail loud on nested LIST/MAP columns in output
    for (output_specs) |spec| {
        switch (spec) {
            .passthrough => |ci| {
                if (ci < list_map_cols.len and list_map_cols[ci]) {
                    return error.NestedRowOutputNotSupported;
                }
            },
            .computed => |comp| {
                const comp_cols = try arena.alloc(bool, tree0.leaves.len);
                @memset(comp_cols, false);
                comp.expr.collectColumns(comp_cols);
                for (comp_cols, 0..) |referenced, ci| {
                    if (referenced and ci < list_map_cols.len and list_map_cols[ci]) {
                        return error.NestedRowOutputNotSupported;
                    }
                }
            },
        }
    }

    var agg = try consumer.initOutputAggregator(arena, meta0, output_specs);

    var stdout_writer = StdoutWriter{ .fd = 1 };
    defer stdout_writer.flush() catch {};

    const names = try proj.names(arena);
    const is_jsonl = (format == .jsonl);
    if (!is_jsonl) {
        for (names, 0..) |name, col_idx| {
            if (col_idx > 0) try stdout_writer.writeAll(",");
            try writeCsvString(&stdout_writer, name);
        }
        try stdout_writer.writeAll("\n");
    }

    var total_printed: usize = 0;
    var rows_in: usize = 0;

    var t: Timings = .{};
    t.core.prof = prof;

    for (opened.inputs, metas, 0..) |in, meta_i, file_i| {
        const rg_src: consumer.RGSrc = .{ .bytes = in.bytes, .byte_origin = 0 };
        const meta_const = meta_i;
        for (meta_const.row_groups.items, 0..) |*src_rg, rg_i| {
            if (limit) |lim| {
                if (total_printed >= lim) break;
            }

            rows_in += @intCast(src_rg.num_rows);

            const rg_span = profile.Span.begin(prof);
            var skipped = false;
            defer {
                const decision: u8 = if (skipped) 0 else 2;
                rg_span.end(.row_group, profile.args(.{ file_i, rg_i, decision, src_rg.num_rows }));
            }
            if (filter_opt) |f| if (!args.scan_all) {
                if (filter_prune.pruneRowGroup(src_rg, f, &meta_const) == .skip) {
                    skipped = true;
                    continue;
                }
            };

            _ = try consumer.appendProjectedRGWithOptions(
                &agg,
                ctx.gpa,
                src_rg,
                &meta_const,
                rg_src,
                filter_opt,
                fetch_arr,
                output_specs,
                &t.core,
                .{ .fast_levels = args.fast_levels },
            );

            const print_span = profile.Span.begin(prof);
            try flushAggregator(&agg, names, &stdout_writer, is_jsonl, limit, &total_printed);
            print_span.end(.sink_write, profile.args(.{agg.num_rows}));
            consumer.resetAggregator(&agg);
        }
    }

    // JSON envelope to stderr
    var stderr_writer = StdoutWriter{ .fd = 2 };
    defer stderr_writer.flush() catch {};
    const total_ms = @divTrunc(nowMonoNs() - t_start, std.time.ns_per_ms);
    stderr_writer.print("{{\"ok\":true,\"rows_in\":{d},\"rows_printed\":{d},\"total_ms\":{d}}}\n", .{ rows_in, total_printed, total_ms }) catch {};
}

// ============================================================
// Compile-time API check.
// ============================================================
test "engine: API is well-typed" {
    _ = runQuery;
    _ = runPrint;
}

test "re-encoded FLOAT16 footer widens to DOUBLE" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    var items: std.ArrayListUnmanaged(schema.SchemaElement) = .empty;
    try items.append(arena, .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 2,
        .repetition_type = .OPTIONAL,
        .name = "half",
        .num_children = 0,
        .logical_type = .{ .FLOAT16 = .{} },
        .scale = null,
        .precision = null,
        .field_id = 4,
    });
    try items.append(arena, .{
        .type = .FIXED_LEN_BYTE_ARRAY,
        .type_length = 16,
        .repetition_type = .OPTIONAL,
        .name = "opaque",
        .num_children = 0,
        .scale = null,
        .precision = null,
        .field_id = 5,
    });

    coerceFloat16LeavesToDouble(&items);

    try testing.expectEqual(schema.Type.DOUBLE, items.items[0].type.?);
    try testing.expect(items.items[0].type_length == null);
    try testing.expect(items.items[0].logical_type == null);
    try testing.expectEqual(@as(?i32, 4), items.items[0].field_id);
    try testing.expectEqual(schema.Type.FIXED_LEN_BYTE_ARRAY, items.items[1].type.?);
    try testing.expectEqual(@as(?i32, 16), items.items[1].type_length);
}

fn pageIndexTestChunk(data_page_offset: i64) schema.ColumnChunk {
    return .{
        .file_path = null,
        .file_offset = data_page_offset,
        .meta_data = .{
            .type = .INT32,
            .encodings = .empty,
            .path_in_schema = .empty,
            .codec = .UNCOMPRESSED,
            .num_values = 0,
            .total_uncompressed_size = 0,
            .total_compressed_size = 0,
            .data_page_offset = data_page_offset,
            .index_page_offset = null,
            .dictionary_page_offset = null,
            .statistics = null,
        },
        .column_index_offset = 32,
        .column_index_length = 16,
        .offset_index_offset = 96,
        .offset_index_length = 16,
    };
}

test "rebaseFetchedOffsets clears page-index pointers that would resolve inside the compact buffer" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // The index pointers deliberately fall *inside* the implied 128-byte compact buffer, modelling the noncanonical
    // file this clearing exists for; a conforming writer's offsets would fail bounds anyway.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    for ([_]i64{ 1_000_000, 2_000_000 }) |dpo| {
        var rg: schema.RowGroup = .{
            .columns = .empty,
            .total_byte_size = 0,
            .num_rows = 0,
        };
        const chunk = pageIndexTestChunk(dpo);
        try rg.columns.append(arena, chunk);
        try meta.row_groups.append(arena, rg);
    }

    const ranges = [_]CompactRange{
        .{ .start = 1_000_000, .end = 1_000_064, .dst_start = 0 },
        .{ .start = 2_000_000, .end = 2_000_064, .dst_start = 64 },
    };
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{ &fetch_arr, &fetch_arr };

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns);

    for (meta.row_groups.items, 0..) |rg, i| {
        const chunk = rg.columns.items[0];
        try std.testing.expectEqual(
            @as(i64, @intCast(i * 64)),
            chunk.meta_data.?.data_page_offset,
        );
        try std.testing.expectEqual(@as(?i64, null), chunk.column_index_offset);
        try std.testing.expectEqual(@as(?i32, null), chunk.column_index_length);
        try std.testing.expectEqual(@as(?i64, null), chunk.offset_index_offset);
        try std.testing.expectEqual(@as(?i32, null), chunk.offset_index_length);
    }
}

test "rebaseFetchedOffsets clears page-index pointers in pruned row groups too" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // A pruned row group is skipped by the rebase loop, so clearing that rode along with rebasing would leave its
    // stale pointers.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    var rg: schema.RowGroup = .{
        .columns = .empty,
        .total_byte_size = 0,
        .num_rows = 0,
    };
    var chunk = pageIndexTestChunk(9_000_000);
    chunk.column_index_offset = 128;
    chunk.column_index_length = 32;
    chunk.offset_index_offset = 160;
    try rg.columns.append(arena, chunk);
    try meta.row_groups.append(arena, rg);

    const ranges = [_]CompactRange{};
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{null};

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns);

    const out = meta.row_groups.items[0].columns.items[0];
    try std.testing.expectEqual(@as(?i64, null), out.column_index_offset);
    try std.testing.expectEqual(@as(?i64, null), out.offset_index_offset);
    try std.testing.expectEqual(@as(i64, 9_000_000), out.meta_data.?.data_page_offset);
}

test "rebaseFetchedOffsets poisons filter-only chunks it left unfetched in proven row groups" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();

    // Two row groups x two columns (0 = filter-only, 1 = aggregate input). Only row group 1 is proven, so its column-0
    // chunk was never fetched: a rebase would fail on it, and a stale source offset could land inside the compact
    // buffer and decode unrelated bytes.
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    for ([_][2]i64{ .{ 1_000, 2_000 }, .{ 3_000, 4_000 } }) |offs| {
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 0 };
        for (offs) |o| try rg.columns.append(arena, pageIndexTestChunk(o));
        try meta.row_groups.append(arena, rg);
    }
    const ranges = [_]CompactRange{
        .{ .start = 1_000, .end = 1_064, .dst_start = 0 },
        .{ .start = 2_000, .end = 2_064, .dst_start = 64 },
        .{ .start = 4_000, .end = 4_064, .dst_start = 128 },
    };
    const fetch_arr = [_]bool{ true, true };
    const consumed = [_]bool{ false, true };
    const rg_columns = [_]?[]const bool{ &fetch_arr, &consumed };

    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns);

    const rg0 = meta.row_groups.items[0].columns.items;
    try std.testing.expectEqual(@as(i64, 0), rg0[0].meta_data.?.data_page_offset);
    try std.testing.expectEqual(@as(i64, 64), rg0[1].meta_data.?.data_page_offset);
    const rg1 = meta.row_groups.items[1].columns.items;
    try std.testing.expectEqual(UNFETCHED_CHUNK_OFFSET, rg1[0].meta_data.?.data_page_offset);
    try std.testing.expectEqual(UNFETCHED_CHUNK_OFFSET, rg1[0].file_offset);
    try std.testing.expectEqual(@as(i64, 128), rg1[1].meta_data.?.data_page_offset);
}

test "printCell prints an unsigned column's stored value, not its sign bit" {
    const testing = std.testing;
    var arena_state = std.heap.ArenaAllocator.init(testing.allocator);
    defer arena_state.deinit();
    const a = arena_state.allocator();

    var c32: consumer.OutputAggregator.TypedBuf(i32) = .{};
    try c32.values.append(a, @bitCast(@as(u32, 3_000_000_000)));
    var c64: consumer.OutputAggregator.TypedBuf(i64) = .{};
    try c64.values.append(a, @bitCast(@as(u64, 18_000_000_000_000_000_000)));

    const plain: schema.SchemaElement = .{
        .type = .INT32,
        .type_length = null,
        .repetition_type = .REQUIRED,
        .name = "c",
        .num_children = null,
        .scale = null,
        .precision = null,
        .field_id = null,
    };
    var u32_elem = plain;
    u32_elem.converted_type = .UINT_32;
    var u64_elem = plain;
    u64_elem.type = .INT64;
    u64_elem.logical_type = .{ .INTEGER = .{ .bitWidth = 64, .isSigned = false } };

    var buf: [128]u8 = undefined;
    var w: std.Io.Writer = .fixed(&buf);
    try printCell(&w, .{ .i32 = c32 }, 0, u32_elem, true);
    try w.writeByte(' ');
    try printCell(&w, .{ .i64 = c64 }, 0, u64_elem, false);
    try w.writeByte(' ');
    try printCell(&w, .{ .i32 = c32 }, 0, plain, true);
    try testing.expectEqualStrings("3000000000 18000000000000000000 -1294967296", w.buffered());
}

test "writeJsonString: escapes, and replaces invalid UTF-8 with U+FFFD per byte" {
    const cases = [_]struct { in: []const u8, want: []const u8 }{
        .{ .in = "a\"b\\c\n\x01", .want = "a\\\"b\\\\c\\n\\u0001" },
        .{ .in = "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80", .want = "caf\xc3\xa9 \xe2\x82\xac \xf0\x9f\x98\x80" },
        .{ .in = "\xff\xfeA", .want = "\u{FFFD}\u{FFFD}A" }, // bad lead bytes
        .{ .in = "x\xe2\x80", .want = "x\u{FFFD}\u{FFFD}" }, // truncated at the end
        .{ .in = "\xe2\x80y", .want = "\u{FFFD}\u{FFFD}y" }, // truncated mid-string
        .{ .in = "\xc0\xaf", .want = "\u{FFFD}\u{FFFD}" }, // overlong '/'
        .{ .in = "\xed\xa0\x80", .want = "\u{FFFD}\u{FFFD}\u{FFFD}" }, // UTF-16 surrogate
        .{ .in = "\x80", .want = "\u{FFFD}" }, // lone continuation byte
        .{ .in = "\xe2\x80\xa8|\xe2\x80\xa9", .want = "\\u2028|\\u2029" },
    };
    for (cases) |c| {
        var aw: std.Io.Writer.Allocating = .init(std.testing.allocator);
        defer aw.deinit();
        try writeJsonString(&aw.writer, c.in);
        try std.testing.expectEqualStrings(c.want, aw.written());
        try std.testing.expect(std.unicode.utf8ValidateSlice(aw.written()));
    }
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

test "windowed fan-out runs budget producers at once on pool threads beside a writer that holds its thread" {
    const Fake = struct {
        const Self = @This();
        const writer = fakeWriter;
        const producer = fakeProducer;

        gpa: std.mem.Allocator,
        io: std.Io = undefined,
        window: *std.Io.Semaphore,
        cpu: std.Io.Semaphore = .{},
        first_err: ?anyerror = null,
        budget: usize,
        n: usize,
        dispatcher: std.Thread.Id,
        finished: std.Io.Semaphore = .{},
        active: std.atomic.Value(usize) = .init(0),
        peak: std.atomic.Value(usize) = .init(0),
        inline_runs: std.atomic.Value(usize) = .init(0),

        fn fakeWriter(self: *Self) void {
            for (0..self.n) |_| {
                self.finished.waitUncancelable(self.io);
                self.window.post(self.io);
            }
        }

        fn fakeProducer(self: *Self, i: usize) void {
            if (std.Thread.getCurrentId() == self.dispatcher) _ = self.inline_runs.fetchAdd(1, .monotonic);
            _ = self.peak.fetchMax(self.active.fetchAdd(1, .acq_rel) + 1, .monotonic);
            // The first `budget` producers wait to see one another: one run inline on the dispatching thread stops
            // admission, so the rest never start and the wait times out short of the budget. The rest stand for real
            // row groups, which take far longer than a pool thread does to come back after releasing its permit.
            if (i < self.budget) {
                var waited: usize = 0;
                while (self.active.load(.acquire) < self.budget and waited < 2000) : (waited += 1)
                    self.io.sleep(.fromMilliseconds(1), .awake) catch {};
            } else self.io.sleep(.fromMilliseconds(10), .awake) catch {};
            _ = self.active.fetchSub(1, .acq_rel);
            self.finished.post(self.io);
        }
    };

    for ([_]usize{ 1, 2, 3 }) |budget| {
        var window: std.Io.Semaphore = .{ .permits = 4 };
        var f: Fake = .{
            .gpa = std.testing.allocator,
            .window = &window,
            .budget = budget,
            .n = 12,
            .dispatcher = std.Thread.getCurrentId(),
        };
        try runWindowed(Fake, &f, f.n, budget);
        try std.testing.expectEqual(@as(usize, 0), f.inline_runs.load(.monotonic));
        try std.testing.expectEqual(budget, f.peak.load(.monotonic));
    }
}

test "windowed re-encode writes the same bytes at every parallelism" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    // "ts >= 500" prunes two row groups, filters one and byte-copies the rest; "x > 0" re-encodes all eight.
    for ([_][]const u8{ "ts >= 500", "x > 0" }) |filter| {
        var first: ?[]u8 = null;
        defer if (first) |b| gpa.free(b);
        for ([_]usize{ 1, 2, 3, 8 }) |j| {
            var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
                .inputs = &.{fixture},
                .output = out,
                .filter = filter,
                .parallelism = j,
            });
            defer r.deinit(gpa);
            const bytes = try metadata.readFileSlice(out, gpa);
            if (first) |b| {
                defer gpa.free(bytes);
                try std.testing.expectEqualSlices(u8, b, bytes);
            } else first = bytes;
        }
    }
}

test "a recompressing copy writes the same bytes at every parallelism and reads back the same answer" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});
    const agg = "count(*) AS n, sum(x) AS sx, max(s) AS ms, sum(ts) AS st";

    var want = try runQuery(ctx, .{ .inputs = &.{fixture}, .aggregate = agg });
    defer want.aggregate.deinit(gpa);

    // The fixture is SNAPPY: every chunk changes codec, with and without a projection.
    const all: ?[]const []const u8 = null;
    const projections = [_]?[]const []const u8{ all, &.{ "ts", "x", "s" } };
    for (projections) |columns| for ([_]schema.CompressionCodec{ .ZSTD, .UNCOMPRESSED }) |codec| {
        var first: ?[]u8 = null;
        defer if (first) |b| gpa.free(b);
        for ([_]usize{ 1, 2, 3, 8 }) |j| {
            var r = try runQuery(ctx, .{
                .inputs = &.{fixture},
                .output = out,
                .columns = columns,
                .codec = codec,
                .parallelism = j,
            });
            defer r.deinit(gpa);
            try std.testing.expectEqual(@as(?schema.CompressionCodec, codec), r.write.codec);
            try std.testing.expectEqual(@as(usize, 8), r.write.row_groups_kept);
            const bytes = try metadata.readFileSlice(out, gpa);
            if (first) |b| {
                defer gpa.free(bytes);
                try std.testing.expectEqualSlices(u8, b, bytes);
            } else first = bytes;
        }
        var got = try runQuery(ctx, .{ .inputs = &.{out}, .aggregate = agg });
        defer got.aggregate.deinit(gpa);
        for (want.aggregate.output.aggs, got.aggregate.output.aggs) |w, g| switch (w.value) {
            .s => |ws| try std.testing.expectEqualStrings(ws, g.value.s),
            else => try std.testing.expectEqualDeep(w.value, g.value),
        };
    };
}

test "a select output naming one column twice is rejected and named, like aggregate outputs" {
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };

    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    const Case = struct { select: []const u8, column: []const u8 };
    const cases = [_]Case{
        .{ .select = "x AS v, ts AS v", .column = "v" },
        // Case-insensitive, as the aggregate rule is; a passthrough column keeps its own name.
        .{ .select = "x AS v, ts AS V", .column = "V" },
        .{ .select = "s, x AS S", .column = "S" },
    };
    for (cases) |c| {
        // The write, and both printed formats.
        var diag: Diag = .{};
        try std.testing.expectError(error.DuplicateOutputColumn, runQuery(ctx, .{
            .inputs = &.{fixture},
            .output = out,
            .select = c.select,
            .diag = &diag,
        }));
        try std.testing.expectEqualStrings(c.column, diag.column.get());
        for ([_]PrintFormat{ .csv, .jsonl }) |fmt| {
            var print_diag: Diag = .{};
            try std.testing.expectError(error.DuplicateOutputColumn, runPrint(ctx, .{
                .inputs = &.{fixture},
                .select = c.select,
                .diag = &print_diag,
            }, fmt, 0));
            try std.testing.expectEqualStrings(c.column, print_diag.column.get());
        }
    }
    // Nothing was written.
    try std.testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(std.testing.io, out, .{}));

    // The SQL front end's SELECT list reaches the same check.
    const sql_parser = if (@import("build_options").enable_sql) @import("core/expr/sql_parser.zig") else return;
    const q = try sql_parser.parseSqlQuery(gpa, "SELECT x AS v, ts AS v FROM 't.parquet'");
    defer q.deinit(gpa);
    var diag: Diag = .{};
    try std.testing.expectError(error.DuplicateOutputColumn, runQuery(ctx, .{
        .inputs = &.{fixture},
        .output = out,
        .select = q.select_str,
        .diag = &diag,
    }));
    try std.testing.expectEqualStrings("v", diag.column.get());
}

test "an S3 query with no region says so rather than blaming the credentials" {
    const gpa = std.testing.allocator;
    const Case = struct { env: []const ?[*:0]const u8, want: anyerror };
    const cases = [_]Case{
        // Anonymous requests read no keys, so only the region can be missing. (Signed requests reach the same
        // `credentialsFromEnv`; `s3.Credentials.fromEnv`'s own tests cover their missing region.)
        .{ .env = &.{"S3_NO_SIGN_REQUEST=1"}, .want = error.NoRegion },
        .{ .env = &.{"AWS_REGION=us-west-2"}, .want = error.NoCredentials },
    };
    for (cases) |c| {
        const block = try gpa.allocSentinel(?[*:0]const u8, c.env.len, null);
        defer gpa.free(block);
        @memcpy(block, c.env);
        const env: std.process.Environ = .{ .block = .{ .slice = block } };
        try std.testing.expectError(c.want, runQuery(.{ .gpa = gpa, .env = env, .io = std.testing.io }, .{
            .inputs = &.{"s3://bucket/key.parquet"},
            .aggregate = "count(*) AS n",
        }));
    }
}

// ── Profiling ──

const profile_summary = @import("profile_summary.zig");

/// Check `p`'s trace the way a viewer reads it: valid JSON, every slice complete, and each thread's slices nested
/// (each starts after the enclosing one ends, or ends within it). Returns how many slices each kind has.
fn checkTraceNests(gpa: std.mem.Allocator, p: *profile.Profiler) !std.enums.EnumArray(profile.Kind, usize) {
    var aw: std.Io.Writer.Allocating = .init(gpa);
    defer aw.deinit();
    try p.writeTrace(gpa, &aw.writer);
    const parsed = try std.json.parseFromSlice(std.json.Value, gpa, aw.written(), .{});
    defer parsed.deinit();
    const root = parsed.value.object;
    try std.testing.expectEqualStrings("ns", root.get("displayTimeUnit").?.string);
    try std.testing.expect(root.get("otherData").?.object.get("zpq_version") != null);

    const Slice = struct { tid: i64, start: i64, end: i64 };
    var slices: std.ArrayList(Slice) = .empty;
    defer slices.deinit(gpa);
    var counts: std.enums.EnumArray(profile.Kind, usize) = .initFill(0);
    for (root.get("traceEvents").?.array.items) |ev| {
        const o = ev.object;
        const ph = o.get("ph").?.string;
        try std.testing.expect(o.get("pid") != null);
        if (!std.mem.eql(u8, ph, "X")) {
            try std.testing.expect(std.mem.eql(u8, ph, "M") or std.mem.eql(u8, ph, "C"));
            continue;
        }
        const kind = std.meta.stringToEnum(profile.Kind, o.get("name").?.string).?;
        counts.getPtr(kind).* += 1;
        try std.testing.expectEqualStrings(@tagName(kind.phase()), o.get("cat").?.string);
        // Microseconds with three decimals: exact nanoseconds.
        const ts: i64 = @intFromFloat(@round(jsonNumber(o.get("ts").?) * 1000));
        const dur: i64 = @intFromFloat(@round(jsonNumber(o.get("dur").?) * 1000));
        try std.testing.expect(dur >= 0);
        try slices.append(gpa, .{ .tid = o.get("tid").?.integer, .start = ts, .end = ts + dur });
    }
    std.mem.sort(Slice, slices.items, {}, struct {
        fn lt(_: void, a: Slice, b: Slice) bool {
            if (a.tid != b.tid) return a.tid < b.tid;
            if (a.start != b.start) return a.start < b.start;
            return a.end > b.end;
        }
    }.lt);
    var stack: std.ArrayList(Slice) = .empty;
    defer stack.deinit(gpa);
    for (slices.items) |sl| {
        if (stack.items.len > 0 and stack.items[0].tid != sl.tid) stack.clearRetainingCapacity();
        while (stack.items.len > 0 and stack.items[stack.items.len - 1].end <= sl.start) _ = stack.pop();
        if (stack.items.len > 0) {
            const top = stack.items[stack.items.len - 1];
            std.testing.expect(sl.end <= top.end) catch |err| {
                const fmt = "slice [{d},{d}) overlaps [{d},{d}) on tid {d}\n";
                std.debug.print(fmt, .{ sl.start, sl.end, top.start, top.end, sl.tid });
                return err;
            };
        }
        try stack.append(gpa, sl);
    }
    return counts;
}

fn jsonNumber(v: std.json.Value) f64 {
    return switch (v) {
        .float => |f| f,
        .integer => |i| @floatFromInt(i),
        else => std.math.nan(f64),
    };
}

fn expectKinds(counts: std.enums.EnumArray(profile.Kind, usize), kinds: []const profile.Kind) !void {
    for (kinds) |k| std.testing.expect(counts.get(k) > 0) catch |err| {
        std.debug.print("no {s} span in the trace\n", .{@tagName(k)});
        return err;
    };
}

test "a profiled aggregate records every phase, nests per thread, and reconciles with its timings" {
    if (!profile.enabled) return error.SkipZigTest;
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;

    // Unfiltered, filtered (two row groups skipped by statistics, one filtered, five proven to match), grouped.
    const Case = struct { filter: ?[]const u8, group_by: ?[]const u8 = null, aggregate: []const u8 };
    for ([_]Case{
        .{ .filter = null, .aggregate = "count(*) AS n, sum(x) AS sx, max(s) AS ms" },
        .{ .filter = "ts >= 500", .aggregate = "count(*) AS n, sum(x) AS sx" },
        .{ .filter = "x > 0", .group_by = "b", .aggregate = "sum(x) AS sx" },
    }) |c| {
        const p = try profile.Profiler.create(gpa, .{ .sample_interval_ns = 0 }, .{ .query = "test" });
        defer p.destroy();
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{fixture},
            .filter = c.filter,
            .aggregate = c.aggregate,
            .group_by = c.group_by,
            .parallelism = 4,
            .profiler = p,
        });
        defer r.deinit(gpa);
        p.finish();

        const counts = try checkTraceNests(gpa, p);
        try expectKinds(counts, &.{
            .query,       .open_inputs, .open_file, .footer_parse, .parse_query, .plan,      .scan,
            .scan_worker, .row_group,   .decode,    .column_chunk, .decompress,  .aggregate, .merge,
            .finalize,
        });
        if (c.filter != null) try expectKinds(counts, &.{ .filter, .prune_pages });

        const s = try profile_summary.compute(gpa, p);
        const t = r.aggregate.timings;
        // Same timestamps, so equal to the nanosecond.
        try std.testing.expectEqual(t.read_ns, s.reconcile.read_ns);
        try std.testing.expectEqual(t.parse_ns, s.reconcile.parse_ns);
        try std.testing.expectEqual(t.core.decode_ns, s.reconcile.decode_ns);
        try std.testing.expectEqual(t.core.eval_ns, s.reconcile.eval_ns);
        try std.testing.expectEqual(t.core.encode_ns, s.reconcile.agg_ns);
        try std.testing.expectEqual(t.decode_wall_ns, s.reconcile.decode_wall_ns);
        try std.testing.expectEqual(@as(u64, 8), s.prune.row_groups);
        try std.testing.expectEqual(@as(u64, r.aggregate.row_groups_pruned), s.prune.skipped);
        try std.testing.expectEqual(@as(u64, r.aggregate.row_groups_full_match), s.prune.full_match);
        if (r.aggregate.row_groups_pruned > 0) {
            try std.testing.expectEqual(@as(usize, 1), s.prune.n_reasons);
            try std.testing.expectEqualStrings("ts", p.columnName(s.prune.reasons[0].col));
        }
        // The wall shares account for every nanosecond of the query.
        var share: u64 = s.idle_ns;
        for (s.phases) |ph| share += ph.share_ns;
        try std.testing.expect(share + 16 >= s.total_ns and share <= s.total_ns + 16);
        try std.testing.expectEqual(@as(u64, 0), s.dropped);
    }
}

test "a profiled write records encode and sink spans and reconciles with its timings" {
    if (!profile.enabled) return error.SkipZigTest;
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    for ([_][]const u8{ "x > 0", "ts >= 500" }) |filter| {
        const p = try profile.Profiler.create(gpa, .{ .sample_interval_ns = 0 }, .{});
        defer p.destroy();
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{fixture},
            .output = out,
            .filter = filter,
            .parallelism = 3,
            .profiler = p,
        });
        defer r.deinit(gpa);
        p.finish();
        const counts = try checkTraceNests(gpa, p);
        try expectKinds(counts, &.{
            .query, .open_inputs, .footer_parse, .parse_query, .reencode, .encode_rg, .footer_write, .sink_write,
        });
        if (std.mem.eql(u8, filter, "x > 0")) try expectKinds(counts, &.{
            .decode, .column_chunk, .filter, .project, .encode, .encode_chunk,
        });
        const s = try profile_summary.compute(gpa, p);
        const t = r.write.timings;
        try std.testing.expectEqual(t.read_ns, s.reconcile.read_ns);
        try std.testing.expectEqual(t.parse_ns, s.reconcile.parse_ns);
        try std.testing.expectEqual(t.core.decode_ns, s.reconcile.decode_ns);
        try std.testing.expectEqual(t.core.eval_ns, s.reconcile.eval_ns);
        try std.testing.expectEqual(t.core.encode_ns, s.reconcile.encode_ns);
    }
}

test "profiling stays well-formed and complete under -j N, run after run" {
    if (!profile.enabled) return error.SkipZigTest;
    const fixture = "ci/fixtures/parquet/full_match.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    for (0..50) |i| {
        const p = try profile.Profiler.create(gpa, .{ .sample_interval_ns = if (i % 2 == 0) 100_000 else 0 }, .{});
        defer p.destroy();
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{ fixture, fixture, fixture },
            .filter = "x > 0",
            .aggregate = "count(*) AS n, sum(x) AS sx, min(s) AS ms",
            .parallelism = 2 + i % 7,
            .profiler = p,
        });
        defer r.deinit(gpa);
        p.finish();
        const counts = try checkTraceNests(gpa, p);
        // One row_group span per work item, one decode per row group a work item read.
        try std.testing.expect(counts.get(.row_group) >= 24);
        try std.testing.expectEqual(counts.get(.decode), counts.get(.aggregate));
        const s = try profile_summary.compute(gpa, p);
        try std.testing.expectEqual(@as(u64, 24), s.prune.row_groups);
        try std.testing.expectEqual(r.aggregate.timings.core.decode_ns, s.reconcile.decode_ns);
        try std.testing.expectEqual(@as(u64, 0), s.dropped);
    }
}

test "rebaseFetchedOffsets moves a fetched Bloom filter into the compact buffer and clears any other" {
    const gpa = std.testing.allocator;
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    var meta: schema.FileMetaData = .{
        .version = 1,
        .schema = .empty,
        .num_rows = 0,
        .created_by = null,
        .row_groups = .empty,
    };
    // Row group 0's filter was fetched whole; row group 1's was not; row group 2's half was, and it is pruned.
    for ([_]i64{ 1_000_000, 2_000_000, 3_000_000 }, [_]i64{ 9_000_016, 9_500_000, 9_000_040 }) |dpo, bfo| {
        var rg: schema.RowGroup = .{ .columns = .empty, .total_byte_size = 0, .num_rows = 0 };
        var chunk = pageIndexTestChunk(dpo);
        chunk.meta_data.?.bloom_filter_offset = bfo;
        chunk.meta_data.?.bloom_filter_length = 16;
        try rg.columns.append(arena, chunk);
        try meta.row_groups.append(arena, rg);
    }
    const ranges = [_]CompactRange{
        .{ .start = 1_000_000, .end = 1_000_064, .dst_start = 0 },
        .{ .start = 2_000_000, .end = 2_000_064, .dst_start = 64 },
        .{ .start = 9_000_000, .end = 9_000_048, .dst_start = 128 },
    };
    const fetch_arr = [_]bool{true};
    const rg_columns = [_]?[]const bool{ &fetch_arr, &fetch_arr, null };
    try rebaseFetchedOffsets(&meta, &fetch_arr, &ranges, &rg_columns);
    const got = [_]?i64{ 144, null, null };
    for (meta.row_groups.items, got) |rg, want| {
        const cm = rg.columns.items[0].meta_data.?;
        try std.testing.expectEqual(want, cm.bloom_filter_offset);
        try std.testing.expectEqual(@as(?i32, if (want != null) 16 else null), cm.bloom_filter_length);
    }
}

test "a write builds the Bloom filters asked for, in re-encoded and copied row groups alike" {
    const fixture = "ci/fixtures/parquet/bloom_keys.parquet";
    std.Io.Dir.cwd().access(std.testing.io, fixture, .{}) catch return error.SkipZigTest;
    const gpa = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{});
    defer tmp.cleanup();
    var path_buf: [96]u8 = undefined;
    const out = try std.fmt.bufPrint(&path_buf, ".zig-cache/tmp/{s}/out.parquet", .{tmp.sub_path});

    // `id >= 2` re-encodes row group 0 (it holds id 0) and copies the rest, which carry `id` and `s` filters of their
    // own; `cat` and `x` have none anywhere. The bytes must not depend on parallelism.
    var first: ?[]u8 = null;
    defer if (first) |b| gpa.free(b);
    for ([_]usize{ 1, 4 }) |j| {
        var r = try runQuery(.{ .gpa = gpa, .env = .empty, .io = std.testing.io }, .{
            .inputs = &.{fixture},
            .output = out,
            .filter = "id >= 2",
            .parallelism = j,
            .bloom_filters = &.{ .{ .column = "cat" }, .{ .column = "X", .fpp = 0.001 } },
        });
        defer r.deinit(gpa);
        const bytes = try metadata.readFileSlice(out, gpa);
        if (first) |b| {
            defer gpa.free(bytes);
            try std.testing.expectEqualSlices(u8, b, bytes);
        } else first = bytes;
    }
    var arena_state = std.heap.ArenaAllocator.init(gpa);
    defer arena_state.deinit();
    const arena = arena_state.allocator();
    const bytes = first.?;
    const meta = try metadata.open(arena, bytes);
    const bloom = sbbf;
    var key_buf: [32]u8 = undefined;
    for (meta.row_groups.items, 0..) |rg, g| {
        const has = [_]bool{ g > 0, g > 0, true, true }; // id, s carried where copied; cat, x built everywhere
        for (rg.columns.items, has) |c, want| {
            try std.testing.expectEqual(want, c.meta_data.?.bloom_filter_offset != null);
        }
        const cat_bits = bloom.bitset(bytes[@intCast(rg.columns.items[2].meta_data.?.bloom_filter_offset.?)..]).?;
        for ([_]u8{ 'a', 'm', 'z' }) |p| {
            const v = try std.fmt.bufPrint(&key_buf, "{c}-{d}", .{ p, g });
            try std.testing.expect(bloom.Probe.init(bloom.hashBytes(v)).mightContain(cat_bits));
        }
        const x_bits = bloom.bitset(bytes[@intCast(rg.columns.items[3].meta_data.?.bloom_filter_offset.?)..]).?;
        for (0..500) |jj| {
            const id = 2 * (8 * jj + g);
            if (id < 2) continue;
            const x: f64 = @floatFromInt(id / 2);
            const xb = std.mem.toBytes(std.mem.nativeToLittle(u64, @bitCast(x)));
            try std.testing.expect(bloom.Probe.init(bloom.hashBytes(&xb)).mightContain(x_bits));
        }
    }

    // Asking for a filter on a column the output lacks, or on an aggregate, is an error.
    const ctx: Context = .{ .gpa = gpa, .env = .empty, .io = std.testing.io };
    var diag: Diag = .{};
    try std.testing.expectError(error.UnknownColumn, runQuery(ctx, .{
        .inputs = &.{fixture},
        .output = out,
        .columns = &.{"id"},
        .bloom_filters = &.{.{ .column = "cat" }},
        .diag = &diag,
    }));
    try std.testing.expectEqualStrings("cat", diag.column.get());
    try std.testing.expectError(error.BloomFilterNeedsRows, runQuery(ctx, .{
        .inputs = &.{fixture},
        .aggregate = "count(*) AS n",
        .bloom_filters = &.{.{ .column = "id" }},
    }));
}

test "BloomSpec.parse: a column, optionally with a probability" {
    try std.testing.expectEqualDeep(BloomSpec{ .column = "id" }, try BloomSpec.parse("id"));
    try std.testing.expectEqualDeep(BloomSpec{ .column = "id", .fpp = 0.05 }, try BloomSpec.parse(" id:0.05 "));
    // A suffix that is no number belongs to the name.
    try std.testing.expectEqualDeep(BloomSpec{ .column = "a:b" }, try BloomSpec.parse("a:b"));
    for ([_][]const u8{ "", "id:0", "id:1", "id:-0.1", "id:1.5", ":0.1" }) |bad| {
        try std.testing.expectError(error.BadBloomFilter, BloomSpec.parse(bad));
    }
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
